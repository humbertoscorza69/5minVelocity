//! Websocket ingestion: a generic reconnect supervisor plus the two clients.
//!
//! [`run_with_reconnect`] owns the reconnect policy so the Binance and Polymarket
//! clients only have to implement "one session" (connect → subscribe → read
//! until the link ends).
//!
//! Policy (prudent auto-recovery — the supervisor NEVER gives up):
//!   * FAST regime (ordinary transport faults — reset/EOF/timeout/server close):
//!     exponential backoff 1 → 2 → 4 → 8 → 16 → capped at 30s.
//!   * SLOW regime (rate-limiting — Cloudflare HTTP 403 / 429): a long, patient
//!     backoff 60s → 120s → … → capped at 5 min, so we never hammer an endpoint
//!     that is already throttling us. Entered on a rate-limit failure OR when the
//!     FAST regime exceeds `max_attempts_per_60s` (a reconnect storm). The slow
//!     regime is a soft latch: it persists across attempts until a session
//!     genuinely recovers, so we don't oscillate back to hammering.
//!   * A session that stays up ≥ 60s is "stable" → it counts as recovered:
//!     both backoffs and the attempt window reset, and `health` clears.
//!   * `health_failed` is set while we are in the slow regime (so the dashboard
//!     can see a degraded feed) and AUTO-CLEARS on the next stable session — it
//!     is NOT a permanent latch. The supervisor keeps retrying indefinitely; a
//!     transient throttle (like the 2026-06-19 Cloudflare 403) is ridden out and
//!     recovered from on its own, with no human in the loop.
//!
//! NOTE: this governs only the WS *connection*. Trading-side safety (not acting
//! on a stale book) is the separate feed-dead guard in the decision loop, which
//! reads book freshness — never `health_failed` — so connection backoff never
//! touches trading logic. When the feed returns, the book updates and trading
//! resumes on its own.

pub mod binance;
pub mod polymarket;

use std::collections::VecDeque;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use serde_json::json;
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio_tungstenite::tungstenite::Error as WsError;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use tracing::{error, info, warn};

use crate::state::{Shared, now_ms};

/// How a connect/session failure should be treated by the backoff policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureClass {
    /// Server is rate-limiting us — Cloudflare HTTP 403 or an explicit HTTP 429.
    /// Back off long and patiently; hammering only deepens the block.
    RateLimited,
    /// Ordinary transport fault (connection reset, EOF, read error, connect
    /// timeout, clean server close). Recover fast.
    Transient,
}

/// Classify a [`SessionEnd::Lost`] reason. The only source of a rate-limit
/// signal is the connect path, whose error string we own (see
/// [`connect_err_string`]) — so matching `HTTP 403` / `HTTP 429` is a stable,
/// deterministic marker, not a fragile guess. We also tolerate the raw
/// tungstenite `HTTP error: 403` shape for defence in depth.
pub fn classify_failure(reason: &str) -> FailureClass {
    let hit = |code: &str| {
        reason.contains(&format!("HTTP {code}")) || reason.contains(&format!("error: {code}"))
    };
    if hit("403") || hit("429") {
        FailureClass::RateLimited
    } else {
        FailureClass::Transient
    }
}

/// Why a single websocket session ended.
pub enum SessionEnd {
    /// The connection dropped or errored — reconnect should be attempted (counts
    /// toward the storm budget + exponential backoff).
    Lost(String),
    /// The subscription set changed → reconnect immediately with the new list.
    /// NOT an error: no backoff, and it does not charge the storm budget.
    Refresh,
    /// Graceful shutdown was requested — do not reconnect.
    Shutdown,
}

/// Reconnect tuning. Built from config; backoff curves are fixed by spec.
#[derive(Clone)]
pub struct ReconnectPolicy {
    /// FAST-regime attempts within a rolling 60s window above which we switch to
    /// the SLOW regime (a reconnect storm). No longer a stop condition.
    pub max_attempts_per_60s: u32,
    /// FAST regime floor (ordinary transport faults).
    pub base_backoff: Duration,
    /// FAST regime cap.
    pub max_backoff: Duration,
    /// SLOW regime floor (rate-limit / storm cool-down).
    pub rate_limit_backoff: Duration,
    /// SLOW regime cap.
    pub rate_limit_max_backoff: Duration,
    /// A session up at least this long counts as recovered (resets backoff +
    /// attempt window, clears health).
    pub stable_after: Duration,
}

impl ReconnectPolicy {
    pub fn from_config(max_attempts_per_60s: u32) -> Self {
        Self {
            max_attempts_per_60s,
            base_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(30),
            // Patient curve for a throttling endpoint: 60s → 2m → 4m → cap 5m.
            rate_limit_backoff: Duration::from_secs(60),
            rate_limit_max_backoff: Duration::from_secs(300),
            stable_after: Duration::from_secs(60),
        }
    }
}

/// Supervise `connect_and_run`, reconnecting per [`ReconnectPolicy`].
///
/// `connect_and_run` is called with the current reconnect count (0 on the first
/// connect) and performs exactly one session, returning why it ended. It should
/// itself honor `shutdown` so it can break out of its read loop promptly.
pub async fn run_with_reconnect<F, Fut>(
    name: &'static str,
    policy: ReconnectPolicy,
    state: Shared,
    alert_dir: String,
    mut shutdown: watch::Receiver<bool>,
    mut connect_and_run: F,
) where
    F: FnMut(u32) -> Fut,
    Fut: std::future::Future<Output = SessionEnd>,
{
    let mut fast_backoff = policy.base_backoff;
    let mut slow_backoff = policy.rate_limit_backoff;
    let mut attempts: VecDeque<Instant> = VecDeque::new();
    let mut reconnects: u32 = 0;
    // Set after a planned `Refresh`: the immediate reconnect that follows is not
    // a failure, so it skips the storm-budget accounting below.
    let mut skip_gate = false;
    // Soft latch for the SLOW regime: true while we are patiently riding out a
    // rate-limit / storm. Persists across attempts (so we never oscillate back
    // to hammering) and AUTO-CLEARS on the next stable session. Never stops the
    // loop — unlike the old human-in-loop latch.
    let mut cooling = false;

    // Passive observability (#3): mirror the connection lifecycle into the oplog
    // the dashboard reads. `None` in tests / pure-baseline ⇒ a no-op. This NEVER
    // gates reconnect or trading — it only records what already happened.
    let oplog = state.oplog().cloned();
    let emit = |kind: &str, data: serde_json::Value| {
        if let Some(ol) = &oplog {
            ol.sys(kind, data);
        }
    };

    loop {
        if *shutdown.borrow() {
            info!(ws = name, "shutdown requested before connect; stopping");
            return;
        }

        // Rolling-window storm detector: prune attempts older than 60s, record
        // this one, then flag a storm if we've exceeded the budget. A planned
        // resubscribe (Refresh) bypasses it — not a failure. A storm no longer
        // STOPS the loop; it escalates us into the SLOW regime (cool-down).
        let mut storm = false;
        if !skip_gate {
            let now = Instant::now();
            while let Some(front) = attempts.front() {
                if now.duration_since(*front) > Duration::from_secs(60) {
                    attempts.pop_front();
                } else {
                    break;
                }
            }
            attempts.push_back(now);
            storm = attempts.len() as u32 > policy.max_attempts_per_60s;
        }
        skip_gate = false;

        let session_start = Instant::now();
        let outcome = connect_and_run(reconnects).await;
        let uptime = session_start.elapsed();

        match outcome {
            SessionEnd::Shutdown => {
                info!(ws = name, "session ended for shutdown; stopping");
                return;
            }
            SessionEnd::Refresh => {
                // Planned: the subscription set changed. Reconnect immediately
                // with the new list — no backoff, not charged to the storm budget.
                //
                // A Refresh can ONLY arise from a live session (we connected,
                // subscribed, and read until discovery changed the token set), so
                // it is positive proof the endpoint is reachable — a 403 fails at
                // connect and never reaches the read loop. If we were cooling,
                // this is recovery: clear it so we don't stay throttled forever
                // just because healthy sessions keep ending in token rolls.
                state.counters.resubscribes.fetch_add(1, Ordering::Relaxed);
                fast_backoff = policy.base_backoff;
                slow_backoff = policy.rate_limit_backoff;
                attempts.clear();
                if cooling || !state.is_healthy() {
                    cooling = false;
                    state.mark_healthy();
                    info!(ws = name, "connection recovered on resubscribe; health cleared");
                    emit("ws_recovered", json!({ "ws": name, "via": "resubscribe" }));
                }
                skip_gate = true;
                info!(ws = name, "subscription set changed; reconnecting immediately");
                continue;
            }
            SessionEnd::Lost(reason) => {
                state.counters.reconnects.fetch_add(1, Ordering::Relaxed);
                reconnects += 1;
                let class = classify_failure(&reason);
                let class_str = match class {
                    FailureClass::RateLimited => "rate_limited",
                    FailureClass::Transient => "transient",
                };
                emit(
                    "ws_lost",
                    json!({ "ws": name, "reason": reason.as_str(), "class": class_str,
                            "uptime_s": uptime.as_secs() }),
                );

                if uptime >= policy.stable_after {
                    // Session ran long enough to count as recovered: forgive the
                    // history, reset BOTH curves, and clear health if it was set.
                    fast_backoff = policy.base_backoff;
                    slow_backoff = policy.rate_limit_backoff;
                    attempts.clear();
                    if cooling || !state.is_healthy() {
                        cooling = false;
                        state.mark_healthy();
                        info!(
                            ws = name,
                            uptime_s = uptime.as_secs(),
                            "connection recovered: stable session, health cleared"
                        );
                        emit(
                            "ws_recovered",
                            json!({ "ws": name, "via": "stable", "uptime_s": uptime.as_secs() }),
                        );
                    }
                }

                // Enter (or stay in) the SLOW regime on a rate-limit OR a storm.
                // The soft latch persists until a stable session clears it above.
                if (class == FailureClass::RateLimited || storm) && !cooling {
                    cooling = true;
                    // Surface a degraded feed to the dashboard. NOT a stop, and
                    // NOT a permanent latch — auto-clears on recovery.
                    state.mark_health_failed();
                    let kind = if class == FailureClass::RateLimited {
                        "rate_limited"
                    } else {
                        "reconnect_storm_cooldown"
                    };
                    warn!(
                        ws = name,
                        reason,
                        attempts = attempts.len(),
                        max = policy.max_attempts_per_60s,
                        kind,
                        "entering slow reconnect regime (patient backoff; retrying indefinitely)"
                    );
                    write_alert(&alert_dir, name, kind, &reason);
                    emit("ws_cooldown", json!({ "ws": name, "kind": kind, "reason": reason.as_str() }));
                }

                // Pick the curve: SLOW while cooling, otherwise FAST.
                let backoff = if cooling {
                    let w = slow_backoff;
                    slow_backoff = (slow_backoff * 2).min(policy.rate_limit_max_backoff);
                    w
                } else {
                    let w = fast_backoff;
                    fast_backoff = (fast_backoff * 2).min(policy.max_backoff);
                    w
                };
                info!(
                    ws = name,
                    reason,
                    uptime_s = uptime.as_secs(),
                    backoff_s = backoff.as_secs(),
                    attempt = reconnects,
                    regime = if cooling { "slow" } else { "fast" },
                    "connection lost; backing off before reconnect"
                );
                emit(
                    "ws_reconnecting",
                    json!({ "ws": name, "attempt": reconnects, "backoff_s": backoff.as_secs(),
                            "regime": if cooling { "slow" } else { "fast" } }),
                );

                // Back off, but wake immediately on shutdown.
                let sleep = tokio::time::sleep(backoff);
                tokio::pin!(sleep);
                tokio::select! {
                    _ = &mut sleep => {}
                    _ = shutdown.changed() => {
                        if *shutdown.borrow() {
                            info!(ws = name, "shutdown during backoff; stopping");
                            return;
                        }
                    }
                }
            }
        }
    }
}

/// Establish a websocket connection, bounded by `timeout`.
///
/// A connect to a dead or silent peer (a routing black hole, or a TCP peer that
/// accepts but never finishes the handshake) can otherwise block for the full OS
/// SYN-retry window — or indefinitely. While it blocks, the reconnect supervisor
/// is stuck inside `connect_and_run` and CANNOT count attempts, so the storm
/// guard never fires and no alert is written: the bot goes silently dead while
/// still reporting `healthy=true`. Bounding the connect turns that hang into a
/// normal [`SessionEnd::Lost`], so backoff + the storm guard + alerting all work.
async fn connect_with_timeout(
    url: &str,
    timeout: Duration,
) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, String> {
    match tokio::time::timeout(timeout, connect_async(url)).await {
        Ok(Ok((ws, _resp))) => Ok(ws),
        Ok(Err(e)) => Err(connect_err_string(&e)),
        Err(_elapsed) => Err(format!("connect timeout after {timeout:?}")),
    }
}

/// Render a connect error with the HTTP status spelled out (`HTTP 403 Forbidden`)
/// when the handshake was rejected, so [`classify_failure`] can deterministically
/// recognise Cloudflare throttling (403/429). Other errors keep their Display.
fn connect_err_string(e: &WsError) -> String {
    match e {
        WsError::Http(resp) => {
            let s = resp.status();
            format!(
                "connect failed: HTTP {} {}",
                s.as_u16(),
                s.canonical_reason().unwrap_or("")
            )
        }
        other => format!("connect failed: {other}"),
    }
}

/// Write a single-shot alert JSON file into `dir`. Best-effort; never panics.
pub fn write_alert(dir: &str, component: &str, kind: &str, detail: &str) {
    if let Err(e) = std::fs::create_dir_all(dir) {
        warn!(error = %e, dir, "could not create alert dir; alert dropped");
        return;
    }
    let ts = now_ms();
    let path = Path::new(dir).join(format!("{ts}_{component}_{kind}.json"));
    let body = json!({
        "ts_ms": ts,
        "component": component,
        "kind": kind,
        "detail": detail,
    });
    match serde_json::to_vec_pretty(&body) {
        Ok(bytes) => match std::fs::write(&path, bytes) {
            Ok(()) => warn!(alert = %path.display(), component, kind, "alert written"),
            Err(e) => error!(error = %e, "failed to write alert file"),
        },
        Err(e) => error!(error = %e, "failed to serialize alert"),
    }
}

/// Periodically log ingestion counters + connection/health flags. Useful while a
/// 2h baseline run is in progress; exits on shutdown.
pub async fn stats_loop(state: Shared, mut shutdown: watch::Receiver<bool>) {
    let mut iv = tokio::time::interval(Duration::from_secs(30));
    iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = iv.tick() => {
                let c = &state.counters;
                info!(
                    uptime_s = state.started_at.elapsed().as_secs(),
                    bn_connected = state.binance_connected.load(Ordering::Relaxed),
                    pm_connected = state.polymarket_connected.load(Ordering::Relaxed),
                    healthy = state.is_healthy(),
                    bn_msgs = c.binance_msgs.load(Ordering::Relaxed),
                    bn_klines = c.binance_klines.load(Ordering::Relaxed),
                    bn_aggtrades = c.binance_aggtrades.load(Ordering::Relaxed),
                    pm_msgs = c.polymarket_msgs.load(Ordering::Relaxed),
                    pm_book = c.polymarket_book.load(Ordering::Relaxed),
                    pm_price_change = c.polymarket_price_change.load(Ordering::Relaxed),
                    pm_other = c.polymarket_other.load(Ordering::Relaxed),
                    parse_errors = c.parse_errors.load(Ordering::Relaxed),
                    reconnects = c.reconnects.load(Ordering::Relaxed),
                    resubscribes = c.resubscribes.load(Ordering::Relaxed),
                    discovery_failures = c.discovery_failures.load(Ordering::Relaxed),
                    active_tokens = state.active_tokens.load(Ordering::Relaxed),
                    bbo_tokens = state.bbo.len(),
                    "stats"
                );
            }
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{FailureClass, ReconnectPolicy, SessionEnd, classify_failure, run_with_reconnect};
    use crate::state::{SharedState, now_ms};
    use futures_util::{SinkExt, StreamExt};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
    use std::time::{Duration, Instant};
    use tokio::net::TcpListener;
    use tokio::sync::watch;
    use tokio_tungstenite::tungstenite::Message;
    use tokio_tungstenite::{accept_async, connect_async};

    /// A unique temp dir path for a test's alert output (no two tests collide).
    fn unique_dir(tag: &str) -> String {
        static N: AtomicU32 = AtomicU32::new(0);
        let id = N.fetch_add(1, Ordering::SeqCst);
        std::env::temp_dir()
            .join(format!("rb_test_{tag}_{}_{}", now_ms(), id))
            .to_string_lossy()
            .to_string()
    }

    /// Alert file names written into `dir` (one per cool-down episode). The kind
    /// is encoded in the name (`{ts}_{component}_{kind}.json`), so a test can
    /// assert WHICH regime fired (`rate_limited` vs `reconnect_storm_cooldown`).
    fn alert_kinds(dir: &str) -> Vec<String> {
        std::fs::read_dir(dir)
            .map(|rd| {
                rd.filter_map(|e| e.ok())
                    .map(|e| e.file_name().to_string_lossy().to_string())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Classification is the gate between the FAST and SLOW backoff regimes, so it
    /// must recognise rate-limit signals exactly — and NOT false-positive on a
    /// "403" that is merely part of a port number or token.
    #[test]
    fn classify_failure_recognises_rate_limits() {
        assert_eq!(
            classify_failure("connect failed: HTTP 403 Forbidden"),
            FailureClass::RateLimited
        );
        assert_eq!(
            classify_failure("connect failed: HTTP 429 Too Many Requests"),
            FailureClass::RateLimited
        );
        // Raw tungstenite Display shape (defence in depth).
        assert_eq!(
            classify_failure("connect failed: HTTP error: 403 Forbidden"),
            FailureClass::RateLimited
        );
        // Ordinary transport faults → fast recovery.
        assert_eq!(classify_failure("read error: connection reset"), FailureClass::Transient);
        assert_eq!(classify_failure("connect timeout after 10s"), FailureClass::Transient);
        assert_eq!(classify_failure("server close: None"), FailureClass::Transient);
        assert_eq!(classify_failure("stream ended"), FailureClass::Transient);
        // "40329" contains the substring "403" but is NOT a rate-limit → must not trip.
        assert_eq!(
            classify_failure("connect failed: ws://127.0.0.1:40329 refused"),
            FailureClass::Transient
        );
    }

    /// A reconnect storm (transient failures exceeding the budget) must NOT stop
    /// the supervisor any more. It enters the SLOW regime (cool-down): marks the
    /// feed unhealthy, writes ONE `reconnect_storm_cooldown` alert, and keeps
    /// retrying patiently — never the old human-in-loop latch.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn storm_enters_cooldown_without_stopping() {
        let state = SharedState::new();
        let dir = unique_dir("storm");
        let dir_task = dir.clone();
        let policy = ReconnectPolicy {
            max_attempts_per_60s: 3,
            base_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(1),
            rate_limit_backoff: Duration::from_millis(40),
            rate_limit_max_backoff: Duration::from_millis(80),
            stable_after: Duration::from_secs(60),
        };
        let (sd_tx, sd_rx) = watch::channel(false);
        let st = state.clone();
        let h = tokio::spawn(async move {
            run_with_reconnect("storm", policy, st, dir_task, sd_rx, |_n| async {
                SessionEnd::Lost("read error: connection reset".to_string())
            })
            .await;
        });

        // Wait for the cool-down to ENGAGE (unhealthy) — the race-free signal.
        // (Polling `reconnects` would race: the counter is bumped at the top of
        // the Lost arm, a hair before the same iteration marks health failed.)
        let deadline = Instant::now() + Duration::from_secs(5);
        while state.is_healthy() && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(!state.is_healthy(), "storm cool-down should mark the feed unhealthy");
        assert!(!h.is_finished(), "storm must NOT stop the supervisor (no latch)");
        // Still alive (not latched) AND throttled: over a fixed window the SLOW
        // backoff yields a handful, never the hundreds a fast (1ms) backoff would.
        let before = state.counters.reconnects.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(150)).await;
        let delta = state.counters.reconnects.load(Ordering::SeqCst) - before;
        assert!(delta <= 8, "slow regime must not hammer; delta={delta}");

        sd_tx.send(true).unwrap();
        let joined = tokio::time::timeout(Duration::from_secs(1), h).await;
        assert!(joined.is_ok(), "supervisor did not exit on shutdown");

        let kinds = alert_kinds(&dir);
        assert!(
            kinds.iter().any(|k| k.contains("reconnect_storm_cooldown")),
            "expected a reconnect_storm_cooldown alert, got {kinds:?}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A persistent HTTP 403 (Cloudflare throttle — the 2026-06-19 outage) must be
    /// classified RateLimited → SLOW regime: never stop, mark unhealthy, write a
    /// `rate_limited` alert, and back off long enough to NOT hammer the endpoint.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rate_limit_403_enters_slow_regime_and_never_stops() {
        let state = SharedState::new();
        let dir = unique_dir("ratelimit");
        let dir_task = dir.clone();
        let policy = ReconnectPolicy {
            max_attempts_per_60s: 3,
            base_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(1),
            rate_limit_backoff: Duration::from_millis(40),
            rate_limit_max_backoff: Duration::from_millis(80),
            stable_after: Duration::from_secs(60),
        };
        let (sd_tx, sd_rx) = watch::channel(false);
        let st = state.clone();
        let h = tokio::spawn(async move {
            run_with_reconnect("ratelimit", policy, st, dir_task, sd_rx, |_n| async {
                SessionEnd::Lost("connect failed: HTTP 403 Forbidden".to_string())
            })
            .await;
        });

        // Wait for the cool-down to engage (unhealthy) — race-free vs the counter.
        let deadline = Instant::now() + Duration::from_secs(5);
        while state.is_healthy() && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(!state.is_healthy(), "403 cool-down should mark the feed unhealthy");
        assert!(!h.is_finished(), "a 403 must NOT stop the supervisor (no latch)");
        // The patient backoff must NOT hammer the throttling endpoint.
        let before = state.counters.reconnects.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(150)).await;
        let delta = state.counters.reconnects.load(Ordering::SeqCst) - before;
        assert!(delta <= 8, "403 slow backoff must not hammer; delta={delta}");

        sd_tx.send(true).unwrap();
        let joined = tokio::time::timeout(Duration::from_secs(1), h).await;
        assert!(joined.is_ok(), "supervisor did not exit on shutdown");

        let kinds = alert_kinds(&dir);
        assert!(
            kinds.iter().any(|k| k.contains("rate_limited")),
            "expected a rate_limited alert (not a generic storm), got {kinds:?}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Ordinary transient drops, under budget, must use the FAST regime and stay
    /// healthy — they must NOT be mistaken for a rate-limit / cool-down.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn transient_failures_stay_fast_and_healthy() {
        let state = SharedState::new();
        let dir = unique_dir("transient");
        let dir_task = dir.clone();
        let policy = ReconnectPolicy {
            max_attempts_per_60s: 1000, // far above any count in this window: no storm
            base_backoff: Duration::from_millis(5),
            max_backoff: Duration::from_millis(10),
            rate_limit_backoff: Duration::from_millis(200),
            rate_limit_max_backoff: Duration::from_millis(200),
            stable_after: Duration::from_secs(60),
        };
        let (sd_tx, sd_rx) = watch::channel(false);
        let st = state.clone();
        let h = tokio::spawn(async move {
            run_with_reconnect("transient", policy, st, dir_task, sd_rx, |_n| async {
                SessionEnd::Lost("read error: connection reset".to_string())
            })
            .await;
        });

        // Wait (to a deadline, robust under load) for several FAST reconnects.
        let deadline = Instant::now() + Duration::from_secs(5);
        while state.counters.reconnects.load(Ordering::SeqCst) < 5 && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let r = state.counters.reconnects.load(Ordering::SeqCst);
        // Transient drops under budget never cool down → always healthy, fast loop.
        assert!(state.is_healthy(), "transient drops under budget must stay healthy");
        assert!(r >= 5, "fast regime should retry briskly; got {r}");

        sd_tx.send(true).unwrap();
        let joined = tokio::time::timeout(Duration::from_secs(1), h).await;
        assert!(joined.is_ok(), "supervisor did not exit on shutdown");

        assert!(alert_kinds(&dir).is_empty(), "no cool-down alert expected for transient drops");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The full auto-recovery cycle: a 403 drives us into cool-down (unhealthy),
    /// then a session that stays up past `stable_after` counts as recovered and
    /// CLEARS health on its own — no human, no restart. This is precisely what
    /// would have happened on 2026-06-19 had this fix been in place.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn recovers_and_clears_health_after_cooldown() {
        let state = SharedState::new();
        let dir = unique_dir("recover");
        let policy = ReconnectPolicy {
            max_attempts_per_60s: 100, // isolate the 403 path; no storm
            base_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(2),
            rate_limit_backoff: Duration::from_millis(5),
            rate_limit_max_backoff: Duration::from_millis(10),
            stable_after: Duration::from_millis(40),
        };
        let (_sd_tx, sd_rx) = watch::channel(false);
        let calls = Arc::new(AtomicU32::new(0));
        let unhealthy_during_cooldown = Arc::new(AtomicBool::new(false));
        let st_closure = state.clone();
        let c = calls.clone();
        let saw = unhealthy_during_cooldown.clone();

        run_with_reconnect("recover", policy, state.clone(), dir.clone(), sd_rx, move |_n| {
            let st = st_closure.clone();
            let c = c.clone();
            let saw = saw.clone();
            async move {
                let n = c.fetch_add(1, Ordering::SeqCst);
                if n < 3 {
                    // Immediate 403 (uptime ~0 < stable_after) → drives cool-down.
                    SessionEnd::Lost("connect failed: HTTP 403 Forbidden".to_string())
                } else if n == 3 {
                    // The throttle has lifted: record that we WERE unhealthy, then
                    // hold a session open past `stable_after` (a real recovery).
                    saw.store(!st.is_healthy(), Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(60)).await;
                    SessionEnd::Lost("read error: connection reset".to_string())
                } else {
                    SessionEnd::Shutdown
                }
            }
        })
        .await;

        assert!(
            unhealthy_during_cooldown.load(Ordering::SeqCst),
            "feed should have been marked unhealthy during the 403 cool-down"
        );
        assert!(
            state.is_healthy(),
            "health must AUTO-CLEAR after a stable session (self-recovery, no human)"
        );
        // It entered cool-down at least once (rate_limited alert present).
        assert!(
            alert_kinds(&dir).iter().any(|k| k.contains("rate_limited")),
            "expected a rate_limited alert from the cool-down"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Recovery via `Refresh`: a 403 cool-down, then a normal token roll (which can
    /// only happen on a live, connected session) must clear health — even though
    /// `stable_after` is large, so the Lost-after-stable path can NOT be what
    /// clears it. Guards the bug where healthy sessions that always end in a
    /// resubscribe would otherwise leave the feed latched unhealthy forever.
    #[tokio::test]
    async fn refresh_after_cooldown_clears_health() {
        let state = SharedState::new();
        let dir = unique_dir("refresh_recover");
        let policy = ReconnectPolicy {
            max_attempts_per_60s: 100, // isolate the 403 path; no storm
            base_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(2),
            rate_limit_backoff: Duration::from_millis(5),
            rate_limit_max_backoff: Duration::from_millis(10),
            stable_after: Duration::from_secs(60), // the Lost-stable path CANNOT fire here
        };
        let (_sd_tx, sd_rx) = watch::channel(false);
        let calls = Arc::new(AtomicU32::new(0));
        let unhealthy_before_refresh = Arc::new(AtomicBool::new(false));
        let st_closure = state.clone();
        let c = calls.clone();
        let saw = unhealthy_before_refresh.clone();

        run_with_reconnect("refresh_recover", policy, state.clone(), dir.clone(), sd_rx, move |_n| {
            let st = st_closure.clone();
            let c = c.clone();
            let saw = saw.clone();
            async move {
                let n = c.fetch_add(1, Ordering::SeqCst);
                if n < 2 {
                    SessionEnd::Lost("connect failed: HTTP 403 Forbidden".to_string())
                } else if n == 2 {
                    // The throttle lifted: a live session whose tokens rolled.
                    saw.store(!st.is_healthy(), Ordering::SeqCst);
                    SessionEnd::Refresh
                } else {
                    SessionEnd::Shutdown
                }
            }
        })
        .await;

        assert!(
            unhealthy_before_refresh.load(Ordering::SeqCst),
            "feed should have been unhealthy during the 403 cool-down"
        );
        assert!(
            state.is_healthy(),
            "a resubscribe (proof of a live connection) must clear the cool-down latch"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// #3 observability: with an oplog sink installed, the connection lifecycle is
    /// mirrored into it (ws_lost / ws_cooldown / ws_reconnecting) for the dashboard
    /// — carrying the failure class and the ws name. Without a sink it is a no-op
    /// (every other test here runs that path), so emission can't affect reconnect.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn oplog_records_connection_lifecycle_when_sink_installed() {
        let state = SharedState::new();
        let dir = unique_dir("oplogws");
        std::fs::create_dir_all(&dir).unwrap();
        let oplog_path = std::path::Path::new(&dir).join("oplog.jsonl");
        state.set_oplog(Arc::new(crate::oplog::OpLog::new(&oplog_path)));
        let policy = ReconnectPolicy {
            max_attempts_per_60s: 100,
            base_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(2),
            rate_limit_backoff: Duration::from_millis(20),
            rate_limit_max_backoff: Duration::from_millis(40),
            stable_after: Duration::from_secs(60),
        };
        let (sd_tx, sd_rx) = watch::channel(false);
        let st = state.clone();
        let dir_task = dir.clone();
        let h = tokio::spawn(async move {
            run_with_reconnect("polymarket_ws", policy, st, dir_task, sd_rx, |_n| async {
                SessionEnd::Lost("connect failed: HTTP 403 Forbidden".to_string())
            })
            .await;
        });
        let deadline = Instant::now() + Duration::from_secs(3);
        while state.counters.reconnects.load(Ordering::SeqCst) < 2 && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        sd_tx.send(true).unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(1), h).await;

        let body = std::fs::read_to_string(&oplog_path).unwrap_or_default();
        assert!(body.contains(r#""kind":"ws_lost""#), "oplog missing ws_lost: {body}");
        assert!(body.contains(r#""class":"rate_limited""#), "ws_lost must carry the class");
        assert!(body.contains(r#""kind":"ws_cooldown""#), "oplog missing ws_cooldown");
        assert!(body.contains(r#""kind":"ws_reconnecting""#), "oplog missing ws_reconnecting");
        assert!(body.contains(r#""ws":"polymarket_ws""#), "events must carry the ws name");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Sessions that stay up beyond `stable_after` reset the attempt window, so a
    /// long-lived-then-dropped connection never trips the storm guard.
    #[tokio::test]
    async fn stable_sessions_reset_attempt_window() {
        let state = SharedState::new();
        let dir = unique_dir("stable");
        let policy = ReconnectPolicy {
            max_attempts_per_60s: 3,
            base_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(2),
            rate_limit_backoff: Duration::from_millis(1),
            rate_limit_max_backoff: Duration::from_millis(2),
            stable_after: Duration::from_millis(20),
        };
        let (_tx, rx) = watch::channel(false);
        let calls = Arc::new(AtomicU32::new(0));
        let c = calls.clone();

        run_with_reconnect("stable", policy, state.clone(), dir.clone(), rx, move |_n| {
            let c = c.clone();
            async move {
                let n = c.fetch_add(1, Ordering::SeqCst);
                if n >= 5 {
                    return SessionEnd::Shutdown;
                }
                // Stay up past `stable_after` so each drop resets the window.
                tokio::time::sleep(Duration::from_millis(30)).await;
                SessionEnd::Lost("drop".to_string())
            }
        })
        .await;

        // Without the reset, the 4th attempt would exceed the budget of 3.
        assert!(state.is_healthy(), "stable sessions must not trip the storm guard");
        assert!(calls.load(Ordering::SeqCst) >= 5);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Toggling the shutdown signal while the loop is backing off must end it
    /// promptly and leave health intact.
    #[tokio::test]
    async fn shutdown_during_backoff_exits() {
        let state = SharedState::new();
        let dir = unique_dir("sd");
        let policy = ReconnectPolicy {
            max_attempts_per_60s: 50,
            base_backoff: Duration::from_millis(200),
            max_backoff: Duration::from_millis(200),
            rate_limit_backoff: Duration::from_millis(200),
            rate_limit_max_backoff: Duration::from_millis(200),
            stable_after: Duration::from_secs(60),
        };
        let (tx, rx) = watch::channel(false);
        let st = state.clone();
        let h = tokio::spawn(async move {
            run_with_reconnect("sd", policy, st, dir, rx, |_n| async {
                SessionEnd::Lost("x".to_string())
            })
            .await;
        });

        tokio::time::sleep(Duration::from_millis(50)).await; // loop is now backing off
        tx.send(true).unwrap();
        let res = tokio::time::timeout(Duration::from_secs(1), h).await;
        assert!(res.is_ok(), "loop did not exit on shutdown");
        assert!(state.is_healthy());
    }

    /// Mock WS server: accept → greet → read one frame → send Close, looping so it
    /// can serve repeated reconnects. Counts accepted connections.
    async fn spawn_clean_close_server() -> (String, Arc<AtomicU32>, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let count = Arc::new(AtomicU32::new(0));
        let counter = count.clone();
        let handle = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                counter.fetch_add(1, Ordering::SeqCst);
                if let Ok(mut ws) = accept_async(stream).await {
                    let _ = ws.send(Message::Text("hello".to_string().into())).await;
                    let _ = ws.next().await; // drain the client's subscribe frame
                    let _ = ws.send(Message::Close(None)).await;
                }
            }
        });
        (url, count, handle)
    }

    /// One real session: connect, subscribe, read until the link ends.
    async fn generic_session(url: &str, sd: &mut watch::Receiver<bool>) -> SessionEnd {
        let (ws, _) = match connect_async(url).await {
            Ok(ok) => ok,
            Err(e) => return SessionEnd::Lost(format!("connect: {e}")),
        };
        let (mut write, mut read) = ws.split();
        let _ = write.send(Message::Text("sub".to_string().into())).await;
        loop {
            tokio::select! {
                changed = sd.changed() => {
                    if changed.is_err() || *sd.borrow() {
                        let _ = write.send(Message::Close(None)).await;
                        return SessionEnd::Shutdown;
                    }
                }
                msg = read.next() => match msg {
                    Some(Ok(Message::Close(_))) => return SessionEnd::Lost("server close".to_string()),
                    Some(Ok(Message::Ping(p))) => { let _ = write.send(Message::Pong(p)).await; }
                    Some(Ok(_)) => {}
                    Some(Err(e)) => return SessionEnd::Lost(format!("read: {e}")),
                    None => return SessionEnd::Lost("ended".to_string()),
                }
            }
        }
    }

    /// F1 — clean server close: a real tokio-tungstenite client driven by the
    /// reconnect supervisor must reconnect repeatedly, then exit on shutdown.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn f1_client_reconnects_after_clean_close() {
        let (url, count, server) = spawn_clean_close_server().await;
        let state = SharedState::new();
        let dir = unique_dir("f1");
        let policy = ReconnectPolicy {
            max_attempts_per_60s: 50, // don't trip the storm guard during the test
            base_backoff: Duration::from_millis(5),
            max_backoff: Duration::from_millis(10),
            rate_limit_backoff: Duration::from_millis(5),
            rate_limit_max_backoff: Duration::from_millis(10),
            stable_after: Duration::from_secs(60),
        };
        let (sd_tx, sd_rx) = watch::channel(false);
        let sd_closure = sd_rx.clone();
        let st = state.clone();

        let loop_handle = tokio::spawn(async move {
            run_with_reconnect("f1", policy, st, dir, sd_rx, move |_n| {
                let url = url.clone();
                let mut sd = sd_closure.clone();
                async move { generic_session(&url, &mut sd).await }
            })
            .await;
        });

        // Wait for several reconnect cycles.
        let deadline = Instant::now() + Duration::from_secs(5);
        while count.load(Ordering::SeqCst) < 3 && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let conns = count.load(Ordering::SeqCst);

        sd_tx.send(true).unwrap();
        let joined = tokio::time::timeout(Duration::from_secs(2), loop_handle).await;
        server.abort();

        assert!(conns >= 3, "expected >=3 reconnects, got {conns}");
        assert!(joined.is_ok(), "reconnect loop did not exit on shutdown");
        assert!(state.is_healthy());
        assert!(state.counters.reconnects.load(Ordering::SeqCst) >= 2);
    }

    /// Mock WS server that accepts, greets, drains one frame, then DROPS the
    /// connection with no close handshake — simulating a network blip / killed
    /// peer. Loops so it can serve repeated reconnects. Counts accepts.
    async fn spawn_abrupt_drop_server() -> (String, Arc<AtomicU32>, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let count = Arc::new(AtomicU32::new(0));
        let counter = count.clone();
        let handle = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                counter.fetch_add(1, Ordering::SeqCst);
                if let Ok(mut ws) = accept_async(stream).await {
                    let _ = ws.send(Message::Text("hello".to_string().into())).await;
                    let _ = ws.next().await; // drain the client's subscribe frame
                    // No Close frame: dropping the stream tears down TCP abruptly.
                    drop(ws);
                }
            }
        });
        (url, count, handle)
    }

    /// F2 — abrupt transport drop (no close frame): the client must notice the
    /// dead link (read error / EOF), reconnect, and still exit on shutdown.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn f2_client_reconnects_after_abrupt_drop() {
        let (url, count, server) = spawn_abrupt_drop_server().await;
        let state = SharedState::new();
        let dir = unique_dir("f2");
        let policy = ReconnectPolicy {
            max_attempts_per_60s: 50, // don't trip the storm guard during the test
            base_backoff: Duration::from_millis(5),
            max_backoff: Duration::from_millis(10),
            rate_limit_backoff: Duration::from_millis(5),
            rate_limit_max_backoff: Duration::from_millis(10),
            stable_after: Duration::from_secs(60),
        };
        let (sd_tx, sd_rx) = watch::channel(false);
        let sd_closure = sd_rx.clone();
        let st = state.clone();

        let loop_handle = tokio::spawn(async move {
            run_with_reconnect("f2", policy, st, dir, sd_rx, move |_n| {
                let url = url.clone();
                let mut sd = sd_closure.clone();
                async move { generic_session(&url, &mut sd).await }
            })
            .await;
        });

        let deadline = Instant::now() + Duration::from_secs(5);
        while count.load(Ordering::SeqCst) < 3 && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let conns = count.load(Ordering::SeqCst);

        sd_tx.send(true).unwrap();
        let joined = tokio::time::timeout(Duration::from_secs(2), loop_handle).await;
        server.abort();

        assert!(conns >= 3, "expected >=3 reconnects after abrupt drops, got {conns}");
        assert!(joined.is_ok(), "reconnect loop did not exit on shutdown");
        assert!(state.is_healthy());
        assert!(state.counters.reconnects.load(Ordering::SeqCst) >= 2);
    }

    /// Mock server that accepts the TCP connection but drops it *before* the WS
    /// handshake completes, so `connect_async` fails fast and deterministically on
    /// every OS — unlike connecting to a closed port, whose refuse-vs-hang timing
    /// is OS-dependent (Windows loopback can silently stall instead of RST'ing).
    /// Loops so it can serve repeated attempts. Counts accepts.
    async fn spawn_handshake_reject_server() -> (String, Arc<AtomicU32>, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let count = Arc::new(AtomicU32::new(0));
        let counter = count.clone();
        let handle = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                counter.fetch_add(1, Ordering::SeqCst);
                drop(stream); // close before the HTTP upgrade → client handshake fails
            }
        });
        (url, count, handle)
    }

    /// F3 — server unavailable then recovers: the server first drops every
    /// connection before the handshake (attempts fail), then after a delay starts
    /// completing handshakes. The supervisor must back off through the failures and
    /// connect once the server recovers, then exit cleanly on shutdown.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn f3_client_backs_off_then_connects_when_server_returns() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let state = SharedState::new();
        let dir = unique_dir("f3");
        let policy = ReconnectPolicy {
            max_attempts_per_60s: 50, // failures must NOT trip the storm guard here
            base_backoff: Duration::from_millis(20),
            max_backoff: Duration::from_millis(40),
            rate_limit_backoff: Duration::from_millis(20),
            rate_limit_max_backoff: Duration::from_millis(40),
            stable_after: Duration::from_secs(60),
        };
        let (sd_tx, sd_rx) = watch::channel(false);
        let sd_closure = sd_rx.clone();
        let st = state.clone();

        // Reject (drop pre-handshake) until `ready_at`, then serve real sessions.
        let accepted = Arc::new(AtomicU32::new(0)); // successful handshakes
        let acc = accepted.clone();
        let server = tokio::spawn(async move {
            let ready_at = Instant::now() + Duration::from_millis(250);
            while let Ok((stream, _)) = listener.accept().await {
                if Instant::now() < ready_at {
                    drop(stream); // pre-handshake drop → this attempt fails fast
                    continue;
                }
                if let Ok(mut ws) = accept_async(stream).await {
                    acc.fetch_add(1, Ordering::SeqCst);
                    let _ = ws.send(Message::Text("hello".to_string().into())).await;
                    // Hold the link open until the client closes it (on shutdown).
                    while let Some(Ok(m)) = ws.next().await {
                        if m.is_close() {
                            break;
                        }
                    }
                }
            }
        });

        let loop_handle = tokio::spawn(async move {
            run_with_reconnect("f3", policy, st, dir, sd_rx, move |_n| {
                let url = url.clone();
                let mut sd = sd_closure.clone();
                async move { generic_session(&url, &mut sd).await }
            })
            .await;
        });

        // Wait until the server (once recovered) completes a handshake.
        let deadline = Instant::now() + Duration::from_secs(5);
        while accepted.load(Ordering::SeqCst) < 1 && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let ok = accepted.load(Ordering::SeqCst);
        let failures = state.counters.reconnects.load(Ordering::SeqCst);

        sd_tx.send(true).unwrap();
        let joined = tokio::time::timeout(Duration::from_secs(2), loop_handle).await;
        server.abort();

        assert!(failures >= 1, "expected >=1 failed attempt before recovery, got {failures}");
        assert!(ok >= 1, "client never connected after the server recovered");
        assert!(joined.is_ok(), "reconnect loop did not exit on shutdown");
        assert!(state.is_healthy());
    }

    /// F4 — failure storm (socket level): a server that is reachable but never
    /// completes the WS handshake makes every attempt fail fast. The supervisor
    /// must trip the budget, enter the SLOW cool-down (unhealthy + alert), and
    /// KEEP RETRYING — never stop. It exits only on shutdown. This is the socket
    /// analogue of `storm_enters_cooldown_without_stopping`, and the exact shape
    /// of the 2026-06-19 outage: persistent connect failures must self-heal, not
    /// latch a dead process.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn f4_failure_storm_enters_cooldown_without_stopping() {
        let (url, _count, server) = spawn_handshake_reject_server().await;
        let state = SharedState::new();
        let dir = unique_dir("f4");
        let dir_task = dir.clone();
        let policy = ReconnectPolicy {
            max_attempts_per_60s: 3,
            base_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(2),
            rate_limit_backoff: Duration::from_millis(30),
            rate_limit_max_backoff: Duration::from_millis(60),
            stable_after: Duration::from_secs(60),
        };
        let (sd_tx, sd_rx) = watch::channel(false);
        let sd_closure = sd_rx.clone();
        let st = state.clone();

        let h = tokio::spawn(async move {
            run_with_reconnect("f4", policy, st, dir_task, sd_rx, move |_n| {
                let url = url.clone();
                let mut sd = sd_closure.clone();
                async move { generic_session(&url, &mut sd).await }
            })
            .await;
        });

        // Wait for the cool-down to engage (unhealthy) — race-free signal.
        let deadline = Instant::now() + Duration::from_secs(5);
        while state.is_healthy() && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(!state.is_healthy(), "storm cool-down should mark unhealthy");
        assert!(!h.is_finished(), "storm must NOT stop the supervisor (no human-in-loop latch)");

        sd_tx.send(true).unwrap();
        let joined = tokio::time::timeout(Duration::from_secs(2), h).await;
        server.abort();
        assert!(joined.is_ok(), "reconnect loop did not exit on shutdown");

        // Handshake-reject is a transient connect error (not a 403) → storm path.
        let kinds = alert_kinds(&dir);
        assert!(
            kinds.iter().any(|k| k.contains("reconnect_storm_cooldown")),
            "expected reconnect_storm_cooldown alert, got {kinds:?}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Mock server that binds and holds the port but NEVER accepts: the client's
    /// TCP connect still succeeds (the kernel completes the handshake into the
    /// accept backlog), yet the WS upgrade never completes — so a client without a
    /// connect timeout would hang here forever. Deterministic on every OS (unlike
    /// a closed port, whose refuse-vs-hang behaviour is OS-dependent).
    async fn spawn_hang_server() -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let handle = tokio::spawn(async move {
            let _held = listener; // keep the port bound; never call accept()
            std::future::pending::<()>().await;
        });
        (url, handle)
    }

    /// Connect timeout: a peer that accepts TCP but never finishes the WS
    /// handshake must NOT freeze the supervisor. With a bounded connect, each
    /// attempt returns `Lost` (a transient timeout), backoff fires, and persistent
    /// failures enter the SLOW cool-down (unhealthy + alert) while STILL retrying
    /// indefinitely — never the old stop-and-wait-for-a-human. That the reconnect
    /// counter keeps climbing also proves the connect stayed bounded (a hung
    /// connect would leave it stuck at 0).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn connect_timeout_hung_peer_cools_down_without_stopping() {
        let (url, server) = spawn_hang_server().await;
        let state = SharedState::new();
        let dir = unique_dir("ctimeout");
        let dir_task = dir.clone();
        let policy = ReconnectPolicy {
            max_attempts_per_60s: 3,
            base_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(2),
            rate_limit_backoff: Duration::from_millis(20),
            rate_limit_max_backoff: Duration::from_millis(40),
            stable_after: Duration::from_secs(60),
        };
        let connect_timeout = Duration::from_millis(80); // short, to keep the test fast
        let (sd_tx, sd_rx) = watch::channel(false);
        let st = state.clone();

        // Drive the REAL timed-connect path (same helper the clients use).
        let h = tokio::spawn(async move {
            run_with_reconnect("ctimeout", policy, st, dir_task, sd_rx, move |_n| {
                let url = url.clone();
                async move {
                    match super::connect_with_timeout(&url, connect_timeout).await {
                        Ok(_ws) => SessionEnd::Lost("unexpected connect to hang server".to_string()),
                        Err(reason) => SessionEnd::Lost(reason),
                    }
                }
            })
            .await;
        });

        // Wait for the cool-down to engage (unhealthy). That it engages at all
        // proves the connect stayed BOUNDED — a hung connect would never return,
        // so attempts (and thus the cool-down) could never accrue.
        let deadline = Instant::now() + Duration::from_secs(10);
        while state.is_healthy() && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(!state.is_healthy(), "persistent connect timeouts must mark unhealthy (cool-down)");
        assert!(!h.is_finished(), "persistent connect timeouts must NOT stop the supervisor");

        sd_tx.send(true).unwrap();
        let joined = tokio::time::timeout(Duration::from_secs(2), h).await;
        server.abort();
        assert!(joined.is_ok(), "reconnect loop did not exit on shutdown");

        let kinds = alert_kinds(&dir);
        assert!(
            kinds.iter().any(|k| k.contains("reconnect_storm_cooldown")),
            "expected reconnect_storm_cooldown alert, got {kinds:?}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A planned `Refresh` (token set changed) must NOT charge the storm budget:
    /// many rapid resubscribes in a row stay healthy and never trip the guard,
    /// even with a budget far smaller than the number of refreshes.
    #[tokio::test]
    async fn refresh_does_not_charge_storm_budget() {
        let state = SharedState::new();
        let dir = unique_dir("refresh");
        let policy = ReconnectPolicy {
            max_attempts_per_60s: 2, // tiny: refreshes would trip it if charged
            base_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(2),
            rate_limit_backoff: Duration::from_millis(1),
            rate_limit_max_backoff: Duration::from_millis(2),
            stable_after: Duration::from_secs(60),
        };
        let (_tx, rx) = watch::channel(false);
        let calls = Arc::new(AtomicU32::new(0));
        let c = calls.clone();

        run_with_reconnect("refresh", policy, state.clone(), dir.clone(), rx, move |_n| {
            let c = c.clone();
            async move {
                let n = c.fetch_add(1, Ordering::SeqCst);
                if n >= 8 {
                    SessionEnd::Shutdown
                } else {
                    SessionEnd::Refresh
                }
            }
        })
        .await;

        assert!(state.is_healthy(), "planned refreshes must not trip the storm guard");
        assert_eq!(state.counters.resubscribes.load(Ordering::SeqCst), 8);
        // Only the first connect charged the gate (1 attempt) → no storm alert.
        let alerts = std::fs::read_dir(&dir).map(|d| d.count()).unwrap_or(0);
        assert_eq!(alerts, 0, "no storm alert expected for planned refreshes");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
