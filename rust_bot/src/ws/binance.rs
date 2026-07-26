//! Binance spot websocket client (read-only).
//!
//! Connects to the raw `/ws` endpoint and SUBSCRIBEs to `<sym>@kline_1s` and
//! `<sym>@aggTrade` for every configured asset (BTC → btcusdt, …). The SUBSCRIBE
//! frame is re-sent on every (re)connect, which is our "resubscribe → fresh
//! snapshot" path. We answer protocol Pings with Pongs to stay alive.
//!
//! Strictly WS-only: no REST. Strictly read-only: no orders.

use std::sync::atomic::Ordering;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::sync::watch;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, info, warn};

use super::{ReconnectPolicy, SessionEnd, connect_with_timeout, run_with_reconnect};
use crate::config::Config;
use crate::events::{EventLogger, EventRecord};
use crate::state::{Shared, now_ms};

/// Map a config asset (e.g. "BTC") to a Binance spot symbol ("btcusdt").
fn symbol_for(asset: &str) -> String {
    format!("{}usdt", asset.to_lowercase())
}

/// Reverse of [`symbol_for`]: a Binance symbol ("btcusdt") → the asset ("BTC"). The
/// strategy universe is BTC/ETH; returns `None` for anything else.
fn asset_for_symbol(sym: &str) -> Option<String> {
    let base = sym.strip_suffix("usdt")?;
    if base.is_empty() {
        return None;
    }
    Some(base.to_uppercase())
}

/// Entry point: supervise the Binance session under the reconnect policy.
pub async fn run(state: Shared, cfg: Config, shutdown: watch::Receiver<bool>, logger: EventLogger) {
    let url = cfg.connections.binance_ws_url.clone();
    let streams: Vec<String> = cfg
        .markets
        .assets
        .iter()
        .flat_map(|a| {
            let sym = symbol_for(a);
            [format!("{sym}@kline_1s"), format!("{sym}@aggTrade")]
        })
        .collect();

    if streams.is_empty() {
        warn!("binance: no assets configured; ingestion idle");
    }

    let policy = ReconnectPolicy::from_config(cfg.connections.reconnect_max_attempts_per_60s);
    let connect_timeout = Duration::from_secs(cfg.connections.connect_timeout_s);
    // ORDER #14 A: idle watchdog threshold. Floor at 5s so a fat-fingered config
    // can't make it fire on normal jitter.
    let idle_limit = Duration::from_secs(cfg.connections.binance_idle_timeout_s.max(5));
    let alert_dir = cfg.paths.alert_dir.clone();
    let loop_state = state.clone();

    run_with_reconnect(
        "binance_ws",
        policy,
        loop_state,
        alert_dir,
        shutdown.clone(),
        move |reconnect_no| {
            let url = url.clone();
            let streams = streams.clone();
            let state = state.clone();
            let logger = logger.clone();
            let mut shutdown = shutdown.clone();
            async move {
                session(
                    reconnect_no,
                    &url,
                    &streams,
                    &state,
                    &logger,
                    &mut shutdown,
                    connect_timeout,
                    idle_limit,
                )
                .await
            }
        },
    )
    .await;
}

/// One connect → subscribe → read-until-end cycle.
async fn session(
    reconnect_no: u32,
    url: &str,
    streams: &[String],
    state: &Shared,
    logger: &EventLogger,
    shutdown: &mut watch::Receiver<bool>,
    connect_timeout: Duration,
    idle_limit: Duration,
) -> SessionEnd {
    let ws = match connect_with_timeout(url, connect_timeout).await {
        Ok(ws) => ws,
        Err(reason) => return SessionEnd::Lost(reason),
    };

    if reconnect_no > 0 {
        info!(
            ws = "binance_ws",
            attempt = reconnect_no,
            streams = streams.len(),
            "ws_reconnect_complete"
        );
    } else {
        info!(ws = "binance_ws", streams = streams.len(), url, "ws_connected");
    }
    state.binance_connected.store(true, Ordering::Relaxed);

    let (mut write, mut read) = ws.split();

    // (Re)subscribe on every connect.
    if !streams.is_empty() {
        let sub = json!({ "method": "SUBSCRIBE", "params": streams, "id": 1 });
        if let Err(e) = write.send(Message::Text(sub.to_string().into())).await {
            state.binance_connected.store(false, Ordering::Relaxed);
            return SessionEnd::Lost(format!("subscribe send failed: {e}"));
        }
    }

    let end = read_until_end(
        &mut read,
        &mut write,
        state,
        logger,
        shutdown,
        idle_limit,
        Duration::from_secs(KEEPALIVE_SECS),
    )
    .await;

    state.binance_connected.store(false, Ordering::Relaxed);
    end
}

/// Client keepalive period. Mirrors the Polymarket client (cc61a53): proactively
/// pinging turns some silent half-open sockets into an observable WRITE error, an
/// independent detector alongside the idle watchdog below.
const KEEPALIVE_SECS: u64 = 15;

/// ORDER #14 A — the read loop, with an IDLE WATCHDOG.
///
/// The 2026-07-25 incident: the Binance socket went half-open (server gone, no FIN,
/// no RST — the classic cloud/NAT idle drop). Every exit path here required the
/// stream to *produce* something (error / close frame / EOF), so `read.next()` never
/// resolved and this loop parked forever: 45 hours, zero klines, zero decisions,
/// `binance_connected=true`, `healthy=true`, no reconnect, no alert.
///
/// The fix is the `ticker` arm: track the instant of the last INBOUND frame (any
/// frame — text, ping, pong, binary) and break `SessionEnd::Lost` once it exceeds
/// `idle_limit`, which puts the session on the existing, proven reconnect path.
///
/// Sizing: 2 symbols × 1s klines + aggTrades means the normal inter-message gap is
/// well under a second, and Binance's server pings every ~20s even on a silent tape
/// — so a 30s default is ~30× the normal gap yet safely above the ping interval.
///
/// Split out of `session` (which owns the connect) so the watchdog is unit-testable
/// against a synthetic stream. Uses `tokio::time::Instant` (NOT `std::time::Instant`)
/// so `tokio::time::pause()` drives the tests deterministically.
async fn read_until_end<R, W, E>(
    read: &mut R,
    write: &mut W,
    state: &Shared,
    logger: &EventLogger,
    shutdown: &mut watch::Receiver<bool>,
    idle_limit: Duration,
    keepalive_every: Duration,
) -> SessionEnd
where
    R: futures_util::Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
    W: futures_util::Sink<Message, Error = E> + Unpin,
{
    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut keepalive = tokio::time::interval(keepalive_every);
    keepalive.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // First tick of a tokio interval completes immediately — burn both so neither
    // fires before any real time has passed.
    ticker.tick().await;
    keepalive.tick().await;
    let mut last_frame = tokio::time::Instant::now();

    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    let _ = write.send(Message::Close(None)).await;
                    break SessionEnd::Shutdown;
                }
            }
            _ = ticker.tick() => {
                let idle = last_frame.elapsed();
                if idle >= idle_limit {
                    warn!(
                        ws = "binance_ws", idle_s = idle.as_secs(),
                        "ws_idle_timeout: no inbound frame — treating socket as dead"
                    );
                    break SessionEnd::Lost(format!("idle timeout: no frame for {}s", idle.as_secs()));
                }
            }
            _ = keepalive.tick() => {
                if write.send(Message::Ping(Default::default())).await.is_err() {
                    break SessionEnd::Lost("keepalive ping send failed".into());
                }
            }
            msg = read.next() => {
                // EVERY inbound frame refreshes the watchdog, including Ping/Pong —
                // liveness is what we are measuring, not payload usefulness.
                last_frame = tokio::time::Instant::now();
                match msg {
                    Some(Ok(Message::Text(txt))) => handle_text(&txt, state, logger),
                    Some(Ok(Message::Ping(payload))) => {
                        let _ = write.send(Message::Pong(payload)).await;
                    }
                    Some(Ok(Message::Close(frame))) => {
                        break SessionEnd::Lost(format!("server close: {frame:?}"));
                    }
                    Some(Ok(_)) => {} // Pong / Binary / Frame — ignore
                    Some(Err(e)) => break SessionEnd::Lost(format!("read error: {e}")),
                    None => break SessionEnd::Lost("stream ended".into()),
                }
            }
        }
    }
}

/// Parse one Binance text frame and update shared state + the event log.
fn handle_text(txt: &str, state: &Shared, logger: &EventLogger) {
    let v: Value = match serde_json::from_str(txt) {
        Ok(v) => v,
        Err(_) => {
            state.counters.parse_errors.fetch_add(1, Ordering::Relaxed);
            return;
        }
    };

    // Subscription ack: {"result":null,"id":1} — no "e" field.
    if v.get("result").is_some() && v.get("e").is_none() {
        debug!(ws = "binance_ws", "subscribe ack");
        return;
    }

    let etype = v.get("e").and_then(Value::as_str).unwrap_or("");
    state.counters.binance_msgs.fetch_add(1, Ordering::Relaxed);
    let recv = now_ms();

    match etype {
        "aggTrade" => {
            let sym = v
                .get("s")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_lowercase();
            let price = v
                .get("p")
                .and_then(Value::as_str)
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0);
            let tms = v.get("T").and_then(Value::as_i64).unwrap_or(recv);
            let mut emit_tick = false;
            {
                let mut e = state.binance.entry(sym.clone()).or_default();
                e.symbol = sym.clone();
                e.last_price = price;
                e.last_trade_ms = tms;
                e.updated_ms = recv;
                // v2 tick-driven: throttle the aggTrade firehose, then fire a
                // decision trigger with the freshest sub-second price.
                if state.tick_driven.load(Ordering::Relaxed) && price > 0.0 {
                    let thr = state.tick_throttle_ms.load(Ordering::Relaxed);
                    if recv - e.tick_emit_ms >= thr {
                        e.tick_emit_ms = recv;
                        emit_tick = true;
                    }
                }
            }
            // Feed the freshest tick into the decision loop (current second) so the
            // entry fires on the sub-second move, not the 1s bar close. The 1s
            // finalized kline still arrives too (authoritative close + vol). The
            // PriceHistory update-in-place keeps the current second = latest tick.
            if emit_tick
                && let Some(tx) = state.kline_tx.get()
                && let Some(asset) = asset_for_symbol(&sym)
            {
                let _ = tx.send(crate::state::KlineClose {
                    asset,
                    t_s: recv / 1000,
                    close: price,
                    received_at_ms: recv,
                });
            }
            state.counters.binance_aggtrades.fetch_add(1, Ordering::Relaxed);
            logger.record(EventRecord {
                recv_ms: recv,
                source: "binance",
                event_type: "aggTrade".to_string(),
                key: sym,
                exch_ms: Some(tms),
                px: Some(price),
            });
        }
        "kline" => {
            let sym = v
                .get("s")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_lowercase();
            let k = v.get("k");
            let close = k
                .and_then(|k| k.get("c"))
                .and_then(Value::as_str)
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0);
            let open_ms = k.and_then(|k| k.get("t")).and_then(Value::as_i64).unwrap_or(0);
            let close_ms = k.and_then(|k| k.get("T")).and_then(Value::as_i64).unwrap_or(0);
            let is_final = k
                .and_then(|k| k.get("x"))
                .and_then(Value::as_bool)
                .unwrap_or(false);
            {
                let mut e = state.binance.entry(sym.clone()).or_default();
                e.symbol = sym.clone();
                e.kline_close = close;
                e.kline_open_ms = open_ms;
                e.kline_close_ms = close_ms;
                e.kline_final = is_final;
                e.updated_ms = recv;
            }
            state.counters.binance_klines.fetch_add(1, Ordering::Relaxed);
            // ORDER #14 B: DATA liveness. The counter above is what froze (identical
            // value for 45 h) while `binance_connected` still read true; this stamp is
            // what makes that condition observable to health + the feed watchdog.
            state.last_kline_ms.store(recv, Ordering::Relaxed);
            // Phase 6 D1: feed FINALIZED bars to the decision loop (open-second keyed,
            // matching the Capa B replay's `t_open_ms / 1000`). `kline_tx` is unset in
            // ingestion-only runs, so this is a no-op there.
            if is_final && close > 0.0 && open_ms > 0
                && let Some(tx) = state.kline_tx.get()
                && let Some(asset) = asset_for_symbol(&sym)
            {
                let _ = tx.send(crate::state::KlineClose {
                    asset,
                    t_s: open_ms / 1000,
                    close,
                    received_at_ms: recv,
                });
            }
            logger.record(EventRecord {
                recv_ms: recv,
                source: "binance",
                event_type: "kline".to_string(),
                key: sym,
                exch_ms: Some(close_ms),
                px: Some(close),
            });
        }
        other => debug!(ws = "binance_ws", etype = other, "unhandled binance event"),
    }
}

#[cfg(test)]
mod order14_tests {
    use super::*;
    use crate::state::SharedState;
    use tokio_tungstenite::tungstenite::Error as WsError;

    fn logger() -> EventLogger {
        // Disabled logger: returns before touching the filesystem.
        crate::events::spawn(std::path::Path::new("unused"), false).unwrap().0
    }

    /// ORDER #14 A — THE REGRESSION TEST FOR THE 45-HOUR OUTAGE.
    /// A half-open socket produces nothing at all: no error, no close frame, no EOF.
    /// Before the watchdog, `read.next()` never resolved and this loop parked forever
    /// while the bot reported healthy. Now it must break `Lost("idle timeout…")` and
    /// hand the session to the existing (proven) reconnect path.
    /// `start_paused` auto-advances virtual time, so 30s elapses instantly.
    #[tokio::test(start_paused = true)]
    async fn idle_timeout_breaks_a_half_open_session() {
        let state = SharedState::new();
        let lg = logger();
        let (_tx, mut rx) = watch::channel(false);
        // The exact incident shape: a stream that never yields anything, ever.
        let mut read = futures_util::stream::pending::<Result<Message, WsError>>();
        let mut write = futures_util::sink::drain::<Message>();

        let end = read_until_end(
            &mut read, &mut write, &state, &lg, &mut rx,
            Duration::from_secs(30), Duration::from_secs(15),
        ).await;

        match end {
            SessionEnd::Lost(r) => assert!(
                r.contains("idle timeout"),
                "half-open socket must end as an idle timeout, got: {r}"
            ),
            SessionEnd::Refresh => panic!("expected Lost(idle timeout), got Refresh"),
            SessionEnd::Shutdown => panic!("expected Lost(idle timeout), got Shutdown"),
        }
        // And the session must mark the feed disconnected on the way out (the caller
        // does this; here we assert the watchdog didn't silently keep it "connected").
        assert!(!state.binance_connected.load(Ordering::Relaxed));
    }

    /// Regression against a TOO-TIGHT threshold: a live-but-quiet socket that delivers
    /// a frame every idle_limit/2 must NEVER trip the watchdog. It ends only when the
    /// stream genuinely ends. (Binance server-pings every ~20s even on a silent tape,
    /// which is why 30s is safe.)
    #[tokio::test(start_paused = true)]
    async fn frames_at_half_the_limit_never_time_out() {
        let state = SharedState::new();
        let lg = logger();
        let (_tx, mut rx) = watch::channel(false);
        // A subscribe-ack every 15s, ten times, then EOF.
        let mut read = Box::pin(futures_util::stream::unfold(0u32, |i| async move {
            if i >= 10 {
                return None;
            }
            tokio::time::sleep(Duration::from_secs(15)).await;
            Some((Ok(Message::Text(r#"{"result":null,"id":1}"#.into())), i + 1))
        }));
        let mut write = futures_util::sink::drain::<Message>();

        let end = read_until_end(
            &mut read, &mut write, &state, &lg, &mut rx,
            Duration::from_secs(30), Duration::from_secs(15),
        ).await;

        match end {
            SessionEnd::Lost(r) => assert!(
                r.contains("stream ended") && !r.contains("idle timeout"),
                "a frame every idle/2 must not trip the watchdog; got: {r}"
            ),
            SessionEnd::Refresh => panic!("expected Lost(stream ended), got Refresh"),
            SessionEnd::Shutdown => panic!("expected Lost(stream ended), got Shutdown"),
        }
    }

    /// A PING counts as liveness (the watchdog measures frames, not payloads) — a
    /// tape so quiet that only server pings arrive must stay connected.
    #[tokio::test(start_paused = true)]
    async fn server_pings_alone_hold_the_session_open() {
        let state = SharedState::new();
        let lg = logger();
        let (_tx, mut rx) = watch::channel(false);
        let mut read = Box::pin(futures_util::stream::unfold(0u32, |i| async move {
            if i >= 5 {
                return None;
            }
            tokio::time::sleep(Duration::from_secs(20)).await; // Binance's ~20s ping
            Some((Ok(Message::Ping(Default::default())), i + 1))
        }));
        let mut write = futures_util::sink::drain::<Message>();

        let end = read_until_end(
            &mut read, &mut write, &state, &lg, &mut rx,
            Duration::from_secs(30), Duration::from_secs(15),
        ).await;

        match end {
            SessionEnd::Lost(r) => assert!(
                !r.contains("idle timeout"),
                "server pings must refresh the watchdog; got: {r}"
            ),
            _ => panic!("expected Lost(stream ended)"),
        }
    }

    /// Shutdown still wins over the watchdog (graceful stop must not look like a loss).
    #[tokio::test(start_paused = true)]
    async fn shutdown_beats_the_watchdog() {
        let state = SharedState::new();
        let lg = logger();
        let (tx, mut rx) = watch::channel(false);
        tx.send(true).unwrap();
        let mut read = futures_util::stream::pending::<Result<Message, WsError>>();
        let mut write = futures_util::sink::drain::<Message>();

        let end = read_until_end(
            &mut read, &mut write, &state, &lg, &mut rx,
            Duration::from_secs(30), Duration::from_secs(15),
        ).await;
        assert!(matches!(end, SessionEnd::Shutdown), "shutdown must not report Lost");
    }
}
