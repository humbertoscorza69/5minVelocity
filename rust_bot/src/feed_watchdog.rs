//! ORDER #14 C — Binance feed watchdog: automated action, not a banner.
//!
//! On 2026-07-25 the Binance socket went half-open and the bot ran blind for 45
//! hours while reporting `healthy=true`. Order #14 A stops the socket from parking
//! forever; Order #14 B makes health measure DATA. This module is the part that
//! *acts*: it watches kline liveness, records the outage in the oplog (there was
//! literally no record of the 45-hour one), halts NEW ENTRIES while the feed is
//! dead, and only re-enables them after a full vol-ring warmup so z/vol are never
//! computed on a partially-refilled ring.
//!
//! Deliberately NOT wired to `Controls::trading_enabled`: that flag is the
//! operator's, and it is persisted to `controls.json`. Auto-clearing it on recovery
//! could resurrect trading a human had deliberately paused. The halt lives in
//! `Shared::feed_halt` and gates opens only — exits, stops, settlement and
//! redemption run untouched, which is what you want during an outage.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::sync::watch;
use tracing::{error, info, warn};

use crate::oplog::OpLog;
use crate::state::{Shared, now_ms};

/// Escalate to an `alerts/` file once the feed has been dead this long. The
/// dashboard pill is not enough — nobody was looking at it for 45 hours.
const ESCALATE_MS: i64 = 600_000; // 10 min

/// A transition worth recording. Steady states produce nothing (no log spam — the
/// pre-#12 photo-finish flood is the cautionary tale).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeedEvent {
    /// Feed just went dead → halt entries, emit `feed_dead`.
    Dead { age_s: i64 },
    /// Still dead past [`ESCALATE_MS`] → write an alert file. Fires exactly once
    /// per outage.
    Escalate { down_s: i64 },
    /// Klines are flowing again → emit `feed_recovered`. Entries STAY halted for
    /// the warmup.
    Recovered { down_s: i64 },
    /// Warmup satisfied (a full vol lookback of fresh klines) → entries resume.
    WarmupDone,
}

/// The pure transition machine. Split from the task so every edge is unit-testable
/// without timers, sockets or an oplog.
#[derive(Debug)]
pub struct FeedWatchdog {
    dead: bool,
    dead_since_ms: i64,
    recovered_ms: i64,
    escalated: bool,
    halt: bool,
    warmup_ms: i64,
}

impl FeedWatchdog {
    #[must_use]
    pub fn new(warmup_ms: i64) -> Self {
        Self {
            dead: false,
            dead_since_ms: 0,
            recovered_ms: 0,
            escalated: false,
            halt: false,
            warmup_ms: warmup_ms.max(0),
        }
    }

    /// May the decision loop open new positions? Authoritative after [`Self::step`].
    #[must_use]
    pub fn halted(&self) -> bool {
        self.halt
    }

    /// Advance the machine one tick. `is_dead` / `age_ms` come from
    /// `Shared::feed_is_dead` / `Shared::feed_stale_ms`.
    pub fn step(&mut self, now: i64, is_dead: bool, age_ms: i64) -> Option<FeedEvent> {
        if is_dead {
            if !self.dead {
                self.dead = true;
                self.dead_since_ms = now;
                self.escalated = false;
                self.halt = true;
                return Some(FeedEvent::Dead { age_s: age_ms / 1000 });
            }
            // Still dead: escalate ONCE, then stay quiet until recovery.
            if !self.escalated && now - self.dead_since_ms >= ESCALATE_MS {
                self.escalated = true;
                return Some(FeedEvent::Escalate { down_s: (now - self.dead_since_ms) / 1000 });
            }
            return None;
        }
        if self.dead {
            // Data is flowing again — but the ring is only partially refilled, so
            // entries stay halted until the warmup below completes.
            self.dead = false;
            self.recovered_ms = now;
            self.halt = true;
            return Some(FeedEvent::Recovered { down_s: (now - self.dead_since_ms) / 1000 });
        }
        if self.halt && self.recovered_ms > 0 && now - self.recovered_ms >= self.warmup_ms {
            self.halt = false;
            return Some(FeedEvent::WarmupDone);
        }
        None
    }
}

/// Spawnable task: poll feed liveness, drive [`FeedWatchdog`], mirror the halt into
/// `Shared`, and record every transition.
///
/// `warmup_s` should be the LARGEST configured `vol_lookback_s` across intervals
/// (15m uses 120s vs 5m's 60s) so both interval strategies see a complete ring.
pub async fn run_feed_watchdog(
    state: Shared,
    oplog: Arc<OpLog>,
    alert_dir: String,
    warmup_s: i64,
    period: Duration,
    mut shutdown: watch::Receiver<bool>,
) {
    info!(
        warmup_s,
        period_s = period.as_secs(),
        feed_dead_ms = state.feed_dead_ms.load(Ordering::Relaxed),
        "task started: feed_watchdog (Binance kline liveness → entry halt)"
    );
    let mut wd = FeedWatchdog::new(warmup_s * 1000);
    let mut iv = tokio::time::interval(period);
    iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = iv.tick() => {
                let now = now_ms();
                let age = state.feed_stale_ms(now);
                let ev = wd.step(now, state.feed_is_dead(now), age);
                // Mirror the halt every tick so a restart of this task can't leave a
                // stale flag behind.
                state.feed_halt.store(wd.halted(), Ordering::Relaxed);
                match ev {
                    Some(FeedEvent::Dead { age_s }) => {
                        error!(age_s, "feed_dead: Binance klines stopped — NEW ENTRIES HALTED");
                        oplog.sys("feed_dead", serde_json::json!({
                            "ws": "binance_ws",
                            "last_kline_ms": state.last_kline_ms.load(Ordering::Relaxed),
                            "age_s": age_s,
                        }));
                    }
                    Some(FeedEvent::Escalate { down_s }) => {
                        error!(down_s, "feed_dead: still dead past 10 min — writing alert file");
                        crate::ws::write_alert(
                            &alert_dir,
                            "binance_ws",
                            "feed_dead",
                            &format!("no Binance kline for {down_s}s; entries halted"),
                        );
                        oplog.sys("feed_dead_escalated", serde_json::json!({
                            "ws": "binance_ws", "down_s": down_s,
                        }));
                    }
                    Some(FeedEvent::Recovered { down_s }) => {
                        state.feed_recovered_ms.store(now, Ordering::Relaxed);
                        warn!(down_s, warmup_s, "feed_recovered: klines flowing; entries held for warmup");
                        oplog.sys("feed_recovered", serde_json::json!({
                            "ws": "binance_ws", "down_s": down_s, "warmup_s": warmup_s,
                        }));
                    }
                    Some(FeedEvent::WarmupDone) => {
                        info!("feed warmup complete — entries re-enabled");
                        oplog.sys("feed_warmup_complete", serde_json::json!({
                            "ws": "binance_ws", "warmup_s": warmup_s,
                        }));
                    }
                    None => {}
                }
            }
            _ = shutdown.changed() => if *shutdown.borrow() { break },
        }
    }
    info!("feed_watchdog: shutdown");
}

#[cfg(test)]
mod tests {
    use super::*;

    const WARMUP_MS: i64 = 120_000; // 15m vol_lookback

    /// The 2026-07-25 shape: feed dies → exactly one `Dead` event, entries halted,
    /// and NO repeat events while it stays dead (until the 10-min escalation).
    #[test]
    fn dead_transition_halts_once_then_escalates_once() {
        let mut wd = FeedWatchdog::new(WARMUP_MS);
        let t0 = 1_000_000i64;
        assert_eq!(wd.step(t0, false, 0), None, "healthy feed is silent");
        assert!(!wd.halted());

        assert_eq!(wd.step(t0 + 1_000, true, 61_000), Some(FeedEvent::Dead { age_s: 61 }));
        assert!(wd.halted(), "entries must halt the moment the feed is dead");
        // Steady dead → silence (no 127k-event flood).
        assert_eq!(wd.step(t0 + 2_000, true, 62_000), None);
        assert_eq!(wd.step(t0 + 300_000, true, 361_000), None);
        // Past 10 min → escalate exactly once.
        let esc = wd.step(t0 + 1_000 + ESCALATE_MS, true, 660_000);
        assert!(matches!(esc, Some(FeedEvent::Escalate { .. })), "must escalate at 10 min, got {esc:?}");
        assert_eq!(wd.step(t0 + 2_000 + ESCALATE_MS, true, 661_000), None, "escalates only once");
        assert!(wd.halted());
    }

    /// Recovery does NOT immediately resume entries: the ring is partial, so the
    /// halt persists for a full vol lookback, then clears exactly once.
    #[test]
    fn recovery_holds_entries_until_warmup_completes() {
        let mut wd = FeedWatchdog::new(WARMUP_MS);
        let t0 = 1_000_000i64;
        wd.step(t0, true, 61_000); // dead
        assert!(wd.halted());

        let rec = wd.step(t0 + 60_000, false, 0);
        assert!(matches!(rec, Some(FeedEvent::Recovered { down_s: 60 })), "got {rec:?}");
        assert!(wd.halted(), "entries must STAY halted through the warmup");

        // Mid-warmup: still halted, still silent.
        assert_eq!(wd.step(t0 + 100_000, false, 0), None);
        assert!(wd.halted(), "a partially refilled ring must not be traded on");

        // Warmup satisfied → resume, once.
        assert_eq!(wd.step(t0 + 60_000 + WARMUP_MS, false, 0), Some(FeedEvent::WarmupDone));
        assert!(!wd.halted(), "entries resume after a full vol lookback of fresh klines");
        assert_eq!(wd.step(t0 + 60_000 + WARMUP_MS + 1_000, false, 0), None, "resumes only once");
    }

    /// A healthy process must never halt on its own (no spurious WarmupDone from the
    /// zero-initialised recovered timestamp).
    #[test]
    fn never_halts_without_a_death() {
        let mut wd = FeedWatchdog::new(WARMUP_MS);
        for i in 0..1_000 {
            assert_eq!(wd.step(1_000_000 + i * 1_000, false, 500), None);
            assert!(!wd.halted());
        }
    }

    /// A second outage after a clean recovery re-arms the whole cycle (including a
    /// fresh escalation), rather than staying latched from the first one.
    #[test]
    fn second_outage_rearms() {
        let mut wd = FeedWatchdog::new(WARMUP_MS);
        let t0 = 1_000_000i64;
        wd.step(t0, true, 61_000);
        wd.step(t0 + 10_000, false, 0);
        wd.step(t0 + 10_000 + WARMUP_MS, false, 0); // warmup done → running
        assert!(!wd.halted());

        assert!(matches!(wd.step(t0 + 500_000, true, 61_000), Some(FeedEvent::Dead { .. })));
        assert!(wd.halted());
        let esc = wd.step(t0 + 500_000 + ESCALATE_MS, true, 700_000);
        assert!(matches!(esc, Some(FeedEvent::Escalate { .. })), "a NEW outage escalates again");
    }
}
