//! Order #15 B3 — apply the Order #14 lesson from day one.
//!
//! We just lost 45 hours to a feed that died while reporting healthy. The recorder
//! must not be able to do that silently, so staleness detection and gap accounting
//! are built in from the first commit rather than bolted on after an incident:
//!
//!   * per-channel message counters + last-message timestamp, flushed to a heartbeat
//!     file every 30 s;
//!   * a staleness watchdog that reconnects when a subscribed channel goes quiet;
//!   * **a gap log as a FIRST-CLASS OUTPUT** — an analysis that silently sits on
//!     missing hours is worse than no analysis. That is what voided the weekend exam.
//!
//! Everything here is pure and clock-injected so the "simulated disconnect produces
//! exactly one gap record with correct bounds" test is exact rather than timing-flaky.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// A recorded outage. First-class output: written to its own `gaps.jsonl`, never
/// folded into a summary line, so a downstream analysis cannot fail to see it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Gap {
    pub channel: String,
    /// Last moment we had data (the last message before the silence).
    pub start_ms: i64,
    /// When data resumed.
    pub end_ms: i64,
    pub duration_ms: i64,
    /// What ended the gap: a reconnect, or the process starting up again.
    pub cause: GapCause,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GapCause {
    /// The staleness watchdog fired and the channel later resumed.
    Stale,
    /// The process was not running (PC sleep / restart). B3 requires we survive this
    /// and STILL record the gap.
    ProcessDown,
}

/// Per-channel liveness counters. Flushed to the heartbeat file as-is.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ChannelStat {
    pub messages: u64,
    pub last_ms: i64,
    /// Gaps closed on this channel so far this run.
    pub gaps: u64,
}

/// Tracks liveness per channel and emits exactly one [`Gap`] per outage.
#[derive(Debug)]
pub struct GapTracker {
    stale_after_ms: i64,
    stats: BTreeMap<String, ChannelStat>,
    /// Channels currently considered dead → the timestamp of their last message.
    dead_since: BTreeMap<String, i64>,
}

impl GapTracker {
    #[must_use]
    pub fn new(stale_after_ms: i64) -> Self {
        Self { stale_after_ms, stats: BTreeMap::new(), dead_since: BTreeMap::new() }
    }

    #[must_use]
    pub fn stats(&self) -> &BTreeMap<String, ChannelStat> {
        &self.stats
    }

    #[must_use]
    pub fn is_stale(&self, channel: &str, now_ms: i64) -> bool {
        self.stats
            .get(channel)
            .is_some_and(|s| s.last_ms > 0 && now_ms - s.last_ms > self.stale_after_ms)
    }

    /// Every channel currently past the staleness threshold — the watchdog's
    /// reconnect trigger.
    #[must_use]
    pub fn stale_channels(&self, now_ms: i64) -> Vec<String> {
        self.stats
            .keys()
            .filter(|c| self.is_stale(c, now_ms))
            .cloned()
            .collect()
    }

    /// Record a message. Closes an open gap and returns it — exactly once, because
    /// `dead_since` is removed as it is reported.
    pub fn on_message(&mut self, channel: &str, now_ms: i64) -> Option<Gap> {
        let st = self.stats.entry(channel.to_string()).or_default();
        st.messages += 1;
        let prev_last = st.last_ms;
        st.last_ms = now_ms;

        // An explicitly-marked outage takes precedence over inferring one — but only
        // if it actually lasted longer than the staleness threshold. A reconnect blip
        // (observed live: 1.4s) is NOT a data gap, and logging it as one fills the
        // gap log with noise, which defeats the whole point of B3: a real missing
        // hour must be impossible to overlook.
        if let Some(start) = self.dead_since.remove(channel) {
            if now_ms - start <= self.stale_after_ms {
                return None;
            }
            let st = self.stats.entry(channel.to_string()).or_default();
            st.gaps += 1;
            return Some(Gap {
                channel: channel.to_string(),
                start_ms: start,
                end_ms: now_ms,
                duration_ms: now_ms - start,
                cause: GapCause::Stale,
            });
        }
        // Otherwise infer: silence longer than the threshold that the watchdog never
        // got to see (e.g. the process was asleep between the two messages).
        if prev_last > 0 && now_ms - prev_last > self.stale_after_ms {
            let st = self.stats.entry(channel.to_string()).or_default();
            st.gaps += 1;
            return Some(Gap {
                channel: channel.to_string(),
                start_ms: prev_last,
                end_ms: now_ms,
                duration_ms: now_ms - prev_last,
                cause: GapCause::ProcessDown,
            });
        }
        None
    }

    /// Mark a channel dead (the watchdog fired, or we saw a disconnect). Idempotent:
    /// calling it repeatedly during one outage still yields ONE gap, anchored to the
    /// last good message.
    pub fn mark_dead(&mut self, channel: &str) {
        let last = self.stats.get(channel).map(|s| s.last_ms).unwrap_or(0);
        if last > 0 {
            self.dead_since.entry(channel.to_string()).or_insert(last);
        }
    }

    /// Mark every currently-stale channel dead. Returns how many were newly marked.
    pub fn mark_stale_dead(&mut self, now_ms: i64) -> usize {
        let stale = self.stale_channels(now_ms);
        let before = self.dead_since.len();
        for c in stale {
            self.mark_dead(&c);
        }
        self.dead_since.len() - before
    }

    /// Resuming after a process restart: seed each channel's last-seen from the
    /// previous run's heartbeat so the downtime is recorded rather than lost (B3:
    /// "survive PC sleep/restart: resume cleanly, and ALWAYS record the gap").
    pub fn seed_from_heartbeat(&mut self, hb: &Heartbeat) {
        for (channel, st) in &hb.channels {
            self.stats.insert(channel.clone(), ChannelStat { messages: 0, last_ms: st.last_ms, gaps: 0 });
        }
    }
}

/// Flushed to disk every 30 s so an outage is visible from outside the process — and
/// so the NEXT run can measure the downtime it was absent for.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Heartbeat {
    pub ts_ms: i64,
    pub pid: u32,
    pub channels: BTreeMap<String, ChannelStat>,
    /// Pre-compression bytes written per channel — B4 asks the real byte rate be
    /// measured in the first hour and reported before a retention policy is chosen.
    #[serde(default)]
    pub bytes: BTreeMap<String, u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    const STALE: i64 = 30_000;

    /// ORDER #15 B3 — THE REQUIRED TEST: a simulated disconnect produces EXACTLY ONE
    /// gap record, with correct bounds (last good message → resumption).
    #[test]
    fn simulated_disconnect_produces_exactly_one_gap_with_correct_bounds() {
        let mut t = GapTracker::new(STALE);
        assert!(t.on_message("book", 1_000).is_none(), "first message opens no gap");
        assert!(t.on_message("book", 2_000).is_none());

        // Silence. The watchdog notices at 100_000 and fires repeatedly while dead.
        assert!(t.is_stale("book", 100_000), "30s+ of silence is stale");
        assert_eq!(t.mark_stale_dead(100_000), 1, "one channel newly marked dead");
        t.mark_dead("book"); // watchdog fires again mid-outage
        t.mark_dead("book"); // and again
        assert_eq!(t.mark_stale_dead(150_000), 0, "already dead — not re-marked");

        // Data resumes.
        let gap = t.on_message("book", 200_000).expect("the gap must be reported");
        assert_eq!(gap.channel, "book");
        assert_eq!(gap.start_ms, 2_000, "bounded by the LAST GOOD message");
        assert_eq!(gap.end_ms, 200_000, "…and by resumption");
        assert_eq!(gap.duration_ms, 198_000);
        assert_eq!(gap.cause, GapCause::Stale);

        // Exactly one: subsequent messages must not re-report it.
        assert!(t.on_message("book", 200_100).is_none(), "the gap is reported ONCE");
        assert!(t.on_message("book", 200_200).is_none());
        assert_eq!(t.stats()["book"].gaps, 1);
    }

    /// Silence the watchdog never saw (process asleep) must STILL be recorded — the
    /// 45-hour lesson: a gap nobody logged is a gap nobody notices.
    #[test]
    fn unobserved_silence_is_still_recorded_as_a_gap() {
        let mut t = GapTracker::new(STALE);
        t.on_message("price_change", 1_000);
        // Next message arrives 10 minutes later; nothing marked it dead in between.
        let gap = t.on_message("price_change", 601_000).expect("must infer the gap");
        assert_eq!(gap.cause, GapCause::ProcessDown);
        assert_eq!((gap.start_ms, gap.end_ms, gap.duration_ms), (1_000, 601_000, 600_000));
    }

    /// Normal jitter under the threshold is NOT a gap (no false positives, or the
    /// gap log becomes noise nobody reads).
    #[test]
    fn jitter_under_the_threshold_is_not_a_gap() {
        let mut t = GapTracker::new(STALE);
        t.on_message("book", 0);
        for i in 1..=20 {
            assert!(t.on_message("book", i * 25_000).is_none(), "25s jitter must not fire");
        }
        assert_eq!(t.stats()["book"].gaps, 0);
        assert_eq!(t.stats()["book"].messages, 21);
    }

    /// Channels are tracked INDEPENDENTLY — `book` dying while `price_change` flows
    /// is precisely the failure that crippled the June archive.
    #[test]
    fn channels_are_tracked_independently() {
        let mut t = GapTracker::new(STALE);
        t.on_message("book", 1_000);
        t.on_message("price_change", 1_000);
        // price_change keeps flowing; book goes quiet.
        for i in 1..=10 {
            t.on_message("price_change", 1_000 + i * 10_000);
        }
        assert!(t.is_stale("book", 101_000), "book is stale");
        assert!(!t.is_stale("price_change", 101_000), "…while price_change is fine");
        assert_eq!(t.stale_channels(101_000), vec!["book"]);
    }

    /// LIVE-CAUGHT REGRESSION: a websocket reconnect blip is not a data gap. The
    /// first live run produced five 1.0–1.5s "gaps" from one ordinary reconnect —
    /// noise that would bury a real missing hour. Only outages longer than the
    /// staleness threshold count.
    #[test]
    fn a_reconnect_blip_is_not_a_gap() {
        let mut t = GapTracker::new(STALE);
        t.on_message("book", 1_000);
        // A reconnect: marked dead, back 1.4s later.
        t.mark_dead("book");
        assert!(t.on_message("book", 2_400).is_none(), "1.4s reconnect must NOT log a gap");
        assert_eq!(t.stats()["book"].gaps, 0);

        // But a genuine outage past the threshold still does.
        t.mark_dead("book");
        let gap = t.on_message("book", 2_400 + STALE + 1).expect("a real outage must be logged");
        assert_eq!(gap.start_ms, 2_400);
        assert_eq!(t.stats()["book"].gaps, 1);
    }

    /// A restart seeds from the previous heartbeat, so downtime the process was
    /// absent for is recorded instead of vanishing.
    #[test]
    fn restart_records_the_downtime_it_was_absent_for() {
        let mut hb = Heartbeat { ts_ms: 500_000, pid: 1, ..Default::default() };
        hb.channels.insert("book".into(), ChannelStat { messages: 900, last_ms: 500_000, gaps: 0 });

        let mut t = GapTracker::new(STALE);
        t.seed_from_heartbeat(&hb);
        // The process was down for an hour; the first message back must report it.
        let gap = t.on_message("book", 4_100_000).expect("restart downtime must be recorded");
        assert_eq!(gap.start_ms, 500_000, "anchored to the last heartbeat before the restart");
        assert_eq!(gap.duration_ms, 3_600_000);
        assert_eq!(gap.cause, GapCause::ProcessDown);
    }
}
