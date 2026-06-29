//! Market lifecycle clock (deferred from Phase 1 §3 — its consumer, the decision
//! engine, now exists).
//!
//! A Polymarket up/down market for `(asset, interval, epoch)` resolves at
//! `epoch + interval_secs` (its `end_time`). The active→expired transition is
//! evaluated against the WALL CLOCK continuously — NOT against the discovery poll
//! — so the decision engine never decides on (and the executor closes at) a
//! market that has crossed its resolution instant. Pure integer arithmetic; the
//! `now`/clock is injected by the caller (live = real clock, replay = data).

/// Resolution instant (epoch SECONDS) of a market = `epoch + interval_secs`.
pub fn resolution_ts(epoch: i64, interval_secs: i64) -> i64 {
    epoch + interval_secs
}

/// Time-to-resolution in seconds at `at_ts` (negative once past resolution).
pub fn ttr(epoch: i64, interval_secs: i64, at_ts: i64) -> i64 {
    resolution_ts(epoch, interval_secs) - at_ts
}

/// Expired iff wall-clock `now_ms` is AT or PAST the resolution instant
/// (`end_time`). The boundary is inclusive: at the exact end_time the market is
/// already expired (no entries, no book-bid exits — the book is gone/settling).
pub fn is_expired(epoch: i64, interval_secs: i64, now_ms: i64) -> bool {
    now_ms >= resolution_ts(epoch, interval_secs) * 1000
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolution_and_ttr() {
        // 5m market opening at epoch 1000 resolves at 1300.
        assert_eq!(resolution_ts(1000, 300), 1300);
        assert_eq!(ttr(1000, 300, 1000), 300); // full window at open
        assert_eq!(ttr(1000, 300, 1270), 30); // 30s left
        assert_eq!(ttr(1000, 300, 1300), 0); // at resolution
        assert_eq!(ttr(1000, 300, 1310), -10); // past
    }

    #[test]
    fn expired_at_exact_end_time_instant() {
        let epoch = 1_780_000_200i64; // a 15m epoch
        let secs = 900i64;
        let end_ms = resolution_ts(epoch, secs) * 1000; // 1_780_001_100_000
        assert!(!is_expired(epoch, secs, end_ms - 1), "1ms before end_time: active");
        assert!(is_expired(epoch, secs, end_ms), "exactly at end_time: expired");
        assert!(is_expired(epoch, secs, end_ms + 1), "1ms after: expired");
    }

    #[test]
    fn five_minute_market_expiry() {
        let epoch = 1_780_000_200i64; // divisible by 300
        let secs = 300i64;
        let end_ms = 1_780_000_500_000i64;
        assert!(!is_expired(epoch, secs, end_ms - 1000));
        assert!(is_expired(epoch, secs, end_ms));
    }
}
