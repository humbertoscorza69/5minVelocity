//! Pure exit-rule trigger predicates, shared between the backtester
//! (`backtest_tp::decide_exit_for_variant` — Smart and F6Only branches) and
//! the live exit task (consumed by piece 4 from `run_exit_task`).
//!
//! The goal is mechanical parity: given the same `(now_ms, entry_ms,
//! exit_ts_ms, ts_max_bid_ms, params)`, both code paths MUST return the
//! same bool. If a future change to the rule semantics is needed, edit it
//! HERE (single source of truth) so backtest and live cannot drift.
//!
//! These functions do NOT update the running max -- that lives in
//! `trading_loop::update_running_max` (live) and inline in the backtester's
//! trajectory walk. They only answer "with the current state, does the
//! rule trigger?".
//!
//! Both functions are total: they never panic on any (i64, i64, f64, i64)
//! input. Overflow on the `y_sec * 1000` multiplication is clamped via
//! `saturating_mul` (i64::MAX seconds * 1000 still overflows in theory, but
//! 9.2e18 / 1000 = 9.2e15 s = ~290 million years; not a realistic concern).

/// F6Only trigger: sell when time since the last running-max strict-high
/// reaches `y_sec` seconds. Returns true iff
/// `now_ms - ts_max_bid_ms >= y_sec * 1000`.
///
/// Caller's responsibility:
///   * Pass `now_ms` and `ts_max_bid_ms` from the SAME clock domain
///     (both wall-clock local ms, NOT exchange ts -- the live updates
///     `ts_max_bid_ms` from `now_ms()` so the difference is meaningful).
///   * `y_sec` is validated `> 0` at config load (`config.rs::validate`);
///     the function itself accepts any i64, but `y_sec <= 0` would make
///     the rule trigger on the very first observation post-entry.
///
/// Used by:
///   * Backtester: each trajectory sample where `ms - running_max_ms >=
///     y_sec * 1000` selects that sample as the exit (`backtest_tp.rs`).
///   * Live (piece 4): each 1s exit-task tick checks this against the
///     position's `ts_max_bid_ms` to decide if it must SELL now.
#[must_use]
pub fn f6_triggers(now_ms: i64, ts_max_bid_ms: i64, y_sec: i64) -> bool {
    now_ms - ts_max_bid_ms >= y_sec.saturating_mul(1_000)
}

/// Smart trigger: sell when BOTH conditions hold simultaneously --
///   1. f1 (time elapsed as a fraction of the hold): `100 * (now -
///      entry) / max(1, exit - entry) >= x_pct`.
///   2. f6 (time since last running-max strict-high): `now - ts_max_bid_ms
///      >= y_sec * 1000`.
///
/// The `max(1)` on the denominator mirrors the backtester's defensive
/// behavior for zero-duration positions (an edge case that should not
/// occur in production but must not divide by zero).
///
/// Parameter validation (`x_pct in (0.0, 100.0]`, `y_sec > 0`) happens at
/// config load (`config.rs::validate`); the function accepts any
/// `(f64, i64)` and behaves mathematically.
///
/// Used by:
///   * Backtester: each trajectory sample where both conditions hold
///     selects that sample as the exit (`backtest_tp.rs`).
///   * Live (piece 4): each 1s exit-task tick checks both conditions
///     against the position's state.
#[must_use]
pub fn smart_triggers(
    now_ms: i64,
    entry_ms: i64,
    exit_ts_ms: i64,
    ts_max_bid_ms: i64,
    x_pct: f64,
    y_sec: i64,
) -> bool {
    let hold_duration_ms = (exit_ts_ms - entry_ms).max(1) as f64;
    let f1_pct = 100.0 * (now_ms - entry_ms) as f64 / hold_duration_ms;
    let f6_ms = now_ms - ts_max_bid_ms;
    f1_pct >= x_pct && f6_ms >= y_sec.saturating_mul(1_000)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // f6_triggers
    // ========================================================================

    #[test]
    fn f6_triggers_returns_false_just_below_threshold() {
        // y_sec = 30 -> y_ms = 30_000. now - ts_max = 29_999 -> false.
        let t = 1_780_000_000_000_i64;
        assert!(!f6_triggers(t + 29_999, t, 30));
    }

    #[test]
    fn f6_triggers_returns_true_at_exact_threshold() {
        // Boundary: >= must include equality. y_sec=30 -> trigger at exactly 30_000ms.
        let t = 1_780_000_000_000_i64;
        assert!(f6_triggers(t + 30_000, t, 30));
    }

    #[test]
    fn f6_triggers_returns_true_far_past_threshold() {
        let t = 1_780_000_000_000_i64;
        assert!(f6_triggers(t + 5 * 60 * 1000, t, 30));
    }

    // ========================================================================
    // smart_triggers
    // ========================================================================

    /// Common params used in the smart_triggers tests below: 1-hour hold,
    /// x_pct=50%, y_sec=30s. Trigger requires t >= entry + 30 min (f1) AND
    /// t >= ts_max + 30 s (f6).
    const X_PCT: f64 = 50.0;
    const Y_SEC: i64 = 30;
    fn entry() -> i64 { 1_780_000_000_000 }
    fn exit_ts() -> i64 { entry() + 3_600_000 } // 1h hold

    #[test]
    fn smart_triggers_false_when_neither_met() {
        // 10 min in (f1 ~16.7%), peak 10s ago (f6 ~10s): NEITHER met.
        let now = entry() + 10 * 60 * 1000;
        let ts_max = now - 10 * 1000;
        assert!(!smart_triggers(now, entry(), exit_ts(), ts_max, X_PCT, Y_SEC));
    }

    #[test]
    fn smart_triggers_false_when_only_f1_met() {
        // 45 min in (f1=75% >= 50%) BUT peak was just now (f6 = 0 < 30s).
        let now = entry() + 45 * 60 * 1000;
        let ts_max = now; // same instant -> f6 = 0
        assert!(!smart_triggers(now, entry(), exit_ts(), ts_max, X_PCT, Y_SEC));
    }

    #[test]
    fn smart_triggers_false_when_only_f6_met() {
        // 10 min in (f1 ~16.7% < 50%), peak 60s ago (f6 = 60s >= 30s).
        let now = entry() + 10 * 60 * 1000;
        let ts_max = now - 60 * 1000;
        assert!(!smart_triggers(now, entry(), exit_ts(), ts_max, X_PCT, Y_SEC));
    }

    #[test]
    fn smart_triggers_true_when_both_met() {
        // 45 min in (f1=75%) + peak 60s ago (f6=60s): both met.
        let now = entry() + 45 * 60 * 1000;
        let ts_max = now - 60 * 1000;
        assert!(smart_triggers(now, entry(), exit_ts(), ts_max, X_PCT, Y_SEC));
    }

    #[test]
    fn smart_triggers_zero_hold_duration_safety() {
        // entry_ms == exit_ts_ms -> denominator would be 0; max(1) guard
        // makes it 1 ms, so f1 explodes to ~Inf% and trivially >= x_pct.
        // The function must not panic / overflow.
        let now = entry() + 1_000;
        let ts_max = now - 60 * 1_000; // f6 met
        let exit = entry(); // ZERO-duration hold
        let result = smart_triggers(now, entry(), exit, ts_max, X_PCT, Y_SEC);
        assert!(result, "zero-duration: f1 explodes, f6 met -> trigger (no panic)");
    }
}
