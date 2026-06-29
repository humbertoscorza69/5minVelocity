//! Phase 6 E′ — minimum safety guards (strict, BEFORE autonomous capital in D).
//!
//! A2 proved the execution path hides read bugs that only surface live (4 caught).
//! D runs sustained autonomous capital, where the bug N+1 will appear — these guards
//! are what protect the wallet when it does. Strict guards FIRST, autonomy AFTER.
//!
//! The five (and WHEN each fires):
//!   1. CAPS               — before EVERY entry POST (stake / per-token / total / hard)
//!   2. DAILY-LOSS-STOP    — before every entry; on hit → CLOSE-ONLY (no new opens)
//!   3. FREQUENCY BREAKER  — before every entry (~10/hr; cuts a runaway loop early)
//!   4. KILL-SWITCH        — at startup + every loop pass (a file = clean halt)
//!   5. FEED-DEAD + STALE  — before every entry (no trading on a frozen book)
//!
//! CRITICAL — the caps read the CORRECTED state from A2, or they would not protect:
//!   * exposure = Σ ACTIVE lots only (`exec::active_exposure*`, fix 2) — resolved /
//!     redeemable / decided positions are NOT exposure (the "21 open" miscount).
//!   * daily P&L = Σ NET p&l (after fees, the §4 fix) — gross would understate losses.

#![allow(dead_code)] // wired into the LiveExecutor here + the integrated bot (D)

use std::path::{Path, PathBuf};

use rust_decimal::Decimal;

/// Caps + thresholds. Defaults are the approved E′ values.
#[derive(Debug, Clone)]
pub struct GuardConfig {
    pub stake_cap: Decimal,          // $1 per order
    pub per_token_cap: Decimal,      // $5 active per token
    pub total_exposure_cap: Decimal, // $25 active total
    pub daily_loss_stop_stakes: Decimal, // daily stop (USD) = stake_cap * this; stake-multiple, not fixed $
    /// COMBO Phase 3 (2026-06-08): ABSOLUTE daily-loss cap override, in USD.
    /// When `Some`, `daily_loss_cap()` returns this fixed dollar amount and
    /// IGNORES the stake-multiple derivation. When `None`, falls back to the
    /// stake-multiple (`stake_cap * daily_loss_stop_stakes`).
    ///
    /// WHY ABSOLUTE: the B3 deploy decision is a fixed $15/day risk budget.
    /// $15 is NOT a clean multiple of the $1.05 stake ($15 / $1.05 = 14.2857),
    /// so the stake-multiple model can't express it exactly. Reasoning about
    /// risk in absolute dollars (not stake-multiples) is also the right frame
    /// for a live-capital deploy. Set to `None` to revert to stake-scaling.
    pub daily_loss_cap_usdc: Option<Decimal>,
    pub hard_cap: Decimal,           // $100 absolute (Phase 0)
    // W7 (2026-06-05): the FREQUENCY breaker (max_orders_per_hour) was removed.
    // Rationale: runaway is already bounded by max_open_positions=3 (a loop
    // cannot open more than 3 lots) and capital risk by daily_loss_stop. The
    // breaker also had a latent counting bug -- it incremented at intent
    // dispatch (not real POST), so paper / gated / aborted opens all counted.
    pub feed_dead_ms: i64,           // 30_000 — no price_change ⇒ feed stale
    pub staleness_max_ms: i64,       // 3_000 — book older than this ⇒ reject (Capa B)
    pub kill_switch_path: PathBuf,
}

impl Default for GuardConfig {
    fn default() -> Self {
        Self {
            // D4 stake: $1.05 (user-confirmed). Cap matches so stake_ok($1.05, $1.05)
            // passes; raise here (and base_usdc in bot.toml) to lift in D5.
            stake_cap: Decimal::new(105, 2),
            per_token_cap: Decimal::new(5, 0),
            total_exposure_cap: Decimal::new(25, 0),
            daily_loss_stop_stakes: Decimal::new(12, 0), // 12 stakes: > worst hist ~8-9, < strangling
            // COMBO Phase 3: absolute $15.00/day cap (user decision for the B3
            // deploy). Overrides the stake-multiple above. $15.00 = Decimal(1500, 2).
            daily_loss_cap_usdc: Some(Decimal::new(1500, 2)),
            hard_cap: Decimal::new(100, 0),
            feed_dead_ms: 30_000,
            staleness_max_ms: 3_000,
            kill_switch_path: PathBuf::from("KILL_SWITCH.txt"),
        }
    }
}

impl GuardConfig {
    /// Daily-loss-stop in USD, DERIVED = `stake_cap * daily_loss_stop_stakes`. Tied to
    /// the STAKE, not a fixed dollar number, because drawdown scales with stake. Worst
    /// historical drawdown was ~8-9 stakes; 12 leaves room for a normal bad streak to
    /// recover (the stop is CLOSE-ONLY, not a liquidation) yet trips if the loss
    /// clearly exceeds history (= "not normal variance, something is broken").
    /// Stake $1 -> $12; stake $2 -> $24. Adjust holgura via `daily_loss_stop_stakes`
    /// in config (e.g. 15) with no code change.
    ///
    /// COMBO Phase 3: if `daily_loss_cap_usdc` is `Some`, that ABSOLUTE dollar
    /// amount wins and the stake-multiple is ignored (the B3 deploy uses a
    /// fixed $15/day). `None` falls back to the stake-multiple derivation.
    #[must_use]
    pub fn daily_loss_cap(&self) -> Decimal {
        match self.daily_loss_cap_usdc {
            Some(usd) => usd,
            None => self.stake_cap * self.daily_loss_stop_stakes,
        }
    }
}

/// The verdict for a proposed ENTRY (a new BUY). `Deny` = no order at all; `CloseOnly`
/// = daily-loss-stop hit, don't OPEN new (in-flight closes still allowed).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GuardVerdict {
    Allow,
    Deny(String),
    CloseOnly(String),
}

impl GuardVerdict {
    #[must_use]
    pub fn is_allow(&self) -> bool {
        matches!(self, GuardVerdict::Allow)
    }
    #[must_use]
    pub fn reason(&self) -> Option<&str> {
        match self {
            GuardVerdict::Allow => None,
            GuardVerdict::Deny(r) | GuardVerdict::CloseOnly(r) => Some(r),
        }
    }
}

// ============================ pure checks (each unit-tested) ============================

/// Per-order stake cap.
#[must_use]
pub fn stake_ok(stake: Decimal, cap: Decimal) -> bool {
    stake <= cap
}

/// Active per-token exposure + this order ≤ per-token cap. `active_token_exposure`
/// MUST come from `exec::active_exposure_for_token` (active-only).
#[must_use]
pub fn per_token_ok(active_token_exposure: Decimal, order_usd: Decimal, cap: Decimal) -> bool {
    active_token_exposure + order_usd <= cap
}

/// Active TOTAL exposure + this order ≤ total cap. `active_total_exposure` MUST come
/// from `exec::active_exposure` (active-only — resolved positions excluded).
#[must_use]
pub fn total_exposure_ok(active_total_exposure: Decimal, order_usd: Decimal, cap: Decimal) -> bool {
    active_total_exposure + order_usd <= cap
}

/// Absolute hard cap (Phase 0): active total + order ≤ $100.
#[must_use]
pub fn hard_cap_ok(active_total_exposure: Decimal, order_usd: Decimal, hard_cap: Decimal) -> bool {
    active_total_exposure + order_usd <= hard_cap
}

/// Daily-loss-stop: `true` (keep opening) while the day's NET P&L is ABOVE −cap.
/// `daily_net_pnl` MUST be the sum of NET (after-fee) P&L — gross would mis-trigger.
#[must_use]
pub fn daily_loss_ok(daily_net_pnl: Decimal, cap: Decimal) -> bool {
    daily_net_pnl > -cap
}

/// Kill-switch present ⇒ halt.
#[must_use]
pub fn kill_switch_active(path: &Path) -> bool {
    path.exists()
}

/// Default arming-file path for the AGENT's manual real-order path.
pub const LIVE_ARMED_PATH: &str = "LIVE_ARMED.txt";

/// OPT-IN arming gate for the AGENT's manual real-capital path (`--live-test-execute`
/// and any dev tool that can POST a real order). This is opt-IN (must be present to
/// allow), the inverse of the kill-switch (opt-out, present to stop). It exists
/// because the agent fired an unauthorized real round-trip while "testing a flag":
/// real capital must require the user's explicit arming, not merely the absence of a
/// brake.
///
/// IMPORTANT — this gate is for the AGENT-DEVELOPING ONLY. The PRODUCTION BOT
/// (`--mode live`, launched by the user with `--confirm-live`) is armed ONCE at
/// launch and then executes its strategy autonomously, order by order, with NO
/// per-order gate. This function MUST NOT be called on the production trading-loop
/// path — only on the manual dev/test execute path.
///
/// Returns `true` iff the arming file exists AND is non-empty (a deliberate act:
/// the user writes a token into it). An empty/missing file = NOT armed.
#[must_use]
pub fn live_armed(path: &Path) -> bool {
    std::fs::read_to_string(path).map(|s| !s.trim().is_empty()).unwrap_or(false)
}

/// Feed-dead: no price_change within `timeout_ms` on an active token (vivo-pero-mudo).
#[must_use]
pub fn feed_dead(last_price_change_ms: i64, now_ms: i64, timeout_ms: i64) -> bool {
    now_ms - last_price_change_ms > timeout_ms
}

/// Staleness: book older than `max_ms` (Capa B per-decision gate; first line).
#[must_use]
pub fn book_stale(book_ts_ms: i64, now_ms: i64, max_ms: i64) -> bool {
    now_ms - book_ts_ms > max_ms
}

/// UTC day index (for resetting the daily-loss accumulator at midnight UTC).
#[must_use]
pub fn utc_day(now_ms: i64) -> i64 {
    now_ms.div_euclid(86_400_000)
}

// ============================ stateful coordinator ============================

/// Halt-state transition reported by [`Guards::check_continuous`]. The caller
/// (decision_loop) uses this to log ONLY on transitions, not every pass --
/// without that discipline a real `Stable(Some)` repeats the same oplog line
/// once per second forever once a halt latches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HaltTransition {
    /// No state change from the previous call (Some->Some or None->None).
    /// Caller MUST NOT log -- this is the normal idle case (or the normal
    /// "still halted" case during a multi-second incident).
    Stable,
    /// None -> Some. A halt just tripped this pass. Caller emits the
    /// `guard_halt` oplog event ONCE here.
    Tripped,
    /// Some -> None. The halt cleared this pass (W6-redux fix: the
    /// pre-W6-redux code never produced this -- halts latched forever).
    /// Caller emits `guard_resume` here.
    Resumed,
}

/// Result of one [`Guards::check_continuous`] pass: the CURRENT halt state
/// (`None` = clear to process) plus the transition since the previous call.
#[derive(Debug, Clone, PartialEq)]
pub struct ContinuousCheckResult {
    /// Current latched halt reason, if any. `None` = clear to process this pass.
    pub halted: Option<String>,
    /// Edge transition since the previous call (caller uses this to log once
    /// per transition, not once per pass).
    pub transition: HaltTransition,
}

/// Holds the runtime state the guards accumulate (order times, daily net P&L,
/// per-token last price_change) + runs the full pre-entry check in priority order.
pub struct Guards {
    pub cfg: GuardConfig,
    daily_net_pnl: Decimal,
    daily_day: i64,
    /// Latched halt reason; cleared automatically by `check_continuous`
    /// when both conditions (kill-switch + feed-dead) become inactive
    /// (W6-redux fix; pre-W6-redux this latch had no unlatch path and any
    /// transient FEED-DEAD at startup pinned the bot to halted forever).
    halted: Option<String>,
}

/// What the executor must supply to evaluate one proposed entry.
pub struct EntryContext<'a> {
    pub stake_usd: Decimal,
    pub order_usd: Decimal, // notional this order adds to exposure (≈ stake)
    pub token: &'a str,
    pub active_total_exposure: Decimal, // from exec::active_exposure (active-only)
    pub active_token_exposure: Decimal, // from exec::active_exposure_for_token
    pub book_ts_ms: i64,
    pub last_price_change_ms: i64,
    pub now_ms: i64,
}

impl Guards {
    #[must_use]
    pub fn new(cfg: GuardConfig) -> Self {
        Self { cfg, daily_net_pnl: Decimal::ZERO, daily_day: 0, halted: None }
    }

    /// Record a realized NET P&L, resetting the accumulator at the UTC day boundary.
    pub fn record_net_pnl(&mut self, net: Decimal, now_ms: i64) {
        let day = utc_day(now_ms);
        if day != self.daily_day {
            self.daily_day = day;
            self.daily_net_pnl = Decimal::ZERO;
        }
        self.daily_net_pnl += net;
    }

    /// G8: explicit manual reset of the daily P&L accumulator to zero.
    /// Used by the `--guard-reset-daily-pnl` CLI flag as an operator escape
    /// hatch when the counter is known to be "dirty" (e.g. a partial catch-up
    /// inflated today's number with stale losses, or a bug got the wrong sign
    /// into the counter). The caller logs the reason explicitly so the action
    /// is traceable in the audit log.
    ///
    /// NOT triggered automatically anywhere. Production code MUST keep using
    /// `record_net_pnl` for normal accounting -- this is purely manual.
    pub fn reset_daily_pnl(&mut self) {
        self.daily_net_pnl = Decimal::ZERO;
    }

    #[must_use]
    pub fn daily_net_pnl(&self) -> Decimal {
        self.daily_net_pnl
    }

    /// Latched halt? (kill-switch / feed-dead set it; checked each loop pass.)
    #[must_use]
    pub fn halted(&self) -> Option<&str> {
        self.halted.as_deref()
    }

    /// Continuous safety check (run at startup + every loop pass, NOT only
    /// pre-POST): kill-switch + feed-dead set/clear a halt. Returns the
    /// current halt state PLUS the edge transition since the previous call
    /// so the caller can log ONLY on transitions (not every pass).
    ///
    /// W6-redux (2026-06-05 fix): previously this was a latch with NO
    /// unlatch path -- once `self.halted` became `Some`, it stayed `Some`
    /// forever (no `else` branch cleared it). The reason string ALSO
    /// froze, producing the diagnostic-confusing oplog where
    /// `last_feed_ms` / `now_ms` were both fresh (13 ms apart) but the
    /// reason said `"no price_change for 30139ms"` (frozen from the
    /// original startup trip 30 s after boot, when the feed was still
    /// warming up). Without unlatch, removing the kill-switch file ALSO
    /// failed to resume the bot. Now both conditions auto-clear when
    /// they go inactive; the caller emits `guard_halt` on `Tripped` and
    /// `guard_resume` on `Resumed`, NOTHING on `Stable`.
    pub fn check_continuous(&mut self, last_price_change_ms: i64, now_ms: i64) -> ContinuousCheckResult {
        let was_halted = self.halted.is_some();
        if kill_switch_active(&self.cfg.kill_switch_path) {
            self.halted = Some(format!("KILL-SWITCH present ({})", self.cfg.kill_switch_path.display()));
        } else if feed_dead(last_price_change_ms, now_ms, self.cfg.feed_dead_ms) {
            self.halted = Some(format!(
                "FEED-DEAD: no price_change for {}ms (> {}ms)",
                now_ms - last_price_change_ms, self.cfg.feed_dead_ms
            ));
        } else {
            // W6-redux: neither condition active -> clear the latch.
            self.halted = None;
        }
        let is_halted = self.halted.is_some();
        let transition = match (was_halted, is_halted) {
            (false, true) => HaltTransition::Tripped,
            (true, false) => HaltTransition::Resumed,
            _ => HaltTransition::Stable,
        };
        ContinuousCheckResult { halted: self.halted.clone(), transition }
    }

    /// Full pre-entry gate — ALL guards, in priority order, BEFORE any POST.
    #[must_use]
    pub fn check_entry(&self, c: &EntryContext) -> GuardVerdict {
        let cfg = &self.cfg;
        // 1) panic button + frozen-feed (also latched by check_continuous).
        if kill_switch_active(&cfg.kill_switch_path) {
            return GuardVerdict::Deny("KILL-SWITCH present".into());
        }
        if feed_dead(c.last_price_change_ms, c.now_ms, cfg.feed_dead_ms) {
            return GuardVerdict::Deny(format!(
                "FEED-DEAD: no price_change for {}ms (> {}ms)",
                c.now_ms - c.last_price_change_ms, cfg.feed_dead_ms
            ));
        }
        if book_stale(c.book_ts_ms, c.now_ms, cfg.staleness_max_ms) {
            return GuardVerdict::Deny(format!(
                "BOOK STALE: {}ms (> {}ms)", c.now_ms - c.book_ts_ms, cfg.staleness_max_ms
            ));
        }
        // 2) [W7 2026-06-05] FREQUENCY breaker REMOVED. Runaway loops are
        //    bounded by max_open_positions (a loop cannot open >N lots) and
        //    capital risk by the daily-loss-stop below; the breaker was
        //    redundant. It also had a latent counting bug -- it incremented
        //    at intent dispatch (decision_loop) not at real POST, so paper /
        //    gated / aborted opens all counted, falsely tripping with 0 real
        //    orders posted. If a per-hour cap is ever needed again, gate it
        //    on the live_open Posted-outcome branch in run_execution_task.
        //
        // 3) daily-loss-stop (NET) → CLOSE-ONLY (don't open new; in-flight closes ok).
        if !daily_loss_ok(self.daily_net_pnl, cfg.daily_loss_cap()) {
            return GuardVerdict::CloseOnly(format!(
                "DAILY-LOSS-STOP: net ${} past -${} today -> CLOSE-ONLY", self.daily_net_pnl, cfg.daily_loss_cap()
            ));
        }
        // 4) capital caps (active-only exposure, fix 2).
        if !stake_ok(c.stake_usd, cfg.stake_cap) {
            return GuardVerdict::Deny(format!("STAKE cap: ${} > ${}", c.stake_usd, cfg.stake_cap));
        }
        if !per_token_ok(c.active_token_exposure, c.order_usd, cfg.per_token_cap) {
            return GuardVerdict::Deny(format!(
                "PER-TOKEN cap: active ${} + ${} > ${}", c.active_token_exposure, c.order_usd, cfg.per_token_cap
            ));
        }
        if !total_exposure_ok(c.active_total_exposure, c.order_usd, cfg.total_exposure_cap) {
            return GuardVerdict::Deny(format!(
                "TOTAL exposure cap: active ${} + ${} > ${}", c.active_total_exposure, c.order_usd, cfg.total_exposure_cap
            ));
        }
        if !hard_cap_ok(c.active_total_exposure, c.order_usd, cfg.hard_cap) {
            return GuardVerdict::Deny(format!(
                "HARD cap: active ${} + ${} > ${}", c.active_total_exposure, c.order_usd, cfg.hard_cap
            ));
        }
        GuardVerdict::Allow
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::{PosRow, active_exposure, active_exposure_for_token};
    use rust_decimal_macros::dec;

    fn ctx_ok<'a>(token: &'a str, total: Decimal, tok: Decimal, now: i64) -> EntryContext<'a> {
        EntryContext {
            stake_usd: dec!(1), order_usd: dec!(1), token,
            active_total_exposure: total, active_token_exposure: tok,
            book_ts_ms: now - 100, last_price_change_ms: now - 100, now_ms: now,
        }
    }

    // --- exposure cap reads ACTIVE-ONLY (the fix-2 wiring) ---
    #[test]
    fn total_exposure_cap_counts_active_only() {
        // $24 active + 21 RESOLVED lots. The resolved must NOT count → a $1 order fits.
        let mut rows = vec![
            PosRow { token: "A".into(), size: 48.0, cur_price: 0.50, redeemable: false, end_in_past: false }, // active $24
        ];
        for i in 0..21 {
            rows.push(PosRow { token: format!("R{i}"), size: 50.0, cur_price: 1.0, redeemable: true, end_in_past: true });
        }
        let (_, active_total) = active_exposure(&rows); // resolved excluded → $24
        assert!((active_total - 24.0).abs() < 1e-9, "active total {active_total}");
        let g = Guards::new(GuardConfig::default());
        let total = Decimal::try_from(active_total).unwrap();
        // $24 + $1 = $25 ≤ $25 → allow; a $1.01 order would breach.
        assert!(g.check_entry(&ctx_ok("A", total, dec!(0), 1_000)).is_allow());
        let mut over = ctx_ok("A", total, dec!(0), 1_000);
        over.order_usd = dec!(1.01);
        assert!(matches!(g.check_entry(&over), GuardVerdict::Deny(_)), "should breach $25");
    }

    #[test]
    fn per_token_cap_sums_active_lots_of_that_token() {
        let rows = vec![
            PosRow { token: "BTC".into(), size: 8.0, cur_price: 0.50, redeemable: false, end_in_past: false }, // $4 active
            PosRow { token: "BTC".into(), size: 20.0, cur_price: 1.0, redeemable: true, end_in_past: true },   // resolved (excluded)
        ];
        let tok = Decimal::try_from(active_exposure_for_token(&rows, "BTC")).unwrap(); // $4
        assert_eq!(tok, dec!(4));
        let g = Guards::new(GuardConfig::default());
        // $4 + $1 = $5 ≤ $5 → allow; $4 + $1.5 > $5 → deny.
        assert!(g.check_entry(&ctx_ok("BTC", dec!(4), tok, 1_000)).is_allow());
        let mut over = ctx_ok("BTC", dec!(4), tok, 1_000);
        over.order_usd = dec!(1.5);
        assert!(matches!(g.check_entry(&over), GuardVerdict::Deny(_)));
    }

    // --- daily-loss-stop: absolute override ($15) + stake-multiple fallback ---
    #[test]
    fn daily_loss_stop_absolute_override_and_stake_multiple_fallback() {
        // COMBO Phase 3: the DEFAULT now uses the absolute $15.00 override
        // (the B3 deploy decision). The stake-multiple is the FALLBACK when
        // the override is None.
        let mut cfg = GuardConfig::default();
        assert_eq!(cfg.stake_cap, dec!(1.05));
        assert_eq!(cfg.daily_loss_stop_stakes, dec!(12));
        // Default: absolute override wins -> exactly $15.00 (NOT $1.05*12=$12.60).
        assert_eq!(cfg.daily_loss_cap_usdc, Some(dec!(15.00)));
        assert_eq!(cfg.daily_loss_cap(), dec!(15.00));
        // Absolute is decoupled from stake: raising stake does NOT move the cap.
        cfg.stake_cap = dec!(2);
        assert_eq!(cfg.daily_loss_cap(), dec!(15.00)); // still $15 (absolute)
        // Fallback path: override None -> stake-multiple derivation.
        cfg.daily_loss_cap_usdc = None;
        cfg.stake_cap = dec!(1.05);
        assert_eq!(cfg.daily_loss_cap(), dec!(12.60)); // stake $1.05 * 12 stakes
        cfg.stake_cap = dec!(2);
        assert_eq!(cfg.daily_loss_cap(), dec!(24)); // stake $2 -> $24 (scales)
        cfg.stake_cap = dec!(1);
        cfg.daily_loss_stop_stakes = dec!(15);
        assert_eq!(cfg.daily_loss_cap(), dec!(15)); // $1 * 15 stakes
    }

    #[test]
    fn daily_loss_stop_tolerates_normal_bad_streak_then_trips() {
        // COMBO Phase 3: absolute $15.00/day stop (default override). A bad
        // streak below -$15 keeps trading (allow same-day recovery); past
        // -$15 → CLOSE-ONLY.
        let mut g = Guards::new(GuardConfig::default()); // $15.00 stop (absolute)
        // -$13 is a bad streak but WITHIN the $15 budget -> keep trading.
        g.record_net_pnl(dec!(-13), 1_000);
        assert!(g.check_entry(&ctx_ok("A", dec!(0), dec!(0), 1_500)).is_allow(),
            "a -$13 streak (< $15) must keep trading to allow recovery");
        // Past -$15 → not normal variance → CLOSE-ONLY.
        g.record_net_pnl(dec!(-3), 2_000); // total -16 (NET), past -$15
        assert_eq!(g.daily_net_pnl(), dec!(-16));
        match g.check_entry(&ctx_ok("A", dec!(0), dec!(0), 3_000)) {
            GuardVerdict::CloseOnly(_) => {}
            v => panic!("expected CloseOnly past -$15, got {v:?}"),
        }
        // Uses NET (after fees); gross would be less negative → net trips correctly/sooner.
    }

    #[test]
    fn daily_loss_resets_at_utc_midnight() {
        let mut g = Guards::new(GuardConfig::default());
        g.record_net_pnl(dec!(-13), 1_000); // day 0 accumulated loss (within $15 cap)
        g.record_net_pnl(dec!(5), 86_400_000 + 1_000); // next UTC day → reset then +5
        assert_eq!(g.daily_net_pnl(), dec!(5));
        assert!(g.check_entry(&ctx_ok("A", dec!(0), dec!(0), 86_400_000 + 2_000)).is_allow(),
            "new UTC day resets the stop");
    }

    // --- frequency breaker ---
    #[test]
    fn frequency_breaker_removed_does_not_block_burst_entries() {
        // W7 (2026-06-05): the FREQUENCY breaker was removed. This test
        // replaces the prior `frequency_breaker_blocks_11th_in_an_hour`
        // and PINS the new contract: ANY number of consecutive entries
        // within an hour passes the entry gate (runaway is now bounded
        // by max_open_positions + daily-loss-stop, both checked here too
        // to confirm they were NOT broken by the removal).
        let g = Guards::new(GuardConfig::default());
        let now = 100_000_000;
        // 100 consecutive entries -- pre-W7 this would have tripped at 11.
        for _ in 0..100 {
            assert!(
                g.check_entry(&ctx_ok("A", dec!(0), dec!(0), now)).is_allow(),
                "frequency breaker REMOVED -> no per-hour entry limit"
            );
        }
        // The OTHER guards still work. Daily-loss-stop fires CLOSE-ONLY past
        // the cap (unaffected by the frequency removal).
        let mut g_loss = Guards::new(GuardConfig::default());
        // COMBO Phase 3: default cap is now the absolute $15.00 override.
        // -$15.50 is past -$15.00 -> CLOSE-ONLY.
        g_loss.record_net_pnl(dec!(-15.50), now);
        assert!(
            matches!(g_loss.check_entry(&ctx_ok("A", dec!(0), dec!(0), now)), GuardVerdict::CloseOnly(_)),
            "daily-loss-stop must still fire (not broken by frequency removal)"
        );
        // Stake cap still works (unaffected). ctx_ok hard-codes stake_usd=$1
        // which is under the default $1.05 cap; construct the EntryContext
        // manually to exercise stake_ok directly with a $2 stake.
        let g_stake = Guards::new(GuardConfig::default());
        let ectx_big = EntryContext {
            stake_usd: dec!(2.00),       // > stake_cap $1.05 -> trips
            order_usd: dec!(2.00),
            token: "A",
            active_total_exposure: dec!(0),
            active_token_exposure: dec!(0),
            book_ts_ms: now - 100,
            last_price_change_ms: now - 100,
            now_ms: now,
        };
        assert!(
            matches!(g_stake.check_entry(&ectx_big), GuardVerdict::Deny(_)),
            "stake cap must still fire (not broken by frequency removal)"
        );
    }

    // --- arming gate (opt-in, agent dev path only) ---
    #[test]
    fn live_armed_requires_nonempty_file() {
        let p = std::env::temp_dir().join("rust_bot_test_live_armed.txt");
        let _ = std::fs::remove_file(&p);
        // missing → not armed
        assert!(!live_armed(&p));
        // empty / whitespace → not armed (a blank file is not a deliberate act)
        std::fs::write(&p, "   \n").unwrap();
        assert!(!live_armed(&p));
        // non-empty token → armed
        std::fs::write(&p, "ARM-2026-05-30").unwrap();
        assert!(live_armed(&p));
        let _ = std::fs::remove_file(&p);
    }

    // --- kill-switch ---
    #[test]
    fn kill_switch_denies_and_latches() {
        let p = std::env::temp_dir().join("rust_bot_test_killswitch.txt");
        let _ = std::fs::remove_file(&p);
        let mut cfg = GuardConfig::default();
        cfg.kill_switch_path = p.clone();
        let mut g = Guards::new(cfg);
        assert!(g.check_entry(&ctx_ok("A", dec!(0), dec!(0), 1_000)).is_allow());
        std::fs::write(&p, "stop").unwrap();
        assert!(matches!(g.check_entry(&ctx_ok("A", dec!(0), dec!(0), 1_000)), GuardVerdict::Deny(_)));
        assert!(
            g.check_continuous(1_000, 1_000).halted.is_some(),
            "kill-switch file present -> halt"
        );
        let _ = std::fs::remove_file(&p);
    }

    // ========================================================================
    // W6-redux: the latch MUST clear automatically when conditions go inactive.
    // Pre-W6-redux this had no `else { self.halted = None }`, so any transient
    // FEED-DEAD at startup pinned the bot to halted forever (the exact user-
    // reported symptom: oplog showed last_feed_ms / now_ms 13 ms apart but
    // reason string said "no price_change for 30139ms" -- frozen).
    // ========================================================================

    /// CRITICAL test for the user's exact reported bug. Simulates the
    /// production startup pattern: a transient FEED-DEAD at boot (no events
    /// for >30 s while WS connects + discovery seeds), then the feed starts
    /// flowing. The halt MUST clear automatically on the next pass.
    #[test]
    fn feed_dead_unlatches_after_feed_recovers() {
        let mut cfg = GuardConfig::default();
        cfg.kill_switch_path = std::env::temp_dir().join("rb_w6_no_kill_a.txt");
        let _ = std::fs::remove_file(&cfg.kill_switch_path);
        cfg.feed_dead_ms = 30_000;
        let mut g = Guards::new(cfg);

        // Pass 1: startup -- no price_change for 31 s -> trip.
        let now = 1_780_000_000_000_i64;
        let r1 = g.check_continuous(now - 31_000, now);
        assert!(r1.halted.is_some(), "feed silent for 31s > 30s -> halted");
        assert_eq!(r1.transition, HaltTransition::Tripped,
                   "first trip emits Tripped (caller logs once)");
        assert!(r1.halted.as_ref().unwrap().contains("31000ms"),
                "reason mentions the actual gap: {:?}", r1.halted);

        // Pass 2: feed recovered, last price_change 13ms ago (matches user's oplog).
        let r2 = g.check_continuous(now + 1_000 - 13, now + 1_000);
        assert!(r2.halted.is_none(),
                "feed fresh (13ms) -> halt MUST clear (the W6-redux fix); got {:?}",
                r2.halted);
        assert_eq!(r2.transition, HaltTransition::Resumed,
                   "Some -> None transition emits Resumed");

        // Pass 3: still fresh -> no transition, no log.
        let r3 = g.check_continuous(now + 2_000 - 10, now + 2_000);
        assert!(r3.halted.is_none());
        assert_eq!(r3.transition, HaltTransition::Stable,
                   "still clear -> Stable, caller does NOT log");
    }

    /// CRITICAL test for the operator's "deshalteo desde el dashboard" path
    /// that did NOT work pre-fix: touch the file -> halt. Remove the file ->
    /// halt MUST clear. Pre-W6-redux the latch kept the halt pinned.
    #[test]
    fn kill_switch_unlatches_after_file_removed() {
        let p = std::env::temp_dir().join(format!(
            "rb_w6_ks_unlatch_{}_{}.txt", std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        let _ = std::fs::remove_file(&p);
        let mut cfg = GuardConfig::default();
        cfg.kill_switch_path = p.clone();
        cfg.feed_dead_ms = 30_000;
        let mut g = Guards::new(cfg);

        // Pre-touch: file absent, feed fresh -> no halt.
        let now = 1_780_000_000_000_i64;
        assert!(g.check_continuous(now, now).halted.is_none());

        // Touch the file (operator panics): halt.
        std::fs::write(&p, "STOP").unwrap();
        let r_trip = g.check_continuous(now + 100, now + 100);
        assert!(r_trip.halted.is_some(), "kill-switch present -> halted");
        assert_eq!(r_trip.transition, HaltTransition::Tripped);
        assert!(r_trip.halted.as_ref().unwrap().contains("KILL-SWITCH"));

        // Still present: stable.
        let r_stable = g.check_continuous(now + 200, now + 200);
        assert!(r_stable.halted.is_some());
        assert_eq!(r_stable.transition, HaltTransition::Stable,
                   "still halted, same condition -> Stable (no log spam)");

        // Operator removes the file -> halt MUST clear.
        std::fs::remove_file(&p).unwrap();
        let r_resume = g.check_continuous(now + 300, now + 300);
        assert!(
            r_resume.halted.is_none(),
            "kill-switch removed -> halt MUST clear (the dashboard-unhalt fix); got {:?}",
            r_resume.halted
        );
        assert_eq!(r_resume.transition, HaltTransition::Resumed);
    }

    /// When BOTH kill-switch present AND feed is dead, the kill-switch
    /// reason wins (the `if / else if` order). When the kill-switch is
    /// removed but the feed is still dead, the FEED-DEAD reason takes over
    /// -- the halt stays Some but the reason CHANGES (not Stable, not
    /// Resumed; both transitions accepted depending on impl, but reason
    /// MUST refresh).
    #[test]
    fn kill_switch_takes_priority_over_feed_dead() {
        let p = std::env::temp_dir().join(format!(
            "rb_w6_priority_{}_{}.txt", std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        let _ = std::fs::remove_file(&p);
        let mut cfg = GuardConfig::default();
        cfg.kill_switch_path = p.clone();
        cfg.feed_dead_ms = 30_000;
        let mut g = Guards::new(cfg);

        std::fs::write(&p, "STOP").unwrap();
        let now = 1_780_000_000_000_i64;
        // Feed ALSO dead (last 60 s ago). Kill-switch must win.
        let r1 = g.check_continuous(now - 60_000, now);
        assert!(r1.halted.as_ref().unwrap().contains("KILL-SWITCH"),
                "kill-switch wins priority; got {:?}", r1.halted);

        // Remove the file. Feed is still dead. Reason MUST change to FEED-DEAD,
        // halt MUST remain Some (the bot is still halted, just for a different
        // reason now).
        std::fs::remove_file(&p).unwrap();
        let r2 = g.check_continuous(now + 100 - 60_000, now + 100);
        assert!(r2.halted.is_some(), "still halted (now by feed-dead)");
        assert!(r2.halted.as_ref().unwrap().contains("FEED-DEAD"),
                "reason transitioned to FEED-DEAD after kill-switch removed; got {:?}",
                r2.halted);

        // Finally feed recovers -> clear.
        let r3 = g.check_continuous(now + 200, now + 200);
        assert!(r3.halted.is_none(), "both conditions cleared -> halt clears");
        assert_eq!(r3.transition, HaltTransition::Resumed);
    }

    /// REGRESSION GUARD against the exact user-observed symptom: pre-W6-redux,
    /// once a FEED-DEAD reason was latched (e.g. "30139ms" from startup), it
    /// stayed FROZEN forever. Even on a re-trip with a different gap, the
    /// reason carried the old number. This test re-trips after a recovery
    /// and verifies the NEW gap is reflected in the reason -- if a future
    /// change re-introduces the latch, the assert that "55000ms" appears
    /// (not "31000ms") fails.
    #[test]
    fn feed_dead_reason_string_recomputed_on_re_trip() {
        let mut cfg = GuardConfig::default();
        cfg.kill_switch_path = std::env::temp_dir().join("rb_w6_no_kill_d.txt");
        let _ = std::fs::remove_file(&cfg.kill_switch_path);
        cfg.feed_dead_ms = 30_000;
        let mut g = Guards::new(cfg);

        // First trip: gap = 31 s -> reason mentions "31000ms".
        let t = 1_780_000_000_000_i64;
        let r1 = g.check_continuous(t - 31_000, t);
        assert!(r1.halted.as_ref().unwrap().contains("31000ms"),
                "first trip reason: {:?}", r1.halted);

        // Recover.
        let r2 = g.check_continuous(t + 1_000 - 10, t + 1_000);
        assert!(r2.halted.is_none(), "must clear");

        // Second trip: gap = 55 s -> reason MUST mention "55000ms", NOT "31000ms".
        // Pre-W6-redux this would have left "31000ms" cached forever.
        let r3 = g.check_continuous(t + 2_000 - 55_000, t + 2_000);
        let reason = r3.halted.as_ref().unwrap();
        assert!(reason.contains("55000ms"),
                "re-trip reason MUST refresh to the new gap; got: {reason}");
        assert!(!reason.contains("31000ms"),
                "re-trip reason MUST NOT carry the stale prior gap; got: {reason}");
        assert_eq!(r3.transition, HaltTransition::Tripped,
                   "None -> Some again is a fresh Tripped, not Stable");
    }

    // --- feed-dead + staleness ---
    #[test]
    fn feed_dead_denies_on_silent_feed() {
        let g = Guards::new(GuardConfig::default());
        let now = 1_000_000;
        let mut c = ctx_ok("A", dec!(0), dec!(0), now);
        c.last_price_change_ms = now - 31_000; // 31s silent > 30s
        assert!(matches!(g.check_entry(&c), GuardVerdict::Deny(_)));
    }

    #[test]
    fn stale_book_denies() {
        let g = Guards::new(GuardConfig::default());
        let now = 1_000_000;
        let mut c = ctx_ok("A", dec!(0), dec!(0), now);
        c.book_ts_ms = now - 3_500; // 3.5s old > 3s
        assert!(matches!(g.check_entry(&c), GuardVerdict::Deny(_)));
    }

    #[test]
    fn all_clear_allows() {
        let g = Guards::new(GuardConfig::default());
        assert_eq!(g.check_entry(&ctx_ok("A", dec!(10), dec!(2), 1_000)), GuardVerdict::Allow);
    }
}
