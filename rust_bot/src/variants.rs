//! ORDER #16 — three-variant paper A/B (one process, N virtual portfolios).
//!
//! Answers: does the tournament's burst/union finding survive contact with a live
//! feed, and by how much? (5m sealed holdout +$56.41/day vs deployed +$14.11,
//! permutation p=0.0000, same config won 5/5 folds.)
//!
//! **Virtual portfolios, not parallel bots.** All three variants see the *identical*
//! tick, the *identical* book snapshot and the *identical* decision latency, so any
//! P&L difference is attributable to the gate alone. Three separate processes would
//! each carry their own WS jitter and the difference would be partly noise.
//!
//! Everything here is PURE so the gates, the kill model and the decision rule are
//! testable without a feed — and so the decision rule is committed as code BEFORE the
//! run, which is what stops it being tuned afterwards.
//!
//! # Guardrails for the decision-loop wiring (the remaining step)
//!
//! These are not style preferences; each one protects a result we would otherwise
//! lose, and they are recorded here so they survive between sessions.
//!
//! 1. **V0 is NOT a third symmetric portfolio — it is the actual bot, end to end.**
//!    Its positions, settlement path, band-stop, invariants and recal stay the
//!    existing audited machinery, untouched. V1/V2 are ADDITIVE shadow portfolios
//!    with their own state. A hand-rebuilt control is the single most likely way
//!    this A/B returns a confident wrong answer — every disaster in this project's
//!    ledger has been a population-mismatch error.
//! 2. **V1/V2 recal instances must NEVER write `recal.json` or `recal_15m.json`.**
//!    They trade larger populations with different biases by construction, so a
//!    shared file would contaminate the 15m audition, which is mid-verdict at n=140
//!    and must keep maturing on exactly the samples it would have seen anyway.
//!    Shadow recal state goes to its own paths.
//! 3. **One entry per market is PER VARIANT, not global** — variants disagreeing
//!    about which second to take is the measurement, not a bug to be deduplicated.
//! 4. **Re-entry-after-stop applies per variant**, for the same reason.
//! 5. Stakes flat $1.05 everywhere; **no sizing tiers in this experiment** — they are
//!    a separate axis and would confound the comparison.
//! 6. The 7-day clock only starts with Order #14 deployed and the feed healthy. A
//!    repeat of the 45-hour blind window voids this A/B exactly as it voided the
//!    weekend exam.
//!
//! # The six leak surfaces
//!
//! Isolation fails at BOUNDARIES, not inside the data structure — and this codebase
//! has precedent: Order #12 B's 79 phantom positions counting toward exposure caps
//! was exactly this bug in an earlier costume. Each surface must be severed AND
//! tested:
//!
//! 1. **Guard / exposure budget.** Shadow opens must not consume
//!    `max_open_positions`, the stake cap, total exposure, the per-token cap or
//!    `daily_loss_cap`. If they do, V1/V2 throttle V0 and the treatment corrupts the
//!    control — the worst failure available here, because it is invisible in the P&L
//!    and looks like a real result.
//! 2. **Canary.** V0's per-asset AMBER/RED arms off hold-WR. Shadow settles must
//!    never feed it, or V1/V2's larger population changes V0's stake multipliers and
//!    re-entry suspension.
//! 3. **Settlement sweep.** Paper settlement drains `state.v2_settled`; shadows need
//!    their OWN map or they are drained into V0's recal feed — the survivorship bug
//!    from Order #11 C.
//! 4. **Accounting invariant.** The "every posted token in exactly one of
//!    {close_posted, pnl_recorded}" banner fires on shadow positions unless it is
//!    variant-scoped. A noisy invariant is a dead invariant, and it is the only thing
//!    catching booking bugs.
//! 5. **State persistence.** Shadow state lives in its own file, never inside
//!    `state.json` — a schema change to the shared file is a way to break V0's state
//!    load mid-audition.
//! 6. **Recal files.** Own paths, enforced in code (see `ShadowBook::new`).
//!
//! # What makes it "provably"
//!
//! An integration test drives the loop over an identical synthetic event sequence
//! TWICE — variants disabled, then enabled — and asserts V0's intents, positions,
//! P&L rows and recal samples are byte-identical across the two runs. That converts
//! isolation from a design intention into a regression-tested invariant, and it is
//! cheap because the loop is already deterministic given a fixed event sequence.

// The gates, the kill model and the decision rule are complete and tested; the
// decision-loop wiring that calls them is the remaining step of Order #16. Allowed at
// module scope so the not-yet-called items do not bury real warnings in the meantime.
#![allow(dead_code)]

use serde::{Deserialize, Serialize};

/// The three arms. `V0` is the incumbent and MUST stay byte-identical to what runs
/// today — it is both the control and the sanity check that the harness reproduces
/// known behaviour.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Variant {
    /// Exactly today's deployed config, untouched.
    V0,
    /// UNION-2: V0 **OR** burst ≥ 2bps with NO other gate (no z/edge/disp/vol/
    /// book-unmoved/frozen, no ask cap). The tournament winner as selected — testing
    /// it unmodified rather than a softened version.
    V1,
    /// UNION-3-CAPPED: V0 **OR** (burst ≥ 3bps AND ask ≤ 0.75). Risk-tempered: trims
    /// the loosest burst tier and the near-certainty tail, so it should degrade more
    /// gracefully if live fills disappoint.
    V2,
}

impl Variant {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Variant::V0 => "v0",
            Variant::V1 => "v1",
            Variant::V2 => "v2",
        }
    }
    #[must_use]
    pub fn all() -> [Variant; 3] {
        [Variant::V0, Variant::V1, Variant::V2]
    }
}

/// Frozen variant thresholds. Pre-registered; **no mid-run tuning** (order: if a
/// variant is obviously broken, fix the bug and restart the clock rather than
/// adjusting the gate).
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct VariantConfig {
    /// V1 burst floor (bps).
    pub v1_burst_bps: f64,
    /// V2 burst floor (bps).
    pub v2_burst_bps: f64,
    /// V2 ask cap — trims the near-certainty tail.
    pub v2_max_ask: f64,
}

impl Default for VariantConfig {
    fn default() -> Self {
        Self { v1_burst_bps: 2.0, v2_burst_bps: 3.0, v2_max_ask: 0.75 }
    }
}

/// Does `variant` admit this signal?
///
/// `v0_admits` is the FULL deployed gate stack's verdict — computed by the existing
/// code path, never re-implemented here, so V0 cannot drift from production. The
/// burst arms are a UNION on top of it: they can only ever ADD entries, never remove
/// one V0 would have taken.
#[must_use]
pub fn admits(variant: Variant, cfg: &VariantConfig, v0_admits: bool, burst_bps: f64, ask: f64) -> bool {
    match variant {
        Variant::V0 => v0_admits,
        // Deliberately no other gate: this is the tournament winner as selected.
        Variant::V1 => v0_admits || burst_bps >= cfg.v1_burst_bps,
        Variant::V2 => v0_admits || (burst_bps >= cfg.v2_burst_bps && ask <= cfg.v2_max_ask),
    }
}

/// One market/side the decision loop resolved far enough to price, emitted for the
/// variant arms REGARDLESS of whether V0's gate stack went on to reject it.
///
/// This exists because V1 is "V0 **OR** burst ≥ 2bps with no other gate", so by
/// construction most V1 entries are signals V0 threw away — they never appear in the
/// returned commands and cannot be derived from them.
///
/// Crucially the `ask` and `ttl_s` here are resolved at the SAME instant, from the
/// same computation, that V0 used. A parallel re-scan would resolve them a few
/// microseconds later in the loop, and that difference lands directly in `fill_ask`
/// and therefore in the kill rate — letting enumeration timing masquerade as a gate
/// effect, which is the one confound this A/B cannot survive.
#[derive(Debug, Clone, PartialEq)]
pub struct Candidate {
    pub asset: String,
    pub interval: String,
    pub epoch: i64,
    pub token_id: String,
    pub up: bool,
    pub ask: f64,
    pub ttl_s: i64,
    /// True when V0's full gate stack went on to admit this candidate. The union
    /// arms read it rather than re-deriving V0's verdict.
    pub v0_admitted: bool,
}

/// Outcome of modelling a fill-or-kill at the real observed latency.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct FokOutcome {
    /// True ⇒ no position was taken.
    pub killed: bool,
    /// Where we actually filled (only meaningful when `!killed`).
    pub fill_ask: f64,
    /// `ask_now − quote_ask`. Negative when the book moved in our favour.
    pub slip: f64,
}

/// THE critical addition (order §"model FOK kills").
///
/// The paper bot fills at the quoted ask, which is exactly the assumption the burst
/// arm needs to be *wrong* about — so without this the A/B would simply replay the
/// backtest and teach us nothing. At fill time we compare the CURRENT ask against the
/// quoted one:
///   * `ask_now ≤ quote + max_slippage` → fill at `ask_now` (we pay the real price,
///     including when it improves);
///   * otherwise → KILL, no position.
///
/// Why this decides the experiment: the burst population's ask decays **+3.33¢/s and
/// moves against us 60% of the time**, versus +1.35¢/s and 31% for the deployed
/// population. If that translates into a materially higher kill rate, V1's backtested
/// advantage shrinks or inverts — and we see it in week one for free.
#[must_use]
pub fn fok_outcome(quote_ask: f64, ask_now: f64, max_slippage: f64) -> FokOutcome {
    let slip = ask_now - quote_ask;
    // Slipping exactly TO the tolerance still fills, matching deployed `max_slippage`
    // semantics. The epsilon is load-bearing, not decoration: 0.64 − 0.60 evaluates to
    // 0.04000000000000001, so a bare `>` kills entries that are exactly at tolerance —
    // silently inflating the kill rate, which is the metric this whole experiment
    // decides on.
    let killed = slip > max_slippage + 1e-9;
    FokOutcome { killed, fill_ask: if killed { 0.0 } else { ask_now }, slip }
}

/// Per-variant daily tallies, the input to the pre-registered decision rule.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct DayStat {
    pub net_usd: f64,
    pub entries: u64,
    pub kills: u64,
}

/// The verdict, exactly as pre-registered before the run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    /// Beats V0 by ≥ +50% net $/day AND kill rate < 25% AND positive on ≥ 5 of 7 days.
    Win,
    /// Ahead but misses a leg → extend to 14 days, ONCE.
    Inconclusive,
    /// Does not beat V0, or kill rate ≥ 25% → the tournament result was a recorder
    /// artifact. Record it as such and stop.
    Fail,
}

/// Evaluate the pre-registered rule for one challenger against the control.
///
/// Encoded as code, committed before the run, precisely so it cannot be softened
/// afterwards — every leg must pass on its own; there is no averaging across them.
#[must_use]
pub fn evaluate(control: &[DayStat], challenger: &[DayStat], min_days: usize) -> Verdict {
    if control.len() < min_days || challenger.len() < min_days {
        return Verdict::Inconclusive; // not enough clean days yet
    }
    let c_net: f64 = control.iter().map(|d| d.net_usd).sum();
    let x_net: f64 = challenger.iter().map(|d| d.net_usd).sum();
    let entries: u64 = challenger.iter().map(|d| d.entries).sum();
    let kills: u64 = challenger.iter().map(|d| d.kills).sum();
    // Kill rate is over ATTEMPTS (entries that filled + those killed).
    let attempts = entries + kills;
    let kill_rate = if attempts == 0 { 1.0 } else { kills as f64 / attempts as f64 };
    let positive_days = challenger.iter().filter(|d| d.net_usd > 0.0).count();

    // A kill rate at or above 25% is a hard FAIL regardless of P&L: it means the
    // population cannot actually be traded at the prices the backtest assumed.
    if kill_rate >= 0.25 {
        return Verdict::Fail;
    }
    // "Beats V0 by >= +50%". With a non-positive control, any positive challenger
    // clears the spirit of the bar (there is no percentage of a loss to beat).
    let beats = if c_net > 0.0 { x_net >= c_net * 1.5 } else { x_net > 0.0 };
    if !beats {
        return Verdict::Fail;
    }
    if positive_days >= 5 {
        Verdict::Win
    } else {
        Verdict::Inconclusive
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CFG: VariantConfig = VariantConfig { v1_burst_bps: 2.0, v2_burst_bps: 3.0, v2_max_ask: 0.75 };

    /// V0 is the incumbent: it admits exactly what the deployed stack admits, and the
    /// burst arms never subtract from it — a union can only add.
    #[test]
    fn v0_is_the_deployed_verdict_and_unions_never_subtract() {
        // V0 passes → all three must take it, whatever the burst looks like.
        for burst in [-5.0, 0.0, 1.9, 10.0] {
            for v in Variant::all() {
                assert!(
                    admits(v, &CFG, true, burst, 0.60),
                    "{v:?} must never drop an entry V0 would take (burst {burst})"
                );
            }
        }
    }

    /// V1 UNION-2: burst ≥ 2bps alone is sufficient — no z, edge, disp, vol,
    /// book-unmoved or frozen gate, and NO ask cap. Tested as selected.
    #[test]
    fn v1_admits_on_burst_alone_with_no_ask_cap() {
        assert!(!admits(Variant::V1, &CFG, false, 1.99, 0.60), "below 2bps and V0 says no");
        assert!(admits(Variant::V1, &CFG, false, 2.0, 0.60), "exactly 2bps admits");
        assert!(admits(Variant::V1, &CFG, false, 9.0, 0.60));
        // No ask cap: even a near-certainty ask is admitted on burst.
        assert!(admits(Variant::V1, &CFG, false, 5.0, 0.97), "V1 has NO ask cap by design");
    }

    /// V2 UNION-3-CAPPED needs BOTH legs: a higher burst floor and the ask cap.
    #[test]
    fn v2_requires_both_burst_and_ask_cap() {
        assert!(!admits(Variant::V2, &CFG, false, 2.5, 0.60), "2.5bps is below V2's 3bps floor");
        assert!(admits(Variant::V2, &CFG, false, 3.0, 0.75), "3bps at the cap boundary admits");
        assert!(!admits(Variant::V2, &CFG, false, 9.0, 0.76), "past the ask cap is rejected");
        // The tier V1 takes and V2 deliberately trims.
        assert!(admits(Variant::V1, &CFG, false, 2.5, 0.60));
        assert!(!admits(Variant::V2, &CFG, false, 2.5, 0.60), "V2 trims the loosest burst tier");
    }

    /// THE measurement the experiment exists for. Fill when the ask has not run past
    /// tolerance, kill when it has, and record the slip either way.
    #[test]
    fn fok_kills_only_past_the_slippage_tolerance() {
        let max_slip = 0.04;
        // Unchanged book → fill at the quote, zero slip.
        let o = fok_outcome(0.60, 0.60, max_slip);
        assert!(!o.killed && (o.fill_ask - 0.60).abs() < 1e-12 && o.slip.abs() < 1e-12);

        // Moved against us but within tolerance → fill at the WORSE, REAL price.
        let o = fok_outcome(0.60, 0.635, max_slip);
        assert!(!o.killed, "within tolerance must fill");
        assert!((o.fill_ask - 0.635).abs() < 1e-12, "fill at ask_now, not the quote");
        assert!((o.slip - 0.035).abs() < 1e-12);

        // Exactly at tolerance still fills (matches deployed max_slippage semantics).
        assert!(!fok_outcome(0.60, 0.64, max_slip).killed, "exactly at tolerance fills");

        // Past tolerance → KILL, no position.
        let o = fok_outcome(0.60, 0.6401, max_slip);
        assert!(o.killed, "past tolerance must kill");
        assert!((o.slip - 0.0401).abs() < 1e-9, "slip is recorded even on a kill");

        // Book improved → fill better than quoted, negative slip.
        let o = fok_outcome(0.60, 0.57, max_slip);
        assert!(!o.killed && (o.fill_ask - 0.57).abs() < 1e-12 && o.slip < 0.0);
    }

    /// The pre-registered WIN: all three legs pass.
    #[test]
    fn prereg_win_requires_every_leg() {
        let v0 = vec![DayStat { net_usd: 2.0, entries: 50, kills: 5 }; 7];
        // +100% net, 10% kill rate, positive every day.
        let v1 = vec![DayStat { net_usd: 4.0, entries: 90, kills: 10 }; 7];
        assert_eq!(evaluate(&v0, &v1, 7), Verdict::Win);
    }

    /// A high kill rate is a hard FAIL even when the P&L looks spectacular — it means
    /// the population cannot be traded at the prices the backtest assumed.
    #[test]
    fn prereg_high_kill_rate_fails_regardless_of_pnl() {
        let v0 = vec![DayStat { net_usd: 2.0, entries: 50, kills: 5 }; 7];
        // +400% net but 30% of attempts killed.
        let v1 = vec![DayStat { net_usd: 10.0, entries: 70, kills: 30 }; 7];
        assert_eq!(
            evaluate(&v0, &v1, 7),
            Verdict::Fail,
            "25%+ kill rate must fail on its own — that IS the finding"
        );
    }

    /// Ahead but not by enough → FAIL, not a generous rounding up.
    #[test]
    fn prereg_missing_the_50pct_bar_is_a_fail() {
        let v0 = vec![DayStat { net_usd: 2.0, entries: 50, kills: 2 }; 7];
        let v1 = vec![DayStat { net_usd: 2.8, entries: 60, kills: 3 }; 7]; // +40%
        assert_eq!(evaluate(&v0, &v1, 7), Verdict::Fail);
        // Exactly +50% clears it.
        let v1b = vec![DayStat { net_usd: 3.0, entries: 60, kills: 3 }; 7];
        assert_eq!(evaluate(&v0, &v1b, 7), Verdict::Win);
    }

    /// Beats the bar and the kill rate, but is positive on too few days → extend once.
    #[test]
    fn prereg_choppy_days_are_inconclusive_not_a_win() {
        let v0 = vec![DayStat { net_usd: 1.0, entries: 50, kills: 2 }; 7];
        let mut v1 = vec![DayStat { net_usd: -3.0, entries: 60, kills: 3 }; 3];
        v1.extend(vec![DayStat { net_usd: 9.0, entries: 60, kills: 3 }; 4]);
        // Net = +27 vs +7 (well past +50%), kill rate low, but only 4 positive days.
        assert_eq!(evaluate(&v0, &v1, 7), Verdict::Inconclusive);
    }

    /// A short run cannot produce a verdict — the rule requires 7 FULL days.
    #[test]
    fn prereg_needs_seven_full_days() {
        let v0 = vec![DayStat { net_usd: 1.0, entries: 50, kills: 1 }; 6];
        let v1 = vec![DayStat { net_usd: 9.0, entries: 60, kills: 1 }; 6];
        assert_eq!(evaluate(&v0, &v1, 7), Verdict::Inconclusive, "6 days cannot decide");
    }

    /// A challenger that never fills (every attempt killed) is a FAIL, not a
    /// divide-by-zero that reads as a perfect kill rate.
    #[test]
    fn prereg_zero_entries_is_a_fail_not_a_pass() {
        let v0 = vec![DayStat { net_usd: 1.0, entries: 50, kills: 1 }; 7];
        let v1 = vec![DayStat { net_usd: 0.0, entries: 0, kills: 0 }; 7];
        assert_eq!(evaluate(&v0, &v1, 7), Verdict::Fail);
    }
}
