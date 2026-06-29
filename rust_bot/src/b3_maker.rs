//! COMBO Phase 3 — B3 maker exit: the capital-critical PURE decision logic.
//!
//! These functions are PURE (no I/O, no SDK, no locks) on purpose, so they can be
//!   * exhaustively unit/property-tested in isolation (this file), and
//!   * shared by the PAPER sim (Pieza 9) AND the LIVE path (Phase B) — the SAME
//!     code decides the race + post_only outcome in both, so paper can never
//!     behave differently from live on the parts that touch capital.
//!
//! The network I/O (place / cancel / `Client::order` reconcile) lives in the
//! exit task (Pieza 1) / live executor (Phase B). It FEEDS these functions the
//! observed facts (size_matched, order status, post error/response) and ACTS on
//! their verdict. This file decides NOTHING about WHEN to call them — only WHAT
//! the right answer is given the facts.

#![allow(dead_code)] // wired by the exit task in the next sub-commit (Pieza 1)

use rust_decimal::Decimal;

// ============================================================================
// #4 RACE — fill-vs-cancel reconciliation
// ============================================================================

/// Order status observed from `Client::order(id)` after a cancel, abstracted
/// from the SDK's `OrderStatusType` so this stays pure. Phase B maps the SDK
/// enum onto this. The distinction that matters: is the order TERMINAL (no more
/// fills possible) or could it still fill?
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MakerOrderStatus {
    /// Terminal: cancelled (no more fills).
    Cancelled,
    /// Terminal: fully matched.
    Matched,
    /// Terminal: unmatched / expired with no fill.
    Unmatched,
    /// NON-terminal: still working on the book (the cancel did not confirm).
    Live,
    /// NON-terminal: delayed / queued — could still fill (treat like Live).
    Delayed,
    /// Indeterminate — treat conservatively as NON-terminal (defer).
    Unknown,
}

impl MakerOrderStatus {
    /// A terminal status means no further fills are possible -> it is safe to
    /// act on `size_matched`. A non-terminal status means the order could still
    /// fill, so the remainder must NOT be sold yet (defer).
    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Cancelled | Self::Matched | Self::Unmatched)
    }
}

/// The decision label (for the `b3_maker_reconcile` oplog event).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconcileDecision {
    FullFill,
    Partial,
    CleanCancel,
    Ambiguous,
}

impl ReconcileDecision {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::FullFill => "full_fill",
            Self::Partial => "partial",
            Self::CleanCancel => "clean_cancel",
            Self::Ambiguous => "ambiguous",
        }
    }
}

/// The reconciliation verdict.
///
/// INVARIANT (guaranteed by `reconcile_decision`, pinned by a property test):
///   * if `defer`:  maker_filled == 0  AND  fallback_size == 0
///   * else:        maker_filled + fallback_size == original_size
///
/// So total shares sold (maker fill + F2 fallback) can NEVER exceed the
/// position — OVERSELL IS UNREPRESENTABLE. The caller closes `maker_filled`
/// shares at target (maker) and sells `fallback_size` via F2_market (taker);
/// on `defer` it sells nothing and retries next tick.
#[derive(Debug, Clone, PartialEq)]
pub struct ReconcileOutcome {
    /// Shares the maker filled at target (close as maker).
    pub maker_filled: Decimal,
    /// Shares to sell via the F2_market fallback (taker).
    pub fallback_size: Decimal,
    /// DEFER: do nothing this tick — the order is still live, could fill more.
    pub defer: bool,
    /// The decision label for the oplog.
    pub decision: ReconcileDecision,
}

/// Decide what to do after issuing a cancel, from the AUTHORITATIVE reconcile
/// (`Client::order(id)` -> status + size_matched). NOT from the cancel response
/// (which can say "canceled" while a fill landed just before it).
///
/// `size_matched` is the SDK's CUMULATIVE matched size for this order. Since the
/// lot has exactly ONE maker order, it equals the total filled. Defensively
/// clamped to `[0, original_size]` so a buggy over-report can't oversell.
#[must_use]
pub fn reconcile_decision(
    status: MakerOrderStatus,
    original_size: Decimal,
    size_matched: Decimal,
) -> ReconcileOutcome {
    let orig = original_size.max(Decimal::ZERO);
    // Never trust a reported match below 0 or above the order size.
    let matched = size_matched.clamp(Decimal::ZERO, orig);

    // 1) Fully filled -> terminal by definition, regardless of the status
    //    string. The maker did its job; no fallback.
    if orig > Decimal::ZERO && matched >= orig {
        return ReconcileOutcome {
            maker_filled: orig,
            fallback_size: Decimal::ZERO,
            defer: false,
            decision: ReconcileDecision::FullFill,
        };
    }

    // 2) Non-terminal status with an incomplete fill: the cancel did NOT
    //    confirm, so the order could still fill MORE. Selling the remainder now
    //    risks oversell. DEFER -- sell nothing, retry next tick.
    if !status.is_terminal() {
        return ReconcileOutcome {
            maker_filled: Decimal::ZERO,
            fallback_size: Decimal::ZERO,
            defer: true,
            decision: ReconcileDecision::Ambiguous,
        };
    }

    // 3) Terminal + zero fill -> clean cancel; fallback the FULL size.
    if matched == Decimal::ZERO {
        return ReconcileOutcome {
            maker_filled: Decimal::ZERO,
            fallback_size: orig,
            defer: false,
            decision: ReconcileDecision::CleanCancel,
        };
    }

    // 4) Terminal + partial fill -> the filled part closed at target (maker);
    //    fallback ONLY the remainder.
    ReconcileOutcome {
        maker_filled: matched,
        fallback_size: orig - matched,
        defer: false,
        decision: ReconcileDecision::Partial,
    }
}

/// What the exit task does at B3 TIMEOUT, given the reconcile verdict. Keeps
/// the recon-driven branching PURE + testable for every case (paper can only
/// ever produce `CleanCancel` -> `FullFallback`; this also covers the live
/// cases so Phase B inherits a tested decision). The no-oversell invariant is
/// preserved by construction: `Defer` and `Partial` sell 0 via this path
/// (Partial needs lot-splitting that only Phase B has), `FullFallback` /
/// `FullMakerFill` sell the whole lot exactly once.
#[derive(Debug, Clone, PartialEq)]
pub enum B3TimeoutAction {
    /// Ambiguous reconcile (cancel unconfirmed): sell NOTHING, retry next tick.
    /// The anti-oversell fail-safe.
    Defer,
    /// Clean cancel (no maker fill): sell the whole lot via F2_market (taker).
    FullFallback,
    /// The maker fully filled at/just-before the timeout: close the whole lot
    /// at target (maker).
    FullMakerFill,
    /// PARTIAL fill: `maker_filled` shares closed at target + `fallback_size`
    /// via F2_market. This needs LOT-SPLITTING (reduce the lot, close two
    /// portions) which only the LIVE path (Phase B) implements. The PAPER sim
    /// NEVER produces this (clean cancel), so the caller treats it as `Defer`
    /// (sell nothing) until Phase B -- guaranteeing no oversell in the interim.
    Partial { maker_filled: Decimal, fallback_size: Decimal },
}

/// Map a reconcile verdict to the timeout action. Pure; unit-tested for every
/// `ReconcileDecision`.
#[must_use]
pub fn b3_timeout_action(recon: &ReconcileOutcome) -> B3TimeoutAction {
    if recon.defer {
        return B3TimeoutAction::Defer;
    }
    let mf = recon.maker_filled;
    let fb = recon.fallback_size;
    if mf > Decimal::ZERO && fb > Decimal::ZERO {
        B3TimeoutAction::Partial { maker_filled: mf, fallback_size: fb }
    } else if fb > Decimal::ZERO {
        B3TimeoutAction::FullFallback
    } else if mf > Decimal::ZERO {
        B3TimeoutAction::FullMakerFill
    } else {
        // orig == 0 (degenerate): nothing to sell.
        B3TimeoutAction::Defer
    }
}

// ============================================================================
// PHASE B Pieza 2 — LIVE poll decision + partial lot-split (PURE)
// ============================================================================

/// What a per-tick `order(id)` poll of a resting maker means. The LIVE fill
/// detector (replaces the paper sim's `best_bid >= target`): the authoritative
/// `size_matched` from the SDK drives it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PollDecision {
    /// `size_matched >= orig`: the maker fully filled -> close at the maker fill.
    Filled,
    /// `0 < size_matched < orig`, not due: still working on the book -> keep.
    Working,
    /// `size_matched == 0`, not due: resting, nothing yet -> keep waiting.
    Waiting,
    /// Due (exit_ts_s reached) OR the order terminated early (cancelled/expired
    /// externally before a full fill) -> cancel + reconcile (the race).
    Timeout,
}

/// Decide what a poll of a resting maker means, from the AUTHORITATIVE
/// `(status, size_matched)` of `order(id)` + whether the hold window elapsed.
/// `size_matched` (the SDK Decimal) feeds straight in -- same type, same name
/// as `reconcile_decision`. Defensively clamps a buggy over-report.
#[must_use]
pub fn b3_poll_decision(
    status: MakerOrderStatus,
    size_matched: Decimal,
    orig: Decimal,
    due: bool,
) -> PollDecision {
    let orig = orig.max(Decimal::ZERO);
    let matched = size_matched.clamp(Decimal::ZERO, orig);
    // A full fill is terminal regardless of `due` or the status string.
    if orig > Decimal::ZERO && matched >= orig {
        return PollDecision::Filled;
    }
    // Due, OR the order died early (terminal status without a full fill) ->
    // reconcile. A terminal status mid-window means the maker is gone
    // (cancelled/expired by the venue) and the unfilled part needs the fallback.
    if due || status.is_terminal() {
        return PollDecision::Timeout;
    }
    if matched > Decimal::ZERO {
        PollDecision::Working
    } else {
        PollDecision::Waiting
    }
}

/// Decimal convex taker fee: `0.07 * shares * price * (1 - price)`. The Decimal
/// twin of `taker_fee` (f64), for exact live P&L on the fallback portion.
#[must_use]
pub fn taker_fee_dec(shares: Decimal, price: Decimal) -> Decimal {
    Decimal::new(7, 2) * shares * price * (Decimal::ONE - price)
}

/// The P&L of a PARTIAL maker fill, split into the two portions that close it.
/// INVARIANT (pinned by a property test): `maker_shares + fallback_shares ==
/// orig` ALWAYS -> the position is sold EXACTLY once, never oversold.
#[derive(Debug, Clone, PartialEq)]
pub struct SplitClose {
    /// Shares the maker filled (closed at `target`, maker fee 0). == size_matched.
    pub maker_shares: Decimal,
    /// Shares sold via the F2 fallback (taker). == orig - size_matched.
    pub fallback_shares: Decimal,
    /// Net P&L of the maker portion (sold at target, maker floor fee 0).
    pub maker_pnl: Decimal,
    /// Net P&L of the fallback portion (sold at `fallback_fill`, taker fee).
    pub fallback_pnl: Decimal,
}

/// PHASE B Pieza 3 — the partial lot-split. When a maker partially filled
/// (`maker_filled` = the real `size_matched` from `order(id)`, with the cancel
/// confirming the order is terminal so the remainder is truly unsold), close
/// the position in TWO portions:
///   * `maker_filled` shares already sold on-chain at `target` (maker, fee 0),
///   * `orig - maker_filled` shares sold via a real F2 FOK taker SELL at
///     `fallback_fill`.
/// The original BUY fee is prorated by share fraction across the two portions.
///
/// NO-OVERSELL by construction: `maker_filled` is clamped to `[0, orig]`, so
/// `maker_shares + fallback_shares == orig` for ANY input. This is the same
/// guarantee `reconcile_decision` makes, now applied to the P&L close, tied to
/// the REAL `size_matched` (not best_bid).
#[must_use]
pub fn split_close(
    orig: Decimal,
    maker_filled: Decimal,
    target: Decimal,
    fallback_fill: Decimal,
    entry: Decimal,
    buy_fee_total: Decimal,
) -> SplitClose {
    let orig = orig.max(Decimal::ZERO);
    let mf = maker_filled.clamp(Decimal::ZERO, orig);
    let fb = orig - mf; // >= 0 by the clamp
    // Prorate the BUY fee; take the fallback share as the remainder so the two
    // pieces sum EXACTLY to buy_fee_total (no rounding drift).
    let maker_buy = if orig > Decimal::ZERO { buy_fee_total * mf / orig } else { Decimal::ZERO };
    let fb_buy = buy_fee_total - maker_buy;
    // Maker portion: gross USDC = mf*target (fills AT the limit), maker fee 0.
    let maker_pnl = mf * target - (mf * entry + maker_buy);
    // Fallback portion: gross USDC = fb*fallback_fill, taker fee on the fill.
    let fb_sell_fee = taker_fee_dec(fb, fallback_fill);
    let fallback_pnl = (fb * fallback_fill - fb_sell_fee) - (fb * entry + fb_buy);
    SplitClose { maker_shares: mf, fallback_shares: fb, maker_pnl, fallback_pnl }
}

// ============================================================================
// #5 POST_ONLY — classify the maker POST result
// ============================================================================

/// What the maker POST produced.
#[derive(Debug, Clone, PartialEq)]
pub enum PostOutcome {
    /// The maker order is on the book (track it; cancel/reconcile later).
    Posted { order_id: String },
    /// post_only rejected because the order would cross (bid >= target at
    /// submit). The target is already achievable -> the caller taker-captures
    /// (sell at the bid, which is >= target). This is a WIN, paid as taker.
    RejectedCrossable,
    /// Any OTHER failure (network / auth / balance / timeout / unknown / a
    /// success with no order id). FAIL-CLOSED: the caller must NOT sell on
    /// this — treat it as a real error (retry / escalate), never a capture.
    RealError { text: String },
}

/// Case-insensitive substrings that identify a post_only / crossable rejection.
/// ONLY these map to `RejectedCrossable`; EVERYTHING else is a `RealError`
/// (fail-closed). The exact Polymarket wording must be confirmed against the
/// live API in Phase B — but the safety direction is one-way: an UNRECOGNIZED
/// crossable message falls to `RealError` (no wrong sell), never the reverse.
const CROSSABLE_SIGNALS: &[&str] = &[
    "post only",
    "post-only",
    "postonly",
    "would cross",
    "crossing",
    "would match",
    "marketable",
];

/// Classify a maker POST result. `success` + `order_id` come from
/// `PostOrderResponse`; `error_msg` from `PostOrderResponse.error_msg` OR the
/// `Err(..)` text. Pure: the live path extracts these three and calls this; the
/// paper sim feeds simulated values to exercise every branch.
#[must_use]
pub fn classify_post_outcome(
    success: bool,
    order_id: Option<&str>,
    error_msg: Option<&str>,
) -> PostOutcome {
    // Success path: a real (non-empty) order id means it is on the book.
    if success {
        if let Some(id) = order_id {
            if !id.is_empty() {
                return PostOutcome::Posted { order_id: id.to_string() };
            }
        }
        // success=true but no usable order id: we cannot track/cancel it.
        // Fail-closed -> real error.
        return PostOutcome::RealError {
            text: "post reported success but returned no order_id".to_string(),
        };
    }

    // Failure path: ONLY a recognized crossable message is benign.
    let text = error_msg.unwrap_or("").to_string();
    let lc = text.to_ascii_lowercase();
    if CROSSABLE_SIGNALS.iter().any(|s| lc.contains(s)) {
        return PostOutcome::RejectedCrossable;
    }
    // Anything else (including empty / unknown) -> real error, do NOT sell.
    PostOutcome::RealError { text }
}

// ============================================================================
// B3 FIRST-TOUCH SIM — the maker-exit P&L. A FAITHFUL Rust port of the Python
// analyzer's `b3_first_touch_pnl` (floor maker fee, F2_market fallback). The
// Python analyzer is the SOURCE OF TRUTH (it produced the validated May
// numbers); this port is pinned to it by a golden-fixture test, so the paper
// sim can never drift from the validated B3 number. Shared by the paper sim
// (Pieza 9) AND the live exit (Phase B).
// ============================================================================

/// Convex taker-fee coefficient. MUST match the Python analyzer's CONVEX_COEFF
/// (0.07) and the Rust backtester `fee_f64`.
pub const CONVEX_COEFF: f64 = 0.07;

/// Convex taker fee: `coeff * shares * price * (1 - price)`. Mirrors Python
/// `taker_fee`.
#[must_use]
pub fn taker_fee(shares: f64, price: f64) -> f64 {
    CONVEX_COEFF * shares * price * (1.0 - price)
}

/// Honest net P&L: buy at `entry` (taker fee) -> sell at `exit_price`
/// (`exit_fee`). Mirrors Python `pnl(s, e, exit, exit_fee)`.
#[must_use]
pub fn net_pnl_with_fees(shares: f64, entry: f64, exit_price: f64, exit_fee: f64) -> f64 {
    let buy_fee = taker_fee(shares, entry);
    (shares * exit_price - exit_fee) - (shares * entry + buy_fee)
}

/// Which exit path the B3 maker took.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum B3ExitKind {
    /// Maker filled at target (bid touched target in the window). Maker floor fee (0).
    FirstTouch,
    /// Fallback: break-even market stop (bid rose above entry then fell back to
    /// <= entry). Taker.
    F2BreakEven,
    /// Fallback: time-exit at the 120s bid (break-even never fired). Taker.
    F1TimeExit,
}

impl B3ExitKind {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::FirstTouch => "first_touch",
            Self::F2BreakEven => "f2_break_even",
            Self::F1TimeExit => "f1_time_exit",
        }
    }
}

/// The B3 maker-exit outcome for one lot. Carries BOTH gross and net P&L (the
/// Phase-3 point-3 decision): NET is the real B3 number (what you earn); GROSS
/// (`shares * (exit - entry)`, no fees) is for apples-to-apples comparison with
/// the gross baseline cells in a paper run.
#[derive(Debug, Clone, PartialEq)]
pub struct B3SimOutcome {
    pub exit_price: f64,
    pub exit_kind: B3ExitKind,
    /// `true` iff the maker first-touch filled (the WIN path).
    pub maker_filled: bool,
    /// GROSS P&L: `shares * (exit_price - entry)` — NO fees.
    pub gross_pnl: f64,
    /// NET P&L: full fee model (buy taker + exit fee). The real B3 number.
    pub net_pnl: f64,
}

/// First HWM entry with `bid >= target`. Mirrors Python `first_touch_offset`.
fn first_touch(hwm: &[(i64, f64)], target: f64) -> Option<(i64, f64)> {
    hwm.iter().copied().find(|&(_, b)| b >= target)
}

/// CAUSAL break-even: ARM at the first HWM bid > entry; FIRE at the first LWM
/// entry with `off > arm_off AND bid <= entry`, returning the ACTUAL bid at the
/// firing moment. Mirrors Python `break_even_hits` (incl. the `off > arm_off`
/// causality that skips an LWM dip occurring BEFORE the arm).
fn break_even(hwm: &[(i64, f64)], lwm: &[(i64, f64)], entry: f64) -> Option<(i64, f64)> {
    let arm_off = hwm.iter().copied().find(|&(_, b)| b > entry).map(|(off, _)| off)?;
    lwm.iter().copied().find(|&(off, b)| off > arm_off && b <= entry)
}

/// Polymarket penny-market tick size. The live path should read the actual
/// `tick_size()` per market; the paper sim uses this constant.
pub const TICK: f64 = 0.01;

/// The valid maker target = `entry + delta`, but ONLY if it stays `<= 1 - tick`.
/// `None` means `entry + delta` would reach/exceed `1 - tick`: NO maker can be
/// placed (a SELL limit at a price >= 1 is infeasible on a binary market). The
/// caller then NEVER places a maker for this lot and uses the F2_market
/// fallback directly at timeout — which MATCHES the analyzer, where a target
/// above 1 simply never first-touches and always falls back. We do NOT clamp
/// (clamping to `1 - tick` would let a live maker fill where the analyzer's
/// unreachable target never would — a divergence).
#[must_use]
pub fn valid_maker_target(entry: f64, delta: f64, tick: f64) -> Option<f64> {
    let t = entry + delta;
    if t <= 1.0 - tick { Some(t) } else { None }
}

/// Derive running-max (hwm) and running-min (lwm) traces from raw bid samples
/// `(offset_ms, bid)` taken over the hold window. The paper sim collects one
/// sample per exit-task tick; this turns them into the analyzer-shaped
/// monotone traces `b3_first_touch_sim` expects (same shape as the backtester's
/// `high_water_marks` / `low_water_marks`). Samples are assumed already in
/// time order (the exit task appends them as ticks advance).
#[must_use]
pub fn hwm_lwm_from_samples(samples: &[(i64, f64)]) -> (Vec<(i64, f64)>, Vec<(i64, f64)>) {
    let mut hwm = Vec::with_capacity(samples.len());
    let mut lwm = Vec::with_capacity(samples.len());
    let mut run_max = f64::NEG_INFINITY;
    let mut run_min = f64::INFINITY;
    for &(off, bid) in samples {
        run_max = run_max.max(bid);
        run_min = run_min.min(bid);
        hwm.push((off, run_max));
        lwm.push((off, run_min));
    }
    (hwm, lwm)
}

/// B3 maker first-touch exit with F2_market fallback, FLOOR (0) maker fee.
/// Faithful port of Python `b3_first_touch_pnl(delta, 'floor', 'F2_market')`
/// chained to `apply_fallback('F2_market')` -> `F1_time_stop_taker`.
///
/// `target` is precomputed (entry + delta) by the caller; the caller ALSO
/// handles the invalid-target case (entry+delta > 1-tick) BEFORE calling this
/// (no maker placed -> fallback used directly), so here `target` is assumed
/// reachable and the math mirrors the analyzer exactly. `time_exit_bid` is the
/// bid at the hold boundary (the analyzer's `baseline_exit_price`), used by the
/// F1 fallback.
#[must_use]
pub fn b3_first_touch_sim(
    entry: f64,
    target: f64,
    shares: f64,
    hwm: &[(i64, f64)],
    lwm: &[(i64, f64)],
    time_exit_bid: f64,
) -> B3SimOutcome {
    let (exit_price, exit_kind, exit_fee, maker_filled) =
        if first_touch(hwm, target).is_some() {
            // Maker fills at target; floor maker fee = 0.
            (target, B3ExitKind::FirstTouch, 0.0, true)
        } else if let Some((_, be_bid)) = break_even(hwm, lwm, entry) {
            // F2 break-even market stop at the actual bid, taker.
            (be_bid, B3ExitKind::F2BreakEven, taker_fee(shares, be_bid), false)
        } else {
            // F1 time-exit at the 120s bid, taker.
            (time_exit_bid, B3ExitKind::F1TimeExit, taker_fee(shares, time_exit_bid), false)
        };
    B3SimOutcome {
        exit_price,
        exit_kind,
        maker_filled,
        gross_pnl: shares * (exit_price - entry),
        net_pnl: net_pnl_with_fees(shares, entry, exit_price, exit_fee),
    }
}

// ============================================================================
// PAPER STRATEGY REPLAY (Phase A gate) — run the FULL production strategy over
// a real May b1 JSONL using the REAL bot B3 math (b3_first_touch_sim, the same
// function process_b3_makers calls). NOT the live bot (paper mode = live WS
// stream; can't replay May without touching the network). This exercises the
// Rust B3 EXIT MATH over the full real dataset -> the coherence gate (Rust ==
// the Python analyzer over ALL positions, not just the 8 golden fixtures) +
// the per-cell numbers (gross & net) + the maker lifecycle distribution.
//
// The tick-loop ORCHESTRATION (process_b3_makers: place/track/timeout) + the 7
// oplog events are covered by the sub-commit-3 integration tests (which drive
// process_b3_makers + assert the events). Driving process_b3_makers over the
// full May would need the raw 1s bid stream (not in the emitted JSONL); that
// is a capa_b extension, the next increment if wanted.
// ============================================================================

/// Per-cell aggregate for the strategy replay.
#[derive(Debug, Default, Clone)]
struct CellAgg {
    n: usize,
    gross: f64,
    net: f64,
    net_wins: usize,
    net_gross_gain: f64,
    net_gross_loss: f64,
    // B3 lifecycle counts.
    first_touch: usize,
    f2_break_even: usize,
    f1_time_exit: usize,
    invalid_target: usize,
}

/// Run the production strategy over a b1 JSONL and print the report.
/// Strategy (matches the operative bot.toml): BTC_5m baseline, ETH_5m + BTC_15m
/// B3Maker d=0.10 F2_market, ETH_15m RETIRED (excluded).
pub fn run_strategy_replay(input: &std::path::Path, dump_enabled: bool) -> anyhow::Result<()> {
    use std::collections::BTreeMap;
    use std::io::BufRead;
    use anyhow::Context;

    const DELTA: f64 = 0.10;
    let f = std::fs::File::open(input).with_context(|| format!("open {}", input.display()))?;
    let mut cells: BTreeMap<String, CellAgg> = BTreeMap::new();
    let mut excluded_eth15m = 0usize;
    let mut n_total = 0usize;
    // OPT-IN per-position dump (B3 cells) for the coherence diff vs the Python
    // analyzer. Off by default (the gate run only needs the aggregate report).
    let mut dump = if dump_enabled {
        let p = format!("{}.b3dump.jsonl", input.display());
        Some(std::io::BufWriter::new(std::fs::File::create(&p)?))
    } else {
        None
    };

    for line in std::io::BufReader::new(f).lines() {
        let line = line?;
        if line.trim().is_empty() { continue; }
        let v: serde_json::Value = serde_json::from_str(&line)?;
        n_total += 1;
        let asset = v["asset"].as_str().unwrap_or("");
        let interval = v["interval"].as_str().unwrap_or("");
        let cell = format!("{asset}_{interval}");

        // ETH_15m: RETIRED -> excluded (mirrors disabled_cells).
        if cell == "ETH_15m" {
            excluded_eth15m += 1;
            continue;
        }

        let entry = v["entry_price"].as_f64().unwrap_or(0.0);
        let shares = v["shares"].as_f64().unwrap_or(0.0);
        let time_exit_bid = v["baseline_exit_price"].as_f64().unwrap_or(entry);
        let agg = cells.entry(cell.clone()).or_default();
        agg.n += 1;

        let (gross, net) = if cell == "BTC_5m" {
            // Baseline: time-exit at the 120s bid, taker.
            let g = shares * (time_exit_bid - entry);
            let nt = net_pnl_with_fees(shares, entry, time_exit_bid, taker_fee(shares, time_exit_bid));
            (g, nt)
        } else {
            // B3 cells (ETH_5m, BTC_15m): the REAL Rust b3_first_touch_sim.
            let hwm = parse_pairs(&v["hwm_120s"]);
            let lwm = parse_pairs(&v["lwm_120s"]);
            let target = entry + DELTA;
            // Lifecycle bookkeeping: invalid target (entry+delta > 1-tick) never
            // first-touches -> always falls back (matches the analyzer + router).
            if target > 1.0 - TICK {
                agg.invalid_target += 1;
            }
            let o = b3_first_touch_sim(entry, target, shares, &hwm, &lwm, time_exit_bid);
            match o.exit_kind {
                B3ExitKind::FirstTouch => agg.first_touch += 1,
                B3ExitKind::F2BreakEven => agg.f2_break_even += 1,
                B3ExitKind::F1TimeExit => agg.f1_time_exit += 1,
            }
            if let Some(d) = dump.as_mut() {
                use std::io::Write as _;
                let _ = writeln!(d, "{}", serde_json::json!({
                    "signal_id": v["signal_id"].as_str().unwrap_or(""),
                    "cell": cell, "entry": entry, "target": target, "shares": shares,
                    "exit_kind": o.exit_kind.as_str(), "exit_price": o.exit_price,
                    "gross": o.gross_pnl, "net": o.net_pnl,
                }));
            }
            (o.gross_pnl, o.net_pnl)
        };
        agg.gross += gross;
        agg.net += net;
        if net > 0.0 {
            agg.net_wins += 1;
            agg.net_gross_gain += net;
        } else if net < 0.0 {
            agg.net_gross_loss += -net;
        }
    }

    // Report.
    println!("=========================================================================");
    println!("  PAPER STRATEGY REPLAY (real Rust B3 math over {})", input.display());
    println!("  Strategy: b1 entry | BTC_5m baseline | ETH_5m + BTC_15m B3 d=0.10 F2 | ETH_15m RETIRED");
    println!("=========================================================================");
    println!("  trades in file: {n_total} | ETH_15m EXCLUDED (retired): {excluded_eth15m}");
    println!();
    println!("  {:<9} {:>6} {:>11} {:>11} {:>7} {:>7}   {:<28}", "cell", "n", "gross$", "net$", "pf_net", "wr_net", "B3 lifecycle (ft/f2/f1, invalid)");
    println!("  {}", "-".repeat(95));
    let mut agg_n = 0usize; let mut agg_gross = 0.0; let mut agg_net = 0.0;
    let mut agg_wins = 0usize; let mut agg_gain = 0.0; let mut agg_loss = 0.0;
    for (cell, a) in &cells {
        let pf = if a.net_gross_loss > 0.0 { a.net_gross_gain / a.net_gross_loss }
                 else if a.net_gross_gain > 0.0 { f64::INFINITY } else { 0.0 };
        let wr = if a.n > 0 { a.net_wins as f64 / a.n as f64 } else { 0.0 };
        let life = if cell == "BTC_5m" { "(baseline)".to_string() }
                   else { format!("ft={} f2={} f1={} inv={}", a.first_touch, a.f2_break_even, a.f1_time_exit, a.invalid_target) };
        let pf_s = if pf == f64::INFINITY { "  inf".to_string() } else { format!("{pf:6.2}") };
        println!("  {:<9} {:>6} {:>+11.2} {:>+11.2} {:>7} {:>6.1}%   {:<28}",
            cell, a.n, a.gross, a.net, pf_s, 100.0 * wr, life);
        agg_n += a.n; agg_gross += a.gross; agg_net += a.net;
        agg_wins += a.net_wins; agg_gain += a.net_gross_gain; agg_loss += a.net_gross_loss;
    }
    println!("  {}", "-".repeat(95));
    let agg_pf = if agg_loss > 0.0 { agg_gain / agg_loss } else { 0.0 };
    let agg_wr = if agg_n > 0 { agg_wins as f64 / agg_n as f64 } else { 0.0 };
    println!("  {:<9} {:>6} {:>+11.2} {:>+11.2} {:>6.2} {:>6.1}%   (active cells, ETH_15m excluded)",
        "AGGREGATE", agg_n, agg_gross, agg_net, agg_pf, 100.0 * agg_wr);
    println!();
    Ok(())
}

/// Parse a JSON array of `[offset_ms, bid]` pairs into `Vec<(i64, f64)>`.
fn parse_pairs(v: &serde_json::Value) -> Vec<(i64, f64)> {
    v.as_array().map(|arr| {
        arr.iter().filter_map(|e| {
            let p = e.as_array()?;
            Some((p.first()?.as_i64()?, p.get(1)?.as_f64()?))
        }).collect()
    }).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    // ---- #4 race: reconcile_decision ----

    const ALL_STATUSES: &[MakerOrderStatus] = &[
        MakerOrderStatus::Cancelled,
        MakerOrderStatus::Matched,
        MakerOrderStatus::Unmatched,
        MakerOrderStatus::Live,
        MakerOrderStatus::Delayed,
        MakerOrderStatus::Unknown,
    ];

    /// THE INVARIANT (the thing that makes oversell impossible). Exhaustive
    /// enumeration over every status x every (orig, matched) in a fine grid:
    ///   * defer  => maker_filled == 0 AND fallback_size == 0
    ///   * !defer => maker_filled + fallback_size == orig
    /// AND maker_filled is never more than what was reported matched (clamped),
    /// AND nothing is ever negative.
    #[test]
    fn reconcile_invariant_holds_for_all_inputs() {
        // orig in {0.00, 0.25, ..., 10.00}; matched in {-1.00 .. orig+1.00}
        // (including OUT-OF-RANGE matched to prove the clamp).
        let mut orig_cents = 0;
        while orig_cents <= 1000 {
            let orig = Decimal::new(orig_cents, 2);
            let mut m_cents = -100;
            while m_cents <= orig_cents + 100 {
                let matched = Decimal::new(m_cents, 2);
                for &st in ALL_STATUSES {
                    let o = reconcile_decision(st, orig, matched);
                    assert!(o.maker_filled >= Decimal::ZERO, "maker_filled >= 0");
                    assert!(o.fallback_size >= Decimal::ZERO, "fallback_size >= 0");
                    assert!(o.maker_filled <= orig, "maker_filled <= orig (no oversell side)");
                    if o.defer {
                        assert_eq!(o.maker_filled, Decimal::ZERO, "defer sells nothing (maker)");
                        assert_eq!(o.fallback_size, Decimal::ZERO, "defer sells nothing (fallback)");
                    } else {
                        assert_eq!(
                            o.maker_filled + o.fallback_size, orig,
                            "non-defer: maker_filled + fallback_size MUST == orig \
                             (status={st:?}, orig={orig}, matched={matched})"
                        );
                    }
                }
                m_cents += 25;
            }
            orig_cents += 25;
        }
    }

    #[test]
    fn reconcile_full_fill() {
        let o = reconcile_decision(MakerOrderStatus::Matched, dec!(8.0), dec!(8.0));
        assert_eq!(o.decision, ReconcileDecision::FullFill);
        assert_eq!(o.maker_filled, dec!(8.0));
        assert_eq!(o.fallback_size, dec!(0));
        assert!(!o.defer);
    }

    #[test]
    fn reconcile_full_fill_even_if_status_still_live() {
        // matched == orig is terminal BY DEFINITION (filled), regardless of the
        // status string lagging at Live. Must NOT defer or oversell.
        let o = reconcile_decision(MakerOrderStatus::Live, dec!(8.0), dec!(8.0));
        assert_eq!(o.decision, ReconcileDecision::FullFill);
        assert_eq!(o.maker_filled, dec!(8.0));
        assert_eq!(o.fallback_size, dec!(0));
        assert!(!o.defer);
    }

    #[test]
    fn reconcile_clean_cancel() {
        let o = reconcile_decision(MakerOrderStatus::Cancelled, dec!(8.0), dec!(0));
        assert_eq!(o.decision, ReconcileDecision::CleanCancel);
        assert_eq!(o.maker_filled, dec!(0));
        assert_eq!(o.fallback_size, dec!(8.0));
        assert!(!o.defer);
    }

    #[test]
    fn reconcile_partial() {
        let o = reconcile_decision(MakerOrderStatus::Cancelled, dec!(8.0), dec!(3.0));
        assert_eq!(o.decision, ReconcileDecision::Partial);
        assert_eq!(o.maker_filled, dec!(3.0));
        assert_eq!(o.fallback_size, dec!(5.0));
        assert!(!o.defer);
    }

    #[test]
    fn reconcile_ambiguous_defers_no_sell() {
        // The cancel did not confirm (still Live/Delayed/Unknown) and the fill
        // is incomplete: the order could still fill more. MUST defer (sell
        // nothing) -- the anti-oversell fail-safe (the watchdog lesson:
        // abstain when the state is in doubt).
        for &st in &[MakerOrderStatus::Live, MakerOrderStatus::Delayed, MakerOrderStatus::Unknown] {
            let o = reconcile_decision(st, dec!(8.0), dec!(3.0));
            assert_eq!(o.decision, ReconcileDecision::Ambiguous, "status {st:?}");
            assert!(o.defer, "status {st:?} must defer");
            assert_eq!(o.maker_filled, dec!(0), "defer sells nothing");
            assert_eq!(o.fallback_size, dec!(0), "defer sells nothing");
        }
    }

    #[test]
    fn reconcile_over_report_is_clamped_to_full_fill() {
        // A buggy SDK report of matched > orig must NOT produce a negative
        // fallback or an oversell -> clamp to orig (full fill).
        let o = reconcile_decision(MakerOrderStatus::Matched, dec!(8.0), dec!(99.0));
        assert_eq!(o.decision, ReconcileDecision::FullFill);
        assert_eq!(o.maker_filled, dec!(8.0));
        assert_eq!(o.fallback_size, dec!(0));
    }

    // ---- timeout action driven by the reconcile verdict ----

    #[test]
    fn b3_timeout_action_for_every_reconcile_case() {
        // Defer (ambiguous) -> sell nothing.
        let amb = reconcile_decision(MakerOrderStatus::Live, dec!(8.0), dec!(3.0));
        assert_eq!(b3_timeout_action(&amb), B3TimeoutAction::Defer);
        // Clean cancel -> full fallback.
        let cc = reconcile_decision(MakerOrderStatus::Cancelled, dec!(8.0), dec!(0));
        assert_eq!(b3_timeout_action(&cc), B3TimeoutAction::FullFallback);
        // Full fill -> full maker close.
        let ff = reconcile_decision(MakerOrderStatus::Matched, dec!(8.0), dec!(8.0));
        assert_eq!(b3_timeout_action(&ff), B3TimeoutAction::FullMakerFill);
        // Partial -> Partial{maker_filled, fallback_size} (lot-split, Phase B).
        let pf = reconcile_decision(MakerOrderStatus::Cancelled, dec!(8.0), dec!(3.0));
        assert_eq!(
            b3_timeout_action(&pf),
            B3TimeoutAction::Partial { maker_filled: dec!(3.0), fallback_size: dec!(5.0) }
        );
    }

    #[test]
    fn b3_timeout_action_never_oversells() {
        // For EVERY reconcile outcome, the action sells at most `orig`:
        //   Defer            -> 0
        //   FullFallback     -> orig (== fallback_size on clean cancel)
        //   FullMakerFill    -> orig (== maker_filled on full fill)
        //   Partial          -> maker_filled + fallback_size == orig (but the
        //                       caller treats Partial as Defer until Phase B
        //                       lot-split, so 0 sold in the interim).
        let orig = dec!(10.0);
        for st in [MakerOrderStatus::Cancelled, MakerOrderStatus::Matched,
                   MakerOrderStatus::Unmatched, MakerOrderStatus::Live,
                   MakerOrderStatus::Delayed, MakerOrderStatus::Unknown] {
            for m in [0i64, 3, 7, 10] {
                let recon = reconcile_decision(st, orig, Decimal::new(m, 0));
                match b3_timeout_action(&recon) {
                    B3TimeoutAction::Defer => {}
                    B3TimeoutAction::FullFallback | B3TimeoutAction::FullMakerFill => {}
                    B3TimeoutAction::Partial { maker_filled, fallback_size } => {
                        assert_eq!(maker_filled + fallback_size, orig,
                            "partial must split to exactly orig (st={st:?}, m={m})");
                    }
                }
            }
        }
    }

    // ---- Phase B Pieza 2: b3_poll_decision ----

    #[test]
    fn poll_full_fill_is_filled_even_when_due_or_live() {
        // matched >= orig is Filled regardless of `due` or the status string.
        assert_eq!(b3_poll_decision(MakerOrderStatus::Live, dec!(10), dec!(10), false), PollDecision::Filled);
        assert_eq!(b3_poll_decision(MakerOrderStatus::Live, dec!(10), dec!(10), true), PollDecision::Filled);
        assert_eq!(b3_poll_decision(MakerOrderStatus::Matched, dec!(11), dec!(10), false), PollDecision::Filled); // over-report clamped
    }

    #[test]
    fn poll_working_waiting_when_not_due_and_live() {
        // resting, partial-so-far -> Working; nothing yet -> Waiting.
        assert_eq!(b3_poll_decision(MakerOrderStatus::Live, dec!(3), dec!(10), false), PollDecision::Working);
        assert_eq!(b3_poll_decision(MakerOrderStatus::Live, dec!(0), dec!(10), false), PollDecision::Waiting);
    }

    #[test]
    fn poll_timeout_when_due_or_terminated_early() {
        // due -> Timeout (reconcile).
        assert_eq!(b3_poll_decision(MakerOrderStatus::Live, dec!(3), dec!(10), true), PollDecision::Timeout);
        assert_eq!(b3_poll_decision(MakerOrderStatus::Live, dec!(0), dec!(10), true), PollDecision::Timeout);
        // terminal status (cancelled/expired externally) before a full fill, NOT
        // due -> still Timeout (the order died early; reconcile the remainder).
        assert_eq!(b3_poll_decision(MakerOrderStatus::Cancelled, dec!(3), dec!(10), false), PollDecision::Timeout);
        assert_eq!(b3_poll_decision(MakerOrderStatus::Unmatched, dec!(0), dec!(10), false), PollDecision::Timeout);
    }

    // ---- Phase B Pieza 3: split_close (the partial lot-split) ----

    /// THE NO-OVERSELL INVARIANT of the lot-split. For EVERY (orig, maker_filled)
    /// on a fine grid -- INCLUDING out-of-range maker_filled (negative / above
    /// orig) to prove the clamp -- the two portions sum EXACTLY to orig and
    /// neither is negative. Tied to the real size_matched (maker_filled), this is
    /// what makes a partial fill UNABLE to oversell.
    #[test]
    fn split_close_shares_always_sum_to_orig() {
        let mut o_cents = 0;
        while o_cents <= 1000 {
            let orig = Decimal::new(o_cents, 2);
            let mut m_cents = -100;
            while m_cents <= o_cents + 100 {
                let mf = Decimal::new(m_cents, 2);
                let s = split_close(orig, mf, dec!(0.65), dec!(0.55), dec!(0.50), dec!(0.01));
                assert!(s.maker_shares >= Decimal::ZERO, "maker_shares >= 0");
                assert!(s.fallback_shares >= Decimal::ZERO, "fallback_shares >= 0");
                assert!(s.maker_shares <= orig, "maker_shares <= orig");
                assert_eq!(
                    s.maker_shares + s.fallback_shares, orig,
                    "split MUST sum to orig (orig={orig}, maker_filled={mf})"
                );
                m_cents += 25;
            }
            o_cents += 25;
        }
    }

    #[test]
    fn split_close_clean_partial_3_of_10() {
        // 3 of 10 filled at target 0.65; 7 fall back at 0.55. entry 0.50, buy fee 0.01.
        let s = split_close(dec!(10), dec!(3), dec!(0.65), dec!(0.55), dec!(0.50), dec!(0.01));
        assert_eq!(s.maker_shares, dec!(3));
        assert_eq!(s.fallback_shares, dec!(7));
        // maker portion: 3*0.65 - (3*0.50 + 0.01*3/10) = 1.95 - 1.503 = 0.447
        assert_eq!(s.maker_pnl, dec!(0.447));
        // fallback portion: (7*0.55 - taker(7,0.55)) - (7*0.50 + 0.007)
        //   taker(7,0.55) = 0.07*7*0.55*0.45 = 0.121275
        //   = (3.85 - 0.121275) - (3.50 + 0.007) = 3.728725 - 3.507 = 0.221725
        assert_eq!(s.fallback_pnl, dec!(0.221725));
    }

    #[test]
    fn split_close_full_fill_has_zero_fallback() {
        // maker_filled == orig -> all maker, no fallback.
        let s = split_close(dec!(10), dec!(10), dec!(0.65), dec!(0.55), dec!(0.50), dec!(0.01));
        assert_eq!(s.maker_shares, dec!(10));
        assert_eq!(s.fallback_shares, dec!(0));
        assert_eq!(s.fallback_pnl, dec!(0)); // no shares -> 0 (taker fee on 0 == 0)
    }

    // ---- #5 post_only: classify_post_outcome ----

    #[test]
    fn post_success_with_id_is_posted() {
        let o = classify_post_outcome(true, Some("0xabc"), None);
        assert_eq!(o, PostOutcome::Posted { order_id: "0xabc".to_string() });
    }

    #[test]
    fn post_success_without_id_is_real_error_fail_closed() {
        // success but no order id -> cannot track/cancel -> fail-closed.
        let o = classify_post_outcome(true, None, None);
        assert!(matches!(o, PostOutcome::RealError { .. }), "got {o:?}");
        let o2 = classify_post_outcome(true, Some(""), None);
        assert!(matches!(o2, PostOutcome::RealError { .. }), "empty id -> real error");
    }

    #[test]
    fn post_recognized_crossable_messages_map_to_rejected_crossable() {
        let msgs = [
            "order would cross the book",
            "post only order is marketable",
            "POSTONLY violation",
            "Post-Only would match resting order",
            "crossing not allowed for post only",
        ];
        for m in msgs {
            assert_eq!(
                classify_post_outcome(false, None, Some(m)),
                PostOutcome::RejectedCrossable,
                "message {m:?} must be RejectedCrossable",
            );
        }
    }

    #[test]
    fn post_unrecognized_errors_are_real_error_fail_closed() {
        // The fail-closed contract: ANY error that is NOT a recognized
        // crossable message -> RealError -> caller must NOT sell. A battery of
        // real failure modes + empty/None.
        let msgs = [
            "connection reset by peer",
            "401 unauthorized: bad api key",
            "insufficient balance",
            "request timed out after 5000ms",
            "invalid amounts, max accuracy of 2 decimals",
            "internal server error",
            "", // empty
        ];
        for m in msgs {
            assert!(
                matches!(classify_post_outcome(false, None, Some(m)), PostOutcome::RealError { .. }),
                "message {m:?} must be RealError (fail-closed, NOT a sell)",
            );
        }
        // None error_msg on a failure -> RealError too.
        assert!(matches!(classify_post_outcome(false, None, None), PostOutcome::RealError { .. }));
    }

    #[test]
    fn post_real_error_preserves_exact_text() {
        let exact = "insufficient balance: need 10.50 USDC, have 2.00";
        match classify_post_outcome(false, None, Some(exact)) {
            PostOutcome::RealError { text } => assert_eq!(text, exact, "exact text preserved"),
            o => panic!("expected RealError, got {o:?}"),
        }
    }

    // ---- B3 first-touch sim: COHERENCE vs the Python analyzer (golden fixture) ----

    /// The fixture is generated by `scripts/gen_b3_golden_fixture.py`, which
    /// computes each expected outcome with the PYTHON analyzer
    /// (`b3_first_touch_pnl`, the validated source of truth) and cross-checks it
    /// against that function. We embed it at compile time so the test has no
    /// runtime file/cwd dependency. If `b3_first_touch_sim` ever drifts from the
    /// analyzer, this fails; regenerate the fixture after any deliberate change
    /// to the analyzer's B3 math.
    const GOLDEN: &str = include_str!("b3_golden_fixture.json");

    fn pairs(v: &serde_json::Value) -> Vec<(i64, f64)> {
        v.as_array().unwrap().iter().map(|e| {
            let a = e.as_array().unwrap();
            (a[0].as_i64().unwrap(), a[1].as_f64().unwrap())
        }).collect()
    }

    // ---- router helpers: valid_maker_target + hwm_lwm_from_samples ----

    #[test]
    fn valid_maker_target_normal_case() {
        // entry 0.65 + 0.10 = 0.75 <= 0.99 -> valid.
        assert_eq!(valid_maker_target(0.65, 0.10, TICK), Some(0.75));
    }

    #[test]
    fn valid_maker_target_invalid_near_one_no_clamp() {
        // entry 0.95 + 0.10 = 1.05 > 0.99 -> None (NO maker; NOT clamped to
        // 0.99). The caller goes straight to F2_market, matching the analyzer.
        assert_eq!(valid_maker_target(0.95, 0.10, TICK), None);
        // entry 0.90 + 0.10 = 1.00 > 0.99 -> None (boundary: 1.00 is not <= 0.99).
        assert_eq!(valid_maker_target(0.90, 0.10, TICK), None);
        // entry 0.89 + 0.10 = 0.99 <= 0.99 -> just valid.
        assert_eq!(valid_maker_target(0.89, 0.10, TICK), Some(0.99));
    }

    #[test]
    fn hwm_lwm_from_samples_running_max_min() {
        let samples = [(0, 0.50), (1000, 0.55), (2000, 0.52), (3000, 0.60), (4000, 0.48)];
        let (hwm, lwm) = hwm_lwm_from_samples(&samples);
        // hwm is the running MAX; lwm the running MIN, both monotone.
        assert_eq!(hwm, vec![(0, 0.50), (1000, 0.55), (2000, 0.55), (3000, 0.60), (4000, 0.60)]);
        assert_eq!(lwm, vec![(0, 0.50), (1000, 0.50), (2000, 0.50), (3000, 0.50), (4000, 0.48)]);
    }

    #[test]
    fn hwm_lwm_from_samples_empty() {
        let (hwm, lwm) = hwm_lwm_from_samples(&[]);
        assert!(hwm.is_empty() && lwm.is_empty());
    }

    /// Coherence bridge: the paper sim builds hwm/lwm from raw 1s samples, then
    /// calls b3_first_touch_sim. Verify that path reproduces the same exit as
    /// feeding analyzer-shaped traces — i.e. running-max/min derivation +
    /// b3_first_touch_sim agree with a hand-built expectation. (b3_first_touch_sim
    /// itself is pinned to Python by the golden fixture; this checks the
    /// sample->trace derivation the paper sim adds on top.)
    #[test]
    fn paper_sample_path_first_touch_and_fallback() {
        // Path that TOUCHES target 0.75 at t=2s -> FirstTouch @ 0.75.
        let samples = [(0_i64, 0.66), (1000, 0.70), (2000, 0.76)];
        let (hwm, lwm) = hwm_lwm_from_samples(&samples);
        let o = b3_first_touch_sim(0.65, 0.75, 10.0, &hwm, &lwm, 0.70);
        assert_eq!(o.exit_kind, B3ExitKind::FirstTouch);
        assert_eq!(o.exit_price, 0.75);

        // Path that never touches 0.75 but arms (0.70>0.65) then falls to 0.62
        // (<=0.65) -> F2 break-even at the running-min low.
        let samples2 = [(0_i64, 0.66), (1000, 0.70), (2000, 0.62)];
        let (hwm2, lwm2) = hwm_lwm_from_samples(&samples2);
        let o2 = b3_first_touch_sim(0.65, 0.75, 10.0, &hwm2, &lwm2, 0.63);
        assert_eq!(o2.exit_kind, B3ExitKind::F2BreakEven);
        assert_eq!(o2.exit_price, 0.62, "F2 sells at the actual low bid");
    }

    #[test]
    fn coherence_paper_sim_matches_python_golden_fixture() {
        let doc: serde_json::Value = serde_json::from_str(GOLDEN).expect("parse golden fixture");
        let scenarios = doc["scenarios"].as_array().expect("scenarios array");
        assert!(!scenarios.is_empty(), "fixture must have scenarios");

        for sc in scenarios {
            let name = sc["name"].as_str().unwrap();
            let entry = sc["entry"].as_f64().unwrap();
            let delta = sc["delta"].as_f64().unwrap();
            let shares = sc["shares"].as_f64().unwrap();
            let hwm = pairs(&sc["hwm"]);
            let lwm = pairs(&sc["lwm"]);
            let time_exit_bid = sc["time_exit_bid"].as_f64().unwrap();
            let target = entry + delta;

            let got = b3_first_touch_sim(entry, target, shares, &hwm, &lwm, time_exit_bid);

            let exp = &sc["expected"];
            // exit_price + kind + maker_filled are EXACT (selection, no arithmetic).
            assert_eq!(
                got.exit_price, exp["exit_price"].as_f64().unwrap(),
                "[{name}] exit_price"
            );
            assert_eq!(
                got.exit_kind.as_str(), exp["exit_kind"].as_str().unwrap(),
                "[{name}] exit_kind"
            );
            assert_eq!(
                got.maker_filled, exp["maker_filled"].as_bool().unwrap(),
                "[{name}] maker_filled"
            );
            // gross + net match within float epsilon (cross-language ULP slack).
            let eps = 1e-9;
            assert!(
                (got.gross_pnl - exp["gross_pnl"].as_f64().unwrap()).abs() < eps,
                "[{name}] gross_pnl: rust {} vs python {}",
                got.gross_pnl, exp["gross_pnl"].as_f64().unwrap()
            );
            assert!(
                (got.net_pnl - exp["net_pnl"].as_f64().unwrap()).abs() < eps,
                "[{name}] net_pnl: rust {} vs python {}",
                got.net_pnl, exp["net_pnl"].as_f64().unwrap()
            );
        }
    }
}
