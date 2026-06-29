//! PHASE B Pieza 2+3 — the LIVE B3 maker orchestration.
//!
//! The 3-phase concurrency structure (lock -> snapshot / unlock -> await SDK /
//! lock -> apply) over a `MakerBackend` SEAM. The SDK calls live behind the
//! trait so the orchestration is generic + testable end-to-end with a MOCK
//! backend (NO network). The real impl (`LiveMakerBackend`) calls the SDK but
//! is NOT exercised against Polymarket until the ARM (and is wired into the
//! exit task in the final integration step, not here).
//!
//! CONCURRENCY: `std::sync::Mutex` guards are NOT `Send`, so the compiler
//! REJECTS holding one across an `.await`. The 3-phase structure keeps every
//! SDK `.await` OUTSIDE the lock; the lot is keyed by `signal_id` (stable),
//! never by index (which shifts when the decision task adds a position).
//!
//! SIGNAL_ID UNIQUENESS CONTRACT: keying by signal_id across the lock release
//! is only sound if signal_id is UNIQUE per live lot and NEVER reused. The
//! decision engine guarantees this: `signal_id = "{asset}-{trigger_ts}-
//! {interval}-{dir}"` (decision/mod.rs) and `trigger_ts` is MONOTONIC (wall
//! time advances), so a new trigger always yields a fresh signal_id. A vanished
//! signal_id (closed by another path between phases) is found as `None` in
//! apply and SKIPPED, never mis-applied to a different lot.
//!
//! THE PHASE-A LESSON: every reconcile verdict (FullMakerFill / FullFallback /
//! Partial / Defer) is ACTED ON explicitly here — NOTHING closes the full lot
//! blindly. Defer sells 0; Partial uses `split_close` (the real `size_matched`),
//! never a full close.
//!
//! HARD-TIMEOUT FORCE-CLOSE — DELIBERATELY NOT IMPLEMENTED (2026-06-09). If a
//! maker is due but its token's BBO is dead (no fresh bid), we emit
//! `b3_maker_fallback_deferred_no_bid` and HOLD (sell nothing), retrying when a
//! bid returns. We do NOT force-sell at the worst available price: a dead BBO
//! means no liquidity, so a force-sell would dump at a fire-sale price, turning
//! a stuck ~$1 position (cap $1.05) into a realized loss at a terrible level.
//! That is the opposite of the "abstain under uncertainty" discipline applied
//! everywhere else — under a rare/unknown state the bot makes itself VISIBLE
//! (the warning) and leaves the call to a human, instead of selling blind. The
//! systemic case (a globally dead feed) is covered by the feed-dead guard
//! (30 s -> halt). Reconsider if position size scales up materially.

#![allow(dead_code)] // the live exit-task wiring lands with the final integration step

use std::collections::HashSet;
use std::sync::Mutex;

use async_trait::async_trait;
use dashmap::DashMap;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

use crate::b3_maker::{
    self, MakerOrderStatus, PollDecision, PostOutcome, b3_poll_decision, b3_timeout_action,
    reconcile_decision, split_close, taker_fee_dec, valid_maker_target,
};
use crate::config::{ExitRule, ExitsConfig};
use crate::decision::executor::{ClosedTrade, ExitKind};
use crate::oplog::OpLog;
use crate::state::persist::{MakerExitState, MakerExitStatus, OpenPosition};
use crate::state::{EventBbo, STALE_BBO_MS};

/// Whether a cancel was confirmed by the venue (for the oplog `cancel_resp`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelResp {
    Canceled,
    NotCanceled,
}
impl CancelResp {
    fn as_str(self) -> &'static str {
        match self {
            CancelResp::Canceled => "canceled",
            CancelResp::NotCanceled => "not_canceled",
        }
    }
}

/// THE SEAM. Abstracts the four SDK touchpoints of the maker lifecycle so the
/// orchestration is testable with a mock. The real impl calls Polymarket; the
/// mock returns scripted responses. All are `&self` async; the orchestration
/// awaits them OUTSIDE the state lock.
#[async_trait]
pub trait MakerBackend {
    /// Post a GTC + post_only maker SELL at `target`. `signal_id` keys the
    /// idempotency coid (the real impl write-aheads it). Returns the parsed
    /// verdict (`classify_maker_post`): Posted{order_id}/RejectedCrossable/RealError.
    async fn place_maker(&self, signal_id: &str, token_id: &str, shares: Decimal, target: Decimal) -> anyhow::Result<PostOutcome>;
    /// Poll one resting order: `order(id)` -> (mapped status, cumulative size_matched).
    async fn poll_order(&self, order_id: &str) -> anyhow::Result<(MakerOrderStatus, Decimal)>;
    /// The race: `cancel_order(id)` then `order(id)` for the AUTHORITATIVE state.
    /// On a network failure of the `order(id)` truth-read the real impl returns
    /// `Err` -> the orchestration DEFERS (never sells on an unknown state).
    async fn cancel_then_reconcile(&self, order_id: &str) -> anyhow::Result<(MakerOrderStatus, Decimal, CancelResp)>;
    /// A real FOK taker SELL at the bid (F2 fallback / crossable capture / no-maker
    /// fallback). `signal_id` keys the FOK idempotency coid. Returns the real
    /// `taking_amount` (USDC received).
    async fn taker_sell(&self, signal_id: &str, token_id: &str, shares: Decimal, bid: Decimal) -> anyhow::Result<Decimal>;
    /// RECONCILE-ON-STARTUP: list ALL of the bot's currently-open orders
    /// (`clob().orders()`, paginated, flattened). Each row is one resting order.
    async fn list_open_orders(&self) -> anyhow::Result<Vec<OpenOrderRow>>;
    /// Cancel one order by id (`cancel_order`). Risk-reducing (no LIVE_ARMED gate).
    async fn cancel(&self, order_id: &str) -> anyhow::Result<CancelResp>;
}

/// One resting order from `list_open_orders` (a flattened `OpenOrderResponse`).
#[derive(Debug, Clone, PartialEq)]
pub struct OpenOrderRow {
    pub order_id: String,
    pub token_id: String,
    pub is_sell: bool,
    pub price: Decimal,
    pub original_size: Decimal,
    pub size_matched: Decimal,
}

// ============================================================================
// PHASE 1 — collect (PURE, under the lock, no await)
// ============================================================================

/// One per-position action decided from the snapshot. Plain data (Send), keyed
/// by `signal_id`.
#[derive(Debug, Clone, PartialEq)]
enum B3Action {
    /// No maker, valid target, not due, fresh market -> place a maker.
    Place { signal_id: String, token_id: String, cell: String, shares: Decimal, target: Decimal, entry: Decimal, bid: Decimal },
    /// A maker is resting -> poll it (fill detect; at `due`, cancel+reconcile).
    Poll { signal_id: String, token_id: String, order_id: String, orig: Decimal, target: Decimal, entry: Decimal, due: bool, exit_ts_s: i64, bid: Option<Decimal> },
    /// No maker (invalid target, or never placed) + due -> F2 the whole lot.
    FallbackOnly { signal_id: String, token_id: String, shares: Decimal, entry: Decimal, bid: Decimal },
}

const DELTA: f64 = 0.10;
const TICK: f64 = b3_maker::TICK;

fn fresh_bid(bbo: &DashMap<String, EventBbo>, token: &str, now_ms: i64) -> Option<Decimal> {
    let e = bbo.get(token)?;
    if now_ms - e.ts_ms > STALE_BBO_MS {
        return None;
    }
    match e.best_bid {
        Some(b) if b.is_finite() => Decimal::try_from(b).ok(),
        _ => None,
    }
}

/// PHASE 1: from a snapshot of positions, decide the per-position action.
fn collect_b3_actions(
    positions: &[OpenPosition],
    cfg: &ExitsConfig,
    bbo: &DashMap<String, EventBbo>,
    now_ms: i64,
) -> Vec<B3Action> {
    let now_s = now_ms / 1000;
    let mut out = Vec::new();
    for p in positions {
        // B3Maker cell only.
        let cell = format!("{}:{}", p.asset, p.interval);
        if !matches!(cfg.cells.get(&cell), Some(ExitRule::B3Maker { .. })) {
            continue;
        }
        // Same sentinels as the paper path.
        if p.entry_ts_ms == 0 || p.asset.is_empty() {
            continue;
        }
        // A maker already terminal -> closing/closed; skip.
        if let Some(m) = &p.maker_exit {
            if m.status != MakerExitStatus::Placed {
                continue;
            }
        }
        let due = p.exit_ts_s <= now_s;
        let entry = p.entry_price;
        let entry_f = entry.to_f64().unwrap_or(0.0);
        let bid = fresh_bid(bbo, &p.token_id, now_ms);

        match &p.maker_exit {
            // Resting maker -> poll.
            Some(m) => {
                out.push(B3Action::Poll {
                    signal_id: p.signal_id.clone(), token_id: p.token_id.clone(),
                    order_id: m.order_id.clone(), orig: p.shares,
                    target: Decimal::try_from(m.target).unwrap_or(entry), entry, due,
                    exit_ts_s: p.exit_ts_s, bid,
                });
            }
            // No maker.
            None => {
                if due {
                    // Need a bid to F2; hold if none.
                    if let Some(b) = bid {
                        out.push(B3Action::FallbackOnly {
                            signal_id: p.signal_id.clone(), token_id: p.token_id.clone(),
                            shares: p.shares, entry, bid: b,
                        });
                    }
                } else if let Some(target) = valid_maker_target(entry_f, DELTA, TICK) {
                    // Valid target + not due: place IF the market is fresh.
                    if let Some(b) = bid {
                        let snapped = snap_dec(entry + Decimal::try_from(DELTA).unwrap());
                        let _ = target; // (f64 validity check; the real price is the Decimal snap)
                        out.push(B3Action::Place {
                            signal_id: p.signal_id.clone(), token_id: p.token_id.clone(),
                            cell: cell.clone(), shares: p.shares, target: snapped, entry, bid: b,
                        });
                    }
                }
                // invalid target + not due -> nothing (waits for due -> FallbackOnly).
            }
        }
    }
    out
}

/// Snap a Decimal price to the 0.01 tick (round to 2 dp). Mirrors
/// live_executor::snap_to_tick for the maker target.
fn snap_dec(p: Decimal) -> Decimal {
    let tick = Decimal::new(1, 2); // 0.01
    (p / tick).round() * tick
}

// ============================================================================
// PHASE 2 — execute (await SDK, NO lock)
// ============================================================================

/// What PHASE 3 applies. Carries fully-decided P&L (computed from real amounts).
#[derive(Debug, Clone, PartialEq)]
enum B3Apply {
    /// Posted -> set maker_exit = Some{Placed}.
    SetMaker { signal_id: String, order_id: String, target: Decimal },
    /// Close the whole lot once at `net_pnl` (maker fill / F2 / crossable).
    Close { signal_id: String, net_pnl: Decimal, exit_price: Decimal, kind: ExitKind },
    /// Partial: close in two portions (maker at target + fallback via F2). The
    /// realized total = maker_net + fallback_net (sum to the whole lot).
    CloseSplit { signal_id: String, maker_net: Decimal, fallback_net: Decimal, fallback_fill: Decimal },
    /// Defer / Working / Waiting / RealError -> do NOTHING (the anti-oversell path).
    Nothing { signal_id: String },
}

/// The model BUY taker fee for `shares` bought at `entry` (the BUY was a taker).
fn buy_fee_of(shares: Decimal, entry: Decimal) -> Decimal {
    taker_fee_dec(shares, entry)
}

/// PHASE 2: run one action against the backend + the PURE decisions. Emits the
/// oplog events at the point each resolves. Returns the apply-instruction.
async fn execute_b3_action<B: MakerBackend + ?Sized>(
    backend: &B,
    action: B3Action,
    oplog: &OpLog,
) -> B3Apply {
    match action {
        B3Action::Place { signal_id, token_id, cell, shares, target, entry, bid } => {
            match backend.place_maker(&signal_id, &token_id, shares, target).await {
                Ok(PostOutcome::Posted { order_id }) => {
                    oplog.sys("b3_maker_placed", serde_json::json!({
                        "signal_id": signal_id, "token_id": token_id, "cell": cell, "order_id": order_id,
                        "entry_price": entry.to_string(), "target_price": target.to_string(),
                        "delta": DELTA, "shares": shares.to_string(),
                    }));
                    B3Apply::SetMaker { signal_id, order_id, target }
                }
                Ok(PostOutcome::RejectedCrossable) => {
                    // The target is already reachable -> taker-capture at the bid (>= target).
                    close_via_taker(backend, &token_id, shares, bid, entry, &signal_id,
                        ExitKind::B3MakerFill, "taker_capture_crossable", oplog).await
                }
                Ok(PostOutcome::RealError { text }) => {
                    oplog.sys("b3_maker_rejected_crossable", serde_json::json!({
                        "signal_id": signal_id, "token_id": token_id, "order_id": "",
                        "target": target.to_string(), "bid_at_submit": bid.to_string(),
                        "action_taken": "error", "error": text,
                    }));
                    oplog.err("b3_maker_place", &text, serde_json::json!({ "signal_id": signal_id }));
                    B3Apply::Nothing { signal_id } // fail-closed: do NOT sell
                }
                Err(e) => {
                    oplog.err("b3_maker_place", &e.to_string(), serde_json::json!({ "signal_id": signal_id }));
                    B3Apply::Nothing { signal_id }
                }
            }
        }
        B3Action::FallbackOnly { signal_id, token_id, shares, entry, bid } => {
            close_via_taker(backend, &token_id, shares, bid, entry, &signal_id,
                ExitKind::B3Fallback, "no_maker", oplog).await
        }
        B3Action::Poll { signal_id, token_id, order_id, orig, target, entry, due, exit_ts_s, bid } => {
            let (status, matched) = match backend.poll_order(&order_id).await {
                Ok(v) => v,
                Err(e) => {
                    oplog.err("b3_maker_poll", &e.to_string(), serde_json::json!({ "signal_id": signal_id, "order_id": order_id }));
                    return B3Apply::Nothing { signal_id }; // retry next tick
                }
            };
            match b3_poll_decision(status, matched, orig, due) {
                PollDecision::Working | PollDecision::Waiting => B3Apply::Nothing { signal_id },
                PollDecision::Filled => {
                    let net = maker_net(orig, target, entry);
                    oplog.sys("b3_maker_filled", serde_json::json!({
                        "signal_id": signal_id, "token_id": token_id, "order_id": order_id,
                        "fill_price": target.to_string(), "shares": orig.to_string(),
                        "fee_paid": "0", "fee_model": "maker_floor", "net_pnl": net.to_string(),
                    }));
                    B3Apply::Close { signal_id, net_pnl: net, exit_price: target, kind: ExitKind::B3MakerFill }
                }
                PollDecision::Timeout => {
                    let (rstatus, rmatched, cancel_resp) = match backend.cancel_then_reconcile(&order_id).await {
                        Ok(v) => v,
                        Err(e) => {
                            oplog.err("b3_maker_cancel_reconcile", &e.to_string(), serde_json::json!({ "signal_id": signal_id, "order_id": order_id }));
                            return B3Apply::Nothing { signal_id }; // hold; retry
                        }
                    };
                    oplog.sys("b3_maker_timeout_cancel", serde_json::json!({
                        "signal_id": signal_id, "token_id": token_id, "order_id": order_id,
                        "exit_ts_s": exit_ts_s,
                        "size_matched_at_cancel": rmatched.to_string(), "cancel_resp": cancel_resp.as_str(),
                    }));
                    let recon = reconcile_decision(rstatus, orig, rmatched);
                    oplog.sys("b3_maker_reconcile", serde_json::json!({
                        "signal_id": signal_id, "token_id": token_id, "order_id": order_id,
                        "original_size": orig.to_string(), "size_matched": rmatched.to_string(),
                        "decision": recon.decision.as_str(),
                    }));
                    match b3_timeout_action(&recon) {
                        // THE PHASE-A LESSON: each verdict ACTED ON; none closes blind.
                        b3_maker::B3TimeoutAction::Defer => B3Apply::Nothing { signal_id }, // sells 0
                        b3_maker::B3TimeoutAction::FullMakerFill => {
                            let net = maker_net(orig, target, entry);
                            oplog.sys("b3_maker_filled", serde_json::json!({
                                "signal_id": signal_id, "token_id": token_id, "order_id": order_id,
                                "fill_price": target.to_string(), "shares": orig.to_string(),
                                "fee_paid": "0", "fee_model": "maker_floor", "net_pnl": net.to_string(),
                            }));
                            B3Apply::Close { signal_id, net_pnl: net, exit_price: target, kind: ExitKind::B3MakerFill }
                        }
                        b3_maker::B3TimeoutAction::FullFallback => {
                            match bid {
                                Some(b) => close_via_taker(backend, &token_id, orig, b, entry, &signal_id,
                                    ExitKind::B3Fallback, "clean_cancel", oplog).await,
                                None => {
                                    // No fresh bid to F2 -> sell NOTHING (hold, retry). The
                                    // maker is already cancelled; a stale single-token BBO
                                    // would otherwise be SILENT, so WARN for visibility (a
                                    // permanently dead feed trips the global feed-dead guard).
                                    oplog.sys("b3_maker_fallback_deferred_no_bid", serde_json::json!({
                                        "signal_id": signal_id, "token_id": token_id, "reason": "full_fallback",
                                    }));
                                    B3Apply::Nothing { signal_id }
                                }
                            }
                        }
                        b3_maker::B3TimeoutAction::Partial { maker_filled, fallback_size } => {
                            let b = match bid {
                                Some(b) => b,
                                None => {
                                    oplog.sys("b3_maker_fallback_deferred_no_bid", serde_json::json!({
                                        "signal_id": signal_id, "token_id": token_id, "reason": "partial_remainder",
                                    }));
                                    return B3Apply::Nothing { signal_id };
                                }
                            };
                            // Sell ONLY the unfilled remainder via F2 (taker).
                            let taking = match backend.taker_sell(&signal_id, &token_id, fallback_size, b).await {
                                Ok(t) => t,
                                Err(e) => { oplog.err("b3_maker_f2", &e.to_string(), serde_json::json!({ "signal_id": signal_id })); return B3Apply::Nothing { signal_id }; }
                            };
                            let fallback_fill = if fallback_size > Decimal::ZERO { taking / fallback_size } else { b };
                            let sc = split_close(orig, maker_filled, target, fallback_fill, entry, buy_fee_of(orig, entry));
                            let f2_fee = taker_fee_dec(fallback_size, fallback_fill);
                            oplog.sys("b3_maker_partial_fill", serde_json::json!({
                                "signal_id": signal_id, "token_id": token_id, "order_id": order_id,
                                "original_size": orig.to_string(), "size_matched": maker_filled.to_string(),
                                "size_remaining": fallback_size.to_string(), "fill_price": target.to_string(),
                            }));
                            oplog.sys("b3_maker_fallback_f2", serde_json::json!({
                                "signal_id": signal_id, "token_id": token_id,
                                "fallback_shares": fallback_size.to_string(), "f2_price": fallback_fill.to_string(),
                                "f2_fee": f2_fee.to_string(), "reason": "partial_remainder",
                                "net_pnl": (sc.maker_pnl + sc.fallback_pnl).to_string(),
                            }));
                            B3Apply::CloseSplit { signal_id, maker_net: sc.maker_pnl, fallback_net: sc.fallback_pnl, fallback_fill }
                        }
                    }
                }
            }
        }
    }
}

/// The maker-fill net P&L: sell `shares` at `target` (maker floor fee 0) vs the
/// BUY cost (entry + the BUY taker fee).
fn maker_net(shares: Decimal, target: Decimal, entry: Decimal) -> Decimal {
    shares * target - (shares * entry + buy_fee_of(shares, entry))
}

/// Close the whole lot via a real F2 FOK taker SELL at `bid`. Emits fallback_f2.
async fn close_via_taker<B: MakerBackend + ?Sized>(
    backend: &B, token_id: &str, shares: Decimal, bid: Decimal, entry: Decimal,
    signal_id: &str, kind: ExitKind, reason: &str, oplog: &OpLog,
) -> B3Apply {
    let taking = match backend.taker_sell(signal_id, token_id, shares, bid).await {
        Ok(t) => t,
        Err(e) => {
            oplog.err("b3_maker_taker_sell", &e.to_string(), serde_json::json!({ "signal_id": signal_id }));
            return B3Apply::Nothing { signal_id: signal_id.to_string() };
        }
    };
    let fill = if shares > Decimal::ZERO { taking / shares } else { bid };
    let net = taking - (shares * entry + buy_fee_of(shares, entry));
    if kind == ExitKind::B3MakerFill {
        oplog.sys("b3_maker_rejected_crossable", serde_json::json!({
            "signal_id": signal_id, "token_id": token_id, "action_taken": "taker_capture",
            "bid": bid.to_string(), "net_pnl": net.to_string(),
        }));
    } else {
        oplog.sys("b3_maker_fallback_f2", serde_json::json!({
            "signal_id": signal_id, "token_id": token_id, "fallback_shares": shares.to_string(),
            "f2_price": fill.to_string(), "f2_fee": taker_fee_dec(shares, fill).to_string(),
            "reason": reason, "net_pnl": net.to_string(),
        }));
    }
    B3Apply::Close { signal_id: signal_id.to_string(), net_pnl: net, exit_price: fill, kind }
}

// ============================================================================
// PHASE 3 — apply (PURE, under the lock, no await). Lot found by signal_id.
// ============================================================================

fn apply_b3_results(
    positions: &mut Vec<OpenPosition>,
    applies: Vec<B3Apply>,
    now_ms: i64,
) -> Vec<ClosedTrade> {
    let mut closed = Vec::new();
    let mut remove: HashSet<String> = HashSet::new();
    for a in applies {
        match a {
            B3Apply::SetMaker { signal_id, order_id, target } => {
                if let Some(p) = positions.iter_mut().find(|p| p.signal_id == signal_id) {
                    p.maker_exit = Some(MakerExitState {
                        order_id, placed_ms: now_ms, target: target.to_f64().unwrap_or(0.0),
                        status: MakerExitStatus::Placed,
                    });
                }
            }
            B3Apply::Close { signal_id, net_pnl, exit_price, kind } => {
                if let Some(p) = positions.iter().find(|p| p.signal_id == signal_id) {
                    closed.push(closed_trade(p, exit_price, p.shares, net_pnl, kind, now_ms));
                    remove.insert(signal_id);
                }
            }
            B3Apply::CloseSplit { signal_id, maker_net, fallback_net, fallback_fill } => {
                if let Some(p) = positions.iter().find(|p| p.signal_id == signal_id) {
                    // One lot, closed in two portions; the guard/pnl sees the total.
                    closed.push(closed_trade(p, fallback_fill, p.shares, maker_net + fallback_net,
                        ExitKind::B3Fallback, now_ms));
                    remove.insert(signal_id);
                }
            }
            B3Apply::Nothing { .. } => {}
        }
    }
    positions.retain(|p| !remove.contains(&p.signal_id));
    closed
}

fn closed_trade(p: &OpenPosition, exit_price: Decimal, shares: Decimal, net: Decimal, kind: ExitKind, now_ms: i64) -> ClosedTrade {
    ClosedTrade {
        token_id: p.token_id.clone(), side: p.side, interval: p.interval.clone(),
        signal_id: p.signal_id.clone(), entry_price: p.entry_price, exit_price, shares,
        realized_pnl: net, exit_kind: kind, opened_at_ms: p.opened_at_ms, closed_at_ms: now_ms,
    }
}

// ============================================================================
// THE 3-PHASE ORCHESTRATION (the concurrency test pins this compiles)
// ============================================================================

/// lock -> snapshot/collect (drop lock) -> await SDK per action (NO lock) ->
/// lock -> apply. The `std::sync::Mutex` guard is dropped before any `.await`;
/// if an await crossed the lock this would NOT compile (guard is not `Send`).
pub async fn process_b3_makers_live<B: MakerBackend + ?Sized>(
    positions: &Mutex<Vec<OpenPosition>>,
    cfg: &ExitsConfig,
    bbo: &DashMap<String, EventBbo>,
    now_ms: i64,
    backend: &B,
    oplog: &OpLog,
) -> Vec<ClosedTrade> {
    // PHASE 1 — lock -> snapshot.
    let actions = {
        let guard = positions.lock().expect("positions lock");
        collect_b3_actions(&guard, cfg, bbo, now_ms)
    }; // guard DROPPED here, before any await.
    if actions.is_empty() {
        return Vec::new();
    }
    // PHASE 2 — await SDK (NO lock).
    let mut applies = Vec::with_capacity(actions.len());
    for action in actions {
        applies.push(execute_b3_action(backend, action, oplog).await);
    }
    // PHASE 3 — lock -> apply.
    let mut guard = positions.lock().expect("positions lock");
    apply_b3_results(&mut guard, applies, now_ms)
}

// ============================================================================
// RECONCILE-ON-STARTUP — orphan-maker recovery (capital safety)
// ============================================================================
//
// An orphan maker (a real SELL resting on the book the bot is not tracking) is
// uncontrolled capital. Two dangers: (a) it could FILL untracked; (b) it may
// have ALREADY filled while the bot was down -> the shares are gone but our
// state shows the lot open -> re-placing a maker SELL would OVERSELL.
//
// Runs at BOOT, AFTER the (synchronous) state load, BEFORE the exit task posts
// any new maker -- so old orphans never mix with new makers. Runs in live mode
// (REST up) regardless of LIVE_ARMED: cancelling is risk-REDUCING.
//
// CORRELATION: the client_order_id is LOCAL (the CLOB never sees it), so we
// cannot read it off an orphan. Instead we recompute each position's maker coid
// and look it up in the write-ahead intent log, which recorded Submitted{order_
// id} after the POST -> coid -> order_id -> position. INEQUÍVOCO (unique coid
// per lot). Only the narrow window (POST sent, Submitted not yet written: a
// Pending with no order_id) falls to param-match (token+price); ambiguity there
// -> CANCEL (never guess the lot). cancel_all is NOT enough: a Matched order
// can't be cancelled, and forgetting its fill would oversell.

/// The deterministic maker `client_order_id` for a position -- the contract the
/// (future) real backend and this reconcile MUST share to bridge coid<->order.
#[must_use]
pub fn maker_coid(signal_id: &str, target: Decimal) -> String {
    format!("{signal_id}-maker-{target}")
}

/// A position as the reconcile sees it (resolved against the persisted state +
/// the intent log; the coid<->order_id bridge is done by the caller).
#[derive(Debug, Clone, PartialEq)]
pub struct ReconPos {
    pub signal_id: String,
    pub token_id: String,
    pub shares: Decimal,
    pub entry: Decimal,
    pub target: Decimal,                     // the maker target (entry+delta snapped)
    pub persisted_order_id: Option<String>,  // maker_exit.order_id (state v5)
    pub submitted_order_id: Option<String>,  // intent log Submitted{order_id} (the coid bridge)
    pub has_pending_intent: bool,            // intent log Pending (no order_id) for the maker coid
}

/// What the reconcile decides per position / per orphan order. PURE (the I/O
/// wrapper applies them via the backend).
#[derive(Debug, Clone, PartialEq)]
pub enum ReconItem {
    /// Adopt a resting maker (set maker_exit); `size_matched` already partial-ok.
    Adopt { signal_id: String, order_id: String },
    /// The order FILLED while the bot was down -> close the lot at the fill (the
    /// shares are gone; do NOT re-sell -> no oversell post-crash).
    CloseFillWhileDown { signal_id: String, order_id: String, size_matched: Decimal },
    /// The order is gone with NO fill (cancelled while down) -> clear maker_exit
    /// so the lot re-places a fresh maker.
    ClearMaker { signal_id: String },
    /// Cancel an orphan order (no correlatable position, or ambiguous).
    CancelOrphan { order_id: String },
    /// Capital-uncertain (ambiguous orphan that partially filled, or a terminal
    /// partial fill-while-down) -> HALT these lots + escalate. Abstain.
    Halt { signal_ids: Vec<String>, reason: String },
}

/// Decide what to do with a PERSISTED maker (one with maker_exit.order_id),
/// given the AUTHORITATIVE `order(id)` poll. PURE.
#[must_use]
pub fn persisted_maker_decision(
    signal_id: &str, order_id: &str,
    status: MakerOrderStatus, size_matched: Decimal, orig: Decimal,
) -> ReconItem {
    let matched = size_matched.clamp(Decimal::ZERO, orig.max(Decimal::ZERO));
    if orig > Decimal::ZERO && matched >= orig {
        // Fully filled (while down or just now) -> close at the fill. No re-sell.
        return ReconItem::CloseFillWhileDown { signal_id: signal_id.into(), order_id: order_id.into(), size_matched: matched };
    }
    if !status.is_terminal() {
        // Still resting (Live/Delayed) -> adopt + let the lifecycle manage it.
        return ReconItem::Adopt { signal_id: signal_id.into(), order_id: order_id.into() };
    }
    if matched == Decimal::ZERO {
        // Terminal + zero fill (cancelled/expired while down) -> clear, re-place.
        return ReconItem::ClearMaker { signal_id: signal_id.into() };
    }
    // Terminal + PARTIAL fill while down: some shares sold, some not, no clean
    // resume -> capital-uncertain -> HALT (abstain, escalate to a human).
    ReconItem::Halt {
        signal_ids: vec![signal_id.into()],
        reason: format!("terminal partial fill-while-down: matched {matched}/{orig}"),
    }
}

/// Decide the ORPHAN sweep over the open orders not already owned by a position
/// (via persisted_order_id or submitted_order_id). PURE. `positions` are the
/// candidate owners (those with a pending intent for the narrow no-order_id
/// window). Strong layer (id ∈ submitted) handled by the caller as `Adopt`.
#[must_use]
pub fn orphan_decisions(open_orders: &[OpenOrderRow], positions: &[ReconPos]) -> Vec<ReconItem> {
    let mut out = Vec::new();
    let expected: HashSet<&str> = positions.iter()
        .filter_map(|p| p.persisted_order_id.as_deref())
        .chain(positions.iter().filter_map(|p| p.submitted_order_id.as_deref()))
        .collect();
    for ord in open_orders {
        if expected.contains(ord.order_id.as_str()) {
            continue; // known order -> adopted/resumed elsewhere, NOT an orphan.
        }
        // Param-match against pending positions (token + SELL + price). The
        // ONLY candidates are positions with NEITHER a persisted NOR a submitted
        // order_id (the narrow POST-sent-but-Submitted-not-yet-written window);
        // a submitted one is already in `expected` and skipped above, but we
        // exclude it here too to make the intent explicit.
        let matches: Vec<&ReconPos> = positions.iter()
            .filter(|p| p.has_pending_intent
                && p.persisted_order_id.is_none() && p.submitted_order_id.is_none()
                && p.token_id == ord.token_id && p.target == ord.price)
            .collect();
        if matches.len() == 1 && open_matches(open_orders, &expected, &ord.token_id, ord.price) == 1 {
            // Unambiguous: exactly one pending position AND one orphan order for
            // this (token, price) -> adopt the link.
            out.push(ReconItem::Adopt { signal_id: matches[0].signal_id.clone(), order_id: ord.order_id.clone() });
        } else {
            // Ambiguous or no owner -> cancel. If it partially filled, we cannot
            // forget the sold shares -> HALT the candidate lots too.
            out.push(ReconItem::CancelOrphan { order_id: ord.order_id.clone() });
            if ord.size_matched > Decimal::ZERO && !matches.is_empty() {
                out.push(ReconItem::Halt {
                    signal_ids: matches.iter().map(|p| p.signal_id.clone()).collect(),
                    reason: format!("ambiguous orphan partially filled: matched {}", ord.size_matched),
                });
            }
        }
    }
    out
}

/// Count orphan (unexpected) orders for a (token, price).
fn open_matches(open: &[OpenOrderRow], expected: &HashSet<&str>, token: &str, price: Decimal) -> usize {
    open.iter().filter(|o| !expected.contains(o.order_id.as_str()) && o.token_id == token && o.price == price).count()
}

/// Outcome of the reconcile (the exit task consumes `halted` + `closed`).
#[derive(Debug, Default, Clone, PartialEq)]
pub struct ReconcileReport {
    pub closed: Vec<ClosedTrade>,
    pub halted: HashSet<String>,
    pub adopted: usize,
    pub cancelled: usize,
}

/// RECONCILE-ON-STARTUP (the I/O wrapper). `recon_pos` is the snapshot the
/// caller builds from the persisted positions + the write-ahead intent log
/// (recomputing each maker coid -> `maker_coid` -> intent lookup to fill
/// `submitted_order_id`). The SDK calls (poll/list/cancel) go through the
/// backend (mock in tests). `await`s happen WITHOUT the positions lock; the
/// state mutations are applied under the lock at the end (no await).
///
/// CALLER CONTRACT (the final wiring step builds `recon_pos`): for each
/// position, recompute its maker coid (`maker_coid(signal_id, target)`) and look
/// it up in the intent log; fill `submitted_order_id` from a `Submitted{order_id}`
/// record, `has_pending_intent` from a `Pending` record. This coid bridge is
/// what makes the match INEQUÍVOCO. If the lookup is incomplete the reconcile is
/// STILL SAFE (it falls to param-match and HALTS on ambiguity, never guesses),
/// but a recoverable order could be needlessly cancelled — so the builder must
/// be correct for the bridge to do its job.
pub async fn reconcile_makers_on_startup<B: MakerBackend + ?Sized>(
    positions: &Mutex<Vec<OpenPosition>>,
    recon_pos: &[ReconPos],
    backend: &B,
    oplog: &OpLog,
    now_ms: i64,
) -> ReconcileReport {
    let mut report = ReconcileReport::default();
    let mut state_items: Vec<ReconItem> = Vec::new();

    // --- KNOWN makers (persisted OR submitted order_id): poll each (the truth). ---
    for p in recon_pos {
        let oid = match p.persisted_order_id.as_deref().or(p.submitted_order_id.as_deref()) {
            Some(o) => o,
            None => continue, // no maker order -> candidate for the orphan param-match.
        };
        match backend.poll_order(oid).await {
            Ok((status, matched)) => {
                state_items.push(persisted_maker_decision(&p.signal_id, oid, status, matched, p.shares));
            }
            Err(e) => {
                // Couldn't reconcile this order -> HALT it (don't guess), escalate.
                oplog.err("reconcile_poll", &e.to_string(), serde_json::json!({ "signal_id": p.signal_id, "order_id": oid }));
                state_items.push(ReconItem::Halt { signal_ids: vec![p.signal_id.clone()], reason: "poll failed at startup".into() });
            }
        }
    }

    // --- ORPHAN sweep: list open orders, decide, CANCEL the orphans (await). ---
    match backend.list_open_orders().await {
        Ok(open) => {
            for item in orphan_decisions(&open, recon_pos) {
                match item {
                    ReconItem::CancelOrphan { order_id } => {
                        match backend.cancel(&order_id).await {
                            Ok(resp) => {
                                report.cancelled += 1;
                                oplog.sys("reconcile_orphan_cancelled", serde_json::json!({
                                    "order_id": order_id, "cancel_resp": resp.as_str(),
                                }));
                            }
                            Err(e) => oplog.err("reconcile_cancel", &e.to_string(), serde_json::json!({ "order_id": order_id })),
                        }
                    }
                    other => state_items.push(other), // Adopt / Halt -> phase 3.
                }
            }
        }
        Err(e) => oplog.err("reconcile_list_open_orders", &e.to_string(), serde_json::json!({})),
    }

    // --- APPLY state mutations under the lock (NO await). ---
    let mut guard = positions.lock().expect("positions lock");
    let mut remove: HashSet<String> = HashSet::new();
    for item in state_items {
        match item {
            ReconItem::Adopt { signal_id, order_id } => {
                if let Some(p) = guard.iter_mut().find(|p| p.signal_id == signal_id) {
                    // Re-seed maker_exit (idempotent if already set). The lifecycle
                    // re-polls the real size_matched next tick.
                    let target = p.maker_exit.as_ref().map(|m| m.target)
                        .unwrap_or_else(|| recon_pos.iter().find(|r| r.signal_id == signal_id).map(|r| r.target.to_f64().unwrap_or(0.0)).unwrap_or(0.0));
                    p.maker_exit = Some(MakerExitState { order_id, placed_ms: now_ms, target, status: MakerExitStatus::Placed });
                    report.adopted += 1;
                }
            }
            ReconItem::CloseFillWhileDown { signal_id, size_matched, .. } => {
                if let Some(p) = guard.iter().find(|p| p.signal_id == signal_id) {
                    // The maker fully filled while down: close at target (maker fee
                    // 0). The shares are GONE -> record the close, NEVER re-sell.
                    let target = p.maker_exit.as_ref().map(|m| Decimal::try_from(m.target).unwrap_or(p.entry_price))
                        .or_else(|| recon_pos.iter().find(|r| r.signal_id == signal_id).map(|r| r.target))
                        .unwrap_or(p.entry_price);
                    let net = maker_net(size_matched, target, p.entry_price);
                    oplog.sys("b3_maker_filled", serde_json::json!({
                        "signal_id": signal_id, "token_id": p.token_id, "fill_price": target.to_string(),
                        "shares": size_matched.to_string(), "fee_paid": "0", "fee_model": "maker_floor",
                        "net_pnl": net.to_string(), "reason": "fill_while_down",
                    }));
                    report.closed.push(closed_trade(p, target, size_matched, net, ExitKind::B3MakerFill, now_ms));
                    remove.insert(signal_id);
                }
            }
            ReconItem::ClearMaker { signal_id } => {
                if let Some(p) = guard.iter_mut().find(|p| p.signal_id == signal_id) {
                    p.maker_exit = None; // order gone with no fill -> re-place fresh.
                }
            }
            ReconItem::Halt { signal_ids, reason } => {
                for s in &signal_ids { report.halted.insert(s.clone()); }
                oplog.err("reconcile_halt", &reason, serde_json::json!({ "signal_ids": signal_ids }));
            }
            ReconItem::CancelOrphan { .. } => {} // already executed in phase 2.
        }
    }
    guard.retain(|p| !remove.contains(&p.signal_id));
    report
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use std::collections::HashMap;
    use std::sync::Mutex as StdMutex;

    // ---- a scripted MOCK backend (no network) ----
    #[derive(Default)]
    struct MockBackend {
        place: HashMap<String, PostOutcome>,            // token -> outcome
        poll: HashMap<String, (MakerOrderStatus, Decimal)>, // order_id -> (status, matched)
        recon: HashMap<String, (MakerOrderStatus, Decimal, CancelResp)>,
        taker_taking: Decimal,                          // taking_amount returned by taker_sell
        open_orders: Vec<OpenOrderRow>,                 // list_open_orders
        cancelled: std::sync::Mutex<Vec<String>>,       // order_ids cancel() was called on
    }
    #[async_trait]
    impl MakerBackend for MockBackend {
        async fn place_maker(&self, _signal_id: &str, token_id: &str, _s: Decimal, _t: Decimal) -> anyhow::Result<PostOutcome> {
            Ok(self.place.get(token_id).cloned().unwrap_or(PostOutcome::Posted { order_id: format!("oid-{token_id}") }))
        }
        async fn poll_order(&self, order_id: &str) -> anyhow::Result<(MakerOrderStatus, Decimal)> {
            Ok(self.poll.get(order_id).cloned().unwrap_or((MakerOrderStatus::Live, dec!(0))))
        }
        async fn cancel_then_reconcile(&self, order_id: &str) -> anyhow::Result<(MakerOrderStatus, Decimal, CancelResp)> {
            Ok(self.recon.get(order_id).cloned().unwrap_or((MakerOrderStatus::Cancelled, dec!(0), CancelResp::Canceled)))
        }
        async fn taker_sell(&self, _signal_id: &str, _t: &str, _s: Decimal, _b: Decimal) -> anyhow::Result<Decimal> {
            Ok(self.taker_taking)
        }
        async fn list_open_orders(&self) -> anyhow::Result<Vec<OpenOrderRow>> {
            Ok(self.open_orders.clone())
        }
        async fn cancel(&self, order_id: &str) -> anyhow::Result<CancelResp> {
            self.cancelled.lock().unwrap().push(order_id.to_string());
            Ok(CancelResp::Canceled)
        }
    }

    fn oplog() -> OpLog {
        OpLog::new(std::env::temp_dir().join(format!("rb_b3live_{}.jsonl", std::process::id())))
    }
    fn cfg() -> ExitsConfig {
        let mut c = ExitsConfig::default();
        c.cells.insert("ETH:5m".into(), ExitRule::B3Maker { delta: 0.10, fallback: crate::config::B3Fallback::F2Market });
        c
    }
    fn bbo_with(token: &str, bid: f64, now: i64) -> DashMap<String, EventBbo> {
        let m = DashMap::new();
        m.insert(token.into(), EventBbo { best_bid: Some(bid), best_ask: Some(bid + 0.02), ts_ms: now });
        m
    }
    fn pos(token: &str, entry: f64, t0: i64, maker: Option<MakerExitState>) -> OpenPosition {
        use crate::state::persist::{Outcome, OrderStatus, OpenPosition};
        OpenPosition {
            token_id: token.into(), asset: "ETH".into(), side: Outcome::Up,
            entry_price: Decimal::try_from(entry).unwrap(), shares: dec!(10.0),
            opened_at_ms: t0, signal_id: format!("{token}-sig"), interval: "5m".into(),
            exit_ts_s: t0 / 1000 + 120, entry_ts_ms: t0,
            running_max_bid: entry, ts_max_bid_ms: t0, status: OrderStatus::Confirmed,
            order_id: Some("o".into()), ack_at_ms: Some(t0), confirmed_at_ms: Some(t0),
            confirmation_source: None, maker_exit: maker,
        }
    }
    fn placed(order_id: &str, target: f64, t0: i64) -> MakerExitState {
        MakerExitState { order_id: order_id.into(), placed_ms: t0, target, status: MakerExitStatus::Placed }
    }
    /// Like `pos` but with an explicit signal_id (for two lots on the same token).
    fn pos_named(signal_id: &str, token: &str, entry: f64, t0: i64, maker: Option<MakerExitState>) -> OpenPosition {
        let mut p = pos(token, entry, t0, maker);
        p.signal_id = signal_id.into();
        p
    }

    const T0: i64 = 1_780_000_000_000;

    #[tokio::test]
    async fn lifecycle_place_then_fill_complete() {
        // tick 1: no maker -> place. tick 2: poll -> Filled -> close at target.
        let positions = StdMutex::new(vec![pos("T", 0.55, T0, None)]);
        let mut mock = MockBackend::default();
        let now1 = T0 + 1_000;
        let bbo = bbo_with("T", 0.55, now1);
        let lg = oplog();
        // tick 1: place (mock default -> Posted oid-T).
        let c1 = process_b3_makers_live(&positions, &cfg(), &bbo, now1, &mock, &lg).await;
        assert!(c1.is_empty());
        assert!(positions.lock().unwrap()[0].maker_exit.is_some());
        let oid = positions.lock().unwrap()[0].maker_exit.as_ref().unwrap().order_id.clone();
        // tick 2: poll -> full fill.
        mock.poll.insert(oid, (MakerOrderStatus::Matched, dec!(10.0)));
        let now2 = T0 + 2_000;
        let bbo2 = bbo_with("T", 0.66, now2);
        let c2 = process_b3_makers_live(&positions, &cfg(), &bbo2, now2, &mock, &lg).await;
        assert_eq!(c2.len(), 1, "full fill closes the lot");
        assert_eq!(c2[0].exit_kind, ExitKind::B3MakerFill);
        assert_eq!(c2[0].exit_price, dec!(0.65), "fills at the target (maker)");
        // net = 10*0.65 - (10*0.55 + taker(10,0.55)) = 6.5 - 5.5 - 0.17325 = 0.82675
        assert_eq!(c2[0].realized_pnl, dec!(0.82675));
        assert!(positions.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn lifecycle_timeout_clean_cancel_fallback() {
        // maker placed; at timeout, reconcile = clean cancel -> F2 whole lot.
        let mut positions_v = vec![pos("T", 0.55, T0, Some(placed("oid-T", 0.65, T0)))];
        positions_v[0].exit_ts_s = T0 / 1000; // already due
        let positions = StdMutex::new(positions_v);
        let mut mock = MockBackend::default();
        mock.poll.insert("oid-T".into(), (MakerOrderStatus::Live, dec!(0))); // not filled
        mock.recon.insert("oid-T".into(), (MakerOrderStatus::Cancelled, dec!(0), CancelResp::Canceled));
        mock.taker_taking = dec!(5.40); // 10 sh * 0.54 bid
        let now = T0 + 120_000;
        let bbo = bbo_with("T", 0.54, now);
        let c = process_b3_makers_live(&positions, &cfg(), &bbo, now, &mock, &oplog()).await;
        assert_eq!(c.len(), 1, "timeout closes via F2 fallback");
        assert_eq!(c[0].exit_kind, ExitKind::B3Fallback);
        // net = 5.40 - (10*0.55 + taker(10,0.55)) = 5.40 - 5.5 - 0.17325 = -0.27325
        assert_eq!(c[0].realized_pnl, dec!(-0.27325));
        assert!(positions.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn timeout_no_fresh_bid_defers_and_warns_no_sell() {
        // placed maker, due, but the BBO is STALE -> can't F2 -> sells NOTHING +
        // emits the no-bid WARNING (visibility, not silent), position kept.
        let mut positions_v = vec![pos("T", 0.55, T0, Some(placed("oid-T", 0.65, T0)))];
        positions_v[0].exit_ts_s = T0 / 1000;
        let positions = StdMutex::new(positions_v);
        let mut mock = MockBackend::default();
        mock.poll.insert("oid-T".into(), (MakerOrderStatus::Live, dec!(0)));
        mock.recon.insert("oid-T".into(), (MakerOrderStatus::Cancelled, dec!(0), CancelResp::Canceled));
        mock.taker_taking = dec!(999); // must NOT fire -> if it did, a Close would appear
        let now = T0 + 120_000;
        let bbo = DashMap::new(); // STALE: ts older than STALE_BBO_MS -> fresh_bid None
        bbo.insert("T".into(), EventBbo { best_bid: Some(0.54), best_ask: Some(0.56), ts_ms: now - STALE_BBO_MS - 1 });
        let path = std::env::temp_dir().join(format!("rb_b3live_nobid_{}.jsonl", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let lg = OpLog::new(&path);
        let c = process_b3_makers_live(&positions, &cfg(), &bbo, now, &mock, &lg).await;
        assert!(c.is_empty(), "no fresh bid at timeout -> sells NOTHING");
        assert_eq!(positions.lock().unwrap().len(), 1, "position kept for retry");
        let body = std::fs::read_to_string(&path).unwrap_or_default();
        assert!(body.contains("b3_maker_fallback_deferred_no_bid"), "must WARN, not be silent");
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn lifecycle_partial_split_sums_to_orig() {
        // at timeout, reconcile = partial 3/10 -> split: 3 maker + 7 F2.
        let mut positions_v = vec![pos("T", 0.55, T0, Some(placed("oid-T", 0.65, T0)))];
        positions_v[0].exit_ts_s = T0 / 1000;
        let positions = StdMutex::new(positions_v);
        let mut mock = MockBackend::default();
        mock.poll.insert("oid-T".into(), (MakerOrderStatus::Live, dec!(0)));
        mock.recon.insert("oid-T".into(), (MakerOrderStatus::Cancelled, dec!(3.0), CancelResp::Canceled));
        mock.taker_taking = dec!(3.78); // 7 sh * 0.54
        let now = T0 + 120_000;
        let bbo = bbo_with("T", 0.54, now);
        let c = process_b3_makers_live(&positions, &cfg(), &bbo, now, &mock, &oplog()).await;
        assert_eq!(c.len(), 1, "partial split closes the lot once (total)");
        assert_eq!(c[0].exit_kind, ExitKind::B3Fallback);
        assert_eq!(c[0].shares, dec!(10.0), "whole lot accounted (3 maker + 7 fallback)");
        assert!(positions.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn lifecycle_ambiguous_defers_sells_nothing() {
        // at timeout, the cancel did NOT confirm (status Live) + partial matched
        // -> reconcile defers -> NOTHING sold (the anti-oversell, end to end).
        let mut positions_v = vec![pos("T", 0.55, T0, Some(placed("oid-T", 0.65, T0)))];
        positions_v[0].exit_ts_s = T0 / 1000;
        let positions = StdMutex::new(positions_v);
        let mut mock = MockBackend::default();
        mock.poll.insert("oid-T".into(), (MakerOrderStatus::Live, dec!(3.0))); // partial, due -> Timeout
        mock.recon.insert("oid-T".into(), (MakerOrderStatus::Live, dec!(3.0), CancelResp::NotCanceled)); // NON-terminal
        mock.taker_taking = dec!(999); // if a taker_sell fired, the close would show it -> must NOT fire
        let now = T0 + 120_000;
        let bbo = bbo_with("T", 0.54, now);
        let c = process_b3_makers_live(&positions, &cfg(), &bbo, now, &mock, &oplog()).await;
        assert!(c.is_empty(), "ambiguous -> sells NOTHING, no close");
        assert_eq!(positions.lock().unwrap().len(), 1, "position kept (retry next tick)");
        assert!(positions.lock().unwrap()[0].maker_exit.is_some(), "maker still tracked");
    }

    #[tokio::test]
    async fn apply_finds_by_signal_id_not_index() {
        // A position is REMOVED between phase 1 (collect) and phase 3 (apply) by
        // another path -> apply must find by signal_id and SKIP the gone one,
        // not blow up on a stale index. We simulate by applying a Close for a
        // signal_id that is no longer present.
        let mut positions = vec![pos("A", 0.55, T0, None), pos("B", 0.55, T0, None)];
        // apply a Close for "A-sig" + a Close for a GONE "Z-sig".
        let applies = vec![
            B3Apply::Close { signal_id: "A-sig".into(), net_pnl: dec!(1.0), exit_price: dec!(0.65), kind: ExitKind::B3MakerFill },
            B3Apply::Close { signal_id: "Z-sig".into(), net_pnl: dec!(1.0), exit_price: dec!(0.65), kind: ExitKind::B3MakerFill },
        ];
        let closed = apply_b3_results(&mut positions, applies, T0 + 1);
        assert_eq!(closed.len(), 1, "only the present signal_id closed; the gone one skipped");
        assert_eq!(closed[0].signal_id, "A-sig");
        assert_eq!(positions.len(), 1, "A removed, B kept, Z was never there");
        assert_eq!(positions[0].signal_id, "B-sig");
    }

    #[test]
    fn no_oversell_invariant_wiring() {
        // The split's invariant, asserted on the APPLY path: a CloseSplit closes
        // exactly the lot's shares once (the whole lot), never more. (The pure
        // split_close property test pins maker+fallback==orig; here we confirm
        // the apply records the whole lot once and removes it.)
        let mut positions = vec![pos("T", 0.55, T0, None)];
        let applies = vec![B3Apply::CloseSplit {
            signal_id: "T-sig".into(), maker_net: dec!(0.3), fallback_net: dec!(-0.1), fallback_fill: dec!(0.54),
        }];
        let closed = apply_b3_results(&mut positions, applies, T0 + 1);
        assert_eq!(closed.len(), 1, "split closes the lot exactly once");
        assert_eq!(closed[0].shares, dec!(10.0), "the WHOLE lot (never more)");
        assert_eq!(closed[0].realized_pnl, dec!(0.2), "total = maker_net + fallback_net");
        assert!(positions.is_empty(), "lot removed once, no double-close");
    }

    // ======================= reconcile-on-startup =======================

    fn rp(signal_id: &str, token: &str, target: f64, persisted: Option<&str>, submitted: Option<&str>, pending: bool) -> ReconPos {
        ReconPos {
            signal_id: signal_id.into(), token_id: token.into(), shares: dec!(10.0),
            entry: dec!(0.55), target: Decimal::try_from(target).unwrap(),
            persisted_order_id: persisted.map(Into::into), submitted_order_id: submitted.map(Into::into),
            has_pending_intent: pending,
        }
    }
    fn ord(order_id: &str, token: &str, price: f64, orig: f64, matched: f64) -> OpenOrderRow {
        OpenOrderRow {
            order_id: order_id.into(), token_id: token.into(), is_sell: true,
            price: Decimal::try_from(price).unwrap(), original_size: Decimal::try_from(orig).unwrap(),
            size_matched: Decimal::try_from(matched).unwrap(),
        }
    }

    #[tokio::test]
    async fn reconcile_match_inequivoco_via_coid() {
        // Two positions, SAME token + target, both Submitted (distinct order_ids,
        // maker_exit None = the gap). Each order_id binds to ITS position via the
        // coid bridge (submitted_order_id) -> NOT crossed.
        let positions = StdMutex::new(vec![
            pos_named("A-sig", "T", 0.55, T0, None),
            pos_named("B-sig", "T", 0.55, T0, None),
        ]);
        let recon = vec![
            rp("A-sig", "T", 0.65, None, Some("oid-A"), false),
            rp("B-sig", "T", 0.65, None, Some("oid-B"), false),
        ];
        let mut mock = MockBackend::default();
        mock.poll.insert("oid-A".into(), (MakerOrderStatus::Live, dec!(0)));
        mock.poll.insert("oid-B".into(), (MakerOrderStatus::Live, dec!(0)));
        mock.open_orders = vec![ord("oid-A", "T", 0.65, 10.0, 0.0), ord("oid-B", "T", 0.65, 10.0, 0.0)];
        let r = reconcile_makers_on_startup(&positions, &recon, &mock, &oplog(), T0).await;
        assert_eq!(r.adopted, 2);
        assert_eq!(r.cancelled, 0, "submitted orders are expected -> NOT orphans");
        let g = positions.lock().unwrap();
        let a = g.iter().find(|p| p.signal_id == "A-sig").unwrap();
        let b = g.iter().find(|p| p.signal_id == "B-sig").unwrap();
        assert_eq!(a.maker_exit.as_ref().unwrap().order_id, "oid-A", "A bound to its own order");
        assert_eq!(b.maker_exit.as_ref().unwrap().order_id, "oid-B", "B bound to its own order (not crossed)");
    }

    #[tokio::test]
    async fn reconcile_ambiguous_pending_cancels_both() {
        // Two positions same token+target, Pending (NO order_id recorded), two
        // open orders same token+price -> param-match ambiguous -> cancel BOTH.
        let positions = StdMutex::new(vec![
            pos_named("A-sig", "T", 0.55, T0, None),
            pos_named("B-sig", "T", 0.55, T0, None),
        ]);
        let recon = vec![
            rp("A-sig", "T", 0.65, None, None, true),
            rp("B-sig", "T", 0.65, None, None, true),
        ];
        let mut mock = MockBackend::default();
        mock.open_orders = vec![ord("oid-X", "T", 0.65, 10.0, 0.0), ord("oid-Y", "T", 0.65, 10.0, 0.0)];
        let r = reconcile_makers_on_startup(&positions, &recon, &mock, &oplog(), T0).await;
        assert_eq!(r.cancelled, 2, "both ambiguous orphans cancelled");
        assert_eq!(r.adopted, 0, "neither adopted");
        let cancelled = mock.cancelled.lock().unwrap();
        assert!(cancelled.contains(&"oid-X".to_string()) && cancelled.contains(&"oid-Y".to_string()));
        let g = positions.lock().unwrap();
        assert!(g.iter().all(|p| p.maker_exit.is_none()), "no maker adopted to a guessed lot");
    }

    #[tokio::test]
    async fn reconcile_fill_while_down_closes_position() {
        // Persisted maker; poll says Matched/size_matched==orig (filled WHILE the
        // bot was down) -> close at the fill, NEVER re-sell (no oversell post-crash).
        let positions = StdMutex::new(vec![pos_named("A-sig", "T", 0.55, T0, Some(placed("oid-A", 0.65, T0)))]);
        let recon = vec![rp("A-sig", "T", 0.65, Some("oid-A"), None, false)];
        let mut mock = MockBackend::default();
        mock.poll.insert("oid-A".into(), (MakerOrderStatus::Matched, dec!(10.0)));
        mock.taker_taking = dec!(999); // must NOT fire -> no re-sell
        mock.open_orders = vec![]; // filled order is NOT in the open list
        let r = reconcile_makers_on_startup(&positions, &recon, &mock, &oplog(), T0).await;
        assert_eq!(r.closed.len(), 1, "fill-while-down closes the lot");
        assert_eq!(r.closed[0].exit_kind, ExitKind::B3MakerFill);
        assert_eq!(r.closed[0].exit_price, dec!(0.65));
        assert!(mock.cancelled.lock().unwrap().is_empty(), "nothing cancelled");
        assert!(positions.lock().unwrap().is_empty(), "lot removed (shares already gone)");
    }

    #[tokio::test]
    async fn reconcile_ambiguous_with_partial_halts() {
        // Ambiguous orphan that PARTIALLY filled (size_matched>0) -> cancel + HALT
        // the candidate lots (capital-uncertain -> abstain, escalate).
        let positions = StdMutex::new(vec![
            pos_named("A-sig", "T", 0.55, T0, None),
            pos_named("B-sig", "T", 0.55, T0, None),
        ]);
        let recon = vec![
            rp("A-sig", "T", 0.65, None, None, true),
            rp("B-sig", "T", 0.65, None, None, true),
        ];
        let mut mock = MockBackend::default();
        mock.open_orders = vec![ord("oid-X", "T", 0.65, 10.0, 3.0)]; // partially filled
        let r = reconcile_makers_on_startup(&positions, &recon, &mock, &oplog(), T0).await;
        assert_eq!(r.cancelled, 1, "the orphan is cancelled");
        assert!(r.halted.contains("A-sig") && r.halted.contains("B-sig"), "candidate lots HALTED (uncertain)");
    }

    #[tokio::test]
    async fn reconcile_does_not_cancel_loaded_legit() {
        // An open order whose id matches a LOADED position's persisted maker_exit
        // -> resumed/adopted, NOT cancelled (the load-then-reconcile ordering).
        let positions = StdMutex::new(vec![pos_named("A-sig", "T", 0.55, T0, Some(placed("oid-A", 0.65, T0)))]);
        let recon = vec![rp("A-sig", "T", 0.65, Some("oid-A"), None, false)];
        let mut mock = MockBackend::default();
        mock.poll.insert("oid-A".into(), (MakerOrderStatus::Live, dec!(0))); // still resting
        mock.open_orders = vec![ord("oid-A", "T", 0.65, 10.0, 0.0)]; // the legit order is open
        let r = reconcile_makers_on_startup(&positions, &recon, &mock, &oplog(), T0).await;
        assert_eq!(r.cancelled, 0, "a loaded position's order is NEVER cancelled");
        assert!(mock.cancelled.lock().unwrap().is_empty());
        assert!(positions.lock().unwrap()[0].maker_exit.is_some(), "still tracked (resumed)");
    }
}
