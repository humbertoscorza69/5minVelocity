//! Phase 6 D3.5 (Piece 6) -- LIVE-mode trading backend.
//!
//! The LAST zero-capital piece before D4. Wires the production trading loop
//! (decision/execution/exit/refresh) to actually POST real orders under
//! `--mode live`, instead of being a paper-only loop.
//!
//! Three protections layered on top of the path -- all of them MUST pass
//! before a real POST goes out:
//!   1. LIVE_ARMED gate (per-POST): without `LIVE_ARMED.txt` (non-empty), the
//!      backend REFUSES to POST and returns silently with an oplog refusal.
//!   2. max_trades_per_session: after N completed trades (BUY+SELL closed),
//!      the backend triggers a clean shutdown via the shared shutdown channel.
//!      D4 = 1; D5 may raise it.
//!   3. Idempotency (piece 1): every POST goes through `place_order_idempotent`
//!      with the production intent log -- a crash mid-POST cannot double-submit.
//!
//! ZERO capital in this module's tests. The tests cover the pure gate + the
//! counter logic; live_open / live_close (which require REST + signer) are
//! exercised via the gate path -- a refused gate never reaches the network.

#![allow(dead_code)] // wired by main.rs into the live trading-tasks branch (D4 arming)

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::Result;
use tokio::sync::watch;
use tracing::{info, warn};

use crate::decision::executor::{ClosedTrade, OpenIntent};
use crate::guards::live_armed;
use crate::oplog::OpLog;
use crate::rest::RestClient;
use crate::trading_loop::DecisionCtx;

/// Order #8 C: a BUY that filled more than $0.05 BETTER than the quoted ask — the
/// real book repriced against our side (stale-mirror / adverse-selection tell).
#[must_use]
pub fn favorable_fill_anomaly(
    quote: polymarket_client_sdk_v2::types::Decimal,
    fill: polymarket_client_sdk_v2::types::Decimal,
) -> bool {
    quote - fill > polymarket_client_sdk_v2::types::Decimal::new(5, 2)
}

#[cfg(test)]
mod order8_tests {
    use super::favorable_fill_anomaly;
    use polymarket_client_sdk_v2::types::Decimal;

    #[test]
    fn favorable_fill_anomaly_threshold() {
        // The phantom: quote 0.32, fill 0.0746 → 0.2454 improvement → anomaly.
        assert!(favorable_fill_anomaly(Decimal::new(32, 2), Decimal::new(746, 4)));
        // 2c improvement (0.32 → 0.30) is normal book movement → silent.
        assert!(!favorable_fill_anomaly(Decimal::new(32, 2), Decimal::new(30, 2)));
        // exactly 5c is not > 5c → silent.
        assert!(!favorable_fill_anomaly(Decimal::new(35, 2), Decimal::new(30, 2)));
        // an ADVERSE fill (paid worse) is never an anomaly here.
        assert!(!favorable_fill_anomaly(Decimal::new(30, 2), Decimal::new(35, 2)));
    }
}

/// The LIVE-mode execution backend. Carries everything the execution + exit
/// tasks need to POST a real order, plus the three safety gates.
///
/// F2 (fix bug #1): the gate counts INITIATED trades (BUY POSTed), not
/// completed ones. This prevents D4-style double-fire where a single Binance
/// trigger that matches multiple market intervals (5m + 15m) bypasses max=1
/// because the SELL hasn't completed yet. Shutdown fires when ALL initiated
/// trades have had their close ATTEMPT (Posted, Phantom, Mismatch, error --
/// any definitive outcome), so a stuck SELL never holds the session open.
pub struct LiveBackend {
    pub rest: Arc<RestClient>,
    pub pk: String,
    pub max_slippage: f64,
    /// Production write-ahead intent log (`data/live/order_intents.jsonl`).
    pub intent_log: PathBuf,
    /// Opt-in arming gate (defense vs accidental live capital): even with
    /// `--mode live`, no POST happens without this file present + non-empty.
    pub live_armed_path: PathBuf,
    /// Hard ceiling on TRADES INITIATED per session. D4 = 1; D5 may raise.
    /// usize::MAX = no cap.
    pub max_trades_per_session: usize,
    /// F2: BUYs successfully POSTed in this session. Gate uses this -- a second
    /// BUY attempt while `opened >= max_trades_per_session` is REFUSED.
    pub trades_opened: Arc<AtomicUsize>,
    /// F2: close ATTEMPTS finished in this session (Posted / Phantom / Mismatch
    /// / NoFill / error -- ANY definitive close outcome counts). When
    /// `closed >= opened` AND `opened >= max`, the shutdown signal fires.
    /// Decoupled from "SELL success" so a stuck SELL can't pin the session.
    pub trades_closed: Arc<AtomicUsize>,
    /// Shutdown signal -- set to true once the F2 condition above is met.
    pub shutdown_tx: Arc<watch::Sender<bool>>,
}

/// Per-POST gate verdict.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LivePostDecision {
    /// All gates passed -- a real POST may proceed.
    Allow,
    /// LIVE_ARMED.txt missing or empty -- the arming gate refuses the POST.
    RefuseDisarmed { armed_path: PathBuf },
    /// `max_trades_per_session` already reached -- session done; no more POSTs.
    RefuseMaxReached { completed: usize, max: usize },
}

impl LivePostDecision {
    #[must_use]
    pub fn is_allow(&self) -> bool {
        matches!(self, LivePostDecision::Allow)
    }
    #[must_use]
    pub fn reason(&self) -> String {
        match self {
            LivePostDecision::Allow => "allow".into(),
            LivePostDecision::RefuseDisarmed { armed_path } =>
                format!("LIVE_ARMED missing or empty ({})", armed_path.display()),
            LivePostDecision::RefuseMaxReached { completed, max } =>
                format!("max_trades_per_session reached ({completed}/{max})"),
        }
    }
}

/// PURE gate for OPEN (BUY) paths: combines LIVE_ARMED + max_trades_per_session.
/// F2: `opened` is the count of BUYs successfully POSTed -- NOT completed
/// trades. This is the fix for D4 bug #1 (a single Binance trigger matched
/// 5m + 15m markets; both BUYs slipped past max=1 because `completed` was 0
/// at both gate evals -- no SELL had run yet).
#[must_use]
pub fn gate(armed_path: &Path, opened: usize, max: usize) -> LivePostDecision {
    if !live_armed(armed_path) {
        return LivePostDecision::RefuseDisarmed { armed_path: armed_path.to_path_buf() };
    }
    if opened >= max {
        return LivePostDecision::RefuseMaxReached { completed: opened, max };
    }
    LivePostDecision::Allow
}

/// PURE gate for CLOSE (SELL) paths: LIVE_ARMED only. The SELL must NOT be
/// blocked by `max_trades_per_session` -- once a BUY is on the books, the
/// close is what releases the position; refusing it would lock capital until
/// market resolution (the D4 bug #2 failure mode -- different cause, same
/// destructive effect). The SELL still requires LIVE_ARMED -- a deliberate
/// disarm mid-session means "don't POST anything", and any unclosed position
/// gets handled by hold_recovery on restart.
#[must_use]
pub fn gate_close(armed_path: &Path) -> LivePostDecision {
    if !live_armed(armed_path) {
        return LivePostDecision::RefuseDisarmed { armed_path: armed_path.to_path_buf() };
    }
    LivePostDecision::Allow
}

/// F2: record a successfully POSTed BUY. Increments `trades_opened` so the
/// gate refuses additional opens once `max_trades_per_session` is hit.
/// Returns the new count. Does NOT signal shutdown -- shutdown waits until
/// all initiated trades have had their CLOSE attempt (see
/// `record_trade_closed`), so a stuck SELL can't pin the session open OR
/// shutdown the bot before the SELL has even tried.
pub fn record_trade_opened(opened: &Arc<AtomicUsize>) -> usize {
    opened.fetch_add(1, Ordering::SeqCst) + 1
}

/// F2: record that a close was ATTEMPTED for one position (ANY outcome --
/// Posted, Phantom, Mismatch, NoFill, or POST error). When `closed >= opened`
/// AND `opened >= max`, the shutdown signal fires. Returns `(new_closed,
/// signaled)`. This is the F2 fix for the "5h alive" symptom: the shutdown
/// no longer depends on a clean SELL Posted; it triggers once every initiated
/// trade has had its close decision settled, whatever the decision.
pub fn record_trade_closed(
    opened: &Arc<AtomicUsize>,
    closed: &Arc<AtomicUsize>,
    max: usize,
    shutdown_tx: &Arc<watch::Sender<bool>>,
) -> (usize, bool) {
    let new_closed = closed.fetch_add(1, Ordering::SeqCst) + 1;
    let opened_now = opened.load(Ordering::SeqCst);
    let signaled = opened_now >= max && new_closed >= opened_now;
    if signaled {
        let _ = shutdown_tx.send(true);
    }
    (new_closed, signaled)
}

/// True when the production trading-loop tasks should be spawned for this mode.
/// PIECE 6 expands this to cover BOTH paper and live (was: paper-only).
#[must_use]
pub fn trading_tasks_enabled(mode: &str) -> bool {
    mode == "paper" || mode == "live"
}

// ===================== Real-POST paths (gated) =====================

/// LIVE BUY POST. Gated: LIVE_ARMED + max_trades. Without either, returns
/// `Ok(None)` without touching the network. With both, calls
/// `place_order_idempotent` (shadow=false) -- a real order goes out.
///
/// F1 (fix bug #2): on `Posted`, returns `Some(taking_amount)` = the REAL
/// shares received on-chain. The caller (execution_task) overwrites the
/// position's `shares` with this value so the later SELL has the actual
/// fill (not the bot's pre-POST `stake/ask` estimate, which is wrong when
/// the book walks during the POST -- exactly the D4 trade A case:
/// computed 1.9811320754716981 but real fill 1.693547 due to slippage).
pub async fn live_open(
    lb: &Arc<LiveBackend>,
    intent: &OpenIntent,
    ctx: &DecisionCtx,
    oplog: &OpLog,
) -> Result<Option<(polymarket_client_sdk_v2::types::Decimal, polymarket_client_sdk_v2::types::Decimal)>> {
    // Returns Some((real_shares, real_usdc_spent)) on a Posted fill so the caller
    // can set the position's cost basis to the ACTUAL fill (shares AND price),
    // not the pre-POST estimate. Both are needed: overwriting shares alone while
    // leaving entry_price at the intended quote understates cost basis and
    // over-reports P&L on every trade.
    use crate::idempotency::client_order_id;
    use crate::live_executor::{ExecOutcome, OrderSide, OrderSpec, assess_slippage, place_order_idempotent};
    use polymarket_client_sdk_v2::POLYGON;
    use polymarket_client_sdk_v2::auth::{LocalSigner, Signer};
    use polymarket_client_sdk_v2::clob::types::Side;
    use polymarket_client_sdk_v2::types::{Decimal, U256};
    use std::str::FromStr;

    // F2: gate uses `trades_opened` (BUYs POSTed), not the old `trades_completed`.
    let opened_now = lb.trades_opened.load(Ordering::SeqCst);
    let verdict = gate(&lb.live_armed_path, opened_now, lb.max_trades_per_session);
    oplog.sys("live_open_gate", serde_json::json!({
        "verdict": format!("{verdict:?}"), "reason": verdict.reason(),
        "opened": opened_now, "max": lb.max_trades_per_session,
        "token_id": intent.token_id, "signal_id": ctx.signal_id,
    }));
    if !verdict.is_allow() {
        warn!(reason = %verdict.reason(), token = %intent.token_id, "live_open: gate refused; NO POST");
        return Ok(None);
    }

    let coid = client_order_id(&ctx.asset, &ctx.interval, ctx.epoch, &ctx.side, &ctx.signal_id, 0);
    let tok = U256::from_str_radix(&intent.token_id, 10)
        .map_err(|e| anyhow::anyhow!("token parse: {e}"))?;
    let tick = lb.rest.clob().tick_size(tok).await
        .map(|t| t.minimum_tick_size.as_decimal())
        .unwrap_or(Decimal::new(1, 2));
    let ask_now = lb.rest.get_price(&intent.token_id, Side::Sell).await
        .unwrap_or(intent.fill_price);
    let slip = assess_slippage(OrderSide::Buy, ctx.ask_at_signal, ask_now, lb.max_slippage);
    // F3: BUY worst_price = quote + max_slippage (clamped to 1-tick). The FOK
    // kills if the book has walked beyond tolerance -- replaces the old 0.99
    // hardcoded cap that accepted any price (D4 trade A: quote 0.53, real
    // fill 0.62, no protection).
    let max_slip_dec = Decimal::try_from(lb.max_slippage).unwrap_or(Decimal::new(2, 2));
    let quote_dec = Decimal::try_from(ctx.ask_at_signal).unwrap_or_default();
    let worst = crate::live_executor::compute_worst_price_buy(quote_dec, max_slip_dec, tick);
    let spec = OrderSpec {
        token_id: intent.token_id.clone(),
        side: OrderSide::Buy,
        amount: Decimal::try_from(ctx.stake_usd).unwrap_or_default(),
        worst_price: worst,
        quote_price: quote_dec,
    };
    let signer = LocalSigner::from_str(&lb.pk)
        .map_err(|e| anyhow::anyhow!("signer parse: {e}"))?
        .with_chain_id(Some(POLYGON));

    let t0 = oplog.api_call("clob/place_order_idempotent", "POST", serde_json::json!({
        "phase": "open", "side": "BUY", "token_id": intent.token_id,
        "coid": coid, "shadow": false,
        "usdc": ctx.stake_usd, "worst_price": worst.to_string(),
    }));
    match place_order_idempotent(&lb.rest, &signer, &spec, slip, false, &coid, ctx.epoch, &lb.intent_log).await {
        Ok(ExecOutcome::Posted(resp)) => {
            oplog.api_ok("clob/place_order_idempotent", t0, 200, serde_json::json!({
                "order_id": resp.order_id, "coid": coid,
            }));
            // F1: extract the REAL fill from the BUY response.
            // BUY: taking_amount = shares received; making_amount = USDC spent.
            let real_shares = resp.taking_amount;
            let real_usdc = resp.making_amount;
            // F2: count this BUY as INITIATED. Gate will refuse further BUYs
            // once `opened >= max_trades_per_session`. Without this (pre-F2),
            // a single trigger that matched multiple intervals slipped 2 BUYs
            // past max=1 because the counter only moved on SELL completion.
            let opened_after = record_trade_opened(&lb.trades_opened);
            // F3: real slippage from making/taking. Logged for autopsy
            // (the FOK worst_price above is the hard limit; this is the
            // observation, comparable across trades).
            let real_price = crate::live_executor::real_fill_price_buy(real_usdc, real_shares);
            let real_slip = real_price.map(|p| {
                crate::live_executor::real_adverse_slippage(OrderSide::Buy, quote_dec, p)
            });
            let adverse_beyond_max = real_slip.map(|s| s > max_slip_dec).unwrap_or(false);
            oplog.sys("live_open_posted", serde_json::json!({
                "order_id": resp.order_id, "coid": coid, "token_id": intent.token_id,
                "computed_shares": intent.shares,           // pre-POST estimate (stake/ask)
                "real_taking_shares": real_shares.to_string(),
                "real_making_usdc": real_usdc.to_string(),
                "trades_opened": opened_after, "max_trades_per_session": lb.max_trades_per_session,
            }));
            oplog.sys("live_open_slippage_observed", serde_json::json!({
                "token_id": intent.token_id, "signal_id": ctx.signal_id,
                "quote_price": quote_dec.to_string(),
                "worst_price_fok_limit": worst.to_string(),
                "real_fill_price": real_price.map(|p| p.to_string()),
                "real_adverse_slippage": real_slip.map(|s| s.to_string()),
                "max_slippage_threshold": max_slip_dec.to_string(),
                "adverse_beyond_max": adverse_beyond_max,
            }));
            if adverse_beyond_max {
                warn!(quote = %quote_dec, real = ?real_price, worst = %worst,
                    "live_open: real slippage exceeded max_slippage threshold (FOK should have killed this -- audit needed)");
            }
            // ORDER #8 C: FAVORABLE-FILL anomaly. A buy that filled > $0.05 BETTER
            // than the quote means the real book repriced against our side (the
            // stale-mirror tell — the phantom filled 25c better against a book that
            // knew we were wrong). Instrument only: WARN + a log the auditor can
            // cohort by token. Do NOT auto-exit (unvalidated; may still be correct).
            if let Some(rp) = real_price {
                let improvement = quote_dec - rp;
                if favorable_fill_anomaly(quote_dec, rp) {
                    oplog.sys("live_fill_anomaly", serde_json::json!({
                        "token_id": intent.token_id, "signal_id": ctx.signal_id,
                        "quote": quote_dec.to_string(), "fill": rp.to_string(),
                        "improvement": improvement.to_string(),
                    }));
                    warn!(token = %intent.token_id, quote = %quote_dec, fill = %rp,
                        improvement = %improvement,
                        "live_fill_anomaly: fill >5c better than quote (stale-mirror tell) — INSTRUMENT only");
                }
            }
            info!(order_id = %resp.order_id, coid = %coid, token = %intent.token_id,
                computed = intent.shares, real_shares = %real_shares, real_usdc = %real_usdc,
                real_price = ?real_price, real_slip = ?real_slip,
                worst = %worst, quote = %quote_dec,
                opened = opened_after, max = lb.max_trades_per_session,
                "live_open: POSTED -> opened {opened}/{max} (F3 worst_price={worst} kills FOK above tolerance)",
                opened = opened_after, max = lb.max_trades_per_session, worst = worst);
            Ok(Some((real_shares, real_usdc)))
        }
        Ok(ExecOutcome::IdempotencyRefused { coid: c, reason }) => {
            oplog.sys("live_open_idempotency_refused", serde_json::json!({"coid": c, "reason": reason}));
            warn!(coid = %c, %reason, "live_open: idempotency refused; NO POST");
            Ok(None)
        }
        Ok(ExecOutcome::SlippageAbort { slippage }) => {
            oplog.sys("live_open_slippage_abort", serde_json::json!({
                "slip": slippage.slippage, "max": slippage.max_slippage,
            }));
            warn!(slip = slippage.slippage, max = slippage.max_slippage, "live_open: slippage abort; NO POST");
            Ok(None)
        }
        Ok(ExecOutcome::Shadow { .. }) => {
            oplog.err("live_open", "got Shadow outcome with shadow=false (impossible)", serde_json::json!({}));
            Ok(None)
        }
        Err(e) => {
            oplog.api_err("clob/place_order_idempotent", t0, None, &e.to_string());
            warn!(error = %e, "live_open: POST failed");
            Err(e)
        }
    }
}

/// F1 PURE helper: combine the bot's tracked shares (`ct_shares`, ideally the
/// real `taking_amount` from the BUY response per F1 plumbing) and a fresh
/// `/positions` snapshot to decide what to SELL. Delegates to `exec::decide_sell`
/// (A2 logic, anti-phantom + anti-mismatch + min(both, truncated to LOT_SCALE)).
/// ZERO network -- callers pass the REST snapshot.
#[must_use]
pub fn plan_sell_from_rest(
    ct_shares: polymarket_client_sdk_v2::types::Decimal,
    positions: &[crate::rest::PositionInfo],
    token_id: &str,
) -> crate::exec::SellPlan {
    let pos_shares = positions
        .iter()
        .find(|p| p.token_id == token_id)
        .map(|p| polymarket_client_sdk_v2::types::Decimal::try_from(p.size)
            .unwrap_or(polymarket_client_sdk_v2::types::Decimal::ZERO));
    crate::exec::decide_sell(true, ct_shares, pos_shares)
}

/// G2 PHANTOM-RETRY POLICY: how aggressively to re-poll `/positions` before
/// concluding Phantom. The data-api indexer lags on-chain settlement by a
/// non-deterministic delay (typically <5 s with G1 removing regla_c's 43 s
/// close path; the close now fires at 120/300 s post-BUY when /positions is
/// almost always settled, but edge cases remain -- e.g. a market that just
/// closed, an indexer hiccup, a BUY confirmed by the CLOB response that
/// hasn't yet propagated to the data-api at exit_ts).
///
/// CHOSEN DEFAULTS: 3 attempts, 2500 ms between attempts = total worst case
/// `(N-1) * backoff = 5 s` added latency before declaring Phantom. The first
/// attempt is immediate (the existing fetch); attempts 2 and 3 are the new
/// retry buffer. With 3 attempts the bot probes /positions at t, t+2.5 s,
/// t+5 s -- enough to ride out a typical indexer lag without holding the
/// exit_task tick for arbitrary time.
///
/// NOTE: this policy is for the "/positions doesn't yet show the position"
/// case ONLY. It does NOT help past-close trades (the market is already
/// resolved -> no book, no SELL -- those need on-chain redeem, G5).
#[derive(Debug, Clone, Copy)]
pub struct PhantomRetryPolicy {
    pub max_attempts: u32,
    pub backoff_ms: u64,
}

impl Default for PhantomRetryPolicy {
    fn default() -> Self {
        Self { max_attempts: 3, backoff_ms: 2500 }
    }
}

/// G2 PURE: given a SEQUENCE of /positions snapshots (one per retry attempt)
/// and the target token, return the on-chain shares to feed `decide_sell`.
/// Returns the FIRST positive observation (early-return semantics of the
/// async driver), else the LAST observation (Some(0) or None). Pure +
/// testable -- the async driver is just this + sleep + actual fetch.
#[must_use]
pub fn pick_best_pos_shares(
    snapshots: &[Vec<crate::rest::PositionInfo>],
    token_id: &str,
) -> Option<polymarket_client_sdk_v2::types::Decimal> {
    use polymarket_client_sdk_v2::types::Decimal as SdkDec;
    let mut last: Option<SdkDec> = None;
    for snap in snapshots {
        let p = snap
            .iter()
            .find(|p| p.token_id == token_id)
            .map(|p| SdkDec::try_from(p.size).unwrap_or(SdkDec::ZERO));
        if let Some(s) = p {
            if s > SdkDec::ZERO {
                return Some(s);
            }
        }
        last = p;
    }
    last
}

/// G2 ASYNC DRIVER: drive the phantom-retry policy by repeatedly calling
/// `fetch(attempt)` until a positive on-chain share count appears for
/// `token_id`, or `max_attempts` is reached. Returns the same Option that
/// `pick_best_pos_shares` would have returned on the collected snapshots.
///
/// Generic over the fetcher so unit tests inject a stub without needing a
/// real RestClient. Emits per-attempt oplog events
/// (`live_close_phantom_retry` / `live_close_pos_settled` /
/// `live_close_phantom_retry_err`) so the autopsy can reconstruct exactly
/// what the bot saw at each attempt.
pub async fn drive_phantom_retry<F, Fut>(
    token_id: &str,
    signal_id: &str,
    policy: PhantomRetryPolicy,
    oplog: &OpLog,
    mut fetch: F,
) -> Option<polymarket_client_sdk_v2::types::Decimal>
where
    F: FnMut(u32) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<Vec<crate::rest::PositionInfo>>>,
{
    use polymarket_client_sdk_v2::types::Decimal as SdkDec;
    for attempt in 1..=policy.max_attempts {
        match fetch(attempt).await {
            Ok(positions) => {
                let pos_shares = positions
                    .iter()
                    .find(|p| p.token_id == token_id)
                    .map(|p| SdkDec::try_from(p.size).unwrap_or(SdkDec::ZERO));
                if let Some(s) = pos_shares {
                    if s > SdkDec::ZERO {
                        oplog.sys("live_close_pos_settled", serde_json::json!({
                            "token_id": token_id, "signal_id": signal_id,
                            "attempt": attempt, "max_attempts": policy.max_attempts,
                            "shares": s.to_string(),
                        }));
                        return Some(s);
                    }
                }
                oplog.sys("live_close_phantom_retry", serde_json::json!({
                    "token_id": token_id, "signal_id": signal_id,
                    "attempt": attempt, "max_attempts": policy.max_attempts,
                    "observation": pos_shares.map(|s| s.to_string()),
                    "will_retry": attempt < policy.max_attempts,
                }));
            }
            Err(e) => {
                oplog.sys("live_close_phantom_retry_err", serde_json::json!({
                    "token_id": token_id, "signal_id": signal_id,
                    "attempt": attempt, "max_attempts": policy.max_attempts,
                    "error": e.to_string(),
                    "will_retry": attempt < policy.max_attempts,
                }));
            }
        }
        if attempt < policy.max_attempts {
            tokio::time::sleep(std::time::Duration::from_millis(policy.backoff_ms)).await;
        }
    }
    None
}

/// G2 LIVE wrapper: drives `drive_phantom_retry` against the real RestClient
/// + emits per-attempt api_call/api_response so the autopsy has the raw
/// latency + response data.
pub async fn fetch_pos_shares_with_retry(
    rest: &RestClient,
    token_id: &str,
    signal_id: &str,
    policy: PhantomRetryPolicy,
    oplog: &OpLog,
) -> Option<polymarket_client_sdk_v2::types::Decimal> {
    drive_phantom_retry(token_id, signal_id, policy, oplog, |attempt| async move {
        let t0 = oplog.api_call("clob/positions", "GET", serde_json::json!({
            "phase": "close_retry",
            "attempt": attempt, "max_attempts": policy.max_attempts,
            "signal_id": signal_id,
        }));
        match rest.get_positions().await {
            Ok(ps) => {
                oplog.api_ok("clob/positions", t0, 200, serde_json::json!({
                    "count": ps.len(), "attempt": attempt,
                }));
                Ok(ps)
            }
            Err(e) => {
                oplog.api_err("clob/positions", t0, None, &e.to_string());
                Err(e)
            }
        }
    })
    .await
}

/// LIVE SELL POST (close path). F2: gated by `gate_close` (LIVE_ARMED only --
/// the max_trades cap MUST NOT block closes). After EVERY definitive close
/// outcome (Posted, Phantom, Mismatch, NoFill, error -- but NOT a re-armable
/// gate refusal), increments `trades_closed`; once `closed >= opened` AND
/// `opened >= max`, the shutdown signal fires. This decouples shutdown from
/// SELL success so a stuck SELL no longer pins the session open (the D4
/// "5h alive" failure mode).
pub async fn live_close(
    lb: &Arc<LiveBackend>,
    ct: &ClosedTrade,
    oplog: &OpLog,
) -> Result<Option<polymarket_client_sdk_v2::types::Decimal>> {
    // R1: returns Some(usdc_received) on a Posted sell so the caller books the
    // stop at the REAL proceeds (realized = usdc - shares*entry), not the model
    // bid it was quoted at. None = no sell landed (gate/phantom/non-posted).
    use crate::live_executor::{ExecOutcome, OrderSide, OrderSpec, assess_slippage, place_order_idempotent};
    use polymarket_client_sdk_v2::POLYGON;
    use polymarket_client_sdk_v2::auth::{LocalSigner, Signer};
    use polymarket_client_sdk_v2::clob::types::Side;
    use polymarket_client_sdk_v2::types::{Decimal, U256};
    use std::str::FromStr;

    // F2: SELL uses `gate_close` (LIVE_ARMED only). The max_trades cap MUST
    // NOT block closes -- once a BUY is open, refusing the SELL locks capital
    // until resolution (different code path, identical destructive effect to
    // bug #2). A deliberate disarm (LIVE_ARMED missing) still refuses; an
    // unclosed position then gets handled by hold_recovery on restart.
    let verdict = gate_close(&lb.live_armed_path);
    oplog.sys("live_close_gate", serde_json::json!({
        "verdict": format!("{verdict:?}"), "reason": verdict.reason(),
        "opened": lb.trades_opened.load(Ordering::SeqCst),
        "closed": lb.trades_closed.load(Ordering::SeqCst),
        "max": lb.max_trades_per_session,
        "token_id": ct.token_id, "signal_id": ct.signal_id,
    }));
    if !verdict.is_allow() {
        warn!(reason = %verdict.reason(), token = %ct.token_id, "live_close: gate refused; NO POST (close not counted -- waiting for re-arm)");
        return Ok(None);
    }

    // F1 + G2: fetch /positions with PHANTOM RETRY (ground truth on-chain shares
    // for the SELL). The bot's tracked `ct.shares` should already be the REAL fill
    // (F1 plumbed it from BUY response.taking_amount); /positions is the
    // ground-truth corroboration.
    //
    // G2: retry up to PhantomRetryPolicy::default().max_attempts (= 3) times with
    // backoff (= 2500 ms) before declaring Phantom. The data-api indexer can lag
    // on-chain settlement by a few seconds; without the retry, a fresh /positions
    // miss => immediate Phantom => SELL skipped => position dangles on-chain
    // until resolution. With the retry, we ride out the typical indexer lag
    // before giving up.
    //
    // CRUCIAL: the retry does NOT change the semantics of decide_sell -- it just
    // gives /positions more chances to surface the on-chain position. Once we
    // get Some(p > 0), we early-return and feed it to decide_sell (which still
    // takes min(both, truncated to LOT_SCALE=2) -- NEVER over-sells). All-null
    // exhaustion => decide_sell sees None => Phantom verdict (correct).
    let policy = PhantomRetryPolicy::default();
    let pos_shares = fetch_pos_shares_with_retry(
        &lb.rest, &ct.token_id, &ct.signal_id, policy, oplog,
    ).await;
    let plan = crate::exec::decide_sell(true, ct.shares, pos_shares);
    oplog.sys("live_close_decide_sell", serde_json::json!({
        "ct_shares": ct.shares.to_string(),
        "pos_shares": pos_shares.map(|s| s.to_string()),
        "plan": format!("{plan:?}"),
        "token_id": ct.token_id, "signal_id": ct.signal_id,
        "retry_policy": {
            "max_attempts": policy.max_attempts,
            "backoff_ms": policy.backoff_ms,
        },
    }));
    // F2 helper: record a CLOSE ATTEMPT (definitive non-POST outcome) without
    // duplicating the oplog/info block at every early-return below.
    let record_attempt = |outcome: &'static str| -> bool {
        let (closed_n, signaled) = record_trade_closed(
            &lb.trades_opened, &lb.trades_closed, lb.max_trades_per_session, &lb.shutdown_tx,
        );
        let opened_now = lb.trades_opened.load(Ordering::SeqCst);
        oplog.sys("trade_close_attempted", serde_json::json!({
            "opened": opened_now, "closed": closed_n, "max": lb.max_trades_per_session,
            "shutdown_signaled": signaled, "outcome_summary": outcome,
            "token_id": ct.token_id, "signal_id": ct.signal_id,
        }));
        info!(opened = opened_now, closed = closed_n, max = lb.max_trades_per_session,
            shutdown = signaled,
            "live_close: ATTEMPT recorded (outcome={outcome}) -> opened={opened_now} closed={closed_n}/max={max}",
            max = lb.max_trades_per_session);
        signaled
    };
    let sell_shares = match plan {
        crate::exec::SellPlan::Sell(q) => q,
        crate::exec::SellPlan::Phantom => {
            warn!(token = %ct.token_id, ct_shares = %ct.shares,
                "live_close: PHANTOM (/positions empty or 0 shares); NO SELL POST; ESCALATE");
            record_attempt("Phantom");
            return Ok(None);
        }
        crate::exec::SellPlan::Mismatch => {
            warn!(token = %ct.token_id, ct_shares = %ct.shares,
                "live_close: MISMATCH (bot's tracked shares vs /positions differ > dust); NO SELL POST; ESCALATE");
            record_attempt("Mismatch");
            return Ok(None);
        }
        crate::exec::SellPlan::NoFill => {
            warn!(token = %ct.token_id, "live_close: NoFill outcome (defensive; shouldn't reach here from exit_task)");
            record_attempt("NoFill");
            return Ok(None);
        }
    };
    if sell_shares <= Decimal::ZERO {
        warn!(token = %ct.token_id, %sell_shares, "live_close: sell_shares <= 0; NO POST");
        record_attempt("ZeroShares");
        return Ok(None);
    }

    // SELL coid scheme: prefix + signal_id + closed_at_ms. Unique per close.
    let coid = format!("close-{}-{}", ct.signal_id, ct.closed_at_ms);
    let tok = U256::from_str_radix(&ct.token_id, 10)
        .map_err(|e| anyhow::anyhow!("token parse: {e}"))?;
    let tick = lb.rest.clob().tick_size(tok).await
        .map(|t| t.minimum_tick_size.as_decimal())
        .unwrap_or(Decimal::new(1, 2));
    let bid_now = lb.rest.get_price(&ct.token_id, Side::Buy).await.unwrap_or(0.0);
    let slip = assess_slippage(OrderSide::Sell, bid_now, bid_now, lb.max_slippage);
    // F3: SELL worst_price = quote - max_slippage (floored at tick). FOK
    // kills the SELL if the book has fallen beyond tolerance. Replaces the
    // old `worst = tick` that accepted any fill (down to 0.01 = pure
    // capitulation). TRADEOFF: an adverse market sends the position to
    // resolution (relax max_slippage in config to widen tolerance).
    let max_slip_dec = Decimal::try_from(lb.max_slippage).unwrap_or(Decimal::new(2, 2));
    let quote_dec = Decimal::try_from(bid_now).unwrap_or_default();
    let worst = crate::live_executor::compute_worst_price_sell(quote_dec, max_slip_dec, tick);
    let spec = OrderSpec {
        token_id: ct.token_id.clone(),
        side: OrderSide::Sell,
        amount: sell_shares, // F1: from decide_sell -- guaranteed scale<=2 and <= held.
        worst_price: worst,
        quote_price: quote_dec,
    };
    let signer = LocalSigner::from_str(&lb.pk)
        .map_err(|e| anyhow::anyhow!("signer parse: {e}"))?
        .with_chain_id(Some(POLYGON));

    let epoch = ct.closed_at_ms / 1000;
    let t0 = oplog.api_call("clob/place_order_idempotent", "POST", serde_json::json!({
        "phase": "close", "side": "SELL", "token_id": ct.token_id,
        "coid": coid, "shadow": false,
        "shares": sell_shares.to_string(), "worst_price": worst.to_string(),
    }));
    let outcome = place_order_idempotent(&lb.rest, &signer, &spec, slip, false, &coid, epoch, &lb.intent_log).await;
    let mut sold_usdc: Option<Decimal> = None; // R1: real proceeds on a Posted sell
    match &outcome {
        Ok(ExecOutcome::Posted(resp)) => {
            sold_usdc = Some(resp.taking_amount); // taking_amount = USDC received
            oplog.api_ok("clob/place_order_idempotent", t0, 200, serde_json::json!({
                "order_id": resp.order_id, "coid": coid,
            }));
            // F3: real slippage from SELL response. making_amount=shares sold,
            // taking_amount=USDC received. real_price = taking/making.
            let real_price = crate::live_executor::real_fill_price_sell(resp.making_amount, resp.taking_amount);
            let real_slip = real_price.map(|p| {
                crate::live_executor::real_adverse_slippage(OrderSide::Sell, quote_dec, p)
            });
            let adverse_beyond_max = real_slip.map(|s| s > max_slip_dec).unwrap_or(false);
            oplog.sys("live_close_posted", serde_json::json!({
                "order_id": resp.order_id, "coid": coid, "token_id": ct.token_id,
                "shares_sold": resp.making_amount.to_string(),
                "usdc_received": resp.taking_amount.to_string(),
            }));
            oplog.sys("live_close_slippage_observed", serde_json::json!({
                "token_id": ct.token_id, "signal_id": ct.signal_id,
                "quote_price": quote_dec.to_string(),
                "worst_price_fok_limit": worst.to_string(),
                "real_fill_price": real_price.map(|p| p.to_string()),
                "real_adverse_slippage": real_slip.map(|s| s.to_string()),
                "max_slippage_threshold": max_slip_dec.to_string(),
                "adverse_beyond_max": adverse_beyond_max,
            }));
            if adverse_beyond_max {
                warn!(quote = %quote_dec, real = ?real_price, worst = %worst,
                    "live_close: real slippage exceeded max_slippage (FOK should have killed -- audit needed)");
            }
            info!(order_id = %resp.order_id, coid = %coid, token = %ct.token_id,
                real_price = ?real_price, real_slip = ?real_slip, worst = %worst, quote = %quote_dec,
                "live_close: POSTED (F3 worst_price={worst} would kill below tolerance)",
                worst = worst);
        }
        Ok(other) => {
            oplog.sys("live_close_outcome", serde_json::json!({"outcome": format!("{other:?}")}));
            warn!(?other, "live_close: non-Posted outcome");
        }
        Err(e) => {
            oplog.api_err("clob/place_order_idempotent", t0, None, &e.to_string());
            warn!(error = %e, "live_close: POST failed");
        }
    }
    // F2: count this as a CLOSE ATTEMPT regardless of outcome (Posted / non-
    // Posted ExecOutcome / Err). Decoupling shutdown from SELL success is the
    // fix for the "5h alive" symptom of D4: previously a failed SELL pinned
    // the session open forever because `trades_completed` only moved on
    // Posted. With F2, every close decision counts; once `closed >= opened`
    // AND `opened >= max_trades_per_session`, the shutdown fires.
    let (new_closed, signaled) = record_trade_closed(
        &lb.trades_opened, &lb.trades_closed, lb.max_trades_per_session, &lb.shutdown_tx,
    );
    let opened_now = lb.trades_opened.load(Ordering::SeqCst);
    oplog.sys("trade_close_attempted", serde_json::json!({
        "opened": opened_now, "closed": new_closed, "max": lb.max_trades_per_session,
        "shutdown_signaled": signaled,
        "outcome_summary": match &outcome {
            Ok(ExecOutcome::Posted(_)) => "Posted",
            Ok(ExecOutcome::IdempotencyRefused { .. }) => "IdempotencyRefused",
            Ok(ExecOutcome::SlippageAbort { .. }) => "SlippageAbort",
            Ok(ExecOutcome::Shadow { .. }) => "Shadow",
            Err(_) => "Err",
        },
        "token_id": ct.token_id, "signal_id": ct.signal_id,
    }));
    info!(
        opened = opened_now, closed = new_closed, max = lb.max_trades_per_session,
        shutdown = signaled,
        "live_close: ATTEMPT recorded -> opened={opened_now} closed={new_closed}/max={max} (shutdown_signaled={signaled})",
        max = lb.max_trades_per_session,
    );
    Ok(sold_usdc)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_arm(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!("rb_lb_arm_{tag}_{}_{}.txt",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()))
    }

    #[test]
    fn gate_refuses_when_live_armed_missing() {
        let p = tmp_arm("missing");
        let _ = std::fs::remove_file(&p);
        let v = gate(&p, 0, 1);
        assert!(matches!(v, LivePostDecision::RefuseDisarmed { .. }), "got {v:?}");
        println!("[piece6-test] LIVE_ARMED missing -> verdict={v:?} reason={}", v.reason());
        assert!(!v.is_allow());
    }

    #[test]
    fn gate_refuses_when_live_armed_empty() {
        let p = tmp_arm("empty");
        std::fs::write(&p, "   \n").unwrap();
        let v = gate(&p, 0, 1);
        assert!(matches!(v, LivePostDecision::RefuseDisarmed { .. }), "got {v:?}");
        println!("[piece6-test] LIVE_ARMED empty/whitespace -> verdict={v:?} reason={}", v.reason());
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn gate_allows_when_armed_and_under_max() {
        let p = tmp_arm("armed");
        std::fs::write(&p, "ARM-D4-2026-05-30").unwrap();
        let v = gate(&p, 0, 1);
        assert_eq!(v, LivePostDecision::Allow);
        println!("[piece6-test] LIVE_ARMED present + completed 0/1 -> {v:?}");
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn gate_refuses_when_max_trades_reached() {
        let p = tmp_arm("max");
        std::fs::write(&p, "ARM-D4").unwrap();
        let v = gate(&p, 1, 1);
        match &v {
            LivePostDecision::RefuseMaxReached { completed, max } => {
                assert_eq!(*completed, 1);
                assert_eq!(*max, 1);
                println!("[piece6-test] max_trades reached -> completed={completed} max={max}, reason={}", v.reason());
            }
            other => panic!("expected RefuseMaxReached, got {other:?}"),
        }
        let _ = std::fs::remove_file(&p);
    }

    // ============================================================================
    // F2 (fix bug #1: max_trades counts INITIATED not COMPLETED + shutdown
    // decoupled from clean SELL).
    //
    // D4 had TWO failure modes here:
    //   * Bug #1a (counter): max=1 but TWO BUYs went through. A single trigger
    //     matched 5m + 15m markets simultaneously; the old counter only moved
    //     on SELL completion, so both Opens saw completed=0 and the gate
    //     allowed both. F2 fix: the gate uses `trades_opened` (BUYs POSTed).
    //   * Bug #1b (shutdown): the bot stayed alive ~5 hours because the SELLs
    //     failed (bug #2) -> trades_completed stuck at 0 -> shutdown never
    //     fired. F2 fix: shutdown fires when `closed >= opened AND opened >=
    //     max`, where `closed` increments on ANY definitive close outcome
    //     (Posted, Phantom, Mismatch, NoFill, POST error).

    #[test]
    fn f2_gate_open_refuses_second_buy_after_first_posted_d4_scenario() {
        // EXACT D4 sequence: 1st BUY POSTed -> opened=1; 2nd BUY (simultaneous
        // from the same trigger -- a different market interval) hits the gate
        // with opened=1, max=1 -> RefuseMaxReached.
        let p = tmp_arm("d4_double");
        std::fs::write(&p, "ARM-D4").unwrap();
        // 1st: opened=0 -> Allow.
        let v1 = gate(&p, 0, 1);
        assert_eq!(v1, LivePostDecision::Allow);
        // (post-Posted, record_trade_opened would set opened=1)
        // 2nd: opened=1 -> RefuseMaxReached.
        let v2 = gate(&p, 1, 1);
        match &v2 {
            LivePostDecision::RefuseMaxReached { completed, max } => {
                assert_eq!(*completed, 1);
                assert_eq!(*max, 1);
                println!("[F2-test] D4 second-BUY scenario: opened=1 max=1 -> {} ({:?})", v2.reason(), v2);
            }
            other => panic!("expected RefuseMaxReached, got {other:?}"),
        }
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn f2_gate_close_only_checks_live_armed_never_blocks_on_max() {
        // The SELL path MUST NOT be blocked by max_trades -- once a BUY is on
        // the books, refusing the SELL locks capital until resolution
        // (different cause, same destructive effect as bug #2). gate_close
        // only checks LIVE_ARMED; max_trades is irrelevant here.
        let p = tmp_arm("close_arm");
        std::fs::write(&p, "ARM").unwrap();
        let v = gate_close(&p);
        assert_eq!(v, LivePostDecision::Allow);
        println!("[F2-test] gate_close with LIVE_ARMED: {v:?} (max_trades irrelevant)");
        let _ = std::fs::remove_file(&p);
        // Without LIVE_ARMED, refuse.
        let p2 = tmp_arm("close_disarm");
        let _ = std::fs::remove_file(&p2);
        let v2 = gate_close(&p2);
        assert!(matches!(v2, LivePostDecision::RefuseDisarmed { .. }));
        println!("[F2-test] gate_close without LIVE_ARMED: {} ({:?})", v2.reason(), v2);
    }

    #[test]
    fn f2_record_trade_opened_increments_counter() {
        let opened = Arc::new(AtomicUsize::new(0));
        assert_eq!(record_trade_opened(&opened), 1);
        assert_eq!(record_trade_opened(&opened), 2);
        assert_eq!(record_trade_opened(&opened), 3);
    }

    #[tokio::test]
    async fn f2_record_trade_closed_signals_shutdown_when_all_initiated_closed() {
        // D4 fixed scenario: max=1, open 1, close 1 -> shutdown fires.
        let opened = Arc::new(AtomicUsize::new(0));
        let closed = Arc::new(AtomicUsize::new(0));
        let (tx, mut rx) = watch::channel(false);
        let tx = Arc::new(tx);
        // Simulate: 1 BUY Posted.
        let opened_n = record_trade_opened(&opened);
        assert_eq!(opened_n, 1);
        assert!(!*rx.borrow(), "no shutdown after open alone (waiting for close)");
        // Simulate: that trade's close attempted (any outcome).
        let (closed_n, signaled) = record_trade_closed(&opened, &closed, 1, &tx);
        assert_eq!(closed_n, 1);
        assert!(signaled, "1 opened + 1 closed + max=1 -> shutdown MUST fire");
        rx.changed().await.expect("shutdown signal");
        assert!(*rx.borrow());
        println!("[F2-test] open 1 + close 1 (max=1) -> shutdown fired");
    }

    #[test]
    fn f2_record_trade_closed_does_not_signal_when_more_trades_pending_close() {
        // 2 trades opened (max=2), only 1 closed so far -> NO shutdown yet
        // (other trade still pending its close attempt). Once 2nd closes,
        // shutdown fires. Proves the "wait until all initiated have had their
        // close decision" semantic.
        let opened = Arc::new(AtomicUsize::new(0));
        let closed = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = watch::channel(false);
        let tx = Arc::new(tx);
        record_trade_opened(&opened);
        record_trade_opened(&opened);
        let (n1, sig1) = record_trade_closed(&opened, &closed, 2, &tx);
        assert_eq!(n1, 1);
        assert!(!sig1, "1/2 closes -> no shutdown yet");
        assert!(!*rx.borrow());
        let (n2, sig2) = record_trade_closed(&opened, &closed, 2, &tx);
        assert_eq!(n2, 2);
        assert!(sig2, "2/2 closes -> shutdown fires");
        assert!(*rx.borrow());
        println!("[F2-test] open 2 + close 2 (max=2) -> shutdown fired on 2nd close, not 1st");
    }

    #[test]
    fn f2_record_trade_closed_does_not_signal_below_max() {
        // opened=1, max=2 -> bot is mid-session, not at cap. Close fires but
        // shutdown does NOT (we expect more trades). Proves the `opened >= max`
        // condition.
        let opened = Arc::new(AtomicUsize::new(0));
        let closed = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = watch::channel(false);
        let tx = Arc::new(tx);
        record_trade_opened(&opened);
        let (n, sig) = record_trade_closed(&opened, &closed, 2, &tx);
        assert_eq!(n, 1);
        assert!(!sig, "below max -> no shutdown");
        assert!(!*rx.borrow());
        println!("[F2-test] opened=1 < max=2 -> no shutdown after close");
    }

    #[tokio::test]
    async fn f2_shutdown_fires_regardless_of_close_outcome_d4_fix_5h_alive() {
        // The 5-hour-alive D4 symptom: SELL failed -> old `trades_completed`
        // stayed at 0 -> shutdown never fired. F2: `record_trade_closed` is
        // called on EVERY close outcome (Phantom/Mismatch/Posted/error), so
        // even a failed SELL still advances the counter and the bot shuts
        // down at the planned cap. This test simulates a Phantom outcome:
        // opened=1, max=1, close attempt counted -> shutdown fires.
        let opened = Arc::new(AtomicUsize::new(0));
        let closed = Arc::new(AtomicUsize::new(0));
        let (tx, mut rx) = watch::channel(false);
        let tx = Arc::new(tx);
        record_trade_opened(&opened);
        // The close was Phantom (or Mismatch, or NoFill, or POST error -- all
        // count as a "definitive close decision" in F2).
        let (n, sig) = record_trade_closed(&opened, &closed, 1, &tx);
        assert_eq!(n, 1);
        assert!(sig, "shutdown MUST fire even if the close was Phantom/Mismatch (not Posted)");
        rx.changed().await.expect("shutdown");
        assert!(*rx.borrow());
        println!("[F2-test] Phantom/Mismatch close still triggers shutdown (D4 '5h alive' fix)");
    }

    #[test]
    fn trading_tasks_enabled_covers_paper_and_live() {
        assert!(trading_tasks_enabled("paper"));
        assert!(trading_tasks_enabled("live"), "PIECE 6: live mode MUST now also spawn tasks");
        assert!(!trading_tasks_enabled(""));
        assert!(!trading_tasks_enabled("dry"));
        assert!(!trading_tasks_enabled("shadow"));
    }

    // ============================================================================
    // F1 (fix bug #2: SELL sizing/precision) tests.
    //
    // plan_sell_from_rest combines the bot's tracked shares (F1: real BUY fill
    // from response.taking_amount, plumbed via execution_task) with a fresh
    // /positions snapshot, delegating to exec::decide_sell. The tests below
    // reproduce the EXACT D4 scenarios that broke under the old "pass ct.shares
    // raw to OrderSpec" code:
    //   * D4 trade A: bot computed 1.9811320754716981 (stake/ask = 1.05/0.53),
    //     real on-chain fill was 1.693547 (book walked to 0.62 from slippage).
    //     Old: tried SELL 1.9811320754716981 -> SDK rejected (15 decimal places).
    //     F1: with real shares plumbed, ct=1.693547, pos=1.693547 -> Sell(1.69).
    //   * D4 trade B: bot computed 1.640625, real fill matched 1.640625 (no
    //     slippage). Old: tried SELL 1.640625 -> SDK rejected (6 decimal places).
    //     F1: ct=1.640625, pos=1.640625 -> Sell(1.64) (truncated to LOT_SCALE=2).
    use crate::exec::SellPlan;
    use crate::rest::PositionInfo;
    use polymarket_client_sdk_v2::types::{Decimal as SdkDec, NaiveDate};
    use rust_decimal_macros::dec;

    fn pos(token: &str, size_f64: f64) -> PositionInfo {
        PositionInfo {
            token_id: token.into(),
            size: size_f64,
            avg_price: 0.5,
            cur_price: 0.5,
            cash_pnl: 0.0,
            redeemable: false,
            end_date: NaiveDate::from_ymd_opt(2027, 1, 1),
            condition_id: String::new(), // G5: unused by SELL plan tests.
        }
    }

    #[test]
    fn f1_plan_sell_d4_trade_a_real_fill_truncates_to_lot_scale() {
        // D4 trade A reproduction: had F1 been in place, ct.shares = real fill
        // 1.693547 (from BUY response.taking_amount), pos_shares = 1.693547
        // on-chain. decide_sell returns min(both).trunc_with_scale(2) = 1.69.
        let ct_shares = dec!(1.693547); // F1: real BUY taking_amount
        let positions = vec![pos("TOK-5M", 1.693547)];
        let plan = plan_sell_from_rest(ct_shares, &positions, "TOK-5M");
        println!("[F1-test] D4 trade A: ct=1.693547, pos=1.693547 -> {plan:?}");
        match plan {
            SellPlan::Sell(q) => {
                assert_eq!(q, dec!(1.69), "SELL amount must be 1.69 (truncated to LOT_SCALE=2)");
                assert!(q.scale() <= 2, "scale must be <= 2 (SDK requirement); got {}", q.scale());
            }
            other => panic!("expected Sell(1.69), got {other:?}"),
        }
    }

    #[test]
    fn f1_plan_sell_d4_trade_b_six_decimals_truncates_to_two() {
        // D4 trade B: 1.640625 has 6 decimals -> SDK rejected with
        // "Unable to build Amount with 6 decimal points, must be <= 2".
        // F1: decide_sell truncates to 1.64.
        let ct_shares = dec!(1.640625);
        let positions = vec![pos("TOK-15M", 1.640625)];
        let plan = plan_sell_from_rest(ct_shares, &positions, "TOK-15M");
        println!("[F1-test] D4 trade B: ct=1.640625 (6 decimals), pos=1.640625 -> {plan:?}");
        match plan {
            SellPlan::Sell(q) => {
                assert_eq!(q, dec!(1.64));
                assert!(q.scale() <= 2, "scale must be <= 2; got {}", q.scale());
            }
            other => panic!("expected Sell(1.64), got {other:?}"),
        }
    }

    #[test]
    fn f1_plan_sell_with_computed_shares_would_have_mismatched_proving_real_plumbing_required() {
        // Counter-example: WITHOUT the F1 plumbing (using the bot's pre-POST
        // computed 1.9811320754716981), decide_sell sees a 0.29-share gap vs
        // the 1.693547 on-chain -- well beyond `dust` -- and returns Mismatch,
        // REFUSING the SELL. This proves the BUY response.taking_amount
        // plumbing IS required; truncating the computed shares alone is NOT
        // a sufficient fix.
        let ct_shares_computed = dec!(1.9811320754716981); // pre-F1 bug source
        let positions = vec![pos("TOK-5M", 1.693547)];
        let plan = plan_sell_from_rest(ct_shares_computed, &positions, "TOK-5M");
        println!("[F1-test] WITHOUT real shares plumbing: ct=1.98 (computed), pos=1.69 -> {plan:?}");
        assert!(matches!(plan, SellPlan::Mismatch),
            "computed shares would Mismatch /positions; got {plan:?}. THIS is why F1 plumbs the real fill.");
    }

    #[test]
    fn f1_plan_sell_15_decimal_precision_still_truncates_safely() {
        // The 15-decimal extreme: if for any reason a 15-decimal value reached
        // ct.shares, decide_sell still truncates and never overflows the SDK's
        // <=2 decimal validation. Defense in depth.
        let ct_shares = dec!(1.9811320754716981); // 15 decimal places
        // pos also 15 decimals so the diff is 0 (otherwise Mismatch).
        let positions = vec![pos("TOK", 1.9811320754716981)];
        let plan = plan_sell_from_rest(ct_shares, &positions, "TOK");
        println!("[F1-test] 15-decimal input ct=1.9811320754716981: {plan:?}");
        match plan {
            SellPlan::Sell(q) => {
                assert!(q.scale() <= 2, "scale must be <= 2; got {} (qty {})", q.scale(), q);
                assert_eq!(q, dec!(1.98), "expected truncated to 1.98");
            }
            other => panic!("expected Sell(1.98), got {other:?}"),
        }
    }

    #[test]
    fn f1_plan_sell_phantom_when_positions_missing_token() {
        // /positions doesn't have our token (BUY never reflected on-chain ->
        // phantom). decide_sell refuses the SELL (Phantom) -- safer to let the
        // market resolve than risk selling a non-existent position.
        let ct_shares = dec!(1.69);
        let positions = vec![pos("OTHER-TOKEN", 5.0)];
        let plan = plan_sell_from_rest(ct_shares, &positions, "MY-TOKEN");
        println!("[F1-test] /positions missing token -> {plan:?}");
        assert!(matches!(plan, SellPlan::Phantom),
            "missing token in /positions must Phantom; got {plan:?}");
    }

    #[test]
    fn f1_plan_sell_phantom_when_positions_size_zero() {
        // /positions reports the token but with size=0 -> also Phantom.
        let positions = vec![pos("TOK", 0.0)];
        let plan = plan_sell_from_rest(dec!(1.69), &positions, "TOK");
        println!("[F1-test] /positions size=0 -> {plan:?}");
        assert!(matches!(plan, SellPlan::Phantom),
            "size=0 in /positions must Phantom; got {plan:?}");
    }

    #[test]
    fn f1_plan_sell_dust_drift_still_sells_min_truncated() {
        // Small drift (< dust threshold) between ct.shares and /positions.size
        // is normal protocol rounding -- decide_sell accepts it and sells
        // min(both).trunc_with_scale(LOT_SCALE). Real-world: 1 lot delta on a
        // ~$1 position.
        let ct_shares = dec!(1.7500);
        let positions = vec![pos("TOK", 1.7480)]; // 0.002 drift
        let plan = plan_sell_from_rest(ct_shares, &positions, "TOK");
        println!("[F1-test] dust drift ct=1.7500 pos=1.7480 -> {plan:?}");
        match plan {
            SellPlan::Sell(q) => {
                assert_eq!(q, dec!(1.74), "min(1.75, 1.748).trunc(2) = 1.74");
                assert!(q.scale() <= 2);
            }
            other => panic!("expected Sell(1.74), got {other:?}"),
        }
    }

    // Quick sanity: the helper `plan_sell_from_rest` accepts SdkDec, so this
    // build/link check confirms types align with the live_close call site.
    #[test]
    fn f1_plan_sell_compiles_with_sdk_decimal() {
        let q: SdkDec = dec!(1.69);
        let positions: Vec<PositionInfo> = vec![pos("X", 1.69)];
        let _: SellPlan = plan_sell_from_rest(q, &positions, "X");
    }

    // ============================================================================
    // G2 (Phantom retry): re-poll /positions before declaring Phantom.
    //
    // Defends against the data-api lag: a fresh BUY can take seconds to surface
    // on /positions even though the CLOB response confirmed it. With G1 (regla_c
    // off) the close fires at 120/300 s post-BUY, so /positions is usually
    // settled -- but edge cases remain. Retry gives the indexer 2.5 s × (N-1)
    // extra time before giving up.
    //
    // Two layers tested:
    //   * PURE  : pick_best_pos_shares -- snapshot-sequence decision logic
    //   * ASYNC : drive_phantom_retry  -- the live loop body, stub fetcher
    //
    // The async tests use backoff_ms=1 (1 ms total wait) so the suite runs fast
    // without needing tokio's `test-util` feature for paused-time.
    // ============================================================================

    fn tmp_oplog(tag: &str) -> (PathBuf, Arc<OpLog>) {
        let p = std::env::temp_dir().join(format!(
            "rb_g2_{tag}_{}_{}.jsonl",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        let _ = std::fs::remove_file(&p);
        (p.clone(), Arc::new(OpLog::new(p)))
    }

    fn read_oplog_kinds(path: &std::path::Path) -> Vec<(String, serde_json::Value)> {
        let body = std::fs::read_to_string(path).unwrap_or_default();
        body.lines()
            .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
            .map(|v| (
                v["kind"].as_str().unwrap_or("").to_string(),
                v["data"].clone(),
            ))
            .collect()
    }

    // ---------- PURE: pick_best_pos_shares ----------

    #[test]
    fn g2_pick_best_empty_snapshots_returns_none() {
        let result = pick_best_pos_shares(&[], "TOK");
        assert_eq!(result, None, "empty snapshots -> None");
    }

    #[test]
    fn g2_pick_best_all_token_absent_returns_none() {
        // 3 snapshots, none has the token => None (decide_sell: Phantom).
        let snaps = vec![
            vec![pos("OTHER", 5.0)],
            vec![],
            vec![pos("ALSO-OTHER", 1.0)],
        ];
        let result = pick_best_pos_shares(&snaps, "TOK");
        assert_eq!(result, None, "3 attempts, token never present -> None");
    }

    #[test]
    fn g2_pick_best_all_zero_returns_zero() {
        // 3 snapshots, all have token=TOK with size=0 => Some(0) (still Phantom).
        let snaps = vec![
            vec![pos("TOK", 0.0)],
            vec![pos("TOK", 0.0)],
            vec![pos("TOK", 0.0)],
        ];
        let result = pick_best_pos_shares(&snaps, "TOK");
        assert_eq!(result, Some(dec!(0)), "all zero -> Some(0) (still Phantom via decide_sell)");
    }

    #[test]
    fn g2_pick_best_settled_on_third_attempt() {
        // Snapshots [absent, absent, settled] -> early returns Some(positive)
        // on the third snapshot. THE CANONICAL CASE this whole feature exists for.
        let snaps = vec![
            vec![], // attempt 1: indexer hasn't caught up
            vec![pos("TOK", 0.0)], // attempt 2: still pending (zero seen)
            vec![pos("TOK", 2.14)], // attempt 3: settled
        ];
        let result = pick_best_pos_shares(&snaps, "TOK");
        assert_eq!(result, Some(dec!(2.14)),
            "G2 SCENARIO: indexer settles at attempt 3 -> SELL proceeds (not Phantom)");
    }

    #[test]
    fn g2_pick_best_positive_first_returns_immediately() {
        // No retry needed; first snapshot already has positive shares.
        let snaps = vec![
            vec![pos("TOK", 2.14)],
            vec![pos("TOK", 5.0)], // wouldn't be reached
        ];
        let result = pick_best_pos_shares(&snaps, "TOK");
        assert_eq!(result, Some(dec!(2.14)),
            "first attempt positive -> early-return; later attempts ignored");
    }

    #[test]
    fn g2_pick_best_zero_then_positive_returns_positive() {
        // [zero, positive] -> Some(positive). The zero was indexer "in flight".
        let snaps = vec![
            vec![pos("TOK", 0.0)],
            vec![pos("TOK", 1.69)],
        ];
        let result = pick_best_pos_shares(&snaps, "TOK");
        assert_eq!(result, Some(dec!(1.69)),
            "zero observation then positive -> use the positive (settled)");
    }

    // ---------- ASYNC: drive_phantom_retry with scripted stub ----------

    /// Fast retry policy for tests: 3 attempts, 1 ms backoff (= 2 ms total).
    /// Keeps the live default's 3 attempts so the loop's CONTROL FLOW is
    /// exercised exactly; only the sleep wait is shortened to keep tests fast.
    fn fast_policy() -> PhantomRetryPolicy {
        PhantomRetryPolicy { max_attempts: 3, backoff_ms: 1 }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn g2_drive_phantom_retry_settles_on_third_attempt() {
        // GIVEN: a stubbed /positions fetcher that returns empty, empty,
        // then a settled position on the 3rd attempt.
        let (path, oplog) = tmp_oplog("settled3");
        let scripted: Vec<anyhow::Result<Vec<PositionInfo>>> = vec![
            Ok(vec![]),
            Ok(vec![]),
            Ok(vec![pos("TOK", 2.14)]),
        ];
        let mut iter = scripted.into_iter();
        let result = drive_phantom_retry(
            "TOK", "sig-1", fast_policy(), &oplog,
            move |_attempt| {
                let next = iter.next().expect("scripted not exhausted");
                async move { next }
            },
        ).await;
        // THEN: SELL proceeds with the settled shares (NOT Phantom).
        assert_eq!(result, Some(dec!(2.14)),
            "G2: 3rd-attempt settle -> SELL proceeds with on-chain shares");

        // AND: the oplog records 2 retry events (attempts 1+2) and 1 settle
        // event (attempt 3). No final Phantom.
        let kinds = read_oplog_kinds(&path);
        let retry_n = kinds.iter().filter(|(k, _)| k == "live_close_phantom_retry").count();
        let settled_n = kinds.iter().filter(|(k, _)| k == "live_close_pos_settled").count();
        println!("[G2-test] settle@3: oplog kinds = {:?}",
            kinds.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>());
        assert_eq!(retry_n, 2, "expected 2 retry events (attempts 1, 2 with no shares)");
        assert_eq!(settled_n, 1, "expected 1 settle event (attempt 3 success)");
        // Settle event must carry attempt=3 + shares=2.14.
        let settle_data = &kinds.iter()
            .find(|(k, _)| k == "live_close_pos_settled").unwrap().1;
        assert_eq!(settle_data["attempt"], 3);
        assert_eq!(settle_data["shares"], "2.14");
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn g2_drive_phantom_retry_all_null_returns_phantom() {
        // GIVEN: all 3 attempts return empty /positions.
        let (path, oplog) = tmp_oplog("phantom3");
        let scripted: Vec<anyhow::Result<Vec<PositionInfo>>> = vec![
            Ok(vec![]),
            Ok(vec![]),
            Ok(vec![]),
        ];
        let mut iter = scripted.into_iter();
        let result = drive_phantom_retry(
            "TOK", "sig-2", fast_policy(), &oplog,
            move |_attempt| {
                let next = iter.next().expect("scripted not exhausted");
                async move { next }
            },
        ).await;
        // THEN: None -> live_close will produce SellPlan::Phantom (correct: give up).
        assert_eq!(result, None,
            "G2: all attempts null -> None -> Phantom verdict (correct to give up)");

        // AND: 3 retry events, NO settle event.
        let kinds = read_oplog_kinds(&path);
        let retry_n = kinds.iter().filter(|(k, _)| k == "live_close_phantom_retry").count();
        let settled_n = kinds.iter().filter(|(k, _)| k == "live_close_pos_settled").count();
        println!("[G2-test] all-null: oplog kinds = {:?}",
            kinds.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>());
        assert_eq!(retry_n, 3, "expected 3 retry events (all attempts had no shares)");
        assert_eq!(settled_n, 0, "no settle event when all null");
        // Last retry event must mark will_retry=false (final attempt).
        let last_retry = kinds.iter()
            .filter(|(k, _)| k == "live_close_phantom_retry")
            .last().unwrap();
        assert_eq!(last_retry.1["attempt"], 3);
        assert_eq!(last_retry.1["will_retry"], false,
            "final attempt's event must mark will_retry=false");
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn g2_drive_phantom_retry_first_attempt_positive_no_extra_calls() {
        // GIVEN: first attempt already has the settled position.
        let (path, oplog) = tmp_oplog("fast1");
        let scripted: Vec<anyhow::Result<Vec<PositionInfo>>> = vec![
            Ok(vec![pos("TOK", 1.69)]),
            // Attempts 2 and 3 would panic if called -- they're not scripted.
        ];
        let mut iter = scripted.into_iter();
        let result = drive_phantom_retry(
            "TOK", "sig-3", fast_policy(), &oplog,
            move |attempt| {
                let next = iter.next()
                    .unwrap_or_else(|| panic!("attempt {attempt} should NOT have been made"));
                async move { next }
            },
        ).await;
        // THEN: early return on attempt 1, no further fetches.
        assert_eq!(result, Some(dec!(1.69)),
            "G2: first attempt positive -> early return, no further /positions calls");

        // AND: oplog has exactly 1 settle event, 0 retry events.
        let kinds = read_oplog_kinds(&path);
        let retry_n = kinds.iter().filter(|(k, _)| k == "live_close_phantom_retry").count();
        let settled_n = kinds.iter().filter(|(k, _)| k == "live_close_pos_settled").count();
        assert_eq!(retry_n, 0, "no retry events when first attempt succeeds");
        assert_eq!(settled_n, 1, "exactly 1 settle event");
        let settle = kinds.iter().find(|(k, _)| k == "live_close_pos_settled").unwrap();
        assert_eq!(settle.1["attempt"], 1, "settled on attempt 1");
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn g2_drive_phantom_retry_fetch_error_retries_then_settles() {
        // GIVEN: attempt 1 errors (e.g. transient network), attempt 2 settles.
        let (path, oplog) = tmp_oplog("errthen");
        let scripted: Vec<anyhow::Result<Vec<PositionInfo>>> = vec![
            Err(anyhow::anyhow!("transient network error")),
            Ok(vec![pos("TOK", 2.14)]),
        ];
        let mut iter = scripted.into_iter();
        let result = drive_phantom_retry(
            "TOK", "sig-err", fast_policy(), &oplog,
            move |_attempt| {
                let next = iter.next().expect("scripted not exhausted");
                async move { next }
            },
        ).await;
        // THEN: the network error did NOT abort -- attempt 2 succeeded.
        assert_eq!(result, Some(dec!(2.14)),
            "G2: fetch error retries, doesn't abort the SELL path");

        // AND: oplog has 1 retry_err event + 1 settle event.
        let kinds = read_oplog_kinds(&path);
        let err_n = kinds.iter().filter(|(k, _)| k == "live_close_phantom_retry_err").count();
        let settled_n = kinds.iter().filter(|(k, _)| k == "live_close_pos_settled").count();
        assert_eq!(err_n, 1, "1 error event on attempt 1");
        assert_eq!(settled_n, 1, "1 settle event on attempt 2");
        let err_evt = kinds.iter().find(|(k, _)| k == "live_close_phantom_retry_err").unwrap();
        assert_eq!(err_evt.1["attempt"], 1);
        assert!(err_evt.1["error"].as_str().unwrap().contains("transient network"));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn g2_default_policy_matches_documented_values() {
        // The DECISIONS.md commentary references 3 attempts × 2500 ms backoff.
        // Catch silent changes that would invalidate the analysis.
        let p = PhantomRetryPolicy::default();
        assert_eq!(p.max_attempts, 3, "default attempts must be 3");
        assert_eq!(p.backoff_ms, 2500, "default backoff must be 2500 ms");
    }
}
