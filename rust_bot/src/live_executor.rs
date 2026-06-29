//! Phase 6 Sub-paso D2 — the LiveExecutor (real-order mechanics), with a SHADOW
//! mode that does everything EXCEPT `post_order`.
//!
//! Extracted from the A2 round-trip (`live_test.rs`): build the FOK order via the
//! SDK builder (runs the SDK's precision/amount validation), sign it LOCALLY
//! (EIP-712 + ECDSA, no network — the A1 path), and then either STOP (shadow) or
//! POST (live). The Polymarket CLOB has NO dry-run endpoint, so "shadow" is
//! client-side: build + sign + compute everything + log the order that WOULD have
//! been sent (its real signed hash), and return before any network write.
//!
//! ZERO capital in shadow. The live POST path is reached only when `shadow=false`
//! AND the caller is the production bot (`--mode live`); the agent's manual dev path
//! is separately gated by `LIVE_ARMED` (guards::live_armed) — this module does not
//! re-implement that, it assumes the caller already passed its gate.
//!
//! SLIPPAGE (D2): the projected fill vs the send-time quote. The FOK worst-price is
//! a hard cap; the `max_slippage` threshold is the strategy's "is this still worth
//! it". Shadow computes the PROJECTED slippage against the current book and reports
//! whether the threshold would allow or abort — without posting.

#![allow(dead_code)] // shadow path wired by the trading loop (D2); live POST in D4

use anyhow::{Context, Result, anyhow, bail};
use polymarket_client_sdk_v2::auth::Signer;
use polymarket_client_sdk_v2::clob::types::response::PostOrderResponse;
use polymarket_client_sdk_v2::clob::types::{Amount, OrderStatusType, OrderType, Side, SignedOrder};
use polymarket_client_sdk_v2::types::{Decimal, U256};

use crate::rest::RestClient;

/// One side of an order to build (BUY=open, SELL=close).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderSide {
    Buy,
    Sell,
}
impl OrderSide {
    fn sdk(self) -> Side {
        match self {
            OrderSide::Buy => Side::Buy,
            OrderSide::Sell => Side::Sell,
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            OrderSide::Buy => "BUY",
            OrderSide::Sell => "SELL",
        }
    }
}

/// What to build: a BUY (amount=USDC) or a SELL (amount=shares), FOK at worst-price.
#[derive(Debug, Clone)]
pub struct OrderSpec {
    pub token_id: String,
    pub side: OrderSide,
    /// BUY: USDC notional. SELL: shares to sell. (The SDK `Amount` distinguishes.)
    pub amount: Decimal,
    /// FOK worst-price limit (BUY: 1−tick; SELL: tick).
    pub worst_price: Decimal,
    /// The quote the decision saw (BUY: best_ask; SELL: best_bid) — slippage anchor.
    pub quote_price: Decimal,
}

/// Slippage assessment for a projected fill.
#[derive(Debug, Clone, PartialEq)]
pub struct Slippage {
    /// Quote the decision/send saw.
    pub quote_price: f64,
    /// Current book price now (projected fill reference). For shadow this is the
    /// re-fetched book; for a real fill it's the response fill price.
    pub projected_fill: f64,
    /// Signed slippage = projected_fill − quote (BUY: positive = worse/more expensive).
    pub slippage: f64,
    /// Whether |adverse slippage| is within the acceptable threshold.
    pub within_threshold: bool,
    pub max_slippage: f64,
}

/// Adverse-direction slippage for a side: BUY is hurt when the price RISES above the
/// quote; SELL is hurt when it FALLS below. Returns the adverse magnitude (≥0 = the
/// amount it moved AGAINST us; negative = it moved in our favor).
#[must_use]
pub fn adverse_slippage(side: OrderSide, quote: f64, projected_fill: f64) -> f64 {
    match side {
        OrderSide::Buy => projected_fill - quote, // up = worse for a buy
        OrderSide::Sell => quote - projected_fill, // down = worse for a sell
    }
}

/// Assess slippage against the threshold. `within_threshold` = the adverse move does
/// not exceed `max_slippage` (favorable or zero always passes).
#[must_use]
pub fn assess_slippage(side: OrderSide, quote: f64, projected_fill: f64, max_slippage: f64) -> Slippage {
    let adverse = adverse_slippage(side, quote, projected_fill);
    // Epsilon: prices are cents on a 0.01 tick; float subtraction leaves ~1e-17 dust
    // (e.g. 0.52−0.50 = 0.02000000000000002). A 1e-9 tolerance keeps "exactly at the
    // threshold" passing without admitting a real over-threshold fill.
    Slippage {
        quote_price: quote,
        projected_fill,
        slippage: projected_fill - quote, // signed, BUY-frame; report adverse separately
        within_threshold: adverse <= max_slippage + 1e-9,
        max_slippage,
    }
}

// ============================================================================
// F3 (fix bug #3: slippage gate ciego). Two layers, both required:
//
//   1. PRE-POST (FOK hard limit): worst_price = quote ± max_slippage. The FOK
//      will NOT fill beyond this. If the book has walked past tolerance, the
//      order is KILLED (no fill) instead of taking an adverse fill at any
//      price up to $0.99 (the old behavior -- D4 trade A's smoking gun: quote
//      0.53, real fill 0.62, gate reported slip=0 because the FOK accepted
//      any price up to 0.99).
//   2. POST-FILL (autopsy): real_fill_price computed from the BUY/SELL
//      response (making/taking) and compared to quote. Logged to oplog so
//      the trade-log autopsy has the EXACT slippage that hit capital,
//      independent of the bot's ask snapshots.
//
// The two existing helpers above (`adverse_slippage` / `assess_slippage`) use
// f64 and operate on PROJECTED fills (ask-snapshot-based) -- kept for the
// pre-POST early-abort if the book has visibly walked before we even POST.
// The F3 helpers below operate on Decimal (no float drift on tick boundaries)
// and on the REAL fill (post-Posted) -- the FOK hard limit + autopsy.

/// F3 PURE: FOK worst_price for a BUY. `quote` = ask at decision; the FOK
/// will NOT fill above `quote + max_slippage`. Clamped to `1 - tick` (the
/// SDK's upper bound on any order price -- never returns > 0.99 on tick 0.01).
#[must_use]
pub fn compute_worst_price_buy(quote: Decimal, max_slippage: Decimal, tick: Decimal) -> Decimal {
    let upper = Decimal::ONE - tick;
    (quote + max_slippage).min(upper)
}

/// F3 PURE: FOK worst_price for a SELL. `quote` = bid at decision; the FOK
/// will NOT fill below `quote - max_slippage`. Floored at `tick`.
///
/// TRADEOFF: a strict worst_price means an adverse market sends the position
/// to RESOLUTION (FOK kills the SELL -> position held -> outcome decides).
/// For lag-arb's design "vender-en-hold" this is undesirable when avoiding
/// losses is the priority, but desirable when avoiding adverse fills is.
/// Relax `max_slippage` in config to widen tolerance if running into kills.
#[must_use]
pub fn compute_worst_price_sell(quote: Decimal, max_slippage: Decimal, tick: Decimal) -> Decimal {
    let lower = tick;
    (quote - max_slippage).max(lower)
}

/// PHASE B Pieza 5 — snap a price to the nearest valid tick (round-to-nearest),
/// in EXACT Decimal arithmetic. A maker limit MUST sit on the tick grid or
/// Polymarket's CLOB rejects it; snapping also kills any f64 representation
/// wobble that leaks in (e.g. an `entry+delta` computed in f64 as
/// 0.9900000000000001 -> the Decimal here is exact, but the snap is the
/// defensive belt for a future non-tick `delta`). With `tick == 0.01` this is
/// "round to 2 decimals". Returns `price` unchanged if `tick <= 0`.
#[must_use]
pub fn snap_to_tick(price: Decimal, tick: Decimal) -> Decimal {
    if tick <= Decimal::ZERO {
        return price;
    }
    (price / tick).round() * tick
}

/// F3 PURE: REAL BUY fill price from PostOrderResponse fields.
/// BUY: `making_amount` = USDC spent, `taking_amount` = shares received.
/// real_price = making / taking. Returns `None` on zero-fill (avoid div/0).
#[must_use]
pub fn real_fill_price_buy(making_amount: Decimal, taking_amount: Decimal) -> Option<Decimal> {
    if taking_amount.is_zero() { None } else { Some(making_amount / taking_amount) }
}

/// F3 PURE: REAL SELL fill price from PostOrderResponse fields.
/// SELL: `making_amount` = shares sold, `taking_amount` = USDC received.
/// real_price = taking / making. Returns `None` on zero-fill.
#[must_use]
pub fn real_fill_price_sell(making_amount: Decimal, taking_amount: Decimal) -> Option<Decimal> {
    if making_amount.is_zero() { None } else { Some(taking_amount / making_amount) }
}

/// F3 PURE: signed real slippage (adverse-positive convention).
/// BUY: real_price - quote (positive = paid more than quote = adverse).
/// SELL: quote - real_price (positive = received less than quote = adverse).
#[must_use]
pub fn real_adverse_slippage(side: OrderSide, quote: Decimal, real_fill: Decimal) -> Decimal {
    match side {
        OrderSide::Buy => real_fill - quote,
        OrderSide::Sell => quote - real_fill,
    }
}

/// The outcome of an order attempt through the LiveExecutor.
#[derive(Debug)]
pub enum ExecOutcome {
    /// SHADOW: built + signed, NOT posted. Carries the signed-order hash that WOULD
    /// have been sent + the projected slippage assessment.
    Shadow {
        order_hash: String,
        side: OrderSide,
        token_id: String,
        amount: Decimal,
        worst_price: Decimal,
        slippage: Slippage,
    },
    /// SLIPPAGE ABORT: projected fill worse than the threshold → not built/posted.
    SlippageAbort { slippage: Slippage },
    /// LIVE: actually posted. Carries the real response.
    Posted(Box<PostOrderResponse>),
    /// IDEMPOTENCY REFUSED: a prior intent for this client_order_id blocks a POST
    /// (in-flight / landed / needs-reconcile) -> nothing built or sent.
    IdempotencyRefused { coid: String, reason: String },
}

/// Builds + signs a FOK order via the SDK; in shadow STOPS before `post_order`.
/// `shadow=true` = ZERO network write (the safe D2 path).
///
/// The caller computes the [`Slippage`] (it has the projected fill from the live
/// book) and passes it; an over-threshold (adverse) slippage ABORTS before the build,
/// so the order is never constructed/signed/posted when the edge has decayed past the
/// threshold. Within-threshold (incl. favorable, incl. 0 when quote==fill) proceeds.
pub async fn build_sign_maybe_post<S: Signer>(
    rest: &RestClient,
    signer: &S,
    spec: &OrderSpec,
    slip: Slippage,
    shadow: bool,
) -> Result<ExecOutcome> {
    // 1. Slippage gate — abort BEFORE building if the projected fill is worse than the
    //    acceptable threshold (the strategy's "still worth it"; FOK worst-price is a
    //    separate hard cap).
    if !slip.within_threshold {
        return Ok(ExecOutcome::SlippageAbort { slippage: slip });
    }

    // 2. Build via the SDK (runs precision/amount validation — the A2 fix-3 path).
    let tok = U256::from_str_radix(&spec.token_id, 10)
        .map_err(|e| anyhow!("token_id parse: {e}"))?;
    let amount = match spec.side {
        OrderSide::Buy => Amount::usdc(spec.amount).map_err(|e| anyhow!("usdc amount: {e}"))?,
        OrderSide::Sell => Amount::shares(spec.amount).map_err(|e| anyhow!("shares amount: {e}"))?,
    };
    let built = rest.clob().market_order()
        .token_id(tok).side(spec.side.sdk())
        .amount(amount).price(spec.worst_price).order_type(OrderType::FOK)
        .build().await.map_err(|e| anyhow!("{} build: {e}", spec.side.as_str()))?;

    // 3. Sign LOCALLY (EIP-712 + ECDSA; no network — the A1 path).
    let signed = rest.clob().sign(signer, built).await
        .map_err(|e| anyhow!("{} sign: {e}", spec.side.as_str()))?;

    if shadow {
        // STOP before any network write. Report the order that WOULD have been sent.
        return Ok(ExecOutcome::Shadow {
            order_hash: signed_order_hash(&signed),
            side: spec.side,
            token_id: spec.token_id.clone(),
            amount: spec.amount,
            worst_price: spec.worst_price,
            slippage: slip,
        });
    }

    // 4. LIVE: actually POST. Reached only for the production bot path (shadow=false).
    let resp = rest.clob().post_order(signed).await
        .map_err(|e| anyhow!("{} post_order: {e}", spec.side.as_str()))?;
    Ok(ExecOutcome::Posted(Box::new(resp)))
}

/// PHASE B Pieza 1 — build + sign (+ maybe post) a REAL GTC + post_only MAKER
/// limit SELL (the B3 maker exit). It is the FIRST time the bot signs a MAKER
/// order; the FOK path so far was always a taker market order.
///
/// THE ONLY DIFFERENCES vs the FOK `build_sign_maybe_post` are in the builder:
///   FOK:   .market_order() .amount(Amount::shares) .order_type(OrderType::FOK)
///   MAKER: .limit_order()  .size(shares) .price(limit) .order_type(GTC) .post_only(true)
/// `sign` + `post_order` are BYTE-FOR-BYTE the same call: `order_type` and
/// `post_only` are NOT part of the EIP-712 signed hash (they are serialized
/// into the HTTP POST body AFTER signing). So this introduces NO new signing
/// logic — only new builder parameters. The SDK rejects `post_only` on a market
/// order and requires it to pair with GTC/GTD (order_builder.rs:328,525).
///
/// `limit_price` MUST already be tick-snapped by the caller (`snap_to_tick`).
/// In `shadow=true` it STOPS before `post_order` (the offline signing gate):
/// build (read-only tick fetch) + sign (local ECDSA + read-only neg_risk) and
/// return the order that WOULD have been sent — ZERO capital, no POST.
pub async fn build_sign_maybe_post_maker<S: Signer>(
    rest: &RestClient,
    signer: &S,
    token_id: &str,
    shares: Decimal,
    limit_price: Decimal,
    shadow: bool,
) -> Result<ExecOutcome> {
    let tok = U256::from_str_radix(token_id, 10)
        .map_err(|e| anyhow!("maker token_id parse: {e}"))?;

    // Build via the LIMIT builder (GTC + post_only). build() runs the SDK's
    // precision + tick-bound validation (the same gate the FOK path relies on).
    let built = rest.clob().limit_order()
        .token_id(tok).side(Side::Sell)
        .size(shares).price(limit_price)
        .order_type(OrderType::GTC).post_only(true)
        .build().await.map_err(|e| anyhow!("maker build: {e}"))?;

    // Sign LOCALLY — IDENTICAL to the FOK sign path (EIP-712 V2 + ECDSA).
    let signed = rest.clob().sign(signer, built).await
        .map_err(|e| anyhow!("maker sign: {e}"))?;

    if shadow {
        // STOP before any network write — the offline signing gate.
        return Ok(ExecOutcome::Shadow {
            order_hash: signed_order_hash(&signed),
            side: OrderSide::Sell,
            token_id: token_id.to_string(),
            amount: shares,
            worst_price: limit_price,
            // A resting maker has no slippage notion; degenerate placeholder.
            slippage: Slippage {
                quote_price: 0.0, projected_fill: 0.0, slippage: 0.0,
                within_threshold: true, max_slippage: 0.0,
            },
        });
    }

    // LIVE: POST. Reached only for the production maker path (Pieza 2/3 wiring).
    let resp = rest.clob().post_order(signed).await
        .map_err(|e| anyhow!("maker post_order: {e}"))?;
    Ok(ExecOutcome::Posted(Box::new(resp)))
}

/// PHASE B Pieza 4 — map a REAL maker `PostOrderResponse` to the placement
/// outcome, via the PURE `classify_post_outcome` (sub-commit 1, verified).
///
/// CRITICAL — it READS `resp.success`. An `Ok(resp)` with `success == false` is
/// a REJECTION, not a fill: `post_order` returns `Ok` even for an
/// application-level rejection (the verdict is in `success` + `error_msg`, not a
/// network `Err`). The existing FOK path does NOT check `success` — it wraps any
/// `Ok(resp)` as Posted (a pre-existing bug, fixed in a separate commit); the
/// maker path MUST NOT repeat it.
///
/// The raw `error_msg` is passed THROUGH untouched — the pure function owns the
/// crossable allow-list; the wiring never pre-filters or transforms it. If a
/// real Polymarket crossable message uses words outside the allow-list it would
/// land in `RealError` (no-sell, fail-closed) and we would SEE it live; we do
/// NOT speculate on the wording here.
///
/// Action per outcome (wired by Pieza 2/3, NOT here):
///   Posted{order_id}    -> maker_exit = Some{order_id, target, Placed}; emit b3_maker_placed.
///   RejectedCrossable   -> taker-capture: FOK SELL at the bid (target already reachable = WIN).
///   RealError{text}     -> NO sell (fail-closed); emit error; retry next tick / fall to time-exit.
pub fn classify_maker_post(resp: &PostOrderResponse) -> crate::b3_maker::PostOutcome {
    crate::b3_maker::classify_post_outcome(
        resp.success,
        Some(resp.order_id.as_str()),
        resp.error_msg.as_deref(),
    )
}

/// PHASE B Pieza 2 — the SDK adapter: map the SDK `OrderStatusType` (from
/// `order(id)` / a post response) onto the pure `MakerOrderStatus` that feeds
/// `reconcile_decision` / `b3_poll_decision`. Total + 1:1 (verified at Step 0).
/// Lives at the SDK boundary so b3_maker.rs stays SDK-free.
#[must_use]
pub fn map_status(s: &OrderStatusType) -> crate::b3_maker::MakerOrderStatus {
    use crate::b3_maker::MakerOrderStatus as M;
    match s {
        OrderStatusType::Matched => M::Matched,
        OrderStatusType::Canceled => M::Cancelled,
        OrderStatusType::Unmatched => M::Unmatched,
        OrderStatusType::Live => M::Live,
        OrderStatusType::Delayed => M::Delayed,
        OrderStatusType::Unknown(_) => M::Unknown,
        // OrderStatusType is #[non_exhaustive]: any FUTURE SDK variant maps to
        // Unknown (non-terminal -> defer), the fail-safe direction.
        _ => M::Unknown,
    }
}

/// A stable hash/id for a signed order, for the shadow log (the "what would have been
/// sent"). Uses the signature bytes (deterministic given the signed order).
fn signed_order_hash(signed: &SignedOrder) -> String {
    use polymarket_client_sdk_v2::clob::types::OrderSignature;
    match &signed.signature {
        OrderSignature::Ecdsa(s) => format!("0x{}", alloy::hex::encode(s.as_bytes())),
        OrderSignature::Wrapped(w) => w.clone(),
        other => format!("{other:?}"),
    }
}

/// IDEMPOTENT order placement (the wired shadow/live path):
/// 1) GATE: read the write-ahead log; if any prior intent for `coid` blocks,
///    return `IdempotencyRefused` WITHOUT building anything.
/// 2) WRITE-AHEAD: append a `Pending` row BEFORE calling build+sign+(maybe)post.
///    This is the row a crash will leave behind to drive reconcile-before-resend.
/// 3) ATTEMPT: a single call to `build_sign_maybe_post`. NEVER blind-retry on error.
/// 4) TERMINAL TRANSITION: append the post-attempt status:
///    - `Posted(_)` -> `Submitted { order_id }`
///    - `Shadow { .. }` / `SlippageAbort { .. }` -> `Abandoned { reason }`
///    - build/sign/post `Err(_)` -> `Ambiguous { reason }` (caller MUST reconcile;
///       do NOT resend the same coid until reconcile resolves it).
pub async fn place_order_idempotent<S: Signer>(
    rest: &RestClient,
    signer: &S,
    spec: &OrderSpec,
    slip: Slippage,
    shadow: bool,
    coid: &str,
    epoch: i64,
    intent_log: &std::path::Path,
) -> Result<ExecOutcome> {
    use crate::idempotency::{GateDecision, IntentStatus, OrderIntent, idempotency_gate};
    use crate::state::now_ms;

    if let GateDecision::Refuse(status) = idempotency_gate(intent_log, coid) {
        return Ok(ExecOutcome::IdempotencyRefused {
            coid: coid.to_string(),
            reason: format!("prior intent blocks POST: {status:?}"),
        });
    }

    let pending = OrderIntent::new_pending(
        coid, &spec.token_id, spec.side.as_str(),
        &spec.amount.to_string(), &spec.worst_price.to_string(), epoch, now_ms(),
    );
    let _ = pending.append(intent_log);

    let outcome = match build_sign_maybe_post(rest, signer, spec, slip, shadow).await {
        Ok(o) => o,
        Err(e) => {
            let mut amb = pending.clone();
            amb.status = IntentStatus::Ambiguous { reason: format!("build/sign/post error: {e}") };
            let _ = amb.append(intent_log);
            return Err(e);
        }
    };

    let mut done = pending;
    done.status = match &outcome {
        ExecOutcome::Posted(r) => IntentStatus::Submitted { order_id: r.order_id.clone() },
        ExecOutcome::Shadow { .. } => IntentStatus::Abandoned { reason: "shadow: not posted".into() },
        ExecOutcome::SlippageAbort { .. } => IntentStatus::Abandoned { reason: "slippage abort: not posted".into() },
        ExecOutcome::IdempotencyRefused { .. } => unreachable!("gate already passed"),
    };
    let _ = done.append(intent_log);
    Ok(outcome)
}

/// D2 SHADOW SELF-TEST (`--d2-shadow-selftest`): DETERMINISTICALLY exercise the real
/// shadow path on a live token — build + sign the order that WOULD be sent — WITHOUT
/// posting. Read-only REST (tick/price) + LOCAL sign; `shadow=true` is hardcoded so
/// `post_order` is unreachable. ZERO capital. This is the on-demand D2 gate (the A1
/// methodology applied to the integrated executor) so the gate doesn't depend on a
/// probabilistic Binance trigger firing during a watch window.
///
/// Picks the first active discovered token, fetches its ask + tick, forces a BUY of
/// `stake_usd` at worst-price (1−tick), runs `build_sign_maybe_post(shadow=true)`, and
/// asserts the outcome is `Shadow` (never `Posted`). Prints the would-be order.
pub async fn run_shadow_selftest(config: &crate::config::Config, stake_usd: f64) -> Result<()> {
    use std::str::FromStr;
    use std::time::Duration;
    use polymarket_client_sdk_v2::POLYGON;
    use polymarket_client_sdk_v2::auth::LocalSigner;
    use crate::discovery::{RestTokenSource, TokenSource};
    use polymarket_client_sdk_v2::clob::types::Side as RestSide;

    let _ = dotenvy::dotenv();
    let _ = dotenvy::from_filename("rust_bot/.env");
    let getv = |k: &str| std::env::var(k).map_err(|_| anyhow!("missing .env var {k}"));
    let pk = getv("POLYMARKET_PRIVATE_KEY")?;
    let rest = RestClient::connect(
        &config.connections.polymarket_rest_url, "https://data-api.polymarket.com",
        &pk, &getv("POLYMARKET_API_KEY")?, &getv("POLYMARKET_API_SECRET")?,
        &getv("POLYMARKET_API_PASSPHRASE")?, &getv("POLYMARKET_FUNDER_ADDRESS")?,
        Duration::from_secs(15),
    ).await.context("shadow-selftest: REST connect")?;
    let signer = LocalSigner::from_str(&pk).context("shadow-selftest: signer")?
        .with_chain_id(Some(POLYGON));

    // Operational log (the firehose) — every API call + system event + error, ms-stamped.
    let oplog = crate::oplog::OpLog::default_path();
    oplog.sys("selftest_start", serde_json::json!({ "stake_usd": stake_usd, "max_slippage": config.rules.max_slippage }));

    // Discover one active token (read-only) — logged with latency.
    let http = reqwest::Client::builder().timeout(Duration::from_secs(15)).build()?;
    let source = RestTokenSource::new(
        http, config.markets.assets.clone(), config.markets.intervals.clone(),
        config.markets.discovery_lookahead,
        config.connections.polymarket_gamma_url.clone(),
        config.connections.polymarket_rest_url.clone(),
    );
    let t0 = oplog.api_call("gamma/discovery", "GET", serde_json::json!({ "assets": config.markets.assets, "intervals": config.markets.intervals }));
    let tokens = match source.discover().await {
        Ok(t) => { oplog.api_ok("gamma/discovery", t0, 200, serde_json::json!({ "tokens": t.len() })); t }
        Err(e) => { oplog.api_err("gamma/discovery", t0, None, &e.to_string()); return Err(e).context("shadow-selftest: discovery"); }
    };
    let token = tokens.into_iter().next()
        .ok_or_else(|| anyhow!("shadow-selftest: no active tokens discovered"))?;

    // --- DELIBERATE read-only ERROR-CAPTURE demo: tick_size on a bogus token id. The
    //     real API rejects it; we log the EXACT textual error verbatim (proving the
    //     api_err path preserves the unsummarized message). Read-only, zero capital. ---
    let bogus = U256::from(1u64);
    let te = oplog.api_call("clob/tick-size", "GET", serde_json::json!({ "token_id": "1", "note": "deliberate bogus id (error-capture demo)" }));
    match rest.clob().tick_size(bogus).await {
        Ok(t) => oplog.api_ok("clob/tick-size", te, 200, serde_json::json!({ "tick": t.minimum_tick_size.as_decimal().to_string(), "note": "bogus id unexpectedly accepted" })),
        Err(e) => oplog.api_err("clob/tick-size", te, None, &e.to_string()),
    }

    // Read-only price + tick (real token) — each logged with latency.
    let tp = oplog.api_call("clob/price", "GET", serde_json::json!({ "token_id": token, "side": "SELL" }));
    let ask = match rest.get_price(&token, RestSide::Sell).await {
        Ok(p) => { oplog.api_ok("clob/price", tp, 200, serde_json::json!({ "ask": p })); p }
        Err(e) => { oplog.api_err("clob/price", tp, None, &e.to_string()); 0.50 }
    };
    let tok_u = U256::from_str_radix(&token, 10).map_err(|e| anyhow!("token parse: {e}"))?;
    let tt = oplog.api_call("clob/tick-size", "GET", serde_json::json!({ "token_id": token }));
    let tick = match rest.clob().tick_size(tok_u).await {
        Ok(t) => { let d = t.minimum_tick_size.as_decimal(); oplog.api_ok("clob/tick-size", tt, 200, serde_json::json!({ "tick": d.to_string() })); d }
        Err(e) => { oplog.api_err("clob/tick-size", tt, None, &e.to_string()); return Err(anyhow!("tick_size: {e}")); }
    };
    let worst = Decimal::ONE - tick;
    let spec = OrderSpec {
        token_id: token.clone(),
        side: OrderSide::Buy,
        amount: Decimal::try_from(stake_usd).unwrap_or_default(),
        worst_price: worst,
        quote_price: Decimal::try_from(ask).unwrap_or_default(),
    };
    // Self-test: projected fill = the quote (no live drift modeled here) → slip 0.
    let slip = assess_slippage(OrderSide::Buy, ask, ask, config.rules.max_slippage);
    println!("[d2-selftest] token=…{} ask={ask:.4} tick={tick} worst_price={worst} stake=${stake_usd}",
        &token[token.len().saturating_sub(8)..]);

    // THE REAL SHADOW PATH — shadow=true hardcoded; post_order unreachable.
    let tb = oplog.api_call("clob/build+sign", "LOCAL", serde_json::json!({
        "token_id": token, "side": "BUY", "usdc": stake_usd, "worst_price": worst.to_string() }));
    // PIECE 1 wiring: route through `place_order_idempotent` so the write-ahead
    // intent log (selftest_intents.jsonl) records pending -> abandoned("shadow").
    // A fresh per-second coid keeps every selftest invocation independent.
    let now_s = crate::state::now_ms() / 1000;
    let coid = format!("selftest-{}-{}", &token[token.len().saturating_sub(8)..], now_s);
    let intent_log = std::path::PathBuf::from("data/live/selftest_intents.jsonl");
    let outcome = match place_order_idempotent(&rest, &signer, &spec, slip, true, &coid, now_s, &intent_log).await {
        Ok(o) => o,
        Err(e) => { oplog.err("place_order_idempotent", &e.to_string(), serde_json::json!({ "token_id": token, "coid": coid })); return Err(e); }
    };
    match outcome {
        ExecOutcome::Shadow { order_hash, side, token_id, amount, worst_price, slippage } => {
            oplog.api_ok("clob/build+sign", tb, 0, serde_json::json!({ "shadow": true, "would_post": false }));
            oplog.sys("shadow_would_send", serde_json::json!({
                "side": side.as_str(), "token_id": token_id, "usdc": amount.to_string(),
                "worst_price": worst_price.to_string(), "signed_order_hash": order_hash,
                "slippage": slippage.slippage, "within_threshold": slippage.within_threshold,
                "posted": false,
            }));
            println!("[d2-selftest] WOULD-SEND (NOT posted):");
            println!("  side={} token=…{} usdc=${amount} worst_price={worst_price}",
                side.as_str(), &token_id[token_id.len().saturating_sub(8)..]);
            println!("  signed_order_hash={order_hash}");
            println!("  slippage: quote={:.4} projected_fill={:.4} slip={:.4} within_threshold={} (max={:.4})",
                slippage.quote_price, slippage.projected_fill, slippage.slippage,
                slippage.within_threshold, slippage.max_slippage);
            println!("[d2-selftest] PASS — real order built + signed, NO POST (zero capital).");
            println!("[d2-selftest] oplog → {}", crate::oplog::OPLOG_PATH);
            Ok(())
        }
        ExecOutcome::SlippageAbort { slippage } => {
            oplog.sys("slippage_abort", serde_json::json!({
                "slip": slippage.slippage, "max": slippage.max_slippage, "posted": false }));
            println!("[d2-selftest] SLIPPAGE ABORT (no order): slip={:.4} > max={:.4}",
                slippage.slippage, slippage.max_slippage);
            Ok(())
        }
        ExecOutcome::IdempotencyRefused { coid, reason } => {
            oplog.sys("idempotency_refused", serde_json::json!({ "coid": coid, "reason": reason }));
            println!("[d2-selftest] IDEMPOTENCY REFUSED coid={coid} reason={reason}");
            Ok(())
        }
        ExecOutcome::Posted(_) => {
            oplog.err("shadow_selftest", "outcome was Posted in shadow mode", serde_json::json!({}));
            bail!("d2-selftest: outcome was Posted — shadow MUST NOT post! (safety violation)");
        }
    }
}

/// PHASE B — THE OFFLINE MAKER SIGNING GATE (`--maker-sign-selftest`).
///
/// The compressed-shadow replacement: confirm the bot can BUILD + SIGN a real
/// GTC + post_only MAKER limit SELL (the B3 exit) BEFORE the first real post —
/// WITHOUT posting (shadow=true is hardcoded -> `post_order` unreachable). It
/// exercises the SDK's limit-order build VALIDATION (precision + tick bounds on
/// the real market tick) and the maker SIGN path. ZERO capital, no LIVE_ARMED.
///
/// Read-only network: discovery + tick (no auth-write). Local ECDSA sign. The
/// price/size are NOMINAL-SAFE (entry 0.50 -> target snap(0.60)); we are testing
/// the build+sign of a maker, not placing a trade. Asserts `Shadow` (a `Posted`
/// here is a hard safety violation). Run it with your creds (.env); it never
/// posts and uses your key only to sign locally.
pub async fn run_maker_sign_selftest(config: &crate::config::Config) -> Result<()> {
    use std::str::FromStr;
    use std::time::Duration;
    use polymarket_client_sdk_v2::POLYGON;
    use polymarket_client_sdk_v2::auth::LocalSigner;
    use crate::discovery::{RestTokenSource, TokenSource};

    let _ = dotenvy::dotenv();
    let _ = dotenvy::from_filename("rust_bot/.env");
    let getv = |k: &str| std::env::var(k).map_err(|_| anyhow!("missing .env var {k}"));
    let pk = getv("POLYMARKET_PRIVATE_KEY")?;
    let rest = RestClient::connect(
        &config.connections.polymarket_rest_url, "https://data-api.polymarket.com",
        &pk, &getv("POLYMARKET_API_KEY")?, &getv("POLYMARKET_API_SECRET")?,
        &getv("POLYMARKET_API_PASSPHRASE")?, &getv("POLYMARKET_FUNDER_ADDRESS")?,
        Duration::from_secs(15),
    ).await.context("maker-selftest: REST connect")?;
    let signer = LocalSigner::from_str(&pk).context("maker-selftest: signer")?
        .with_chain_id(Some(POLYGON));

    let oplog = crate::oplog::OpLog::default_path();
    oplog.sys("maker_selftest_start", serde_json::json!({ "note": "GTC+post_only build+sign, shadow (no post)" }));

    // Discover one active token (read-only).
    let http = reqwest::Client::builder().timeout(Duration::from_secs(15)).build()?;
    let source = RestTokenSource::new(
        http, config.markets.assets.clone(), config.markets.intervals.clone(),
        config.markets.discovery_lookahead,
        config.connections.polymarket_gamma_url.clone(),
        config.connections.polymarket_rest_url.clone(),
    );
    let tokens = source.discover().await.context("maker-selftest: discovery")?;
    let token = tokens.into_iter().next()
        .ok_or_else(|| anyhow!("maker-selftest: no active tokens discovered"))?;
    let tok_u = U256::from_str_radix(&token, 10).map_err(|e| anyhow!("token parse: {e}"))?;

    // Real market tick (read-only) — the build validates the maker price against it.
    let tt = oplog.api_call("clob/tick-size", "GET", serde_json::json!({ "token_id": token }));
    let tick = match rest.clob().tick_size(tok_u).await {
        Ok(t) => { let d = t.minimum_tick_size.as_decimal(); oplog.api_ok("clob/tick-size", tt, 200, serde_json::json!({ "tick": d.to_string() })); d }
        Err(e) => { oplog.api_err("clob/tick-size", tt, None, &e.to_string()); return Err(anyhow!("tick_size: {e}")); }
    };

    // NOMINAL-SAFE maker params: entry 0.50, delta 0.10 -> target snap(0.60).
    let entry = Decimal::new(50, 2);   // 0.50
    let delta = Decimal::new(10, 2);   // 0.10
    let target = snap_to_tick(entry + delta, tick); // 0.60, on-grid
    let shares = Decimal::new(210, 2); // 2.10 shares (clean 2-dec, ~$1.05/0.50)
    println!("[maker-selftest] token=…{} tick={tick} entry={entry} delta={delta} -> target(snapped)={target} size={shares}",
        &token[token.len().saturating_sub(8)..]);

    // THE REAL MAKER BUILD + SIGN — shadow=true hardcoded; post_order unreachable.
    let tb = oplog.api_call("clob/build+sign", "LOCAL", serde_json::json!({
        "kind": "maker_GTC_post_only", "token_id": token, "side": "SELL",
        "size": shares.to_string(), "price": target.to_string() }));
    let outcome = build_sign_maybe_post_maker(&rest, &signer, &token, shares, target, true).await
        .map_err(|e| { oplog.err("build_sign_maybe_post_maker", &e.to_string(), serde_json::json!({ "token_id": token })); e })?;

    match outcome {
        ExecOutcome::Shadow { order_hash, side, token_id, amount, worst_price, .. } => {
            oplog.api_ok("clob/build+sign", tb, 0, serde_json::json!({ "shadow": true, "would_post": false }));
            oplog.sys("maker_shadow_would_send", serde_json::json!({
                "side": side.as_str(), "order_type": "GTC", "post_only": true,
                "token_id": token_id, "size": amount.to_string(),
                "limit_price": worst_price.to_string(), "signed_order_hash": order_hash, "posted": false,
            }));
            println!("[maker-selftest] WOULD-SEND MAKER (NOT posted):");
            println!("  side={} order_type=GTC post_only=true", side.as_str());
            println!("  token=…{} size={amount} limit_price={worst_price}",
                &token_id[token_id.len().saturating_sub(8)..]);
            println!("  signed_order_hash={order_hash}");
            println!("[maker-selftest] PASS — real GTC+post_only maker built + signed, NO POST (zero capital).");
            Ok(())
        }
        ExecOutcome::Posted(_) => {
            oplog.err("maker_selftest", "outcome was Posted in shadow mode", serde_json::json!({}));
            bail!("maker-selftest: outcome was Posted — shadow MUST NOT post! (safety violation)");
        }
        other => bail!("maker-selftest: unexpected outcome {other:?}"),
    }
}

// ============================================================================
// PHASE B WIRING — LiveMakerBackend (the REAL impl of MakerBackend)
// ============================================================================
//
// THE MOCK->REAL BOUNDARY. Every method translates the RAW SDK response into the
// EXACT domain type the MockBackend returned, using the ALREADY-VERIFIED pure
// functions (classify_maker_post: Pieza 4 vs real JSON; map_status: 1:1). The
// risk lives in (a) field extraction and (b) NETWORK ERRORS the mock never
// models -- handled FAIL-CLOSED: a network failure = "I don't know the state" =
// return Err (the orchestration defers / retries), NEVER fabricate a result.

/// Idempotent maker POST: write-ahead `Pending` (coid = maker_coid) BEFORE
/// build+sign+post, terminal `Submitted{order_id}` / `Ambiguous` after. Mirrors
/// `place_order_idempotent` but for the GTC+post_only maker.
pub async fn place_maker_idempotent<S: Signer>(
    rest: &RestClient, signer: &S,
    signal_id: &str, token_id: &str, shares: Decimal, target: Decimal,
    epoch: i64, intent_log: &std::path::Path,
) -> Result<ExecOutcome> {
    use crate::idempotency::{GateDecision, IntentStatus, OrderIntent, idempotency_gate};
    use crate::state::now_ms;
    let coid = crate::b3_live::maker_coid(signal_id, target);
    if let GateDecision::Refuse(status) = idempotency_gate(intent_log, &coid) {
        return Ok(ExecOutcome::IdempotencyRefused { coid, reason: format!("prior intent blocks maker POST: {status:?}") });
    }
    let pending = OrderIntent::new_pending(&coid, token_id, "SELL", &shares.to_string(), &target.to_string(), epoch, now_ms());
    let _ = pending.append(intent_log);
    let outcome = match build_sign_maybe_post_maker(rest, signer, token_id, shares, target, false).await {
        Ok(o) => o,
        Err(e) => {
            let mut amb = pending.clone();
            amb.status = IntentStatus::Ambiguous { reason: format!("maker build/sign/post error: {e}") };
            let _ = amb.append(intent_log);
            return Err(e); // fail-closed: the caller maps Err -> RealError (no maker set)
        }
    };
    let mut done = pending;
    done.status = match &outcome {
        ExecOutcome::Posted(r) => IntentStatus::Submitted { order_id: r.order_id.clone() },
        _ => IntentStatus::Abandoned { reason: "maker: not posted".into() },
    };
    let _ = done.append(intent_log);
    Ok(outcome)
}

use crate::b3_live::{CancelResp, MakerBackend, OpenOrderRow};
use crate::b3_maker::{MakerOrderStatus, PostOutcome};

/// The REAL MakerBackend: talks to Polymarket. Built once at boot (live mode);
/// the maker exit task + the reconcile-on-startup drive it.
pub struct LiveMakerBackend {
    pub rest: std::sync::Arc<RestClient>,
    /// The private key (the concrete `LocalSigner` is generic; like LiveBackend
    /// we hold the pk and build the signer per POST -- cheap, and only place/
    /// taker need it).
    pub pk: String,
    pub max_slippage: f64,
    pub intent_log: std::path::PathBuf,
    pub live_armed_path: std::path::PathBuf,
    pub epoch: i64,
}

impl LiveMakerBackend {
    fn signer(&self) -> Result<impl Signer + '_> {
        use std::str::FromStr;
        Ok(polymarket_client_sdk_v2::auth::LocalSigner::from_str(&self.pk)
            .map_err(|e| anyhow!("signer: {e}"))?
            .with_chain_id(Some(polymarket_client_sdk_v2::POLYGON)))
    }
}

#[async_trait::async_trait]
impl MakerBackend for LiveMakerBackend {
    async fn place_maker(&self, signal_id: &str, token_id: &str, shares: Decimal, target: Decimal) -> Result<PostOutcome> {
        // GATE: posting a NEW order is gated by LIVE_ARMED. Disarmed -> do NOT post.
        if !crate::guards::live_armed(&self.live_armed_path) {
            return Ok(PostOutcome::RealError { text: "disarmed: LIVE_ARMED absent".into() });
        }
        let signer = self.signer()?;
        match place_maker_idempotent(&self.rest, &signer, signal_id, token_id, shares, target, self.epoch, &self.intent_log).await {
            // Posted -> the SDK response decides (success/order_id/error_msg) via
            // the VERIFIED classify_maker_post (Pieza 4). Same output the mock gave.
            Ok(ExecOutcome::Posted(resp)) => Ok(classify_maker_post(&resp)),
            // Idempotency refused (a prior intent for this coid) -> fail-closed: do
            // NOT post; treat as a real error (no maker set, retry/F2 at timeout).
            Ok(ExecOutcome::IdempotencyRefused { reason, .. }) => Ok(PostOutcome::RealError { text: format!("idempotency: {reason}") }),
            Ok(other) => Ok(PostOutcome::RealError { text: format!("maker not posted: {other:?}") }),
            // NETWORK/build error -> RealError. The write-ahead log + the gate
            // prevent a double-post on retry; a landed-but-lost POST is recovered
            // by the reconcile-on-startup at the next boot.
            Err(e) => Ok(PostOutcome::RealError { text: format!("maker place failed: {e}") }),
        }
    }

    async fn poll_order(&self, order_id: &str) -> Result<(MakerOrderStatus, Decimal)> {
        // order(id) is the authoritative state. A network Err propagates -> the
        // orchestration's poll handler returns Nothing (defer, retry next tick).
        let resp = self.rest.clob().order(order_id).await
            .map_err(|e| anyhow!("order({order_id}): {e}"))?;
        Ok((map_status(&resp.status), resp.size_matched))
    }

    async fn cancel_then_reconcile(&self, order_id: &str) -> Result<(MakerOrderStatus, Decimal, CancelResp)> {
        // 1) Cancel (best-effort). The cancel response is INFORMATIONAL only.
        let cancel_resp = match self.rest.clob().cancel_order(order_id).await {
            Ok(r) => if r.canceled.iter().any(|c| c == order_id) { CancelResp::Canceled } else { CancelResp::NotCanceled },
            Err(_) => CancelResp::NotCanceled, // cancel failed -> order(id) is still the truth.
        };
        // 2) THE TRUTH: order(id). If THIS read fails, return Err -> the
        //    orchestration DEFERS (sells NOTHING). The size_matched ALWAYS comes
        //    from a successful order(id); we NEVER reconcile on an unknown state.
        let resp = self.rest.clob().order(order_id).await
            .map_err(|e| anyhow!("reconcile order({order_id}): {e}"))?;
        Ok((map_status(&resp.status), resp.size_matched, cancel_resp))
    }

    async fn taker_sell(&self, signal_id: &str, token_id: &str, shares: Decimal, bid: Decimal) -> Result<Decimal> {
        // GATE: a SELL POST is gated by LIVE_ARMED. Disarmed -> Err (defer).
        if !crate::guards::live_armed(&self.live_armed_path) {
            bail!("disarmed: LIVE_ARMED absent");
        }
        let max_slip = Decimal::try_from(self.max_slippage).unwrap_or_default();
        let worst = compute_worst_price_sell(bid, max_slip, Decimal::try_from(crate::b3_maker::TICK).unwrap());
        let spec = OrderSpec { token_id: token_id.into(), side: OrderSide::Sell, amount: shares, worst_price: worst, quote_price: bid };
        let bid_f = bid.to_string().parse::<f64>().unwrap_or(0.0);
        let slip = assess_slippage(OrderSide::Sell, bid_f, bid_f, self.max_slippage); // projected=quote -> within
        let coid = format!("{signal_id}-f2"); // one F2 per position lifetime (idempotent)
        let signer = self.signer()?;
        match place_order_idempotent(&self.rest, &signer, &spec, slip, false, &coid, self.epoch, &self.intent_log).await {
            Ok(ExecOutcome::Posted(resp)) => {
                // CHECK success (the FOK bug): a killed FOK returns success=false.
                if !resp.success {
                    bail!("F2 FOK not filled: success=false err={:?}", resp.error_msg);
                }
                Ok(resp.taking_amount) // real USDC received for the SELL
            }
            Ok(other) => bail!("F2 not posted: {other:?}"),
            Err(e) => Err(e), // network -> defer; idempotency prevents a double-sell on retry
        }
    }

    async fn list_open_orders(&self) -> Result<Vec<OpenOrderRow>> {
        use polymarket_client_sdk_v2::clob::types::request::OrdersRequest;
        let req = OrdersRequest::default(); // all of my open orders
        let mut out = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let page = self.rest.clob().orders(&req, cursor.clone()).await
                .map_err(|e| anyhow!("orders(): {e}"))?;
            for o in &page.data {
                out.push(OpenOrderRow {
                    order_id: o.id.clone(),
                    token_id: o.asset_id.to_string(),
                    is_sell: o.side == Side::Sell,
                    price: o.price,
                    original_size: o.original_size,
                    size_matched: o.size_matched,
                });
            }
            // Polymarket signals "no more pages" with an empty or "LTE=" cursor.
            if page.next_cursor.is_empty() || page.next_cursor == "LTE=" {
                break;
            }
            cursor = Some(page.next_cursor);
        }
        Ok(out)
    }

    async fn cancel(&self, order_id: &str) -> Result<CancelResp> {
        // Risk-REDUCING -> NOT gated by LIVE_ARMED.
        let r = self.rest.clob().cancel_order(order_id).await
            .map_err(|e| anyhow!("cancel_order({order_id}): {e}"))?;
        Ok(if r.canceled.iter().any(|c| c == order_id) { CancelResp::Canceled } else { CancelResp::NotCanceled })
    }
}

/// READ-ONLY self-test of the mock->real boundary for `list_open_orders` (the
/// highest-risk read -- it feeds the reconcile). Lists YOUR real open orders via
/// the SDK and prints the parsed `OpenOrderRow`s. NO post, NO cancel, ZERO
/// capital, no LIVE_ARMED. Run it before the ARM to confirm the parsing is right.
pub async fn run_maker_list_orders_selftest(config: &crate::config::Config) -> Result<()> {
    let rest = connect_rest_for_selftest(config).await?;
    use polymarket_client_sdk_v2::clob::types::request::OrdersRequest;
    let req = OrdersRequest::default();
    let mut cursor: Option<String> = None;
    let mut n = 0;
    println!("[maker-list-selftest] listing open orders (read-only)...");
    loop {
        let page = rest.clob().orders(&req, cursor.clone()).await.context("orders()")?;
        for o in &page.data {
            n += 1;
            println!("  order_id={} token=…{} side={:?} price={} orig={} matched={}",
                o.id, &o.asset_id.to_string()[o.asset_id.to_string().len().saturating_sub(8)..],
                o.side, o.price, o.original_size, o.size_matched);
        }
        if page.next_cursor.is_empty() || page.next_cursor == "LTE=" { break; }
        cursor = Some(page.next_cursor);
    }
    println!("[maker-list-selftest] PASS — {n} open order(s) parsed (OpenOrderRow mapping confirmed vs real SDK).");
    Ok(())
}

/// READ-ONLY self-test of the mock->real boundary for `poll_order` / `order(id)`
/// (status + size_matched -- the fields that govern no-oversell). Fetches ONE
/// real order by id and prints the mapped `(MakerOrderStatus, size_matched)`. NO
/// post/cancel, ZERO capital, no LIVE_ARMED.
pub async fn run_maker_poll_selftest(config: &crate::config::Config, order_id: &str) -> Result<()> {
    let rest = connect_rest_for_selftest(config).await?;
    let resp = rest.clob().order(order_id).await.with_context(|| format!("order({order_id})"))?;
    let mapped = map_status(&resp.status);
    println!("[maker-poll-selftest] order_id={order_id}");
    println!("  raw status={:?} -> mapped MakerOrderStatus={:?}", resp.status, mapped);
    println!("  original_size={} size_matched={} price={}", resp.original_size, resp.size_matched, resp.price);
    println!("[maker-poll-selftest] PASS — status mapping + size_matched extraction confirmed vs real SDK.");
    Ok(())
}

/// Shared READ-ONLY REST connect for the maker self-tests (.env creds). No
/// signer needed -- both self-tests are read-only (orders/order).
async fn connect_rest_for_selftest(config: &crate::config::Config) -> Result<RestClient> {
    use std::time::Duration;
    let _ = dotenvy::dotenv();
    let _ = dotenvy::from_filename("rust_bot/.env");
    let getv = |k: &str| std::env::var(k).map_err(|_| anyhow!("missing .env var {k}"));
    let pk = getv("POLYMARKET_PRIVATE_KEY")?;
    let rest = RestClient::connect(
        &config.connections.polymarket_rest_url, "https://data-api.polymarket.com",
        &pk, &getv("POLYMARKET_API_KEY")?, &getv("POLYMARKET_API_SECRET")?,
        &getv("POLYMARKET_API_PASSPHRASE")?, &getv("POLYMARKET_FUNDER_ADDRESS")?,
        Duration::from_secs(15),
    ).await.context("maker-selftest: REST connect")?;
    Ok(rest)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buy_slippage_adverse_when_price_rises() {
        // BUY quoted 0.50, projected fill 0.53 → adverse +0.03.
        assert!((adverse_slippage(OrderSide::Buy, 0.50, 0.53) - 0.03).abs() < 1e-9);
        // price fell to 0.48 → favorable (negative adverse).
        assert!(adverse_slippage(OrderSide::Buy, 0.50, 0.48) < 0.0);
    }
    #[test]
    fn sell_slippage_adverse_when_price_falls() {
        // SELL quoted 0.50, projected fill 0.47 → adverse +0.03.
        assert!((adverse_slippage(OrderSide::Sell, 0.50, 0.47) - 0.03).abs() < 1e-9);
        assert!(adverse_slippage(OrderSide::Sell, 0.50, 0.52) < 0.0);
    }
    #[test]
    fn threshold_allows_within_aborts_beyond() {
        // max 0.02: +0.02 adverse passes (<=), +0.03 fails.
        assert!(assess_slippage(OrderSide::Buy, 0.50, 0.52, 0.02).within_threshold);
        assert!(!assess_slippage(OrderSide::Buy, 0.50, 0.53, 0.02).within_threshold);
        // favorable always passes.
        assert!(assess_slippage(OrderSide::Buy, 0.50, 0.48, 0.02).within_threshold);
    }
    #[test]
    fn zero_slippage_when_fill_equals_quote() {
        let s = assess_slippage(OrderSide::Buy, 0.50, 0.50, 0.02);
        assert_eq!(s.slippage, 0.0);
        assert!(s.within_threshold);
    }

    // ===================== Phase B Pieza 5: snap_to_tick =====================
    use rust_decimal_macros::dec;

    #[test]
    fn snap_already_on_grid_is_identity() {
        // The operative case: entry+delta is on-grid by construction (entry on
        // the 0.01 grid + delta=0.10) -> snap is a no-op that returns the tick.
        assert_eq!(snap_to_tick(dec!(0.99), dec!(0.01)), dec!(0.99));
        assert_eq!(snap_to_tick(dec!(0.60), dec!(0.01)), dec!(0.60));
        assert_eq!(snap_to_tick(dec!(0.75), dec!(0.01)), dec!(0.75));
        // entry 0.89 + delta 0.10 in EXACT Decimal = 0.99 (no f64 wobble).
        assert_eq!(snap_to_tick(dec!(0.89) + dec!(0.10), dec!(0.01)), dec!(0.99));
    }

    #[test]
    fn snap_off_grid_rounds_to_nearest_tick() {
        // Defensive case (a future non-tick delta or an off-grid input).
        assert_eq!(snap_to_tick(dec!(0.654), dec!(0.01)), dec!(0.65)); // down
        assert_eq!(snap_to_tick(dec!(0.656), dec!(0.01)), dec!(0.66)); // up
        // A value carrying f64-style wobble (slightly above 0.99) snaps to 0.99.
        assert_eq!(snap_to_tick(dec!(0.9900000001), dec!(0.01)), dec!(0.99));
        // Result is on the 2-decimal grid (scale handled).
        assert_eq!(snap_to_tick(dec!(0.6249), dec!(0.01)), dec!(0.62));
    }

    #[test]
    fn snap_respects_a_finer_tick() {
        // tick 0.001 (a thousandth market): round to 3 decimals.
        assert_eq!(snap_to_tick(dec!(0.6234), dec!(0.001)), dec!(0.623));
        assert_eq!(snap_to_tick(dec!(0.6236), dec!(0.001)), dec!(0.624));
    }

    #[test]
    fn snap_zero_tick_is_identity_no_panic() {
        // Defensive: a zero/negative tick must NOT div-by-zero; return as-is.
        assert_eq!(snap_to_tick(dec!(0.65), dec!(0)), dec!(0.65));
    }

    // ============ Phase B Pieza 4: classify_maker_post (real PostOrderResponse) ============
    // The responses are DESERIALIZED from the real Polymarket JSON shape
    // (camelCase, `orderID`, string amounts via empty_string_as_zero) -> this
    // exercises the actual API parse + the resp.success check + the pure
    // classify_post_outcome, end to end.
    use crate::b3_maker::PostOutcome;
    use polymarket_client_sdk_v2::clob::types::response::PostOrderResponse;

    fn por(json: &str) -> PostOrderResponse {
        serde_json::from_str(json).expect("PostOrderResponse parse")
    }

    #[test]
    fn maker_post_success_with_order_id_is_posted() {
        // success=true + a real orderID -> Posted (the wiring tracks the maker).
        let r = por(r#"{"errorMsg":null,"makingAmount":"2.10","takingAmount":"1.26","orderID":"0xMAKER1","status":"LIVE","success":true}"#);
        assert_eq!(classify_maker_post(&r), PostOutcome::Posted { order_id: "0xMAKER1".to_string() });
    }

    #[test]
    fn maker_post_crossable_rejection_is_rejected_crossable() {
        // success=false + a crossable error -> RejectedCrossable (taker-capture).
        let r = por(r#"{"errorMsg":"order would cross the book","makingAmount":"0","takingAmount":"0","orderID":"","status":"UNMATCHED","success":false}"#);
        assert_eq!(classify_maker_post(&r), PostOutcome::RejectedCrossable);
        // a post_only-worded rejection too.
        let r2 = por(r#"{"errorMsg":"rejected: post only order is marketable","makingAmount":"0","takingAmount":"0","orderID":"","status":"UNMATCHED","success":false}"#);
        assert_eq!(classify_maker_post(&r2), PostOutcome::RejectedCrossable);
    }

    #[test]
    fn maker_post_unknown_failure_is_real_error_fail_closed() {
        // success=false + a NON-crossable error -> RealError (NO sell, fail-closed).
        let r = por(r#"{"errorMsg":"insufficient balance","makingAmount":"0","takingAmount":"0","orderID":"","status":"UNMATCHED","success":false}"#);
        assert!(matches!(classify_maker_post(&r), PostOutcome::RealError { .. }), "got {:?}", classify_maker_post(&r));
    }

    #[test]
    fn maker_post_ok_with_success_false_no_errmsg_is_real_error() {
        // Ok(resp) but success=false and NO error text -> RealError (fail-closed):
        // a rejection is NOT a fill just because the POST returned Ok.
        let r = por(r#"{"errorMsg":null,"makingAmount":"0","takingAmount":"0","orderID":"","status":"UNMATCHED","success":false}"#);
        assert!(matches!(classify_maker_post(&r), PostOutcome::RealError { .. }));
    }

    #[test]
    fn maker_post_success_without_order_id_is_real_error() {
        // success=true but EMPTY orderID -> RealError: we cannot track/cancel an
        // order with no id, so do NOT assume it posted (sub-commit 1 design).
        let r = por(r#"{"errorMsg":null,"makingAmount":"0","takingAmount":"0","orderID":"","status":"LIVE","success":true}"#);
        assert!(matches!(classify_maker_post(&r), PostOutcome::RealError { .. }));
    }

    #[test]
    fn map_status_is_total_and_1to1() {
        use crate::b3_maker::MakerOrderStatus as M;
        assert_eq!(map_status(&OrderStatusType::Matched), M::Matched);
        assert_eq!(map_status(&OrderStatusType::Canceled), M::Cancelled);
        assert_eq!(map_status(&OrderStatusType::Unmatched), M::Unmatched);
        assert_eq!(map_status(&OrderStatusType::Live), M::Live);
        assert_eq!(map_status(&OrderStatusType::Delayed), M::Delayed);
        assert_eq!(map_status(&OrderStatusType::Unknown("weird".into())), M::Unknown);
    }

    // ===================== F3 (slippage hard-limit + real-fill autopsy) =====================

    #[test]
    fn f3_compute_worst_price_buy_quote_plus_tolerance() {
        // quote 0.53, max_slip 0.02, tick 0.01 -> worst = min(0.55, 0.99) = 0.55.
        let w = compute_worst_price_buy(dec!(0.53), dec!(0.02), dec!(0.01));
        assert_eq!(w, dec!(0.55));
        println!("[F3-test] BUY quote=0.53 + max_slip=0.02 (tick=0.01) -> worst={w}");
    }

    #[test]
    fn f3_compute_worst_price_buy_d4_trade_a_would_have_killed_fok() {
        // D4 trade A reproduction: quote=0.53, tolerance=0.02 -> worst=0.55.
        // The actual on-chain fill was 0.62 (book walked during POST). With
        // worst=0.55, the FOK would have NOT matched (no shares <= 0.55)
        // and the order would have been KILLED -- saving the $1.05 stake
        // from the 17% adverse fill.
        let worst = compute_worst_price_buy(dec!(0.53), dec!(0.02), dec!(0.01));
        let observed_fill_d4 = dec!(0.62);
        println!("[F3-test] D4 trade A: quote=0.53, worst={worst}, observed_fill={observed_fill_d4}");
        assert!(observed_fill_d4 > worst,
            "FOK at worst={worst} would have KILLED the order (no shares <= 0.55 in the book that walked to 0.62)");
    }

    #[test]
    fn f3_compute_worst_price_buy_clamps_at_one_minus_tick_for_huge_tolerance() {
        // Pathological tolerance > 1 -> clamped at 1 - tick.
        let w = compute_worst_price_buy(dec!(0.50), dec!(1.0), dec!(0.01));
        assert_eq!(w, dec!(0.99), "upper bound = 1 - tick");
    }

    #[test]
    fn f3_compute_worst_price_sell_quote_minus_tolerance() {
        // bid 0.53, max_slip 0.02, tick 0.01 -> worst = max(0.51, 0.01) = 0.51.
        let w = compute_worst_price_sell(dec!(0.53), dec!(0.02), dec!(0.01));
        assert_eq!(w, dec!(0.51));
        println!("[F3-test] SELL bid=0.53 - max_slip=0.02 (tick=0.01) -> worst={w}");
    }

    #[test]
    fn f3_compute_worst_price_sell_floors_at_tick_for_huge_tolerance() {
        // Pathological tolerance > bid -> floored at tick.
        let w = compute_worst_price_sell(dec!(0.05), dec!(0.10), dec!(0.01));
        assert_eq!(w, dec!(0.01), "lower bound = tick");
    }

    #[test]
    fn f3_real_fill_price_buy_from_response_d4_trade_a() {
        // D4 trade A reproduction from /activity: usdcSize=1.077919, size=1.693547
        // (paid 1.077919 USDC for 1.693547 shares). real_price = 1.077919 / 1.693547
        // = 0.63648... -- close to the 0.62 from /activity (rounding).
        // For the test, use exact numbers from BUY response convention:
        // making_amount=1.077919 USDC, taking_amount=1.693547 shares.
        let p = real_fill_price_buy(dec!(1.077919), dec!(1.693547)).unwrap();
        // Real fill ~ 0.6364 (vs 0.6199 from /activity rounding); the math holds.
        let quote_d4 = dec!(0.53);
        let slip = real_adverse_slippage(OrderSide::Buy, quote_d4, p);
        println!("[F3-test] D4 trade A: making=1.077919 taking=1.693547 -> real_fill={p}, adverse_slippage_vs_quote_0.53 = {slip}");
        assert!(p > dec!(0.60), "real fill > 0.60 (the 17% adverse the D4 oplog showed)");
        assert!(slip > dec!(0.05), "adverse slippage > 5c (D4 trade A was ~9c)");
    }

    #[test]
    fn f3_real_fill_price_zero_shares_returns_none_avoids_div_by_zero() {
        assert_eq!(real_fill_price_buy(dec!(1.05), dec!(0)), None);
        assert_eq!(real_fill_price_sell(dec!(0), dec!(1.05)), None);
    }

    #[test]
    fn f3_real_fill_price_sell_from_response() {
        // SELL: making=shares, taking=USDC. shares 2.0 -> received 1.04 USDC ->
        // real_price = 1.04/2.0 = 0.52.
        let p = real_fill_price_sell(dec!(2.0), dec!(1.04)).unwrap();
        assert_eq!(p, dec!(0.52));
        // Quoted 0.53, sold for 0.52 -> adverse 0.01.
        let slip = real_adverse_slippage(OrderSide::Sell, dec!(0.53), p);
        assert_eq!(slip, dec!(0.01));
        println!("[F3-test] SELL making=2.0 taking=1.04 -> real_fill=0.52, adverse_vs_bid_0.53 = {slip}");
    }

    #[test]
    fn f3_real_adverse_slippage_sign_convention() {
        // Adverse-positive: BUY paid more than quote = positive; SELL got less than quote = positive.
        // Favorable = negative.
        assert!(real_adverse_slippage(OrderSide::Buy, dec!(0.50), dec!(0.53)) > dec!(0), "BUY paid more = adverse positive");
        assert!(real_adverse_slippage(OrderSide::Buy, dec!(0.50), dec!(0.48)) < dec!(0), "BUY paid less = favorable negative");
        assert!(real_adverse_slippage(OrderSide::Sell, dec!(0.50), dec!(0.47)) > dec!(0), "SELL got less = adverse positive");
        assert!(real_adverse_slippage(OrderSide::Sell, dec!(0.50), dec!(0.52)) < dec!(0), "SELL got more = favorable negative");
    }
}
