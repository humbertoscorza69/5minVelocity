//! Phase 6 execution helpers — pure + testable. The "read the REAL state, not a
//! proxy" fixes from the A2 round-trip post-mortems (two real attempts, two ~$1
//! losses, each a bug a guard caught before it could mis-trade):
//!
//!   1. SELL sizing from the confirmed fill, with on-chain corroboration. The order
//!      RESPONSE is authoritative for SIZE but NOT for EXISTENCE — CLOB v2 issue #54
//!      shows phantom fills on btc-updown-5m (success+matched+takingAmount, but
//!      CTF.balanceOf stays 0). So: size from the response, but REQUIRE `/positions`
//!      to corroborate (long timeout) before selling; no corroboration → phantom →
//!      do NOT sell.
//!   2. BUY vs SELL invert which leg is shares vs USDC (ctf-exchange CalculatorHelper):
//!      BUY  → makerAmount=USDC(collateral), takerAmount=shares(tokens);
//!      SELL → makerAmount=shares,           takerAmount=USDC.
//!      So in the RESPONSE: BUY filled shares = takingAmount; SELL filled shares =
//!      makingAmount, SELL USDC proceeds = takingAmount. (`takingAmount` is DECIMAL
//!      shares/usdc, NOT raw 6-decimal — confirmed vs py-clob-client #185.)
//!   3. Decimal precision: a SELL POST is rejected if share scale > 2 (the Rust SDK
//!      `Amount::shares` errors; py-clob-client's ROUNDING_CONFIG rounds size→2 for
//!      tick 0.01). Truncate shares to 2 places before the POST.
//!   4. Realized P&L from the real result — SELL fill proceeds (normal close) or the
//!      on-chain resolution outcome price (held-to-resolution). NEVER `/positions`
//!      curPrice (a mark value, not the settlement).

// Exposure accounting + P&L helpers are consumed by the live orchestrator in
// Sub-paso D (caps / reconcile / daily-loss-stop); tested now, wired there.
#![allow(dead_code)]

use rust_decimal::Decimal;

/// Lot precision: shares carried to at most this many decimals (SDK LOT_SIZE_SCALE).
pub const LOT_SCALE: u32 = 2;
/// Dust-snap threshold (NautilusTrader DUST_SNAP_THRESHOLD): a fill/position drift
/// below this many shares is normal protocol rounding → snap; at/above it, for a
/// FOK (all-or-nothing, no legitimate partial), the two sources disagree → a real
/// discrepancy (unit error / phantom partial) → do not sell a mis-sized amount.
pub fn dust() -> Decimal {
    Decimal::new(1, 2) // 0.01
}

// ===================== Fix 1+2+3: SELL sizing =====================

/// The decision for how to size (or skip) the SELL after a BUY.
#[derive(Debug, PartialEq, Eq)]
pub enum SellPlan {
    /// Sell exactly this many shares (already truncated to lot scale).
    Sell(Decimal),
    /// The BUY did not fill (killed / failed / not settled) → no position → no SELL.
    NoFill,
    /// Response says filled, but `/positions` never corroborated the position on-chain
    /// within the (long) timeout → likely a PHANTOM fill (issue #54) → nothing to
    /// sell; do NOT sell, escalate.
    Phantom,
    /// Response and `/positions` disagree by ≥ dust → do NOT sell a mis-sized amount.
    Mismatch,
}

/// Decide the SELL size after a BUY.
/// * `fill_confirmed` — response is a real fill: success && settled (tx) && shares>0.
/// * `resp_shares` — filled shares from the BUY response for THIS lot (takingAmount,
///   DECIMAL — no /1e6 rescale).
/// * `pos_shares` — `/positions` size after a LONG corroboration timeout (`None` = the
///   position never appeared → treat as phantom; do not sell on the response alone).
///   In the multi-lot model this is the AGGREGATE across ALL lots of the same token.
///
/// FOK is all-or-nothing, so the per-lot SELL must be safely backable by on-chain
/// shares. Returns Sell(`min(resp_shares, p)` truncated to lot scale) — never over-sells.
#[must_use]
pub fn decide_sell(fill_confirmed: bool, resp_shares: Decimal, pos_shares: Option<Decimal>) -> SellPlan {
    if !fill_confirmed {
        return SellPlan::NoFill;
    }
    match pos_shares {
        // Anti-phantom: never sell on the response alone — require on-chain proof.
        None => SellPlan::Phantom,
        Some(p) if p <= Decimal::ZERO => SellPlan::Phantom,
        Some(p) => {
            // G9-pre-A: under the multi-lot model (see decision/executor.rs:8-13),
            // the same token may have MULTIPLE OpenPosition lots in bs.positions
            // while /positions reports the AGGREGATE shares across all lots. The
            // pre-G9-pre-A check `abs(p - resp_shares) >= dust()` mis-flagged this
            // case as a bug -- e.g. lot1 says resp=2.18 but /positions=4.43 (=
            // lot1+lot2 aggregated) -> abs(2.25) >> dust -> Mismatch. In
            // production this blocked ~95% of SELLs (60+/73 closes one day) and
            // forced the bot to hold to resolution instead of executing its
            // time-exit strategy.
            //
            // The correct check has TWO distinct guards (each catching a real bug):
            //
            //   (a) Rescale-bug guard: `resp_shares < 0.1`.
            //       Derivation: stake_cap = $1.05 (Guards default) and Polymarket
            //       binary prices in (0, 1] -> min legitimate shares per BUY =
            //       1.05 (= $1.05 / $1.00). 0.1 is ~10x below that, giving safe
            //       headroom for any reasonable stake. A resp_shares below this
            //       floor (e.g. the F1 /1e6 case at line 288: resp=0.000002) is
            //       never a real BUY response. NOTE: if the operator ever lowers
            //       `stakes.base_usdc` below ~$0.10 in bot.toml, revisit this
            //       floor.
            //
            //   (b) Insufficient-balance guard: `p < resp_shares - dust()`.
            //       Genuine shortage of shares on-chain to cover THIS lot. Could
            //       be BUY that didn't land, severe indexer lag, or stale
            //       bs.positions entry. Refusing to over-sell is correct; the
            //       chain-side would reject the POST anyway, but a clean
            //       Mismatch outcome (logged, escalated) beats an exchange-
            //       rejected POST.
            //
            // The multi-lot case (`p > resp_shares + dust()`) is NORMAL and
            // passes BOTH guards -- we sell exactly resp_shares (this lot's
            // worth); the next lot's SELL will see the reduced /positions
            // (post-this-sell or, under indexer lag, still inflated) and
            // process its own resp_shares similarly. The chain enforces
            // non-negative balance so over-sell is impossible even under lag.
            if resp_shares < rust_decimal_macros::dec!(0.1) {
                return SellPlan::Mismatch;
            }
            if p < resp_shares - dust() {
                return SellPlan::Mismatch;
            }
            // Safe: p >= resp_shares (modulo dust). Sell exactly resp_shares;
            // `.min(p)` is belt-and-braces for the dust-tolerance band where
            // p is microscopically below resp_shares (e.g. 2.18 lot vs 2.179
            // reported under FP rounding / precision normalization).
            SellPlan::Sell(p.min(resp_shares).trunc_with_scale(LOT_SCALE))
        }
    }
}

// ===================== Fix 2: response field semantics =====================

/// Shares filled by a BUY = the response `takingAmount` (BUY receives tokens).
#[must_use]
pub fn buy_filled_shares(taking_amount: Decimal) -> Decimal {
    taking_amount
}
/// USDC spent by a BUY = the response `makingAmount` (BUY gives collateral).
#[must_use]
pub fn buy_usdc_spent(making_amount: Decimal) -> Decimal {
    making_amount
}
/// Shares sold by a SELL = the response `makingAmount` (SELL gives tokens). NOTE the
/// inversion vs BUY — using `takingAmount` here (the BUY field) would be the next bug.
#[must_use]
pub fn sell_filled_shares(making_amount: Decimal) -> Decimal {
    making_amount
}
/// USDC received by a SELL = the response `takingAmount` (SELL receives collateral).
#[must_use]
pub fn sell_usdc_received(taking_amount: Decimal) -> Decimal {
    taking_amount
}

// ===================== Fix: active-only exposure =====================

/// A position is RESOLVED (NOT open exposure) when it is redeemable, its outcome is
/// decided (curPrice ≈ 0 or ≈ 1), or its market already ended.
#[must_use]
pub fn is_resolved(redeemable: bool, cur_price: f64, end_in_past: bool) -> bool {
    redeemable || cur_price <= 0.001 || cur_price >= 0.999 || end_in_past
}

/// Per-position inputs for exposure accounting (from data-api `/positions`).
pub struct PosRow {
    pub token: String,
    pub size: f64,
    pub cur_price: f64,
    pub redeemable: bool,
    pub end_in_past: bool,
}

/// `(active_count, active_exposure_usd)` — resolved positions excluded (fix 2). Feeds
/// the TOTAL exposure cap ($25). Resolved-pending-redemption is NOT exposure.
#[must_use]
pub fn active_exposure(rows: &[PosRow]) -> (usize, f64) {
    rows.iter()
        .filter(|r| !is_resolved(r.redeemable, r.cur_price, r.end_in_past))
        .fold((0usize, 0.0f64), |(n, e), r| (n + 1, e + r.size * r.cur_price))
}

/// Active exposure (USD) of ONE token — resolved lots excluded. Feeds the per-token
/// cap ($5). Same active-only filter as `active_exposure`.
#[must_use]
pub fn active_exposure_for_token(rows: &[PosRow], token: &str) -> f64 {
    rows.iter()
        .filter(|r| r.token == token && !is_resolved(r.redeemable, r.cur_price, r.end_in_past))
        .map(|r| r.size * r.cur_price)
        .sum()
}

// ===================== PIECE 4: enrich PosRow from REST /positions =====================
//
// The live trading loop builds PosRows from LOCAL OpenPositions (state.json). That
// state has NO resolution info (the bot opens a lot; it doesn't know if the market
// later resolved). The data-api /positions DOES carry `redeemable` + `end_date`.
// `enrich_pos_rows_from_rest` overlays those fields onto matching rows so the
// active-only filter is precise (the "fix 2" intent fully delivered), not just the
// conservative curPrice in {0,1} proxy.

/// Overlay `redeemable` + `end_in_past` onto matching rows by token id, using the
/// REST /positions snapshot and today's UTC date. Rows whose token isn't in the
/// snapshot are left untouched (conservative fallback: data-api lag means a
/// just-opened lot may not yet appear).
pub fn enrich_pos_rows_from_rest(
    rows: &mut [PosRow],
    rest: &[crate::rest::PositionInfo],
    today_utc: polymarket_client_sdk_v2::types::NaiveDate,
) {
    use std::collections::HashMap;
    let map: HashMap<&str, &crate::rest::PositionInfo> =
        rest.iter().map(|p| (p.token_id.as_str(), p)).collect();
    for r in rows.iter_mut() {
        if let Some(p) = map.get(r.token.as_str()) {
            let (resolved, _why) = p.is_resolved_at(today_utc);
            // The combined resolution flag is folded into `redeemable` so the
            // existing `is_resolved(redeemable, cur_price, end_in_past)` chain
            // catches it. We also set `end_in_past` for traceability.
            if p.redeemable {
                r.redeemable = true;
            }
            if let Some(end) = p.end_date {
                if end < today_utc {
                    r.end_in_past = true;
                }
            }
            // Catch the extreme-price case explicitly too (defense in depth: if
            // r.cur_price hasn't been refreshed from REST, fold REST's cur_price).
            if resolved && p.cur_price > r.cur_price.max(0.999) {
                // REST sees a resolved extreme but local row didn't; use REST.
                r.cur_price = p.cur_price;
            }
        }
    }
}

/// Convenience: pure active-exposure directly over the REST snapshot. Returns
/// `(active_count, exposure_usd)` where exposure = Σ size × curPrice for non-resolved
/// positions. Use this when the source-of-truth is the REST snapshot itself
/// (e.g., a manual reconcile check).
#[must_use]
pub fn active_exposure_from_rest(
    rest: &[crate::rest::PositionInfo],
    today_utc: polymarket_client_sdk_v2::types::NaiveDate,
) -> (usize, f64) {
    rest.iter()
        .filter(|p| !p.is_resolved_at(today_utc).0)
        .fold((0usize, 0.0f64), |(n, e), p| (n + 1, e + p.size * p.cur_price))
}

/// Per-token variant of `active_exposure_from_rest`.
#[must_use]
pub fn active_exposure_for_token_from_rest(
    rest: &[crate::rest::PositionInfo],
    token: &str,
    today_utc: polymarket_client_sdk_v2::types::NaiveDate,
) -> f64 {
    rest.iter()
        .filter(|p| p.token_id == token && !p.is_resolved_at(today_utc).0)
        .map(|p| p.size * p.cur_price)
        .sum()
}

// ===================== Fix 4: realized P&L from the real result =====================

/// Normal close (SELL at +hold): proceeds collected − cost paid. Read both from the
/// order responses (SELL takingAmount − BUY makingAmount), per the field semantics.
#[must_use]
pub fn realized_pnl_sell(sell_proceeds: Decimal, buy_cost: Decimal) -> Decimal {
    sell_proceeds - buy_cost
}

/// Held-to-resolution: the held outcome's FINAL price (1 = won, 0 = lost) × shares,
/// minus the cost paid. Read `outcome_final_price` from on-chain / Gamma
/// `outcomePrices` — NEVER from `/positions` curPrice. `buy_cost` should be the NET
/// wallet cost (= `buy_wallet_cost`, fee-inclusive) for a fee-correct P&L.
#[must_use]
pub fn realized_pnl_resolution(outcome_final_price: Decimal, shares: Decimal, buy_cost: Decimal) -> Decimal {
    shares * outcome_final_price - buy_cost
}

// ===================== Fees: NET (wallet) vs gross (match) =====================
//
// The order response + data-api `/activity` + `/positions` all report the GROSS
// MATCH amount (`making`/`taking` = shares×price, pre-fee). The fee is taken
// on-chain, so only the WALLET MOVEMENT (balance delta / dashboard) is net. The bot
// must compute NET P&L (the truth that feeds the daily-loss-stop) — not the gross
// match. The fee formula below reconciles the A2 round-trip to the cent.

/// Polymarket trade fee, EXACT vs the A2 round-trip (Σfee observed $0.077980; this
/// formula $0.077990): **fee = 0.07 × shares × price × (1 − price)**.
/// NOT `0.07 × min(p,1−p) × shares` (the `live_execution_test` form — overstates ~2×).
#[must_use]
pub fn polymarket_fee(shares: Decimal, price: Decimal) -> Decimal {
    Decimal::new(7, 2) * shares * price * (Decimal::ONE - price)
}

/// NET USDC a BUY removes from the wallet = gross notional + fee (≈ dashboard "entry").
#[must_use]
pub fn buy_wallet_cost(shares: Decimal, price: Decimal) -> Decimal {
    shares * price + polymarket_fee(shares, price)
}

/// NET USDC a SELL adds to the wallet = gross notional − fee (≈ dashboard "proceeds").
#[must_use]
pub fn sell_wallet_proceeds(shares: Decimal, price: Decimal) -> Decimal {
    shares * price - polymarket_fee(shares, price)
}

/// NET realized P&L of a closed round trip = SELL wallet proceeds − BUY wallet cost.
/// This equals the real balance delta / dashboard — the number the daily-loss-stop
/// uses. (Gross match P&L overstates gains / understates losses by the fees.)
#[must_use]
pub fn realized_pnl_net(buy_shares: Decimal, buy_price: Decimal, sell_shares: Decimal, sell_price: Decimal) -> Decimal {
    sell_wallet_proceeds(sell_shares, sell_price) - buy_wallet_cost(buy_shares, buy_price)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    // ---- Fix 1+3: SELL sizing, validated against the REAL attempt-2 BUY response ----
    #[test]
    fn real_buy_response_sizes_sell_correctly() {
        // attempt 2: BUY takingAmount=2.142856 (DECIMAL shares, no /1e6), /positions=2.1428.
        let resp = dec!(2.142856); // = buy_filled_shares(takingAmount)
        let pos = Some(dec!(2.1428));
        // |2.142856 - 2.1428| = 0.000056 < 0.01 dust → sell min, trunc to lot scale 2.
        assert_eq!(decide_sell(true, resp, pos), SellPlan::Sell(dec!(2.14)));
    }
    #[test]
    fn old_unit_bug_caught_as_mismatch() {
        // the /1e6 bug: resp 0.000002142856 vs /positions 2.1428 → ≥dust → Mismatch.
        assert_eq!(decide_sell(true, dec!(0.000002142856), Some(dec!(2.1428))), SellPlan::Mismatch);
    }
    #[test]
    fn phantom_when_positions_never_corroborate() {
        // issue #54: response Matched but /positions stays empty after long timeout.
        assert_eq!(decide_sell(true, dec!(2.142856), None), SellPlan::Phantom);
        assert_eq!(decide_sell(true, dec!(2.142856), Some(dec!(0))), SellPlan::Phantom);
    }
    #[test]
    fn no_sell_when_buy_not_filled() {
        assert_eq!(decide_sell(false, dec!(2.14), Some(dec!(2.14))), SellPlan::NoFill);
    }
    #[test]
    fn dust_drift_snaps_never_oversells() {
        // tiny drift (< 0.01) → sell min, never more than held.
        assert_eq!(decide_sell(true, dec!(1.7500), Some(dec!(1.7480))), SellPlan::Sell(dec!(1.74)));
    }
    #[test]
    fn sell_amount_never_exceeds_lot_scale() {
        // whatever we return must have scale <= 2 (else the SDK rejects the POST).
        if let SellPlan::Sell(s) = decide_sell(true, dec!(2.142856), Some(dec!(2.142856))) {
            assert!(s.scale() <= LOT_SCALE, "sell scale {} > {LOT_SCALE}", s.scale());
        } else {
            panic!("expected Sell");
        }
    }

    // ---- G9-pre-A: multi-lot support tests ----
    //
    // Pre-G9-pre-A, decide_sell asserted `abs(p - resp_shares) >= dust()` as the
    // Mismatch condition. That broke under the multi-lot model (decision/executor.rs:8-13):
    // two BUYs of the same token produce two OpenPosition lots in bs.positions
    // but /positions reports the SINGLE aggregated entry. Result in production:
    // ~95% of SELLs Mismatch'd (60+/73 closes one day), the bot held to resolution
    // instead of executing time-exit, paper P&L diverged grotesquely from on-chain
    // reality. The G9-pre-A fix splits the check into (a) rescale-bug guard
    // (`resp_shares < 0.1`) + (b) insufficient-balance guard (`p < resp - dust`)
    // and allows `p > resp + dust` (= multi-lot) as the legitimate normal case.
    // These tests pin every relevant boundary.

    #[test]
    fn g9_pre_a_multi_lot_sells_this_lots_shares_when_pos_inflated_by_other_lots() {
        // (a) THE FIX: this lot says resp_shares=2.18; /positions reports p=4.43
        // (= this lot + ANOTHER lot of the same token, aggregated by Polymarket).
        // Pre-G9-pre-A: Mismatch (the production bug). Post: Sell(2.18).
        assert_eq!(
            decide_sell(true, dec!(2.18), Some(dec!(4.43))),
            SellPlan::Sell(dec!(2.18))
        );
    }

    #[test]
    fn g9_pre_a_single_lot_continues_to_work_when_resp_matches_pos_exactly() {
        // (b) Single-lot case (legacy dominant scenario): resp == pos exactly.
        // Must keep working post-fix.
        assert_eq!(
            decide_sell(true, dec!(2.18), Some(dec!(2.18))),
            SellPlan::Sell(dec!(2.18))
        );
    }

    #[test]
    fn g9_pre_a_mismatch_when_on_chain_has_far_less_than_lot_claims() {
        // (c) THE PROTECTION (opposite-direction shortage): this lot claims
        // 2.18 shares but /positions reports 0.000002 -- could be BUY that
        // didn't land, indexer lost the entry, etc. Refuse to over-sell.
        // The `p < resp_shares - dust()` guard catches this.
        assert_eq!(
            decide_sell(true, dec!(2.18), Some(dec!(0.000002))),
            SellPlan::Mismatch
        );
    }

    #[test]
    fn g9_pre_a_phantom_when_position_is_zero_even_with_multi_lot_sized_resp() {
        // (d) Even with multi-lot-sized resp_shares, p=0 means BOTH lots
        // phantom on-chain. The pos_shares <= 0 match arm catches this
        // BEFORE the new G9-pre-A checks (Phantom precedence preserved).
        assert_eq!(
            decide_sell(true, dec!(2.18), Some(Decimal::ZERO)),
            SellPlan::Phantom
        );
    }

    #[test]
    fn g9_pre_a_rescale_bug_still_caught_even_with_multi_lot_sized_position() {
        // (e) Belt-and-braces: the F1 /1e6 rescale bug protection (the existing
        // `old_unit_bug_caught_as_mismatch` test at line ~286) must keep firing
        // even now that we tolerate p > resp_shares for multi-lot. The new
        // `resp_shares < 0.1` floor catches this -- verify it explicitly with a
        // multi-lot-sized pos value (4.43) instead of single-lot pos (2.1428),
        // to assert the floor is independent of the pos size.
        assert_eq!(
            decide_sell(true, dec!(0.00005), Some(dec!(4.43))),
            SellPlan::Mismatch,
            "tiny resp_shares (rescale bug) with multi-lot-sized pos MUST still flag"
        );
    }

    // ---- Fix 2: BUY/SELL field inversion ----
    #[test]
    fn buy_sell_field_semantics_invert() {
        // BUY: taking=shares, making=USDC.
        assert_eq!(buy_filled_shares(dec!(2.142856)), dec!(2.142856));
        assert_eq!(buy_usdc_spent(dec!(1.05)), dec!(1.05));
        // SELL: making=shares, taking=USDC (inverted) — matches #185 (taking 0.9802 USDC, making 1.69 sh).
        assert_eq!(sell_filled_shares(dec!(1.69)), dec!(1.69));
        assert_eq!(sell_usdc_received(dec!(0.9802)), dec!(0.9802));
    }

    // ---- exposure ----
    #[test]
    fn exposure_counts_active_only() {
        let rows = vec![
            PosRow { token: "BTC1".into(), size: 2.0, cur_price: 0.50, redeemable: false, end_in_past: false }, // active $1.00
            PosRow { token: "ETH1".into(), size: 20.0, cur_price: 1.0, redeemable: true, end_in_past: true },    // resolved won
            PosRow { token: "ETH2".into(), size: 20.0, cur_price: 0.0, redeemable: true, end_in_past: true },    // resolved lost
            PosRow { token: "BTC1".into(), size: 3.0, cur_price: 0.40, redeemable: false, end_in_past: false }, // active $1.20
        ];
        let (n, e) = active_exposure(&rows);
        assert_eq!(n, 2);
        assert!((e - 2.20).abs() < 1e-9, "active exposure {e}");
        // per-token: both active BTC1 lots sum; resolved tokens contribute 0.
        assert!((active_exposure_for_token(&rows, "BTC1") - 2.20).abs() < 1e-9);
        assert_eq!(active_exposure_for_token(&rows, "ETH1"), 0.0);
    }
    #[test]
    fn all_resolved_means_zero_exposure() {
        let rows: Vec<PosRow> = (0..21)
            .map(|i| PosRow { token: format!("T{i}"), size: 20.0, cur_price: if i % 2 == 0 { 1.0 } else { 0.0 }, redeemable: true, end_in_past: true })
            .collect();
        assert_eq!(active_exposure(&rows), (0, 0.0));
    }

    // ---- Fix 4: P&L ----
    #[test]
    fn pnl_resolution_loss_matches_real_test() {
        // attempt-1 BTC 6:30-6:45: held Up, resolved 0, cost $1.08 → -$1.08.
        assert_eq!(realized_pnl_resolution(dec!(0), dec!(1.75), dec!(1.08)), dec!(-1.08));
    }
    #[test]
    fn pnl_resolution_win() {
        assert_eq!(realized_pnl_resolution(dec!(1), dec!(1.75), dec!(1.05)), dec!(0.70));
    }
    #[test]
    fn pnl_sell_normal_close() {
        // proceeds = SELL takingAmount, cost = BUY makingAmount.
        assert_eq!(realized_pnl_sell(sell_usdc_received(dec!(1.10)), buy_usdc_spent(dec!(1.05))), dec!(0.05));
    }

    // ---- fees: reconcile the REAL A2 round-trip to the cent ----
    #[test]
    fn fee_total_reconciles_real_round_trip() {
        // A2: BUY 2.282608 @ 0.46, SELL 2.28 @ 0.60. Observed gross−net = 0.077980.
        let total = polymarket_fee(dec!(2.282608), dec!(0.46)) + polymarket_fee(dec!(2.28), dec!(0.60));
        assert!((total - dec!(0.07798)).abs() < dec!(0.0001), "total fee {total}");
    }
    #[test]
    fn net_pnl_equals_real_balance_delta() {
        // NET P&L must equal the real balance delta +0.240021 (dashboard 1.33−1.09).
        let net = realized_pnl_net(dec!(2.282608), dec!(0.46), dec!(2.28), dec!(0.60));
        assert!((net - dec!(0.240021)).abs() < dec!(0.0005), "net {net}");
    }
    #[test]
    fn wallet_amounts_match_dashboard_cents() {
        // dashboard rounds to cents: entry $1.09, proceeds $1.33.
        assert_eq!(buy_wallet_cost(dec!(2.282608), dec!(0.46)).round_dp(2), dec!(1.09));
        assert_eq!(sell_wallet_proceeds(dec!(2.28), dec!(0.60)).round_dp(2), dec!(1.33));
    }
    #[test]
    fn net_pnl_is_below_gross_by_the_fees() {
        // gross match P&L = 1.368 − 1.04999 = 0.31801; net = gross − total_fee.
        let gross = dec!(1.368) - dec!(1.04999);
        let net = realized_pnl_net(dec!(2.282608), dec!(0.46), dec!(2.28), dec!(0.60));
        let fees = polymarket_fee(dec!(2.282608), dec!(0.46)) + polymarket_fee(dec!(2.28), dec!(0.60));
        assert!((gross - net - fees).abs() < dec!(0.0001), "gross {gross} net {net} fees {fees}");
    }

    // ===================== PIECE 4 enrichment tests =====================
    //
    // The 4 resolution paths (mirroring check_positions.py) + the 21+N scenario
    // (the real "21 open" miscount from A2) with REST-derived redeemable/end_date.

    use crate::rest::PositionInfo;
    use polymarket_client_sdk_v2::types::NaiveDate;

    fn pi(token: &str, size: f64, cur: f64, redeemable: bool, end: Option<(i32, u32, u32)>) -> PositionInfo {
        PositionInfo {
            token_id: token.into(),
            size, avg_price: cur, cur_price: cur, cash_pnl: 0.0,
            redeemable,
            end_date: end.and_then(|(y, m, d)| NaiveDate::from_ymd_opt(y, m, d)),
            condition_id: String::new(), // G5: unused by is_resolved_at; tests don't redeem.
        }
    }

    #[test]
    fn position_info_is_resolved_at_covers_all_four_paths() {
        let today = NaiveDate::from_ymd_opt(2026, 5, 30).unwrap();
        // 1. Active: not redeemable, mid price, future end_date.
        let active = pi("ACT", 10.0, 0.50, false, Some((2027, 1, 1)));
        assert_eq!(active.is_resolved_at(today), (false, "active"));
        // 2. Resolved by redeemable.
        let red = pi("RED", 10.0, 0.50, true, Some((2027, 1, 1)));
        assert_eq!(red.is_resolved_at(today), (true, "redeemable"));
        // 3. Resolved by extreme curPrice (won / lost).
        let won = pi("WON", 10.0, 1.0, false, Some((2027, 1, 1)));
        assert_eq!(won.is_resolved_at(today), (true, "cur_price_extreme"));
        let lost = pi("LOST", 10.0, 0.0, false, Some((2027, 1, 1)));
        assert_eq!(lost.is_resolved_at(today), (true, "cur_price_extreme"));
        // 4. Resolved by end_date in the past.
        let ended = pi("END", 10.0, 0.50, false, Some((2025, 12, 31)));
        assert_eq!(ended.is_resolved_at(today), (true, "end_in_past"));
        // Edge: missing end_date + otherwise-active -> active.
        let no_end = pi("NOEND", 10.0, 0.50, false, None);
        assert_eq!(no_end.is_resolved_at(today), (false, "active"));
    }

    #[test]
    fn enriched_filter_drops_21_resolved_and_counts_only_active() {
        // THE REAL "21 OPEN" miscount: 21 resolved + N=2 active. The enriched
        // filter MUST only count the 2 active towards exposure.
        let today = NaiveDate::from_ymd_opt(2026, 5, 30).unwrap();
        let mut rest = Vec::new();
        // 7 redeemable + 7 extreme-price + 7 ended_in_past = 21 resolved.
        for i in 0..7 {
            rest.push(pi(&format!("RED-{i}"), 50.0, 0.50, true, Some((2027, 1, 1))));
            rest.push(pi(&format!("EXT-{i}"), 50.0, if i % 2 == 0 { 1.0 } else { 0.0 }, false, Some((2027, 1, 1))));
            rest.push(pi(&format!("END-{i}"), 50.0, 0.50, false, Some((2024, 1, 1))));
        }
        // 2 genuinely active.
        rest.push(pi("ACT-1", 10.0, 0.40, false, Some((2027, 6, 1)))); // $4.00 active
        rest.push(pi("ACT-2", 5.0,  0.60, false, None));               // $3.00 active

        let (n, exposure) = active_exposure_from_rest(&rest, today);
        println!("[piece4-test] active count = {n}, exposure = ${exposure:.4}");
        assert_eq!(n, 2, "only the 2 ACT-* are active");
        assert!((exposure - 7.00).abs() < 1e-9, "exposure = $4 + $3 = $7; got ${exposure}");
        // Per-token: resolved tokens contribute 0; active tokens contribute their size*price.
        assert_eq!(active_exposure_for_token_from_rest(&rest, "RED-0", today), 0.0);
        assert!((active_exposure_for_token_from_rest(&rest, "ACT-1", today) - 4.00).abs() < 1e-9);
        assert!((active_exposure_for_token_from_rest(&rest, "ACT-2", today) - 3.00).abs() < 1e-9);
    }

    #[test]
    fn enrich_pos_rows_overlays_rest_resolution_onto_local_rows() {
        // Pre-enrich: a local row built from state knows only the bot's intent
        // side -- redeemable=false, end_in_past=false (the status quo before
        // piece 4). After enrichment from REST, the resolved tokens flip and
        // active_exposure correctly drops them.
        let today = NaiveDate::from_ymd_opt(2026, 5, 30).unwrap();
        let mut rows = vec![
            // Local view: all "active" (the conservative default).
            PosRow { token: "RED".into(), size: 50.0, cur_price: 0.50, redeemable: false, end_in_past: false },
            PosRow { token: "ACT".into(), size: 10.0, cur_price: 0.40, redeemable: false, end_in_past: false },
            PosRow { token: "END".into(), size: 20.0, cur_price: 0.50, redeemable: false, end_in_past: false },
            PosRow { token: "ORPHAN".into(), size: 1.0, cur_price: 0.5, redeemable: false, end_in_past: false }, // not in REST snapshot
        ];
        let rest = vec![
            pi("RED", 50.0, 0.50, true, Some((2027, 1, 1))),
            pi("ACT", 10.0, 0.40, false, Some((2027, 6, 1))),
            pi("END", 20.0, 0.50, false, Some((2024, 1, 1))),
        ];

        let (before_n, before_e) = active_exposure(&rows);
        println!("[piece4-test] BEFORE enrich: active={before_n}, exposure=${before_e:.4} (conservative all-active)");
        assert_eq!(before_n, 4, "pre-enrich: all 4 rows counted (conservative)");

        enrich_pos_rows_from_rest(&mut rows, &rest, today);

        // Post-enrich assertions:
        assert!(rows[0].redeemable, "RED should now be marked redeemable");
        assert!(!rows[1].redeemable && !rows[1].end_in_past, "ACT remains active");
        assert!(rows[2].end_in_past, "END should now be marked end_in_past");
        assert!(!rows[3].redeemable && !rows[3].end_in_past, "ORPHAN (not in REST) untouched -- conservative");

        let (after_n, after_e) = active_exposure(&rows);
        println!("[piece4-test] AFTER  enrich: active={after_n}, exposure=${after_e:.4} (precise)");
        // Active = ACT ($4) + ORPHAN ($0.5) = $4.5 ($25 RED + $10 END dropped).
        assert_eq!(after_n, 2, "ACT + ORPHAN active");
        assert!((after_e - 4.5).abs() < 1e-9, "$4 + $0.5 = $4.5; got ${after_e}");
    }
}
