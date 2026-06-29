//! Phase 6 Sub-paso A — isolated LiveExecutor (`--live-test`), the FIRST real
//! capital path. Replicates the validated `scripts/live_execution_test.py`
//! (POLY_PROXY sig_type=1, FOK market orders, worst-price limits, /positions
//! confirm) IN RUST, using the SDK's order builder + signer (signing certified
//! byte-exact vs Python at STOP A1). NOT integrated to the decision engine.
//!
//! MODES:
//!   * `--live-test` (default): ONE read-only PREFLIGHT pass (auth + balance +
//!     candidate + plan + cap). ZERO orders.
//!   * `--live-test --live-test-execute`: ONE attempt — preflight, then if a
//!     balanced candidate exists, the real BUY+SELL (else it refuses).
//!   * `+ --live-test-watch`: WATCH MODE. Loops the read-only discovery (≤30 min)
//!     until a balanced window appears, then fires EXACTLY ONE autonomous order
//!     (BUY+SELL) and HARD-STOPS — never a second order, never re-arms. This is the
//!     code-enforced "one autonomous order then stop": autonomy is ONLY over the
//!     timing of one test order; every capital guard stays strict.
//!
//! GUARDS (all strict, all kept): fresh re-pick each pass · ask∈[0.40,0.60] +
//! cap-abort ($5) re-checked BEFORE the POST · skew between detection and POST →
//! ABORT (no order), keep watching · CLEANUP-CRITICAL (BUY fills + SELL fails) →
//! STOP, report exact open position, NO retry · NO auto-retry of any order ·
//! HARD STOP after exactly one order POST.

use std::path::Path;
use std::str::FromStr;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use polymarket_client_sdk_v2::auth::{LocalSigner, Signer};
use polymarket_client_sdk_v2::clob::types::response::PostOrderResponse;
use polymarket_client_sdk_v2::clob::types::{Amount, OrderType, Side};
use polymarket_client_sdk_v2::types::{Decimal, U256};
use polymarket_client_sdk_v2::POLYGON;
use rust_decimal::prelude::ToPrimitive; // Decimal::to_f64 for reporting
use serde_json::{Value, json};

use crate::rest::RestClient;
use crate::trade_log::{Thresholds, TradeLog, now_ms};

/// Execution-log path (one JSONL row per order attempt; the trade-autopsy source).
const EXEC_LOG: &str = "data/live/execution_log.jsonl";

/// f64 helper for log fields.
fn df(d: Decimal) -> f64 {
    ToPrimitive::to_f64(&d).unwrap_or(0.0)
}

/// Seconds per interval (5m/15m only, the strategy universe).
fn interval_secs(interval: &str) -> Option<i64> {
    match interval {
        "5m" => Some(300),
        "15m" => Some(900),
        _ => None,
    }
}

/// Gamma event slug for an up/down market (matches the recorder/discovery format).
fn slug_for(asset: &str, interval: &str, epoch: i64) -> String {
    format!("{}-updown-{}-{}", asset.to_lowercase(), interval, epoch)
}

// ---- hard caps + candidate criteria (approved) ----
const MAX_RISK_USD: f64 = 5.0; // hardcoded abort: cost must be <= this BEFORE any POST
const STAKE_USD: f64 = 1.05; // BUY notional (> Polymarket $1 min, well under cap)
const ASK_LO: f64 = 0.40;
const ASK_HI: f64 = 0.60;
const MIN_TTR_S: i64 = 120; // A2-TEST candidate cushion ONLY (≈8× the ~15s round-trip).
// NOT the strategy's MIN_TTR (=30s, in the decision engine) — that is untouched.
const WATCH_MAX_MIN: u64 = 30; // hard cap on the read-only watch
const WATCH_INTERVAL_S: u64 = 60;
const CLOB_HOST: &str = "https://clob.polymarket.com";
const DATA_HOST: &str = "https://data-api.polymarket.com";
const GAMMA_HOST: &str = "https://gamma-api.polymarket.com";

struct Candidate {
    asset: &'static str,
    interval: &'static str,
    epoch: i64,
    token_id: String,
    ask: f64,
    bid: f64,
    ttr: i64,
}

/// Outcome of one discovery+(maybe)order pass.
enum PassResult {
    /// Read-only preflight printed; nothing more to do.
    Preflight,
    /// No balanced candidate this pass → (watch) keep looking.
    NoCandidate,
    /// Aborted BEFORE any POST (skew/cap) → no order placed → (watch) keep looking.
    SkewAbort(String),
    /// Exactly one order was POSTed and the round-trip completed clean → HARD STOP.
    Done,
}

pub async fn run_cli(execute: bool, watch: bool, out_dir: Option<&Path>) -> Result<()> {
    let out = out_dir
        .map(Path::to_path_buf)
        .unwrap_or_else(|| Path::new("data/derived/live_test").to_path_buf());
    std::fs::create_dir_all(&out)?;

    // ---- credentials (.env; presence already confirmed; values never logged) ----
    let _ = dotenvy::dotenv();
    let _ = dotenvy::from_filename("rust_bot/.env");
    let getv = |k: &str| std::env::var(k).map_err(|_| anyhow!("missing .env var {k}"));
    let pk = getv("POLYMARKET_PRIVATE_KEY")?;
    let api_key = getv("POLYMARKET_API_KEY")?;
    let api_secret = getv("POLYMARKET_API_SECRET")?;
    let api_pass = getv("POLYMARKET_API_PASSPHRASE")?;
    let funder_addr = getv("POLYMARKET_FUNDER_ADDRESS")?;
    println!("[live-test] .env creds loaded (5/5). mode: execute={execute} watch={watch}");

    let rest = RestClient::connect(
        CLOB_HOST, DATA_HOST, &pk, &api_key, &api_secret, &api_pass, &funder_addr,
        Duration::from_secs(15),
    )
    .await
    .context("RestClient::connect (real creds)")?;
    let signer = LocalSigner::from_str(&pk)
        .context("parsing private key for signer")?
        .with_chain_id(Some(POLYGON));
    let funder = rest.funder();
    let funder_tail = {
        let s = format!("{funder:?}");
        format!("…{}", &s[s.len().saturating_sub(6)..])
    };
    let bal = rest.get_balance().await.context("get_balance")?;
    let balance_before = bal.balance_usdc;
    println!("[live-test] funder={funder_tail} balance=${:.4} allowance_contracts={}",
        bal.balance_usdc, bal.allowance_contracts);

    // ---- ARMING GATE (agent dev path): real capital is OPT-IN ----
    // `--live-test-execute` is the AGENT's manual real-order path. It refuses to POST
    // anything unless the user has explicitly ARMED it by writing a non-empty
    // LIVE_ARMED.txt. This is the control added after the agent fired an unauthorized
    // real round-trip while "testing a flag": capital requires explicit arming, not
    // just the absence of a kill-switch. (The PRODUCTION bot — `--mode live` — is a
    // DIFFERENT path, armed once at launch, and is NOT gated per-order here.)
    let armed_path = std::path::Path::new(crate::guards::LIVE_ARMED_PATH);
    if execute && !crate::guards::live_armed(armed_path) {
        bail!(
            "REFUSING real-order path: not ARMED. `--live-test-execute` moves REAL capital and \
             requires explicit arming — write a non-empty {} to arm. (No order was attempted.) \
             This gate is for the agent's manual dev path only; the production bot is armed at launch.",
            crate::guards::LIVE_ARMED_PATH
        );
    }
    if execute {
        println!("[live-test] ARMED — {} present; real-order path enabled (agent dev path).",
            crate::guards::LIVE_ARMED_PATH);
    }

    // ---- E' guards ----
    let g_cfg = crate::guards::GuardConfig::default();
    let kill_path = g_cfg.kill_switch_path.clone();
    if crate::guards::kill_switch_active(&kill_path) {
        bail!("E' KILL-SWITCH present at startup ({}) - refusing to start.", kill_path.display());
    }

    let http = reqwest::Client::builder().timeout(Duration::from_secs(15)).build()?;
    let deadline = Instant::now() + Duration::from_secs(WATCH_MAX_MIN * 60);
    let mut pass = 0u32;
    loop {
        pass += 1;
        if watch {
            println!("\n[live-test] ===== watch pass {pass} =====");
        }
        match one_pass(&rest, &signer, &http, execute, &out, balance_before).await? {
            PassResult::Preflight => return Ok(()),
            PassResult::Done => {
                println!("[live-test] ONE order placed → HARD STOP (no second order).");
                return Ok(());
            }
            PassResult::NoCandidate => {
                if !watch {
                    bail!("no balanced candidate (ask∈[{ASK_LO},{ASK_HI}], ttr>{MIN_TTR_S}s) — markets rolling");
                }
            }
            PassResult::SkewAbort(why) => {
                println!("[live-test] aborted BEFORE any POST (no order): {why}");
                if !watch {
                    bail!("{why}");
                }
            }
        }
        // watch continuation (read-only) — bounded.
        if Instant::now() >= deadline {
            println!("\n[live-test] WATCH EXHAUSTED ({WATCH_MAX_MIN} min, {pass} passes) — no balanced window. ZERO orders placed.");
            return Ok(());
        }
        tokio::time::sleep(Duration::from_secs(WATCH_INTERVAL_S)).await;
    }
}

/// One discovery pass; in execute mode, fires AT MOST one order then returns Done.
async fn one_pass<S: Signer>(
    rest: &RestClient,
    signer: &S,
    http: &reqwest::Client,
    execute: bool,
    out: &Path,
    balance_before: f64,
) -> Result<PassResult> {
    let now_ts = clob_now(http).await.context("clob /time")?;
    let mut cands: Vec<Candidate> = Vec::new();
    for asset in ["BTC", "ETH"] {
        for interval in ["5m", "15m"] {
            let Some(step) = interval_secs(interval) else { continue };
            let epoch = (now_ts / step) * step;
            let ttr = (epoch + step) - now_ts;
            let slug = slug_for(asset, interval, epoch);
            let url = format!("{GAMMA_HOST}/events/slug/{slug}");
            let event = match get_json(http, &url).await? {
                Some(e) => e,
                None => continue,
            };
            for token in clob_token_ids(&event) {
                let ask = rest.get_price(&token, Side::Sell).await.unwrap_or(f64::NAN);
                let bid = rest.get_price(&token, Side::Buy).await.unwrap_or(f64::NAN);
                let pass = ask.is_finite() && bid.is_finite()
                    && (ASK_LO..=ASK_HI).contains(&ask) && bid > 0.0 && ttr > MIN_TTR_S;
                println!("[live-test]   {asset}/{interval} ttr={ttr}s ask={ask:.4} bid={bid:.4} pass={pass} token=…{}",
                    &token[token.len().saturating_sub(6)..]);
                if pass {
                    cands.push(Candidate { asset, interval, epoch, token_id: token, ask, bid, ttr });
                }
            }
        }
    }
    cands.sort_by(|a, b| {
        (a.ask - 0.50).abs().partial_cmp(&(b.ask - 0.50).abs()).unwrap().then(b.ttr.cmp(&a.ttr))
    });
    let Some(pick) = cands.first() else {
        return Ok(PassResult::NoCandidate);
    };
    let mid = (pick.ask + pick.bid) / 2.0;
    let tick_dec = rest.clob().tick_size(U256::from_str_radix(&pick.token_id, 10)?)
        .await.map_err(|e| anyhow!("tick_size: {e}"))?
        .minimum_tick_size.as_decimal();
    let neg_risk = rest.get_neg_risk(&pick.token_id).await.context("get_neg_risk")?;
    let buy_price = Decimal::ONE - tick_dec;
    let sell_price = tick_dec;
    let est_shares = STAKE_USD / pick.ask;
    let cost = STAKE_USD;
    let cap_ok = cost <= MAX_RISK_USD && (est_shares * pick.ask) <= MAX_RISK_USD;

    println!("[live-test] CANDIDATE {}/{} epoch={} token=…{} ask={:.4} bid={:.4} mid={:.4} ttr={}s tick={} neg_risk={}",
        pick.asset, pick.interval, pick.epoch, &pick.token_id[pick.token_id.len().saturating_sub(8)..],
        pick.ask, pick.bid, mid, pick.ttr, tick_dec, neg_risk);
    println!("[live-test] PLAN stake=${STAKE_USD} est_shares≈{est_shares:.4} cost≈${:.4} BUY@{buy_price} SELL@{sell_price} | CAP(≤${MAX_RISK_USD})={}",
        est_shares * pick.ask, if cap_ok { "PASS" } else { "FAIL" });
    let plan = json!({
        "funder_tail": "redacted", "balance_usd": balance_before,
        "candidate": {"asset": pick.asset, "interval": pick.interval, "epoch": pick.epoch,
            "token_id": pick.token_id, "ask": pick.ask, "bid": pick.bid, "mid": mid,
            "ttr_s": pick.ttr, "tick": tick_dec.to_string(), "neg_risk": neg_risk},
        "plan": {"stake_usd": STAKE_USD, "est_shares": est_shares, "cost_usd": est_shares * pick.ask,
            "buy_worst_price": buy_price.to_string(), "sell_worst_price": sell_price.to_string(),
            "cap_usd": MAX_RISK_USD, "cap_ok": cap_ok},
    });
    std::fs::write(out.join("preflight.json"), serde_json::to_string_pretty(&plan)?)?;

    if !cap_ok {
        return Ok(PassResult::SkewAbort("cap check FAILED".into()));
    }
    if !execute {
        println!("[live-test] PREFLIGHT COMPLETE (read-only, ZERO orders).");
        return Ok(PassResult::Preflight);
    }
    execute_round_trip(rest, signer, pick, tick_dec, out, balance_before).await
}

/// Real BUY+SELL. Returns SkewAbort (no order placed, keep watching) ONLY for
/// pre-POST guards; once a BUY is POSTed it returns Done or bails (hard stop).
async fn execute_round_trip<S: Signer>(
    rest: &RestClient,
    signer: &S,
    pick: &Candidate,
    tick_dec: Decimal,
    out: &Path,
    balance_before: f64,
) -> Result<PassResult> {
    let tok = U256::from_str_radix(&pick.token_id, 10)?;
    // ---- re-check ask + cap abort BEFORE the POST (price may have moved) ----
    let ask_now = rest.get_price(&pick.token_id, Side::Sell).await.context("re-check ask")?;
    println!("[live-test] re-check ask={ask_now:.4} (was {:.4})", pick.ask);
    if !(ASK_LO..=ASK_HI).contains(&ask_now) {
        log_attempt(pick, ask_now, "skew_abort", None, None);
        return Ok(PassResult::SkewAbort(format!("ask {ask_now:.4} moved out of [{ASK_LO},{ASK_HI}]")));
    }
    if STAKE_USD > MAX_RISK_USD || (STAKE_USD / ask_now) * ask_now > MAX_RISK_USD {
        log_attempt(pick, ask_now, "cap_abort", None, None);
        return Ok(PassResult::SkewAbort(format!("cap abort: cost ${STAKE_USD} > ${MAX_RISK_USD}")));
    }

    // ---- E' guards: caps (active-only exposure, fix 2) + kill-switch + frequency,
    //      evaluated from the REAL /positions state BEFORE any POST ----
    {
        let positions = rest.get_positions().await.unwrap_or_default();
        // PositionInfo exposes only {token,size,avg,cur}; the resolved filter here
        // relies on cur_price∈{0,1} (redeemable/endDate unavailable in this struct).
        // Counting MORE as active is the conservative direction for a cap (stricter).
        let rows: Vec<crate::exec::PosRow> = positions.iter().map(|p| crate::exec::PosRow {
            token: p.token_id.clone(), size: p.size, cur_price: p.cur_price,
            redeemable: false, end_in_past: false,
        }).collect();
        let (_, active_total) = crate::exec::active_exposure(&rows); // active-only (fix 2)
        let active_token = crate::exec::active_exposure_for_token(&rows, &pick.token_id);
        let nm = now_ms();
        let gd = crate::guards::Guards::new(crate::guards::GuardConfig::default());
        let ectx = crate::guards::EntryContext {
            stake_usd: Decimal::try_from(STAKE_USD)?, order_usd: Decimal::try_from(STAKE_USD)?,
            token: &pick.token_id,
            active_total_exposure: Decimal::try_from(active_total).unwrap_or_default(),
            active_token_exposure: Decimal::try_from(active_token).unwrap_or_default(),
            book_ts_ms: nm, last_price_change_ms: nm, now_ms: nm, // fresh REST prices; real ts at D
        };
        let verdict = gd.check_entry(&ectx);
        println!("[live-test] E' guards: active_total=${active_total:.4} active_token=${active_token:.4} -> {verdict:?}");
        if !verdict.is_allow() {
            return Ok(PassResult::SkewAbort(format!("E' guard blocked entry: {:?}", verdict.reason())));
        }
    }

    // ===== from here a real order is POSTed; any outcome HARD-STOPS =====
    let buy_price = Decimal::ONE - tick_dec;
    let buy = rest.clob().market_order()
        .token_id(tok).side(Side::Buy)
        .amount(Amount::usdc(Decimal::try_from(STAKE_USD)?).map_err(|e| anyhow!("amount: {e}"))?)
        .price(buy_price).order_type(OrderType::FOK)
        .build().await.map_err(|e| anyhow!("BUY build: {e}"))?;
    let buy_signed = rest.clob().sign(signer, buy).await.map_err(|e| anyhow!("BUY sign: {e}"))?;
    println!("[live-test] >>> POSTING BUY FOK (amount=${STAKE_USD}, worst-price={buy_price}) <<<");
    let t_buy_sent = now_ms();
    let buy_r = match rest.clob().post_order(buy_signed).await {
        Ok(r) => r,
        Err(e) => {
            log_attempt(pick, ask_now, "buy_post_error", None, None);
            bail!("BUY post network error (AMBIGUOUS — verify on web; NO retry): {e}");
        }
    };
    let t_buy_response = now_ms();
    let buy_ok = buy_r.success;
    let buy_oid = buy_r.order_id.clone();
    let buy_resp = order_resp_json(&buy_r);
    println!("[live-test] BUY resp: success={buy_ok} orderID={buy_oid} status={:?} txs={:?}",
        buy_r.status, buy_r.transaction_hashes);
    if !buy_ok {
        log_attempt(pick, ask_now, "rejected", None, None);
        bail!("BUY rejected (no position): error_msg={:?} — one order attempted, HARD STOP", buy_r.error_msg);
    }

    // ---- confirm fill from the RESPONSE (authoritative for SIZE), corroborate
    //      on-chain (authoritative for EXISTENCE), then size the SELL ----
    // Fill confirmed iff success + on-chain settlement (tx) + filled shares>0.
    let fill_confirmed = buy_r.success
        && !buy_r.transaction_hashes.is_empty()
        && buy_r.taking_amount > Decimal::ZERO;
    // BUY response: takingAmount = shares received (BUY gets tokens), DECIMAL — NOT
    // raw 6-decimal (verified vs py-clob-client #185; the /1e6 was the attempt-2 bug).
    // makingAmount = USDC spent.
    let resp_shares = crate::exec::buy_filled_shares(buy_r.taking_amount);
    let usdc_spent = crate::exec::buy_usdc_spent(buy_r.making_amount);
    // /positions = on-chain corroborator (anti-phantom, CLOB v2 #54): retried up to
    // ~30s (data-api lags several seconds; the 3s gate was attempt-1's bug). A real
    // fill MUST appear; if it never does → phantom → nothing to sell.
    // corroborate on-chain while sampling the bid (best_bid_during_hold drives the
    // HOLD-vs-SIGNAL autopsy). Returns (position, best_bid seen, t_corroboration).
    let (pos_shares, best_bid_hold, t_corrob) =
        corroborate_and_sample(rest, &pick.token_id, 10, 3).await;
    println!("[live-test] fill_confirmed={fill_confirmed} resp_shares(takingAmount)={resp_shares} \
        usdc_spent(makingAmount)={usdc_spent} /positions={pos_shares:?} best_bid_hold={best_bid_hold:?}");
    let pos_dec = pos_shares.and_then(|f| Decimal::try_from(f).ok());
    let shares_dec = match crate::exec::decide_sell(fill_confirmed, resp_shares, pos_dec) {
        crate::exec::SellPlan::Sell(s) => s,
        crate::exec::SellPlan::NoFill => {
            log_attempt(pick, ask_now, "rejected", Some(df(resp_shares)), Some(df(usdc_spent)));
            bail!("BUY did not actually fill (no tx / takingAmount=0) — no position, HARD STOP.");
        }
        crate::exec::SellPlan::Phantom => {
            log_attempt(pick, ask_now, "buy_only_phantom", Some(df(resp_shares)), Some(df(usdc_spent)));
            cleanup_report(out, &pick.token_id, 0.0, &buy_oid,
                &format!("PHANTOM fill suspected (CLOB #54): response Matched (shares {resp_shares}) but \
                    /positions never corroborated on-chain after ~30s. Nothing to sell. Verify on web."))?;
            bail!("PHANTOM fill — no on-chain position; HARD STOP, no SELL.");
        }
        crate::exec::SellPlan::Mismatch => {
            log_attempt(pick, ask_now, "buy_only_cleanup", Some(df(resp_shares)), Some(df(usdc_spent)));
            cleanup_report(out, &pick.token_id, pos_shares.unwrap_or(0.0), &buy_oid,
                &format!("fill-size MISMATCH: response {resp_shares} vs /positions {pos_shares:?} (≥0.01 dust). \
                    NOT selling a mis-sized amount."))?;
            bail!("CLEANUP-CRITICAL: fill-size mismatch — HARD STOP, verify + close manual.");
        }
    };
    let shares = ToPrimitive::to_f64(&shares_dec).unwrap_or(0.0);
    println!("[live-test] SELL size decided = {shares_dec} shares (min of response + /positions, \
        trunc to lot scale 2, never over-sell)");
    let sell_price = tick_dec;
    let sell = rest.clob().market_order()
        .token_id(tok).side(Side::Sell)
        .amount(Amount::shares(shares_dec).map_err(|e| anyhow!("sell amount: {e}"))?)
        .price(sell_price).order_type(OrderType::FOK)
        .build().await.map_err(|e| anyhow!("SELL build: {e}"))?;
    let sell_signed = rest.clob().sign(signer, sell).await.map_err(|e| anyhow!("SELL sign: {e}"))?;
    println!("[live-test] >>> POSTING SELL FOK (shares={shares_dec}, worst-price={sell_price}) <<<");
    let t_sell_sent = now_ms();
    let sell_r = match rest.clob().post_order(sell_signed).await {
        Ok(r) => r,
        Err(e) => {
            log_attempt(pick, ask_now, "buy_only_cleanup", Some(df(resp_shares)), Some(df(usdc_spent)));
            cleanup_report(out, &pick.token_id, shares, &buy_oid,
                &format!("SELL post ERRORED while position OPEN: {e}"))?;
            bail!("CLEANUP-CRITICAL: SELL failed, position OPEN — HARD STOP, no retry.");
        }
    };
    let t_sell_response = now_ms();
    let sell_ok = sell_r.success;
    let sell_oid = sell_r.order_id.clone();
    let sell_resp = order_resp_json(&sell_r);
    println!("[live-test] SELL resp: success={sell_ok} orderID={sell_oid} status={:?} txs={:?}",
        sell_r.status, sell_r.transaction_hashes);
    if !sell_ok {
        log_attempt(pick, ask_now, "buy_only_cleanup", Some(df(resp_shares)), Some(df(usdc_spent)));
        cleanup_report(out, &pick.token_id, shares, &buy_oid,
            &format!("SELL REJECTED while position OPEN: error_msg={:?}", sell_r.error_msg))?;
        bail!("CLEANUP-CRITICAL: SELL rejected, position OPEN — HARD STOP, no retry.");
    }

    // ---- realized NET P&L (fee-inclusive — the daily-loss-stop truth) ----
    // making/taking are GROSS (pre-fee MATCH); the fee is taken on-chain. NET =
    // wallet movement, via the verified fee formula (0.07·s·p·(1−p)), reconciled to
    // the balance delta below. SELL response: making=shares, taking=USDC (inverted
    // vs BUY). NEVER /positions curPrice.
    let sell_shares = crate::exec::sell_filled_shares(sell_r.making_amount);
    let sell_gross = crate::exec::sell_usdc_received(sell_r.taking_amount);
    let buy_price = if resp_shares > Decimal::ZERO { usdc_spent / resp_shares } else { Decimal::ZERO };
    let sell_price = if sell_shares > Decimal::ZERO { sell_gross / sell_shares } else { Decimal::ZERO };
    let buy_fee = crate::exec::polymarket_fee(resp_shares, buy_price);
    let sell_fee = crate::exec::polymarket_fee(sell_shares, sell_price);
    let buy_wallet_cost = crate::exec::buy_wallet_cost(resp_shares, buy_price);
    let sell_wallet_proceeds = crate::exec::sell_wallet_proceeds(sell_shares, sell_price);
    let gross_pnl = sell_gross - usdc_spent;
    let net_pnl = crate::exec::realized_pnl_net(resp_shares, buy_price, sell_shares, sell_price);
    println!("[live-test] BUY  {resp_shares}sh @{buy_price}: gross={usdc_spent} fee={buy_fee} wallet_cost={buy_wallet_cost}");
    println!("[live-test] SELL {sell_shares}sh @{sell_price}: gross={sell_gross} fee={sell_fee} wallet_proceeds={sell_wallet_proceeds}");
    println!("[live-test] P&L: gross(match)={gross_pnl}  fees={}  NET(daily-loss-stop)={net_pnl}", buy_fee + sell_fee);

    // ---- confirm closed (longer wait; data-api lags) ----
    tokio::time::sleep(Duration::from_secs(8)).await;
    let remaining = position_shares(rest, &pick.token_id).await.unwrap_or(0.0);
    let balance_after = rest.get_balance().await.map(|b| b.balance_usdc).unwrap_or(f64::NAN);
    let closed = remaining <= 0.01;

    // ---- execution-log row (trade autopsy; one per attempt) ----
    let mut tl = TradeLog {
        trade_id: format!("{}-{}-{}-{}", pick.asset, pick.interval, pick.epoch, t_buy_sent),
        client_order_id: Some(buy_oid.clone()),
        asset: pick.asset.to_string(), interval: pick.interval.to_string(), epoch: pick.epoch,
        token_id: pick.token_id.clone(), side: "?".to_string(), status: "completed".to_string(),
        t_buy_sent_ms: Some(t_buy_sent), t_buy_response_ms: Some(t_buy_response),
        t_corroboration_ms: t_corrob, t_sell_sent_ms: Some(t_sell_sent),
        t_sell_response_ms: Some(t_sell_response),
        ask_at_signal: Some(pick.ask), ask_at_decision: Some(pick.ask), ask_at_buy_sent: Some(ask_now),
        buy_fill_price: Some(df(buy_price)), best_bid_during_hold: best_bid_hold,
        sell_fill_price: Some(df(sell_price)),
        stake_usd: Some(STAKE_USD), shares_filled: Some(df(resp_shares)), shares_sold: Some(df(sell_shares)),
        buy_gross: Some(df(usdc_spent)), buy_fee: Some(df(buy_fee)), buy_wallet_cost: Some(df(buy_wallet_cost)),
        sell_gross: Some(df(sell_gross)), sell_fee: Some(df(sell_fee)),
        sell_wallet_proceeds: Some(df(sell_wallet_proceeds)),
        gross_pnl: Some(df(gross_pnl)), fees_total: Some(df(buy_fee + sell_fee)), net_pnl: Some(df(net_pnl)),
        net_pnl_fills: Some(df(net_pnl)), net_pnl_balance_delta: Some(balance_after - balance_before),
        ..Default::default()
    };
    tl.finalize(&Thresholds::default());
    let _ = tl.append_jsonl(Path::new(EXEC_LOG));
    println!("[live-test] exec-log row appended → {EXEC_LOG} (outcome={:?} cause={:?})",
        tl.outcome, tl.cause_primary);

    let report = json!({
        "buy": {"orderID": buy_oid, "resp": buy_resp, "shares_filled": resp_shares.to_string(),
            "usdc_spent": usdc_spent.to_string()},
        "sell": {"orderID": sell_oid, "resp": sell_resp, "shares_sold": sell_shares.to_string(),
            "usdc_gross": sell_gross.to_string(), "wallet_proceeds": sell_wallet_proceeds.to_string(),
            "fee": sell_fee.to_string(), "price": sell_price.to_string()},
        "candidate": {"asset": pick.asset, "interval": pick.interval, "token_id": pick.token_id,
            "ask_at_detect": pick.ask, "ask_at_post": ask_now, "mid": (pick.ask + pick.bid) / 2.0, "ttr_s": pick.ttr},
        "shares_sold": shares, "position_after_sell": remaining, "closed": closed,
        "pnl_gross_match": gross_pnl.to_string(),
        "fees_total": (buy_fee + sell_fee).to_string(),
        "pnl_net": net_pnl.to_string(),
        "balance_before": balance_before, "balance_after": balance_after,
        "balance_delta": balance_after - balance_before,
    });
    std::fs::write(out.join("execute_report.json"), serde_json::to_string_pretty(&report)?)?;
    // Reconcile the computed NET P&L against the actual wallet movement.
    let net_pnl_f = ToPrimitive::to_f64(&net_pnl).unwrap_or(f64::NAN);
    let balance_delta = balance_after - balance_before;
    let reconciles = (net_pnl_f - balance_delta).abs() < 0.01;
    println!("\n[live-test] === ROUND-TRIP REPORT ===");
    println!("  BUY  orderID={buy_oid} txs={:?}", buy_r.transaction_hashes);
    println!("  SELL orderID={sell_oid} txs={:?}", sell_r.transaction_hashes);
    println!("  shares sold={shares}  position after SELL={remaining} (closed={closed})");
    println!("  P&L: gross(match)=${gross_pnl}  fees=${}  NET=${net_pnl}", buy_fee + sell_fee);
    println!("  balance ${balance_before:.4} -> ${balance_after:.4}  delta=${balance_delta:.4}");
    println!("  NET P&L vs balance delta: {} (|{net_pnl_f:.4} - {balance_delta:.4}| < 0.01)",
        if reconciles { "RECONCILES ✓" } else { "MISMATCH — investigate" });
    println!("  wrote {}", out.join("execute_report.json").display());
    if !closed {
        cleanup_report(out, &pick.token_id, remaining, &buy_oid,
            "SELL accepted but /positions still shows shares after 5s — verify on web.")?;
        bail!("CLEANUP-CRITICAL: position still open after SELL — HARD STOP.");
    }
    Ok(PassResult::Done)
}

async fn position_shares(rest: &RestClient, token: &str) -> Option<f64> {
    let positions = rest.get_positions().await.ok()?;
    positions.iter().find(|p| p.token_id == token).map(|p| p.size)
}

/// Corroborate the fill on-chain (`/positions`) up to `tries`×`interval_s`s while
/// sampling the bid — captures `best_bid_during_hold` (the HOLD-vs-SIGNAL signal).
/// Returns (position shares once corroborated, best bid seen, t_corroboration_ms).
/// `None` position = never corroborated → phantom. The fill is already confirmed
/// from the BUY response, so waiting is safe (corroboration, not a blind gate).
async fn corroborate_and_sample(
    rest: &RestClient, token: &str, tries: u32, interval_s: u64,
) -> (Option<f64>, Option<f64>, Option<i64>) {
    let mut best_bid: Option<f64> = None;
    for i in 0..tries {
        if let Ok(b) = rest.get_price(token, Side::Buy).await {
            best_bid = Some(best_bid.map_or(b, |m| m.max(b)));
        }
        if let Some(s) = position_shares(rest, token).await
            && s > 0.0
        {
            return (Some(s), best_bid, Some(now_ms()));
        }
        if i + 1 < tries {
            tokio::time::sleep(Duration::from_secs(interval_s)).await;
        }
    }
    (None, best_bid, None)
}

/// Emit a partial execution-log row for a non-completed attempt (abort/phantom/etc).
fn log_attempt(pick: &Candidate, ask_now: f64, status: &str, shares: Option<f64>, usdc: Option<f64>) {
    let mut tl = TradeLog {
        trade_id: format!("{}-{}-{}", pick.asset, pick.interval, pick.epoch),
        asset: pick.asset.to_string(), interval: pick.interval.to_string(), epoch: pick.epoch,
        token_id: pick.token_id.clone(), side: "?".to_string(), status: status.to_string(),
        ask_at_signal: Some(pick.ask), ask_at_decision: Some(pick.ask), ask_at_buy_sent: Some(ask_now),
        stake_usd: Some(STAKE_USD), shares_filled: shares, buy_gross: usdc,
        ..Default::default()
    };
    tl.finalize(&Thresholds::default());
    let _ = tl.append_jsonl(Path::new(EXEC_LOG));
}

fn cleanup_report(out: &Path, token: &str, shares: f64, buy_oid: &str, why: &str) -> Result<()> {
    let body = json!({
        "CLEANUP_CRITICAL": true, "reason": why,
        "token_id": token, "open_shares": shares, "buy_order_id": buy_oid,
    });
    let path = out.join("CLEANUP_CRITICAL.json");
    std::fs::write(&path, serde_json::to_string_pretty(&body)?)?;
    eprintln!("\n{}", "*".repeat(64));
    eprintln!("*** CLEANUP-CRITICAL ***\n  {why}\n  token_id: {token}\n  open_shares: {shares}\n  buy_orderID: {buy_oid}");
    eprintln!("  ACTION: verify on polymarket.com; close manually if open. NO auto-retry.");
    eprintln!("  wrote {}\n{}", path.display(), "*".repeat(64));
    Ok(())
}

/// Typed PostOrderResponse → JSON (it is not Serialize). Includes tx hashes.
fn order_resp_json(r: &PostOrderResponse) -> Value {
    json!({
        "success": r.success, "orderID": r.order_id, "status": format!("{:?}", r.status),
        "makingAmount": r.making_amount.to_string(), "takingAmount": r.taking_amount.to_string(),
        "transactionHashes": r.transaction_hashes.iter().map(|h| format!("{h}")).collect::<Vec<_>>(),
        "tradeIds": r.trade_ids, "errorMsg": r.error_msg,
    })
}

async fn clob_now(http: &reqwest::Client) -> Result<i64> {
    let v = get_json(http, &format!("{CLOB_HOST}/time")).await?
        .ok_or_else(|| anyhow!("/time 404"))?;
    v.as_i64().or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        .ok_or_else(|| anyhow!("unexpected /time payload: {v}"))
}

async fn get_json(http: &reqwest::Client, url: &str) -> Result<Option<Value>> {
    let resp = http.get(url).send().await.with_context(|| format!("GET {url}"))?;
    if resp.status().as_u16() == 404 {
        return Ok(None);
    }
    if !resp.status().is_success() {
        bail!("GET {url} → http {}", resp.status());
    }
    Ok(Some(resp.json::<Value>().await.context("decode json")?))
}

fn clob_token_ids(event: &Value) -> Vec<String> {
    let Some(market) = event.get("markets").and_then(Value::as_array).and_then(|m| m.first()) else {
        return Vec::new();
    };
    match market.get("clobTokenIds") {
        Some(Value::String(s)) => serde_json::from_str::<Vec<String>>(s).unwrap_or_default(),
        Some(Value::Array(a)) => a.iter().filter_map(|v| v.as_str().map(str::to_string)).collect(),
        _ => Vec::new(),
    }
}
