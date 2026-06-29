//! Phase 6 — execution logging + trade autopsy. ONE record per order ATTEMPT
//! (completed / phantom / cleanup / skew / cap / rejected), with ms timestamps,
//! prices along the chain, the net-P&L decomposition (3 reconciled sources), and a
//! loss-cause classification (5 causes → 5 distinct actions). This is the
//! edge-health monitor for live: if the LATENCY_RACE share climbs, the lag-arb edge
//! is being eaten. It also feeds the daily-loss-stop (NET P&L, never gross).
//!
//! The 5 causes (and the action each implies):
//!   SIGNAL       — clean entry, never in profit; the prediction was wrong → strategy
//!   LATENCY_RACE — PM repriced before our fill; would've won at the signal price → cut latency
//!   SLIPPAGE     — FOK filled worse than the send-time quote → size down / deeper books
//!   FEES         — directionally positive but fees flipped it → raise the trigger
//!   HOLD         — clean entry, WAS in profit, gave it back over the hold → exit timing
//!
//! Fields are Option: the A2 isolated executor populates order/fill/P&L now; the
//! signal-chain fields (t_signal, ask_at_signal, latency) populate when the
//! LiveExecutor sits behind the decision engine (sub-paso D).

#![allow(dead_code)] // consumed by the LiveExecutor here + the integrated bot (D)

use std::io::Write as _;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;

/// Wall-clock now in UTC ms (for absolute timestamps + latency deltas; same machine).
#[must_use]
pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// f64 trade fee = 0.07 × shares × price × (1 − price) — the verified formula (A2
/// §4). f64 here because the log is for analysis; exact money lives in `exec` (Decimal).
#[must_use]
pub fn fee_f64(shares: f64, price: f64) -> f64 {
    0.07 * shares * price * (1.0 - price)
}

/// Classification thresholds (price units; START as guesses ≈ 1 tick, CALIBRATED
/// from the first live sample — no magic numbers baked into logic).
#[derive(Debug, Clone, Copy)]
pub struct Thresholds {
    /// Adverse ask move (signal→send) that counts as the edge closing.
    pub latency: f64,
    /// Fill worse than the send-time quote that counts as slippage.
    pub slippage: f64,
    /// Adverse/favorable hold drift that counts as a real move (vs dust).
    pub hold: f64,
    /// Max−min across net-P&L sources still considered "agrees" (A2 dust = 0.01).
    pub reconcile: f64,
}
impl Default for Thresholds {
    fn default() -> Self {
        Self { latency: 0.01, slippage: 0.01, hold: 0.01, reconcile: 0.01 }
    }
}

/// One execution-log record (JSONL row). All amounts USDC, prices 0..1, times UTC ms.
#[derive(Debug, Default, Clone, Serialize)]
pub struct TradeLog {
    // ---- identity ----
    pub trade_id: String,
    pub client_order_id: Option<String>,
    pub signal_id: Option<String>,
    pub asset: String,
    pub interval: String,
    pub epoch: i64,
    pub token_id: String,
    pub side: String, // "Up" / "Down"
    pub slug: Option<String>,
    /// completed | buy_only_phantom | buy_only_cleanup | skew_abort | cap_abort | rejected
    pub status: String,

    // ---- timestamps (UTC ms) ----
    pub t_signal_ms: Option<i64>,
    pub t_decision_ms: Option<i64>,
    pub t_buy_sent_ms: Option<i64>,
    pub t_buy_response_ms: Option<i64>,
    pub t_corroboration_ms: Option<i64>,
    pub t_sell_sent_ms: Option<i64>,
    pub t_sell_response_ms: Option<i64>,
    pub t_resolution_ms: Option<i64>,

    // ---- prices (bet token) ----
    pub ask_at_signal: Option<f64>,
    pub ask_at_decision: Option<f64>,
    pub ask_at_buy_sent: Option<f64>,
    pub buy_fill_price: Option<f64>,
    pub bid_at_buy: Option<f64>,
    pub best_bid_during_hold: Option<f64>, // refinement 1: was the position ever green?
    pub bid_at_sell_sent: Option<f64>,
    pub sell_fill_price: Option<f64>,
    pub resolution_price: Option<f64>, // 0/1 on-chain — never curPrice
    pub binance_ret_bps: Option<f64>,
    pub window_s: Option<i64>,

    // ---- sizes ----
    pub stake_usd: Option<f64>,
    pub shares_intended: Option<f64>,
    pub shares_filled: Option<f64>,
    pub shares_sold: Option<f64>,

    // ---- P&L decomposition (net = the daily-loss-stop number) ----
    pub buy_gross: Option<f64>,
    pub buy_fee: Option<f64>,
    pub buy_wallet_cost: Option<f64>,
    pub sell_gross: Option<f64>,
    pub sell_fee: Option<f64>,
    pub sell_wallet_proceeds: Option<f64>,
    pub gross_pnl: Option<f64>,
    pub fees_total: Option<f64>,
    pub net_pnl: Option<f64>,

    // ---- 3-source net-P&L reconcile (refinement 3) ----
    pub net_pnl_fills: Option<f64>,
    pub net_pnl_activity: Option<f64>,
    pub net_pnl_balance_delta: Option<f64>,
    pub reconciles: Option<bool>,

    // ---- derived ----
    pub latency_signal_to_fill_ms: Option<i64>,
    pub edge_decay: Option<f64>,
    pub slippage: Option<f64>,
    pub hold_move: Option<f64>,
    pub hypothetical_pnl_at_signal_price: Option<f64>, // refinement 2
    pub latency_cost: Option<f64>,                     // refinement 2: real − hypothetical
    pub outcome: Option<String>,                       // win | loss | scratch
    pub cause_primary: Option<String>,
    pub cause_flags: Vec<String>,
}

impl TradeLog {
    /// Compute the derived metrics + classification from the raw fields. Idempotent.
    pub fn finalize(&mut self, th: &Thresholds) {
        if let (Some(a_sig), Some(a_send)) = (self.ask_at_signal, self.ask_at_buy_sent) {
            self.edge_decay = Some(a_send - a_sig); // BUY: positive = ask rose against us
        }
        if let (Some(fill), Some(a_send)) = (self.buy_fill_price, self.ask_at_buy_sent) {
            self.slippage = Some(fill - a_send); // positive = filled worse than the quote
        }
        if let (Some(sf), Some(bb)) = (self.sell_fill_price, self.bid_at_buy) {
            self.hold_move = Some(sf - bb);
        }
        if let (Some(ts), Some(tr)) = (self.t_signal_ms, self.t_buy_response_ms) {
            self.latency_signal_to_fill_ms = Some(tr - ts);
        }
        // counterfactual: P&L had the BUY filled at the signal-time ask (instant exec).
        if let (Some(a_sig), Some(shares), Some(sell_net)) =
            (self.ask_at_signal, self.shares_filled, self.sell_wallet_proceeds)
        {
            let hyp_buy_cost = shares * a_sig + fee_f64(shares, a_sig);
            let hyp = sell_net - hyp_buy_cost;
            self.hypothetical_pnl_at_signal_price = Some(hyp);
            if let Some(net) = self.net_pnl {
                self.latency_cost = Some(net - hyp); // negative = latency hurt
            }
        }
        // reconcile the net-P&L sources that are present.
        let srcs: Vec<f64> = [self.net_pnl_fills, self.net_pnl_activity, self.net_pnl_balance_delta]
            .into_iter()
            .flatten()
            .collect();
        if srcs.len() >= 2 {
            let mx = srcs.iter().cloned().fold(f64::MIN, f64::max);
            let mn = srcs.iter().cloned().fold(f64::MAX, f64::min);
            self.reconciles = Some((mx - mn).abs() < th.reconcile);
        }
        if let Some(net) = self.net_pnl {
            self.outcome = Some(
                if net > 0.0001 { "win" } else if net < -0.0001 { "loss" } else { "scratch" }.to_string(),
            );
        }
        let (primary, flags) = classify(self, th);
        self.cause_primary = Some(primary);
        self.cause_flags = flags;
    }

    /// Append this record as one JSONL line (create parent dir if needed).
    pub fn append_jsonl(&self, path: &Path) -> std::io::Result<()> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let mut f = std::fs::OpenOptions::new().create(true).append(true).open(path)?;
        writeln!(f, "{}", serde_json::to_string(self).unwrap_or_default())
    }
}

/// Classify outcome cause. Returns `(primary, flags)`. For a loss the primary is the
/// FIRST decisive cause; flags lists ALL contributing causes (a loss can be both
/// latency_race and fees). Wins → `edge_captured` (+ `hold_favorable` if the drift
/// helped). LATENCY_RACE is PRIMARY only when the counterfactual shows it was
/// decisive (would've won at the signal price). HOLD vs SIGNAL is decided by whether
/// the position was ever in profit (`best_bid_during_hold` > entry).
#[must_use]
pub fn classify(t: &TradeLog, th: &Thresholds) -> (String, Vec<String>) {
    let net = t.net_pnl.unwrap_or(0.0);
    if net >= -0.0001 {
        // win / scratch
        let mut flags = Vec::new();
        if t.hold_move.unwrap_or(0.0) > th.hold {
            flags.push("hold_favorable".to_string());
        }
        return ("edge_captured".to_string(), flags);
    }

    let edge_decay = t.edge_decay.unwrap_or(0.0);
    let slip = t.slippage.unwrap_or(0.0);
    let gross = t.gross_pnl;
    let hyp = t.hypothetical_pnl_at_signal_price;

    let mut flags = Vec::new();
    if edge_decay >= th.latency {
        flags.push("latency_race".to_string());
    }
    if slip >= th.slippage {
        flags.push("slippage".to_string());
    }
    let fees_flipped = gross.is_some_and(|g| g >= 0.0); // gross OK but net < 0 → fees flipped it
    if fees_flipped {
        flags.push("fees".to_string());
    }
    // directional loss = the match value itself fell (gross < 0). If gross unknown,
    // treat as directional (conservative).
    let directional = gross.is_none_or(|g| g < 0.0);
    let was_in_profit = match (t.best_bid_during_hold, t.buy_fill_price) {
        (Some(bb), Some(bf)) => bb > bf + th.hold,
        _ => false,
    };
    if directional {
        flags.push(if was_in_profit { "hold" } else { "signal" }.to_string());
    }

    // PRIMARY: latency only if the counterfactual makes it decisive (would've won
    // instant); else slippage; else fees; else the directional cause.
    let latency_decisive = edge_decay >= th.latency && hyp.is_some_and(|h| h > 0.0);
    let primary = if latency_decisive {
        "latency_race"
    } else if slip >= th.slippage {
        "slippage"
    } else if fees_flipped {
        "fees"
    } else if directional && was_in_profit {
        "hold"
    } else {
        "signal"
    };
    (primary.to_string(), flags)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_loss() -> TradeLog {
        // a clean-ish losing scaffold; tests tweak the relevant fields.
        TradeLog {
            ask_at_signal: Some(0.50),
            ask_at_decision: Some(0.50),
            ask_at_buy_sent: Some(0.50),
            buy_fill_price: Some(0.50),
            bid_at_buy: Some(0.49),
            shares_filled: Some(2.0),
            sell_wallet_proceeds: Some(0.90),
            gross_pnl: Some(-0.10),
            net_pnl: Some(-0.12),
            ..Default::default()
        }
    }

    #[test]
    fn a2_real_round_trip_is_a_clean_win() {
        // Real A2: BUY 2.282607 @0.46 (signal ask 0.48), SELL 2.28 @0.60. net +0.24,
        // 3 sources reconcile, latency HELPED (filled better than signal), hold favorable.
        let mut t = TradeLog {
            asset: "BTC".into(), interval: "5m".into(), epoch: 1780132200,
            token_id: "…252792".into(), side: "Up".into(), status: "completed".into(),
            ask_at_signal: Some(0.48), ask_at_decision: Some(0.48), ask_at_buy_sent: Some(0.48),
            buy_fill_price: Some(0.46), bid_at_buy: Some(0.47), sell_fill_price: Some(0.60),
            shares_filled: Some(2.282607), shares_sold: Some(2.28),
            buy_gross: Some(1.04999), buy_fee: Some(0.03969), buy_wallet_cost: Some(1.08968),
            sell_gross: Some(1.368), sell_fee: Some(0.0383), sell_wallet_proceeds: Some(1.3297),
            gross_pnl: Some(0.31801), fees_total: Some(0.07799), net_pnl: Some(0.24002),
            net_pnl_fills: Some(0.24002), net_pnl_activity: Some(0.24002),
            net_pnl_balance_delta: Some(0.240021),
            ..Default::default()
        };
        t.finalize(&Thresholds::default());
        assert_eq!(t.outcome.as_deref(), Some("win"));
        assert_eq!(t.cause_primary.as_deref(), Some("edge_captured"));
        assert_eq!(t.reconciles, Some(true));
        assert!(t.hold_move.unwrap() > 0.0, "hold drift was favorable");
        assert!(t.latency_cost.unwrap() > 0.0, "filling better than signal → latency helped");
        // Emit the populated row as a demo artifact (repo data/; cwd = crate dir).
        let _ = std::fs::create_dir_all("../data/derived/exec_log");
        let _ = std::fs::write(
            "../data/derived/exec_log/example_a2.jsonl",
            serde_json::to_string(&t).unwrap() + "\n",
        );
    }

    #[test]
    fn latency_race_when_counterfactual_would_win() {
        let mut t = base_loss();
        t.ask_at_signal = Some(0.50);
        t.ask_at_buy_sent = Some(0.56); // ask rose 6c during our latency
        t.buy_fill_price = Some(0.56);
        t.shares_filled = Some(2.0);
        t.sell_wallet_proceeds = Some(1.05); // would've been a winner at 0.50 entry
        t.gross_pnl = Some(-0.07);
        t.net_pnl = Some(-0.10);
        t.finalize(&Thresholds::default());
        assert_eq!(t.cause_primary.as_deref(), Some("latency_race"));
        assert!(t.latency_cost.unwrap() < 0.0, "latency hurt");
        assert!(t.cause_flags.contains(&"latency_race".to_string()));
    }

    #[test]
    fn slippage_when_fill_worse_than_quote() {
        let mut t = base_loss();
        t.ask_at_signal = Some(0.50);
        t.ask_at_buy_sent = Some(0.50); // no edge decay
        t.buy_fill_price = Some(0.54); // filled 4c worse than the quote
        t.sell_wallet_proceeds = Some(0.95);
        t.gross_pnl = Some(-0.05);
        t.net_pnl = Some(-0.08);
        t.finalize(&Thresholds::default());
        assert_eq!(t.cause_primary.as_deref(), Some("slippage"));
    }

    #[test]
    fn fees_when_gross_positive_but_net_negative() {
        let mut t = base_loss();
        t.ask_at_buy_sent = Some(0.50);
        t.buy_fill_price = Some(0.50); // clean entry
        t.gross_pnl = Some(0.02); // directionally positive
        t.net_pnl = Some(-0.03); // fees flipped it
        t.finalize(&Thresholds::default());
        assert_eq!(t.cause_primary.as_deref(), Some("fees"));
        assert!(t.cause_flags.contains(&"fees".to_string()));
    }

    #[test]
    fn hold_when_was_in_profit_then_gave_it_back() {
        let mut t = base_loss();
        t.ask_at_buy_sent = Some(0.50);
        t.buy_fill_price = Some(0.50); // clean entry
        t.best_bid_during_hold = Some(0.58); // was up 8c at peak
        t.sell_fill_price = Some(0.45);
        t.gross_pnl = Some(-0.10); // ended down
        t.net_pnl = Some(-0.12);
        t.finalize(&Thresholds::default());
        assert_eq!(t.cause_primary.as_deref(), Some("hold"));
    }

    #[test]
    fn signal_when_never_in_profit() {
        let mut t = base_loss();
        t.ask_at_buy_sent = Some(0.50);
        t.buy_fill_price = Some(0.50); // clean entry
        t.best_bid_during_hold = Some(0.49); // never above entry
        t.gross_pnl = Some(-0.10);
        t.net_pnl = Some(-0.12);
        t.finalize(&Thresholds::default());
        assert_eq!(t.cause_primary.as_deref(), Some("signal"));
    }

    #[test]
    fn reconcile_flags_a_disagreement() {
        let mut t = base_loss();
        t.net_pnl_fills = Some(0.24);
        t.net_pnl_balance_delta = Some(0.10); // a read bug would look like this
        t.finalize(&Thresholds::default());
        assert_eq!(t.reconciles, Some(false));
    }
}
