//! Phase 5 decision engine — the Capa B layer. Given the Phase 4 scope signals
//! for a trigger + the book + the clock + (live) a REST cross-check, it decides
//! whether to fire / reject / close-an-opposite, with the EXACT semantics of the
//! operational Python `trading_loop` (variant: REGLA C opposite-closes).
//!
//! ONE LOGIC, REPLAY + LIVE: the only differences are the INJECTED dependencies.
//! `book` ([`BookProvider`]) is live `state.bbo` vs replay reconstructed-as-of;
//! `now_ms` is the live wall clock vs the replay kline received_at; `rest`
//! (`Fn(&str) -> RestProbe`) is the live REST `/price` vs replay `Skip`. So what
//! Capa B validates here is exactly what runs live.
//!
//! SCOPE: entry decision (fire / reject+reason / close-opposite) + sizing. The
//! exit/P&L is the [`executor`]. H2 is NOT applied (deferred post-parity).

#![allow(dead_code)] // live wiring (the orchestrator calling decide+executor) lands after Capa B

pub mod executor;
pub mod lifecycle;

use tracing::debug;

use crate::signal::{Direction, Signal, Stratum};
use crate::state::EventBbo;
use crate::state::persist::{OpenPosition, Outcome};

/// Decision-engine constants (the operational Python values). `stake_usd` is the
/// only production knob; for Capa B parity it MUST equal the oracle's (10.0).
///
/// `regla_c_enabled` selects the VARIANT (DECISIONS.md, Phase 5): OFF (default) =
/// the BASELINE (`paper_trade_lagarb_directional.py`) — the operative variant; ON
/// = opposite-closes (`..._opposite_closes.py`) — the reversible alternative. The
/// entry logic is identical; the flag only toggles the opposite-side close.
#[derive(Debug, Clone, Copy)]
pub struct DecisionConfig {
    pub stake_usd: f64,
    pub rw_qlo: f64,
    pub rw_qhi: f64,
    pub im_qlo: f64,
    pub im_qhi: f64,
    pub rw_hold_s: i64,
    pub im_hold_s: i64,
    pub stale_book_max_ms: i64,
    pub phantom_threshold: f64,
    /// REGLA C (opposite-side close). OFF = baseline (operative). See above.
    pub regla_c_enabled: bool,
}

impl Default for DecisionConfig {
    fn default() -> Self {
        Self {
            stake_usd: 10.0, // Python STAKE_USD (operational); Capa B oracle value
            rw_qlo: 0.26,
            rw_qhi: 0.97,
            im_qlo: 0.30,
            im_qhi: 0.76,
            rw_hold_s: 120,
            im_hold_s: 300,
            stale_book_max_ms: 3000,
            phantom_threshold: 0.10,
            regla_c_enabled: false, // baseline = the operative variant
        }
    }
}

impl DecisionConfig {
    /// `(qlo, qhi, hold_s)` for a stratum.
    pub(crate) fn band(&self, stratum: Stratum) -> (f64, f64, i64) {
        match stratum {
            Stratum::ResolvedWindow => (self.rw_qlo, self.rw_qhi, self.rw_hold_s),
            Stratum::Immediate => (self.im_qlo, self.im_qhi, self.im_hold_s),
        }
    }
}

/// The token's BBO source — the DI seam. Live wraps `&SharedState.bbo`; replay
/// wraps the event-BBO reconstructed as-of the decision instant.
pub trait BookProvider {
    fn bbo(&self, token: &str) -> Option<EventBbo>;
}

/// Live [`BookProvider`] over the ingestion `state.bbo` (the single price source).
/// The replay provider (reconstructed-as-of) lands with the Capa B harness.
pub struct LiveBook<'a>(pub &'a crate::state::SharedState);

impl BookProvider for LiveBook<'_> {
    fn bbo(&self, token: &str) -> Option<EventBbo> {
        self.0.bbo.get(token).map(|b| *b)
    }
}

/// REST cross-check for the phantom gate (injected). `Skip` in replay (phantom is
/// excluded from the deterministic Capa B gate); live returns `Price`/`Failed`.
#[derive(Debug, Clone)]
pub enum RestProbe {
    Skip,
    Price(f64),
    /// REST failed (timeout/http/parse) → fail-closed reject `rest_<status>`.
    Failed(String),
}

/// Per-stratum decision outcome.
#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    /// Open (or DCA-add) the bet side. `shares` = stake/fill (f64, Python-parity).
    Fire {
        interval: String,
        stratum: Stratum,
        bet_token: String,
        fill: f64,
        shares: f64,
    },
    /// REGLA C: an opposite-side position exists → close it (skip opening).
    /// `close_price` = opposite token's best_bid (None ⇒ can't close, leave open,
    /// but STILL skip opening — matches the Python).
    CloseOpposite {
        interval: String,
        stratum: Stratum,
        opposite_token: String,
        close_price: Option<f64>,
    },
    /// Rejected (phantom only at the per-stratum level; Q2-Q4/staleness/no_book
    /// rejections happen earlier and feed the trigger-level `reject_reason`).
    Reject {
        interval: String,
        stratum: Stratum,
        reason: String,
    },
}

/// The decision for one trigger: per-stratum actions + the per-trigger signal row
/// (mirrors the Python signals.csv: fired + reject_reason + primary interval).
#[derive(Debug, Clone, PartialEq)]
pub struct TriggerDecision {
    pub asset: String,
    pub trigger_ts: i64,
    pub direction: Direction,
    pub actions: Vec<Action>,
    pub fired: bool,
    pub reject_reason: String,
    pub primary_interval: Option<String>,
}

/// Map a signal Direction to the persisted position Outcome.
fn outcome_of(d: Direction) -> Outcome {
    match d {
        Direction::Up => Outcome::Up,
        Direction::Down => Outcome::Down,
    }
}

/// Deterministic signal/position id (our construction; the Python uses uuid4).
fn signal_id(s: &Signal) -> String {
    format!(
        "{}-{}-{}-{}",
        s.asset,
        s.trigger_ts,
        s.interval,
        s.direction.as_str()
    )
}

/// A scope signal that PASSED Q2-Q4 + staleness + lifecycle.
#[derive(Debug, Clone)]
struct Candidate {
    signal: Signal,
    best_ask: f64,
    best_bid: Option<f64>,
    phantom_reject: Option<String>,
}

/// Reject tallies (drive the trigger-level reject_reason priority, like the
/// Python `reject_counts`).
#[derive(Debug, Default, Clone, Copy)]
struct RejectCounts {
    no_book: u32,
    stale_book: u32,
    out_of_q2q4: u32,
}

/// The decision. `signals` = the Phase 4 scope signals for ONE trigger (per
/// interval). Pure w.r.t. side effects (reads `book`/`positions`, returns data);
/// the [`executor`] applies the actions.
#[allow(clippy::too_many_arguments)] // cohesive decision inputs + injected deps
pub fn decide(
    asset: &str,
    trigger_ts: i64,
    direction: Direction,
    signals: &[Signal],
    cfg: &DecisionConfig,
    book: &dyn BookProvider,
    now_ms: i64,
    rest: impl Fn(&str) -> RestProbe,
    positions: &[OpenPosition],
) -> TriggerDecision {
    let mut counts = RejectCounts::default();
    let mut qualifying: Vec<Candidate> = Vec::new();
    // First eligible rejected candidate WITH book data (for the signal-row primary,
    // mirroring the Python `sample_rejected`; prefer stale_book over out_of_q2q4).
    let mut sample_rejected: Option<Signal> = None;

    for s in signals {
        // Lifecycle defense: never decide on a market past its end_time. For a
        // valid signal (ttr >= 30 at trigger_ts) this is a no-op, but it is the
        // clock-based guard (Phase 1 §3 consumer).
        let secs = interval_secs(&s.interval);
        if lifecycle::is_expired(s.epoch, secs, now_ms) {
            debug!(token = %s.bet_token_id, "decision: market expired at now; skipping");
            continue;
        }
        let (qlo, qhi, _hold) = cfg.band(s.stratum);
        let bbo = match book.bbo(&s.bet_token_id) {
            Some(b) => b,
            None => {
                counts.no_book += 1;
                continue;
            }
        };
        if bbo.ts_ms <= 0 {
            counts.no_book += 1;
            continue;
        }
        if now_ms - bbo.ts_ms > cfg.stale_book_max_ms {
            counts.stale_book += 1;
            if sample_rejected.is_none() {
                sample_rejected = Some(s.clone());
            }
            continue;
        }
        match bbo.best_ask {
            Some(ask) if qlo <= ask && ask < qhi => {
                qualifying.push(Candidate {
                    signal: s.clone(),
                    best_ask: ask,
                    best_bid: bbo.best_bid,
                    phantom_reject: None,
                });
            }
            _ => {
                counts.out_of_q2q4 += 1;
                // Prefer stale_book sample if one was already taken (it wasn't if we
                // are here first); else take this out_of_q2q4 as the sample.
                if sample_rejected.is_none() {
                    sample_rejected = Some(s.clone());
                }
            }
        }
    }

    // to_validate: one stratum-winning candidate per stratum (min TTR), strata in
    // a FIXED [RW, IM] order so the signal-row primary is deterministic.
    //
    // SIMULTANEOUS-RESOLUTION TIE: in the last 5 min of a 15m window the 5m and 15m
    // markets resolve at the SAME instant, so both are RW with IDENTICAL ttr. The
    // operative Python breaks the `min(ttr)` tie by state.markets insertion order,
    // which in production consistently fires the 5m (shorter interval, verified vs
    // the live signals.csv/paper_trades.csv). We DETERMINIZE that arbitrary choice
    // with the secondary key `interval_secs` (5m=300 < 15m=900 → shorter wins),
    // matching the live bot AND the oracle. The key is then a total order (same
    // interval ⇒ same epoch ⇒ same candidate), so no residual ambiguity.
    let mut to_validate: Vec<Candidate> = Vec::new();
    for stratum in [Stratum::ResolvedWindow, Stratum::Immediate] {
        if let Some(best) = qualifying
            .iter()
            .filter(|c| c.signal.stratum == stratum)
            .min_by_key(|c| (c.signal.ttr, interval_secs(&c.signal.interval)))
        {
            to_validate.push(best.clone());
        }
    }

    // Phantom cross-check per stratum-winner (Skip in replay → no phantom reject).
    for cand in &mut to_validate {
        match rest(&cand.signal.bet_token_id) {
            RestProbe::Skip => {}
            RestProbe::Failed(status) => {
                cand.phantom_reject = Some(format!("rest_{status}"));
            }
            RestProbe::Price(p) => {
                if (cand.best_ask - p).abs() >= cfg.phantom_threshold {
                    cand.phantom_reject = Some("phantom_ws_vs_rest".to_string());
                }
            }
        }
    }

    // Fire loop: REGLA C (opposite-close, skip open) → phantom gate → fire.
    let mut actions: Vec<Action> = Vec::new();
    let mut any_fired = false;
    let mut any_closed = false;
    for cand in &to_validate {
        let s = &cand.signal;
        // REGLA C (only when enabled — the opposite-closes variant): opposite-side
        // position(s) on the SAME market = any LOT on the opposite token (token ids
        // are unique per market). All such lots close, and the new side does NOT
        // open. With REGLA C OFF (baseline, operative), this is skipped → the new
        // side fires normally regardless of opposite positions.
        if cfg.regla_c_enabled && positions.iter().any(|p| p.token_id == s.opposite_token_id) {
            let close_price = book.bbo(&s.opposite_token_id).and_then(|b| b.best_bid);
            if close_price.is_some() {
                any_closed = true;
            }
            actions.push(Action::CloseOpposite {
                interval: s.interval.clone(),
                stratum: s.stratum,
                opposite_token: s.opposite_token_id.clone(),
                close_price,
            });
            continue; // skip opening the new side
        }
        if let Some(reason) = &cand.phantom_reject {
            actions.push(Action::Reject {
                interval: s.interval.clone(),
                stratum: s.stratum,
                reason: reason.clone(),
            });
            continue;
        }
        // Fire: paper takes top-of-book; shares = stake / fill (f64, Python-parity).
        let fill = cand.best_ask;
        let shares = if fill > 0.0 { cfg.stake_usd / fill } else { 0.0 };
        actions.push(Action::Fire {
            interval: s.interval.clone(),
            stratum: s.stratum,
            bet_token: s.bet_token_id.clone(),
            fill,
            shares,
        });
        any_fired = true;
    }

    // Primary candidate for the signal row (Python: to_validate[0] else sample).
    let primary_interval = to_validate
        .first()
        .map(|c| c.signal.interval.clone())
        .or_else(|| sample_rejected.as_ref().map(|s| s.interval.clone()));

    // reject_reason priority (Python trading_loop):
    //   "" if fired | closed_existing_instead | phantom | stale_book | no_qualifying_market
    let phantom_primary = to_validate
        .first()
        .and_then(|c| c.phantom_reject.clone());
    let reject_reason = if any_fired {
        String::new()
    } else if any_closed {
        "closed_existing_instead".to_string()
    } else if let Some(p) = phantom_primary {
        p
    } else if counts.stale_book > 0 && counts.out_of_q2q4 == 0 && counts.no_book == 0 {
        "stale_book".to_string()
    } else {
        "no_qualifying_market".to_string()
    };

    TriggerDecision {
        asset: asset.to_string(),
        trigger_ts,
        direction,
        actions,
        fired: any_fired || any_closed,
        reject_reason,
        primary_interval,
    }
}

/// Interval → seconds (mirrors the signal engine's INTERVALS).
fn interval_secs(interval: &str) -> i64 {
    crate::signal::INTERVALS
        .iter()
        .find(|(iv, _)| *iv == interval)
        .map(|(_, s)| *s)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::signal::{Direction, Signal, Stratum};
    use std::collections::HashMap;

    /// Book provider backed by a fixed map (token → EventBbo).
    struct MapBook(HashMap<String, EventBbo>);
    impl BookProvider for MapBook {
        fn bbo(&self, token: &str) -> Option<EventBbo> {
            self.0.get(token).copied()
        }
    }

    fn bbo(ask: Option<f64>, bid: Option<f64>, ts: i64) -> EventBbo {
        EventBbo {
            best_ask: ask,
            best_bid: bid,
            ts_ms: ts,
        }
    }

    fn sig(asset: &str, t: i64, interval: &str, dir: Direction, stratum: Stratum, ttr: i64) -> Signal {
        Signal {
            asset: asset.to_string(),
            trigger_ts: t,
            window_s: 1,
            ret_bps: if matches!(dir, Direction::Up) { 7.0 } else { -7.0 },
            direction: dir,
            interval: interval.to_string(),
            epoch: (t / interval_secs(interval)) * interval_secs(interval),
            ttr,
            stratum,
            bet_token_id: format!("{asset}-{interval}-bet"),
            opposite_token_id: format!("{asset}-{interval}-opp"),
        }
    }

    fn no_rest(_t: &str) -> RestProbe {
        RestProbe::Skip
    }

    #[test]
    fn fires_in_band_and_sizes() {
        let cfg = DecisionConfig::default();
        let t = 1_780_000_100;
        let s = sig("BTC", t, "5m", Direction::Up, Stratum::ResolvedWindow, 200);
        let mut bk = HashMap::new();
        bk.insert(s.bet_token_id.clone(), bbo(Some(0.50), Some(0.49), now_fresh(t)));
        let d = decide("BTC", t, Direction::Up, &[s], &cfg, &MapBook(bk), (t + 1) * 1000, no_rest, &[]);
        assert!(d.fired);
        assert_eq!(d.reject_reason, "");
        match &d.actions[0] {
            Action::Fire { fill, shares, .. } => {
                assert_eq!(*fill, 0.50);
                assert!((*shares - 20.0).abs() < 1e-9); // 10 / 0.50
            }
            a => panic!("expected Fire, got {a:?}"),
        }
    }

    #[test]
    fn rejects_out_of_band_q2q4() {
        let cfg = DecisionConfig::default();
        let t = 1_780_000_100;
        let s = sig("BTC", t, "5m", Direction::Up, Stratum::ResolvedWindow, 200);
        let mut bk = HashMap::new();
        // RW band is [0.26, 0.97). 0.97 is OUT (strict <).
        bk.insert(s.bet_token_id.clone(), bbo(Some(0.97), Some(0.96), now_fresh(t)));
        let d = decide("BTC", t, Direction::Up, &[s], &cfg, &MapBook(bk), (t + 1) * 1000, no_rest, &[]);
        assert!(!d.fired);
        assert_eq!(d.reject_reason, "no_qualifying_market");
    }

    #[test]
    fn boundary_026_is_in_097_is_out() {
        let cfg = DecisionConfig::default();
        let t = 1_780_000_100;
        // 0.26 in-band (>=), 0.97 out (< strict).
        for (ask, want_fire) in [(0.26, true), (0.97, false), (0.969, true), (0.259, false)] {
            let s = sig("BTC", t, "5m", Direction::Up, Stratum::ResolvedWindow, 200);
            let mut bk = HashMap::new();
            bk.insert(s.bet_token_id.clone(), bbo(Some(ask), Some(ask - 0.01), now_fresh(t)));
            let d = decide("BTC", t, Direction::Up, &[s], &cfg, &MapBook(bk), (t + 1) * 1000, no_rest, &[]);
            assert_eq!(d.fired, want_fire, "ask={ask}");
        }
    }

    #[test]
    fn stale_book_rejected() {
        let cfg = DecisionConfig::default();
        let t = 1_780_000_100;
        let s = sig("BTC", t, "5m", Direction::Up, Stratum::ResolvedWindow, 200);
        let mut bk = HashMap::new();
        // book_ts 4s before now → age 4000 > 3000 → stale.
        let now = (t + 1) * 1000;
        bk.insert(s.bet_token_id.clone(), bbo(Some(0.50), Some(0.49), now - 4000));
        let d = decide("BTC", t, Direction::Up, &[s], &cfg, &MapBook(bk), now, no_rest, &[]);
        assert!(!d.fired);
        assert_eq!(d.reject_reason, "stale_book");
    }

    #[test]
    fn no_book_rejected() {
        let cfg = DecisionConfig::default();
        let t = 1_780_000_100;
        let s = sig("BTC", t, "5m", Direction::Up, Stratum::ResolvedWindow, 200);
        let d = decide("BTC", t, Direction::Up, &[s], &cfg, &MapBook(HashMap::new()), (t + 1) * 1000, no_rest, &[]);
        assert!(!d.fired);
        assert_eq!(d.reject_reason, "no_qualifying_market");
    }

    #[test]
    fn regla_c_closes_opposite_and_skips_open() {
        use crate::state::persist::{OpenPosition, OrderStatus};
        use rust_decimal_macros::dec;
        // REGLA C ON (opposite-closes variant).
        let cfg = DecisionConfig { regla_c_enabled: true, ..Default::default() };
        let t = 1_780_000_100;
        let s = sig("BTC", t, "5m", Direction::Up, Stratum::ResolvedWindow, 200);
        // An opposite (Down) lot exists on the opposite token.
        let positions = vec![OpenPosition {
            token_id: s.opposite_token_id.clone(),
            asset: "BTC".to_string(),
            side: Outcome::Down,
            entry_price: dec!(0.45),
            shares: dec!(22.0),
            opened_at_ms: (t - 30) * 1000,
            signal_id: "prev".into(),
            interval: "5m".into(),
            exit_ts_s: t + 90,
            // v3 (Pieza 1) sentinels: these test fixtures predate smart-exit
            // and only exercise the decision engine (which never reads them).
            entry_ts_ms: 0,
            running_max_bid: 0.0,
            ts_max_bid_ms: 0,
            status: OrderStatus::Confirmed,
            order_id: None,
            ack_at_ms: None,
            confirmed_at_ms: None,
            confirmation_source: None,
            maker_exit: None,
        }];
        let mut bk = HashMap::new();
        bk.insert(s.bet_token_id.clone(), bbo(Some(0.50), Some(0.49), now_fresh(t)));
        bk.insert(s.opposite_token_id.clone(), bbo(Some(0.55), Some(0.54), now_fresh(t)));
        let d = decide("BTC", t, Direction::Up, &[s], &cfg, &MapBook(bk), (t + 1) * 1000, no_rest, &positions);
        assert!(d.fired); // closed-by-opposite counts as fired
        assert_eq!(d.reject_reason, "closed_existing_instead");
        match &d.actions[0] {
            Action::CloseOpposite { close_price, opposite_token, .. } => {
                assert_eq!(*close_price, Some(0.54)); // opposite token's best_bid
                assert_eq!(opposite_token, &"BTC-5m-opp".to_string());
            }
            a => panic!("expected CloseOpposite, got {a:?}"),
        }
    }

    #[test]
    fn same_side_existing_does_not_trigger_regla_c() {
        use crate::state::persist::{OpenPosition, OrderStatus};
        use rust_decimal_macros::dec;
        // REGLA C ON, but the existing lot is SAME-side → no opposite → fires (DCA).
        let cfg = DecisionConfig { regla_c_enabled: true, ..Default::default() };
        let t = 1_780_000_100;
        let s = sig("BTC", t, "5m", Direction::Up, Stratum::ResolvedWindow, 200);
        // A SAME-side (Up = bet token) lot exists → REGLA C does NOT fire; the new
        // trigger opens normally (DCA = another lot).
        let positions = vec![OpenPosition {
            token_id: s.bet_token_id.clone(),
            asset: "BTC".to_string(),
            side: Outcome::Up,
            entry_price: dec!(0.48),
            shares: dec!(20.0),
            opened_at_ms: (t - 30) * 1000,
            signal_id: "prev".into(),
            interval: "5m".into(),
            exit_ts_s: t + 90,
            // v3 (Pieza 1) sentinels: these test fixtures predate smart-exit
            // and only exercise the decision engine (which never reads them).
            entry_ts_ms: 0,
            running_max_bid: 0.0,
            ts_max_bid_ms: 0,
            status: OrderStatus::Confirmed,
            order_id: None,
            ack_at_ms: None,
            confirmed_at_ms: None,
            confirmation_source: None,
            maker_exit: None,
        }];
        let mut bk = HashMap::new();
        bk.insert(s.bet_token_id.clone(), bbo(Some(0.50), Some(0.49), now_fresh(t)));
        let d = decide("BTC", t, Direction::Up, &[s], &cfg, &MapBook(bk), (t + 1) * 1000, no_rest, &positions);
        assert!(matches!(d.actions[0], Action::Fire { .. }), "same-side → fire (DCA)");
    }

    #[test]
    fn baseline_off_fires_despite_opposite_position() {
        use crate::state::persist::{OpenPosition, OrderStatus};
        use rust_decimal_macros::dec;
        // REGLA C OFF (baseline, operative): an opposite position exists, but the
        // new trigger OPENS normally (no close) — the baseline behavior.
        let cfg = DecisionConfig::default(); // regla_c_enabled = false
        let t = 1_780_000_100;
        let s = sig("BTC", t, "5m", Direction::Up, Stratum::ResolvedWindow, 200);
        let positions = vec![OpenPosition {
            token_id: s.opposite_token_id.clone(),
            asset: "BTC".to_string(),
            side: Outcome::Down,
            entry_price: dec!(0.45),
            shares: dec!(22.0),
            opened_at_ms: (t - 30) * 1000,
            signal_id: "prev".into(),
            interval: "5m".into(),
            exit_ts_s: t + 90,
            // v3 (Pieza 1) sentinels: these test fixtures predate smart-exit
            // and only exercise the decision engine (which never reads them).
            entry_ts_ms: 0,
            running_max_bid: 0.0,
            ts_max_bid_ms: 0,
            status: OrderStatus::Confirmed,
            order_id: None,
            ack_at_ms: None,
            confirmed_at_ms: None,
            confirmation_source: None,
            maker_exit: None,
        }];
        let mut bk = HashMap::new();
        bk.insert(s.bet_token_id.clone(), bbo(Some(0.50), Some(0.49), now_fresh(t)));
        let d = decide("BTC", t, Direction::Up, &[s], &cfg, &MapBook(bk), (t + 1) * 1000, no_rest, &positions);
        assert!(d.fired);
        assert_eq!(d.reject_reason, "");
        assert!(matches!(d.actions[0], Action::Fire { .. }), "baseline opens despite opposite");
    }

    #[test]
    fn phantom_rejects_when_rest_disagrees() {
        let cfg = DecisionConfig::default();
        let t = 1_780_000_100;
        let s = sig("BTC", t, "5m", Direction::Up, Stratum::ResolvedWindow, 200);
        let mut bk = HashMap::new();
        bk.insert(s.bet_token_id.clone(), bbo(Some(0.50), Some(0.49), now_fresh(t)));
        // REST says 0.65; |0.50 - 0.65| = 0.15 >= 0.10 → phantom.
        let d = decide("BTC", t, Direction::Up, &[s], &cfg, &MapBook(bk), (t + 1) * 1000, |_| RestProbe::Price(0.65), &[]);
        assert!(!d.fired);
        assert_eq!(d.reject_reason, "phantom_ws_vs_rest");
    }

    #[test]
    fn simultaneous_resolution_tie_fires_shorter_interval() {
        // Last 5 min of a 15m window: 5m and 15m resolve at the SAME instant, both
        // RW with identical ttr. Exactly ONE fires (per-stratum winner), and the
        // determinized tie-break (shorter interval_secs) picks the 5m — matching
        // the live bot. Verified order-independent.
        let cfg = DecisionConfig::default();
        let t = 1_779_932_485; // a real tie trigger (ttr_5m == ttr_15m == 215)
        let s5 = sig("ETH", t, "5m", Direction::Down, Stratum::ResolvedWindow, 215);
        let s15 = sig("ETH", t, "15m", Direction::Down, Stratum::ResolvedWindow, 215);
        let now = (t + 1) * 1000;
        for order in [vec![s5.clone(), s15.clone()], vec![s15.clone(), s5.clone()]] {
            let mut bk = HashMap::new();
            bk.insert(s5.bet_token_id.clone(), bbo(Some(0.50), Some(0.49), now));
            bk.insert(s15.bet_token_id.clone(), bbo(Some(0.50), Some(0.49), now));
            let d = decide("ETH", t, Direction::Down, &order, &cfg, &MapBook(bk), now, no_rest, &[]);
            assert!(d.fired);
            let fires: Vec<_> = d.actions.iter().filter(|a| matches!(a, Action::Fire { .. })).collect();
            assert_eq!(fires.len(), 1, "exactly one RW winner");
            match fires[0] {
                Action::Fire { bet_token, interval, .. } => {
                    assert_eq!(interval, "5m", "shorter interval wins the tie");
                    assert_eq!(bet_token, &s5.bet_token_id);
                }
                _ => unreachable!(),
            }
            assert_eq!(d.primary_interval.as_deref(), Some("5m"));
        }
    }

    /// A fresh book_ts for trigger `t`: now = (t+1)*1000, so ts = now keeps age 0.
    fn now_fresh(t: i64) -> i64 {
        (t + 1) * 1000
    }
}
