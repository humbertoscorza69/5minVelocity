//! Phase 4: the signal engine — Capa A parity with the Python operational bot
//! (`scripts/paper_trade_lagarb_directional_opposite_closes.py`).
//!
//! SCOPE (Option A, locked at STOP 1): this layer is ONLY the part of the Python
//! that depends solely on Binance klines + integer epoch arithmetic — i.e. it is
//! fully deterministic and replay-reproducible:
//!
//!   * trigger detection (`check_trigger`): the 1s/3s rolling-return rule,
//!   * direction (sign of the trigger return),
//!   * market scope (`find_qualifying_markets` MINUS the trade gates): the
//!     scope-guard epoch, the TTR bounds, the RW/IM stratum, and the bet token.
//!
//! Everything that depends on the Polymarket BOOK PRICE or wall-clock — the
//! Q2-Q4 entry band, the staleness gate, the phantom REST cross-check, REGLA C,
//! and sizing — is the DECISION engine (Phase 5). Those are intentionally NOT
//! here: they cannot be replayed bit-exactly (staleness reads `time.time()`, the
//! phantom check makes a live REST call), so keeping them out is what makes the
//! Capa A 100% gate on `(asset, trigger_ts, interval, direction)` achievable.
//!
//! PARITY NOTE — f64, not Decimal. The Python computes the trigger with `float`
//! (IEEE-754 double). Rust `f64` is the same type; with the SAME ORDER OF
//! OPERATIONS the result is bit-identical. `Decimal` would DIVERGE (exact decimal
//! vs binary), so the signal math is f64. (Decimal stays for money/state, Phase 3.)
//! The exact Python expression is `(last_close / c_prev - 1.0) * 10_000.0` and is
//! reproduced verbatim below — the operation order is load-bearing for the last
//! ULP of the result, which is the difference between 100% and 99.9%.
//!
//! SAME LOGIC IN REPLAY AND LIVE: this module is the engine; `replay.rs` is only a
//! file-fed driver for Capa A. The live wiring (Phase 5) feeds the SAME
//! [`SignalEngine::on_kline`] from the Binance WS client. One codebase, one logic.

#![allow(dead_code)] // live wiring (expand_signals/Catalog from the WS task) lands in Phase 5

use std::collections::{HashMap, VecDeque};

pub mod replay;

/// |rolling return| in bps that fires a trigger. Python `TRIGGER_BPS`.
pub const TRIGGER_BPS: f64 = 5.0;
/// Per-asset debounce window in seconds. Python `DEBOUNCE_S`.
pub const DEBOUNCE_S: i64 = 5;
/// Klines kept per asset = `KLINE_WINDOW_S * 2`. Python `deque(maxlen=KLINE_WINDOW_S*2)`.
pub const KLINE_WINDOW_S: usize = 30;
pub const KLINE_MAXLEN: usize = KLINE_WINDOW_S * 2;
/// Skip triggers within this many seconds of resolution. Python `MIN_TTR_FROM_RES_S`.
pub const MIN_TTR_FROM_RES_S: i64 = 30;
/// Upper TTR bound (redundant with the scope guard, kept for fidelity). Python `ttr > 900`.
pub const MAX_TTR_S: i64 = 900;
/// The market intervals the strategy trades and their second-lengths. Python `INTERVAL_SECS`.
pub const INTERVALS: [(&str, i64); 2] = [("5m", 300), ("15m", 900)];

/// The side a trigger points to. `sign_up = ret_bps > 0` in the Python.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Up,
    Down,
}

impl Direction {
    /// Matches the Python `bet_side` / signal `side` string ("Up"/"Down").
    pub fn as_str(self) -> &'static str {
        match self {
            Direction::Up => "Up",
            Direction::Down => "Down",
        }
    }
}

/// TTR stratum. Python: `"RESOLVED-WINDOW"` (ttr <= 300) or `"IMMEDIATE"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stratum {
    ResolvedWindow,
    Immediate,
}

impl Stratum {
    pub fn as_str(self) -> &'static str {
        match self {
            Stratum::ResolvedWindow => "RESOLVED-WINDOW",
            Stratum::Immediate => "IMMEDIATE",
        }
    }
}

/// A Binance trigger: `|return| >= TRIGGER_BPS` on the 1s or 3s rolling window.
#[derive(Debug, Clone, PartialEq)]
pub struct Trigger {
    pub asset: String,
    /// Kline OPEN-second (`int(k["t"]) // 1000`), NOT the close time.
    pub trigger_ts: i64,
    /// Signed return in bps of the firing window (sign drives the direction).
    pub ret_bps: f64,
    /// Which window fired: 1 or 3. 1s is checked first and wins ties.
    pub window_s: u8,
}

/// One scope-qualified signal = a trigger projected onto one active interval-market.
/// The Capa A parity key is `(asset, trigger_ts, interval, direction)`.
#[derive(Debug, Clone, PartialEq)]
pub struct Signal {
    pub asset: String,
    pub trigger_ts: i64,
    pub window_s: u8,
    pub ret_bps: f64,
    pub direction: Direction,
    pub interval: String,
    pub epoch: i64,
    pub ttr: i64,
    pub stratum: Stratum,
    /// Up or Down token of the active market (from the catalog). Not in the parity
    /// key, but carried for the decision engine + as a support column.
    pub bet_token_id: String,
    /// The OTHER side's token of the same market — what REGLA C (Phase 5) matches
    /// open positions against (opposite-side close). Token ids are unique per
    /// market, so this uniquely identifies the opposite-side position.
    pub opposite_token_id: String,
}

/// One active market the trigger can be projected onto.
#[derive(Debug, Clone, PartialEq)]
pub struct MarketRef {
    pub up_token_id: String,
    pub down_token_id: String,
    pub condition_id: String,
    pub end_time: String,
    /// W9-Pieza1: maker/taker fees + fee_type captured AS-IS from the markets
    /// log (`maker_base_fee`, `taker_base_fee`, `fee_type`). Defaults are 0/0/""
    /// (used by tests / synthetic catalogs). Interpretation of the units is the
    /// responsibility of the consumer (see `run_backtest_exits_trace` for the
    /// pass that surfaces these to JSONL for Python analysis).
    pub maker_base_fee: i64,
    pub taker_base_fee: i64,
    pub fee_type: String,
}

/// Source of active markets, by `(asset, interval, epoch)`. Implemented by the
/// replay catalog (recorder markets log) now, and by Phase 1 discovery in live.
pub trait MarketCatalog {
    fn active_market(&self, asset: &str, interval: &str, epoch: i64) -> Option<&MarketRef>;
}

/// The stateful core. Holds the per-asset kline ring + last-trigger debounce clock.
/// `on_kline` is the single entry point — identical in replay and live.
#[derive(Debug, Default)]
pub struct SignalEngine {
    /// `kline_history[asset]` = ring of `(open_second, close)`, capped at KLINE_MAXLEN.
    kline_history: HashMap<String, VecDeque<(i64, f64)>>,
    /// `last_trigger_ts[asset]` for the debounce. Python defaultdict(0) → absent = 0.
    last_trigger_ts: HashMap<String, i64>,
}

impl SignalEngine {
    pub fn new() -> Self {
        Self::default()
    }

    /// Ingest one FINALIZED kline (`k.x == true`), then test for a trigger. Mirrors
    /// the body of the Python `collect_binance` finalized-bar branch: `add_kline`
    /// then `check_trigger`, and on a fire it sets `last_trigger_ts[asset] = t_s`
    /// (the Python sets this in the collector right after pushing the trigger).
    pub fn on_kline(&mut self, asset: &str, t_s: i64, close: f64) -> Option<Trigger> {
        self.add_kline(asset, t_s, close);
        let (ret_bps, window_s) = self.check_trigger(asset, t_s)?;
        self.last_trigger_ts.insert(asset.to_string(), t_s);
        Some(Trigger {
            asset: asset.to_string(),
            trigger_ts: t_s,
            ret_bps,
            window_s,
        })
    }

    /// Python `add_kline`: update in place iff the LAST entry has the same second,
    /// else append; the ring auto-evicts the front past `KLINE_MAXLEN`.
    fn add_kline(&mut self, asset: &str, t_s: i64, close: f64) {
        let q = self.kline_history.entry(asset.to_string()).or_default();
        match q.back_mut() {
            Some(back) if back.0 == t_s => *back = (t_s, close),
            _ => {
                q.push_back((t_s, close));
                while q.len() > KLINE_MAXLEN {
                    q.pop_front();
                }
            }
        }
    }

    /// Python `check_trigger`. Returns `(ret_bps, window_s)` on the FIRST matching
    /// window (1s checked before 3s), else None. The arithmetic order is verbatim.
    fn check_trigger(&self, asset: &str, t_s: i64) -> Option<(f64, u8)> {
        let q = self.kline_history.get(asset)?;
        if q.len() < 4 {
            return None;
        }
        let &(last_t, last_close) = q.back()?;
        if last_t != t_s {
            return None;
        }
        if last_close <= 0.0 {
            return None;
        }
        // Debounce (absent => 0, matching Python defaultdict).
        let last_trig = self.last_trigger_ts.get(asset).copied().unwrap_or(0);
        if t_s - last_trig < DEBOUNCE_S {
            return None;
        }
        // 1s window first.
        if let Some(c1) = get_close_at(q, t_s - 1)
            && c1 > 0.0
        {
            let ret_1 = (last_close / c1 - 1.0) * 10_000.0;
            if ret_1.abs() >= TRIGGER_BPS {
                return Some((ret_1, 1));
            }
        }
        // 3s window.
        if let Some(c3) = get_close_at(q, t_s - 3)
            && c3 > 0.0
        {
            let ret_3 = (last_close / c3 - 1.0) * 10_000.0;
            if ret_3.abs() >= TRIGGER_BPS {
                return Some((ret_3, 3));
            }
        }
        None
    }
}

/// Python `get_close_at`: the close at the latest entry with `t <= target`, else
/// None. Scans front→back and stops at the first `t > target` (assumes the ring is
/// ascending in t, which `add_kline` maintains when klines arrive in order).
fn get_close_at(q: &VecDeque<(i64, f64)>, target: i64) -> Option<f64> {
    let mut last = None;
    for &(t, c) in q.iter() {
        if t <= target {
            last = Some(c);
        } else {
            break;
        }
    }
    last
}

/// Project a trigger onto the active markets. Python `find_qualifying_markets`
/// MINUS the trade gates (Q2-Q4 band, staleness, phantom): for each interval, the
/// scope-guard epoch is `(trigger_ts / secs) * secs`; emit a signal iff the TTR is
/// in `[MIN_TTR_FROM_RES_S, MAX_TTR_S]` AND the catalog has that active market.
pub fn expand_signals(trig: &Trigger, catalog: &dyn MarketCatalog) -> Vec<Signal> {
    let sign_up = trig.ret_bps > 0.0;
    let direction = if sign_up { Direction::Up } else { Direction::Down };
    let mut out = Vec::new();
    for (interval, secs) in INTERVALS {
        let epoch = (trig.trigger_ts / secs) * secs; // scope guard: current epoch only
        let resolution_ts = epoch + secs;
        let ttr = resolution_ts - trig.trigger_ts;
        // Two separate guards, mirroring the Python (`ttr < MIN_TTR_FROM_RES_S` then
        // `ttr > 900`). With the scope guard above, ttr <= secs-1, so the upper bound
        // is redundant for 5m and kept only for fidelity (matches the Python comment).
        if ttr < MIN_TTR_FROM_RES_S {
            continue;
        }
        if ttr > MAX_TTR_S {
            continue;
        }
        let stratum = if ttr <= 300 {
            Stratum::ResolvedWindow
        } else {
            Stratum::Immediate
        };
        if let Some(m) = catalog.active_market(&trig.asset, interval, epoch) {
            let (bet_token_id, opposite_token_id) = if sign_up {
                (m.up_token_id.clone(), m.down_token_id.clone())
            } else {
                (m.down_token_id.clone(), m.up_token_id.clone())
            };
            out.push(Signal {
                asset: trig.asset.clone(),
                trigger_ts: trig.trigger_ts,
                window_s: trig.window_s,
                ret_bps: trig.ret_bps,
                direction,
                interval: interval.to_string(),
                epoch,
                ttr,
                stratum,
                bet_token_id,
                opposite_token_id,
            });
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A catalog that always has a market (continuous BTC/ETH 5m/15m), so the
    /// scope tests exercise the arithmetic, not catalog gaps.
    struct AlwaysCatalog;
    impl MarketCatalog for AlwaysCatalog {
        fn active_market(&self, _asset: &str, _interval: &str, epoch: i64) -> Option<&MarketRef> {
            // Leak a per-call ref via a thread-local-free trick: return a static.
            static M: MarketRef = MarketRef {
                up_token_id: String::new(),
                down_token_id: String::new(),
                condition_id: String::new(),
                end_time: String::new(),
                maker_base_fee: 0,
                taker_base_fee: 0,
                fee_type: String::new(),
            };
            let _ = epoch;
            Some(&M)
        }
    }

    fn feed(eng: &mut SignalEngine, asset: &str, klines: &[(i64, f64)]) -> Vec<Trigger> {
        let mut trigs = Vec::new();
        for &(t, c) in klines {
            if let Some(tr) = eng.on_kline(asset, t, c) {
                trigs.push(tr);
            }
        }
        trigs
    }

    #[test]
    fn no_trigger_under_four_klines() {
        let mut eng = SignalEngine::new();
        // 3 klines, even with a huge jump, cannot trigger (len < 4).
        let trigs = feed(&mut eng, "BTC", &[(100, 100.0), (101, 100.0), (102, 200.0)]);
        assert!(trigs.is_empty());
    }

    #[test]
    fn trigger_fires_on_1s_window_up() {
        let mut eng = SignalEngine::new();
        // +6 bps over 1s at t=103.
        let trigs = feed(
            &mut eng,
            "BTC",
            &[(100, 100.0), (101, 100.0), (102, 100.0), (103, 100.06)],
        );
        assert_eq!(trigs.len(), 1);
        assert_eq!(trigs[0].trigger_ts, 103);
        assert_eq!(trigs[0].window_s, 1);
        assert!((trigs[0].ret_bps - 6.0).abs() < 1e-9);
        assert!(trigs[0].ret_bps > 0.0); // Up
    }

    #[test]
    fn trigger_fires_on_3s_window_when_1s_quiet() {
        let mut eng = SignalEngine::new();
        // 1s flat (no fire), 3s back is +10 bps → window 3. (NB: 100.05/100.0 in f64
        // is 4.999…bps < 5.0 and would NOT fire — the exact parity subtlety. Use a
        // value clearly above threshold so the test asserts the WINDOW, not the edge.)
        let trigs = feed(
            &mut eng,
            "ETH",
            &[(300, 100.0), (301, 100.1), (302, 100.1), (303, 100.1)],
        );
        assert_eq!(trigs.len(), 1);
        assert_eq!(trigs[0].window_s, 3);
        assert!(trigs[0].ret_bps > 0.0);
    }

    #[test]
    fn one_second_window_wins_ties() {
        let mut eng = SignalEngine::new();
        // Both 1s and 3s qualify; the 1s must be returned (checked first).
        let trigs = feed(
            &mut eng,
            "BTC",
            &[(400, 100.0), (401, 100.0), (402, 100.0), (403, 100.1)],
        );
        assert_eq!(trigs.len(), 1);
        assert_eq!(trigs[0].window_s, 1);
    }

    #[test]
    fn debounce_suppresses_within_5s() {
        let mut eng = SignalEngine::new();
        // Trigger at 500, then a would-be trigger at 502 (<5s later) is suppressed,
        // and one at 505 (exactly 5s) is allowed again.
        let trigs = feed(
            &mut eng,
            "BTC",
            &[
                (497, 100.0),
                (498, 100.0),
                (499, 100.0),
                (500, 100.1), // fires (1s, +10bps)
                (501, 100.1),
                (502, 100.2), // 502-500=2 <5 → suppressed
                (503, 100.2),
                (504, 100.2),
                (505, 100.3), // 505-500=5 not <5 → allowed
            ],
        );
        let ts: Vec<i64> = trigs.iter().map(|t| t.trigger_ts).collect();
        assert_eq!(ts, vec![500, 505]);
    }

    #[test]
    fn negative_move_is_down() {
        let mut eng = SignalEngine::new();
        let trigs = feed(
            &mut eng,
            "BTC",
            &[(600, 100.0), (601, 100.0), (602, 100.0), (603, 99.94)],
        );
        assert_eq!(trigs.len(), 1);
        assert!(trigs[0].ret_bps < 0.0); // Down
    }

    #[test]
    fn expand_emits_5m_and_15m() {
        // 1_780_000_200 is a 15m epoch (divisible by 900). T = epoch15 + 100 puts the
        // 5m at ttr=200 (RW) and the 15m at ttr=800 (IMMEDIATE) — distinct strata.
        let t = 1_780_000_300i64;
        let trig = Trigger {
            asset: "BTC".into(),
            trigger_ts: t,
            ret_bps: 7.0,
            window_s: 1,
        };
        let sigs = expand_signals(&trig, &AlwaysCatalog);
        assert_eq!(sigs.len(), 2);
        let by_iv: HashMap<&str, &Signal> =
            sigs.iter().map(|s| (s.interval.as_str(), s)).collect();
        // 5m: epoch = floor(t/300)*300 = 1_780_000_200, ttr = 200, RW.
        assert_eq!(by_iv["5m"].epoch, 1_780_000_200);
        assert_eq!(by_iv["5m"].ttr, 200);
        assert_eq!(by_iv["5m"].stratum, Stratum::ResolvedWindow);
        // 15m: epoch = 1_780_000_200, ttr = 900 - 100 = 800, IMMEDIATE.
        assert_eq!(by_iv["15m"].epoch, 1_780_000_200);
        assert_eq!(by_iv["15m"].ttr, 800);
        assert_eq!(by_iv["15m"].stratum, Stratum::Immediate);
        assert_eq!(by_iv["15m"].direction, Direction::Up);
    }

    #[test]
    fn expand_excludes_last_30s_of_an_interval() {
        // T = epoch15 + 290: ttr5 = 10 (< 30 → 5m EXCLUDED) but ttr15 = 610 (15m IN).
        let base15 = 1_780_000_200i64; // divisible by 900 and 300
        let trig = Trigger {
            asset: "BTC".into(),
            trigger_ts: base15 + 290,
            ret_bps: -8.0,
            window_s: 3,
        };
        let sigs = expand_signals(&trig, &AlwaysCatalog);
        let intervals: Vec<&str> = sigs.iter().map(|s| s.interval.as_str()).collect();
        assert!(!intervals.contains(&"5m"), "5m must be excluded in last 30s");
        assert!(intervals.contains(&"15m"), "15m (ttr=610) must still be in");
    }

    #[test]
    fn get_close_at_tolerates_gaps() {
        let q: VecDeque<(i64, f64)> = [(10, 1.0), (11, 2.0), (14, 3.0)].into_iter().collect();
        // target 13: latest t <= 13 is t=11 → 2.0 (gap at 12,13 tolerated).
        assert_eq!(get_close_at(&q, 13), Some(2.0));
        assert_eq!(get_close_at(&q, 14), Some(3.0));
        assert_eq!(get_close_at(&q, 9), None);
    }
}
