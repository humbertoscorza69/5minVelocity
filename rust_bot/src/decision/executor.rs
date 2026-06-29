//! Execution layer. The [`Executor`] trait is the seam between the decision
//! engine and HOW a trade is placed; [`PaperExecutor`] simulates fills at the
//! event-BBO and never touches capital. The future LiveExecutor (Phase 6) will
//! place real orders behind the same interface.
//!
//! POSITION MODEL: a LIST of lots ([`crate::state::persist::BotState::positions`]
//! = `Vec<OpenPosition>`). Each fire appends a lot (DCA = multiple lots on the
//! same token). On-chain Polymarket AGGREGATES these into a single position
//! (`/positions`: one entry per token, avg price) — `Σ lots per token` equals
//! that aggregate (reality), while each lot exits independently at its own
//! `exit_ts_s` (paper-faithful: matches the Python `position_close_loop`). The
//! total P&L is identical either way (the equivalence proven in DECISIONS.md).

use rust_decimal::Decimal;

use super::BookProvider;
use crate::state::persist::{OpenPosition, OrderStatus, Outcome};

/// Intent to open a lot — produced by the decision engine on a fire.
#[derive(Debug, Clone)]
pub struct OpenIntent {
    pub token_id: String,
    /// Asset ticker (e.g. "BTC", "ETH"). Populated from `DecisionCtx.asset`
    /// at the call site -- the structured value, not parsed from any string.
    /// Pieza 4 propagates this through to OpenPosition.asset so the smart-
    /// exit evaluator can build the cell key (`"<asset>:<interval>"`) for
    /// per-cell rule lookup.
    pub asset: String,
    pub side: Outcome,
    /// Fill price = best_ask (event-BBO). f64 for Python parity; stored as Decimal.
    pub fill_price: f64,
    /// Shares for THIS fire = stake / fill (f64 for parity; stored as Decimal).
    pub shares: f64,
    pub interval: String,
    pub signal_id: String,
    /// Wall-clock second the time-based exit fires (trigger_ts + hold_s).
    pub exit_ts_s: i64,
    pub now_ms: i64,
}

/// Why a lot closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitKind {
    /// Time-based exit at +hold_s via best_bid (the strategy's design exit).
    BookBid,
    /// Closed early by an opposite-side trigger (REGLA C).
    OppositeTriggerClose,
    /// Pieza 4: closed early by an Smart rule (f1 + f6 both met). Per-cell
    /// rule looked up in `bot.toml [exits.cells]`.
    SmartTriggered,
    /// Pieza 4: closed early by an F6Only rule (time since last running-max
    /// strict-high crossed y_sec).
    F6Triggered,
    /// COMBO Phase 3: B3 maker first-touch FILL at target (the WIN path) OR a
    /// crossable taker-capture at the bid (both close at >= target).
    B3MakerFill,
    /// COMBO Phase 3: B3 F2_market fallback (timeout cancel -> break-even / time
    /// exit at the bid, taker). Distinct from BookBid so the analysis can tell a
    /// B3 fallback from a baseline time-exit.
    B3Fallback,
}

impl ExitKind {
    pub fn as_str(self) -> &'static str {
        match self {
            ExitKind::BookBid => "book_bid",
            ExitKind::OppositeTriggerClose => "opposite_trigger_close",
            ExitKind::SmartTriggered => "smart_triggered",
            ExitKind::F6Triggered => "f6_triggered",
            ExitKind::B3MakerFill => "b3_maker_fill",
            ExitKind::B3Fallback => "b3_fallback",
        }
    }
}

/// A closed paper lot (realized P&L), for eval/logging. P&L uses the LOT's entry
/// (paper-exact, = the Python); summed over lots it equals the avg-basis aggregate.
#[derive(Debug, Clone, PartialEq)]
pub struct ClosedTrade {
    pub token_id: String,
    pub side: Outcome,
    pub interval: String,
    pub signal_id: String,
    pub entry_price: Decimal,
    pub exit_price: Decimal,
    pub shares: Decimal,
    pub realized_pnl: Decimal,
    pub exit_kind: ExitKind,
    pub opened_at_ms: i64,
    pub closed_at_ms: i64,
}

/// The execution interface. Paper simulates; the future LiveExecutor places orders.
pub trait Executor {
    /// Append a lot — paper: simulated immediate fill.
    fn open(&mut self, intent: OpenIntent);
    /// Close ALL lots on `token` at `exit_price` (REGLA C closes every opposite
    /// lot at one price). Returns the closed lots (empty if none).
    fn close_token(
        &mut self,
        token: &str,
        exit_price: f64,
        kind: ExitKind,
        now_ms: i64,
    ) -> Vec<ClosedTrade>;
}

/// Paper executor: lots live in the borrowed `BotState.positions` Vec; fills are
/// simulated at the given price; realized P&L is accumulated.
pub struct PaperExecutor<'a> {
    positions: &'a mut Vec<OpenPosition>,
    pub closed: Vec<ClosedTrade>,
    pub realized_pnl: Decimal,
}

impl<'a> PaperExecutor<'a> {
    pub fn new(positions: &'a mut Vec<OpenPosition>) -> Self {
        Self {
            positions,
            closed: Vec::new(),
            realized_pnl: Decimal::ZERO,
        }
    }

    /// Realize a removed lot at `exit`, recording the closed trade + P&L.
    fn realize(
        &mut self,
        pos: OpenPosition,
        exit: Decimal,
        kind: ExitKind,
        now_ms: i64,
    ) -> ClosedTrade {
        let realized = pos.shares * (exit - pos.entry_price);
        self.realized_pnl += realized;
        let ct = ClosedTrade {
            token_id: pos.token_id,
            side: pos.side,
            interval: pos.interval,
            signal_id: pos.signal_id,
            entry_price: pos.entry_price,
            exit_price: exit,
            shares: pos.shares,
            realized_pnl: realized,
            exit_kind: kind,
            opened_at_ms: pos.opened_at_ms,
            closed_at_ms: now_ms,
        };
        self.closed.push(ct.clone());
        ct
    }

    /// Close every lot whose `exit_ts_s` has passed, EACH at its token's CURRENT
    /// best_bid (faithful to the Python's per-trade independent time-exit). A lot
    /// whose token has no bid is left for a later tick (matches the Python skip).
    /// The caller drives this at the close cadence (1s, like `position_close_loop`).
    pub fn close_due(&mut self, now_ms: i64, book: &dyn BookProvider) -> Vec<ClosedTrade> {
        let now_s = now_ms / 1000;
        let mut closed = Vec::new();
        let mut i = 0;
        while i < self.positions.len() {
            // COMBO Phase 3 defense: a position with an ACTIVE maker exit is
            // owned by `process_b3_makers` (place/fill/timeout-fallback). It
            // must NEVER be closed here via the baseline book-bid -- that would
            // double-handle it (and, on a live `defer`, sell while a maker is
            // still working). process_b3_makers removes due B3 lots before
            // close_due runs, so in paper this is belt-and-suspenders.
            if self.positions[i].exit_ts_s <= now_s && self.positions[i].maker_exit.is_none() {
                let tok = self.positions[i].token_id.clone();
                if let Some(bid) = book.bbo(&tok).and_then(|b| b.best_bid) {
                    let pos = self.positions.remove(i);
                    let ct = self.realize(pos, dec(bid), ExitKind::BookBid, now_ms);
                    closed.push(ct);
                    continue; // element at i shifted down; re-check the same index
                }
            }
            i += 1;
        }
        closed
    }

    pub fn open_count(&self) -> usize {
        self.positions.len()
    }

    /// Pieza 4: apply pre-computed smart-exit decisions. Each tuple is
    /// `(lot_index, exit_price, exit_kind)` where lot_index references the
    /// position in `self.positions` at evaluation time. The caller MUST
    /// pass tuples in DESCENDING `lot_index` order (already enforced via
    /// the sort here) so that earlier `remove()` calls do not shift the
    /// indices of subsequent ones.
    ///
    /// Returns the closed trades in the order they were applied (descending
    /// index = most recent first). A tuple whose `lot_index` is out of
    /// bounds is silently skipped (defensive against stale decisions).
    pub fn apply_smart_exits(
        &mut self,
        mut decisions: Vec<(usize, f64, ExitKind)>,
        now_ms: i64,
    ) -> Vec<ClosedTrade> {
        // Sort descending so remove() at higher index doesn't invalidate
        // lower indices in the same batch.
        decisions.sort_by(|a, b| b.0.cmp(&a.0));
        let mut closed = Vec::new();
        for (idx, exit_price, kind) in decisions {
            if idx < self.positions.len() {
                let pos = self.positions.remove(idx);
                let ct = self.realize(pos, dec(exit_price), kind, now_ms);
                closed.push(ct);
            }
        }
        closed
    }
}

fn dec(x: f64) -> Decimal {
    Decimal::try_from(x).unwrap_or(Decimal::ZERO)
}

impl Executor for PaperExecutor<'_> {
    fn open(&mut self, intent: OpenIntent) {
        // Pieza 2: populate the smart-exit state at open time.
        //
        //   entry_ts_ms     = intent.now_ms (wall-clock at fill ack; the f1
        //                     denominator `hold_duration_ms` is computed from
        //                     this in the rule evaluator).
        //   running_max_bid = entry_price (conservative initialization: the
        //                     ask we paid is treated as the first observed
        //                     "peak". Any subsequent bid > entry_price will
        //                     overwrite it on the next 1s tick via
        //                     update_running_max. The Pieza 2 plan picked
        //                     Option A here -- no need to pass the BBO into
        //                     the executor; the first tick refines it).
        //   ts_max_bid_ms   = entry_ts_ms (same wall-clock as the peak; f6 =
        //                     now - ts_max starts counting from this instant
        //                     until the first new high updates it).
        let entry_ts_ms = intent.now_ms;
        let entry_price_f64 = intent.fill_price;
        self.positions.push(OpenPosition {
            token_id: intent.token_id.clone(),
            asset: intent.asset.clone(),
            side: intent.side,
            entry_price: dec(intent.fill_price),
            shares: dec(intent.shares),
            opened_at_ms: intent.now_ms,
            signal_id: intent.signal_id,
            interval: intent.interval,
            exit_ts_s: intent.exit_ts_s,
            entry_ts_ms,
            running_max_bid: entry_price_f64,
            ts_max_bid_ms: entry_ts_ms,
            // Paper: the fill is simulated as immediate + certain.
            status: OrderStatus::Confirmed,
            order_id: Some(format!("paper-{}-{}", intent.token_id, intent.now_ms)),
            ack_at_ms: Some(intent.now_ms),
            confirmed_at_ms: Some(intent.now_ms),
            confirmation_source: None,
            // v5: maker exit is placed later by the exit task (Pieza 1), not at
            // open. None at open time -- byte-identical to pre-v5 paper opens.
            maker_exit: None,
        });
    }

    fn close_token(
        &mut self,
        token: &str,
        exit_price: f64,
        kind: ExitKind,
        now_ms: i64,
    ) -> Vec<ClosedTrade> {
        let exit = dec(exit_price);
        let mut closed = Vec::new();
        let mut i = 0;
        while i < self.positions.len() {
            if self.positions[i].token_id == token {
                let pos = self.positions.remove(i);
                let ct = self.realize(pos, exit, kind, now_ms);
                closed.push(ct);
            } else {
                i += 1;
            }
        }
        closed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::EventBbo;
    use rust_decimal_macros::dec as d;
    use std::collections::HashMap;

    fn intent(token: &str, side: Outcome, fill: f64, shares: f64, exit_ts_s: i64) -> OpenIntent {
        OpenIntent {
            token_id: token.to_string(),
            asset: "BTC".to_string(),
            side,
            fill_price: fill,
            shares,
            interval: "5m".to_string(),
            signal_id: format!("{token}-sig"),
            exit_ts_s,
            now_ms: 1_000,
        }
    }

    struct MapBook(HashMap<String, EventBbo>);
    impl BookProvider for MapBook {
        fn bbo(&self, token: &str) -> Option<EventBbo> {
            self.0.get(token).copied()
        }
    }

    #[test]
    fn open_then_close_token_pnl() {
        let mut positions = Vec::new();
        let mut ex = PaperExecutor::new(&mut positions);
        ex.open(intent("tok", Outcome::Up, 0.50, 20.0, 1_120));
        assert_eq!(ex.open_count(), 1);
        let closed = ex.close_token("tok", 0.55, ExitKind::BookBid, 2_000);
        assert_eq!(closed.len(), 1);
        assert_eq!(closed[0].realized_pnl, d!(1.0)); // 20*(0.55-0.50)
        assert_eq!(ex.open_count(), 0);
        assert_eq!(ex.realized_pnl, d!(1.0));
    }

    // ========================================================================
    // Pieza 2: smart-exit state is populated at paper open time.
    // (Sentinel zeros from Pieza 1 are now replaced with real values.)
    // ========================================================================

    #[test]
    fn paper_open_populates_entry_ts_ms_from_intent_now_ms() {
        let mut positions = Vec::new();
        let mut ex = PaperExecutor::new(&mut positions);
        let mut it = intent("tok", Outcome::Up, 0.50, 20.0, 1_120);
        it.now_ms = 1_780_000_000_500;
        ex.open(it);
        assert_eq!(positions.len(), 1);
        assert_eq!(
            positions[0].entry_ts_ms, 1_780_000_000_500,
            "entry_ts_ms MUST mirror intent.now_ms exactly (the f1 denominator)"
        );
        assert_ne!(
            positions[0].entry_ts_ms, 0,
            "entry_ts_ms must NOT be sentinel 0 after Pieza 2 (would mark the \
             position smart-exit ineligible incorrectly)"
        );
    }

    #[test]
    fn paper_open_populates_running_max_bid_from_entry_price() {
        let mut positions = Vec::new();
        let mut ex = PaperExecutor::new(&mut positions);
        ex.open(intent("tok", Outcome::Up, 0.52, 20.0, 1_120));
        assert_eq!(
            positions[0].running_max_bid, 0.52,
            "running_max_bid MUST initialize to entry_price (the ask we paid; \
             conservative seed before any tick refines it)"
        );
    }

    #[test]
    fn paper_open_populates_ts_max_bid_ms_equals_entry_ts_ms() {
        let mut positions = Vec::new();
        let mut ex = PaperExecutor::new(&mut positions);
        let mut it = intent("tok", Outcome::Up, 0.50, 20.0, 1_120);
        it.now_ms = 1_780_000_000_777;
        ex.open(it);
        let p = &positions[0];
        assert_eq!(
            p.ts_max_bid_ms, p.entry_ts_ms,
            "ts_max_bid_ms MUST start equal to entry_ts_ms (f6 = now - ts_max \
             begins counting from the open instant; first new high overrides)"
        );
        assert_eq!(p.ts_max_bid_ms, 1_780_000_000_777);
    }

    #[test]
    fn dca_appends_separate_lots() {
        let mut positions = Vec::new();
        let mut ex = PaperExecutor::new(&mut positions);
        ex.open(intent("tok", Outcome::Up, 0.50, 20.0, 1_120));
        ex.open(intent("tok", Outcome::Up, 0.60, 20.0, 1_150));
        assert_eq!(ex.open_count(), 2, "DCA = two separate lots, NOT combined");
    }

    #[test]
    fn close_token_closes_all_lots_at_one_price() {
        // REGLA C: every lot on the opposite token closes at the same best_bid.
        let mut positions = Vec::new();
        let mut ex = PaperExecutor::new(&mut positions);
        ex.open(intent("opp", Outcome::Down, 0.40, 25.0, 1_120)); // +0.10*25=2.5
        ex.open(intent("opp", Outcome::Down, 0.45, 22.0, 1_150)); // +0.05*22=1.1
        let closed = ex.close_token("opp", 0.50, ExitKind::OppositeTriggerClose, 2_000);
        assert_eq!(closed.len(), 2);
        assert_eq!(ex.realized_pnl, d!(3.6)); // 2.5 + 1.1
        assert!(closed.iter().all(|c| c.exit_kind == ExitKind::OppositeTriggerClose));
    }

    #[test]
    fn close_due_closes_each_lot_at_its_exit() {
        let mut positions = Vec::new();
        let mut ex = PaperExecutor::new(&mut positions);
        // lot1 exits at 1120, lot2 at 1150 (same token).
        ex.open(intent("tok", Outcome::Up, 0.50, 20.0, 1_120));
        ex.open(intent("tok", Outcome::Up, 0.60, 20.0, 1_150));
        let mut bk = HashMap::new();
        bk.insert("tok".to_string(), EventBbo { best_ask: Some(0.56), best_bid: Some(0.55), ts_ms: 1 });
        let book = MapBook(bk);
        // now = 1125s → only lot1 (exit 1120) is due.
        let c1 = ex.close_due(1_125_000, &book);
        assert_eq!(c1.len(), 1);
        assert_eq!(ex.open_count(), 1);
        // now = 1160s → lot2 (exit 1150) due.
        let c2 = ex.close_due(1_160_000, &book);
        assert_eq!(c2.len(), 1);
        assert_eq!(ex.open_count(), 0);
    }

    #[test]
    fn close_due_skips_lot_with_no_bid() {
        let mut positions = Vec::new();
        let mut ex = PaperExecutor::new(&mut positions);
        ex.open(intent("tok", Outcome::Up, 0.50, 20.0, 1_120));
        let mut bk = HashMap::new();
        bk.insert("tok".to_string(), EventBbo { best_ask: Some(0.99), best_bid: None, ts_ms: 1 });
        let closed = ex.close_due(1_200_000, &MapBook(bk));
        assert!(closed.is_empty(), "no bid → leave for a later tick");
        assert_eq!(ex.open_count(), 1);
    }

    #[test]
    fn close_token_missing_is_empty() {
        let mut positions = Vec::new();
        let mut ex = PaperExecutor::new(&mut positions);
        assert!(ex.close_token("nope", 0.5, ExitKind::BookBid, 1).is_empty());
    }
}
