//! Order #15 A3 — the virtual fill model.
//!
//! **A maker paper fill is a MODEL, not an observation** (order A0). Every number
//! downstream depends on a queue-position assumption that cannot be verified from
//! the public feed. So this module is deliberately pure and event-driven: it takes a
//! sequence of book snapshots and trade prints and produces fills deterministically.
//! Feed it the same events twice and you get the same fills — which is what makes
//! the recorded log re-scorable offline under a *different* queue assumption without
//! re-running the bot. The previous auditor's caches were lost and the P&L
//! convention became unrecoverable; that is the failure this design prevents.
//!
//! Queue rules (order A3), and why each is pessimistic:
//!   1. On post we JOIN THE BACK: `queue_ahead` = the full displayed size at our
//!      level. We never assume priority we cannot prove.
//!   2. A trade print that reaches our level consumes `queue_ahead` FIRST; only the
//!      remainder is our fill.
//!   3. A displayed-size decrease NOT explained by prints is a cancel. We cannot see
//!      whether it sat ahead of or behind us, so we assume BEHIND — `queue_ahead` is
//!      NOT improved. The amount is recorded so the optimistic variant (cancels
//!      ahead → queue_ahead reduced) can be scored offline from the log alone.
//!
//!      This assumption is the single biggest unknown in the queue model, and it is
//!      MEASURABLE rather than permanent: `price_change` (level updates) combined with
//!      the REST print feed decomposes each size decrease into trade volume (known
//!      from REST) and cancels (the residual). The Part B recorder captures both, so a
//!      week of data replaces this pessimistic guess with a measured cancel fraction.
//!      That is why `price_change` earns its ~90% share of the recorder's disk.
//!   4. BBO moving away does not cancel anything: the naive config leaves the order
//!      resting and marks it stale-priced. (Lag defense *inverted* the study result,
//!      +0.35 → +0.25, so there is deliberately none.)
//!   5. Partial fills are normal — `size_remaining` is tracked, never all-or-nothing.

use serde::{Deserialize, Serialize};

/// One price level of the visible book.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Level {
    pub price: f64,
    pub size: f64,
}

/// Book side. `price_change` carries this, and an ask-only strategy must not let a
/// bid-side update touch its queue estimate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    Bid,
    Ask,
}

/// Everything the engine consumes. A recorded stream of these IS the replay input,
/// which is why they carry raw observed values and no derived conclusions.
///
/// **THE TWO SOURCES ARE NOT THE SAME FEED.** `Snapshot`/`LevelUpdate` come from the
/// Polymarket WS (`book`, `price_change`); `Trade` does NOT — there is no trade-print
/// channel on that socket at all. See [`MarketEvent::Trade`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum MarketEvent {
    /// Full book snapshot (the `book` channel). Full snapshots are mandatory:
    /// reconstructing depth from `price_change` deltas alone is INVALID (validated —
    /// match rate decays ~90% → 0% over a token's life).
    Snapshot {
        ts_ms: i64,
        token: String,
        /// Ascending by price. Only the ask side is used by an ask-only strategy,
        /// but bids are carried so mid/spread stay computable offline.
        asks: Vec<Level>,
        bids: Vec<Level>,
    },
    /// One `price_change` entry: the NEW RESTING SIZE at a level — **not** a traded
    /// quantity.
    ///
    /// This distinction is the whole ballgame and it is easy to get backwards: the
    /// event carries `price`, `size`, `side` and `hash`, which reads exactly like a
    /// print. It is not. Across 297,884 consecutive updates at the same
    /// (token, price, side) the size *increased* 51.4% of the time, decreased 48.3%,
    /// and was exactly 0 in 0.9% — a traded quantity can never make a level grow, and
    /// "traded zero" is meaningless. So `size` is the level's new resting depth, and
    /// treating it as volume would manufacture fills out of thin air.
    LevelUpdate { ts_ms: i64, token: String, price: f64, size: f64, side: Side },
    /// A trade print — from the REST print feed, NOT the websocket.
    ///
    /// `https://data-api.polymarket.com/trades?market=<condition_id>` is the only
    /// COMPLETE source of executed volume, and it is the same source the validated
    /// queue backtest used (4.38M re-fetched prints → the +0.455¢/share OOS figure),
    /// so scoring against that model stays apples-to-apples.
    ///
    /// The WS *does* emit `last_trade_price` — but measured against REST by
    /// `transaction_hash` over one window it carried only **30.2% of prints and ~28%
    /// of volume** (273 of 898 hashes; 627 REST-only, 2 WS-only). It has last-price
    /// semantics: consecutive fills at one price collapse into a single event. Driving
    /// the queue off it would consume ~30% of real volume, so our simulated queue
    /// would advance too slowly and the run would UNDERSTATE fill rate — the core
    /// metric of the whole business case. Use it as a low-latency hint and for the
    /// `transaction_hash` (which gives A8 counterparty logging for free); never as the
    /// fill driver.
    ///
    /// METHODOLOGY WARNING for anyone re-measuring this: the data-api indexer lags by
    /// MINUTES. Querying it immediately returned 16 rows and implied 93.8% WS coverage
    /// — the exact opposite conclusion. Wait ~300s and pass `takerOnly=false`.
    ///
    /// Consequence for the caller: prints arrive on a LAG. A paper fill needs no
    /// real-time determination, so the driver must merge WS book events and REST
    /// prints into ONE timestamp-ordered stream and feed the engine only events older
    /// than the reconciliation horizon. Feeding live level updates against
    /// not-yet-fetched prints would mis-attribute every traded decrease as a cancel.
    Trade { ts_ms: i64, token: String, price: f64, size: f64 },
}

/// Why a post attempt did not produce a resting order. Every one of these is logged
/// — a paper run that silently declines to quote measures nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PostReject {
    /// Order A2: posting would breach `max_net_inventory_shares`. We do NOT hedge and
    /// do NOT cross the spread to flatten — we simply stop quoting.
    InventoryCap,
    /// Outside the validated 0.10–0.90 band (0.70–0.90 was toxic in-sample).
    PriceBand,
    /// No ask side to join.
    NoBook,
    /// Already resting at this token+level; we do not stack.
    AlreadyResting,
}

/// A resting virtual ask.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RestingOrder {
    pub order_id: u64,
    pub token: String,
    /// Our ask level.
    pub price: f64,
    pub size_total: f64,
    pub size_remaining: f64,
    /// Shares we believe sit AHEAD of us at this level. Only ever decreases via
    /// prints (rule 3 forbids improving it on cancels).
    pub queue_ahead: f64,
    pub posted_ts_ms: i64,
    /// Snapshot sequence number the queue estimate was taken from — the audit trail
    /// for "which book state did we believe when we posted".
    pub book_seq_at_post: u64,
    /// Diagnostics carried to the fill record so the fill is reconstructible.
    pub queue_consumed_by_prints: f64,
    /// Observed cancel volume at our level. NOT applied to `queue_ahead` (rule 3);
    /// logged so the optimistic variant is scorable offline.
    pub cancels_observed: f64,
    /// Set once the BBO has moved away from our level (rule 4). We keep resting.
    pub stale_priced: bool,
}

/// One virtual fill.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Fill {
    pub order_id: u64,
    pub token: String,
    pub ts_ms: i64,
    /// The level we were resting at (our sale price).
    pub price: f64,
    pub size: f64,
    /// The print that caused it, for reconstruction.
    pub print_price: f64,
    pub print_size: f64,
    pub time_to_fill_ms: i64,
    /// Fraction of the ORIGINAL order filled by this fill.
    pub fraction_of_order: f64,
    pub queue_consumed_by_prints: f64,
    pub cancels_observed: f64,
    pub inventory_after: f64,
}

/// A cancel observation at one of our levels (rule 3). Logged, never acted on.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CancelObservation {
    pub ts_ms: i64,
    pub token: String,
    pub price: f64,
    /// Displayed size that vanished without a print to explain it.
    pub shares: f64,
    pub queue_ahead_unchanged: f64,
}

/// Frozen strategy parameters (order A1 — do NOT re-optimise on the paper run;
/// selecting new values here would be selecting on the validation set).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MakerConfig {
    /// Validated band. 0.70–0.90 was toxic in-sample; band chosen IS, held OOS.
    pub price_min: f64,
    pub price_max: f64,
    /// Nominal size S. The validated unit; per-share economics are logged so it rescales.
    pub size_shares: f64,
    /// Order A2. The measured tail is why this exists: worst single 5-minute window
    /// −$1,956 at S=50 full coverage, p5 −$924, ~50% of windows negative.
    pub max_net_inventory_shares: f64,
}

impl Default for MakerConfig {
    fn default() -> Self {
        Self {
            price_min: 0.10,
            price_max: 0.90,
            size_shares: 50.0,
            max_net_inventory_shares: 150.0,
        }
    }
}

/// Deterministic virtual-fill engine. Pure: no clock, no I/O, no randomness — every
/// output is a function of the event sequence, which is what makes the replay test
/// meaningful and the log re-scorable.
#[derive(Debug)]
pub struct FillEngine {
    cfg: MakerConfig,
    resting: Vec<RestingOrder>,
    /// Displayed ask size per (token, price) from the last snapshot, and the print
    /// volume seen at that level since — together these separate cancels from trades.
    level_state: std::collections::HashMap<(String, u64), LevelState>,
    /// Net SHORT shares from fills. Selling an ask means we sold a YES share, so this
    /// only grows in one direction for an ask-only strategy.
    inventory_shares: f64,
    /// Inventory attributed to each token, so settlement can RELEASE it.
    ///
    /// Without this the ask-only inventory is monotonic (it is only ever added to on
    /// fill) and `exposure()` ratchets until the cap binds permanently: at S=50
    /// against a 150-share cap the engine stops quoting after three fills and every
    /// later post is rejected InventoryCap. Fine for a single-window test, fatal for
    /// a multi-day run over markets that settle every five minutes.
    inv_by_token: std::collections::HashMap<String, f64>,
    next_order_id: u64,
    seq: u64,
}

#[derive(Debug, Default, Clone, Copy)]
struct LevelState {
    displayed: f64,
    prints_since_snapshot: f64,
}

/// Price → integer key, so f64 can index a map without NaN/epsilon surprises.
/// Polymarket ticks are 1c; 1e6 scaling is far finer than any real level.
fn pk(price: f64) -> u64 {
    (price * 1_000_000.0).round() as u64
}

impl FillEngine {
    #[must_use]
    pub fn new(cfg: MakerConfig) -> Self {
        Self {
            cfg,
            resting: Vec::new(),
            level_state: std::collections::HashMap::new(),
            inventory_shares: 0.0,
            inv_by_token: std::collections::HashMap::new(),
            next_order_id: 1,
            seq: 0,
        }
    }

    #[must_use]
    pub fn inventory(&self) -> f64 {
        self.inventory_shares
    }

    #[must_use]
    pub fn resting_orders(&self) -> &[RestingOrder] {
        &self.resting
    }

    #[must_use]
    pub fn seq(&self) -> u64 {
        self.seq
    }

    /// Settle one token: its market resolved, so the short is closed and the capacity
    /// it consumed is released. Returns `(shares_settled, orders_dropped)`.
    ///
    /// Call this once per token after its market resolves. Resting orders on a
    /// resolved market can never fill again, so leaving them would hold capacity
    /// against the cap forever.
    pub fn settle_token(&mut self, token: &str) -> (f64, usize) {
        let shares = self.inv_by_token.remove(token).unwrap_or(0.0);
        self.inventory_shares -= shares;
        if self.inventory_shares.abs() < 1e-9 {
            self.inventory_shares = 0.0;   // keep float drift out of the cap check
        }
        let before = self.resting.len();
        self.resting.retain(|o| o.token != token);
        self.level_state.retain(|(t, _), _| t != token);
        (shares, before - self.resting.len())
    }

    /// Inventory currently attributed to one token.
    #[must_use]
    pub fn inventory_of(&self, token: &str) -> f64 {
        self.inv_by_token.get(token).copied().unwrap_or(0.0)
    }

    /// Total shares that could still become inventory: what we already hold plus
    /// everything still resting. The cap is enforced against THIS, not against
    /// inventory alone — otherwise fills on already-resting orders could carry us
    /// past the cap after the fact, and the order requires the cap is never exceeded.
    #[must_use]
    pub fn exposure(&self) -> f64 {
        self.inventory_shares + self.resting.iter().map(|o| o.size_remaining).sum::<f64>()
    }

    /// Displayed ask size at a level from the most recent snapshot (the queue estimate
    /// input). `None` when we have never seen the level.
    #[must_use]
    pub fn displayed_at(&self, token: &str, price: f64) -> Option<f64> {
        self.level_state.get(&(token.to_string(), pk(price))).map(|l| l.displayed)
    }

    /// Attempt to join the BBO ask at `price`, back of queue.
    ///
    /// Returns the resting order (a copy, for logging) or the reason we declined.
    /// `queue_ahead` is the FULL displayed size at the level: we assume every share
    /// there is ahead of us, because we cannot prove otherwise.
    pub fn try_post(
        &mut self,
        ts_ms: i64,
        token: &str,
        price: f64,
        size: f64,
    ) -> Result<RestingOrder, PostReject> {
        if price < self.cfg.price_min || price > self.cfg.price_max {
            return Err(PostReject::PriceBand);
        }
        if self.resting.iter().any(|o| o.token == token && pk(o.price) == pk(price)) {
            return Err(PostReject::AlreadyResting);
        }
        let Some(displayed) = self.displayed_at(token, price) else {
            return Err(PostReject::NoBook);
        };
        // Order A2: stop posting rather than breach. No hedge, no crossing.
        if self.exposure() + size > self.cfg.max_net_inventory_shares {
            return Err(PostReject::InventoryCap);
        }
        let order = RestingOrder {
            order_id: self.next_order_id,
            token: token.to_string(),
            price,
            size_total: size,
            size_remaining: size,
            queue_ahead: displayed,
            posted_ts_ms: ts_ms,
            book_seq_at_post: self.seq,
            queue_consumed_by_prints: 0.0,
            cancels_observed: 0.0,
            stale_priced: false,
        };
        self.next_order_id += 1;
        self.resting.push(order.clone());
        Ok(order)
    }

    /// Apply one market event. Returns any fills it produced plus any cancel
    /// observations at our resting levels.
    ///
    /// The caller MUST feed events in timestamp order across both sources (WS book +
    /// REST prints) — see [`MarketEvent::Trade`].
    pub fn apply(&mut self, ev: &MarketEvent) -> (Vec<Fill>, Vec<CancelObservation>) {
        match ev {
            MarketEvent::Snapshot { ts_ms, token, asks, .. } => {
                (Vec::new(), self.on_snapshot(*ts_ms, token, asks))
            }
            MarketEvent::LevelUpdate { ts_ms, token, price, size, side } => {
                // Ask-only strategy: a bid-side update can never affect our queue.
                if *side == Side::Bid {
                    return (Vec::new(), Vec::new());
                }
                self.seq += 1;
                let c = self.observe_level(*ts_ms, token, *price, *size);
                (Vec::new(), c.into_iter().collect())
            }
            MarketEvent::Trade { ts_ms, token, price, size } => {
                (self.on_trade(*ts_ms, token, *price, *size), Vec::new())
            }
        }
    }

    /// Reconcile ONE level against its new displayed size, deriving a cancel if the
    /// decrease is not explained by prints. Shared by the snapshot and delta paths so
    /// both channels apply rule 3 identically.
    fn observe_level(
        &mut self,
        ts_ms: i64,
        token: &str,
        price: f64,
        new_displayed: f64,
    ) -> Option<CancelObservation> {
        let key = (token.to_string(), pk(price));
        let prev = self.level_state.get(&key).copied().unwrap_or_default();
        // Record the new depth and clear the print accumulator we just consumed.
        self.level_state
            .insert(key, LevelState { displayed: new_displayed, prints_since_snapshot: 0.0 });

        let ours = self
            .resting
            .iter()
            .find(|o| o.token == token && pk(o.price) == pk(price))
            .map(|o| o.queue_ahead)?;
        // What the level SHOULD read if only prints had consumed it.
        let expected = (prev.displayed - prev.prints_since_snapshot).max(0.0);
        let vanished = expected - new_displayed;
        if vanished <= 1e-9 {
            return None;
        }
        // Rule 3: unexplained by prints ⇒ a cancel. We cannot see whether it sat
        // ahead of or behind us; assume BEHIND (queue_ahead untouched).
        if let Some(o) =
            self.resting.iter_mut().find(|o| o.token == token && pk(o.price) == pk(price))
        {
            o.cancels_observed += vanished;
        }
        Some(CancelObservation {
            ts_ms,
            token: token.to_string(),
            price,
            shares: vanished,
            queue_ahead_unchanged: ours,
        })
    }

    /// Refresh level state from a snapshot and derive cancels at our levels.
    fn on_snapshot(&mut self, ts_ms: i64, token: &str, asks: &[Level]) -> Vec<CancelObservation> {
        self.seq += 1;
        let mut cancels = Vec::new();
        // Levels we hold an order at — the only ones where cancels matter to us.
        // A level absent from the snapshot reads as depth 0.
        let ours: Vec<f64> =
            self.resting.iter().filter(|o| o.token == token).map(|o| o.price).collect();
        for price in ours {
            let new_displayed =
                asks.iter().find(|l| pk(l.price) == pk(price)).map(|l| l.size).unwrap_or(0.0);
            if let Some(c) = self.observe_level(ts_ms, token, price, new_displayed) {
                cancels.push(c);
            }
        }
        // Rewrite level state for every OTHER ask level in the snapshot, resetting the
        // per-level print accumulator. (Our own levels were just handled above.)
        let ours_keys: Vec<u64> =
            self.resting.iter().filter(|o| o.token == token).map(|o| pk(o.price)).collect();
        self.level_state.retain(|(t, k), _| t != token || ours_keys.contains(k));
        for l in asks {
            if ours_keys.contains(&pk(l.price)) {
                continue;
            }
            self.level_state.insert(
                (token.to_string(), pk(l.price)),
                LevelState { displayed: l.size, prints_since_snapshot: 0.0 },
            );
        }
        // Rule 4: BBO moved away ⇒ mark stale, keep resting (naive config, no cancel).
        let best_ask = asks.first().map(|l| l.price);
        for o in self.resting.iter_mut().filter(|o| o.token == token) {
            o.stale_priced = match best_ask {
                Some(b) => pk(b) != pk(o.price),
                None => true,
            };
        }
        cancels
    }

    /// Consume queue with a trade print.
    ///
    /// NOTE ON DIRECTION: a resting ASK at level L is lifted by a BUY that reaches L,
    /// and such a sweep prints at L or above (it consumes cheaper asks first). So the
    /// consuming condition is `print_price >= L`. The order text says "at a price ≤
    /// our ask level"; that reads inverted for an ask book and would make our order
    /// fill on prints strictly below it, which cannot touch a higher ask. Implemented
    /// as `>=`, and every raw print is logged, so the opposite convention remains
    /// scorable offline if the auditor intended something else.
    fn on_trade(&mut self, ts_ms: i64, token: &str, price: f64, size: f64) -> Vec<Fill> {
        let mut fills = Vec::new();
        let mut remaining_print = size;
        // Best-priced levels first: a sweep consumes the cheapest asks before ours.
        let mut idx: Vec<usize> = (0..self.resting.len())
            .filter(|&i| self.resting[i].token == token && price + 1e-9 >= self.resting[i].price)
            .collect();
        idx.sort_by(|&a, &b| {
            self.resting[a].price.partial_cmp(&self.resting[b].price).unwrap_or(std::cmp::Ordering::Equal)
        });
        for i in idx {
            if remaining_print <= 1e-9 {
                break;
            }
            // Queue ahead absorbs the print first.
            let to_queue = remaining_print.min(self.resting[i].queue_ahead);
            self.resting[i].queue_ahead -= to_queue;
            self.resting[i].queue_consumed_by_prints += to_queue;
            remaining_print -= to_queue;
            if remaining_print <= 1e-9 {
                break;
            }
            // Whatever is left hits US — partial fills are normal.
            let filled = remaining_print.min(self.resting[i].size_remaining);
            if filled > 1e-9 {
                self.resting[i].size_remaining -= filled;
                remaining_print -= filled;
                self.inventory_shares += filled;
                *self.inv_by_token.entry(token.to_string()).or_insert(0.0) += filled;
                let o = &self.resting[i];
                fills.push(Fill {
                    order_id: o.order_id,
                    token: o.token.clone(),
                    ts_ms,
                    price: o.price,
                    size: filled,
                    print_price: price,
                    print_size: size,
                    time_to_fill_ms: ts_ms - o.posted_ts_ms,
                    fraction_of_order: filled / o.size_total.max(1e-9),
                    queue_consumed_by_prints: o.queue_consumed_by_prints,
                    cancels_observed: o.cancels_observed,
                    inventory_after: self.inventory_shares,
                });
            }
        }
        // Track print volume per level so the NEXT snapshot can tell cancels from trades.
        for (key, st) in self.level_state.iter_mut() {
            if key.0 == token && key.1 <= pk(price) {
                st.prints_since_snapshot += size;
            }
        }
        // Fully filled orders stop resting.
        self.resting.retain(|o| o.size_remaining > 1e-9);
        fills
    }
}

/// P&L convention (order A4). A filled ask means we SOLD a YES share at `price`, so
/// we are SHORT that outcome: if it resolves YES we pay $1, otherwise we keep the
/// premium. Primary P&L is mark-to-settlement — these are binaries that settle, and
/// settlement is fee-free.
#[must_use]
pub fn settle_pnl_per_share(sold_at: f64, resolved_yes: bool) -> f64 {
    sold_at - if resolved_yes { 1.0 } else { 0.0 }
}

/// Modeled maker rebate: 20% of taker fees paid on filled maker volume, fee-curve
/// weighted, ≈0.35¢/share at p=0.5. Kept as a SEPARATE field and never folded into
/// gross — it is a third of a 1-tick capture, a kicker not a business, and it is only
/// verifiable from live receipts.
#[must_use]
pub fn modeled_rebate_per_share(price: f64, taker_fee_rate: f64, rebate_share: f64) -> f64 {
    let p = price.clamp(0.0, 1.0);
    rebate_share * taker_fee_rate * p.min(1.0 - p)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snap(ts: i64, token: &str, asks: &[(f64, f64)]) -> MarketEvent {
        MarketEvent::Snapshot {
            ts_ms: ts,
            token: token.to_string(),
            asks: asks.iter().map(|&(price, size)| Level { price, size }).collect(),
            bids: vec![Level { price: 0.49, size: 100.0 }],
        }
    }
    fn trade(ts: i64, token: &str, price: f64, size: f64) -> MarketEvent {
        MarketEvent::Trade { ts_ms: ts, token: token.to_string(), price, size }
    }

    /// Rule 1+2: we join the BACK, so the displayed size is queue ahead of us, and a
    /// print consumes that queue BEFORE it touches us.
    #[test]
    fn prints_consume_queue_ahead_before_us() {
        let mut e = FillEngine::new(MakerConfig::default());
        e.apply(&snap(1_000, "T", &[(0.50, 200.0)]));
        let o = e.try_post(1_000, "T", 0.50, 50.0).expect("post");
        assert_eq!(o.queue_ahead, 200.0, "join the back: all displayed size is ahead");

        // A 150-share print is fully absorbed by the queue — no fill.
        let (fills, _) = e.apply(&trade(1_100, "T", 0.50, 150.0));
        assert!(fills.is_empty(), "print smaller than the queue must not fill us");
        assert_eq!(e.resting_orders()[0].queue_ahead, 50.0);
        assert_eq!(e.inventory(), 0.0);

        // Next print of 60: 50 finishes the queue, the remaining 10 is OUR fill.
        let (fills, _) = e.apply(&trade(1_200, "T", 0.50, 60.0));
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].size, 10.0, "only the post-queue remainder fills us");
        assert_eq!(fills[0].time_to_fill_ms, 200);
        assert_eq!(e.inventory(), 10.0);
        assert_eq!(e.resting_orders()[0].size_remaining, 40.0, "partial fill leaves the rest resting");
    }

    /// Rule 5: partial fills accumulate until the order is exhausted, then it stops
    /// resting. Never all-or-nothing.
    #[test]
    fn partial_fills_accumulate_then_order_retires() {
        let mut e = FillEngine::new(MakerConfig::default());
        e.apply(&snap(0, "T", &[(0.50, 0.0)])); // empty level: we are first in queue
        e.try_post(0, "T", 0.50, 50.0).expect("post");
        let (f1, _) = e.apply(&trade(10, "T", 0.50, 20.0));
        let (f2, _) = e.apply(&trade(20, "T", 0.50, 20.0));
        let (f3, _) = e.apply(&trade(30, "T", 0.50, 20.0));
        assert_eq!((f1[0].size, f2[0].size, f3[0].size), (20.0, 20.0, 10.0), "last print over-fills, clipped");
        assert_eq!(e.inventory(), 50.0);
        assert!(e.resting_orders().is_empty(), "fully filled order stops resting");
        assert!((f3[0].fraction_of_order - 0.2).abs() < 1e-12);
    }

    /// Rule 3, THE pessimistic assumption: displayed size that vanishes without a
    /// print is a cancel, and we must NOT credit it against our queue — we cannot see
    /// whether it sat ahead of or behind us. The amount is recorded so the optimistic
    /// variant is scorable offline.
    #[test]
    fn cancels_are_recorded_but_never_improve_our_queue() {
        let mut e = FillEngine::new(MakerConfig::default());
        e.apply(&snap(0, "T", &[(0.50, 200.0)]));
        e.try_post(0, "T", 0.50, 50.0).expect("post");

        // Level drops 200 → 60 with NO prints: 140 shares cancelled.
        let (_, cancels) = e.apply(&snap(100, "T", &[(0.50, 60.0)]));
        assert_eq!(cancels.len(), 1);
        assert_eq!(cancels[0].shares, 140.0);
        assert_eq!(cancels[0].queue_ahead_unchanged, 200.0);
        assert_eq!(
            e.resting_orders()[0].queue_ahead, 200.0,
            "PESSIMISTIC: cancels must not reduce queue_ahead"
        );
        assert_eq!(e.resting_orders()[0].cancels_observed, 140.0, "…but must be logged for re-scoring");
    }

    /// A decrease fully explained by prints is NOT a cancel.
    #[test]
    fn prints_do_not_masquerade_as_cancels() {
        let mut e = FillEngine::new(MakerConfig::default());
        e.apply(&snap(0, "T", &[(0.50, 200.0)]));
        e.try_post(0, "T", 0.50, 50.0).expect("post");
        e.apply(&trade(50, "T", 0.50, 80.0)); // consumes queue 200 → 120
        let (_, cancels) = e.apply(&snap(100, "T", &[(0.50, 120.0)]));
        assert!(cancels.is_empty(), "a print-explained decrease is not a cancel");
        assert_eq!(e.resting_orders()[0].queue_ahead, 120.0, "prints DO reduce the queue");
    }

    /// Order A2 — the cap binds at POST time and is enforced against total exposure
    /// (inventory + everything still resting), so fills can never carry us past it.
    /// We stop quoting; we never hedge and never cross to flatten.
    #[test]
    fn inventory_cap_binds_and_is_never_exceeded() {
        let cfg = MakerConfig { max_net_inventory_shares: 150.0, ..MakerConfig::default() };
        let mut e = FillEngine::new(cfg);
        e.apply(&snap(0, "A", &[(0.50, 0.0)]));
        e.apply(&snap(0, "B", &[(0.50, 0.0)]));
        e.apply(&snap(0, "C", &[(0.50, 0.0)]));
        e.apply(&snap(0, "D", &[(0.50, 0.0)]));
        assert!(e.try_post(0, "A", 0.50, 50.0).is_ok());
        assert!(e.try_post(0, "B", 0.50, 50.0).is_ok());
        assert!(e.try_post(0, "C", 0.50, 50.0).is_ok()); // exposure now exactly 150
        assert_eq!(
            e.try_post(0, "D", 0.50, 50.0),
            Err(PostReject::InventoryCap),
            "a 4th post would breach the cap — stop quoting"
        );
        assert_eq!(e.exposure(), 150.0);
    }

    /// The cap must hold across a TRENDING window — the −$1,956 scenario, where
    /// capacity refreshes on every requote and inventory runs away. Repeated
    /// fill→requote cycles must never push inventory past the cap.
    #[test]
    fn inventory_never_exceeds_cap_across_a_trending_window() {
        let cfg = MakerConfig { max_net_inventory_shares: 150.0, ..MakerConfig::default() };
        let mut e = FillEngine::new(cfg);
        let mut ts = 0i64;
        // 60 requote-clips of a one-way market, exactly the runaway shape.
        for round in 0..60 {
            let token = format!("T{}", round % 4);
            ts += 100;
            e.apply(&snap(ts, &token, &[(0.50, 0.0)]));
            // Requote whatever the cap still allows.
            let _ = e.try_post(ts, &token, 0.50, 50.0);
            ts += 100;
            // The whole level trades through.
            e.apply(&trade(ts, &token, 0.50, 500.0));
            assert!(
                e.inventory() <= 150.0 + 1e-9,
                "cap breached at round {round}: inventory {}",
                e.inventory()
            );
            assert!(e.exposure() <= 150.0 + 1e-9, "exposure breached at round {round}");
        }
        assert_eq!(e.inventory(), 150.0, "a trending window pins us AT the cap, never past it");
    }

    /// Rule 4: when the BBO moves away we do NOT cancel (naive config — lag defense
    /// inverted the study result). The order keeps resting and is flagged stale.
    #[test]
    fn bbo_move_marks_stale_but_leaves_us_resting() {
        let mut e = FillEngine::new(MakerConfig::default());
        e.apply(&snap(0, "T", &[(0.50, 100.0)]));
        e.try_post(0, "T", 0.50, 50.0).expect("post");
        assert!(!e.resting_orders()[0].stale_priced);
        // Best ask moves to 0.52; our 0.50 is no longer BBO.
        e.apply(&snap(100, "T", &[(0.52, 80.0), (0.53, 90.0)]));
        assert_eq!(e.resting_orders().len(), 1, "naive config never cancels");
        assert!(e.resting_orders()[0].stale_priced, "…but records that it is stale-priced");
    }

    /// The validated band is enforced (0.70–0.90 was toxic in-sample; band held OOS).
    #[test]
    fn price_band_is_enforced() {
        let mut e = FillEngine::new(MakerConfig::default());
        e.apply(&snap(0, "T", &[(0.05, 100.0), (0.50, 100.0), (0.95, 100.0)]));
        assert_eq!(e.try_post(0, "T", 0.05, 50.0), Err(PostReject::PriceBand));
        assert_eq!(e.try_post(0, "T", 0.95, 50.0), Err(PostReject::PriceBand));
        assert!(e.try_post(0, "T", 0.50, 50.0).is_ok());
        // Boundaries are inclusive.
        let mut e2 = FillEngine::new(MakerConfig::default());
        e2.apply(&snap(0, "T", &[(0.10, 10.0), (0.90, 10.0)]));
        assert!(e2.try_post(0, "T", 0.10, 5.0).is_ok());
        assert!(e2.try_post(0, "T", 0.90, 5.0).is_ok());
    }

    /// A print strictly BELOW our ask cannot touch it (it consumes cheaper asks).
    /// This pins the direction convention documented on `on_trade`.
    #[test]
    fn prints_below_our_level_never_fill_us() {
        let mut e = FillEngine::new(MakerConfig::default());
        e.apply(&snap(0, "T", &[(0.50, 0.0), (0.60, 0.0)]));
        e.try_post(0, "T", 0.60, 50.0).expect("post");
        let (fills, _) = e.apply(&trade(10, "T", 0.55, 500.0));
        assert!(fills.is_empty(), "a 0.55 print cannot lift a 0.60 ask");
        let (fills, _) = e.apply(&trade(20, "T", 0.60, 30.0));
        assert_eq!(fills.len(), 1, "a print AT our level does");
        assert_eq!(fills[0].size, 30.0);
    }

    /// ORDER #15 A0 — THE REPLAY GUARANTEE. The same recorded event sequence must
    /// produce byte-identical fills on a fresh engine, with no clock, no I/O and no
    /// hidden state. This is what makes the log re-scorable offline under a different
    /// queue assumption, and it is the property whose absence made the previous
    /// study's caches unrecoverable.
    #[test]
    fn replay_from_events_alone_is_deterministic() {
        // A recorded sequence, exactly as it would be reconstructed from the log.
        let events = vec![
            snap(1_000, "T", &[(0.50, 120.0), (0.51, 300.0)]),
            trade(1_050, "T", 0.50, 40.0),
            snap(1_100, "T", &[(0.50, 55.0), (0.51, 300.0)]), // 80-40 = 25 cancelled
            trade(1_150, "T", 0.50, 90.0),
            snap(1_200, "T", &[(0.51, 300.0)]),
            trade(1_250, "T", 0.51, 500.0),
        ];
        let run = || {
            let mut e = FillEngine::new(MakerConfig::default());
            // Post after the first snapshot, exactly as the live loop would.
            let mut fills = Vec::new();
            let mut cancels = Vec::new();
            for (i, ev) in events.iter().enumerate() {
                let (f, c) = e.apply(ev);
                fills.extend(f);
                cancels.extend(c);
                if i == 0 {
                    e.try_post(1_000, "T", 0.50, 50.0).expect("post");
                }
            }
            (fills, cancels, e.inventory())
        };
        let a = run();
        let b = run();
        assert_eq!(a, b, "identical inputs must yield identical fills — replay is the audit");
        assert!(!a.0.is_empty(), "the fixture must actually produce fills");
        // And the pessimistic cancel is present in the reconstruction.
        assert!(a.1.iter().any(|c| c.shares > 0.0), "the cancel must be recorded for re-scoring");
    }

    fn level(ts: i64, token: &str, price: f64, size: f64) -> MarketEvent {
        MarketEvent::LevelUpdate { ts_ms: ts, token: token.to_string(), price, size, side: Side::Ask }
    }

    /// THE TRAP: `price_change` carries price/size/side/hash and reads exactly like a
    /// print, but `size` is the level's NEW RESTING DEPTH. If it were treated as
    /// traded volume the engine would manufacture fills out of quote churn. A level
    /// update must NEVER produce a fill — only a REST print can.
    #[test]
    fn level_updates_never_produce_fills() {
        let mut e = FillEngine::new(MakerConfig::default());
        e.apply(&snap(0, "T", &[(0.50, 0.0)])); // empty level: we would be first in queue
        e.try_post(0, "T", 0.50, 50.0).expect("post");

        // A torrent of level churn — the shape that is 51.4% increases in the wild.
        for (i, size) in [80.0, 120.0, 60.0, 200.0, 30.0, 300.0].iter().enumerate() {
            let (fills, _) = e.apply(&level(100 + i as i64, "T", 0.50, *size));
            assert!(fills.is_empty(), "a price_change must never fill us (size is depth, not volume)");
        }
        assert_eq!(e.inventory(), 0.0, "quote churn cannot create inventory");
        assert_eq!(e.resting_orders()[0].size_remaining, 50.0);

        // Only a REST print fills.
        let (fills, _) = e.apply(&trade(200, "T", 0.50, 25.0));
        assert_eq!(fills.len(), 1, "the REST print feed is the only source of executions");
        assert_eq!(fills[0].size, 25.0);
    }

    /// A level GROWING is normal (new depth joining behind us) and must not be read as
    /// a cancel, nor improve our queue.
    #[test]
    fn level_growth_is_not_a_cancel_and_does_not_help_us() {
        let mut e = FillEngine::new(MakerConfig::default());
        e.apply(&snap(0, "T", &[(0.50, 100.0)]));
        e.try_post(0, "T", 0.50, 50.0).expect("post");
        let (_, cancels) = e.apply(&level(10, "T", 0.50, 260.0));
        assert!(cancels.is_empty(), "depth joining behind us is not a cancel");
        assert_eq!(e.resting_orders()[0].queue_ahead, 100.0, "…and never changes our queue");
    }

    /// A level shrinking with no print behind it is still a pessimistic cancel when it
    /// arrives as a delta, exactly as it is via a snapshot.
    #[test]
    fn level_shrink_without_a_print_is_a_pessimistic_cancel() {
        let mut e = FillEngine::new(MakerConfig::default());
        e.apply(&snap(0, "T", &[(0.50, 200.0)]));
        e.try_post(0, "T", 0.50, 50.0).expect("post");
        let (_, cancels) = e.apply(&level(10, "T", 0.50, 150.0));
        assert_eq!(cancels.len(), 1);
        assert_eq!(cancels[0].shares, 50.0);
        assert_eq!(e.resting_orders()[0].queue_ahead, 200.0, "PESSIMISTIC: unchanged");
        // But a shrink EXPLAINED by a print is not a cancel.
        e.apply(&trade(20, "T", 0.50, 30.0));
        let (_, cancels) = e.apply(&level(30, "T", 0.50, 120.0));
        assert!(cancels.is_empty(), "print-explained shrink is not a cancel");
    }

    /// Bid-side updates are ignored outright — this is an asks-only strategy (asks
    /// +0.57¢/sh vs bids +0.06, and 87% of taker prints are BUYS).
    #[test]
    fn bid_side_updates_are_ignored() {
        let mut e = FillEngine::new(MakerConfig::default());
        e.apply(&snap(0, "T", &[(0.50, 200.0)]));
        e.try_post(0, "T", 0.50, 50.0).expect("post");
        let bid = MarketEvent::LevelUpdate {
            ts_ms: 10,
            token: "T".into(),
            price: 0.50,
            size: 5.0,
            side: Side::Bid,
        };
        let (fills, cancels) = e.apply(&bid);
        assert!(fills.is_empty() && cancels.is_empty(), "the bid book is not ours to model");
        assert_eq!(e.resting_orders()[0].queue_ahead, 200.0);
    }

    /// P&L convention: a filled ask is a SHORT. Keep the premium on NO, pay $1 on YES.
    #[test]
    fn settlement_pnl_convention() {
        // Sold at 0.60, resolves NO → we keep 0.60.
        assert!((settle_pnl_per_share(0.60, false) - 0.60).abs() < 1e-12);
        // Sold at 0.60, resolves YES → we pay 1, net −0.40.
        assert!((settle_pnl_per_share(0.60, true) + 0.40).abs() < 1e-12);
    }

    /// Rebate is calibrated to ≈0.35¢/share at p=0.5 and is symmetric about 0.5
    /// (fee-curve weighted on min(p, 1−p)). Logged separately, never in gross.
    #[test]
    fn rebate_matches_the_calibration_point() {
        let r = modeled_rebate_per_share(0.50, 0.035, 0.20);
        assert!((r - 0.0035).abs() < 1e-9, "≈0.35c/share at p=0.5, got {r}");
        // Symmetric, and smaller in the tails.
        let a = modeled_rebate_per_share(0.20, 0.035, 0.20);
        let b = modeled_rebate_per_share(0.80, 0.035, 0.20);
        assert!((a - b).abs() < 1e-12, "fee curve is symmetric about 0.5");
        assert!(a < r, "tails earn less rebate than the middle");
    }

    /// Ask-only inventory is monotonic: `apply` only ever ADDS on fill. Over a
    /// multi-day run against markets that settle every five minutes, that ratchets
    /// `exposure()` until the cap binds permanently -- at S=50 against a 150-share cap
    /// the engine stops quoting after three fills and every later post is rejected
    /// InventoryCap. A paper run in that state measures nothing at all.
    ///
    /// Mutation check: delete the `self.inventory_shares -= shares` line in
    /// `settle_token` and the post after settlement fails with InventoryCap.
    #[test]
    fn settlement_releases_capacity_so_the_engine_keeps_quoting() {
        let cfg = MakerConfig {
            price_min: 0.10, price_max: 0.90, size_shares: 50.0,
            max_net_inventory_shares: 150.0,
        };
        let mut e = FillEngine::new(cfg);

        // Fill three markets to the cap, one after another.
        for i in 0..3 {
            let tok = format!("M{i}");
            e.apply(&MarketEvent::Snapshot {
                ts_ms: 1000 * i, token: tok.clone(),
                asks: vec![Level { price: 0.50, size: 0.0 }],
                bids: vec![Level { price: 0.49, size: 10.0 }],
            });
            e.try_post(1000 * i, &tok, 0.50, 50.0).expect("should post below the cap");
            e.apply(&MarketEvent::Trade {
                ts_ms: 1000 * i + 1, token: tok, price: 0.50, size: 50.0,
            });
        }
        assert!((e.inventory() - 150.0).abs() < 1e-9, "inventory {}", e.inventory());

        // At the cap, a fresh market is refused -- correct, and the reason A2 exists.
        e.apply(&MarketEvent::Snapshot {
            ts_ms: 9_000, token: "M9".into(),
            asks: vec![Level { price: 0.50, size: 0.0 }],
            bids: vec![Level { price: 0.49, size: 10.0 }],
        });
        assert_eq!(e.try_post(9_000, "M9", 0.50, 50.0), Err(PostReject::InventoryCap));

        // Settle the finished markets: capacity must come back.
        for i in 0..3 {
            let (shares, _) = e.settle_token(&format!("M{i}"));
            assert!((shares - 50.0).abs() < 1e-9, "released {shares}");
        }
        assert_eq!(e.inventory(), 0.0, "settlement must clear inventory");
        e.try_post(9_100, "M9", 0.50, 50.0)
            .expect("after settlement the engine must quote again");
    }

    /// Settling must also drop that token's resting orders: a resolved market can
    /// never fill again, so leaving them holds capacity against the cap forever.
    #[test]
    fn settlement_drops_resting_orders_on_the_resolved_market() {
        let mut e = FillEngine::new(MakerConfig::default());
        e.apply(&MarketEvent::Snapshot {
            ts_ms: 1, token: "A".into(),
            asks: vec![Level { price: 0.40, size: 5.0 }],
            bids: vec![Level { price: 0.39, size: 5.0 }],
        });
        e.try_post(1, "A", 0.40, 50.0).unwrap();
        assert_eq!(e.resting_orders().len(), 1);
        let (_, dropped) = e.settle_token("A");
        assert_eq!(dropped, 1, "resting order on a resolved market must be dropped");
        assert!(e.resting_orders().is_empty());
        assert_eq!(e.exposure(), 0.0);
    }
}
