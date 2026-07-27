//! ORDER #16 — `ShadowBook`: a self-contained virtual portfolio for V1/V2.
//!
//! V0 is NOT one of these. V0 is the actual bot, end to end — its positions,
//! settlement path, band-stop, invariants and recal remain the existing audited
//! machinery. A `ShadowBook` is purely ADDITIVE state that the decision loop feeds
//! and that shares nothing mutable with V0.
//!
//! The design rule is that isolation must be *provable*, not careful. So every one of
//! the six leak surfaces in [`crate::variants`] is severed by CONSTRUCTION here —
//! the shadow owns its positions, its settled map, its predictions, its dedup sets,
//! its recal and its ledger — and the one surface that cannot be expressed as
//! ownership (the recal FILE PATH) is enforced as a runtime guard in
//! [`ShadowBook::new`], which refuses to construct against the audition's files.
//!
//! Nothing in this module touches `guards`, `bs.positions`, `state.v2_settled`,
//! `state.canary` or `state.json`. That is not a convention to be respected by future
//! edits — it is why the type exists.

#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::v2::Recalibrator;
use crate::variants::{DayStat, Variant};

/// The audition's recal files. A shadow that wrote either of these would contaminate
/// the 15m verdict (mid-flight at n=140) with a different, larger population.
pub const PROTECTED_RECAL_PATHS: &[&str] = &["recal.json", "recal_15m.json"];

/// A position held by a shadow portfolio. Deliberately NOT `OpenPosition`: shadow
/// positions must never be assignable into `bs.positions`, and using a distinct type
/// makes that a compile error rather than a code-review question.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ShadowPosition {
    pub token_id: String,
    pub asset: String,
    pub interval: String,
    pub up: bool,
    pub entry_price: f64,
    pub shares: f64,
    pub stake_usd: f64,
    pub opened_at_ms: i64,
    pub resolution_s: i64,
    /// Raw pcal at entry, for this shadow's own recal feed.
    pub pred_raw: f64,
}

/// One booked shadow settlement.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ShadowPnl {
    pub token_id: String,
    pub variant: Variant,
    pub ts_ms: i64,
    pub interval: String,
    pub entry_price: f64,
    pub shares: f64,
    pub resolved_price: f64,
    pub net_pnl: f64,
}

/// Why a shadow declined to open. Shadow caps are enforced against SHADOW state only
/// — they never read or decrement the live `guards` budget (leak surface 1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShadowReject {
    AlreadyInMarket,
    MaxEntriesPerMarket,
    MaxOpenPositions,
}

/// Constructing a shadow against a protected path is refused, not warned about.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtectedPathError(pub String);

impl std::fmt::Display for ProtectedPathError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "refusing to build a shadow recal on {} — that is the audition's file",
            self.0
        )
    }
}

/// A complete virtual portfolio: own positions, own settled map, own recal, own
/// ledger, own dedup. Shares nothing mutable with V0.
#[derive(Debug)]
pub struct ShadowBook {
    variant: Variant,
    recal_path: String,
    recal: Recalibrator,
    positions: Vec<ShadowPosition>,
    /// Own settled map — NOT `state.v2_settled`, which the paper settlement sweep
    /// drains (leak surface 3).
    settled: HashMap<String, bool>,
    /// Own per-market entry counts and dedup. One-entry-per-market is PER VARIANT:
    /// variants disagreeing about which second to take is the measurement.
    entered: HashSet<String>,
    market_entries: HashMap<String, u8>,
    /// Own re-entry eligibility, keyed like V0's but never shared.
    reentry: HashMap<String, (i64, bool)>,
    ledger: Vec<ShadowPnl>,
    day_stats: BTreeMap<String, DayStat>,
    max_open_positions: usize,
    max_entries_per_market: u8,
}

impl ShadowBook {
    /// Build a shadow portfolio.
    ///
    /// Refuses any `recal_path` that resolves to one of [`PROTECTED_RECAL_PATHS`].
    /// This is the one leak surface that ownership cannot express — two `Recalibrator`
    /// values are independent in memory but can still collide on disk — so it is a
    /// runtime guard, checked on the FILE NAME so a different directory prefix cannot
    /// smuggle it past.
    pub fn new(
        variant: Variant,
        recal_path: impl Into<String>,
        capacity: usize,
        warmup: usize,
        max_open_positions: usize,
        max_entries_per_market: u8,
    ) -> Result<Self, ProtectedPathError> {
        let recal_path = recal_path.into();
        let file = recal_path
            .rsplit(['/', '\\'])
            .next()
            .unwrap_or(recal_path.as_str());
        if PROTECTED_RECAL_PATHS.contains(&file) {
            return Err(ProtectedPathError(recal_path));
        }
        Ok(Self {
            variant,
            recal_path,
            recal: Recalibrator::new(capacity, warmup),
            positions: Vec::new(),
            settled: HashMap::new(),
            entered: HashSet::new(),
            market_entries: HashMap::new(),
            reentry: HashMap::new(),
            ledger: Vec::new(),
            day_stats: BTreeMap::new(),
            max_open_positions,
            max_entries_per_market,
        })
    }

    #[must_use]
    pub fn variant(&self) -> Variant {
        self.variant
    }
    #[must_use]
    pub fn recal_path(&self) -> &str {
        &self.recal_path
    }
    #[must_use]
    pub fn recal_bias(&self) -> f64 {
        self.recal.bias()
    }
    #[must_use]
    pub fn recal_samples(&self) -> usize {
        self.recal.samples()
    }
    #[must_use]
    pub fn positions(&self) -> &[ShadowPosition] {
        &self.positions
    }
    #[must_use]
    pub fn ledger(&self) -> &[ShadowPnl] {
        &self.ledger
    }
    #[must_use]
    pub fn day_stats(&self) -> &BTreeMap<String, DayStat> {
        &self.day_stats
    }
    #[must_use]
    pub fn open_count(&self) -> usize {
        self.positions.len()
    }

    /// Market key, matching V0's convention so the two are comparable offline.
    #[must_use]
    pub fn market_key(asset: &str, interval: &str, epoch: i64) -> String {
        format!("{asset}:{interval}:{epoch}")
    }

    /// Open a shadow position, or say why not.
    ///
    /// Caps are checked against SHADOW state only — this function never reads the
    /// live `guards`, so a shadow can neither be throttled by V0's budget nor consume
    /// it (leak surface 1). `killed` FOK attempts must NOT call this; record them via
    /// [`Self::record_kill`] so kill rate and entries stay distinguishable.
    pub fn open(
        &mut self,
        mkey: &str,
        pos: ShadowPosition,
        day: &str,
    ) -> Result<(), ShadowReject> {
        if self.entered.contains(&pos.token_id) {
            return Err(ShadowReject::AlreadyInMarket);
        }
        let entries = self.market_entries.get(mkey).copied().unwrap_or(0);
        if entries >= self.max_entries_per_market {
            return Err(ShadowReject::MaxEntriesPerMarket);
        }
        if self.positions.len() >= self.max_open_positions {
            return Err(ShadowReject::MaxOpenPositions);
        }
        self.entered.insert(pos.token_id.clone());
        *self.market_entries.entry(mkey.to_string()).or_insert(0) += 1;
        self.positions.push(pos);
        self.day_stats.entry(day.to_string()).or_default().entries += 1;
        Ok(())
    }

    /// Record an FOK kill: no position, but the attempt counts toward kill rate —
    /// which is a hard-FAIL leg of the pre-registered rule at 25%.
    pub fn record_kill(&mut self, day: &str) {
        self.day_stats.entry(day.to_string()).or_default().kills += 1;
    }

    /// Mark this shadow's own view of a settlement (never `state.v2_settled`).
    pub fn mark_settled(&mut self, token_id: &str, won: bool) {
        self.settled.insert(token_id.to_string(), won);
    }

    #[must_use]
    pub fn settled_outcome(&self, token_id: &str) -> Option<bool> {
        self.settled.get(token_id).copied()
    }

    /// Book a settled shadow position: removes it, appends to this shadow's ledger and
    /// day stats, and feeds THIS shadow's recal. Returns the booked row.
    ///
    /// `feed_recal` is the caller's photo-finish decision — a pf label is unreliable
    /// (~20% flips vs Chainlink) and must not train any recal, shadow or not.
    pub fn settle(
        &mut self,
        token_id: &str,
        won: bool,
        ts_ms: i64,
        day: &str,
        feed_recal: bool,
    ) -> Option<ShadowPnl> {
        let idx = self.positions.iter().position(|p| p.token_id == token_id)?;
        let p = self.positions.remove(idx);
        let resolved = if won { 1.0 } else { 0.0 };
        let net = p.shares * (resolved - p.entry_price);
        if feed_recal {
            self.recal.record(p.pred_raw, won);
        }
        let row = ShadowPnl {
            token_id: p.token_id,
            variant: self.variant,
            ts_ms,
            interval: p.interval,
            entry_price: p.entry_price,
            shares: p.shares,
            resolved_price: resolved,
            net_pnl: net,
        };
        self.day_stats.entry(day.to_string()).or_default().net_usd += net;
        self.ledger.push(row.clone());
        Some(row)
    }

    /// Mark a market re-entry-eligible after a stop. Per variant (leak surface: V0's
    /// `state.v2_reentry` is untouched).
    pub fn mark_reentry_eligible(&mut self, mkey: &str, at_s: i64, was_up: bool) {
        self.reentry.insert(mkey.to_string(), (at_s, was_up));
        // Clearing the token dedup is what lets a fresh same-side signal re-fire,
        // mirroring V0's behaviour inside this shadow's own state.
        self.entered.retain(|t| !t.starts_with(mkey));
    }

    #[must_use]
    pub fn reentry_eligible(&self, mkey: &str) -> Option<(i64, bool)> {
        self.reentry.get(mkey).copied()
    }

    /// Day tallies in registration order, for [`crate::variants::evaluate`].
    #[must_use]
    pub fn days_in_order(&self) -> Vec<DayStat> {
        self.day_stats.values().copied().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn book(v: Variant) -> ShadowBook {
        ShadowBook::new(v, "data/v2/shadow_v1.json", 300, 50, 20, 2).expect("builds")
    }

    fn pos(token: &str, entry: f64, shares: f64) -> ShadowPosition {
        ShadowPosition {
            token_id: token.into(),
            asset: "BTC".into(),
            interval: "5m".into(),
            up: true,
            entry_price: entry,
            shares,
            stake_usd: 1.05,
            opened_at_ms: 1_000,
            resolution_s: 2_000,
            pred_raw: 0.68,
        }
    }

    /// LEAK SURFACE 6, enforced rather than documented: a shadow cannot be built
    /// against the audition's recal files. The 15m verdict is mid-flight at n=140 and
    /// a shadow's larger population would contaminate it.
    #[test]
    fn refuses_to_build_on_the_auditions_recal_files() {
        for p in ["recal.json", "recal_15m.json", "data/v2/recal.json", "data\\v2\\recal_15m.json"] {
            assert!(
                ShadowBook::new(Variant::V1, p, 300, 50, 20, 2).is_err(),
                "must refuse the audition file: {p}"
            );
        }
        // A distinct file is fine.
        let b = ShadowBook::new(Variant::V1, "data/v2/shadow_v1.json", 300, 50, 20, 2).unwrap();
        assert_eq!(b.recal_path(), "data/v2/shadow_v1.json");
        // A lookalike that is NOT the protected name is allowed.
        assert!(ShadowBook::new(Variant::V2, "data/v2/recal_shadow.json", 300, 50, 20, 2).is_ok());
    }

    /// LEAK SURFACE 1 (the one that matters most): shadow caps bind against SHADOW
    /// state only. Two shadows filling up must not affect each other, and neither
    /// reads or decrements the live guard budget — which is why `open` takes no
    /// `guards` argument at all.
    #[test]
    fn shadow_caps_are_per_shadow_and_never_shared() {
        let mut v1 = ShadowBook::new(Variant::V1, "s1.json", 300, 50, 2, 2).unwrap();
        let mut v2 = ShadowBook::new(Variant::V2, "s2.json", 300, 50, 2, 2).unwrap();
        assert!(v1.open("m1", pos("t1", 0.6, 1.75), "d1").is_ok());
        assert!(v1.open("m2", pos("t2", 0.6, 1.75), "d1").is_ok());
        // V1 is now full…
        assert_eq!(v1.open("m3", pos("t3", 0.6, 1.75), "d1"), Err(ShadowReject::MaxOpenPositions));
        // …and V2 is entirely unaffected.
        assert!(v2.open("m3", pos("t3", 0.6, 1.75), "d1").is_ok());
        assert!(v2.open("m4", pos("t4", 0.6, 1.75), "d1").is_ok());
        assert_eq!(v1.open_count(), 2);
        assert_eq!(v2.open_count(), 2);
    }

    /// One entry per market is PER VARIANT, and the max-2 (original + one re-entry)
    /// cap is enforced inside the shadow.
    #[test]
    fn per_market_dedup_and_reentry_cap_are_shadow_local() {
        let mut b = book(Variant::V1);
        assert!(b.open("BTC:5m:100", pos("t1", 0.6, 1.75), "d1").is_ok());
        // Same token again → rejected.
        assert_eq!(
            b.open("BTC:5m:100", pos("t1", 0.6, 1.75), "d1"),
            Err(ShadowReject::AlreadyInMarket)
        );
        // A second token in the same market is the one permitted re-entry.
        assert!(b.open("BTC:5m:100", pos("t2", 0.6, 1.75), "d1").is_ok());
        // A third is capped.
        assert_eq!(
            b.open("BTC:5m:100", pos("t3", 0.6, 1.75), "d1"),
            Err(ShadowReject::MaxEntriesPerMarket)
        );
    }

    /// LEAK SURFACE 3: the shadow's settled map is its own. Settling here books into
    /// the shadow's ledger and feeds the SHADOW's recal — V0's recal never sees it.
    #[test]
    fn settlement_books_to_the_shadow_ledger_and_its_own_recal() {
        let mut b = book(Variant::V1);
        b.open("m1", pos("t1", 0.60, 1.75), "d1").unwrap();
        b.mark_settled("t1", true);
        assert_eq!(b.settled_outcome("t1"), Some(true));
        assert_eq!(b.recal_samples(), 0);

        let row = b.settle("t1", true, 5_000, "d1", true).expect("books");
        assert_eq!(row.variant, Variant::V1, "every P&L row carries its variant");
        // Won: shares * (1 - entry) = 1.75 * 0.40.
        assert!((row.net_pnl - 0.70).abs() < 1e-12);
        assert_eq!(b.open_count(), 0, "settled position leaves the book");
        assert_eq!(b.recal_samples(), 1, "the SHADOW's recal is fed");
        assert_eq!(b.ledger().len(), 1);
        assert!((b.day_stats()["d1"].net_usd - 0.70).abs() < 1e-12);
    }

    /// A photo-finish label must not train ANY recal — the Order #11 C rule applies to
    /// shadows too, or V1/V2's biases become unusable for the same reason V0's were.
    #[test]
    fn photo_finish_labels_do_not_train_the_shadow_recal() {
        let mut b = book(Variant::V1);
        b.open("m1", pos("t1", 0.60, 1.75), "d1").unwrap();
        let row = b.settle("t1", true, 5_000, "d1", /*feed_recal=*/ false).expect("still books");
        assert!(row.net_pnl > 0.0, "P&L is still booked…");
        assert_eq!(b.recal_samples(), 0, "…but the unreliable label never trains the recal");
    }

    /// A loss books negative, so day stats can go either way — the pre-registered rule
    /// counts positive DAYS and would be meaningless if losses were dropped.
    #[test]
    fn losses_book_negative_into_day_stats() {
        let mut b = book(Variant::V1);
        b.open("m1", pos("t1", 0.60, 1.75), "d1").unwrap();
        let row = b.settle("t1", false, 5_000, "d1", true).unwrap();
        assert!((row.net_pnl + 1.05).abs() < 1e-12, "lost the full stake: 1.75 * -0.60");
        assert!(b.day_stats()["d1"].net_usd < 0.0);
    }

    /// Kills are counted but create NO position — they must stay distinguishable from
    /// entries, because kill rate is a hard-FAIL leg of the pre-registered rule.
    #[test]
    fn kills_count_without_creating_a_position() {
        let mut b = book(Variant::V1);
        b.record_kill("d1");
        b.record_kill("d1");
        b.open("m1", pos("t1", 0.6, 1.75), "d1").unwrap();
        assert_eq!(b.open_count(), 1, "a kill is not a position");
        let d = b.day_stats()["d1"];
        assert_eq!((d.entries, d.kills), (1, 2));
        // 2 kills / 3 attempts — the shape `evaluate` consumes.
        assert!((d.kills as f64 / (d.entries + d.kills) as f64 - 2.0 / 3.0).abs() < 1e-12);
    }

    /// Day stats feed the pre-registered rule directly, in registration order.
    #[test]
    fn day_stats_feed_the_prereg_rule() {
        let mut b = book(Variant::V1);
        for (i, day) in ["d1", "d2", "d3"].iter().enumerate() {
            b.open("m", pos(&format!("t{i}"), 0.60, 1.75), day).unwrap();
            b.settle(&format!("t{i}"), true, 1_000, day, true);
            b.mark_reentry_eligible("m", 1, true); // frees the market for the next day
            b.market_entries.clear();
        }
        let days = b.days_in_order();
        assert_eq!(days.len(), 3);
        assert!(days.iter().all(|d| d.net_usd > 0.0 && d.entries == 1));
    }
}
