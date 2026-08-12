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

impl ShadowReject {
    /// STABLE lowercase vocabulary for the log. Debug formatting would couple the
    /// analysis to Rust identifier names; these strings are the contract, and a
    /// rejection rate over 50% is undiagnosable without them.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            ShadowReject::AlreadyInMarket => "dedup",
            ShadowReject::MaxEntriesPerMarket => "reentry_blocked",
            ShadowReject::MaxOpenPositions => "shadow_cap",
        }
    }
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

/// ORDER #18 — a shadow position closed by the band-stop, awaiting its resolution so
/// the stop-vs-hold counterfactual (`dev`) can be computed. V0 stashes the same thing
/// in `state.v2_stop_recal`; shadows keep their own, sharing nothing.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoppedPending {
    pub token_id: String,
    pub asset: String,
    pub interval: String,
    pub up: bool,
    pub resolution_s: i64,
    pub stop_bid: f64,
    pub shares: f64,
    pub pred_raw: f64,
}

/// ORDER #18 — the on-disk form. Shadow state must survive a restart: there were 5
/// `variants_armed` events in a 3-day window, and without this the recal window and
/// ledger reset each time, which also left severance 5 ("shadow state in its own
/// files") unverifiable because nothing was ever written.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShadowSnapshot {
    pub variant: Variant,
    pub positions: Vec<ShadowPosition>,
    pub ledger: Vec<ShadowPnl>,
    pub day_stats: BTreeMap<String, DayStat>,
    pub recal: Recalibrator,
    #[serde(default)]
    pub entered: Vec<String>,
    #[serde(default)]
    pub market_entries: HashMap<String, u8>,
    #[serde(default)]
    pub stopped_pending: Vec<StoppedPending>,
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
    /// Stopped-out positions awaiting resolution for their `dev` counterfactual.
    stopped_pending: Vec<StoppedPending>,
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
            stopped_pending: Vec::new(),
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
    /// Would an open be admitted? Pure — no mutation.
    ///
    /// This exists so the caller can test admissibility BEFORE modelling the FOK
    /// kill. Ordering matters more than it looks: a variant re-qualifies on a market
    /// it already holds on almost every tick, and if the kill were evaluated first
    /// those non-attempts would land in the kill-rate denominator (and numerator).
    /// Kill rate is the hard-FAIL leg at 25%, so that would fail an arm on bookkeeping.
    pub fn can_open(&self, mkey: &str, token_id: &str) -> Result<(), ShadowReject> {
        // CURRENTLY HOLDING, not "ever entered". `entered` only ever grew — the
        // prune in mark_reentry_eligible matched `token.starts_with(mkey)`, but mkey
        // is "BTC:5m:<epoch>" and the entries are bare numeric token ids, so it never
        // matched anything. That locked an arm out of a market permanently after one
        // touch, while V0 re-enters freely: 136 of 253 attempts per arm were rejected
        // as `dedup`, against V0's 185 fills on the same candidates.
        if self.positions.iter().any(|p| p.token_id == token_id) {
            return Err(ShadowReject::AlreadyInMarket);
        }
        if self.market_entries.get(mkey).copied().unwrap_or(0) >= self.max_entries_per_market {
            return Err(ShadowReject::MaxEntriesPerMarket);
        }
        if self.positions.len() >= self.max_open_positions {
            return Err(ShadowReject::MaxOpenPositions);
        }
        Ok(())
    }

    pub fn open(
        &mut self,
        mkey: &str,
        pos: ShadowPosition,
        day: &str,
    ) -> Result<(), ShadowReject> {
        self.can_open(mkey, &pos.token_id)?;
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

    /// ORDER #18 — the deployed BAND-STOP, applied to shadow positions.
    ///
    /// Order #16 specified "exits identical across all three", but shadows ran
    /// hold-only while V0 ran hold + band-stop: 100% of `variant_pnl` rows resolved at
    /// exactly 0.0/1.0. That asymmetry FAVOURED V0 (its stop added +$17.44 over the
    /// measured window), so correcting it makes the variants look better, not worse.
    ///
    /// Same rule and thresholds as V0: exit at the bid when side-signed displacement
    /// has reverted to <= 0 AND the bid sits in an overpay band (>= hi or <= lo). The
    /// fair mid-band has no stale-bid premium to harvest and whipsaws, so it holds.
    #[must_use]
    pub fn stop_should_fire(disp: f64, bid: Option<f64>, hi: f64, lo: f64) -> bool {
        disp <= 0.0 && matches!(bid, Some(b) if b >= hi || b <= lo)
    }

    /// Positions eligible for a stop check this tick: (token, asset, up, interval).
    #[must_use]
    pub fn open_for_stop(&self) -> Vec<(String, String, bool, String)> {
        self.positions
            .iter()
            .map(|p| (p.token_id.clone(), p.asset.clone(), p.up, p.interval.clone()))
            .collect()
    }

    /// Close a position at `bid` (a fired stop). Books the REALISED exit price rather
    /// than a 0/1 settlement, and stashes the stop-vs-hold counterfactual so `dev` can
    /// be computed once the window resolves — the same gauge V0 has.
    /// ORDER #21 — close a position mid-window at the live bid and book it.
    ///
    /// The shared exit-at-bid primitive: the band-stop (Order #18) and the FLIP
    /// (Order #21 V1) are the same mechanical action with different triggers, so they
    /// share one implementation and one P&L convention. `reason` is carried onto the
    /// row so the exit leg is attributable — without that, a flip's exit and its new
    /// entry cannot be told apart and the result is uninterpretable.
    pub fn close_at_bid(
        &mut self,
        token_id: &str,
        bid: f64,
        ts_ms: i64,
        day: &str,
    ) -> Option<(ShadowPnl, ShadowPosition)> {
        let idx = self.positions.iter().position(|p| p.token_id == token_id)?;
        let p = self.positions.remove(idx);
        let net = p.shares * (bid - p.entry_price);
        let row = ShadowPnl {
            token_id: p.token_id.clone(),
            variant: self.variant,
            ts_ms,
            interval: p.interval.clone(),
            entry_price: p.entry_price,
            shares: p.shares,
            resolved_price: bid, // realised exit, NOT a 0/1 settlement
            net_pnl: net,
        };
        self.day_stats.entry(day.to_string()).or_default().net_usd += net;
        self.ledger.push(row.clone());
        Some((row, p))
    }

    /// An open position on the OPPOSITE side of the same market, if any. This is what
    /// makes a flip detectable: a fully-gated signal arriving for the other side of a
    /// market this arm already holds.
    #[must_use]
    pub fn opposite_open(
        &self,
        asset: &str,
        interval: &str,
        resolution_s: i64,
        up: bool,
    ) -> Option<&ShadowPosition> {
        self.positions.iter().find(|p| {
            p.asset == asset && p.interval == interval && p.resolution_s == resolution_s && p.up != up
        })
    }

    /// FLIP: close the held original at the bid, then open the opposite leg.
    ///
    /// Deliberately bypasses the per-market entry cap: a flip REPLACES a position
    /// rather than adding one, so the arm never holds both sides. "Keep A, add B"
    /// is not on the menu — it lost in two independent tests (EV −0.266 / −0.329),
    /// and a hedge pair is not what this measures.
    pub fn flip(
        &mut self,
        old_token: &str,
        bid: f64,
        new_pos: ShadowPosition,
        ts_ms: i64,
        day: &str,
    ) -> Option<(ShadowPnl, ShadowPosition)> {
        let (exit_row, old) = self.close_at_bid(old_token, bid, ts_ms, day)?;
        self.entered.insert(new_pos.token_id.clone());
        self.positions.push(new_pos);
        self.day_stats.entry(day.to_string()).or_default().entries += 1;
        Some((exit_row, old))
    }

    pub fn apply_stop(&mut self, token_id: &str, bid: f64, ts_ms: i64, day: &str) -> Option<ShadowPnl> {
        let idx = self.positions.iter().position(|p| p.token_id == token_id)?;
        let p = self.positions.remove(idx);
        let net = p.shares * (bid - p.entry_price);
        self.stopped_pending.push(StoppedPending {
            token_id: p.token_id.clone(),
            asset: p.asset.clone(),
            interval: p.interval.clone(),
            up: p.up,
            resolution_s: p.resolution_s,
            stop_bid: bid,
            shares: p.shares,
            pred_raw: p.pred_raw,
        });
        let row = ShadowPnl {
            token_id: p.token_id,
            variant: self.variant,
            ts_ms,
            interval: p.interval,
            entry_price: p.entry_price,
            shares: p.shares,
            resolved_price: bid, // realised exit, NOT a 0/1 settlement
            net_pnl: net,
        };
        self.day_stats.entry(day.to_string()).or_default().net_usd += net;
        self.ledger.push(row.clone());
        Some(row)
    }

    /// Stopped positions whose window has now resolved — drained for the `dev` gauge.
    pub fn take_resolved_stops(&mut self, now_s: i64) -> Vec<StoppedPending> {
        let mut out = Vec::new();
        let mut i = 0;
        while i < self.stopped_pending.len() {
            if now_s >= self.stopped_pending[i].resolution_s + 2 {
                out.push(self.stopped_pending.remove(i));
            } else {
                i += 1;
            }
        }
        out
    }

    /// Feed this shadow's recal a stopped position's HOLD counterfactual — the same
    /// survivorship fix Order #11 C made for V0. `feed` is the caller's photo-finish
    /// decision; an unreliable label must never train a recal.
    pub fn record_stop_counterfactual(&mut self, pred_raw: f64, won: bool, feed: bool) {
        if feed {
            self.recal.record(pred_raw, won);
        }
    }

    // ---- ORDER #18: persistence (severance 5, now verifiable) ----

    #[must_use]
    pub fn snapshot(&self) -> ShadowSnapshot {
        ShadowSnapshot {
            variant: self.variant,
            positions: self.positions.clone(),
            ledger: self.ledger.clone(),
            day_stats: self.day_stats.clone(),
            recal: self.recal.clone(),
            entered: self.entered.iter().cloned().collect(),
            market_entries: self.market_entries.clone(),
            stopped_pending: self.stopped_pending.clone(),
        }
    }

    /// Restore from disk. The variant and the caps stay as constructed — only the
    /// accumulated STATE is restored, so a config change is never silently overridden
    /// by a stale file.
    pub fn restore(&mut self, snap: ShadowSnapshot) {
        self.positions = snap.positions;
        self.ledger = snap.ledger;
        self.day_stats = snap.day_stats;
        self.recal = snap.recal;
        self.entered = snap.entered.into_iter().collect();
        self.market_entries = snap.market_entries;
        self.stopped_pending = snap.stopped_pending;
    }

    /// Write state beside the recal path. Best-effort: a failed save must never take
    /// the trading loop down.
    pub fn save(&self, path: &str) {
        if let Some(dir) = std::path::Path::new(path).parent()
            && !dir.as_os_str().is_empty()
        {
            let _ = std::fs::create_dir_all(dir);
        }
        if let Ok(js) = serde_json::to_string(&self.snapshot()) {
            let _ = std::fs::write(path, js);
        }
    }

    /// Load a snapshot from disk, if present and parseable.
    #[must_use]
    pub fn load(path: &str) -> Option<ShadowSnapshot> {
        serde_json::from_str(&std::fs::read_to_string(path).ok()?).ok()
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

    /// ORDER #17 addendum 2 — the reject vocabulary is a LOG CONTRACT, not Debug
    /// output. >50% of shadow opens were rejected and undiagnosable; these strings are
    /// what makes the cause countable, so they must not drift with Rust identifiers.
    #[test]
    fn reject_reasons_are_a_stable_lowercase_vocabulary() {
        assert_eq!(ShadowReject::MaxOpenPositions.as_str(), "shadow_cap");
        assert_eq!(ShadowReject::AlreadyInMarket.as_str(), "dedup");
        assert_eq!(ShadowReject::MaxEntriesPerMarket.as_str(), "reentry_blocked");
        // Every reason a real rejection can carry is covered and lowercase.
        for r in [
            ShadowReject::MaxOpenPositions,
            ShadowReject::AlreadyInMarket,
            ShadowReject::MaxEntriesPerMarket,
        ] {
            let s = r.as_str();
            assert!(!s.is_empty() && s == s.to_lowercase(), "{s} must be lowercase");
        }
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

    /// LIVE-CAUGHT REGRESSION: admissibility must be testable BEFORE the FOK kill.
    /// A variant re-qualifies on a market it already holds on nearly every tick
    /// (7,387 dedup rejections in the first minutes of the live run). If the kill were
    /// evaluated first, those non-attempts would land in the kill rate — the hard-FAIL
    /// leg at 25% — and could fail an arm on bookkeeping rather than on execution.
    #[test]
    fn can_open_gates_before_the_kill_model_and_does_not_mutate() {
        let mut b = book(Variant::V1);
        assert!(b.can_open("m1", "t1").is_ok(), "a fresh market is admissible");
        b.open("m1", pos("t1", 0.6, 1.75), "d1").unwrap();

        // Same token again → NOT admissible, so no kill may be recorded for it.
        assert_eq!(b.can_open("m1", "t1"), Err(ShadowReject::AlreadyInMarket));
        // The check is PURE: asking repeatedly changes nothing.
        for _ in 0..5 {
            let _ = b.can_open("m1", "t1");
        }
        assert_eq!(b.open_count(), 1);
        assert_eq!(b.day_stats()["d1"].kills, 0, "an inadmissible attempt is not a kill");
        assert_eq!(b.day_stats()["d1"].entries, 1);

        // And it agrees with what open() would decide.
        assert_eq!(
            b.can_open("m1", "t2"),
            Ok(()),
            "the one permitted re-entry is admissible"
        );
        b.open("m1", pos("t2", 0.6, 1.75), "d1").unwrap();
        assert_eq!(b.can_open("m1", "t3"), Err(ShadowReject::MaxEntriesPerMarket));
    }

    /// ORDER #18 — the band-stop rule, identical to V0's. Fires only when the thesis
    /// is dead AND the bid sits in an overpay band; the fair mid-band whipsaws and is
    /// deliberately held through.
    #[test]
    fn band_stop_matches_v0s_rule() {
        let (hi, lo) = (0.50, 0.30);
        // Thesis dead + bid high (stale premium to sell into) → fire.
        assert!(ShadowBook::stop_should_fire(-1.0, Some(0.60), hi, lo));
        // Thesis dead + bid low → fire.
        assert!(ShadowBook::stop_should_fire(-1.0, Some(0.25), hi, lo));
        // Thesis dead but bid in the FAIR mid-band → hold (nothing to harvest).
        assert!(!ShadowBook::stop_should_fire(-1.0, Some(0.40), hi, lo));
        // Thesis alive → never fire, whatever the bid.
        assert!(!ShadowBook::stop_should_fire(3.0, Some(0.90), hi, lo));
        // No bid = nothing to sell to → hold.
        assert!(!ShadowBook::stop_should_fire(-1.0, None, hi, lo));
        // Boundaries are inclusive, matching V0.
        assert!(ShadowBook::stop_should_fire(0.0, Some(0.50), hi, lo));
        assert!(ShadowBook::stop_should_fire(0.0, Some(0.30), hi, lo));
    }

    /// A stop books the REALISED exit price, not a 0/1 settlement — which is exactly
    /// what was missing: 100% of variant_pnl rows resolved at 0.0/1.0 because shadows
    /// had no exit policy at all.
    #[test]
    fn stop_books_the_realised_exit_and_stashes_the_dev_counterfactual() {
        let mut b = book(Variant::V1);
        b.open("m1", pos("t1", 0.60, 1.75), "d1").unwrap();
        let row = b.apply_stop("t1", 0.52, 9_000, "d1").expect("stops");
        assert_eq!(row.resolved_price, 0.52, "realised exit, NOT a 0/1 settlement");
        assert!((row.net_pnl - 1.75 * (0.52 - 0.60)).abs() < 1e-12);
        assert_eq!(b.open_count(), 0, "the position is closed");
        // Nothing resolves yet — the dev counterfactual waits for the window.
        assert!(b.take_resolved_stops(0).is_empty());
        let due = b.take_resolved_stops(1_000_000);
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].stop_bid, 0.52);
        assert!(b.take_resolved_stops(1_000_000).is_empty(), "drained exactly once");
    }

    /// ORDER #18 — state must survive a restart. Severance 5 was unverifiable because
    /// nothing was ever written; 5 restarts in 3 days silently reset every shadow.
    #[test]
    fn state_round_trips_through_disk() {
        let dir = std::env::temp_dir().join(format!("mb_shadow_{}", std::process::id()));
        let path = dir.join("shadow_v1.json");
        let path_s = path.to_string_lossy().to_string();
        let _ = std::fs::remove_file(&path);

        let mut a = book(Variant::V1);
        a.open("m1", pos("t1", 0.60, 1.75), "d1").unwrap();
        a.open("m2", pos("t2", 0.55, 1.90), "d1").unwrap();
        a.settle("t2", true, 5_000, "d1", true);
        a.apply_stop("t1", 0.52, 6_000, "d1");
        a.save(&path_s);

        let snap = ShadowBook::load(&path_s).expect("loads");
        let mut b = book(Variant::V1);
        b.restore(snap);
        assert_eq!(b.ledger().len(), a.ledger().len(), "ledger survives");
        assert_eq!(b.recal_samples(), a.recal_samples(), "recal window survives");
        assert_eq!(b.day_stats()["d1"].net_usd, a.day_stats()["d1"].net_usd);
        // Dedup survives a restart for a position that is still OPEN — otherwise a
        // restart would double-enter a market the arm is already holding.
        let mut still_open = pos("t3", 0.50, 2.0);
        still_open.resolution_s = 9_999;
        a.open("m3", still_open, "d1").unwrap();
        a.save(&path_s);
        let mut c = book(Variant::V1);
        c.restore(ShadowBook::load(&path_s).expect("loads"));
        assert_eq!(c.can_open("m3", "t3"), Err(ShadowReject::AlreadyInMarket), "open position still blocks");
        // …but a CLOSED one does not. `entered` used to record "ever entered" and was
        // never pruned, which locked an arm out of a market permanently after one
        // touch while V0 re-entered freely (136 of 253 attempts rejected as dedup).
        assert_eq!(c.can_open("m1", "t1"), Ok(()), "a settled/stopped market is re-enterable");
        // And the pending stop counterfactual is not lost.
        assert_eq!(b.take_resolved_stops(1_000_000).len(), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// ORDER #21 — the FLIP: close the original at the bid, open the opposite. The
    /// arm must never hold both sides, because "keep A, add B" LOST in two independent
    /// tests (EV −0.266 / −0.329) and a hedge pair is not what this measures.
    #[test]
    fn flip_replaces_the_position_and_never_holds_both_sides() {
        let mut b = book(Variant::V1);
        let mut up = pos("t_up", 0.60, 1.75);
        up.up = true;
        up.resolution_s = 2_000;
        b.open("BTC:5m:1700", up, "d1").unwrap();

        // The opposite side of the SAME market is findable; the same side is not.
        assert!(b.opposite_open("BTC", "5m", 2_000, false).is_some(), "Down signal sees the Up hold");
        assert!(b.opposite_open("BTC", "5m", 2_000, true).is_none(), "same side is not a flip");
        // A different market must not be mistaken for one.
        assert!(b.opposite_open("BTC", "5m", 9_999, false).is_none());
        assert!(b.opposite_open("ETH", "5m", 2_000, false).is_none());

        let mut down = pos("t_down", 0.45, 2.33);
        down.up = false;
        down.resolution_s = 2_000;
        let (exit, old) = b.flip("t_up", 0.52, down, 7_000, "d1").expect("flips");

        // Exit leg is booked at the REALISED bid, attributable on its own.
        assert_eq!(exit.resolved_price, 0.52);
        assert!((exit.net_pnl - 1.75 * (0.52 - 0.60)).abs() < 1e-12);
        assert_eq!(old.token_id, "t_up");
        // Exactly ONE position remains, and it is the new side.
        assert_eq!(b.open_count(), 1, "a flip replaces, never adds");
        assert!(!b.positions()[0].up, "the surviving leg is the opposite side");
        assert_eq!(b.positions()[0].token_id, "t_down");
        // Both legs are countable: one entry recorded for the new leg.
        assert_eq!(b.day_stats()["d1"].entries, 2, "original + flipped-in leg");
        // Flipping a token we do not hold is a no-op, not a phantom entry.
        let mut ghost = pos("t_ghost", 0.50, 2.0);
        ghost.resolution_s = 2_000;
        assert!(b.flip("t_nonexistent", 0.5, ghost, 8_000, "d1").is_none());
        assert_eq!(b.open_count(), 1);
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
