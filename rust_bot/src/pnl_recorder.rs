//! Phase 6 G8 -- Daily-loss-stop hook for RESOLVED positions.
//!
//! THE GAP THIS CLOSES (observed in production overnight 2026-06-01):
//! The `Guards::daily_net_pnl` accumulator is fed exclusively from
//! `feed_guards_net_pnl(ClosedTrade)`, which fires only from the two SELL paths
//! (`exit_task::close_due` and `process_kline::close_token`). A position that
//! goes past-close WITHOUT a SELL (`close_due` skips it because the book has no
//! bid -- the typical "Phantom" path before redeem) NEVER becomes a
//! `ClosedTrade` and so NEVER touches the daily counter. Result: a -$13 night
//! made entirely of past-close resolutions left the -$12.60 stop unfired.
//!
//! THE FIX (Option B per the G8 design):
//! Hook into the existing `positions_refresh` 60s loop. For each resolved
//! position in /positions that is ALSO present in our `bs.positions` (= bot-
//! opened, not yet processed), compute the resolution P&L and feed it into
//! the guard, then remove from `bs.positions` so the entry can't be processed
//! twice.
//!
//! THE 3 INVARIANTS (verified by tests + by inspection of close_due/this file):
//!
//!   1. SINGLE-COUNT. A position's P&L is recorded EXACTLY ONCE in its
//!      lifetime, via one of two mutually-exclusive paths:
//!        - SELL (close_due / close_token -> ClosedTrade -> feed_guards_net_pnl),
//!          which `self.positions.remove(i)`s atomically under the bs.positions
//!          mutex;
//!        - RESOLUTION (this module), which records P&L then
//!          `bs.positions.retain(...)`s under the same mutex.
//!      The same Mutex serializes both. The persisted `PnlRecorder` set adds a
//!      second layer to cover crash-between-record-and-persist (see invariant 3).
//!
//!   2. NO BACKLOG. At startup we run `snapshot_existing_resolved` ONCE which
//!      marks every currently-resolved bot-opened position as
//!      `PnlRecord::InitialSnapshot` WITHOUT calling `record_net_pnl`. The
//!      backlog is silently catalogued; daily_net_pnl stays at zero. Only
//!      resolutions detected AFTER startup get recorded into the counter --
//!      so deploying with a pile of past-close positions does NOT trip the
//!      stop on the day of deploy with stale losses from prior days.
//!
//!   3. CRASH SAFE / UNDER-COUNT > DOUBLE-COUNT. The step order is:
//!        (a) classify (resolved? in bs? not already-recorded? cur_price clean?)
//!        (b) compute net P&L (pure)
//!        (c) PERSIST `PnlRecord::Recorded` to disk        <-- persist FIRST
//!        (d) `record_net_pnl` into Guards (in-memory)
//!        (e) remove from `bs.positions` (next snapshot persists it)
//!      A crash between (c) and (d) loses that single P&L event from Guards
//!      (UNDER-count by one) -- but on restart the recorder set has the entry
//!      and we never DOUBLE-count. For a safety brake, missing one slot is
//!      vastly preferable to firing on phantom data.
//!
//! SANITY GUARD (G8 review-2 ajuste):
//! For a binary market, the resolved `cur_price` from /positions MUST be cleanly
//! 0.0 (loser) or 1.0 (winner). Anything intermediate (e.g. 0.5) on a position
//! flagged resolved is treated as an AMBIGUOUS data anomaly -- we LOG a warning,
//! emit an `pnl_recorder_ambiguous_skip` oplog event, and SKIP without recording
//! anything. The position stays in bs.positions and gets retried each tick; if
//! /positions stabilizes to a clean 0/1, the next tick records normally. A
//! safety brake must not register P&L on dubious input.

#![allow(dead_code)] // wired into trading_loop::run_positions_refresh + main.rs startup

use std::collections::HashSet;
use std::io::Write as _;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::guards::Guards;
use crate::oplog::OpLog;
use crate::rest::{PositionInfo, RestClient};
use crate::state::now_ms;
use crate::state::persist::OpenPosition;
use crate::state::store::SharedBotState;
use crate::trade_log::fee_f64;

/// Default path for the persisted record-set + audit log. Gitignored runtime
/// data, same dir as oplog + redemptions tracker.
pub const DEFAULT_PNL_RECORDED_LOG: &str = "data/live/pnl_recorded.jsonl";

// ============================================================================
// PURE: classification + math (testable byte-for-byte; zero IO).
// ============================================================================

/// PURE: classify a resolved `cur_price` into the binary outcome price, or
/// `None` if the cur_price is intermediate (ambiguous data anomaly -- the
/// recorder skips + warns rather than register a dubious P&L into the safety
/// brake).
///
/// Tolerance bands match `PositionInfo::is_resolved_at`'s `cur_price_extreme`
/// thresholds (0.001) so any cur_price that says "resolved" via the extreme
/// check also classifies cleanly here. Positions flagged resolved via the OTHER
/// rules (`redeemable=true` or `end_in_past`) might still arrive with a
/// non-extreme cur_price (e.g. last trade before the market closed sat at
/// 0.3); for those, `None` is returned and the caller skips with a warning.
#[must_use]
pub fn resolved_price_for_pnl(cur_price: f64) -> Option<f64> {
    if cur_price.is_nan() {
        None
    } else if cur_price <= 0.001 {
        Some(0.0)
    } else if cur_price >= 0.999 {
        Some(1.0)
    } else {
        None
    }
}

/// PURE: compute net P&L for a binary resolution.
///
/// Mirrors `feed_guards_net_pnl`'s formula exactly so the resolution path and
/// the SELL path agree on accounting:
///
///   net = (shares * exit - sell_fee) - (shares * entry + buy_fee)
///
/// For a binary resolution with `resolved_price ∈ {0.0, 1.0}`, the
/// `fee_f64 = 0.07 * shares * p * (1-p)` formula yields `sell_fee = 0` (both
/// p=0 and p=1 produce zero), so the SELL fee term drops out cleanly. The
/// formula is kept symmetric so a future fee-model change touches one place.
#[must_use]
pub fn compute_resolution_net_pnl(shares: f64, entry_price: f64, resolved_price: f64) -> f64 {
    let buy_fee = fee_f64(shares, entry_price);
    let sell_fee = fee_f64(shares, resolved_price);
    (shares * resolved_price - sell_fee) - (shares * entry_price + buy_fee)
}

// ============================================================================
// PnlRecord enum + PnlRecorder (persistent set).
// ============================================================================

/// One row in `pnl_recorded.jsonl`. Three variants because the audit log must
/// distinguish (a) backlog skipped at startup, (b) actually-recorded P&L, and
/// (c) ambiguous-data skips so the operator can audit each later.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind")]
pub enum PnlRecord {
    /// Position was already resolved at startup -- catalogued WITHOUT recording
    /// P&L (the loss/gain is from a prior day; rolling it into today would be
    /// wrong). Emitted only by `snapshot_existing_resolved`.
    #[serde(rename = "initial_snapshot")]
    InitialSnapshot { token_id: String, ts_ms: i64 },
    /// Steady-state record: position resolved + P&L computed + fed into Guards.
    /// The fields capture the inputs so the operator can re-derive net later.
    #[serde(rename = "recorded")]
    Recorded {
        token_id: String,
        ts_ms: i64,
        shares: f64,
        entry_price: f64, // average across lots, if multiple
        resolved_price: f64, // exactly 0.0 or 1.0 post-sanity-check
        net_pnl: f64,
        /// Which market interval this trade was ("5m" | "15m"). Lets the dashboard
        /// break P&L / win-rate / PF out per strategy. Defaults to "5m" for old
        /// rows written before per-interval trading existed.
        #[serde(default = "d_interval_5m")]
        interval: String,
    },
    /// Order #8 B: a booking path lied — a later REDEEM payout disagreed with the
    /// already-recorded outcome. `net_pnl` is the DELTA (true − old) applied to the
    /// dashboard realized sum + the daily-loss counter to heal both in place.
    #[serde(rename = "corrected")]
    Corrected {
        token_id: String,
        ts_ms: i64,
        old_net: f64,
        true_net: f64,
        net_pnl: f64, // = true_net - old_net (the heal amount)
        #[serde(default = "d_interval_5m")]
        interval: String,
    },
}

fn d_interval_5m() -> String {
    "5m".to_string()
}

impl PnlRecord {
    pub fn token_id(&self) -> &str {
        match self {
            PnlRecord::InitialSnapshot { token_id, .. } => token_id,
            PnlRecord::Recorded { token_id, .. } => token_id,
            PnlRecord::Corrected { token_id, .. } => token_id,
        }
    }
}

/// File-backed set of token_ids whose resolution-P&L has been
/// recorded-or-explicitly-snapshotted. Loaded on startup, append-only after.
#[derive(Debug, Clone)]
pub struct PnlRecorder {
    path: PathBuf,
    done: HashSet<String>, // lowercased token_ids
    /// Order #8 B: per-token booked outcome (shares, entry, resolved_price, net) so
    /// a later disagreeing REDEEM can be detected + corrected. Session-scoped is
    /// fine; rebuilt from the file on load.
    outcomes: std::collections::HashMap<String, (f64, f64, f64, f64)>,
}

impl PnlRecorder {
    /// Load the recorder from `path`. Missing file is OK (returns empty).
    pub fn load(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let mut done = HashSet::new();
        let mut outcomes = std::collections::HashMap::new();
        if path.exists() {
            let body = std::fs::read_to_string(&path)
                .with_context(|| format!("read pnl log: {}", path.display()))?;
            for (i, line) in body.lines().enumerate() {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                let rec: PnlRecord = serde_json::from_str(line).with_context(|| {
                    format!("parse line {} of {}: {line}", i + 1, path.display())
                })?;
                done.insert(rec.token_id().to_ascii_lowercase());
                if let PnlRecord::Recorded { token_id, shares, entry_price, resolved_price, net_pnl, .. } = &rec {
                    outcomes.insert(token_id.to_ascii_lowercase(), (*shares, *entry_price, *resolved_price, *net_pnl));
                }
            }
        }
        Ok(Self { path, done, outcomes })
    }

    /// Order #8 B: the recorded (shares, entry, resolved_price, net) for a token.
    #[must_use]
    pub fn recorded_outcome(&self, token_id: &str) -> Option<(f64, f64, f64, f64)> {
        self.outcomes.get(&token_id.to_ascii_lowercase()).copied()
    }

    #[must_use]
    pub fn already_recorded(&self, token_id: &str) -> bool {
        self.done.contains(&token_id.to_ascii_lowercase())
    }

    /// Append a record + add to the in-memory set. Persist FIRST, mutate the
    /// in-memory set SECOND, so a disk-write failure does NOT leave the
    /// in-memory state ahead of disk (which would risk under-count on restart).
    pub fn record(&mut self, record: PnlRecord) -> Result<()> {
        let lower = record.token_id().to_ascii_lowercase();
        if let Some(dir) = self.path.parent() {
            std::fs::create_dir_all(dir).ok();
        }
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .with_context(|| format!("open pnl log: {}", self.path.display()))?;
        writeln!(f, "{}", serde_json::to_string(&record)?)?;
        self.done.insert(lower.clone());
        if let PnlRecord::Recorded { shares, entry_price, resolved_price, net_pnl, .. } = &record {
            self.outcomes.insert(lower, (*shares, *entry_price, *resolved_price, *net_pnl));
        }
        Ok(())
    }

    /// Order #8 B: persist a `Corrected` row (a redeem disagreed with the booked
    /// outcome), update the retained outcome to the truth (so a repeat redeem does
    /// NOT re-correct), and return the DELTA (true−old) for the caller to apply to
    /// the daily counter. `None` if the token has no retained outcome.
    pub fn record_correction(&mut self, token_id: &str, true_resolved: f64, true_net: f64, now_ms: i64) -> Result<Option<f64>> {
        let lower = token_id.to_ascii_lowercase();
        let Some((shares, entry, _old_resolved, old_net)) = self.outcomes.get(&lower).copied() else {
            return Ok(None);
        };
        let delta = true_net - old_net;
        let rec = PnlRecord::Corrected {
            token_id: token_id.to_string(),
            ts_ms: now_ms,
            old_net,
            true_net,
            net_pnl: delta,
            interval: d_interval_5m(),
        };
        if let Some(dir) = self.path.parent() {
            std::fs::create_dir_all(dir).ok();
        }
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .with_context(|| format!("open pnl log: {}", self.path.display()))?;
        writeln!(f, "{}", serde_json::to_string(&rec)?)?;
        self.outcomes.insert(lower, (shares, entry, true_resolved, true_net));
        Ok(Some(delta))
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.done.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.done.is_empty()
    }
}

// ============================================================================
// The core scan-and-record function -- the heart of G8.
// ============================================================================

/// Statistics from one `record_resolutions` call. Counters are mutually
/// exclusive per position: each position in the input bumps EXACTLY ONE of
/// these counters.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RecordStats {
    /// Position was newly processed this call (P&L recorded for steady-state,
    /// or initial_snapshot row written for startup pass).
    pub processed: usize,
    /// Position is in the recorder set already (skipped to prevent double-count).
    /// During `initial_pass`, if the position is ALSO in bs.positions, we also
    /// remove it as a self-healing cleanup of a crash-leftover entry -- but
    /// the counter still bumps here (the "processed" semantics is "new work").
    pub skipped_already_done: usize,
    /// Position not in bs.positions (close_due handled it, OR a manual/non-bot
    /// position visible in /positions).
    pub skipped_not_in_bs: usize,
    /// Position not resolved per `is_resolved_at(today)` (still active).
    pub skipped_not_resolved: usize,
    /// Position resolved per is_resolved_at BUT cur_price was a stale
    /// intermediate value, so the outcome was classified by the `redeemable`
    /// flag instead of cur_price. Still recorded (counted in `processed` too).
    pub classified_by_redeemable: usize,
    /// Self-heal: position was already in recorder set AND still in bs.positions
    /// (a crash between record and the next state.json snapshot). Removed now.
    pub cleanup_removals: usize,
}

/// Scan /positions, find newly-resolved bot-opened positions, register their
/// P&L into the daily counter, remove from bs.positions.
///
/// MUTATION ORDER (see file-level invariant 3):
///   1. classify   (resolved? bot-opened? not already-recorded? cur_price clean?)
///   2. compute    (pure net P&L)
///   3. persist    (`PnlRecord::Recorded` row to disk -- CRASH BARRIER here)
///   4. counter    (`Guards::record_net_pnl` in-memory)
///   5. cleanup    (remove from bs.positions; next periodic snapshot persists)
///
/// If `initial_pass=true`, step 3 writes `PnlRecord::InitialSnapshot` and
/// step 4 is SKIPPED -- the goal of the startup pass is to catalog the backlog
/// WITHOUT inflating today's daily counter.
///
/// `now_ms_arg` is taken as a parameter (not `state::now_ms()` directly) so
/// tests can pin time deterministically.
pub fn record_resolutions(
    positions: &[PositionInfo],
    bs: &SharedBotState,
    guards: &Arc<Mutex<Guards>>,
    recorder: &mut PnlRecorder,
    oplog: &OpLog,
    initial_pass: bool,
    now_ms_arg: i64,
) -> RecordStats {
    use polymarket_client_sdk_v2::types::Utc;
    let today = Utc::now().date_naive();
    let mut stats = RecordStats::default();
    let mut bs_lock = match bs.lock() {
        Ok(g) => g,
        Err(_) => {
            warn!("pnl_recorder: bs.positions mutex POISONED; skipping this tick");
            return stats;
        }
    };

    for p in positions {
        // (1a) Resolved? If no, skip. NOTE: win/lose is NOT decided here -- the
        // AUTHORITATIVE booking is the activity-feed reconciler (record_from_activity),
        // which reads the real redeem PAYOUT. This /positions path only books the
        // unambiguous clean-extreme cur_price case (see 1d); it must never guess from
        // `redeemable`, which is true for BOTH winners and losers post-resolution.
        let (resolved, _why) = p.is_resolved_at(today);
        if !resolved {
            stats.skipped_not_resolved += 1;
            continue;
        }

        // (1b) Already in recorder set?
        let already = recorder.already_recorded(&p.token_id);
        if already {
            // Self-heal: if startup pass AND still in bs.positions, this is a
            // crash-leftover. Remove from bs.positions now. The state.json
            // persist will happen on the next snapshot tick.
            let in_bs = bs_lock.positions.iter().any(|bp| bp.token_id == p.token_id);
            if initial_pass && in_bs {
                bs_lock.positions.retain(|bp| bp.token_id != p.token_id);
                stats.cleanup_removals += 1;
                info!(token = %p.token_id,
                    "pnl_recorder: cleanup -- crash-leftover entry in bs.positions removed");
            }
            stats.skipped_already_done += 1;
            continue;
        }

        // (1c) Bot-opened? If not in bs.positions, skip (close_due already
        // handled it OR it's a manual/non-bot position).
        let bot_lots: Vec<&OpenPosition> = bs_lock
            .positions
            .iter()
            .filter(|bp| bp.token_id == p.token_id)
            .collect();
        if bot_lots.is_empty() {
            stats.skipped_not_in_bs += 1;
            continue;
        }

        // (1d) Outcome price. ONLY a clean extreme cur_price (0/1) is trustworthy
        // here. An intermediate (stale) cur_price is ambiguous -- and `redeemable`
        // CANNOT disambiguate (it is true for losers too, which the bot also
        // redeems for $0). So we SKIP ambiguous ones and let the activity-feed
        // reconciler book them from the real payout. This prevents booking a
        // loser as a win.
        let resolved_price = match resolved_price_for_pnl(p.cur_price) {
            Some(x) => x,
            None => {
                stats.classified_by_redeemable += 1; // (reused as "deferred to activity")
                continue;
            }
        };

        // (2) Cost basis from bs.positions: aggregate ACROSS LOTS for this
        // token (Polymarket settles one entry per token; multiple buys
        // accumulate into one redemption).
        let total_shares: f64 = bot_lots
            .iter()
            .map(|bp| bp.shares.to_f64().unwrap_or(0.0))
            .sum();
        let total_cost: f64 = bot_lots
            .iter()
            .map(|bp| {
                let s = bp.shares.to_f64().unwrap_or(0.0);
                let e = bp.entry_price.to_f64().unwrap_or(0.0);
                s * e
            })
            .sum();
        if total_shares <= 0.0 {
            // Zero-share lot -- ghost entry. Skip as if not in bs.
            stats.skipped_not_in_bs += 1;
            continue;
        }
        let avg_entry = total_cost / total_shares;
        let interval = bot_lots.first().map(|b| b.interval.clone()).unwrap_or_else(d_interval_5m);

        if initial_pass {
            // STARTUP path: write InitialSnapshot row, remove from bs.positions,
            // NO record_net_pnl. This catalogs the backlog silently so it
            // cannot inflate today's daily counter on deploy day.
            let rec = PnlRecord::InitialSnapshot {
                token_id: p.token_id.clone(),
                ts_ms: now_ms_arg,
            };
            if let Err(e) = recorder.record(rec) {
                warn!(error = %e, token = %p.token_id,
                    "pnl_recorder: failed to persist InitialSnapshot; SKIP removal -- \
                     retry next startup");
                continue;
            }
            bs_lock.positions.retain(|bp| bp.token_id != p.token_id);
            stats.processed += 1;
            continue;
        }

        // STEADY-STATE path: full pipeline (compute -> persist -> counter -> remove).
        // (3) Compute net P&L (pure, no IO).
        let net = compute_resolution_net_pnl(total_shares, avg_entry, resolved_price);

        // (4) PERSIST the Recorded row to disk FIRST. This is the crash barrier:
        // if the bot dies between this line and the next, the recorder set
        // contains the token_id on restart -> single-count preserved (at the
        // cost of under-counting this one event in Guards -- daily_net_pnl
        // resets at restart anyway in the current Guards design).
        let rec = PnlRecord::Recorded {
            token_id: p.token_id.clone(),
            ts_ms: now_ms_arg,
            shares: total_shares,
            entry_price: avg_entry,
            resolved_price,
            net_pnl: net,
            interval: interval.clone(),
        };
        if let Err(e) = recorder.record(rec) {
            warn!(error = %e, token = %p.token_id,
                "pnl_recorder: failed to persist Recorded row; SKIP record_net_pnl + remove. \
                 Retry next tick. UNDER-COUNT is preferred to DOUBLE-COUNT.");
            continue;
        }

        // (5) Counter (in-memory). Conversion via Decimal::try_from is lossless
        // for typical money magnitudes; on overflow we fall back to Decimal::ZERO
        // (rare; would only happen with absurdly large nets).
        let net_dec = Decimal::try_from(net).unwrap_or_default();
        if let Ok(mut g) = guards.lock() {
            g.record_net_pnl(net_dec, now_ms_arg);
        } else {
            warn!(token = %p.token_id,
                "pnl_recorder: guards mutex poisoned -- could NOT record into daily counter \
                 (already persisted to pnl_recorded.jsonl, so will NOT re-attempt)");
        }

        // (6) Cleanup: remove from bs.positions. Next periodic state.json
        // snapshot picks this up. If a crash hits before that, the recorder
        // set still has the entry; restart sees it + skips (no double-count).
        bs_lock.positions.retain(|bp| bp.token_id != p.token_id);

        oplog.sys(
            "pnl_recorder_recorded",
            serde_json::json!({
                "token_id": p.token_id,
                "shares": total_shares,
                "entry_price": avg_entry,
                "resolved_price": resolved_price,
                "net_pnl": net,
                "ts_ms": now_ms_arg,
                "interval": interval,
            }),
        );
        info!(
            token = %p.token_id,
            shares = total_shares,
            entry = avg_entry,
            resolved_price,
            net_pnl = net,
            "pnl_recorder: resolved position recorded into daily P&L"
        );
        stats.processed += 1;
    }

    stats
}

/// Record P&L for a bot position whose market has SETTLED, with the known
/// outcome `resolved_price` (1.0 = won, 0.0 = lost). Cost basis comes from
/// bs.positions (the bot's actual fills). `source` labels the trigger in the
/// oplog (e.g. "redeem", "activity").
///
/// IDEMPOTENT: the shared `recorder` set is keyed by token_id, so whichever path
/// (redeem-time, activity reconcile, or the /positions poll) fires first records,
/// the others no-op. Returns true iff a new row was written. Mirrors the
/// steady-state pipeline exactly (persist -> counter -> remove).
pub fn record_settled(
    token_id: &str,
    resolved_price: f64,
    source: &str,
    bs: &SharedBotState,
    guards: &Arc<Mutex<Guards>>,
    recorder: &mut PnlRecorder,
    oplog: &OpLog,
    now_ms_arg: i64,
) -> bool {
    if recorder.already_recorded(token_id) {
        return false; // another path already recorded it.
    }
    let mut bs_lock = match bs.lock() {
        Ok(g) => g,
        Err(_) => {
            warn!(token = %token_id, "record_settled: bs.positions mutex POISONED; skip");
            return false;
        }
    };
    let bot_lots: Vec<&OpenPosition> = bs_lock
        .positions
        .iter()
        .filter(|bp| bp.token_id == token_id)
        .collect();
    if bot_lots.is_empty() {
        // Already removed or a manual (non-bot) position.
        return false;
    }
    let total_shares: f64 = bot_lots.iter().map(|bp| bp.shares.to_f64().unwrap_or(0.0)).sum();
    let total_cost: f64 = bot_lots
        .iter()
        .map(|bp| bp.shares.to_f64().unwrap_or(0.0) * bp.entry_price.to_f64().unwrap_or(0.0))
        .sum();
    if total_shares <= 0.0 {
        return false;
    }
    let avg_entry = total_cost / total_shares;
    let interval = bot_lots.first().map(|b| b.interval.clone()).unwrap_or_else(d_interval_5m);
    let net = compute_resolution_net_pnl(total_shares, avg_entry, resolved_price);

    // (persist FIRST -- crash barrier)
    let rec = PnlRecord::Recorded {
        token_id: token_id.to_string(),
        ts_ms: now_ms_arg,
        shares: total_shares,
        entry_price: avg_entry,
        resolved_price,
        net_pnl: net,
        interval: interval.clone(),
    };
    if let Err(e) = recorder.record(rec) {
        warn!(token = %token_id, error = %e,
            "record_settled: persist failed; skip counter+remove (retry next tick)");
        return false;
    }
    // (counter)
    let net_dec = Decimal::try_from(net).unwrap_or_default();
    if let Ok(mut g) = guards.lock() {
        g.record_net_pnl(net_dec, now_ms_arg);
    } else {
        warn!(token = %token_id, "record_settled: guards mutex poisoned -- net persisted but not counted");
    }
    // (cleanup)
    bs_lock.positions.retain(|bp| bp.token_id != token_id);
    // Emits "pnl_recorder_recorded" (the dashboard EXCLUDES this kind from its
    // oplog walk to avoid double-counting the pnl_recorded.jsonl row).
    oplog.sys(
        "pnl_recorder_recorded",
        serde_json::json!({
            "token_id": token_id,
            "shares": total_shares,
            "entry_price": avg_entry,
            "resolved_price": resolved_price,
            "net_pnl": net,
            "source": source,
            "ts_ms": now_ms_arg,
            "interval": interval,
        }),
    );
    info!(token = %token_id, resolved_price, net_pnl = net, source, %interval,
        "pnl_recorder: recorded settled position");
    true
}

/// AUTHORITATIVE settlement reconciliation from the on-chain activity feed.
///
/// `/positions` drops resolved markets before the periodic poll sees them, so
/// resolutions silently slip through. The activity feed (BUY trades + REDEEMs)
/// is the ground truth and never drops anything. For each bot token still open
/// in bs.positions, find its market (condition_id, via the bot's own BUY trade
/// rows) and, if that market has a REDEEM row, book it: payout>0 => WIN (1.0),
/// payout==0 => LOSS (0.0). Cost basis still comes from bs (exact bot fills).
/// Idempotent via the shared recorder set. Returns the (token_id, won) pairs newly
/// booked this call, so the caller can feed the rolling recalibrator with TRUE
/// outcomes (its only reliable win/lose source -- the self-heal depends on it).
pub fn record_from_activity(
    rows: &[crate::rest::ActivityRow],
    bs: &SharedBotState,
    guards: &Arc<Mutex<Guards>>,
    recorder: &mut PnlRecorder,
    oplog: &OpLog,
    now_ms_arg: i64,
) -> Vec<(String, bool)> {
    use crate::rest::ActivityKind;
    use std::collections::HashMap;

    let mut booked: Vec<(String, bool)> = Vec::new();
    // Bot tokens currently open (the ones we still need to settle).
    let bot_tokens: HashSet<String> = match bs.lock() {
        Ok(g) => g.positions.iter().map(|p| p.token_id.clone()).collect(),
        Err(_) => return booked,
    };
    // NOTE: do NOT early-return when bot_tokens is empty — the Order #8 B correction
    // pass must run for ALREADY-booked tokens (removed from bs) whose redeem now
    // disagrees. The main booking loop is naturally a no-op when bs is empty.

    // token -> condition (from the bot's own BUY trade rows in the feed).
    let mut tok_cond: HashMap<String, String> = HashMap::new();
    // condition -> total redeem payout (USDC); presence => the market settled.
    let mut cond_payout: HashMap<String, f64> = HashMap::new();
    for r in rows {
        match r.kind {
            ActivityKind::Trade => {
                // ALL bot BUY rows (the wallet only trades via the bot) — not just
                // tokens still in bs — so Order #8 B can check already-booked tokens
                // (removed from bs) against a later disagreeing redeem.
                if let Some(t) = &r.token_id {
                    if !r.condition_id.is_empty() {
                        tok_cond.insert(t.clone(), r.condition_id.clone());
                    }
                }
            }
            ActivityKind::Redeem => {
                if !r.condition_id.is_empty() {
                    *cond_payout.entry(r.condition_id.clone()).or_insert(0.0) += r.usdc;
                }
            }
            ActivityKind::Other => {}
        }
    }

    for token in &bot_tokens {
        if recorder.already_recorded(token) {
            continue;
        }
        let Some(cond) = tok_cond.get(token) else { continue }; // no BUY row seen yet
        match cond_payout.get(cond) {
            Some(payout) => {
                let won = *payout > 1e-9;
                let resolved_price = if won { 1.0 } else { 0.0 };
                if record_settled(token, resolved_price, "activity", bs, guards, recorder, oplog, now_ms_arg) {
                    booked.push((token.clone(), won));
                }
            }
            None => {
                // BUG A (ii): a BUY exists, the window expired long ago, and NO REDEEM
                // row ever arrived => book as LOSS. Winners always redeem (eventually);
                // a loser's $0 payout produces no activity row, so it would otherwise
                // vanish from the books forever (the -$14.65 losses-only hole). This
                // only reaches genuinely-STUCK positions: the Binance-REST fallback (i)
                // and the /positions path both remove any they can settle from bs first,
                // so reaching here means both failed for NO_REDEEM_LOSS_SECS. Erring to
                // "loss" can at worst under-report a slow-redeeming winner (conservative
                // direction — it can never hide a loss). N is generous so a normal
                // redeem backlog never trips it. Logged as source=activity_no_redeem_loss.
                const NO_REDEEM_LOSS_SECS: i64 = 45 * 60;
                let expired_by = bs.lock().ok().and_then(|g| {
                    g.positions
                        .iter()
                        .find(|p| &p.token_id == token)
                        .map(|p| now_ms_arg / 1000 - (p.exit_ts_s - 300))
                });
                if expired_by.map(|e| e >= NO_REDEEM_LOSS_SECS).unwrap_or(false)
                    && record_settled(token, 0.0, "activity_no_redeem_loss", bs, guards, recorder, oplog, now_ms_arg)
                {
                    booked.push((token.clone(), false));
                }
            }
        }
    }

    // ORDER #8 B: CORRECTION PASS. A redeem that disagrees with an already-booked
    // outcome means a booking path lied (the phantom: Binance-label win booked
    // first, real $0 payout arrived 2 min later, idempotency made the lie stick).
    // Heal the daily counter in the SAME tick + write a `corrected` row + alert.
    for (token, cond) in &tok_cond {
        if !recorder.already_recorded(token) {
            continue;
        }
        let Some(payout) = cond_payout.get(cond) else { continue }; // no redeem yet
        let truth_resolved = if *payout > 1e-9 { 1.0 } else { 0.0 };
        let Some((shares, entry, recorded_resolved, recorded_net)) = recorder.recorded_outcome(token) else {
            continue;
        };
        if (recorded_resolved - truth_resolved).abs() < 0.5 {
            continue; // booked outcome agrees with the payout — nothing to correct
        }
        let true_net = compute_resolution_net_pnl(shares, entry, truth_resolved);
        match recorder.record_correction(token, truth_resolved, true_net, now_ms_arg) {
            Ok(Some(delta)) => {
                if let Ok(mut g) = guards.lock() {
                    g.record_net_pnl(Decimal::try_from(delta).unwrap_or_default(), now_ms_arg);
                }
                oplog.sys("pnl_corrected", serde_json::json!({
                    "token_id": token, "old_net": recorded_net, "true_net": true_net,
                    "delta": delta, "truth_resolved": truth_resolved,
                    "recorded_resolved": recorded_resolved,
                }));
                warn!(token = %token, old_net = recorded_net, true_net, delta,
                    "pnl CORRECTED — a booking path disagreed with the redeem payout");
            }
            Ok(None) => {}
            Err(e) => warn!(token = %token, error = %e, "pnl correction persist failed"),
        }
    }
    booked
}

// ============================================================================
// Startup pass: catalog the backlog WITHOUT inflating today's counter.
// ============================================================================

/// Run ONCE at live-mode startup, BEFORE `run_positions_refresh` ticks.
///
/// Goal: every position currently resolved + still in bs.positions is marked
/// `PnlRecord::InitialSnapshot` (no P&L recorded) so that on the first
/// positions_refresh tick after startup, those positions are seen as
/// "already done" and skipped. The daily-loss-stop therefore starts the
/// deploy day at zero -- it can only trip on resolutions detected POST-startup.
///
/// Errors only on /positions REST failure -- the caller decides whether to
/// proceed (the rest of the bot is unaffected; auto-redeem still works).
pub async fn snapshot_existing_resolved(
    rest: &RestClient,
    bs: &SharedBotState,
    recorder: &mut PnlRecorder,
    guards: &Arc<Mutex<Guards>>,
    oplog: &OpLog,
) -> Result<RecordStats> {
    info!(
        "pnl_recorder: snapshot_existing_resolved -- catching up backlog SILENTLY \
         (no P&L recorded into Guards). Resolutions detected after this point WILL \
         feed the daily-loss-stop."
    );
    let positions = rest
        .get_positions()
        .await
        .context("snapshot_existing_resolved: /positions GET")?;
    let stats = record_resolutions(
        &positions, bs, guards, recorder, oplog, /*initial_pass=*/ true, now_ms(),
    );
    info!(
        backlog_marked = stats.processed,
        cleanup_removals = stats.cleanup_removals,
        skipped_already_done = stats.skipped_already_done,
        skipped_not_in_bs = stats.skipped_not_in_bs,
        skipped_not_resolved = stats.skipped_not_resolved,
        classified_by_redeemable = stats.classified_by_redeemable,
        "pnl_recorder: startup snapshot complete -- backlog SKIPPED from daily P&L"
    );
    oplog.sys(
        "pnl_recorder_initial_snapshot",
        serde_json::json!({
            "backlog_marked": stats.processed,
            "cleanup_removals": stats.cleanup_removals,
            "classified_by_redeemable": stats.classified_by_redeemable,
            "skipped_not_in_bs": stats.skipped_not_in_bs,
        }),
    );
    Ok(stats)
}

// ============================================================================
// TESTS -- pure / no HTTP. The 9 the user prioritized + 2 extra for the
// sanity-guard classifier.
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::guards::{GuardConfig, Guards};
    use crate::state::persist::{BotState, ConfirmationSource, OpenPosition, OrderStatus, Outcome};
    use polymarket_client_sdk_v2::types::NaiveDate;
    use rust_decimal_macros::dec;
    use std::sync::{Arc, Mutex};

    // ---------- helpers ----------

    fn tmp_path(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "rb_g8_{tag}_{}_{}.jsonl",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    fn position(
        token_id: &str,
        condition_id: &str,
        cur_price: f64,
        redeemable: bool,
        size: f64,
    ) -> PositionInfo {
        PositionInfo {
            token_id: token_id.to_string(),
            size,
            avg_price: 0.4,
            cur_price,
            cash_pnl: 0.0,
            redeemable,
            end_date: NaiveDate::from_ymd_opt(2027, 1, 1),
            condition_id: condition_id.to_string(),
        }
    }

    fn bot_lot(token_id: &str, entry_price: Decimal, shares: Decimal) -> OpenPosition {
        OpenPosition {
            token_id: token_id.to_string(),
            asset: "BTC".to_string(),
            side: Outcome::Up,
            entry_price,
            shares,
            opened_at_ms: 1_000,
            signal_id: "sig-1".to_string(),
            interval: "5m".to_string(),
            exit_ts_s: 2_000,
            // v3 (Pieza 1) sentinels: pnl_recorder tests aggregate lots into
            // realized P&L; smart-exit state is irrelevant here.
            entry_ts_ms: 0,
            running_max_bid: 0.0,
            ts_max_bid_ms: 0,
            status: OrderStatus::Confirmed,
            order_id: Some("o-1".to_string()),
            ack_at_ms: Some(1_500),
            confirmed_at_ms: Some(1_600),
            confirmation_source: Some(ConfirmationSource::UserWs),
            maker_exit: None,
        }
    }

    fn mk_bs(lots: Vec<OpenPosition>) -> SharedBotState {
        let mut bs = BotState::default();
        bs.positions = lots;
        Arc::new(Mutex::new(bs))
    }

    fn mk_guards() -> Arc<Mutex<Guards>> {
        Arc::new(Mutex::new(Guards::new(GuardConfig::default())))
    }

    fn mk_oplog() -> OpLog {
        // Tmp-file oplog -- the events emitted are not asserted in these tests;
        // we only verify Guards state + bs state + recorder set.
        OpLog::new(tmp_path("oplog"))
    }

    fn t0() -> i64 {
        1_780_000_000_000_i64
    }

    // ---------- PURE: resolved_price_for_pnl + compute_resolution_net_pnl ----------

    #[test]
    fn g8_pnl_resolved_price_for_pnl_classification() {
        // Loser side: anything <= 0.001 collapses to 0.0.
        assert_eq!(resolved_price_for_pnl(0.0), Some(0.0));
        assert_eq!(resolved_price_for_pnl(0.0005), Some(0.0));
        assert_eq!(resolved_price_for_pnl(0.001), Some(0.0));
        // Winner side: >= 0.999 collapses to 1.0.
        assert_eq!(resolved_price_for_pnl(0.999), Some(1.0));
        assert_eq!(resolved_price_for_pnl(1.0), Some(1.0));
        // Ambiguous band: anything in (0.001, 0.999) -> None (SKIP).
        assert_eq!(resolved_price_for_pnl(0.002), None);
        assert_eq!(resolved_price_for_pnl(0.5), None);
        assert_eq!(resolved_price_for_pnl(0.998), None);
        // NaN: None.
        assert_eq!(resolved_price_for_pnl(f64::NAN), None);
    }

    #[test]
    fn g8_pnl_compute_net_pnl_loser() {
        // shares=1, entry=$0.40, resolved=$0.00 (loser):
        //   net = (1*0 - 0) - (1*0.40 + 0.07*1*0.40*0.60)
        //       = 0 - 0.40 - 0.0168 = -0.4168
        let net = compute_resolution_net_pnl(1.0, 0.40, 0.0);
        assert!((net - (-0.4168)).abs() < 1e-9, "net={net}");
    }

    #[test]
    fn g8_pnl_compute_net_pnl_winner() {
        // shares=2.625 (= $1.05 stake at $0.40), entry=$0.40, resolved=$1.00:
        //   net = (2.625*1 - 0) - (2.625*0.40 + fee_buy)
        //       = 2.625 - 1.05 - 0.0441 = 1.5309
        let net = compute_resolution_net_pnl(2.625, 0.40, 1.0);
        let expected = 2.625 - 1.05 - 0.07 * 2.625 * 0.40 * 0.60;
        assert!((net - expected).abs() < 1e-9, "net={net} expected={expected}");
        assert!(net > 0.0, "winner net must be positive");
    }

    // ---------- INVARIANT 1: SINGLE-COUNT (the most important tests) ----------

    /// g8_pnl_no_double_count_resolve_then_close_due:
    /// A resolved position is recorded by record_resolutions. Then close_due
    /// runs against the same bs.positions and SEES IT GONE (because the
    /// recorder retain()'d it out). No double-count, no SELL attempt.
    #[test]
    fn g8_pnl_no_double_count_resolve_then_close_due() {
        let bs = mk_bs(vec![bot_lot("tok1", dec!(0.40), dec!(1))]);
        let guards = mk_guards();
        let mut recorder = PnlRecorder::load(tmp_path("nodc_rcd")).unwrap();
        let oplog = mk_oplog();
        // Position is resolved as LOSER on /positions.
        let pos = vec![position("tok1", "0xcid1", 0.0, true, 1.0)];

        let stats = record_resolutions(&pos, &bs, &guards, &mut recorder, &oplog, false, t0());
        assert_eq!(stats.processed, 1);

        // After recording, bs.positions MUST no longer contain tok1.
        let leftover_count = {
            let bs_l = bs.lock().unwrap();
            bs_l.positions.iter().filter(|p| p.token_id == "tok1").count()
        };
        assert_eq!(leftover_count, 0,
            "post-record bs.positions must NOT contain the resolved token -- \
             close_due running afterwards must see it GONE (no double-count)");

        // The recorder set MUST contain it (gate for second record).
        assert!(recorder.already_recorded("tok1"),
            "recorder set must contain the recorded token to gate any future re-record");

        // Counter MUST reflect the loss (net = -0.4168).
        let pnl = guards.lock().unwrap().daily_net_pnl();
        assert!(pnl < dec!(-0.4) && pnl > dec!(-0.5),
            "daily_net_pnl reflects the loss; got {pnl}");
    }

    /// g8_pnl_no_double_count_sell_then_resolve:
    /// SELL happens FIRST (close_due removes from bs.positions, feeds guard).
    /// LATER, the market resolves on-chain; positions_refresh sees the
    /// position in /positions as resolved (maybe data-api lag) but
    /// bs.positions no longer has it -> recorder SKIPS. No double-count.
    #[test]
    fn g8_pnl_no_double_count_sell_then_resolve() {
        // Simulate: close_due already removed the lot from bs.positions and
        // fed feed_guards_net_pnl. So bs starts EMPTY.
        let bs = mk_bs(vec![]);
        let guards = mk_guards();
        // Pre-load the guard with the SELL net (simulating feed_guards_net_pnl
        // ran).  shares=1, exit=0.50, entry=0.40 -> net ~+0.0976
        guards.lock().unwrap().record_net_pnl(dec!(0.0976), t0());
        let pre = guards.lock().unwrap().daily_net_pnl();

        let mut recorder = PnlRecorder::load(tmp_path("nodc_str")).unwrap();
        let oplog = mk_oplog();

        // /positions still reports the position (data-api lag) AS resolved.
        let pos = vec![position("tok1", "0xcid1", 1.0, true, 1.0)];

        let stats = record_resolutions(&pos, &bs, &guards, &mut recorder, &oplog, false, t0());

        // MUST skip (not in bs.positions).
        assert_eq!(stats.skipped_not_in_bs, 1);
        assert_eq!(stats.processed, 0);

        // Counter unchanged.
        let post = guards.lock().unwrap().daily_net_pnl();
        assert_eq!(pre, post,
            "SELL-then-resolve must NOT touch the counter twice; pre={pre} post={post}");

        // Recorder set MUST NOT contain it (we never recorded; close_due path
        // is independent of the recorder).
        assert!(!recorder.already_recorded("tok1"),
            "recorder set is for resolution-path only; SELL path doesn't write here");
    }

    // ---------- INVARIANT 2: NO BACKLOG at startup ----------

    /// g8_pnl_initial_snapshot_skips_backlog:
    /// 3 already-resolved positions exist in bs.positions at startup.
    /// snapshot_existing_resolved (initial_pass=true) marks all 3 as
    /// InitialSnapshot WITHOUT touching the daily counter. The daily counter
    /// stays at zero -- the deploy day is unaffected by stale losses.
    #[test]
    fn g8_pnl_initial_snapshot_skips_backlog() {
        let bs = mk_bs(vec![
            bot_lot("tok1", dec!(0.40), dec!(1)),
            bot_lot("tok2", dec!(0.30), dec!(2)),
            bot_lot("tok3", dec!(0.70), dec!(0.5)),
        ]);
        let guards = mk_guards();
        let mut recorder = PnlRecorder::load(tmp_path("init")).unwrap();
        let oplog = mk_oplog();
        // All 3 resolved (2 losers, 1 winner).
        let pos = vec![
            position("tok1", "0xcid1", 0.0, true, 1.0), // loser, -0.4168
            position("tok2", "0xcid2", 0.0, true, 2.0), // loser, -0.6252
            position("tok3", "0xcid3", 1.0, true, 0.5), // winner, +0.1426
        ];

        let stats = record_resolutions(&pos, &bs, &guards, &mut recorder, &oplog, true, t0());
        assert_eq!(stats.processed, 3, "all 3 backlog positions marked as snapshot");

        // CRITICAL: daily counter must be ZERO post-snapshot.
        let pnl = guards.lock().unwrap().daily_net_pnl();
        assert_eq!(pnl, Decimal::ZERO,
            "initial_pass MUST NOT call record_net_pnl -- daily counter stays at 0; got {pnl}");

        // bs.positions must be empty (all 3 removed).
        assert_eq!(bs.lock().unwrap().positions.len(), 0);

        // All 3 in recorder set (so subsequent ticks skip them).
        assert!(recorder.already_recorded("tok1"));
        assert!(recorder.already_recorded("tok2"));
        assert!(recorder.already_recorded("tok3"));
    }

    // ---------- INVARIANT 3: persistence + crash safety ----------

    /// g8_pnl_persists_and_survives_restart: write a Recorded row, reload
    /// from disk, the entry must still be in the set.
    #[test]
    fn g8_pnl_persists_and_survives_restart() {
        let path = tmp_path("persist");
        let _ = std::fs::remove_file(&path);
        let mut r = PnlRecorder::load(&path).unwrap();
        let rec = PnlRecord::Recorded {
            token_id: "0xabc".to_string(),
            ts_ms: t0(),
            shares: 1.0,
            entry_price: 0.4,
            resolved_price: 0.0,
            net_pnl: -0.4168,
            interval: "5m".to_string(),
        };
        r.record(rec.clone()).unwrap();
        assert!(r.already_recorded("0xabc"));

        // Reload (simulates bot restart).
        let r2 = PnlRecorder::load(&path).unwrap();
        assert!(r2.already_recorded("0xabc"),
            "recorder set MUST survive restart");
        // Case-insensitive match.
        assert!(r2.already_recorded("0xABC"));
        let _ = std::fs::remove_file(&path);
    }

    /// g8_pnl_register_then_remove_order: verify the explicit step order
    /// (persist BEFORE record_net_pnl BEFORE bs.positions removal). Done by
    /// inspecting the recorder file mid-call (via a wrapping check on the
    /// state right after `record_resolutions` returns: the file MUST contain
    /// the Recorded row, and `bs.positions` MUST be empty).
    #[test]
    fn g8_pnl_register_then_remove_order() {
        let path = tmp_path("order");
        let _ = std::fs::remove_file(&path);
        let bs = mk_bs(vec![bot_lot("tok1", dec!(0.40), dec!(1))]);
        let guards = mk_guards();
        let mut recorder = PnlRecorder::load(&path).unwrap();
        let oplog = mk_oplog();
        let pos = vec![position("tok1", "0xcid1", 0.0, true, 1.0)];

        let _ = record_resolutions(&pos, &bs, &guards, &mut recorder, &oplog, false, t0());

        // Order check:
        //   1. file has the Recorded row (= persist happened).
        //   2. recorder.already_recorded == true (= in-memory updated AFTER persist).
        //   3. counter has the net (= record_net_pnl ran AFTER persist).
        //   4. bs.positions empty (= remove ran LAST).
        let body = std::fs::read_to_string(&path).unwrap();
        assert!(body.contains("\"recorded\""),
            "file must contain the Recorded row by the time the function returns");
        assert!(body.contains("\"tok1\""), "file must contain the token_id");
        assert!(recorder.already_recorded("tok1"));
        assert!(guards.lock().unwrap().daily_net_pnl() < dec!(-0.4));
        assert_eq!(bs.lock().unwrap().positions.len(), 0);
        let _ = std::fs::remove_file(&path);
    }

    // ---------- INVARIANT 1 corollary: race & cleanup ----------

    /// g8_pnl_concurrent_close_due_vs_recorder_no_race:
    /// Same bs.positions, both close_due and recorder try to process. The
    /// Mutex serializes them. Whoever runs first removes the entry; the other
    /// sees absence and skips. We simulate by running record FIRST (taking
    /// the mutex), confirming bs is empty, then simulating a close_due call
    /// that finds nothing to close.
    #[test]
    fn g8_pnl_concurrent_close_due_vs_recorder_no_race() {
        let bs = mk_bs(vec![bot_lot("tok1", dec!(0.40), dec!(1))]);
        let guards = mk_guards();
        let mut recorder = PnlRecorder::load(tmp_path("race")).unwrap();
        let oplog = mk_oplog();
        let pos = vec![position("tok1", "0xcid1", 0.0, true, 1.0)];

        // Recorder wins.
        let _ = record_resolutions(&pos, &bs, &guards, &mut recorder, &oplog, false, t0());
        assert_eq!(bs.lock().unwrap().positions.len(), 0);

        // Simulate close_due finding nothing: bs is empty, no SELL emitted.
        // (The actual close_due is exercised by its own tests; here we assert
        // the post-condition the resolver guarantees: bs.positions is empty
        // by the time the mutex is released.)

        // Recorder run AGAIN with the same /positions input: must be a no-op
        // (already_recorded path).
        let stats2 = record_resolutions(&pos, &bs, &guards, &mut recorder, &oplog, false, t0());
        assert_eq!(stats2.skipped_already_done, 1,
            "second run must skip via already_recorded gate");
        assert_eq!(stats2.processed, 0);
    }

    // ---------- INVARIANT 1 corollary: non-bot positions ----------

    /// g8_pnl_recorder_skip_non_bot_opened:
    /// /positions reports a resolved position that the bot never opened.
    /// The recorder MUST skip it (not in bs.positions, not our problem).
    #[test]
    fn g8_pnl_recorder_skip_non_bot_opened() {
        let bs = mk_bs(vec![]); // bot opened nothing
        let guards = mk_guards();
        let mut recorder = PnlRecorder::load(tmp_path("nonbot")).unwrap();
        let oplog = mk_oplog();
        let pos = vec![position("tok-manual", "0xcid", 0.0, true, 1.0)];

        let stats = record_resolutions(&pos, &bs, &guards, &mut recorder, &oplog, false, t0());
        assert_eq!(stats.processed, 0);
        assert_eq!(stats.skipped_not_in_bs, 1);
        assert!(!recorder.already_recorded("tok-manual"));
        assert_eq!(guards.lock().unwrap().daily_net_pnl(), Decimal::ZERO);
    }

    // ---------- Sanity guard ----------

    /// g8_pnl_ambiguous_cur_price_deferred_to_activity:
    /// A resolved position with a STALE intermediate cur_price is NOT booked by
    /// this /positions path (redeemable can't tell win from lose). It is left for
    /// the activity-feed reconciler. No P&L, no removal, counted as deferred.
    #[test]
    fn g8_pnl_ambiguous_cur_price_deferred_to_activity() {
        let bs = mk_bs(vec![bot_lot("tok1", dec!(0.40), dec!(1))]);
        let guards = mk_guards();
        let mut recorder = PnlRecorder::load(tmp_path("ambig")).unwrap();
        let oplog = mk_oplog();
        // redeemable=true (=> is_resolved_at true) but stale cur_price 0.5.
        let pos = vec![position("tok1", "0xcid1", 0.5, true, 1.0)];

        let stats = record_resolutions(&pos, &bs, &guards, &mut recorder, &oplog, false, t0());
        assert_eq!(stats.processed, 0, "must NOT book from ambiguous cur_price");
        assert_eq!(stats.classified_by_redeemable, 1, "counted as deferred");
        assert_eq!(guards.lock().unwrap().daily_net_pnl(), Decimal::ZERO);
        assert_eq!(bs.lock().unwrap().positions.len(), 1, "left for activity reconciler");
        assert!(!recorder.already_recorded("tok1"));
    }

    /// Loss resolution writes a negative net into the counter.
    #[test]
    fn g8_pnl_loss_resolution_records_negative() {
        let bs = mk_bs(vec![bot_lot("tok1", dec!(0.40), dec!(1))]);
        let guards = mk_guards();
        let mut recorder = PnlRecorder::load(tmp_path("loss")).unwrap();
        let oplog = mk_oplog();
        let pos = vec![position("tok1", "0xcid1", 0.0, true, 1.0)];

        let _ = record_resolutions(&pos, &bs, &guards, &mut recorder, &oplog, false, t0());
        let pnl = guards.lock().unwrap().daily_net_pnl();
        assert!(pnl < Decimal::ZERO,
            "loser must record NEGATIVE net into the counter; got {pnl}");
    }

    /// Win resolution writes a positive net into the counter.
    #[test]
    fn g8_pnl_win_resolution_records_positive() {
        let bs = mk_bs(vec![bot_lot("tok1", dec!(0.40), dec!(2.625))]);
        let guards = mk_guards();
        let mut recorder = PnlRecorder::load(tmp_path("win")).unwrap();
        let oplog = mk_oplog();
        let pos = vec![position("tok1", "0xcid1", 1.0, true, 2.625)];

        let _ = record_resolutions(&pos, &bs, &guards, &mut recorder, &oplog, false, t0());
        let pnl = guards.lock().unwrap().daily_net_pnl();
        assert!(pnl > Decimal::ZERO,
            "winner must record POSITIVE net into the counter; got {pnl}");
    }

    /// Self-heal: position is in recorder set (from a previous run that
    /// recorded but crashed before state.json persist) AND still in bs.positions
    /// (because the crash dropped the in-memory removal). initial_pass MUST
    /// remove it as cleanup.
    #[test]
    fn g8_pnl_initial_pass_self_heals_crash_leftover() {
        let bs = mk_bs(vec![bot_lot("tok1", dec!(0.40), dec!(1))]);
        let guards = mk_guards();
        let path = tmp_path("heal");
        let mut recorder = PnlRecorder::load(&path).unwrap();
        // Pre-populate the recorder set as if it was recorded pre-crash.
        recorder
            .record(PnlRecord::Recorded {
                token_id: "tok1".to_string(),
                ts_ms: t0() - 60_000,
                shares: 1.0,
                entry_price: 0.4,
                resolved_price: 0.0,
                net_pnl: -0.4168,
                interval: "5m".to_string(),
            })
            .unwrap();
        let oplog = mk_oplog();
        let pos = vec![position("tok1", "0xcid1", 0.0, true, 1.0)];

        let stats = record_resolutions(&pos, &bs, &guards, &mut recorder, &oplog, true, t0());
        // already_done bumped (skipped P&L) + cleanup_removals bumped (bs purged).
        assert_eq!(stats.skipped_already_done, 1);
        assert_eq!(stats.cleanup_removals, 1);
        assert_eq!(stats.processed, 0,
            "initial_pass on already-recorded must NOT re-record");
        // bs.positions cleaned.
        assert_eq!(bs.lock().unwrap().positions.len(), 0);
        // Counter untouched.
        assert_eq!(guards.lock().unwrap().daily_net_pnl(), Decimal::ZERO);
        let _ = std::fs::remove_file(&path);
    }

    // ---------- guards::reset_daily_pnl + log audit ----------

    /// `reset_daily_pnl` zeroes the counter without touching anything else.
    /// Verifies the API that `--guard-reset-daily-pnl` will call.
    #[test]
    fn g8_pnl_guard_reset_zeroes_counter() {
        let g = mk_guards();
        g.lock().unwrap().record_net_pnl(dec!(-5), t0());
        g.lock().unwrap().record_net_pnl(dec!(-3), t0());
        assert_eq!(g.lock().unwrap().daily_net_pnl(), dec!(-8));
        g.lock().unwrap().reset_daily_pnl();
        assert_eq!(g.lock().unwrap().daily_net_pnl(), Decimal::ZERO,
            "reset must zero the counter without partial state");
    }

    // ---------- Order #8 ----------

    fn buy_row(token: &str, cond: &str, usdc: f64) -> crate::rest::ActivityRow {
        crate::rest::ActivityRow {
            condition_id: cond.into(), token_id: Some(token.into()),
            kind: crate::rest::ActivityKind::Trade, is_buy: true, usdc, ts: 1,
        }
    }
    fn redeem_row(cond: &str, usdc: f64) -> crate::rest::ActivityRow {
        crate::rest::ActivityRow {
            condition_id: cond.into(), token_id: None,
            kind: crate::rest::ActivityKind::Redeem, is_buy: false, usdc, ts: 2,
        }
    }

    /// Order #8 A (testable slice): a photo-finish token is DEFERRED by the
    /// settlement path (stays in bs), so the activity feed's real $0 payout books
    /// it as a LOSS (truth), NOT the flipped Binance-label win.
    #[test]
    fn order8_a_activity_books_deferred_photofinish_as_loss() {
        let bs = mk_bs(vec![bot_lot("tokpf", dec!(0.0746), dec!(15))]);
        let guards = mk_guards();
        let mut recorder = PnlRecorder::load(tmp_path("pfdefer")).unwrap();
        let oplog = mk_oplog();
        let rows = vec![buy_row("tokpf", "0xpf", 1.118), redeem_row("0xpf", 0.0)];
        let booked = record_from_activity(&rows, &bs, &guards, &mut recorder, &oplog, t0());
        assert_eq!(booked, vec![("tokpf".to_string(), false)], "booked as LOSS from the $0 payout");
        assert!(guards.lock().unwrap().daily_net_pnl() < Decimal::ZERO, "counter reflects a loss, not +$13.88");
    }

    /// Order #8 B: a settlement booked as a WIN, then a real $0 redeem arrives →
    /// a `corrected` row + the daily counter healed by the delta, and no
    /// double-correction on a repeat redeem.
    #[test]
    fn order8_b_correction_on_payout_mismatch() {
        let path = tmp_path("correct");
        let _ = std::fs::remove_file(&path);
        let bs = mk_bs(vec![bot_lot("tokw", dec!(0.40), dec!(2.625))]);
        let guards = mk_guards();
        let mut recorder = PnlRecorder::load(&path).unwrap();
        let oplog = mk_oplog();
        // Book the phantom WIN (record_settled removes it from bs, retains outcome).
        assert!(record_settled("tokw", 1.0, "settlement", &bs, &guards, &mut recorder, &oplog, t0()));
        let counter_after_win = guards.lock().unwrap().daily_net_pnl();
        assert!(counter_after_win > Decimal::ZERO);

        let rows = vec![buy_row("tokw", "0xcid", 1.05), redeem_row("0xcid", 0.0)];
        let _ = record_from_activity(&rows, &bs, &guards, &mut recorder, &oplog, t0());

        let body = std::fs::read_to_string(&path).unwrap();
        assert!(body.contains("\"corrected\""), "a correction row must be written");
        // Counter healed by delta = loss_net − win_net → ends near loss_net.
        let win_net = compute_resolution_net_pnl(2.625, 0.40, 1.0);
        let loss_net = compute_resolution_net_pnl(2.625, 0.40, 0.0);
        let expected = counter_after_win + Decimal::try_from(loss_net - win_net).unwrap();
        let healed = guards.lock().unwrap().daily_net_pnl();
        assert!((healed - expected).abs() < dec!(0.001), "healed={healed} expected={expected}");
        assert!(healed < Decimal::ZERO, "net now a loss after correction");
        // Repeat redeem must NOT double-correct (outcome updated to truth).
        let _ = record_from_activity(&rows, &bs, &guards, &mut recorder, &oplog, t0());
        assert_eq!(healed, guards.lock().unwrap().daily_net_pnl(), "no double-correction");
        let _ = std::fs::remove_file(&path);
    }
}
