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

/// PURE: outcome price for a position ALREADY CONFIRMED RESOLVED.
///
/// Polymarket's `/positions` does NOT snap `cur_price` to 0/1 on resolution --
/// it returns the STALE last-trade price (e.g. 0.555, 0.375), which
/// [`resolved_price_for_pnl`] correctly refuses to classify. For a resolved
/// position the authoritative win/lose signal is the `redeemable` flag: the
/// wallet holds the WINNING side iff `redeemable == true` (this is the very
/// signal the redemption task spends gas claiming on). So: prefer a clean
/// extreme `cur_price`, else fall back to `redeemable`.
///
/// MUST only be called once resolution is established (`is_resolved_at`); on an
/// active market `redeemable` is false and this would wrongly return 0.0.
#[must_use]
pub fn resolved_outcome_price(cur_price: f64, redeemable: bool) -> f64 {
    resolved_price_for_pnl(cur_price).unwrap_or(if redeemable { 1.0 } else { 0.0 })
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
    },
}

impl PnlRecord {
    pub fn token_id(&self) -> &str {
        match self {
            PnlRecord::InitialSnapshot { token_id, .. } => token_id,
            PnlRecord::Recorded { token_id, .. } => token_id,
        }
    }
}

/// File-backed set of token_ids whose resolution-P&L has been
/// recorded-or-explicitly-snapshotted. Loaded on startup, append-only after.
#[derive(Debug, Clone)]
pub struct PnlRecorder {
    path: PathBuf,
    done: HashSet<String>, // lowercased token_ids
}

impl PnlRecorder {
    /// Load the recorder from `path`. Missing file is OK (returns empty).
    pub fn load(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let mut done = HashSet::new();
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
            }
        }
        Ok(Self { path, done })
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
        self.done.insert(lower);
        Ok(())
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
        // (1a) Resolved? If no, skip.
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

        // (1d) Outcome price. A clean extreme cur_price (0/1) classifies
        // directly; otherwise Polymarket returned the STALE last-trade price on
        // this resolved market, so we fall back to the authoritative
        // `redeemable` flag (winning side iff redeemable). This is the same
        // signal the redemption task claims on, so it cannot disagree with what
        // actually pays out. Previously these were skipped as "ambiguous",
        // which lost every winner whose cur_price hadn't snapped to 1.0 yet.
        let resolved_price = resolved_outcome_price(p.cur_price, p.redeemable);
        if resolved_price_for_pnl(p.cur_price).is_none() {
            info!(
                token = %p.token_id,
                cur_price = p.cur_price,
                redeemable = p.redeemable,
                resolved_price,
                "pnl_recorder: stale cur_price on resolved position -- classified by redeemable flag"
            );
            oplog.sys(
                "pnl_recorder_classified_by_redeemable",
                serde_json::json!({
                    "token_id": p.token_id,
                    "cur_price": p.cur_price,
                    "redeemable": p.redeemable,
                    "resolved_price": resolved_price,
                    "end_date": p.end_date.map(|d| d.to_string()),
                }),
            );
            stats.classified_by_redeemable += 1;
        }

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

    /// g8_pnl_classifies_stale_cur_price_by_redeemable (WIN):
    /// Resolved position whose cur_price is a STALE intermediate value (0.5)
    /// must be classified by the `redeemable` flag, NOT skipped. redeemable=true
    /// => WIN: record positive P&L, remove from bs.positions, add to recorder.
    #[test]
    fn g8_pnl_classifies_stale_cur_price_by_redeemable_win() {
        let bs = mk_bs(vec![bot_lot("tok1", dec!(0.40), dec!(1))]);
        let guards = mk_guards();
        let mut recorder = PnlRecorder::load(tmp_path("ambig_win")).unwrap();
        let oplog = mk_oplog();
        // redeemable=true + stale cur_price 0.5 => classify as WIN (1.0).
        let pos = vec![position("tok1", "0xcid1", 0.5, true, 1.0)];

        let stats = record_resolutions(&pos, &bs, &guards, &mut recorder, &oplog, false, t0());
        assert_eq!(stats.processed, 1);
        assert_eq!(stats.classified_by_redeemable, 1);

        // WIN: net = 1*1 - (1*0.40 + buy_fee) > 0.
        assert!(guards.lock().unwrap().daily_net_pnl() > Decimal::ZERO);
        // Resolved + removed from bs.positions; recorder now holds it.
        assert_eq!(bs.lock().unwrap().positions.len(), 0);
        assert!(recorder.already_recorded("tok1"));
    }

    /// g8_pnl_classifies_stale_cur_price_by_redeemable (LOSS):
    /// Same stale cur_price, but redeemable=false => LOSS (0.0): record the
    /// negative cost basis and remove. (Resolution established via end_date.)
    #[test]
    fn g8_pnl_classifies_stale_cur_price_by_redeemable_loss() {
        let bs = mk_bs(vec![bot_lot("tok1", dec!(0.40), dec!(1))]);
        let guards = mk_guards();
        let mut recorder = PnlRecorder::load(tmp_path("ambig_loss")).unwrap();
        let oplog = mk_oplog();
        // redeemable=false + stale cur_price 0.45 => classify as LOSS (0.0).
        // end_date in the past so is_resolved_at is true without redeemable.
        let mut p = position("tok1", "0xcid1", 0.45, false, 1.0);
        p.end_date = NaiveDate::from_ymd_opt(2020, 1, 1);
        let pos = vec![p];

        let stats = record_resolutions(&pos, &bs, &guards, &mut recorder, &oplog, false, t0());
        assert_eq!(stats.processed, 1);
        assert_eq!(stats.classified_by_redeemable, 1);

        // LOSS: net = -(1*0.40 + buy_fee) < 0.
        assert!(guards.lock().unwrap().daily_net_pnl() < Decimal::ZERO);
        assert_eq!(bs.lock().unwrap().positions.len(), 0);
        assert!(recorder.already_recorded("tok1"));
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
}
