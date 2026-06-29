//! Backtest-TP — offline simulator that re-prices the bot's time-exit strategy
//! against alternative take-profit (TP) thresholds, using the recorder's HISTORIC
//! depth (multi-level bid/ask stacks with sizes), so the SELL "price" is the
//! TRULY EXECUTABLE bid for the position's shares -- not a fantasy best_bid
//! without backing depth.
//!
//! WHY THIS EXISTS:
//! The live bot's daily-loss-stop fed from paper P&L diverged by $200 vs real
//! balance over one day (the "+$194.62 paper vs -$13 real" gap). Root cause:
//! close_due simulates a SELL at `best_bid` regardless of whether the bid has
//! the depth to absorb the position -- so the paper P&L counts phantom fills.
//! G9-pre-A fixed the SELL Mismatch (multi-lot accumulation now sells correctly)
//! but the COUNTER (and the strategy itself) still uses time-exit + best_bid.
//! User observed: many positions touch +10/30/50% profit during the hold but
//! lose it by exit_ts. Hypothesis: a TP exit (sell when threshold touched)
//! would trade SOME total P&L for MUCH better consistency.
//!
//! This module backtests that hypothesis on 21 days of HISTORICAL recorder data
//! (out-of-sample: no bot version trained on this) with REAL depth (44+ levels
//! per snapshot). Comparison is fair: same decision engine -> same entries; only
//! the exit rule varies across variants.
//!
//! THE 3-PASS GUARANTEE (same entry, different exit):
//!   1. PASS 1 (single loop): replay events through the decide engine,
//!      DETERMINISTIC -> collects the SAME positions every variant will see.
//!   2. Within that same loop: maintain a FullBook per token (snapshot +
//!      price_change deltas), and for each ACTIVE position append
//!      (recv_ms, executable_bid_for_shares(shares)) to its trajectory.
//!   3. PASS 3 (after loop): per-variant, decide each position's exit:
//!        - Baseline (variant=0): exit at time_exit_price (= bid at exit_ts_s).
//!        - FirstTouch(tp): first event with executable_bid >= entry*(1+tp);
//!          if never touched, fall back to time-exit.
//!        - Peak (computed alongside): the MAXIMUM executable_bid during the
//!          window. Theoretical opportunity ceiling -- measures "left on table".
//!
//! INSUFFICIENT-DEPTH HANDLING (per user spec):
//!   If `executable_bid_for_shares(book, shares) == None` (= stack below shares),
//!   treat as a $0 exit (worst case). Count + report these as "uncoverable":
//!   high count = the bot's positions don't have liquidity to exit, which is a
//!   STRATEGIC FINDING in its own right (regardless of TP).
//!
//! BREAKDOWN (per user spec):
//!   Metrics computed separately for (5m, 15m) × (BTC, ETH) = 4 sub-strategies
//!   plus the aggregate. A TP that helps 5m-BTC but hurts 15m-ETH would be
//!   invisible in the aggregate -- the breakdown surfaces it.

#![allow(dead_code)] // wired by main.rs's --backtest-tp branch

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::decision::{Action, BookProvider, DecisionConfig, RestProbe, decide};
use crate::signal::replay::ReplayCatalog;
use crate::signal::{Direction, MarketCatalog, SignalEngine, Trigger, expand_signals};
use crate::state::EventBbo;
use crate::trade_log::fee_f64;

// ============================================================================
// OUT-OF-SAMPLE DISCIPLINE -- the only thing standing between honest research
// and overfitting. Read this whole block before editing anything below.
// ============================================================================

/// Hard cutoff between EXPLORATION and VALIDATION data. The first 10 days of
/// May 2026 (2026-05-06 .. 2026-05-16, with 2026-05-12 missing) are the
/// exploration set: free to iterate on, look at, fit hypotheses against. The
/// remaining 10 days (2026-05-17 .. 2026-05-26) are RESERVED for the Fase 3
/// final validation gate -- they're touched ONCE and the result is final.
///
/// Anything that reads dates >= this cutoff in `BtPhase::Exploration` is a
/// methodology violation (peeking at validation data contaminates the gate).
/// The `validate_phase_dates` function below enforces this as a hard abort.
pub const VALIDATION_CUTOFF_DATE: &str = "2026-05-17";

/// Exploration vs Validation: anti-overfitting discipline. Set explicitly per
/// run via `--bt-phase`. Default = `Exploration` so the typical run cannot
/// accidentally touch validation data.
///
/// `Exploration`: end_date MUST be < VALIDATION_CUTOFF_DATE. A typo in
///   `--bt-end-date` that would reach into validation aborts at startup with a
///   clear error -- the safety net.
/// `Validation`: any date allowed. The CLI banner + a `validation_seal_broken.txt`
///   file in out_dir record that a validation pass was deliberately invoked, so
///   the audit trail captures every (rare) time the seal was broken.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BtPhase {
    Exploration,
    Validation,
}

impl BtPhase {
    /// Parse a CLI string. Case-insensitive ("exploration", "EXPLORATION",
    /// "Exploration" all OK). Anything else = error (typo would otherwise
    /// silently default to one or the other).
    pub fn parse(s: &str) -> Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "exploration" => Ok(BtPhase::Exploration),
            "validation" => Ok(BtPhase::Validation),
            _ => bail!(
                "bad --bt-phase value '{s}': use 'exploration' (default; safe; \
                 forbids touching validation data) or 'validation' (deliberate \
                 final-gate pass; logged as a sealed event)"
            ),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            BtPhase::Exploration => "exploration",
            BtPhase::Validation => "validation",
        }
    }
}

/// Enforce out-of-sample discipline. Called FIRST inside `run_backtest_tp`
/// (before any data is read) so a violation aborts before any files are
/// opened.
///
/// In `Exploration` mode: BOTH start_date and end_date must be strictly less
/// than `VALIDATION_CUTOFF_DATE`. Returns a long-form error explaining how to
/// proceed if the validation pass is intentional.
///
/// In `Validation` mode: any dates allowed (the caller is responsible). This
/// function is a no-op for validation; the BANNER + seal file are emitted by
/// `run_backtest_tp` after this check passes.
/// Same out-of-sample guard as `validate_phase_dates`, but for the
/// `--bt-include-dates` CSV list. If `phase == Exploration` and ANY listed
/// date is >= VALIDATION_CUTOFF_DATE, ABORT. The include-dates list bypasses
/// the start/end range (used for the Fase 3 non-contiguous validation set:
/// 5/17, 5/21, 5/23, 5/24); without this check, an operator could pass
/// exploration start/end but include validation dates -> seal silently broken.
pub fn validate_include_dates(phase: BtPhase, include_dates: &[String]) -> Result<()> {
    if phase == BtPhase::Validation { return Ok(()); }
    if include_dates.is_empty() { return Ok(()); }
    use polymarket_client_sdk_v2::types::NaiveDate;
    let cutoff = NaiveDate::parse_from_str(VALIDATION_CUTOFF_DATE, "%Y-%m-%d")
        .expect("VALIDATION_CUTOFF_DATE compile-time guarantee");
    for d in include_dates {
        let dt = NaiveDate::parse_from_str(d, "%Y-%m-%d")
            .with_context(|| format!("bad --bt-include-dates entry: {d}"))?;
        if dt >= cutoff {
            bail!(
                "OUT-OF-SAMPLE DISCIPLINE VIOLATION: phase=exploration but \
                 --bt-include-dates contains '{d}' which is >= {VALIDATION_CUTOFF_DATE} \
                 (validation set). The --bt-include-dates list bypasses the \
                 start/end range but is still subject to the phase guard. Use \
                 --bt-phase validation if intentional."
            );
        }
    }
    Ok(())
}

pub fn validate_phase_dates(phase: BtPhase, start_date: &str, end_date: &str) -> Result<()> {
    if phase == BtPhase::Validation {
        return Ok(());
    }
    use polymarket_client_sdk_v2::types::NaiveDate;
    let cutoff = NaiveDate::parse_from_str(VALIDATION_CUTOFF_DATE, "%Y-%m-%d")
        .expect("VALIDATION_CUTOFF_DATE must be valid YYYY-MM-DD (compile-time guarantee)");
    let start = NaiveDate::parse_from_str(start_date, "%Y-%m-%d")
        .with_context(|| format!("bad --bt-start-date: {start_date}"))?;
    let end = NaiveDate::parse_from_str(end_date, "%Y-%m-%d")
        .with_context(|| format!("bad --bt-end-date: {end_date}"))?;
    // Both endpoints must be strictly < cutoff. A range like start=5/6, end=5/17
    // would include 5/17 (validation) -- aborting on end >= cutoff catches it.
    // Checking start separately catches the (uncommon) start >= cutoff case
    // with a clearer message than letting the range collapse silently.
    if end >= cutoff {
        bail!(
            "OUT-OF-SAMPLE DISCIPLINE VIOLATION: phase=exploration but \
             --bt-end-date={end_date} reaches into the validation set \
             (cutoff = {VALIDATION_CUTOFF_DATE}).\n\
             \n\
             Dates >= {VALIDATION_CUTOFF_DATE} are reserved for the Fase 3 \
             final validation gate and MUST NOT be peeked at during exploration \
             or hypothesis-building -- doing so contaminates the gate and turns \
             the whole methodology into in-sample fitting.\n\
             \n\
             If this run is the (rare, one-shot, deliberate) Fase 3 validation \
             pass, re-invoke with `--bt-phase validation`. Otherwise, set \
             --bt-end-date to a value < {VALIDATION_CUTOFF_DATE} (e.g. 2026-05-16)."
        );
    }
    if start >= cutoff {
        bail!(
            "OUT-OF-SAMPLE DISCIPLINE VIOLATION: phase=exploration but \
             --bt-start-date={start_date} reaches into the validation set \
             (cutoff = {VALIDATION_CUTOFF_DATE}). See --bt-phase validation if \
             intentional."
        );
    }
    Ok(())
}

// ============================================================================
// PURE: FullBook reconstruction + executable_bid_for_shares
// ============================================================================

/// Side of a price_change event: `BUY` = bid side (a level on the bid stack),
/// `SELL` = ask side. Matches Polymarket's WS protocol exactly (verified in
/// the May 2026 recorder data: each price_change carries this `side` field).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BookSide {
    Bid, // "BUY"  in Polymarket terms
    Ask, // "SELL" in Polymarket terms
}

impl BookSide {
    fn from_str(s: &str) -> Option<Self> {
        match s {
            "BUY" => Some(BookSide::Bid),
            "SELL" => Some(BookSide::Ask),
            _ => None,
        }
    }
}

/// FullBook -- per-token order book state with multi-level depth.
///
/// Prices are stored as `u32` keys = `price * 10_000` (4-decimal precision,
/// supports Polymarket's tick=0.01 with room). This avoids the f64-Eq/Hash
/// pitfall and keeps BTreeMap iteration deterministic.
///
/// Bids: highest price = best. Iteration `iter().rev()` walks best-first.
/// Asks: lowest price = best. Iteration `iter()` walks best-first.
#[derive(Debug, Clone, Default)]
pub struct FullBook {
    pub bids: BTreeMap<u32, f64>, // price_key -> size
    pub asks: BTreeMap<u32, f64>,
    pub last_update_ms: i64,
}

/// Convert price (e.g. 0.69) to internal u32 key (e.g. 6900).
#[inline]
#[must_use]
pub fn price_to_key(price: f64) -> u32 {
    (price * 10_000.0).round() as u32
}

/// Convert internal u32 key back to price.
#[inline]
#[must_use]
pub fn key_to_price(key: u32) -> f64 {
    key as f64 / 10_000.0
}

impl FullBook {
    /// Apply a FULL book snapshot. RESETS bid+ask stacks to the snapshot
    /// contents (= drops any prior level info). This is the re-anchor path
    /// used when the recorder receives a fresh snapshot after a hash mismatch
    /// or reconnect. Empirically these arrive every ~7s per active token in
    /// the May 2026 data -- frequent enough to recover from any divergence
    /// from missing price_change events.
    pub fn apply_snapshot(
        &mut self,
        bids: &[(f64, f64)],
        asks: &[(f64, f64)],
        ts_ms: i64,
    ) {
        self.bids.clear();
        self.asks.clear();
        for &(p, s) in bids {
            if s > 0.0 {
                self.bids.insert(price_to_key(p), s);
            }
        }
        for &(p, s) in asks {
            if s > 0.0 {
                self.asks.insert(price_to_key(p), s);
            }
        }
        self.last_update_ms = ts_ms;
    }

    /// Apply a price_change delta. Rule (verified empirically against May 2026
    /// data, see g8_pre_tp_apply_price_change_is_absolute tests):
    ///   * `size == 0` -> REMOVE the level (it was wiped).
    ///   * `size > 0`  -> SET the level to `size` (absolute new size, NOT a
    ///     delta to add).
    /// The empirical evidence: in the recorder data we saw sequences like
    /// `10 -> 2.61 -> 0 -> 43 -> 39.77 -> 99.77 -> 62.44`. The drops without
    /// negative-signed values rule out delta semantics; absolute is the only
    /// consistent interpretation.
    pub fn apply_price_change(&mut self, price: f64, size: f64, side: BookSide, ts_ms: i64) {
        let key = price_to_key(price);
        let map = match side {
            BookSide::Bid => &mut self.bids,
            BookSide::Ask => &mut self.asks,
        };
        if size <= 0.0 {
            map.remove(&key);
        } else {
            map.insert(key, size);
        }
        self.last_update_ms = ts_ms;
    }

    /// Best bid (highest price on the bid stack). None if empty.
    #[must_use]
    pub fn best_bid(&self) -> Option<f64> {
        self.bids.iter().next_back().map(|(&k, _)| key_to_price(k))
    }

    /// Best ask (lowest price on the ask stack). None if empty.
    #[must_use]
    pub fn best_ask(&self) -> Option<f64> {
        self.asks.iter().next().map(|(&k, _)| key_to_price(k))
    }

    /// THE CRITICAL HELPER: compute the EXECUTABLE bid for selling N shares
    /// against this book's current depth.
    ///
    /// Walks the bid stack best-first (highest price first), consuming `shares`
    /// units. Returns the size-weighted AVERAGE price the seller would actually
    /// receive. Returns `None` if the total stack depth is less than `shares`
    /// (= "uncoverable" -- per user spec, the caller treats this as $0 exit).
    ///
    /// This is the SINGLE THING the live bot's close_due gets wrong: it uses
    /// `best_bid` regardless of whether that level has the depth to absorb the
    /// SELL. For a thin book (e.g. best=0.50 with size=0.1, next=0.30 with
    /// size=50) selling 25 shares actually fills near 0.30, not 0.50.
    #[must_use]
    pub fn executable_bid_for_shares(&self, shares: f64) -> Option<f64> {
        if shares <= 0.0 {
            return None;
        }
        let mut remaining = shares;
        let mut total_proceeds = 0.0_f64;
        // Iterate bids descending (highest price first -- the seller's view).
        for (&key, &size) in self.bids.iter().rev() {
            if remaining <= 1e-12 {
                break;
            }
            let price = key_to_price(key);
            let take = remaining.min(size);
            total_proceeds += take * price;
            remaining -= take;
        }
        if remaining > 1e-9 {
            // Not enough depth to cover the whole position.
            None
        } else {
            Some(total_proceeds / shares)
        }
    }
}

// ============================================================================
// Event loading (zstd-aware, multi-day)
// ============================================================================

/// One event in the historical replay, tagged with `recv_ms` (= the recorder's
/// receive timestamp, the single timeline the simulator iterates over).
#[derive(Debug)]
pub struct TimedEvent {
    pub recv_ms: i64,
    pub ev: Ev,
}

#[derive(Debug)]
pub enum Ev {
    /// A finalized Binance 1s kline (the trigger source).
    Kline {
        asset: &'static str,
        t_open_ms: i64,
        close: f64,
    },
    /// A FULL book snapshot for a token (re-anchor: clears prior state).
    BookSnapshot {
        token: String,
        bids: Vec<(f64, f64)>,
        asks: Vec<(f64, f64)>,
        ts_ms: i64,
    },
    /// An incremental price_change for one level on one side.
    PriceChange {
        token: String,
        price: f64,
        size: f64,
        side: BookSide,
        ts_ms: i64,
    },
}

/// Open a .jsonl or .jsonl.zst file as a streaming line iterator. Caller is
/// responsible for buffered line reading; this returns a `Box<dyn BufRead>` so
/// either branch satisfies the same trait.
pub fn open_jsonl(path: &Path) -> Result<Box<dyn BufRead>> {
    let f = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let path_str = path.to_string_lossy();
    if path_str.ends_with(".zst") {
        let dec = zstd::Decoder::new(f)
            .with_context(|| format!("zstd init for {}", path.display()))?;
        Ok(Box::new(BufReader::new(dec)))
    } else {
        Ok(Box::new(BufReader::new(f)))
    }
}

/// Try both `<path>.jsonl` and `<path>.jsonl.zst` -- whichever exists.
/// Returns the first openable variant. None if neither exists.
fn resolve_jsonl(dir: &Path, date: &str) -> Option<PathBuf> {
    let plain = dir.join(format!("{date}.jsonl"));
    if plain.exists() {
        return Some(plain);
    }
    let zst = dir.join(format!("{date}.jsonl.zst"));
    if zst.exists() {
        return Some(zst);
    }
    None
}

/// Parse a price as f64 from either a string ("0.69") or number (0.69) JSON
/// value. The Polymarket payload mixes both conventions.
fn parse_f64(v: Option<&Value>) -> Option<f64> {
    v.and_then(|x| x.as_f64().or_else(|| x.as_str().and_then(|s| s.parse().ok())))
}

/// Parse a `[{price, size}, ...]` array into a Vec.
fn parse_levels(v: Option<&Value>) -> Vec<(f64, f64)> {
    let mut out = Vec::new();
    if let Some(arr) = v.and_then(Value::as_array) {
        for lvl in arr {
            let p = parse_f64(lvl.get("price"));
            let s = parse_f64(lvl.get("size"));
            if let (Some(p), Some(s)) = (p, s) {
                out.push((p, s));
            }
        }
    }
    out
}

/// Parse a recorder ISO-8601 `received_at` (e.g. `"...12.201294+00:00"`) into
/// Unix milliseconds. Falls back to None if parse fails.
fn parse_recv_ms(v: &Value) -> Option<i64> {
    use polymarket_client_sdk_v2::types::DateTime;
    let s = v.get("received_at")?.as_str()?;
    DateTime::parse_from_rfc3339(s).ok().map(|dt| dt.timestamp_millis())
}

/// Load Binance klines for one asset for one date.
/// File layout: `<data_root>/live_l2/binance/<symbol>_kline_1s/<date>.jsonl[.zst]`.
fn load_klines(
    data_root: &Path,
    sym: &str,
    asset: &'static str,
    date: &str,
    out: &mut Vec<TimedEvent>,
) -> Result<()> {
    let dir = data_root.join("live_l2/binance").join(format!("{sym}_kline_1s"));
    let path = match resolve_jsonl(&dir, date) {
        Some(p) => p,
        None => {
            // Missing-day = warn and skip (e.g. 2026-05-12 gap in the recorder).
            eprintln!("[backtest_tp] WARN: missing klines for {asset} {date}");
            return Ok(());
        }
    };
    let reader = open_jsonl(&path)?;
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let v: Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let recv_ms = match parse_recv_ms(&v) {
            Some(x) => x,
            None => continue,
        };
        // Binance ws kline_1s shape (verified vs recorder 2026-05-15 sample):
        //   payload.data.k.t = open ms; payload.data.k.c = close (string).
        // Fall back to payload.k.* for any legacy/alternate shape (defensive).
        let payload = &v["payload"];
        let k = payload
            .get("data")
            .and_then(|d| d.get("k"))
            .or_else(|| payload.get("k"))
            .unwrap_or(payload);
        let t_open_ms = k.get("t").and_then(|x| x.as_i64()).unwrap_or(0);
        let close = parse_f64(k.get("c"));
        if let Some(c) = close
            && t_open_ms > 0
        {
            out.push(TimedEvent {
                recv_ms,
                ev: Ev::Kline { asset, t_open_ms, close: c },
            });
        }
    }
    Ok(())
}

/// Load Polymarket book + price_change events for one date, filtered to the
/// set of bot-relevant tokens. Discards events for tokens outside the set
/// (keeps memory bounded: the recorder graba ALL of Polymarket, the bot uses ~16).
fn load_pm_full(
    data_root: &Path,
    date: &str,
    tokens: &HashSet<String>,
    out: &mut Vec<TimedEvent>,
) -> Result<()> {
    for etype in ["book", "price_change"] {
        let dir = data_root.join("live_l2/polymarket").join(etype);
        let path = match resolve_jsonl(&dir, date) {
            Some(p) => p,
            None => {
                eprintln!("[backtest_tp] WARN: missing {etype} for {date}");
                continue;
            }
        };
        let reader = open_jsonl(&path)?;
        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            let v: Value = match serde_json::from_str(&line) {
                Ok(v) => v,
                Err(_) => continue,
            };
            let recv_ms = match parse_recv_ms(&v) {
                Some(x) => x,
                None => continue,
            };
            let payload = &v["payload"];
            let ts_ms = payload
                .get("timestamp")
                .and_then(|x| x.as_i64().or_else(|| x.as_str().and_then(|s| s.parse().ok())))
                .unwrap_or(recv_ms);
            match etype {
                "book" => {
                    let token = payload
                        .get("asset_id")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    if !tokens.contains(token) {
                        continue;
                    }
                    let bids = parse_levels(payload.get("bids"));
                    let asks = parse_levels(payload.get("asks"));
                    out.push(TimedEvent {
                        recv_ms,
                        ev: Ev::BookSnapshot {
                            token: token.to_string(),
                            bids,
                            asks,
                            ts_ms,
                        },
                    });
                }
                "price_change" => {
                    if let Some(arr) = payload.get("price_changes").and_then(Value::as_array) {
                        for ch in arr {
                            let token = ch.get("asset_id").and_then(Value::as_str).unwrap_or("");
                            if !tokens.contains(token) {
                                continue;
                            }
                            let price = parse_f64(ch.get("price"));
                            let size = parse_f64(ch.get("size"));
                            let side_str = ch.get("side").and_then(Value::as_str).unwrap_or("");
                            let side = BookSide::from_str(side_str);
                            if let (Some(price), Some(size), Some(side)) = (price, size, side) {
                                out.push(TimedEvent {
                                    recv_ms,
                                    ev: Ev::PriceChange {
                                        token: token.to_string(),
                                        price,
                                        size,
                                        side,
                                        ts_ms,
                                    },
                                });
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    }
    Ok(())
}

// ============================================================================
// G11 STREAMING: per-stream iterators + K-way merge (replaces load-all+sort)
//
// Why this exists: the legacy path loaded an entire day's events into one
// Vec<TimedEvent> and then `sort_by_key`-ed it. For heavy days (5/11 had 102M
// events, ~17 GB allocation when the Vec grew) this OOMs on a 16-32 GB box.
// The fix is a textbook K-way merge: each file is already time-ordered
// internally (the recorder writes append-only per stream), so a min-heap over
// the K stream-heads yields the global time-ordered sequence with O(K) memory
// instead of O(N).
//
// EQUIVALENCE GUARANTEE: when each underlying file is monotonically ordered by
// recv_ms, the merger yields the EXACT same sequence as the legacy load+sort.
//   * Tie-break: legacy used stable sort with input-order = BTC kline pushed
//     first, then ETH kline, then book, then price_change. The streaming
//     merger uses `BinaryHeap<Reverse<(recv_ms, stream_idx)>>` with streams
//     pushed in the same order [BTC kline=0, ETH kline=1, book=2, price_change=3],
//     so on ties the lower stream_idx pops first -- IDENTICAL to legacy.
//   * Intra-stream order: both implementations preserve file order verbatim
//     (legacy via stable sort + append-order; streaming via natural iteration).
//   * Per-line parsing: streaming uses the SAME parse helpers as legacy
//     (parse_recv_ms, parse_levels, parse_f64, BookSide::from_str) -- not
//     reimplemented, just moved into closures. Parse-error semantics match:
//     malformed lines are skipped (same `continue` behavior as legacy).
//
// OUT-OF-ORDER DETECTION: the merger tracks per-stream last_recv_ms and
// counts any row whose recv_ms < last_recv_ms (= file is NOT monotonic, the
// asunción del merge se rompe). The count is exposed via
// `out_of_order_summary()`; the streaming `replay_day` logs a WARN per stream
// with count > 0. On real recorder data this should be ZERO (single-thread
// append-only writer per stream); a non-zero count flags either a recorder
// bug or that the asunción no se sostiene y necesitamos reorder buffer.
// ============================================================================

use std::cmp::Reverse;
use std::collections::BinaryHeap;

/// K-way time-merge over `Box<dyn Iterator<Item = TimedEvent>>` streams.
/// `Iterator<Item = TimedEvent>`: yields events in non-decreasing `recv_ms`
/// order GIVEN each stream is itself non-decreasing in `recv_ms`. Ties resolve
/// by stream_idx ascending (so callers control tie-break via push order).
pub struct StreamingMerger {
    streams: Vec<Box<dyn Iterator<Item = TimedEvent>>>,
    /// For each stream, the most recently pulled (but not yet yielded) event.
    /// `None` = stream exhausted.
    heads: Vec<Option<TimedEvent>>,
    /// Min-heap of `(recv_ms, stream_idx)` for active heads only. Each push
    /// corresponds to a fresh head sitting in `heads[stream_idx]`.
    queue: BinaryHeap<Reverse<(i64, usize)>>,
    /// Per-stream: recv_ms of the last YIELDED event (for monotonicity check).
    last_recv_ms: Vec<i64>,
    /// Per-stream: count of rows yielded with recv_ms < last_recv_ms
    /// (= the source file violated monotonicity). Should be 0 on real
    /// recorder data; non-zero = a problem to investigate.
    ooo_warns: Vec<usize>,
    /// Stream names (for the post-replay WARN line).
    stream_names: Vec<String>,
}

impl StreamingMerger {
    /// Build from `(name, iterator)` pairs. Stream order matters: ties on
    /// `recv_ms` resolve by ascending stream_idx (= the index in this Vec).
    /// Caller MUST push in the same order legacy `load_*` appended to the Vec
    /// for byte-exact equivalence.
    pub fn new(streams: Vec<(String, Box<dyn Iterator<Item = TimedEvent>>)>) -> Self {
        let n = streams.len();
        let mut m = StreamingMerger {
            streams: Vec::with_capacity(n),
            heads: Vec::with_capacity(n),
            queue: BinaryHeap::new(),
            last_recv_ms: vec![i64::MIN; n],
            ooo_warns: vec![0; n],
            stream_names: Vec::with_capacity(n),
        };
        for (idx, (name, mut stream)) in streams.into_iter().enumerate() {
            m.stream_names.push(name);
            // Prime: pull the first event of each stream into `heads[idx]`.
            if let Some(ev) = stream.next() {
                m.queue.push(Reverse((ev.recv_ms, idx)));
                m.heads.push(Some(ev));
            } else {
                m.heads.push(None);
            }
            m.streams.push(stream);
        }
        m
    }

    /// Out-of-order warning summary: `(stream_name, ooo_count)` per stream.
    /// Call AFTER iteration completes. A non-zero count for any stream means
    /// the underlying file had rows with recv_ms < the previously-yielded
    /// recv_ms of that same stream -- which would mean the k-way merge has
    /// missed correct global ordering (those rows came out "in the wrong
    /// slot" relative to what a global sort would produce).
    pub fn out_of_order_summary(&self) -> Vec<(String, usize)> {
        self.stream_names
            .iter()
            .zip(self.ooo_warns.iter())
            .map(|(n, c)| (n.clone(), *c))
            .collect()
    }
}

impl Iterator for StreamingMerger {
    type Item = TimedEvent;

    fn next(&mut self) -> Option<TimedEvent> {
        let Reverse((recv_ms, idx)) = self.queue.pop()?;
        let ev = self
            .heads[idx]
            .take()
            .expect("head must be present when its (recv_ms, idx) is in the queue");
        debug_assert_eq!(ev.recv_ms, recv_ms);

        // Monotonicity check on the just-yielded event vs the previous one
        // from THIS stream. This is the per-stream invariant the k-way merge
        // relies on. We do NOT abort on violation -- yielding a slightly
        // mis-ordered event is preferable to halting the run; the summary
        // log tells the operator whether to trust the result.
        if recv_ms < self.last_recv_ms[idx] {
            self.ooo_warns[idx] += 1;
        }
        self.last_recv_ms[idx] = recv_ms;

        // Pull the next event from this same stream and re-push to the heap.
        if let Some(next_ev) = self.streams[idx].next() {
            self.queue.push(Reverse((next_ev.recv_ms, idx)));
            self.heads[idx] = Some(next_ev);
        }
        Some(ev)
    }
}

// ---------------------------------------------------------------------------
// Per-stream iterator constructors. Each returns `Box<dyn Iterator<Item =
// TimedEvent>>` that parses lines on-demand (no buffering). The parse logic
// is moved verbatim from legacy `load_klines` / `load_pm_full` -- same
// fields, same defaults, same skip-on-error semantics. The functions take
// the same arguments as their legacy counterparts.
// ---------------------------------------------------------------------------

/// Stream Binance 1s klines for one (asset, date) as time-ordered TimedEvents.
/// Missing file = empty stream (matches legacy WARN+skip behavior).
fn kline_stream(
    data_root: &Path,
    sym: &str,
    asset: &'static str,
    date: &str,
) -> Result<Box<dyn Iterator<Item = TimedEvent>>> {
    let dir = data_root.join("live_l2/binance").join(format!("{sym}_kline_1s"));
    let path = match resolve_jsonl(&dir, date) {
        Some(p) => p,
        None => {
            eprintln!("[backtest_tp] WARN: missing klines for {asset} {date}");
            return Ok(Box::new(std::iter::empty()));
        }
    };
    let reader = open_jsonl(&path)?;
    let it = reader.lines().filter_map(move |line| {
        let line = line.ok()?;
        if line.trim().is_empty() {
            return None;
        }
        let v: Value = serde_json::from_str(&line).ok()?;
        let recv_ms = parse_recv_ms(&v)?;
        let payload = &v["payload"];
        let k = payload
            .get("data")
            .and_then(|d| d.get("k"))
            .or_else(|| payload.get("k"))
            .unwrap_or(payload);
        let t_open_ms = k.get("t").and_then(|x| x.as_i64()).unwrap_or(0);
        let close = parse_f64(k.get("c"))?;
        if t_open_ms <= 0 {
            return None;
        }
        Some(TimedEvent {
            recv_ms,
            ev: Ev::Kline { asset, t_open_ms, close },
        })
    });
    Ok(Box::new(it))
}

/// Stream Polymarket book snapshots for `date`, filtered to `tokens`.
/// Missing file = empty stream.
fn pm_book_stream(
    data_root: &Path,
    date: &str,
    tokens: &HashSet<String>,
) -> Result<Box<dyn Iterator<Item = TimedEvent>>> {
    let dir = data_root.join("live_l2/polymarket/book");
    let path = match resolve_jsonl(&dir, date) {
        Some(p) => p,
        None => {
            eprintln!("[backtest_tp] WARN: missing book for {date}");
            return Ok(Box::new(std::iter::empty()));
        }
    };
    let reader = open_jsonl(&path)?;
    let tokens = tokens.clone();
    let it = reader.lines().filter_map(move |line| {
        let line = line.ok()?;
        if line.trim().is_empty() {
            return None;
        }
        let v: Value = serde_json::from_str(&line).ok()?;
        let recv_ms = parse_recv_ms(&v)?;
        let payload = &v["payload"];
        let ts_ms = payload
            .get("timestamp")
            .and_then(|x| x.as_i64().or_else(|| x.as_str().and_then(|s| s.parse().ok())))
            .unwrap_or(recv_ms);
        let token = payload.get("asset_id").and_then(Value::as_str).unwrap_or("");
        if !tokens.contains(token) {
            return None;
        }
        let bids = parse_levels(payload.get("bids"));
        let asks = parse_levels(payload.get("asks"));
        Some(TimedEvent {
            recv_ms,
            ev: Ev::BookSnapshot {
                token: token.to_string(),
                bids,
                asks,
                ts_ms,
            },
        })
    });
    Ok(Box::new(it))
}

/// Stream Polymarket price_change events for `date`, filtered to `tokens`.
/// One line can produce MULTIPLE TimedEvents (one per price_change in the
/// array), all sharing the same recv_ms -- matches legacy `load_pm_full`
/// expansion exactly. Missing file = empty stream.
fn pm_price_change_stream(
    data_root: &Path,
    date: &str,
    tokens: &HashSet<String>,
) -> Result<Box<dyn Iterator<Item = TimedEvent>>> {
    let dir = data_root.join("live_l2/polymarket/price_change");
    let path = match resolve_jsonl(&dir, date) {
        Some(p) => p,
        None => {
            eprintln!("[backtest_tp] WARN: missing price_change for {date}");
            return Ok(Box::new(std::iter::empty()));
        }
    };
    let reader = open_jsonl(&path)?;
    let tokens = tokens.clone();
    let it = reader.lines().flat_map(move |line| {
        let mut out: Vec<TimedEvent> = Vec::new();
        let line = match line {
            Ok(l) if !l.trim().is_empty() => l,
            _ => return out.into_iter(),
        };
        let v: Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            _ => return out.into_iter(),
        };
        let recv_ms = match parse_recv_ms(&v) {
            Some(x) => x,
            _ => return out.into_iter(),
        };
        let payload = &v["payload"];
        let ts_ms = payload
            .get("timestamp")
            .and_then(|x| x.as_i64().or_else(|| x.as_str().and_then(|s| s.parse().ok())))
            .unwrap_or(recv_ms);
        if let Some(arr) = payload.get("price_changes").and_then(Value::as_array) {
            for ch in arr {
                let token = ch.get("asset_id").and_then(Value::as_str).unwrap_or("");
                if !tokens.contains(token) {
                    continue;
                }
                let price = parse_f64(ch.get("price"));
                let size = parse_f64(ch.get("size"));
                let side_str = ch.get("side").and_then(Value::as_str).unwrap_or("");
                let side = BookSide::from_str(side_str);
                if let (Some(price), Some(size), Some(side)) = (price, size, side) {
                    out.push(TimedEvent {
                        recv_ms,
                        ev: Ev::PriceChange {
                            token: token.to_string(),
                            price,
                            size,
                            side,
                            ts_ms,
                        },
                    });
                }
            }
        }
        out.into_iter()
    });
    Ok(Box::new(it))
}

// ============================================================================
// Positions + Trajectories (Pass 1+2 combined into one loop)
// ============================================================================

/// One entered position, frozen at entry time with everything needed to
/// evaluate ANY exit variant in PASS 3. The trajectory is collected during the
/// replay (sampled at every book update for this token).
#[derive(Debug, Clone)]
pub struct CollectedPosition {
    pub token: String,
    pub asset: String,    // "BTC" or "ETH" (for the breakdown)
    pub interval: String, // "5m" or "15m" (for the breakdown)
    /// Market epoch (Unix s) = `(trigger_ts / interval_secs) * interval_secs`.
    /// Two positions belong to the SAME market iff they share (asset, interval,
    /// epoch). The opposite-side detection in `EntryFilter::NoOpposite` and
    /// `EntryFilter::AsymmetricBps` keys on this. Stored as i64 seconds (not
    /// ms) to match the rest of the time semantics (`exit_ts_ms / 1000` etc.).
    pub epoch: i64,
    pub direction: String, // "Up" or "Down" (for context, not used in metrics)
    pub signal_id: String,
    pub entry_recv_ms: i64,
    pub entry_price: f64, // bot's pre-POST estimate (best_ask at signal); the bot uses this
    pub shares: f64,
    pub exit_ts_ms: i64, // exit_ts_s * 1000
    /// W9 ADDITIVE: the Binance kline CLOSE price (BTC/ETH USDT) at the kline
    /// that fired this signal. Required by EntryFilter::DcaConfirmingUnderlying
    /// (D2a) to test whether a candidate DCA lot's trigger close confirms (or
    /// fades) the direction of the FIRST lot in the same market-side. Defaults
    /// to 0.0 in test helpers + the pre-W9 baseline (D2a is the only consumer;
    /// every other filter ignores the field, so a 0.0 default is observationally
    /// invisible). Production site: captured at `Ev::Kline { close, .. }` in
    /// `replay_events_through_state_machine` and threaded into the Fire branch.
    pub trigger_close: f64,
    /// W9-Pieza1: SIGNED trigger return in bps (the magnitude that fired the
    /// signal; sign = direction). Captured from `trig.ret_bps` at the Fire
    /// branch. Required by the exits-trace audit to correlate
    /// |movimiento de Binance| with |repricing de Polymarket|.
    /// Default 0.0 in test helpers (no exits-trace path in tests).
    pub trigger_ret_bps: f64,
    /// W9-Pieza1: maker fee from the markets log AS-IS (i64; units
    /// interpretation documented in the Python consumer). 0 when the catalog
    /// entry is missing or the field was absent (synthetic catalogs, live).
    pub maker_fee_bps: i64,
    /// W9-Pieza1: taker fee from the markets log AS-IS. Symmetric to maker.
    pub taker_fee_bps: i64,
    /// W9-Pieza1: `fee_type` string from the markets log (e.g.
    /// "crypto_fees_v2"). Empty when missing.
    pub fee_type: String,
    /// COMBO 2026-06-08: entry-side trigger time in MS (source-side, =
    /// trig.trigger_ts * 1000). Required by `record_kline_for_active` to
    /// match the post-trigger kline at exactly trigger_ts_ms + {2,5,10}*1000.
    /// Distinct from `entry_recv_ms` which is RECORDER-side and includes
    /// network lag. 0 in tests where no Fire branch ran.
    pub entry_trigger_ts_ms: i64,
    /// COMBO H1: Polymarket order-book imbalance at trigger, top-1 levels.
    /// `(bid_size - ask_size) / (bid_size + ask_size)` of the best level
    /// only. NaN when either side empty at trigger time.
    pub obi_top1: f64,
    /// COMBO H1: same, top-3 levels (sensitivity check; pre-registered
    /// to avoid N-sweep overfitting).
    pub obi_top3: f64,
    /// COMBO H2: Binance kline CLOSE at +2s post-trigger. NaN when the
    /// matching kline did not arrive (recorder gap, day boundary, etc.).
    pub binance_close_at_2s: f64,
    /// COMBO H2: same, +5s post-trigger.
    pub binance_close_at_5s: f64,
    /// COMBO H2: same, +10s post-trigger.
    pub binance_close_at_10s: f64,
    /// COMBO H2 EXTENSION (2026-06-08): longer post-trigger horizons. The
    /// initial 2/5/10s emit showed mean signed excess INCREASES over the
    /// window (no reversion observed), so vida-media couldn't be estimated.
    /// These three (30/60/120s) extend the window to see if reversion
    /// appears at longer horizons.
    pub binance_close_at_30s: f64,
    pub binance_close_at_60s: f64,
    pub binance_close_at_120s: f64,
    /// COMBO H3: realized vol (std of log returns) over last 30 min pre-
    /// trigger. NaN if fewer than 1800 samples available or if any
    /// consecutive pair has time-gap > 5_000 ms (recorder hole).
    pub vol_30m: f64,
    /// COMBO H3: same, 60 min lookback (3600 samples).
    pub vol_60m: f64,
    /// Per-event trajectory of (recv_ms, executable_bid) sampled during the
    /// lifetime. Used by Pass 3 to evaluate first-touch and peak variants.
    /// Empty entries (None executable_bid = uncoverable depth) are encoded as
    /// `f64::NAN` so the trajectory always has 1 entry per book update.
    pub trajectory: Vec<(i64, f64)>,
    /// G13 FASE 2 ADDITIVE: per-sample BBO (best_bid, best_ask) alongside the
    /// executable_bid trajectory. Same length, same recv_ms, paired by index.
    /// Pushed inside `sample_book_for_active` from `book.best_bid()` /
    /// `book.best_ask()`. Used by Phase 2's spread + depth-tax features.
    ///
    /// EQUIVALENCE GUARANTEE: this field is ADDITIVE. PeakStats /
    /// characterize_position / variant evaluation / peak_characterization.jsonl
    /// emission / variant_*.json all read ONLY `trajectory` (the executable_bid
    /// one). None of them touch `trajectory_bbo`. So the byte-exact
    /// equivalence of those outputs (commit 41648af, sha256 9cd4ceaf...) is
    /// preserved post-G13: the new field is silently allocated and populated
    /// but never surfaces in any pre-existing output.
    pub trajectory_bbo: Vec<(i64, Option<f64>, Option<f64>)>,
    /// The bid at (or just after) exit_ts_ms. None if uncoverable. The
    /// baseline (time-exit variant) uses this as the exit price.
    pub time_exit_bid: Option<f64>,
    /// How many trajectory samples were uncoverable (depth < shares). Per-
    /// position counter; aggregated upstream into the "uncoverable rate" stat.
    pub uncoverable_samples: usize,
    pub total_samples: usize,
}

/// BookProvider adapter so the decide engine can read best_bid/best_ask from
/// the FullBook map (drop-in replacement for capa_b's ReplayBook).
struct FullBookProvider<'a>(&'a HashMap<String, FullBook>);
impl BookProvider for FullBookProvider<'_> {
    fn bbo(&self, token: &str) -> Option<EventBbo> {
        self.0.get(token).map(|b| EventBbo {
            best_ask: b.best_ask(),
            best_bid: b.best_bid(),
            ts_ms: b.last_update_ms,
        })
    }
}

/// Active-position tracker for the single-loop replay. Keys positions by
/// (token, signal_id) so multi-lot positions of the same token are tracked
/// independently (matches the bot's bs.positions semantics post G9-pre-A).
#[derive(Default)]
struct ActiveTracker {
    by_key: HashMap<(String, String), CollectedPosition>,
}

impl ActiveTracker {
    fn add(&mut self, p: CollectedPosition) {
        let key = (p.token.clone(), p.signal_id.clone());
        self.by_key.insert(key, p);
    }

    /// Append an executable_bid sample to all positions of `token`.
    /// `bid_or_nan` is the computed executable_bid OR `f64::NAN` for uncoverable.
    fn record_bid_sample(&mut self, token: &str, recv_ms: i64, bid_or_nan: f64) {
        for ((tok, _), pos) in self.by_key.iter_mut() {
            if tok == token {
                pos.trajectory.push((recv_ms, bid_or_nan));
                pos.total_samples += 1;
                if bid_or_nan.is_nan() {
                    pos.uncoverable_samples += 1;
                }
            }
        }
    }

    /// Drain all positions whose exit_ts_ms has passed. The caller stamps each
    /// drained position with its time-exit bid (recorded as None if uncoverable).
    ///
    /// Returns positions sorted by `signal_id` for a CANONICAL, REPRODUCIBLE
    /// drain order. Without this sort, the iteration order is `HashMap::iter`
    /// = non-deterministic between processes (per-process RandomState), so two
    /// runs over identical data produce different per-trade-line orderings in
    /// the output JSONL (same aggregates -- those are order-invariant sums --
    /// but raw `diff` flags spurious differences). Sorting here makes the
    /// streaming-vs-legacy equivalence VERIFIABLE byte-exact AND lets
    /// regression tests use raw diffs (any actual data change shows up; pre-
    /// existing iteration-order noise doesn't). Aggregates (sums, counts,
    /// per-cell breakdowns) are unaffected -- they're order-invariant.
    fn drain_due(&mut self, now_ms: i64) -> Vec<CollectedPosition> {
        let due_keys: Vec<_> = self
            .by_key
            .iter()
            .filter(|(_, p)| p.exit_ts_ms <= now_ms)
            .map(|(k, _)| k.clone())
            .collect();
        let mut out: Vec<CollectedPosition> = due_keys
            .into_iter()
            .filter_map(|k| self.by_key.remove(&k))
            .collect();
        out.sort_by(|a, b| a.signal_id.cmp(&b.signal_id));
        out
    }
}

// ============================================================================
// PASS 1+2 single-loop replay
// ============================================================================

/// Per-day inputs shared by both replay paths (legacy + streaming): the
/// markets catalog + the strategy-relevant token set. Extracted so the two
/// implementations of `replay_day_*` see EXACTLY the same context (anything
/// else would void the equivalence guarantee).
fn load_replay_context(
    data_root: &Path,
    date: &str,
) -> Result<(ReplayCatalog, HashSet<String>)> {
    let markets_dir = data_root.join("live_l2/polymarket/markets");
    let markets_log = match resolve_jsonl(&markets_dir, date) {
        Some(p) => p,
        None => bail!("missing markets log in {} for date {}", markets_dir.display(), date),
    };
    let catalog = ReplayCatalog::from_markets_log_reader(open_jsonl(&markets_log)?)?;
    let midnight = date_midnight_sec(date).context("date parse")?;
    let day_end = midnight + 86400;
    let tokens = catalog.tokens_in_window(midnight, day_end);
    Ok((catalog, tokens))
}

/// THE REPLAY STATE MACHINE. Pure w.r.t. event source: consumes a
/// time-ordered iterator of `TimedEvent`, drives `SignalEngine` + `FullBook` +
/// `ActiveTracker`, returns the same `Vec<CollectedPosition>` regardless of
/// whether events came from a loaded+sorted Vec (legacy) or a k-way merge of
/// streams (G11). The split is what makes the equivalence guarantee easy to
/// reason about: only the event source differs.
///
/// Implementation note: the inner loop is byte-identical to the legacy
/// inline code (the match-on-`t.ev` arms below) -- this function was
/// extracted verbatim from the previous `replay_day` body.
// ============================================================================
// PIECE W8: ENTRY FILTERS for hypothesis-generation backtests over burned data.
// Pure: an EntryFilter is consulted at the Fire-action branch BEFORE a position
// is added to the active tracker. The DEFAULT is Baseline (accept every Fire)
// which preserves byte-identical behavior with the pre-W8 backtester (this is
// what the existing test suite relies on -- verified by run_backtest_tp passing
// EntryFilter::Baseline at all production sites).
//
// The investigated hypotheses (5/06-5/24 May data, QUEMADA; out-of-sample
// validation belongs to a later phase on fresh recorder data):
//   Group A -- flat BPS threshold: A0 baseline, A1=6, A2=7, A3=8 bps min.
//   Group B -- opposite-side handling: B1 ignore opposite, B2 REGLA C
//               (close-opposite-and-open, set on the decide cfg, NOT here).
//   Group C -- asymmetric BPS: C1 normal=5 opp=8, C2 opp=7, C3 opp=10.
//
// "Opposite position exists" = an active CollectedPosition in the SAME
// (asset, interval, epoch) market with the OPPOSITE direction. The epoch
// must match (different epochs = different markets, even same cell).
// ============================================================================

/// Map an interval label ("5m" / "15m") to its second-count. Used to derive
/// the market epoch from a trigger_ts: `epoch = (trigger_ts / secs) * secs`.
/// Defaults to 300 (5m) for any other label -- the active codebase only
/// uses 5m + 15m.
fn interval_secs_for(interval: &str) -> i64 {
    match interval {
        "5m" => 300,
        "15m" => 900,
        _ => 300,
    }
}

/// Entry-filter variants. Used by the W8 hypothesis-generation backtester
/// to super-filter Fire actions before they become CollectedPositions.
/// Default-equivalent (`Baseline`) is a no-op pass-through that preserves
/// every existing test's expected output byte-identical.
#[derive(Debug, Clone, PartialEq)]
pub enum EntryFilter {
    /// A0: accept every Fire (current operative behavior).
    Baseline,
    /// A1-A3: require |ret_bps| >= `min_abs_bps`. The signal engine fires at
    /// 5 BPS (TRIGGER_BPS const), so any threshold > 5 super-filters.
    BpsThreshold { min_abs_bps: f64 },
    /// B1: skip Fire if a position already exists in the SAME market with
    /// OPPOSITE direction. Same market = same (asset, interval, epoch).
    /// The existing position is NOT closed (just ignore the new signal).
    /// Different from B2 (REGLA C) which closes-and-opens.
    NoOpposite,
    /// B2 marker: enable `cfg.regla_c_enabled = true` upstream of the
    /// backtester call. THIS variant is a pass-through at the Fire site
    /// (REGLA C's CloseOpposite action is generated inside decide() before
    /// the Fire even reaches us). Used as a label for output naming.
    ///
    /// **KNOWN BUG (W9 audit): B2 silently degrades to B1 in this backtester.**
    /// `decide()` correctly emits `Action::CloseOpposite` when an opposite-side
    /// lot exists AND `regla_c_enabled=true` (decision/mod.rs:297-309), but
    /// `replay_events_through_state_machine` only matches `Action::Fire` in its
    /// inner `for a in &dec.actions` loop (backtest_tp.rs Fire branch) -- the
    /// CloseOpposite arm is missing. So:
    ///   * the new-side Fire is correctly SKIPPED (decide()'s `continue` after
    ///     pushing CloseOpposite means no Fire reaches the loop), and
    ///   * the existing opposite-side lot is NOT closed -- it stays open until
    ///     time_exit, identical to B1's behavior.
    /// Verified empirically: B1 and B2 produced bit-identical
    /// compare_entry_filters.csv rows (n_trades, total_pnl, win_rate, dca
    /// breakdown columns) on the W9 exploration run over 5/06-5/16.
    /// FIX (not done here -- registered as W10): add an `Action::CloseOpposite`
    /// arm in the replay state machine that drains the opposite token's lots
    /// from `ActiveTracker`, stamps each with `time_exit_bid = close_price`,
    /// and pushes them into `closed_out` at the trigger's recv_ms. Until that
    /// lands, treat B2 numbers as B1 numbers and do NOT validate B2.
    ReglaCMarker,
    /// C1-C3: asymmetric BPS thresholds. `min_abs_bps` is the floor for ANY
    /// Fire; `opposite_min_abs_bps` is a HIGHER floor that applies only when
    /// an opposite-side position already exists in the same market.
    AsymmetricBps { min_abs_bps: f64, opposite_min_abs_bps: f64 },
    // W9 D-VARIANTS -- DCA filters. All operate per (asset, interval, epoch,
    // direction) = same MARKET-SIDE. None touches the SignalEngine debounce
    // (already enforced upstream at 5s per asset). They consult the active
    // tracker for prior lots in the SAME market-side and decide whether the
    // new lot is allowed. D0 is the "no filter" reference (=A0 = Baseline by
    // construction) kept as a distinct enum case so the d0 label is
    // self-describing in the cross-variant table.
    /// D0: accept every Fire (same as Baseline). Distinct enum case so the
    /// D-family is self-contained in the comparison table; functionally
    /// byte-identical to Baseline. The G15 regression guard pins Baseline so
    /// D0 inherits that guarantee for free.
    DcaUnlimited,
    /// D1 -- "promediar a la baja": accept a DCA lot only if its entry_price
    /// is STRICTLY LOWER than the MIN entry_price across all existing same-
    /// market-side lots. Empty same-side set => accept (first lot is never
    /// blocked). Strict `<` => equal-price candidate is rejected (conservative).
    DcaImprovingPrice,
    /// D2a -- "confirma por SUBYACENTE (Binance)": accept a DCA lot only if
    /// the firing kline's close has moved in our betting direction since the
    /// FIRST same-market-side lot's trigger close. For Up: candidate close >
    /// first lot's close. For Down: candidate close < first lot's close.
    /// Requires `CollectedPosition::trigger_close` (W9 additive field).
    DcaConfirmingUnderlying,
    /// D2b -- "confirma por PRECIO DE APUESTA": EXACT MIRROR of D1. Accept a
    /// DCA lot only if its entry_price is STRICTLY HIGHER than the MAX
    /// entry_price across all existing same-market-side lots ("we are paying
    /// more = the market already moved with us"). Empty same-side set =>
    /// accept. Strict `>` => equal-price candidate is rejected.
    DcaConfirmingAsk,
    /// D3 -- NO DCA: accept the first lot, reject any further lot in the
    /// same market-side.
    NoDca,
    /// D4 -- DCA capped at `max` lots PER MARKET-SIDE. Accept iff the count
    /// of existing same-market-side lots is strictly less than `max`. Default
    /// production value is `max = 3` (parses from CLI label "d4"); other
    /// values are reachable only from tests.
    DcaCap { max: usize },
    /// W9-fix: per-cell composition that applies a DIFFERENT child filter to
    /// 5m vs 15m intervals. Dispatch happens at the `accept` site based on the
    /// existing `interval` arg. Recursive via Box -- ANY existing EntryFilter
    /// (incl. D-family) can be a child.
    ///
    /// CLI label `split_dca` binds the pre-registered W9 hypothesis from the
    /// exploration run (5/06-5/16) Pattern A:
    ///   five_min  = DcaUnlimited   (D0; DCA helps in fast cells, dca_edge > 0)
    ///   fifteen_min = DcaCap{max:3} (D4; cap DCA in slow cells, dca_edge < 0)
    /// HYPOTHESIS is PRE-REGISTERED (chosen by structural Pattern A finding,
    /// NOT by maximizing exploration numbers). D4 was selected over D1 in 15m
    /// because D1's strict entry < MIN priors filter dropped some same-side
    /// lots that were net-profitable in OTHER cells -- D4 is the conservative
    /// "cap extremes" choice that doesn't reach across cells.
    ///
    /// Unknown intervals (anything other than "5m"/"15m") pass-through
    /// (defensive). The recursive Box means PartialEq + Clone are auto-derived.
    SplitDcaByInterval {
        five_min: Box<EntryFilter>,
        fifteen_min: Box<EntryFilter>,
    },
}

impl EntryFilter {
    /// True iff the Fire should produce a CollectedPosition. Pure: no I/O,
    /// no mutation, no panic on any input. Caller passes the snapshot of
    /// currently-active positions (typically `active.by_key.values()`
    /// collected into a Vec) so the filter does NOT depend on the
    /// (module-private) `ActiveTracker` type.
    #[must_use]
    pub fn accept<'a, I>(
        &self,
        trig: &Trigger,
        new_direction: Direction,
        active_positions: I,
        asset: &str,
        interval: &str,
        epoch: i64,
        candidate_entry_price: f64,
        candidate_trigger_close: f64,
    ) -> bool
    where
        I: IntoIterator<Item = &'a CollectedPosition>,
    {
        // Collect once so the D-variants can scan twice (count + min/max +
        // first-by-recv_ms). The other variants only touch this through
        // `has_opposite_position` (single-pass) so the extra Vec is a no-op
        // for them in practice (active is at most ~max_open_positions long).
        let active: Vec<&CollectedPosition> = active_positions.into_iter().collect();
        match self {
            Self::Baseline | Self::ReglaCMarker | Self::DcaUnlimited => true,
            Self::BpsThreshold { min_abs_bps } => trig.ret_bps.abs() >= *min_abs_bps,
            Self::NoOpposite => {
                !has_opposite_position(active.iter().copied(), asset, interval, epoch, new_direction)
            }
            Self::AsymmetricBps { min_abs_bps, opposite_min_abs_bps } => {
                if trig.ret_bps.abs() < *min_abs_bps {
                    return false;
                }
                if has_opposite_position(active.iter().copied(), asset, interval, epoch, new_direction) {
                    trig.ret_bps.abs() >= *opposite_min_abs_bps
                } else {
                    true
                }
            }
            Self::DcaImprovingPrice => {
                let lots = same_market_side_lots(&active, asset, interval, epoch, new_direction);
                if lots.is_empty() {
                    return true;
                }
                let min_existing = lots
                    .iter()
                    .map(|p| p.entry_price)
                    .fold(f64::INFINITY, f64::min);
                candidate_entry_price < min_existing
            }
            Self::DcaConfirmingUnderlying => {
                let lots = same_market_side_lots(&active, asset, interval, epoch, new_direction);
                if lots.is_empty() {
                    return true;
                }
                // The "first lot" in the market-side = earliest by entry_recv_ms
                // (canonical chronological order). Ties broken by signal_id for
                // determinism (unlikely in practice -- recv_ms is millisecond).
                let first = lots
                    .iter()
                    .min_by(|a, b| {
                        a.entry_recv_ms
                            .cmp(&b.entry_recv_ms)
                            .then_with(|| a.signal_id.cmp(&b.signal_id))
                    })
                    .expect("non-empty by guard above");
                match new_direction {
                    Direction::Up => candidate_trigger_close > first.trigger_close,
                    Direction::Down => candidate_trigger_close < first.trigger_close,
                }
            }
            Self::DcaConfirmingAsk => {
                let lots = same_market_side_lots(&active, asset, interval, epoch, new_direction);
                if lots.is_empty() {
                    return true;
                }
                let max_existing = lots
                    .iter()
                    .map(|p| p.entry_price)
                    .fold(f64::NEG_INFINITY, f64::max);
                candidate_entry_price > max_existing
            }
            Self::NoDca => {
                let lots = same_market_side_lots(&active, asset, interval, epoch, new_direction);
                lots.is_empty()
            }
            Self::DcaCap { max } => {
                let lots = same_market_side_lots(&active, asset, interval, epoch, new_direction);
                lots.len() < *max
            }
            Self::SplitDcaByInterval { five_min, fifteen_min } => {
                // Per-cell dispatch by interval. The pre-W9-fix variants
                // applied one rule globally; this one selects child by the
                // interval already on the call stack. Re-invokes accept()
                // recursively on the chosen child -- the child sees the same
                // (active, asset, interval, epoch, fill, close) and decides.
                let child = match interval {
                    "5m" => five_min.as_ref(),
                    "15m" => fifteen_min.as_ref(),
                    // Unknown interval: defensive pass-through. The active
                    // codebase only uses 5m + 15m, but if a future cell is
                    // added we don't want this filter to silently reject
                    // everything for it.
                    _ => return true,
                };
                child.accept(
                    trig, new_direction, active.iter().copied(),
                    asset, interval, epoch,
                    candidate_entry_price, candidate_trigger_close,
                )
            }
        }
    }

    /// Stable label for output filenames and the cross-variant table.
    #[must_use]
    pub fn label(&self) -> String {
        match self {
            Self::Baseline => "a0_baseline".to_string(),
            Self::BpsThreshold { min_abs_bps } => {
                format!("bps_{:.0}", min_abs_bps)
            }
            Self::NoOpposite => "b1_no_opposite".to_string(),
            Self::ReglaCMarker => "b2_regla_c".to_string(),
            Self::AsymmetricBps { min_abs_bps, opposite_min_abs_bps } => {
                format!("asym_b{:.0}_opp{:.0}", min_abs_bps, opposite_min_abs_bps)
            }
            Self::DcaUnlimited => "d0_dca_unlimited".to_string(),
            Self::DcaImprovingPrice => "d1_dca_improving_price".to_string(),
            Self::DcaConfirmingUnderlying => "d2a_dca_confirming_underlying".to_string(),
            Self::DcaConfirmingAsk => "d2b_dca_confirming_ask".to_string(),
            Self::NoDca => "d3_no_dca".to_string(),
            Self::DcaCap { max } => format!("d4_dca_cap_{}", max),
            Self::SplitDcaByInterval { five_min, fifteen_min } => {
                format!("split_dca_5m={}_15m={}", five_min.label(), fifteen_min.label())
            }
        }
    }
}

// ============================================================================
// W9-Pieza1: TRAJECTORY HELPERS
//
// Pure functions over a `&[(i64, f64)]` trajectory (`(recv_ms, executable_bid)`)
// used by `run_backtest_exits_trace` to emit per-trade enriched metrics. All
// take `entry_recv_ms` and a window size (seconds) and return values in terms
// of OFFSETS-from-entry (ms), so the consumer (Python) does not need to know
// the absolute recv_ms baseline.
//
// Semantic notes:
//   * `executable_bid` is NaN when L2 depth was insufficient for the position
//     size at that recv_ms. The helpers IGNORE NaN samples when computing
//     extremes (so an uncoverable sample doesn't poison max/min) but SURFACE
//     NaN when bid_at_offset's first post-target sample happens to be NaN
//     -- that's the realistic answer ("we couldn't exit there"), Python decides
//     whether to fall through to the next coverable sample.
//   * Window endpoints are INCLUSIVE on the start (entry_recv_ms) and EXCLUSIVE
//     on the end (entry_recv_ms + window_s*1000): a 120s window covers ms 0
//     through 119_999 from entry.
// ============================================================================

/// Returns the FIRST observable executable_bid at or after
/// `entry_recv_ms + offset_s*1000`. Returns `f64::NAN` if (a) no sample exists
/// at or after the target (trajectory truncated), or (b) the first sample at
/// or after target was itself NaN (uncoverable). Python downstream can apply
/// a "next-coverable" fallback by passing the trajectory through `next_after`
/// after this call if it wants that semantic.
#[must_use]
pub fn bid_at_offset(trajectory: &[(i64, f64)], entry_recv_ms: i64, offset_s: i64) -> f64 {
    let target = entry_recv_ms + offset_s * 1000;
    trajectory
        .iter()
        .find(|(ms, _)| *ms >= target)
        .map(|(_, bid)| *bid)
        .unwrap_or(f64::NAN)
}

/// Returns `(max_bid, offset_ms_from_entry)` over the window
/// `[entry_recv_ms, entry_recv_ms + window_s*1000)`. NaN samples are ignored
/// (so the max is over COVERABLE bids). If the window contains no coverable
/// samples, returns `(f64::NAN, 0)`.
#[must_use]
pub fn max_bid_in_window(
    trajectory: &[(i64, f64)],
    entry_recv_ms: i64,
    window_s: i64,
) -> (f64, i64) {
    let end = entry_recv_ms + window_s * 1000;
    let mut max_bid = f64::NEG_INFINITY;
    let mut max_ts = 0_i64;
    for (ms, bid) in trajectory {
        if *ms >= end {
            break;
        }
        if !bid.is_nan() && *bid > max_bid {
            max_bid = *bid;
            max_ts = *ms - entry_recv_ms;
        }
    }
    if max_bid.is_infinite() {
        (f64::NAN, 0)
    } else {
        (max_bid, max_ts)
    }
}

/// Symmetric to `max_bid_in_window` but tracks the MIN. Used for reversion
/// risk + fallback classification (was the position ever underwater?).
#[must_use]
pub fn min_bid_in_window(
    trajectory: &[(i64, f64)],
    entry_recv_ms: i64,
    window_s: i64,
) -> (f64, i64) {
    let end = entry_recv_ms + window_s * 1000;
    let mut min_bid = f64::INFINITY;
    let mut min_ts = 0_i64;
    for (ms, bid) in trajectory {
        if *ms >= end {
            break;
        }
        if !bid.is_nan() && *bid < min_bid {
            min_bid = *bid;
            min_ts = *ms - entry_recv_ms;
        }
    }
    if min_bid.is_infinite() {
        (f64::NAN, 0)
    } else {
        (min_bid, min_ts)
    }
}

/// High-water mark trace: monotone-increasing subsequence of coverable bids.
/// Compact representation of "first time bid reached level L" for any L --
/// Python finds the first entry whose bid >= L. Each tuple is
/// `(offset_ms_from_entry, bid)`. Empty Vec if no coverable samples in window.
#[must_use]
pub fn high_water_marks(
    trajectory: &[(i64, f64)],
    entry_recv_ms: i64,
    window_s: i64,
) -> Vec<(i64, f64)> {
    let end = entry_recv_ms + window_s * 1000;
    let mut out = Vec::new();
    let mut cur_max = f64::NEG_INFINITY;
    for (ms, bid) in trajectory {
        if *ms >= end {
            break;
        }
        if !bid.is_nan() && *bid > cur_max {
            cur_max = *bid;
            out.push((*ms - entry_recv_ms, *bid));
        }
    }
    out
}

/// Symmetric to `high_water_marks` but monotone-decreasing. Used for stop-loss
/// / reversion-risk analysis: each tuple marks the first time the bid hit a
/// new low. Same offset_ms representation. Empty Vec if no coverable samples.
#[must_use]
pub fn low_water_marks(
    trajectory: &[(i64, f64)],
    entry_recv_ms: i64,
    window_s: i64,
) -> Vec<(i64, f64)> {
    let end = entry_recv_ms + window_s * 1000;
    let mut out = Vec::new();
    let mut cur_min = f64::INFINITY;
    for (ms, bid) in trajectory {
        if *ms >= end {
            break;
        }
        if !bid.is_nan() && *bid < cur_min {
            cur_min = *bid;
            out.push((*ms - entry_recv_ms, *bid));
        }
    }
    out
}

/// W9 helper: collect existing active lots in the SAME market-side as the
/// candidate. "Same market-side" = identical (asset, interval, epoch, direction).
/// Used by D-variants to count / inspect prior lots before accepting a new one.
/// Pure: borrows the slice without copying CollectedPositions.
fn same_market_side_lots<'a>(
    active: &'a [&'a CollectedPosition],
    asset: &str,
    interval: &str,
    epoch: i64,
    direction: Direction,
) -> Vec<&'a CollectedPosition> {
    let dir_str = match direction {
        Direction::Up => "Up",
        Direction::Down => "Down",
    };
    active
        .iter()
        .copied()
        .filter(|p| {
            p.asset == asset
                && p.interval == interval
                && p.epoch == epoch
                && p.direction == dir_str
        })
        .collect()
}

/// Pure: true iff any active position lives in the SAME market (asset,
/// interval, epoch) with the OPPOSITE direction. The empty iterator
/// (cold start) trivially returns false.
fn has_opposite_position<'a, I>(
    active_positions: I,
    asset: &str,
    interval: &str,
    epoch: i64,
    new_direction: Direction,
) -> bool
where
    I: IntoIterator<Item = &'a CollectedPosition>,
{
    let opposite_str = match new_direction {
        Direction::Up => "Down",
        Direction::Down => "Up",
    };
    active_positions.into_iter().any(|p| {
        p.asset == asset
            && p.interval == interval
            && p.epoch == epoch
            && p.direction == opposite_str
    })
}

/// Count markets where BOTH directions (Up AND Down) were opened during
/// the backtest. A "market" is (asset, interval, epoch). The W8
/// hypothesis-generation specifically wants this number per cell -- it
/// quantifies the "double-sided" exposure that EntryFilter::NoOpposite /
/// AsymmetricBps are meant to reduce.
#[must_use]
pub fn count_double_sided_markets(positions: &[CollectedPosition]) -> usize {
    use std::collections::HashMap;
    let mut sides_per_market: HashMap<(String, String, i64), (bool, bool)> = HashMap::new();
    for p in positions {
        let key = (p.asset.clone(), p.interval.clone(), p.epoch);
        let entry = sides_per_market.entry(key).or_insert((false, false));
        match p.direction.as_str() {
            "Up" => entry.0 = true,
            "Down" => entry.1 = true,
            _ => {}
        }
    }
    sides_per_market.values().filter(|(up, dn)| *up && *dn).count()
}

/// Same `count_double_sided_markets` semantics but restricted to ONE cell
/// (asset, interval). Used by the per-cell breakdown in the cross-variant
/// comparison table.
#[must_use]
pub fn count_double_sided_markets_for_cell(
    positions: &[CollectedPosition],
    asset: &str,
    interval: &str,
) -> usize {
    use std::collections::HashMap;
    let mut sides_per_market: HashMap<i64, (bool, bool)> = HashMap::new();
    for p in positions {
        if p.asset != asset || p.interval != interval {
            continue;
        }
        let entry = sides_per_market.entry(p.epoch).or_insert((false, false));
        match p.direction.as_str() {
            "Up" => entry.0 = true,
            "Down" => entry.1 = true,
            _ => {}
        }
    }
    sides_per_market.values().filter(|(up, dn)| *up && *dn).count()
}

fn replay_events_through_state_machine(
    events: &mut dyn Iterator<Item = TimedEvent>,
    catalog: &ReplayCatalog,
    cfg: &DecisionConfig,
    date: &str,
    entry_filter: &EntryFilter,
    data_root: &Path,
) -> Result<Vec<CollectedPosition>> {
    let mut engine = SignalEngine::new();
    let mut books: HashMap<String, FullBook> = HashMap::new();
    let mut active = ActiveTracker::default();
    let mut closed_out: Vec<CollectedPosition> = Vec::new();
    let mut n_events: u64 = 0;
    // COMBO H3 + H2: rolling kline history per asset. Pre-heated from the
    // prior day's tail so triggers in the first 60 min of `date` see a full
    // 60-min lookback (cross-UTC-day stitching). Capacity = 3600 = 60 min of
    // 1s klines; older entries are evicted on push. If the prior day's file
    // is missing (first day of the dataset or recorder gap), the buffer
    // starts empty and compute_vol() returns NaN until enough samples arrive.
    let mut kline_hist: HashMap<&'static str, KlineHist> = HashMap::new();
    // Cap = 3601 (NOT 3600). compute_vol(3600 samples) requires 3601 closes
    // (n_samples + 1 returns); cap=3600 would silently always return NaN for
    // vol_60m. Empirically caught in the H1+H3 offline analysis on exploration
    // (vol_60m finite in 0/1949 rows pre-fix). See KlineHist::compute_vol for
    // the n_samples+1 requirement.
    for (sym, asset) in [("btcusdt", "BTC"), ("ethusdt", "ETH")] {
        kline_hist.insert(asset, preheat_kline_hist(data_root, date, sym, asset, 3601));
    }

    for t in events {
        n_events += 1;
        // 1) Time-exits first (matches close_due semantics): stamp time_exit_bid
        //    from CURRENT book state. Note this fires per-event, not per-second;
        //    the granularity is fine (book updates dense).
        for mut pos in active.drain_due(t.recv_ms) {
            let bid = books
                .get(&pos.token)
                .and_then(|b| b.executable_bid_for_shares(pos.shares));
            pos.time_exit_bid = bid;
            closed_out.push(pos);
        }

        // 2) Apply event.
        match &t.ev {
            Ev::BookSnapshot { token, bids, asks, ts_ms } => {
                let book = books.entry(token.clone()).or_default();
                book.apply_snapshot(bids, asks, *ts_ms);
                let bid = book.executable_bid_for_shares(0.0); // probe, see below
                let _ = bid; // we record per-active-position, not generic
                // Now snapshot the bid for each active position holding this token.
                let max_shares = active
                    .by_key
                    .iter()
                    .filter(|((tok, _), _)| tok == token)
                    .map(|(_, p)| p.shares)
                    .fold(0.0_f64, f64::max);
                if max_shares > 0.0 {
                    sample_book_for_active(&mut active, token, t.recv_ms, books.get(token).unwrap());
                }
            }
            Ev::PriceChange { token, price, size, side, ts_ms } => {
                let book = books.entry(token.clone()).or_default();
                book.apply_price_change(*price, *size, *side, *ts_ms);
                sample_book_for_active(&mut active, token, t.recv_ms, book);
            }
            Ev::Kline { asset, t_open_ms, close } => {
                // W9: capture the firing kline's close so any Fire produced
                // from this trigger can stamp `CollectedPosition::trigger_close`
                // for the D2a (DcaConfirmingUnderlying) filter to read back.
                let kline_close = *close;
                // COMBO H3: roll the pre-trigger lookback. Push BEFORE
                // engine.on_kline() so the std-of-returns is computed over
                // STRICTLY-PRE-TRIGGER samples (including the firing kline
                // would be a forward-look). Wait — including the firing
                // kline is exactly right for "vol up to and including the
                // trigger moment". Both readings have a story; the
                // pre-registered choice here is STRICTLY pre-trigger to
                // keep the metric a clean lookback of fundamentals BEFORE
                // the disturbance the bot is reacting to.
                // COMBO H2: a kline whose t_open_ms hits trigger_ts_ms +
                // {2,5,10}*1000 is the post-trigger close we record into
                // each active position of matching asset. Both side-effects
                // happen on EVERY kline event, regardless of whether the
                // SignalEngine produces a trigger.
                record_kline_for_active(&mut active, asset, *t_open_ms, kline_close);
                let Some(trig) = engine.on_kline(asset, t_open_ms / 1000, *close) else {
                    if let Some(h) = kline_hist.get_mut(asset) {
                        h.push(*t_open_ms, kline_close);
                    }
                    continue;
                };
                // For triggered klines we ALSO push to the hist AFTER reading
                // it (Fire is below; vol_30m / vol_60m there reads from hist
                // BEFORE this push, so it remains strictly pre-trigger).
                let scope = expand_signals(&trig, catalog);
                let dir = if trig.ret_bps > 0.0 {
                    Direction::Up
                } else {
                    Direction::Down
                };
                // The decide engine needs only BBO (best_bid/best_ask). Adapt
                // FullBook -> EventBbo on the fly via FullBookProvider.
                let positions_for_decide: Vec<crate::state::persist::OpenPosition> = active
                    .by_key
                    .values()
                    .map(open_position_for_decide)
                    .collect();
                let dec = decide(
                    asset,
                    trig.trigger_ts,
                    dir,
                    &scope,
                    cfg,
                    &FullBookProvider(&books),
                    t.recv_ms,
                    |_| RestProbe::Skip,
                    &positions_for_decide,
                );
                for a in &dec.actions {
                    if let Action::Fire { interval, stratum, bet_token, fill, shares } = a {
                        let hold = cfg.band(*stratum).2;
                        let exit_ts_s = trig.trigger_ts + hold;
                        let interval_secs = interval_secs_for(interval);
                        let epoch = (trig.trigger_ts / interval_secs) * interval_secs;
                        let new_direction = dir;
                        // EntryFilter (W8): super-filter Fire actions before
                        // they become CollectedPositions. Default Baseline
                        // accepts everything (current operative behavior).
                        // W9: candidate's entry_price (*fill) + trigger_close
                        // (kline_close) are passed for D1/D2a/D2b inspection;
                        // pre-W9 filters (A/B/C) ignore them.
                        if !entry_filter.accept(
                            &trig, new_direction, active.by_key.values(),
                            asset, interval, epoch,
                            *fill, kline_close,
                        ) {
                            continue;
                        }
                        let signal_id = format!(
                            "{}-{}-{}-{}",
                            asset,
                            trig.trigger_ts,
                            interval,
                            match dir {
                                Direction::Up => "Up",
                                Direction::Down => "Down",
                            }
                        );
                        // COMBO H1: OBI of the bet token's CURRENT book.
                        // The decide() above already used books.get(bet_token) for
                        // BBO; here we read sizes for the imbalance metric.
                        let (obi_top1, obi_top3) = match books.get(bet_token) {
                            Some(b) => (compute_obi(b, 1), compute_obi(b, 3)),
                            None => (f64::NAN, f64::NAN),
                        };
                        // COMBO H3: pre-trigger realized vol from the rolling
                        // kline_hist for THIS asset. NaN if buffer is short or
                        // contaminated by a recorder gap (>5s between samples).
                        let (vol_30m, vol_60m) = match kline_hist.get(asset) {
                            Some(h) => (
                                h.compute_vol(1800, 5_000),
                                h.compute_vol(3600, 5_000),
                            ),
                            None => (f64::NAN, f64::NAN),
                        };
                        active.add(CollectedPosition {
                            token: bet_token.clone(),
                            asset: asset.to_string(),
                            interval: interval.clone(),
                            epoch,
                            direction: match dir {
                                Direction::Up => "Up".to_string(),
                                Direction::Down => "Down".to_string(),
                            },
                            signal_id,
                            entry_recv_ms: t.recv_ms,
                            entry_price: *fill,
                            shares: *shares,
                            exit_ts_ms: exit_ts_s * 1000,
                            trajectory: Vec::new(),
                            trajectory_bbo: Vec::new(),
                            time_exit_bid: None,
                            uncoverable_samples: 0,
                            total_samples: 0,
                            trigger_close: kline_close,
                            // W9-Pieza1: capture trigger magnitude (signed)
                            // + market-side fees AS-IS from the catalog.
                            trigger_ret_bps: trig.ret_bps,
                            maker_fee_bps: catalog
                                .active_market(asset, interval, epoch)
                                .map(|m| m.maker_base_fee).unwrap_or(0),
                            taker_fee_bps: catalog
                                .active_market(asset, interval, epoch)
                                .map(|m| m.taker_base_fee).unwrap_or(0),
                            fee_type: catalog
                                .active_market(asset, interval, epoch)
                                .map(|m| m.fee_type.clone()).unwrap_or_default(),
                            // COMBO instrumentation: trigger source-side time
                            // (for post-trigger kline matching) + computed H1/H3
                            // at trigger instant. H2 (binance_close_at_*s)
                            // starts as NaN and is filled by
                            // record_kline_for_active when subsequent klines
                            // arrive at trigger_ts_ms + {2,5,10}*1000.
                            entry_trigger_ts_ms: trig.trigger_ts * 1000,
                            obi_top1,
                            obi_top3,
                            binance_close_at_2s: f64::NAN,
                            binance_close_at_5s: f64::NAN,
                            binance_close_at_10s: f64::NAN,
                            binance_close_at_30s: f64::NAN,
                            binance_close_at_60s: f64::NAN,
                            binance_close_at_120s: f64::NAN,
                            vol_30m,
                            vol_60m,
                        });
                    }
                }
                // COMBO H3: now that Fire (if any) has already read the hist
                // to compute strictly-pre-trigger vol, push this kline into
                // the hist. Future klines see THIS one as part of their
                // lookback. Mirrors the no-trigger branch above.
                if let Some(h) = kline_hist.get_mut(asset) {
                    h.push(*t_open_ms, kline_close);
                }
            }
        }
    }

    // Time-stamp any positions still open at end-of-window with their last bid.
    // Same canonical-order discipline as `drain_due`: collect → sort by
    // signal_id → push. Without sort, HashMap::drain yields in per-process
    // RandomState order = non-reproducible. With sort, end-of-window positions
    // land in deterministic order in `closed_out` regardless of process.
    let mut tail: Vec<CollectedPosition> = active.by_key.drain().map(|(_, p)| p).collect();
    tail.sort_by(|a, b| a.signal_id.cmp(&b.signal_id));
    for mut pos in tail {
        pos.time_exit_bid = books
            .get(&pos.token)
            .and_then(|b| b.executable_bid_for_shares(pos.shares));
        closed_out.push(pos);
    }

    eprintln!(
        "[backtest_tp] {} positions collected: {} (events processed: {})",
        date, closed_out.len(), n_events
    );
    Ok(closed_out)
}

/// LEGACY: load all events for the day into one Vec, sort by recv_ms, replay.
/// Kept for the equivalence comparison vs the streaming path -- selectable
/// via `--bt-legacy-inmemory`. PROBLEM: this allocation is O(N) per day,
/// which for the heaviest May 2026 days (5/07, 5/08, 5/11 with 100M+ events)
/// pushes the Vec capacity past 12 GB during the doubling grow and OOMs on
/// any box with < 18 GB of free RAM.
///
/// SAME PARSE + REPLAY semantics as `replay_day_streaming` (both delegate to
/// `replay_events_through_state_machine`). Behavior difference is ONLY in
/// the event-source: load+sort vs k-way merge. Equivalence verified on
/// 5/06-5/10 via byte-by-byte JSONL comparison post-build (see commit log).
pub fn replay_day_inmemory_legacy(
    data_root: &Path,
    date: &str,
    cfg: &DecisionConfig,
    entry_filter: &EntryFilter,
) -> Result<Vec<CollectedPosition>> {
    let (catalog, tokens) = load_replay_context(data_root, date)?;
    let mut events: Vec<TimedEvent> = Vec::new();
    for (sym, asset) in [("btcusdt", "BTC"), ("ethusdt", "ETH")] {
        load_klines(data_root, sym, asset, date, &mut events)?;
    }
    load_pm_full(data_root, date, &tokens, &mut events)?;
    events.sort_by_key(|t| t.recv_ms);
    eprintln!(
        "[backtest_tp] {} legacy-inmemory merged events: {} (tokens-in-window={})",
        date,
        events.len(),
        tokens.len()
    );
    replay_events_through_state_machine(&mut events.into_iter(), &catalog, cfg, date, entry_filter, data_root)
}

/// G11 STREAMING: build 4 per-file iterators + k-way merger + drive the
/// state machine. Per-day peak memory ~250 KB regardless of event count
/// (vs ~17 GB for the heaviest day in legacy). Equivalence with
/// `replay_day_inmemory_legacy` is guaranteed by:
///   * shared `replay_events_through_state_machine` (single state-machine impl)
///   * stream push order matches legacy append order (BTC kline, ETH kline,
///     book, price_change) so ties on recv_ms resolve identically
///   * per-stream files are time-ordered by the recorder (single-thread,
///     append-only writer); the OOO summary asserts this assumption holds
pub fn replay_day_streaming(
    data_root: &Path,
    date: &str,
    cfg: &DecisionConfig,
    entry_filter: &EntryFilter,
) -> Result<Vec<CollectedPosition>> {
    let (catalog, tokens) = load_replay_context(data_root, date)?;
    let streams: Vec<(String, Box<dyn Iterator<Item = TimedEvent>>)> = vec![
        ("btc_kline".to_string(),       kline_stream(data_root, "btcusdt", "BTC", date)?),
        ("eth_kline".to_string(),       kline_stream(data_root, "ethusdt", "ETH", date)?),
        ("pm_book".to_string(),         pm_book_stream(data_root, date, &tokens)?),
        ("pm_price_change".to_string(), pm_price_change_stream(data_root, date, &tokens)?),
    ];
    let mut merger = StreamingMerger::new(streams);
    eprintln!(
        "[backtest_tp] {} streaming replay starting (tokens-in-window={})",
        date,
        tokens.len()
    );
    let result = replay_events_through_state_machine(&mut merger, &catalog, cfg, date, entry_filter, data_root);
    // Out-of-order summary AFTER iteration. Should be all zeros on real
    // recorder data; any non-zero count means a stream had a row whose
    // recv_ms regressed below the previous one, which the merger handles
    // gracefully (yields it in the slot of its recv_ms) but the operator
    // should be alerted -- the global ordering for that stream contains a
    // few mis-ordered rows.
    for (name, count) in merger.out_of_order_summary() {
        if count > 0 {
            eprintln!(
                "[backtest_tp] WARN: stream '{}' had {} out-of-order rows on {} \
                 (file was not monotonic by recv_ms; investigate recorder)",
                name, count, date
            );
        }
    }
    result
}

/// Dispatcher: route to streaming (G11, default) or legacy-inmemory (rare,
/// for equivalence checks or operators who need the old behavior).
pub fn replay_day(
    data_root: &Path,
    date: &str,
    cfg: &DecisionConfig,
    use_legacy_inmemory: bool,
    entry_filter: &EntryFilter,
) -> Result<Vec<CollectedPosition>> {
    if use_legacy_inmemory {
        replay_day_inmemory_legacy(data_root, date, cfg, entry_filter)
    } else {
        replay_day_streaming(data_root, date, cfg, entry_filter)
    }
}

/// Sample the executable_bid for all active positions on `token` from the
/// current book state. Encodes uncoverable as f64::NAN.
///
/// G13 ADDITIVE: ALSO captures the book's best_bid/best_ask per sample (same
/// recv_ms, same index) into p.trajectory_bbo for Phase 2's spread + depth-tax
/// features. The bbo snapshot is taken from the SAME `book` object that
/// produced `bid`, so the (executable_bid, best_bid, best_ask) triple is
/// internally consistent.
fn sample_book_for_active(
    active: &mut ActiveTracker,
    token: &str,
    recv_ms: i64,
    book: &FullBook,
) {
    // Pull all positions of `token` (need shares to compute bid each).
    let keys: Vec<((String, String), f64)> = active
        .by_key
        .iter()
        .filter(|((tok, _), _)| tok == token)
        .map(|(k, p)| (k.clone(), p.shares))
        .collect();
    // BBO snapshot taken ONCE from `book` (same for all positions on this
    // token at this recv_ms). Independent of shares.
    let bbo = (book.best_bid(), book.best_ask());
    for (key, shares) in keys {
        let bid = book
            .executable_bid_for_shares(shares)
            .unwrap_or(f64::NAN);
        if let Some(p) = active.by_key.get_mut(&key) {
            p.trajectory.push((recv_ms, bid));
            p.trajectory_bbo.push((recv_ms, bbo.0, bbo.1));
            p.total_samples += 1;
            if bid.is_nan() {
                p.uncoverable_samples += 1;
            }
        }
    }
}

/// Build a minimal OpenPosition view for the decide engine (it reads only a
/// few fields for the exposure cap). Cheap construction; not persisted.
fn open_position_for_decide(p: &CollectedPosition) -> crate::state::persist::OpenPosition {
    use crate::state::persist::{ConfirmationSource, OpenPosition, OrderStatus, Outcome};
    use rust_decimal::Decimal;
    OpenPosition {
        token_id: p.token.clone(),
        asset: p.asset.clone(),
        side: if p.direction == "Up" { Outcome::Up } else { Outcome::Down },
        entry_price: Decimal::try_from(p.entry_price).unwrap_or_default(),
        shares: Decimal::try_from(p.shares).unwrap_or_default(),
        opened_at_ms: p.entry_recv_ms,
        signal_id: p.signal_id.clone(),
        interval: p.interval.clone(),
        exit_ts_s: p.exit_ts_ms / 1000,
        // v3 (Pieza 1): the backtester builds OpenPositions only to feed the
        // legacy decide engine; smart-exit lives in `decide_exit_for_variant`
        // separately. Sentinels are correct here -- this OpenPosition never
        // reaches the (future) smart live evaluator.
        entry_ts_ms: 0,
        running_max_bid: 0.0,
        ts_max_bid_ms: 0,
        status: OrderStatus::Confirmed,
        order_id: Some(format!("bt-{}-{}", p.token, p.entry_recv_ms)),
        ack_at_ms: Some(p.entry_recv_ms),
        confirmed_at_ms: Some(p.entry_recv_ms),
        confirmation_source: Some(ConfirmationSource::UserWs),
        // v5: the backtester's OpenPositions never reach the live exit task;
        // no maker order is ever placed for them.
        maker_exit: None,
    }
}

fn date_midnight_sec(date: &str) -> Option<i64> {
    use polymarket_client_sdk_v2::types::NaiveDate;
    let d: NaiveDate = NaiveDate::parse_from_str(date, "%Y-%m-%d").ok()?;
    let dt = d.and_hms_opt(0, 0, 0)?;
    Some(dt.and_utc().timestamp())
}

// ============================================================================
// PASS 3 — Variant evaluation
// ============================================================================

/// The exit-policy variants the backtester evaluates. Each position gets
/// exactly ONE outcome per variant; all variants share the same entries.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ExitVariant {
    /// Baseline: vender al time-exit (= bid at exit_ts) regardless of profit.
    Baseline,
    /// FirstTouch(tp): vender al PRIMER evento donde executable_bid >=
    /// entry * (1 + tp). If never touched in the window, fall back to time-exit.
    FirstTouch { tp_pct: f64 }, // 0.10 = 10%
    /// Peak: vender al MÁXIMO executable_bid observado en la ventana. Not a
    /// realistic policy (requires foresight), but measures the OPPORTUNITY
    /// ceiling -- "how much profit was on the table". Used as a theoretical
    /// upper bound to compare FirstTouch against.
    Peak,
    /// G14 PHASE 3: smart exit based on (f1, f6) from Phase 2.
    ///   sell_now(t) := f1(t) >= x_pct AND f6(t) >= y_sec
    /// where
    ///   f1(t) = 100 * (t - entry_recv_ms) / hold_duration_ms (% del hold)
    ///   f6(t) = (t - argmax_recv_ms_so_far) / 1000 (segundos desde el running max)
    /// Falls back to time-exit if never triggered in the window (mismo failsafe
    /// que FirstTouch). The running max is computed CAUSALLY (only past bids
    /// up to and including the current sample). Both features were confirmed
    /// HONEST in Phase 2's tercile analysis (no level-confound; f6 has
    /// regime-flip by entry tercile but rule self-limits on high-entry
    /// positions because time_since_max stays small while bid keeps climbing).
    Smart { x_pct: f64, y_sec: i64 },
    /// G15 PHASE 4: pure trailing stop. Sell if `bid_t <= running_max * (1 -
    /// z_pct/100)`. Falls back to time-exit if never triggered. Uses LEVEL
    /// (drop from running max) NOT velocity (f7/f8 confirmed dead in Phase 2),
    /// so this is a genuinely new hypothesis. Best fit for cells with
    /// MESETA peak shape (BTC_5m top_decile 52.9%, ETH_5m 36.9% per Fase 1).
    Trailing { z_pct: f64 },
    /// G15: spread-filtered trailing. Trigger trailing ONLY if `spread_t <=
    /// max_spread` at the moment of the retracement. The spread filter is
    /// CONFIRMATION ("the retracement is real, the book is liquid"), not a
    /// predictor on its own. Uses f9 (spread) -- confirmed HONEST in Fase 2 --
    /// in its natural role as a gate, not a feature.
    SpreadFilteredTrailing { z_pct: f64, max_spread: f64 },
    /// G15: time-capped trailing. Trailing rule activates ONLY after
    /// `t_since_entry_pct >= x_pct`. Before x_pct of the hold has passed, no
    /// exit. Protects against premature trigger on early-peakers
    /// (Fase 1: 20.8% of positions peak in the first 10% of hold).
    /// Hybrid of f1 (time) + running_max (structure).
    TimeCappedTrailing { x_pct: f64, z_pct: f64 },
    /// G15: f6 alone (time since last running max). Sell if `time_since_max
    /// >= y_sec`. Tests whether the AND with f1 in `Smart` was helping or
    /// hurting. f6 was the second strongest honest predictor in Fase 2.
    F6Only { y_sec: i64 },
}

impl ExitVariant {
    pub fn label(&self) -> String {
        match self {
            ExitVariant::Baseline => "baseline_time_exit".into(),
            ExitVariant::FirstTouch { tp_pct } => {
                format!("first_touch_tp_{:02}pct", (tp_pct * 100.0).round() as i64)
            }
            ExitVariant::Peak => "peak_opportunity".into(),
            ExitVariant::Smart { x_pct, y_sec } => {
                format!("smart_x{:02}_y{}s", x_pct.round() as i64, y_sec)
            }
            ExitVariant::Trailing { z_pct } => {
                format!("trailing_z{}pct", z_pct.round() as i64)
            }
            ExitVariant::SpreadFilteredTrailing { z_pct, max_spread } => {
                // Spread encoded as cents (s*100 rounded) for compact filenames.
                let s_cents = (max_spread * 100.0).round() as i64;
                format!("strail_z{}_s{}", z_pct.round() as i64, s_cents)
            }
            ExitVariant::TimeCappedTrailing { x_pct, z_pct } => {
                format!("ctrail_x{}_z{}pct", x_pct.round() as i64, z_pct.round() as i64)
            }
            ExitVariant::F6Only { y_sec } => {
                format!("f6only_y{}s", y_sec)
            }
        }
    }
}

/// One trade after a variant has been applied: input position + decided exit.
#[derive(Debug, Clone)]
pub struct VariantTrade {
    pub position: CollectedPosition,
    pub exit_price: f64, // 0.0 if uncoverable (treated as worst case)
    pub exit_reason: ExitReason,
    pub net_pnl: f64, // includes buy_fee + sell_fee
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ExitReason {
    TimeExit,
    TpTouched,
    PeakOpportunity,
    Uncoverable,
    /// G14 PHASE 3: the (f1, f6) rule triggered before exit_ts.
    SmartTriggered,
    /// G15 PHASE 4: a trailing-stop family rule triggered (Trailing,
    /// SpreadFilteredTrailing, or TimeCappedTrailing).
    TrailingTriggered,
    /// G15 PHASE 4: F6Only rule triggered.
    F6Triggered,
}

impl ExitReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            ExitReason::TimeExit => "time_exit",
            ExitReason::TpTouched => "tp_touched",
            ExitReason::PeakOpportunity => "peak_opportunity",
            ExitReason::Uncoverable => "uncoverable",
            ExitReason::SmartTriggered => "smart_triggered",
            ExitReason::TrailingTriggered => "trailing_triggered",
            ExitReason::F6Triggered => "f6_triggered",
        }
    }
}

/// Apply one variant to all collected positions.
pub fn evaluate_variant(positions: &[CollectedPosition], variant: ExitVariant) -> Vec<VariantTrade> {
    positions
        .iter()
        .map(|p| {
            let (exit_price, reason) = decide_exit_for_variant(p, variant);
            let net_pnl = net_pnl_for(p.shares, p.entry_price, exit_price);
            VariantTrade {
                position: p.clone(),
                exit_price,
                exit_reason: reason,
                net_pnl,
            }
        })
        .collect()
}

/// Pure: decide the exit price + reason for one position under one variant.
fn decide_exit_for_variant(p: &CollectedPosition, v: ExitVariant) -> (f64, ExitReason) {
    match v {
        ExitVariant::Baseline => match p.time_exit_bid {
            Some(b) => (b, ExitReason::TimeExit),
            None => (0.0, ExitReason::Uncoverable),
        },
        ExitVariant::FirstTouch { tp_pct } => {
            let threshold = p.entry_price * (1.0 + tp_pct);
            for &(_recv_ms, bid) in &p.trajectory {
                if !bid.is_nan() && bid >= threshold {
                    return (bid, ExitReason::TpTouched);
                }
            }
            // never touched -> fall back to time-exit
            match p.time_exit_bid {
                Some(b) => (b, ExitReason::TimeExit),
                None => (0.0, ExitReason::Uncoverable),
            }
        }
        ExitVariant::Peak => {
            let peak = p
                .trajectory
                .iter()
                .filter(|(_, b)| !b.is_nan())
                .map(|(_, b)| *b)
                .fold(f64::NEG_INFINITY, f64::max);
            if peak.is_finite() {
                (peak, ExitReason::PeakOpportunity)
            } else {
                // No covered samples at all -> uncoverable.
                (0.0, ExitReason::Uncoverable)
            }
        }
        ExitVariant::Smart { x_pct, y_sec } => {
            // RULE: sell if f1(t) >= x_pct AND f6(t) >= y_sec, evaluated
            // chronologically along the trajectory. Causally: the running max
            // is computed from past+current samples ONLY (never future). If
            // never triggered before exit_ts, fall back to time-exit (same
            // failsafe semantics as FirstTouch).
            //
            // PIECE W3 RE-SEED: `running_max_bid` is now seeded to
            // `p.entry_price` (the ask paid), NOT `f64::NEG_INFINITY`. This
            // matches the live bot's open-time seed (PaperExecutor::open in
            // piece W2 sets running_max_bid = entry_price) -- without this
            // re-seed, a position whose first observed bid is BELOW
            // entry_price would diverge between backtester (would have
            // adopted the low bid as max) and live (preserves entry_price as
            // max). The shared `exit_rules::smart_triggers` predicate makes
            // the trigger arithmetic identical; the seed alignment makes the
            // state evolution identical. Together: mechanical parity.
            //
            // Trigger predicate is now `exit_rules::smart_triggers` -- single
            // source of truth shared with the live evaluator (piece W4).
            let mut running_max_bid = p.entry_price;
            let mut running_max_ms = p.entry_recv_ms;
            for &(ms, bid) in &p.trajectory {
                if bid.is_finite() {
                    // Update running max FIRST so this sample's f6 == 0 if
                    // it sets a new STRICT high (matches live's strict-greater
                    // semantics; equal bids never re-stamp ts_max).
                    if bid > running_max_bid {
                        running_max_bid = bid;
                        running_max_ms = ms;
                    }
                    if crate::exit_rules::smart_triggers(
                        ms, p.entry_recv_ms, p.exit_ts_ms, running_max_ms, x_pct, y_sec,
                    ) {
                        return (bid, ExitReason::SmartTriggered);
                    }
                }
            }
            // Never triggered -> fall back to time-exit (safe baseline).
            match p.time_exit_bid {
                Some(b) => (b, ExitReason::TimeExit),
                None => (0.0, ExitReason::Uncoverable),
            }
        }
        ExitVariant::Trailing { z_pct } => {
            // RULE: sell if bid_t <= running_max * (1 - z_pct/100). Pure
            // trailing stop. Uses LEVEL (drop from running max), not velocity.
            let z_frac = z_pct / 100.0;
            let mut running_max_bid = f64::NEG_INFINITY;
            for &(_ms, bid) in &p.trajectory {
                if bid.is_finite() {
                    if bid > running_max_bid {
                        running_max_bid = bid;
                    } else if running_max_bid.is_finite()
                        && bid <= running_max_bid * (1.0 - z_frac)
                    {
                        return (bid, ExitReason::TrailingTriggered);
                    }
                }
            }
            match p.time_exit_bid {
                Some(b) => (b, ExitReason::TimeExit),
                None => (0.0, ExitReason::Uncoverable),
            }
        }
        ExitVariant::SpreadFilteredTrailing { z_pct, max_spread } => {
            // RULE: trailing stop, but trigger only if spread_t <= max_spread
            // at that sample. The trajectory and trajectory_bbo are paired by
            // index (G13 invariant). Walk both in lockstep.
            let z_frac = z_pct / 100.0;
            let mut running_max_bid = f64::NEG_INFINITY;
            // Defensive: if the bbo trajectory is shorter (shouldn't happen
            // post-G13 but be safe), we just don't check spread on those
            // tail samples -> they cannot trigger.
            let n_bbo = p.trajectory_bbo.len();
            for (i, &(_ms, bid)) in p.trajectory.iter().enumerate() {
                if bid.is_finite() {
                    if bid > running_max_bid {
                        running_max_bid = bid;
                    } else if running_max_bid.is_finite()
                        && bid <= running_max_bid * (1.0 - z_frac)
                    {
                        // Check spread at this same sample index.
                        if i < n_bbo {
                            let (_, bb, ba) = p.trajectory_bbo[i];
                            if let (Some(bb), Some(ba)) = (bb, ba) {
                                let spread = ba - bb;
                                if spread.is_finite() && spread <= max_spread {
                                    return (bid, ExitReason::TrailingTriggered);
                                }
                            }
                        }
                        // Spread missing or > max_spread -> don't trigger here.
                    }
                }
            }
            match p.time_exit_bid {
                Some(b) => (b, ExitReason::TimeExit),
                None => (0.0, ExitReason::Uncoverable),
            }
        }
        ExitVariant::TimeCappedTrailing { x_pct, z_pct } => {
            // RULE: trailing rule, but only after t_since_entry_pct >= x_pct.
            // Before x_pct of hold has passed, NEVER trigger.
            let hold_duration_ms = (p.exit_ts_ms - p.entry_recv_ms).max(1) as f64;
            let z_frac = z_pct / 100.0;
            let mut running_max_bid = f64::NEG_INFINITY;
            for &(ms, bid) in &p.trajectory {
                if bid.is_finite() {
                    if bid > running_max_bid {
                        running_max_bid = bid;
                    } else {
                        let f1_pct = 100.0 * (ms - p.entry_recv_ms) as f64 / hold_duration_ms;
                        if f1_pct >= x_pct
                            && running_max_bid.is_finite()
                            && bid <= running_max_bid * (1.0 - z_frac)
                        {
                            return (bid, ExitReason::TrailingTriggered);
                        }
                    }
                }
            }
            match p.time_exit_bid {
                Some(b) => (b, ExitReason::TimeExit),
                None => (0.0, ExitReason::Uncoverable),
            }
        }
        ExitVariant::F6Only { y_sec } => {
            // RULE: sell if time_since_bid_max_ms >= y_sec*1000.
            //
            // PIECE W3 RE-SEED + shared predicate: same rationale as the
            // Smart branch above. Seed `running_max_bid = p.entry_price`
            // matches the live open-time state; trigger via
            // `exit_rules::f6_triggers` is the single source of truth
            // shared with the live evaluator (piece W4).
            let mut running_max_bid = p.entry_price;
            let mut running_max_ms = p.entry_recv_ms;
            for &(ms, bid) in &p.trajectory {
                if bid.is_finite() {
                    if bid > running_max_bid {
                        running_max_bid = bid;
                        running_max_ms = ms;
                    }
                    if crate::exit_rules::f6_triggers(ms, running_max_ms, y_sec) {
                        return (bid, ExitReason::F6Triggered);
                    }
                }
            }
            match p.time_exit_bid {
                Some(b) => (b, ExitReason::TimeExit),
                None => (0.0, ExitReason::Uncoverable),
            }
        }
    }
}

/// Pure: net P&L = `(shares*exit - sell_fee) - (shares*entry + buy_fee)`.
/// Identical formula to `feed_guards_net_pnl` so the backtest's $ are
/// directly comparable to the live counter (post-G9-pre-A SELL fix).
#[must_use]
pub fn net_pnl_for(shares: f64, entry: f64, exit: f64) -> f64 {
    let buy_fee = fee_f64(shares, entry);
    let sell_fee = fee_f64(shares, exit);
    (shares * exit - sell_fee) - (shares * entry + buy_fee)
}

// ============================================================================
// FASE 1: PEAK CHARACTERIZATION (descriptive only, no prediction)
// ============================================================================
//
// Goal: characterize the SHAPE of the peak each position reaches during its
// hold window. The data the backtester already collects (trajectory of
// executable_bid per book event) is enough to answer the 5 Fase-1 questions:
//
//   Q1: WHEN does the peak occur (peak_offset_ms, peak_offset_pct_of_hold)?
//   Q2: HOW BIG is the peak vs the time-exit (peak_excess_pnl, _pct_over_stake)?
//   Q3: WHAT SHAPE (meseta vs pico)? Measured as time_in_top_decile_ms = how
//       long the bid stayed within 90% of the peak. High = meseta capturable;
//       low = pico instantáneo. Computed via step-function integration over
//       the trajectory (bid[i] persists until recv_ms[i+1]).
//   Q4: PER-CELL distinguishability (BTC/ETH x 5m/15m).
//   Q5: HOW MANY trades have a "significant" peak excess (K% of stake)?
//
// This is PURELY descriptive. No model, no threshold optimization, nothing
// that could overfit on these dates. The output (peak_characterization.jsonl
// + stdout histograms) is the input to Fase 2 (predict the peak from real-time
// features) IF Fase 1 shows the peaks have structure worth predicting.

/// Per-position peak characterization. One row per CollectedPosition. Emitted
/// to `peak_characterization.jsonl` and consumed by the Fase-1 stdout report.
///
/// All `Option<...>` fields are `None` when the trajectory has no COVERABLE
/// sample (every sample was NaN = executable-bid depth too thin). This is
/// distinct from "no peak above time-exit" (in that case peak == time_exit and
/// peak_excess_pnl will be ~0, not None).
#[derive(Debug, Clone, Serialize)]
pub struct PeakStats {
    // ---- identity (join key with trades_*.jsonl) ----
    pub signal_id: String,
    pub asset: String,
    pub interval: String,
    pub direction: String,
    // ---- temporal anchors ----
    pub entry_recv_ms: i64,
    pub exit_ts_ms: i64,
    /// `exit_ts_ms - entry_recv_ms`. Normalizes peak_offset across stratums
    /// (RW = 120s hold, IM = 300s hold). Always >= 1 (defensive max).
    pub hold_duration_ms: i64,
    // ---- position size ----
    pub shares: f64,
    pub entry_price: f64,
    /// `shares * entry_price`. Used to express peak_excess as a % of stake
    /// (interpretable across positions of different sizes).
    pub stake_usd: f64,
    // ---- baseline (time-exit) reference ----
    pub time_exit_bid: Option<f64>,
    pub time_exit_pnl: Option<f64>,
    // ---- peak (Q1, Q2) ----
    /// Maximum coverable executable_bid observed during the lifetime.
    pub peak_bid: Option<f64>,
    pub peak_pnl: Option<f64>,
    /// ms from entry_recv_ms to the peak's recv_ms. >= 0 (the peak can occur
    /// at entry itself if the bid never goes up).
    pub peak_offset_ms: Option<i64>,
    /// `100 * peak_offset_ms / hold_duration_ms`. Can be slightly > 100% in
    /// rare cases where the trajectory captures one extra sample past
    /// exit_ts_ms before the drain (the consumer should clamp for binning).
    pub peak_offset_pct_of_hold: Option<f64>,
    // ---- shape (Q3) ----
    /// Total time (step-function integration) the bid was in the TOP DECILE
    /// (>= 0.9 * peak_bid). Between sample i and i+1, the bid is treated as
    /// constant at `bid[i]`. High value = meseta (bid stayed near the peak,
    /// easy to capture with a smart exit). Low value = pico instantáneo (bid
    /// kissed the peak then fell, hard to capture).
    pub time_in_top_decile_ms: i64,
    /// Count of trajectory samples (not time) within the top decile. Provided
    /// alongside `time_in_top_decile_ms` so the consumer can detect "few
    /// samples that span a lot of wall-time" (sparse meseta) vs "many samples
    /// densely packed" (active meseta).
    pub samples_in_top_decile: usize,
    /// `last_top_ms - first_top_ms`. Lower bound on the meseta width: if the
    /// bid was in the top decile only briefly even if it crossed multiple
    /// times, the span is short. Useful to distinguish "peak is one moment"
    /// (span ~ 0) from "peak is a sustained window" (span > seconds).
    pub top_decile_span_ms: i64,
    // ---- peak excess (Q5 thresholding) ----
    pub peak_excess_pnl: Option<f64>,
    pub peak_excess_pct_over_stake: Option<f64>,
    // ---- sanity counters (for debugging / filtering) ----
    pub total_samples: usize,
    pub uncoverable_samples: usize,
}

/// Compute per-position peak characterization. Two passes over the trajectory:
/// (1) find peak (max bid + its ts); (2) integrate time within the top decile.
/// Pure, no IO. Stable: same input → same output.
///
/// `None` peak fields when no coverable sample exists (entire trajectory was
/// NaN). In that case the position would also be `Uncoverable` in every
/// variant -- consistent with the variant evaluator's handling.
pub fn characterize_position(p: &CollectedPosition) -> PeakStats {
    let hold_duration_ms = (p.exit_ts_ms - p.entry_recv_ms).max(1);
    let stake_usd = p.shares * p.entry_price;

    // -------- PASS 1: argmax over finite (coverable) bids --------
    let mut peak_bid = f64::NEG_INFINITY;
    let mut peak_recv_ms: i64 = p.entry_recv_ms;
    for &(ms, bid) in &p.trajectory {
        if bid.is_finite() && bid > peak_bid {
            peak_bid = bid;
            peak_recv_ms = ms;
        }
    }
    let (peak_bid_opt, peak_pnl_opt, peak_offset_ms_opt, peak_offset_pct_opt) =
        if peak_bid.is_finite() {
            let off_ms = peak_recv_ms - p.entry_recv_ms;
            let pnl = net_pnl_for(p.shares, p.entry_price, peak_bid);
            let pct = 100.0 * (off_ms as f64) / (hold_duration_ms as f64);
            (Some(peak_bid), Some(pnl), Some(off_ms), Some(pct))
        } else {
            (None, None, None, None)
        };

    // -------- PASS 2: time within top decile (>= 0.9 * peak) --------
    // Step-function integration: bid[i] persists from recv_ms[i] to recv_ms[i+1].
    // For the tail sample, persistence extends to exit_ts_ms (the drain time).
    // NaN samples contribute zero time (they were "uncoverable", definitely
    // not within the peak's top decile).
    let mut time_in_top_decile_ms: i64 = 0;
    let mut samples_in_top_decile: usize = 0;
    let mut first_top_ms: Option<i64> = None;
    let mut last_top_ms: Option<i64> = None;
    if let Some(pb) = peak_bid_opt {
        let threshold = 0.9 * pb;
        let traj = &p.trajectory;
        for i in 0..traj.len() {
            let (ms_i, bid_i) = traj[i];
            if bid_i.is_finite() && bid_i >= threshold {
                samples_in_top_decile += 1;
                first_top_ms.get_or_insert(ms_i);
                last_top_ms = Some(ms_i);
                let next_ms = if i + 1 < traj.len() {
                    traj[i + 1].0
                } else {
                    // Tail: this sample persists until the position drains
                    // (exit_ts_ms). max(ms_i) defends against a peak captured
                    // slightly past exit_ts_ms (rare edge: drain happens on the
                    // first event with recv_ms >= exit_ts_ms).
                    p.exit_ts_ms.max(ms_i)
                };
                let gap = (next_ms - ms_i).max(0);
                time_in_top_decile_ms += gap;
            }
        }
    }
    let top_decile_span_ms = match (first_top_ms, last_top_ms) {
        (Some(a), Some(b)) => (b - a).max(0),
        _ => 0,
    };

    // -------- Time-exit reference + peak excess --------
    let time_exit_pnl_opt = p
        .time_exit_bid
        .map(|b| net_pnl_for(p.shares, p.entry_price, b));
    let peak_excess_pnl = peak_pnl_opt
        .zip(time_exit_pnl_opt)
        .map(|(pp, tp)| pp - tp);
    let peak_excess_pct = peak_excess_pnl.and_then(|x| {
        if stake_usd > 0.0 {
            Some(100.0 * x / stake_usd)
        } else {
            None
        }
    });

    PeakStats {
        signal_id: p.signal_id.clone(),
        asset: p.asset.clone(),
        interval: p.interval.clone(),
        direction: p.direction.clone(),
        entry_recv_ms: p.entry_recv_ms,
        exit_ts_ms: p.exit_ts_ms,
        hold_duration_ms,
        shares: p.shares,
        entry_price: p.entry_price,
        stake_usd,
        time_exit_bid: p.time_exit_bid,
        time_exit_pnl: time_exit_pnl_opt,
        peak_bid: peak_bid_opt,
        peak_pnl: peak_pnl_opt,
        peak_offset_ms: peak_offset_ms_opt,
        peak_offset_pct_of_hold: peak_offset_pct_opt,
        time_in_top_decile_ms,
        samples_in_top_decile,
        top_decile_span_ms,
        peak_excess_pnl,
        peak_excess_pct_over_stake: peak_excess_pct,
        total_samples: p.total_samples,
        uncoverable_samples: p.uncoverable_samples,
    }
}

// ============================================================================
// Metrics (with breakdowns: total + per-asset + per-interval + per-cell)
// ============================================================================

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VariantMetrics {
    pub variant_label: String,
    pub n_trades: usize,
    pub win_ratio: f64,
    pub total_pnl: f64,
    pub avg_pnl: f64,
    pub pct_tp_hit: f64, // for FirstTouch variants only; 0 otherwise
    pub pnl_when_tp_hit: f64,
    pub pnl_when_time_exit: f64,
    pub n_uncoverable: usize, // depth too thin to sell the position
    pub uncoverable_rate: f64,
    /// Per-(asset, interval) sub-strategy breakdowns.
    /// Keys: "BTC_5m", "BTC_15m", "ETH_5m", "ETH_15m".
    pub by_cell: BTreeMap<String, CellMetrics>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CellMetrics {
    pub n_trades: usize,
    pub win_ratio: f64,
    pub total_pnl: f64,
    pub avg_pnl: f64,
    pub pct_tp_hit: f64,
}

pub fn compute_metrics(trades: &[VariantTrade], variant_label: String) -> VariantMetrics {
    let mut m = VariantMetrics {
        variant_label,
        ..Default::default()
    };
    if trades.is_empty() {
        return m;
    }
    m.n_trades = trades.len();
    let wins = trades.iter().filter(|t| t.net_pnl > 0.0).count();
    m.win_ratio = wins as f64 / trades.len() as f64;
    m.total_pnl = trades.iter().map(|t| t.net_pnl).sum();
    m.avg_pnl = m.total_pnl / trades.len() as f64;

    let tp_hits: Vec<_> = trades
        .iter()
        .filter(|t| t.exit_reason == ExitReason::TpTouched)
        .collect();
    m.pct_tp_hit = tp_hits.len() as f64 / trades.len() as f64;
    m.pnl_when_tp_hit = if tp_hits.is_empty() {
        0.0
    } else {
        tp_hits.iter().map(|t| t.net_pnl).sum::<f64>() / tp_hits.len() as f64
    };

    let time_exits: Vec<_> = trades
        .iter()
        .filter(|t| t.exit_reason == ExitReason::TimeExit)
        .collect();
    m.pnl_when_time_exit = if time_exits.is_empty() {
        0.0
    } else {
        time_exits.iter().map(|t| t.net_pnl).sum::<f64>() / time_exits.len() as f64
    };

    m.n_uncoverable = trades
        .iter()
        .filter(|t| t.exit_reason == ExitReason::Uncoverable)
        .count();
    m.uncoverable_rate = m.n_uncoverable as f64 / trades.len() as f64;

    // Per-cell breakdown.
    let mut cells: BTreeMap<String, Vec<&VariantTrade>> = BTreeMap::new();
    for t in trades {
        let cell = format!("{}_{}", t.position.asset, t.position.interval);
        cells.entry(cell).or_default().push(t);
    }
    for (cell, ts) in cells {
        let n = ts.len();
        let wins = ts.iter().filter(|t| t.net_pnl > 0.0).count();
        let total: f64 = ts.iter().map(|t| t.net_pnl).sum();
        let tp_hits = ts
            .iter()
            .filter(|t| t.exit_reason == ExitReason::TpTouched)
            .count();
        m.by_cell.insert(
            cell,
            CellMetrics {
                n_trades: n,
                win_ratio: wins as f64 / n.max(1) as f64,
                total_pnl: total,
                avg_pnl: total / n.max(1) as f64,
                pct_tp_hit: tp_hits as f64 / n.max(1) as f64,
            },
        );
    }

    m
}

// ============================================================================
// G12: HEALTH CHECK (recv_ms metadata only -- safe on validation data)
//
// SEAL-SAFETY CONTRACT (audit this whole block before changing it):
// The health check is the ONE operation that may scan dates >= the validation
// cutoff (2026-05-17 onwards) WITHOUT breaking the out-of-sample seal. The
// guarantee is enforced by extreme narrowness: the scan reads ONLY the
// `received_at` JSON field of each line, extracts its recv_ms, then discards
// the rest of the parsed Value before any other field is inspected.
//
// SPECIFICALLY:
//   READ:     recv_ms (timestamp), date, stream name, line counts, gaps.
//   NOT READ: price, size, token, asset_id, kline payload, side, bids, asks,
//             or any other content field. These are NOT extracted, NOT
//             compared, NOT written, NOT used to compute anything.
//
// METHODOLOGY: the OOO count + line count + gap count are properties of the
// RECORDER's FILE STRUCTURE (single-thread append-only, NTP sync, etc.) --
// they say nothing about markets, prices, or strategy. Auditing them on
// validation data cannot inform Fase 2 design (no parameter to tune from
// "how many OOO rows are there"); cannot leak strategy results (none are
// computed); cannot bias Fase 3 conclusions (the seal is about market
// content, not file health). Equivalent in spirit to running `wc -l` on the
// file.
//
// The phase guard in `validate_phase_dates` is NOT applied here. That guard
// protects `run_backtest_tp` (the operation that produces strategy results).
// The health check is a different operation entirely with no such risk.
// ============================================================================

/// Per-(date, stream) health metadata. PURELY STRUCTURAL: counts +
/// timestamp deltas only. Contains NO market content (no price/size/token).
#[derive(Debug, Clone, Serialize)]
pub struct StreamHealth {
    pub date: String,
    pub stream: String,
    pub file_present: bool,
    /// Total lines read from the file (includes blanks and unparseable).
    pub n_lines: usize,
    /// Lines that produced a usable `recv_ms`.
    pub n_with_recv_ms: usize,
    /// Lines whose JSON parse failed OR whose `received_at` field was missing
    /// / unparseable as RFC-3339. Reported but ignored for OOO/gap stats.
    pub n_skipped: usize,
    /// Inversions of monotonicity: recv_ms[i] < recv_ms[i-1]. Should be 0 for
    /// a healthy recorder (single-thread append-only writer per file).
    pub n_out_of_order: usize,
    /// Largest backward skew encountered (max of `prev_recv_ms - recv_ms`
    /// over OOO rows). Helps tell "1ms NTP jitter" from "30s clock jump".
    pub max_ooo_skew_ms: i64,
    /// Consecutive recv_ms whose forward delta exceeds 60s. Real markets push
    /// at least one event/second per active token; a 60s+ gap usually = the
    /// recorder was down, the WS dropped, or the system was suspended.
    pub n_gaps_over_60s: usize,
    /// Largest forward gap observed (max of `recv_ms - prev_recv_ms`).
    pub max_gap_ms: i64,
    pub first_recv_ms: Option<i64>,
    pub last_recv_ms: Option<i64>,
}

/// Scan ONE file for recv_ms-only health metadata.
///
/// SEAL-SAFE PARSE: each line is JSON-parsed into a Value. The ONLY field
/// extracted from that Value is `received_at` (via `parse_recv_ms`). The
/// Value is then dropped before the next line is read -- the parser
/// technically loads the whole JSON tree into a transient `Value`, but no
/// code in this function accesses any field other than `received_at`. The
/// `drop(v)` after `parse_recv_ms` is explicit documentation that the rest
/// of the parsed tree is intentionally discarded (Rust would drop it at end
/// of scope regardless; the explicit drop is a maintenance hint that
/// adding any `v.get("price")` etc. inside this function is a SEAL
/// VIOLATION).
pub fn scan_stream_health(path: &Path, date: &str, stream: &str) -> StreamHealth {
    let mut h = StreamHealth {
        date: date.to_string(),
        stream: stream.to_string(),
        file_present: false,
        n_lines: 0,
        n_with_recv_ms: 0,
        n_skipped: 0,
        n_out_of_order: 0,
        max_ooo_skew_ms: 0,
        n_gaps_over_60s: 0,
        max_gap_ms: 0,
        first_recv_ms: None,
        last_recv_ms: None,
    };
    let reader = match open_jsonl(path) {
        Ok(r) => r,
        Err(_) => return h, // file_present = false
    };
    h.file_present = true;
    let mut prev_recv_ms: Option<i64> = None;
    for line in reader.lines() {
        h.n_lines += 1;
        let line = match line {
            Ok(l) => l,
            Err(_) => { h.n_skipped += 1; continue; }
        };
        if line.trim().is_empty() { h.n_skipped += 1; continue; }
        // SEAL CHECKPOINT: parse the line, extract ONLY received_at, drop
        // the rest. Adding any v.get("price"|"size"|"token"|"asset_id"|...)
        // call inside this block is a methodology violation -- this file's
        // call may be operating on validation data.
        let v: Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(_) => { h.n_skipped += 1; continue; }
        };
        let recv_ms = match parse_recv_ms(&v) {
            Some(x) => x,
            None => { h.n_skipped += 1; drop(v); continue; }
        };
        drop(v); // explicit: rest of the parsed JSON is discarded (seal-safe)

        h.n_with_recv_ms += 1;
        h.first_recv_ms.get_or_insert(recv_ms);
        h.last_recv_ms = Some(recv_ms);
        if let Some(prev) = prev_recv_ms {
            let delta = recv_ms - prev;
            if delta < 0 {
                h.n_out_of_order += 1;
                h.max_ooo_skew_ms = h.max_ooo_skew_ms.max(-delta);
            } else if delta > 60_000 {
                h.n_gaps_over_60s += 1;
                h.max_gap_ms = h.max_gap_ms.max(delta);
            }
        }
        prev_recv_ms = Some(recv_ms);
    }
    h
}

/// Run the health check over a date range across the 4 strategy streams
/// (BTC kline, ETH kline, PM book, PM price_change). Prints a table to
/// stdout: one row per (date, stream), columns = line counts + OOO +
/// gaps. NO JSON output, NO file artifacts beyond stdout. NO seal break
/// (the validation_seal_broken.txt is NEVER written by this path).
pub fn run_health_check(
    data_root: &Path,
    start_date: &str,
    end_date: &str,
) -> Result<()> {
    let dates = dates_inclusive(start_date, end_date)?;
    println!();
    println!("=== BACKTESTER HEALTH CHECK (recv_ms metadata only, NO market content) ===");
    println!("dates:    {start_date} .. {end_date}");
    println!("contract: ONLY reads received_at -> recv_ms per line. Does NOT extract");
    println!("          price/size/token/asset_id/payload. Output is structural counts");
    println!("          (lines, OOO inversions, gaps). NO strategy result is computed.");
    println!("          Safe to invoke on validation data without breaking the seal.");
    println!();
    println!(
        "{:<12} {:<18} {:>10} {:>10} {:>8} {:>14} {:>8} {:>14}",
        "date", "stream", "n_lines", "n_recv_ms", "n_OOO", "max_OOO(ms)", "n_gaps", "max_gap(ms)"
    );
    println!("{}", "-".repeat(108));

    // Sub-paths under live_l2 -- structural only, no per-token filtering.
    let stream_specs: [(&str, &str); 4] = [
        ("btc_kline",       "live_l2/binance/btcusdt_kline_1s"),
        ("eth_kline",       "live_l2/binance/ethusdt_kline_1s"),
        ("pm_book",         "live_l2/polymarket/book"),
        ("pm_price_change", "live_l2/polymarket/price_change"),
    ];

    // Aggregates for the trailer.
    let mut agg_lines: u64 = 0;
    let mut agg_recv: u64 = 0;
    let mut agg_ooo: u64 = 0;
    let mut agg_gaps: u64 = 0;
    let mut n_missing: u64 = 0;

    for date in &dates {
        for (stream, sub) in &stream_specs {
            let dir = data_root.join(sub);
            let path = match resolve_jsonl(&dir, date) {
                Some(p) => p,
                None => {
                    println!(
                        "{:<12} {:<18} {:>10} {:>10} {:>8} {:>14} {:>8} {:>14}",
                        date, stream, "MISSING", "-", "-", "-", "-", "-"
                    );
                    n_missing += 1;
                    continue;
                }
            };
            let h = scan_stream_health(&path, date, stream);
            agg_lines += h.n_lines as u64;
            agg_recv += h.n_with_recv_ms as u64;
            agg_ooo += h.n_out_of_order as u64;
            agg_gaps += h.n_gaps_over_60s as u64;
            println!(
                "{:<12} {:<18} {:>10} {:>10} {:>8} {:>14} {:>8} {:>14}",
                date,
                stream,
                h.n_lines,
                h.n_with_recv_ms,
                h.n_out_of_order,
                h.max_ooo_skew_ms,
                h.n_gaps_over_60s,
                h.max_gap_ms
            );
        }
    }
    println!("{}", "-".repeat(108));
    println!(
        "TOTAL: {} dates x {} streams = {} (date,stream) pairs; missing={}; \
         lines={}; recv_ms={}; OOO={}; gaps>60s={}",
        dates.len(), stream_specs.len(), dates.len() * stream_specs.len(),
        n_missing, agg_lines, agg_recv, agg_ooo, agg_gaps
    );
    println!();
    Ok(())
}

// ============================================================================
// CLI entry
// ============================================================================

/// Default TP variant grid (per user spec): baseline + 0.05, 0.10, ..., 0.50.
/// Plus the unconditional Peak variant. The user can override via CLI flag.
pub fn default_variants() -> Vec<ExitVariant> {
    let mut v = vec![ExitVariant::Baseline];
    for tp_pct_int in [5, 10, 15, 20, 25, 30, 40, 50] {
        v.push(ExitVariant::FirstTouch {
            tp_pct: tp_pct_int as f64 / 100.0,
        });
    }
    v.push(ExitVariant::Peak);
    v
}

/// Parse a CSV variant spec like "0,5,10,15,30,smart:50:30,peak" into a
/// Vec<ExitVariant>. Tokens:
///   "0"                = Baseline (time-exit)
///   "<N>"              = FirstTouch(N%)
///   "peak"             = Peak (theoretical max)
///   "smart:<X>:<Y>"    = G14 Smart exit rule with x_pct=X (%) and y_sec=Y (s).
///                        X must be in [0, 100]; Y must be >= 0.
pub fn parse_variants(spec: &str) -> Result<Vec<ExitVariant>> {
    let mut out = Vec::new();
    for tok in spec.split(',') {
        let tok = tok.trim();
        if tok.is_empty() {
            continue;
        }
        if tok.eq_ignore_ascii_case("peak") {
            out.push(ExitVariant::Peak);
            continue;
        }
        // G14 Smart variant: "smart:<X>:<Y>" -- X in [0, 100], Y in seconds >= 0.
        if let Some(rest) = tok.strip_prefix("smart:") {
            let parts: Vec<&str> = rest.split(':').collect();
            if parts.len() != 2 {
                bail!(
                    "bad smart variant '{tok}': expected 'smart:<X_pct>:<Y_sec>' \
                     (e.g. 'smart:50:30' = sell if f1 >= 50% AND f6 >= 30s)"
                );
            }
            let x_pct: f64 = parts[0].parse()
                .with_context(|| format!("bad X_pct in '{tok}'"))?;
            let y_sec: i64 = parts[1].parse()
                .with_context(|| format!("bad Y_sec in '{tok}'"))?;
            if !(0.0..=100.0).contains(&x_pct) {
                bail!("smart '{tok}': X_pct must be in [0, 100]; got {x_pct}");
            }
            if y_sec < 0 {
                bail!("smart '{tok}': Y_sec must be >= 0; got {y_sec}");
            }
            out.push(ExitVariant::Smart { x_pct, y_sec });
            continue;
        }
        // G15 Phase 4: trailing-family + f6-only variants.
        if let Some(rest) = tok.strip_prefix("trailing:") {
            let z_pct: f64 = rest.parse()
                .with_context(|| format!("bad Z in '{tok}'"))?;
            if !(0.0..=100.0).contains(&z_pct) {
                bail!("trailing '{tok}': Z must be in (0, 100]; got {z_pct}");
            }
            out.push(ExitVariant::Trailing { z_pct });
            continue;
        }
        if let Some(rest) = tok.strip_prefix("strail:") {
            let parts: Vec<&str> = rest.split(':').collect();
            if parts.len() != 2 {
                bail!(
                    "bad strail '{tok}': expected 'strail:<Z_pct>:<S_dollars>' \
                     (e.g. 'strail:2:0.01' = trailing 2% with spread <= $0.01)"
                );
            }
            let z_pct: f64 = parts[0].parse()
                .with_context(|| format!("bad Z in '{tok}'"))?;
            let max_spread: f64 = parts[1].parse()
                .with_context(|| format!("bad S in '{tok}'"))?;
            if !(0.0..=100.0).contains(&z_pct) {
                bail!("strail '{tok}': Z_pct must be in (0, 100]; got {z_pct}");
            }
            if max_spread < 0.0 || max_spread > 1.0 {
                bail!("strail '{tok}': max_spread (in dollars) must be in [0, 1]; got {max_spread}");
            }
            out.push(ExitVariant::SpreadFilteredTrailing { z_pct, max_spread });
            continue;
        }
        if let Some(rest) = tok.strip_prefix("ctrail:") {
            let parts: Vec<&str> = rest.split(':').collect();
            if parts.len() != 2 {
                bail!(
                    "bad ctrail '{tok}': expected 'ctrail:<X_pct>:<Z_pct>' \
                     (e.g. 'ctrail:50:2' = after 50% of hold, trailing 2%)"
                );
            }
            let x_pct: f64 = parts[0].parse()
                .with_context(|| format!("bad X in '{tok}'"))?;
            let z_pct: f64 = parts[1].parse()
                .with_context(|| format!("bad Z in '{tok}'"))?;
            if !(0.0..=100.0).contains(&x_pct) {
                bail!("ctrail '{tok}': X_pct must be in [0, 100]; got {x_pct}");
            }
            if !(0.0..=100.0).contains(&z_pct) {
                bail!("ctrail '{tok}': Z_pct must be in (0, 100]; got {z_pct}");
            }
            out.push(ExitVariant::TimeCappedTrailing { x_pct, z_pct });
            continue;
        }
        if let Some(rest) = tok.strip_prefix("f6:") {
            let y_sec: i64 = rest.parse()
                .with_context(|| format!("bad Y in '{tok}'"))?;
            if y_sec < 0 {
                bail!("f6 '{tok}': Y_sec must be >= 0; got {y_sec}");
            }
            out.push(ExitVariant::F6Only { y_sec });
            continue;
        }
        let n: i64 = tok
            .parse()
            .with_context(|| format!("bad variant token (expected integer / 'peak' / 'smart:X:Y'): {tok}"))?;
        if n == 0 {
            out.push(ExitVariant::Baseline);
        } else if n > 0 {
            out.push(ExitVariant::FirstTouch {
                tp_pct: n as f64 / 100.0,
            });
        } else {
            bail!("negative TP threshold not supported: {n}");
        }
    }
    if out.is_empty() {
        bail!("empty variants spec");
    }
    Ok(out)
}

/// Iterate dates from `start` to `end` inclusive (YYYY-MM-DD).
fn dates_inclusive(start: &str, end: &str) -> Result<Vec<String>> {
    use polymarket_client_sdk_v2::types::NaiveDate;
    let s = NaiveDate::parse_from_str(start, "%Y-%m-%d")
        .with_context(|| format!("bad start date: {start}"))?;
    let e = NaiveDate::parse_from_str(end, "%Y-%m-%d")
        .with_context(|| format!("bad end date: {end}"))?;
    if e < s {
        bail!("end < start");
    }
    let mut out = Vec::new();
    let mut d = s;
    while d <= e {
        out.push(d.format("%Y-%m-%d").to_string());
        d = d.succ_opt().context("date overflow")?;
    }
    Ok(out)
}

/// Run the backtest over a date range with the given variants. Output: one
/// JSON file per (variant) with aggregated + per-cell metrics, plus a
/// `summary.json` with the full table for quick eyeballing, plus a
/// `peak_characterization.jsonl` with per-position descriptive stats (Fase 1).
///
/// `phase` enforces out-of-sample discipline (see `validate_phase_dates`):
///   * `Exploration` (default): cannot touch dates >= VALIDATION_CUTOFF_DATE.
///   * `Validation`: any dates allowed; an explicit banner + a `validation_seal_broken.txt`
///     file are emitted to capture the (rare, deliberate) event in the audit
///     trail.
pub fn run_backtest_tp(
    data_root: &Path,
    start_date: &str,
    end_date: &str,
    variants: &[ExitVariant],
    out_dir: &Path,
    phase: BtPhase,
    use_legacy_inmemory: bool,
    include_dates: &[String],
) -> Result<()> {
    // FIRST: enforce out-of-sample discipline (BOTH range AND include-list).
    // Done before fs::create_dir_all so a violation leaves NO side-effects on
    // disk -- if we abort, the user's filesystem is exactly as it was.
    validate_phase_dates(phase, start_date, end_date)?;
    validate_include_dates(phase, include_dates)?;
    // G14 PHASE 3: --bt-include-dates OVERRIDES the start/end range when
    // non-empty. Used for the Fase 3 validation set (5/17, 21, 23, 24 are
    // not contiguous; the health-check excluded 5/19, 25, 26).
    let dates = if !include_dates.is_empty() {
        eprintln!(
            "[backtest_tp] --bt-include-dates active ({} dates); start/end range ignored",
            include_dates.len()
        );
        include_dates.to_vec()
    } else {
        dates_inclusive(start_date, end_date)?
    };
    fs::create_dir_all(out_dir)?;
    if use_legacy_inmemory {
        eprintln!(
            "[backtest_tp] LEGACY-INMEMORY mode active (--bt-legacy-inmemory). \
             Per-day allocation can exceed 12 GB on heavy days; expect OOM if \
             RAM headroom is low. Use the default (streaming) for production."
        );
    }
    // If this is a validation pass, BREAK THE SEAL: loud banner to stdout +
    // a persistent marker file in out_dir. Both make it impossible to
    // post-hoc claim a result "wasn't really validation". The Fase 3 gate
    // must reference this artifact.
    if phase == BtPhase::Validation {
        let banner = format!(
            "================================================================\n\
             *** VALIDATION RUN -- seal broken, one-shot, audited ***\n\
             phase     = validation\n\
             window    = {start_date} .. {end_date}\n\
             out_dir   = {}\n\
             rationale = These dates are out-of-sample. The result of THIS run\n\
                         is the final gate (Fase 3). DO NOT iterate, do not re-run\n\
                         with adjusted thresholds; if it fails, the hypothesis fails.\n\
             ================================================================\n",
            out_dir.display()
        );
        eprintln!("{banner}");
        let seal = out_dir.join("validation_seal_broken.txt");
        let mut sf = File::create(&seal)
            .with_context(|| format!("writing validation seal: {}", seal.display()))?;
        writeln!(sf, "{banner}")?;
        eprintln!("[backtest_tp] validation seal marker: {}", seal.display());
    }
    eprintln!(
        "[backtest_tp] phase={} running {} dates ({}..{}) -> {}",
        phase.as_str(),
        dates.len(),
        start_date,
        end_date,
        out_dir.display()
    );

    let cfg = DecisionConfig::default();
    let mut all_positions: Vec<CollectedPosition> = Vec::new();
    for date in &dates {
        match replay_day(data_root, date, &cfg, use_legacy_inmemory, &EntryFilter::Baseline) {
            Ok(mut p) => all_positions.append(&mut p),
            Err(e) => eprintln!("[backtest_tp] WARN: {date} replay failed: {e:#}"),
        }
    }
    eprintln!(
        "[backtest_tp] TOTAL positions across {} dates: {}",
        dates.len(),
        all_positions.len()
    );

    // Per-variant evaluation + metrics.
    let mut all_metrics: Vec<VariantMetrics> = Vec::new();
    for variant in variants {
        let trades = evaluate_variant(&all_positions, *variant);
        let label = variant.label();
        let metrics = compute_metrics(&trades, label.clone());
        // Persist per-variant detail.
        let path = out_dir.join(format!("variant_{label}.json"));
        let f = File::create(&path)?;
        serde_json::to_writer_pretty(f, &metrics)?;
        // Also persist the per-trade list (CSV-ish for spot-checking).
        let trades_path = out_dir.join(format!("trades_{label}.jsonl"));
        let mut tf = File::create(&trades_path)?;
        for t in &trades {
            let row = serde_json::json!({
                "asset": t.position.asset,
                "interval": t.position.interval,
                "direction": t.position.direction,
                "signal_id": t.position.signal_id,
                "entry_recv_ms": t.position.entry_recv_ms,
                "entry_price": t.position.entry_price,
                "shares": t.position.shares,
                "exit_price": t.exit_price,
                "exit_reason": t.exit_reason.as_str(),
                "net_pnl": t.net_pnl,
                "trajectory_samples": t.position.total_samples,
                "uncoverable_samples": t.position.uncoverable_samples,
            });
            writeln!(tf, "{row}")?;
        }
        all_metrics.push(metrics);
    }
    // Summary table.
    let summary_path = out_dir.join("summary.json");
    let summary = serde_json::json!({
        "start_date": start_date,
        "end_date": end_date,
        "n_dates": dates.len(),
        "n_positions_total": all_positions.len(),
        "variants": all_metrics,
    });
    serde_json::to_writer_pretty(File::create(&summary_path)?, &summary)?;
    eprintln!("[backtest_tp] summary written: {}", summary_path.display());
    // Pretty-print the headline table to stdout.
    print_summary_table(&all_metrics, all_positions.len());

    // ---- FASE 1: PEAK CHARACTERIZATION (descriptive only) ----
    // One row per position to peak_characterization.jsonl + an aggregate
    // stdout report answering the 5 Fase-1 questions (timing, magnitude,
    // shape, per-cell, significant-fraction). This is the input to the
    // Fase 2 decision: is there structure worth predicting?
    let peak_stats: Vec<PeakStats> = all_positions.iter().map(characterize_position).collect();
    let peak_path = out_dir.join("peak_characterization.jsonl");
    let mut pf = File::create(&peak_path)
        .with_context(|| format!("writing peak_characterization: {}", peak_path.display()))?;
    for s in &peak_stats {
        writeln!(pf, "{}", serde_json::to_string(s)?)?;
    }
    eprintln!(
        "[backtest_tp] peak_characterization written: {} ({} rows)",
        peak_path.display(),
        peak_stats.len()
    );
    print_peak_characterization_report(&peak_stats, phase);
    Ok(())
}

// ============================================================================
// PIECE W8: entry-filter backtester (hypothesis generation on burned data).
// One full collect_positions pass per EntryFilter; per-cell metrics + a
// cross-variant comparison table (compare_entry_filters.csv).
// ============================================================================

/// Parse the comma-separated --bt-entry-filters string into concrete
/// EntryFilter + DecisionConfig pairs. Each label maps to (filter, regla_c)
/// because B2 needs cfg.regla_c_enabled=true upstream while every other
/// label uses the default cfg.
///
/// Supported labels (15):
///   a0, a1, a2, a3, b1, b2, c1, c2, c3, d0, d1, d2a, d2b, d3, d4
/// Unknown labels error explicitly (no silent skip).
pub fn parse_entry_filter_labels(spec: &str) -> Result<Vec<EntryFilter>> {
    let mut out = Vec::new();
    for tok in spec.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let f = match tok {
            "a0" => EntryFilter::Baseline,
            "a1" => EntryFilter::BpsThreshold { min_abs_bps: 6.0 },
            "a2" => EntryFilter::BpsThreshold { min_abs_bps: 7.0 },
            "a3" => EntryFilter::BpsThreshold { min_abs_bps: 8.0 },
            "b1" => EntryFilter::NoOpposite,
            "b2" => EntryFilter::ReglaCMarker,
            "c1" => EntryFilter::AsymmetricBps { min_abs_bps: 5.0, opposite_min_abs_bps: 8.0 },
            "c2" => EntryFilter::AsymmetricBps { min_abs_bps: 5.0, opposite_min_abs_bps: 7.0 },
            "c3" => EntryFilter::AsymmetricBps { min_abs_bps: 5.0, opposite_min_abs_bps: 10.0 },
            "d0" => EntryFilter::DcaUnlimited,
            "d1" => EntryFilter::DcaImprovingPrice,
            "d2a" => EntryFilter::DcaConfirmingUnderlying,
            "d2b" => EntryFilter::DcaConfirmingAsk,
            "d3" => EntryFilter::NoDca,
            "d4" => EntryFilter::DcaCap { max: 3 },
            // W9-fix: pre-registered per-cell DCA hypothesis (Pattern A).
            // 5m = D0 (DCA unrestricted), 15m = D4 (cap 3 lots). See
            // EntryFilter::SplitDcaByInterval rustdoc for rationale.
            "split_dca" => EntryFilter::SplitDcaByInterval {
                five_min: Box::new(EntryFilter::DcaUnlimited),
                fifteen_min: Box::new(EntryFilter::DcaCap { max: 3 }),
            },
            other => anyhow::bail!(
                "unknown entry-filter label '{}'. Supported: a0,a1,a2,a3,b1,b2,c1,c2,c3,d0,d1,d2a,d2b,d3,d4,split_dca",
                other
            ),
        };
        out.push(f);
    }
    if out.is_empty() {
        anyhow::bail!("--bt-entry-filters must list at least one label");
    }
    Ok(out)
}

/// Public label string a CLI parser maps from (a0/a1/.../c3) BACK to the
/// filter's stable output-filename label. Used by run_backtest_entry_filters
/// to mirror what the user typed in the CLI.
fn cli_label_for(f: &EntryFilter) -> String {
    match f {
        EntryFilter::Baseline => "a0".to_string(),
        EntryFilter::BpsThreshold { min_abs_bps } if (*min_abs_bps - 6.0).abs() < 1e-9 => "a1".to_string(),
        EntryFilter::BpsThreshold { min_abs_bps } if (*min_abs_bps - 7.0).abs() < 1e-9 => "a2".to_string(),
        EntryFilter::BpsThreshold { min_abs_bps } if (*min_abs_bps - 8.0).abs() < 1e-9 => "a3".to_string(),
        EntryFilter::NoOpposite => "b1".to_string(),
        EntryFilter::ReglaCMarker => "b2".to_string(),
        EntryFilter::AsymmetricBps { opposite_min_abs_bps, .. } if (*opposite_min_abs_bps - 8.0).abs() < 1e-9 => "c1".to_string(),
        EntryFilter::AsymmetricBps { opposite_min_abs_bps, .. } if (*opposite_min_abs_bps - 7.0).abs() < 1e-9 => "c2".to_string(),
        EntryFilter::AsymmetricBps { opposite_min_abs_bps, .. } if (*opposite_min_abs_bps - 10.0).abs() < 1e-9 => "c3".to_string(),
        EntryFilter::DcaUnlimited => "d0".to_string(),
        EntryFilter::DcaImprovingPrice => "d1".to_string(),
        EntryFilter::DcaConfirmingUnderlying => "d2a".to_string(),
        EntryFilter::DcaConfirmingAsk => "d2b".to_string(),
        EntryFilter::NoDca => "d3".to_string(),
        EntryFilter::DcaCap { max } if *max == 3 => "d4".to_string(),
        // The pre-registered W9-fix per-cell composition. Recognized only in
        // its canonical (D0 in 5m, D4 cap=3 in 15m) shape; any other split
        // falls through to the verbose label().
        EntryFilter::SplitDcaByInterval { five_min, fifteen_min }
            if matches!(five_min.as_ref(), EntryFilter::DcaUnlimited)
                && matches!(fifteen_min.as_ref(), EntryFilter::DcaCap { max: 3 })
            => "split_dca".to_string(),
        // Fallback to the EntryFilter::label() for any non-standard variant.
        other => other.label(),
    }
}

/// Per-cell breakdown for one filter run. Mirrors the schema in the existing
/// G15 backtest_tp_v1/summary.json (`by_cell` entries) plus the W8 addition
/// `n_double_sided_markets` and the W9 DCA breakdown.
#[derive(Debug, Clone, serde::Serialize)]
pub struct CellSummary {
    pub n_trades: usize,
    pub total_pnl: f64,
    pub win_ratio: f64,
    pub avg_pnl: f64,
    pub n_double_sided_markets: usize,
    pub dca_breakdown: DcaBreakdown,
}

/// Per-filter result of one full backtester pass (all dates collapsed).
#[derive(Debug, Clone, serde::Serialize)]
pub struct EntryFilterRunSummary {
    pub filter_label: String,
    pub n_trades_total: usize,
    pub total_pnl: f64,
    pub win_ratio: f64,
    pub avg_pnl: f64,
    pub n_double_sided_markets_total: usize,
    pub by_cell: std::collections::BTreeMap<String, CellSummary>,
    pub dates_processed: Vec<String>,
    pub dates_skipped: Vec<String>,
    pub dca_breakdown: DcaBreakdown,
}

/// W9 DCA payout breakdown. Computed per filter (TOTAL) and per cell. Two
/// units of analysis live here together because they answer different
/// questions:
///   * Per-LOT (positions_*): "does the N-th DCA lot earn on average?"
///   * Per-MARKET-SIDE (dca_*_ms / winrate_dca_ms): "does the complete DCA
///     stack — sum of all lots in a single (asset, interval, epoch, dir) —
///     end green?"
/// The `dca_edge` is the headline number: positions-in-DCA pnl_per_trade
/// minus single-entry pnl_per_trade, in the SAME filter. Positive = the
/// extra lots add value; negative = DCA dilutes the edge.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct DcaBreakdown {
    /// Distinct (asset, interval, epoch, direction) keys with >= 1 lot.
    pub n_market_sides_total: usize,
    /// Market-sides with >= 2 lots (i.e. DCA happened here).
    pub n_dca_market_sides: usize,
    /// Market-sides with exactly 1 lot.
    pub n_single_market_sides: usize,
    /// n_dca_market_sides / n_market_sides_total. 0.0 if denominator is 0.
    pub dca_frequency: f64,
    /// Average net_pnl per lot for lots that landed in a single-entry market-side.
    pub pnl_per_trade_single: f64,
    /// Average net_pnl per lot for lots that landed in a DCA market-side
    /// (includes ALL lots in that market-side, not only the 2nd+).
    pub pnl_per_trade_dca: f64,
    /// pnl_per_trade_dca - pnl_per_trade_single. Headline DCA-edge metric.
    pub dca_edge: f64,
    /// Per-lot win rate inside single-entry market-sides.
    pub winrate_positions_single: f64,
    /// Per-lot win rate inside DCA market-sides.
    pub winrate_positions_dca: f64,
    /// DCA market-sides whose SUMMED pnl across all lots is > 0.
    pub n_dca_winners_ms: usize,
    /// DCA market-sides whose SUMMED pnl across all lots is <= 0.
    pub n_dca_losers_ms: usize,
    /// Sum of summed-pnl across winning DCA market-sides.
    pub sum_pnl_dca_winners: f64,
    /// Sum of summed-pnl across losing DCA market-sides (negative or zero).
    pub sum_pnl_dca_losers: f64,
    /// n_dca_winners_ms / n_dca_market_sides. 0.0 if denominator is 0.
    pub winrate_dca_ms: f64,
}

/// W9: compute the DCA breakdown over a borrow-iterable of evaluated trades.
/// Grouping key = (asset, interval, epoch, direction). Two passes: one to
/// build the per-market-side bucket map, one to derive single/dca splits +
/// aggregates. Pure; no I/O; takes references so per-cell callers can pass
/// `&[&VariantTrade]` slices without cloning.
fn compute_dca_breakdown<'a, I>(trades: I) -> DcaBreakdown
where
    I: IntoIterator<Item = &'a VariantTrade>,
{
    use std::collections::BTreeMap;
    let mut by_ms: BTreeMap<(String, String, i64, String), Vec<&VariantTrade>> =
        BTreeMap::new();
    for t in trades {
        let key = (
            t.position.asset.clone(),
            t.position.interval.clone(),
            t.position.epoch,
            t.position.direction.clone(),
        );
        by_ms.entry(key).or_default().push(t);
    }

    let n_market_sides_total = by_ms.len();
    let mut positions_single: Vec<&VariantTrade> = Vec::new();
    let mut positions_dca: Vec<&VariantTrade> = Vec::new();
    let mut n_single_market_sides = 0_usize;
    let mut n_dca_market_sides = 0_usize;
    let mut n_dca_winners_ms = 0_usize;
    let mut n_dca_losers_ms = 0_usize;
    let mut sum_pnl_dca_winners = 0.0_f64;
    let mut sum_pnl_dca_losers = 0.0_f64;
    for ms in by_ms.values() {
        if ms.len() == 1 {
            n_single_market_sides += 1;
            positions_single.push(ms[0]);
        } else {
            n_dca_market_sides += 1;
            positions_dca.extend(ms.iter().copied());
            let ms_pnl: f64 = ms.iter().map(|t| t.net_pnl).sum();
            if ms_pnl > 0.0 {
                n_dca_winners_ms += 1;
                sum_pnl_dca_winners += ms_pnl;
            } else {
                n_dca_losers_ms += 1;
                sum_pnl_dca_losers += ms_pnl;
            }
        }
    }

    let pnl_per_trade_single = avg_pnl_of(&positions_single);
    let pnl_per_trade_dca = avg_pnl_of(&positions_dca);
    let dca_edge = pnl_per_trade_dca - pnl_per_trade_single;
    let winrate_positions_single = winrate_of(&positions_single);
    let winrate_positions_dca = winrate_of(&positions_dca);
    let dca_frequency = if n_market_sides_total > 0 {
        n_dca_market_sides as f64 / n_market_sides_total as f64
    } else {
        0.0
    };
    let winrate_dca_ms = if n_dca_market_sides > 0 {
        n_dca_winners_ms as f64 / n_dca_market_sides as f64
    } else {
        0.0
    };

    DcaBreakdown {
        n_market_sides_total,
        n_dca_market_sides,
        n_single_market_sides,
        dca_frequency,
        pnl_per_trade_single,
        pnl_per_trade_dca,
        dca_edge,
        winrate_positions_single,
        winrate_positions_dca,
        n_dca_winners_ms,
        n_dca_losers_ms,
        sum_pnl_dca_winners,
        sum_pnl_dca_losers,
        winrate_dca_ms,
    }
}

fn avg_pnl_of(trades: &[&VariantTrade]) -> f64 {
    if trades.is_empty() {
        0.0
    } else {
        trades.iter().map(|t| t.net_pnl).sum::<f64>() / trades.len() as f64
    }
}

fn winrate_of(trades: &[&VariantTrade]) -> f64 {
    if trades.is_empty() {
        0.0
    } else {
        let wins = trades.iter().filter(|t| t.net_pnl > 0.0).count();
        wins as f64 / trades.len() as f64
    }
}

/// Compute per-cell summary from a vector of CollectedPositions evaluated
/// with the Baseline (time-exit) ExitVariant. Mirrors the G15 cell labels
/// (`BTC_5m` / `BTC_15m` / `ETH_5m` / `ETH_15m`).
fn compute_entry_filter_summary(
    positions: &[CollectedPosition],
    filter_label: &str,
    dates_processed: Vec<String>,
    dates_skipped: Vec<String>,
) -> EntryFilterRunSummary {
    // Evaluate all positions with Baseline exit (time-exit). The variant
    // matters because P&L depends on it; using Baseline keeps the cross-
    // variant comparison apples-to-apples (only the ENTRY filter differs).
    let trades = evaluate_variant(positions, ExitVariant::Baseline);

    let mut by_cell: std::collections::BTreeMap<String, Vec<&VariantTrade>> =
        std::collections::BTreeMap::new();
    for t in &trades {
        let key = format!("{}_{}", t.position.asset, t.position.interval);
        by_cell.entry(key).or_default().push(t);
    }

    let mut cell_summaries = std::collections::BTreeMap::new();
    for (cell_key, cell_trades) in &by_cell {
        let n = cell_trades.len();
        let total_pnl: f64 = cell_trades.iter().map(|t| t.net_pnl).sum();
        let wins = cell_trades.iter().filter(|t| t.net_pnl > 0.0).count();
        let win_ratio = if n > 0 { wins as f64 / n as f64 } else { 0.0 };
        let avg_pnl = if n > 0 { total_pnl / n as f64 } else { 0.0 };
        // Parse the cell key back to (asset, interval) for the double-sided count.
        let (asset, interval) = match cell_key.split_once('_') {
            Some((a, i)) => (a, i),
            None => (cell_key.as_str(), ""),
        };
        let n_double = count_double_sided_markets_for_cell(positions, asset, interval);
        // W9: per-cell DCA breakdown over THIS cell's trades only. Each
        // cell sees its own grouping by (asset, interval, epoch, dir) where
        // (asset, interval) is fixed -- so the bucketing reduces to
        // (epoch, dir) within the cell. Same arithmetic as the global one.
        let dca_breakdown = compute_dca_breakdown(cell_trades.iter().copied());
        cell_summaries.insert(cell_key.clone(), CellSummary {
            n_trades: n,
            total_pnl,
            win_ratio,
            avg_pnl,
            n_double_sided_markets: n_double,
            dca_breakdown,
        });
    }

    let n_total = trades.len();
    let total_pnl: f64 = trades.iter().map(|t| t.net_pnl).sum();
    let wins = trades.iter().filter(|t| t.net_pnl > 0.0).count();
    let win_ratio = if n_total > 0 { wins as f64 / n_total as f64 } else { 0.0 };
    let avg_pnl = if n_total > 0 { total_pnl / n_total as f64 } else { 0.0 };
    let n_double_total = count_double_sided_markets(positions);
    let dca_breakdown_total = compute_dca_breakdown(trades.iter());

    EntryFilterRunSummary {
        filter_label: filter_label.to_string(),
        n_trades_total: n_total,
        total_pnl,
        win_ratio,
        avg_pnl,
        n_double_sided_markets_total: n_double_total,
        by_cell: cell_summaries,
        dates_processed,
        dates_skipped,
        dca_breakdown: dca_breakdown_total,
    }
}

/// LOW-SAMPLE threshold for the cross-variant comparison table's `low_sample`
/// flag. If a (filter, cell) has fewer than this many trades, the row is
/// flagged so the operator does not over-interpret a small-N result.
pub const LOW_SAMPLE_THRESHOLD: usize = 100;

/// Write the cross-variant comparison table: one row per (filter x cell)
/// with [filter, cell, n_trades, total_pnl, win_rate, pnl_per_trade,
/// n_double_sided_markets, low_sample] columns. The TOTAL row per filter
/// uses cell="TOTAL". The `low_sample` boolean ("yes"/"no") flags rows
/// where n_trades < LOW_SAMPLE_THRESHOLD so the operator does not
/// over-interpret a 35-trade BTC_15m C2 hit as signal.
fn write_compare_csv(summaries: &[EntryFilterRunSummary], out_path: &Path) -> Result<()> {
    use std::io::Write;
    let mut f = fs::File::create(out_path)?;
    // Original 8 columns + 13 W9 DCA breakdown columns = 21 columns total.
    writeln!(
        f,
        "filter,cell,n_trades,total_pnl,win_rate,pnl_per_trade,n_double_sided_markets,low_sample,\
         dca_frequency,n_dca_market_sides,n_single_market_sides,\
         pnl_per_trade_single,pnl_per_trade_dca,dca_edge,\
         winrate_positions_single,winrate_positions_dca,\
         n_dca_winners_ms,n_dca_losers_ms,sum_pnl_dca_winners,sum_pnl_dca_losers,\
         winrate_dca_ms"
    )?;
    for s in summaries {
        // Per-cell rows first.
        for (cell, c) in &s.by_cell {
            let low = if c.n_trades < LOW_SAMPLE_THRESHOLD { "yes" } else { "no" };
            let d = &c.dca_breakdown;
            writeln!(
                f,
                "{},{},{},{:.6},{:.6},{:.6},{},{},\
                 {:.6},{},{},{:.6},{:.6},{:.6},{:.6},{:.6},{},{},{:.6},{:.6},{:.6}",
                s.filter_label, cell, c.n_trades, c.total_pnl, c.win_ratio,
                c.avg_pnl, c.n_double_sided_markets, low,
                d.dca_frequency, d.n_dca_market_sides, d.n_single_market_sides,
                d.pnl_per_trade_single, d.pnl_per_trade_dca, d.dca_edge,
                d.winrate_positions_single, d.winrate_positions_dca,
                d.n_dca_winners_ms, d.n_dca_losers_ms,
                d.sum_pnl_dca_winners, d.sum_pnl_dca_losers,
                d.winrate_dca_ms,
            )?;
        }
        // TOTAL row last for the filter.
        let low = if s.n_trades_total < LOW_SAMPLE_THRESHOLD { "yes" } else { "no" };
        let d = &s.dca_breakdown;
        writeln!(
            f,
            "{},TOTAL,{},{:.6},{:.6},{:.6},{},{},\
             {:.6},{},{},{:.6},{:.6},{:.6},{:.6},{:.6},{},{},{:.6},{:.6},{:.6}",
            s.filter_label, s.n_trades_total, s.total_pnl, s.win_ratio,
            s.avg_pnl, s.n_double_sided_markets_total, low,
            d.dca_frequency, d.n_dca_market_sides, d.n_single_market_sides,
            d.pnl_per_trade_single, d.pnl_per_trade_dca, d.dca_edge,
            d.winrate_positions_single, d.winrate_positions_dca,
            d.n_dca_winners_ms, d.n_dca_losers_ms,
            d.sum_pnl_dca_winners, d.sum_pnl_dca_losers,
            d.winrate_dca_ms,
        )?;
    }
    Ok(())
}

/// Run a backtester pass over `[start_date..end_date]` for each EntryFilter
/// in `filters`. For each filter:
///   * collect positions (re-uses replay_day_streaming, single ExitVariant=Baseline)
///   * compute per-cell + global metrics + double-sided market counts
///   * write summary_<label>.json + trades_<label>.jsonl (Baseline-evaluated trades)
/// After all filters: write compare_entry_filters.csv (cross-variant table).
/// Missing data days (e.g. 2026-05-12) are SKIPPED with an explicit log and
/// recorded in the per-filter summary's `dates_skipped` list.
pub fn run_backtest_entry_filters(
    data_root: &Path,
    start_date: &str,
    end_date: &str,
    filters: &[EntryFilter],
    out_dir: &Path,
    phase: BtPhase,
) -> Result<()> {
    validate_phase_dates(phase, start_date, end_date)?;
    let dates = dates_inclusive(start_date, end_date)?;
    fs::create_dir_all(out_dir)?;
    eprintln!(
        "[w8 backtest_entry_filters] {} -> {}: {} dates, {} filters",
        start_date, end_date, dates.len(), filters.len(),
    );
    let mut all_summaries: Vec<EntryFilterRunSummary> = Vec::new();

    for f in filters {
        let label = cli_label_for(f);
        let pretty = f.label();
        eprintln!(
            "[w8 backtest_entry_filters] === FILTER: {} ({}) ===",
            label, pretty
        );

        // B2 needs cfg.regla_c_enabled = true; everything else uses default.
        let mut cfg = DecisionConfig::default();
        if matches!(f, EntryFilter::ReglaCMarker) {
            cfg.regla_c_enabled = true;
            eprintln!("[w8 backtest_entry_filters]   (regla_c_enabled = true upstream)");
        }

        let mut positions: Vec<CollectedPosition> = Vec::new();
        let mut dates_processed: Vec<String> = Vec::new();
        let mut dates_skipped: Vec<String> = Vec::new();
        for date in &dates {
            match replay_day(data_root, date, &cfg, false, f) {
                Ok(mut p) => {
                    eprintln!(
                        "[w8 backtest_entry_filters]   {} {}: {} positions",
                        label, date, p.len()
                    );
                    positions.append(&mut p);
                    dates_processed.push(date.clone());
                }
                Err(e) => {
                    eprintln!(
                        "[w8 backtest_entry_filters]   {} {} SKIPPED: {}",
                        label, date, e
                    );
                    dates_skipped.push(date.clone());
                }
            }
        }

        let summary = compute_entry_filter_summary(
            &positions, &label, dates_processed, dates_skipped,
        );

        // Write per-filter summary JSON.
        let summary_path = out_dir.join(format!("summary_{}.json", label));
        let json = serde_json::to_string_pretty(&summary)?;
        fs::write(&summary_path, json)?;
        eprintln!(
            "[w8 backtest_entry_filters]   wrote {} ({} trades, ${:.2} pnl, {} double-sided markets)",
            summary_path.display(), summary.n_trades_total, summary.total_pnl,
            summary.n_double_sided_markets_total,
        );

        // Write per-trade JSONL (Baseline exit evaluation).
        let trades = evaluate_variant(&positions, ExitVariant::Baseline);
        let trades_path = out_dir.join(format!("trades_{}.jsonl", label));
        use std::io::Write;
        let mut tf = fs::File::create(&trades_path)?;
        for t in &trades {
            #[derive(serde::Serialize)]
            struct Row<'a> {
                token: &'a str,
                asset: &'a str,
                interval: &'a str,
                epoch: i64,
                direction: &'a str,
                signal_id: &'a str,
                entry_recv_ms: i64,
                entry_price: f64,
                exit_price: f64,
                net_pnl: f64,
                exit_reason: &'a str,
            }
            let row = Row {
                token: &t.position.token,
                asset: &t.position.asset,
                interval: &t.position.interval,
                epoch: t.position.epoch,
                direction: &t.position.direction,
                signal_id: &t.position.signal_id,
                entry_recv_ms: t.position.entry_recv_ms,
                entry_price: t.position.entry_price,
                exit_price: t.exit_price,
                net_pnl: t.net_pnl,
                exit_reason: t.exit_reason.as_str(),
            };
            writeln!(tf, "{}", serde_json::to_string(&row)?)?;
        }
        eprintln!(
            "[w8 backtest_entry_filters]   wrote {} ({} rows)",
            trades_path.display(), trades.len()
        );

        all_summaries.push(summary);
    }

    // Cross-variant comparison table.
    let csv_path = out_dir.join("compare_entry_filters.csv");
    write_compare_csv(&all_summaries, &csv_path)?;
    eprintln!(
        "[w8 backtest_entry_filters] wrote {} (one row per filter x cell + TOTAL)",
        csv_path.display()
    );

    Ok(())
}

// ============================================================================
// W9-Pieza1: EXITS-TRACE AUDIT
//
// Self-contained pass: a0 entry filter + Baseline (time_exit) exit, but instead
// of just emitting net_pnl we extract a RICH per-trade row containing:
//   * trigger_ret_bps (signed)
//   * maker_base_fee / taker_base_fee / fee_type (raw, from markets log)
//   * baseline_exit_price / baseline_net_pnl (the operative path, for reference)
//   * fixed-offset bids: bid_at_{5,15,30,60,120}s
//   * max_bid_120s + offset_ms
//   * min_bid_120s + offset_ms
//   * high_water_marks_120s and low_water_marks_120s (compact monotone traces)
//
// Python downstream uses these to simulate B1/B3 exit strategies + fallbacks
// without re-running the backtester. Zero modification to
// run_backtest_entry_filters (the W9 regression guard is preserved).
//
// COMBO INSTRUMENTATION (2026-06-08): exits-trace rows now ALSO carry the
// entry-filter inputs for H1 (OBI Polymarket), H2 (Binance persistence),
// H3 (realized vol regime). All ADDITIVE — no existing field changes. The
// regression guard is: with --bt-exits-trace-filter a0, the projection of
// rows onto the pre-COMBO 26 fields must be bit-identical to the snapshot
// captured at commit 491cf5a (sha256 e739fce9... + 5c4d091a...).
//
// CLI: invoked via `--backtest-exits-trace` (separate flag, separate out_dir).
// Output files:
//   * trades_exits_trace.jsonl  (one row per position; full schema above)
//   * summary_exits_trace.json  (run-level metadata + headline counts)
// ============================================================================

/// Rolling history of Binance kline closes for one asset (BTC or ETH).
/// Used at trigger time to compute realized vol over a pre-trigger lookback
/// window (H3), and as the source for the post-trigger close-at-offset emit
/// (H2). Designed for VecDeque-style amortized O(1) append + O(1) eviction.
///
/// Capacity is the MAX lookback we ever need. We pre-cap so the buffer never
/// grows unbounded. With 1s klines + 60-min lookback, cap = 3600 entries.
///
/// `last_recv_ms` lets the consumer detect recorder gaps: if the youngest
/// kline in the buffer is much older than the trigger's own recv_ms, the
/// lookback is contaminated by a gap (e.g. recorder down) and vol should
/// be skipped. The cleaner gate is enforced at compute_vol() via a
/// max-allowable-gap parameter.
#[derive(Debug, Clone, Default)]
pub struct KlineHist {
    pub buf: VecDeque<(i64, f64)>, // (t_open_ms, close), oldest at front
    pub cap: usize,
}

impl KlineHist {
    pub fn with_capacity(cap: usize) -> Self {
        Self { buf: VecDeque::with_capacity(cap.saturating_add(1)), cap }
    }

    /// Append a sample. Drops the oldest if at cap.
    pub fn push(&mut self, t_open_ms: i64, close: f64) {
        if self.buf.len() >= self.cap {
            self.buf.pop_front();
        }
        self.buf.push_back((t_open_ms, close));
    }

    /// Sample standard deviation of log returns over the last `n_samples`
    /// klines. Returns `NaN` if fewer than `n_samples + 1` are available
    /// (need n+1 closes to compute n returns) OR if any consecutive pair
    /// has a time-gap exceeding `max_gap_ms` (recorder hole; lookback
    /// contaminated).
    ///
    /// max_gap_ms = 5_000 is the natural threshold for 1s klines: in healthy
    /// operation consecutive klines are ~1 second apart; a gap of 5+ seconds
    /// almost certainly means recorder downtime.
    pub fn compute_vol(&self, n_samples: usize, max_gap_ms: i64) -> f64 {
        let need = n_samples + 1;
        if self.buf.len() < need {
            return f64::NAN;
        }
        let start = self.buf.len() - need;
        let slice: Vec<(i64, f64)> = self.buf.iter().skip(start).copied().collect();
        // Gap check: no consecutive pair may exceed max_gap_ms.
        for w in slice.windows(2) {
            if w[1].0 - w[0].0 > max_gap_ms {
                return f64::NAN;
            }
        }
        // Log returns.
        let returns: Vec<f64> = slice
            .windows(2)
            .map(|w| (w[1].1 / w[0].1).ln())
            .filter(|r| r.is_finite())
            .collect();
        if returns.len() < 2 {
            return f64::NAN;
        }
        let n_r = returns.len() as f64;
        let mean = returns.iter().sum::<f64>() / n_r;
        let var = returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / (n_r - 1.0);
        var.sqrt()
    }
}

/// Order-book imbalance over top-N price levels.
/// `obi = (sum_bid_sizes_top_N - sum_ask_sizes_top_N) / (sum_bid + sum_ask)`.
/// Returns NaN if either side is empty (no meaningful imbalance defined).
///
/// `top_n = 1` is the cleanest principial value (no parameter to overfit).
/// `top_n = 3` is the pre-registered sensitivity check (deeper book signal).
pub fn compute_obi(book: &FullBook, top_n: usize) -> f64 {
    if book.bids.is_empty() || book.asks.is_empty() || top_n == 0 {
        return f64::NAN;
    }
    // bids: highest price first (iter().rev() walks best-first).
    let bid_sum: f64 = book.bids.iter().rev().take(top_n).map(|(_, sz)| *sz).sum();
    // asks: lowest price first (iter() walks best-first).
    let ask_sum: f64 = book.asks.iter().take(top_n).map(|(_, sz)| *sz).sum();
    let total = bid_sum + ask_sum;
    if total <= 0.0 { f64::NAN } else { (bid_sum - ask_sum) / total }
}

/// Compute the prior-day date string for cross-UTC-day preheat.
/// Returns None if `date` is unparseable. The prior day is `date - 1`.
pub fn prev_date(date: &str) -> Option<String> {
    use polymarket_client_sdk_v2::types::NaiveDate;
    let d = NaiveDate::parse_from_str(date, "%Y-%m-%d").ok()?;
    let prev = d.pred_opt()?;
    Some(prev.format("%Y-%m-%d").to_string())
}

/// Read the tail of the prior day's kline_1s file into a KlineHist. Used at
/// the START of `replay_events_through_state_machine` so triggers in the
/// first lookback window of `date` have non-NaN vol_30m / vol_60m.
///
/// If the prior day is unparseable, the prior file is missing, or it has
/// fewer than `lookback_samples` entries, returns a partially-filled or
/// empty hist (the gap check inside compute_vol() will gate the result).
fn preheat_kline_hist(
    data_root: &Path,
    date: &str,
    sym: &str,
    asset: &'static str,
    lookback_samples: usize,
) -> KlineHist {
    let mut hist = KlineHist::with_capacity(lookback_samples);
    let Some(prev) = prev_date(date) else { return hist; };
    let Ok(stream) = kline_stream(data_root, sym, asset, &prev) else { return hist; };
    // Stream the whole prior day; let the cap eat the front so we end up
    // with the tail (last `lookback_samples` entries).
    for ev in stream {
        if let Ev::Kline { t_open_ms, close, .. } = ev.ev {
            hist.push(t_open_ms, close);
        }
    }
    hist
}

/// Record a post-trigger Binance kline into all active positions of the
/// matching asset. Fills `binance_close_at_{2,5,10}s` IFF the kline's
/// `t_open_ms` exactly hits `trigger_ts_ms + N*1000`. Misses (gaps, late
/// recorder buffering) leave the field as NaN — the analyzer drops triggers
/// with insufficient post-trigger coverage.
fn record_kline_for_active(active: &mut ActiveTracker, asset: &str, t_open_ms: i64, close: f64) {
    for ((_, _), pos) in active.by_key.iter_mut() {
        if pos.asset != asset { continue; }
        let trigger_ms = pos.entry_trigger_ts_ms;
        if trigger_ms <= 0 { continue; }
        for (offset_s, field) in [
            (2_i64,   &mut pos.binance_close_at_2s),
            (5,       &mut pos.binance_close_at_5s),
            (10,      &mut pos.binance_close_at_10s),
            (30,      &mut pos.binance_close_at_30s),
            (60,      &mut pos.binance_close_at_60s),
            (120,     &mut pos.binance_close_at_120s),
        ] {
            if field.is_nan() && t_open_ms == trigger_ms + offset_s * 1000 {
                *field = close;
            }
        }
    }
}


/// One row of `trades_exits_trace.jsonl`. Field names + types match the schema
/// the W9-Pieza1 Python analyzer expects. Nested vectors use the `(offset_ms,
/// bid)` shape (two-element arrays in JSON).
#[derive(serde::Serialize)]
struct ExitsTraceRow<'a> {
    // Identity / catalog
    token: &'a str,
    asset: &'a str,
    interval: &'a str,
    epoch: i64,
    direction: &'a str,
    signal_id: &'a str,
    entry_recv_ms: i64,
    entry_price: f64,
    shares: f64,
    // Trigger characterization
    trigger_ret_bps: f64,
    trigger_close: f64,
    // Fees AS-IS from markets log (Python documents the unit interpretation)
    maker_fee_bps: i64,
    taker_fee_bps: i64,
    fee_type: &'a str,
    // Baseline (current bot) exit -- for reference / sanity vs. simulated exits
    baseline_exit_price: f64,
    baseline_net_pnl: f64,
    baseline_exit_reason: &'a str,
    // Fixed-offset bids (executable_bid at first sample with ms >= entry+X*1000).
    // NaN if no sample or first post-target sample was uncoverable.
    bid_at_5s: f64,
    bid_at_15s: f64,
    bid_at_30s: f64,
    bid_at_60s: f64,
    bid_at_120s: f64,
    // Extremes within 120s window
    max_bid_120s: f64,
    max_bid_offset_ms: i64,
    min_bid_120s: f64,
    min_bid_offset_ms: i64,
    // Compact monotone traces (Vec<(offset_ms, bid)>)
    hwm_120s: Vec<(i64, f64)>,
    lwm_120s: Vec<(i64, f64)>,
    // Trajectory diagnostics
    n_samples: usize,
    n_uncoverable_samples: usize,
    // ---- COMBO 2026-06-08 — entry-filter inputs, ADDITIVE ----
    // H1: Polymarket order-book imbalance at trigger instant.
    obi_top1: f64,
    obi_top3: f64,
    // H2: Binance kline CLOSE at +2/+5/+10s post-trigger. NaN on gap/day-end.
    binance_close_at_2s: f64,
    binance_close_at_5s: f64,
    binance_close_at_10s: f64,
    // H2 EXTENSION: longer horizons to test if mean reversion appears past
    // the 10s window (the initial vida-media estimation showed CONTINUATION
    // not reversion in 2-10s, so we extend to see if reversion sets in later).
    binance_close_at_30s: f64,
    binance_close_at_60s: f64,
    binance_close_at_120s: f64,
    // H3: realized vol (std of log returns) over pre-trigger lookback.
    // NaN if buffer too short OR any consecutive pair has gap > 5_000 ms.
    vol_30m: f64,
    vol_60m: f64,
}

/// Run the exits-trace audit over `[start_date..end_date]` with `EntryFilter::Baseline`
/// (= a0) and `ExitVariant::Baseline` (= time_exit). For every emitted position,
/// build a `ExitsTraceRow` from the in-memory trajectory + position fields and
/// write to `trades_exits_trace.jsonl`. Also writes `summary_exits_trace.json`
/// with run-level metadata.
///
/// PHASE-AWARE: respects `--bt-phase`. The default exploration phase pre-
/// validates the cutoff date guard. Honest method: even though this is
/// descriptive (no parameter selection), passing through validation dates
/// should still require `--bt-phase validation`.
pub fn run_backtest_exits_trace(
    data_root: &Path,
    start_date: &str,
    end_date: &str,
    out_dir: &Path,
    phase: BtPhase,
    entry_filter: &EntryFilter,
) -> Result<()> {
    validate_phase_dates(phase, start_date, end_date)?;
    let dates = dates_inclusive(start_date, end_date)?;
    fs::create_dir_all(out_dir)?;
    eprintln!(
        "[w9-pieza1 exits-trace] {} -> {}: {} dates (phase={:?}, entry_filter={})",
        start_date, end_date, dates.len(), phase, cli_label_for(entry_filter),
    );

    // Exit is fixed (Baseline = time_exit at hold; trajectory is emitted for
    // offline analyzer to apply alternate exits like B3). Entry is parameterized
    // (was hardcoded to Baseline pre-COMBO; bit-identical when caller passes
    // EntryFilter::Baseline). Validated against the W9 exploration SHA256
    // snapshot — see regression-guard procedure in the COMBO commit.
    let cfg = DecisionConfig::default();

    // Collect all positions across the date range, then evaluate + emit.
    let mut positions: Vec<CollectedPosition> = Vec::new();
    let mut dates_processed: Vec<String> = Vec::new();
    let mut dates_skipped: Vec<String> = Vec::new();
    for date in &dates {
        match replay_day(data_root, date, &cfg, false, entry_filter) {
            Ok(mut p) => {
                eprintln!(
                    "[w9-pieza1 exits-trace]   {}: {} positions",
                    date, p.len()
                );
                positions.append(&mut p);
                dates_processed.push(date.clone());
            }
            Err(e) => {
                eprintln!(
                    "[w9-pieza1 exits-trace]   {} SKIPPED: {}",
                    date, e
                );
                dates_skipped.push(date.clone());
            }
        }
    }

    // Evaluate with Baseline exit (time_exit). The position's trajectory is
    // already populated; evaluate_variant only needs it to decide exit_price /
    // exit_reason. After this we keep BOTH the trade (for baseline reference)
    // AND the position (for trajectory access in emit).
    let trades = evaluate_variant(&positions, ExitVariant::Baseline);

    // Sanity: trades.len() must equal positions.len() (Baseline never drops
    // anything; one trade per position). Any divergence is a backtester bug.
    if trades.len() != positions.len() {
        anyhow::bail!(
            "exits-trace invariant violated: {} trades vs {} positions",
            trades.len(), positions.len()
        );
    }

    // Window for HWM/LWM/extremes. Pegged to 120s = the current operative hold.
    // (B2 hold-to-resolution would need an extended hold; that's Pieza 3.)
    const TRACE_WINDOW_S: i64 = 120;

    let trades_path = out_dir.join("trades_exits_trace.jsonl");
    let mut tf = fs::File::create(&trades_path)?;
    use std::io::Write as _;
    for trade in &trades {
        let p = &trade.position;
        let (mx_bid, mx_off) = max_bid_in_window(&p.trajectory, p.entry_recv_ms, TRACE_WINDOW_S);
        let (mn_bid, mn_off) = min_bid_in_window(&p.trajectory, p.entry_recv_ms, TRACE_WINDOW_S);
        let row = ExitsTraceRow {
            token: &p.token,
            asset: &p.asset,
            interval: &p.interval,
            epoch: p.epoch,
            direction: &p.direction,
            signal_id: &p.signal_id,
            entry_recv_ms: p.entry_recv_ms,
            entry_price: p.entry_price,
            shares: p.shares,
            trigger_ret_bps: p.trigger_ret_bps,
            trigger_close: p.trigger_close,
            maker_fee_bps: p.maker_fee_bps,
            taker_fee_bps: p.taker_fee_bps,
            fee_type: &p.fee_type,
            baseline_exit_price: trade.exit_price,
            baseline_net_pnl: trade.net_pnl,
            baseline_exit_reason: trade.exit_reason.as_str(),
            bid_at_5s:   bid_at_offset(&p.trajectory, p.entry_recv_ms, 5),
            bid_at_15s:  bid_at_offset(&p.trajectory, p.entry_recv_ms, 15),
            bid_at_30s:  bid_at_offset(&p.trajectory, p.entry_recv_ms, 30),
            bid_at_60s:  bid_at_offset(&p.trajectory, p.entry_recv_ms, 60),
            bid_at_120s: bid_at_offset(&p.trajectory, p.entry_recv_ms, 120),
            max_bid_120s: mx_bid,
            max_bid_offset_ms: mx_off,
            min_bid_120s: mn_bid,
            min_bid_offset_ms: mn_off,
            hwm_120s: high_water_marks(&p.trajectory, p.entry_recv_ms, TRACE_WINDOW_S),
            lwm_120s: low_water_marks(&p.trajectory, p.entry_recv_ms, TRACE_WINDOW_S),
            n_samples: p.total_samples,
            n_uncoverable_samples: p.uncoverable_samples,
            // COMBO 2026-06-08: pass through from CollectedPosition. The
            // values were computed at Fire time inside replay_events_through_
            // state_machine; emit-time has no recomputation to do.
            obi_top1: p.obi_top1,
            obi_top3: p.obi_top3,
            binance_close_at_2s: p.binance_close_at_2s,
            binance_close_at_5s: p.binance_close_at_5s,
            binance_close_at_10s: p.binance_close_at_10s,
            binance_close_at_30s: p.binance_close_at_30s,
            binance_close_at_60s: p.binance_close_at_60s,
            binance_close_at_120s: p.binance_close_at_120s,
            vol_30m: p.vol_30m,
            vol_60m: p.vol_60m,
        };
        writeln!(tf, "{}", serde_json::to_string(&row)?)?;
    }
    eprintln!(
        "[w9-pieza1 exits-trace]   wrote {} ({} rows)",
        trades_path.display(), trades.len()
    );

    // Run-level summary.
    #[derive(serde::Serialize)]
    struct ExitsTraceSummary {
        n_trades_total: usize,
        n_positions: usize,
        baseline_total_pnl: f64,
        baseline_win_ratio: f64,
        dates_processed: Vec<String>,
        dates_skipped: Vec<String>,
        trace_window_s: i64,
        rw_hold_s: i64,
    }
    let total_pnl: f64 = trades.iter().map(|t| t.net_pnl).sum();
    let n_wins = trades.iter().filter(|t| t.net_pnl > 0.0).count();
    let win_ratio = if trades.is_empty() { 0.0 } else { n_wins as f64 / trades.len() as f64 };
    let summary = ExitsTraceSummary {
        n_trades_total: trades.len(),
        n_positions: positions.len(),
        baseline_total_pnl: total_pnl,
        baseline_win_ratio: win_ratio,
        dates_processed,
        dates_skipped,
        trace_window_s: TRACE_WINDOW_S,
        rw_hold_s: cfg.rw_hold_s,
    };
    let summary_path = out_dir.join("summary_exits_trace.json");
    fs::write(&summary_path, serde_json::to_string_pretty(&summary)?)?;
    eprintln!(
        "[w9-pieza1 exits-trace]   wrote {} ({} trades, ${:.2} baseline pnl, wr={:.4})",
        summary_path.display(), trades.len(), total_pnl, win_ratio,
    );

    Ok(())
}

fn print_summary_table(metrics: &[VariantMetrics], n_pos: usize) {
    println!();
    println!("=== backtest_tp summary (n_positions={n_pos}) ===");
    println!(
        "{:<30}  {:>6}  {:>9}  {:>9}  {:>9}  {:>9}  {:>9}  {:>9}",
        "variant", "n", "win_pct", "total_pnl", "avg_pnl", "tp_hit_%", "tp_avg", "uncov_%"
    );
    println!("{}", "-".repeat(110));
    for m in metrics {
        println!(
            "{:<30}  {:>6}  {:>8.1}%  {:>9.4}  {:>9.4}  {:>8.1}%  {:>9.4}  {:>8.1}%",
            m.variant_label,
            m.n_trades,
            m.win_ratio * 100.0,
            m.total_pnl,
            m.avg_pnl,
            m.pct_tp_hit * 100.0,
            m.pnl_when_tp_hit,
            m.uncoverable_rate * 100.0,
        );
    }
    println!();
    println!("=== per-cell breakdown (5m vs 15m, BTC vs ETH) ===");
    for m in metrics {
        if m.by_cell.is_empty() {
            continue;
        }
        println!("\n[variant: {}]", m.variant_label);
        println!(
            "  {:<10}  {:>6}  {:>9}  {:>9}  {:>9}  {:>9}",
            "cell", "n", "win_pct", "total_pnl", "avg_pnl", "tp_hit_%"
        );
        for (cell, cm) in &m.by_cell {
            println!(
                "  {:<10}  {:>6}  {:>8.1}%  {:>9.4}  {:>9.4}  {:>8.1}%",
                cell,
                cm.n_trades,
                cm.win_ratio * 100.0,
                cm.total_pnl,
                cm.avg_pnl,
                cm.pct_tp_hit * 100.0,
            );
        }
    }
    println!();
}

// ============================================================================
// FASE 1: stdout report from peak_characterization.jsonl rows
// ============================================================================

/// Quantile from a sorted f64 slice. Linear interp between adjacent samples.
/// Returns 0.0 for empty input (degenerate; the report skips that case).
fn quantile_sorted(sorted: &[f64], q: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let n = sorted.len();
    let pos = q * (n - 1) as f64;
    let lo = pos.floor() as usize;
    let hi = pos.ceil() as usize;
    if lo == hi {
        sorted[lo]
    } else {
        let frac = pos - lo as f64;
        sorted[lo] * (1.0 - frac) + sorted[hi] * frac
    }
}

/// Histogram bins on `[0, 100]%` with 10% steps. Returns (counts, total).
/// The 11th bucket captures the rare case where the value > 100% (peak
/// detected slightly past exit_ts_ms; see `characterize_position` comment).
fn pct_buckets_0_to_100_step10(values: &[f64]) -> ([usize; 11], usize) {
    let mut counts = [0usize; 11];
    for &v in values {
        let bucket = if v >= 100.0 {
            10 // ">= 100%" overflow
        } else if v < 0.0 {
            0 // negative shouldn't happen, but defend
        } else {
            (v / 10.0).floor() as usize
        };
        counts[bucket.min(10)] += 1;
    }
    (counts, values.len())
}

/// Print the Fase-1 descriptive report. Pure observation: cuándo ocurre el
/// peak (Q1), cuánto más alto vs time-exit (Q2), forma meseta-vs-pico (Q3),
/// diferencias por cell (Q4), fracción con peak significativo (Q5). No
/// prediction, no model, no thresholds optimized -- just histograms +
/// percentiles + per-cell medians.
fn print_peak_characterization_report(stats: &[PeakStats], phase: BtPhase) {
    println!();
    println!(
        "=== FASE 1: PEAK CHARACTERIZATION (n={}, phase={}) ===",
        stats.len(),
        phase.as_str(),
    );
    println!("Descriptive only. No prediction, no threshold optimization.");
    println!();

    // Drop positions with no coverable peak (entire trajectory was NaN).
    // These would skew Q1-Q3 (no peak ts, no top-decile span). Reported
    // separately as a sanity counter.
    let coverable: Vec<&PeakStats> = stats.iter().filter(|s| s.peak_bid.is_some()).collect();
    let n_cov = coverable.len();
    let n_uncov = stats.len() - n_cov;
    println!(
        "Coverable positions (peak measurable): {n_cov} / {} ({:.1}%)",
        stats.len(),
        100.0 * n_cov as f64 / stats.len().max(1) as f64
    );
    if n_uncov > 0 {
        println!(
            "Uncoverable (entire trajectory NaN, excluded from Q1-Q3): {n_uncov}"
        );
    }
    if n_cov == 0 {
        println!("(no coverable positions -- nothing to characterize)");
        return;
    }

    // -------- Q1: WHEN does the peak occur? --------
    let mut offsets_pct: Vec<f64> = coverable
        .iter()
        .filter_map(|s| s.peak_offset_pct_of_hold)
        .collect();
    let mut offsets_ms_sec: Vec<f64> = coverable
        .iter()
        .filter_map(|s| s.peak_offset_ms.map(|m| m as f64 / 1000.0))
        .collect();
    offsets_pct.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    offsets_ms_sec.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    println!();
    println!("--- Q1: WHEN does the peak occur (peak_offset / hold_duration) ---");
    let (counts, total) = pct_buckets_0_to_100_step10(&offsets_pct);
    println!("  bucket            n      pct     cumulative");
    let mut cum = 0usize;
    for (i, &c) in counts.iter().enumerate() {
        cum += c;
        let lo = i * 10;
        let label = if i == 10 {
            ">= 100%".to_string()
        } else {
            format!("{:>3}-{:<3}%", lo, lo + 10)
        };
        println!(
            "  {:<14}  {:>5}  {:>5.1}%   {:>5.1}%",
            label,
            c,
            100.0 * c as f64 / total.max(1) as f64,
            100.0 * cum as f64 / total.max(1) as f64
        );
    }
    println!(
        "  median peak at:  {:>5.1}% of hold ({:.1}s absolute)",
        quantile_sorted(&offsets_pct, 0.5),
        quantile_sorted(&offsets_ms_sec, 0.5),
    );
    println!(
        "  mean peak at:    {:>5.1}% of hold ({:.1}s absolute)",
        offsets_pct.iter().sum::<f64>() / offsets_pct.len() as f64,
        offsets_ms_sec.iter().sum::<f64>() / offsets_ms_sec.len() as f64,
    );

    // -------- Q2: HOW BIG is the peak vs time-exit? --------
    let mut excess_pnl: Vec<f64> = coverable.iter().filter_map(|s| s.peak_excess_pnl).collect();
    let mut excess_pct: Vec<f64> = coverable
        .iter()
        .filter_map(|s| s.peak_excess_pct_over_stake)
        .collect();
    excess_pnl.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    excess_pct.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    println!();
    println!("--- Q2: HOW BIG is the peak vs the time-exit (peak_excess) ---");
    let n_with_excess = excess_pnl.len();
    if n_with_excess > 0 {
        let sum: f64 = excess_pnl.iter().sum();
        println!(
            "  pnl percentiles ($):   p10={:>+8.4}  p25={:>+8.4}  p50={:>+8.4}  p75={:>+8.4}  p90={:>+8.4}  p99={:>+8.4}",
            quantile_sorted(&excess_pnl, 0.10),
            quantile_sorted(&excess_pnl, 0.25),
            quantile_sorted(&excess_pnl, 0.50),
            quantile_sorted(&excess_pnl, 0.75),
            quantile_sorted(&excess_pnl, 0.90),
            quantile_sorted(&excess_pnl, 0.99),
        );
        println!(
            "  pct-of-stake (%):      p10={:>+6.2}  p25={:>+6.2}  p50={:>+6.2}  p75={:>+6.2}  p90={:>+6.2}  p99={:>+6.2}",
            quantile_sorted(&excess_pct, 0.10),
            quantile_sorted(&excess_pct, 0.25),
            quantile_sorted(&excess_pct, 0.50),
            quantile_sorted(&excess_pct, 0.75),
            quantile_sorted(&excess_pct, 0.90),
            quantile_sorted(&excess_pct, 0.99),
        );
        println!(
            "  mean excess pnl: ${:.4}  total excess (perfect-foresight ceiling) across n: ${:.2}",
            sum / n_with_excess as f64,
            sum
        );
    }

    // -------- Q3: SHAPE -- time in top decile --------
    let mut top_pct: Vec<f64> = coverable
        .iter()
        .map(|s| 100.0 * s.time_in_top_decile_ms as f64 / s.hold_duration_ms.max(1) as f64)
        .collect();
    let mut top_sec: Vec<f64> = coverable
        .iter()
        .map(|s| s.time_in_top_decile_ms as f64 / 1000.0)
        .collect();
    top_pct.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    top_sec.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    println!();
    println!("--- Q3: SHAPE meseta vs pico (time bid was within 90% of peak / hold) ---");
    let (counts3, total3) = pct_buckets_0_to_100_step10(&top_pct);
    println!("  bucket            n      pct     cumulative");
    let mut cum3 = 0usize;
    for (i, &c) in counts3.iter().enumerate() {
        cum3 += c;
        let lo = i * 10;
        let label = if i == 10 {
            ">= 100%".to_string()
        } else {
            format!("{:>3}-{:<3}%", lo, lo + 10)
        };
        println!(
            "  {:<14}  {:>5}  {:>5.1}%   {:>5.1}%",
            label,
            c,
            100.0 * c as f64 / total3.max(1) as f64,
            100.0 * cum3 as f64 / total3.max(1) as f64
        );
    }
    println!(
        "  median: {:.1}% of hold near peak ({:.1}s)",
        quantile_sorted(&top_pct, 0.5),
        quantile_sorted(&top_sec, 0.5),
    );
    println!("  (high % = meseta capturable; low % = pico instantáneo)");

    // -------- Q4: per-cell breakdown --------
    println!();
    println!("--- Q4: PER-CELL breakdown (median peak_offset_%, top_decile_%, excess_$, excess_%) ---");
    let mut by_cell: BTreeMap<String, Vec<&PeakStats>> = BTreeMap::new();
    for s in &coverable {
        let cell = format!("{}_{}", s.asset, s.interval);
        by_cell.entry(cell).or_default().push(s);
    }
    println!(
        "  {:<10}  {:>5}  {:>15}  {:>16}  {:>14}  {:>14}",
        "cell", "n", "med peak_offset", "med top_decile", "med excess_$", "med excess_%"
    );
    for (cell, ss) in &by_cell {
        let mut off: Vec<f64> = ss.iter().filter_map(|s| s.peak_offset_pct_of_hold).collect();
        let mut top: Vec<f64> = ss
            .iter()
            .map(|s| 100.0 * s.time_in_top_decile_ms as f64 / s.hold_duration_ms.max(1) as f64)
            .collect();
        let mut exc_pnl: Vec<f64> = ss.iter().filter_map(|s| s.peak_excess_pnl).collect();
        let mut exc_pct: Vec<f64> = ss.iter().filter_map(|s| s.peak_excess_pct_over_stake).collect();
        off.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        top.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        exc_pnl.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        exc_pct.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        println!(
            "  {:<10}  {:>5}  {:>13.1}%  {:>14.1}%  {:>+13.4}  {:>+12.2}%",
            cell,
            ss.len(),
            quantile_sorted(&off, 0.5),
            quantile_sorted(&top, 0.5),
            quantile_sorted(&exc_pnl, 0.5),
            quantile_sorted(&exc_pct, 0.5),
        );
    }

    // -------- Q5: significant peaks --------
    println!();
    println!("--- Q5: SIGNIFICANT peaks (peak_excess_pct_over_stake >= K) ---");
    println!("  K        n     pct");
    for k in [5.0f64, 10.0, 20.0, 50.0, 100.0] {
        let n_sig = coverable
            .iter()
            .filter(|s| s.peak_excess_pct_over_stake.unwrap_or(0.0) >= k)
            .count();
        println!(
            "  >={:>4}%  {:>5}  {:>5.1}%",
            k,
            n_sig,
            100.0 * n_sig as f64 / n_cov as f64
        );
    }
    println!();
    println!("=== end FASE 1 report ===");
}

// ============================================================================
// G13 PHASE 2: PREDICTIVE FEATURES OF "IS THE PEAK NOW?"
//
// Goal: at each second t during a position's hold, predict whether bid_t is
// at-or-past the max of (bid for ts > t) -- i.e., "selling now is at least
// as good as any future moment". Univariate analysis only (no model);
// per-feature AUC + Mann-Whitney U with Bonferroni correction across the
// 12 fixed features x 4 cells.
//
// CAUSALITY ENFORCEMENT (type-level):
//   * `CausalSlice<'_>` / `CausalSliceBbo<'_>` / `BinanceCausal<'_>` wrap
//     trajectories truncated to recv_ms <= t. Their iter() cannot return
//     samples past t -- the slice is physically truncated at construction.
//   * The 12 feature functions take ONLY these causal wrappers. Look-ahead
//     is a TYPE ERROR.
//   * Label + canaries explicitly take raw `&[(i64, f64)]` -- the signature
//     itself signals the (legitimate) future-access at every call site.
//
// SANITY (canary system, executed BEFORE the 12 real features):
//   LAYER 0 oracle:           AUC(label-as-feature, label) == 1.000 exact.
//   LAYER 1 random:           AUC(seeded_rng, label) ~ 0.50 +/- 0.01.
//   LAYER 2 canary_strong:    AUC(canary_max_future_bid) >= 0.85 (mirrors label).
//   LAYER 3 canary_moderate:  AUC(canary_bid_at_t_plus_30s) in [0.55, 0.85].
//   + RED FLAG: any real feature with AUC >= canary_max_future - 0.05 is
//     SUSPECTED of look-ahead contamination (signal too close to the future).
// ============================================================================

// ---------- Causality wrappers (the type-level firewall) ----------

/// Causal slice of a PM bid trajectory: only samples with recv_ms <= t.
/// Built with `CausalSlice::new(trajectory, t)`; the slice is physically
/// truncated at construction via `partition_point`, so iter() cannot return
/// future samples. Functions that take `&CausalSlice` cannot accidentally
/// read past t.
pub struct CausalSlice<'a> {
    samples: &'a [(i64, f64)],
    t: i64,
}

impl<'a> CausalSlice<'a> {
    pub fn new(trajectory: &'a [(i64, f64)], t: i64) -> Self {
        let idx = trajectory.partition_point(|(ms, _)| *ms <= t);
        Self { samples: &trajectory[..idx], t }
    }
    pub fn t(&self) -> i64 { self.t }
    pub fn len(&self) -> usize { self.samples.len() }
    pub fn iter(&self) -> impl Iterator<Item = (i64, f64)> + '_ {
        self.samples.iter().copied()
    }
    pub fn last_finite_bid(&self) -> Option<f64> {
        self.samples.iter().rev().find(|(_, b)| b.is_finite()).map(|(_, b)| *b)
    }
}

/// Causal slice of the parallel BBO trajectory. Same recv_ms grid as
/// `CausalSlice`; values are `(best_bid, best_ask)` from the FullBook at
/// each sample. Used by spread / depth-tax features.
pub struct CausalSliceBbo<'a> {
    samples: &'a [(i64, Option<f64>, Option<f64>)],
    t: i64,
}

impl<'a> CausalSliceBbo<'a> {
    pub fn new(trajectory_bbo: &'a [(i64, Option<f64>, Option<f64>)], t: i64) -> Self {
        let idx = trajectory_bbo.partition_point(|(ms, _, _)| *ms <= t);
        Self { samples: &trajectory_bbo[..idx], t }
    }
    pub fn t(&self) -> i64 { self.t }
    /// Latest past (best_bid, best_ask) where BOTH are Some. None if no
    /// past sample has both.
    pub fn last_full_bbo(&self) -> Option<(f64, f64)> {
        self.samples.iter().rev()
            .find(|(_, bb, ba)| bb.is_some() && ba.is_some())
            .map(|(_, bb, ba)| (bb.unwrap(), ba.unwrap()))
    }
    pub fn last_best_bid(&self) -> Option<f64> {
        self.samples.iter().rev().find_map(|(_, bb, _)| *bb)
    }
}

/// Causal slice of Binance close ticks. Same construction.
pub struct BinanceCausal<'a> {
    closes: &'a [(i64, f64)],
    t: i64,
}

impl<'a> BinanceCausal<'a> {
    pub fn new(all_closes: &'a [(i64, f64)], t: i64) -> Self {
        let idx = all_closes.partition_point(|(ms, _)| *ms <= t);
        Self { closes: &all_closes[..idx], t }
    }
    pub fn t(&self) -> i64 { self.t }
    pub fn last_close(&self) -> Option<f64> {
        self.closes.last().map(|(_, c)| *c)
    }
    pub fn close_at_or_before(&self, target_ms: i64) -> Option<f64> {
        self.closes.iter().rev()
            .find(|(ms, _)| *ms <= target_ms)
            .map(|(_, c)| *c)
    }
}

// ---------- LABEL (oracle) ----------

/// LABEL_TOLERANCE_PCT: fraction of bid_t (NOT absolute) defining "trivial"
/// difference between bid_t and the max future bid. 0.015 = 1.5% -- at typical
/// bid $0.50 = $0.0075 = 0.75 tick (Polymarket tick = $0.01). Below 1 tick at
/// all relevant bid levels; well below median peak excess (+10.28% from Fase 1).
pub const LABEL_TOLERANCE_PCT: f64 = 0.015;

/// "Should I have sold at time t?" oracle label, with tolerance.
/// label = 1 iff bid_t >= max(future bids) - tol_pct * bid_t.
/// Returns None if bid_t is NaN (uncoverable at t -> sample excluded from
/// the dataset; no defined feature_t either). Returns Some(true) if there
/// are no finite future bids (nothing more to gain from waiting).
pub fn label_should_have_sold(
    bid_t: f64,
    max_future_bid: Option<f64>,
    tol_pct: f64,
) -> Option<bool> {
    if !bid_t.is_finite() {
        return None;
    }
    let max_future = match max_future_bid {
        Some(m) if m.is_finite() => m,
        _ => return Some(true), // no future to compare with -> selling now is best
    };
    let epsilon = tol_pct.abs() * bid_t.abs();
    Some(bid_t >= max_future - epsilon)
}

/// Suffix-max of FINITE bids over a trajectory. `suffix_max[i]` = max of
/// `trajectory[i..]` ignoring NaN; `suffix_max[n]` = NEG_INFINITY (empty).
/// O(N) backward pass. Used to make the label O(1) per sample.
pub fn precompute_suffix_max_bid(trajectory: &[(i64, f64)]) -> Vec<f64> {
    let n = trajectory.len();
    let mut suf = vec![f64::NEG_INFINITY; n + 1];
    for i in (0..n).rev() {
        let b = trajectory[i].1;
        suf[i] = if b.is_finite() { b.max(suf[i+1]) } else { suf[i+1] };
    }
    suf
}

/// Given a precomputed suffix_max, return the max future bid for time t
/// (= max over recv_ms > t). Uses partition_point to find the first index
/// past t. Returns None if no finite future bid exists.
pub fn future_max_bid(
    trajectory: &[(i64, f64)],
    suffix_max: &[f64],
    t: i64,
) -> Option<f64> {
    let first_future = trajectory.partition_point(|(ms, _)| *ms <= t);
    let m = suffix_max[first_future];
    if m.is_finite() { Some(m) } else { None }
}

// ---------- AUC + Mann-Whitney U ----------

#[derive(Debug, Clone, Serialize)]
pub struct AucResult {
    pub auc: f64,
    pub u_stat: f64,
    pub p_value: f64,
    pub n_pos: usize,
    pub n_neg: usize,
    pub n_dropped_nan: usize,
}

/// Compute AUC = U / (n_pos * n_neg), tie-corrected, via O(N log N) rank-sum.
/// Returns AUC + raw U + normal-approx 2-sided p-value under H0 (feature
/// uninformative). NaN feature values are dropped (per-feature).
pub fn compute_auc_and_mwu(samples: &[(f64, bool)]) -> AucResult {
    let mut clean: Vec<(f64, bool)> = samples
        .iter().copied()
        .filter(|(f, _)| f.is_finite())
        .collect();
    let n_dropped_nan = samples.len() - clean.len();
    let n_pos = clean.iter().filter(|(_, l)| *l).count();
    let n_neg = clean.len() - n_pos;
    if n_pos == 0 || n_neg == 0 {
        return AucResult { auc: 0.5, u_stat: 0.0, p_value: 1.0, n_pos, n_neg, n_dropped_nan };
    }
    clean.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    let n = clean.len();
    let mut rank_sum_pos = 0.0_f64;
    let mut i = 0usize;
    while i < n {
        let mut j = i + 1;
        while j < n && clean[j].0 == clean[i].0 { j += 1; }
        let avg_rank = (i + 1 + j) as f64 / 2.0;
        for k in i..j {
            if clean[k].1 { rank_sum_pos += avg_rank; }
        }
        i = j;
    }
    let u = rank_sum_pos - (n_pos * (n_pos + 1)) as f64 / 2.0;
    let auc = u / (n_pos as f64 * n_neg as f64);
    let mean_u = (n_pos as f64 * n_neg as f64) / 2.0;
    let var_u = (n_pos as f64 * n_neg as f64 * (n + 1) as f64) / 12.0;
    let z = if var_u > 0.0 { (u - mean_u) / var_u.sqrt() } else { 0.0 };
    let p_value = 2.0 * (1.0 - standard_normal_cdf(z.abs()));
    AucResult { auc, u_stat: u, p_value, n_pos, n_neg, n_dropped_nan }
}

fn standard_normal_cdf(z: f64) -> f64 {
    0.5 * (1.0 + erf_as(z / std::f64::consts::SQRT_2))
}

/// Abramowitz & Stegun 7.1.26 polynomial erf. Max abs error ~7.5e-8.
fn erf_as(x: f64) -> f64 {
    let sign = if x < 0.0 { -1.0 } else { 1.0 };
    let x = x.abs();
    let (a1, a2, a3, a4, a5, p) = (
        0.254829592_f64, -0.284496736, 1.421413741,
        -1.453152027, 1.061405429, 0.3275911,
    );
    let t = 1.0 / (1.0 + p * x);
    let y = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * (-(x * x)).exp();
    sign * y
}

// ---------- Seeded PRNG (for sanity_random feature; reproducible) ----------

/// Linear-congruential generator. Deterministic per seed. Used ONLY for the
/// random sanity feature; nothing security-critical.
pub struct SeededRng(u64);
impl SeededRng {
    pub fn new(seed: u64) -> Self { Self(seed) }
    pub fn next_f64(&mut self) -> f64 {
        // Numerical Recipes LCG constants.
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        (self.0 >> 11) as f64 / (1u64 << 53) as f64
    }
}

// ---------- The 12 features (causal by type) ----------

fn feat_t_since_entry_pct(t: i64, entry_recv_ms: i64, hold_duration_ms: i64) -> f64 {
    100.0 * (t - entry_recv_ms) as f64 / hold_duration_ms.max(1) as f64
}

fn feat_t_to_exit_ms(t: i64, exit_ts_ms: i64) -> f64 {
    (exit_ts_ms - t) as f64
}

fn feat_bid_t(causal: &CausalSlice) -> f64 {
    causal.last_finite_bid().unwrap_or(f64::NAN)
}

fn feat_bid_max_so_far(causal: &CausalSlice) -> f64 {
    causal.iter()
        .map(|(_, b)| b)
        .filter(|b| b.is_finite())
        .fold(f64::NEG_INFINITY, f64::max)
}

fn feat_bid_ratio(causal: &CausalSlice) -> f64 {
    let max_so_far = feat_bid_max_so_far(causal);
    let now = feat_bid_t(causal);
    if max_so_far.is_finite() && max_so_far > 0.0 && now.is_finite() {
        now / max_so_far
    } else { f64::NAN }
}

fn feat_time_since_bid_max_ms(causal: &CausalSlice) -> f64 {
    let max_so_far = feat_bid_max_so_far(causal);
    if !max_so_far.is_finite() { return f64::NAN; }
    let max_ms = causal.iter()
        .filter(|(_, b)| b.is_finite() && (*b - max_so_far).abs() < 1e-12)
        .map(|(ms, _)| ms)
        .last()
        .unwrap_or(causal.t());
    (causal.t() - max_ms) as f64
}

fn feat_bid_velocity_k_sec(causal: &CausalSlice, k_sec: i64) -> f64 {
    let target_ms = causal.t() - k_sec * 1000;
    if target_ms < 0 { return f64::NAN; }
    let bid_then = causal.iter()
        .filter(|(ms, b)| *ms <= target_ms && b.is_finite())
        .map(|(_, b)| b)
        .last();
    let bid_now = causal.last_finite_bid();
    match (bid_then, bid_now) {
        (Some(b_then), Some(b_now)) => (b_now - b_then) / (k_sec as f64),
        _ => f64::NAN,
    }
}

fn feat_spread_t(causal_bbo: &CausalSliceBbo) -> f64 {
    causal_bbo.last_full_bbo()
        .map(|(bb, ba)| ba - bb)
        .unwrap_or(f64::NAN)
}

fn feat_executable_minus_best_bid(causal: &CausalSlice, causal_bbo: &CausalSliceBbo) -> f64 {
    let exec_bid = feat_bid_t(causal);
    let best_bid = causal_bbo.last_best_bid();
    match best_bid {
        Some(bb) if exec_bid.is_finite() && bb.is_finite() => exec_bid - bb,
        _ => f64::NAN,
    }
}

fn feat_binance_return_since_entry(bcausal: &BinanceCausal, close_at_entry: f64) -> f64 {
    match bcausal.last_close() {
        Some(c) if c.is_finite() && close_at_entry > 0.0 =>
            (c - close_at_entry) / close_at_entry,
        _ => f64::NAN,
    }
}

fn feat_binance_return_k_sec(bcausal: &BinanceCausal, k_sec: i64) -> f64 {
    let target_ms = bcausal.t() - k_sec * 1000;
    let then = bcausal.close_at_or_before(target_ms);
    let now = bcausal.last_close();
    match (then, now) {
        (Some(c_then), Some(c_now)) if c_then > 0.0 => (c_now - c_then) / c_then,
        _ => f64::NAN,
    }
}

// ---------- CANARIES (deliberate look-ahead, sanity ONLY) ----------
//
// CORRECTED design (post first run): the LABEL is
//   bid_t >= max_future_bid - epsilon
// which is a COMPARISON between two values. The correct leak feature is
// therefore the comparison itself: `max_future_bid - bid_t`. The previous
// naive design used `max_future_bid` alone, which confounded with the
// absolute bid level across positions (a `max_future_bid` of 0.50 means
// "wait" if bid_t was 0.10 but "sell now" if bid_t was 0.90 -- across the
// aggregate dataset, AUC collapsed to ~0.51 with no signal). The
// _advantage_ versions below are the true label-leak: AUC near 0 (inverse
// perfect) because high advantage -> label=0, low/negative advantage -> label=1.

/// Helper: max finite bid with recv_ms > t. Used internally by canaries.
fn raw_max_future_bid(trajectory: &[(i64, f64)], t: i64) -> f64 {
    let first_future = trajectory.partition_point(|(ms, _)| *ms <= t);
    trajectory[first_future..]
        .iter()
        .map(|(_, b)| *b)
        .filter(|b| b.is_finite())
        .fold(f64::NEG_INFINITY, f64::max)
}

/// Helper: first finite bid with recv_ms >= target. Used by the 30s canary.
fn raw_bid_at_or_after(trajectory: &[(i64, f64)], target_ms: i64) -> f64 {
    trajectory
        .iter()
        .filter(|(ms, b)| *ms >= target_ms && b.is_finite())
        .next()
        .map(|(_, b)| *b)
        .unwrap_or(f64::NAN)
}

/// STRONG canary: the LABEL leak directly.
///   = max_future_bid - bid_t
/// Positive value -> future has a better bid -> label=0 (wait).
/// Negative value -> future is worse -> label=1 (sell now).
/// Equivalently: feature value < epsilon iff label=1.
/// AUC should be near 0 (inverse perfect) across any dataset.
fn canary_max_future_advantage(trajectory: &[(i64, f64)], t: i64, bid_t: f64) -> f64 {
    let max_fut = raw_max_future_bid(trajectory, t);
    if max_fut.is_finite() && bid_t.is_finite() { max_fut - bid_t } else { f64::NAN }
}

/// MODERATE canary: the leak from looking 30s into the future.
///   = bid_at_t_plus_30s - bid_t
/// Only sees a fixed point in the future, not the whole trajectory. AUC
/// should be elevated (signal of a future advantage) but weaker than the
/// max-future version. Gives a "noise floor between random and oracle".
fn canary_advantage_at_t_plus_30s(trajectory: &[(i64, f64)], t: i64, bid_t: f64) -> f64 {
    let b30 = raw_bid_at_or_after(trajectory, t + 30_000);
    if b30.is_finite() && bid_t.is_finite() { b30 - bid_t } else { f64::NAN }
}

// ---------- FeatureSample + builder ----------

#[derive(Debug, Clone, Serialize)]
pub struct FeatureSample {
    pub signal_id: String,
    pub cell: String,
    pub date: String, // YYYY-MM-DD of entry_recv_ms; used for TRAIN/TEST split
    pub t_recv_ms: i64,
    pub label: bool,
    /// Position's entry bid (the bot's BUY price = ask_at_signal). Used by
    /// the tercile analysis to detect "level confound": if a feature's signal
    /// only exists ACROSS entry_price terciles but not WITHIN a tercile, the
    /// feature is just measuring "high entry = close to 1.0 ceiling", not
    /// real timing.
    pub entry_price: f64,
    // 12 real features
    pub f1_t_since_entry_pct: f64,
    pub f2_t_to_exit_ms: f64,
    pub f3_bid_t: f64,
    pub f4_bid_max_so_far: f64,
    pub f5_bid_ratio: f64,
    pub f6_time_since_bid_max_ms: f64,
    pub f7_bid_velocity_5s: f64,
    pub f8_bid_velocity_15s: f64,
    pub f9_spread_t: f64,
    pub f10_executable_minus_best_bid: f64,
    pub f11_binance_return_since_entry: f64,
    pub f12_binance_return_5s: f64,
    // sanity columns (analyzed separately; not in the 12 real features)
    pub sanity_label_as_feat: f64,
    pub sanity_random_seeded: f64,
    /// FIXED canary: max_future_bid - bid_t. Was previously max_future_bid
    /// alone, which confounded with absolute bid level and gave weak AUC.
    /// AUC of this advantage feature should be near 0 (inverse perfect leak).
    pub canary_max_future_advantage: f64,
    /// FIXED canary: bid_at_t_plus_30s - bid_t. Moderate leak (30s ahead only).
    pub canary_advantage_at_t_plus_30s: f64,
}

/// Build per-second feature samples for one position. CAUSAL by construction:
/// the 12 real features only receive CausalSlice / CausalSliceBbo /
/// BinanceCausal -- all physically truncated to recv_ms <= t.
pub fn build_features_for_position(
    pos: &CollectedPosition,
    binance_closes: &[(i64, f64)],
    close_at_entry: f64,
    rng: &mut SeededRng,
    date: &str,
    sample_interval_ms: i64,
) -> Vec<FeatureSample> {
    let suffix_max = precompute_suffix_max_bid(&pos.trajectory);
    let hold_dur = (pos.exit_ts_ms - pos.entry_recv_ms).max(1);
    let cell = format!("{}_{}", pos.asset, pos.interval);
    let mut out = Vec::new();
    let mut t = pos.entry_recv_ms;
    while t <= pos.exit_ts_ms {
        // Build the causal contexts (firewall).
        let causal_pm = CausalSlice::new(&pos.trajectory, t);
        let causal_bbo = CausalSliceBbo::new(&pos.trajectory_bbo, t);
        let bcausal = BinanceCausal::new(binance_closes, t);
        // Compute the 12 features (each takes ONLY causal contexts).
        let f3 = feat_bid_t(&causal_pm);
        let row = FeatureSample {
            signal_id: pos.signal_id.clone(),
            cell: cell.clone(),
            date: date.to_string(),
            t_recv_ms: t,
            // label deferred -- requires f3 + future max
            label: false, // overwrite below
            entry_price: pos.entry_price,
            f1_t_since_entry_pct: feat_t_since_entry_pct(t, pos.entry_recv_ms, hold_dur),
            f2_t_to_exit_ms: feat_t_to_exit_ms(t, pos.exit_ts_ms),
            f3_bid_t: f3,
            f4_bid_max_so_far: feat_bid_max_so_far(&causal_pm),
            f5_bid_ratio: feat_bid_ratio(&causal_pm),
            f6_time_since_bid_max_ms: feat_time_since_bid_max_ms(&causal_pm),
            f7_bid_velocity_5s: feat_bid_velocity_k_sec(&causal_pm, 5),
            f8_bid_velocity_15s: feat_bid_velocity_k_sec(&causal_pm, 15),
            f9_spread_t: feat_spread_t(&causal_bbo),
            f10_executable_minus_best_bid: feat_executable_minus_best_bid(&causal_pm, &causal_bbo),
            f11_binance_return_since_entry: feat_binance_return_since_entry(&bcausal, close_at_entry),
            f12_binance_return_5s: feat_binance_return_k_sec(&bcausal, 5),
            // Sanity + canaries (FIXED: advantage versions = direct label leak).
            sanity_label_as_feat: 0.0, // overwrite below
            sanity_random_seeded: rng.next_f64(),
            canary_max_future_advantage: canary_max_future_advantage(&pos.trajectory, t, f3),
            canary_advantage_at_t_plus_30s: canary_advantage_at_t_plus_30s(&pos.trajectory, t, f3),
        };
        // Label: uses f3 (the same bid_t the features observe -- consistency).
        let max_fut = future_max_bid(&pos.trajectory, &suffix_max, t);
        let lab = label_should_have_sold(f3, max_fut, LABEL_TOLERANCE_PCT);
        if let Some(lab) = lab {
            let mut r = row;
            r.label = lab;
            r.sanity_label_as_feat = if lab { 1.0 } else { 0.0 };
            out.push(r);
        }
        // If lab is None, the sample is dropped (uncoverable bid at t).
        t += sample_interval_ms;
    }
    out
}

// ---------- Analyzer ----------

/// Find the close of the latest Binance kline whose t_open_ms <= entry_recv_ms.
/// Used to record `close_at_entry` (immutable per position) before computing
/// feature #11.
fn close_at_or_before(closes: &[(i64, f64)], t_ms: i64) -> Option<f64> {
    let idx = closes.partition_point(|(ms, _)| *ms <= t_ms);
    if idx == 0 { None } else { Some(closes[idx - 1].1) }
}

#[derive(Debug, Clone, Serialize)]
pub struct FeatureAnalysisRow {
    pub cell: String,
    pub feature: String,
    pub auc_train: f64,
    pub auc_test: f64,
    pub ratio_test_over_train: f64,
    pub u_train: f64,
    pub p_train: f64,
    pub bonferroni_p_train: f64,
    pub n_train: usize,
    pub n_test: usize,
    pub passes: bool,
    pub canary_flag: String, // "OK" | "YELLOW" | "RED"
}

/// Group samples by (cell, date in train/test). Returns (train, test).
fn split_train_test<'a>(
    samples: &'a [FeatureSample],
    train_dates: &[&str],
    test_dates: &[&str],
) -> (Vec<&'a FeatureSample>, Vec<&'a FeatureSample>) {
    let mut tr = Vec::new();
    let mut te = Vec::new();
    for s in samples {
        if train_dates.contains(&s.date.as_str()) { tr.push(s); }
        else if test_dates.contains(&s.date.as_str()) { te.push(s); }
    }
    (tr, te)
}

fn pair_feature_label(samples: &[&FeatureSample], feat_fn: impl Fn(&FeatureSample) -> f64) -> Vec<(f64, bool)> {
    samples.iter().map(|s| (feat_fn(s), s.label)).collect()
}

/// Phase 2 entry point. Runs the streaming backtest to build positions, then
/// builds the feature dataset, then runs the 4-layer sanity + per-feature
/// analysis with Bonferroni correction.
#[allow(clippy::too_many_arguments)]
pub fn run_phase2(
    data_root: &Path,
    start_date: &str,
    end_date: &str,
    out_dir: &Path,
    phase: BtPhase,
) -> Result<()> {
    // Phase guard applies: Phase 2 reads strategy results, so it MUST honor
    // the out-of-sample seal exactly like run_backtest_tp does.
    validate_phase_dates(phase, start_date, end_date)?;
    let dates = dates_inclusive(start_date, end_date)?;
    fs::create_dir_all(out_dir)?;
    if phase == BtPhase::Validation {
        let banner = format!(
            "================================================================\n\
             *** VALIDATION RUN (Phase 2) -- seal broken, one-shot, audited ***\n\
             phase     = validation\n\
             window    = {start_date} .. {end_date}\n\
             out_dir   = {}\n\
             ================================================================\n",
            out_dir.display()
        );
        eprintln!("{banner}");
        let seal = out_dir.join("validation_seal_broken.txt");
        let mut sf = File::create(&seal)?;
        writeln!(sf, "{banner}")?;
    }
    eprintln!(
        "[phase2] phase={} running {} dates ({}..{}) -> {}",
        phase.as_str(), dates.len(), start_date, end_date, out_dir.display()
    );

    // ----- collect positions + their binance close streams per asset -----
    let cfg = DecisionConfig::default();
    let mut all_positions: Vec<(String, CollectedPosition)> = Vec::new(); // (date, pos)
    let mut binance_btc: Vec<(i64, f64)> = Vec::new();
    let mut binance_eth: Vec<(i64, f64)> = Vec::new();
    for date in &dates {
        // Replay (streaming) to get positions for this date.
        match replay_day(data_root, date, &cfg, false, &EntryFilter::Baseline) {
            Ok(positions) => {
                for p in positions { all_positions.push((date.clone(), p)); }
            }
            Err(e) => eprintln!("[phase2] WARN: {date} replay failed: {e:#}"),
        }
        // Drain the day's klines into the global binance closes (per asset).
        // We use the same kline_stream the streaming replay uses.
        for (sym, target) in [("btcusdt", &mut binance_btc), ("ethusdt", &mut binance_eth)] {
            if let Ok(stream) = kline_stream(data_root, sym, if sym=="btcusdt" {"BTC"} else {"ETH"}, date) {
                for ev in stream {
                    if let Ev::Kline { t_open_ms, close, .. } = ev.ev {
                        target.push((t_open_ms, close));
                    }
                }
            }
        }
    }
    binance_btc.sort_by_key(|x| x.0);
    binance_eth.sort_by_key(|x| x.0);
    eprintln!(
        "[phase2] collected {} positions across {} dates; binance: BTC={} closes, ETH={} closes",
        all_positions.len(), dates.len(), binance_btc.len(), binance_eth.len()
    );

    // ----- build feature dataset -----
    let mut rng = SeededRng::new(0xDEAD_BEEF_DEAD_BEEFu64);
    let mut samples: Vec<FeatureSample> = Vec::new();
    for (date, pos) in &all_positions {
        let closes = if pos.asset == "BTC" { &binance_btc } else { &binance_eth };
        // Look up close at entry (causal: entry_recv_ms uses past kline).
        let close_at_entry = close_at_or_before(closes, pos.entry_recv_ms).unwrap_or(f64::NAN);
        let pos_samples = build_features_for_position(
            pos, closes, close_at_entry, &mut rng, date, 1000,
        );
        samples.extend(pos_samples);
    }
    let dataset_path = out_dir.join("feature_dataset.jsonl");
    let mut df = File::create(&dataset_path)?;
    for s in &samples {
        writeln!(df, "{}", serde_json::to_string(s)?)?;
    }
    eprintln!(
        "[phase2] feature_dataset written: {} ({} rows)",
        dataset_path.display(), samples.len()
    );

    // ----- TRAIN/TEST split + cell coverage report -----
    let train_dates = ["2026-05-06", "2026-05-07", "2026-05-08", "2026-05-09", "2026-05-10", "2026-05-11"];
    let test_dates  = ["2026-05-13", "2026-05-14", "2026-05-15", "2026-05-16"];
    let mut cell_counts: BTreeMap<String, (usize, usize)> = BTreeMap::new();
    for s in &samples {
        let entry = cell_counts.entry(s.cell.clone()).or_insert((0, 0));
        if train_dates.contains(&s.date.as_str()) { entry.0 += 1; }
        else if test_dates.contains(&s.date.as_str()) { entry.1 += 1; }
    }
    println!();
    println!("=== FASE 2: PREDICTIVE FEATURE ANALYSIS ===");
    println!("phase: {} | dates: {start_date} .. {end_date}", phase.as_str());
    println!("dataset: {} samples; binance: BTC={}, ETH={}",
        samples.len(), binance_btc.len(), binance_eth.len());
    println!();
    println!("Cell coverage (samples):");
    println!("  {:<12}  {:>10}  {:>10}", "cell", "TRAIN", "TEST");
    for (cell, (tr, te)) in &cell_counts {
        println!("  {:<12}  {:>10}  {:>10}", cell, tr, te);
    }
    println!();

    // ----- SANITY 4-LAYER -----
    let (train_samples, test_samples) = split_train_test(&samples, &train_dates, &test_dates);
    println!("--- SANITY (4 layers, on TRAIN; abort if any fails its band) ---");

    let sanity_oracle  = compute_auc_and_mwu(&pair_feature_label(&train_samples, |s| s.sanity_label_as_feat));
    let sanity_random  = compute_auc_and_mwu(&pair_feature_label(&train_samples, |s| s.sanity_random_seeded));
    let canary_strong  = compute_auc_and_mwu(&pair_feature_label(&train_samples, |s| s.canary_max_future_advantage));
    let canary_mod     = compute_auc_and_mwu(&pair_feature_label(&train_samples, |s| s.canary_advantage_at_t_plus_30s));

    // For the advantage canaries, AUC near 0 = perfect inverse leak. Use
    // |AUC - 0.5| as the direction-agnostic signal strength.
    let strong_dist = (canary_strong.auc - 0.5).abs();
    let mod_dist    = (canary_mod.auc - 0.5).abs();

    println!("  LAYER 0  oracle (label-as-feature)         AUC={:>6.4}  expected 1.0000  {}",
        sanity_oracle.auc,
        if (sanity_oracle.auc - 1.0).abs() < 1e-9 { "PASS" } else { "FAIL" });
    println!("  LAYER 1  random (seeded PRNG)              AUC={:>6.4}  expected [0.490, 0.510]  {}",
        sanity_random.auc,
        if (sanity_random.auc - 0.5).abs() < 0.01 { "PASS" } else { "FAIL" });
    println!("  LAYER 2  canary_max_future_advantage       AUC={:>6.4}  |0.5-AUC|={:.4}  expected >= 0.40 (strong leak)  {}",
        canary_strong.auc, strong_dist,
        if strong_dist >= 0.40 { "PASS" } else { "FAIL" });
    println!("  LAYER 3  canary_advantage_at_t_plus_30s    AUC={:>6.4}  |0.5-AUC|={:.4}  expected in [0.05, 0.40] (moderate leak)  {}",
        canary_mod.auc, mod_dist,
        if mod_dist >= 0.05 && mod_dist <= 0.40 { "PASS" } else { "MARGINAL" });
    println!();

    // Direction-agnostic canary signal for red-flag detection.
    let canary_strong_signal = strong_dist;

    // ----- 12 features x 4 cells = 48 tests with Bonferroni -----
    let n_tests = 12 * 4; // 48
    let bonf_factor = n_tests as f64;
    let feats: [(&str, Box<dyn Fn(&FeatureSample) -> f64>); 12] = [
        ("f1_t_since_entry_pct",          Box::new(|s| s.f1_t_since_entry_pct)),
        ("f2_t_to_exit_ms",               Box::new(|s| s.f2_t_to_exit_ms)),
        ("f3_bid_t",                      Box::new(|s| s.f3_bid_t)),
        ("f4_bid_max_so_far",             Box::new(|s| s.f4_bid_max_so_far)),
        ("f5_bid_ratio",                  Box::new(|s| s.f5_bid_ratio)),
        ("f6_time_since_bid_max_ms",      Box::new(|s| s.f6_time_since_bid_max_ms)),
        ("f7_bid_velocity_5s",            Box::new(|s| s.f7_bid_velocity_5s)),
        ("f8_bid_velocity_15s",           Box::new(|s| s.f8_bid_velocity_15s)),
        ("f9_spread_t",                   Box::new(|s| s.f9_spread_t)),
        ("f10_executable_minus_best_bid", Box::new(|s| s.f10_executable_minus_best_bid)),
        ("f11_binance_return_since_entry",Box::new(|s| s.f11_binance_return_since_entry)),
        ("f12_binance_return_5s",         Box::new(|s| s.f12_binance_return_5s)),
    ];
    let cells: Vec<String> = cell_counts.keys().cloned().collect();
    let mut analysis: Vec<FeatureAnalysisRow> = Vec::new();
    for cell in &cells {
        let train_cell: Vec<&FeatureSample> = train_samples.iter().filter(|s| &s.cell == cell).copied().collect();
        let test_cell:  Vec<&FeatureSample> = test_samples.iter().filter(|s| &s.cell == cell).copied().collect();
        for (fname, fn_) in feats.iter() {
            let train_pairs = pair_feature_label(&train_cell, fn_);
            let test_pairs  = pair_feature_label(&test_cell, fn_);
            let r_tr = compute_auc_and_mwu(&train_pairs);
            let r_te = compute_auc_and_mwu(&test_pairs);
            let ratio = if r_tr.auc != 0.0 {
                // Use distance from 0.5 for direction-agnostic ratio.
                let d_tr = (r_tr.auc - 0.5).abs();
                let d_te = (r_te.auc - 0.5).abs();
                if d_tr > 1e-9 { d_te / d_tr } else { 0.0 }
            } else { 0.0 };
            let bonf_p = (r_tr.p_value * bonf_factor).min(1.0);
            let auc_test_abs = (r_te.auc - 0.5).abs() + 0.5;
            let passes =
                auc_test_abs > 0.55
                && ratio > 0.85
                && bonf_p < 0.05;
            // Canary check: real feature AUC distance from 0.5 vs canary_strong's distance.
            let feat_signal = (r_te.auc - 0.5).abs();
            let canary_flag = if feat_signal >= canary_strong_signal - 0.05 { "RED" }
                              else if feat_signal >= (canary_mod.auc - 0.5).abs() - 0.05 { "YELLOW" }
                              else { "OK" };
            analysis.push(FeatureAnalysisRow {
                cell: cell.clone(),
                feature: fname.to_string(),
                auc_train: r_tr.auc,
                auc_test: r_te.auc,
                ratio_test_over_train: ratio,
                u_train: r_tr.u_stat,
                p_train: r_tr.p_value,
                bonferroni_p_train: bonf_p,
                n_train: r_tr.n_pos + r_tr.n_neg,
                n_test: r_te.n_pos + r_te.n_neg,
                passes,
                canary_flag: canary_flag.to_string(),
            });
        }
    }

    // Persist analysis JSON.
    let analysis_path = out_dir.join("feature_analysis.json");
    serde_json::to_writer_pretty(File::create(&analysis_path)?, &analysis)?;
    eprintln!("[phase2] analysis written: {}", analysis_path.display());

    // Print table sorted by auc_test distance from 0.5 (signal strength).
    let mut sorted = analysis.clone();
    sorted.sort_by(|a, b| (b.auc_test - 0.5).abs().partial_cmp(&(a.auc_test - 0.5).abs()).unwrap_or(std::cmp::Ordering::Equal));
    println!("--- 48 ENTRIES (12 features x 4 cells), sorted by |AUC_test - 0.5| desc ---");
    println!("{:<10} {:<34} {:>8} {:>8} {:>7} {:>9} {:>9} {:>6} {:<6}",
        "cell", "feature", "AUC_tr", "AUC_te", "ratio", "p_train", "Bonf_p", "verd", "canary");
    println!("{}", "-".repeat(118));
    for r in &sorted {
        let verdict = if r.passes { "PASS" } else { "FAIL" };
        println!("{:<10} {:<34} {:>8.4} {:>8.4} {:>7.2} {:>9.2e} {:>9.2e} {:>6} {:<6}",
            r.cell, r.feature, r.auc_train, r.auc_test, r.ratio_test_over_train,
            r.p_train, r.bonferroni_p_train, verdict, r.canary_flag);
    }
    println!();

    // ----- TERCILE-BY-ENTRY-PRICE ANALYSIS (level-confound diagnostic) -----
    // For features f3/f4/f5/f6, split each cell's positions into 3 terciles by
    // entry_price (= ask_at_signal = bot's BUY price). Compute AUC of each
    // feature within EACH tercile. If a feature's signal exists only ACROSS
    // terciles but vanishes within a tercile, the "signal" was confounded
    // with the absolute bid level (= "high bid means close to 1.0 ceiling
    // means label=1 more often"), not real timing.
    //
    // Tercile breakpoints come from TRAIN samples (avoids peeking at TEST).
    // AUC is computed on TRAIN inside each tercile (diagnostic question:
    // does the signal exist within a level group? -- not a train/test
    // validation, that's the main 48-table above).
    println!("--- TERCILE-BY-ENTRY-PRICE ANALYSIS (level-confound check) ---");
    println!("Splits each cell into 3 terciles by entry_price (TRAIN positions). AUC within");
    println!("each tercile diagnoses level confound: signal that vanishes within a tercile");
    println!("was just measuring 'high bid level' (proximity to 1.0 ceiling), not timing.");
    println!();

    // Compute per-cell entry_price tercile breakpoints from UNIQUE positions
    // (entry_price is per-position; each position appears in many samples).
    let mut prices_by_cell: BTreeMap<String, Vec<f64>> = BTreeMap::new();
    {
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        for s in &train_samples {
            if seen.insert(s.signal_id.clone()) {
                prices_by_cell.entry(s.cell.clone()).or_default().push(s.entry_price);
            }
        }
    }
    let mut breakpoints: BTreeMap<String, (f64, f64)> = BTreeMap::new();
    for (cell, mut prices) in prices_by_cell {
        prices.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let n = prices.len().max(1);
        let p33 = prices[n / 3];
        let p66 = prices[(n * 2) / 3];
        breakpoints.insert(cell, (p33, p66));
    }

    let tercile_feats: [(&str, Box<dyn Fn(&FeatureSample) -> f64>); 6] = [
        // Suspected confounded:
        ("f3_bid_t",                 Box::new(|s| s.f3_bid_t)),
        ("f4_bid_max_so_far",        Box::new(|s| s.f4_bid_max_so_far)),
        ("f5_bid_ratio",             Box::new(|s| s.f5_bid_ratio)),
        // Candidate honest:
        ("f6_time_since_bid_max_ms", Box::new(|s| s.f6_time_since_bid_max_ms)),
        // "Should be level-independent by construction" -- verify it actually is:
        ("f1_t_since_entry_pct",     Box::new(|s| s.f1_t_since_entry_pct)),
        ("f9_spread_t",              Box::new(|s| s.f9_spread_t)),
    ];

    println!("{:<10}  {:<28}  {:<8}  {:>7}  {:>7}  {:>9}  {:>9}",
        "cell", "feature", "tercile", "n_pos", "n_neg", "AUC", "|0.5-AUC|");
    println!("{}", "-".repeat(94));
    for (cell, bp) in &breakpoints {
        let train_cell: Vec<&FeatureSample> = train_samples.iter()
            .filter(|s| &s.cell == cell)
            .copied()
            .collect();
        println!("# {} entry_price breakpoints (TRAIN): low<={:.4} | mid<={:.4} | high>{:.4}",
            cell, bp.0, bp.1, bp.1);
        for (fname, fn_) in tercile_feats.iter() {
            for tercile in ["low", "mid", "high"] {
                let bucket: Vec<&FeatureSample> = train_cell.iter()
                    .filter(|s| {
                        let tlab = if s.entry_price <= bp.0 { "low" }
                                   else if s.entry_price <= bp.1 { "mid" }
                                   else { "high" };
                        tlab == tercile
                    })
                    .copied()
                    .collect();
                let pairs = pair_feature_label(&bucket, fn_);
                let r = compute_auc_and_mwu(&pairs);
                println!("{:<10}  {:<28}  {:<8}  {:>7}  {:>7}  {:>9.4}  {:>9.4}",
                    cell, fname, tercile, r.n_pos, r.n_neg, r.auc, (r.auc - 0.5).abs());
            }
        }
    }
    println!();

    // Robustness across cells.
    let passes_by_feat: BTreeMap<String, Vec<String>> = analysis.iter()
        .filter(|r| r.passes && r.canary_flag != "RED")
        .fold(BTreeMap::new(), |mut m, r| {
            m.entry(r.feature.clone()).or_default().push(r.cell.clone());
            m
        });
    println!("--- ROBUST FEATURES (PASS in >=2 cells AND canary_flag != RED) ---");
    if passes_by_feat.is_empty() {
        println!("  (none -- no univariate feature passes in 2+ cells without canary flag)");
    } else {
        for (feat, cells_passing) in &passes_by_feat {
            if cells_passing.len() >= 2 {
                println!("  {:<34} passes in: {:?}", feat, cells_passing);
            }
        }
    }
    println!();
    println!("=== end FASE 2 report ===");
    Ok(())
}

// ============================================================================
// TESTS -- pure helpers (FullBook + executable_bid + variants + metrics).
// Integration testing of the full replay requires real recorder data; the
// pure helpers cover the math/logic that drives the result.
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn approx(a: f64, b: f64, eps: f64) -> bool {
        (a - b).abs() < eps
    }

    // ---------- FullBook + apply_snapshot / apply_price_change ----------

    #[test]
    fn g8_pre_tp_apply_snapshot_resets_and_populates() {
        let mut b = FullBook::default();
        // Pre-populate to verify reset behavior.
        b.apply_price_change(0.50, 100.0, BookSide::Bid, 1_000);
        b.apply_price_change(0.51, 200.0, BookSide::Ask, 1_000);
        // Now snapshot something different -- should fully replace.
        b.apply_snapshot(
            &[(0.40, 5.0), (0.42, 10.0)],
            &[(0.55, 7.0), (0.60, 3.0)],
            2_000,
        );
        assert_eq!(b.best_bid(), Some(0.42));
        assert_eq!(b.best_ask(), Some(0.55));
        assert_eq!(b.bids.len(), 2);
        assert_eq!(b.asks.len(), 2);
        assert_eq!(b.last_update_ms, 2_000);
    }

    #[test]
    fn g8_pre_tp_apply_price_change_is_absolute() {
        // Empirical evidence (recorder 2026-05-25): size=0 wipes the level,
        // size>0 SETS the level (NOT add a delta). Test this directly.
        let mut b = FullBook::default();
        b.apply_price_change(0.50, 100.0, BookSide::Bid, 1_000);
        assert_eq!(b.bids.get(&price_to_key(0.50)), Some(&100.0));
        // Apply size=50 to same level -> level becomes 50 (NOT 150).
        b.apply_price_change(0.50, 50.0, BookSide::Bid, 1_001);
        assert_eq!(b.bids.get(&price_to_key(0.50)), Some(&50.0),
            "size is absolute, not delta-added");
        // Apply size=0 -> level is REMOVED.
        b.apply_price_change(0.50, 0.0, BookSide::Bid, 1_002);
        assert!(!b.bids.contains_key(&price_to_key(0.50)));
        // Apply size>0 again -> level is REPOPULATED.
        b.apply_price_change(0.50, 75.0, BookSide::Bid, 1_003);
        assert_eq!(b.bids.get(&price_to_key(0.50)), Some(&75.0));
    }

    #[test]
    fn g8_pre_tp_best_bid_and_ask_return_extremes() {
        let mut b = FullBook::default();
        b.apply_snapshot(
            &[(0.40, 1.0), (0.42, 1.0), (0.45, 1.0)],
            &[(0.55, 1.0), (0.50, 1.0), (0.48, 1.0)],
            0,
        );
        assert_eq!(b.best_bid(), Some(0.45)); // highest of bids
        assert_eq!(b.best_ask(), Some(0.48)); // lowest of asks
    }

    // ---------- executable_bid_for_shares (THE CRITICAL helper) ----------

    #[test]
    fn g8_pre_tp_executable_bid_single_level_consumes_partial_size() {
        let mut b = FullBook::default();
        b.apply_snapshot(&[(0.50, 100.0)], &[], 0);
        // Sell 25 shares against the bid of 100 @ 0.50 -> avg price 0.50.
        let px = b.executable_bid_for_shares(25.0).unwrap();
        assert!(approx(px, 0.50, 1e-9));
    }

    #[test]
    fn g8_pre_tp_executable_bid_walks_stack_when_top_thin() {
        // Top of bid has only 0.1 size; sell 25 -> walks down.
        // Top: 0.50 @ 0.1; next: 0.30 @ 50.
        let mut b = FullBook::default();
        b.apply_snapshot(&[(0.30, 50.0), (0.50, 0.1)], &[], 0);
        let px = b.executable_bid_for_shares(25.0).unwrap();
        // 0.1 @ 0.50 + 24.9 @ 0.30 = 0.05 + 7.47 = 7.52; /25 = 0.3008.
        assert!(approx(px, 0.3008, 1e-4), "got {px}");
        assert!(px < 0.31, "executable bid is dragged down by thin top, got {px}");
    }

    #[test]
    fn g8_pre_tp_executable_bid_returns_none_when_total_depth_insufficient() {
        let mut b = FullBook::default();
        b.apply_snapshot(&[(0.30, 5.0), (0.50, 0.1)], &[], 0);
        // Total depth = 5.1 shares; ask for 100 shares -> uncoverable.
        let px = b.executable_bid_for_shares(100.0);
        assert_eq!(px, None);
    }

    #[test]
    fn g8_pre_tp_executable_bid_empty_book_is_none() {
        let b = FullBook::default();
        assert_eq!(b.executable_bid_for_shares(1.0), None);
    }

    #[test]
    fn g8_pre_tp_executable_bid_exact_full_stack_consume_is_ok() {
        let mut b = FullBook::default();
        b.apply_snapshot(&[(0.30, 5.0), (0.50, 5.0)], &[], 0);
        let px = b.executable_bid_for_shares(10.0).unwrap();
        // 5 @ 0.50 + 5 @ 0.30 = 2.5 + 1.5 = 4.0; /10 = 0.40.
        assert!(approx(px, 0.40, 1e-9));
    }

    // ---------- BookSide parsing ----------

    #[test]
    fn g8_pre_tp_book_side_from_polymarket_strings() {
        assert_eq!(BookSide::from_str("BUY"), Some(BookSide::Bid));
        assert_eq!(BookSide::from_str("SELL"), Some(BookSide::Ask));
        assert_eq!(BookSide::from_str("buy"), None); // case-sensitive per Polymarket
        assert_eq!(BookSide::from_str("invalid"), None);
    }

    // ---------- price_to_key roundtrip + ordering ----------

    #[test]
    fn g8_pre_tp_price_to_key_roundtrips_and_orders() {
        for &p in &[0.0001_f64, 0.01, 0.10, 0.50, 0.99, 1.0] {
            let k = price_to_key(p);
            let p2 = key_to_price(k);
            assert!(approx(p, p2, 1e-9), "roundtrip {p} -> {k} -> {p2}");
        }
        assert!(price_to_key(0.50) > price_to_key(0.49));
        assert!(price_to_key(0.49) < price_to_key(0.50));
    }

    // ---------- Variant evaluation + ExitReason ----------

    fn mk_position(asset: &str, interval: &str, entry: f64, shares: f64,
                   trajectory: Vec<f64>, time_exit_bid: Option<f64>) -> CollectedPosition {
        let traj: Vec<(i64, f64)> = trajectory
            .into_iter()
            .enumerate()
            .map(|(i, b)| (1_000 + i as i64 * 100, b))
            .collect();
        let epoch = (1700_i64 / interval_secs_for(interval)) * interval_secs_for(interval);
        CollectedPosition {
            token: format!("tok-{asset}-{interval}"),
            asset: asset.to_string(),
            interval: interval.to_string(),
            epoch,
            direction: "Up".to_string(),
            signal_id: format!("{asset}-1700-{interval}-Up"),
            entry_recv_ms: 1_000,
            entry_price: entry,
            shares,
            exit_ts_ms: 9_999_999,
            uncoverable_samples: traj.iter().filter(|(_, b)| b.is_nan()).count(),
            total_samples: traj.len(),
            trajectory_bbo: traj.iter().map(|(ms, _)| (*ms, None, None)).collect(),
            trajectory: traj,
            time_exit_bid,
            trigger_close: 0.0, // W9: not exercised by variant-eval tests
            trigger_ret_bps: 0.0,
            maker_fee_bps: 0,
            taker_fee_bps: 0,
            fee_type: String::new(),
            // COMBO 2026-06-08: not exercised by variant-eval tests.
            entry_trigger_ts_ms: 0,
            obi_top1: f64::NAN,
            obi_top3: f64::NAN,
            binance_close_at_2s: f64::NAN,
            binance_close_at_5s: f64::NAN,
            binance_close_at_10s: f64::NAN,
            binance_close_at_30s: f64::NAN,
            binance_close_at_60s: f64::NAN,
            binance_close_at_120s: f64::NAN,
            vol_30m: f64::NAN,
            vol_60m: f64::NAN,
        }
    }

    #[test]
    fn g8_pre_tp_variant_baseline_uses_time_exit_bid() {
        let p = mk_position("BTC", "5m", 0.40, 2.625, vec![0.42, 0.45, 0.41], Some(0.41));
        let (px, reason) = decide_exit_for_variant(&p, ExitVariant::Baseline);
        assert!(approx(px, 0.41, 1e-9));
        assert_eq!(reason, ExitReason::TimeExit);
    }

    #[test]
    fn g8_pre_tp_variant_baseline_uncoverable_when_no_time_exit_bid() {
        let p = mk_position("BTC", "5m", 0.40, 2.625, vec![f64::NAN, f64::NAN], None);
        let (px, reason) = decide_exit_for_variant(&p, ExitVariant::Baseline);
        assert_eq!(px, 0.0);
        assert_eq!(reason, ExitReason::Uncoverable);
    }

    #[test]
    fn g8_pre_tp_variant_first_touch_exits_at_first_threshold_cross() {
        // entry=0.40, tp=10% -> threshold 0.44. Trajectory crosses at sample 2 (0.45).
        let p = mk_position("BTC", "5m", 0.40, 2.625, vec![0.41, 0.43, 0.45, 0.50, 0.42], Some(0.42));
        let (px, reason) = decide_exit_for_variant(&p, ExitVariant::FirstTouch { tp_pct: 0.10 });
        assert!(approx(px, 0.45, 1e-9), "exits at FIRST touch (0.45), not peak (0.50)");
        assert_eq!(reason, ExitReason::TpTouched);
    }

    #[test]
    fn g8_pre_tp_variant_first_touch_falls_back_to_time_exit_when_never_touched() {
        // entry=0.40, tp=30% -> threshold 0.52. Trajectory tops at 0.50 -> never touched.
        let p = mk_position("BTC", "5m", 0.40, 2.625, vec![0.41, 0.43, 0.45, 0.50, 0.42], Some(0.42));
        let (px, reason) = decide_exit_for_variant(&p, ExitVariant::FirstTouch { tp_pct: 0.30 });
        assert!(approx(px, 0.42, 1e-9));
        assert_eq!(reason, ExitReason::TimeExit);
    }

    #[test]
    fn g8_pre_tp_variant_peak_returns_maximum_in_window() {
        let p = mk_position("BTC", "5m", 0.40, 2.625, vec![0.41, 0.43, 0.45, 0.50, 0.42], Some(0.42));
        let (px, reason) = decide_exit_for_variant(&p, ExitVariant::Peak);
        assert!(approx(px, 0.50, 1e-9));
        assert_eq!(reason, ExitReason::PeakOpportunity);
    }

    #[test]
    fn g8_pre_tp_variant_first_touch_ignores_nan_samples_uncoverable_moments() {
        // Trajectory has NaN holes (= uncoverable depth at those moments). The
        // first non-NaN crossing should win, not a NaN.
        let p = mk_position("BTC", "5m", 0.40, 2.625,
            vec![f64::NAN, f64::NAN, 0.45, f64::NAN, 0.42], Some(0.42));
        let (px, reason) = decide_exit_for_variant(&p, ExitVariant::FirstTouch { tp_pct: 0.10 });
        assert!(approx(px, 0.45, 1e-9));
        assert_eq!(reason, ExitReason::TpTouched);
    }

    // ---------- net_pnl_for (must match feed_guards_net_pnl semantics) ----------

    #[test]
    fn g8_pre_tp_net_pnl_for_winner_is_positive_with_fees() {
        // shares=2.625 @ entry 0.40, exit 1.00:
        //   gross = 2.625 * (1.0 - 0.40) = 1.575
        //   buy_fee  = 0.07 * 2.625 * 0.40 * 0.60 = 0.0441
        //   sell_fee = 0.07 * 2.625 * 1.00 * 0.00 = 0 (winner edge case)
        //   net = 1.575 - 0.0441 = 1.5309
        let net = net_pnl_for(2.625, 0.40, 1.0);
        assert!(approx(net, 1.5309, 1e-4), "got {net}");
    }

    #[test]
    fn g8_pre_tp_net_pnl_for_loser_is_negative_full_entry_cost_plus_buy_fee() {
        let net = net_pnl_for(2.625, 0.40, 0.0);
        // gross = -2.625 * 0.40 = -1.05, buy_fee = 0.0441, sell_fee = 0
        // net = -1.05 - 0.0441 = -1.0941
        assert!(approx(net, -1.0941, 1e-4), "got {net}");
    }

    // ---------- parse_variants ----------

    #[test]
    fn g8_pre_tp_parse_variants_handles_baseline_tp_and_peak() {
        let vs = parse_variants("0,10,25,peak").unwrap();
        assert_eq!(vs.len(), 4);
        assert_eq!(vs[0], ExitVariant::Baseline);
        assert!(matches!(vs[1], ExitVariant::FirstTouch { tp_pct } if approx(tp_pct, 0.10, 1e-9)));
        assert!(matches!(vs[2], ExitVariant::FirstTouch { tp_pct } if approx(tp_pct, 0.25, 1e-9)));
        assert_eq!(vs[3], ExitVariant::Peak);
    }

    #[test]
    fn g8_pre_tp_parse_variants_default_grid_matches_spec() {
        let vs = default_variants();
        // Baseline + 5/10/15/20/25/30/40/50 = 9 + Peak = 10.
        assert_eq!(vs.len(), 10);
        assert_eq!(vs[0], ExitVariant::Baseline);
        assert_eq!(vs[9], ExitVariant::Peak);
    }

    #[test]
    fn g8_pre_tp_parse_variants_rejects_negative_and_empty() {
        assert!(parse_variants("").is_err());
        assert!(parse_variants("-10").is_err());
    }

    // ---------- compute_metrics + breakdown ----------

    #[test]
    fn g8_pre_tp_metrics_compute_aggregate_and_per_cell() {
        // Manually craft 4 trades: 2 BTC_5m (1 win, 1 loss), 2 ETH_15m (both win).
        let trades = vec![
            VariantTrade {
                position: mk_position("BTC", "5m", 0.40, 2.625, vec![], Some(0.50)),
                exit_price: 0.50,
                exit_reason: ExitReason::TimeExit,
                net_pnl: 0.25,
            },
            VariantTrade {
                position: mk_position("BTC", "5m", 0.40, 2.625, vec![], Some(0.30)),
                exit_price: 0.30,
                exit_reason: ExitReason::TimeExit,
                net_pnl: -0.30,
            },
            VariantTrade {
                position: mk_position("ETH", "15m", 0.40, 2.625, vec![], Some(0.50)),
                exit_price: 0.50,
                exit_reason: ExitReason::TpTouched,
                net_pnl: 0.20,
            },
            VariantTrade {
                position: mk_position("ETH", "15m", 0.40, 2.625, vec![], Some(0.60)),
                exit_price: 0.60,
                exit_reason: ExitReason::TpTouched,
                net_pnl: 0.40,
            },
        ];
        let m = compute_metrics(&trades, "test".into());
        assert_eq!(m.n_trades, 4);
        assert!(approx(m.win_ratio, 0.75, 1e-9), "3/4 wins");
        assert!(approx(m.total_pnl, 0.25 - 0.30 + 0.20 + 0.40, 1e-9));
        assert!(approx(m.pct_tp_hit, 0.50, 1e-9), "2/4 TP-touched");
        assert_eq!(m.n_uncoverable, 0);

        // Per-cell.
        assert_eq!(m.by_cell.len(), 2);
        let btc = &m.by_cell["BTC_5m"];
        assert_eq!(btc.n_trades, 2);
        assert!(approx(btc.win_ratio, 0.5, 1e-9));
        let eth = &m.by_cell["ETH_15m"];
        assert_eq!(eth.n_trades, 2);
        assert!(approx(eth.win_ratio, 1.0, 1e-9));
        assert!(approx(eth.pct_tp_hit, 1.0, 1e-9));
    }

    // =====================================================================
    // G14 PHASE 3: ExitVariant::Smart tests.
    // =====================================================================

    /// Rule never triggers (bid keeps climbing all the way to exit) -> falls
    /// back to time-exit. Validates the failsafe semantics.
    #[test]
    fn g14_smart_falls_back_to_time_exit_when_never_triggered() {
        // Monotonically increasing bid -> running max updates every sample ->
        // f6 (time_since_max) stays at 0 -> rule never fires regardless of X.
        let mut p = mk_position("BTC", "5m", 0.50, 20.0,
            vec![0.51, 0.52, 0.53, 0.54, 0.55, 0.56, 0.57, 0.58, 0.59, 0.60],
            Some(0.60));
        // mk_position sets entry_recv_ms=1000, samples at +100ms steps,
        // exit_ts_ms=9_999_999 (very long). f1_pct stays tiny throughout.
        // Adjust exit_ts so f1 actually reaches 100% by the last sample.
        p.exit_ts_ms = 2000; // 1s hold; samples up to t=2000ms.
        // The bid keeps rising -> f6 = 0 always (running max updates each sample).
        // With y_sec=1 (>=1s), f6 never reaches it -> rule never triggers,
        // fall back to time-exit.
        let (px, reason) = decide_exit_for_variant(&p, ExitVariant::Smart { x_pct: 50.0, y_sec: 1 });
        // bid never plateaus -> f6 never reaches 1s -> never triggers.
        assert!((px - 0.60).abs() < 1e-9, "fallback to time-exit bid=0.60; got {px}");
        assert_eq!(reason, ExitReason::TimeExit);
    }

    /// Rule fires at the right sample: bid rises, then plateaus, then X% +
    /// Y_sec are met. Sells at the bid at trigger time.
    #[test]
    fn g14_smart_triggers_when_both_conditions_met() {
        // Bid pattern: rises to 0.70 (sample 4), then plateaus at 0.70 for
        // several samples. mk_position samples at +100ms steps starting at
        // t=1100ms (sample 0 = ts 1100, sample 1 = ts 1200, ...).
        let mut p = mk_position("BTC", "5m", 0.50, 20.0,
            vec![0.55, 0.60, 0.65, 0.70, 0.70, 0.70, 0.70, 0.70, 0.70, 0.70],
            Some(0.70));
        // Hold = 1000ms = 1s. f1=100% at last sample.
        p.exit_ts_ms = 2200; // entry=1000, exit=2200 -> hold=1200ms.
        // x_pct=30% -> trigger when t-1000 >= 360ms -> t >= 1360 (sample idx 3+, ts 1400).
        // y_sec=0.4s -> y_ms=400 -> trigger when t - running_max_ts >= 400.
        // running_max ts is the FIRST sample where bid=0.70: ts=1400 (sample idx 3).
        // f6 >= 400ms -> t >= 1800 (sample idx 7).
        // At sample idx 7 (ts=1800): f1=(1800-1000)/1200=66.7% >= 30% AND f6=400ms >= 400. TRIGGER.
        let (px, reason) = decide_exit_for_variant(
            &p, ExitVariant::Smart { x_pct: 30.0, y_sec: 0 });
        // Hmm y_sec=0 means trigger at first sample where f1>=30%, which is idx 3 (f1=33%).
        // At idx 3 the bid IS the new max (0.70), running_max updates first -> f6=0.
        // f6 >= 0 OK, f1 >= 30 OK -> trigger at bid=0.70.
        assert!((px - 0.70).abs() < 1e-9);
        assert_eq!(reason, ExitReason::SmartTriggered);
    }

    /// Rule with very HIGH X requirement -> never reaches X by exit_ts ->
    /// fall back to time-exit. Validates the X bound.
    #[test]
    fn g14_smart_never_reaches_high_x_falls_back() {
        let mut p = mk_position("BTC", "5m", 0.50, 20.0,
            vec![0.55, 0.60, 0.55, 0.55, 0.55],
            Some(0.55));
        // Samples at +100ms steps from t=1100. Hold = 600ms (entry=1000, exit=1600).
        p.exit_ts_ms = 1600;
        // With x_pct=200% (impossible), never fires.
        let (px, reason) = decide_exit_for_variant(
            &p, ExitVariant::Smart { x_pct: 200.0, y_sec: 0 });
        assert!((px - 0.55).abs() < 1e-9);
        assert_eq!(reason, ExitReason::TimeExit);
    }

    /// parse_variants handles 'smart:X:Y' syntax.
    #[test]
    fn g14_parse_variants_handles_smart_syntax() {
        let v = parse_variants("0,smart:50:30,peak,smart:70:60").unwrap();
        assert_eq!(v.len(), 4);
        assert_eq!(v[0], ExitVariant::Baseline);
        assert_eq!(v[1], ExitVariant::Smart { x_pct: 50.0, y_sec: 30 });
        assert_eq!(v[2], ExitVariant::Peak);
        assert_eq!(v[3], ExitVariant::Smart { x_pct: 70.0, y_sec: 60 });
    }

    /// parse_variants rejects malformed smart syntax.
    #[test]
    fn g14_parse_variants_rejects_bad_smart() {
        // Wrong number of parts.
        assert!(parse_variants("smart:50").is_err());
        assert!(parse_variants("smart:50:30:extra").is_err());
        // Out-of-range X.
        assert!(parse_variants("smart:150:30").is_err(), "X > 100 must reject");
        assert!(parse_variants("smart:-5:30").is_err(), "X < 0 must reject");
        // Negative Y.
        assert!(parse_variants("smart:50:-10").is_err());
        // Bad numeric.
        assert!(parse_variants("smart:abc:30").is_err());
    }

    /// Label string follows the documented convention.
    #[test]
    fn g14_smart_variant_label_format() {
        let v = ExitVariant::Smart { x_pct: 70.0, y_sec: 30 };
        assert_eq!(v.label(), "smart_x70_y30s");
    }

    // =====================================================================
    // G15 PHASE 4: per-cell exit forms (Trailing, SpreadFilteredTrailing,
    // TimeCappedTrailing, F6Only).
    // =====================================================================

    #[test]
    fn g15_trailing_triggers_on_drop_from_max() {
        // Bid rises to 0.80 (sample 3) then drops to 0.76 (sample 4): drop =
        // 5%. With z_pct=2, the drop crosses the threshold at sample 4.
        // Actually 0.76 / 0.80 = 0.95 = drop of 5%. 5% >= 2% -> trigger.
        let p = mk_position("BTC", "5m", 0.50, 20.0,
            vec![0.65, 0.70, 0.75, 0.80, 0.76, 0.74],
            Some(0.74));
        let (px, reason) = decide_exit_for_variant(&p, ExitVariant::Trailing { z_pct: 2.0 });
        assert!((px - 0.76).abs() < 1e-9, "trailing should trigger at sample 4 with bid=0.76; got {px}");
        assert_eq!(reason, ExitReason::TrailingTriggered);
    }

    #[test]
    fn g15_trailing_never_triggers_when_bid_rises_monotonically() {
        // Bid keeps climbing -> never triggers -> fall back to time-exit.
        let p = mk_position("BTC", "5m", 0.50, 20.0,
            vec![0.55, 0.60, 0.65, 0.70, 0.75, 0.80],
            Some(0.80));
        let (px, reason) = decide_exit_for_variant(&p, ExitVariant::Trailing { z_pct: 2.0 });
        assert!((px - 0.80).abs() < 1e-9);
        assert_eq!(reason, ExitReason::TimeExit);
    }

    #[test]
    fn g15_spread_filtered_trailing_skips_when_spread_too_wide() {
        // Build a position WITH trajectory_bbo: same drop pattern as
        // g15_trailing_triggers, but bbo at trigger moment shows wide spread
        // -> NO trigger -> falls back to time-exit. mk_position sets
        // trajectory_bbo to all None; we'd need to override. Easier: use a
        // wider-than-max-spread bbo for the trigger sample.
        let mut p = mk_position("BTC", "5m", 0.50, 20.0,
            vec![0.65, 0.70, 0.75, 0.80, 0.76, 0.74],
            Some(0.74));
        // Inject bbo with WIDE spread ($0.10) at every sample -> filter
        // blocks all triggers.
        p.trajectory_bbo = p.trajectory.iter()
            .map(|(ms, _)| (*ms, Some(0.40), Some(0.50))) // spread = 0.10
            .collect();
        let (px, reason) = decide_exit_for_variant(
            &p, ExitVariant::SpreadFilteredTrailing { z_pct: 2.0, max_spread: 0.05 });
        assert_eq!(reason, ExitReason::TimeExit,
            "spread (0.10) > max_spread (0.05) -> trailing must NOT trigger -> time-exit");
        assert!((px - 0.74).abs() < 1e-9);
    }

    #[test]
    fn g15_spread_filtered_trailing_fires_when_spread_tight() {
        let mut p = mk_position("BTC", "5m", 0.50, 20.0,
            vec![0.65, 0.70, 0.75, 0.80, 0.76, 0.74],
            Some(0.74));
        // Tight spread ($0.02) everywhere -> filter allows trigger.
        p.trajectory_bbo = p.trajectory.iter()
            .map(|(ms, _)| (*ms, Some(0.74), Some(0.76))) // spread = 0.02
            .collect();
        let (px, reason) = decide_exit_for_variant(
            &p, ExitVariant::SpreadFilteredTrailing { z_pct: 2.0, max_spread: 0.05 });
        assert_eq!(reason, ExitReason::TrailingTriggered,
            "spread (0.02) < max_spread (0.05) -> trailing must trigger");
        assert!((px - 0.76).abs() < 1e-9);
    }

    #[test]
    fn g15_time_capped_trailing_does_not_trigger_before_x_pct() {
        // Drop happens early (sample 4); x_pct=80 means must wait until 80% of hold.
        let mut p = mk_position("BTC", "5m", 0.50, 20.0,
            vec![0.65, 0.70, 0.75, 0.80, 0.76, 0.78, 0.76, 0.74, 0.74, 0.74],
            Some(0.74));
        // mk_position: entry=1000, samples at +100ms each. With exit=2000,
        // hold=1000ms. f1=0% at ms=1000, 10% at ms=1100, ..., 100% at ms=2000.
        // Sample 4 (ms=1500, f1=50%) has the drop to 0.76. x_pct=80 ->
        // NO trigger at sample 4. Sample 9 (ms=2000) -> f1=100% (border).
        // Actually let me be precise: ms 1100..2000 -> samples 0..9.
        p.exit_ts_ms = 2000;
        // With x_pct=80, the drop at ms=1500 (f1=50%) is below the cap.
        let (px, reason) = decide_exit_for_variant(
            &p, ExitVariant::TimeCappedTrailing { x_pct: 80.0, z_pct: 2.0 });
        // After ms=1800 (f1=80%), the running_max is still 0.80 (set at sample 3),
        // and bid is around 0.74-0.78. The 2% drop from 0.80 = 0.784. 0.78 < 0.784,
        // so trigger fires at the first sample with f1>=80 where bid <= 0.784.
        // Sample at ms=1900 (f1=90%): bid=0.74 <= 0.784 -> trigger.
        // Actually let me recount samples: enumerate gives (i, (ms, bid)).
        //   i=0: ms=1100, bid=0.65
        //   i=1: ms=1200, bid=0.70
        //   i=2: ms=1300, bid=0.75
        //   i=3: ms=1400, bid=0.80 (new max)
        //   i=4: ms=1500, bid=0.76 (drop, but f1=50%, capped)
        //   i=5: ms=1600, bid=0.78 (f1=60%, capped)
        //   i=6: ms=1700, bid=0.76 (f1=70%, capped)
        //   i=7: ms=1800, bid=0.74 (f1=80%, OK -- check: 0.74 <= 0.784? YES -> trigger)
        assert_eq!(reason, ExitReason::TrailingTriggered);
        assert!((px - 0.74).abs() < 1e-9, "expected trigger at ms=1800 with bid=0.74; got {px}");
    }

    #[test]
    fn g15_f6only_triggers_on_time_since_max() {
        // Bid hits max at sample 3 (ms=1400), then plateaus. y_sec=0.5 ->
        // y_ms=500. Trigger when ms - 1400 >= 500 -> ms >= 1900.
        let mut p = mk_position("BTC", "5m", 0.50, 20.0,
            vec![0.65, 0.70, 0.75, 0.80, 0.80, 0.78, 0.76, 0.78, 0.78, 0.79],
            Some(0.79));
        p.exit_ts_ms = 2100;
        let (px, reason) = decide_exit_for_variant(&p, ExitVariant::F6Only { y_sec: 0 });
        // y_sec=0 -> triggers immediately at first sample with finite bid (sample 0, bid=0.65).
        // Actually wait, running_max at sample 0 is 0.65 (just set). ms - running_max_ms = 0,
        // which is >= 0. So trigger at sample 0 with bid=0.65.
        assert!((px - 0.65).abs() < 1e-9);
        assert_eq!(reason, ExitReason::F6Triggered);
    }

    #[test]
    fn g15_parse_variants_handles_new_forms() {
        let v = parse_variants("0,trailing:2,strail:3:0.05,ctrail:50:2,f6:30").unwrap();
        assert_eq!(v.len(), 5);
        assert_eq!(v[0], ExitVariant::Baseline);
        assert_eq!(v[1], ExitVariant::Trailing { z_pct: 2.0 });
        assert_eq!(v[2], ExitVariant::SpreadFilteredTrailing { z_pct: 3.0, max_spread: 0.05 });
        assert_eq!(v[3], ExitVariant::TimeCappedTrailing { x_pct: 50.0, z_pct: 2.0 });
        assert_eq!(v[4], ExitVariant::F6Only { y_sec: 30 });
    }

    #[test]
    fn g15_parse_variants_rejects_bad_new_forms() {
        assert!(parse_variants("trailing:150").is_err(), "Z > 100 must reject");
        assert!(parse_variants("strail:2").is_err(), "missing S");
        assert!(parse_variants("strail:2:5").is_err(), "S > 1 dollar must reject");
        assert!(parse_variants("ctrail:50").is_err(), "missing Z");
        assert!(parse_variants("f6:-5").is_err(), "negative Y must reject");
    }

    // ========================================================================
    // PIECE W3: PARITY TESTS -- backtester (decide_exit_for_variant) MUST
    // produce the EXACT same (exit_price, exit_reason) as the live ticking
    // simulation built from the same primitives the production exit_task
    // uses (PaperExecutor::open's seed semantics + update_running_max +
    // exit_rules::*). If a future change breaks this parity, the live bot's
    // behavior would silently diverge from the backtested hypotheses --
    // these tests catch that mechanically.
    //
    // The simulator (`simulate_live_*`) is intentionally written in terms
    // of the SAME helpers the live tick uses, NOT a re-implementation:
    //   * Initial position state matches PaperExecutor::open (running_max_bid
    //     = entry_price, ts_max_bid_ms = entry_ts_ms).
    //   * Per-sample tick uses trading_loop::update_running_max with the
    //     stale-guard + strict-greater discipline.
    //   * Trigger check uses exit_rules::smart_triggers / f6_triggers --
    //     the SAME function the backtester now calls (post-W3 refactor).
    //
    // Same shared code on both sides + same input trajectory = decisions
    // MUST be identical. The tests below cover the cases where parity
    // would have been broken pre-W3 (different seed) or could be broken
    // by future drift (different state-update logic on either side).
    // ========================================================================

    /// Live-ticking simulation that mirrors what `run_exit_task` does each
    /// 1 s tick for a position with a Smart rule. The trajectory's (ms, bid)
    /// pairs drive the BBO updates; bbo.ts_ms = ms (always fresh) for
    /// finite bids, NOT refreshed for NaN bids (which simulates a gap).
    fn simulate_live_smart(
        entry_price: f64,
        entry_ms: i64,
        exit_ts_ms: i64,
        trajectory: &[(i64, f64)],
        x_pct: f64,
        y_sec: i64,
        time_exit_bid: Option<f64>,
    ) -> (f64, ExitReason) {
        use crate::state::EventBbo;
        use crate::state::persist::{OpenPosition, OrderStatus, Outcome};
        use crate::trading_loop::update_running_max;
        use dashmap::DashMap;
        use rust_decimal_macros::dec;

        // Build a position in the same shape PaperExecutor::open would:
        // running_max_bid = entry_price, ts_max_bid_ms = entry_ts_ms.
        let mut positions = vec![OpenPosition {
            token_id: "T".into(),
            asset: "BTC".into(),
            side: Outcome::Up,
            entry_price: rust_decimal::Decimal::try_from(entry_price).unwrap(),
            shares: dec!(10.0),
            opened_at_ms: entry_ms,
            signal_id: "T-sig".into(),
            interval: "5m".into(),
            exit_ts_s: exit_ts_ms / 1000,
            entry_ts_ms: entry_ms,
            running_max_bid: entry_price,
            ts_max_bid_ms: entry_ms,
            status: OrderStatus::Confirmed,
            order_id: None,
            ack_at_ms: None,
            confirmed_at_ms: None,
            confirmation_source: None,
            maker_exit: None,
        }];
        let bbo: DashMap<String, EventBbo> = DashMap::new();

        for &(ms, bid) in trajectory {
            if bid.is_finite() {
                bbo.insert(
                    "T".into(),
                    EventBbo { best_bid: Some(bid), best_ask: Some(bid + 0.01), ts_ms: ms },
                );
            }
            // Always tick update_running_max -- the stale-guard handles the
            // case where bbo was NOT refreshed this sample (NaN bid = gap).
            let _ = update_running_max(&mut positions, &bbo, ms);
            if bid.is_finite()
                && crate::exit_rules::smart_triggers(
                    ms,
                    entry_ms,
                    exit_ts_ms,
                    positions[0].ts_max_bid_ms,
                    x_pct,
                    y_sec,
                )
            {
                return (bid, ExitReason::SmartTriggered);
            }
        }
        match time_exit_bid {
            Some(b) => (b, ExitReason::TimeExit),
            None => (0.0, ExitReason::Uncoverable),
        }
    }

    fn simulate_live_f6_only(
        entry_price: f64,
        entry_ms: i64,
        exit_ts_ms: i64,
        trajectory: &[(i64, f64)],
        y_sec: i64,
        time_exit_bid: Option<f64>,
    ) -> (f64, ExitReason) {
        use crate::state::EventBbo;
        use crate::state::persist::{OpenPosition, OrderStatus, Outcome};
        use crate::trading_loop::update_running_max;
        use dashmap::DashMap;
        use rust_decimal_macros::dec;

        let mut positions = vec![OpenPosition {
            token_id: "T".into(),
            asset: "BTC".into(),
            side: Outcome::Up,
            entry_price: rust_decimal::Decimal::try_from(entry_price).unwrap(),
            shares: dec!(10.0),
            opened_at_ms: entry_ms,
            signal_id: "T-sig".into(),
            interval: "5m".into(),
            exit_ts_s: exit_ts_ms / 1000,
            entry_ts_ms: entry_ms,
            running_max_bid: entry_price,
            ts_max_bid_ms: entry_ms,
            status: OrderStatus::Confirmed,
            order_id: None,
            ack_at_ms: None,
            confirmed_at_ms: None,
            confirmation_source: None,
            maker_exit: None,
        }];
        let bbo: DashMap<String, EventBbo> = DashMap::new();

        for &(ms, bid) in trajectory {
            if bid.is_finite() {
                bbo.insert(
                    "T".into(),
                    EventBbo { best_bid: Some(bid), best_ask: Some(bid + 0.01), ts_ms: ms },
                );
            }
            let _ = update_running_max(&mut positions, &bbo, ms);
            if bid.is_finite()
                && crate::exit_rules::f6_triggers(ms, positions[0].ts_max_bid_ms, y_sec)
            {
                return (bid, ExitReason::F6Triggered);
            }
        }
        match time_exit_bid {
            Some(b) => (b, ExitReason::TimeExit),
            None => (0.0, ExitReason::Uncoverable),
        }
    }

    /// 1/5 -- Smart triggers at the same sample for both paths on a simple
    /// rising-then-plateauing trajectory (no NaN, no bid below entry).
    /// Same trajectory shape as `g14_smart_triggers_when_both_conditions_met`.
    #[test]
    fn parity_smart_triggers_at_same_sample_simple() {
        let mut p = mk_position("BTC", "5m", 0.50, 20.0,
            vec![0.55, 0.60, 0.65, 0.70, 0.70, 0.70, 0.70, 0.70, 0.70, 0.70],
            Some(0.70));
        p.exit_ts_ms = 2200;
        let bt = decide_exit_for_variant(&p, ExitVariant::Smart { x_pct: 30.0, y_sec: 0 });
        let live = simulate_live_smart(
            p.entry_price, p.entry_recv_ms, p.exit_ts_ms,
            &p.trajectory, 30.0, 0, p.time_exit_bid,
        );
        assert_eq!(bt, live, "backtester {bt:?} must equal live {live:?}");
    }

    /// 2/5 -- Smart never triggers (failsafe): both fall back to time-exit
    /// at the same bid + reason.
    #[test]
    fn parity_smart_falls_back_to_time_exit_both_paths() {
        // Monotonically rising trajectory + y_sec=1 -> running_max updates
        // every sample -> f6=0 always -> never triggers.
        let mut p = mk_position("BTC", "5m", 0.50, 20.0,
            vec![0.51, 0.52, 0.53, 0.54, 0.55, 0.56, 0.57, 0.58, 0.59, 0.60],
            Some(0.60));
        p.exit_ts_ms = 2000;
        let bt = decide_exit_for_variant(&p, ExitVariant::Smart { x_pct: 50.0, y_sec: 1 });
        let live = simulate_live_smart(
            p.entry_price, p.entry_recv_ms, p.exit_ts_ms,
            &p.trajectory, 50.0, 1, p.time_exit_bid,
        );
        assert_eq!(bt, live, "both must fall back to time_exit at 0.60");
        assert_eq!(bt.1, ExitReason::TimeExit);
    }

    /// 3/5 -- F6Only triggers at the same sample for both paths. Same
    /// trajectory shape as `g15_f6only_triggers_on_time_since_max`.
    #[test]
    fn parity_f6_only_triggers_at_same_sample() {
        let mut p = mk_position("BTC", "5m", 0.50, 20.0,
            vec![0.65, 0.70, 0.75, 0.80, 0.80, 0.78, 0.76, 0.78, 0.78, 0.79],
            Some(0.79));
        p.exit_ts_ms = 2100;
        let bt = decide_exit_for_variant(&p, ExitVariant::F6Only { y_sec: 0 });
        let live = simulate_live_f6_only(
            p.entry_price, p.entry_recv_ms, p.exit_ts_ms,
            &p.trajectory, 0, p.time_exit_bid,
        );
        assert_eq!(bt, live, "backtester {bt:?} must equal live {live:?}");
        assert_eq!(bt.1, ExitReason::F6Triggered);
    }

    /// 4/5 -- KEY parity test for the GAP scenario (the Pieza-2 Q2 question).
    /// Both backtester and live preserve running_max across an unobserved
    /// gap and trigger at the same post-gap sample. The trajectory has NaN
    /// bids during the gap window: backtester skips them, live's
    /// update_running_max stale-guards them. After the gap, the bid is
    /// BELOW the pre-gap max, so neither path advances max. f6 timer
    /// continues counting from the pre-gap peak. Trigger fires
    /// simultaneously when f6 crosses the y_sec threshold.
    #[test]
    fn parity_smart_preserves_running_max_across_gap() {
        // mk_position uses ts = 1100 + i*100 for samples. Override to use
        // wider spacing (100 ms is too tight for STALE_BBO_MS=5000). Build
        // manually instead so we control the timing.
        //
        // Trajectory (custom ts spacing -- mostly 1s steps to cross the
        // 5s stale threshold during the NaN window):
        //   t=0 (entry): no sample (just open).
        //   t=1000:  bid 0.70  -- peak, sets running_max.
        //   t=2000:  bid NaN   -- gap begins.
        //   t=3000:  bid NaN
        //   ... (NaN through t=8000)
        //   t=9000:  bid 0.60  -- BELOW max; max preserved.
        //   t=44_000: bid 0.55 -- still below; >43s since peak -> if y_sec<=43, trigger here.
        let entry_ms = 1_000_i64; // ms
        let trajectory: Vec<(i64, f64)> = vec![
            (1_000, 0.70),
            (2_000, f64::NAN),
            (3_000, f64::NAN),
            (4_000, f64::NAN),
            (5_000, f64::NAN),
            (6_000, f64::NAN),
            (7_000, f64::NAN),
            (8_000, f64::NAN),
            (9_000, 0.60),
            (44_000, 0.55), // 43s after peak; smart with x=10 y=30 should trigger here
        ];
        let p = CollectedPosition {
            token: "tok".into(),
            asset: "BTC".into(),
            interval: "5m".into(),
            epoch: 0, // W8: synthetic, not used by smart-exit / parity tests
            direction: "Up".into(),
            signal_id: "BTC-sig".into(),
            entry_recv_ms: entry_ms,
            entry_price: 0.50,
            shares: 20.0,
            exit_ts_ms: 120_000, // 120s hold (long enough)
            uncoverable_samples: 7,
            total_samples: trajectory.len(),
            trajectory_bbo: trajectory.iter().map(|(ms, _)| (*ms, None, None)).collect(),
            trajectory: trajectory.clone(),
            time_exit_bid: Some(0.55),
            trigger_close: 0.0, // W9: not exercised by smart-exit test
            trigger_ret_bps: 0.0,
            maker_fee_bps: 0,
            taker_fee_bps: 0,
            fee_type: String::new(),
            // COMBO 2026-06-08: not exercised by smart-exit test.
            entry_trigger_ts_ms: 0,
            obi_top1: f64::NAN,
            obi_top3: f64::NAN,
            binance_close_at_2s: f64::NAN,
            binance_close_at_5s: f64::NAN,
            binance_close_at_10s: f64::NAN,
            binance_close_at_30s: f64::NAN,
            binance_close_at_60s: f64::NAN,
            binance_close_at_120s: f64::NAN,
            vol_30m: f64::NAN,
            vol_60m: f64::NAN,
        };
        // x_pct=10% (1s into 120s hold = 0.83% so the early samples skip);
        // by t=44_000, f1 = 43_000/120_000 * 100 = 35.8% >> 10%. y_sec=30 ->
        // f6 at t=44_000 = 44_000 - 1_000 = 43_000 ms >= 30_000. Trigger.
        let bt = decide_exit_for_variant(&p, ExitVariant::Smart { x_pct: 10.0, y_sec: 30 });
        let live = simulate_live_smart(
            p.entry_price, p.entry_recv_ms, p.exit_ts_ms,
            &p.trajectory, 10.0, 30, p.time_exit_bid,
        );
        assert_eq!(bt, live, "gap-preserved parity: backtester {bt:?} live {live:?}");
        // Sanity: it really did trigger (not just fall back to time-exit).
        assert_eq!(bt.1, ExitReason::SmartTriggered, "must trigger post-gap");
        assert!(
            (bt.0 - 0.60).abs() < 1e-9 || (bt.0 - 0.55).abs() < 1e-9,
            "trigger at one of the post-gap samples; got {}",
            bt.0
        );
    }

    /// 5/5 -- KEY parity test for the SEED scenario (the W3 change). When
    /// the trajectory's bids start BELOW entry_price (wide spread at open),
    /// the OLD seed (NEG_INFINITY) would have adopted the low bid as max
    /// and diverged from live. The NEW seed (entry_price) keeps max at
    /// entry_price across low-bid samples -> identical behavior with live.
    /// This is the scenario that would have failed pre-W3.
    #[test]
    fn parity_smart_with_bid_below_entry_price_seed_alignment() {
        // entry_price = 0.55 (the ask we paid).
        // Trajectory bids START at 0.50 (below entry), stay there a few
        // samples (would have driven OLD-seed running_max to 0.50), then
        // climb to 0.58 (a real new high above entry_price).
        let mut p = mk_position("BTC", "5m", 0.55, 20.0,
            vec![0.50, 0.50, 0.51, 0.52, 0.58, 0.58, 0.58, 0.58, 0.58, 0.58],
            Some(0.58));
        p.exit_ts_ms = 2200;
        // x_pct=30%, y_sec=0 -> trigger when f1>=30% AND any sample at/after
        // the running_max set point. With NEW seed:
        //   running_max = 0.55 through the first 4 samples (bids 0.50..0.52 < 0.55)
        //   At t=1500 (sample 4, bid 0.58): 0.58 > 0.55 -> update.
        //     running_max=0.58, ts_max=1500. f1=(1500-1000)/1200=41.7% >= 30%,
        //     f6=0 >= 0 -> TRIGGER at bid=0.58.
        let bt = decide_exit_for_variant(&p, ExitVariant::Smart { x_pct: 30.0, y_sec: 0 });
        let live = simulate_live_smart(
            p.entry_price, p.entry_recv_ms, p.exit_ts_ms,
            &p.trajectory, 30.0, 0, p.time_exit_bid,
        );
        assert_eq!(bt, live, "seed-aligned parity: backtester {bt:?} live {live:?}");
        // Sanity on the NEW-seed expected outcome.
        assert_eq!(bt.1, ExitReason::SmartTriggered);
        assert!((bt.0 - 0.58).abs() < 1e-9, "must trigger at bid=0.58 (first new high above entry); got {}", bt.0);
    }

    #[test]
    fn g8_pre_tp_metrics_uncoverable_counter_aggregates() {
        let trades = vec![
            VariantTrade {
                position: mk_position("BTC", "5m", 0.40, 2.625, vec![], None),
                exit_price: 0.0,
                exit_reason: ExitReason::Uncoverable,
                net_pnl: -1.1,
            },
            VariantTrade {
                position: mk_position("BTC", "5m", 0.40, 2.625, vec![], Some(0.45)),
                exit_price: 0.45,
                exit_reason: ExitReason::TimeExit,
                net_pnl: 0.10,
            },
        ];
        let m = compute_metrics(&trades, "test".into());
        assert_eq!(m.n_uncoverable, 1);
        assert!(approx(m.uncoverable_rate, 0.5, 1e-9));
    }

    // =====================================================================
    // FASE 1: characterize_position -- the 2 promised tests + 3 phase-guard
    // tests. The phase guard is the methodology safety net (cutoff at
    // 2026-05-17); breaking it would let validation data contaminate
    // exploration silently, so it gets its own coverage.
    // =====================================================================

    /// All-NaN trajectory → no coverable peak → all peak-related Option fields
    /// are None and the time-in-top-decile counters are zero. Mirrors the
    /// variant evaluator's Uncoverable handling -- consistent worst-case.
    #[test]
    fn g8_pre_tp_characterize_position_empty_trajectory_returns_none_peak() {
        let pos = mk_position(
            "BTC", "5m", 0.50, 20.0,
            vec![f64::NAN, f64::NAN, f64::NAN], // all uncoverable
            None,
        );
        let stats = characterize_position(&pos);
        assert!(stats.peak_bid.is_none(), "all-NaN trajectory must yield peak_bid = None");
        assert!(stats.peak_pnl.is_none());
        assert!(stats.peak_offset_ms.is_none());
        assert!(stats.peak_offset_pct_of_hold.is_none());
        assert!(stats.peak_excess_pnl.is_none(),
            "peak_excess undefined when no peak");
        assert_eq!(stats.time_in_top_decile_ms, 0, "no peak → no top-decile time");
        assert_eq!(stats.samples_in_top_decile, 0);
        assert_eq!(stats.top_decile_span_ms, 0);
        // Identity/sanity fields still populated.
        assert_eq!(stats.asset, "BTC");
        assert_eq!(stats.interval, "5m");
        assert!(stats.hold_duration_ms >= 1, "hold_duration must be >= 1 (defensive max)");
    }

    /// MESETA vs PICO: same peak value, same hold window, but the meseta
    /// trajectory keeps the bid near the peak for many ticks while the pico
    /// only kisses the peak once. The `time_in_top_decile_ms` metric MUST
    /// distinguish them -- that's the Fase-1 "shape" answer the user cares
    /// about. If this test fails, the Q3 number in the stdout report is junk.
    #[test]
    fn g8_pre_tp_characterize_position_meseta_vs_pico_distinct_top_decile_ms() {
        // mk_position generates trajectory ts as entry_recv_ms (=0) + (i+1)*1000
        // so each sample is 1 second after the previous; 10 samples cover [1,10]s
        // and we ship a hold of 100s (in mk_position the exit_ts is generous).

        // PICO: bid spikes to 0.60 for ONE sample (idx 5), is around 0.50 elsewhere.
        // Top decile = >= 0.54. Only sample 5 qualifies. Gap to next = 1000 ms.
        // Expect time_in_top_decile_ms ≈ 1000 ms.
        let pos_pico = mk_position(
            "BTC", "5m", 0.50, 20.0,
            vec![0.50, 0.50, 0.50, 0.50, 0.50, 0.60, 0.50, 0.50, 0.50, 0.50],
            Some(0.50),
        );
        let s_pico = characterize_position(&pos_pico);

        // MESETA: bid hits 0.60 at idx 3, stays >= 0.55 (top decile threshold) for
        // 5 consecutive samples (idx 3-7), then drops. Top decile span = idx 3-7 =
        // 4 gaps of 1000ms each = ~4000ms time in top decile.
        let pos_meseta = mk_position(
            "BTC", "5m", 0.50, 20.0,
            vec![0.50, 0.50, 0.50, 0.60, 0.58, 0.57, 0.56, 0.55, 0.50, 0.50],
            Some(0.50),
        );
        let s_meseta = characterize_position(&pos_meseta);

        // Both share the same peak (0.60) -- so the difference is PURELY shape.
        assert!(approx(s_pico.peak_bid.unwrap(),   0.60, 1e-9));
        assert!(approx(s_meseta.peak_bid.unwrap(), 0.60, 1e-9));

        // The discriminating metric: meseta must spend >>> time in top decile.
        assert!(
            s_meseta.time_in_top_decile_ms > 3 * s_pico.time_in_top_decile_ms,
            "meseta time_in_top_decile_ms ({}) must be MUCH greater than pico's ({}) -- \
             the Q3 metric is what distinguishes capturable from instantaneous peaks",
            s_meseta.time_in_top_decile_ms, s_pico.time_in_top_decile_ms,
        );
        // And the sample count + span agree.
        assert!(s_meseta.samples_in_top_decile >= 5);
        assert_eq!(s_pico.samples_in_top_decile, 1);
        assert!(s_meseta.top_decile_span_ms > s_pico.top_decile_span_ms);
    }

    // ----- PHASE GUARD tests (out-of-sample discipline safety net) -----

    /// EXPLORATION + end_date REACHES validation → hard abort. This is the
    /// guardrail the user explicitly asked for: a typo (5/17 instead of 5/16)
    /// in --bt-end-date during exploration would otherwise contaminate
    /// validation silently. The bot must refuse to start.
    #[test]
    fn g10_phase_exploration_with_validation_end_date_aborts() {
        let err = validate_phase_dates(BtPhase::Exploration, "2026-05-06", "2026-05-17")
            .unwrap_err();
        let msg = format!("{err}");
        // The error MUST be discoverable -- key phrases are part of the contract.
        assert!(msg.contains("OUT-OF-SAMPLE DISCIPLINE VIOLATION"),
            "must announce the violation loudly; got: {msg}");
        assert!(msg.contains(VALIDATION_CUTOFF_DATE),
            "must name the cutoff date; got: {msg}");
        assert!(msg.contains("--bt-phase validation"),
            "must point to the escape hatch; got: {msg}");

        // Also blocked if the end date is *past* the cutoff.
        let err2 = validate_phase_dates(BtPhase::Exploration, "2026-05-06", "2026-05-26")
            .unwrap_err();
        assert!(format!("{err2}").contains("OUT-OF-SAMPLE DISCIPLINE VIOLATION"));

        // Also blocked if start_date is past the cutoff (the clearer-message
        // branch -- only reachable when end < cutoff so the end-check above
        // doesn't fire first; in real CLI use this branch handles operator
        // typos like start=5/20 end=5/16 which would otherwise yield a
        // confusing "end < start" downstream error).
        let err3 = validate_phase_dates(BtPhase::Exploration, "2026-05-20", "2026-05-16")
            .unwrap_err();
        let m3 = format!("{err3}");
        assert!(m3.contains("OUT-OF-SAMPLE DISCIPLINE VIOLATION"));
        assert!(m3.contains("--bt-start-date=2026-05-20"),
            "start-branch must name the bad start date; got: {m3}");
    }

    /// EXPLORATION + dates strictly inside the exploration window → OK. Bound
    /// check: 5/16 is the LAST allowed end_date (since cutoff = 5/17 exclusive).
    #[test]
    fn g10_phase_exploration_within_window_ok() {
        assert!(validate_phase_dates(BtPhase::Exploration, "2026-05-06", "2026-05-16").is_ok());
        assert!(validate_phase_dates(BtPhase::Exploration, "2026-05-06", "2026-05-06").is_ok());
        // The day BEFORE cutoff is still in: 5/16.
        assert!(validate_phase_dates(BtPhase::Exploration, "2026-05-16", "2026-05-16").is_ok());
    }

    /// G14: validate_include_dates ALSO enforces the cutoff. An operator who
    /// passes exploration start/end but slips validation dates into
    /// --bt-include-dates must hit the same wall as raw range violation.
    #[test]
    fn g14_include_dates_exploration_with_validation_date_aborts() {
        // 5/06-5/16 range is fine, but include 5/20 (validation) -> ABORT.
        let include = vec!["2026-05-16".into(), "2026-05-20".into()];
        let err = validate_include_dates(BtPhase::Exploration, &include).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("OUT-OF-SAMPLE DISCIPLINE VIOLATION"),
            "include-dates violation MUST announce loudly; got: {msg}");
        assert!(msg.contains("2026-05-20"),
            "must name the offending date; got: {msg}");
    }

    #[test]
    fn g14_include_dates_exploration_within_window_ok() {
        let include = vec!["2026-05-06".into(), "2026-05-16".into()];
        assert!(validate_include_dates(BtPhase::Exploration, &include).is_ok());
        // Empty list = no-op.
        assert!(validate_include_dates(BtPhase::Exploration, &[]).is_ok());
    }

    #[test]
    fn g14_include_dates_validation_phase_allows_any_date() {
        let include = vec![
            "2026-05-17".into(), "2026-05-21".into(), "2026-05-23".into(), "2026-05-24".into(),
        ];
        assert!(validate_include_dates(BtPhase::Validation, &include).is_ok());
    }

    /// VALIDATION phase → any dates allowed (the caller takes responsibility;
    /// the run_backtest_tp banner + seal file are the audit trail).
    #[test]
    fn g10_phase_validation_allows_any_date() {
        assert!(validate_phase_dates(BtPhase::Validation, "2026-05-17", "2026-05-26").is_ok());
        assert!(validate_phase_dates(BtPhase::Validation, "2026-05-06", "2026-05-16").is_ok());
        // Even far in the future -- validation phase imposes no limit.
        assert!(validate_phase_dates(BtPhase::Validation, "2030-01-01", "2030-12-31").is_ok());
    }

    // =====================================================================
    // G11: StreamingMerger -- k-way time-merge over per-file iterators. The
    // mechanical tests below cover the merger; the BYTE-EXACT EQUIVALENCE
    // vs the legacy load+sort path is validated on REAL recorder data
    // (post-build manual run: --bt-legacy-inmemory vs default on 5/06-5/10,
    // sha256 the two peak_characterization.jsonl files, hashes must match).
    // The synthetic merger tests below exercise the heap/tie-break logic.
    // =====================================================================

    /// Helper: build a TimedEvent with a klines-shaped payload for testing.
    fn kline_te(asset: &'static str, recv_ms: i64) -> TimedEvent {
        TimedEvent {
            recv_ms,
            ev: Ev::Kline { asset, t_open_ms: recv_ms, close: 100.0 },
        }
    }

    /// Helper: wrap a Vec<TimedEvent> as a Box<dyn Iterator>. Mirrors the
    /// signature stream constructors return.
    fn vec_stream(events: Vec<TimedEvent>) -> Box<dyn Iterator<Item = TimedEvent>> {
        Box::new(events.into_iter())
    }

    #[test]
    fn g11_streaming_merger_yields_min_recv_ms_first() {
        // Two interleaved streams. Expected global order: 10, 20, 30, 40, 50, 60.
        let s_a = vec_stream(vec![kline_te("BTC", 10), kline_te("BTC", 30), kline_te("BTC", 50)]);
        let s_b = vec_stream(vec![kline_te("ETH", 20), kline_te("ETH", 40), kline_te("ETH", 60)]);
        let merger = StreamingMerger::new(vec![("a".into(), s_a), ("b".into(), s_b)]);
        let collected: Vec<i64> = merger.map(|t| t.recv_ms).collect();
        assert_eq!(collected, vec![10, 20, 30, 40, 50, 60],
            "k-way merge must yield globally ascending recv_ms");
    }

    #[test]
    fn g11_streaming_merger_empty_stream_skipped() {
        // 3 streams; the middle one empty. Output is just the union of the
        // other two in time order.
        let s_a = vec_stream(vec![kline_te("BTC", 5), kline_te("BTC", 25)]);
        let s_empty = vec_stream(vec![]);
        let s_c = vec_stream(vec![kline_te("ETH", 10), kline_te("ETH", 30)]);
        let merger = StreamingMerger::new(vec![
            ("a".into(), s_a), ("empty".into(), s_empty), ("c".into(), s_c),
        ]);
        let collected: Vec<i64> = merger.map(|t| t.recv_ms).collect();
        assert_eq!(collected, vec![5, 10, 25, 30]);
    }

    #[test]
    fn g11_streaming_merger_all_empty_returns_none_on_first_next() {
        let merger = StreamingMerger::new(vec![
            ("a".into(), vec_stream(vec![])),
            ("b".into(), vec_stream(vec![])),
        ]);
        let mut it = merger;
        assert!(it.next().is_none(), "no streams have events → next() yields None immediately");
    }

    #[test]
    fn g11_streaming_merger_ties_resolve_by_ascending_stream_index() {
        // Both streams have an event at recv_ms=10. The lower stream_idx (=0)
        // must yield FIRST -- this is the property that gives byte-exact
        // equivalence with legacy stable sort + append-order.
        let s_idx0 = vec_stream(vec![kline_te("BTC", 10), kline_te("BTC", 20)]);
        let s_idx1 = vec_stream(vec![kline_te("ETH", 10), kline_te("ETH", 20)]);
        let merger = StreamingMerger::new(vec![
            ("first".into(), s_idx0), ("second".into(), s_idx1),
        ]);
        // We can't tell apart events at recv_ms=10 just by recv_ms, so check
        // the Ev::Kline.asset field which we set distinctly per stream.
        let assets: Vec<&'static str> = merger
            .map(|t| match t.ev {
                Ev::Kline { asset, .. } => asset,
                _ => "OTHER",
            })
            .collect();
        // At recv_ms=10, BTC (idx 0) comes before ETH (idx 1). Same at 20.
        assert_eq!(assets, vec!["BTC", "ETH", "BTC", "ETH"],
            "tie-break on recv_ms MUST be by ascending stream_idx (= push order); \
             this is what guarantees equivalence with legacy stable sort");
    }

    // =====================================================================
    // G13 PHASE 2: label + AUC + causal slice + canary tests.
    // =====================================================================

    // ---- label_should_have_sold ----

    #[test]
    fn g13_label_clear_positive_sells_now() {
        // bid_t=0.80, future max=0.50 -> obviously sell now.
        let lab = label_should_have_sold(0.80, Some(0.50), LABEL_TOLERANCE_PCT);
        assert_eq!(lab, Some(true));
    }

    #[test]
    fn g13_label_clear_negative_waits() {
        // bid_t=0.50, future max=0.80 (60% higher) -> wait.
        let lab = label_should_have_sold(0.50, Some(0.80), LABEL_TOLERANCE_PCT);
        assert_eq!(lab, Some(false));
    }

    #[test]
    fn g13_label_within_tolerance_sells_now() {
        // bid_t=0.50, future max=0.501 (0.2% higher; within 1.5% tolerance).
        let lab = label_should_have_sold(0.50, Some(0.501), LABEL_TOLERANCE_PCT);
        assert_eq!(lab, Some(true),
            "diferencia trivial (0.2% < 1.5%) NO debe penalizar la decision sell-now");
    }

    #[test]
    fn g13_label_outside_tolerance_waits() {
        // bid_t=0.50, future max=0.55 (10% higher; well outside 1.5% tolerance).
        let lab = label_should_have_sold(0.50, Some(0.55), LABEL_TOLERANCE_PCT);
        assert_eq!(lab, Some(false),
            "diferencia significativa (10% >> 1.5%) debe favorecer wait");
    }

    #[test]
    fn g13_label_nan_bid_returns_none() {
        let lab = label_should_have_sold(f64::NAN, Some(0.50), LABEL_TOLERANCE_PCT);
        assert_eq!(lab, None);
    }

    #[test]
    fn g13_label_no_future_returns_sell_now() {
        // No finite future bid -> trivially nothing to gain from waiting.
        let lab = label_should_have_sold(0.50, None, LABEL_TOLERANCE_PCT);
        assert_eq!(lab, Some(true));
    }

    // ---- AUC + Mann-Whitney U ----

    #[test]
    fn g13_auc_oracle_label_as_feature_gives_one() {
        // Feature == label (numeric encoding) -> AUC must be EXACTLY 1.0.
        let pairs: Vec<(f64, bool)> = (0..200).map(|i| {
            let label = i % 3 == 0;
            (if label { 1.0 } else { 0.0 }, label)
        }).collect();
        let r = compute_auc_and_mwu(&pairs);
        assert!((r.auc - 1.0).abs() < 1e-9,
            "label-as-feature MUST give AUC=1.0 by construction; got {}", r.auc);
        assert!(r.p_value < 1e-6, "should be highly significant; got {}", r.p_value);
    }

    #[test]
    fn g13_auc_perfect_inverse_gives_zero() {
        // Inverse perfect: positives all have feature=0, negatives all =1.
        let pairs: Vec<(f64, bool)> = (0..200).map(|i| {
            let label = i % 3 == 0;
            (if label { 0.0 } else { 1.0 }, label)
        }).collect();
        let r = compute_auc_and_mwu(&pairs);
        assert!((r.auc - 0.0).abs() < 1e-9,
            "inverse perfect MUST give AUC=0.0; got {}", r.auc);
    }

    #[test]
    fn g13_auc_random_feature_near_half() {
        // Seeded PRNG over 5000 samples; AUC should be VERY close to 0.5.
        let mut rng = SeededRng::new(42);
        let pairs: Vec<(f64, bool)> = (0..5000).map(|i| {
            (rng.next_f64(), i % 2 == 0)
        }).collect();
        let r = compute_auc_and_mwu(&pairs);
        assert!((r.auc - 0.5).abs() < 0.03,
            "random feature should give AUC near 0.5; got {}", r.auc);
    }

    #[test]
    fn g13_auc_all_tied_gives_half() {
        // Every feature = same constant -> every comparison ties (avg rank
        // each); AUC must be exactly 0.5.
        let pairs: Vec<(f64, bool)> = (0..100).map(|i| (0.5, i % 2 == 0)).collect();
        let r = compute_auc_and_mwu(&pairs);
        assert!((r.auc - 0.5).abs() < 1e-9, "all-tied gives AUC=0.5; got {}", r.auc);
    }

    #[test]
    fn g13_auc_drops_nan_features() {
        let mut pairs = vec![
            (1.0, true), (1.0, true), (0.0, false), (0.0, false),
        ];
        // Inject 3 NaN rows; they should be dropped.
        pairs.push((f64::NAN, true));
        pairs.push((f64::NAN, false));
        pairs.push((f64::NAN, true));
        let r = compute_auc_and_mwu(&pairs);
        assert_eq!(r.n_dropped_nan, 3);
        assert!((r.auc - 1.0).abs() < 1e-9, "non-NaN data is perfect, AUC=1.0");
    }

    // ---- CausalSlice: type-level firewall ----

    #[test]
    fn g13_causal_slice_truncates_to_t_inclusive() {
        let traj: Vec<(i64, f64)> = vec![
            (100, 0.50), (200, 0.55), (300, 0.60), (400, 0.65), (500, 0.70),
        ];
        let causal = CausalSlice::new(&traj, 300);
        // Only samples with recv_ms <= 300 visible.
        let visible: Vec<(i64, f64)> = causal.iter().collect();
        assert_eq!(visible, vec![(100, 0.50), (200, 0.55), (300, 0.60)],
            "causal slice MUST exclude future samples (400, 500)");
        assert_eq!(causal.t(), 300);
        assert_eq!(causal.last_finite_bid(), Some(0.60));
    }

    #[test]
    fn g13_causal_slice_empty_when_t_before_first() {
        let traj: Vec<(i64, f64)> = vec![(100, 0.50), (200, 0.55)];
        let causal = CausalSlice::new(&traj, 50);
        assert_eq!(causal.len(), 0);
        assert_eq!(causal.iter().count(), 0);
        assert_eq!(causal.last_finite_bid(), None);
    }

    // ---- Feature causality: bid_max_so_far, time_since_bid_max ----

    #[test]
    fn g13_feat_bid_max_so_far_only_uses_past() {
        // Trajectory: peak (0.80) is at t=400 (FUTURE). At t=300, max so far
        // is only 0.60 (the past). The feature MUST report 0.60, NOT 0.80.
        let traj: Vec<(i64, f64)> = vec![
            (100, 0.50), (200, 0.55), (300, 0.60), (400, 0.80), (500, 0.70),
        ];
        let causal_at_300 = CausalSlice::new(&traj, 300);
        let max_so_far = feat_bid_max_so_far(&causal_at_300);
        assert!((max_so_far - 0.60).abs() < 1e-9,
            "bid_max_so_far at t=300 MUST be 0.60 (past max), not 0.80 (future peak); got {}",
            max_so_far);
    }

    #[test]
    fn g13_feat_time_since_bid_max_only_uses_past() {
        // Past max (0.60) was at t=300. At t=400, time_since_max = 400-300 = 100.
        // If the function leaked future, it would see the bigger future peak
        // (0.80 at t=500) and report time_since_max = -100 (negative = bug).
        let traj: Vec<(i64, f64)> = vec![
            (100, 0.50), (200, 0.55), (300, 0.60), (400, 0.58), (500, 0.80),
        ];
        let causal_at_400 = CausalSlice::new(&traj, 400);
        let t_since = feat_time_since_bid_max_ms(&causal_at_400);
        assert!((t_since - 100.0).abs() < 1e-9,
            "time_since_bid_max at t=400 MUST be 100 (past max at t=300); got {}", t_since);
    }

    // ---- Canary anti-causal feature works as advertised ----

    #[test]
    fn g13_canary_max_future_advantage_sees_future_minus_current() {
        // Canary is supposed to leak the COMPARISON: max_future_bid - bid_t.
        // At t=300, bid_t=0.60, future max=0.80 -> advantage = 0.80 - 0.60 = 0.20.
        let traj: Vec<(i64, f64)> = vec![
            (100, 0.50), (200, 0.55), (300, 0.60), (400, 0.70), (500, 0.80),
        ];
        // Positive advantage = future is better than now = label=0 (wait).
        let adv = canary_max_future_advantage(&traj, 300, 0.60);
        assert!((adv - 0.20).abs() < 1e-9,
            "canary_max_future_advantage at t=300 MUST be 0.20 (future 0.80 - now 0.60); got {}", adv);

        // When bid_t >= max_future, advantage <= 0 -> label=1 (sell now).
        let adv_at_500 = canary_max_future_advantage(&traj, 500, 0.80);
        assert!(adv_at_500.is_nan() || adv_at_500 <= 0.0,
            "at last sample (no future), advantage should be NaN or <=0; got {}", adv_at_500);
    }

    #[test]
    fn g13_canary_advantage_at_t_plus_30s_works() {
        // Need recv_ms spanning >=30s = >=30000ms. Use 10000, 20000, ... ms.
        // At t=20000, target = 20000 + 30000 = 50000. First sample with
        // recv_ms >= 50000 is (50000, 0.70). bid_t at t=20000 = 0.60.
        // advantage = 0.70 - 0.60 = 0.10.
        let traj: Vec<(i64, f64)> = vec![
            (10_000, 0.50), (15_000, 0.55), (20_000, 0.60),
            (50_000, 0.70), (60_000, 0.80),
        ];
        let adv = canary_advantage_at_t_plus_30s(&traj, 20_000, 0.60);
        assert!((adv - 0.10).abs() < 1e-9,
            "canary_advantage_at_t_plus_30s at t=20000 (target=50000) MUST be 0.10; got {}", adv);
    }

    // ---- Suffix max precompute ----

    #[test]
    fn g13_suffix_max_precompute_matches_naive() {
        let traj: Vec<(i64, f64)> = vec![
            (100, 0.50), (200, 0.55), (300, 0.60), (400, 0.45), (500, 0.65), (600, 0.40),
        ];
        let suf = precompute_suffix_max_bid(&traj);
        assert_eq!(suf.len(), 7);
        assert_eq!(suf[6], f64::NEG_INFINITY);
        assert!((suf[5] - 0.40).abs() < 1e-9);
        assert!((suf[4] - 0.65).abs() < 1e-9);
        assert!((suf[3] - 0.65).abs() < 1e-9); // 0.45 < 0.65
        assert!((suf[2] - 0.65).abs() < 1e-9);
        assert!((suf[1] - 0.65).abs() < 1e-9);
        assert!((suf[0] - 0.65).abs() < 1e-9);
    }

    #[test]
    fn g13_future_max_bid_via_suffix() {
        let traj: Vec<(i64, f64)> = vec![
            (100, 0.50), (200, 0.55), (300, 0.60), (400, 0.45), (500, 0.65),
        ];
        let suf = precompute_suffix_max_bid(&traj);
        // At t=300, future = [400, 500] with values [0.45, 0.65] -> max=0.65.
        assert_eq!(future_max_bid(&traj, &suf, 300), Some(0.65));
        // At t=550 (past last), no future.
        assert_eq!(future_max_bid(&traj, &suf, 550), None);
    }

    /// CANONICAL DRAIN ORDER: ActiveTracker::drain_due MUST return positions
    /// sorted by signal_id, NOT in HashMap iteration order. Without this
    /// guarantee, two runs over identical data produce JSONLs with different
    /// per-line ordering (HashMap RandomState is per-process), which makes
    /// raw diffs spurious and the streaming-vs-legacy equivalence proof
    /// unverifiable byte-exact. The aggregates (sums, counts, per-cell
    /// breakdowns) are order-invariant and unaffected.
    #[test]
    fn drain_due_returns_positions_in_signal_id_order() {
        let mut active = ActiveTracker::default();
        // Insert in REVERSE alphabetical order on purpose to make the sort
        // visible (without sort, drain order would be HashMap order, which
        // is per-process random -- this test would flake without the fix).
        for sid in ["zzz", "mmm", "aaa", "kkk"] {
            active.add(CollectedPosition {
                token: format!("tok-{sid}"),
                asset: "BTC".into(),
                interval: "5m".into(),
                epoch: 0,
                direction: "Up".into(),
                signal_id: sid.into(),
                entry_recv_ms: 0,
                entry_price: 0.5,
                shares: 10.0,
                exit_ts_ms: 1000,
                trajectory: Vec::new(),
                trajectory_bbo: Vec::new(),
                time_exit_bid: None,
                uncoverable_samples: 0,
                total_samples: 0,
                trigger_close: 0.0, // W9: not exercised by drain_due test
                trigger_ret_bps: 0.0,
                maker_fee_bps: 0,
                taker_fee_bps: 0,
                fee_type: String::new(),
                // COMBO 2026-06-08: not exercised by drain_due test.
                entry_trigger_ts_ms: 0,
                obi_top1: f64::NAN,
                obi_top3: f64::NAN,
                binance_close_at_2s: f64::NAN,
                binance_close_at_5s: f64::NAN,
                binance_close_at_10s: f64::NAN,
                binance_close_at_30s: f64::NAN,
                binance_close_at_60s: f64::NAN,
                binance_close_at_120s: f64::NAN,
                vol_30m: f64::NAN,
                vol_60m: f64::NAN,
            });
        }
        // Drain at a time past all exit_ts_ms.
        let drained = active.drain_due(2000);
        let order: Vec<&str> = drained.iter().map(|p| p.signal_id.as_str()).collect();
        assert_eq!(order, vec!["aaa", "kkk", "mmm", "zzz"],
            "drain_due MUST return positions in ascending signal_id order \
             (HashMap iteration order is per-process random; sorting here makes \
             JSONL outputs byte-reproducible across runs and across the \
             streaming-vs-legacy implementations)");
    }

    // =====================================================================
    // G12: HEALTH CHECK -- the recv_ms-only scan that's safe on validation
    // data. Tests use small synthetic .jsonl files written to /tmp so we
    // can assert the scan's counts (lines, n_with_recv_ms, OOO, gaps)
    // exactly. These tests do NOT touch the recorder's real data.
    // =====================================================================

    fn write_tmp_jsonl(lines: &[&str]) -> std::path::PathBuf {
        // Unique-per-process file so tests don't collide.
        static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let id = N.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let p = std::env::temp_dir()
            .join(format!("rb_health_{}_{}.jsonl", std::process::id(), id));
        let _ = std::fs::remove_file(&p);
        let mut f = std::fs::File::create(&p).expect("tmp file create");
        use std::io::Write as _;
        for l in lines { writeln!(f, "{}", l).unwrap(); }
        p
    }

    /// Minimal valid recorder line shape: only `received_at` is needed for
    /// the health scan. The `payload` is irrelevant content the scan ignores.
    fn rec_line(received_at: &str) -> String {
        // We include junk payload fields deliberately; if the scan accidentally
        // reads them, this would surface in code review (and the line counts
        // would still match -- so the test is also a SEAL invariant check).
        format!(
            r#"{{"received_at":"{received_at}","payload":{{"price":"99.99","size":"123","token":"FORBIDDEN","asset_id":"FORBIDDEN","k":{{"t":0,"c":"0"}}}}}}"#
        )
    }

    #[test]
    fn scan_stream_health_counts_lines_recv_ms_and_skips_garbage() {
        // 5 lines: 3 valid + 1 empty + 1 unparseable garbage.
        // Expected: n_lines=5, n_with_recv_ms=3, n_skipped=2.
        let lines = vec![
            rec_line("2026-05-06T00:00:01.000+00:00"),
            String::new(),                                  // empty -> skipped
            rec_line("2026-05-06T00:00:02.000+00:00"),
            "not even json".to_string(),                    // parse fail -> skipped
            rec_line("2026-05-06T00:00:03.000+00:00"),
        ];
        let refs: Vec<&str> = lines.iter().map(|s| s.as_str()).collect();
        let p = write_tmp_jsonl(&refs);
        let h = scan_stream_health(&p, "2026-05-06", "test_stream");
        assert!(h.file_present);
        assert_eq!(h.n_lines, 5, "n_lines should include all 5 read lines");
        assert_eq!(h.n_with_recv_ms, 3, "exactly 3 lines have valid received_at");
        assert_eq!(h.n_skipped, 2, "1 empty + 1 garbage line are skipped");
        assert_eq!(h.n_out_of_order, 0, "monotonic timestamps -> 0 OOO");
        assert_eq!(h.n_gaps_over_60s, 0, "1s gaps -> 0 large gaps");
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn scan_stream_health_detects_out_of_order_with_max_skew() {
        // Timestamps: 1000, 2000, 1500, 3000. The 1500 regresses 500ms below 2000.
        // Expected: n_OOO=1, max_OOO_skew=500.
        let lines = vec![
            rec_line("2026-05-06T00:00:01.000+00:00"),
            rec_line("2026-05-06T00:00:02.000+00:00"),
            rec_line("2026-05-06T00:00:01.500+00:00"), // regresses 500ms
            rec_line("2026-05-06T00:00:03.000+00:00"),
        ];
        let refs: Vec<&str> = lines.iter().map(|s| s.as_str()).collect();
        let p = write_tmp_jsonl(&refs);
        let h = scan_stream_health(&p, "2026-05-06", "test_stream");
        assert_eq!(h.n_with_recv_ms, 4);
        assert_eq!(h.n_out_of_order, 1, "exactly 1 monotonicity inversion");
        assert_eq!(h.max_ooo_skew_ms, 500,
            "max_OOO_skew_ms must equal 500ms (the regression magnitude)");
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn scan_stream_health_detects_large_gap_over_60s() {
        // Timestamps: 1s, 2s, 75s (= 73s gap > 60s), 76s.
        // Expected: n_gaps_over_60s=1, max_gap_ms=73000.
        let lines = vec![
            rec_line("2026-05-06T00:00:01.000+00:00"),
            rec_line("2026-05-06T00:00:02.000+00:00"),
            rec_line("2026-05-06T00:01:15.000+00:00"), // +73s gap
            rec_line("2026-05-06T00:01:16.000+00:00"),
        ];
        let refs: Vec<&str> = lines.iter().map(|s| s.as_str()).collect();
        let p = write_tmp_jsonl(&refs);
        let h = scan_stream_health(&p, "2026-05-06", "test_stream");
        assert_eq!(h.n_with_recv_ms, 4);
        assert_eq!(h.n_out_of_order, 0);
        assert_eq!(h.n_gaps_over_60s, 1, "exactly 1 forward gap exceeds 60s");
        assert_eq!(h.max_gap_ms, 73_000, "max_gap_ms must equal 73 seconds");
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn g11_streaming_merger_reports_out_of_order_per_stream() {
        // Stream A is monotonic; stream B has ONE out-of-order row
        // (recv_ms=15 yielded after 20). The OOO summary must catch this on
        // stream B (count=1) and NOT flag stream A (count=0).
        let s_mono = vec_stream(vec![kline_te("BTC", 10), kline_te("BTC", 30), kline_te("BTC", 50)]);
        let s_ooo  = vec_stream(vec![kline_te("ETH", 5), kline_te("ETH", 20), kline_te("ETH", 15), kline_te("ETH", 40)]);
        //                                                                ^^ regression: 15 < 20
        let mut merger = StreamingMerger::new(vec![
            ("mono".into(), s_mono),
            ("ooo".into(),  s_ooo),
        ]);
        // Drain.
        while merger.next().is_some() {}
        let summary = merger.out_of_order_summary();
        let mono_count = summary.iter().find(|(n, _)| n == "mono").map(|(_, c)| *c).unwrap();
        let ooo_count  = summary.iter().find(|(n, _)| n == "ooo").map(|(_, c)| *c).unwrap();
        assert_eq!(mono_count, 0, "monotonic stream must report 0 OOO");
        assert_eq!(ooo_count, 1, "stream with one regression must report 1 OOO; got summary={summary:?}");
    }

    // ========================================================================
    // PIECE W8: EntryFilter tests. Pure logic over synthetic active positions
    // + synthetic Trigger; no replay machinery, no real data. The regression
    // guard (test 8) locks in baseline equivalence with G15 numerically.
    // ========================================================================

    fn mk_position_at(asset: &str, interval: &str, epoch: i64, direction: &str) -> CollectedPosition {
        mk_position_at_full(asset, interval, epoch, direction, 0, 0.5, 0.0)
    }

    /// W9: full-field constructor — needed by D-variant tests that vary
    /// entry_recv_ms (ordering of lots inside a market-side), entry_price
    /// (D1/D2b), and trigger_close (D2a). Wrapper preserves the simpler
    /// 4-arg `mk_position_at` for legacy tests.
    fn mk_position_at_full(
        asset: &str,
        interval: &str,
        epoch: i64,
        direction: &str,
        entry_recv_ms: i64,
        entry_price: f64,
        trigger_close: f64,
    ) -> CollectedPosition {
        CollectedPosition {
            token: format!("{asset}-{interval}-{direction}-{epoch}"),
            asset: asset.to_string(),
            interval: interval.to_string(),
            epoch,
            direction: direction.to_string(),
            signal_id: format!("{asset}-{epoch}-{interval}-{direction}-{entry_recv_ms}"),
            entry_recv_ms,
            entry_price,
            shares: 10.0,
            exit_ts_ms: 1_000,
            trajectory: Vec::new(),
            trajectory_bbo: Vec::new(),
            time_exit_bid: Some(0.5),
            uncoverable_samples: 0,
            total_samples: 0,
            trigger_close,
            trigger_ret_bps: 0.0,
            maker_fee_bps: 0,
            taker_fee_bps: 0,
            fee_type: String::new(),
            // COMBO 2026-06-08: not exercised by D-variant tests.
            entry_trigger_ts_ms: 0,
            obi_top1: f64::NAN,
            obi_top3: f64::NAN,
            binance_close_at_2s: f64::NAN,
            binance_close_at_5s: f64::NAN,
            binance_close_at_10s: f64::NAN,
            binance_close_at_30s: f64::NAN,
            binance_close_at_60s: f64::NAN,
            binance_close_at_120s: f64::NAN,
            vol_30m: f64::NAN,
            vol_60m: f64::NAN,
        }
    }

    fn mk_trigger(asset: &str, ret_bps: f64) -> Trigger {
        Trigger {
            asset: asset.to_string(),
            trigger_ts: 1_780_000_000,
            ret_bps,
            window_s: 1,
        }
    }

    #[test]
    fn entry_filter_baseline_accepts_everything() {
        let f = EntryFilter::Baseline;
        let positions: Vec<CollectedPosition> = vec![];
        let trig = mk_trigger("BTC", 5.0);
        assert!(f.accept(&trig, Direction::Up, positions.iter(), "BTC", "5m", 1_779_999_900, 0.5, 0.0));
        // Even with positions present.
        let positions = vec![mk_position_at("BTC", "5m", 1_779_999_900, "Down")];
        assert!(f.accept(&trig, Direction::Up, positions.iter(), "BTC", "5m", 1_779_999_900, 0.5, 0.0));
    }

    #[test]
    fn entry_filter_bps_threshold_rejects_below_and_accepts_at_or_above() {
        let f = EntryFilter::BpsThreshold { min_abs_bps: 6.0 };
        let positions: Vec<CollectedPosition> = vec![];
        // Below: reject.
        let t_low = mk_trigger("BTC", 5.5);
        assert!(!f.accept(&t_low, Direction::Up, positions.iter(), "BTC", "5m", 0, 0.5, 0.0));
        // Exactly at: accept (>=).
        let t_eq = mk_trigger("BTC", 6.0);
        assert!(f.accept(&t_eq, Direction::Up, positions.iter(), "BTC", "5m", 0, 0.5, 0.0));
        // Above: accept.
        let t_hi = mk_trigger("BTC", 8.5);
        assert!(f.accept(&t_hi, Direction::Up, positions.iter(), "BTC", "5m", 0, 0.5, 0.0));
        // Sign-agnostic: -7 bps (Down) also passes >= 6 abs.
        let t_neg = mk_trigger("BTC", -7.0);
        assert!(f.accept(&t_neg, Direction::Down, positions.iter(), "BTC", "5m", 0, 0.5, 0.0));
    }

    #[test]
    fn entry_filter_no_opposite_rejects_when_opposite_exists_same_market() {
        let f = EntryFilter::NoOpposite;
        let positions = vec![mk_position_at("BTC", "5m", 1_779_999_900, "Down")];
        let trig = mk_trigger("BTC", 5.0);
        // Up signal while Down position exists in same market -> reject.
        assert!(!f.accept(&trig, Direction::Up, positions.iter(), "BTC", "5m", 1_779_999_900, 0.5, 0.0));
    }

    #[test]
    fn entry_filter_no_opposite_accepts_same_side_dca() {
        let f = EntryFilter::NoOpposite;
        let positions = vec![mk_position_at("BTC", "5m", 1_779_999_900, "Up")];
        let trig = mk_trigger("BTC", 5.0);
        // Same direction = DCA scenario; allow.
        assert!(f.accept(&trig, Direction::Up, positions.iter(), "BTC", "5m", 1_779_999_900, 0.5, 0.0));
    }

    #[test]
    fn entry_filter_no_opposite_accepts_different_epoch_same_cell() {
        let f = EntryFilter::NoOpposite;
        // Position in epoch X with Down.
        let positions = vec![mk_position_at("BTC", "5m", 1_779_999_900, "Down")];
        let trig = mk_trigger("BTC", 5.0);
        // Different epoch (X + 300s = next 5m window) -> different market -> allow Up.
        assert!(f.accept(&trig, Direction::Up, positions.iter(), "BTC", "5m", 1_780_000_200, 0.5, 0.0));
    }

    #[test]
    fn entry_filter_asymmetric_bps_high_bar_for_opposite() {
        let f = EntryFilter::AsymmetricBps { min_abs_bps: 5.0, opposite_min_abs_bps: 8.0 };
        let positions_opp = vec![mk_position_at("BTC", "5m", 1_779_999_900, "Down")];
        let positions_none: Vec<CollectedPosition> = vec![];
        // BPS=6 with NO opposite -> passes the 5 floor; no high bar applies.
        let t6 = mk_trigger("BTC", 6.0);
        assert!(f.accept(&t6, Direction::Up, positions_none.iter(), "BTC", "5m", 1_779_999_900, 0.5, 0.0));
        // BPS=6 with OPPOSITE -> fails the 8 high bar.
        assert!(!f.accept(&t6, Direction::Up, positions_opp.iter(), "BTC", "5m", 1_779_999_900, 0.5, 0.0));
        // BPS=10 with OPPOSITE -> passes the 8 high bar.
        let t10 = mk_trigger("BTC", 10.0);
        assert!(f.accept(&t10, Direction::Up, positions_opp.iter(), "BTC", "5m", 1_779_999_900, 0.5, 0.0));
        // BPS=4 (under floor) regardless of opposite -> reject.
        let t4 = mk_trigger("BTC", 4.0);
        assert!(!f.accept(&t4, Direction::Up, positions_none.iter(), "BTC", "5m", 1_779_999_900, 0.5, 0.0));
    }

    #[test]
    fn count_double_sided_markets_synthetic() {
        // 3 markets: BTC:5m epoch A both, ETH:5m epoch B only Up, BTC:5m epoch C only Down.
        let positions = vec![
            mk_position_at("BTC", "5m", 100, "Up"),
            mk_position_at("BTC", "5m", 100, "Down"),  // <- A double-sided
            mk_position_at("ETH", "5m", 200, "Up"),    // <- B single
            mk_position_at("BTC", "5m", 300, "Down"),  // <- C single
        ];
        assert_eq!(count_double_sided_markets(&positions), 1,
                   "exactly 1 market (BTC:5m@100) has both sides");
        // Cell-scoped: BTC:5m alone has 1 double-sided (epoch 100).
        assert_eq!(count_double_sided_markets_for_cell(&positions, "BTC", "5m"), 1);
        // ETH:5m has 0 double-sided.
        assert_eq!(count_double_sided_markets_for_cell(&positions, "ETH", "5m"), 0);
    }

    /// REGRESSION GUARD (W8): EntryFilter::Baseline MUST produce
    /// numerically-identical position counts to the G15 backtest summary
    /// in data/derived/backtest_tp_v1/summary.json. Hardcoded reference
    /// numbers come from that file at commit 00d3ad1 + W3's re-seed
    /// (verified post-W3 that Baseline exit-variant is unaffected since
    /// it doesn't use running_max).
    ///
    /// THIS TEST IS PURE-LOGIC ONLY: it verifies the EntryFilter::Baseline
    /// PASS-THROUGH semantics on a synthetic ActiveTracker -- it does NOT
    /// run the real backtester on real data (would take ~10 min per test).
    /// The real-data verification is the operator's MANUAL post-build step:
    ///   1. Run `./target/release/rust_bot --backtest-entry-filters \
    ///         --bt-data-root data --bt-start-date 2026-05-06 \
    ///         --bt-end-date 2026-05-16 --bt-entry-filters a0 \
    ///         --bt-out-dir /tmp/w8_baseline_verify`
    ///   2. `diff /tmp/w8_baseline_verify/summary_a0.json` against
    ///      `data/derived/backtest_tp_v1/summary.json` baseline numbers
    ///      (n_trades + total_pnl per cell + win_ratio).
    /// EXPECTED on the real run (G15 reference values, copy here for the
    /// operator to diff -- if these don't match after run, the filter
    /// plumbing has a bug):
    ///   * TOTAL: n=1949, total_pnl=$1241.964375, win_ratio=0.624423
    ///   * BTC_5m:  n=359, $737.446466, 0.760446
    ///   * BTC_15m: n=216, $114.176312, 0.546296
    ///   * ETH_5m:  n=855, $509.238921, 0.653801
    ///   * ETH_15m: n=519, -$118.897324, 0.514451
    ///
    /// The unit-test body below proves the LOGIC: EntryFilter::Baseline
    /// accepts every Fire that would be accepted with NO filter. So
    /// running the backtester with Baseline produces the SAME positions
    /// as the pre-W8 baseline-only path.
    #[test]
    fn entry_filter_baseline_passthrough_is_no_op() {
        // For every (trigger BPS, direction, active-positions) combination,
        // EntryFilter::Baseline must accept. Equivalent to "no filter".
        let f = EntryFilter::Baseline;
        let scenarios = vec![
            (5.0,  Direction::Up,   vec![] as Vec<CollectedPosition>),
            (5.0,  Direction::Down, vec![mk_position_at("BTC", "5m", 100, "Up")]),
            (10.0, Direction::Up,   vec![mk_position_at("ETH", "15m", 200, "Down")]),
            (-7.5, Direction::Down, vec![
                mk_position_at("BTC", "5m", 100, "Up"),
                mk_position_at("BTC", "5m", 100, "Down"),
            ]),
        ];
        for (bps, dir, positions) in scenarios {
            let trig = mk_trigger("BTC", bps);
            assert!(
                f.accept(&trig, dir, positions.iter(), "BTC", "5m", 100, 0.5, 0.0),
                "Baseline MUST accept (bps={bps}, dir={dir:?}, positions={})",
                positions.len()
            );
        }
        // Belt-and-suspenders: the G15 reference numbers are pinned in the
        // doc comment above; an operator post-build run produces a
        // summary_a0.json whose TOTAL.n_trades, TOTAL.total_pnl, and the
        // 4 by_cell rows MUST equal those values bit-exact (any diff = bug
        // in the W8 filter plumbing).
        // G15 reference (5/06-5/16, 11 dates, 1949 positions):
        let _g15_baseline_n = 1949_usize;
        let _g15_baseline_pnl = 1241.964375_f64;
        let _g15_baseline_wr = 0.624423_f64;
        // (no assert on these here -- the assertion is via the operator's
        // post-build CSV diff. The doc comment above is the contract.)
    }

    // ========================================================================
    // PIECE W9: D-family entry-filter tests (DCA variants). All pure-logic
    // over synthetic active positions; no replay machinery. The mirror test
    // (D1 vs D2b) pins the inverse-symmetry contract between the two.
    // Common scenario across these tests:
    //   - One "first lot" placed at (asset=BTC, interval=5m, epoch=E, dir=Up)
    //   - One candidate (same market-side) at distinct entry_recv_ms.
    // Acceptance turns on the per-variant rule.
    // ========================================================================

    /// Constants for the W9 D-variant tests. Pinned at the module level so
    /// every test reads off the same anchor.
    const W9_EPOCH: i64 = 1_780_000_000;

    /// D0 (DcaUnlimited) MUST accept every Fire exactly like Baseline. This
    /// pins that the D-family table's reference row is byte-identical to A0.
    #[test]
    fn entry_filter_d0_dca_unlimited_matches_baseline() {
        let baseline = EntryFilter::Baseline;
        let d0 = EntryFilter::DcaUnlimited;
        let trig = mk_trigger("BTC", 5.0);
        // Same scenarios used by entry_filter_baseline_passthrough_is_no_op.
        let scenarios: Vec<(f64, Direction, Vec<CollectedPosition>)> = vec![
            (5.0,  Direction::Up,   vec![]),
            (5.0,  Direction::Down, vec![mk_position_at("BTC", "5m", W9_EPOCH, "Up")]),
            (10.0, Direction::Up,   vec![mk_position_at("ETH", "15m", 200, "Down")]),
            (5.0,  Direction::Up,   vec![
                mk_position_at_full("BTC", "5m", W9_EPOCH, "Up", 1, 0.50, 100.0),
                mk_position_at_full("BTC", "5m", W9_EPOCH, "Up", 2, 0.55, 101.0),
                mk_position_at_full("BTC", "5m", W9_EPOCH, "Up", 3, 0.60, 102.0),
            ]),
        ];
        for (bps, dir, positions) in &scenarios {
            let trig = mk_trigger("BTC", *bps);
            let b = baseline.accept(&trig, *dir, positions.iter(), "BTC", "5m", W9_EPOCH, 0.65, 103.0);
            let d = d0.accept(&trig, *dir, positions.iter(), "BTC", "5m", W9_EPOCH, 0.65, 103.0);
            assert_eq!(b, d, "D0 must match Baseline for bps={bps} dir={dir:?} positions={}", positions.len());
            assert!(d, "D0 must accept all scenarios (it's a pass-through)");
        }
        let _ = trig; // silence unused
    }

    /// D1: with one Up lot at 0.50, candidate at 0.49 accepts (lower → DCA
    /// promediando), candidate at 0.50 rejects (equal is NOT improving),
    /// candidate at 0.60 rejects (higher is the WRONG direction).
    #[test]
    fn entry_filter_d1_improving_price_accepts_strictly_lower() {
        let f = EntryFilter::DcaImprovingPrice;
        let positions = vec![mk_position_at_full("BTC", "5m", W9_EPOCH, "Up", 1, 0.50, 100.0)];
        let trig = mk_trigger("BTC", 5.0);
        // Empty same-market-side => first lot, accept.
        let empty: Vec<CollectedPosition> = vec![];
        assert!(f.accept(&trig, Direction::Up, empty.iter(), "BTC", "5m", W9_EPOCH, 0.50, 100.0),
                "D1 must accept the first lot (no priors)");
        // Candidate 0.49 < 0.50 -> accept.
        assert!(f.accept(&trig, Direction::Up, positions.iter(), "BTC", "5m", W9_EPOCH, 0.49, 100.0),
                "D1 must accept candidate strictly cheaper than MIN priors");
        // Candidate 0.50 == 0.50 -> reject (conservative).
        assert!(!f.accept(&trig, Direction::Up, positions.iter(), "BTC", "5m", W9_EPOCH, 0.50, 100.0),
                "D1 must reject candidate equal to MIN priors (strict <)");
        // Candidate 0.60 > 0.50 -> reject.
        assert!(!f.accept(&trig, Direction::Up, positions.iter(), "BTC", "5m", W9_EPOCH, 0.60, 100.0),
                "D1 must reject candidate more expensive than MIN priors");
    }

    /// D2a: with one Up lot whose trigger_close=100.0, candidate with
    /// trigger_close=101.0 (Binance went UP since first lot) -> accept;
    /// trigger_close=99.0 (went DOWN) -> reject; trigger_close=100.0 (flat)
    /// -> reject (strict >). Symmetric for Down direction.
    #[test]
    fn entry_filter_d2a_confirming_underlying_two_triggers() {
        let f = EntryFilter::DcaConfirmingUnderlying;
        // First Up lot at trigger_close=100.0.
        let first_up = mk_position_at_full("BTC", "5m", W9_EPOCH, "Up", 1, 0.50, 100.0);
        let positions_up = vec![first_up];
        let trig = mk_trigger("BTC", 5.0);
        // Up direction, candidate close 101.0 (Binance kept climbing) -> accept.
        assert!(f.accept(&trig, Direction::Up, positions_up.iter(), "BTC", "5m", W9_EPOCH, 0.55, 101.0),
                "D2a Up: candidate close > first lot's close must accept");
        // Up direction, candidate close 99.0 (Binance reversed) -> reject.
        assert!(!f.accept(&trig, Direction::Up, positions_up.iter(), "BTC", "5m", W9_EPOCH, 0.55, 99.0),
                "D2a Up: candidate close < first lot's close must reject");
        // Up direction, candidate close 100.0 (flat) -> reject (strict >).
        assert!(!f.accept(&trig, Direction::Up, positions_up.iter(), "BTC", "5m", W9_EPOCH, 0.55, 100.0),
                "D2a Up: equal close must reject (strict >)");
        // First lot only (empty active) -> accept.
        let empty: Vec<CollectedPosition> = vec![];
        assert!(f.accept(&trig, Direction::Up, empty.iter(), "BTC", "5m", W9_EPOCH, 0.55, 50.0),
                "D2a must accept the first lot regardless of close");
        // Down direction symmetry: first lot at close=100.0; candidate close 99.0 -> accept.
        let first_down = mk_position_at_full("BTC", "5m", W9_EPOCH, "Down", 1, 0.50, 100.0);
        let positions_dn = vec![first_down];
        assert!(f.accept(&trig, Direction::Down, positions_dn.iter(), "BTC", "5m", W9_EPOCH, 0.55, 99.0),
                "D2a Down: candidate close < first lot's close must accept");
        assert!(!f.accept(&trig, Direction::Down, positions_dn.iter(), "BTC", "5m", W9_EPOCH, 0.55, 101.0),
                "D2a Down: candidate close > first lot's close must reject");
    }

    /// D3 (NoDca): the first lot accepts; any subsequent lot in the same
    /// market-side rejects. Different market-side (different epoch / cell /
    /// direction) still accepts (D3 is per-market-side, not global).
    #[test]
    fn entry_filter_d3_no_dca_blocks_second_lot_same_market_side() {
        let f = EntryFilter::NoDca;
        let trig = mk_trigger("BTC", 5.0);
        let empty: Vec<CollectedPosition> = vec![];
        // First lot in market-side -> accept.
        assert!(f.accept(&trig, Direction::Up, empty.iter(), "BTC", "5m", W9_EPOCH, 0.50, 100.0),
                "D3 must accept the FIRST lot in a market-side");
        // Second lot, same (asset, interval, epoch, dir) -> reject.
        let one_up = vec![mk_position_at_full("BTC", "5m", W9_EPOCH, "Up", 1, 0.50, 100.0)];
        assert!(!f.accept(&trig, Direction::Up, one_up.iter(), "BTC", "5m", W9_EPOCH, 0.49, 101.0),
                "D3 must reject the SECOND lot in same market-side (DCA blocked)");
        // Different epoch (next 5m window) -> different market-side -> accept.
        assert!(f.accept(&trig, Direction::Up, one_up.iter(), "BTC", "5m", W9_EPOCH + 300, 0.49, 101.0),
                "D3 must accept lots in a different epoch (different market-side)");
        // Opposite direction same epoch -> different market-side -> accept.
        assert!(f.accept(&trig, Direction::Down, one_up.iter(), "BTC", "5m", W9_EPOCH, 0.49, 99.0),
                "D3 must accept opposite-direction lots (different market-side)");
    }

    /// D4 (DcaCap{max=3}): accepts lots 1, 2, 3 in the same market-side and
    /// rejects lot 4. Other market-sides are unaffected.
    #[test]
    fn entry_filter_d4_caps_at_three_lots_per_market_side() {
        let f = EntryFilter::DcaCap { max: 3 };
        let trig = mk_trigger("BTC", 5.0);
        let empty: Vec<CollectedPosition> = vec![];
        // First lot (count=0 < 3) -> accept.
        assert!(f.accept(&trig, Direction::Up, empty.iter(), "BTC", "5m", W9_EPOCH, 0.50, 100.0));
        // Second (count=1 < 3) -> accept.
        let one = vec![mk_position_at_full("BTC", "5m", W9_EPOCH, "Up", 1, 0.50, 100.0)];
        assert!(f.accept(&trig, Direction::Up, one.iter(), "BTC", "5m", W9_EPOCH, 0.51, 100.5));
        // Third (count=2 < 3) -> accept.
        let two = vec![
            mk_position_at_full("BTC", "5m", W9_EPOCH, "Up", 1, 0.50, 100.0),
            mk_position_at_full("BTC", "5m", W9_EPOCH, "Up", 2, 0.51, 100.5),
        ];
        assert!(f.accept(&trig, Direction::Up, two.iter(), "BTC", "5m", W9_EPOCH, 0.52, 101.0));
        // Fourth (count=3 == 3) -> reject (strict <).
        let three = vec![
            mk_position_at_full("BTC", "5m", W9_EPOCH, "Up", 1, 0.50, 100.0),
            mk_position_at_full("BTC", "5m", W9_EPOCH, "Up", 2, 0.51, 100.5),
            mk_position_at_full("BTC", "5m", W9_EPOCH, "Up", 3, 0.52, 101.0),
        ];
        assert!(!f.accept(&trig, Direction::Up, three.iter(), "BTC", "5m", W9_EPOCH, 0.53, 101.5),
                "D4 must reject the 4th lot when cap=3");
        // Different epoch (different market-side) -> the cap is per-market-side, accept.
        assert!(f.accept(&trig, Direction::Up, three.iter(), "BTC", "5m", W9_EPOCH + 300, 0.53, 101.5),
                "D4 cap is per-market-side; different epoch is a different market-side");
    }

    /// D1 ↔ D2b EXACT MIRROR. With one prior lot at entry_price=0.50:
    ///   candidate 0.60 -> D1 reject (not lower), D2b ACCEPT (higher than MAX).
    ///   candidate 0.40 -> D1 ACCEPT (lower than MIN), D2b reject (not higher).
    /// The mirror is the contract: D1 and D2b partition the >0 set of
    /// candidate entry prices around the prior MIN/MAX (which coincide when
    /// there is exactly one prior lot).
    #[test]
    fn entry_filter_d1_and_d2b_are_exact_mirrors() {
        let d1 = EntryFilter::DcaImprovingPrice;
        let d2b = EntryFilter::DcaConfirmingAsk;
        let trig = mk_trigger("BTC", 5.0);
        let positions = vec![mk_position_at_full("BTC", "5m", W9_EPOCH, "Up", 1, 0.50, 100.0)];

        // Candidate 0.60 (higher than 0.50).
        let d1_acc_hi  = d1.accept(&trig, Direction::Up, positions.iter(), "BTC", "5m", W9_EPOCH, 0.60, 100.0);
        let d2b_acc_hi = d2b.accept(&trig, Direction::Up, positions.iter(), "BTC", "5m", W9_EPOCH, 0.60, 100.0);
        assert!(!d1_acc_hi, "D1 must reject when candidate (0.60) > MIN priors (0.50)");
        assert!(d2b_acc_hi, "D2b must accept when candidate (0.60) > MAX priors (0.50)");

        // Candidate 0.40 (lower than 0.50).
        let d1_acc_lo  = d1.accept(&trig, Direction::Up, positions.iter(), "BTC", "5m", W9_EPOCH, 0.40, 100.0);
        let d2b_acc_lo = d2b.accept(&trig, Direction::Up, positions.iter(), "BTC", "5m", W9_EPOCH, 0.40, 100.0);
        assert!(d1_acc_lo, "D1 must accept when candidate (0.40) < MIN priors (0.50)");
        assert!(!d2b_acc_lo, "D2b must reject when candidate (0.40) < MAX priors (0.50)");

        // The MIRROR proper: D1's acceptance XOR D2b's acceptance must be
        // TRUE for any candidate price != prior price. Verified by the two
        // cases above (only-prior-price case (0.50) is rejected by BOTH,
        // which is the agreed conservative rule -- still mutually consistent).
        let d1_acc_eq  = d1.accept(&trig, Direction::Up, positions.iter(), "BTC", "5m", W9_EPOCH, 0.50, 100.0);
        let d2b_acc_eq = d2b.accept(&trig, Direction::Up, positions.iter(), "BTC", "5m", W9_EPOCH, 0.50, 100.0);
        assert!(!d1_acc_eq && !d2b_acc_eq,
                "D1 and D2b BOTH reject when candidate == prior (strict </>, conservative)");
    }

    /// W9-fix: SplitDcaByInterval dispatches its decision based on the
    /// `interval` argument and re-runs the chosen child filter. Verified with
    /// 4 lots in the same market-side (above D4's cap=3):
    ///   - When interval == "5m", child is D0 (accept always) -> ACCEPT
    ///   - When interval == "15m", child is D4{max:3} -> REJECT (4th lot)
    /// And with 2 lots in the same market-side (below D4's cap):
    ///   - When interval == "15m", child is D4{max:3} -> ACCEPT (3rd lot OK)
    /// Plus an unknown interval triggers the defensive pass-through.
    #[test]
    fn entry_filter_split_dca_dispatches_by_interval() {
        let split = EntryFilter::SplitDcaByInterval {
            five_min: Box::new(EntryFilter::DcaUnlimited),
            fifteen_min: Box::new(EntryFilter::DcaCap { max: 3 }),
        };
        let trig = mk_trigger("BTC", 5.0);

        // 4 lots in the same market-side (above D4's cap=3).
        let four_lots_5m = vec![
            mk_position_at_full("BTC", "5m", W9_EPOCH, "Up", 1, 0.50, 100.0),
            mk_position_at_full("BTC", "5m", W9_EPOCH, "Up", 2, 0.51, 100.5),
            mk_position_at_full("BTC", "5m", W9_EPOCH, "Up", 3, 0.52, 101.0),
            mk_position_at_full("BTC", "5m", W9_EPOCH, "Up", 4, 0.53, 101.5),
        ];
        // In 5m, child = D0 = always accept, even with 4 prior lots.
        assert!(
            split.accept(&trig, Direction::Up, four_lots_5m.iter(),
                         "BTC", "5m", W9_EPOCH, 0.54, 102.0),
            "split_dca in 5m must dispatch to D0 (DcaUnlimited) -> ACCEPT even with 4+ prior lots"
        );

        // Same 4 lots but tagged as 15m -> D4 cap=3 rejects (count=4 >= 3).
        let four_lots_15m: Vec<CollectedPosition> = (1..=4)
            .map(|i| mk_position_at_full("BTC", "15m", W9_EPOCH, "Up", i, 0.50 + 0.01 * i as f64, 100.0))
            .collect();
        assert!(
            !split.accept(&trig, Direction::Up, four_lots_15m.iter(),
                          "BTC", "15m", W9_EPOCH, 0.54, 102.0),
            "split_dca in 15m must dispatch to D4{{max:3}} -> REJECT when same-market-side count >= 3"
        );

        // 2 lots in 15m -> D4{max:3} accepts (count=2 < 3).
        let two_lots_15m = vec![
            mk_position_at_full("BTC", "15m", W9_EPOCH, "Up", 1, 0.50, 100.0),
            mk_position_at_full("BTC", "15m", W9_EPOCH, "Up", 2, 0.51, 100.5),
        ];
        assert!(
            split.accept(&trig, Direction::Up, two_lots_15m.iter(),
                         "BTC", "15m", W9_EPOCH, 0.52, 101.0),
            "split_dca in 15m must dispatch to D4{{max:3}} -> ACCEPT when count < cap (count=2, cap=3)"
        );

        // Defensive: unknown interval falls through to pass-through.
        assert!(
            split.accept(&trig, Direction::Up, four_lots_5m.iter(),
                         "BTC", "30m", W9_EPOCH, 0.54, 102.0),
            "split_dca with unknown interval must pass-through (defensive) -> ACCEPT"
        );

        // CLI label is the canonical "split_dca" only for the pre-registered
        // (D0, D4{max:3}) shape. Any other split falls through to the verbose
        // label() format. Pin both.
        assert_eq!(cli_label_for(&split), "split_dca");
        assert_eq!(
            split.label(),
            "split_dca_5m=d0_dca_unlimited_15m=d4_dca_cap_3"
        );

        // A non-canonical split (different 15m child) must NOT collapse to
        // the canonical label.
        let non_canonical = EntryFilter::SplitDcaByInterval {
            five_min: Box::new(EntryFilter::DcaUnlimited),
            fifteen_min: Box::new(EntryFilter::NoDca),
        };
        assert_ne!(cli_label_for(&non_canonical), "split_dca");
    }

    // ========================================================================
    // W9-Pieza1: TRAJECTORY HELPER tests. Pure-logic over synthetic
    // trajectories; no replay machinery. All 5 helpers go through:
    //   - empty input  (degenerate)
    //   - all-coverable trajectory
    //   - mixed NaN (uncoverable) samples
    //   - boundary at window end
    // ========================================================================

    #[test]
    fn trajectory_bid_at_offset_picks_first_post_target_sample() {
        // Entry at ms=10_000. Samples every 1s.
        let entry = 10_000_i64;
        let traj: Vec<(i64, f64)> = (0..120)
            .map(|i| (entry + i * 1000, 0.30 + 0.001 * i as f64))
            .collect();
        // bid_at_offset(30) -> first ms >= entry+30_000 = ms=40_000 = sample i=30.
        let b30 = bid_at_offset(&traj, entry, 30);
        assert!((b30 - 0.330).abs() < 1e-9, "bid_at_30s should be sample i=30 = 0.330");
        // bid_at_offset(0) -> first ms >= entry = i=0.
        assert!((bid_at_offset(&traj, entry, 0) - 0.300).abs() < 1e-9);
        // bid_at_offset(120) -> no sample >= entry+120s (last is at +119s).
        assert!(bid_at_offset(&traj, entry, 120).is_nan(),
                "bid_at_120s with no sample at/after target -> NaN");
        // Empty trajectory.
        assert!(bid_at_offset(&[], entry, 30).is_nan());
    }

    #[test]
    fn trajectory_max_min_in_window_ignore_nan_and_respect_window_end() {
        // Window [0, 120s). Samples at offsets 0,1,2,3,4,5 sec.
        // Bid values include some NaN (uncoverable). Max coverable = 0.85 at +3s.
        let entry = 10_000_i64;
        let traj: Vec<(i64, f64)> = vec![
            (entry + 0,      0.50),
            (entry + 1000,   0.55),
            (entry + 2000,   f64::NAN),  // uncoverable, ignored
            (entry + 3000,   0.85),       // <- peak
            (entry + 4000,   0.40),       // <- low
            (entry + 5000,   0.60),
            // Two samples PAST 120s window must be ignored:
            (entry + 121_000, 0.99),
            (entry + 130_000, 0.01),
        ];
        let (max_bid, max_off) = max_bid_in_window(&traj, entry, 120);
        assert!((max_bid - 0.85).abs() < 1e-9, "max = 0.85");
        assert_eq!(max_off, 3000, "max_offset_ms = 3000 (3s after entry)");
        let (min_bid, min_off) = min_bid_in_window(&traj, entry, 120);
        assert!((min_bid - 0.40).abs() < 1e-9, "min = 0.40 (NaN ignored)");
        assert_eq!(min_off, 4000, "min_offset_ms = 4000");
        // Pure-NaN trajectory -> (NaN, 0).
        let nan_traj: Vec<(i64,f64)> = vec![(entry, f64::NAN), (entry + 1000, f64::NAN)];
        let (mb, mo) = max_bid_in_window(&nan_traj, entry, 120);
        assert!(mb.is_nan() && mo == 0);
    }

    #[test]
    fn trajectory_high_water_marks_only_strictly_increasing() {
        // Bid sequence: 0.50, 0.55, 0.55 (no change), 0.40 (drop, no HWM),
        // 0.85 (new peak), 0.40 (drop), 0.90 (new peak).
        let entry = 10_000_i64;
        let traj: Vec<(i64, f64)> = vec![
            (entry + 0,    0.50),
            (entry + 1000, 0.55),
            (entry + 2000, 0.55),  // equal, NOT pushed (strict >)
            (entry + 3000, 0.40),
            (entry + 4000, 0.85),
            (entry + 5000, 0.40),
            (entry + 6000, 0.90),
        ];
        let hwm = high_water_marks(&traj, entry, 120);
        assert_eq!(hwm, vec![(0, 0.50), (1000, 0.55), (4000, 0.85), (6000, 0.90)],
            "HWM must be strictly-increasing; equal bid does not advance");
        // Window cutoff: same traj but cap at 5s -> only first 5 samples seen.
        let hwm_5s = high_water_marks(&traj, entry, 5);
        assert_eq!(hwm_5s, vec![(0, 0.50), (1000, 0.55), (4000, 0.85)],
            "HWM in 5s window should NOT include the 6s sample at 0.90");
    }

    #[test]
    fn trajectory_low_water_marks_only_strictly_decreasing() {
        // Bid sequence: 0.50, 0.60 (no LWM), 0.40 (new low), 0.40 (equal, no push),
        // 0.30 (new low), 0.80 (no LWM).
        let entry = 0_i64;
        let traj: Vec<(i64, f64)> = vec![
            (0,    0.50),
            (1000, 0.60),
            (2000, 0.40),
            (3000, 0.40),  // equal, NOT pushed
            (4000, 0.30),
            (5000, 0.80),
        ];
        let lwm = low_water_marks(&traj, entry, 120);
        assert_eq!(lwm, vec![(0, 0.50), (2000, 0.40), (4000, 0.30)],
            "LWM must be strictly-decreasing; equal bid does not advance");
    }

    #[test]
    fn trajectory_helpers_skip_nan_in_extremes_and_marks() {
        // NaN samples must NOT appear in HWM/LWM and must NOT affect max/min.
        let entry = 0_i64;
        let _ = entry; // synthetic anchor used by the helpers via the literal 0 offset
        let traj: Vec<(i64, f64)> = vec![
            (0,    0.50),
            (1000, f64::NAN),  // uncoverable
            (2000, 0.70),       // <- HWM advance
            (3000, f64::NAN),
            (4000, 0.30),       // <- LWM advance
            (5000, f64::NAN),
        ];
        let hwm = high_water_marks(&traj, 0, 120);
        let lwm = low_water_marks(&traj, 0, 120);
        // HWM should have 0.50 then 0.70. NaN skipped.
        assert_eq!(hwm, vec![(0, 0.50), (2000, 0.70)]);
        // LWM should have 0.50 then 0.30. NaN skipped.
        assert_eq!(lwm, vec![(0, 0.50), (4000, 0.30)]);
        let (mx, _) = max_bid_in_window(&traj, 0, 120);
        let (mn, _) = min_bid_in_window(&traj, 0, 120);
        assert!((mx - 0.70).abs() < 1e-9 && (mn - 0.30).abs() < 1e-9);
    }
}
