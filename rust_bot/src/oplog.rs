//! Phase 6 — operational log (the firehose). Append-only JSONL, ONE event per line,
//! every line `{ts_ms, kind, data}` with a single wall clock (`state::now_ms`), so the
//! exact temporal sequence of ANY event is reconstructable — the thing that matters
//! most for a lag-arb post-mortem.
//!
//! This is BROADER than `execution_log.jsonl` (the per-trade autopsy): the oplog
//! captures EVERYTHING — every API call + its COMPLETE response (status, latency, the
//! EXACT textual error from the API, never summarized), every system event (WS
//! reconnect, feed-stale, kline, trigger, decision, guard eval with the numbers,
//! order intent/POST/response, state transition, reconcile, fill, hold, close), and
//! every error with full context. When a live trade goes wrong, this is how you
//! diagnose EXACTLY what happened instead of "it lost".
//!
//! Why a separate file: execution_log = curated per-attempt autopsy (the trade view);
//! oplog = the raw operational stream (the system view). Both append-only, both ms
//! timestamps from the same clock, so they cross-reference by `ts_ms`.
//!
//! ============================================================================
//! B3 MAKER EVENT CONTRACT (COMBO Phase 3 — LOCKED 2026-06-09)
//! ============================================================================
//! These events are NOT emitted here. They are emitted INLINE (the dominant
//! oplog pattern — `oplog.sys(kind, json!{...})` at the call site) inside the
//! B3 maker exit logic, which lands in Pieza 1 (the router + maker path). This
//! block is the WRITTEN CONTRACT: Pieza 1 MUST emit exactly these kinds with
//! exactly these fields, with a per-event test asserting kind + field set.
//! Designed/approved before Pieza 1 so the observability is complete and a
//! future reader knows which events must exist to reconstruct a B3 trade.
//!
//! LINK KEY: every maker event carries `signal_id` (the position's stable
//! identity, e.g. "BTC-1780185600-5m-Up"). It already threads entry→exit in
//! this oplog (decision_fired / intent_open / exit_close use it), so filtering
//! by signal_id + ordering by ts_ms reconstructs the full lifecycle. `lot_index`
//! is NOT a valid link (it shifts as positions are removed). Events also carry
//! `token_id` (market) and the maker SELL `order_id` (for the order ops).
//!
//! THE 7 EVENTS (kind -> fields):
//!   b3_maker_placed            { signal_id, token_id, cell, order_id,
//!                                entry_price, target_price, delta, shares }
//!   b3_maker_filled            { signal_id, token_id, order_id,
//!                                fill_price(=target), shares, fee_paid,
//!                                fee_model:"maker_floor" }
//!   b3_maker_partial_fill      { signal_id, token_id, order_id, original_size,
//!                                size_matched, size_remaining, fill_price }
//!   b3_maker_timeout_cancel    { signal_id, token_id, order_id, exit_ts_s,
//!                                size_matched_at_cancel,
//!                                cancel_resp:"canceled"|"not_canceled" }
//!   b3_maker_reconcile         { signal_id, token_id, order_id,
//!                                status(OrderStatusType), original_size,
//!                                size_matched,
//!                                decision:"full_fill"|"partial"|
//!                                         "clean_cancel"|"ambiguous" }
//!   b3_maker_fallback_f2       { signal_id, token_id, maker_order_id,
//!                                fallback_shares, f2_price, f2_fee,
//!                                reason:"clean_cancel"|"partial_remainder" }
//!   b3_maker_rejected_crossable{ signal_id, token_id, order_id(opt/empty),
//!                                target, bid_at_submit,
//!                                action_taken:"taker_capture"|"error" }
//!
//! NOTES:
//!   * The F2 fallback's real taker SELL (FOK) is posted via
//!     place_order_idempotent, which ALREADY emits `live_close_posted` — do NOT
//!     duplicate that order here; b3_maker_fallback_f2 carries `maker_order_id`
//!     only for the lifecycle link.
//!   * `original_size` is required wherever `size_matched` appears (else the
//!     match fraction can't be read). `reason` on fallback distinguishes
//!     clean-cancel from partial-remainder (so shares aren't double-counted).
//!   * `fee_model` on `filled` documents that fee 0 is the maker floor (not an
//!     omission). The backtest→live fee divergence (post_only rejected when
//!     crossable → taker capture) is measurable via b3_maker_rejected_crossable
//!     + the f2_fee.
//! ============================================================================

#![allow(dead_code)] // helpers consumed by the executor + loop as each path is wired

use std::io::Write as _;
use std::path::PathBuf;

use serde_json::{Value, json};

use crate::state::now_ms;

/// Default operational-log path (gitignored, like execution_log).
pub const OPLOG_PATH: &str = "data/live/oplog.jsonl";

/// Rotate the oplog once it exceeds this many bytes.
///
/// The header below claims oplog writes are "infrequent vs market data". Over a
/// multi-day live run that assumption failed badly: the file reached 1.3 GB. An
/// unbounded append-only file is not free, because the dashboard re-reads and
/// JSON-parses the WHOLE thing on every `/api/stats` request — so 1.3 GB turned a
/// 20 KB response into 28 seconds, saturated a tokio worker, and wedged the accept
/// loop (5 connections stuck unaccepted). Trading was unaffected, but the operator
/// lost all visibility into a live-money run.
///
/// Bounding the writer bounds every reader. Rotated segments are KEPT (renamed, not
/// deleted) — they are the raw evidence for settlement forensics.
pub const OPLOG_MAX_BYTES: u64 = 128 * 1024 * 1024;

/// Append-only operational logger. Cheap to clone (just a path); safe to share. Writes
/// are infrequent vs market data (API calls / decisions / orders, not every tick), so
/// a per-event file append is fine — and crash-safe (each line flushed independently).
#[derive(Debug, Clone)]
pub struct OpLog {
    path: PathBuf,
}

impl OpLog {
    #[must_use]
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    /// The default `data/live/oplog.jsonl` logger.
    #[must_use]
    pub fn default_path() -> Self {
        Self::new(OPLOG_PATH)
    }

    /// THE PRIMITIVE: append one event `{ts_ms, kind, data}`. `ts_ms` is stamped here
    /// from the single wall clock so every event shares one timeline. Best-effort: a
    /// write failure must never break trading, so the error is swallowed (but the file
    /// open is create+append, so it only fails on real IO faults).
    pub fn event(&self, kind: &str, data: Value) {
        let row = json!({ "ts_ms": now_ms(), "kind": kind, "data": data });
        let _ = self.append_line(&row);
    }

    fn append_line(&self, row: &Value) -> std::io::Result<()> {
        if let Some(dir) = self.path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        self.rotate_if_large();
        let mut f = std::fs::OpenOptions::new().create(true).append(true).open(&self.path)?;
        writeln!(f, "{}", serde_json::to_string(row).unwrap_or_default())
    }

    /// Rename the current segment aside once it passes [`OPLOG_MAX_BYTES`].
    ///
    /// Best-effort by design: a rotation failure must never break trading, so every
    /// error is swallowed and the write proceeds against the existing file. `rename`
    /// is atomic, so a race between threads costs at most one redundant rename.
    fn rotate_if_large(&self) {
        let Ok(md) = std::fs::metadata(&self.path) else { return };
        if md.len() < OPLOG_MAX_BYTES {
            return;
        }
        let stem = self.path.file_stem().and_then(|s| s.to_str()).unwrap_or("oplog").to_string();
        let mut rotated = self.path.clone();
        rotated.set_file_name(format!("{stem}.{}.jsonl", now_ms()));
        let _ = std::fs::rename(&self.path, &rotated);
    }

    // ---- typed helpers (the common operational cases) ----

    /// Record that an API call is ABOUT TO be made. Returns the start ms so the caller
    /// passes it to `api_ok`/`api_err` for the latency.
    #[must_use]
    pub fn api_call(&self, endpoint: &str, method: &str, params: Value) -> i64 {
        let t = now_ms();
        self.event("api_call", json!({ "endpoint": endpoint, "method": method, "params": params }));
        t
    }

    /// Record an API SUCCESS: status + latency (from `started_ms`) + the response body.
    pub fn api_ok(&self, endpoint: &str, started_ms: i64, status: u16, body: Value) {
        self.event("api_response", json!({
            "endpoint": endpoint, "status": status, "latency_ms": now_ms() - started_ms, "body": body,
        }));
    }

    /// Record an API ERROR with the EXACT textual error from the API (never
    /// summarized) + the status code (if any) + latency.
    pub fn api_err(&self, endpoint: &str, started_ms: i64, status: Option<u16>, error_text: &str) {
        self.event("api_error", json!({
            "endpoint": endpoint, "status_code": status,
            "latency_ms": now_ms() - started_ms, "error_text": error_text,
        }));
    }

    /// A system event (ws_reconnect / feed_stale / kline / trigger / decision /
    /// guard_eval / order_intent / order_transition / reconcile / fill / hold / close).
    /// `kind` is the event name; `detail` the payload.
    pub fn sys(&self, kind: &str, detail: Value) {
        self.event(kind, detail);
    }

    /// An error / exception with full context: what the bot was doing + the exact text.
    pub fn err(&self, context: &str, error_text: &str, detail: Value) {
        self.event("error", json!({ "context": context, "error_text": error_text, "detail": detail }));
    }
}

#[cfg(test)]
mod tests {
    /// Rotation must fire on size, and must PRESERVE the old segment — those bytes
    /// are the settlement-forensics evidence. Mutation check: drop the
    /// `rotate_if_large()` call from `append_line` and this fails on `rotated == 0`.
    #[test]
    fn oplog_rotates_on_size_and_keeps_the_old_segment() {
        let dir = std::env::temp_dir().join(format!("rb_rot_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("oplog.jsonl");
        let log = OpLog::new(&path);

        // Write past the cap. Each row carries a fat payload so this stays quick.
        let blob = "x".repeat(64 * 1024);
        let mut wrote = 0u64;
        while wrote < OPLOG_MAX_BYTES + (1 << 20) {
            log.event("filler", json!({ "b": blob }));
            wrote += blob.len() as u64;
        }

        // The live file must have been cut back down...
        let live = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        assert!(live < OPLOG_MAX_BYTES, "live segment not rotated: {live} bytes");

        // ...and the rotated segment must still be on disk, not deleted.
        let rotated: Vec<_> = std::fs::read_dir(&dir).unwrap()
            .filter_map(Result::ok)
            .filter(|e| {
                let n = e.file_name().to_string_lossy().to_string();
                n.starts_with("oplog.") && n != "oplog.jsonl"
            })
            .collect();
        assert!(!rotated.is_empty(), "rotation deleted the old segment instead of keeping it");
        let kept: u64 = rotated.iter().map(|e| e.metadata().map(|m| m.len()).unwrap_or(0)).sum();
        assert!(kept > 0, "rotated segment is empty");

        let _ = std::fs::remove_dir_all(&dir);
    }

    use super::*;

    fn tmp(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!("rb_oplog_{tag}_{}.jsonl", std::process::id()))
    }

    #[test]
    fn event_appends_one_line_with_ts_kind_data() {
        let p = tmp("evt");
        let _ = std::fs::remove_file(&p);
        let log = OpLog::new(&p);
        log.event("kline", json!({ "asset": "BTC", "close": 74537.5 }));
        let body = std::fs::read_to_string(&p).unwrap();
        let lines: Vec<&str> = body.lines().collect();
        assert_eq!(lines.len(), 1);
        let v: Value = serde_json::from_str(lines[0]).unwrap();
        assert!(v.get("ts_ms").and_then(Value::as_i64).is_some(), "has ts_ms");
        assert_eq!(v["kind"], "kline");
        assert_eq!(v["data"]["asset"], "BTC");
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn append_only_accumulates() {
        let p = tmp("append");
        let _ = std::fs::remove_file(&p);
        let log = OpLog::new(&p);
        log.event("a", json!({}));
        log.event("b", json!({}));
        log.event("c", json!({}));
        let body = std::fs::read_to_string(&p).unwrap();
        assert_eq!(body.lines().count(), 3, "append-only: all three rows present");
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn api_call_then_ok_records_latency_and_body() {
        let p = tmp("apiok");
        let _ = std::fs::remove_file(&p);
        let log = OpLog::new(&p);
        let started = log.api_call("/price", "GET", json!({ "token": "T", "side": "SELL" }));
        log.api_ok("/price", started, 200, json!({ "price": "0.51" }));
        let rows: Vec<Value> = std::fs::read_to_string(&p).unwrap()
            .lines().map(|l| serde_json::from_str(l).unwrap()).collect();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["kind"], "api_call");
        assert_eq!(rows[0]["data"]["endpoint"], "/price");
        assert_eq!(rows[1]["kind"], "api_response");
        assert_eq!(rows[1]["data"]["status"], 200);
        assert!(rows[1]["data"]["latency_ms"].as_i64().unwrap() >= 0);
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn api_err_preserves_exact_error_text() {
        let p = tmp("apierr");
        let _ = std::fs::remove_file(&p);
        let log = OpLog::new(&p);
        // The EXACT API text must survive verbatim — no summarizing.
        let exact = "invalid amounts, the sell orders maker amount supports a max accuracy of 2 decimals";
        let started = log.api_call("/order", "POST", json!({}));
        log.api_err("/order", started, Some(400), exact);
        let rows: Vec<Value> = std::fs::read_to_string(&p).unwrap()
            .lines().map(|l| serde_json::from_str(l).unwrap()).collect();
        assert_eq!(rows[1]["kind"], "api_error");
        assert_eq!(rows[1]["data"]["status_code"], 400);
        assert_eq!(rows[1]["data"]["error_text"], exact, "exact API text preserved verbatim");
        let _ = std::fs::remove_file(&p);
    }
}
