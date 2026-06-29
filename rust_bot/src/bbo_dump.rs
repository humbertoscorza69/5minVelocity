//! Phase 5 verification: confirm the Rust event-BBO extraction == the Python's.
//!
//! Reads the recorder's `book` / `price_change` / `best_bid_ask` jsonl, filters to
//! the strategy tokens (BTC/ETH 5m/15m up/down, from the markets log), runs each
//! event through the REAL live handler ([`crate::ws::polymarket::handle_event`]),
//! and dumps the resulting per-event BBO. A Python comparator extracts the same
//! from the same events and diffs — expected 0 divergence (both take the best
//! reported by the event). Offline + deterministic; does not touch the live bot.

use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;

use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::{Value, json};

use crate::events;
use crate::state::SharedState;
use crate::ws::polymarket::handle_event;

/// One dumped BBO row, keyed by (received_at, event_type, token) for the diff.
#[derive(Serialize)]
struct BboOut<'a> {
    received_at: &'a str,
    event_type: &'a str,
    token: &'a str,
    best_ask: Option<f64>,
    best_bid: Option<f64>,
    ts_ms: i64,
}

/// Strategy token universe (BTC/ETH 5m/15m up + down) from the markets log.
fn load_strategy_tokens(markets_log: &Path) -> Result<HashSet<String>> {
    let f = File::open(markets_log)
        .with_context(|| format!("opening markets log {}", markets_log.display()))?;
    let mut set = HashSet::new();
    for line in BufReader::new(f).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let v: Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let m = &v["market"];
        let asset = m.get("asset").and_then(Value::as_str).unwrap_or("");
        let interval = m.get("interval").and_then(Value::as_str).unwrap_or("");
        if !matches!(asset, "BTC" | "ETH") || !matches!(interval, "5m" | "15m") {
            continue;
        }
        for k in ["up_token_id", "down_token_id"] {
            if let Some(t) = m.get(k).and_then(Value::as_str)
                && !t.is_empty()
            {
                set.insert(t.to_string());
            }
        }
    }
    Ok(set)
}

pub fn run_cli(data_root: &Path, date: &str, out: Option<&Path>, max_per_file: u64) -> Result<()> {
    let markets_log = data_root
        .join("live_l2/polymarket/markets")
        .join(format!("{date}.jsonl"));
    let out_path = out.map(Path::to_path_buf).unwrap_or_else(|| {
        data_root
            .join("derived/capa_a")
            .join(format!("rust_bbo_{date}.jsonl"))
    });

    let tokens = load_strategy_tokens(&markets_log)?;
    println!("[bbo-dump] date={date} strategy tokens: {}", tokens.len());
    if tokens.is_empty() {
        anyhow::bail!("no strategy tokens — check markets log path/date");
    }

    // The real handler signature requires an event logger, but bbo_dump is an
    // offline replay tool — the recorded events would only be written to a
    // throwaway tmp file and never read. Spawn the logger DISABLED so no file
    // (and no parent dir) is created at all; `record()` becomes a cheap no-op
    // and `handle_event` runs the rest of its live-extraction path unchanged.
    let state = SharedState::new();
    let tmp = std::env::temp_dir().join(format!("bbo_dump_ts_{}.jsonl", crate::state::now_ms()));
    let (logger, _h) = events::spawn(&tmp, false)?;

    if let Some(p) = out_path.parent() {
        fs::create_dir_all(p)?;
    }
    let mut w = BufWriter::new(File::create(&out_path)?);
    let mut total = 0u64;

    for etype in ["book", "price_change", "best_bid_ask"] {
        let path = data_root
            .join("live_l2/polymarket")
            .join(etype)
            .join(format!("{date}.jsonl"));
        let f = File::open(&path).with_context(|| format!("opening {}", path.display()))?;
        let mut n = 0u64;
        for line in BufReader::new(f).lines() {
            if n >= max_per_file {
                break;
            }
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            let rec: Value = match serde_json::from_str(&line) {
                Ok(v) => v,
                Err(_) => continue,
            };
            let received_at = rec
                .get("received_at")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let mut payload = match rec.get("payload") {
                Some(p) => p.clone(),
                None => continue,
            };
            // Force the event_type to match the file (book payloads may omit it).
            payload["event_type"] = json!(etype);
            let touched = event_touches(&payload, &tokens);
            if touched.is_empty() {
                continue;
            }
            // Run the EXACT live extraction path.
            handle_event(&payload, &state, &logger);
            for token in &touched {
                if let Some(b) = state.bbo.get(token) {
                    writeln!(
                        w,
                        "{}",
                        serde_json::to_string(&BboOut {
                            received_at: &received_at,
                            event_type: etype,
                            token,
                            best_ask: b.best_ask,
                            best_bid: b.best_bid,
                            ts_ms: b.ts_ms,
                        })?
                    )?;
                    total += 1;
                }
            }
            n += 1;
        }
        println!("[bbo-dump] {etype}: {n} qualifying events processed");
    }
    w.flush()?;
    drop(logger);
    let _ = fs::remove_file(&tmp);
    println!("[bbo-dump] wrote {} ({total} bbo rows)", out_path.display());
    Ok(())
}

/// The strategy tokens an event touches (book/best_bid_ask → top-level asset_id;
/// price_change → each per-change asset_id).
fn event_touches(payload: &Value, tokens: &HashSet<String>) -> Vec<String> {
    let et = payload.get("event_type").and_then(Value::as_str).unwrap_or("");
    let mut out = Vec::new();
    if et == "price_change" {
        let changes = payload
            .get("price_changes")
            .or_else(|| payload.get("changes"));
        if let Some(arr) = changes.and_then(Value::as_array) {
            for ch in arr {
                // Only pcs with BOTH best sides (the handler skips the rest), so the
                // dump and the Python oracle stay aligned.
                let has_best = ch.get("best_ask").is_some() && ch.get("best_bid").is_some();
                if has_best
                    && let Some(t) = ch.get("asset_id").and_then(Value::as_str)
                    && tokens.contains(t)
                {
                    out.push(t.to_string());
                }
            }
        }
    } else if let Some(t) = payload.get("asset_id").and_then(Value::as_str)
        && tokens.contains(t)
    {
        out.push(t.to_string());
    }
    out
}
