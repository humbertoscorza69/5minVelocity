//! Capa A replay driver — replay-ONLY file IO around the [`super::SignalEngine`].
//!
//! This drives the SAME engine the live bot will use (Phase 5), but fed from the
//! recorder's historical jsonl instead of the live WS. It:
//!   1. builds a [`ReplayCatalog`] of active markets from the recorder markets log,
//!   2. confirms epoch continuity per (asset, interval) (the scope-guard precondition),
//!   3. replays the per-asset 1s-kline jsonl through the engine, projecting each
//!      trigger onto the catalog,
//!   4. writes the deterministic signal sequence to jsonl for the parity diff.
//!
//! The kline line shape is the recorder's wrapper around the raw Binance combined
//! stream: `{"payload": {"data": {"e":"kline", "k": {"t","c","x","s"}}}}` — i.e.
//! `payload.data.k`, exactly what the Python `collect_binance` consumes.

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;

use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::Value;

use super::{expand_signals, MarketCatalog, MarketRef, Signal, SignalEngine, Trigger, INTERVALS};

/// The only assets the strategy triggers on (Python subscribes btcusdt/ethusdt).
const STRATEGY_ASSETS: [&str; 2] = ["BTC", "ETH"];

/// Active-market catalog loaded from the recorder markets log, keyed
/// `(asset, interval, epoch)`. Filtered to the strategy universe (BTC/ETH, 5m/15m).
pub struct ReplayCatalog {
    map: HashMap<(String, String, i64), MarketRef>,
}

/// Per-(asset, interval) epoch-continuity summary (scope-guard precondition).
#[derive(Debug, Clone)]
pub struct ContinuityRow {
    pub asset: String,
    pub interval: String,
    pub count: usize,
    pub first: i64,
    pub last: i64,
    pub expected: i64,
    pub gaps: i64,
}

impl ReplayCatalog {
    /// Path-based loader (backward-compat, used by capa_b). Wraps the reader-based
    /// loader; for zstd-compressed markets logs, call `from_markets_log_reader`
    /// with a `zstd::Decoder`-backed BufRead directly.
    pub fn from_markets_log(path: &Path) -> Result<Self> {
        let f = File::open(path)
            .with_context(|| format!("opening markets log {}", path.display()))?;
        Self::from_markets_log_reader(BufReader::new(f))
    }

    /// Reader-based loader (G8-pre-TP). The caller chooses how to open the file
    /// (plain `BufReader<File>` or `BufReader<zstd::Decoder>`), so historical
    /// .jsonl.zst markets logs can be consumed without pre-decompression.
    pub fn from_markets_log_reader<R: BufRead>(reader: R) -> Result<Self> {
        let mut map: HashMap<(String, String, i64), MarketRef> = HashMap::new();
        for line in reader.lines() {
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
            if !STRATEGY_ASSETS.contains(&asset) {
                continue;
            }
            if !INTERVALS.iter().any(|(iv, _)| *iv == interval) {
                continue;
            }
            let epoch = match m.get("epoch").and_then(value_as_i64) {
                Some(e) if e > 0 => e,
                _ => continue,
            };
            map.entry((asset.to_string(), interval.to_string(), epoch))
                .or_insert_with(|| MarketRef {
                    up_token_id: str_field(m, "up_token_id"),
                    down_token_id: str_field(m, "down_token_id"),
                    condition_id: str_field(m, "condition_id"),
                    end_time: str_field(m, "end_time"),
                    // W9-Pieza1: fees AS-IS from the markets log. JSON ints
                    // come through `as_i64`; missing or non-int fields fall
                    // to 0 (the catalog stays loadable even for older logs
                    // that lack these fields). Python downstream documents
                    // the unit interpretation.
                    maker_base_fee: m.get("maker_base_fee")
                        .and_then(Value::as_i64).unwrap_or(0),
                    taker_base_fee: m.get("taker_base_fee")
                        .and_then(Value::as_i64).unwrap_or(0),
                    fee_type: str_field(m, "fee_type"),
                });
        }
        Ok(Self { map })
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Every up + down token across the catalog (the strategy token universe) —
    /// used to filter the recorder's Polymarket event streams.
    pub fn token_set(&self) -> std::collections::HashSet<String> {
        let mut s = std::collections::HashSet::new();
        for m in self.map.values() {
            if !m.up_token_id.is_empty() {
                s.insert(m.up_token_id.clone());
            }
            if !m.down_token_id.is_empty() {
                s.insert(m.down_token_id.clone());
            }
        }
        s
    }

    /// Tokens whose market is active during `[start_sec, end_sec)` — i.e. whose
    /// `[epoch, epoch+interval_secs]` overlaps the window. Used to shrink the event
    /// stream to the markets actually tradeable in the replay window.
    pub fn tokens_in_window(&self, start_sec: i64, end_sec: i64) -> std::collections::HashSet<String> {
        let mut s = std::collections::HashSet::new();
        for ((_, interval, epoch), m) in &self.map {
            let secs = INTERVALS
                .iter()
                .find(|(iv, _)| iv == interval)
                .map(|(_, sc)| *sc)
                .unwrap_or(0);
            if *epoch < end_sec && epoch + secs > start_sec {
                if !m.up_token_id.is_empty() {
                    s.insert(m.up_token_id.clone());
                }
                if !m.down_token_id.is_empty() {
                    s.insert(m.down_token_id.clone());
                }
            }
        }
        s
    }

    /// Sorted-epoch gap report per (asset, interval). `gaps == 0` ⟹ every epoch in
    /// `[first, last]` had an active market (continuous roll), so the scope guard's
    /// "current epoch always exists" assumption holds across the window.
    pub fn continuity(&self) -> Vec<ContinuityRow> {
        let mut out = Vec::new();
        for asset in STRATEGY_ASSETS {
            for (interval, secs) in INTERVALS {
                let mut epochs: Vec<i64> = self
                    .map
                    .keys()
                    .filter(|(a, iv, _)| a == asset && iv == interval)
                    .map(|(_, _, e)| *e)
                    .collect();
                if epochs.is_empty() {
                    continue;
                }
                epochs.sort_unstable();
                let first = epochs[0];
                let last = *epochs.last().unwrap();
                let expected = (last - first) / secs + 1;
                let count = epochs.len();
                out.push(ContinuityRow {
                    asset: asset.to_string(),
                    interval: interval.to_string(),
                    count,
                    first,
                    last,
                    expected,
                    gaps: expected - count as i64,
                });
            }
        }
        out
    }
}

impl MarketCatalog for ReplayCatalog {
    fn active_market(&self, asset: &str, interval: &str, epoch: i64) -> Option<&MarketRef> {
        self.map
            .get(&(asset.to_string(), interval.to_string(), epoch))
    }
}

/// The output record (one jsonl line per signal). Field order is irrelevant to the
/// parity diff (it keys on the tuple); `ret_bps` is emitted as the f64 so the diff
/// can parse it back and assert exact equality.
#[derive(Serialize)]
struct SignalOut<'a> {
    asset: &'a str,
    trigger_ts: i64,
    interval: &'a str,
    direction: &'a str,
    window_s: u8,
    ret_bps: f64,
    ttr: i64,
    stratum: &'a str,
    epoch: i64,
    bet_token_id: &'a str,
}

fn signal_json(s: &Signal) -> String {
    let out = SignalOut {
        asset: &s.asset,
        trigger_ts: s.trigger_ts,
        interval: &s.interval,
        direction: s.direction.as_str(),
        window_s: s.window_s,
        ret_bps: s.ret_bps,
        ttr: s.ttr,
        stratum: s.stratum.as_str(),
        epoch: s.epoch,
        bet_token_id: &s.bet_token_id,
    };
    serde_json::to_string(&out).expect("signal serialize")
}

/// One raw trigger (pre-scope), for the trigger-level parity diff.
#[derive(Serialize)]
struct TriggerOut<'a> {
    asset: &'a str,
    trigger_ts: i64,
    window_s: u8,
    ret_bps: f64,
}

fn trigger_json(t: &Trigger) -> String {
    let out = TriggerOut {
        asset: &t.asset,
        trigger_ts: t.trigger_ts,
        window_s: t.window_s,
        ret_bps: t.ret_bps,
    };
    serde_json::to_string(&out).expect("trigger serialize")
}

/// CLI entry: derive the standard recorder paths from `data_root` + `date`, run the
/// replay, write the signals jsonl, and print a parity-ready report.
pub fn run_cli(data_root: &Path, date: &str, out: Option<&Path>) -> Result<()> {
    let markets_log = data_root
        .join("live_l2/polymarket/markets")
        .join(format!("{date}.jsonl"));
    let btc_klines = data_root
        .join("live_l2/binance/btcusdt_kline_1s")
        .join(format!("{date}.jsonl"));
    let eth_klines = data_root
        .join("live_l2/binance/ethusdt_kline_1s")
        .join(format!("{date}.jsonl"));
    let out_path = out.map(Path::to_path_buf).unwrap_or_else(|| {
        data_root
            .join("derived/capa_a")
            .join(format!("rust_signals_{date}.jsonl"))
    });

    println!("[capa-a replay] date={date}");
    println!("[capa-a replay] markets log: {}", markets_log.display());
    println!(
        "[capa-a replay] klines: {} | {}",
        btc_klines.display(),
        eth_klines.display()
    );

    let catalog = ReplayCatalog::from_markets_log(&markets_log)?;
    println!(
        "[capa-a replay] catalog markets (BTC/ETH 5m/15m): {}",
        catalog.len()
    );
    for c in catalog.continuity() {
        println!(
            "[capa-a replay]   continuity {:<3} {:<3}: {} epochs [{}..{}], expected {}, gaps {}",
            c.asset, c.interval, c.count, c.first, c.last, c.expected, c.gaps
        );
    }
    if catalog.is_empty() {
        anyhow::bail!("empty catalog — check markets log path/date");
    }

    let mut eng = SignalEngine::new();
    let mut signals: Vec<Signal> = Vec::new();
    let mut triggers: Vec<Trigger> = Vec::new();
    let kb = replay_asset(&mut eng, &catalog, &btc_klines, &mut triggers, &mut signals)?;
    let ke = replay_asset(&mut eng, &catalog, &eth_klines, &mut triggers, &mut signals)?;

    // Deterministic ordering for stable files + an easy diff (the gate is set-based).
    signals.sort_by(|a, b| {
        (a.trigger_ts, a.asset.as_str(), a.interval.as_str()).cmp(&(
            b.trigger_ts,
            b.asset.as_str(),
            b.interval.as_str(),
        ))
    });
    triggers.sort_by(|a, b| (a.trigger_ts, a.asset.as_str()).cmp(&(b.trigger_ts, b.asset.as_str())));

    if let Some(parent) = out_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut w = BufWriter::new(File::create(&out_path).with_context(|| {
        format!("creating output {}", out_path.display())
    })?);
    for s in &signals {
        writeln!(w, "{}", signal_json(s))?;
    }
    w.flush()?;

    // Sibling triggers file (pre-scope), for the trigger-level parity diff.
    let trig_path = out_path.with_file_name(format!("rust_triggers_{date}.jsonl"));
    let mut tw = BufWriter::new(File::create(&trig_path)?);
    for t in &triggers {
        writeln!(tw, "{}", trigger_json(t))?;
    }
    tw.flush()?;

    // ---- report ----
    let n5 = signals.iter().filter(|s| s.interval == "5m").count();
    let n15 = signals.iter().filter(|s| s.interval == "15m").count();
    let n_btc = signals.iter().filter(|s| s.asset == "BTC").count();
    let n_eth = signals.iter().filter(|s| s.asset == "ETH").count();
    let t_btc = triggers.iter().filter(|t| t.asset == "BTC").count();
    let t_eth = triggers.iter().filter(|t| t.asset == "ETH").count();
    println!("[capa-a replay] klines processed: BTC={kb} ETH={ke}");
    println!(
        "[capa-a replay] triggers detected: total={} (BTC={t_btc} ETH={t_eth})",
        triggers.len()
    );
    println!(
        "[capa-a replay] signals: total={}, by asset BTC={n_btc} ETH={n_eth}, by interval 5m={n5} 15m={n15}",
        signals.len()
    );
    println!("[capa-a replay] wrote {} + {}", out_path.display(), trig_path.display());
    Ok(())
}

/// Replay one asset's kline file through the engine, appending triggers + signals.
/// Returns the count of finalized klines processed.
fn replay_asset(
    eng: &mut SignalEngine,
    catalog: &dyn MarketCatalog,
    path: &Path,
    triggers: &mut Vec<Trigger>,
    signals: &mut Vec<Signal>,
) -> Result<u64> {
    let f = File::open(path).with_context(|| format!("opening klines {}", path.display()))?;
    let reader = BufReader::new(f);
    let mut nk = 0u64;
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let v: Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let data = &v["payload"]["data"];
        if data.get("e").and_then(Value::as_str) != Some("kline") {
            continue;
        }
        let k = &data["k"];
        if k.get("x").and_then(Value::as_bool) != Some(true) {
            continue; // only finalized bars
        }
        let symbol = k.get("s").and_then(Value::as_str).unwrap_or("");
        let asset = match symbol.to_ascii_uppercase().as_str() {
            "BTCUSDT" => "BTC",
            "ETHUSDT" => "ETH",
            _ => continue,
        };
        let t_ms = match k.get("t").and_then(value_as_i64) {
            Some(t) => t,
            None => continue,
        };
        // close is the decimal STRING "73621.38000000" → f64, exactly like the live
        // client's `.parse::<f64>()` and Python's `float(k["c"])`.
        let close = match k.get("c").and_then(Value::as_str).and_then(|s| s.parse::<f64>().ok()) {
            Some(c) => c,
            None => continue,
        };
        nk += 1;
        if let Some(trig) = eng.on_kline(asset, t_ms / 1000, close) {
            signals.extend(expand_signals(&trig, catalog));
            triggers.push(trig);
        }
    }
    Ok(nk)
}

fn str_field(m: &Value, key: &str) -> String {
    m.get(key).and_then(Value::as_str).unwrap_or("").to_string()
}

/// Parse an i64 from a JSON number OR a numeric string (epoch/t fields vary).
fn value_as_i64(v: &Value) -> Option<i64> {
    v.as_i64()
        .or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok()))
        .or_else(|| v.as_f64().map(|f| f as i64))
}
