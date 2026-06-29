//! Phase 5 Capa B — Rust side. Replays the recorder (klines + Polymarket
//! book/price_change/best_bid_ask for the strategy tokens) in `received_at` order,
//! reconstructs the event-BBO as-of each instant, and runs the SAME engine the
//! live bot will use (`SignalEngine` then `decide` then `PaperExecutor`). It emits
//! per-trigger decisions + per-fire trades for the Capa B diff vs the Python
//! oracle (Level 2).
//!
//! It also emits `tape_<date>.jsonl` — the merged, `received_at`-ordered stream
//! of klines + event-BBO updates — which the Python oracle (`capa_b_oracle.py`)
//! replays to drive the REAL operational `find_qualifying_markets`. The event-BBO
//! extraction was verified 0-divergence (Rust == Python via --bbo-dump), so the
//! tape is a faithful shared input: it carries the SAME reconstructed book to both
//! sides, isolating the diff to the decision logic.

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;

use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::Value;

use crate::decision::executor::{Executor, ExitKind, OpenIntent, PaperExecutor};
use crate::decision::{Action, BookProvider, DecisionConfig, RestProbe, decide};
use crate::signal::replay::ReplayCatalog;
use crate::signal::{Direction, SignalEngine, expand_signals};
use crate::state::EventBbo;
use crate::state::persist::{OpenPosition, Outcome};
use crate::trading_loop;

/// A recorder event tagged with its `received_at` (ms, the recorder clock).
struct Timed {
    recv_ms: i64,
    ev: Ev,
}

enum Ev {
    Kline { asset: &'static str, t_open_ms: i64, close: f64 },
    Bbo { token: String, best_ask: Option<f64>, best_bid: Option<f64>, ts_ms: i64 },
}

/// BookProvider over the live-updated event-BBO map.
struct ReplayBook<'a>(&'a HashMap<String, EventBbo>);
impl BookProvider for ReplayBook<'_> {
    fn bbo(&self, token: &str) -> Option<EventBbo> {
        self.0.get(token).copied()
    }
}

/// One tape event (the shared input the Python oracle replays). Tagged so the
/// oracle can branch on kline vs bbo.
#[derive(Serialize)]
#[serde(tag = "t")]
enum TapeEv<'a> {
    #[serde(rename = "k")]
    Kline { recv_ms: i64, asset: &'a str, t_open_ms: i64, close: f64 },
    #[serde(rename = "b")]
    Bbo { recv_ms: i64, token: &'a str, ba: Option<f64>, bb: Option<f64>, ts: i64 },
}

#[derive(Serialize)]
struct DecisionOut<'a> {
    asset: &'a str,
    trigger_ts: i64,
    direction: &'a str,
    fired: bool,
    reject_reason: &'a str,
    primary_interval: Option<&'a str>,
    now_ms: i64,
}

#[derive(Serialize)]
struct TradeOut<'a> {
    kind: &'a str, // "fire" | "close_opposite" | "close_book"
    asset: &'a str,
    trigger_ts: i64,
    interval: &'a str,
    token: &'a str,
    side: &'a str,
    shares: f64,
    price: f64,
}

pub fn run_cli(
    data_root: &Path,
    date: &str,
    start_hour: u32,
    end_hour: u32,
    regla_c: bool,
    out_dir: Option<&Path>,
) -> Result<()> {
    let variant = if regla_c { "opposite" } else { "baseline" };
    let markets_log = data_root
        .join("live_l2/polymarket/markets")
        .join(format!("{date}.jsonl"));
    let catalog = ReplayCatalog::from_markets_log(&markets_log)?;
    let midnight = date_midnight_sec(date).context("parsing date (YYYY-MM-DD)")?;
    let win_start = midnight + start_hour as i64 * 3600;
    let win_end = midnight + end_hour as i64 * 3600;
    // Only tokens whose market is active in the window (current/rolling), not every
    // epoch of the day — keeps the event stream + tape tractable.
    let tokens = catalog.tokens_in_window(win_start, win_end);
    let out = out_dir
        .map(Path::to_path_buf)
        .unwrap_or_else(|| data_root.join("derived/capa_b"));
    fs::create_dir_all(&out)?;
    println!("[capa-b rust] date={date} hours=[{start_hour},{end_hour}) window-tokens={}", tokens.len());

    // ---- load + merge events (windowed, filtered) ----
    let mut events: Vec<Timed> = Vec::new();
    for (sym, asset) in [("btcusdt", "BTC"), ("ethusdt", "ETH")] {
        let p = data_root
            .join("live_l2/binance")
            .join(format!("{sym}_kline_1s"))
            .join(format!("{date}.jsonl"));
        load_klines(&p, asset, start_hour, end_hour, &mut events)?;
    }
    for etype in ["book", "price_change", "best_bid_ask"] {
        let p = data_root
            .join("live_l2/polymarket")
            .join(etype)
            .join(format!("{date}.jsonl"));
        load_pm(&p, etype, &tokens, start_hour, end_hour, &mut events)?;
    }
    events.sort_by_key(|t| t.recv_ms);
    println!("[capa-b rust] merged events: {}", events.len());

    // ---- simulate ----
    let cfg = DecisionConfig { regla_c_enabled: regla_c, ..DecisionConfig::default() };
    println!("[capa-b rust] variant={variant} (regla_c={regla_c})");
    let mut engine = SignalEngine::new();
    let mut bbo: HashMap<String, EventBbo> = HashMap::new();
    let mut positions: Vec<OpenPosition> = Vec::new();
    let mut total_pnl = rust_decimal::Decimal::ZERO;

    let dec_path = out.join(format!("rust_decisions_{variant}_{date}.jsonl"));
    let trade_path = out.join(format!("rust_trades_{variant}_{date}.jsonl"));
    let tape_path = out.join(format!("tape_{date}.jsonl")); // events are variant-independent
    let mut dw = BufWriter::new(File::create(&dec_path)?);
    let mut tw = BufWriter::new(File::create(&trade_path)?);
    let mut tape_w = BufWriter::new(File::create(&tape_path)?);
    let mut n_dec = 0u64;
    let mut n_fire = 0u64;
    let mut n_close_opp = 0u64;

    for t in &events {
        // Emit to the tape (the shared, received_at-ordered input the oracle replays).
        let tev = match &t.ev {
            Ev::Kline { asset, t_open_ms, close } => {
                TapeEv::Kline { recv_ms: t.recv_ms, asset, t_open_ms: *t_open_ms, close: *close }
            }
            Ev::Bbo { token, best_ask, best_bid, ts_ms } => {
                TapeEv::Bbo { recv_ms: t.recv_ms, token, ba: *best_ask, bb: *best_bid, ts: *ts_ms }
            }
        };
        writeln!(tape_w, "{}", serde_json::to_string(&tev)?)?;
        // 1. Time-based exits up to `now` (per-event cadence; identical in the oracle).
        {
            let mut ex = PaperExecutor::new(&mut positions);
            for ct in ex.close_due(t.recv_ms, &ReplayBook(&bbo)) {
                writeln!(tw, "{}", trade_json("close_book", "", 0, &ct.interval, &ct.token_id, side_str(ct.side), f64_dec(ct.shares), f64_dec(ct.exit_price)))?;
            }
            total_pnl += ex.realized_pnl;
        }
        // 2. Apply the event.
        match &t.ev {
            Ev::Bbo { token, best_ask, best_bid, ts_ms } => {
                bbo.insert(token.clone(), EventBbo { best_ask: *best_ask, best_bid: *best_bid, ts_ms: *ts_ms });
            }
            Ev::Kline { asset, t_open_ms, close } => {
                let Some(trig) = engine.on_kline(asset, t_open_ms / 1000, *close) else {
                    continue;
                };
                let scope = expand_signals(&trig, &catalog);
                let dir = if trig.ret_bps > 0.0 { Direction::Up } else { Direction::Down };
                let dec = decide(
                    asset,
                    trig.trigger_ts,
                    dir,
                    &scope,
                    &cfg,
                    &ReplayBook(&bbo),
                    t.recv_ms,
                    |_| RestProbe::Skip,
                    &positions,
                );
                // Apply actions (executor scoped so it can borrow positions mut).
                {
                    let mut ex = PaperExecutor::new(&mut positions);
                    for a in &dec.actions {
                        match a {
                            Action::Fire { interval, stratum, bet_token, fill, shares } => {
                                let hold = cfg.band(*stratum).2;
                                ex.open(OpenIntent {
                                    token_id: bet_token.clone(),
                                    asset: asset.to_string(),
                                    side: dir_to_outcome(dir),
                                    fill_price: *fill,
                                    shares: *shares,
                                    interval: interval.clone(),
                                    signal_id: format!("{}-{}-{}-{}", asset, trig.trigger_ts, interval, dir_str(dir)),
                                    exit_ts_s: trig.trigger_ts + hold,
                                    now_ms: t.recv_ms,
                                });
                                writeln!(tw, "{}", trade_json("fire", asset, trig.trigger_ts, interval, bet_token, dir_str(dir), *shares, *fill))?;
                                n_fire += 1;
                            }
                            Action::CloseOpposite { interval, opposite_token, close_price, .. } => {
                                if let Some(px) = close_price {
                                    for ct in ex.close_token(opposite_token, *px, ExitKind::OppositeTriggerClose, t.recv_ms) {
                                        writeln!(tw, "{}", trade_json("close_opposite", asset, trig.trigger_ts, interval, &ct.token_id, side_str(ct.side), f64_dec(ct.shares), *px))?;
                                    }
                                    n_close_opp += 1;
                                }
                            }
                            Action::Reject { .. } => {}
                        }
                    }
                    total_pnl += ex.realized_pnl;
                }
                writeln!(dw, "{}", serde_json::to_string(&DecisionOut {
                    asset,
                    trigger_ts: trig.trigger_ts,
                    direction: dir_str(dir),
                    fired: dec.fired,
                    reject_reason: &dec.reject_reason,
                    primary_interval: dec.primary_interval.as_deref(),
                    now_ms: t.recv_ms,
                })?)?;
                n_dec += 1;
            }
        }
    }
    dw.flush()?;
    tw.flush()?;
    tape_w.flush()?;
    println!("[capa-b rust] decisions={n_dec} fires={n_fire} close_opposite={n_close_opp} realized_pnl={total_pnl}");
    println!(
        "[capa-b rust] wrote {} + {} + tape {}",
        dec_path.display(),
        trade_path.display(),
        tape_path.display()
    );
    Ok(())
}

/// Phase 6 D1 PARITY GATE: replay the SAME recorder tape through BOTH the
/// Capa-B-validated decision path (direct `decide`) AND the live loop seam
/// (`trading_loop::process_kline`), asserting every trigger's `TriggerDecision` is
/// IDENTICAL. Proves the loop ROUTES the validated logic without corrupting it (the
/// Capa B path is already 100% vs the Python oracle). Baseline variant (REGLA C off →
/// positions don't affect the decision, so both paths use `&[]`). Read-only.
pub fn run_d1_parity(data_root: &Path, date: &str, start_hour: u32, end_hour: u32) -> Result<()> {
    let markets_log = data_root.join("live_l2/polymarket/markets").join(format!("{date}.jsonl"));
    let catalog = ReplayCatalog::from_markets_log(&markets_log)?;
    let midnight = date_midnight_sec(date).context("parsing date (YYYY-MM-DD)")?;
    let win_start = midnight + start_hour as i64 * 3600;
    let win_end = midnight + end_hour as i64 * 3600;
    let tokens = catalog.tokens_in_window(win_start, win_end);
    println!("[d1-parity] date={date} hours=[{start_hour},{end_hour}) window-tokens={}", tokens.len());

    let mut events: Vec<Timed> = Vec::new();
    for (sym, asset) in [("btcusdt", "BTC"), ("ethusdt", "ETH")] {
        let p = data_root.join("live_l2/binance").join(format!("{sym}_kline_1s")).join(format!("{date}.jsonl"));
        load_klines(&p, asset, start_hour, end_hour, &mut events)?;
    }
    for etype in ["book", "price_change", "best_bid_ask"] {
        let p = data_root.join("live_l2/polymarket").join(etype).join(format!("{date}.jsonl"));
        load_pm(&p, etype, &tokens, start_hour, end_hour, &mut events)?;
    }
    events.sort_by_key(|t| t.recv_ms);
    println!("[d1-parity] merged events: {}", events.len());

    let cfg = DecisionConfig::default(); // baseline (regla_c off), stake 10 = oracle
    let mut eng_a = SignalEngine::new();
    let mut eng_b = SignalEngine::new();
    let mut bbo: HashMap<String, EventBbo> = HashMap::new();
    let (mut triggers, mut matches, mut mism) = (0u64, 0u64, 0u64);
    let mut first_mism: Vec<String> = Vec::new();

    for t in &events {
        match &t.ev {
            Ev::Bbo { token, best_ask, best_bid, ts_ms } => {
                bbo.insert(token.clone(), EventBbo { best_ask: *best_ask, best_bid: *best_bid, ts_ms: *ts_ms });
            }
            Ev::Kline { asset, t_open_ms, close } => {
                let t_s = t_open_ms / 1000;
                // Path A — the Capa-B-validated direct path.
                let dec_a = eng_a.on_kline(asset, t_s, *close).map(|trig| {
                    let scope = expand_signals(&trig, &catalog);
                    let dir = if trig.ret_bps > 0.0 { Direction::Up } else { Direction::Down };
                    decide(asset, trig.trigger_ts, dir, &scope, &cfg, &ReplayBook(&bbo), t.recv_ms, |_| RestProbe::Skip, &[])
                });
                // Path B — the live loop seam.
                let kc = crate::state::KlineClose { asset: asset.to_string(), t_s, close: *close, received_at_ms: t.recv_ms };
                let dec_b = trading_loop::process_kline(
                    &mut eng_b, &kc, &catalog, &ReplayBook(&bbo), &cfg, |_| RestProbe::Skip, &[], t.recv_ms,
                    false, // G5-test-rig: D1 parity must run the FULL signal path
                    &[],   // disabled_cells: D1 parity must NOT apply production filter
                           // (parity gate validates the Capa-B-clean signal expansion)
                ).map(|o| o.decision);
                if dec_a.is_some() || dec_b.is_some() {
                    triggers += 1;
                    if dec_a == dec_b {
                        matches += 1;
                    } else {
                        mism += 1;
                        if first_mism.len() < 5 {
                            first_mism.push(format!("  {asset}@{t_s}: A={dec_a:?} B={dec_b:?}"));
                        }
                    }
                }
            }
        }
    }
    println!("[d1-parity] triggers={triggers} matches={matches} mismatches={mism}");
    if mism > 0 {
        for m in &first_mism {
            println!("{m}");
        }
        anyhow::bail!("D1 parity FAILED: {mism}/{triggers} decisions differ between direct path and loop seam");
    }
    println!("[d1-parity] PASS — process_kline routes the validated decision logic identically ({matches}/{triggers}).");
    Ok(())
}

fn dir_to_outcome(d: Direction) -> Outcome {
    match d {
        Direction::Up => Outcome::Up,
        Direction::Down => Outcome::Down,
    }
}
fn dir_str(d: Direction) -> &'static str {
    d.as_str()
}
fn side_str(o: Outcome) -> &'static str {
    match o {
        Outcome::Up => "Up",
        Outcome::Down => "Down",
    }
}
fn f64_dec(d: rust_decimal::Decimal) -> f64 {
    use std::str::FromStr;
    f64::from_str(&d.to_string()).unwrap_or(0.0)
}

#[allow(clippy::too_many_arguments)]
fn trade_json(kind: &str, asset: &str, trigger_ts: i64, interval: &str, token: &str, side: &str, shares: f64, price: f64) -> String {
    serde_json::to_string(&TradeOut { kind, asset, trigger_ts, interval, token, side, shares, price }).unwrap()
}

fn load_klines(path: &Path, asset: &'static str, sh: u32, eh: u32, out: &mut Vec<Timed>) -> Result<()> {
    let f = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    for line in BufReader::new(f).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        match window(&line, sh, eh) {
            Win::Skip => continue,
            Win::Stop => break,
            Win::Take => {}
        }
        let v: Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let Some((recv_ms, _hour)) = recv_ms(&v) else { continue };
        let k = &v["payload"]["data"]["k"];
        if k.get("x").and_then(Value::as_bool) != Some(true) {
            continue;
        }
        let t_open_ms = match k.get("t").and_then(value_i64) {
            Some(t) => t,
            None => continue,
        };
        let close = match k.get("c").and_then(Value::as_str).and_then(|s| s.parse::<f64>().ok()) {
            Some(c) => c,
            None => continue,
        };
        out.push(Timed { recv_ms, ev: Ev::Kline { asset, t_open_ms, close } });
    }
    Ok(())
}

fn load_pm(path: &Path, etype: &str, tokens: &std::collections::HashSet<String>, sh: u32, eh: u32, out: &mut Vec<Timed>) -> Result<()> {
    let f = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    for line in BufReader::new(f).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        match window(&line, sh, eh) {
            Win::Skip => continue,
            Win::Stop => break,
            Win::Take => {}
        }
        let v: Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let Some((recv_ms, _hour)) = recv_ms(&v) else { continue };
        let p = &v["payload"];
        let ts = p.get("timestamp").and_then(value_i64).unwrap_or(recv_ms);
        match etype {
            "price_change" => {
                if let Some(arr) = p.get("price_changes").and_then(Value::as_array) {
                    for ch in arr {
                        let tok = ch.get("asset_id").and_then(Value::as_str).unwrap_or("");
                        if !tokens.contains(tok) {
                            continue;
                        }
                        let (ba, bb) = (pf(ch, "best_ask"), pf(ch, "best_bid"));
                        if ba.is_none() || bb.is_none() {
                            continue;
                        }
                        out.push(Timed { recv_ms, ev: Ev::Bbo { token: tok.to_string(), best_ask: ba, best_bid: bb, ts_ms: ts } });
                    }
                }
            }
            "best_bid_ask" => {
                let tok = p.get("asset_id").and_then(Value::as_str).unwrap_or("");
                if tokens.contains(tok) {
                    out.push(Timed { recv_ms, ev: Ev::Bbo { token: tok.to_string(), best_ask: pf(p, "best_ask"), best_bid: pf(p, "best_bid"), ts_ms: ts } });
                }
            }
            _ => {
                // book
                let tok = p.get("asset_id").and_then(Value::as_str).unwrap_or("");
                if tokens.contains(tok) {
                    let ba = last_level(p.get("asks"));
                    let bb = last_level(p.get("bids"));
                    out.push(Timed { recv_ms, ev: Ev::Bbo { token: tok.to_string(), best_ask: ba, best_bid: bb, ts_ms: ts } });
                }
            }
        }
    }
    Ok(())
}

fn pf(v: &Value, key: &str) -> Option<f64> {
    v.get(key).and_then(|x| x.as_f64().or_else(|| x.as_str().and_then(|s| s.parse().ok())))
}
fn last_level(v: Option<&Value>) -> Option<f64> {
    v.and_then(Value::as_array)
        .and_then(|a| a.last())
        .and_then(|l| pf(l, "price"))
}
fn value_i64(v: &Value) -> Option<i64> {
    v.as_i64().or_else(|| v.as_str().and_then(|s| s.parse().ok()))
}

/// Fast UTC-hour extraction from a recorder line's `received_at` prefix
/// (`{"received_at":"YYYY-MM-DDTHH:`), avoiding a full JSON parse for out-of-window
/// lines. None if the line doesn't match the expected prefix.
fn peek_hour(line: &str) -> Option<u32> {
    let b = line.as_bytes();
    if b.len() < 29 || &b[0..16] != b"{\"received_at\":\"" {
        return None;
    }
    line.get(27..29)?.parse().ok()
}

/// Window decision for a line: `Skip` (before window), `Stop` (past window — the
/// file is received_at-monotonic so we can break), or `Take`.
enum Win {
    Skip,
    Stop,
    Take,
}
fn window(line: &str, sh: u32, eh: u32) -> Win {
    match peek_hour(line) {
        Some(h) if h < sh => Win::Skip,
        Some(h) if h >= eh => Win::Stop,
        _ => Win::Take, // in-window, or unknown prefix → parse to be safe
    }
}

/// Parse a recorder `received_at` ("YYYY-MM-DDTHH:MM:SS.ffffff+00:00", UTC) to
/// (epoch_ms, hour). All recorder timestamps are +00:00.
fn recv_ms(v: &Value) -> Option<(i64, u32)> {
    let s = v.get("received_at").and_then(Value::as_str)?;
    let b = s.as_bytes();
    if b.len() < 19 {
        return None;
    }
    let num = |a: usize, z: usize| -> Option<i64> { s.get(a..z)?.parse().ok() };
    let (y, mo, d) = (num(0, 4)?, num(5, 7)?, num(8, 10)?);
    let (h, mi, se) = (num(11, 13)?, num(14, 16)?, num(17, 19)?);
    // fractional seconds (microseconds), if present after '.'
    let frac_ms = if b.len() > 20 && b[19] == b'.' {
        let end = (20..b.len()).find(|&i| !b[i].is_ascii_digit()).unwrap_or(b.len());
        let digits = &s[20..end];
        let micros: i64 = format!("{digits:0<6}")[..6].parse().unwrap_or(0);
        micros / 1000
    } else {
        0
    };
    let days = days_from_civil(y, mo, d);
    let ms = days * 86_400_000 + (h * 3600 + mi * 60 + se) * 1000 + frac_ms;
    Some((ms, h as u32))
}

/// Epoch SECONDS of `YYYY-MM-DD` at 00:00 UTC.
fn date_midnight_sec(date: &str) -> Option<i64> {
    if date.len() < 10 {
        return None;
    }
    let y: i64 = date.get(0..4)?.parse().ok()?;
    let m: i64 = date.get(5..7)?.parse().ok()?;
    let d: i64 = date.get(8..10)?.parse().ok()?;
    Some(days_from_civil(y, m, d) * 86_400)
}

/// Days since 1970-01-01 for a civil (y, m, d). Howard Hinnant's algorithm.
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = (if y >= 0 { y } else { y - 399 }) / 400;
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn recv_ms_parses_utc() {
        // 2026-05-28T00:00:00.000000+00:00 → known epoch ms; just check hour + monotonic.
        let v = json!({"received_at": "2026-05-28T03:06:44.851330+00:00"});
        let (ms, hour) = recv_ms(&v).unwrap();
        assert_eq!(hour, 3);
        let v2 = json!({"received_at": "2026-05-28T03:06:45.851330+00:00"});
        let (ms2, _) = recv_ms(&v2).unwrap();
        assert_eq!(ms2 - ms, 1000); // 1s apart
    }

    #[test]
    fn days_from_civil_epoch() {
        assert_eq!(days_from_civil(1970, 1, 1), 0);
        assert_eq!(days_from_civil(1970, 1, 2), 1);
        assert_eq!(days_from_civil(2000, 1, 1), 10957);
    }
}
