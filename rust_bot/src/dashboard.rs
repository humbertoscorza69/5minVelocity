//! Professional live dashboard — an in-process HTTP server (hand-rolled on tokio,
//! zero new deps) that serves a self-contained single-page UI + a `/api/stats`
//! JSON endpoint. Bound to 127.0.0.1 by default so it is reachable ONLY via an
//! SSH tunnel (never exposed publicly):
//!
//!   ssh -N -L 8787:127.0.0.1:8787 user@vps     # then open http://localhost:8787
//!
//! Metrics (realized/unrealized P&L, profit factor, win rate, trades, P&L curve,
//! open positions, recent trades, recal bias, feed health) are computed live from
//! the in-memory bot state + the oplog firehose. Read-only; never touches trading.

#![allow(dead_code)]

use std::sync::Arc;

use rust_decimal::prelude::ToPrimitive;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tracing::{info, warn};

use crate::state::SharedState;
use crate::state::persist::Outcome;
use crate::state::store::SharedBotState;

/// Spawnable dashboard task. `started_ms` is the wall clock at spawn (for uptime).
#[allow(clippy::too_many_arguments)]
pub async fn run_dashboard(
    state: Arc<SharedState>,
    store_state: SharedBotState,
    recal: crate::v2::RecalSet,
    controls: Arc<crate::v2::Controls>,
    controls_path: String,
    mode: String,
    live_armed_path: String,
    kill_switch_path: String,
    oplog_path: String,
    bind: String,
    port: u16,
    started_ms: i64,
    mut shutdown: watch::Receiver<bool>,
) {
    let listener = match TcpListener::bind((bind.as_str(), port)).await {
        Ok(l) => l,
        Err(e) => {
            warn!(error = %e, %bind, port, "dashboard: bind failed; dashboard disabled this run");
            return;
        }
    };
    info!(%bind, port, "task started: dashboard (http://{bind}:{port} — tunnel this port)");
    // Latch the wallet balance at the FIRST valid reading of this session so the
    // dashboard can reconcile realized P&L against the wallet: realized ==
    // walletΔ + in-flight (money booked at settlement but not yet redeemed into
    // free USDC, or cash currently locked in open buys). -1 = not yet latched.
    let mut session_start_bal_milli: i64 = -1;
    loop {
        tokio::select! {
            accept = listener.accept() => {
                let (mut sock, _peer) = match accept { Ok(x) => x, Err(_) => continue };
                let mut buf = [0u8; 2048];
                let n = sock.read(&mut buf).await.unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]);
                let path = req
                    .lines()
                    .next()
                    .and_then(|l| l.split_whitespace().nth(1))
                    .unwrap_or("/");
                let resp = if path.starts_with("/api/control") {
                    apply_control(&controls, path);
                    controls.save(&controls_path); // persist so it survives restarts
                    // Operator arming buttons → write/remove the gate files the
                    // guards already enforce. `arm` only has effect in --mode live
                    // (the live backend is only built then); `kill` halts the
                    // decision loop in any mode.
                    let q = path.split('?').nth(1).unwrap_or("");
                    set_flag_file(q, "arm", &live_armed_path, "armed\n");
                    set_flag_file(q, "kill", &kill_switch_path, "kill\n");
                    let body = json!({
                        "ok": true,
                        "enabled": controls.enabled(),
                        "base_usd": controls.base_usd(),
                        "max_pos": controls.max_pos_usd(),
                        "armed": std::path::Path::new(&live_armed_path).exists(),
                        "kill": std::path::Path::new(&kill_switch_path).exists(),
                    }).to_string();
                    info!(enabled = controls.enabled(), base_usd = controls.base_usd(),
                        max_pos = controls.max_pos_usd(),
                        armed = std::path::Path::new(&live_armed_path).exists(),
                        kill = std::path::Path::new(&kill_switch_path).exists(),
                        "dashboard: controls updated");
                    http_resp("200 OK", "application/json", body.as_bytes())
                } else if path.starts_with("/api/stats") {
                    // Latch the session-start wallet on the first valid reading.
                    let cur_bal = state.balance_milli.load(std::sync::atomic::Ordering::Relaxed);
                    if session_start_bal_milli < 0 && cur_bal >= 0 {
                        session_start_bal_milli = cur_bal;
                    }
                    let body = compute_stats(
                        &state, &store_state, &recal, &controls,
                        &mode, &live_armed_path, &kill_switch_path,
                        &oplog_path, started_ms, session_start_bal_milli,
                    ).to_string();
                    http_resp("200 OK", "application/json", body.as_bytes())
                } else if path == "/" || path.starts_with("/?") || path.starts_with("/index") {
                    http_resp("200 OK", "text/html; charset=utf-8", DASHBOARD_HTML.as_bytes())
                } else {
                    http_resp("404 Not Found", "text/plain", b"not found")
                };
                let _ = sock.write_all(&resp).await;
                let _ = sock.shutdown().await;
            }
            _ = shutdown.changed() => if *shutdown.borrow() { break },
        }
    }
    info!("dashboard: shutdown");
}

fn http_resp(status: &str, content_type: &str, body: &[u8]) -> Vec<u8> {
    let head = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\n\
         Cache-Control: no-store\r\nAccess-Control-Allow-Origin: *\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let mut out = head.into_bytes();
    out.extend_from_slice(body);
    out
}

fn outcome_str(o: Outcome) -> &'static str {
    match o {
        Outcome::Up => "Up",
        Outcome::Down => "Down",
    }
}

/// If `key` is present in the query, `true` → create the gate file (with
/// `content`), `false` → remove it. Absent key = no change. Backs the arm/kill
/// buttons via the SAME files the guards already enforce.
fn set_flag_file(query: &str, key: &str, path: &str, content: &str) {
    for kv in query.split('&') {
        let mut it = kv.splitn(2, '=');
        if it.next() == Some(key) {
            let v = it.next().unwrap_or("");
            if matches!(v, "1" | "true" | "on") {
                if let Some(dir) = std::path::Path::new(path).parent() {
                    if !dir.as_os_str().is_empty() {
                        let _ = std::fs::create_dir_all(dir);
                    }
                }
                let _ = std::fs::write(path, content);
            } else {
                let _ = std::fs::remove_file(path);
            }
        }
    }
}

/// Parse `?enabled=&base_usd=&max_pos=` from the request path and apply to the
/// live controls. Tolerant: unknown/garbage params are ignored. (Localhost-only
/// via the tunnel, so query-param control is acceptable here.)
fn apply_control(controls: &crate::v2::Controls, path: &str) {
    let q = path.split('?').nth(1).unwrap_or("");
    for kv in q.split('&') {
        let mut it = kv.splitn(2, '=');
        let k = it.next().unwrap_or("");
        let v = it.next().unwrap_or("");
        match k {
            "enabled" => controls.set_enabled(matches!(v, "1" | "true" | "on")),
            // Accept both "1.05" and "1,05" (locale-tolerant) before parsing.
            "base_usd" => if let Ok(x) = v.replace(',', ".").parse::<f64>() { controls.set_base_usd(x) },
            "max_pos" => if let Ok(x) = v.replace(',', ".").parse::<f64>() { controls.set_max_pos_usd(x) },
            "base_usd_15m" => if let Ok(x) = v.replace(',', ".").parse::<f64>() { controls.set_base_usd_15m(x) },
            "max_pos_15m" => if let Ok(x) = v.replace(',', ".").parse::<f64>() { controls.set_max_pos_15m(x) },
            "inval_stop" => controls.set_inval_stop_on(matches!(v, "1" | "true" | "on")),
            "inval_stop_dry" => controls.set_inval_stop_dry(matches!(v, "1" | "true" | "on")),
            _ => {}
        }
    }
}

/// Compute the full stats payload from live state + the oplog firehose.
#[allow(clippy::too_many_arguments)]
fn compute_stats(
    state: &SharedState,
    store_state: &SharedBotState,
    recal: &crate::v2::RecalSet,
    controls: &Arc<crate::v2::Controls>,
    mode: &str,
    live_armed_path: &str,
    kill_switch_path: &str,
    oplog_path: &str,
    started_ms: i64,
    session_start_bal_milli: i64,
) -> Value {
    let now = crate::state::now_ms();

    // ---- Open positions + unrealized P&L (mark to current best bid) ----
    let mut open_rows: Vec<Value> = Vec::new();
    let mut unreal_total = 0.0_f64;
    if let Ok(bs) = store_state.lock() {
        for p in &bs.positions {
            let entry = p.entry_price.to_f64().unwrap_or(0.0);
            let shares = p.shares.to_f64().unwrap_or(0.0);
            let bid = state.bbo.get(&p.token_id).and_then(|b| b.best_bid).unwrap_or(0.0);
            let unreal = shares * bid - shares * entry;
            unreal_total += unreal;
            open_rows.push(json!({
                "token": short_tok(&p.token_id),
                "asset": p.asset,
                "interval": p.interval,
                "side": outcome_str(p.side),
                "entry": entry,
                "usd": shares * entry,
                "shares": shares,
                "bid": bid,
                "unreal": unreal,
                "age_s": (now - p.opened_at_ms) / 1000,
            }));
        }
    }

    // ---- Entries/blocked from the oplog + realized events from BOTH the oplog
    //      (paper closes / REGLA C) AND the P&L recorder file (LIVE resolutions,
    //      written to data/live/pnl_recorded.jsonl — a SEPARATE file). Without the
    //      recorder, live wins (redeemed) never show as closed/realized. ----
    let mut entries = 0u64;
    let mut blocked = 0u64;
    // Health classification: total order rollbacks, DETERMINISTIC rejections (bugs
    // that will keep failing — precision/amount), and normal FOK kills (benign).
    let (mut rolled_back, mut det_errors, mut fok_kills) = (0u64, 0u64, 0u64);
    let mut asleep_null = 0u64; // intents where the asleep telemetry logged null (regression)
    let mut stops_fired = 0u64; // invalidation stops that actually SOLD (action=sell)
    // Trailing-window fill rate: each intent's signal_id in order + the set that
    // rolled back, so the alarm fires on a FRESH rejection storm within ~N intents
    // regardless of how long the session has been healthy (cumulative would dilute
    // for hours). Paired by signal_id (both events carry it).
    let mut intent_sids: Vec<String> = Vec::new();
    let mut rolled_sids: std::collections::HashSet<String> = std::collections::HashSet::new();
    // (ts_ms, net_pnl, token, side, entry, exit, interval)
    let mut rev: Vec<(i64, f64, String, String, Option<f64>, Option<f64>, String)> = Vec::new();
    // ACCOUNTING INVARIANT (Bug A net): every opened position must terminate in a
    // close OR a recorded resolution. `opened` = paper_open ∪ live_open_posted
    // (paper_open fires in BOTH modes; live_open_posted is the confirmed real
    // fill). `rolled` = live_open_rolled_back (phantoms that never landed —
    // excluded). `terminated` = any close/pnl-record. A token opened > GRACE ago,
    // not terminated and not rolled, is a LOSS/WIN that vanished from the books —
    // exactly the hole that showed +$9 on the dashboard while the wallet lost $27.
    let mut opened: std::collections::HashMap<String, i64> = std::collections::HashMap::new();
    let mut terminated: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut rolled_tok: std::collections::HashSet<String> = std::collections::HashSet::new();

    if let Ok(text) = std::fs::read_to_string(oplog_path) {
        for line in text.lines() {
            let v: Value = match serde_json::from_str(line) { Ok(v) => v, Err(_) => continue };
            let kind = v.get("kind").and_then(Value::as_str).unwrap_or("");
            let ts = v.get("ts_ms").and_then(Value::as_i64).unwrap_or(0);
            if ts < started_ms { continue; } // session-scope
            let sid = || v.get("data").and_then(|d| d.get("signal_id")).and_then(Value::as_str);
            match kind {
                "v2_intent_open" => {
                    entries += 1;
                    if let Some(s) = sid() { intent_sids.push(s.to_string()); }
                    // Telemetry self-check: asleep should populate on ~all intents.
                    if v.get("data").and_then(|d| d.get("asleep")).map(Value::is_null).unwrap_or(true) {
                        asleep_null += 1;
                    }
                }
                "v2_guard_blocked_open" => blocked += 1,
                "paper_open" | "live_open_posted" => {
                    if let Some(t) = v.get("data").and_then(|d| d.get("token_id")).and_then(Value::as_str) {
                        opened.entry(t.to_string()).or_insert(ts);
                    }
                }
                "paper_close" | "live_close_posted" | "pnl_recorder_recorded" => {
                    if let Some(t) = v.get("data").and_then(|d| d.get("token_id")).and_then(Value::as_str) {
                        terminated.insert(t.to_string());
                    }
                }
                "inval_stop" => {
                    if v.get("data").and_then(|d| d.get("action")).and_then(Value::as_str) == Some("sell") {
                        stops_fired += 1;
                    }
                }
                "live_open_rolled_back" => {
                    rolled_back += 1;
                    if let Some(s) = sid() { rolled_sids.insert(s.to_string()); }
                    if let Some(t) = v.get("data").and_then(|d| d.get("token_id")).and_then(Value::as_str) {
                        rolled_tok.insert(t.to_string());
                    }
                }
                "error" | "api_error" => {
                    let et = v.get("data").and_then(|d| d.get("error_text"))
                        .and_then(Value::as_str).unwrap_or("");
                    // Deterministic = same order fails forever (precision/amount) — a BUG.
                    if et.contains("accuracy") || et.contains("decimal") || et.contains("invalid amounts") {
                        det_errors += 1;
                    } else if et.contains("fully filled") {
                        fok_kills += 1; // benign: FOK couldn't fill at price
                    }
                }
                _ => {}
            }
            // NOTE: pnl_recorder_recorded / pnl_recorded_at_redeem are the recorder's
            // AUDIT mirror of rows it ALSO writes to pnl_recorded.jsonl (read below).
            // Counting them here too double-counts every resolution. Skip them; the
            // canonical realized P&L for resolutions comes from the file. The oplog
            // still supplies paper_close / exit closes (paper mode, not in the file).
            let data = v.get("data").cloned().unwrap_or(Value::Null);
            if !matches!(kind, "pnl_recorded_at_redeem" | "pnl_recorder_recorded") {
                if let Some(r) = data.get("realized_pnl").or_else(|| data.get("net_pnl")).or_else(|| data.get("pnl")).and_then(num) {
                    rev.push((ts, r,
                        short_tok(data.get("token_id").and_then(Value::as_str).unwrap_or("")),
                        data.get("side").and_then(Value::as_str).unwrap_or("").to_string(),
                        data.get("entry_price").and_then(num),
                        data.get("exit_price").and_then(num),
                        data.get("interval").and_then(Value::as_str).unwrap_or("5m").to_string()));
                }
            }
        }
    }
    // LIVE resolutions: PnlRecorder "recorded" rows carry net_pnl + resolved_price.
    if let Ok(text) = std::fs::read_to_string(crate::pnl_recorder::DEFAULT_PNL_RECORDED_LOG) {
        for line in text.lines() {
            let v: Value = match serde_json::from_str(line) { Ok(v) => v, Err(_) => continue };
            if v.get("kind").and_then(Value::as_str) != Some("recorded") { continue; }
            let ts = v.get("ts_ms").and_then(Value::as_i64).unwrap_or(0);
            if ts < started_ms { continue; }
            let np = v.get("net_pnl").and_then(num).unwrap_or(0.0);
            let rp = v.get("resolved_price").and_then(num);
            let side = match rp { Some(x) if x >= 0.5 => "win", Some(_) => "lose", None => "" };
            if let Some(t) = v.get("token_id").and_then(Value::as_str) { terminated.insert(t.to_string()); }
            rev.push((ts, np,
                short_tok(v.get("token_id").and_then(Value::as_str).unwrap_or("")),
                side.to_string(),
                v.get("entry_price").and_then(num),
                rp,
                v.get("interval").and_then(Value::as_str).unwrap_or("5m").to_string()));
        }
    }

    // Merge both sources by time, then aggregate — GLOBAL and PER-INTERVAL. Each
    // curve/trade point carries its interval so the UI can filter (All / 5m / 15m)
    // and recompute the cumulative P&L client-side from the per-trade deltas.
    #[derive(Default, Clone, Copy)]
    struct Agg { closed: u64, wins: u64, gross_win: f64, gross_loss: f64, realized: f64 }
    fn pf_value(gw: f64, gl: f64) -> Value {
        if gl > 0.0 { json!(gw / gl) } else if gw > 0.0 { json!("∞") } else { json!(0.0) }
    }
    fn stat_obj(a: &Agg) -> Value {
        let wr = if a.closed > 0 { a.wins as f64 / a.closed as f64 } else { 0.0 };
        json!({
            "closed": a.closed, "wins": a.wins, "losses": a.closed.saturating_sub(a.wins),
            "win_rate": wr, "profit_factor": pf_value(a.gross_win, a.gross_loss),
            "gross_win": a.gross_win, "gross_loss": a.gross_loss, "realized": a.realized,
        })
    }
    rev.sort_by_key(|e| e.0);
    let mut g = Agg::default();
    let mut per: std::collections::HashMap<String, Agg> = std::collections::HashMap::new();
    let mut curve: Vec<Value> = Vec::new();
    let mut recent_trades: Vec<Value> = Vec::new();
    for (ts, r, token, side, entry, exit, iv) in &rev {
        let a = per.entry(iv.clone()).or_default();
        g.closed += 1; a.closed += 1;
        g.realized += *r; a.realized += *r;
        if *r >= 0.0 {
            g.gross_win += *r; g.wins += 1; a.gross_win += *r; a.wins += 1;
        } else {
            g.gross_loss += -*r; a.gross_loss += -*r;
        }
        curve.push(json!({ "t": *ts, "d": *r, "iv": iv.as_str() }));
        recent_trades.push(json!({
            "ts": *ts, "token": token.as_str(), "side": side.as_str(),
            "entry": *entry, "exit": *exit, "pnl": *r, "iv": iv.as_str(),
        }));
    }
    let curve = tail(curve, 600);
    let mut recent_trades = tail(recent_trades, 60);
    recent_trades.reverse(); // newest first for the table

    // Global scalars (the "All" view + the top-line P&L cards).
    let closed = g.closed;
    let wins = g.wins;
    let gross_win = g.gross_win;
    let gross_loss = g.gross_loss;
    let realized_total = g.realized;
    let win_rate = if closed > 0 { wins as f64 / closed as f64 } else { 0.0 };
    let profit_factor: Value = pf_value(gross_win, gross_loss);
    let m5 = per.get("5m").copied().unwrap_or_default();
    let m15 = per.get("15m").copied().unwrap_or_default();
    let by_interval = json!({ "5m": stat_obj(&m5), "15m": stat_obj(&m15) });

    let (b5, n5) = recal.m5.lock().map(|r| (r.bias(), r.samples())).unwrap_or((0.0, 0));
    let (b15, n15) = recal.m15.lock().map(|r| (r.bias(), r.samples())).unwrap_or((0.0, 0));

    let bal_milli = state.balance_milli.load(std::sync::atomic::Ordering::Relaxed);
    let balance: Value = if bal_milli < 0 { Value::Null } else { json!(bal_milli as f64 / 1000.0) };
    // Reconciliation: realized P&L is booked at SETTLEMENT (dashboard, real-time),
    // but the wallet only moves when a winner is REDEEMED into free USDC (rate-
    // limited relayer) and is reduced by cash locked in open buys. So
    //   realized  ==  walletΔ (since session start)  +  in-flight
    // where in-flight = money booked-but-not-yet-in-free-cash. Both terms are
    // session-scoped (walletΔ latched at first valid reading ≈ started_ms), so
    // this reconciles on screen and → 0 once everything redeems with 0 open.
    let (wallet_delta, in_flight): (Value, Value) = if session_start_bal_milli >= 0 && bal_milli >= 0 {
        let d = (bal_milli - session_start_bal_milli) as f64 / 1000.0;
        (json!(d), json!(realized_total - d))
    } else {
        (Value::Null, Value::Null)
    };
    let wallet_start: Value = if session_start_bal_milli >= 0 {
        json!(session_start_bal_milli as f64 / 1000.0)
    } else { Value::Null };

    // ---- HEALTH REPORT: surface subtle failures the operator would otherwise miss.
    //      ALERT (red) = money is leaking or the bot is blind; WARN (amber) = degraded.
    use std::sync::atomic::Ordering::Relaxed;
    let posted = entries.saturating_sub(rolled_back);
    let fill_rate = if entries > 0 { posted as f64 / entries as f64 } else { 1.0 }; // cumulative (display)
    // TRAILING-WINDOW fill rate over the last N intents — the ALARM signal. A fresh
    // rejection storm trips this within ~N intents even after days of healthy fills
    // (cumulative would dilute for hours). Paired to outcomes by signal_id.
    const WIN: usize = 40;
    let recent = if intent_sids.len() > WIN { &intent_sids[intent_sids.len() - WIN..] } else { &intent_sids[..] };
    let win_n = recent.len();
    let win_failed = recent.iter().filter(|s| rolled_sids.contains(*s)).count();
    let fill_rate_win = if win_n > 0 { (win_n - win_failed) as f64 / win_n as f64 } else { 1.0 };
    let uptime_h = ((now - started_ms).max(1) as f64) / 3_600_000.0;
    let reconnects = state.counters.reconnects.load(Relaxed);
    let recon_per_hr = reconnects as f64 / uptime_h.max(0.05);
    let bn_up = state.binance_connected.load(Relaxed);
    let pm_up = state.polymarket_connected.load(Relaxed);
    // ACCOUNTING INVARIANT: opened positions past the settlement grace that never
    // terminated in a close/pnl-record and weren't rolled back = the P&L hole.
    // GRACE (20 min) > the 15m window + settlement/redeem lag, so still-settling
    // positions don't false-positive. count > 0 is a hard ALERT.
    const HOLE_GRACE_MS: i64 = 20 * 60 * 1000;
    let accounting_hole = opened
        .iter()
        .filter(|(tok, ts)| now - **ts > HOLE_GRACE_MS && !terminated.contains(*tok) && !rolled_tok.contains(*tok))
        .count();
    let mut alerts: Vec<String> = Vec::new();
    let mut warns: Vec<String> = Vec::new();
    if accounting_hole > 0 {
        alerts.push(format!("{accounting_hole} settled position(s) NEVER booked — P&L accounting hole (loss/win vanished from the books)"));
    }
    if !bn_up { alerts.push("Binance feed DOWN".into()); }
    if !pm_up { alerts.push("Polymarket feed DOWN".into()); }
    if !state.is_healthy() { alerts.push("feed stale / halted".into()); }
    if det_errors > 0 { alerts.push(format!("{det_errors} order rejections — DETERMINISTIC bug (orders won't fill)")); }
    if win_n >= 20 && fill_rate_win < 0.50 { alerts.push(format!("fill rate {:.0}% (last {win_n}) — most orders rejected", fill_rate_win * 100.0)); }
    if recon_per_hr > 20.0 { warns.push(format!("WS unstable: {:.0} reconnects/hr", recon_per_hr)); }
    if entries >= 20 && (asleep_null as f64 / entries as f64) > 0.30 { warns.push(format!("asleep telemetry null on {:.0}% of intents", 100.0 * asleep_null as f64 / entries as f64)); }
    if win_n >= 20 && (0.50..0.70).contains(&fill_rate_win) { warns.push(format!("fill rate {:.0}% (last {win_n})", fill_rate_win * 100.0)); }
    let status = if !alerts.is_empty() { "alert" } else if !warns.is_empty() { "warn" } else { "ok" };
    let issues: Vec<String> = alerts.into_iter().chain(warns).collect();
    let health_report = json!({
        "status": status, "issues": issues,
        "fill_rate": fill_rate_win, "fill_rate_cumulative": fill_rate,
        "window_n": win_n, "posted": posted, "intents_total": entries,
        "det_errors": det_errors, "fok_kills": fok_kills, "rolled_back": rolled_back,
        "reconnects": reconnects, "reconnects_per_hr": recon_per_hr,
        "accounting_hole": accounting_hole,
    });

    json!({
        "health_report": health_report,
        "now_ms": now,
        "uptime_s": (now - started_ms).max(0) / 1000,
        "balance": balance,
        "health": {
            "binance": state.binance_connected.load(std::sync::atomic::Ordering::Relaxed),
            "polymarket": state.polymarket_connected.load(std::sync::atomic::Ordering::Relaxed),
            "healthy": state.is_healthy(),
            "active_tokens": state.active_tokens.load(std::sync::atomic::Ordering::Relaxed),
            "decisions": state.counters.decisions.load(std::sync::atomic::Ordering::Relaxed),
            "bn_klines": state.counters.binance_klines.load(std::sync::atomic::Ordering::Relaxed),
            "pm_msgs": state.counters.polymarket_msgs.load(std::sync::atomic::Ordering::Relaxed),
        },
        "pnl": {
            "realized": realized_total,
            "unrealized": unreal_total,
            "total": realized_total + unreal_total,
            "wallet_start": wallet_start,
            "wallet_delta": wallet_delta,
            "in_flight": in_flight,
        },
        "stats": {
            "entries": entries,
            "closed": closed,
            "open": open_rows.len(),
            "wins": wins,
            "losses": closed.saturating_sub(wins),
            "win_rate": win_rate,
            "profit_factor": profit_factor,
            "gross_win": gross_win,
            "gross_loss": gross_loss,
            "blocked": blocked,
            "stops_fired": stops_fired,
        },
        "by_interval": by_interval,
        "recal": {
            "bias": b5, "samples": n5,
            "m5": { "bias": b5, "samples": n5 },
            "m15": { "bias": b15, "samples": n15 },
        },
        "controls": {
            "enabled": controls.enabled(),
            "base_usd": controls.base_usd(),
            "max_pos": controls.max_pos_usd(),
            "base_usd_15m": controls.base_usd_15m(),
            "max_pos_15m": controls.max_pos_15m(),
            "inval_stop": controls.inval_stop_on(),
            "inval_stop_dry": controls.inval_stop_dry(),
        },
        "live": {
            "mode": mode,
            "armed": std::path::Path::new(live_armed_path).exists(),
            "kill": std::path::Path::new(kill_switch_path).exists(),
        },
        "open_positions": open_rows,
        "recent_trades": recent_trades,
        "curve": curve,
    })
}

fn num(v: &Value) -> Option<f64> {
    v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
}

fn tail(mut v: Vec<Value>, n: usize) -> Vec<Value> {
    if v.len() > n {
        v.split_off(v.len() - n)
    } else {
        std::mem::take(&mut v)
    }
}

fn short_tok(t: &str) -> String {
    if t.len() <= 10 {
        t.to_string()
    } else {
        format!("…{}", &t[t.len() - 8..])
    }
}

/// Self-contained dashboard page (no external CDN — works through the tunnel even
/// if the browser blocks third-party hosts). Vanilla JS polls /api/stats; the P&L
/// curve is drawn on a canvas.
const DASHBOARD_HTML: &str = r##"<!doctype html><html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>v2 bot — live</title>
<style>
:root{--bg:#0b0e14;--panel:#141925;--panel2:#1b2230;--line:#283041;--txt:#e6edf3;--mut:#8b98a9;--grn:#3fb950;--red:#f85149;--acc:#58a6ff;--amb:#d29922}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--txt);font:14px/1.4 -apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif}
header{display:flex;align-items:center;gap:14px;padding:14px 20px;border-bottom:1px solid var(--line);background:var(--panel)}
header h1{font-size:16px;margin:0;letter-spacing:.3px}
.pill{font-size:11px;padding:3px 9px;border-radius:999px;border:1px solid var(--line);color:var(--mut)}
.pill.ok{color:var(--grn);border-color:#1c3a25}.pill.bad{color:var(--red);border-color:#3a1c1c}
.wrap{padding:18px 20px;max-width:1280px;margin:0 auto}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px;margin-bottom:16px}
.card{background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:14px 16px}
.card .lbl{font-size:11px;color:var(--mut);text-transform:uppercase;letter-spacing:.6px}
.card .val{font-size:24px;font-weight:650;margin-top:6px}
.card .sub{font-size:11px;color:var(--mut);margin-top:3px}
.pos{color:var(--grn)}.neg{color:var(--red)}.acc{color:var(--acc)}
.panel{background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:14px 16px;margin-bottom:16px}
.panel h2{font-size:12px;color:var(--mut);text-transform:uppercase;letter-spacing:.6px;margin:0 0 10px}
canvas{width:100%;height:260px;display:block}
table{width:100%;border-collapse:collapse;font-size:13px}
th,td{text-align:right;padding:7px 10px;border-bottom:1px solid var(--line)}
th:first-child,td:first-child{text-align:left}
th{color:var(--mut);font-weight:500;font-size:11px;text-transform:uppercase;letter-spacing:.4px}
tbody tr:hover{background:var(--panel2)}
.muted{color:var(--mut)}.mono{font-variant-numeric:tabular-nums}
.foot{color:var(--mut);font-size:11px;margin-top:10px;text-align:right}
.ctlrow{display:flex;align-items:center;gap:14px;flex-wrap:wrap}
.btn{cursor:pointer;border:1px solid var(--line);background:var(--panel2);color:var(--txt);padding:9px 18px;border-radius:8px;font-size:13px;font-weight:650}
.btn:hover{filter:brightness(1.15)}
.btn.on{color:var(--grn);border-color:#1c3a25;background:#0f2417}
.btn.off{color:var(--red);border-color:#3a1c1c;background:#241010}
.btn.alt{background:var(--acc);color:#06121f;border-color:var(--acc)}
.btn.armed{color:#fff;background:var(--red);border-color:var(--red)}
.btn.kill{color:var(--amb);border-color:#3a2c10;background:#1f1a0a}
.btn.killon{color:#fff;background:var(--red);border-color:var(--red)}
#armpanel{border-color:#3a2c10}
.ctlrow label{font-size:12px;color:var(--mut);display:flex;align-items:center;gap:6px}
.ctlrow input{width:92px;background:var(--bg);border:1px solid var(--line);color:var(--txt);border-radius:6px;padding:7px 9px;font-size:13px;font-variant-numeric:tabular-nums}
.btn.seg{padding:6px 15px;font-size:12px;font-weight:600}
.btn.seg.sel{color:var(--acc);border-color:var(--acc);background:#0d1b2e}
.healthbar{border-radius:10px;padding:11px 15px;margin-bottom:14px;font-weight:650;font-size:13px;letter-spacing:.2px}
.healthbar.ok{background:#0f2417;border:1px solid #1c3a25;color:var(--grn)}
.healthbar.warn{background:#241f0a;border:1px solid #3a2c10;color:var(--amb)}
.healthbar.alert{background:#2a0f12;border:1px solid var(--red);color:var(--red);animation:pulse 1.4s ease-in-out infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.62}}
</style></head><body>
<header>
  <h1>⚡ v2 bot <span class="muted" style="font-weight:400">— Polymarket lag-arb (5minSnip)</span></h1>
  <span id="mode" class="pill">paper</span>
  <span id="bn" class="pill">Binance</span>
  <span id="pm" class="pill">Polymarket</span>
  <span style="flex:1"></span>
  <span id="up" class="pill"></span>
</header>
<div class="wrap">
  <div id="healthbar" class="healthbar ok">● checking…</div>
  <div class="panel"><h2>Controls</h2>
    <div class="ctlrow">
      <button id="toggle" class="btn">—</button>
      <button id="stopbtn" class="btn" title="Cycle: OFF → DRY-RUN (log only) → LIVE (sells 5m)">—</button>
      <label>5m base $ <input id="in_base" type="number" step="0.05" min="0.1"></label>
      <label>5m max $ <input id="in_max" type="number" step="1" min="0.1"></label>
      <label>15m base $ <input id="in_base15" type="number" step="0.05" min="0.1"></label>
      <label>15m max $ <input id="in_max15" type="number" step="1" min="0.1"></label>
      <button id="apply" class="btn alt">Apply</button>
      <span id="ctl_msg" class="muted"></span>
      <span style="flex:1"></span>
      <span class="muted" style="font-size:11px">edge-proportional sizing scales around “Stake base”, capped by “Max position” &amp; book depth</span>
    </div>
  </div>
  <div class="panel" id="armpanel"><h2>Live arming <span id="modebadge" class="pill">—</span></h2>
    <div class="ctlrow">
      <button id="arm" class="btn">—</button>
      <button id="kill" class="btn kill">KILL SWITCH</button>
      <span id="arm_msg" class="muted"></span>
      <span style="flex:1"></span>
      <span class="muted" style="font-size:11px">ARM writes LIVE_ARMED.txt — real orders post ONLY in --mode live. KILL halts the decision loop (any mode).</span>
    </div>
  </div>
  <div class="ctlrow" style="margin-bottom:12px">
    <span class="muted" style="font-size:11px;text-transform:uppercase;letter-spacing:.6px">Strategy</span>
    <button class="btn seg sel" data-f="all">All</button>
    <button class="btn seg" data-f="5m">5m</button>
    <button class="btn seg" data-f="15m">15m</button>
    <span id="filt_note" class="muted" style="font-size:11px"></span>
  </div>
  <div class="grid">
    <div class="card"><div class="lbl">Wallet balance</div><div class="val mono acc" id="balance">—</div><div class="sub" id="balance_sub">USDC (funder wallet)</div></div>
    <div class="card"><div class="lbl">Total P&L</div><div class="val mono" id="pnl_total">—</div><div class="sub" id="pnl_split"></div></div>
    <div class="card"><div class="lbl">Realized</div><div class="val mono" id="pnl_real">—</div></div>
    <div class="card"><div class="lbl">Unrealized</div><div class="val mono" id="pnl_unreal">—</div></div>
    <div class="card"><div class="lbl">Profit Factor</div><div class="val mono" id="pf">—</div><div class="sub" id="pf_sub"></div></div>
    <div class="card"><div class="lbl">Settled WR</div><div class="val mono" id="wr">—</div><div class="sub" id="wr_sub"></div></div>
    <div class="card"><div class="lbl">Trades</div><div class="val mono" id="trades">—</div><div class="sub" id="trades_sub"></div></div>
    <div class="card"><div class="lbl">Recal bias</div><div class="val mono" id="recal">—</div><div class="sub" id="recal_sub"></div></div>
  </div>
  <div class="panel"><h2>Cumulative realized P&L</h2><canvas id="chart"></canvas></div>
  <div class="panel"><h2>Open positions (<span id="open_n">0</span>)</h2>
    <table><thead><tr><th>Token</th><th>Side</th><th>Iv</th><th>Entry</th><th>USD</th><th>Bid</th><th>Shares</th><th>Unreal</th><th>Age</th></tr></thead>
    <tbody id="open_body"></tbody></table></div>
  <div class="panel"><h2>Recent trades</h2>
    <table><thead><tr><th>Time</th><th>Token</th><th>Side</th><th>Iv</th><th>Entry</th><th>Exit</th><th>P&L</th></tr></thead>
    <tbody id="trades_body"></tbody></table></div>
  <div class="foot" id="foot"></div>
</div>
<script>
const $=id=>document.getElementById(id);
const money=x=>(x>=0?"+$":"-$")+Math.abs(x).toFixed(2);
const cls=x=>x>=0?"pos":"neg";
function fmtAge(s){if(s<60)return s+"s";if(s<3600)return Math.floor(s/60)+"m";return Math.floor(s/3600)+"h"}
function fmtTime(ms){const d=new Date(ms);return d.toLocaleTimeString()}
function pill(el,ok,label){el.textContent=label;el.className="pill "+(ok?"ok":"bad")}
let FILT="all",LAST=null;
function drawChart(curve){
  const c=$("chart"),dpr=window.devicePixelRatio||1;
  const w=c.clientWidth,h=c.clientHeight;c.width=w*dpr;c.height=h*dpr;
  const x=c.getContext("2d");x.scale(dpr,dpr);x.clearRect(0,0,w,h);
  if(!curve.length){x.fillStyle="#8b98a9";x.font="13px sans-serif";x.fillText("no closed trades yet",16,28);return}
  const ys=curve.map(p=>p.pnl);
  let mn=Math.min(0,...ys),mx=Math.max(0,...ys);if(mn===mx){mx+=1;mn-=1}
  const n=curve.length,pad=34;
  // X by trade SEQUENCE, not timestamp: settlements booked in the same tick (e.g.
  // a backlog recovered in one pass) share a ts to the millisecond, which would
  // collapse a time-axis to a single vertical line. Index spacing is immune.
  const X=i=>pad+(w-pad-10)*(i/((n-1)||1));
  const Y=v=>10+(h-20-10)*(1-((v-mn)/((mx-mn)||1)));
  // zero line
  x.strokeStyle="#283041";x.lineWidth=1;x.beginPath();x.moveTo(pad,Y(0));x.lineTo(w-10,Y(0));x.stroke();
  x.fillStyle="#8b98a9";x.font="10px sans-serif";x.fillText("$"+mx.toFixed(0),4,Y(mx)+4);x.fillText("$"+mn.toFixed(0),4,Y(mn)+4);
  // area + line
  const last=ys[ys.length-1];const col=last>=0?"#3fb950":"#f85149";
  x.beginPath();curve.forEach((p,i)=>{const px=X(i),py=Y(p.pnl);i?x.lineTo(px,py):x.moveTo(px,py)});
  x.lineTo(X(n-1),Y(0));x.lineTo(X(0),Y(0));x.closePath();x.fillStyle=col+"22";x.fill();
  x.beginPath();curve.forEach((p,i)=>{const px=X(i),py=Y(p.pnl);i?x.lineTo(px,py):x.moveTo(px,py)});
  x.strokeStyle=col;x.lineWidth=2;x.stroke();
}
function notify(title,body){try{if(!("Notification"in window))return;if(Notification.permission==="granted"){new Notification(title,{body})}else if(Notification.permission!=="denied"){Notification.requestPermission().then(p=>{if(p==="granted")new Notification(title,{body})})}}catch(e){}}
function renderHealth(h){
  const hb=$("healthbar");if(!h){return}
  hb.className="healthbar "+h.status;
  if(h.status==="ok"){
    hb.textContent="● HEALTHY — fills "+(h.fill_rate*100).toFixed(0)+"% (last "+h.window_n+") · "+(h.fill_rate_cumulative*100).toFixed(0)+"% overall · "+h.reconnects+" reconnects · "+h.fok_kills+" FOK kills · 0 order bugs";
    document.title="v2 bot — live";
  }else{
    const ic=h.status==="alert"?"⛔ ALERT":"▲ WARN";
    hb.textContent=ic+" — "+h.issues.join("     ·     ");
    document.title=(h.status==="alert"?"⛔ ":"▲ ")+"v2 bot — "+(h.issues[0]||"");
  }
  if(h.status==="alert"&&window.__lastHealth!=="alert")notify("⛔ Bot ALERT",h.issues.join(" · "));
  window.__lastHealth=h.status;
}
async function tick(){
  let s;try{s=await(await fetch("/api/stats",{cache:"no-store"})).json()}catch(e){$("foot").textContent="disconnected — retrying…";$("healthbar").className="healthbar alert";$("healthbar").textContent="⛔ ALERT — dashboard can't reach the bot (process down?)";document.title="⛔ v2 bot — DOWN";return}
  renderHealth(s.health_report);
  pill($("bn"),s.health.binance,"Binance "+(s.health.binance?"●":"○"));
  pill($("pm"),s.health.polymarket,"Polymarket "+(s.health.polymarket?"●":"○"));
  $("up").textContent="uptime "+fmtAge(s.uptime_s)+" · "+s.health.active_tokens+" mkts · "+s.health.decisions+" dec";
  $("balance").textContent=(s.balance==null)?"—":"$"+(+s.balance).toFixed(2);
  // Reconciliation sub-line: realized == walletΔ (this session) + in-flight
  // (booked at settlement but not yet redeemed into free USDC / locked in open
  // buys). Explains why realized P&L can lead the wallet with 0 open positions.
  (function(){const p=s.pnl||{};const b=$("balance_sub");
    if(p.wallet_delta==null){b.textContent="USDC (funder wallet)";return;}
    const d=+p.wallet_delta, f=(p.in_flight==null?0:+p.in_flight);
    b.innerHTML="Δsession "+money(d)+" · <span title=\"booked at settlement, not yet redeemed into free USDC or locked in open buys — converges to $0 once all winners redeem\">awaiting redeem "+money(f)+"</span>";
  })();
  LAST=s;render();
  const c=s.controls;const tg=$("toggle");
  tg.textContent=c.enabled?"● TRADING ON":"○ TRADING OFF";tg.className="btn "+(c.enabled?"on":"off");
  if(document.activeElement!==$("in_base"))$("in_base").value=(+c.base_usd).toFixed(2);
  if(document.activeElement!==$("in_max"))$("in_max").value=(+c.max_pos).toFixed(2);
  if(document.activeElement!==$("in_base15"))$("in_base15").value=(+c.base_usd_15m).toFixed(2);
  if(document.activeElement!==$("in_max15"))$("in_max15").value=(+c.max_pos_15m).toFixed(2);
  const sb=$("stopbtn");
  if(!c.inval_stop){sb.textContent="STOP: OFF";sb.className="btn off";}
  else if(c.inval_stop_dry){sb.textContent="STOP: DRY-RUN";sb.className="btn kill";}
  else{sb.textContent="STOP: LIVE ●";sb.className="btn on";}
  const lv=s.live;
  $("modebadge").textContent=lv.mode.toUpperCase();$("modebadge").className="pill "+(lv.mode==="live"?"bad":"ok");
  $("mode").textContent=lv.mode.toUpperCase();$("mode").className="pill "+(lv.mode==="live"?"bad":"ok");
  const ab=$("arm");ab.textContent=lv.armed?"● ARMED — click to DISARM":"○ DISARMED — click to ARM";ab.className="btn "+(lv.armed?"armed":"");
  const kb=$("kill");kb.textContent=lv.kill?"● KILL ACTIVE — click to CLEAR":"KILL SWITCH";kb.className="btn "+(lv.kill?"killon":"kill");
  $("foot").textContent="updated "+new Date(s.now_ms).toLocaleTimeString();
}
// Render the FILTER-dependent widgets (P&L cards, PF, win rate, trades, recal,
// open positions, trades table, chart) for the selected strategy (all/5m/15m).
function render(){
  const s=LAST;if(!s)return;
  const st=(FILT==="all")?s.stats:(s.by_interval[FILT]||{closed:0,wins:0,losses:0,win_rate:0,profit_factor:0,gross_win:0,gross_loss:0,realized:0});
  const realized=(FILT==="all")?s.pnl.realized:st.realized;
  const opens=(FILT==="all")?s.open_positions:s.open_positions.filter(p=>p.interval===FILT);
  const unreal=opens.reduce((a,p)=>a+(+p.unreal),0);
  const total=realized+unreal;
  const rc=(FILT==="15m")?s.recal.m15:s.recal.m5;
  $("filt_note").textContent=(FILT==="all")?"combined 5m + 15m":(FILT+" only");
  const t=$("pnl_total");t.textContent=money(total);t.className="val mono "+cls(total);
  $("pnl_split").textContent="real "+money(realized)+" · unrl "+money(unreal);
  const r=$("pnl_real");r.textContent=money(realized);r.className="val mono "+cls(realized);
  const u=$("pnl_unreal");u.textContent=money(unreal);u.className="val mono "+cls(unreal);
  $("pf").textContent=(typeof st.profit_factor==="number")?st.profit_factor.toFixed(2):st.profit_factor;
  $("pf_sub").textContent="W $"+st.gross_win.toFixed(0)+" / L $"+st.gross_loss.toFixed(0);
  $("wr").textContent=(st.win_rate*100).toFixed(1)+"%";
  // WR is a COMPOSITION stat now: the invalidation stop clips winners by design,
  // so a settled WR of 45-52% is expected, not a warning. Show the stop rate
  // alongside so the number reads in context (all-view; stops_fired is global).
  const sf=(FILT==="all")?(s.stats.stops_fired||0):0;
  const sr=(FILT==="all"&&st.closed>0&&sf>0)?" · "+sf+" stops ("+Math.round(100*sf/st.closed)+"% of exits)":"";
  $("wr_sub").textContent=st.wins+"W / "+st.losses+"L"+sr;
  // Headline = REAL filled trades (positions that actually opened = open+closed),
  // NOT s.stats.entries (which counts every v2_intent_open, incl. FOK kills /
  // rolled-back orders that never became a position). Intents shown in the sub.
  $("trades").textContent=(FILT==="all")?(s.stats.open+s.stats.closed):st.closed;
  $("trades_sub").textContent=(FILT==="all")?(s.stats.open+" open · "+s.stats.closed+" closed · "+s.stats.entries+" intents · "+s.stats.blocked+" blocked"):(opens.length+" open · "+st.closed+" closed");
  $("recal").textContent=(rc.bias>=0?"+":"")+rc.bias.toFixed(3);
  $("recal_sub").textContent=rc.samples+" samples ("+(FILT==="15m"?"15m":"5m")+")";
  $("open_n").textContent=opens.length;
  $("open_body").innerHTML=opens.map(p=>`<tr><td class="mono">${p.token}</td><td>${p.side}</td><td class="muted">${p.asset}/${p.interval}</td><td class="mono">${p.entry.toFixed(3)}</td><td class="mono">$${(+p.usd).toFixed(2)}</td><td class="mono">${p.bid.toFixed(3)}</td><td class="mono">${p.shares.toFixed(1)}</td><td class="mono ${cls(p.unreal)}">${money(p.unreal)}</td><td class="muted mono">${fmtAge(p.age_s)}</td></tr>`).join("")||`<tr><td colspan=9 class=muted>none</td></tr>`;
  const trs=(FILT==="all")?s.recent_trades:s.recent_trades.filter(x=>x.iv===FILT);
  $("trades_body").innerHTML=trs.map(t=>`<tr><td class="muted mono">${fmtTime(t.ts)}</td><td class="mono">${t.token}</td><td>${t.side||""}</td><td class="muted">${t.iv||"5m"}</td><td class="mono">${t.entry!=null?(+t.entry).toFixed(3):"—"}</td><td class="mono">${t.exit!=null?(+t.exit).toFixed(3):"—"}</td><td class="mono ${cls(t.pnl)}">${money(t.pnl)}</td></tr>`).join("")||`<tr><td colspan=7 class=muted>no closed trades yet</td></tr>`;
  // Cumulative curve from per-trade deltas, filtered by strategy.
  const pts=(FILT==="all")?s.curve:s.curve.filter(p=>p.iv===FILT);
  let run=0;drawChart(pts.map(p=>({t:p.t,pnl:(run+=p.d)})));
}
document.querySelectorAll(".btn.seg").forEach(b=>b.onclick=()=>{
  FILT=b.getAttribute("data-f");
  document.querySelectorAll(".btn.seg").forEach(x=>x.classList.toggle("sel",x===b));
  render();
});
$("toggle").onclick=async()=>{const on=$("toggle").classList.contains("on");try{await fetch("/api/control?enabled="+(on?"false":"true"),{method:"POST"})}catch(e){}tick()};
$("apply").onclick=async()=>{const b=$("in_base").value,m=$("in_max").value,b15=$("in_base15").value,m15=$("in_max15").value;try{await fetch(`/api/control?base_usd=${encodeURIComponent(b)}&max_pos=${encodeURIComponent(m)}&base_usd_15m=${encodeURIComponent(b15)}&max_pos_15m=${encodeURIComponent(m15)}`,{method:"POST"})}catch(e){}$("ctl_msg").textContent="applied ✓";setTimeout(()=>$("ctl_msg").textContent="",1800);tick()};
// Invalidation stop: cycle OFF -> DRY-RUN -> LIVE -> OFF. LIVE prompts (it sells).
$("stopbtn").onclick=async()=>{const t=$("stopbtn").textContent;let q;
  if(t.includes("OFF"))q="inval_stop=true&inval_stop_dry=true";        // -> DRY
  else if(t.includes("DRY")){if(!confirm("Arm the invalidation stop LIVE? It will SELL 5m positions at the bid when the signal invalidates. Win rate will drop to ~52% by design."))return;q="inval_stop=true&inval_stop_dry=false";} // -> LIVE
  else q="inval_stop=false";                                          // -> OFF
  try{await fetch("/api/control?"+q,{method:"POST"})}catch(e){}tick()};
$("arm").onclick=async()=>{const armed=$("arm").classList.contains("armed");
  if(!armed && !confirm("ARM live trading?\n\nReal orders will post when the bot is in --mode live and a signal fires. Make sure your stake is set correctly first.")) return;
  try{await fetch("/api/control?arm="+(armed?"false":"true"),{method:"POST"})}catch(e){}
  $("arm_msg").textContent=armed?"disarmed":"ARMED";setTimeout(()=>$("arm_msg").textContent="",1800);tick()};
$("kill").onclick=async()=>{const on=$("kill").classList.contains("killon");
  if(!on && !confirm("Activate KILL SWITCH?\n\nThis halts the decision loop immediately — no new decisions or entries (any mode).")) return;
  try{await fetch("/api/control?kill="+(on?"false":"true"),{method:"POST"})}catch(e){}tick()};
tick();setInterval(tick,3000);window.addEventListener("resize",()=>{});
</script></body></html>"##;
