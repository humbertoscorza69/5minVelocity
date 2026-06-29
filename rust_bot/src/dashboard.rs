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

use std::sync::{Arc, Mutex};

use rust_decimal::prelude::ToPrimitive;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tracing::{info, warn};

use crate::state::SharedState;
use crate::state::persist::Outcome;
use crate::state::store::SharedBotState;
use crate::v2::Recalibrator;

/// Spawnable dashboard task. `started_ms` is the wall clock at spawn (for uptime).
#[allow(clippy::too_many_arguments)]
pub async fn run_dashboard(
    state: Arc<SharedState>,
    store_state: SharedBotState,
    recal: Arc<Mutex<Recalibrator>>,
    controls: Arc<crate::v2::Controls>,
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
                    let body = compute_stats(
                        &state, &store_state, &recal, &controls,
                        &mode, &live_armed_path, &kill_switch_path,
                        &oplog_path, started_ms,
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
            _ => {}
        }
    }
}

/// Compute the full stats payload from live state + the oplog firehose.
#[allow(clippy::too_many_arguments)]
fn compute_stats(
    state: &SharedState,
    store_state: &SharedBotState,
    recal: &Arc<Mutex<Recalibrator>>,
    controls: &Arc<crate::v2::Controls>,
    mode: &str,
    live_armed_path: &str,
    kill_switch_path: &str,
    oplog_path: &str,
    started_ms: i64,
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

    // ---- Walk the oplog: entries (v2_intent_open) + closed trades (any
    //      data.realized_pnl / net_pnl) → realized P&L, PF, win rate, curve. ----
    let mut entries = 0u64;
    let mut closed = 0u64;
    let mut wins = 0u64;
    let mut gross_win = 0.0_f64;
    let mut gross_loss = 0.0_f64;
    let mut realized_total = 0.0_f64;
    let mut curve: Vec<Value> = Vec::new();
    let mut recent_trades: Vec<Value> = Vec::new();
    let mut blocked = 0u64;

    if let Ok(text) = std::fs::read_to_string(oplog_path) {
        for line in text.lines() {
            let v: Value = match serde_json::from_str(line) {
                Ok(v) => v,
                Err(_) => continue,
            };
            let kind = v.get("kind").and_then(Value::as_str).unwrap_or("");
            let ts = v.get("ts_ms").and_then(Value::as_i64).unwrap_or(0);
            // Session-scope: only count events from THIS run (the oplog is an
            // append-only firehose that also holds prior paper sessions). Keeps
            // the dashboard clean after a paper→live restart without deleting
            // the audit log.
            if ts < started_ms {
                continue;
            }
            let data = v.get("data").cloned().unwrap_or(Value::Null);
            match kind {
                "v2_intent_open" => entries += 1,
                "v2_guard_blocked_open" => blocked += 1,
                _ => {}
            }
            // Any event carrying a realized P&L is a closed trade (paper_close,
            // exit_close, pnl resolution, …) — generic so we don't couple to one name.
            let r = data
                .get("realized_pnl")
                .or_else(|| data.get("net_pnl"))
                .or_else(|| data.get("pnl"))
                .and_then(num);
            if let Some(r) = r {
                closed += 1;
                realized_total += r;
                if r >= 0.0 {
                    gross_win += r;
                    wins += 1;
                } else {
                    gross_loss += -r;
                }
                curve.push(json!({ "t": ts, "pnl": realized_total }));
                recent_trades.push(json!({
                    "ts": ts,
                    "token": short_tok(data.get("token_id").and_then(Value::as_str).unwrap_or("")),
                    "side": data.get("side").and_then(Value::as_str).unwrap_or(""),
                    "entry": data.get("entry_price").and_then(num),
                    "exit": data.get("exit_price").and_then(num),
                    "pnl": r,
                }));
            }
        }
    }

    // Cap arrays for the wire (newest kept).
    let curve = tail(curve, 600);
    let mut recent_trades = tail(recent_trades, 60);
    recent_trades.reverse(); // newest first for the table

    let win_rate = if closed > 0 { wins as f64 / closed as f64 } else { 0.0 };
    let profit_factor: Value = if gross_loss > 0.0 {
        json!(gross_win / gross_loss)
    } else if gross_win > 0.0 {
        json!("∞")
    } else {
        json!(0.0)
    };

    let (recal_bias, recal_samples) = recal
        .lock()
        .map(|r| (r.bias(), r.samples()))
        .unwrap_or((0.0, 0));

    json!({
        "now_ms": now,
        "uptime_s": (now - started_ms).max(0) / 1000,
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
        },
        "recal": { "bias": recal_bias, "samples": recal_samples },
        "controls": {
            "enabled": controls.enabled(),
            "base_usd": controls.base_usd(),
            "max_pos": controls.max_pos_usd(),
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
  <div class="panel"><h2>Controls</h2>
    <div class="ctlrow">
      <button id="toggle" class="btn">—</button>
      <label>Stake base $ <input id="in_base" type="number" step="0.05" min="0.1"></label>
      <label>Max position $ <input id="in_max" type="number" step="1" min="0.1"></label>
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
  <div class="grid">
    <div class="card"><div class="lbl">Total P&L</div><div class="val mono" id="pnl_total">—</div><div class="sub" id="pnl_split"></div></div>
    <div class="card"><div class="lbl">Realized</div><div class="val mono" id="pnl_real">—</div></div>
    <div class="card"><div class="lbl">Unrealized</div><div class="val mono" id="pnl_unreal">—</div></div>
    <div class="card"><div class="lbl">Profit Factor</div><div class="val mono" id="pf">—</div><div class="sub" id="pf_sub"></div></div>
    <div class="card"><div class="lbl">Win Rate</div><div class="val mono" id="wr">—</div><div class="sub" id="wr_sub"></div></div>
    <div class="card"><div class="lbl">Trades</div><div class="val mono" id="trades">—</div><div class="sub" id="trades_sub"></div></div>
    <div class="card"><div class="lbl">Recal bias</div><div class="val mono" id="recal">—</div><div class="sub" id="recal_sub"></div></div>
  </div>
  <div class="panel"><h2>Cumulative realized P&L</h2><canvas id="chart"></canvas></div>
  <div class="panel"><h2>Open positions (<span id="open_n">0</span>)</h2>
    <table><thead><tr><th>Token</th><th>Side</th><th>Iv</th><th>Entry</th><th>USD</th><th>Bid</th><th>Shares</th><th>Unreal</th><th>Age</th></tr></thead>
    <tbody id="open_body"></tbody></table></div>
  <div class="panel"><h2>Recent trades</h2>
    <table><thead><tr><th>Time</th><th>Token</th><th>Side</th><th>Entry</th><th>Exit</th><th>P&L</th></tr></thead>
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
function drawChart(curve){
  const c=$("chart"),dpr=window.devicePixelRatio||1;
  const w=c.clientWidth,h=c.clientHeight;c.width=w*dpr;c.height=h*dpr;
  const x=c.getContext("2d");x.scale(dpr,dpr);x.clearRect(0,0,w,h);
  if(!curve.length){x.fillStyle="#8b98a9";x.font="13px sans-serif";x.fillText("no closed trades yet",16,28);return}
  const ys=curve.map(p=>p.pnl),xs=curve.map(p=>p.t);
  let mn=Math.min(0,...ys),mx=Math.max(0,...ys);if(mn===mx){mx+=1;mn-=1}
  const t0=xs[0],t1=xs[xs.length-1]||t0+1;const pad=34;
  const X=t=>pad+(w-pad-10)*((t-t0)/((t1-t0)||1));
  const Y=v=>10+(h-20-10)*(1-((v-mn)/((mx-mn)||1)));
  // zero line
  x.strokeStyle="#283041";x.lineWidth=1;x.beginPath();x.moveTo(pad,Y(0));x.lineTo(w-10,Y(0));x.stroke();
  x.fillStyle="#8b98a9";x.font="10px sans-serif";x.fillText("$"+mx.toFixed(0),4,Y(mx)+4);x.fillText("$"+mn.toFixed(0),4,Y(mn)+4);
  // area + line
  const last=ys[ys.length-1];const col=last>=0?"#3fb950":"#f85149";
  x.beginPath();curve.forEach((p,i)=>{const px=X(p.t),py=Y(p.pnl);i?x.lineTo(px,py):x.moveTo(px,py)});
  x.lineTo(X(t1),Y(0));x.lineTo(X(t0),Y(0));x.closePath();x.fillStyle=col+"22";x.fill();
  x.beginPath();curve.forEach((p,i)=>{const px=X(p.t),py=Y(p.pnl);i?x.lineTo(px,py):x.moveTo(px,py)});
  x.strokeStyle=col;x.lineWidth=2;x.stroke();
}
async function tick(){
  let s;try{s=await(await fetch("/api/stats",{cache:"no-store"})).json()}catch(e){$("foot").textContent="disconnected — retrying…";return}
  pill($("bn"),s.health.binance,"Binance "+(s.health.binance?"●":"○"));
  pill($("pm"),s.health.polymarket,"Polymarket "+(s.health.polymarket?"●":"○"));
  $("up").textContent="uptime "+fmtAge(s.uptime_s)+" · "+s.health.active_tokens+" mkts · "+s.health.decisions+" dec";
  const t=$("pnl_total");t.textContent=money(s.pnl.total);t.className="val mono "+cls(s.pnl.total);
  $("pnl_split").textContent="real "+money(s.pnl.realized)+" · unrl "+money(s.pnl.unrealized);
  const r=$("pnl_real");r.textContent=money(s.pnl.realized);r.className="val mono "+cls(s.pnl.realized);
  const u=$("pnl_unreal");u.textContent=money(s.pnl.unrealized);u.className="val mono "+cls(s.pnl.unrealized);
  $("pf").textContent=(typeof s.stats.profit_factor==="number")?s.stats.profit_factor.toFixed(2):s.stats.profit_factor;
  $("pf_sub").textContent="W $"+s.stats.gross_win.toFixed(0)+" / L $"+s.stats.gross_loss.toFixed(0);
  $("wr").textContent=(s.stats.win_rate*100).toFixed(1)+"%";
  $("wr_sub").textContent=s.stats.wins+"W / "+s.stats.losses+"L";
  $("trades").textContent=s.stats.entries;
  $("trades_sub").textContent=s.stats.open+" open · "+s.stats.closed+" closed · "+s.stats.blocked+" blocked";
  $("recal").textContent=(s.recal.bias>=0?"+":"")+s.recal.bias.toFixed(3);
  $("recal_sub").textContent=s.recal.samples+" samples";
  const c=s.controls;const tg=$("toggle");
  tg.textContent=c.enabled?"● TRADING ON":"○ TRADING OFF";tg.className="btn "+(c.enabled?"on":"off");
  if(document.activeElement!==$("in_base"))$("in_base").value=(+c.base_usd).toFixed(2);
  if(document.activeElement!==$("in_max"))$("in_max").value=(+c.max_pos).toFixed(2);
  const lv=s.live;
  $("modebadge").textContent=lv.mode.toUpperCase();$("modebadge").className="pill "+(lv.mode==="live"?"bad":"ok");
  const ab=$("arm");ab.textContent=lv.armed?"● ARMED — click to DISARM":"○ DISARMED — click to ARM";ab.className="btn "+(lv.armed?"armed":"");
  const kb=$("kill");kb.textContent=lv.kill?"● KILL ACTIVE — click to CLEAR":"KILL SWITCH";kb.className="btn "+(lv.kill?"killon":"kill");
  $("open_n").textContent=s.open_positions.length;
  $("open_body").innerHTML=s.open_positions.map(p=>`<tr><td class="mono">${p.token}</td><td>${p.side}</td><td class="muted">${p.asset}/${p.interval}</td><td class="mono">${p.entry.toFixed(3)}</td><td class="mono">$${(+p.usd).toFixed(2)}</td><td class="mono">${p.bid.toFixed(3)}</td><td class="mono">${p.shares.toFixed(1)}</td><td class="mono ${cls(p.unreal)}">${money(p.unreal)}</td><td class="muted mono">${fmtAge(p.age_s)}</td></tr>`).join("")||`<tr><td colspan=9 class=muted>none</td></tr>`;
  $("trades_body").innerHTML=s.recent_trades.map(t=>`<tr><td class="muted mono">${fmtTime(t.ts)}</td><td class="mono">${t.token}</td><td>${t.side||""}</td><td class="mono">${t.entry!=null?(+t.entry).toFixed(3):"—"}</td><td class="mono">${t.exit!=null?(+t.exit).toFixed(3):"—"}</td><td class="mono ${cls(t.pnl)}">${money(t.pnl)}</td></tr>`).join("")||`<tr><td colspan=6 class=muted>no closed trades yet</td></tr>`;
  drawChart(s.curve);
  $("foot").textContent="updated "+new Date(s.now_ms).toLocaleTimeString();
}
$("toggle").onclick=async()=>{const on=$("toggle").classList.contains("on");try{await fetch("/api/control?enabled="+(on?"false":"true"),{method:"POST"})}catch(e){}tick()};
$("apply").onclick=async()=>{const b=$("in_base").value,m=$("in_max").value;try{await fetch(`/api/control?base_usd=${encodeURIComponent(b)}&max_pos=${encodeURIComponent(m)}`,{method:"POST"})}catch(e){}$("ctl_msg").textContent="applied ✓";setTimeout(()=>$("ctl_msg").textContent="",1800);tick()};
$("arm").onclick=async()=>{const armed=$("arm").classList.contains("armed");
  if(!armed && !confirm("ARM live trading?\n\nReal orders will post when the bot is in --mode live and a signal fires. Make sure your stake is set correctly first.")) return;
  try{await fetch("/api/control?arm="+(armed?"false":"true"),{method:"POST"})}catch(e){}
  $("arm_msg").textContent=armed?"disarmed":"ARMED";setTimeout(()=>$("arm_msg").textContent="",1800);tick()};
$("kill").onclick=async()=>{const on=$("kill").classList.contains("killon");
  if(!on && !confirm("Activate KILL SWITCH?\n\nThis halts the decision loop immediately — no new decisions or entries (any mode).")) return;
  try{await fetch("/api/control?kill="+(on?"false":"true"),{method:"POST"})}catch(e){}tick()};
tick();setInterval(tick,3000);window.addEventListener("resize",()=>{});
</script></body></html>"##;
