//! Order #15 Part A — maker-book paper bot.
//!
//! Quotes the BTC up/down book as a MAKER and books virtual fills through
//! [`fill_model::FillEngine`]. Nothing is sent to any venue: this process holds no
//! keys and posts no orders. It exists to find out whether the validated queue model
//! (+0.455¢/share OOS, +0.739¢ rebate-inclusive) survives contact with the live tape.
//!
//! WHY A LIVE PAPER RUN AND NOT A REPLAY. The engine is driven by TRADE PRINTS, and
//! prints are not on the websocket: `last_trade_price` carries only ~30% of them
//! (last-price semantics collapse consecutive fills at one price). The complete source
//! is the REST endpoint `data-api.polymarket.com/trades`, whose indexer lags by
//! MINUTES. Our 303 GB of recorded book data therefore cannot drive a replay — it has
//! no print channel — so the prints have to be collected alongside, live.
//!
//! That lag dictates the architecture. Book events and prints are merged into ONE
//! timestamp-ordered stream and the engine is fed only events older than
//! `--horizon-s`. Feeding a live level update against a not-yet-fetched print would
//! mis-attribute every traded decrease as a cancel and silently destroy the fill rate,
//! which is the number the whole business case rests on.
//!
//! Config is FROZEN per order A1 (BTC only, asks only, join BBO at the back of the
//! queue, band 0.10–0.90, S=50, no lag defense, all hours). Re-tuning any of it on
//! this run would be selecting on the validation set.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use maker_bot::fill_model::{
    modeled_rebate_per_share, settle_pnl_per_share, FillEngine, Level, MakerConfig, MarketEvent,
    PostReject, Side,
};
use maker_bot::jsonl::DayWriter;
use maker_bot::universe::{self, MarketRef};
use maker_bot::pmws::{self, RawEvent};
use serde_json::{json, Value};
use tokio::sync::{mpsc, watch};
use tracing::{info, warn};

const PM_WS: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
/// Marks after each fill: the adverse-selection curve (order A4). If we are being
/// picked off, the mid runs away from us on exactly this timescale.
const MARK_OFFSETS_S: [i64; 4] = [1, 5, 30, 60];

#[derive(Parser, Debug, Clone)]
#[command(name = "maker_paper")]
struct Cli {
    #[arg(long, default_value = "data/maker_paper")]
    root: String,
    #[arg(long, default_value = "https://gamma-api.polymarket.com")]
    gamma: String,
    #[arg(long, default_value = "https://data-api.polymarket.com")]
    data_api: String,
    /// Order A1: BTC only. ETH asks were negative on EVERY out-of-sample day.
    #[arg(long, default_value = "btc")]
    assets: String,
    /// 5m and 15m both, tagged; 15m is untested for MM and gets sliced offline.
    #[arg(long, default_value = "5m,15m")]
    intervals: String,
    #[arg(long, default_value_t = 50.0)]
    size: f64,
    #[arg(long, default_value_t = 150.0)]
    max_inventory: f64,
    /// Reconciliation horizon: how far behind live the engine runs so the REST print
    /// feed has caught up. The indexer lags minutes; 300s is the measured safe value.
    #[arg(long, default_value_t = 300)]
    horizon_s: i64,
    #[arg(long, default_value_t = 0.07)]
    taker_fee_rate: f64,
    #[arg(long, default_value_t = 0.20)]
    rebate_share: f64,
}

/// One event on the merged, timestamp-ordered stream.
#[derive(Debug, Clone)]
struct Timed {
    ts_ms: i64,
    ev: MarketEvent,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_target(false).init();
    let cli = Cli::parse();
    let assets: Vec<String> = cli.assets.split(',').map(|s| s.trim().to_lowercase()).collect();
    let intervals: Vec<String> =
        cli.intervals.split(',').map(|s| s.trim().to_lowercase()).collect();
    info!(?assets, ?intervals, size = cli.size, cap = cli.max_inventory,
        horizon_s = cli.horizon_s, "maker_paper starting (PAPER — no keys, no orders)");

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        let _ = shutdown_tx.send(true);
    });

    let http = reqwest::Client::builder().timeout(Duration::from_secs(15)).build()?;
    let (mkt_tx, mkt_rx) = mpsc::unbounded_channel::<MarketRef>();
    let (ws_tx, ws_rx) = mpsc::unbounded_channel::<RawEvent>();
    let (print_tx, print_rx) = mpsc::unbounded_channel::<Timed>();

    tokio::spawn(discovery_loop(http.clone(), cli.clone(), assets, intervals, mkt_tx,
        shutdown_rx.clone()));
    tokio::spawn(prints_loop(http.clone(), cli.clone(), print_rx_seed(), print_tx.clone(),
        shutdown_rx.clone()));
    tokio::spawn(ws_loop(ws_tx, shutdown_rx.clone()));
    engine_loop(cli, mkt_rx, ws_rx, print_rx, shutdown_rx).await
}

/// Placeholder channel so `prints_loop` can be spawned before markets are known; the
/// engine republishes the active condition_ids through a shared registry instead.
fn print_rx_seed() -> std::sync::Arc<std::sync::Mutex<Vec<MarketRef>>> {
    std::sync::Arc::new(std::sync::Mutex::new(Vec::new()))
}

/// Poll gamma for the up/down markets in our window and publish each new one.
async fn discovery_loop(
    http: reqwest::Client,
    cli: Cli,
    assets: Vec<String>,
    intervals: Vec<String>,
    tx: mpsc::UnboundedSender<MarketRef>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut seen: HashSet<String> = HashSet::new();
    let mut tick = tokio::time::interval(Duration::from_secs(30));
    loop {
        tokio::select! {
            _ = tick.tick() => {}
            _ = shutdown.changed() => if *shutdown.borrow() { return },
        }
        let now_s = pmws::now_ms() / 1000;
        for (asset, interval, epoch) in universe::probe_plan(now_s, &assets, &intervals, 2) {
            let slug = universe::slug_for(&asset, &interval, epoch);
            if seen.contains(&slug) {
                continue;
            }
            let url = format!("{}/events/slug/{slug}", cli.gamma);
            let Ok(r) = http.get(&url).send().await else { continue };
            if !r.status().is_success() {
                continue;
            }
            let Ok(v) = r.json::<Value>().await else { continue };
            if let Some(m) = universe::market_from_event(&asset, &interval, epoch, &v) {
                seen.insert(slug);
                let _ = tx.send(m);
            }
        }
    }
}

/// Subscribe to the book channels. Tokens are refreshed by reconnecting whenever the
/// engine's active set changes, mirroring the recorder.
async fn ws_loop(tx: mpsc::UnboundedSender<RawEvent>, mut shutdown: watch::Receiver<bool>) {
    let mut tokens: Vec<String> = Vec::new();
    loop {
        if *shutdown.borrow() {
            return;
        }
        {
            let reg = ACTIVE_TOKENS.lock().unwrap();
            tokens = reg.clone();
        }
        if tokens.is_empty() {
            tokio::time::sleep(Duration::from_secs(5)).await;
            continue;
        }
        if let Err(e) = pmws::run_session(PM_WS, &tokens, &tx, &mut shutdown,
            Duration::from_secs(60), Duration::from_secs(15)).await
        {
            warn!(error = %e, "pm ws session ended");
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

static ACTIVE_TOKENS: std::sync::LazyLock<std::sync::Mutex<Vec<String>>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(Vec::new()));
static ACTIVE_MARKETS: std::sync::LazyLock<std::sync::Mutex<Vec<MarketRef>>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(Vec::new()));

/// Fetch REST prints for every market we have quoted recently.
///
/// This is the ONLY complete source of executed volume (the WS carries ~30%), and the
/// indexer lags minutes — which is why the engine consumes on a horizon rather than
/// live. `takerOnly=false` matters: without it the response omits maker-side rows.
async fn prints_loop(
    http: reqwest::Client,
    cli: Cli,
    _seed: std::sync::Arc<std::sync::Mutex<Vec<MarketRef>>>,
    tx: mpsc::UnboundedSender<Timed>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut seen: HashSet<String> = HashSet::new();
    let mut tick = tokio::time::interval(Duration::from_secs(20));
    loop {
        tokio::select! {
            _ = tick.tick() => {}
            _ = shutdown.changed() => if *shutdown.borrow() { return },
        }
        let markets: Vec<MarketRef> = ACTIVE_MARKETS.lock().unwrap().clone();
        for m in markets {
            let url = format!("{}/trades?market={}&takerOnly=false&limit=500",
                cli.data_api, m.condition_id);
            let Ok(r) = http.get(&url).send().await else { continue };
            let Ok(v) = r.json::<Value>().await else { continue };
            let Some(arr) = v.as_array() else { continue };
            for t in arr {
                // De-dup on the chain hash: the endpoint returns overlapping pages.
                let Some(h) = t.get("transactionHash").and_then(Value::as_str) else { continue };
                let side = t.get("outcome").and_then(Value::as_str).unwrap_or("");
                let key = format!("{h}:{side}:{}", t.get("size").and_then(Value::as_f64).unwrap_or(0.0));
                if !seen.insert(key) {
                    continue;
                }
                // ONLY BUYS consume a resting ASK. A sell of this token hits the bid
                // and can never fill our offer, so counting it would inflate the fill
                // rate — the one number the whole business case rests on. The order
                // notes 87% of taker prints are buys, so this discards ~13%.
                if !t.get("side").and_then(Value::as_str)
                    .is_some_and(|s| s.eq_ignore_ascii_case("buy"))
                {
                    continue;
                }
                let (Some(px), Some(sz), Some(ts)) = (
                    t.get("price").and_then(num),
                    t.get("size").and_then(num),
                    t.get("timestamp").and_then(num),
                ) else { continue };
                let token = match t.get("asset").and_then(Value::as_str) {
                    Some(a) => a.to_string(),
                    None => continue,
                };
                let ts_ms = if ts > 1e12 { ts as i64 } else { (ts * 1000.0) as i64 };
                let _ = tx.send(Timed {
                    ts_ms,
                    ev: MarketEvent::Trade { ts_ms, token, price: px, size: sz },
                });
            }
        }
        if seen.len() > 400_000 {
            seen.clear(); // bounded: old hashes cannot recur once the market resolves
        }
    }
}

fn num(v: &Value) -> Option<f64> {
    v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok()))
}

/// The driver: merge, hold back by the horizon, feed the engine, log everything.
async fn engine_loop(
    cli: Cli,
    mut mkt_rx: mpsc::UnboundedReceiver<MarketRef>,
    mut ws_rx: mpsc::UnboundedReceiver<RawEvent>,
    mut print_rx: mpsc::UnboundedReceiver<Timed>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let mut engine = FillEngine::new(MakerConfig {
        price_min: 0.10,
        price_max: 0.90,
        size_shares: cli.size,
        max_net_inventory_shares: cli.max_inventory,
    });
    let mut w_post = DayWriter::new(&cli.root, "maker", "posts", true);
    let mut w_fill = DayWriter::new(&cli.root, "maker", "fills", true);
    let mut w_cancel = DayWriter::new(&cli.root, "maker", "cancels", true);
    let mut w_metric = DayWriter::new(&cli.root, "maker", "metrics", false);

    let mut pending: Vec<Timed> = Vec::new();
    let mut tok2mkt: HashMap<String, MarketRef> = HashMap::new();
    let mut mid: HashMap<String, f64> = HashMap::new();
    // fills awaiting their +1/+5/+30/+60s marks and their settlement
    let mut open_fills: Vec<(i64, String, f64, f64, u64, BTreeMap<i64, f64>)> = Vec::new();
    let mut stats: BTreeMap<String, (u64, u64, f64, f64)> = BTreeMap::new(); // hour -> posts,fills,shares,gross
    let mut settled: HashSet<String> = HashSet::new();
    let mut last_metric_hour = String::new();
    let mut flush = tokio::time::interval(Duration::from_secs(30));

    loop {
        tokio::select! {
            _ = shutdown.changed() => if *shutdown.borrow() { break },
            Some(m) = mkt_rx.recv() => {
                for t in m.tokens() { tok2mkt.insert(t.to_string(), m.clone()); }
                let mut reg = ACTIVE_MARKETS.lock().unwrap();
                reg.push(m.clone());
                reg.retain(|x| x.epoch + universe::interval_seconds(&x.interval).unwrap_or(300)
                    > pmws::now_ms() / 1000 - 1800);
                let mut toks = ACTIVE_TOKENS.lock().unwrap();
                *toks = reg.iter().flat_map(|x| x.tokens().map(str::to_string)).collect();
            }
            Some(ev) = ws_rx.recv() => {
                for t in book_events(&ev) { pending.push(t); }
            }
            Some(t) = print_rx.recv() => pending.push(t),
            _ = flush.tick() => {
                let _ = w_post.flush(); let _ = w_fill.flush();
                let _ = w_cancel.flush(); let _ = w_metric.flush();
            }
        }

        // Drain everything older than the horizon, in timestamp order.
        let cutoff = pmws::now_ms() - cli.horizon_s * 1000;
        if !pending.iter().any(|t| t.ts_ms <= cutoff) {
            continue;
        }
        pending.sort_by_key(|t| t.ts_ms);
        let mut rest = Vec::new();
        for t in std::mem::take(&mut pending) {
            if t.ts_ms > cutoff {
                rest.push(t);
                continue;
            }
            let day = pmws::utc_day(t.ts_ms);
            let hour = format!("{day}T{:02}", (t.ts_ms / 3_600_000) % 24);

            if let MarketEvent::Snapshot { token, asks, bids, .. } = &t.ev
                && let (Some(a), Some(b)) = (asks.first(), bids.last())
            {
                mid.insert(token.clone(), (a.price + b.price) / 2.0);
            }

            let (fills, cancels) = engine.apply(&t.ev);
            for c in &cancels {
                let _ = w_cancel.write_line(&day, &serde_json::to_string(c)?);
            }
            for f in &fills {
                let m = tok2mkt.get(&f.token);
                let reb = modeled_rebate_per_share(f.price, cli.taker_fee_rate, cli.rebate_share);
                let line = json!({
                    "fill": f, "rebate_per_share": reb,
                    "asset": m.map(|x| x.asset.clone()), "interval": m.map(|x| x.interval.clone()),
                    "epoch": m.map(|x| x.epoch), "mid_at_fill": mid.get(&f.token),
                });
                let _ = w_fill.write_line(&day, &line.to_string());
                open_fills.push((f.ts_ms, f.token.clone(), f.price, f.size, f.order_id,
                    BTreeMap::new()));
                let e = stats.entry(hour.clone()).or_default();
                e.1 += 1; e.2 += f.size;
            }

            // Adverse-selection marks: the mid at +1/+5/+30/+60s after each fill.
            for of in open_fills.iter_mut() {
                if let Some(mv) = mid.get(&of.1) {
                    for k in MARK_OFFSETS_S {
                        let due = of.0 + k * 1000;
                        if t.ts_ms >= due {
                            of.5.entry(k).or_insert(*mv);
                        }
                    }
                }
            }
            open_fills.retain(|of| {
                if t.ts_ms < of.0 + 60_000 {
                    return true;
                }
                let _ = w_fill.write_line(&day, &json!({
                    "kind": "adverse_curve", "order_id": of.4, "token": of.1,
                    "fill_ts": of.0, "fill_price": of.2, "size": of.3,
                    "marks": of.5.iter().map(|(k, v)| (k.to_string(), *v))
                        .collect::<BTreeMap<_, _>>(),
                }).to_string());
                false
            });

            // Quote: join the current best ask, back of queue. Naive — we never cancel
            // (order A1: cancel-rejoin resets queue position and measured WORSE).
            if let MarketEvent::Snapshot { token, asks, .. } = &t.ev
                && let Some(best) = asks.first()
            {
                let inv_before = engine.exposure();
                match engine.try_post(t.ts_ms, token, best.price, cli.size) {
                    Ok(o) => {
                        let m = tok2mkt.get(token);
                        let _ = w_post.write_line(&day, &json!({
                            "order": o, "mid": mid.get(token), "inventory_before": inv_before,
                            "asset": m.map(|x| x.asset.clone()),
                            "interval": m.map(|x| x.interval.clone()),
                            "epoch": m.map(|x| x.epoch),
                            "book_age_ms": pmws::now_ms() - t.ts_ms,
                        }).to_string());
                        stats.entry(hour.clone()).or_default().0 += 1;
                    }
                    Err(r) => {
                        // A post that never happened is a measurement too: a run that
                        // silently declines to quote measures nothing.
                        if !matches!(r, PostReject::AlreadyResting) {
                            let _ = w_post.write_line(&day, &json!({
                                "reject": format!("{r:?}").to_lowercase(),
                                "token": token, "price": best.price,
                                "inventory": inv_before, "ts_ms": t.ts_ms,
                            }).to_string());
                        }
                    }
                }
            }
        }
        pending = rest;

        // Settle finished markets. Without this the ask-only inventory ratchets and
        // the cap binds permanently after three fills — the run would quote nothing
        // and measure nothing. Grace exceeds the settlement lag so the post-close mid
        // has resolved to ~1.0 / ~0.0 before we read it.
        let now_s = pmws::now_ms() / 1000;
        let due: Vec<MarketRef> = ACTIVE_MARKETS.lock().unwrap().iter()
            .filter(|m| {
                let end = m.epoch + universe::interval_seconds(&m.interval).unwrap_or(300);
                now_s > end + 60 && !settled.contains(&m.slug)
            })
            .cloned().collect();
        for m in due {
            settled.insert(m.slug.clone());
            for tok in m.tokens() {
                let (shares, dropped) = engine.settle_token(tok);
                if shares <= 0.0 && dropped == 0 {
                    continue;
                }
                // Resolution read from the post-close mid. Logged alongside the raw
                // mid, not as a bare verdict, so it stays auditable offline.
                let mv = mid.get(tok).copied();
                let yes = mv.map(|v| v >= 0.5);
                let day = pmws::utc_day(pmws::now_ms());
                let _ = w_fill.write_line(&day, &json!({
                    "kind": "settle", "token": tok, "slug": m.slug,
                    "asset": m.asset, "interval": m.interval, "epoch": m.epoch,
                    "shares_short": shares, "orders_dropped": dropped,
                    "post_close_mid": mv, "resolved_yes": yes,
                    "inventory_after": engine.inventory(),
                }).to_string());
            }
        }

        // Hourly rollup, judged against the model the order pre-registered.
        // Written ONCE per hour: the first cut emitted this every drain and produced
        // 36,384 lines / 5.3 MB in four minutes, the same unbounded-log failure that
        // took the taker dashboard down this morning.
        let day = pmws::utc_day(pmws::now_ms());
        if let Some((h, v)) = stats.iter().next_back()
            && *h != last_metric_hour
        {
            last_metric_hour.clone_from(h);
            let _ = w_metric.write_line(&day, &json!({
                "hour": h, "posts": v.0, "fills": v.1, "shares_filled": v.2,
                "fill_rate": if v.0 > 0 { v.1 as f64 / v.0 as f64 } else { 0.0 },
                "inventory": engine.inventory(), "resting": engine.resting_orders().len(),
                "model_target_cents_per_share": 0.455,
            }).to_string());
            let _ = w_metric.flush();
        }
    }

    let _ = w_post.close();
    let _ = w_fill.close();
    let _ = w_cancel.close();
    let _ = w_metric.close();
    info!("maker_paper stopped");
    Ok(())
}

/// Translate one raw WS frame into engine events. `book` gives full snapshots (
/// mandatory — reconstructing depth from deltas alone was measured invalid) and
/// `price_change` gives level updates carrying NEW RESTING SIZE, never traded volume.
fn book_events(ev: &RawEvent) -> Vec<Timed> {
    let mut out = Vec::new();
    let ts = ev.payload.get("timestamp").and_then(num).map_or(ev.recv_ms, |t| {
        if t > 1e12 { t as i64 } else { (t * 1000.0) as i64 }
    });
    match ev.channel.as_str() {
        "book" => {
            let Some(token) = ev.payload.get("asset_id").and_then(Value::as_str) else {
                return out;
            };
            let lv = |k: &str| -> Vec<Level> {
                ev.payload.get(k).and_then(Value::as_array).map_or_else(Vec::new, |a| {
                    let mut v: Vec<Level> = a.iter()
                        .filter_map(|x| Some(Level {
                            price: num(x.get("price")?)?,
                            size: num(x.get("size")?)?,
                        }))
                        .collect();
                    v.sort_by(|p, q| p.price.partial_cmp(&q.price).unwrap());
                    v
                })
            };
            out.push(Timed { ts_ms: ts, ev: MarketEvent::Snapshot {
                ts_ms: ts, token: token.to_string(), asks: lv("asks"), bids: lv("bids"),
            }});
        }
        "price_change" => {
            let Some(arr) = ev.payload.get("price_changes").and_then(Value::as_array) else {
                return out;
            };
            for c in arr {
                let (Some(token), Some(price), Some(size)) = (
                    c.get("asset_id").and_then(Value::as_str),
                    c.get("price").and_then(num),
                    c.get("size").and_then(num),
                ) else { continue };
                let side = match c.get("side").and_then(Value::as_str) {
                    Some(s) if s.eq_ignore_ascii_case("sell") => Side::Ask,
                    Some(s) if s.eq_ignore_ascii_case("buy") => Side::Bid,
                    _ => continue,
                };
                out.push(Timed { ts_ms: ts, ev: MarketEvent::LevelUpdate {
                    ts_ms: ts, token: token.to_string(), price, size, side,
                }});
            }
        }
        _ => {}
    }
    out
}

#[allow(dead_code)]
fn settle(sold_at: f64, yes: bool) -> f64 {
    settle_pnl_per_share(sold_at, yes)
}
