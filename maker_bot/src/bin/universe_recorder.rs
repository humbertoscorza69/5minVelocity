//! Order #15 PART B — up/down universe recorder. Runs on the OPERATOR'S PC, not the
//! VPS: it must never compete with the taker audition for resources.
//!
//! What it records, per discovered market: every websocket channel verbatim (`book`
//! full snapshots are mandatory — delta-replay from `price_change` alone is invalid),
//! plus a self-contained `markets` index so downstream analysis needs no API call.
//!
//! It also answers an open question on day one, which is why it runs first: the REAL
//! byte rate (B4 says measure it, do not guess, before choosing a retention policy).
//!
//! Run:  universe_recorder --root ./recorded --hours 0   (0 = until Ctrl-C)

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use maker_bot::health::{GapTracker, Heartbeat};
use maker_bot::jsonl::{self, DayWriter, DiskState};
use maker_bot::pmws::{self, RawEvent};
use maker_bot::universe::{self, MarketRef, UniverseSnapshot};
use serde_json::json;
use tokio::sync::{mpsc, watch};
use tracing::{error, info, warn};

#[derive(Debug, Parser)]
#[command(name = "universe_recorder", about = "Order #15 Part B — up/down universe recorder")]
struct Cli {
    /// Output root. Layout: <root>/<YYYY-MM-DD>/{polymarket,meta}/...
    #[arg(long, default_value = "recorded")]
    root: PathBuf,
    /// Assets to probe (comma-separated). Default covers the newly-found sol/xrp.
    #[arg(long, default_value = "btc,eth,sol,xrp")]
    assets: String,
    /// Intervals to probe. 1h is probed precisely because we could not confirm it exists.
    #[arg(long, default_value = "5m,15m,1h")]
    intervals: String,
    /// Future windows to pre-subscribe per market family.
    #[arg(long, default_value_t = 2)]
    lookahead: i64,
    /// Disk cap in GB (0 = no cap). Oldest-day eviction of evictable channels only.
    #[arg(long, default_value_t = 200.0)]
    disk_cap_gb: f64,
    /// Seconds a channel may be silent before the watchdog calls it stale.
    #[arg(long, default_value_t = 120)]
    stale_after_s: i64,
    /// Channels to receive but NOT write (comma-separated). Default: keep everything.
    ///
    /// `price_change` is ~90% of the write volume and is the obvious candidate — but
    /// DO NOT drop it casually. Combined with the REST print feed it decomposes a
    /// level's size decrease into trade volume (known from REST) and cancels (the
    /// residual). That turns the maker fill model's pessimistic "assume cancels sat
    /// behind us" into a MEASURED cancel fraction, and that assumption is the single
    /// biggest unknown in the queue model. Drop it only if this machine's I/O actually
    /// strains; the non-evictable core is ~0.74 GB/day either way.
    #[arg(long, default_value = "")]
    skip_channels: String,
    /// Stop after N hours (0 = run until Ctrl-C).
    #[arg(long, default_value_t = 0)]
    hours: u64,
    #[arg(long, default_value = "wss://ws-subscriptions-clob.polymarket.com/ws/market")]
    ws_url: String,
    #[arg(long, default_value = "https://gamma-api.polymarket.com")]
    gamma_url: String,
    #[arg(long, default_value = "wss://stream.binance.com:9443/ws")]
    binance_ws_url: String,
}

const PM_SUBDIR: &str = "polymarket";
const BN_SUBDIR: &str = "binance";
const META_SUBDIR: &str = "meta";

/// Which subdirectory a channel's sink lives in.
fn subdir_for(channel: &str) -> &'static str {
    if channel == "kline_1s" { BN_SUBDIR } else { PM_SUBDIR }
}

/// B4 layout: the polymarket + binance channels are all `.jsonl.zst` (the firehose
/// makes this mandatory and the rest is free). The `meta` channels — the GAP LOG
/// above all — stay plain text: a gap log nobody can `grep` defeats the entire point
/// of B3, which is that a silently missing hour must be impossible to overlook.
fn compress_for(channel: &str) -> bool {
    channel != "gaps" && channel != "universe"
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let cli = Cli::parse();
    let assets: Vec<String> = split_csv(&cli.assets);
    let intervals: Vec<String> = split_csv(&cli.intervals);
    info!(?assets, ?intervals, root = %cli.root.display(), "recorder starting");

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn({
        let tx = shutdown_tx.clone();
        async move {
            // SIGTERM matters as much as ctrl-c here: `systemd`, `timeout` and a
            // shutdown all send it, and an unhandled one leaves the zstd frames
            // unfinished (the failure the checkpoint above bounds, but graceful is
            // better than bounded).
            #[cfg(unix)]
            {
                use tokio::signal::unix::{SignalKind, signal};
                let mut term = match signal(SignalKind::terminate()) {
                    Ok(s) => s,
                    Err(_) => {
                        let _ = tokio::signal::ctrl_c().await;
                        let _ = tx.send(true);
                        return;
                    }
                };
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => info!("ctrl-c — shutting down"),
                    _ = term.recv() => info!("SIGTERM — shutting down"),
                }
            }
            #[cfg(not(unix))]
            {
                let _ = tokio::signal::ctrl_c().await;
                info!("ctrl-c — shutting down");
            }
            info!("finishing zstd frames");
            let _ = tx.send(true);
        }
    });
    if cli.hours > 0 {
        let tx = shutdown_tx.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(cli.hours * 3600)).await;
            info!(hours = cli.hours, "run duration reached");
            let _ = tx.send(true);
        });
    }

    let http = reqwest::Client::builder().timeout(Duration::from_secs(15)).build()?;

    // ---- Discovery: enumerate the whole up/down family by probing slugs. ----
    let (token_tx, token_rx) = watch::channel(Arc::new(Vec::<String>::new()));
    tokio::spawn(discovery_loop(
        http.clone(),
        cli.gamma_url.clone(),
        assets.clone(),
        intervals.clone(),
        cli.lookahead,
        token_tx,
        cli.root.clone(),
        shutdown_rx.clone(),
    ));

    // ---- WS ingest → raw event channel (shared by both feeds). ----
    let (ev_tx, ev_rx) = mpsc::unbounded_channel::<RawEvent>();
    tokio::spawn(ws_loop(cli.ws_url.clone(), token_rx, ev_tx.clone(), shutdown_rx.clone()));

    // ---- Binance 1s klines for every discovered asset (B2). ----
    let symbols: Vec<String> = assets.iter().map(|a| maker_bot::bnws::symbol_for(a)).collect();
    tokio::spawn(binance_loop(
        cli.binance_ws_url.clone(),
        symbols,
        ev_tx,
        shutdown_rx.clone(),
    ));

    // ---- Writer: routes each channel to its own rotating sink. ----
    writer_loop(&cli, ev_rx, shutdown_rx.clone()).await
}

fn split_csv(s: &str) -> Vec<String> {
    s.split(',').map(|x| x.trim().to_lowercase()).filter(|x| !x.is_empty()).collect()
}

/// Probe the up/down family every 60s, publish the token set, and write a daily
/// universe snapshot (B1: "log the discovered universe daily so we can see it change").
#[allow(clippy::too_many_arguments)]
async fn discovery_loop(
    http: reqwest::Client,
    gamma: String,
    assets: Vec<String>,
    intervals: Vec<String>,
    lookahead: i64,
    token_tx: watch::Sender<Arc<Vec<String>>>,
    root: PathBuf,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut iv = tokio::time::interval(Duration::from_secs(60));
    iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut uni_writer = DayWriter::new(&root, META_SUBDIR, "universe", false);
    // `.zst` to match the proven layout exactly, so existing analysis code that
    // globs `polymarket/*.jsonl.zst` picks the market index up unchanged.
    let mut mkt_writer = DayWriter::new(&root, PM_SUBDIR, "markets", true);
    let mut seen_slugs: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut last_universe_day = String::new();

    loop {
        tokio::select! {
            _ = iv.tick() => {
                let now_s = pmws::now_ms() / 1000;
                let plan = universe::probe_plan(now_s, &assets, &intervals, lookahead);
                let mut found: Vec<MarketRef> = Vec::new();
                let mut missing: Vec<String> = Vec::new();
                for (asset, interval, epoch) in plan {
                    let slug = universe::slug_for(&asset, &interval, epoch);
                    let url = format!("{gamma}/events/slug/{slug}");
                    match http.get(&url).send().await {
                        Ok(r) if r.status().is_success() => match r.json::<serde_json::Value>().await {
                            Ok(v) => match universe::market_from_event(&asset, &interval, epoch, &v) {
                                Some(m) => found.push(m),
                                None => missing.push(slug),
                            },
                            Err(_) => missing.push(slug),
                        },
                        // A 404 is the ANSWER to "do hourly markets exist", not an error.
                        Ok(_) => missing.push(slug),
                        Err(e) => {
                            warn!(%slug, error = %e, "discovery probe failed");
                            missing.push(slug);
                        }
                    }
                }
                let day = pmws::utc_day(pmws::now_ms());
                // Self-contained market index — one line per newly seen market, so
                // analysis never needs an API call to interpret the recording.
                for m in &found {
                    if seen_slugs.insert(m.slug.clone())
                        && let Ok(line) = serde_json::to_string(m)
                    {
                        let _ = mkt_writer.write_line(&day, &line);
                    }
                }
                let _ = mkt_writer.flush();

                // Daily universe snapshot: what existed, and what was probed and absent.
                if day != last_universe_day {
                    let snap = UniverseSnapshot {
                        ts_ms: pmws::now_ms(),
                        day: day.clone(),
                        markets: found.clone(),
                        probed_missing: missing.clone(),
                    };
                    if let Ok(line) = serde_json::to_string(&snap) {
                        let _ = uni_writer.write_line(&day, &line);
                        let _ = uni_writer.flush();
                    }
                    last_universe_day = day;
                    info!(markets = found.len(), missing = missing.len(), "universe snapshot written");
                }

                let mut toks: Vec<String> =
                    found.iter().flat_map(|m| m.tokens().map(str::to_string)).collect();
                toks.sort();
                toks.dedup();
                if !toks.is_empty() && **token_tx.borrow() != toks {
                    info!(tokens = toks.len(), markets = found.len(), "token set updated");
                    let _ = token_tx.send(Arc::new(toks));
                }
            }
            _ = shutdown.changed() => if *shutdown.borrow() { break },
        }
    }
    let _ = uni_writer.close();
    let _ = mkt_writer.close();
}

/// Reconnecting WS supervisor. Every session end is a potential gap; the writer marks
/// channels dead so the gap is recorded even if the socket never comes back.
async fn ws_loop(
    url: String,
    mut token_rx: watch::Receiver<Arc<Vec<String>>>,
    ev_tx: mpsc::UnboundedSender<RawEvent>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut backoff = Duration::from_secs(1);
    loop {
        if *shutdown.borrow() {
            break;
        }
        let tokens = token_rx.borrow().clone();
        if tokens.is_empty() {
            // Nothing discovered yet — wait for the first token set.
            tokio::select! {
                _ = token_rx.changed() => {}
                _ = shutdown.changed() => if *shutdown.borrow() { break },
            }
            continue;
        }
        let mut sd = shutdown.clone();
        let res = tokio::select! {
            r = pmws::run_session(
                &url, &tokens, &ev_tx, &mut sd,
                Duration::from_secs(60), Duration::from_secs(15),
            ) => r,
            // A changed token set means resubscribe: end the session cleanly.
            _ = token_rx.changed() => Ok(()),
        };
        match res {
            Ok(()) => {
                backoff = Duration::from_secs(1);
                if *shutdown.borrow() {
                    break;
                }
            }
            Err(e) => {
                warn!(error = %e, backoff_s = backoff.as_secs(), "pm ws session ended — reconnecting");
                let _ = ev_tx.send(RawEvent {
                    recv_ms: pmws::now_ms(),
                    channel: "__disconnect".into(),
                    payload: json!({ "error": e.to_string() }),
                });
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(30));
            }
        }
    }
}

/// Binance supervisor. Symbols are fixed at startup from the configured asset list,
/// so this never needs to resubscribe.
async fn binance_loop(
    url: String,
    symbols: Vec<String>,
    ev_tx: mpsc::UnboundedSender<RawEvent>,
    shutdown: watch::Receiver<bool>,
) {
    let mut backoff = Duration::from_secs(1);
    loop {
        if *shutdown.borrow() {
            break;
        }
        let mut sd = shutdown.clone();
        match maker_bot::bnws::run_session(
            &url,
            &symbols,
            &ev_tx,
            &mut sd,
            Duration::from_secs(30),
            Duration::from_secs(15),
        )
        .await
        {
            Ok(()) => {
                if *shutdown.borrow() {
                    break;
                }
                backoff = Duration::from_secs(1);
            }
            Err(e) => {
                warn!(error = %e, "binance session ended — reconnecting");
                let _ = ev_tx.send(RawEvent {
                    recv_ms: pmws::now_ms(),
                    channel: "__disconnect".into(),
                    payload: json!({ "feed": "binance", "error": e.to_string() }),
                });
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(30));
            }
        }
    }
}

/// Routes events to per-channel sinks, maintains gap/heartbeat state, and runs the
/// disk sweep. Single owner of the writers, so no locking.
async fn writer_loop(
    cli: &Cli,
    mut ev_rx: mpsc::UnboundedReceiver<RawEvent>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let mut writers: BTreeMap<String, DayWriter> = BTreeMap::new();
    let skip: std::collections::HashSet<String> = split_csv(&cli.skip_channels).into_iter().collect();
    if skip.is_empty() {
        info!("recording ALL channels (default)");
    } else {
        // Loud, per the Order #14 D lesson: a silent config override is how a run gets
        // interpreted against data it never actually captured.
        warn!(
            skipped = ?skip,
            "SKIPPING channels — they will be received and counted but NOT written; \
             analysis of this run must not assume they are present"
        );
    }
    let mut gaps = GapTracker::new(cli.stale_after_s * 1000);
    let mut gap_writer = DayWriter::new(&cli.root, META_SUBDIR, "gaps", false);
    let hb_path = cli.root.join("heartbeat.json");

    // B3: seed from the previous run's heartbeat so downtime we were absent for is
    // recorded rather than lost.
    if let Ok(text) = std::fs::read_to_string(&hb_path)
        && let Ok(prev) = serde_json::from_str::<Heartbeat>(&text)
    {
        info!(channels = prev.channels.len(), "seeding from previous heartbeat");
        gaps.seed_from_heartbeat(&prev);
    }

    let mut hb_iv = tokio::time::interval(Duration::from_secs(30));
    hb_iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut sweep_iv = tokio::time::interval(Duration::from_secs(300));
    sweep_iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let started_ms = pmws::now_ms();
    let cap_bytes = (cli.disk_cap_gb * 1e9) as u64;
    let mut reported_rate = false;

    loop {
        tokio::select! {
            Some(ev) = ev_rx.recv() => {
                let day = pmws::utc_day(ev.recv_ms);
                if ev.channel == "__disconnect" {
                    // Anchor a potential gap on the channels of the feed that ACTUALLY
                    // dropped. A Polymarket reconnect says nothing about the Binance
                    // socket, and marking both would attribute an outage to a feed
                    // that never missed a message.
                    let binance = ev.payload.get("feed").and_then(|f| f.as_str()) == Some("binance");
                    for c in gaps.stats().keys().cloned().collect::<Vec<_>>() {
                        if (c == "kline_1s") == binance {
                            gaps.mark_dead(&c);
                        }
                    }
                    continue;
                }
                if let Some(gap) = gaps.on_message(&ev.channel, ev.recv_ms)
                    && let Ok(line) = serde_json::to_string(&gap)
                {
                    warn!(channel = %gap.channel, duration_ms = gap.duration_ms, "GAP recorded");
                    let _ = gap_writer.write_line(&day, &line);
                    let _ = gap_writer.flush();
                }
                // Skipped channels are still COUNTED above (heartbeat and gap tracking
                // stay honest about what the feed is doing) — we only decline to
                // WRITE them, which is where the I/O cost actually is.
                if skip.contains(&ev.channel) {
                    continue;
                }
                let w = writers.entry(ev.channel.clone()).or_insert_with(|| {
                    DayWriter::new(
                        &cli.root,
                        subdir_for(&ev.channel),
                        &ev.channel,
                        compress_for(&ev.channel),
                    )
                });
                let line = json!({ "recv_ms": ev.recv_ms, "data": ev.payload }).to_string();
                if let Err(e) = w.write_line(&day, &line) {
                    error!(channel = %ev.channel, error = %e, "write failed");
                }
            }
            _ = hb_iv.tick() => {
                // checkpoint(), not flush(): finishes the zstd frame so a hard kill
                // (SIGKILL, power loss, PC sleep) costs at most this interval instead
                // of the entire day's compressed data.
                for w in writers.values_mut() { let _ = w.checkpoint(); }
                let bytes: BTreeMap<String, u64> =
                    writers.iter().map(|(k, w)| (k.clone(), w.bytes())).collect();
                let hb = Heartbeat {
                    ts_ms: pmws::now_ms(),
                    pid: std::process::id(),
                    channels: gaps.stats().clone(),
                    bytes: bytes.clone(),
                    skipped_channels: {
                        let mut s: Vec<String> = skip.iter().cloned().collect();
                        s.sort();
                        s
                    },
                };
                if let Ok(text) = serde_json::to_string_pretty(&hb) {
                    let _ = std::fs::create_dir_all(&cli.root);
                    let _ = std::fs::write(&hb_path, text);
                }
                // Watchdog: anything quiet past the threshold is marked dead now, so
                // the gap is anchored even if the socket never returns.
                let now = pmws::now_ms();
                let newly = gaps.mark_stale_dead(now);
                if newly > 0 {
                    warn!(channels = newly, "staleness watchdog: channel(s) went quiet");
                }
                // B4: report the REAL byte rate in the first hour — do not guess.
                let elapsed_s = (now - started_ms) / 1000;
                if !reported_rate && elapsed_s >= 3_600 {
                    let total: u64 = bytes.values().sum();
                    let per_day_gb = (total as f64 / elapsed_s as f64) * 86_400.0 / 1e9;
                    info!(
                        measured_gb_per_day = format!("{per_day_gb:.1}"),
                        first_hour_bytes = total,
                        "MEASURED byte rate (pre-compression) — use this to set retention"
                    );
                    reported_rate = true;
                }
            }
            _ = sweep_iv.tick() => {
                let used = jsonl::dir_size(&cli.root);
                match jsonl::disk_state(used, cap_bytes, 0.8) {
                    DiskState::Ok => {}
                    DiskState::Warn => warn!(
                        used_gb = format!("{:.1}", used as f64 / 1e9),
                        cap_gb = cli.disk_cap_gb, "disk past 80% of cap"
                    ),
                    DiskState::Evict => {
                        let freed = evict_oldest(&cli.root);
                        warn!(
                            used_gb = format!("{:.1}", used as f64 / 1e9),
                            freed_bytes = freed, "disk cap hit — evicted oldest evictable file"
                        );
                    }
                }
            }
            _ = shutdown.changed() => if *shutdown.borrow() { break },
        }
    }

    info!("closing writers (finishing zstd frames)");
    for (_, mut w) in writers {
        let _ = w.close();
    }
    let _ = gap_writer.close();
    Ok(())
}

/// Delete the oldest evictable file (B4: `book` and `markets` are never evicted —
/// full snapshots are what make depth reconstruction possible at all). Returns bytes freed.
fn evict_oldest(root: &std::path::Path) -> u64 {
    for (_day, dir) in jsonl::day_dirs(root) {
        for sub in [PM_SUBDIR, BN_SUBDIR] {
            let Ok(rd) = std::fs::read_dir(dir.join(sub)) else { continue };
            for e in rd.flatten() {
                let name = e.file_name().to_string_lossy().to_string();
                let stem = name.split('.').next().unwrap_or("").to_string();
                if !jsonl::evictable(&stem) {
                    continue;
                }
                let size = e.metadata().map(|m| m.len()).unwrap_or(0);
                if std::fs::remove_file(e.path()).is_ok() {
                    return size;
                }
            }
        }
    }
    0
}
