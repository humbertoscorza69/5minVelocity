//! Binance spot websocket client (read-only).
//!
//! Connects to the raw `/ws` endpoint and SUBSCRIBEs to `<sym>@kline_1s` and
//! `<sym>@aggTrade` for every configured asset (BTC → btcusdt, …). The SUBSCRIBE
//! frame is re-sent on every (re)connect, which is our "resubscribe → fresh
//! snapshot" path. We answer protocol Pings with Pongs to stay alive.
//!
//! Strictly WS-only: no REST. Strictly read-only: no orders.

use std::sync::atomic::Ordering;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::sync::watch;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, info, warn};

use super::{ReconnectPolicy, SessionEnd, connect_with_timeout, run_with_reconnect};
use crate::config::Config;
use crate::events::{EventLogger, EventRecord};
use crate::state::{Shared, now_ms};

/// Map a config asset (e.g. "BTC") to a Binance spot symbol ("btcusdt").
fn symbol_for(asset: &str) -> String {
    format!("{}usdt", asset.to_lowercase())
}

/// Reverse of [`symbol_for`]: a Binance symbol ("btcusdt") → the asset ("BTC"). The
/// strategy universe is BTC/ETH; returns `None` for anything else.
fn asset_for_symbol(sym: &str) -> Option<String> {
    let base = sym.strip_suffix("usdt")?;
    if base.is_empty() {
        return None;
    }
    Some(base.to_uppercase())
}

/// Entry point: supervise the Binance session under the reconnect policy.
pub async fn run(state: Shared, cfg: Config, shutdown: watch::Receiver<bool>, logger: EventLogger) {
    let url = cfg.connections.binance_ws_url.clone();
    let streams: Vec<String> = cfg
        .markets
        .assets
        .iter()
        .flat_map(|a| {
            let sym = symbol_for(a);
            [format!("{sym}@kline_1s"), format!("{sym}@aggTrade")]
        })
        .collect();

    if streams.is_empty() {
        warn!("binance: no assets configured; ingestion idle");
    }

    let policy = ReconnectPolicy::from_config(cfg.connections.reconnect_max_attempts_per_60s);
    let connect_timeout = Duration::from_secs(cfg.connections.connect_timeout_s);
    let alert_dir = cfg.paths.alert_dir.clone();
    let loop_state = state.clone();

    run_with_reconnect(
        "binance_ws",
        policy,
        loop_state,
        alert_dir,
        shutdown.clone(),
        move |reconnect_no| {
            let url = url.clone();
            let streams = streams.clone();
            let state = state.clone();
            let logger = logger.clone();
            let mut shutdown = shutdown.clone();
            async move {
                session(
                    reconnect_no,
                    &url,
                    &streams,
                    &state,
                    &logger,
                    &mut shutdown,
                    connect_timeout,
                )
                .await
            }
        },
    )
    .await;
}

/// One connect → subscribe → read-until-end cycle.
async fn session(
    reconnect_no: u32,
    url: &str,
    streams: &[String],
    state: &Shared,
    logger: &EventLogger,
    shutdown: &mut watch::Receiver<bool>,
    connect_timeout: Duration,
) -> SessionEnd {
    let ws = match connect_with_timeout(url, connect_timeout).await {
        Ok(ws) => ws,
        Err(reason) => return SessionEnd::Lost(reason),
    };

    if reconnect_no > 0 {
        info!(
            ws = "binance_ws",
            attempt = reconnect_no,
            streams = streams.len(),
            "ws_reconnect_complete"
        );
    } else {
        info!(ws = "binance_ws", streams = streams.len(), url, "ws_connected");
    }
    state.binance_connected.store(true, Ordering::Relaxed);

    let (mut write, mut read) = ws.split();

    // (Re)subscribe on every connect.
    if !streams.is_empty() {
        let sub = json!({ "method": "SUBSCRIBE", "params": streams, "id": 1 });
        if let Err(e) = write.send(Message::Text(sub.to_string().into())).await {
            state.binance_connected.store(false, Ordering::Relaxed);
            return SessionEnd::Lost(format!("subscribe send failed: {e}"));
        }
    }

    let end = loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    let _ = write.send(Message::Close(None)).await;
                    break SessionEnd::Shutdown;
                }
            }
            msg = read.next() => match msg {
                Some(Ok(Message::Text(txt))) => handle_text(&txt, state, logger),
                Some(Ok(Message::Ping(payload))) => {
                    let _ = write.send(Message::Pong(payload)).await;
                }
                Some(Ok(Message::Close(frame))) => {
                    break SessionEnd::Lost(format!("server close: {frame:?}"));
                }
                Some(Ok(_)) => {} // Pong / Binary / Frame — ignore
                Some(Err(e)) => break SessionEnd::Lost(format!("read error: {e}")),
                None => break SessionEnd::Lost("stream ended".into()),
            }
        }
    };

    state.binance_connected.store(false, Ordering::Relaxed);
    end
}

/// Parse one Binance text frame and update shared state + the event log.
fn handle_text(txt: &str, state: &Shared, logger: &EventLogger) {
    let v: Value = match serde_json::from_str(txt) {
        Ok(v) => v,
        Err(_) => {
            state.counters.parse_errors.fetch_add(1, Ordering::Relaxed);
            return;
        }
    };

    // Subscription ack: {"result":null,"id":1} — no "e" field.
    if v.get("result").is_some() && v.get("e").is_none() {
        debug!(ws = "binance_ws", "subscribe ack");
        return;
    }

    let etype = v.get("e").and_then(Value::as_str).unwrap_or("");
    state.counters.binance_msgs.fetch_add(1, Ordering::Relaxed);
    let recv = now_ms();

    match etype {
        "aggTrade" => {
            let sym = v
                .get("s")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_lowercase();
            let price = v
                .get("p")
                .and_then(Value::as_str)
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0);
            let tms = v.get("T").and_then(Value::as_i64).unwrap_or(recv);
            {
                let mut e = state.binance.entry(sym.clone()).or_default();
                e.symbol = sym.clone();
                e.last_price = price;
                e.last_trade_ms = tms;
                e.updated_ms = recv;
            }
            state.counters.binance_aggtrades.fetch_add(1, Ordering::Relaxed);
            logger.record(EventRecord {
                recv_ms: recv,
                source: "binance",
                event_type: "aggTrade".to_string(),
                key: sym,
                exch_ms: Some(tms),
                px: Some(price),
            });
        }
        "kline" => {
            let sym = v
                .get("s")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_lowercase();
            let k = v.get("k");
            let close = k
                .and_then(|k| k.get("c"))
                .and_then(Value::as_str)
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0);
            let open_ms = k.and_then(|k| k.get("t")).and_then(Value::as_i64).unwrap_or(0);
            let close_ms = k.and_then(|k| k.get("T")).and_then(Value::as_i64).unwrap_or(0);
            let is_final = k
                .and_then(|k| k.get("x"))
                .and_then(Value::as_bool)
                .unwrap_or(false);
            {
                let mut e = state.binance.entry(sym.clone()).or_default();
                e.symbol = sym.clone();
                e.kline_close = close;
                e.kline_open_ms = open_ms;
                e.kline_close_ms = close_ms;
                e.kline_final = is_final;
                e.updated_ms = recv;
            }
            state.counters.binance_klines.fetch_add(1, Ordering::Relaxed);
            // Phase 6 D1: feed FINALIZED bars to the decision loop (open-second keyed,
            // matching the Capa B replay's `t_open_ms / 1000`). `kline_tx` is unset in
            // ingestion-only runs, so this is a no-op there.
            if is_final && close > 0.0 && open_ms > 0
                && let Some(tx) = state.kline_tx.get()
                && let Some(asset) = asset_for_symbol(&sym)
            {
                let _ = tx.send(crate::state::KlineClose {
                    asset,
                    t_s: open_ms / 1000,
                    close,
                    received_at_ms: recv,
                });
            }
            logger.record(EventRecord {
                recv_ms: recv,
                source: "binance",
                event_type: "kline".to_string(),
                key: sym,
                exch_ms: Some(close_ms),
                px: Some(close),
            });
        }
        other => debug!(ws = "binance_ws", etype = other, "unhandled binance event"),
    }
}
