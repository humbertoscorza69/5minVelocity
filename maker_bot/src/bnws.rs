//! Binance 1s klines for every discovered asset (order B2).
//!
//! 1s is the granularity that reproduces the bot's `vol60` bit-for-bit and the only
//! one fine enough for a 30–240s horizon — a 1m bar cannot express either.
//!
//! Same idle watchdog as everything else in this crate: Order #14 cost us 45 hours to
//! a half-open Binance socket specifically, so this client does not get to repeat it.

use std::time::Duration;

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, warn};

use crate::pmws::{RawEvent, now_ms};

/// Map an up/down asset label ("btc") to a Binance spot symbol ("btcusdt").
#[must_use]
pub fn symbol_for(asset: &str) -> String {
    format!("{}usdt", asset.to_lowercase())
}

/// Stream 1s klines for `symbols` into `tx` as raw events, channel = `kline_1s`.
pub async fn run_session(
    url: &str,
    symbols: &[String],
    tx: &mpsc::UnboundedSender<RawEvent>,
    shutdown: &mut watch::Receiver<bool>,
    idle_limit: Duration,
    connect_timeout: Duration,
) -> Result<()> {
    let (ws, _) = tokio::time::timeout(connect_timeout, tokio_tungstenite::connect_async(url))
        .await
        .context("binance connect timed out")?
        .context("binance connect failed")?;
    info!(symbols = symbols.len(), "binance ws connected");
    let (mut write, mut read) = ws.split();

    let streams: Vec<String> = symbols.iter().map(|s| format!("{s}@kline_1s")).collect();
    let sub = json!({ "method": "SUBSCRIBE", "params": streams, "id": 1 });
    write
        .send(Message::Text(sub.to_string().into()))
        .await
        .context("binance subscribe failed")?;

    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut keepalive = tokio::time::interval(Duration::from_secs(15));
    keepalive.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await;
    keepalive.tick().await;
    let mut last_frame = tokio::time::Instant::now();

    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    let _ = write.send(Message::Close(None)).await;
                    return Ok(());
                }
            }
            _ = ticker.tick() => {
                let idle = last_frame.elapsed();
                if idle >= idle_limit {
                    warn!(idle_s = idle.as_secs(), "binance ws idle timeout");
                    anyhow::bail!("idle timeout: no frame for {}s", idle.as_secs());
                }
            }
            _ = keepalive.tick() => {
                if write.send(Message::Ping(Default::default())).await.is_err() {
                    anyhow::bail!("keepalive ping send failed");
                }
            }
            msg = read.next() => {
                last_frame = tokio::time::Instant::now();
                match msg {
                    Some(Ok(Message::Text(txt))) => {
                        if let Ok(v) = serde_json::from_str::<Value>(&txt)
                            && v.get("e").and_then(Value::as_str) == Some("kline")
                            && tx.send(RawEvent {
                                recv_ms: now_ms(),
                                channel: "kline_1s".into(),
                                payload: v,
                            }).is_err()
                        {
                            return Ok(());
                        }
                    }
                    Some(Ok(Message::Ping(p))) => { let _ = write.send(Message::Pong(p)).await; }
                    Some(Ok(Message::Close(f))) => anyhow::bail!("server close: {f:?}"),
                    Some(Ok(_)) => {}
                    Some(Err(e)) => anyhow::bail!("read error: {e}"),
                    None => anyhow::bail!("stream ended"),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn symbols_cover_the_discovered_universe() {
        assert_eq!(symbol_for("btc"), "btcusdt");
        assert_eq!(symbol_for("ETH"), "ethusdt");
        // The two assets the June inventory never had.
        assert_eq!(symbol_for("sol"), "solusdt");
        assert_eq!(symbol_for("xrp"), "xrpusdt");
    }
}
