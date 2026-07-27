//! Polymarket market-channel websocket client.
//!
//! Carries the Order #14 lesson in its bones: the read loop has an IDLE WATCHDOG and
//! a client keepalive, because the taker bot lost 45 hours to a half-open socket that
//! never errored and so never resolved `read.next()`. A recorder that dies quietly is
//! worse than one that crashes.
//!
//! Channels observed on this socket (the complete set — there is **no trade-print
//! channel**, which is why executions come from the REST print feed instead):
//!   `best_bid_ask`, `book`, `market_resolved`, `markets`, `new_market`,
//!   `price_change`, `rest_book`, `tick_size_change`.

use std::time::Duration;

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, warn};

/// Every channel we record. `book` is MANDATORY — full-depth reconstruction from
/// `price_change` deltas alone is invalid (match rate decays ~90% → 0% over a token's
/// life), and losing it is what permanently crippled the June archive.
pub const CHANNELS: &[&str] = &[
    "best_bid_ask",
    "book",
    "market_resolved",
    "markets",
    "new_market",
    "price_change",
    "rest_book",
    "tick_size_change",
];

/// One raw event, kept verbatim. Order A0/B2: log inputs, not conclusions — the
/// payload is stored exactly as received so it can be re-parsed years later under a
/// schema we have not thought of yet.
#[derive(Debug, Clone)]
pub struct RawEvent {
    pub recv_ms: i64,
    /// `event_type` from the payload, or `"unknown"`. Used only for routing to the
    /// right sink; the payload itself is never rewritten.
    pub channel: String,
    pub payload: Value,
}

/// Connect, subscribe to `tokens`, and stream raw events to `tx` until shutdown.
/// Returns when the socket ends; the caller owns reconnect policy so it can also
/// record the gap.
pub async fn run_session(
    url: &str,
    tokens: &[String],
    tx: &mpsc::UnboundedSender<RawEvent>,
    shutdown: &mut watch::Receiver<bool>,
    idle_limit: Duration,
    connect_timeout: Duration,
) -> Result<()> {
    let (ws, _) = tokio::time::timeout(connect_timeout, tokio_tungstenite::connect_async(url))
        .await
        .context("ws connect timed out")?
        .context("ws connect failed")?;
    info!(tokens = tokens.len(), "pm ws connected");
    let (mut write, mut read) = ws.split();

    // Payload mirrors the proven recorder so both clients see the same stream.
    let sub = json!({ "assets_ids": tokens, "type": "market", "custom_feature_enabled": true });
    write
        .send(Message::Text(sub.to_string().into()))
        .await
        .context("subscribe send failed")?;

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
                // Order #14 A: a half-open socket produces nothing at all — no error,
                // no close frame, no EOF. Without this branch the loop parks forever.
                let idle = last_frame.elapsed();
                if idle >= idle_limit {
                    warn!(idle_s = idle.as_secs(), "pm ws idle timeout — treating socket as dead");
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
                        for ev in parse_frame(&txt, now_ms()) {
                            if tx.send(ev).is_err() {
                                return Ok(()); // consumer gone
                            }
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

/// Split one text frame into raw events. Frames may be a single object or an array.
#[must_use]
pub fn parse_frame(txt: &str, recv_ms: i64) -> Vec<RawEvent> {
    let Ok(v) = serde_json::from_str::<Value>(txt) else {
        return Vec::new();
    };
    match v {
        Value::Array(items) => items.into_iter().map(|it| to_event(it, recv_ms)).collect(),
        other => vec![to_event(other, recv_ms)],
    }
}

fn to_event(payload: Value, recv_ms: i64) -> RawEvent {
    let channel = payload
        .get("event_type")
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_string();
    RawEvent { recv_ms, channel, payload }
}

#[must_use]
pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// UTC `YYYY-MM-DD` for a millisecond timestamp — the rotation key.
#[must_use]
pub fn utc_day(ts_ms: i64) -> String {
    use chrono::{TimeZone, Utc};
    Utc.timestamp_millis_opt(ts_ms)
        .single()
        .map(|d| d.format("%Y-%m-%d").to_string())
        .unwrap_or_else(|| "1970-01-01".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_and_batched_frames_both_parse() {
        let one = parse_frame(r#"{"event_type":"book","asset_id":"t1"}"#, 100);
        assert_eq!(one.len(), 1);
        assert_eq!(one[0].channel, "book");
        assert_eq!(one[0].recv_ms, 100);

        let many = parse_frame(
            r#"[{"event_type":"price_change"},{"event_type":"best_bid_ask"}]"#,
            200,
        );
        assert_eq!(many.len(), 2, "batched frames must not be dropped");
        assert_eq!(many[0].channel, "price_change");
        assert_eq!(many[1].channel, "best_bid_ask");
    }

    /// An unrecognised channel is still RECORDED (as "unknown"), never discarded —
    /// a new channel appearing is exactly the kind of change we want in the archive.
    #[test]
    fn unknown_channels_are_recorded_not_dropped() {
        let ev = parse_frame(r#"{"event_type":"brand_new_thing","x":1}"#, 1);
        assert_eq!(ev[0].channel, "brand_new_thing");
        let no_type = parse_frame(r#"{"x":1}"#, 1);
        assert_eq!(no_type[0].channel, "unknown", "typeless frames are kept too");
        assert_eq!(no_type[0].payload["x"], 1, "payload preserved verbatim");
    }

    #[test]
    fn malformed_frames_are_skipped_without_panicking() {
        assert!(parse_frame("not json", 1).is_empty());
        assert!(parse_frame("", 1).is_empty());
    }

    #[test]
    fn utc_day_is_the_rotation_key() {
        assert_eq!(utc_day(0), "1970-01-01");
        // 2026-07-27T00:00:00Z and one ms before it must land on different days.
        let midnight = 1_785_110_400_000i64;
        assert_eq!(utc_day(midnight), "2026-07-27");
        assert_eq!(utc_day(midnight - 1), "2026-07-26", "rotation boundary is exact");
    }

    #[test]
    fn channel_list_matches_the_recorded_feed() {
        // The complete observed set. `book` is mandatory; there is deliberately no
        // trade-print channel here — executions come from the REST print feed.
        assert!(CHANNELS.contains(&"book"));
        assert!(CHANNELS.contains(&"price_change"));
        assert!(CHANNELS.contains(&"rest_book"));
        assert!(!CHANNELS.contains(&"last_trade_price"), "no such channel exists on this socket");
        assert_eq!(CHANNELS.len(), 8);
    }
}
