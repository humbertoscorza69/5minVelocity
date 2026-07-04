//! Polymarket CLOB websocket client — public `market` channel (read-only).
//!
//! Connects to `<base>/market` and subscribes to the configured `assets_ids`
//! (token ids). The subscribe is re-sent on every (re)connect; the server then
//! replies with a fresh `book` snapshot per token, which REPLACES our cache —
//! that is the reconnect resync. Incremental `price_change` events update levels
//! in place.
//!
//! CAVEAT (surfaced to the operator): Polymarket 5m/15m markets roll
//! continuously, so a static config token-id list goes stale within minutes.
//! For a live verify / 2h baseline, fetch fresh token ids immediately before the
//! run. The Binance feed has no such constraint.
//!
//! Strictly WS-only: no REST. Strictly read-only: no orders.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::sync::watch;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, info};

use super::{ReconnectPolicy, SessionEnd, connect_with_timeout, run_with_reconnect};
use crate::config::Config;
use crate::discovery::prune_books;
use crate::events::{EventLogger, EventRecord};
use crate::state::{EventBbo, Shared, now_ms};

/// Entry point: supervise the Polymarket market session under the reconnect
/// policy. The active token set is supplied dynamically via `token_rx` (the
/// market-discovery task publishes 5m/15m markets as they roll); a change to the
/// set ends the current session with [`SessionEnd::Refresh`] so the supervisor
/// reconnects with the new list and prunes books for markets that rolled off.
pub async fn run(
    state: Shared,
    cfg: Config,
    shutdown: watch::Receiver<bool>,
    logger: EventLogger,
    mut token_rx: watch::Receiver<Arc<Vec<String>>>,
) {
    let base = cfg
        .connections
        .polymarket_ws_url
        .trim_end_matches('/')
        .to_string();
    let url = format!("{base}/market");
    let policy = ReconnectPolicy::from_config(cfg.connections.reconnect_max_attempts_per_60s);
    let connect_timeout = Duration::from_secs(cfg.connections.connect_timeout_s);
    let alert_dir = cfg.paths.alert_dir.clone();
    let loop_state = state.clone();

    run_with_reconnect(
        "polymarket_ws",
        policy,
        loop_state,
        alert_dir,
        shutdown.clone(),
        move |reconnect_no| {
            // Snapshot the current set and mark it seen, so the session's
            // `token_rx.changed()` only fires on the NEXT discovery update.
            let tokens = token_rx.borrow_and_update().clone();
            let mut session_rx = token_rx.clone();
            let url = url.clone();
            let state = state.clone();
            let logger = logger.clone();
            let mut shutdown = shutdown.clone();
            async move {
                if tokens.is_empty() {
                    // No markets discovered yet: idle until discovery populates the
                    // set (→ Refresh, reconnect with it) or shutdown.
                    tokio::select! {
                        _ = shutdown.changed() => SessionEnd::Shutdown,
                        r = session_rx.changed() => {
                            if r.is_err() { SessionEnd::Shutdown } else { SessionEnd::Refresh }
                        }
                    }
                } else {
                    // Drop books for markets that rolled off before (re)subscribing.
                    prune_books(&state, &tokens);
                    session(
                        reconnect_no,
                        &url,
                        &tokens,
                        &state,
                        &logger,
                        &mut shutdown,
                        &mut session_rx,
                        connect_timeout,
                    )
                    .await
                }
            }
        },
    )
    .await;
}

/// One connect → subscribe → read-until-end cycle.
#[allow(clippy::too_many_arguments)] // cohesive session params; a struct would not add clarity
async fn session(
    reconnect_no: u32,
    url: &str,
    tokens: &[String],
    state: &Shared,
    logger: &EventLogger,
    shutdown: &mut watch::Receiver<bool>,
    token_rx: &mut watch::Receiver<Arc<Vec<String>>>,
    connect_timeout: Duration,
) -> SessionEnd {
    let ws = match connect_with_timeout(url, connect_timeout).await {
        Ok(ws) => ws,
        Err(reason) => return SessionEnd::Lost(reason),
    };

    if reconnect_no > 0 {
        info!(
            ws = "polymarket_ws",
            attempt = reconnect_no,
            tokens = tokens.len(),
            "ws_reconnect_complete"
        );
    } else {
        info!(ws = "polymarket_ws", tokens = tokens.len(), url, "ws_connected");
    }
    state.polymarket_connected.store(true, Ordering::Relaxed);

    let (mut write, mut read) = ws.split();

    // (Re)subscribe → server resends a fresh book snapshot per token (resync).
    // Payload mirrors the Python recorder (incl. `custom_feature_enabled`) so the
    // two clients see the same stream → clean baseline comparison.
    let sub = json!({ "assets_ids": tokens, "type": "market", "custom_feature_enabled": true });
    if let Err(e) = write.send(Message::Text(sub.to_string().into())).await {
        state.polymarket_connected.store(false, Ordering::Relaxed);
        return SessionEnd::Lost(format!("subscribe send failed: {e}"));
    }

    // Client-initiated keepalive: proactively ping every 15s so an idle/half-open
    // connection (or a proxy that expects client liveness) can't silently rot into
    // a "connection reset". The server Pongs (ignored below). Cheap insurance on top
    // of the fast-reconnect supervisor.
    let mut keepalive = tokio::time::interval(Duration::from_secs(15));
    keepalive.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let end = loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    let _ = write.send(Message::Close(None)).await;
                    break SessionEnd::Shutdown;
                }
            }
            _ = keepalive.tick() => {
                if write.send(Message::Ping(Default::default())).await.is_err() {
                    break SessionEnd::Lost("keepalive ping send failed".into());
                }
            }
            changed = token_rx.changed() => {
                // Discovery published a new token set, or the discovery task ended.
                let _ = write.send(Message::Close(None)).await;
                if changed.is_err() {
                    // Sender gone (shutdown or fault): stop rather than spin.
                    break SessionEnd::Shutdown;
                }
                break SessionEnd::Refresh;
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

    state.polymarket_connected.store(false, Ordering::Relaxed);
    end
}

/// The CLOB channel sends either a single event object or an array of them.
fn handle_text(txt: &str, state: &Shared, logger: &EventLogger) {
    let v: Value = match serde_json::from_str(txt) {
        Ok(v) => v,
        Err(_) => {
            state.counters.parse_errors.fetch_add(1, Ordering::Relaxed);
            return;
        }
    };
    match v {
        Value::Array(items) => {
            for it in &items {
                handle_event(it, state, logger);
            }
        }
        other => handle_event(&other, state, logger),
    }
}

pub(crate) fn handle_event(v: &Value, state: &Shared, logger: &EventLogger) {
    let etype = v.get("event_type").and_then(Value::as_str).unwrap_or("");
    if etype.is_empty() {
        // Control frame / PONG / unknown shape — count and move on.
        state.counters.polymarket_other.fetch_add(1, Ordering::Relaxed);
        return;
    }

    state.counters.polymarket_msgs.fetch_add(1, Ordering::Relaxed);
    let recv = now_ms();
    let asset = v
        .get("asset_id")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let exch_ms = v.get("timestamp").and_then(parse_ms);

    match etype {
        "book" => {
            // Full snapshot. Event-BBO = top of each side: best_ask = asks[-1],
            // best_bid = bids[-1] (Python `best_levels`: asks DESCENDING, bids
            // ASCENDING → the LAST element is the best). Verified vs the recorder.
            let bids = parse_levels(v.get("bids"));
            let asks = parse_levels(v.get("asks"));
            let ev_ask = last_price(&asks);
            let ev_bid = last_price(&bids);
            update_bbo(state, &asset, ev_ask, ev_bid, exch_ms.unwrap_or(recv));
            state.counters.polymarket_book.fetch_add(1, Ordering::Relaxed);
            logger.record(EventRecord {
                recv_ms: recv,
                source: "polymarket",
                event_type: "book".to_string(),
                key: asset,
                exch_ms,
                px: mid_of(ev_ask, ev_bid),
            });
        }
        "price_change" => {
            // The asset_id lives PER CHANGE inside `price_changes` (or legacy
            // `changes`); the event's top level carries only market/condition_id.
            // Apply each change to ITS token's book and log one record per change,
            // keyed by that token. (Do NOT key off the top-level asset_id — it is
            // absent here, which previously routed every change into book[""].)
            let changes = v.get("price_changes").or_else(|| v.get("changes"));
            if let Some(arr) = changes.and_then(Value::as_array) {
                for ch in arr {
                    apply_price_change(ch, state, logger, recv, exch_ms);
                }
            } else if let Some(ch) = changes {
                // Some payloads carry a single (non-arrayed) change object.
                apply_price_change(ch, state, logger, recv, exch_ms);
            } else {
                // No changes payload at all — count, never mis-attribute.
                state.counters.polymarket_other.fetch_add(1, Ordering::Relaxed);
            }
        }
        "best_bid_ask" => {
            // BBO-only event for one token. Update the event-BBO directly.
            let ev_ask = v.get("best_ask").and_then(parse_f64);
            let ev_bid = v.get("best_bid").and_then(parse_f64);
            update_bbo(state, &asset, ev_ask, ev_bid, exch_ms.unwrap_or(recv));
            state.counters.polymarket_other.fetch_add(1, Ordering::Relaxed);
            logger.record(EventRecord {
                recv_ms: recv,
                source: "polymarket",
                event_type: "best_bid_ask".to_string(),
                key: asset,
                exch_ms,
                px: mid_of(ev_ask, ev_bid),
            });
        }
        other => {
            state.counters.polymarket_other.fetch_add(1, Ordering::Relaxed);
            debug!(ws = "polymarket_ws", etype = other, "other polymarket event");
            logger.record(EventRecord {
                recv_ms: recv,
                source: "polymarket",
                event_type: other.to_string(),
                key: asset,
                exch_ms,
                px: None,
            });
        }
    }
}

/// Apply ONE price_change entry to the book of its OWN `asset_id`, then log it.
/// Each entry self-describes its token; the event's top level only carries the
/// market/condition_id, so attribution must come from the entry, not the top.
fn apply_price_change(
    ch: &Value,
    state: &Shared,
    logger: &EventLogger,
    recv: i64,
    exch_ms: Option<i64>,
) {
    let asset = match ch.get("asset_id").and_then(Value::as_str) {
        Some(a) if !a.is_empty() => a.to_string(),
        _ => {
            // A change with no asset_id can't be attributed — count, never guess.
            state.counters.parse_errors.fetch_add(1, Ordering::Relaxed);
            return;
        }
    };
    // The pc carries the resulting BBO directly (`best_bid`/`best_ask`) — exactly
    // what the Python uses. It SKIPS the change when either side is missing
    // (`if pc_ask is None or pc_bid is None: continue`); mirror that.
    let ev_ask = ch.get("best_ask").and_then(parse_f64);
    let ev_bid = ch.get("best_bid").and_then(parse_f64);
    if ev_ask.is_none() || ev_bid.is_none() {
        return;
    }
    update_bbo(state, &asset, ev_ask, ev_bid, exch_ms.unwrap_or(recv));
    state
        .counters
        .polymarket_price_change
        .fetch_add(1, Ordering::Relaxed);
    logger.record(EventRecord {
        recv_ms: recv,
        source: "polymarket",
        event_type: "price_change".to_string(),
        key: asset,
        exch_ms,
        px: mid_of(ev_ask, ev_bid),
    });
}

/// Store the event-reported BBO for `token` — the single source of truth for
/// prices (decisions, sizing, REGLA C, event-log mid).
fn update_bbo(
    state: &Shared,
    token: &str,
    best_ask: Option<f64>,
    best_bid: Option<f64>,
    ts_ms: i64,
) {
    state
        .bbo
        .insert(token.to_string(), EventBbo { best_ask, best_bid, ts_ms });
    // Feed the mid-ring for the book-asleep flag (LOG-only). Keep ~6s / 64 samples.
    // Stamp with the LOCAL clock (not the event's ts_ms, which may be Polymarket's
    // exchange time) so the window math in book_asleep — which uses the decision
    // loop's local now_ms — is on the same clock. A skew here was making the flag
    // null on ~100% of intents.
    if let Some(mid) = mid_of(best_ask, best_bid) {
        let local = crate::state::now_ms();
        let mut r = state.mid_ring.entry(token.to_string()).or_default();
        r.push_back((local, mid));
        let cutoff = local - 6000;
        while r.front().map(|(t, _)| *t < cutoff).unwrap_or(false) || r.len() > 64 {
            r.pop_front();
        }
    }
}

/// Mid price when both sides are present.
fn mid_of(ask: Option<f64>, bid: Option<f64>) -> Option<f64> {
    match (ask, bid) {
        (Some(a), Some(b)) => Some((a + b) / 2.0),
        _ => None,
    }
}

/// The LAST price in a `[(price, size)]` slice — the best level by Polymarket's
/// book convention (asks DESCENDING, bids ASCENDING → `[-1]` is best). Matches the
/// Python `best_levels` exactly (verified against the recorder book ordering).
fn last_price(levels: &[(f64, f64)]) -> Option<f64> {
    levels.last().map(|(p, _)| *p)
}

/// Parse a `[{price, size}, …]` levels array (values may be strings or numbers).
fn parse_levels(v: Option<&Value>) -> Vec<(f64, f64)> {
    let Some(arr) = v.and_then(Value::as_array) else {
        return Vec::new();
    };
    arr.iter()
        .filter_map(|lvl| {
            let p = lvl.get("price").and_then(parse_f64)?;
            let s = lvl.get("size").and_then(parse_f64)?;
            Some((p, s))
        })
        .collect()
}

/// Accept a float either as a JSON number or a quoted string.
fn parse_f64(v: &Value) -> Option<f64> {
    v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok()))
}

/// Accept a millisecond timestamp as a JSON integer or a quoted string.
fn parse_ms(v: &Value) -> Option<i64> {
    v.as_i64().or_else(|| v.as_str().and_then(|s| s.parse().ok()))
}

#[cfg(test)]
mod tests {
    use super::handle_text;
    use crate::events;
    use crate::state::{SharedState, now_ms};
    use std::sync::atomic::Ordering;

    /// A price_change message carrying multiple changes with DIFFERENT asset_ids
    /// must route each change to its own token's event-BBO — never to bbo[""].
    /// The BBO comes from the per-change `best_bid`/`best_ask` (Python semantics),
    /// NOT the `price`/`size`/`side` level (which the retired delta-book used).
    #[tokio::test]
    async fn price_change_routes_each_change_to_its_own_token() {
        let state = SharedState::new();
        let dir = std::env::temp_dir().join(format!("rb_pc_{}", now_ms()));
        std::fs::create_dir_all(&dir).unwrap();
        let (logger, _h) = events::spawn(&dir.join("ts.jsonl"), true).unwrap();

        // Real Polymarket shape: top-level `market` + per-change `asset_id`, each
        // carrying the resulting `best_bid`/`best_ask`.
        let msg = r#"{"event_type":"price_change","market":"0xabc","timestamp":"1780012814293",
            "price_changes":[
                {"asset_id":"111","price":"0.16","size":"100","side":"BUY","best_bid":"0.40","best_ask":"0.41"},
                {"asset_id":"222","price":"0.70","size":"50","side":"SELL","best_bid":"0.59","best_ask":"0.60"}
            ]}"#;
        handle_text(msg, &state, &logger);

        assert!(!state.bbo.contains_key(""), "bbo[\"\"] must NOT exist");
        let b111 = *state.bbo.get("111").unwrap();
        assert_eq!(b111.best_bid, Some(0.40));
        assert_eq!(b111.best_ask, Some(0.41));
        let b222 = *state.bbo.get("222").unwrap();
        assert_eq!(b222.best_bid, Some(0.59));
        assert_eq!(b222.best_ask, Some(0.60));
        // Two changes ⇒ two price_change events counted (per-change semantics).
        assert_eq!(state.counters.polymarket_price_change.load(Ordering::Relaxed), 2);

        drop(logger);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A `book` snapshot's BBO is the LAST element of each side (asks DESCENDING,
    /// bids ASCENDING → `[-1]` is best), keyed off the top-level asset_id.
    #[tokio::test]
    async fn book_snapshot_bbo_is_top_of_each_side() {
        let state = SharedState::new();
        let dir = std::env::temp_dir().join(format!("rb_bk_{}", now_ms()));
        std::fs::create_dir_all(&dir).unwrap();
        let (logger, _h) = events::spawn(&dir.join("ts.jsonl"), true).unwrap();

        // asks descending (best = last = 0.60), bids ascending (best = last = 0.40).
        let msg = r#"{"event_type":"book","asset_id":"333","timestamp":"1","hash":"h",
            "bids":[{"price":"0.38","size":"100"},{"price":"0.40","size":"100"}],
            "asks":[{"price":"0.62","size":"100"},{"price":"0.60","size":"100"}]}"#;
        handle_text(msg, &state, &logger);

        assert!(state.bbo.contains_key("333"));
        assert!(!state.bbo.contains_key(""));
        let b = *state.bbo.get("333").unwrap();
        assert_eq!(b.best_ask, Some(0.60));
        assert_eq!(b.best_bid, Some(0.40));
        assert_eq!(b.mid(), Some(0.50));

        drop(logger);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A `best_bid_ask` event updates the token's BBO directly (Phase 1 ignored these).
    #[tokio::test]
    async fn best_bid_ask_updates_bbo() {
        let state = SharedState::new();
        let dir = std::env::temp_dir().join(format!("rb_bba_{}", now_ms()));
        std::fs::create_dir_all(&dir).unwrap();
        let (logger, _h) = events::spawn(&dir.join("ts.jsonl"), true).unwrap();

        let msg = r#"{"event_type":"best_bid_ask","asset_id":"444","timestamp":"5",
            "best_bid":"0.24","best_ask":"0.41","spread":"0.17"}"#;
        handle_text(msg, &state, &logger);

        let b = *state.bbo.get("444").unwrap();
        assert_eq!(b.best_bid, Some(0.24));
        assert_eq!(b.best_ask, Some(0.41));
        assert_eq!(b.ts_ms, 5);

        drop(logger);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
