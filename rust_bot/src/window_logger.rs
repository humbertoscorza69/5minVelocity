//! Phase 6 -- `book_window_around_trigger` logger.
//!
//! GOAL: measure the Polymarket repricing window AT EVERY 5-bps Binance trigger,
//! independently of whether the decision engine chose to trade. The repricing
//! window was historically measured ONLY at 25 bps (DECISIONS.md 2026-05-19:
//! 1-4 s median across deep/medium/thin liquidity tiers) -- that is the regime
//! the lag-arb thesis was sized against. The OPERATING regime is 5 bps. This
//! gap is the single largest unmeasured assumption in the strategy: nothing in
//! the historical evidence base proves the 5-bps window has the same shape as
//! the 25-bps window. This module closes the gap in-vivo: every trigger
//! produces one direct measurement of "how much, and how soon, did Polymarket
//! reprice after the Binance move".
//!
//! MECHANIC: on each trigger detected by `SignalEngine::on_kline`, snapshot the
//! PM book at offsets `{0, 500, 1000, 3000, 10000}` ms post-trigger. The 0 ms
//! anchor establishes the pre-reprice book; the 500/1000 ms points cover "did
//! it move before our latency budget (~420 ms Dublin) could arrive"; the
//! 3000/10000 ms tail captures the longer end of the 25-bps measured curve to
//! check whether 5 bps lives in the same regime. ONE `book_window_around_trigger`
//! oplog event per trigger; tokens are the bet (Up or Down) markets the trigger
//! projected onto (5 m, 15 m), tagged with the decision engine's accept/reject
//! verdict so the window can be sliced by "did we actually trade this trigger".
//!
//! NON-INVASIVE: the logger runs in a background `tokio::spawn` and never
//! touches the decision/execution path. A snapshot is a single DashMap lookup
//! against `state.bbo`; cost is negligible. If the bot shuts down mid-sequence
//! (e.g. after `max_trades_per_session` triggers shutdown), in-flight loggers
//! are dropped (the JoinHandle is not retained) -- partial windows are simply
//! absent from the oplog, never half-written, since the emit is at the end.

#![allow(dead_code)] // wired by trading_loop::run_decision_task in this same change

use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};
use tokio::task::JoinHandle;
use tokio::time::sleep;

use crate::decision::{Action, TriggerDecision};
use crate::oplog::OpLog;
use crate::signal::{Direction, Signal, Trigger};
use crate::state::SharedState;

/// Offsets (ms post-trigger) at which the PM book is snapshotted. The chosen
/// values bracket the measured 25-bps regime (median 1-4 s, DECISIONS.md
/// 2026-05-19) so the 5-bps curve can be compared:
///   * `0`     -- anchor: pre-reprice book at the trigger instant.
///   * `500`   -- "did it move BEFORE our Dublin latency budget (~420 ms) could
///                arrive". If non-zero here, we're in a sub-budget regime.
///   * `1000`  -- the 25-bps deep-liquidity median.
///   * `3000`  -- the 25-bps thin-liquidity median.
///   * `10000` -- the long tail; the 25-bps p99 was ~281 s on the deprecated
///                60-s-sampled measure but 10 s is enough to see the full
///                trade-level repricing on the operating timescale.
pub const SNAPSHOT_OFFSETS_MS: &[u64] = &[0, 500, 1000, 3000, 10000];

/// One market the trigger projects onto (per interval, 5 m or 15 m). Carries
/// the decision-engine verdict so a downstream analysis can stratify the
/// repricing curve by "trade was accepted" vs "trade was rejected".
#[derive(Debug, Clone)]
pub struct WindowTokenTarget {
    pub interval: String,
    pub bet_token_id: String,
    pub opposite_token_id: String,
    /// True iff the decision engine emitted a `Fire` for this interval.
    pub accepted: bool,
    /// Reject reason (Q2-Q4 / staleness / phantom / no_qualifying_market /
    /// not_qualified) when `accepted=false`. `None` only when accepted.
    pub reject_reason: Option<String>,
}

/// All data needed to log one window. Built from the trigger + signals +
/// decision so this module stays a pure logger (no decision re-derivation).
#[derive(Debug, Clone)]
pub struct WindowInput {
    pub trigger_ts: i64,
    pub asset: String,
    pub return_bps: f64,
    pub direction: Direction,
    pub window_s: u8,
    pub tokens: Vec<WindowTokenTarget>,
    /// G6 (recorder latency): bot wall-clock (ms) at the moment the Binance WS
    /// client delivered the finalized kline that produced this trigger. With
    /// `trigger_ts` (the kline open-second) this lets downstream threshold
    /// analysis (5/7/10/15 bps decision) measure the SIGNAL ARRIVAL LATENCY
    /// inline -- no cross-reference to `kline_received` events by `t_s`
    /// needed. Latency formula: `received_at_ms - (trigger_ts + 1) * 1000`.
    pub kline_received_at_ms: i64,
}

/// Snapshot the BBO of `token` from the live state; returns a JSON object with
/// `offset_ms`, `ask`, `bid`, `midpoint`, `book_ts_ms`. Missing token => all
/// nulls + `book_ts_ms = 0` (so the analysis can distinguish "we never had a
/// book" from "book is stale").
pub fn snapshot_at(state: &SharedState, token: &str, offset_ms: u64) -> Value {
    let bbo = state.bbo.get(token).map(|b| *b);
    let (ask, bid, ts) = match bbo {
        Some(b) => (b.best_ask, b.best_bid, b.ts_ms),
        None => (None, None, 0i64),
    };
    let midpoint = match (ask, bid) {
        (Some(a), Some(b)) => Some((a + b) / 2.0),
        _ => None,
    };
    json!({
        "offset_ms": offset_ms,
        "ask": ask,
        "bid": bid,
        "midpoint": midpoint,
        "book_ts_ms": ts,
    })
}

/// Build a `WindowInput` from the trigger + signals + decision produced by
/// `trading_loop::process_kline`. The decision's actions are scanned to find
/// the accept/reject verdict per interval; intervals with neither `Fire` nor
/// `Reject` (e.g. dropped before the per-interval loop reached them, like a
/// `no_book` count without a sample) are tagged `not_qualified` so the
/// downstream stratification still sees every projection.
///
/// G6: `kline_received_at_ms` is the bot's wall-clock at which the WS handler
/// delivered the kline that produced this trigger. Captured INLINE for
/// threshold analysis (signal arrival latency).
pub fn build_input_from_decision(
    trigger: &Trigger,
    signals: &[Signal],
    decision: &TriggerDecision,
    kline_received_at_ms: i64,
) -> WindowInput {
    let direction = if trigger.ret_bps > 0.0 {
        Direction::Up
    } else {
        Direction::Down
    };

    let tokens: Vec<WindowTokenTarget> = signals
        .iter()
        .map(|s| {
            let (accepted, reject_reason) = decision
                .actions
                .iter()
                .find_map(|a| match a {
                    Action::Fire { interval, .. } if interval == &s.interval => Some((true, None)),
                    Action::Reject {
                        interval, reason, ..
                    } if interval == &s.interval => Some((false, Some(reason.clone()))),
                    // CloseOpposite is REGLA C (off by default) -- treat as accepted
                    // since a real order DOES fire (the close), so the trigger DID
                    // produce trading activity on this interval.
                    Action::CloseOpposite { interval, .. } if interval == &s.interval => {
                        Some((true, Some("regla_c_close_opposite".to_string())))
                    }
                    _ => None,
                })
                .unwrap_or((false, Some("not_qualified".to_string())));
            WindowTokenTarget {
                interval: s.interval.clone(),
                bet_token_id: s.bet_token_id.clone(),
                opposite_token_id: s.opposite_token_id.clone(),
                accepted,
                reject_reason,
            }
        })
        .collect();

    WindowInput {
        trigger_ts: trigger.trigger_ts,
        asset: trigger.asset.clone(),
        return_bps: trigger.ret_bps,
        direction,
        window_s: trigger.window_s,
        tokens,
        kline_received_at_ms,
    }
}

/// G6 PURE: compute the signal arrival latency (ms from kline-close to bot
/// ingest). Used both by the emit path (inline in the JSON) and by tests.
/// `saturating_sub` so clock-skew weirdness doesn't underflow into garbage.
#[must_use]
pub fn signal_arrival_latency_ms(trigger_ts_s: i64, kline_received_at_ms: i64) -> i64 {
    let kline_close_ms = trigger_ts_s.saturating_add(1).saturating_mul(1000);
    kline_received_at_ms.saturating_sub(kline_close_ms)
}

/// Spawn a background tokio task that snapshots the PM book at the configured
/// offsets and emits ONE `book_window_around_trigger` event when complete.
/// Returns the `JoinHandle` so callers (mainly tests) can await completion;
/// the production caller drops it (fire-and-forget).
pub fn spawn_window_logger(
    state: Arc<SharedState>,
    oplog: Arc<OpLog>,
    input: WindowInput,
) -> JoinHandle<()> {
    spawn_window_logger_with_offsets(state, oplog, input, SNAPSHOT_OFFSETS_MS.to_vec())
}

/// Same as `spawn_window_logger` but with explicit offsets, for tests that
/// don't want to wait the full 10-second sequence.
pub fn spawn_window_logger_with_offsets(
    state: Arc<SharedState>,
    oplog: Arc<OpLog>,
    input: WindowInput,
    offsets: Vec<u64>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        // Even with zero tokens we still emit one event so the trigger is
        // attested in the oplog (e.g. trigger in last 30 s of resolution =>
        // no qualifying market => signals empty => tokens empty). G6: still
        // include the kline arrival latency for threshold analysis -- a
        // trigger with no scope market is just as latency-informative as one
        // that fired a trade.
        if input.tokens.is_empty() {
            oplog.sys(
                "book_window_around_trigger",
                json!({
                    "trigger_ts": input.trigger_ts,
                    "asset": input.asset,
                    "return_bps": input.return_bps,
                    "direction": input.direction.as_str(),
                    "window_s": input.window_s,
                    "tokens": Vec::<Value>::new(),
                    "offsets_ms": offsets,
                    "kline_received_at_ms": input.kline_received_at_ms,
                    "signal_arrival_latency_ms": signal_arrival_latency_ms(
                        input.trigger_ts, input.kline_received_at_ms,
                    ),
                }),
            );
            return;
        }

        // Per-token snapshot accumulators -- one Vec<Value> per token.
        let mut per_token: Vec<(WindowTokenTarget, Vec<Value>)> = input
            .tokens
            .iter()
            .map(|t| (t.clone(), Vec::with_capacity(offsets.len())))
            .collect();

        let mut last_ms: u64 = 0;
        for &offset_ms in offsets.iter() {
            // Sleep the delta to the next offset. First iter with offset=0 is
            // immediate (no sleep). tokio::time::sleep cooperates with the
            // runtime; precision is ~ms, fine for repricing-window analysis.
            let delta = offset_ms.saturating_sub(last_ms);
            if delta > 0 {
                sleep(Duration::from_millis(delta)).await;
            }
            for (t, snaps) in per_token.iter_mut() {
                snaps.push(snapshot_at(&state, &t.bet_token_id, offset_ms));
            }
            last_ms = offset_ms;
        }

        // Build payload and emit ONE event with the full sequence.
        let tokens_payload: Vec<Value> = per_token
            .into_iter()
            .map(|(t, snaps)| {
                json!({
                    "interval": t.interval,
                    "bet_token_id": t.bet_token_id,
                    "opposite_token_id": t.opposite_token_id,
                    "accepted": t.accepted,
                    "reject_reason": t.reject_reason,
                    "snapshots": snaps,
                })
            })
            .collect();

        oplog.sys(
            "book_window_around_trigger",
            json!({
                "trigger_ts": input.trigger_ts,
                "asset": input.asset,
                "return_bps": input.return_bps,
                "direction": input.direction.as_str(),
                "window_s": input.window_s,
                "tokens": tokens_payload,
                "offsets_ms": offsets,
                // G6: arrival latency captured inline -- no cross-reference to
                // kline_received events needed for threshold analysis.
                "kline_received_at_ms": input.kline_received_at_ms,
                "signal_arrival_latency_ms": signal_arrival_latency_ms(
                    input.trigger_ts, input.kline_received_at_ms,
                ),
            }),
        );
    })
}

// ============================ edge15 instrumentation ============================
//
// PER-TRADE marks logger for the BTC:5m edge15 pre-registration. Same proven
// fire-and-forget mechanic as `spawn_window_logger` (reads `state.bbo`, emits ONE
// oplog event, never touches decision/execution/exit), but:
//   * spawned ONCE per guard-approved Open (a TRADE), not per trigger;
//   * snapshots at {0, 15, 20, 25} s post-DECISION (the offline `decision_us + 15s`
//     analog), so edge15 = shares*(mid15 - entry_ask) - fee_in and the post-15s
//     drift can be measured IN VIVO on the bot's REAL trades (the engine that
//     validates == the engine that operates; avoids reconstruction divergence).
//
// PASSIVE: the bot still exits at 120 s via `run_exit_task` (untouched). This task
// only observes. `book_ts_ms` in each snapshot records the exact book time, so
// offline reconciliation is precise regardless of sleep jitter.

/// Offsets (ms post-DECISION) at which the bet token's book is snapshotted for a
/// trade. `0` = entry-mid reference (= exp3 `mid0`); `15000` = the primary edge15
/// mark (`mid15`); `20000`/`25000` feed the early-exit shadow contingency
/// (maker@ask +15-20 s, fallback taker ~25 s). Mirrors exp3_exit_regime.py.
pub const EDGE_MARK_OFFSETS_MS: &[u64] = &[0, 15_000, 20_000, 25_000];

/// Everything needed to log one trade's edge15 marks. Built from the decision
/// `ctx` + `intent` at the guard-approved Open, so this stays a pure logger.
#[derive(Debug, Clone)]
pub struct EdgeMarksInput {
    /// Join key to the `paper_open`/fill event (so offline keeps marks only for
    /// trades that ACTUALLY opened — a slipped/aborted live Open has no fill and
    /// is dropped by the join).
    pub signal_id: String,
    /// The bet (held) token whose book trajectory is the edge.
    pub token_id: String,
    pub asset: String,
    pub interval: String,
    pub side: String,
    /// Decision wall-clock (ms) = `ctx.t_decision_ms`. Marks anchor here: each
    /// offset target = `decision_ts_ms + offset` (the in-vivo analog of the
    /// offline `decision_us + 15s`).
    pub decision_ts_ms: i64,
    /// Ask at decision (`ctx.ask_at_signal`) — the entry-price anchor. Makes the
    /// event self-contained: `edge15 = shares*(mid15 - entry_ask) - fee_in`.
    pub entry_ask: f64,
    /// Shares of the lot (`intent.shares`).
    pub shares: f64,
    /// Trigger magnitude (`ctx.binance_ret_bps`) — for the scaled-target analysis.
    pub ret_bps: f64,
}

/// Spawn a background task that snapshots the bet token's book at the edge15
/// offsets and emits ONE `trade_edge_marks` event. Returns the `JoinHandle` so
/// tests can await; the production caller drops it (fire-and-forget).
pub fn spawn_edge_marks_logger(
    state: Arc<SharedState>,
    oplog: Arc<OpLog>,
    input: EdgeMarksInput,
) -> JoinHandle<()> {
    spawn_edge_marks_logger_with_offsets(state, oplog, input, EDGE_MARK_OFFSETS_MS.to_vec())
}

/// Same as [`spawn_edge_marks_logger`] but with explicit offsets, for tests that
/// don't want to wait the full 25-second sequence.
pub fn spawn_edge_marks_logger_with_offsets(
    state: Arc<SharedState>,
    oplog: Arc<OpLog>,
    input: EdgeMarksInput,
    offsets: Vec<u64>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut snaps: Vec<Value> = Vec::with_capacity(offsets.len());
        for &offset_ms in offsets.iter() {
            // Anchor to the DECISION wall-clock (absolute), not spawn time: the
            // mark must land at decision + offset regardless of dispatch latency.
            // `snapshot_at` records `book_ts_ms`, so the exact book time is
            // auditable offline even if the sleep wakes a few ms late.
            let target = input.decision_ts_ms.saturating_add(offset_ms as i64);
            let wait = target.saturating_sub(crate::state::now_ms());
            if wait > 0 {
                sleep(Duration::from_millis(wait as u64)).await;
            }
            snaps.push(snapshot_at(&state, &input.token_id, offset_ms));
        }
        // ONE event with the full mark sequence (emit at the end: never half-
        // written if the bot shuts down mid-sequence).
        oplog.sys(
            "trade_edge_marks",
            json!({
                "signal_id": input.signal_id,
                "token_id": input.token_id,
                "asset": input.asset,
                "interval": input.interval,
                "side": input.side,
                "decision_ts_ms": input.decision_ts_ms,
                "entry_ask": input.entry_ask,
                "shares": input.shares,
                "ret_bps": input.ret_bps,
                "offsets_ms": offsets,
                "snapshots": snaps,
            }),
        );
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decision::{Action, TriggerDecision};
    use crate::signal::{Direction, Signal, Stratum};
    use crate::state::EventBbo;
    use std::path::PathBuf;

    fn tmp_oplog(tag: &str) -> (PathBuf, Arc<OpLog>) {
        let p = std::env::temp_dir().join(format!(
            "rb_window_{tag}_{}_{}.jsonl",
            std::process::id(),
            crate::state::now_ms()
        ));
        let _ = std::fs::remove_file(&p);
        (p.clone(), Arc::new(OpLog::new(p)))
    }

    fn sig(asset: &str, t: i64, interval: &str) -> Signal {
        let secs: i64 = match interval {
            "5m" => 300,
            "15m" => 900,
            _ => 60,
        };
        Signal {
            asset: asset.to_string(),
            trigger_ts: t,
            window_s: 1,
            ret_bps: 7.0,
            direction: Direction::Up,
            interval: interval.to_string(),
            epoch: (t / secs) * secs,
            ttr: secs - 100,
            stratum: Stratum::ResolvedWindow,
            bet_token_id: format!("{asset}-{interval}-bet"),
            opposite_token_id: format!("{asset}-{interval}-opp"),
        }
    }

    fn trig(asset: &str, t: i64) -> Trigger {
        Trigger {
            asset: asset.to_string(),
            trigger_ts: t,
            ret_bps: 7.0,
            window_s: 1,
        }
    }

    #[test]
    fn snapshot_at_returns_ask_bid_mid_ts_when_book_present() {
        let state = SharedState::new();
        state.bbo.insert(
            "TOK".into(),
            EventBbo {
                best_ask: Some(0.54),
                best_bid: Some(0.50),
                ts_ms: 1_780_000_000_500,
            },
        );
        let v = snapshot_at(&state, "TOK", 1000);
        assert_eq!(v["offset_ms"], 1000);
        assert_eq!(v["ask"], 0.54);
        assert_eq!(v["bid"], 0.50);
        assert!((v["midpoint"].as_f64().unwrap() - 0.52).abs() < 1e-9);
        assert_eq!(v["book_ts_ms"], 1_780_000_000_500i64);
    }

    #[test]
    fn snapshot_at_returns_nulls_when_book_absent() {
        let state = SharedState::new();
        let v = snapshot_at(&state, "MISSING", 500);
        assert_eq!(v["offset_ms"], 500);
        assert!(v["ask"].is_null());
        assert!(v["bid"].is_null());
        assert!(v["midpoint"].is_null());
        assert_eq!(v["book_ts_ms"], 0i64);
    }

    #[test]
    fn build_input_marks_fired_interval_accepted_and_rejected_interval_with_reason() {
        let t = 1_780_000_300i64;
        let s5 = sig("BTC", t, "5m");
        let s15 = sig("BTC", t, "15m");
        // Decision: 5m fired, 15m rejected for phantom.
        let decision = TriggerDecision {
            asset: "BTC".to_string(),
            trigger_ts: t,
            direction: Direction::Up,
            fired: true,
            reject_reason: String::new(),
            primary_interval: Some("5m".to_string()),
            actions: vec![
                Action::Fire {
                    interval: "5m".into(),
                    stratum: Stratum::ResolvedWindow,
                    bet_token: s5.bet_token_id.clone(),
                    fill: 0.55,
                    shares: 1.9,
                },
                Action::Reject {
                    interval: "15m".into(),
                    stratum: Stratum::Immediate,
                    reason: "phantom_ws_vs_rest".into(),
                },
            ],
        };
        let inp = build_input_from_decision(&trig("BTC", t), &[s5, s15], &decision, 0);
        assert_eq!(inp.trigger_ts, t);
        assert_eq!(inp.asset, "BTC");
        assert_eq!(inp.tokens.len(), 2);
        let by_iv: std::collections::HashMap<_, _> =
            inp.tokens.iter().map(|t| (t.interval.clone(), t)).collect();
        assert!(by_iv["5m"].accepted, "5m fired -> accepted");
        assert!(by_iv["5m"].reject_reason.is_none());
        assert!(!by_iv["15m"].accepted, "15m rejected");
        assert_eq!(
            by_iv["15m"].reject_reason.as_deref(),
            Some("phantom_ws_vs_rest")
        );
    }

    #[test]
    fn build_input_marks_signal_not_in_actions_as_not_qualified() {
        // Signal exists (interval projected onto a market) but the decision
        // dropped it before per-stratum (e.g. counted as no_book / stale_book
        // sample_rejected only). Should still appear with not_qualified.
        let t = 1_780_000_300i64;
        let s5 = sig("BTC", t, "5m");
        let decision = TriggerDecision {
            asset: "BTC".to_string(),
            trigger_ts: t,
            direction: Direction::Up,
            fired: false,
            reject_reason: "no_qualifying_market".into(),
            primary_interval: Some("5m".to_string()),
            actions: vec![], // empty actions -> nothing per-interval
        };
        let inp = build_input_from_decision(&trig("BTC", t), &[s5], &decision, 0);
        assert_eq!(inp.tokens.len(), 1);
        assert!(!inp.tokens[0].accepted);
        assert_eq!(
            inp.tokens[0].reject_reason.as_deref(),
            Some("not_qualified")
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn spawn_window_logger_emits_event_with_snapshots_at_all_offsets() {
        // SharedState::new() returns Arc<SharedState> (the `Shared` type alias).
        let state = SharedState::new();
        state.bbo.insert(
            "BTC-5m-bet".into(),
            EventBbo {
                best_ask: Some(0.50),
                best_bid: Some(0.49),
                ts_ms: 1_780_000_000_000,
            },
        );
        let (path, oplog) = tmp_oplog("emit");
        let t = 1_780_000_300i64;
        let s5 = sig("BTC", t, "5m");
        let decision = TriggerDecision {
            asset: "BTC".to_string(),
            trigger_ts: t,
            direction: Direction::Up,
            fired: true,
            reject_reason: String::new(),
            primary_interval: Some("5m".to_string()),
            actions: vec![Action::Fire {
                interval: "5m".into(),
                stratum: Stratum::ResolvedWindow,
                bet_token: s5.bet_token_id.clone(),
                fill: 0.50,
                shares: 2.1,
            }],
        };
        // G6: pass a CONCRETE kline_received_at_ms so the diagnostic
        // --nocapture output shows the latency field with a realistic value.
        // trigger_ts=1780000300s -> kline_close_ms=1780000301000; ingest at
        // +250ms post-close -> signal_arrival_latency_ms = 250.
        let kline_received_at_ms: i64 = (t + 1) * 1000 + 250;
        let input = build_input_from_decision(&trig("BTC", t), &[s5], &decision, kline_received_at_ms);

        // Use TINY offsets so the test is fast (1ms, 2ms, 3ms instead of 500/1000/etc).
        // The production constants are validated by the SNAPSHOT_OFFSETS_MS comment + a
        // separate const-shape test below.
        let h = spawn_window_logger_with_offsets(
            state.clone(),
            oplog,
            input,
            vec![0, 1, 2, 3, 5],
        );
        h.await.expect("logger task completed");

        let body = std::fs::read_to_string(&path).expect("oplog file written");
        let lines: Vec<&str> = body.lines().collect();
        assert_eq!(lines.len(), 1, "exactly one book_window_around_trigger event");
        let row: Value = serde_json::from_str(lines[0]).expect("valid JSON");
        // DIAGNOSTIC: print the full event so `cargo test -- --nocapture` shows
        // exactly what the oplog will contain in production.
        println!(
            "[window_logger-test] EMITTED book_window_around_trigger:\n{}",
            serde_json::to_string_pretty(&row).unwrap_or_default()
        );
        assert_eq!(row["kind"], "book_window_around_trigger");
        let d = &row["data"];
        assert_eq!(d["trigger_ts"], t);
        assert_eq!(d["asset"], "BTC");
        assert_eq!(d["direction"], "Up");
        assert_eq!(d["window_s"], 1);
        assert_eq!(d["offsets_ms"], json!([0, 1, 2, 3, 5]));
        // G6: signal arrival latency propagated inline. With kline_received_at_ms
        // = (t+1)*1000 + 250 -> latency = 250 (= 250ms past kline close).
        assert_eq!(d["kline_received_at_ms"], kline_received_at_ms);
        assert_eq!(d["signal_arrival_latency_ms"], 250i64,
            "G6: latency field must propagate (250ms by test setup)");
        let tokens = d["tokens"].as_array().expect("tokens array");
        assert_eq!(tokens.len(), 1);
        let snaps = tokens[0]["snapshots"]
            .as_array()
            .expect("snapshots array");
        assert_eq!(snaps.len(), 5, "5 snapshots at offsets 0/1/2/3/5 ms");
        for (i, off) in [0u64, 1, 2, 3, 5].iter().enumerate() {
            assert_eq!(snaps[i]["offset_ms"], *off);
            assert_eq!(snaps[i]["ask"], 0.50);
            assert_eq!(snaps[i]["bid"], 0.49);
            assert!((snaps[i]["midpoint"].as_f64().unwrap() - 0.495).abs() < 1e-9);
        }
        assert!(tokens[0]["accepted"].as_bool().unwrap());
        assert_eq!(tokens[0]["interval"], "5m");
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn spawn_window_logger_emits_event_even_with_no_tokens() {
        // Trigger fires but no scope market (e.g. last 30s of resolution).
        // Should still log the trigger (no snapshots, empty tokens), so the
        // 5-bps-trigger event count is recoverable from the oplog.
        let state = SharedState::new();
        let (path, oplog) = tmp_oplog("notok");
        let t = 1_780_000_300i64;
        let kline_received_at_ms: i64 = (t + 1) * 1000 + 420; // G6: +420ms latency
        let input = WindowInput {
            trigger_ts: t,
            asset: "BTC".to_string(),
            return_bps: 7.0,
            direction: Direction::Up,
            window_s: 1,
            tokens: vec![],
            kline_received_at_ms,
        };
        let h = spawn_window_logger_with_offsets(
            state.clone(),
            oplog,
            input,
            vec![0, 1, 2],
        );
        h.await.expect("logger task completed");
        let body = std::fs::read_to_string(&path).expect("oplog file written");
        let row: Value =
            serde_json::from_str(body.lines().next().expect("one line")).expect("valid JSON");
        assert_eq!(row["kind"], "book_window_around_trigger");
        assert_eq!(row["data"]["tokens"].as_array().unwrap().len(), 0);
        assert_eq!(row["data"]["asset"], "BTC");
        // G6: even with no tokens, the latency fields propagate so trigger-rate
        // and latency studies work on this cohort too.
        assert_eq!(row["data"]["kline_received_at_ms"], kline_received_at_ms);
        assert_eq!(row["data"]["signal_arrival_latency_ms"], 420i64);
        let _ = std::fs::remove_file(&path);
    }

    // ---------- G6: signal_arrival_latency_ms (pure helper) ----------

    #[test]
    fn g6_signal_arrival_latency_typical_positive() {
        // trigger_ts in seconds; received_at_ms in ms. kline_close at (t+1)*1000.
        let t = 1_780_000_300i64;
        let received = (t + 1) * 1000 + 250; // 250ms past close
        assert_eq!(signal_arrival_latency_ms(t, received), 250);
    }

    #[test]
    fn g6_signal_arrival_latency_zero_when_received_exactly_at_close() {
        let t = 1_780_000_300i64;
        let received = (t + 1) * 1000; // exactly at kline close
        assert_eq!(signal_arrival_latency_ms(t, received), 0);
    }

    #[test]
    fn g6_signal_arrival_latency_saturating_on_negative_drift() {
        // Hypothetical clock skew: bot wall-clock ahead of Binance kline ts.
        // saturating_sub keeps the value at 0 (not underflow), but here the
        // numbers don't underflow -- they just go negative, which our i64
        // representation handles fine. saturating_sub is for OVERFLOW safety
        // around i64::MIN/MAX, not for clamping. So a -5ms result IS valid.
        let t = 1_780_000_300i64;
        let received = (t + 1) * 1000 - 5; // 5ms BEFORE official kline close
        assert_eq!(signal_arrival_latency_ms(t, received), -5,
            "negative latency is legal (clock skew); not clamped");
    }

    #[test]
    fn g6_signal_arrival_latency_large_dublin_realistic_value() {
        // Realistic Dublin VPS: Binance Tokyo->Dublin ~250ms.
        let t = 1_780_000_300i64;
        let received = (t + 1) * 1000 + 267;
        assert_eq!(signal_arrival_latency_ms(t, received), 267);
    }

    #[test]
    fn production_snapshot_offsets_shape_is_what_we_documented() {
        // The DECISIONS.md note + module doc reference exactly these offsets.
        // Catch accidental edits to the const that would invalidate downstream
        // analysis assumptions.
        assert_eq!(SNAPSHOT_OFFSETS_MS, &[0u64, 500, 1000, 3000, 10000]);
    }

    // ---------- edge15 instrumentation (trade_edge_marks) ----------

    #[tokio::test(flavor = "current_thread")]
    async fn spawn_edge_marks_emits_one_event_with_marks() {
        let state = SharedState::new();
        state.bbo.insert(
            "BTC-5m-bet".into(),
            EventBbo {
                best_ask: Some(0.69),
                best_bid: Some(0.67),
                ts_ms: 1_780_000_015_000,
            },
        );
        let (path, oplog) = tmp_oplog("edge_emit");
        let input = EdgeMarksInput {
            signal_id: "BTC-1778067649-5m-Up".into(),
            token_id: "BTC-5m-bet".into(),
            asset: "BTC".into(),
            interval: "5m".into(),
            side: "Up".into(),
            // Anchor at "now" so the absolute offset targets are near-immediate;
            // tiny offsets keep the test fast (production = [0,15000,20000,25000]).
            decision_ts_ms: crate::state::now_ms(),
            entry_ask: 0.44,
            shares: 2.2727,
            ret_bps: 5.71,
        };
        let h = spawn_edge_marks_logger_with_offsets(
            state.clone(),
            oplog,
            input,
            vec![0, 1, 2, 3],
        );
        h.await.expect("edge-marks task completed");

        let body = std::fs::read_to_string(&path).expect("oplog file written");
        let lines: Vec<&str> = body.lines().collect();
        assert_eq!(lines.len(), 1, "exactly one trade_edge_marks event");
        let row: Value = serde_json::from_str(lines[0]).expect("valid JSON");
        println!(
            "[edge-marks-test] EMITTED trade_edge_marks:\n{}",
            serde_json::to_string_pretty(&row).unwrap_or_default()
        );
        assert_eq!(row["kind"], "trade_edge_marks");
        let d = &row["data"];
        assert_eq!(d["signal_id"], "BTC-1778067649-5m-Up");
        assert_eq!(d["token_id"], "BTC-5m-bet");
        assert_eq!(d["asset"], "BTC");
        assert_eq!(d["interval"], "5m");
        assert_eq!(d["side"], "Up");
        assert_eq!(d["entry_ask"], 0.44);
        assert_eq!(d["shares"], 2.2727);
        assert_eq!(d["ret_bps"], 5.71);
        assert_eq!(d["offsets_ms"], json!([0, 1, 2, 3]));
        let snaps = d["snapshots"].as_array().expect("snapshots array");
        assert_eq!(snaps.len(), 4, "4 marks at offsets 0/1/2/3");
        for (i, off) in [0u64, 1, 2, 3].iter().enumerate() {
            assert_eq!(snaps[i]["offset_ms"], *off);
            assert_eq!(snaps[i]["ask"], 0.69);
            assert_eq!(snaps[i]["bid"], 0.67);
            assert!((snaps[i]["midpoint"].as_f64().unwrap() - 0.68).abs() < 1e-9);
        }
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn spawn_edge_marks_null_midpoint_when_book_absent() {
        let state = SharedState::new(); // no bbo for the token
        let (path, oplog) = tmp_oplog("edge_null");
        let input = EdgeMarksInput {
            signal_id: "BTC-1-5m-Up".into(),
            token_id: "MISSING".into(),
            asset: "BTC".into(),
            interval: "5m".into(),
            side: "Up".into(),
            decision_ts_ms: crate::state::now_ms(),
            entry_ask: 0.50,
            shares: 2.0,
            ret_bps: 6.0,
        };
        let h = spawn_edge_marks_logger_with_offsets(state.clone(), oplog, input, vec![0, 1]);
        h.await.expect("edge-marks task completed");
        let body = std::fs::read_to_string(&path).expect("oplog file written");
        let row: Value =
            serde_json::from_str(body.lines().next().expect("one line")).expect("valid JSON");
        assert_eq!(row["kind"], "trade_edge_marks");
        let snaps = row["data"]["snapshots"].as_array().expect("snapshots array");
        assert_eq!(snaps.len(), 2);
        for s in snaps {
            assert!(s["midpoint"].is_null(), "absent book -> null midpoint");
            assert!(s["ask"].is_null());
            assert!(s["bid"].is_null());
            assert_eq!(s["book_ts_ms"], 0i64, "absent book -> book_ts_ms=0");
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn edge_mark_offsets_shape_is_0_15_20_25() {
        // The pre-registration pins these offsets (mid0 + the +15/20/25s marks).
        // Catch accidental edits that would break edge15/drift/contingency.
        assert_eq!(EDGE_MARK_OFFSETS_MS, &[0u64, 15_000, 20_000, 25_000]);
    }
}
