//! Phase 6 D3 Piece 2 -- HOLD RECOVERY (crash -> restart -> close at exit_ts).
//!
//! The bot opens BUYs and holds 120-300s before selling. If it crashes during
//! that hold, the persisted state has the position AND the idempotency log has
//! a Pending/Ambiguous intent. At startup the bot MUST:
//!   1. Recognize the prior intent (do NOT blind-retry the BUY).
//!   2. Adopt the held position (do NOT orphan it; do NOT open a duplicate).
//!   3. Close it at its persisted `exit_ts_s` (continue the normal hold; the
//!      existing exit-by-clock loop fires the SELL when wall-clock catches up).
//!
//! For the SELL retry path (when close_due's SELL fails at exit_ts), the SAME
//! idempotency primitive applies: NEVER blind-retry; reconcile first and only
//! resend if the prior SELL is proven not landed.
//!
//! All decisions are PURE functions over (intent, position, evidence) so the
//! crash->restart->close behavior is fully testable offline (no capital).

#![allow(dead_code)] // wired by the production loop in piece 5 (oplog) / D4 (arming)

use crate::idempotency::{Evidence, OrderIntent, ReconcileDecision, reconcile};
use crate::state::persist::OpenPosition;

/// What to do with a recovered intent at startup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryAction {
    /// The BUY landed (a position for the intent's token is in persisted state).
    /// Continue managing it -- close at its persisted `exit_ts_s`. NO new BUY.
    HoldToExit { token_id: String, exit_ts_s: i64 },
    /// Reconcile proved the BUY did NOT land. Free the coid -- the next decision
    /// for this signal may re-open cleanly.
    MarkAbandoned { reason: String },
    /// Cannot determine right now (sources unreachable / too soon / on-chain
    /// shows landed but state doesn't have it). Alert + do not open new for
    /// this signal until a human reconciles. NEVER blind-retry.
    Escalate { reason: String },
}

/// PURE: decide what to do with one recovered intent given the persisted
/// position (if any) for its token + the reconcile evidence + the current time.
///
/// Precedence: the cached position WINS (the bot owns its state). Only when no
/// position is present do we fall back to the reconcile evidence -- because the
/// dangerous case is "intent Pending, position absent" (could be did-not-land
/// OR landed-but-orphan; reconcile decides which).
#[must_use]
pub fn plan_recovery(
    intent: &OrderIntent,
    position: Option<&OpenPosition>,
    ev: &Evidence,
    now_ms: i64,
) -> RecoveryAction {
    if let Some(p) = position {
        return RecoveryAction::HoldToExit {
            token_id: p.token_id.clone(),
            exit_ts_s: p.exit_ts_s,
        };
    }
    match reconcile(intent, ev, now_ms) {
        ReconcileDecision::AlreadyLanded => RecoveryAction::Escalate {
            reason: "reconcile shows BUY landed but no cached position; manual review (orphan)".into(),
        },
        ReconcileDecision::ResendSafe => RecoveryAction::MarkAbandoned {
            reason: "reconcile: definitively not landed (sources queried, no trace, past lag window)".into(),
        },
        ReconcileDecision::Ambiguous(msg) => RecoveryAction::Escalate { reason: msg },
    }
}

/// Per-coid recovery plan for a batch of unresolved intents (the output of
/// `idempotency::unresolved_intents`). Pairs each intent with its token's
/// persisted position (if any) and the available evidence; returns one
/// `(coid, action)` per intent.
#[must_use]
pub fn plan_recovery_all(
    intents: &[OrderIntent],
    positions: &[OpenPosition],
    ev_by_coid: &std::collections::HashMap<String, Evidence>,
    now_ms: i64,
) -> Vec<(String, RecoveryAction)> {
    let default_ev = Evidence::default();
    intents
        .iter()
        .map(|it| {
            let pos = positions.iter().find(|p| p.token_id == it.token_id);
            let ev = ev_by_coid.get(&it.client_order_id).unwrap_or(&default_ev);
            (it.client_order_id.clone(), plan_recovery(it, pos, ev, now_ms))
        })
        .collect()
}

/// What to do when a SELL retry attempt is being considered after an earlier
/// SELL at `close_due` failed. The SAME idempotency primitive applies: NEVER
/// blind-retry; reconcile first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SellRetryAction {
    /// Reconcile proved the SELL did NOT land -- safe to retry idempotently
    /// (the caller abandons the prior coid and issues a fresh one).
    Retry { reason: String },
    /// SELL did land (trade seen / position closed). Mark Submitted; no retry.
    AlreadyClosed { reason: String },
    /// Cannot determine right now -- HOLD + alert; the position runs to market
    /// resolution rather than us double-selling.
    HoldAndAlert { reason: String },
}

/// PURE: decide whether to retry a failed SELL based on reconcile evidence.
/// Mirrors `plan_recovery` for the SELL path -- the safe default on ambiguity
/// is HoldAndAlert (a double-sell is worse than a missed exit).
#[must_use]
pub fn plan_sell_retry(prior: &OrderIntent, ev: &Evidence, now_ms: i64) -> SellRetryAction {
    match reconcile(prior, ev, now_ms) {
        ReconcileDecision::AlreadyLanded => SellRetryAction::AlreadyClosed {
            reason: "SELL trade seen / position closed -- no retry".into(),
        },
        ReconcileDecision::ResendSafe => SellRetryAction::Retry {
            reason: "reconcile: SELL definitively did not land -- abandon prior coid and resend".into(),
        },
        ReconcileDecision::Ambiguous(msg) => SellRetryAction::HoldAndAlert { reason: msg },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::idempotency::{IntentStatus, RECONCILE_MIN_AGE_MS, load_log, unresolved_intents};
    use crate::state::now_ms;
    use crate::state::persist::{OpenPosition, Outcome, OrderStatus};
    use crate::state::store::{RecoverySource, StateStore};
    use rust_decimal_macros::dec;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU32, Ordering};

    fn unique_dir(tag: &str) -> PathBuf {
        static N: AtomicU32 = AtomicU32::new(0);
        let id = N.fetch_add(1, Ordering::SeqCst);
        std::env::temp_dir().join(format!("rb_hr_{tag}_{}_{}_{}", std::process::id(), now_ms(), id))
    }

    fn pos(token: &str, exit_ts_s: i64) -> OpenPosition {
        OpenPosition {
            token_id: token.to_string(),
            asset: "BTC".to_string(),
            side: Outcome::Up,
            entry_price: dec!(0.51),
            shares: dec!(1.96),
            opened_at_ms: 1_780_000_000_000,
            signal_id: "sig-1".to_string(),
            interval: "5m".to_string(),
            exit_ts_s,
            // v3 (Pieza 1) sentinels: hold_recovery tests only exercise
            // open-position reconciliation against on-chain holdings, not
            // smart-exit. Sentinels keep the bot's behavior unchanged.
            entry_ts_ms: 0,
            running_max_bid: 0.0,
            ts_max_bid_ms: 0,
            status: OrderStatus::TentativeFilled,
            order_id: Some("0xord-real".to_string()),
            ack_at_ms: Some(1_780_000_000_100),
            confirmed_at_ms: None,
            confirmation_source: None,
            maker_exit: None,
        }
    }

    fn intent(coid: &str, token: &str, side: &str, created_ms: i64) -> OrderIntent {
        OrderIntent::new_pending(coid, token, side, "1", "0.99", created_ms / 1000, created_ms)
    }

    #[test]
    fn position_present_means_hold_to_persisted_exit() {
        let it = intent("buy-1", "TOK", "buy", 1_000);
        let p = pos("TOK", 1_780_000_120);
        let act = plan_recovery(&it, Some(&p), &Evidence::default(), 1_000);
        assert_eq!(
            act,
            RecoveryAction::HoldToExit { token_id: "TOK".into(), exit_ts_s: 1_780_000_120 },
            "position present -> adopt + hold to its persisted exit_ts"
        );
    }

    #[test]
    fn no_position_no_sources_queried_escalates() {
        // Pending intent, no cached position, sources NOT queried -> Ambiguous -> Escalate.
        let it = intent("buy-2", "TOK", "buy", 1_000);
        let act = plan_recovery(&it, None, &Evidence::default(), 1_000);
        assert!(matches!(act, RecoveryAction::Escalate { .. }), "no sources -> Escalate, got {act:?}");
    }

    #[test]
    fn no_position_sources_queried_past_window_abandons() {
        // Pending intent, no cached position, sources queried + no trace + past
        // window -> definitive non-land -> Abandon (free the coid).
        let it = intent("buy-3", "TOK", "buy", 1_000);
        let ev = Evidence { sources_queried: true, ..Evidence::default() };
        let now = 1_000 + RECONCILE_MIN_AGE_MS + 1;
        let act = plan_recovery(&it, None, &ev, now);
        assert!(matches!(act, RecoveryAction::MarkAbandoned { .. }), "definitive non-land -> Abandon, got {act:?}");
    }

    #[test]
    fn no_position_but_evidence_landed_escalates_orphan() {
        // The dangerous case: on-chain shows the BUY landed, but bot state has
        // NO position (we crashed between the POST ack and the state snapshot).
        // MUST escalate -- never silently open a duplicate or ignore.
        let it = intent("buy-4", "TOK", "buy", 1_000);
        let ev = Evidence { trade_seen: true, sources_queried: true, ..Evidence::default() };
        let act = plan_recovery(&it, None, &ev, 1_000 + RECONCILE_MIN_AGE_MS + 1);
        assert!(matches!(act, RecoveryAction::Escalate { .. }), "landed-orphan -> Escalate, got {act:?}");
    }

    #[test]
    fn crash_during_hold_recovers_position_at_persisted_exit_ts() {
        // THE PIECE-2 PROOF (end-to-end, OFFLINE, ZERO capital):
        // 1. Pre-crash: state has an open position with a future exit_ts; the
        //    BUY's write-ahead intent is still Pending (the POST ack landed but
        //    the bot crashed before the Submitted transition was appended).
        // 2. Crash: drop everything; bytes on disk only.
        // 3. Restart: load state, load intent log, run plan_recovery_all.
        // 4. Expected: HoldToExit at the PERSISTED exit_ts_s. No duplicate BUY,
        //    no orphan -- the normal exit-by-clock loop will close it.
        let dir = unique_dir("crash_recover");
        let state_path = dir.join("state.json");
        let intent_log = dir.join("order_intents.jsonl");

        // --- PRE-CRASH ---
        let store = StateStore::load(&state_path).0;
        let held = pos("TOK-A", 1_780_000_120);
        {
            let st = store.state();
            let mut g = st.lock().unwrap();
            g.positions.push(held.clone());
            g.balance_usdc = dec!(100);
            g.record_launch("live", "0.1.0", 1_780_000_000_000);
        }
        store.snapshot().expect("pre-crash snapshot");

        let pending = intent("buy-held-1", "TOK-A", "buy", 1_780_000_000_100);
        pending.append(&intent_log).expect("write-ahead pending");

        // Sanity: BEFORE crash, both artifacts exist on disk.
        assert!(state_path.exists(), "state.json written");
        assert!(intent_log.exists(), "intent log written");
        println!("[hold-recovery] PRE-CRASH: position token=TOK-A exit_ts_s=1780000120 shares=1.96 entry=0.51");
        println!("[hold-recovery] PRE-CRASH: intent coid=buy-held-1 status=Pending (write-ahead, no ack)");

        drop(store); // <-- CRASH (process dies; only bytes on disk remain).
        println!("[hold-recovery] *** CRASH: process dropped; only disk bytes remain ***");

        // --- RESTART (fresh load from disk) ---
        let (store2, oc) = StateStore::load(&state_path);
        assert_eq!(oc.source, RecoverySource::Primary, "loaded cleanly from state.json");
        let positions: Vec<OpenPosition> = store2.state().lock().unwrap().positions.clone();
        assert_eq!(positions.len(), 1, "the held position survived the crash");
        assert_eq!(positions[0].token_id, "TOK-A");
        assert_eq!(positions[0].exit_ts_s, 1_780_000_120, "exit_ts_s preserved exactly");
        println!(
            "[hold-recovery] RESTART: state.json loaded from {:?}; positions[0]={{token={}, exit_ts_s={}, shares={}, entry={}}}",
            oc.source, positions[0].token_id, positions[0].exit_ts_s, positions[0].shares, positions[0].entry_price,
        );

        let all = load_log(&intent_log).expect("load intent log");
        let unresolved = unresolved_intents(&all);
        assert_eq!(unresolved.len(), 1, "the Pending intent for the held BUY is unresolved");
        assert_eq!(unresolved[0].client_order_id, "buy-held-1");
        assert!(matches!(unresolved[0].status, IntentStatus::Pending));
        println!(
            "[hold-recovery] RESTART: intent log loaded; unresolved={} -> coid={}, status={:?}",
            unresolved.len(), unresolved[0].client_order_id, unresolved[0].status,
        );

        // --- THE RECONCILE: position present -> HoldToExit at the persisted ts ---
        let ev_by: HashMap<String, Evidence> = HashMap::new();
        let plan = plan_recovery_all(&unresolved, &positions, &ev_by, 1_780_000_005_000);
        assert_eq!(plan.len(), 1, "one action per unresolved intent");
        assert_eq!(plan[0].0, "buy-held-1");
        assert_eq!(
            plan[0].1,
            RecoveryAction::HoldToExit { token_id: "TOK-A".into(), exit_ts_s: 1_780_000_120 },
            "post-restart: hold the position to its persisted exit_ts (no duplicate BUY, no orphan)"
        );
        println!("[hold-recovery] RECONCILE: plan[0]=({}, {:?})", plan[0].0, plan[0].1);
        println!("[hold-recovery] OUTCOME: HoldToExit at exit_ts_s=1780000120 -- NO duplicate BUY, NO orphan, NO new POST. Normal exit-by-clock loop will close it.");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn crash_during_hold_does_not_duplicate_buy_on_restart() {
        // Defensive twin of the above: if plan_recovery_all said "open a new
        // BUY" for a token we already hold, we'd double-spend. Lock that down:
        // for EVERY unresolved BUY whose token is already held, the action MUST
        // be HoldToExit -- NEVER Abandon, NEVER Escalate, NEVER anything else.
        let p1 = pos("TOK-A", 1_780_000_120);
        let p2 = pos("TOK-B", 1_780_000_300);
        let positions = vec![p1, p2];
        let intents = vec![
            intent("buy-A", "TOK-A", "buy", 1_780_000_000_100),
            intent("buy-B", "TOK-B", "buy", 1_780_000_180_100),
        ];
        let ev_by: HashMap<String, Evidence> = HashMap::new();
        let plan = plan_recovery_all(&intents, &positions, &ev_by, 1_780_000_005_000);
        assert_eq!(plan.len(), 2);
        for (coid, act) in &plan {
            assert!(
                matches!(act, RecoveryAction::HoldToExit { .. }),
                "coid={coid}: held-token recovery MUST be HoldToExit, got {act:?}"
            );
        }
    }

    // ---- SELL retry: reconcile-before-resend (never blind) ----

    #[test]
    fn sell_retry_safe_when_reconcile_proves_not_landed() {
        let sell = intent("sell-1", "TOK-A", "sell", 1_000);
        let ev = Evidence { sources_queried: true, ..Evidence::default() };
        let now = 1_000 + RECONCILE_MIN_AGE_MS + 1;
        let act = plan_sell_retry(&sell, &ev, now);
        assert!(matches!(act, SellRetryAction::Retry { .. }), "definitive non-land -> Retry, got {act:?}");
    }

    #[test]
    fn sell_retry_already_closed_when_trade_seen() {
        let sell = intent("sell-2", "TOK-A", "sell", 1_000);
        let ev = Evidence { trade_seen: true, sources_queried: true, ..Evidence::default() };
        let act = plan_sell_retry(&sell, &ev, 1_000 + RECONCILE_MIN_AGE_MS + 1);
        assert!(matches!(act, SellRetryAction::AlreadyClosed { .. }), "SELL trade -> AlreadyClosed, got {act:?}");
    }

    #[test]
    fn sell_retry_holds_when_evidence_ambiguous() {
        let sell = intent("sell-3", "TOK-A", "sell", 1_000);
        // sources unreachable -> Ambiguous -> HoldAndAlert (NEVER blind-retry).
        let act = plan_sell_retry(&sell, &Evidence::default(), 1_000 + RECONCILE_MIN_AGE_MS + 1);
        assert!(matches!(act, SellRetryAction::HoldAndAlert { .. }), "ambiguous -> HoldAndAlert, got {act:?}");
    }

    #[test]
    fn sell_retry_holds_when_too_soon() {
        // Sources queried but the intent is younger than the lag window -> Ambiguous -> HoldAndAlert.
        let sell = intent("sell-4", "TOK-A", "sell", 1_000);
        let ev = Evidence { sources_queried: true, ..Evidence::default() };
        let act = plan_sell_retry(&sell, &ev, 1_000 + RECONCILE_MIN_AGE_MS - 1);
        assert!(matches!(act, SellRetryAction::HoldAndAlert { .. }), "too-soon -> HoldAndAlert, got {act:?}");
    }
}
