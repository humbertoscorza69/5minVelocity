//! Phase 6 Sub-paso C — order idempotency (no double-submit on resend/restart).
//!
//! CONFIRMED PRIMITIVE (verified in the SDK, NOT assumed):
//!   * The CLOB has NO native idempotency key — no `client_order_id`/`clientOrderId`
//!     field in the order struct or the POST body.
//!   * The SDK salt is RANDOM (`order_salt()` = `(secs * rand::random::<f64>())`),
//!     with NO caller override (`salt_generator`/`set_salt` do not exist). The
//!     EIP-712 order hash also includes `timestamp` (not caller-pinned). So two
//!     builds of the SAME logical order produce DIFFERENT orderIDs.
//!   * => There is NO server-side dedup we can lean on, and we CANNOT force a stable
//!     orderID through the SDK's public path.
//!
//! CONSEQUENCE: idempotency is ENTIRELY client-side. The defense is:
//!   1. client_order_id — a DETERMINISTIC LOCAL key from the logical order
//!      (token, side, signal, lot, epoch). The CLOB never sees it; it is OUR
//!      correlation handle for the intent log + reconcile.
//!   2. Write-ahead intent — persist (coid, params, Pending) to disk BEFORE the POST,
//!      so a crash mid-POST leaves a record to reconcile against.
//!   3. Reconcile-before-resend — on ANY ambiguity (no response / restart), look for
//!      evidence the order landed (/trades, /positions, /activity) BEFORE resending.
//!      Resend ONLY if it definitively did not land, with the same coid.
//!   4. NEVER blind-retry (honors the Phase-2 warning: read-only retry is NOT for
//!      writes).
//!
//! NOT pursued now: a deterministic salt via direct OrderV2 construction (the A1
//! path). It is possible (A1 built+signed an order with a fixed salt byte-exact) but
//! would also need to pin `timestamp`, and — critically — there is NO evidence the
//! CLOB dedupes by order hash, so it buys an UNCONFIRMED benefit. Documented as a
//! future option IF server-side hash-dedup is ever confirmed.

#![allow(dead_code)] // wired into the LiveExecutor (here) + the integrated bot (D)

use std::io::Write as _;
use std::path::Path;

use serde::{Deserialize, Serialize};

/// Deterministic, CLOB-invisible local order key. Same logical order → same id;
/// distinct logical order → distinct id. (`lot_index` distinguishes DCA lots on the
/// same market/signal.) Stable + human-readable so the intent log is auditable.
#[must_use]
pub fn client_order_id(asset: &str, interval: &str, epoch: i64, side: &str, signal_id: &str, lot_index: u32) -> String {
    format!("{}-{}-{}-{}-{}-{}", asset.to_lowercase(), interval, epoch, side.to_lowercase(), signal_id, lot_index)
}

/// Lifecycle of one order intent (the write-ahead record).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum IntentStatus {
    /// Persisted BEFORE the POST. A record in this state after a restart = ambiguous,
    /// MUST reconcile.
    Pending,
    /// POST returned and the order is acknowledged (we have an orderID).
    Submitted { order_id: String },
    /// Confirmed on-chain (trade/position corroborated). Terminal-success.
    Landed { order_id: String },
    /// POST definitively rejected (server said no). Terminal-fail, safe: no position.
    Rejected { reason: String },
    /// POST outcome UNKNOWN (network error / no ack). The dangerous state → reconcile,
    /// never blind-resend.
    Ambiguous { reason: String },
    /// Reconcile proved it did not land; abandoned without resend (or superseded).
    Abandoned { reason: String },
}

/// One write-ahead intent record (JSONL row).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderIntent {
    pub client_order_id: String,
    pub token_id: String,
    pub side: String,
    pub size: String,  // Decimal as string (shares for SELL / usdc for BUY)
    pub price: String, // Decimal as string
    pub epoch: i64,
    pub created_ms: i64,
    pub status: IntentStatus,
}

impl OrderIntent {
    #[must_use]
    pub fn new_pending(coid: &str, token_id: &str, side: &str, size: &str, price: &str, epoch: i64, now_ms: i64) -> Self {
        Self {
            client_order_id: coid.to_string(), token_id: token_id.to_string(), side: side.to_string(),
            size: size.to_string(), price: price.to_string(), epoch, created_ms: now_ms,
            status: IntentStatus::Pending,
        }
    }
    /// Append this record as one JSONL line (write-ahead log; append-only audit trail).
    pub fn append(&self, path: &Path) -> std::io::Result<()> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let mut f = std::fs::OpenOptions::new().create(true).append(true).open(path)?;
        writeln!(f, "{}", serde_json::to_string(self).unwrap_or_default())
    }
}

/// Observed on-chain/exchange evidence for a token, gathered during reconcile.
/// (In the live path these come from /trades, /positions, /activity — here they are
/// plain inputs so the decision logic is fully testable offline.)
#[derive(Debug, Clone, Default)]
pub struct Evidence {
    /// A trade for this token+side at/after the intent time was observed.
    pub trade_seen: bool,
    /// Position size currently held for this token (0 if none).
    pub position_size: f64,
    /// Whether the data sources have been queried at all (false = couldn't check).
    pub sources_queried: bool,
}

/// The resend decision after reconcile.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReconcileDecision {
    /// Evidence shows the order landed → do NOT resend (record Landed/Submitted).
    AlreadyLanded,
    /// Definitively did not land (sources queried, no trace, enough time) → resend OK.
    ResendSafe,
    /// Cannot tell yet (sources unreachable, or too soon) → HOLD, re-check; never
    /// blind-resend.
    Ambiguous(String),
}

/// Minimum age before "no evidence" is allowed to mean "did not land" (data-api lags
/// several seconds; mirrors the A2 anti-phantom corroboration window).
pub const RECONCILE_MIN_AGE_MS: i64 = 30_000;
/// Position-size threshold (shares) that counts as a real landed position.
pub const POSITION_DUST: f64 = 0.01;

/// Decide whether to resend an ambiguous order. PURE — feed it the evidence.
///
/// Resend ONLY when sources were queried AND show no trade AND no position AND the
/// intent is older than the lag window. Anything less is `Ambiguous` (hold) — the
/// safe default, because a false "did not land" causes a DOUBLE submit.
#[must_use]
pub fn reconcile(intent: &OrderIntent, ev: &Evidence, now_ms: i64) -> ReconcileDecision {
    if ev.trade_seen || ev.position_size > POSITION_DUST {
        return ReconcileDecision::AlreadyLanded;
    }
    if !ev.sources_queried {
        return ReconcileDecision::Ambiguous("reconcile sources unreachable — cannot confirm; HOLD".into());
    }
    let age = now_ms - intent.created_ms;
    if age < RECONCILE_MIN_AGE_MS {
        return ReconcileDecision::Ambiguous(format!(
            "only {age}ms since intent (< {RECONCILE_MIN_AGE_MS}ms lag window) — too soon to call it un-landed; HOLD"
        ));
    }
    // Sources queried, no trade, no position, past the lag window → it did not land.
    ReconcileDecision::ResendSafe
}

/// Idempotency gate: may we POST this client_order_id given the intent log? Refuse if
/// a record for it is already Submitted/Landed (already in flight or done) — that is
/// the double-submit we are preventing. Pending/Ambiguous → must reconcile first (also
/// refuse a blind POST). Only a fresh coid (no record) or a proven-not-landed
/// (Abandoned/Rejected being re-decided) may proceed.
#[must_use]
pub fn may_post(prior: Option<&IntentStatus>) -> bool {
    match prior {
        None => true,                              // never seen → safe first POST
        Some(IntentStatus::Rejected { .. }) => false, // terminal fail; a NEW logical order needs a new coid
        Some(IntentStatus::Abandoned { .. }) => true, // proven not-landed → a deliberate resend is allowed
        Some(_) => false, // Pending / Submitted / Landed / Ambiguous → reconcile, never blind-POST
    }
}

/// Latest status per client_order_id from a write-ahead log (last write wins). Used at
/// startup to recover in-flight intents that need reconcile.
#[must_use]
pub fn latest_status(records: &[OrderIntent], coid: &str) -> Option<IntentStatus> {
    records.iter().rev().find(|r| r.client_order_id == coid).map(|r| r.status.clone())
}

/// Load all intents from a JSONL write-ahead log (skips malformed lines).
pub fn load_log(path: &Path) -> std::io::Result<Vec<OrderIntent>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let body = std::fs::read_to_string(path)?;
    Ok(body.lines().filter(|l| !l.trim().is_empty())
        .filter_map(|l| serde_json::from_str::<OrderIntent>(l).ok())
        .collect())
}

/// Default production intent log (gitignored runtime data).
pub const INTENT_LOG: &str = "data/live/order_intents.jsonl";

/// The gate's verdict on a prospective POST for `coid`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GateDecision {
    /// No prior intent blocks: a fresh write-ahead + POST is allowed.
    Proceed,
    /// Prior intent blocks. Carry the latest known status (the reason).
    Refuse(IntentStatus),
}

/// Read the write-ahead log and decide whether `coid` may POST.
///
/// PROCEED iff there is NO prior intent for this coid, OR the latest prior
/// status is one of the resend-safe terminal states (`may_post` says yes).
/// REFUSE otherwise -- including the case where the log is unreadable
/// (fail closed: a read error is ambiguous, not safe).
#[must_use]
pub fn idempotency_gate(log_path: &Path, coid: &str) -> GateDecision {
    let records = match load_log(log_path) {
        Ok(r) => r,
        Err(_) => return GateDecision::Refuse(IntentStatus::Ambiguous { reason: "intent log unreadable".into() }),
    };
    let prior = latest_status(&records, coid);
    if may_post(prior.as_ref()) {
        GateDecision::Proceed
    } else {
        GateDecision::Refuse(prior.unwrap_or(IntentStatus::Ambiguous { reason: "blocked".into() }))
    }
}

/// Scan the log for intents whose LATEST status is Pending or Ambiguous (the
/// "needs reconcile" set). One entry per coid (latest wins). Restart-safe
/// crash recovery starts here.
#[must_use]
pub fn unresolved_intents(records: &[OrderIntent]) -> Vec<OrderIntent> {
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::new();
    for r in records.iter().rev() {
        if !seen.insert(r.client_order_id.clone()) { continue; }
        if matches!(r.status, IntentStatus::Pending | IntentStatus::Ambiguous { .. }) {
            out.push(r.clone());
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn intent(now: i64) -> OrderIntent {
        OrderIntent::new_pending("btc-5m-1780132200-buy-sigA-0", "TOK", "buy", "1.05", "0.99", 1780132200, now)
    }

    // --- client_order_id determinism ---
    #[test]
    fn coid_is_deterministic_and_distinct() {
        let a = client_order_id("BTC", "5m", 1780132200, "Buy", "sigA", 0);
        let b = client_order_id("BTC", "5m", 1780132200, "Buy", "sigA", 0);
        assert_eq!(a, b, "same logical order → same id");
        // distinct on each dimension
        assert_ne!(a, client_order_id("ETH", "5m", 1780132200, "Buy", "sigA", 0));
        assert_ne!(a, client_order_id("BTC", "15m", 1780132200, "Buy", "sigA", 0));
        assert_ne!(a, client_order_id("BTC", "5m", 1780132500, "Buy", "sigA", 0));
        assert_ne!(a, client_order_id("BTC", "5m", 1780132200, "Sell", "sigA", 0));
        assert_ne!(a, client_order_id("BTC", "5m", 1780132200, "Buy", "sigB", 0));
        assert_ne!(a, client_order_id("BTC", "5m", 1780132200, "Buy", "sigA", 1)); // DCA lot
    }

    // --- reconcile-before-resend ---
    #[test]
    fn reconcile_trade_seen_is_already_landed() {
        let ev = Evidence { trade_seen: true, position_size: 0.0, sources_queried: true };
        assert_eq!(reconcile(&intent(0), &ev, 60_000), ReconcileDecision::AlreadyLanded);
    }
    #[test]
    fn reconcile_position_held_is_already_landed() {
        let ev = Evidence { trade_seen: false, position_size: 2.14, sources_queried: true };
        assert_eq!(reconcile(&intent(0), &ev, 60_000), ReconcileDecision::AlreadyLanded);
    }
    #[test]
    fn reconcile_no_trace_after_window_is_resend_safe() {
        let ev = Evidence { trade_seen: false, position_size: 0.0, sources_queried: true };
        assert_eq!(reconcile(&intent(0), &ev, 31_000), ReconcileDecision::ResendSafe);
    }
    #[test]
    fn reconcile_too_soon_holds_not_resend() {
        // No trace but only 5s elapsed (< 30s lag window) → HOLD, never resend.
        let ev = Evidence { trade_seen: false, position_size: 0.0, sources_queried: true };
        assert!(matches!(reconcile(&intent(0), &ev, 5_000), ReconcileDecision::Ambiguous(_)));
    }
    #[test]
    fn reconcile_sources_unreachable_holds() {
        // Couldn't query → never assume un-landed → HOLD (prevents double-submit).
        let ev = Evidence { trade_seen: false, position_size: 0.0, sources_queried: false };
        assert!(matches!(reconcile(&intent(0), &ev, 60_000), ReconcileDecision::Ambiguous(_)));
    }
    #[test]
    fn dust_position_does_not_count_as_landed() {
        let ev = Evidence { trade_seen: false, position_size: 0.005, sources_queried: true };
        // below dust + past window → resend safe (that dust is not our fill)
        assert_eq!(reconcile(&intent(0), &ev, 31_000), ReconcileDecision::ResendSafe);
    }

    // --- idempotency gate (may_post) ---
    #[test]
    fn may_post_rules() {
        assert!(may_post(None), "fresh coid → safe");
        assert!(!may_post(Some(&IntentStatus::Pending)), "pending → reconcile, no blind POST");
        assert!(!may_post(Some(&IntentStatus::Submitted { order_id: "x".into() })), "in flight → no");
        assert!(!may_post(Some(&IntentStatus::Landed { order_id: "x".into() })), "done → no");
        assert!(!may_post(Some(&IntentStatus::Ambiguous { reason: "net".into() })), "ambiguous → reconcile, no");
        assert!(!may_post(Some(&IntentStatus::Rejected { reason: "bad".into() })), "rejected → needs NEW coid");
        assert!(may_post(Some(&IntentStatus::Abandoned { reason: "not landed".into() })), "proven-not-landed → resend ok");
    }

    // --- write-ahead log round-trip + latest-wins recovery ---
    #[test]
    fn write_ahead_log_roundtrip_and_latest_status() {
        let p = std::env::temp_dir().join(format!("rb_intent_test_{}.jsonl", std::process::id()));
        let _ = std::fs::remove_file(&p);
        let mut i = intent(1_000);
        i.append(&p).unwrap(); // Pending (write-ahead, before POST)
        i.status = IntentStatus::Submitted { order_id: "0xabc".into() };
        i.append(&p).unwrap(); // updated after ack
        let recs = load_log(&p).unwrap();
        assert_eq!(recs.len(), 2, "append-only: both rows present");
        assert_eq!(latest_status(&recs, &i.client_order_id),
            Some(IntentStatus::Submitted { order_id: "0xabc".into() }), "last write wins");
        assert_eq!(latest_status(&recs, "nope"), None);
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn crash_after_pending_recovers_as_reconcile_needed() {
        // Simulate: wrote Pending, crashed before POST ack. On restart we see Pending
        // → may_post=false (must reconcile, not blind-POST).
        let recs = vec![intent(1_000)]; // only the Pending write-ahead row
        let st = latest_status(&recs, "btc-5m-1780132200-buy-sigA-0");
        assert_eq!(st, Some(IntentStatus::Pending));
        assert!(!may_post(st.as_ref()), "a recovered Pending must reconcile, never blind-POST");
    }

    // --- gate tests: write-ahead lifecycle on a real (tmp) jsonl path ---

    fn tmp_log(tag: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("rb_idem_{tag}_{}.jsonl", std::process::id()))
    }

    fn pending_for(coid: &str, epoch_ms: i64) -> OrderIntent {
        OrderIntent::new_pending(coid, "TOK", "buy", "1", "0.99", epoch_ms / 1000, epoch_ms)
    }

    #[test]
    fn gate_proceeds_on_fresh_coid() {
        let p = tmp_log("fresh");
        let _ = std::fs::remove_file(&p);
        assert_eq!(idempotency_gate(&p, "never-seen"), GateDecision::Proceed);
    }

    #[test]
    fn gate_refuses_double_submit_after_lost_ack() {
        // THE LOST-ACK PROOF: write Pending (the POST goes out but the network
        // never returns an ack -- process dies / TCP RST / etc.). On the next
        // attempt for the SAME coid, the gate MUST refuse (the prior intent
        // could already be in flight or landed; resending blind would double-fill).
        let p = tmp_log("lost_ack");
        let _ = std::fs::remove_file(&p);
        let coid = "lost-ack-1";
        let pending = pending_for(coid, 1_000);
        pending.append(&p).expect("first Pending write-ahead");
        // No ack ever appended. Second attempt: must refuse.
        match idempotency_gate(&p, coid) {
            GateDecision::Refuse(IntentStatus::Pending) => {}
            other => panic!("expected Refuse(Pending), got {other:?}"),
        }
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn gate_refuses_while_submitted_or_landed() {
        let p = tmp_log("submitted_landed");
        let _ = std::fs::remove_file(&p);
        let coid = "sub-1";
        let pending = pending_for(coid, 1_000);
        pending.append(&p).unwrap();
        let mut sub = pending.clone();
        sub.status = IntentStatus::Submitted { order_id: "ord-1".into() };
        sub.append(&p).unwrap();
        match idempotency_gate(&p, coid) {
            GateDecision::Refuse(IntentStatus::Submitted { .. }) => {}
            other => panic!("expected Refuse(Submitted), got {other:?}"),
        }
        let mut landed = sub.clone();
        landed.status = IntentStatus::Landed { order_id: "ord-1".into() };
        landed.append(&p).unwrap();
        match idempotency_gate(&p, coid) {
            GateDecision::Refuse(IntentStatus::Landed { .. }) => {}
            other => panic!("expected Refuse(Landed), got {other:?}"),
        }
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn gate_proceeds_after_abandoned() {
        // Reconcile/decision proved the prior attempt did NOT land. The terminal
        // Abandoned status frees the coid for a fresh write-ahead + POST.
        let p = tmp_log("abandoned");
        let _ = std::fs::remove_file(&p);
        let coid = "ab-1";
        let pending = pending_for(coid, 1_000);
        pending.append(&p).unwrap();
        let mut ab = pending.clone();
        ab.status = IntentStatus::Abandoned { reason: "reconcile proved not-landed".into() };
        ab.append(&p).unwrap();
        assert_eq!(idempotency_gate(&p, coid), GateDecision::Proceed);
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn unresolved_intents_finds_pending_and_ambiguous_latest_only() {
        // Three coids: A is Pending (latest), B is Pending then Landed (terminal),
        // C is Pending then Ambiguous (latest). unresolved_intents must surface
        // A and C, not B.
        let mut recs = Vec::new();
        recs.push(pending_for("A", 1_000));
        recs.push(pending_for("B", 2_000));
        let mut b_landed = pending_for("B", 2_000);
        b_landed.status = IntentStatus::Landed { order_id: "ob".into() };
        recs.push(b_landed);
        recs.push(pending_for("C", 3_000));
        let mut c_amb = pending_for("C", 3_000);
        c_amb.status = IntentStatus::Ambiguous { reason: "post returned but ack lost".into() };
        recs.push(c_amb);
        let un = unresolved_intents(&recs);
        assert_eq!(un.len(), 2);
        assert!(un.iter().any(|r| r.client_order_id == "A"));
        assert!(un.iter().any(|r| r.client_order_id == "C"));
        assert!(!un.iter().any(|r| r.client_order_id == "B"));
    }
}
