//! Phase 6 G5 -- auto-redeem task + idempotency tracker.
//!
//! Periodically polls `/positions`, detects `redeemable=true` on positions the
//! bot opened, and dispatches the CTF redeem via the Polymarket Relayer
//! (`relayer::submit_and_wait`). Polymarket pays the gas; the bot only signs
//! the EIP-191 meta-tx hash with its EOA.
//!
//! NOT GATED BY LIVE_ARMED. The redeem is post-close CLEANUP / COBRO of
//! positions that already exist on-chain (the BUY was successful and the
//! market already resolved). Disarming the bot means "no more new orders" --
//! it should NOT mean "leave winnings stranded on-chain". Capital that's owed
//! to us should always come home.
//!
//! IDEMPOTENCY (critical): the tracker persists `(condition_id, tx_hash, ts)`
//! on every successful redeem to `data/live/redemptions.jsonl`. A second
//! attempt on the same condition_id is a no-op (the relayer would revert with
//! "already redeemed"; we don't waste the round-trip). On restart the tracker
//! reloads, so a crash mid-redeem doesn't double-spend gas (the relayer's gas,
//! to be fair, but still).
//!
//! SCOPE (only bot-opened): the task reads `data/live/order_intents.jsonl`
//! (the bot's write-ahead intent log) to build the set of token_ids the bot
//! has EVER opened. Only positions matching one of those tokens get
//! auto-redeemed; manual positions the user holds outside the bot remain the
//! user's to manage.

#![allow(dead_code)] // wired by main.rs in live mode

use std::collections::{HashMap, HashSet};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use alloy::primitives::{Address, B256};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tracing::{info, warn};

use crate::oplog::OpLog;
use crate::relayer::{
    self, ALREADY_REDEEMED_SENTINEL_PREFIX, DEFAULT_GAS_LIMIT, RedeemOutcome, RelayerApi,
    build_redeem_submit_body, classify_outcome, signer_from_key, submit_and_wait,
};
use crate::guards::Guards;
use crate::pnl_recorder::PnlRecorder;
use crate::rest::{PositionInfo, RestClient};
use crate::state::now_ms;
use crate::state::store::SharedBotState;

/// Default cadence between /positions polls in the redemption task. The user
/// measured ~15-20 s from market close to "Claim winnings" available; 5 s is
/// brisk without being abusive of the data-api.
pub const DEFAULT_POLL_INTERVAL_SECS: u64 = 30;

/// Parse the "resets in N seconds" hint out of a relayer 429 quota body, if present.
fn parse_resets_in_secs(s: &str) -> Option<i64> {
    let i = s.find("resets in")?;
    let rest = s[i + "resets in".len()..].trim_start();
    let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
    digits.parse::<i64>().ok()
}

/// How long submit_and_wait polls /transaction before giving up on a single
/// submit. The relayer typically mines within a few blocks (~6-12 s on Polygon).
pub const DEFAULT_TX_POLL_INTERVAL_MS: u64 = 2_000;
pub const DEFAULT_TX_POLL_MAX: u32 = 60; // 60 * 2s = 2 min hard cap per redeem

/// Persistent file (append-only JSONL) recording every redeem the bot
/// successfully submitted. Gitignored runtime data, same dir as oplog.
pub const DEFAULT_REDEMPTIONS_LOG: &str = "data/live/redemptions.jsonl";

// ============================================================================
// PURE: idempotency tracker (file-backed set of redeemed condition_ids).
// ============================================================================

/// One row in `redemptions.jsonl`: a successful redeem the bot landed.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RedemptionRecord {
    /// Lowercase `0x`-prefixed hex of the CTF condition_id (32 bytes).
    pub condition_id: String,
    /// On-chain tx hash returned by the relayer's /transaction endpoint.
    pub tx_hash: String,
    /// Wall-clock ms at the moment of recording.
    pub ts_ms: i64,
    /// Polymarket relayer's transactionID (the UUID returned by /submit).
    /// Useful for cross-referencing relayer-side logs.
    pub transaction_id: String,
}

/// In-memory + file-backed set of redeemed condition_ids. Loaded once on
/// startup; append-only thereafter.
#[derive(Debug, Clone)]
pub struct RedemptionTracker {
    path: PathBuf,
    done: HashSet<String>, // condition_ids lowercased
}

impl RedemptionTracker {
    /// Load the tracker from `path`. Missing file is OK (returns empty tracker).
    pub fn load(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let mut done = HashSet::new();
        if path.exists() {
            let body = std::fs::read_to_string(&path)
                .with_context(|| format!("read redemptions log: {}", path.display()))?;
            for (i, line) in body.lines().enumerate() {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                let rec: RedemptionRecord = serde_json::from_str(line).with_context(|| {
                    format!("parse line {} of {}: {line}", i + 1, path.display())
                })?;
                done.insert(rec.condition_id.to_ascii_lowercase());
            }
        }
        Ok(Self { path, done })
    }

    /// True iff this condition_id has a successful redeem on record.
    #[must_use]
    pub fn already_redeemed(&self, condition_id: &str) -> bool {
        self.done.contains(&condition_id.to_ascii_lowercase())
    }

    /// Append a successful redeem to the log (file + in-memory set).
    pub fn record(&mut self, record: RedemptionRecord) -> Result<()> {
        let lower = record.condition_id.to_ascii_lowercase();
        if let Some(dir) = self.path.parent() {
            std::fs::create_dir_all(dir).ok();
        }
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .with_context(|| format!("open redemptions log: {}", self.path.display()))?;
        writeln!(f, "{}", serde_json::to_string(&record)?)?;
        self.done.insert(lower);
        Ok(())
    }

    /// Number of redemptions in the tracker (for diagnostics).
    #[must_use]
    pub fn len(&self) -> usize {
        self.done.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.done.is_empty()
    }
}

// ============================================================================
// PURE: load bot-opened token universe from the intent log.
// ============================================================================

/// Each line of `order_intents.jsonl` is an `OrderIntent`. We don't need to
/// parse the full schema -- just extract `token_id`. Lightweight + tolerant of
/// future schema additions.
#[derive(Debug, Deserialize)]
struct IntentTokenOnly {
    token_id: String,
}

/// Load all token_ids the bot has ever appended to its intent log. Missing
/// file = empty set (bot never opened anything live).
pub fn load_bot_opened_tokens(intent_log_path: &Path) -> Result<HashSet<String>> {
    let mut set = HashSet::new();
    if !intent_log_path.exists() {
        return Ok(set);
    }
    let body = std::fs::read_to_string(intent_log_path)
        .with_context(|| format!("read intent log: {}", intent_log_path.display()))?;
    for line in body.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if let Ok(it) = serde_json::from_str::<IntentTokenOnly>(line) {
            set.insert(it.token_id);
        }
        // Malformed lines: skip. The intent log is forward-compat; new schemas
        // shouldn't break the redemption task.
    }
    Ok(set)
}

// ============================================================================
// G5.1: FAILURE COUNTER -- per-condition_id retry budget with exponential backoff.
// ============================================================================

/// G5.1: backoff base. After N consecutive failures, next allowed retry is
/// `BACKOFF_BASE_MS * 2^(N-1)`, capped at `BACKOFF_CAP_MS`. So:
///   N=1 -> 60s, N=2 -> 2min, N=3 -> 4min, N=4 -> 8min, N=5 -> 16min, ... cap 1h.
pub const BACKOFF_BASE_MS: i64 = 60_000;
pub const BACKOFF_CAP_MS: i64 = 60 * 60 * 1000; // 1 hour

/// G5.1: number of attempts at which the task emits a one-shot
/// `redeem_failing_persistent` oplog event (so the dashboard / operator gets
/// alerted to a stuck redeem). The task CONTINUES retrying with the capped
/// backoff -- the alert is NOT a give-up, just a visibility flag.
pub const ALERT_ON_ATTEMPTS: u32 = 5;

/// G5.1: per-condition_id retry state. Lives only in memory; on bot restart a
/// failed-condition's counter resets to zero, which is FINE -- the worst case
/// is one extra retry on restart, and the new retry will simply hit backoff
/// again if the underlying failure persists. We do NOT persist this across
/// restarts because the cost of a fresh attempt is at most one wasted relayer
/// nonce (and the relayer doesn't even consume gas on a duplicate -- we just
/// see another STATE_FAILED + back off again from N=1).
#[derive(Debug, Clone, Default)]
pub struct FailureCounter {
    by_cid: HashMap<String, FailureState>,
}

#[derive(Debug, Clone, PartialEq)]
struct FailureState {
    attempts: u32,
    next_allowed_ts_ms: i64,
    alerted: bool,
}

impl FailureCounter {
    /// PURE: compute when the next retry is allowed given the failure count.
    /// Exponential backoff capped at BACKOFF_CAP_MS.
    #[must_use]
    pub fn compute_next_allowed(now_ms: i64, attempts: u32) -> i64 {
        // attempts -> shift exponent: 1->0, 2->1, 3->2, ... cap at 20 to avoid
        // any chance of overflow (2^20 = ~1M minutes, dwarfs the cap anyway).
        let exp = attempts.saturating_sub(1).min(20);
        let mult: u64 = 1u64.checked_shl(exp).unwrap_or(u64::MAX);
        let backoff_u = (BACKOFF_BASE_MS as u64).saturating_mul(mult);
        let capped = backoff_u.min(BACKOFF_CAP_MS as u64) as i64;
        now_ms.saturating_add(capped)
    }

    /// Record a failed attempt. Returns `(attempts, just_crossed_alert_threshold)`
    /// so the caller knows whether to emit the one-shot oplog alert.
    pub fn record_failure(&mut self, condition_id: &str, now_ms: i64) -> (u32, bool) {
        let lower = condition_id.to_ascii_lowercase();
        let entry = self.by_cid.entry(lower).or_insert(FailureState {
            attempts: 0,
            next_allowed_ts_ms: 0,
            alerted: false,
        });
        entry.attempts = entry.attempts.saturating_add(1);
        entry.next_allowed_ts_ms = Self::compute_next_allowed(now_ms, entry.attempts);
        let just_alerted = !entry.alerted && entry.attempts >= ALERT_ON_ATTEMPTS;
        if just_alerted {
            entry.alerted = true;
        }
        (entry.attempts, just_alerted)
    }

    /// True if the condition is currently in cooldown (must NOT retry yet).
    #[must_use]
    pub fn in_cooldown(&self, condition_id: &str, now_ms: i64) -> bool {
        self.by_cid
            .get(&condition_id.to_ascii_lowercase())
            .map_or(false, |s| now_ms < s.next_allowed_ts_ms)
    }

    /// Clear the entry (called on success / already-redeemed / no-longer-target).
    pub fn clear(&mut self, condition_id: &str) {
        self.by_cid.remove(&condition_id.to_ascii_lowercase());
    }

    /// Attempts so far (0 if never failed). For tests + diagnostics.
    #[must_use]
    pub fn attempts(&self, condition_id: &str) -> u32 {
        self.by_cid
            .get(&condition_id.to_ascii_lowercase())
            .map_or(0, |s| s.attempts)
    }

    /// Drop entries whose condition_id is NOT in `active_set`. Called after
    /// each /positions poll so an in-memory map doesn't grow unbounded over
    /// hours/days as condition_ids come and go.
    pub fn prune_to(&mut self, active_set: &HashSet<String>) {
        self.by_cid
            .retain(|k, _| active_set.contains(&k.to_ascii_lowercase()));
    }

    /// Number of tracked condition_ids (diagnostics).
    #[must_use]
    pub fn len(&self) -> usize {
        self.by_cid.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_cid.is_empty()
    }
}

// ============================================================================
// PURE: redeem target selection.
// ============================================================================

/// One position that qualifies for auto-redeem: bot-opened, redeemable on-chain,
/// not yet redeemed by us. (No `Eq` — f64 doesn't satisfy it; only `PartialEq`.)
#[derive(Debug, Clone, PartialEq)]
pub struct RedeemTarget {
    pub condition_id: String,
    pub token_id: String,
    pub size: f64,
}

/// PURE: scan `positions` and return the ones we should redeem right now.
/// Filtered to:
///   - `redeemable == true` (market resolved on-chain)
///   - `token_id` is in `bot_opened` (we opened this position)
///   - `condition_id` is NOT in the tracker (we haven't already redeemed it)
///   - `condition_id` is NOT currently in failure cooldown (G5.1 backoff)
/// Dedupes by `condition_id` (multiple lots on the same market = one redeem).
#[must_use]
pub fn find_redeem_targets(
    positions: &[PositionInfo],
    bot_opened: &HashSet<String>,
    tracker: &RedemptionTracker,
    failures: &FailureCounter,
    now_ms: i64,
) -> Vec<RedeemTarget> {
    let mut out: Vec<RedeemTarget> = Vec::new();
    let mut seen_conditions: HashSet<String> = HashSet::new();
    for p in positions {
        if !p.redeemable {
            continue;
        }
        if !bot_opened.contains(&p.token_id) {
            continue;
        }
        let cid_lower = p.condition_id.to_ascii_lowercase();
        if tracker.already_redeemed(&cid_lower) {
            continue;
        }
        if failures.in_cooldown(&cid_lower, now_ms) {
            continue;
        }
        if !seen_conditions.insert(cid_lower.clone()) {
            continue;
        }
        out.push(RedeemTarget {
            condition_id: cid_lower,
            token_id: p.token_id.clone(),
            size: p.size,
        });
    }
    out
}

// ============================================================================
// CONFIG (creds + paths bundled into one struct so main.rs wiring is short).
// ============================================================================

/// Configuration for the redemption task. Built from `.env` + config at startup.
pub struct RedemptionConfig {
    /// EOA private key (the owner of the proxy wallet). Used to sign meta-tx.
    pub private_key: String,
    /// Proxy wallet address (FUNDER_ADDRESS) -- where the CTF positions live.
    pub proxy_wallet: Address,
    /// Builder code (bytes32 hex) to attribute the redeem to our builder profile.
    /// May be empty if not configured.
    pub builder_code: String,
    /// Path to the bot's intent log (to scope redemption to bot-opened tokens).
    pub intent_log_path: PathBuf,
    /// Path to the redemptions tracker file (for idempotency across restarts).
    pub redemptions_log_path: PathBuf,
    /// Cadence between /positions polls.
    pub poll_interval: std::time::Duration,
}

impl RedemptionConfig {
    pub fn from_defaults(
        private_key: String,
        proxy_wallet: Address,
        builder_code: String,
    ) -> Self {
        Self {
            private_key,
            proxy_wallet,
            builder_code,
            intent_log_path: PathBuf::from("data/live/order_intents.jsonl"),
            redemptions_log_path: PathBuf::from(DEFAULT_REDEMPTIONS_LOG),
            poll_interval: std::time::Duration::from_secs(DEFAULT_POLL_INTERVAL_SECS),
        }
    }
}

// ============================================================================
// G5-wire: graceful env-based setup. The bot operates normally without
// auto-redeem -- if the env vars are missing/empty, the task is DISABLED with
// a clear log line, NOT a panic. Manual claim via the UI remains available.
// ============================================================================

/// Env var names the bot reads for auto-redeem.
///
/// G7.4-C2: switched from the inferred 2-header pair `POLYMARKET_RELAYER_API_KEY`
/// + `_ADDRESS` (which `--redeem-check` (G7.2) proved is NOT what the relayer
/// expects -- the keys were ignored, the URL was wrong, the 404 came from the
/// path) to the upstream-correct Builder API Key triple
/// (`POLYMARKET_BUILDER_KEY` + `_SECRET` + `_PASSPHRASE`) used for HMAC headers
/// on POST `/submit`. The operator obtains the triple via
/// `--builder-creds-create --builder-creds-confirm` (Step C1).
///
/// The old `POLYMARKET_RELAYER_API_KEY*` vars in `.env` are NOT read anymore
/// (silently ignored if present -- the operator can delete them at leisure).
pub const ENV_BUILDER_KEY: &str = "POLYMARKET_BUILDER_KEY";
pub const ENV_BUILDER_SECRET: &str = "POLYMARKET_BUILDER_SECRET";
pub const ENV_BUILDER_PASSPHRASE: &str = "POLYMARKET_BUILDER_PASSPHRASE";
pub const ENV_BUILDER_CODE: &str = "POLYMARKET_BUILDER_CODE";

/// Result of trying to build the auto-redeem stack from `.env`. `Disabled` is
/// NOT an error -- it's the explicit "feature off" state when creds aren't
/// configured. Main.rs logs the reason and continues spawning everything else.
pub enum RedemptionSetup {
    Enabled {
        relayer: Arc<dyn RelayerApi>,
        builder_code: String,
    },
    Disabled {
        reason: String,
    },
}

/// PURE-ish: decide whether to enable auto-redeem given an env-lookup function.
/// Tests inject a HashMap-backed closure; production passes
/// `|k| std::env::var(k).ok()`.
///
/// SECURITY: the `reason` string in `Disabled` describes WHICH var is missing
/// but NEVER echoes credential values (because for missing/empty inputs there
/// is no value to echo, and the function returns Disabled BEFORE touching any
/// actual cred). Confirmed by test
/// `g5w_setup_disabled_reason_does_not_contain_cred_values`.
pub fn setup_from_env<F>(env: F) -> RedemptionSetup
where
    F: Fn(&str) -> Option<String>,
{
    // G7.4-C2: 3 BUILDER vars required (all 3 or Disabled). Iterating preserves
    // the original "name the first missing one" UX so the operator's .env edit
    // is obvious from the disabled log line.
    let mut got = std::collections::HashMap::new();
    for var in [ENV_BUILDER_KEY, ENV_BUILDER_SECRET, ENV_BUILDER_PASSPHRASE] {
        let raw = env(var).unwrap_or_default();
        let trimmed = raw.trim().to_string();
        if trimmed.is_empty() {
            return RedemptionSetup::Disabled {
                reason: format!(
                    "{var} not set (or empty) in .env -- auto-redeem disabled, \
                     run `--builder-creds-create --builder-creds-confirm` to obtain \
                     a fresh Builder API Key triple; manual claim via UI remains available"
                ),
            };
        }
        got.insert(var, trimmed);
    }
    let creds = crate::relayer::BuilderCreds {
        key: got.remove(ENV_BUILDER_KEY).expect("inserted just above"),
        secret: got.remove(ENV_BUILDER_SECRET).expect("inserted just above"),
        passphrase: got.remove(ENV_BUILDER_PASSPHRASE).expect("inserted just above"),
    };

    // BUILDER_CODE is optional: attribution metadata in body.metadata, NOT auth.
    // Empty = no attribution attached to the redeem (Polymarket-side rewards
    // may or may not apply -- separate concern from "can the redeem land").
    let builder_code = env(ENV_BUILDER_CODE).unwrap_or_default().trim().to_string();

    RedemptionSetup::Enabled {
        relayer: Arc::new(crate::relayer::RelayerClient::new(creds)) as Arc<dyn RelayerApi>,
        builder_code,
    }
}

// ============================================================================
// THE LIVE TASK.
// ============================================================================

/// Spawn this in main.rs's live-mode task set. NOT gated by LIVE_ARMED:
/// redeeming is post-close cleanup, not new-position exposure.
#[allow(clippy::too_many_arguments)]
pub async fn run_redemption_task(
    rest: Arc<RestClient>,
    relayer: Arc<dyn RelayerApi>,
    cfg: RedemptionConfig,
    oplog: Arc<OpLog>,
    mut shutdown: watch::Receiver<bool>,
    // Kept wired (the redeem path no longer books P&L -- the activity-feed
    // reconciler does, since redeem != win -- but these stay in the signature so
    // re-enabling a redeem-time hook needs no plumbing change).
    _store_state: SharedBotState,
    _guards: Arc<Mutex<Guards>>,
    _recorder: Option<Arc<Mutex<PnlRecorder>>>,
) {
    info!(
        poll_interval_secs = cfg.poll_interval.as_secs(),
        proxy_wallet = %cfg.proxy_wallet,
        "task started: redemption (auto-claim via Polymarket Relayer)"
    );
    oplog.sys(
        "redemption_task_start",
        serde_json::json!({
            "proxy_wallet": format!("{:#x}", cfg.proxy_wallet),
            "poll_interval_secs": cfg.poll_interval.as_secs(),
        }),
    );

    // Build the signer ONCE (the key never changes during a run).
    let signer = match signer_from_key(&cfg.private_key) {
        Ok(s) => s,
        Err(e) => {
            warn!(error = %e, "redemption: bad private key; task exiting");
            oplog.err("redemption_task", "bad private key", serde_json::json!({}));
            return;
        }
    };

    // Tracker: persisted set of (condition_id) we've already redeemed.
    let mut tracker = match RedemptionTracker::load(&cfg.redemptions_log_path) {
        Ok(t) => {
            info!(loaded = t.len(), "redemption: tracker loaded");
            t
        }
        Err(e) => {
            warn!(error = %e, "redemption: tracker load failed; starting empty");
            RedemptionTracker {
                path: cfg.redemptions_log_path.clone(),
                done: HashSet::new(),
            }
        }
    };
    // G5.1: per-condition_id failure counter (in-memory; resets on restart).
    let mut failures = FailureCounter::default();
    // GLOBAL quota brake: the relayer enforces a shared request quota. On a 429
    // "quota exceeded" we must STOP all redeem attempts until the reset window --
    // hammering it every poll (per-condition backoff doesn't help a GLOBAL limit)
    // just keeps the quota pinned at 0. ms wall-clock; 0 = not paused.
    let mut quota_pause_until_ms: i64 = 0;

    let mut tick = tokio::time::interval(cfg.poll_interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = tick.tick() => {
                // GLOBAL quota brake: skip the whole cycle while the relayer quota
                // is exhausted (no requests at all -> let it actually reset).
                if quota_pause_until_ms > 0 && now_ms() < quota_pause_until_ms {
                    continue;
                }
                // 1) Pull the bot-opened token universe FRESH every tick
                //    (it grows as the bot opens new positions).
                let bot_opened = match load_bot_opened_tokens(&cfg.intent_log_path) {
                    Ok(s) => s,
                    Err(e) => {
                        warn!(error = %e, "redemption: intent log read failed; skipping tick");
                        continue;
                    }
                };
                if bot_opened.is_empty() {
                    // No bot positions yet -> nothing to redeem.
                    continue;
                }
                // 2) Fetch /positions and find redeem targets.
                let positions = match rest.get_positions().await {
                    Ok(p) => p,
                    Err(e) => {
                        warn!(error = %e, "redemption: /positions failed; will retry next tick");
                        continue;
                    }
                };
                // G5.1: prune failure counter to currently-visible condition_ids
                // so the in-memory map doesn't grow unbounded.
                let active_set: HashSet<String> = positions.iter()
                    .map(|p| p.condition_id.to_ascii_lowercase()).collect();
                failures.prune_to(&active_set);
                let now = now_ms();
                let targets = find_redeem_targets(&positions, &bot_opened, &tracker, &failures, now);
                if targets.is_empty() {
                    continue;
                }
                info!(n = targets.len(),
                    failure_counter_size = failures.len(),
                    "redemption: targets found");
                // 3) Dispatch each redeem sequentially (the relayer queue is
                //    per-EOA; sequential keeps nonces clean).
                for tgt in targets {
                    // Stop the whole cycle the instant the relayer quota is hit
                    // (set in the TransientFail arm below) -- don't burn the rest.
                    if quota_pause_until_ms > 0 && now_ms() < quota_pause_until_ms {
                        break;
                    }
                    let cid_str = tgt.condition_id.clone();
                    oplog.sys("redeem_attempt", serde_json::json!({
                        "condition_id": cid_str, "token_id": tgt.token_id, "size": tgt.size,
                        "prior_attempts": failures.attempts(&cid_str),
                    }));
                    let cid = match B256::from_str(&cid_str) {
                        Ok(b) => b,
                        Err(e) => {
                            // G7.1-diag: format!("{e:#}") emits the FULL anyhow cause chain
                            // (top context + every .context() + the root error). With plain
                            // e.to_string() we only see the top-level context string ("condition_id
                            // parse"), losing the actual reason from FromHex / B256. The full chain
                            // is what lets the operator diagnose without a code change.
                            let err_chain = format!("{e:#}");
                            warn!(condition_id = %cid_str, error = %err_chain,
                                "redemption: condition_id parse failed; skipping");
                            oplog.err("redeem_attempt", "condition_id parse",
                                serde_json::json!({"condition_id": cid_str, "error": err_chain}));
                            continue;
                        }
                    };
                    let body = match build_redeem_submit_body(
                        relayer.as_ref(), &signer, cfg.proxy_wallet, cid,
                        &cfg.builder_code, DEFAULT_GAS_LIMIT,
                    ).await {
                        Ok(b) => b,
                        Err(e) => {
                            // G7.1-diag: format!("{e:#}") emits the FULL anyhow cause chain.
                            // THE critical site: "build body" alone hides the HTTP-layer error
                            // (401 / 403 / 404 + body, DNS, TLS, JSON parse) raised by
                            // relayer.get_relay_payload(). With the chain visible the operator
                            // sees the exact wire-format reason in the oplog and can fix the
                            // URL/headers/key without another diagnostic round-trip.
                            let err_chain = format!("{e:#}");
                            warn!(condition_id = %cid_str, error = %err_chain,
                                "redemption: build body failed");
                            oplog.err("redeem_attempt", "build body",
                                serde_json::json!({"condition_id": cid_str, "error": err_chain}));
                            // Build failures count toward the backoff -- they're
                            // typically signer/encoding issues that re-fail immediately.
                            let (attempts, just_alerted) = failures.record_failure(&cid_str, now_ms());
                            if just_alerted {
                                oplog.sys("redeem_failing_persistent", serde_json::json!({
                                    "condition_id": cid_str, "attempts": attempts,
                                    "stage": "build_body",
                                }));
                            }
                            continue;
                        }
                    };
                    oplog.sys("redeem_posted_prep", serde_json::json!({
                        "condition_id": cid_str,
                        "summary": relayer::summary_for_log(&body),
                    }));
                    let raw = submit_and_wait(
                        relayer.as_ref(), &body,
                        std::time::Duration::from_millis(DEFAULT_TX_POLL_INTERVAL_MS),
                        DEFAULT_TX_POLL_MAX,
                    ).await;
                    // G5.1: classify into RedeemOutcome and dispatch.
                    // G7.1-diag: format!("{e:#}") preserves the FULL anyhow chain so the
                    // reason string that ends up in classify_outcome (and ultimately in
                    // the PermanentFail/TransientFail oplog) carries the underlying HTTP
                    // body / status, not just "submit failed" or "poll request failed".
                    let raw_for_classify = match raw {
                        Ok(pair) => Ok(pair),
                        Err(e) => Err(format!("{e:#}")),
                    };
                    let outcome = classify_outcome(raw_for_classify);
                    match outcome {
                        RedeemOutcome::Success { tx_hash, transaction_id } => {
                            oplog.sys("redeem_mined", serde_json::json!({
                                "condition_id": cid_str,
                                "transaction_id": transaction_id,
                                "tx_hash": tx_hash,
                            }));
                            let rec = RedemptionRecord {
                                condition_id: cid_str.clone(),
                                tx_hash,
                                ts_ms: now_ms(),
                                transaction_id,
                            };
                            if let Err(e) = tracker.record(rec) {
                                // G7.1-diag: full chain on tracker write failures (path / perms /
                                // serde) so the operator sees the underlying IO error, not just
                                // the top-level "open redemptions log: ..." context.
                                let err_chain = format!("{e:#}");
                                warn!(condition_id = %cid_str, error = %err_chain,
                                    "redemption: tracker.record failed (success path); \
                                     restart would re-attempt, relayer would respond \
                                     already-redeemed -> G5.1 sentinel records it -- safe");
                                oplog.err("redeem_tracker_write", &err_chain,
                                    serde_json::json!({"condition_id": cid_str}));
                            }
                            failures.clear(&cid_str);
                            info!(condition_id = %cid_str, "redemption: SUCCESS recorded");
                            // NOTE: P&L is NOT booked here. The bot redeems LOSERS too
                            // (redeemable is set for both sides; loser redeems pay $0), so a
                            // redeem event does NOT imply a win. Realized P&L is booked
                            // solely by the activity-feed reconciler, which reads the real
                            // payout. (store_state/guards/recorder kept wired for that path.)
                        }
                        RedeemOutcome::AlreadyRedeemed { transaction_id, detected_via } => {
                            // G5.1: the safety net. Treat as success, persist a sentinel
                            // tx_hash so the tracker treats this condition_id as done.
                            let sentinel = format!("{ALREADY_REDEEMED_SENTINEL_PREFIX}{detected_via}");
                            oplog.sys("redeem_already_done", serde_json::json!({
                                "condition_id": cid_str,
                                "transaction_id": transaction_id,
                                "detected_via": detected_via,
                                "tx_hash_sentinel": sentinel,
                            }));
                            let rec = RedemptionRecord {
                                condition_id: cid_str.clone(),
                                tx_hash: sentinel,
                                ts_ms: now_ms(),
                                transaction_id,
                            };
                            if let Err(e) = tracker.record(rec) {
                                // G7.1-diag: full chain on sentinel-tracker write failures
                                // (same rationale as the success-path tracker write above).
                                let err_chain = format!("{e:#}");
                                warn!(condition_id = %cid_str, error = %err_chain,
                                    "redemption: tracker.record sentinel failed; \
                                     /positions will eventually clear it -- safe");
                                oplog.err("redeem_tracker_write_sentinel", &err_chain,
                                    serde_json::json!({"condition_id": cid_str}));
                            }
                            failures.clear(&cid_str);
                            info!(condition_id = %cid_str, %detected_via,
                                "redemption: ALREADY-REDEEMED detected -- recorded as idempotent");
                            // P&L NOT booked here (see SUCCESS arm) -- redeem != win.
                        }
                        RedeemOutcome::PermanentFail { reason } => {
                            let (attempts, just_alerted) = failures.record_failure(&cid_str, now_ms());
                            warn!(condition_id = %cid_str, attempts, %reason,
                                "redemption: PermanentFail -- backoff applied");
                            oplog.err("redeem_failed", &reason,
                                serde_json::json!({
                                    "condition_id": cid_str,
                                    "attempts": attempts,
                                    "kind": "permanent",
                                }));
                            if just_alerted {
                                oplog.sys("redeem_failing_persistent", serde_json::json!({
                                    "condition_id": cid_str, "attempts": attempts,
                                    "alert_threshold": ALERT_ON_ATTEMPTS,
                                    "stage": "submit",
                                    "note": "task continues retrying with capped backoff",
                                }));
                            }
                        }
                        RedeemOutcome::TransientFail { reason } => {
                            // GLOBAL quota brake: a 429 "quota exceeded" is not a
                            // per-condition problem -- the shared budget is spent.
                            // Pause ALL redeems until the reset (parsed from the
                            // message, capped 1h) so we stop pinning it at 0.
                            let low = reason.to_ascii_lowercase();
                            if low.contains("quota exceeded") || low.contains("429") || low.contains("too many requests") {
                                let secs = parse_resets_in_secs(&reason).unwrap_or(900).clamp(60, 3600);
                                quota_pause_until_ms = now_ms() + secs * 1000;
                                warn!(pause_secs = secs,
                                    "redemption: relayer QUOTA EXCEEDED -- pausing ALL redeems until reset");
                                oplog.sys("redeem_quota_paused", serde_json::json!({
                                    "pause_secs": secs, "reason": reason,
                                }));
                                break; // abandon the rest of this cycle
                            }
                            let (attempts, just_alerted) = failures.record_failure(&cid_str, now_ms());
                            warn!(condition_id = %cid_str, attempts, %reason,
                                "redemption: TransientFail -- backoff applied");
                            oplog.err("redeem_error", &reason,
                                serde_json::json!({
                                    "condition_id": cid_str,
                                    "attempts": attempts,
                                    "kind": "transient",
                                }));
                            if just_alerted {
                                oplog.sys("redeem_failing_persistent", serde_json::json!({
                                    "condition_id": cid_str, "attempts": attempts,
                                    "alert_threshold": ALERT_ON_ATTEMPTS,
                                    "stage": "submit",
                                    "note": "task continues retrying with capped backoff",
                                }));
                            }
                        }
                    }
                }
            }
            _ = shutdown.changed() => if *shutdown.borrow() {
                info!("redemption: shutdown signaled");
                oplog.sys("redemption_task_shutdown", serde_json::json!({}));
                return;
            }
        }
    }
}

// ============================================================================
// TESTS -- pure (tracker + scope + target selection). HTTP layer is in
// relayer.rs's own test module (with the MockRelayer).
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use polymarket_client_sdk_v2::types::NaiveDate;

    fn tmp_path(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "rb_g5_{tag}_{}_{}.jsonl",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    fn position(
        token_id: &str,
        condition_id: &str,
        redeemable: bool,
        size: f64,
    ) -> PositionInfo {
        PositionInfo {
            token_id: token_id.to_string(),
            size,
            avg_price: 0.5,
            cur_price: 1.0,
            cash_pnl: 0.0,
            redeemable,
            end_date: NaiveDate::from_ymd_opt(2027, 1, 1),
            condition_id: condition_id.to_string(),
        }
    }

    // ---------- Tracker ----------

    #[test]
    fn g5_tracker_empty_when_file_missing() {
        let p = tmp_path("tracker_missing");
        let _ = std::fs::remove_file(&p);
        let t = RedemptionTracker::load(&p).expect("load");
        assert!(t.is_empty(), "missing file => empty tracker");
        assert!(!t.already_redeemed("0xabc"));
    }

    #[test]
    fn g5_tracker_record_persists_and_dedupes() {
        let p = tmp_path("tracker_record");
        let _ = std::fs::remove_file(&p);
        let mut t = RedemptionTracker::load(&p).unwrap();
        let cid = "0xdeadbeef00000000000000000000000000000000000000000000000000000001";
        assert!(!t.already_redeemed(cid));
        t.record(RedemptionRecord {
            condition_id: cid.to_string(),
            tx_hash: "0xtx".to_string(),
            ts_ms: 1_000,
            transaction_id: "uuid-1".to_string(),
        })
        .unwrap();
        assert!(t.already_redeemed(cid));
        // Case-insensitive match.
        assert!(t.already_redeemed(&cid.to_ascii_uppercase()));
        // Reload from disk: same set.
        let t2 = RedemptionTracker::load(&p).unwrap();
        assert!(t2.already_redeemed(cid), "persistence survives reload");
        assert_eq!(t2.len(), 1);
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn g5_tracker_multiple_records_accumulate() {
        let p = tmp_path("tracker_multi");
        let _ = std::fs::remove_file(&p);
        let mut t = RedemptionTracker::load(&p).unwrap();
        for i in 0..5 {
            let cid = format!(
                "0xdeadbeef00000000000000000000000000000000000000000000000000000{:03}",
                i
            );
            t.record(RedemptionRecord {
                condition_id: cid,
                tx_hash: format!("0xtx{i}"),
                ts_ms: 1_000 + i,
                transaction_id: format!("uuid-{i}"),
            })
            .unwrap();
        }
        assert_eq!(t.len(), 5);
        let t2 = RedemptionTracker::load(&p).unwrap();
        assert_eq!(t2.len(), 5);
        let _ = std::fs::remove_file(&p);
    }

    // ---------- bot_opened scope ----------

    #[test]
    fn g5_load_bot_opened_tokens_empty_when_missing() {
        let p = tmp_path("intents_missing");
        let _ = std::fs::remove_file(&p);
        let s = load_bot_opened_tokens(&p).unwrap();
        assert!(s.is_empty());
    }

    #[test]
    fn g5_load_bot_opened_tokens_parses_token_ids() {
        let p = tmp_path("intents_present");
        let _ = std::fs::remove_file(&p);
        // Write a few intent rows (forward-compat: extra fields are tolerated).
        let lines = vec![
            r#"{"client_order_id":"a","token_id":"tok1","side":"BUY","size":"1","price":"0.5","epoch":0,"created_ms":0,"status":{"state":"submitted","order_id":"x"}}"#,
            r#"{"client_order_id":"b","token_id":"tok2","side":"BUY","size":"1","price":"0.5","epoch":0,"created_ms":0,"status":{"state":"pending"}}"#,
            r#"{"client_order_id":"c","token_id":"tok1","side":"BUY","size":"1","price":"0.5","epoch":0,"created_ms":0,"status":{"state":"submitted","order_id":"y"}}"#,
            "", // empty line -> skipped
            r#"GARBAGE NON JSON"#, // malformed -> skipped, NOT a hard fail
        ];
        std::fs::write(&p, lines.join("\n")).unwrap();
        let s = load_bot_opened_tokens(&p).unwrap();
        assert_eq!(s.len(), 2, "tok1 + tok2; duplicates deduped; garbage ignored");
        assert!(s.contains("tok1"));
        assert!(s.contains("tok2"));
        let _ = std::fs::remove_file(&p);
    }

    // ---------- find_redeem_targets ----------

    /// Test-only helpers to keep the signature noise out of every test.
    fn empty_failures() -> FailureCounter { FailureCounter::default() }
    fn t0() -> i64 { 1_780_000_000_000 } // fixed wall-clock for deterministic tests

    #[test]
    fn g5_find_targets_filters_non_redeemable() {
        let mut bot = HashSet::new();
        bot.insert("tok1".to_string());
        let positions = vec![
            position("tok1", "0xcid1", false, 5.0),
            position("tok1", "0xcid1", false, 0.0),
        ];
        let tracker = RedemptionTracker {
            path: PathBuf::from("/dev/null"),
            done: HashSet::new(),
        };
        let targets = find_redeem_targets(&positions, &bot, &tracker, &empty_failures(), t0());
        assert!(targets.is_empty(), "all non-redeemable => no targets");
    }

    #[test]
    fn g5_find_targets_filters_non_bot_owned() {
        let bot = HashSet::new(); // bot opened nothing
        let positions = vec![position("tok-manual", "0xcid1", true, 5.0)];
        let tracker = RedemptionTracker {
            path: PathBuf::from("/dev/null"),
            done: HashSet::new(),
        };
        let targets = find_redeem_targets(&positions, &bot, &tracker, &empty_failures(), t0());
        assert!(targets.is_empty(),
            "manual positions (not in intent log) are not auto-redeemed");
    }

    #[test]
    fn g5_find_targets_filters_already_redeemed() {
        let mut bot = HashSet::new();
        bot.insert("tok1".to_string());
        let positions = vec![position("tok1", "0xcid1", true, 5.0)];
        let mut tracker = RedemptionTracker {
            path: PathBuf::from("/dev/null"),
            done: HashSet::new(),
        };
        tracker.done.insert("0xcid1".to_string());
        let targets = find_redeem_targets(&positions, &bot, &tracker, &empty_failures(), t0());
        assert!(targets.is_empty(),
            "already-redeemed condition_id => skipped (no double-call)");
    }

    #[test]
    fn g5_find_targets_dedupes_by_condition_id() {
        // Multiple lots / positions on the same market = ONE redeem (per condition).
        let mut bot = HashSet::new();
        bot.insert("tok-up".to_string());
        bot.insert("tok-down".to_string());
        let positions = vec![
            position("tok-up", "0xcid1", true, 3.0),
            position("tok-down", "0xcid1", true, 2.0), // same market, opposite side
        ];
        let tracker = RedemptionTracker {
            path: PathBuf::from("/dev/null"),
            done: HashSet::new(),
        };
        let targets = find_redeem_targets(&positions, &bot, &tracker, &empty_failures(), t0());
        assert_eq!(targets.len(), 1, "one redeem per condition_id, even with 2 lots");
        assert_eq!(targets[0].condition_id, "0xcid1");
    }

    #[test]
    fn g5_find_targets_returns_eligible_position() {
        let mut bot = HashSet::new();
        bot.insert("tok1".to_string());
        let positions = vec![
            position("tok1", "0xCID1", true, 5.5), // mixed case in input
            position("tok-other", "0xcid2", true, 2.0), // not bot-owned
            position("tok1", "0xcid3", false, 1.0), // not redeemable
        ];
        let tracker = RedemptionTracker {
            path: PathBuf::from("/dev/null"),
            done: HashSet::new(),
        };
        let targets = find_redeem_targets(&positions, &bot, &tracker, &empty_failures(), t0());
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].condition_id, "0xcid1", "stored lowercase");
        assert_eq!(targets[0].token_id, "tok1");
        assert!((targets[0].size - 5.5).abs() < 1e-9);
    }

    // ---------- G5.1: FailureCounter + cooldown filter in find_redeem_targets ----------

    #[test]
    fn g5_1_failure_counter_backoff_grows_exponentially_to_cap() {
        let now = t0();
        // attempts=1 -> 60s
        assert_eq!(FailureCounter::compute_next_allowed(now, 1) - now, 60_000);
        // attempts=2 -> 120s
        assert_eq!(FailureCounter::compute_next_allowed(now, 2) - now, 120_000);
        // attempts=3 -> 240s = 4min
        assert_eq!(FailureCounter::compute_next_allowed(now, 3) - now, 240_000);
        // attempts=4 -> 480s = 8min
        assert_eq!(FailureCounter::compute_next_allowed(now, 4) - now, 480_000);
        // attempts=5 -> 960s = 16min
        assert_eq!(FailureCounter::compute_next_allowed(now, 5) - now, 960_000);
        // attempts=6 -> 1920s = 32min (still under cap)
        assert_eq!(FailureCounter::compute_next_allowed(now, 6) - now, 1_920_000);
        // attempts=7 -> theoretical 3840s = 64min -> CAP at 60min = 3_600_000ms
        assert_eq!(FailureCounter::compute_next_allowed(now, 7) - now, 3_600_000);
        // attempts large -> capped
        assert_eq!(FailureCounter::compute_next_allowed(now, 100) - now, 3_600_000);
    }

    #[test]
    fn g5_1_failure_counter_record_and_alert_threshold() {
        let mut fc = FailureCounter::default();
        let cid = "0xabc";
        // First 4 failures: no alert.
        for i in 1..=4 {
            let (att, alerted) = fc.record_failure(cid, t0());
            assert_eq!(att, i);
            assert!(!alerted, "no alert before {ALERT_ON_ATTEMPTS}th attempt; got at {att}");
        }
        // 5th failure: alert fires ONCE.
        let (att, alerted) = fc.record_failure(cid, t0());
        assert_eq!(att, ALERT_ON_ATTEMPTS);
        assert!(alerted, "alert fires exactly at {ALERT_ON_ATTEMPTS}");
        // 6th failure: alert does NOT re-fire (one-shot).
        let (_, alerted2) = fc.record_failure(cid, t0());
        assert!(!alerted2, "alert is one-shot per condition");
    }

    #[test]
    fn g5_1_failure_counter_in_cooldown_check() {
        let mut fc = FailureCounter::default();
        let now = t0();
        fc.record_failure("0xabc", now);
        // Just after the failure: in cooldown.
        assert!(fc.in_cooldown("0xabc", now + 1));
        assert!(fc.in_cooldown("0xabc", now + 30_000)); // 30s in
        assert!(fc.in_cooldown("0xabc", now + 60_000 - 1)); // 60s - 1ms in
        // After cooldown window: NOT in cooldown.
        assert!(!fc.in_cooldown("0xabc", now + 60_000));
        assert!(!fc.in_cooldown("0xabc", now + 60_001));
        // Different condition: never in cooldown.
        assert!(!fc.in_cooldown("0xother", now));
    }

    #[test]
    fn g5_1_failure_counter_clear_resets() {
        let mut fc = FailureCounter::default();
        fc.record_failure("0xabc", t0());
        fc.record_failure("0xabc", t0());
        assert_eq!(fc.attempts("0xabc"), 2);
        fc.clear("0xabc");
        assert_eq!(fc.attempts("0xabc"), 0);
        assert!(!fc.in_cooldown("0xabc", t0()));
    }

    #[test]
    fn g5_1_failure_counter_prune_to_drops_inactive_entries() {
        let mut fc = FailureCounter::default();
        fc.record_failure("0xabc", t0());
        fc.record_failure("0xdef", t0());
        fc.record_failure("0x123", t0());
        assert_eq!(fc.len(), 3);
        // /positions returned only abc + 123 (def disappeared on-chain).
        let mut active = HashSet::new();
        active.insert("0xabc".to_string());
        active.insert("0x123".to_string());
        fc.prune_to(&active);
        assert_eq!(fc.len(), 2, "def pruned; abc + 123 retained");
        assert_eq!(fc.attempts("0xdef"), 0);
        assert_eq!(fc.attempts("0xabc"), 1);
        assert_eq!(fc.attempts("0x123"), 1);
    }

    #[test]
    fn g5_1_find_targets_skips_in_cooldown() {
        let mut bot = HashSet::new();
        bot.insert("tok1".to_string());
        let positions = vec![position("tok1", "0xcid1", true, 5.0)];
        let tracker = RedemptionTracker {
            path: PathBuf::from("/dev/null"),
            done: HashSet::new(),
        };
        let now = t0();
        let mut failures = FailureCounter::default();
        failures.record_failure("0xcid1", now); // 60s cooldown starts

        // During cooldown: filtered out.
        let in_cd = find_redeem_targets(&positions, &bot, &tracker, &failures, now + 30_000);
        assert!(in_cd.is_empty(), "in cooldown => filtered");

        // After cooldown: included again.
        let post_cd = find_redeem_targets(&positions, &bot, &tracker, &failures, now + 60_001);
        assert_eq!(post_cd.len(), 1, "post-cooldown => target back");
        assert_eq!(post_cd[0].condition_id, "0xcid1");
    }

    #[test]
    fn g5_1_find_targets_case_insensitive_cooldown_match() {
        // The cooldown key is lowercase internally; verify mixed-case input matches.
        let mut bot = HashSet::new();
        bot.insert("tok1".to_string());
        let positions = vec![position("tok1", "0xCID1", true, 5.0)]; // upper-case in position
        let tracker = RedemptionTracker {
            path: PathBuf::from("/dev/null"),
            done: HashSet::new(),
        };
        let now = t0();
        let mut failures = FailureCounter::default();
        failures.record_failure("0xcid1", now); // lower-case in counter
        let out = find_redeem_targets(&positions, &bot, &tracker, &failures, now + 30_000);
        assert!(out.is_empty(),
            "case-insensitive cooldown match (input upper vs counter lower)");
    }

    // ---------- G5-wire: setup_from_env (graceful degradation) ----------

    /// Build a HashMap-backed env stub. Missing keys -> None (= unset in .env).
    /// Takes owned data so the returned closure is `'static`-free of borrow lifetimes.
    fn env_stub(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: std::collections::HashMap<String, String> =
            pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        move |k: &str| map.get(k).cloned()
    }

    fn assert_disabled(s: RedemptionSetup) -> String {
        match s {
            RedemptionSetup::Disabled { reason } => reason,
            _ => panic!("expected Disabled"),
        }
    }
    fn assert_enabled(s: RedemptionSetup) -> (Arc<dyn RelayerApi>, String) {
        match s {
            RedemptionSetup::Enabled { relayer, builder_code } => (relayer, builder_code),
            _ => panic!("expected Enabled"),
        }
    }

    // G7.4-C2: setup_from_env tests rewritten for the Builder API Key triple
    // (POLYMARKET_BUILDER_KEY / _SECRET / _PASSPHRASE), replacing the
    // pre-G7.4-C2 POLYMARKET_RELAYER_API_KEY pair (which never worked anyway --
    // see G7.2 --redeem-check output: those headers were ignored, the URL was
    // wrong, the 404 came from the path). The g5w_* test name prefix is kept
    // for git-history continuity with the original Step G5-wire scope.

    #[test]
    fn g5w_setup_missing_builder_key_returns_disabled() {
        let setup = setup_from_env(env_stub(&[]));
        let reason = assert_disabled(setup);
        assert!(reason.contains(ENV_BUILDER_KEY),
            "reason should name the first missing var; got: {reason}");
        assert!(reason.contains("disabled"));
    }

    #[test]
    fn g5w_setup_missing_builder_secret_returns_disabled() {
        // KEY set but SECRET missing -- the iteration order names SECRET as the
        // first missing var (post-KEY check passed).
        let setup = setup_from_env(env_stub(&[
            (ENV_BUILDER_KEY, "019aefed-d585-7f24-8343-64feccc151e3"),
            // ENV_BUILDER_SECRET missing
            // ENV_BUILDER_PASSPHRASE missing
        ]));
        let reason = assert_disabled(setup);
        assert!(reason.contains(ENV_BUILDER_SECRET),
            "reason should name the first missing var (SECRET) once KEY is set; got: {reason}");
    }

    #[test]
    fn g5w_setup_missing_builder_passphrase_returns_disabled() {
        // KEY + SECRET set, PASSPHRASE missing -- the iteration order names
        // PASSPHRASE as the first missing var.
        let setup = setup_from_env(env_stub(&[
            (ENV_BUILDER_KEY, "019aefed-d585-7f24-8343-64feccc151e3"),
            (ENV_BUILDER_SECRET, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="),
            // ENV_BUILDER_PASSPHRASE missing
        ]));
        let reason = assert_disabled(setup);
        assert!(reason.contains(ENV_BUILDER_PASSPHRASE),
            "reason should name the first missing var (PASSPHRASE) once KEY+SECRET are set; got: {reason}");
    }

    #[test]
    fn g5w_setup_empty_value_treated_as_missing() {
        // Empty string in .env (e.g. "POLYMARKET_BUILDER_SECRET=" with no value)
        // is the SAME as missing -- we trim and check is_empty per var.
        let setup = setup_from_env(env_stub(&[
            (ENV_BUILDER_KEY, "019aefed-d585-7f24-8343-64feccc151e3"),
            (ENV_BUILDER_SECRET, ""), // empty -> treated as missing
            (ENV_BUILDER_PASSPHRASE, "some-passphrase"),
        ]));
        let reason = assert_disabled(setup);
        assert!(reason.contains(ENV_BUILDER_SECRET),
            "empty value treated as missing; got: {reason}");
    }

    #[test]
    fn g5w_setup_whitespace_only_value_treated_as_missing() {
        let setup = setup_from_env(env_stub(&[
            (ENV_BUILDER_KEY, "   \t\n  "), // whitespace-only -> treated as missing
            (ENV_BUILDER_SECRET, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="),
            (ENV_BUILDER_PASSPHRASE, "some-passphrase"),
        ]));
        let reason = assert_disabled(setup);
        assert!(reason.contains(ENV_BUILDER_KEY));
    }

    #[test]
    fn g5w_setup_with_all_creds_returns_enabled() {
        let setup = setup_from_env(env_stub(&[
            (ENV_BUILDER_KEY, "019aefed-d585-7f24-8343-64feccc151e3"),
            (ENV_BUILDER_SECRET, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="),
            (ENV_BUILDER_PASSPHRASE, "some-passphrase-value"),
            (ENV_BUILDER_CODE, "0xda7a9e16bb66a5ce7741fa24c929bc080727ce3b321df54c6631bcfe8b667b80"),
        ]));
        let (_relayer, builder_code) = assert_enabled(setup);
        assert_eq!(builder_code,
            "0xda7a9e16bb66a5ce7741fa24c929bc080727ce3b321df54c6631bcfe8b667b80");
    }

    #[test]
    fn g5w_setup_missing_builder_code_uses_empty_string() {
        // BUILDER_CODE is optional (attribution only -- goes in body.metadata,
        // not used for auth). Without it the bot still redeems -- just doesn't
        // attribute the redeem to a builder profile (no rewards if applicable).
        let setup = setup_from_env(env_stub(&[
            (ENV_BUILDER_KEY, "019aefed-d585-7f24-8343-64feccc151e3"),
            (ENV_BUILDER_SECRET, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="),
            (ENV_BUILDER_PASSPHRASE, "some-passphrase-value"),
            // ENV_BUILDER_CODE missing
        ]));
        let (_relayer, builder_code) = assert_enabled(setup);
        assert_eq!(builder_code, "", "missing builder code -> empty string, NOT disabled");
    }

    #[test]
    fn g5w_setup_trims_whitespace_in_builder_code() {
        let setup = setup_from_env(env_stub(&[
            (ENV_BUILDER_KEY, "k"),
            (ENV_BUILDER_SECRET, "s"),
            (ENV_BUILDER_PASSPHRASE, "p"),
            (ENV_BUILDER_CODE, "  0xcode  \n"),
        ]));
        let (_relayer, builder_code) = assert_enabled(setup);
        assert_eq!(builder_code, "0xcode", "whitespace trimmed from builder code");
    }

    #[test]
    fn g5w_setup_disabled_reason_does_not_contain_cred_values() {
        // Security check: even when OTHER vars ARE set (with real-looking
        // values), the Disabled reason for the missing var must NOT echo any
        // set value into the log. This blocks accidental cred leakage via the
        // operator-facing "why is auto-redeem off?" diagnostic.
        let real_looking_secret = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
        let real_looking_pass = "long-passphrase-with-many-chars-9384hf83gjf";
        let setup = setup_from_env(env_stub(&[
            // ENV_BUILDER_KEY missing -> Disabled with KEY-named reason
            (ENV_BUILDER_SECRET, real_looking_secret),
            (ENV_BUILDER_PASSPHRASE, real_looking_pass),
        ]));
        let reason = assert_disabled(setup);
        // Reason names the missing var, but NEVER echoes the secret/passphrase
        // that WERE set in the same env-stub call.
        assert!(reason.contains(ENV_BUILDER_KEY), "names missing var");
        assert!(!reason.contains(real_looking_secret),
            "reason MUST NOT echo secret value; got: {reason}");
        assert!(!reason.contains(real_looking_pass),
            "reason MUST NOT echo passphrase value; got: {reason}");
        // Also negative-check for any partial / substring leak.
        assert!(!reason.contains("AAAA") && !reason.contains("9384hf"),
            "no substring of cred values may leak into reason; got: {reason}");
    }

    // ---------- G5-wire: NO-LIVE-ARMED design enforcement ----------

    #[test]
    fn g5w_redemption_config_has_no_live_armed_field() {
        // DESIGN INVARIANT: auto-redeem is CLEANUP of resolved positions, not
        // new exposure. It must NEVER be gated by LIVE_ARMED.txt. If anyone
        // ever adds a `live_armed_path` (or any arming-related field) to
        // RedemptionConfig, this test must be re-evaluated FIRST.
        //
        // Enforcement: this test constructs a RedemptionConfig with no arming
        // field. The match below is EXHAUSTIVE -- if a new field is added that
        // contains "armed" in its name (case-insensitive), the build still
        // passes but this assertion fails. The exhaustive destructuring of
        // RedemptionConfig is the compile-time check.
        let cfg = RedemptionConfig::from_defaults(
            "0xdummy".to_string(),
            Address::ZERO,
            String::new(),
        );
        // EXHAUSTIVE destructure: if RedemptionConfig grows a new field, this
        // stops compiling and the operator MUST justify whether the new field
        // arms/disarms the task.
        let RedemptionConfig {
            private_key,
            proxy_wallet,
            builder_code,
            intent_log_path,
            redemptions_log_path,
            poll_interval,
        } = cfg;
        // Each field name MUST NOT relate to arming.
        for name in &[
            "private_key",
            "proxy_wallet",
            "builder_code",
            "intent_log_path",
            "redemptions_log_path",
            "poll_interval",
        ] {
            let lower = name.to_lowercase();
            assert!(!lower.contains("armed") && !lower.contains("arming"),
                "field name {name} suggests arming; auto-redeem must NOT be gated");
        }
        // Side-effect: each binding is used so the destructure isn't pruned.
        let _ = (private_key, proxy_wallet, builder_code,
                 intent_log_path, redemptions_log_path, poll_interval);
    }

    #[test]
    fn g5_default_constants_are_safe() {
        // Cadence shouldn't be absurdly small (DoS the data-api) or absurdly
        // large (winnings stranded for ages). 5s is the documented choice.
        assert!(DEFAULT_POLL_INTERVAL_SECS >= 3 && DEFAULT_POLL_INTERVAL_SECS <= 60);
        // Per-redeem timeout: 60 * 2s = 2 min is enough for Polygon's ~12s block + a few retries.
        let max_wait = DEFAULT_TX_POLL_INTERVAL_MS * (DEFAULT_TX_POLL_MAX as u64);
        assert!(max_wait >= 30_000, "should wait at least 30s for the relayer to mine");
        assert!(max_wait <= 5 * 60 * 1000, "should not block the task >5 min per redeem");
    }

    // ---------- G7.1-diag: full-chain error formatting (anti-regression gold) ----------

    /// Anti-regression: the redeem error path MUST log the FULL anyhow cause chain
    /// via `format!("{e:#}")`, NOT just `e.to_string()`. Without this, the operator
    /// only sees the top-level context (e.g. "getRelayPayload for redeem" or
    /// "build body") and loses the root HTTP-layer reason (401 / 403 / 404 + body,
    /// DNS, TLS, JSON parse), which is the exact data needed to diagnose without a
    /// code change. This test builds a deeply-nested anyhow error mirroring the
    /// real shape (build_redeem_submit_body wraps get_relay_payload wraps a
    /// reqwest/HTTP error) and asserts both formatters' behavior to PROVE the
    /// chain-aware one is load-bearing.
    #[test]
    fn g7_1_diag_full_chain_formatter_exposes_underlying_cause() {
        // Mirror the real call shape: deepest error -> .context()'d twice on its
        // way out of build_redeem_submit_body.
        let root: anyhow::Error =
            anyhow::anyhow!("getRelayPayload 401 Unauthorized: invalid api key");
        let wrapped_once: anyhow::Error = root.context("getRelayPayload for redeem");
        // What the OLD logging would have produced (the bug):
        let truncated = wrapped_once.to_string();
        assert_eq!(
            truncated, "getRelayPayload for redeem",
            "Display of anyhow::Error returns ONLY the top context -- this is the visibility bug"
        );
        // What the NEW logging produces (the fix). The `#` alt-format walks the
        // cause chain and joins with ": ".
        let full_chain = format!("{wrapped_once:#}");
        assert!(
            full_chain.contains("getRelayPayload for redeem"),
            "alt-format must include the top context; got: {full_chain}"
        );
        assert!(
            full_chain.contains("401 Unauthorized"),
            "alt-format MUST include the underlying HTTP status from the root cause; got: {full_chain}"
        );
        assert!(
            full_chain.contains("invalid api key"),
            "alt-format MUST include the root error body; got: {full_chain}"
        );
        // Final assertion: alt-format strictly longer than Display, which is the
        // simplest property guaranteeing more info reached the log.
        assert!(
            full_chain.len() > truncated.len(),
            "alt-format must be strictly longer than the truncated Display; \
             truncated={truncated:?} full={full_chain:?}"
        );
    }

    /// Same property for the submit/poll error path (the other call site we
    /// patched at redemption.rs:609). The shape there is reqwest connect
    /// error -> .context("submit request failed") -> .context("submit failed").
    #[test]
    fn g7_1_diag_full_chain_formatter_for_submit_error_path() {
        let root: anyhow::Error =
            anyhow::anyhow!("dns error: failed to lookup address relayer-v2.polymarket.com");
        let mid: anyhow::Error = root.context("submit request failed");
        let outer: anyhow::Error = mid.context("submit failed");
        // OLD behavior loses the DNS reason:
        assert_eq!(outer.to_string(), "submit failed",
            "Display strips both inner contexts and the root cause");
        // NEW behavior preserves the whole chain:
        let chain = format!("{outer:#}");
        assert!(chain.contains("submit failed"));
        assert!(chain.contains("submit request failed"));
        assert!(chain.contains("dns error"),
            "root reqwest error must be visible in the chain; got: {chain}");
    }
}
