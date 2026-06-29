//! Phase 3: crash-safe persistence + recovery + on-chain reconciliation for
//! [`BotState`].
//!
//! Snapshot is atomic: serialize → write `state.json.tmp` → fsync → copy the
//! current `state.json` to `state.json.bak` (1-deep backup) → rename tmp over
//! `state.json` (atomic replace on Linux AND Windows via `std::fs::rename`) →
//! fsync the parent dir (durable on Linux; best-effort no-op on Windows).
//!
//! Load tries `state.json`, falls back to `.bak`, and classifies the recovery so
//! the launch gate can REFUSE `--live` when capital state is unknown.

#![allow(dead_code)] // reconcile / divergence kinds are consumed by Phase 6 (live)

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rust_decimal::Decimal;
use tokio::sync::watch;
use tracing::{info, warn};

use crate::state::persist::{BotState, OpenPosition, SCHEMA_VERSION};

/// Shared, mutex-guarded persistent state.
pub type SharedBotState = Arc<Mutex<BotState>>;

/// How the loaded state was obtained.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoverySource {
    /// Loaded cleanly from `state.json`.
    Primary,
    /// `state.json` was absent/corrupt; recovered from `state.json.bak`.
    Backup,
    /// No state file existed at all — a clean first run (empty state is correct).
    FreshNoFile,
    /// A state file EXISTED but both it and the backup failed to parse — state was
    /// LOST. The dangerous case: `--live` must be refused (open positions on-chain
    /// may be unknown to the bot).
    EmptyAfterFailure,
}

/// Result of [`StateStore::load`].
#[derive(Debug, Clone)]
pub struct LoadOutcome {
    pub source: RecoverySource,
    pub errors: Vec<String>,
}

impl LoadOutcome {
    /// True when persisted state existed but could not be recovered (capital
    /// state unknown). `--live` must be refused in this case.
    pub fn lost_state(&self) -> bool {
        self.source == RecoverySource::EmptyAfterFailure
    }
}

/// Launch-gate verdict combining load outcome + mode + first-live confirmation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LaunchDecision {
    /// OK to launch.
    Allow,
    /// `--live` refused: persisted state was lost (corrupt). Investigate/restore.
    RefuseLostState,
    /// `--live` refused: first-ever live launch needs explicit confirmation.
    RefuseFirstLiveUnconfirmed,
}

/// Decide whether the bot may launch. Paper always allowed. Live requires: state
/// was NOT lost (gate #3) AND either a prior live launch exists or the operator
/// confirmed this first one (gate #4).
pub fn evaluate_launch(
    outcome: &LoadOutcome,
    state: &BotState,
    mode: &str,
    confirm_live: bool,
) -> LaunchDecision {
    if mode != "live" {
        return LaunchDecision::Allow;
    }
    if outcome.lost_state() {
        return LaunchDecision::RefuseLostState;
    }
    if !state.has_launched_live() && !confirm_live {
        return LaunchDecision::RefuseFirstLiveUnconfirmed;
    }
    LaunchDecision::Allow
}

/// Crash-safe store around a [`SharedBotState`].
pub struct StateStore {
    state: SharedBotState,
    dir: PathBuf,
    path: PathBuf,
    bak: PathBuf,
    tmp: PathBuf,
}

impl StateStore {
    /// Load from `state_path` (e.g. `data/state.json`), falling back to
    /// `<state_path>.bak`. Never panics; classifies the recovery.
    pub fn load(state_path: &Path) -> (Self, LoadOutcome) {
        let path = state_path.to_path_buf();
        let bak = with_suffix(state_path, ".bak");
        let tmp = with_suffix(state_path, ".tmp");
        let dir = path.parent().map_or_else(|| PathBuf::from("."), Path::to_path_buf);

        let mut errors = Vec::new();
        let (state, source) = match read_state(&path) {
            Ok(Some(s)) => (s, RecoverySource::Primary),
            Ok(None) => match read_state(&bak) {
                Ok(Some(s)) => {
                    errors.push("state.json absent; recovered from .bak".to_string());
                    (s, RecoverySource::Backup)
                }
                Ok(None) => (BotState::new(), RecoverySource::FreshNoFile),
                Err(e) => {
                    errors.push(format!("state.json absent and .bak corrupt: {e}"));
                    (BotState::new(), RecoverySource::EmptyAfterFailure)
                }
            },
            Err(e) => {
                errors.push(format!("state.json corrupt: {e}"));
                match read_state(&bak) {
                    Ok(Some(s)) => {
                        errors.push("recovered from .bak".to_string());
                        (s, RecoverySource::Backup)
                    }
                    Ok(None) => {
                        errors.push(".bak absent".to_string());
                        (BotState::new(), RecoverySource::EmptyAfterFailure)
                    }
                    Err(e2) => {
                        errors.push(format!(".bak also corrupt: {e2}"));
                        (BotState::new(), RecoverySource::EmptyAfterFailure)
                    }
                }
            }
        };

        let store = Self {
            state: Arc::new(Mutex::new(state)),
            dir,
            path,
            bak,
            tmp,
        };
        (store, LoadOutcome { source, errors })
    }

    /// Handle to the shared state for the engines to read/mutate.
    pub fn state(&self) -> SharedBotState {
        Arc::clone(&self.state)
    }

    /// Atomically persist the current state (the `for_persist` view).
    pub fn snapshot(&self) -> std::io::Result<()> {
        let json = {
            let guard = self.state.lock().expect("state mutex poisoned");
            serde_json::to_vec_pretty(&guard.for_persist())?
        };
        atomic_write(&self.dir, &self.path, &self.bak, &self.tmp, &json)
    }
}

/// Read + parse a state file. `Ok(None)` = file absent; `Err` = present but
/// unparseable (corrupt).
///
/// Migration: an on-disk `schema_version` lower than [`SCHEMA_VERSION`] is
/// bumped IN MEMORY. The new fields on `OpenPosition` (v3, Pieza 1) are
/// populated by serde defaults (sentinel zeros) thanks to
/// `#[serde(default)]` on those fields, so a v2 file deserializes cleanly
/// and the only thing left is to mark the in-memory state as v3 -- the
/// next `snapshot()` will then write v3 to disk. This is intentionally
/// idempotent: re-reading a v2 file always reproduces the same v3 view.
fn read_state(path: &Path) -> Result<Option<BotState>, String> {
    match fs::read(path) {
        Ok(bytes) => {
            let mut state: BotState =
                serde_json::from_slice::<BotState>(&bytes).map_err(|e| e.to_string())?;
            if state.schema_version < SCHEMA_VERSION {
                info!(
                    path = %path.display(),
                    from_schema = state.schema_version,
                    to_schema = SCHEMA_VERSION,
                    positions_migrated = state.positions.len(),
                    "state.json schema migrated in memory; next snapshot will persist v{}",
                    SCHEMA_VERSION
                );
                state.schema_version = SCHEMA_VERSION;
            } else if state.schema_version > SCHEMA_VERSION {
                // Future schema: refuse rather than truncate. The operator
                // probably ran a newer binary then downgraded; recovery is to
                // re-deploy the newer binary, not to lose fields.
                return Err(format!(
                    "state.json schema_version {} > supported {} -- newer binary required, refusing to silently lose fields",
                    state.schema_version, SCHEMA_VERSION
                ));
            }
            Ok(Some(state))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.to_string()),
    }
}

/// `path` with an appended suffix (e.g. `state.json` + `.bak` → `state.json.bak`).
fn with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut s = path.as_os_str().to_os_string();
    s.push(suffix);
    PathBuf::from(s)
}

/// The atomic snapshot write. See the module docs for the crash-safety argument:
/// `state.json` is only ever replaced via an atomic rename of a fully-written,
/// fsync'd temp file, so a crash mid-write can never leave a half-written
/// `state.json` — load gets the old `state.json` (or `.bak`).
fn atomic_write(dir: &Path, path: &Path, bak: &Path, tmp: &Path, bytes: &[u8]) -> std::io::Result<()> {
    fs::create_dir_all(dir)?;
    // 1. Write + fsync the temp file.
    {
        let mut f = File::create(tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    // 2. Back up the current good state.json (1-deep) before replacing it.
    if path.exists() {
        fs::copy(path, bak)?;
        if let Ok(f) = File::open(bak) {
            let _ = f.sync_all(); // best-effort; .bak is the fallback, not the truth
        }
    }
    // 3. Atomic replace. std::fs::rename replaces an existing dest on both Linux
    //    (rename(2)) and Windows (MoveFileExW + MOVEFILE_REPLACE_EXISTING).
    fs::rename(tmp, path)?;
    // 4. fsync the parent dir so the rename entry is durable. Linux: real fsync.
    //    Windows: opening a dir as a File fails → best-effort no-op (the rename's
    //    durability there relies on NTFS metadata journaling).
    if let Ok(d) = File::open(dir) {
        let _ = d.sync_all();
    }
    Ok(())
}

/// Periodic + final-on-shutdown snapshot task. Engines also call
/// [`StateStore::snapshot`] event-driven after each open/close.
pub async fn run_persist(store: Arc<StateStore>, period: Duration, mut shutdown: watch::Receiver<bool>) {
    info!(period_secs = period.as_secs(), "task started: state_persist");
    let mut iv = tokio::time::interval(period);
    iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = iv.tick() => {
                if let Err(e) = store.snapshot() {
                    warn!(error = %e, "state snapshot failed");
                }
            }
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    if let Err(e) = store.snapshot() {
                        warn!(error = %e, "final state snapshot failed");
                    } else {
                        info!("state_persist: shutdown (final snapshot written)");
                    }
                    return;
                }
            }
        }
    }
}

/// A divergence between cached state and on-chain reality.
#[derive(Debug, Clone, PartialEq)]
pub struct Divergence {
    pub token_id: String,
    pub cached_shares: Decimal,
    pub onchain_shares: Decimal,
    pub kind: DivergenceKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DivergenceKind {
    /// Cached and on-chain share counts differ beyond the threshold.
    SizeMismatch,
    /// Cached position not present on-chain.
    MissingOnChain,
    /// On-chain position the bot has no record of.
    UnexpectedOnChain,
}

/// Reconcile cached position LOTS against on-chain `(token_id, shares)` (from the
/// REST `get_positions`). On-chain Polymarket holds ONE combined position per
/// token (DCA aggregated), so the bot's lots are summed PER TOKEN and the
/// aggregate is compared. `threshold` MUST be > 0 to tolerate the documented
/// data-api lag (Phase 2). Pure (no I/O); the f64→Decimal of REST sizes is at the
/// call site.
pub fn reconcile(
    cached: &[OpenPosition],
    onchain: &[(String, Decimal)],
    threshold: Decimal,
) -> Vec<Divergence> {
    // Σ lots per token — what the on-chain aggregate position should equal.
    let mut cached_by_token: HashMap<&str, Decimal> = HashMap::new();
    for p in cached {
        *cached_by_token.entry(p.token_id.as_str()).or_insert(Decimal::ZERO) += p.shares;
    }
    let onchain_map: HashMap<&str, Decimal> =
        onchain.iter().map(|(t, s)| (t.as_str(), *s)).collect();
    let mut out = Vec::new();

    for (tok, &cached_shares) in &cached_by_token {
        match onchain_map.get(tok) {
            Some(&oc) => {
                if (cached_shares - oc).abs() > threshold {
                    out.push(Divergence {
                        token_id: (*tok).to_string(),
                        cached_shares,
                        onchain_shares: oc,
                        kind: DivergenceKind::SizeMismatch,
                    });
                }
            }
            None => out.push(Divergence {
                token_id: (*tok).to_string(),
                cached_shares,
                onchain_shares: Decimal::ZERO,
                kind: DivergenceKind::MissingOnChain,
            }),
        }
    }
    for (tok, oc) in onchain {
        if !cached_by_token.contains_key(tok.as_str()) && *oc > threshold {
            out.push(Divergence {
                token_id: tok.clone(),
                cached_shares: Decimal::ZERO,
                onchain_shares: *oc,
                kind: DivergenceKind::UnexpectedOnChain,
            });
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::now_ms;
    use crate::state::persist::{MakerExitState, MakerExitStatus, Outcome, OrderStatus};
    use rust_decimal_macros::dec;
    use std::sync::atomic::{AtomicU32, Ordering};

    fn unique_dir(tag: &str) -> PathBuf {
        static N: AtomicU32 = AtomicU32::new(0);
        let id = N.fetch_add(1, Ordering::SeqCst);
        std::env::temp_dir().join(format!("rb_store_{tag}_{}_{}", now_ms(), id))
    }

    fn pos(token: &str, shares: Decimal) -> OpenPosition {
        OpenPosition {
            token_id: token.to_string(),
            asset: "BTC".to_string(),
            side: Outcome::Up,
            entry_price: dec!(0.53),
            shares,
            opened_at_ms: 1_780_000_000_000,
            signal_id: "sig-1".to_string(),
            interval: "5m".to_string(),
            exit_ts_s: 1_780_000_120,
            // v3 (Pieza 1) sentinel zeros: the existing GATE tests below do
            // not exercise smart-exit (only schema-roundtrip + crash safety),
            // so leaving the v3 fields at the migration default keeps the
            // test surface intentional.
            entry_ts_ms: 0,
            running_max_bid: 0.0,
            ts_max_bid_ms: 0,
            status: OrderStatus::TentativeFilled,
            order_id: Some("0xorder".to_string()),
            ack_at_ms: Some(1_780_000_000_100),
            confirmed_at_ms: None,
            confirmation_source: None,
            maker_exit: None,
        }
    }

    // ---- GATE 1: disk roundtrip is bit-exact (Decimal) ----
    #[test]
    fn gate1_disk_roundtrip_bit_exact() {
        let dir = unique_dir("g1");
        let sp = dir.join("state.json");
        let store = StateStore::load(&sp).0;
        {
            let st = store.state();
            let mut g = st.lock().unwrap();
            g.positions.push(pos("a", dec!(12.5)));
            g.balance_usdc = dec!(104.513264);
            g.record_launch("paper", "0.1.0", 1_780_000_000_000);
        }
        store.snapshot().unwrap();
        let expected = store.state().lock().unwrap().for_persist();

        let (store2, oc) = StateStore::load(&sp);
        assert_eq!(oc.source, RecoverySource::Primary);
        let loaded = store2.state().lock().unwrap().clone();
        assert_eq!(expected, loaded, "disk roundtrip must be bit-exact");
        let _ = fs::remove_dir_all(&dir);
    }

    // ---- GATE 2: crash mid-write never corrupts state.json ----
    #[test]
    fn gate2_crash_midwrite_recovers() {
        // (a) A leftover garbage .tmp (crash before rename) is ignored; state.json wins.
        let dir = unique_dir("g2a");
        let sp = dir.join("state.json");
        let store = StateStore::load(&sp).0;
        store.state().lock().unwrap().balance_usdc = dec!(50);
        store.snapshot().unwrap(); // state.json = v1
        fs::write(with_suffix(&sp, ".tmp"), b"{ half-written garbage").unwrap();
        let (s2, oc) = StateStore::load(&sp);
        assert_eq!(oc.source, RecoverySource::Primary);
        assert_eq!(s2.state().lock().unwrap().balance_usdc, dec!(50));
        let _ = fs::remove_dir_all(&dir);

        // (b) A corrupt state.json with a valid .bak recovers from .bak.
        let dir = unique_dir("g2b");
        let sp = dir.join("state.json");
        let store = StateStore::load(&sp).0;
        store.state().lock().unwrap().balance_usdc = dec!(10); // v1
        store.snapshot().unwrap();
        store.state().lock().unwrap().balance_usdc = dec!(20); // v2 -> bak=v1, state.json=v2
        store.snapshot().unwrap();
        fs::write(&sp, b"corrupt!!! not json").unwrap();
        let (s3, oc) = StateStore::load(&sp);
        assert_eq!(oc.source, RecoverySource::Backup);
        assert_eq!(s3.state().lock().unwrap().balance_usdc, dec!(10)); // the .bak (v1)
        let _ = fs::remove_dir_all(&dir);
    }

    // ---- GATE 3: both files unrecoverable -> refuse --live, allow --paper ----
    #[test]
    fn gate3_load_failure_refuses_live() {
        let dir = unique_dir("g3");
        fs::create_dir_all(&dir).unwrap();
        let sp = dir.join("state.json");
        fs::write(&sp, b"xxx not json").unwrap();
        fs::write(with_suffix(&sp, ".bak"), b"yyy not json either").unwrap();
        let (store, oc) = StateStore::load(&sp);
        assert_eq!(oc.source, RecoverySource::EmptyAfterFailure);
        assert!(oc.lost_state());
        let shared = store.state();
        let st = shared.lock().unwrap();
        // even WITH confirmation, lost state refuses live (hard):
        assert_eq!(
            evaluate_launch(&oc, &st, "live", true),
            LaunchDecision::RefuseLostState
        );
        // but paper with empty state is allowed:
        assert_eq!(evaluate_launch(&oc, &st, "paper", false), LaunchDecision::Allow);
        drop(st);
        let _ = fs::remove_dir_all(&dir);
    }

    // ---- GATE 4: first-ever live launch requires confirmation ----
    #[test]
    fn gate4_first_live_requires_confirmation() {
        let dir = unique_dir("g4");
        let sp = dir.join("state.json");
        let (store, oc) = StateStore::load(&sp); // fresh
        assert_eq!(oc.source, RecoverySource::FreshNoFile);
        let shared = store.state();
        let mut st = shared.lock().unwrap();
        // first live, unconfirmed -> refuse
        assert_eq!(
            evaluate_launch(&oc, &st, "live", false),
            LaunchDecision::RefuseFirstLiveUnconfirmed
        );
        // first live, confirmed -> allow
        assert_eq!(evaluate_launch(&oc, &st, "live", true), LaunchDecision::Allow);
        // after a live launch is recorded, no further confirmation needed
        st.record_launch("live", "0.1.0", now_ms());
        assert_eq!(evaluate_launch(&oc, &st, "live", false), LaunchDecision::Allow);
        drop(st);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn reconcile_flags_only_real_divergences() {
        // token "a" has TWO lots (DCA) summing to 10 — must aggregate vs on-chain.
        let cached = vec![
            pos("a", dec!(6)),
            pos("a", dec!(4)),
            pos("b", dec!(5)),
        ];
        // a: Σ=10 within threshold of 10.005; b: missing on-chain; c: unexpected.
        let onchain = vec![("a".to_string(), dec!(10.005)), ("c".to_string(), dec!(3))];
        let div = reconcile(&cached, &onchain, dec!(0.01));
        assert_eq!(div.len(), 2);
        assert!(div.iter().any(|d| d.token_id == "b" && d.kind == DivergenceKind::MissingOnChain));
        assert!(div.iter().any(|d| d.token_id == "c" && d.kind == DivergenceKind::UnexpectedOnChain));
        assert!(!div.iter().any(|d| d.token_id == "a"), "lots aggregate to within lag tolerance");
    }

    #[test]
    fn reconcile_flags_dca_lots_sum_mismatch() {
        // Two lots summing to 12, but on-chain shows 10 → SizeMismatch on the sum.
        let cached = vec![pos("a", dec!(7)), pos("a", dec!(5))];
        let onchain = vec![("a".to_string(), dec!(10.0))];
        let div = reconcile(&cached, &onchain, dec!(0.01));
        assert_eq!(div.len(), 1);
        assert_eq!(div[0].kind, DivergenceKind::SizeMismatch);
        assert_eq!(div[0].cached_shares, dec!(12));
    }

    // ========================================================================
    // Pieza 1: v2 -> v3 schema migration. The v3 fields (entry_ts_ms,
    // running_max_bid, ts_max_bid_ms) are absent from v2 files; serde
    // defaults fill sentinels and the in-memory schema_version is bumped.
    // The bot's behavior must be byte-identical to pre-migration time-exit.
    // ========================================================================

    /// REALISTIC v2 state with an open position in flight. This mirrors the
    /// situation we'll hit on the next bot start after Pieza 1 deploys: there
    /// is already a live (or paper) position on disk; the migration must NOT
    /// drop it, corrupt it, or set values that could later trigger a
    /// premature smart exit. The sentinels (0/0.0/0) are the "smart NOT
    /// eligible" marker that the rule-evaluation site (next piece) will
    /// check before any decision.
    #[test]
    fn state_v2_migrates_to_v3_with_open_position() {
        let dir = unique_dir("v2_migrate");
        let sp = dir.join("state.json");
        fs::create_dir_all(&dir).unwrap();

        // Hand-written v2 JSON. One open position in flight, $50 balance,
        // one prior launch record. Schema 2, no v3 fields on the position.
        let v2_json = r#"{
            "schema_version": 2,
            "positions": [
                {
                    "token_id": "57223448503580739992718262906156795047",
                    "side": "up",
                    "entry_price": "0.53",
                    "shares": "9.43",
                    "opened_at_ms": 1780000000000,
                    "signal_id": "btc-15m-1780000000",
                    "interval": "15m",
                    "exit_ts_s": 1780000300,
                    "status": "confirmed",
                    "order_id": "0xabc",
                    "ack_at_ms": 1780000000100,
                    "confirmed_at_ms": 1780000001500,
                    "confirmation_source": "user_ws"
                }
            ],
            "recent_signals": [],
            "balance_usdc": "50.0",
            "mode_history": [
                { "mode": "paper", "version": "0.1.0", "launched_at_ms": 1779999000000 }
            ]
        }"#;
        fs::write(&sp, v2_json).unwrap();

        let (store, oc) = StateStore::load(&sp);
        assert_eq!(oc.source, RecoverySource::Primary, "must load from primary");
        // No errors logged for a clean v2 file -- migration is silent-success.
        assert!(oc.errors.is_empty(), "no errors expected, got {:?}", oc.errors);

        let st = store.state();
        let guard = st.lock().unwrap();

        // (a) Schema bumped in memory.
        assert_eq!(
            guard.schema_version, SCHEMA_VERSION,
            "in-memory schema must be v{SCHEMA_VERSION} after migration"
        );

        // (b) Position survived intact with sentinels for the 3 new fields.
        assert_eq!(guard.positions.len(), 1, "must not drop the in-flight position");
        let p = &guard.positions[0];
        assert_eq!(p.token_id, "57223448503580739992718262906156795047");
        assert_eq!(p.entry_price, dec!(0.53));
        assert_eq!(p.shares, dec!(9.43));
        assert_eq!(p.exit_ts_s, 1_780_000_300, "exit clock must survive");
        // The 3 new fields -- sentinel zeros = smart NOT eligible.
        assert_eq!(
            p.entry_ts_ms, 0,
            "v2 position must migrate to entry_ts_ms=0 (sentinel: pre-migration)"
        );
        assert_eq!(
            p.running_max_bid, 0.0,
            "v2 position must migrate to running_max_bid=0.0 (sentinel)"
        );
        assert_eq!(
            p.ts_max_bid_ms, 0,
            "v2 position must migrate to ts_max_bid_ms=0 (sentinel)"
        );

        // (c) Other state survived.
        assert_eq!(guard.balance_usdc, dec!(50.0));
        assert_eq!(guard.mode_history.len(), 1);

        drop(guard);
        let _ = fs::remove_dir_all(&dir);
    }

    /// REALISTIC v3 state with an open position in flight. Pieza 4 bumps
    /// SCHEMA_VERSION to 4 and adds `asset` to OpenPosition. The migration
    /// must preserve every prior field and set `asset = ""` (sentinel: the
    /// smart-exit evaluator will treat the position as smart-ineligible and
    /// fall back to Baseline -- the same fail-closed pattern that Pieza 1's
    /// entry_ts_ms sentinel uses).
    #[test]
    fn state_v3_migrates_to_v4_with_open_position() {
        let dir = unique_dir("v3_to_v4");
        let sp = dir.join("state.json");
        fs::create_dir_all(&dir).unwrap();

        // Hand-written v3 JSON: schema 3, one open position WITH the v3
        // smart-exit state fields populated (entry_ts_ms non-zero,
        // running_max_bid > 0), but NO asset field (since v3).
        let v3_json = r#"{
            "schema_version": 3,
            "positions": [
                {
                    "token_id": "57223448503580739992718262906156795047",
                    "side": "up",
                    "entry_price": "0.55",
                    "shares": "8.18",
                    "opened_at_ms": 1780000000000,
                    "signal_id": "BTC-1780000000-15m-Up",
                    "interval": "15m",
                    "exit_ts_s": 1780000300,
                    "entry_ts_ms": 1780000000500,
                    "running_max_bid": 0.58,
                    "ts_max_bid_ms": 1780000045000,
                    "status": "confirmed",
                    "order_id": "0xabc",
                    "ack_at_ms": 1780000000100,
                    "confirmed_at_ms": 1780000001500,
                    "confirmation_source": "user_ws"
                }
            ],
            "recent_signals": [],
            "balance_usdc": "50.0",
            "mode_history": [
                { "mode": "paper", "version": "0.1.0", "launched_at_ms": 1779999000000 }
            ]
        }"#;
        fs::write(&sp, v3_json).unwrap();

        let (store, oc) = StateStore::load(&sp);
        assert_eq!(oc.source, RecoverySource::Primary);
        assert!(oc.errors.is_empty(), "no errors expected, got {:?}", oc.errors);

        let st = store.state();
        let guard = st.lock().unwrap();
        assert_eq!(guard.schema_version, SCHEMA_VERSION, "must bump to v{SCHEMA_VERSION}");
        assert_eq!(guard.positions.len(), 1, "must not drop the in-flight position");
        let p = &guard.positions[0];
        // v4 sentinel: asset defaulted to empty string -> smart-ineligible.
        assert_eq!(
            p.asset, "",
            "v3 position must migrate to asset=\"\" (sentinel: pre-migration, \
             smart-exit evaluator falls back to Baseline)"
        );
        // v3 fields preserved (running_max_bid + ts_max_bid_ms were set,
        // so this position WOULD have been smart-eligible by the entry_ts_ms
        // guard; but the new asset="" sentinel makes it ineligible -- defense
        // in depth, no smart-exit on positions opened before this schema).
        assert_eq!(p.entry_ts_ms, 1_780_000_000_500);
        assert_eq!(p.running_max_bid, 0.58);
        assert_eq!(p.ts_max_bid_ms, 1_780_000_045_000);
        assert_eq!(p.exit_ts_s, 1_780_000_300, "exit clock survives");
        assert_eq!(p.entry_price, dec!(0.55));
        assert_eq!(p.shares, dec!(8.18));
        assert_eq!(guard.balance_usdc, dec!(50.0));

        drop(guard);
        let _ = fs::remove_dir_all(&dir);
    }

    /// REALISTIC v4 state with an open position. COMBO Phase 3 Pieza 3 bumps
    /// SCHEMA_VERSION to 5 and adds `maker_exit: Option<MakerExitState>` to
    /// OpenPosition. The migration must preserve every prior field and set
    /// `maker_exit = None` (a v4 position has no maker order outstanding --
    /// exactly the correct state; serde-default does this automatically).
    #[test]
    fn state_v4_migrates_to_v5_with_open_position() {
        let dir = unique_dir("v4_to_v5");
        let sp = dir.join("state.json");
        fs::create_dir_all(&dir).unwrap();

        // Hand-written v4 JSON: schema 4, one open position WITH all v4 fields
        // (incl. asset), but NO maker_exit field (since v4 predates it).
        let v4_json = r#"{
            "schema_version": 4,
            "positions": [
                {
                    "token_id": "57223448503580739992718262906156795047",
                    "asset": "ETH",
                    "side": "up",
                    "entry_price": "0.62",
                    "shares": "8.06",
                    "opened_at_ms": 1780500000000,
                    "signal_id": "ETH-1780500000-5m-Up",
                    "interval": "5m",
                    "exit_ts_s": 1780500120,
                    "entry_ts_ms": 1780500000500,
                    "running_max_bid": 0.66,
                    "ts_max_bid_ms": 1780500030000,
                    "status": "confirmed",
                    "order_id": "0xbuy",
                    "ack_at_ms": 1780500000100,
                    "confirmed_at_ms": 1780500001500,
                    "confirmation_source": "user_ws"
                }
            ],
            "recent_signals": [],
            "balance_usdc": "42.0",
            "mode_history": [
                { "mode": "paper", "version": "0.1.0", "launched_at_ms": 1780499000000 }
            ]
        }"#;
        fs::write(&sp, v4_json).unwrap();

        let (store, oc) = StateStore::load(&sp);
        assert_eq!(oc.source, RecoverySource::Primary);
        assert!(oc.errors.is_empty(), "no errors expected, got {:?}", oc.errors);

        let st = store.state();
        let guard = st.lock().unwrap();
        assert_eq!(guard.schema_version, SCHEMA_VERSION, "must bump to v{SCHEMA_VERSION}");
        assert_eq!(guard.positions.len(), 1, "must not drop the in-flight position");
        let p = &guard.positions[0];
        // v5 default: a v4 position has no maker exit order outstanding.
        assert_eq!(
            p.maker_exit, None,
            "v4 position must migrate to maker_exit=None (no maker order: \
             exactly the correct state for a position that predates the field)"
        );
        // Every prior field survives untouched.
        assert_eq!(p.asset, "ETH", "v4 asset survives");
        assert_eq!(p.entry_ts_ms, 1_780_500_000_500);
        assert_eq!(p.running_max_bid, 0.66);
        assert_eq!(p.ts_max_bid_ms, 1_780_500_030_000);
        assert_eq!(p.exit_ts_s, 1_780_500_120, "exit clock survives");
        assert_eq!(p.entry_price, dec!(0.62));
        assert_eq!(p.shares, dec!(8.06));
        assert_eq!(p.order_id.as_deref(), Some("0xbuy"), "BUY order_id survives (NOT the maker)");
        assert_eq!(guard.balance_usdc, dec!(42.0));

        drop(guard);
        let _ = fs::remove_dir_all(&dir);
    }

    /// v5 round-trip: a position WITH a populated maker_exit must serialize +
    /// deserialize unchanged (all four sub-fields survive). Proves the new
    /// schema field persists, not just defaults to None.
    #[test]
    fn state_v5_maker_exit_roundtrips() {
        let dir = unique_dir("v5_maker_roundtrip");
        let sp = dir.join("state.json");
        let store = StateStore::load(&sp).0;
        {
            let mut p = pos("token-maker", dec!(7));
            p.maker_exit = Some(MakerExitState {
                order_id: "0xmaker-sell".to_string(),
                placed_ms: 1_780_600_000_000,
                target: 0.72,
                status: MakerExitStatus::Placed,
            });
            store.state().lock().unwrap().positions.push(p);
        }
        store.snapshot().expect("snapshot v5");

        let (store2, oc) = StateStore::load(&sp);
        assert_eq!(oc.source, RecoverySource::Primary);
        assert!(oc.errors.is_empty(), "no errors expected, got {:?}", oc.errors);
        let st2 = store2.state();
        let g2 = st2.lock().unwrap();
        assert_eq!(g2.schema_version, SCHEMA_VERSION);
        assert_eq!(g2.positions.len(), 1);
        let me = g2.positions[0].maker_exit.as_ref().expect("maker_exit must survive roundtrip");
        assert_eq!(me.order_id, "0xmaker-sell");
        assert_eq!(me.placed_ms, 1_780_600_000_000);
        assert_eq!(me.target, 0.72);
        assert_eq!(me.status, MakerExitStatus::Placed);
        drop(g2);
        let _ = fs::remove_dir_all(&dir);
    }

    /// A state.json that is already at the latest schema must load without
    /// migration -- the schema_version is preserved and no defaults are
    /// applied over existing field values. Regression guard against the
    /// migration accidentally clobbering valid data.
    #[test]
    fn state_v3_loads_unchanged() {
        let dir = unique_dir("v3_unchanged");
        let sp = dir.join("state.json");
        let store = StateStore::load(&sp).0;
        {
            let mut p = pos("token-x", dec!(5));
            // Non-default v3 state: prove these survive a save+load cycle.
            p.entry_ts_ms = 1_780_000_005_000;
            p.running_max_bid = 0.61;
            p.ts_max_bid_ms = 1_780_000_080_000;
            store.state().lock().unwrap().positions.push(p);
        }
        store.snapshot().expect("snapshot v3");

        let (store2, oc) = StateStore::load(&sp);
        assert_eq!(oc.source, RecoverySource::Primary);
        assert!(oc.errors.is_empty(), "no errors expected, got {:?}", oc.errors);
        let st2 = store2.state();
        let g2 = st2.lock().unwrap();
        assert_eq!(g2.schema_version, SCHEMA_VERSION);
        assert_eq!(g2.positions.len(), 1);
        let p = &g2.positions[0];
        assert_eq!(p.entry_ts_ms, 1_780_000_005_000, "v3 field must survive roundtrip");
        assert_eq!(p.running_max_bid, 0.61);
        assert_eq!(p.ts_max_bid_ms, 1_780_000_080_000);
        drop(g2);
        let _ = fs::remove_dir_all(&dir);
    }
}
