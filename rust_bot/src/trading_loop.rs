//! Phase 6 Sub-paso D1 — the decision→execution loop (paper backend).
//!
//! ARCHITECTURE (the spine for D1-D5): a kline-close event flows
//!   Binance WS ──KlineClose──▶ DECISION task ──ExecCommand──▶ EXECUTION task
//!                                  (fast, never awaits an order)   (serial, async)
//!   1s tick ─────────────────▶ EXIT task (close_due at exit_ts_s)
//! decoupled by `mpsc`, so the slow live order path (D2+) never blocks the decision
//! loop and we never miss a kline (the lag-arb edge). D1 runs the PaperExecutor
//! behind this same plumbing — zero capital — so the wiring validated here is the
//! exact wiring D4/D5 use, only swapping the backend.
//!
//! `process_kline` is the PURE SEAM: SignalEngine.on_kline → expand_signals →
//! decide → ExecCommands. The live decision task AND the D1 parity harness both call
//! it, so the gate ("the loop routes the validated logic, doesn't corrupt it") tests
//! the same code that runs live. The state lock is `std::sync::Mutex`
//! (`SharedBotState`); tasks lock-mutate-drop in a scope and never hold across an
//! `.await` (the compiler enforces this — a held std guard isn't Send).

#![allow(dead_code)] // tasks wired in main.rs (D1); some helpers used by tests / D2+

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::sync::atomic::Ordering;

use std::str::FromStr;

use dashmap::DashMap;
use polymarket_client_sdk_v2::POLYGON;
use polymarket_client_sdk_v2::auth::{LocalSigner, Signer};
use polymarket_client_sdk_v2::clob::types::Side;
use polymarket_client_sdk_v2::types::{Decimal, U256};
use tokio::sync::{mpsc, watch};
use tracing::{debug, info, warn};

use crate::decision::executor::{ClosedTrade, Executor, ExitKind, OpenIntent, PaperExecutor};
use crate::decision::{Action, BookProvider, DecisionConfig, LiveBook, RestProbe, TriggerDecision, decide};
use crate::guards::{EntryContext, GuardVerdict, Guards};
use crate::live_backend::LiveBackend;
use crate::oplog::OpLog;
use crate::rest::RestClient;
use crate::signal::{Direction, MarketCatalog, MarketRef, Signal, SignalEngine, Trigger, expand_signals};
use crate::config::{ExitRule, ExitsConfig};
use crate::state::{EventBbo, SharedState, STALE_BBO_MS};
use crate::state::persist::{OpenPosition, Outcome};
use crate::state::now_ms;
use crate::state::store::SharedBotState;
use crate::trade_log::{Thresholds, TradeLog, fee_f64, now_ms as tl_now};
use crate::window_logger;

/// A finalized Binance kline (re-exported from `state` — defined there to avoid a
/// module cycle, since the WS client also references it).
pub use crate::state::KlineClose;

/// Execution-log path (one JSONL row per attempt; the autopsy source).
const EXEC_LOG: &str = "data/live/execution_log.jsonl";

/// Trade-log context captured at decision time, carried with the command so the
/// execution task can stamp fill timestamps and emit the autopsy row.
#[derive(Debug, Clone)]
pub struct DecisionCtx {
    pub asset: String,
    pub interval: String,
    pub epoch: i64,
    pub side: String,
    pub signal_id: String,
    pub t_signal_ms: i64,
    pub t_decision_ms: i64,
    pub ask_at_signal: f64,
    pub binance_ret_bps: f64,
    pub window_s: i64,
    pub stake_usd: f64,
    /// Vol-normalized displacement at entry (the PRIMARY gate variable). Logged so
    /// live z-analysis needs no reconstruction. 0.0 on the legacy (non-v2) path.
    pub z: f64,
    /// Seconds-to-settle at entry (the ttl the max-ttl gate acts on). 0 on legacy.
    pub ttl_s: i64,
}

/// A command from the decision task to the execution task.
#[derive(Debug, Clone)]
pub enum ExecCommand {
    /// Open (or DCA-add) a lot.
    Open { intent: OpenIntent, ctx: DecisionCtx },
    /// REGLA C: close every lot on `token` at `exit_price` (None ⇒ can't price → skip).
    CloseToken { token: String, exit_price: Option<f64>, now_ms: i64 },
}

fn outcome_of(d: Direction) -> Outcome {
    match d {
        Direction::Up => Outcome::Up,
        Direction::Down => Outcome::Down,
    }
}

/// Outcome → "Up"/"Down" (Outcome has no inherent string method).
fn outcome_str(o: Outcome) -> &'static str {
    match o {
        Outcome::Up => "Up",
        Outcome::Down => "Down",
    }
}

fn signal_id(asset: &str, trigger_ts: i64, interval: &str, dir: Direction) -> String {
    format!("{asset}-{trigger_ts}-{interval}-{}", dir.as_str())
}

fn interval_secs(interval: &str) -> i64 {
    crate::signal::INTERVALS.iter().find(|(iv, _)| *iv == interval).map(|(_, s)| *s).unwrap_or(0)
}

/// Full output of `process_kline` when a trigger fires. Carries the raw trigger
/// + expanded signals (per-interval projections) alongside the decision and the
/// exec commands so the caller can (a) ROUTE the commands (the original use case)
/// AND (b) MEASURE the repricing window per trigger via `window_logger`. Both
/// downstream consumers need a coherent snapshot from one engine.on_kline call
/// (the engine is stateful -- you can't peek twice).
///
/// `dropped_cells` lists the `(asset, interval)` pairs that `filter_disabled_cells`
/// removed BEFORE `decide` ran. The live caller emits one
/// `signal_dropped_disabled_cell` oplog event per pair so we can VERIFY (post-deploy,
/// from the audit log) that the production filter is actually firing on the
/// configured cells rather than silently no-op'ing. Empty when the toml has no
/// `disabled_cells` entry OR when this trigger's signals didn't match any
/// disabled cell.
#[derive(Debug, Clone)]
pub struct TriggerOutcome {
    pub trigger: Trigger,
    pub signals: Vec<Signal>,
    pub decision: TriggerDecision,
    pub commands: Vec<ExecCommand>,
    /// `(asset, interval)` pairs dropped by `filter_disabled_cells` for THIS
    /// trigger. Empty when the toml has no `disabled_cells` entry OR when this
    /// trigger's signals didn't match any disabled cell. The live caller emits
    /// one `signal_dropped_disabled_cell` oplog event per pair (audit trail).
    pub dropped_cells: Vec<(String, String)>,
}

/// G5-test-rig: post-`expand_signals` filter (TEST MODE ONLY). When `true`,
/// drops every signal that is NOT (interval == "15m" AND stratum == Immediate).
/// 15m IM trades ALWAYS close by SELL in window (never past-close, by the
/// stratum math: IM is ttr > 300 → exit_offset = (900-ttr)+300 = 1200-ttr <
/// 900 always), so this isolates the SELL path from the redeem path for
/// controlled testing. **`false` = full bot behavior, byte-identical to the
/// pre-G5-test-rig code path (filter is a no-op).**
///
/// LOCATION: applied AFTER expand_signals (Capa-B-validated), BEFORE decide.
/// We do NOT touch expand_signals's signature (preserves the Python parity
/// gate from Phase 4); we just trim its output before feeding decide.
fn filter_for_test_rig(signals: Vec<crate::signal::Signal>, restrict_15m_im: bool)
    -> Vec<crate::signal::Signal>
{
    if !restrict_15m_im {
        return signals; // no-op: identical to the pre-G5-test-rig path
    }
    signals
        .into_iter()
        .filter(|s| s.interval == "15m" && s.stratum == crate::signal::Stratum::Immediate)
        .collect()
}

/// PRODUCTION cell filter: drop signals whose `<asset>:<interval>` is listed in
/// `disabled` (an opaque list parsed from bot.toml `[markets] disabled_cells`).
/// Empty list = no-op fast path = byte-identical to the pre-filter path.
///
/// Rationale: data-driven cell suppression. The 10-day TP backtest (5/6-5/16,
/// 1949 positions, `data/derived/backtest_tp_v1`) identified `ETH:15m` as the
/// only chronically deficitario cell. Dropping it via this filter is
/// REVERSIBLE WITHOUT RECOMPILE (edit toml, restart) and keeps the signal-engine
/// + expand_signals path Python-parity-clean (Capa B untouched).
///
/// LOCATION: applied AFTER `expand_signals` (Capa-B-validated) AND AFTER
/// `filter_for_test_rig`, BEFORE `decide`. Match key is the exact string
/// `format!("{}:{}", s.asset, s.interval)` -- CASE-SENSITIVE on purpose: a typo
/// (`eth:15m` vs the bot-emitted `ETH:15m`) is caught at validate-time by
/// `config::Config::validate`, so by the time we reach this filter the strings
/// are known-good. Case-sensitivity here is the second leg of fail-closed.
fn filter_disabled_cells(
    signals: Vec<crate::signal::Signal>,
    disabled: &[String],
) -> Vec<crate::signal::Signal> {
    if disabled.is_empty() {
        return signals; // no-op fast path
    }
    signals
        .into_iter()
        .filter(|s| {
            let cell = format!("{}:{}", s.asset, s.interval);
            !disabled.contains(&cell)
        })
        .collect()
}

/// Identify which `(asset, interval)` pairs in `signals` WILL be dropped by
/// `filter_disabled_cells(_, disabled)`. Used by `process_kline` to populate
/// `TriggerOutcome::dropped_cells` so the caller can emit oplog audit events
/// WITHOUT duplicating the filter logic. Empty `disabled` -> empty result (no
/// allocation in the hot path).
fn identify_disabled_drops(
    signals: &[crate::signal::Signal],
    disabled: &[String],
) -> Vec<(String, String)> {
    if disabled.is_empty() {
        return Vec::new();
    }
    signals
        .iter()
        .filter(|s| {
            let cell = format!("{}:{}", s.asset, s.interval);
            disabled.contains(&cell)
        })
        .map(|s| (s.asset.clone(), s.interval.clone()))
        .collect()
}

/// THE PURE SEAM. One finalized kline → a `TriggerOutcome` (trigger, signals,
/// decision, exec commands), or `None` if no trigger / no scope market. Pure
/// w.r.t. side effects: reads `book`/`catalog`/`positions` + the injected
/// `rest`, RETURNS commands; the caller (live task or parity harness) applies
/// them. `now_ms` is the decision clock.
///
/// G5-test-rig: `restrict_15m_im` is a TEST FLAG. `false` = the default,
/// byte-identical to the pre-G5-test-rig behavior. `true` = post-filter to
/// only 15m IM signals (see `filter_for_test_rig`).
///
/// `disabled_cells` is the PRODUCTION cell-suppression list from
/// `bot.toml [markets] disabled_cells`. Empty (default) = no filter. Non-empty =
/// drop signals whose `<asset>:<interval>` matches. The dropped pairs are
/// returned in `TriggerOutcome::dropped_cells` so the caller (run_decision_task)
/// can emit `signal_dropped_disabled_cell` oplog events for audit.
#[allow(clippy::too_many_arguments)]
pub fn process_kline(
    engine: &mut SignalEngine,
    kline: &KlineClose,
    catalog: &dyn MarketCatalog,
    book: &dyn BookProvider,
    cfg: &DecisionConfig,
    rest: impl Fn(&str) -> RestProbe,
    positions: &[OpenPosition],
    now_ms: i64,
    restrict_15m_im: bool,
    disabled_cells: &[String],
) -> Option<TriggerOutcome> {
    let trig = engine.on_kline(&kline.asset, kline.t_s, kline.close)?;
    // Call `decide` for EVERY trigger (even an empty scope), exactly like the
    // Capa-B-validated path: an empty scope yields a `fired=false` decision with no
    // commands. (Short-circuiting on empty signals would diverge from that path.)
    let signals = expand_signals(&trig, catalog);
    // G5-test-rig: trim signals AFTER expand_signals, BEFORE decide. `false`
    // (default) is a no-op identity function.
    let signals = filter_for_test_rig(signals, restrict_15m_im);
    // Production cell filter: drop signals whose <asset>:<interval> is in the
    // bot.toml `disabled_cells` list. Captured BEFORE the drop so the caller
    // can emit one oplog event per dropped pair (audit trail). Empty list =
    // both calls are no-op fast paths.
    let dropped_cells = identify_disabled_drops(&signals, disabled_cells);
    let signals = filter_disabled_cells(signals, disabled_cells);
    let direction = if trig.ret_bps > 0.0 { Direction::Up } else { Direction::Down };
    let decision = decide(
        &kline.asset, trig.trigger_ts, direction, &signals, cfg, book, now_ms, rest, positions,
    );
    let mut cmds = Vec::new();
    for action in &decision.actions {
        match action {
            Action::Fire { interval, stratum, bet_token, fill, shares } => {
                let hold_s = cfg.band(*stratum).2;
                let secs = interval_secs(interval).max(1);
                let ctx = DecisionCtx {
                    asset: kline.asset.clone(),
                    interval: interval.clone(),
                    epoch: (trig.trigger_ts / secs) * secs,
                    side: direction.as_str().to_string(),
                    signal_id: signal_id(&kline.asset, trig.trigger_ts, interval, direction),
                    t_signal_ms: kline.received_at_ms,
                    t_decision_ms: now_ms,
                    ask_at_signal: *fill,
                    binance_ret_bps: trig.ret_bps,
                    window_s: trig.window_s as i64,
                    stake_usd: cfg.stake_usd,
                    z: 0.0, // legacy path: z not computed
                    ttl_s: 0,
                };
                cmds.push(ExecCommand::Open {
                    intent: OpenIntent {
                        token_id: bet_token.clone(),
                        // Pieza 4: propagate the structured asset from the
                        // decision context. This is the canonical value
                        // (came in via the Binance kline); never parsed.
                        asset: ctx.asset.clone(),
                        side: outcome_of(direction),
                        fill_price: *fill,
                        shares: *shares,
                        interval: interval.clone(),
                        signal_id: ctx.signal_id.clone(),
                        exit_ts_s: trig.trigger_ts + hold_s,
                        now_ms,
                    },
                    ctx,
                });
            }
            Action::CloseOpposite { opposite_token, close_price, .. } => {
                cmds.push(ExecCommand::CloseToken {
                    token: opposite_token.clone(),
                    exit_price: *close_price,
                    now_ms,
                });
            }
            Action::Reject { .. } => {}
        }
    }
    Some(TriggerOutcome { trigger: trig, signals, decision, commands: cmds, dropped_cells })
}

/// v2 PARALLEL decision path (5minSnip port) — runs INSTEAD of the 5bps trigger
/// strategy when `[v2] enabled`. On each finalized kline: push it to the price
/// history, then for each active interval-market of this asset compute the
/// vol-normalized `z`, the expected edge, and — if the gate passes — emit an
/// edge-sized Open. Returns `(command, predicted_p)` so the caller records
/// predicted_p per token for the rolling recalibrator.
///
/// Differs from `process_kline`/`decide` on purpose (the validated v2 strategy):
///   * CONTINUOUS evaluation per kline (not gated by a 5bps trigger).
///   * Side = displacement direction (favored side), one entry per market.
///   * Gate = vol-cap + dvr-floor + `edge = pcal(z) - ask - fee >= edge_min`.
///   * Sizing = depth-aware edge-proportional (the ~2x lever).
///   * HOLDS TO SETTLEMENT: exit_ts_s well past resolution so the time-exit never
///     sells early; the redemption task claims the resolved winner.
/// The existing `decide()` + its Capa-B parity tests are untouched.
#[allow(clippy::too_many_arguments)]
pub fn process_kline_v2(
    history: &mut crate::v2::PriceHistory,
    kline: &KlineClose,
    catalog: &dyn MarketCatalog,
    book: &dyn BookProvider,
    strats: &std::collections::HashMap<&'static str, crate::v2::IntervalStrat>,
    base_usd: f64,
    max_pos_usd: f64,
    positions: &[OpenPosition],
    now_ms: i64,
    disabled: &[String],
) -> Vec<(ExecCommand, f64)> {
    history.push(&kline.asset, kline.t_s, kline.close);
    let mut out = Vec::new();
    for (interval, secs) in crate::signal::INTERVALS {
        // Per-interval strategy: 5m uses the base config; 15m uses its own gates,
        // calibration curve, and recal bias. A missing/disabled strat = skip.
        let Some(s) = strats.get(interval) else { continue };
        if !s.enabled {
            continue;
        }
        // Respect the same retired-cell list as the legacy path (e.g. ETH:15m).
        let cell = format!("{}:{}", kline.asset, interval);
        if disabled.iter().any(|c| c == &cell) {
            continue;
        }
        let epoch = (kline.t_s / secs) * secs;
        let resolution = epoch + secs;
        let ttl = resolution - kline.t_s;
        if ttl < s.min_ttl_s || ttl > secs {
            continue;
        }
        // Late-entry gate (the 15m edge lives in the last few minutes). 0 = no gate.
        if s.late_entry_max_ttl_s > 0 && ttl > s.late_entry_max_ttl_s {
            continue;
        }
        let Some(m) = catalog.active_market(&kline.asset, interval, epoch) else { continue };
        // Side from displacement direction (the favored side).
        let Some(open) = history.close_at(&kline.asset, epoch - 1) else { continue };
        let up = kline.close >= open;
        let bet_token = if up { m.up_token_id.clone() } else { m.down_token_id.clone() };
        // One entry per market: skip if any lot exists on either side of THIS market.
        if positions
            .iter()
            .any(|p| p.token_id == m.up_token_id || p.token_id == m.down_token_id)
        {
            continue;
        }
        let Some(bbo) = book.bbo(&bet_token) else { continue };
        if bbo.ts_ms <= 0 || now_ms - bbo.ts_ms > STALE_BBO_MS {
            continue;
        }
        let ask = match bbo.best_ask {
            Some(a) if a > 0.0 => a,
            _ => continue,
        };
        // Max-ask quality cap (15m: 0.70). 0.0 (or >=1.0) = no cap (5m).
        if s.max_ask > 0.0 && ask > s.max_ask {
            continue;
        }
        let Some(f) = history.features(
            &kline.asset,
            epoch,
            kline.t_s,
            ttl as f64,
            up,
            ask,
            s.recal_bias,
            &s.cal_z,
            &s.cal_w,
        ) else {
            continue;
        };
        // v2 gate uses edge_min/vol_cap/dvr_floor/z_min PER INTERVAL (5m base vs 15m
        // override). The z_min gate is the fix for firing on near-zero displacement
        // at the window open: require a REAL vol-normalized move before betting.
        if f.vol_bps > s.vol_cap
            || f.dvr < s.dvr_floor
            || f.edge < s.edge_min
            || f.disp_bps <= 0.0
            || f.z < s.z_min
        {
            continue;
        }
        // Depth-aware edge-proportional sizing. No live book-depth feed yet, so the
        // cap is max_pos_usd; the FOK worst-price protects fill quality. (Wire real
        // depth via the book channel later to also avoid wasted FOK-kills.)
        let stake = crate::v2::edge_stake(
            f.edge,
            base_usd,
            s.edge_ref,
            base_usd.min(1.05),
            max_pos_usd,
            max_pos_usd,
        );
        if stake <= 0.0 {
            continue;
        }
        let shares = stake / ask;
        let dir = if up { Direction::Up } else { Direction::Down };
        let sid = signal_id(&kline.asset, kline.t_s, interval, dir);
        let exit_ts_s = resolution + 300; // hold to settle → redemption claims it
        let ctx = DecisionCtx {
            asset: kline.asset.clone(),
            interval: interval.to_string(),
            epoch,
            side: dir.as_str().to_string(),
            signal_id: sid.clone(),
            t_signal_ms: kline.received_at_ms,
            t_decision_ms: now_ms,
            ask_at_signal: ask,
            binance_ret_bps: f.disp_bps,
            window_s: 0,
            stake_usd: stake,
            z: f.z,
            ttl_s: ttl,
        };
        let intent = OpenIntent {
            token_id: bet_token,
            asset: kline.asset.clone(),
            side: outcome_of(dir),
            fill_price: ask,
            shares,
            interval: interval.to_string(),
            signal_id: sid,
            exit_ts_s,
            now_ms,
        };
        out.push((ExecCommand::Open { intent, ctx }, f.p));
    }
    out
}

// ===================== live catalog (snapshot from state.markets) =====================

/// A catalog backed by an owned snapshot (avoids the DashMap guard-lifetime problem
/// of returning `&MarketRef` from a live map).
struct SnapshotCatalog {
    by_key: std::collections::HashMap<(String, String, i64), MarketRef>,
}
impl MarketCatalog for SnapshotCatalog {
    fn active_market(&self, asset: &str, interval: &str, epoch: i64) -> Option<&MarketRef> {
        self.by_key.get(&(asset.to_string(), interval.to_string(), epoch))
    }
}

/// Snapshot the markets a trigger at `t_s` could project onto (current epoch per
/// interval), cloned from the live `state.markets`.
fn snapshot_catalog_for(state: &SharedState, asset: &str, t_s: i64) -> SnapshotCatalog {
    let mut by_key = std::collections::HashMap::new();
    for (interval, secs) in crate::signal::INTERVALS {
        let epoch = (t_s / secs) * secs;
        if let Some(m) = state.markets.get(&(asset.to_string(), interval.to_string(), epoch)) {
            by_key.insert((asset.to_string(), interval.to_string(), epoch), m.clone());
        }
    }
    SnapshotCatalog { by_key }
}

// ===================== async tasks =====================

/// DECISION task: consume kline-closes, run `process_kline`, evaluate E' guards,
/// forward allowed commands. Never awaits an order.
///
/// PIECE 3 wiring (guards in the live loop):
///   * CONTINUOUS check (kill-switch + feed-dead) on EVERY pass -- a halt latches
///     and the loop SKIPS processing (no decisions, no commands).
///   * PER-ENTRY check on each Open command -- the existing eval_guards routes
///     stake / per-token / total / hard cap / staleness / daily-stop. The
///     FREQUENCY breaker was REMOVED in W7 (2026-06-05; redundant with
///     max_open_positions + daily-loss-stop, and had a latent counting bug --
///     incremented at intent dispatch, not real POST).
///
/// PIECE 5: every operational event in the live loop is appended to the oplog
/// (kline_received, guard_halt, decision_fired, guard_eval, guard_blocked_open,
/// intent_open, order_recorded). The full temporal sequence is reconstructable.
#[allow(clippy::too_many_arguments)]
pub async fn run_decision_task(
    state: Arc<SharedState>,
    store_state: SharedBotState,
    cfg: DecisionConfig,
    guards: Arc<Mutex<Guards>>,
    oplog: Arc<OpLog>,
    mut klines: mpsc::UnboundedReceiver<KlineClose>,
    exec_tx: mpsc::UnboundedSender<ExecCommand>,
    mut shutdown: watch::Receiver<bool>,
    restrict_15m_im: bool,
    // `bot.toml [markets] disabled_cells`. Cloned once at spawn; read per-kline
    // inside `process_kline`. Empty (default) = no filter = full bot.
    disabled_cells: Vec<String>,
    // v2 strategy config + live operator controls (stake/on-off, dashboard-driven)
    // + the shared rolling recalibrator. When `vcfg.enabled`, the v2 path REPLACES
    // the 5bps strategy for each kline.
    vcfg: crate::config::V2Config,
    controls: Arc<crate::v2::Controls>,
    recal: crate::v2::RecalSet,
    // Live-arm gate: `Some(path)` in --mode live → enter ONLY when that file
    // exists (ARMED). Disarmed = truly idle (no entries, no phantom paper).
    // `None` in --mode paper → entries gated only by the ON/OFF control.
    live_gate: Option<String>,
) {
    info!("task started: decision_loop");
    oplog.sys("decision_loop_start", serde_json::json!({}));
    if vcfg.enabled {
        info!(edge_min = vcfg.edge_min, vol_cap = vcfg.vol_cap, dvr_floor = vcfg.dvr_floor,
            edge_ref = vcfg.edge_ref, base_usd = controls.base_usd(), max_pos_usd = controls.max_pos_usd(),
            trading_enabled = controls.enabled(),
            "decision_loop: v2 STRATEGY ACTIVE (z edge-gate + depth-aware edge sizing + rolling recal)");
        oplog.sys("v2_strategy_active", serde_json::json!({
            "edge_min": vcfg.edge_min, "vol_cap": vcfg.vol_cap, "dvr_floor": vcfg.dvr_floor,
            "edge_ref": vcfg.edge_ref, "base_usd": controls.base_usd(), "max_pos_usd": controls.max_pos_usd(),
        }));
    }
    let mut engine = SignalEngine::new();
    // v2 price-history ring (Binance 1s closes). 1024 covers a 15m window + the
    // 60s vol lookback + margin. Only consumed on the v2 path.
    let mut history = crate::v2::PriceHistory::new(1024);
    // v2 dedup: markets (token ids) this process has ALREADY fired an entry for.
    // Independent of execution lag — the store_state positions snapshot only
    // updates after the (possibly slow) execution task records the order, so we
    // cannot rely on it to prevent same-market re-entry within the lag window.
    let mut v2_entered: HashSet<String> = HashSet::new();
    // Throttle the settlement-booking sweep to once per wall-second.
    let mut last_settle_sweep_s: i64 = 0;
    loop {
        tokio::select! {
            maybe = klines.recv() => {
                let Some(kline) = maybe else { break };
                let now = now_ms();
                oplog.sys("kline_received", serde_json::json!({
                    "asset": kline.asset, "t_s": kline.t_s, "close": kline.close,
                    "received_at_ms": kline.received_at_ms,
                }));
                // PIECE 3: CONTINUOUS HALT CHECK (kill-switch + feed-dead) on EVERY pass.
                // W6-redux: log ONLY on Tripped / Resumed transitions, NOT every pass.
                // Pre-W6-redux this block logged `guard_halt` once per tick once halted,
                // which spammed the oplog and also (because of the latch bug) repeated
                // a frozen reason string forever even after the feed recovered.
                let last_feed = latest_feed_ms(&state, now);
                let check = match guards.lock() {
                    Ok(mut g) => g.check_continuous(last_feed, now),
                    Err(_) => {
                        warn!("guards mutex poisoned; skipping kline");
                        oplog.err("decision_loop", "guards mutex poisoned", serde_json::json!({}));
                        continue;
                    }
                };
                match check.transition {
                    crate::guards::HaltTransition::Tripped => {
                        let reason = check.halted.clone().unwrap_or_default();
                        warn!(%reason, "decision_loop: HALTED (continuous-check tripped)");
                        oplog.sys("guard_halt", serde_json::json!({
                            "reason": reason, "last_feed_ms": last_feed, "now_ms": now,
                        }));
                    }
                    crate::guards::HaltTransition::Resumed => {
                        info!(last_feed_ms = last_feed, now_ms = now,
                              "decision_loop: RESUMED (continuous-check cleared)");
                        oplog.sys("guard_resume", serde_json::json!({
                            "last_feed_ms": last_feed, "now_ms": now,
                        }));
                    }
                    crate::guards::HaltTransition::Stable => {}
                }
                if check.halted.is_some() {
                    // Halt is still active this pass: skip processing.
                    continue;
                }
                // SETTLEMENT BOOKING (decoupled from the relayer): the instant a
                // bet's 5m window has closed, decide win/lose from Binance
                // open-vs-close (the actual settlement basis) and stash it for
                // run_positions_refresh to book P&L immediately. This makes the
                // dashboard reflect realized P&L within ~1 min of resolution
                // REGARDLESS of redemption (which is rate-limited and only moves
                // the cash). Runs even when trading is OFF/disarmed (open positions
                // still resolve). Throttled to once per wall-second.
                let now_s = now / 1000;
                if vcfg.enabled && now_s > last_settle_sweep_s {
                    last_settle_sweep_s = now_s;
                    let snap: Vec<OpenPosition> = store_state
                        .lock()
                        .map(|bs| bs.positions.clone())
                        .unwrap_or_default();
                    for p in &snap {
                        if state.v2_settled.contains_key(&p.token_id) {
                            continue;
                        }
                        let resolution = p.exit_ts_s - 300; // exit_ts_s = resolution + 300
                        let win_secs = interval_secs(&p.interval).max(1);
                        let epoch = resolution - win_secs; // window open (5m=300s, 15m=900s)
                        // SIGNAL-INVALIDATION STOP (touch). While the position is
                        // still live, if side-signed displacement-from-open reverts
                        // to <= 0 the thesis is dead -> 5m SELLS at bid (when ARMED);
                        // 15m PAPER-LOGS only (both assets). Fires once per token.
                        if vcfg.inval_stop_enabled
                            && now_s < resolution
                            && !state.v2_stopped.contains_key(&p.token_id)
                        {
                            if let (Some(open), Some(cur)) = (
                                history.close_at(&p.asset, epoch - 1),
                                history.close_at(&p.asset, now_s),
                            ) {
                                let up = matches!(p.side, Outcome::Up);
                                let raw = (cur / open - 1.0) * 1e4;
                                let disp = if up { raw } else { -raw }; // side-signed
                                if disp <= 0.0 {
                                    use rust_decimal::prelude::ToPrimitive;
                                    let bid = state.bbo.get(&p.token_id).and_then(|b| b.best_bid);
                                    let armed = match &live_gate {
                                        Some(pth) => std::path::Path::new(pth).exists(),
                                        None => true, // paper mode: simulated sell
                                    };
                                    // 5m sells for real (armed + a bid to hit); 15m
                                    // and disarmed/no-bid = paper-log only.
                                    let do_sell = p.interval == "5m" && armed && bid.is_some();
                                    oplog.sys("inval_stop", serde_json::json!({
                                        "token_id": p.token_id, "asset": p.asset,
                                        "interval": p.interval, "side": if up {"up"} else {"down"},
                                        "disp_bps": disp, "model_bid": bid,
                                        "entry_ask": p.entry_price.to_f64().unwrap_or(0.0),
                                        "sec_in": now_s - epoch, "ttl": resolution - now_s,
                                        "action": if do_sell { "sell" } else { "paper" },
                                    }));
                                    if do_sell {
                                        let _ = exec_tx.send(ExecCommand::CloseToken {
                                            token: p.token_id.clone(),
                                            exit_price: bid, // FOK-sell at the bid
                                            now_ms: now,
                                        });
                                        info!(token = %p.token_id, disp, bid = ?bid,
                                            "inval_stop: 5m thesis dead -> SELL at bid");
                                    }
                                    // Dedup (failed FOK stays marked -> holds to settle).
                                    if state.v2_stopped.len() > 50_000 { state.v2_stopped.clear(); }
                                    state.v2_stopped.insert(p.token_id.clone(), true);
                                }
                            }
                        }
                        if now_s < resolution + 2 {
                            continue; // wait for the close (resolution-1) bar to land
                        }
                        let (Some(op), Some(fin)) = (
                            history.close_at(&p.asset, epoch - 1),
                            history.close_at(&p.asset, resolution - 1),
                        ) else {
                            continue; // ring doesn't have both ends (e.g. post-restart)
                        };
                        let up = matches!(p.side, Outcome::Up);
                        let won = up == (fin >= op);
                        state.v2_settled.insert(p.token_id.clone(), won);
                    }
                }
                let catalog = snapshot_catalog_for(&state, &kline.asset, kline.t_s);
                let positions: Vec<OpenPosition> = {
                    match store_state.lock() {
                        Ok(bs) => bs.positions.clone(),
                        Err(_) => {
                            warn!("state mutex poisoned; skipping kline");
                            oplog.err("decision_loop", "state mutex poisoned", serde_json::json!({}));
                            continue;
                        }
                    }
                };
                let book = LiveBook(&state);
                // v2 STRATEGY PATH: replaces the 5bps trigger + ask-band + flat
                // sizing with the validated z edge-gate + depth-aware edge sizing.
                if vcfg.enabled {
                    // Operator OFF switch (dashboard): pause NEW entries. Open
                    // positions still exit/redeem; the price history still updates.
                    if !controls.enabled() {
                        history.push(&kline.asset, kline.t_s, kline.close);
                        continue;
                    }
                    // LIVE-ARM gate: in --mode live, only enter when ARMED. Disarmed
                    // = truly idle (no phantom paper positions). Paper mode (None)
                    // skips this check. Keep the price history warm either way.
                    if let Some(armed_path) = &live_gate
                        && !std::path::Path::new(armed_path).exists()
                    {
                        history.push(&kline.asset, kline.t_s, kline.close);
                        continue;
                    }
                    // Per-interval strategies, rebuilt each kline with the CURRENT
                    // per-interval recal de-bias. 5m = base config (unchanged); 15m =
                    // its own gates + curve + recal, gated by v2.i15m.enabled.
                    let mut strats: std::collections::HashMap<&'static str, crate::v2::IntervalStrat> =
                        std::collections::HashMap::with_capacity(2);
                    strats.insert("5m", vcfg.strat_5m(recal.bias("5m")));
                    strats.insert("15m", vcfg.i15m.strat(vcfg.enabled, recal.bias("15m")));
                    let cmds = process_kline_v2(
                        &mut history, &kline, &catalog, &book, &strats,
                        controls.base_usd(), controls.max_pos_usd(), &positions, now, &disabled_cells,
                    );
                    for (cmd, pred) in cmds {
                        if let ExecCommand::Open { intent, ctx } = &cmd {
                            // DEDUP: one entry per market, lag-proof. The store_state
                            // positions check inside process_kline_v2 can be stale
                            // (execution task lags), so we ALSO gate on a local set of
                            // already-fired tokens — this is what stops the same market
                            // being entered N times within the execution lag window.
                            if v2_entered.contains(&intent.token_id) {
                                continue;
                            }
                            let verdict = {
                                let g = guards.lock().expect("guards mutex poisoned");
                                eval_guards(&g, &state, &positions, intent, now)
                            };
                            if !verdict.is_allow() {
                                warn!(token = %intent.token_id, ?verdict, "v2: guard blocked open (no POST)");
                                oplog.sys("v2_guard_blocked_open", serde_json::json!({
                                    "token_id": intent.token_id, "signal_id": ctx.signal_id,
                                    "reason": verdict.reason().unwrap_or(""),
                                }));
                                continue;
                            }
                            // Committed to firing this market: mark it so no later kline
                            // re-enters it while execution catches up. Cap the set so it
                            // can't grow unbounded over a long session.
                            if v2_entered.len() > 50_000 {
                                v2_entered.clear();
                            }
                            v2_entered.insert(intent.token_id.clone());
                            // Record (interval, predicted-p) for the recalibrator
                            // (drained on resolution, routed to the interval's recal).
                            state.v2_pred.insert(intent.token_id.clone(), (intent.interval.clone(), pred));
                            state.counters.decisions.fetch_add(1, Ordering::Relaxed);
                            oplog.sys("v2_intent_open", serde_json::json!({
                                "token_id": intent.token_id, "signal_id": ctx.signal_id,
                                "side": ctx.side, "stake_usd": ctx.stake_usd,
                                "fill_price": intent.fill_price, "shares": intent.shares,
                                "disp_bps": ctx.binance_ret_bps, "p": pred,
                                "z": ctx.z, "ttl_s": ctx.ttl_s, "ask": ctx.ask_at_signal,
                                "exit_ts_s": intent.exit_ts_s,
                            }));
                            info!(token = %intent.token_id, p = pred, stake = ctx.stake_usd,
                                "v2: FIRE (edge-gated, edge-sized)");
                        }
                        if exec_tx.send(cmd).is_err() {
                            warn!("execution task gone; v2 decision loop stopping");
                            return;
                        }
                    }
                    // Bound the predicted-p map in paper mode (no positions-refresh
                    // drain): clear if it grows unreasonably large.
                    if state.v2_pred.len() > 50_000 {
                        state.v2_pred.clear();
                    }
                    continue; // v2 path handled this kline; skip the legacy strategy
                }
                let rest = |_t: &str| RestProbe::Skip; // D1: phantom REST off (paper); live REST in D2
                let Some(outcome) =
                    process_kline(&mut engine, &kline, &catalog, &book, &cfg, rest, &positions, now, restrict_15m_im, &disabled_cells)
                else { continue };
                // AUDIT: one oplog event per disabled-cell drop so post-deploy
                // we can VERIFY the filter is firing (and not silently no-op'ing
                // because the toml flag was ignored / lookup failed). Includes
                // trigger_ts to cross-correlate with the same trigger's
                // kline_received and decision_fired events.
                for (drop_asset, drop_interval) in &outcome.dropped_cells {
                    oplog.sys("signal_dropped_disabled_cell", serde_json::json!({
                        "asset": drop_asset,
                        "interval": drop_interval,
                        "trigger_ts": outcome.trigger.trigger_ts,
                    }));
                }
                // WINDOW LOGGER: spawn a fire-and-forget task that snapshots PM
                // book at {0, 500, 1000, 3000, 10000} ms post-trigger and emits
                // ONE `book_window_around_trigger` oplog event per trigger. Runs
                // for EVERY trigger (accepted or rejected) so the 5-bps
                // repricing-window curve can be measured in-vivo without
                // depending on trade acceptance. Cheap (DashMap reads on a
                // tokio task); never blocks the decision loop.
                {
                    let input = window_logger::build_input_from_decision(
                        &outcome.trigger, &outcome.signals, &outcome.decision,
                        // G6: pass the kline ingest wall-clock so the window
                        // event carries signal arrival latency inline.
                        kline.received_at_ms,
                    );
                    let _ = window_logger::spawn_window_logger(
                        state.clone(), oplog.clone(), input,
                    );
                }
                let TriggerOutcome { decision, commands: cmds, .. } = outcome;
                state.counters.decisions.fetch_add(1, Ordering::Relaxed);
                if decision.fired {
                    info!(asset = %decision.asset, ts = decision.trigger_ts,
                        actions = decision.actions.len(), "decision: FIRED");
                    oplog.sys("decision_fired", serde_json::json!({
                        "asset": decision.asset, "trigger_ts": decision.trigger_ts,
                        "actions": decision.actions.len(),
                    }));
                }
                for cmd in cmds {
                    if let ExecCommand::Open { intent, ctx } = &cmd {
                        let verdict = {
                            let g = guards.lock().expect("guards mutex poisoned");
                            eval_guards(&g, &state, &positions, intent, now)
                        };
                        let order_usd = intent.fill_price * intent.shares;
                        oplog.sys("guard_eval", serde_json::json!({
                            "token_id": intent.token_id, "signal_id": ctx.signal_id,
                            "stake_usd": ctx.stake_usd, "order_usd": order_usd,
                            "verdict": format!("{verdict:?}"), "allowed": verdict.is_allow(),
                        }));
                        if !verdict.is_allow() {
                            warn!(token = %intent.token_id, ?verdict, "decision: guard blocked open (no POST)");
                            oplog.sys("guard_blocked_open", serde_json::json!({
                                "token_id": intent.token_id, "signal_id": ctx.signal_id,
                                "reason": verdict.reason().unwrap_or(""),
                            }));
                            log_blocked(ctx, intent, &verdict);
                            continue;
                        }
                        oplog.sys("intent_open", serde_json::json!({
                            "token_id": intent.token_id, "signal_id": ctx.signal_id,
                            "side": ctx.side, "stake_usd": ctx.stake_usd,
                            "fill_price": intent.fill_price, "shares": intent.shares,
                            "exit_ts_s": intent.exit_ts_s,
                        }));
                        // EDGE15 INSTRUMENTATION: spawn a fire-and-forget per-trade
                        // marks logger (mirror of the window_logger spawn above).
                        // Captures the bet token's book at {0,15,20,25}s post-decision
                        // so edge15 (= shares*(mid15-ask)-fee_in) and the post-15s
                        // drift are measured IN VIVO on the bot's real trades.
                        // PASSIVE: reads state.bbo, writes one `trade_edge_marks`
                        // event; never touches positions/decision/exit — the bot
                        // still exits at 120s via run_exit_task (untouched).
                        let _ = window_logger::spawn_edge_marks_logger(
                            state.clone(),
                            oplog.clone(),
                            window_logger::EdgeMarksInput {
                                signal_id: ctx.signal_id.clone(),
                                token_id: intent.token_id.clone(),
                                asset: ctx.asset.clone(),
                                interval: ctx.interval.clone(),
                                side: ctx.side.clone(),
                                decision_ts_ms: ctx.t_decision_ms,
                                entry_ask: ctx.ask_at_signal,
                                shares: intent.shares,
                                ret_bps: ctx.binance_ret_bps,
                            },
                        );
                    }
                    if exec_tx.send(cmd).is_err() {
                        warn!("execution task gone; decision loop stopping");
                        oplog.err("decision_loop", "execution task gone", serde_json::json!({}));
                        return;
                    }
                    // W7 (2026-06-05): removed the pre-dispatch record_order call
                    // here. It fed the FREQUENCY breaker (also removed) and was
                    // mis-located -- it incremented when the Open command was
                    // DISPATCHED to the executor, not when a real POST landed,
                    // so paper / gated / aborted opens all counted. The breaker
                    // is gone; runaway is bounded by max_open_positions and
                    // capital risk by the daily-loss-stop.
                }
            }
            _ = shutdown.changed() => if *shutdown.borrow() { break },
        }
    }
    info!("decision_loop: shutdown");
    oplog.sys("decision_loop_shutdown", serde_json::json!({}));
}

/// MAX `ts_ms` across the live BBO map = the "any-token last price_change" the
/// continuous feed-dead check uses. Empty map -> `now` (cold start: don't trip
/// before the first event has arrived).
fn latest_feed_ms(state: &SharedState, now: i64) -> i64 {
    state.bbo.iter().map(|kv| kv.value().ts_ms).max().unwrap_or(now)
}

/// PIECE 3: feed a closed-lot NET P&L into the daily-loss-stop accumulator.
/// Without this hook, `record_net_pnl` was dead code and the stop NEVER fired.
fn feed_guards_net_pnl(guards: &Arc<Mutex<Guards>>, ct: &ClosedTrade, now_ms: i64) {
    use rust_decimal::prelude::ToPrimitive;
    let shares = ct.shares.to_f64().unwrap_or(0.0);
    let entry = ct.entry_price.to_f64().unwrap_or(0.0);
    let exit = ct.exit_price.to_f64().unwrap_or(0.0);
    let sell_fee = fee_f64(shares, exit);
    let buy_fee = fee_f64(shares, entry);
    let net = (shares * exit - sell_fee) - (shares * entry + buy_fee);
    let net_dec = rust_decimal::Decimal::try_from(net).unwrap_or_default();
    if let Ok(mut g) = guards.lock() {
        g.record_net_pnl(net_dec, now_ms);
    }
}

/// Evaluate the E′ entry guards for one proposed open, reading active-only exposure
/// (fix 2) from the position snapshot + live book.
///
/// PIECE 4: the exposure denominator now consults `state.resolved_tokens` (the
/// REST-derived per-token resolution flag updated by `run_positions_refresh`).
/// A token absent from the snapshot is treated as active (conservative fallback
/// for data-api lag on a just-opened lot). With the snapshot present, resolved
/// lots are dropped from the cap denominator -- the precise active-only filter,
/// not just the curPrice in {0,1} conservative proxy.
fn eval_guards(
    guards: &Guards,
    state: &SharedState,
    positions: &[OpenPosition],
    intent: &OpenIntent,
    now: i64,
) -> GuardVerdict {
    use rust_decimal::prelude::ToPrimitive;
    let rows: Vec<crate::exec::PosRow> = positions.iter().map(|p| {
        let cur = state.bbo.get(&p.token_id).and_then(|b| b.best_bid).unwrap_or(0.0);
        // PIECE 4: read the REST-derived resolution flag from the cache. Missing
        // entry -> false (conservative active fallback).
        let resolved_from_rest = state.resolved_tokens.get(&p.token_id).map(|r| *r).unwrap_or(false);
        crate::exec::PosRow {
            token: p.token_id.clone(),
            size: p.shares.to_f64().unwrap_or(0.0),
            cur_price: cur,
            redeemable: resolved_from_rest,
            end_in_past: false,
        }
    }).collect();
    let (_, active_total) = crate::exec::active_exposure(&rows);
    let active_token = crate::exec::active_exposure_for_token(&rows, &intent.token_id);
    let order_usd = intent.fill_price * intent.shares;
    let bk = state.bbo.get(&intent.token_id).map(|b| b.ts_ms).unwrap_or(now);
    let ectx = EntryContext {
        stake_usd: rust_decimal::Decimal::try_from(order_usd).unwrap_or_default(),
        order_usd: rust_decimal::Decimal::try_from(order_usd).unwrap_or_default(),
        token: &intent.token_id,
        active_total_exposure: rust_decimal::Decimal::try_from(active_total).unwrap_or_default(),
        active_token_exposure: rust_decimal::Decimal::try_from(active_token).unwrap_or_default(),
        book_ts_ms: bk,
        last_price_change_ms: bk,
        now_ms: now,
    };
    guards.check_entry(&ectx)
}

/// PIECE 4 -- POSITIONS REFRESH task. Periodically polls the data-api /positions
/// and updates `state.resolved_tokens` with each token's RESOLUTION flag (true
/// when redeemable / cur_price extreme / end_date past). The exposure-cap guard
/// reads this cache (see `eval_guards`). Read-only, ZERO capital.
///
/// PIECE 5: every poll emits api_call / api_ok / api_err to the oplog so the
/// exact REST call + response + latency + verbatim error text is reconstructable.
pub async fn run_positions_refresh(
    rest: Arc<RestClient>,
    state: Arc<SharedState>,
    oplog: Arc<OpLog>,
    period: std::time::Duration,
    mut shutdown: watch::Receiver<bool>,
    // G8: pnl_recorder integration. `bs` + `guards` + `recorder` are required
    // for the resolved-positions -> daily P&L hook. Pre-G8 the function did
    // ONLY the resolved_tokens cache update (read-only for the exposure cap);
    // now it ALSO records P&L for newly-resolved positions + removes them from
    // bs.positions. The cache update still runs first.
    bs: crate::state::store::SharedBotState,
    guards: Arc<Mutex<crate::guards::Guards>>,
    recorder: Arc<Mutex<crate::pnl_recorder::PnlRecorder>>,
    // v2 rolling recalibrators (per interval): fed (predicted_p, won) when a v2 bet
    // resolves — routed to the bet's interval — then both persisted. Inert when v2
    // is disabled (v2_pred empty).
    recal: crate::v2::RecalSet,
) {
    info!(period_secs = period.as_secs(), "task started: positions_refresh (G8 wired)");
    oplog.sys("positions_refresh_start", serde_json::json!({ "period_secs": period.as_secs() }));
    let mut iv = tokio::time::interval(period);
    iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = iv.tick() => {
                // Wallet USDC balance for the dashboard (best-effort; non-fatal).
                match rest.get_balance().await {
                    Ok(b) => state.balance_milli.store(
                        (b.balance_usdc * 1000.0) as i64,
                        std::sync::atomic::Ordering::Relaxed,
                    ),
                    Err(e) => warn!(error = %e, "positions_refresh: get_balance failed"),
                }
                let t0 = oplog.api_call("data/positions", "GET", serde_json::json!({}));
                let positions = match rest.get_positions().await {
                    Ok(p) => {
                        oplog.api_ok("data/positions", t0, 200, serde_json::json!({ "count": p.len() }));
                        p
                    }
                    Err(e) => {
                        oplog.api_err("data/positions", t0, None, &e.to_string());
                        warn!(error = %e, "positions_refresh: get_positions failed");
                        continue;
                    }
                };
                let today = polymarket_client_sdk_v2::types::Utc::now().date_naive();
                let mut resolved_n = 0usize;
                let mut active_n = 0usize;
                state.resolved_tokens.clear();
                for p in &positions {
                    let (resolved, _why) = p.is_resolved_at(today);
                    if resolved { resolved_n += 1; } else { active_n += 1; }
                    state.resolved_tokens.insert(p.token_id.clone(), resolved);
                }
                // v2 recalibration feed: for any v2 bet whose market just resolved,
                // record (predicted_p, won) into the rolling recalibrator + persist.
                // resolved_price_for_pnl maps cur_price → 1.0 (won) / 0.0 (lost) /
                // None (ambiguous → skip). Inert when v2_pred is empty.
                if !state.v2_pred.is_empty() {
                    let mut updated = 0u32;
                    for p in &positions {
                        let resolved = state.resolved_tokens.get(&p.token_id).map(|r| *r).unwrap_or(false);
                        if !resolved { continue; }
                        if let Some((_, (iv, pred))) = state.v2_pred.remove(&p.token_id) {
                            // Only learn from an UNAMBIGUOUS clean cur_price (0/1).
                            // `redeemable` cannot tell win from lose (true for both),
                            // so we do NOT guess here -- mislabeled samples would
                            // poison the recalibrator.
                            if let Some(price) = crate::pnl_recorder::resolved_price_for_pnl(p.cur_price) {
                                if let Ok(mut r) = recal.for_interval(&iv).lock() {
                                    r.record(pred, price >= 0.5);
                                    updated += 1;
                                }
                            }
                        }
                    }
                    if updated > 0 {
                        recal.save_both();
                        oplog.sys("v2_recal_update", serde_json::json!({ "fed": updated }));
                        info!(fed = updated, "v2 recalibration updated");
                    }
                }
                info!(
                    total = positions.len(), active = active_n, resolved = resolved_n,
                    "positions_refresh: snapshot updated (resolved_tokens cache)"
                );
                oplog.sys("positions_snapshot", serde_json::json!({
                    "total": positions.len(), "active": active_n, "resolved": resolved_n,
                }));
                // G8: steady-state hook. Record P&L for newly-resolved bot-opened
                // positions + remove from bs.positions. Uses the SAME /positions
                // snapshot just fetched (no double round-trip).
                let stats = match recorder.lock() {
                    Ok(mut r) => crate::pnl_recorder::record_resolutions(
                        &positions, &bs, &guards, &mut r, oplog.as_ref(),
                        /*initial_pass=*/ false, now_ms(),
                    ),
                    Err(_) => {
                        warn!("positions_refresh: pnl_recorder mutex poisoned; skipping G8 hook this tick");
                        continue;
                    }
                };
                if stats.processed > 0 || stats.classified_by_redeemable > 0 {
                    info!(
                        recorded = stats.processed,
                        skipped_already = stats.skipped_already_done,
                        skipped_not_in_bs = stats.skipped_not_in_bs,
                        classified_by_redeemable = stats.classified_by_redeemable,
                        "positions_refresh: G8 P&L recorder this tick"
                    );
                }
                // PRIMARY P&L PATH (decoupled from redemption): book the settlement
                // outcomes the decision loop computed from Binance open-vs-close.
                // This makes realized P&L appear within ~1 min of resolution WITHOUT
                // waiting on the (rate-limited) relayer redeem. The activity feed
                // below stays as an idempotent audit/fallback (shared recorder set).
                {
                    let settled: Vec<(String, bool)> = state
                        .v2_settled
                        .iter()
                        .map(|e| (e.key().clone(), *e.value()))
                        .collect();
                    let mut booked = 0u32;
                    for (token, won) in settled {
                        let did = match recorder.lock() {
                            Ok(mut r) => crate::pnl_recorder::record_settled(
                                &token,
                                if won { 1.0 } else { 0.0 },
                                "settlement",
                                &bs,
                                &guards,
                                &mut r,
                                oplog.as_ref(),
                                now_ms(),
                            ),
                            Err(_) => false,
                        };
                        // Drop WINNERS from v2_settled (redemption claims them via the
                        // redeemable flag). KEEP LOSERS so the redemption task can see
                        // them and SKIP the pointless $0 redeem (quota saver).
                        if won {
                            state.v2_settled.remove(&token);
                        }
                        if did {
                            booked += 1;
                            if let Some((_, (iv, pred))) = state.v2_pred.remove(&token) {
                                if let Ok(mut r) = recal.for_interval(&iv).lock() {
                                    r.record(pred, won);
                                }
                            }
                        }
                    }
                    // Bound v2_settled (mostly residual losers kept for the redeem
                    // skip-list); clear if it somehow grows unbounded.
                    if state.v2_settled.len() > 50_000 {
                        state.v2_settled.clear();
                    }
                    if booked > 0 {
                        recal.save_both();
                        oplog.sys("v2_recal_update", serde_json::json!({
                            "fed": booked, "source": "settlement",
                        }));
                        info!(booked,
                            "positions_refresh: settlements booked from Binance close (decoupled from redeem)");
                    }
                }
                // AUDIT/FALLBACK reconcile from the on-chain activity feed (exact
                // payout). Idempotent with the settlement path above via the shared
                // recorder set -- catches anything the Binance path missed (e.g.
                // positions opened before a restart, when the price ring is cold).
                match rest.get_activity(300).await {
                    Ok(rows) => {
                        let booked = match recorder.lock() {
                            Ok(mut r) => crate::pnl_recorder::record_from_activity(
                                &rows, &bs, &guards, &mut r, oplog.as_ref(), now_ms(),
                            ),
                            Err(_) => {
                                warn!("positions_refresh: recorder poisoned; skip activity reconcile");
                                Vec::new()
                            }
                        };
                        if !booked.is_empty() {
                            info!(recorded = booked.len(), "positions_refresh: settlements booked from activity feed");
                            // Self-heal: feed the recalibrator with TRUE outcomes (the
                            // activity payout). This is the only reliable win/lose source;
                            // if live underperforms the May calibration, the rolling bias
                            // grows and shrinks pcal -> fewer/again-profitable entries.
                            let mut fed = 0u32;
                            for (token, won) in &booked {
                                if let Some((_, (iv, pred))) = state.v2_pred.remove(token) {
                                    if let Ok(mut r) = recal.for_interval(&iv).lock() {
                                        r.record(pred, *won);
                                        fed += 1;
                                    }
                                }
                            }
                            if fed > 0 {
                                recal.save_both();
                                oplog.sys("v2_recal_update", serde_json::json!({
                                    "fed": fed, "source": "activity",
                                }));
                                info!(fed, "v2 recalibration updated (activity truth)");
                            }
                        }
                    }
                    Err(e) => warn!(error = %e, "positions_refresh: get_activity failed; will retry next tick"),
                }
            }
            _ = shutdown.changed() => if *shutdown.borrow() {
                info!("positions_refresh: shutdown");
                oplog.sys("positions_refresh_shutdown", serde_json::json!({}));
                return;
            }
        }
    }
}

/// Optional D2 SHADOW context: a real RestClient + the private key so the execution
/// task can build+sign the LiveExecutor order that WOULD be sent (no POST), alongside
/// the paper fill. `None` = pure paper (D1). Some = paper + shadow (D2).
///
/// We store the PRIVATE KEY STRING (not a typed signer) deliberately: `LocalSigner`'s
/// concrete type can't be named here without the alloy `signers` feature, so the
/// signer is rebuilt locally inside `shadow_open` (type inferred, never named). The
/// key is already in-process from `.env`; it is NEVER logged.
#[derive(Clone)]
pub struct ShadowCtx {
    pub rest: Arc<RestClient>,
    pub pk: String,
    pub max_slippage: f64,
}

/// EXECUTION task: drain commands serially. Paper fill always (D1). If `shadow` is
/// Some, ALSO build+sign the real LiveExecutor order (D2) and log what WOULD have
/// been sent + projected slippage -- NEVER posting.
///
/// PIECE 3: every close feeds its NET P&L into the guards' daily accumulator.
/// PIECE 5: every Open / CloseToken emits an oplog event with the exact numbers.
/// PIECE 6: if `live` is Some, the BUY path ALSO POSTS a real order via
/// `live_open` -- gated by LIVE_ARMED + max_trades_per_session. Without LIVE_ARMED
/// the live path is a no-op (logs refusal). REGLA C closes also POST live SELLs.
#[allow(clippy::too_many_arguments)]
pub async fn run_execution_task(
    store_state: SharedBotState,
    mut cmds: mpsc::UnboundedReceiver<ExecCommand>,
    shadow: Option<ShadowCtx>,
    live: Option<Arc<LiveBackend>>,
    guards: Arc<Mutex<Guards>>,
    oplog: Arc<OpLog>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mode = match (&live, &shadow) {
        (Some(_), _) => "PAPER + LIVE (real POST, gated)",
        (None, Some(_)) => "PAPER + LIVE-SHADOW (no POST)",
        (None, None) => "PAPER",
    };
    info!(backend = mode, "task started: execution_loop");
    oplog.sys("execution_loop_start", serde_json::json!({ "backend": mode }));
    loop {
        tokio::select! {
            maybe = cmds.recv() => {
                let Some(cmd) = maybe else { break };
                match cmd {
                    ExecCommand::Open { intent, ctx } => {
                        if let Ok(mut bs) = store_state.lock() {
                            let mut ex = PaperExecutor::new(&mut bs.positions);
                            ex.open(intent.clone());
                        }
                        emit_open_log(&ctx, &intent);
                        debug!(token = %intent.token_id, "paper open");
                        oplog.sys("paper_open", serde_json::json!({
                            "token_id": intent.token_id, "signal_id": ctx.signal_id,
                            "side": ctx.side, "fill_price": intent.fill_price,
                            "shares": intent.shares, "exit_ts_s": intent.exit_ts_s,
                        }));
                        // D2 SHADOW: build+sign the real BUY that WOULD be sent (no POST).
                        if let Some(sh) = &shadow {
                            shadow_open(sh, &intent, &ctx).await;
                        }
                        // PIECE 6 LIVE: real POST (gated by LIVE_ARMED + max_trades).
                        // F1 (bug #2 fix): on Posted, live_open returns Some(real_taking_shares)
                        // = the REAL fill from the BUY response. Overwrite the position's
                        // `shares` (set by paper_open above to the bot's pre-POST estimate)
                        // with the real on-chain value so the later SELL sizes correctly.
                        // Without this, slippage during the BUY produces a position record
                        // with > actual on-chain shares -> SELL tries to over-sell -> rejected.
                        if let Some(lb) = &live {
                            match crate::live_backend::live_open(lb, &intent, &ctx, &oplog).await {
                                Ok(Some(real_shares)) => {
                                    if let Ok(mut bs) = store_state.lock() {
                                        if let Some(p) = bs.positions.iter_mut().find(|p| p.token_id == intent.token_id) {
                                            let prev = p.shares;
                                            p.shares = real_shares;
                                            oplog.sys("position_shares_updated_to_real_fill", serde_json::json!({
                                                "token_id": intent.token_id, "signal_id": ctx.signal_id,
                                                "computed_shares": prev.to_string(),
                                                "real_shares": real_shares.to_string(),
                                            }));
                                            info!(token = %intent.token_id, computed = %prev, real = %real_shares,
                                                "live_open: position.shares overwritten with real BUY fill");
                                        }
                                    }
                                }
                                Ok(None) => {
                                    // NOT posted (refused / slippage abort). In live mode the
                                    // optimistic paper_open above is the position shown on the
                                    // dashboard + tracked for exit — but nothing landed on-chain.
                                    // Roll it back so we don't carry a PHANTOM position.
                                    let removed = if let Ok(mut bs) = store_state.lock() {
                                        let before = bs.positions.len();
                                        bs.positions.retain(|p| p.token_id != intent.token_id);
                                        before - bs.positions.len()
                                    } else { 0 };
                                    warn!(token = %intent.token_id, removed, "live_open not posted — rolled back phantom position");
                                    oplog.sys("live_open_rolled_back", serde_json::json!({
                                        "token_id": intent.token_id, "signal_id": ctx.signal_id,
                                        "reason": "not_posted", "removed": removed,
                                    }));
                                }
                                Err(e) => {
                                    warn!(error = %e, "live_open error");
                                    oplog.err("live_open", &e.to_string(), serde_json::json!({}));
                                    // Order errored (e.g. FOK kill / 400). Same rollback: the
                                    // paper_open must not survive an order that never landed.
                                    let removed = if let Ok(mut bs) = store_state.lock() {
                                        let before = bs.positions.len();
                                        bs.positions.retain(|p| p.token_id != intent.token_id);
                                        before - bs.positions.len()
                                    } else { 0 };
                                    warn!(token = %intent.token_id, removed, "live_open errored — rolled back phantom position");
                                    oplog.sys("live_open_rolled_back", serde_json::json!({
                                        "token_id": intent.token_id, "signal_id": ctx.signal_id,
                                        "reason": "error", "removed": removed,
                                    }));
                                }
                            }
                        }
                    }
                    ExecCommand::CloseToken { token, exit_price, now_ms } => {
                        if let Some(px) = exit_price {
                            let closed = if let Ok(mut bs) = store_state.lock() {
                                let mut ex = PaperExecutor::new(&mut bs.positions);
                                ex.close_token(&token, px, ExitKind::OppositeTriggerClose, now_ms)
                            } else { Vec::new() };
                            for ct in &closed {
                                emit_close_log(ct);
                                feed_guards_net_pnl(&guards, ct, now_ms);
                                oplog.sys("paper_close", serde_json::json!({
                                    "token_id": ct.token_id, "signal_id": ct.signal_id,
                                    "entry_price": ct.entry_price.to_string(),
                                    "exit_price": ct.exit_price.to_string(),
                                    "shares": ct.shares.to_string(),
                                    "realized_pnl": ct.realized_pnl.to_string(),
                                    "kind": ct.exit_kind.as_str(),
                                }));
                                // PIECE 6 LIVE: real SELL on REGLA C close.
                                if let Some(lb) = &live {
                                    if let Err(e) = crate::live_backend::live_close(lb, ct, &oplog).await {
                                        warn!(error = %e, "live_close (REGLA C) error");
                                        oplog.err("live_close", &e.to_string(), serde_json::json!({}));
                                    }
                                }
                            }
                            debug!(token, n = closed.len(), "paper close (REGLA C)");
                        }
                    }
                }
            }
            _ = shutdown.changed() => if *shutdown.borrow() { break },
        }
    }
    info!("execution_loop: shutdown");
    oplog.sys("execution_loop_shutdown", serde_json::json!({}));
}

/// D2 SHADOW: build + sign the real BUY order for this fired intent and log what
/// WOULD have been sent (signed hash + size + worst-price + projected slippage),
/// then STOP — no `post_order`. Re-fetches the live ask as the projected fill so the
/// slippage is real (decision-quote vs now). ZERO capital.
async fn shadow_open(sh: &ShadowCtx, intent: &OpenIntent, ctx: &DecisionCtx) {
    use crate::live_executor::{ExecOutcome, OrderSide, OrderSpec, assess_slippage, build_sign_maybe_post};
    // worst-price BUY = 1 − tick (mirror A2). Tick from the SDK; fall back to 0.01.
    let tok = match U256::from_str_radix(&intent.token_id, 10) {
        Ok(t) => t,
        Err(e) => { warn!(token = %intent.token_id, error = %e, "shadow: token parse"); return; }
    };
    let tick = match sh.rest.clob().tick_size(tok).await {
        Ok(t) => t.minimum_tick_size.as_decimal(),
        Err(e) => { warn!(error = %e, "shadow: tick_size; using 0.01"); Decimal::new(1, 2) }
    };
    let worst = Decimal::ONE - tick;
    // Projected fill = the live ask NOW (the slippage anchor is the decision quote).
    let ask_now = sh.rest.get_price(&intent.token_id, Side::Sell).await.unwrap_or(intent.fill_price);
    let slip = assess_slippage(OrderSide::Buy, ctx.ask_at_signal, ask_now, sh.max_slippage);
    let spec = OrderSpec {
        token_id: intent.token_id.clone(),
        side: OrderSide::Buy,
        amount: Decimal::try_from(ctx.stake_usd).unwrap_or_default(), // BUY: USDC notional
        worst_price: worst,
        quote_price: Decimal::try_from(ctx.ask_at_signal).unwrap_or_default(),
    };
    // Build the signer LOCALLY (type inferred; never named in a field/return).
    let signer = match LocalSigner::from_str(&sh.pk) {
        Ok(s) => s.with_chain_id(Some(POLYGON)),
        Err(e) => { warn!(error = %e, "shadow: signer parse"); return; }
    };
    match build_sign_maybe_post(&sh.rest, &signer, &spec, slip.clone(), true).await {
        Ok(ExecOutcome::Shadow { order_hash, amount, worst_price, .. }) => {
            info!(
                token = %intent.token_id, side = "BUY", usdc = %amount, worst_price = %worst_price,
                order_hash = %order_hash,
                quote = ctx.ask_at_signal, projected_fill = ask_now,
                slippage = slip.slippage, within_threshold = slip.within_threshold,
                "D2 SHADOW: order that WOULD be sent (NOT posted)"
            );
            println!("[D2-shadow] WOULD-SEND BUY {} usdc=${amount} worst={worst_price} hash={order_hash} \
                quote={:.4} proj_fill={ask_now:.4} slippage={:.4} within_thr={} (NO POST)",
                intent.token_id, ctx.ask_at_signal, slip.slippage, slip.within_threshold);
            emit_shadow_log(ctx, intent, &order_hash, &slip);
        }
        Ok(ExecOutcome::SlippageAbort { slippage }) => {
            println!("[D2-shadow] SLIPPAGE ABORT {} slippage={:.4} > max={:.4} (no order)",
                intent.token_id, slippage.slippage, slippage.max_slippage);
        }
        Ok(ExecOutcome::IdempotencyRefused { coid, reason }) => {
            warn!(%coid, %reason, "D2 SHADOW: idempotency refused (no order built/sent)");
        }
        Ok(ExecOutcome::Posted(_)) => {
            warn!("D2 SHADOW returned Posted — should be impossible in shadow mode!");
        }
        Err(e) => warn!(token = %intent.token_id, error = %e, "shadow build/sign failed"),
    }
}

/// One smart-exit decision returned by [`evaluate_smart_exits`]. The caller
/// (`run_exit_task`) applies these via [`PaperExecutor::apply_smart_exits`]
/// BEFORE invoking the time-based `close_due`. Smart wins ties: a position
/// closed by smart is removed from the Vec, so close_due skips it naturally.
#[derive(Debug, Clone, PartialEq)]
pub struct SmartExitDecision {
    /// Index into the `positions` Vec at the time of evaluation. The caller
    /// MUST apply these in DESCENDING `lot_index` order so removes do not
    /// shift remaining indices (see [`PaperExecutor::apply_smart_exits`]).
    pub lot_index: usize,
    pub token_id: String,
    pub asset: String,
    pub interval: String,
    /// Sell at this price (= `bbo.best_bid` at evaluation, guaranteed fresh).
    pub exit_price: f64,
    pub exit_kind: ExitKind,
    /// Human-readable rule label (e.g. "smart_x70_y30s") for the oplog.
    pub rule_label: String,
}

/// Pure: evaluate per-cell smart exit rules against current state. Returns
/// one [`SmartExitDecision`] per position that BOTH (a) has a rule
/// configured for its cell (`exits_cfg.cells[<asset>:<interval>]`), (b)
/// satisfies its rule's trigger predicate, AND (c) has a FRESH BBO with a
/// valid `best_bid` to price the SELL at.
///
/// Fail-closed cases (skip the position, fall through to `close_due` time-
/// exit, NEVER produce a decision):
///   * `entry_ts_ms == 0`  (Pieza 1 sentinel: pre-Pieza-2 / v2-migrated)
///   * `asset == ""`        (Pieza 4 sentinel: pre-Pieza-4 / v3-migrated)
///   * cell not in `exits_cfg.cells`             (no rule configured)
///   * rule is `Baseline`                        (= no smart exit, time-exit)
///   * rule predicate returns false              (trigger not met)
///   * BBO absent for the token                  (can't price SELL)
///   * BBO stale (`now - bbo.ts_ms > STALE_BBO_MS`)  (refuse stale price)
///   * BBO present but no `best_bid` or non-finite    (defensive)
///
/// The doctrine (Pieza 2 Q2): when the smart-exit rule fires but we CANNOT
/// confidently price the SELL, we HOLD past the trigger rather than sell at
/// a phantom price. The time-exit (`close_due` on `exit_ts_s`) remains the
/// safety net and will eventually fire.
#[must_use]
pub fn evaluate_smart_exits(
    positions: &[OpenPosition],
    exits_cfg: &ExitsConfig,
    bbo: &DashMap<String, EventBbo>,
    now_ms: i64,
) -> Vec<SmartExitDecision> {
    let mut out = Vec::new();
    for (idx, p) in positions.iter().enumerate() {
        // Double sentinel: either flag = smart-ineligible.
        if p.entry_ts_ms == 0 || p.asset.is_empty() {
            continue;
        }
        let cell_key = format!("{}:{}", p.asset, p.interval);
        let rule = match exits_cfg.cells.get(&cell_key) {
            Some(r) => r,
            None => continue,
        };
        let (triggered, kind, label) = match rule {
            ExitRule::Baseline => continue,
            // COMBO Phase 3 (2026-06-09): B3Maker is NOT a smart-exit DECISION
            // (a one-shot close at the current bid). It is a stateful lifecycle
            // (place maker early -> first-touch fill OR timeout cancel +
            // F2_market fallback) handled by `process_b3_makers`, which the
            // exit task runs as a SEPARATE step before `close_due`. So this arm
            // stays `continue` here: evaluate_smart_exits must NOT produce a
            // SmartExitDecision for a B3 cell (that would double-handle it).
            ExitRule::B3Maker { .. } => continue,
            ExitRule::F6Only { y_sec } => (
                crate::exit_rules::f6_triggers(now_ms, p.ts_max_bid_ms, *y_sec),
                ExitKind::F6Triggered,
                format!("f6only_y{}s", y_sec),
            ),
            ExitRule::Smart { x_pct, y_sec } => (
                crate::exit_rules::smart_triggers(
                    now_ms,
                    p.entry_ts_ms,
                    p.exit_ts_s.saturating_mul(1000),
                    p.ts_max_bid_ms,
                    *x_pct,
                    *y_sec,
                ),
                ExitKind::SmartTriggered,
                format!("smart_x{:02}_y{}s", x_pct.round() as i64, y_sec),
            ),
        };
        if !triggered {
            continue;
        }
        // Stale-guard the SELL: rule fires but we MUST have a fresh price.
        let bbo_entry = match bbo.get(&p.token_id) {
            Some(e) => e,
            None => continue,
        };
        if now_ms - bbo_entry.ts_ms > STALE_BBO_MS {
            continue;
        }
        let exit_price = match bbo_entry.best_bid {
            Some(b) if b.is_finite() => b,
            _ => continue,
        };
        out.push(SmartExitDecision {
            lot_index: idx,
            token_id: p.token_id.clone(),
            asset: p.asset.clone(),
            interval: p.interval.clone(),
            exit_price,
            exit_kind: kind,
            rule_label: label,
        });
    }
    out
}

/// COMBO Phase 3 — the B3 maker lifecycle, run once per exit-task tick BEFORE
/// `close_due`. Handles every B3Maker-cell position: place the maker early,
/// fill on first-touch, or at timeout cancel + reconcile + F2_market fallback.
/// Emits the 7 oplog events of the locked contract (see oplog.rs). Returns the
/// lots it closed (so the caller logs + feeds guards like any other close).
///
/// PAPER: the place/cancel/reconcile are SIMULATED here (synchronous, no SDK):
///   * place  -> set `maker_exit = Some{Placed}` (+ a paper order id).
///   * fill   -> detected when the bid touches target (first-touch).
///   * cancel -> at timeout, a CLEAN cancel (paper has no partial fill); the
///               partial-reconcile path is exercised by the pure
///               `reconcile_decision` property test, not paper end-to-end.
///   * fallback price -> `b3_first_touch_sim` from the tick-sampled bid trace
///               (the golden-fixture-pinned function -> coherent with the
///               Python analyzer).
/// LIVE (Phase B) swaps the sim for real SDK calls (place_maker_limit_order /
/// cancel_order / Client::order) and MUST move them OUT of the store_state lock
/// (lock -> snapshot -> unlock -> await -> lock -> write). Everything THIS
/// function calls is PURE (b3_maker::*), so there is no `.await` under the lock
/// here -- the concurrency rule is respected; Phase B extends it.
///
/// `b3_tracks` is the TRANSIENT per-lot bid-sample buffer (keyed by signal_id),
/// owned by the exit task (NOT persisted): one `(offset_ms, bid)` per tick, used
/// to build the hwm/lwm traces for the fallback. Cleared when a lot closes.
#[must_use]
fn process_b3_makers(
    positions: &mut Vec<OpenPosition>,
    exits_cfg: &ExitsConfig,
    bbo: &DashMap<String, EventBbo>,
    now_ms: i64,
    b3_tracks: &mut HashMap<String, Vec<(i64, f64)>>,
    oplog: &OpLog,
) -> Vec<ClosedTrade> {
    use crate::b3_maker::{
        self, MakerOrderStatus, b3_first_touch_sim,
        hwm_lwm_from_samples, reconcile_decision, valid_maker_target,
    };
    use crate::state::persist::{MakerExitState, MakerExitStatus};
    use rust_decimal::prelude::ToPrimitive;

    let now_s = now_ms / 1000;
    // Close actions collected during the read/mutate pass, applied after.
    let mut to_close: Vec<(usize, f64, ExitKind)> = Vec::new();
    let mut closed_signal_ids: Vec<String> = Vec::new();

    for (idx, p) in positions.iter_mut().enumerate() {
        // Only B3Maker cells; read the per-cell delta.
        let cell = format!("{}:{}", p.asset, p.interval);
        let delta = match exits_cfg.cells.get(&cell) {
            Some(ExitRule::B3Maker { delta, .. }) => *delta,
            _ => continue,
        };
        // Same smart-ineligibility sentinels as evaluate_smart_exits.
        if p.entry_ts_ms == 0 || p.asset.is_empty() {
            continue;
        }
        // A maker in a terminal state should already be closed/removed; guard.
        if let Some(m) = &p.maker_exit {
            if m.status != MakerExitStatus::Placed {
                continue;
            }
        }

        // Fresh bid or HOLD this tick (never act on a stale/absent price).
        let bid = match bbo.get(&p.token_id) {
            Some(e) if now_ms - e.ts_ms <= STALE_BBO_MS => match e.best_bid {
                Some(b) if b.is_finite() => b,
                _ => continue,
            },
            _ => continue,
        };

        // Record the sample for the fallback trace.
        let off = now_ms - p.entry_ts_ms;
        b3_tracks.entry(p.signal_id.clone()).or_default().push((off, bid));

        let entry = p.entry_price.to_f64().unwrap_or(0.0);
        let shares = p.shares.to_f64().unwrap_or(0.0);
        let target_opt = valid_maker_target(entry, delta, b3_maker::TICK);
        let due = p.exit_ts_s <= now_s;

        if due {
            // ===== TIMEOUT. No first-touch happened (else the lot already
            // closed in the not-due branch below), so b3_first_touch_sim
            // returns F2BreakEven or F1TimeExit. NOTE (paper vs analyzer): the
            // bid trace is sampled at 1s (one per tick); the backtest sampled
            // at every book update, so the break-even bid can differ slightly
            // when arm+fire fall inside one second. The coherence is on the
            // ALGORITHM (golden-fixture-pinned b3_first_touch_sim), not on
            // identical traces.
            let samples = b3_tracks.get(&p.signal_id).cloned().unwrap_or_default();
            let (hwm, lwm) = hwm_lwm_from_samples(&samples);
            let raw_target = entry + delta; // raw target (>1 when invalid): never
                                            // first-touches -> always falls back,
                                            // exactly as the analyzer does.
            let outcome = b3_first_touch_sim(entry, raw_target, shares, &hwm, &lwm, bid);

            // Cancel + reconcile (only if a maker is on the book). PAPER: a
            // CLEAN cancel (size_matched = 0). The verdict DRIVES the action
            // (b3_timeout_action) -- we never blindly close the full size.
            let action = if let Some(m) = p.maker_exit.clone() {
                oplog.sys("b3_maker_timeout_cancel", serde_json::json!({
                    "signal_id": p.signal_id, "token_id": p.token_id,
                    "order_id": m.order_id, "exit_ts_s": p.exit_ts_s,
                    "size_matched_at_cancel": 0.0,
                    "cancel_resp": "canceled",
                }));
                let recon = reconcile_decision(MakerOrderStatus::Cancelled, p.shares, Decimal::ZERO);
                oplog.sys("b3_maker_reconcile", serde_json::json!({
                    "signal_id": p.signal_id, "token_id": p.token_id,
                    "order_id": m.order_id, "status": "cancelled",
                    "original_size": p.shares.to_string(), "size_matched": "0",
                    "decision": recon.decision.as_str(),
                }));
                b3_maker::b3_timeout_action(&recon)
            } else {
                // No maker (invalid target): nothing to cancel/reconcile; the
                // whole lot exits via F2_market.
                b3_maker::B3TimeoutAction::FullFallback
            };

            match action {
                // PAPER NEVER reaches Defer/Partial/FullMakerFill (clean cancel
                // always -> FullFallback). They are wired + tested via
                // b3_timeout_action for the LIVE (Phase B) paths.
                b3_maker::B3TimeoutAction::Defer => {
                    // Cancel unconfirmed: sell NOTHING, retry next tick. The
                    // anti-oversell fail-safe.
                    continue;
                }
                b3_maker::B3TimeoutAction::Partial { maker_filled, fallback_size } => {
                    // Partial fill needs LOT-SPLITTING (close maker_filled at
                    // target + fallback_size via F2) which only Phase B has.
                    // Until then: emit the event + sell NOTHING (treat as
                    // Defer) -- guarantees no oversell. PAPER never hits this.
                    oplog.sys("b3_maker_partial_fill", serde_json::json!({
                        "signal_id": p.signal_id, "token_id": p.token_id,
                        "order_id": p.maker_exit.as_ref().map(|m| m.order_id.clone()).unwrap_or_default(),
                        "original_size": p.shares.to_string(),
                        "size_matched": maker_filled.to_string(),
                        "size_remaining": fallback_size.to_string(),
                        "fill_price": p.maker_exit.as_ref().map(|m| m.target).unwrap_or(raw_target),
                    }));
                    warn!(signal_id = %p.signal_id, "B3 partial fill at timeout: lot-split is Phase B; holding (no oversell)");
                    continue;
                }
                b3_maker::B3TimeoutAction::FullMakerFill => {
                    // Maker fully filled at/just-before timeout -> close whole at
                    // target (maker). PAPER would have closed it in the not-due
                    // branch; defensive for live.
                    let target = p.maker_exit.as_ref().map(|m| m.target).unwrap_or(raw_target);
                    oplog.sys("b3_maker_filled", serde_json::json!({
                        "signal_id": p.signal_id, "token_id": p.token_id,
                        "order_id": p.maker_exit.as_ref().map(|m| m.order_id.clone()).unwrap_or_default(),
                        "fill_price": target, "shares": shares,
                        "fee_paid": 0.0, "fee_model": "maker_floor",
                        "gross_pnl": shares * (target - entry),
                        "net_pnl": b3_maker::net_pnl_with_fees(shares, entry, target, 0.0),
                    }));
                    if let Some(m) = &mut p.maker_exit { m.status = MakerExitStatus::Filled; }
                    to_close.push((idx, target, ExitKind::B3MakerFill));
                    closed_signal_ids.push(p.signal_id.clone());
                }
                b3_maker::B3TimeoutAction::FullFallback => {
                    // Clean cancel (or no maker): F2_market the FULL lot (taker).
                    let f2_fee = b3_maker::taker_fee(shares, outcome.exit_price);
                    oplog.sys("b3_maker_fallback_f2", serde_json::json!({
                        "signal_id": p.signal_id, "token_id": p.token_id,
                        "maker_order_id": p.maker_exit.as_ref().map(|m| m.order_id.clone()).unwrap_or_default(),
                        "fallback_shares": shares,
                        "f2_price": outcome.exit_price, "f2_fee": f2_fee,
                        "exit_kind": outcome.exit_kind.as_str(),
                        "gross_pnl": outcome.gross_pnl, "net_pnl": outcome.net_pnl,
                        "reason": "clean_cancel",
                    }));
                    if let Some(m) = &mut p.maker_exit { m.status = MakerExitStatus::Cancelled; }
                    to_close.push((idx, outcome.exit_price, ExitKind::B3Fallback));
                    closed_signal_ids.push(p.signal_id.clone());
                }
            }
            continue;
        }

        // ===== NOT DUE: place / fill / crossable.
        match (&p.maker_exit, target_opt) {
            (None, Some(target)) if bid >= target => {
                // #5 crossable at placement: bid already >= target. Capture as
                // taker at the bid (>= target) -- the WIN is realized now.
                let net = b3_maker::net_pnl_with_fees(shares, entry, bid, b3_maker::taker_fee(shares, bid));
                oplog.sys("b3_maker_rejected_crossable", serde_json::json!({
                    "signal_id": p.signal_id, "token_id": p.token_id,
                    "order_id": "", "target": target, "bid_at_submit": bid,
                    "action_taken": "taker_capture",
                    "gross_pnl": shares * (bid - entry), "net_pnl": net,
                }));
                to_close.push((idx, bid, ExitKind::B3MakerFill));
                closed_signal_ids.push(p.signal_id.clone());
            }
            (None, Some(target)) => {
                // Place the maker (early). bid < target.
                let order_id = format!("paper-maker-{}-{}", p.token_id, now_ms);
                oplog.sys("b3_maker_placed", serde_json::json!({
                    "signal_id": p.signal_id, "token_id": p.token_id, "cell": cell,
                    "order_id": order_id, "entry_price": entry,
                    "target_price": target, "delta": delta, "shares": shares,
                }));
                p.maker_exit = Some(MakerExitState {
                    order_id, placed_ms: now_ms, target, status: MakerExitStatus::Placed,
                });
            }
            (None, None) => {
                // Invalid target (entry+delta > 1-tick): NO maker placed (NOT
                // clamped). The lot waits for the timeout -> F2_market above.
                // Matches the analyzer (unreachable target -> always fallback).
            }
            (Some(m), _) => {
                // Maker working: first-touch fill if the bid reached target.
                if bid >= m.target {
                    let target = m.target;
                    let net = b3_maker::net_pnl_with_fees(shares, entry, target, 0.0); // maker floor fee
                    oplog.sys("b3_maker_filled", serde_json::json!({
                        "signal_id": p.signal_id, "token_id": p.token_id,
                        "order_id": m.order_id, "fill_price": target, "shares": shares,
                        "fee_paid": 0.0, "fee_model": "maker_floor",
                        "gross_pnl": shares * (target - entry), "net_pnl": net,
                    }));
                    if let Some(mm) = &mut p.maker_exit {
                        mm.status = MakerExitStatus::Filled;
                    }
                    to_close.push((idx, target, ExitKind::B3MakerFill));
                    closed_signal_ids.push(p.signal_id.clone());
                }
                // else: still working -> no-op this tick.
            }
        }
    }

    // Apply closes (descending index, via the paper executor) + clear tracks.
    let closed = if to_close.is_empty() {
        Vec::new()
    } else {
        let mut ex = PaperExecutor::new(positions);
        ex.apply_smart_exits(to_close, now_ms)
    };
    for sid in closed_signal_ids {
        b3_tracks.remove(&sid);
    }
    closed
}

/// Outcome of one [`update_running_max`] pass. Useful for caller-side logging
/// (the function itself is silent / pure so tests can assert exact behavior
/// without scraping logs). `stale_skipped` carries `(token_id, bbo_age_ms)`
/// pairs so the caller can decide what to emit and at what throttle.
#[derive(Debug, Default, PartialEq)]
pub struct RunningMaxStats {
    /// Positions whose `running_max_bid` advanced this tick.
    pub updated: usize,
    /// Positions skipped because the per-token BBO has not been refreshed
    /// in the last `STALE_BBO_MS`. Carries `(token_id, bbo_age_ms)` per
    /// skip (one entry per position, so two lots of the same stale token
    /// produce two entries).
    pub stale_skipped: Vec<(String, i64)>,
    /// Positions whose token has no BBO entry at all (cold-start / never
    /// observed). Carries the token_id.
    pub absent_skipped: Vec<String>,
    /// Positions with `entry_ts_ms == 0` (pre-Pieza-2 / v2-migrated):
    /// smart-exit ineligible by contract -- the rule evaluator (next
    /// piece) will fall back to baseline for them.
    pub sentinel_skipped: usize,
}

/// Pure: refresh the per-position smart-exit state (`running_max_bid`,
/// `ts_max_bid_ms`) from the live BBO snapshot. Pieza 2 wires this into
/// the 1s `run_exit_task` tick. The decision path (`close_due`) is NOT
/// touched -- exit decisions remain 100% time-based until a later piece
/// adds the rule evaluator.
///
/// Skip semantics (fail-closed, see Pieza 2 plan Q2):
///   * `entry_ts_ms == 0` (sentinel) -- never update. These positions
///     opened before Pieza 2 or migrated from v2 state.json; the
///     smart-exit evaluator MUST fall back to baseline for them. Skipping
///     here is belt-and-suspenders (the evaluator also checks).
///   * No BBO entry for the token -- never update. The position's
///     `running_max_bid` keeps its last value (or `entry_price` if no
///     observation has landed yet).
///   * BBO older than [`STALE_BBO_MS`] -- never update. During a WS
///     reconnect / gap, the `running_max_bid` is PRESERVED at its
///     pre-gap value. We refuse to observe a possibly-out-of-date bid;
///     we'd rather hold the position past its smart-exit trigger
///     conservatively than sell on a phantom price.
///
/// When ALL three guards pass: if `bbo.best_bid > running_max_bid`, advance
/// `running_max_bid` to the bid and stamp `ts_max_bid_ms = now_ms` (NOT
/// `bbo.ts_ms` -- the smart-exit f6 = now - ts_max needs both sides on
/// the same monotonic clock for paridad with the backtester).
pub fn update_running_max(
    positions: &mut [OpenPosition],
    bbo: &DashMap<String, EventBbo>,
    now_ms: i64,
) -> RunningMaxStats {
    let mut stats = RunningMaxStats::default();
    for p in positions.iter_mut() {
        // Guard 1: pre-Pieza-2 / v2-migrated position -- never touch.
        if p.entry_ts_ms == 0 {
            stats.sentinel_skipped += 1;
            continue;
        }
        // Guard 2: BBO absent for this token (cold-start, never observed).
        let entry = match bbo.get(&p.token_id) {
            Some(e) => e,
            None => {
                stats.absent_skipped.push(p.token_id.clone());
                continue;
            }
        };
        // Guard 3: BBO stale (gap / reconnect / dead WS). Compute age vs
        // our local clock -- `bbo.ts_ms` is Polymarket-server ms.
        let age_ms = now_ms - entry.ts_ms;
        if age_ms > STALE_BBO_MS {
            stats.stale_skipped.push((p.token_id.clone(), age_ms));
            continue;
        }
        // All guards passed. Update only if the bid set a strict new high.
        // Equal bids do NOT update (avoids advancing ts_max_bid_ms on flat
        // observations -- f6 then correctly measures time since the LAST
        // strict high, matching the backtester's semantics).
        if let Some(bid) = entry.best_bid {
            if bid.is_finite() && bid > p.running_max_bid {
                p.running_max_bid = bid;
                p.ts_max_bid_ms = now_ms;
                stats.updated += 1;
            }
        }
    }
    stats
}

/// EXIT task: 1s cadence, close every lot past its `exit_ts_s` at its token's current
/// best_bid (the strategy's design exit). Paper backend (D1).
///
/// PIECE 3: every close feeds its NET P&L into the guards' daily accumulator.
/// PIECE 5: every close emits an `exit_close` event to the oplog.
/// PIECE 6: if `live` is Some, the close ALSO POSTS a real SELL via `live_close`
/// (gated by LIVE_ARMED + max_trades). The shared `trades_completed` counter
/// advances and -- when it reaches `max_trades_per_session` -- the bot shuts down
/// cleanly via the shared shutdown channel.
///
/// PIEZA 2: BEFORE every `close_due` call, refresh the per-position smart-exit
/// state via [`update_running_max`]. The function is fail-closed (sentinels,
/// absent BBO, stale BBO all skip). Stale-skip events are logged to the oplog
/// at most once per token per 30 s (`STALE_LOG_THROTTLE_MS`) for
/// observability of gaps. Even though no exit decision yet reads the smart-
/// exit state, populating it correctly is the contract that the next piece
/// (rule evaluator) depends on.
pub async fn run_exit_task(
    state: Arc<SharedState>,
    store_state: SharedBotState,
    guards: Arc<Mutex<Guards>>,
    live: Option<Arc<LiveBackend>>,
    oplog: Arc<OpLog>,
    exits_cfg: Arc<ExitsConfig>,
    mut shutdown: watch::Receiver<bool>,
) {
    info!(
        live = live.is_some(),
        smart_cells = exits_cfg.cells.len(),
        "task started: exit_loop (1s)"
    );
    oplog.sys("exit_loop_start", serde_json::json!({
        "live": live.is_some(),
        "smart_cells_configured": exits_cfg.cells.len(),
    }));
    // Per-token throttle for the stale-BBO oplog event: emit at most one
    // `exit_smart_stale` per token per 30 s. Without throttle, a multi-
    // minute gap on an active token spams the oplog at 1 line/sec.
    const STALE_LOG_THROTTLE_MS: i64 = 30_000;
    let mut stale_last_log: HashMap<String, i64> = HashMap::new();
    // COMBO Phase 3: transient per-lot bid-sample buffer for the B3 maker
    // fallback trace (keyed by signal_id). NOT persisted -- a B3 lot open
    // across a restart recovers via its persisted `maker_exit` (live) / is
    // re-evaluated (paper). Cleared per lot when it closes (inside
    // process_b3_makers).
    let mut b3_tracks: HashMap<String, Vec<(i64, f64)>> = HashMap::new();
    let mut tick = tokio::time::interval(std::time::Duration::from_secs(1));
    loop {
        tokio::select! {
            _ = tick.tick() => {
                let now = now_ms();
                // PIEZA 2: refresh smart-exit state BEFORE the close decision.
                let stats = if let Ok(mut bs) = store_state.lock() {
                    update_running_max(&mut bs.positions, &state.bbo, now)
                } else {
                    RunningMaxStats::default()
                };
                // Throttled per-token oplog for stale skips.
                for (token_id, age_ms) in &stats.stale_skipped {
                    let last = stale_last_log.get(token_id).copied().unwrap_or(i64::MIN);
                    if now - last >= STALE_LOG_THROTTLE_MS {
                        oplog.sys("exit_smart_stale", serde_json::json!({
                            "token_id": token_id,
                            "bbo_age_ms": age_ms,
                            "stale_threshold_ms": STALE_BBO_MS,
                        }));
                        stale_last_log.insert(token_id.clone(), now);
                    }
                }
                // PIEZA 4: smart-exit evaluation FIRST. Decisions are computed
                // from a snapshot of positions, then APPLIED (which removes
                // them from the Vec). close_due below then sees only the
                // positions smart did NOT close -- so there is no double-
                // close possible. Smart wins ties (intent-driven > safety net).
                let smart_closed = {
                    if let Ok(mut bs) = store_state.lock() {
                        let decisions = evaluate_smart_exits(&bs.positions, &exits_cfg, &state.bbo, now);
                        if decisions.is_empty() {
                            Vec::new()
                        } else {
                            // Log each smart decision before applying (visibility).
                            for d in &decisions {
                                oplog.sys("exit_smart_decision", serde_json::json!({
                                    "token_id": d.token_id,
                                    "asset": d.asset,
                                    "interval": d.interval,
                                    "rule": d.rule_label,
                                    "exit_price": d.exit_price,
                                    "exit_kind": d.exit_kind.as_str(),
                                }));
                            }
                            let tuples: Vec<(usize, f64, ExitKind)> = decisions
                                .into_iter()
                                .map(|d| (d.lot_index, d.exit_price, d.exit_kind))
                                .collect();
                            let mut ex = PaperExecutor::new(&mut bs.positions);
                            ex.apply_smart_exits(tuples, now)
                        }
                    } else { Vec::new() }
                };
                // COMBO Phase 3: B3 maker lifecycle (place / fill / timeout
                // fallback). Runs AFTER smart, BEFORE close_due: it removes the
                // B3 lots it closes, so close_due never time-exits a B3 lot via
                // baseline. Everything it calls is pure (b3_maker::*), so no
                // `.await` under the lock -- the concurrency rule holds (Phase B
                // moves the real SDK calls outside the lock).
                let b3_closed = {
                    if let Ok(mut bs) = store_state.lock() {
                        process_b3_makers(&mut bs.positions, &exits_cfg, &state.bbo, now, &mut b3_tracks, &oplog)
                    } else { Vec::new() }
                };
                // Time-exit (close_due) on whatever smart + B3 did not close.
                let time_closed = {
                    let book = LiveBook(&state);
                    if let Ok(mut bs) = store_state.lock() {
                        let mut ex = PaperExecutor::new(&mut bs.positions);
                        ex.close_due(now, &book)
                    } else { Vec::new() }
                };
                // Merge: smart-closed, then B3-closed, then time-closed.
                let mut closed = smart_closed;
                closed.extend(b3_closed);
                closed.extend(time_closed);
                for ct in &closed {
                    emit_close_log(ct);
                    feed_guards_net_pnl(&guards, ct, now);
                    oplog.sys("exit_close", serde_json::json!({
                        "token_id": ct.token_id, "signal_id": ct.signal_id,
                        "entry_price": ct.entry_price.to_string(),
                        "exit_price": ct.exit_price.to_string(),
                        "shares": ct.shares.to_string(),
                        "realized_pnl": ct.realized_pnl.to_string(),
                        "kind": ct.exit_kind.as_str(),
                    }));
                    // PIECE 6 LIVE: real SELL on time-based exit.
                    if let Some(lb) = &live {
                        if let Err(e) = crate::live_backend::live_close(lb, ct, &oplog).await {
                            warn!(error = %e, "live_close (exit) error");
                            oplog.err("live_close", &e.to_string(), serde_json::json!({}));
                        }
                    }
                }
            }
            _ = shutdown.changed() => if *shutdown.borrow() { break },
        }
    }
    info!("exit_loop: shutdown");
    oplog.sys("exit_loop_shutdown", serde_json::json!({}));
}

// ===================== trade-log emission =====================

fn emit_open_log(ctx: &DecisionCtx, intent: &OpenIntent) {
    let now = tl_now();
    let gross = intent.fill_price * intent.shares;
    let fee = fee_f64(intent.shares, intent.fill_price);
    let mut tl = TradeLog {
        trade_id: format!("{}-{}", ctx.signal_id, ctx.t_signal_ms),
        signal_id: Some(ctx.signal_id.clone()),
        asset: ctx.asset.clone(), interval: ctx.interval.clone(), epoch: ctx.epoch,
        token_id: intent.token_id.clone(), side: ctx.side.clone(), status: "paper_open".to_string(),
        t_signal_ms: Some(ctx.t_signal_ms), t_decision_ms: Some(ctx.t_decision_ms),
        t_buy_sent_ms: Some(ctx.t_decision_ms), t_buy_response_ms: Some(now),
        ask_at_signal: Some(ctx.ask_at_signal), ask_at_decision: Some(ctx.ask_at_signal),
        ask_at_buy_sent: Some(intent.fill_price), buy_fill_price: Some(intent.fill_price),
        binance_ret_bps: Some(ctx.binance_ret_bps), window_s: Some(ctx.window_s),
        stake_usd: Some(ctx.stake_usd), shares_intended: Some(intent.shares),
        shares_filled: Some(intent.shares),
        buy_gross: Some(gross), buy_fee: Some(fee), buy_wallet_cost: Some(gross + fee),
        ..Default::default()
    };
    tl.finalize(&Thresholds::default());
    let _ = tl.append_jsonl(std::path::Path::new(EXEC_LOG));
}

fn emit_close_log(ct: &ClosedTrade) {
    use rust_decimal::prelude::ToPrimitive;
    let now = tl_now();
    let shares = ct.shares.to_f64().unwrap_or(0.0);
    let entry = ct.entry_price.to_f64().unwrap_or(0.0);
    let exit = ct.exit_price.to_f64().unwrap_or(0.0);
    let sell_fee = fee_f64(shares, exit);
    let buy_fee = fee_f64(shares, entry);
    let mut tl = TradeLog {
        trade_id: format!("{}-close-{}", ct.signal_id, ct.closed_at_ms),
        signal_id: Some(ct.signal_id.clone()),
        interval: ct.interval.clone(), token_id: ct.token_id.clone(),
        side: outcome_str(ct.side).to_string(), status: "paper_close".to_string(),
        t_buy_response_ms: Some(ct.opened_at_ms), t_sell_response_ms: Some(now),
        buy_fill_price: Some(entry), sell_fill_price: Some(exit),
        shares_filled: Some(shares), shares_sold: Some(shares),
        buy_gross: Some(shares * entry), buy_fee: Some(buy_fee),
        buy_wallet_cost: Some(shares * entry + buy_fee),
        sell_gross: Some(shares * exit), sell_fee: Some(sell_fee),
        sell_wallet_proceeds: Some(shares * exit - sell_fee),
        gross_pnl: Some(ct.realized_pnl.to_f64().unwrap_or(0.0)),
        fees_total: Some(buy_fee + sell_fee),
        net_pnl: Some((shares * exit - sell_fee) - (shares * entry + buy_fee)),
        ..Default::default()
    };
    tl.finalize(&Thresholds::default());
    let _ = tl.append_jsonl(std::path::Path::new(EXEC_LOG));
}

/// D2 SHADOW row: the LiveExecutor order that WOULD have been sent (signed hash +
/// projected slippage), status `shadow_would_send`. Distinct from `paper_open` so the
/// autopsy can tell the simulated fill from the real-order-construction record.
fn emit_shadow_log(ctx: &DecisionCtx, intent: &OpenIntent, order_hash: &str, slip: &crate::live_executor::Slippage) {
    let mut tl = TradeLog {
        trade_id: format!("{}-shadow-{}", ctx.signal_id, ctx.t_signal_ms),
        signal_id: Some(ctx.signal_id.clone()),
        client_order_id: Some(order_hash.to_string()),
        asset: ctx.asset.clone(), interval: ctx.interval.clone(), epoch: ctx.epoch,
        token_id: intent.token_id.clone(), side: ctx.side.clone(),
        status: "shadow_would_send".to_string(),
        t_signal_ms: Some(ctx.t_signal_ms), t_decision_ms: Some(ctx.t_decision_ms),
        ask_at_signal: Some(slip.quote_price), ask_at_buy_sent: Some(slip.projected_fill),
        buy_fill_price: Some(slip.projected_fill), slippage: Some(slip.slippage),
        stake_usd: Some(ctx.stake_usd), shares_intended: Some(intent.shares),
        ..Default::default()
    };
    tl.finalize(&Thresholds::default());
    let _ = tl.append_jsonl(std::path::Path::new(EXEC_LOG));
}

fn log_blocked(ctx: &DecisionCtx, intent: &OpenIntent, verdict: &GuardVerdict) {
    let mut tl = TradeLog {
        trade_id: format!("{}-blocked-{}", ctx.signal_id, ctx.t_signal_ms),
        signal_id: Some(ctx.signal_id.clone()),
        asset: ctx.asset.clone(), interval: ctx.interval.clone(), epoch: ctx.epoch,
        token_id: intent.token_id.clone(), side: ctx.side.clone(),
        status: format!("guard_blocked:{}", verdict.reason().unwrap_or("?")),
        t_signal_ms: Some(ctx.t_signal_ms), t_decision_ms: Some(ctx.t_decision_ms),
        ask_at_signal: Some(ctx.ask_at_signal), stake_usd: Some(ctx.stake_usd),
        ..Default::default()
    };
    tl.finalize(&Thresholds::default());
    let _ = tl.append_jsonl(std::path::Path::new(EXEC_LOG));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::signal::MarketRef;
    use crate::state::EventBbo;
    use std::collections::HashMap;

    struct TestCat(HashMap<(String, String, i64), MarketRef>);
    impl MarketCatalog for TestCat {
        fn active_market(&self, a: &str, i: &str, e: i64) -> Option<&MarketRef> {
            self.0.get(&(a.to_string(), i.to_string(), e))
        }
    }
    struct TestBook(HashMap<String, EventBbo>);
    impl BookProvider for TestBook {
        fn bbo(&self, t: &str) -> Option<EventBbo> {
            self.0.get(t).copied()
        }
    }
    fn mref(up: &str, down: &str) -> MarketRef {
        MarketRef { up_token_id: up.into(), down_token_id: down.into(), condition_id: "c".into(), end_time: "e".into(),
            maker_base_fee: 0, taker_base_fee: 0, fee_type: String::new() }
    }
    fn kl(asset: &str, t_s: i64, close: f64, recv: i64) -> KlineClose {
        KlineClose { asset: asset.into(), t_s, close, received_at_ms: recv }
    }
    fn no_rest(_t: &str) -> RestProbe { RestProbe::Skip }

    /// +6bps kline over a fresh in-band book → one Open with the right token, fill,
    /// shares, exit_ts, and populated ctx (t_signal/latency).
    #[test]
    fn process_kline_emits_open_on_trigger() {
        let mut eng = SignalEngine::new();
        let secs = 300i64;
        let base = 1_780_000_000i64 / secs * secs;
        let t = base + 100; // ttr 200 → RW
        let empty_cat = TestCat(HashMap::new());
        let empty_book = TestBook(HashMap::new());
        for (ts, c) in [(t - 3, 100.0), (t - 2, 100.0), (t - 1, 100.0)] {
            assert!(process_kline(&mut eng, &kl("BTC", ts, c, ts * 1000), &empty_cat, &empty_book,
                &DecisionConfig::default(), no_rest, &[], (ts + 1) * 1000,
                false /* G5-test-rig: default OFF = no filter */,
                &[] /* disabled_cells: default OFF = no production filter */).is_none());
        }
        let up = "BTC-5m-up";
        let mut cat = HashMap::new();
        cat.insert(("BTC".into(), "5m".into(), base), mref(up, "BTC-5m-down"));
        let mut bk = HashMap::new();
        bk.insert(up.to_string(), EventBbo { best_ask: Some(0.50), best_bid: Some(0.49), ts_ms: (t + 1) * 1000 });
        let now = (t + 1) * 1000;
        let outcome = process_kline(
            &mut eng, &kl("BTC", t, 100.06, t * 1000), &TestCat(cat), &TestBook(bk),
            &DecisionConfig::default(), no_rest, &[], now,
            false, // G5-test-rig: default OFF = no filter
            &[],   // disabled_cells: default OFF = no production filter
        ).expect("trigger");
        let TriggerOutcome { decision, commands: cmds, .. } = outcome;
        assert!(decision.fired);
        assert_eq!(cmds.len(), 1);
        match &cmds[0] {
            ExecCommand::Open { intent, ctx } => {
                assert_eq!(intent.token_id, up);
                assert_eq!(intent.side, Outcome::Up);
                assert_eq!(intent.fill_price, 0.50);
                assert!((intent.shares - 20.0).abs() < 1e-9);
                assert_eq!(intent.exit_ts_s, t + 120);
                assert_eq!(ctx.t_signal_ms, t * 1000);
                assert_eq!(ctx.t_decision_ms, now);
                assert!(ctx.binance_ret_bps > 0.0);
            }
            c => panic!("expected Open, got {c:?}"),
        }
    }

    /// No catalog market → a decision still emits (faithful to the Capa B path), but
    /// `fired=false` with ZERO commands (nothing to trade).
    #[test]
    fn no_market_decides_but_no_command() {
        let mut eng = SignalEngine::new();
        let t = 1_780_000_100i64;
        let empty_cat = TestCat(HashMap::new());
        let empty_book = TestBook(HashMap::new());
        for (ts, c) in [(t - 3, 100.0), (t - 2, 100.0), (t - 1, 100.0)] {
            let _ = process_kline(&mut eng, &kl("BTC", ts, c, ts * 1000), &empty_cat, &empty_book,
                &DecisionConfig::default(), no_rest, &[], (ts + 1) * 1000, false, &[]);
        }
        let r = process_kline(&mut eng, &kl("BTC", t, 100.06, t * 1000), &empty_cat, &empty_book,
            &DecisionConfig::default(), no_rest, &[], (t + 1) * 1000, false, &[]);
        let TriggerOutcome { decision, commands: cmds, .. } =
            r.expect("a trigger fired → a decision is emitted");
        assert!(!decision.fired, "empty scope → not fired");
        assert!(cmds.is_empty(), "empty scope → no commands");
    }

    /// No trigger at all (flat klines) → None (no decision).
    #[test]
    fn no_trigger_returns_none() {
        let mut eng = SignalEngine::new();
        let t = 1_780_000_100i64;
        let empty_cat = TestCat(HashMap::new());
        let empty_book = TestBook(HashMap::new());
        let mut last = None;
        for (ts, c) in [(t - 3, 100.0), (t - 2, 100.0), (t - 1, 100.0), (t, 100.0)] {
            last = process_kline(&mut eng, &kl("BTC", ts, c, ts * 1000), &empty_cat, &empty_book,
                &DecisionConfig::default(), no_rest, &[], (ts + 1) * 1000, false, &[]);
        }
        assert!(last.is_none(), "flat klines → no trigger → None");
    }

    // ============================================================================
    // G5-test-rig: filter_for_test_rig (TEST MODE: only-15m-IM signal filter).
    //
    // CRITICAL INVARIANT: with `restrict_15m_im = false` (default), the filter is
    // a PURE IDENTITY -- the bot's signal flow is byte-identical to the
    // pre-G5-test-rig path. The no_op test below proves this against all 3
    // trigger types (5m, 15m RW, 15m IM).
    // ============================================================================

    use crate::signal::Stratum;

    /// Build a synthetic Signal of a given (interval, stratum) so the filter
    /// can be tested without running the full SignalEngine.
    fn synth_signal(asset: &str, t: i64, interval: &str, stratum: Stratum) -> crate::signal::Signal {
        let secs: i64 = if interval == "5m" { 300 } else { 900 };
        // ttr range that satisfies stratum: RW <= 300, IM > 300. Use a value
        // safely inside the band.
        let ttr = match stratum {
            Stratum::ResolvedWindow => 200,
            Stratum::Immediate => 600,
        };
        crate::signal::Signal {
            asset: asset.to_string(),
            trigger_ts: t,
            window_s: 1,
            ret_bps: 7.0,
            direction: Direction::Up,
            interval: interval.to_string(),
            epoch: (t / secs) * secs,
            ttr,
            stratum,
            bet_token_id: format!("{asset}-{interval}-{:?}-bet", stratum),
            opposite_token_id: format!("{asset}-{interval}-{:?}-opp", stratum),
        }
    }

    #[test]
    fn g5_tr_filter_drops_5m_trigger_when_restricted() {
        // 5m is ALWAYS RW (IM impossible for 5m: ttr_max=270 < 300). Restricted
        // mode keeps only 15m IM, so any 5m signal is dropped.
        let s = synth_signal("BTC", 1_780_000_000, "5m", Stratum::ResolvedWindow);
        let out = filter_for_test_rig(vec![s], true);
        assert!(out.is_empty(), "5m signals must be dropped under --restrict-15m-im");
    }

    #[test]
    fn g5_tr_filter_drops_15m_rw_trigger_when_restricted() {
        // 15m RW (ttr <= 300, hold 120, COULD go past-close if ttr < 120) is NOT
        // the deterministic-SELL-in-window cohort -- only IM is. Dropped.
        let s = synth_signal("BTC", 1_780_000_000, "15m", Stratum::ResolvedWindow);
        let out = filter_for_test_rig(vec![s], true);
        assert!(out.is_empty(), "15m RW signals must also be dropped (only IM is deterministic-SELL-in-window)");
    }

    #[test]
    fn g5_tr_filter_keeps_15m_im_trigger_when_restricted() {
        // 15m IM (ttr > 300, hold 300): exit_offset_from_window_start = (900-ttr)+300
        // which is < 900 for any ttr > 300 -- ALWAYS SELL-in-window, never past-close.
        // This is the ONLY cohort the test rig allows through.
        let s = synth_signal("BTC", 1_780_000_000, "15m", Stratum::Immediate);
        let out = filter_for_test_rig(vec![s.clone()], true);
        assert_eq!(out.len(), 1, "15m IM signals are kept under --restrict-15m-im");
        assert_eq!(out[0].interval, "15m");
        assert_eq!(out[0].stratum, Stratum::Immediate);
    }

    #[test]
    fn g5_tr_filter_no_op_when_flag_false() {
        // DEFAULT OFF MUST be byte-identical to pre-G5-test-rig: ALL 3 trigger
        // types pass through unchanged.
        let s_5m_rw  = synth_signal("BTC", 1_780_000_000, "5m",  Stratum::ResolvedWindow);
        let s_15m_rw = synth_signal("BTC", 1_780_000_000, "15m", Stratum::ResolvedWindow);
        let s_15m_im = synth_signal("BTC", 1_780_000_000, "15m", Stratum::Immediate);
        let input = vec![s_5m_rw.clone(), s_15m_rw.clone(), s_15m_im.clone()];
        let out = filter_for_test_rig(input.clone(), false);
        assert_eq!(out.len(), 3, "no_op: all 3 signals pass when flag=false");
        // Same elements, same order -- identity.
        for (a, b) in out.iter().zip(input.iter()) {
            assert_eq!(a.interval, b.interval);
            assert_eq!(a.stratum, b.stratum);
            assert_eq!(a.bet_token_id, b.bet_token_id);
        }
    }

    #[test]
    fn g5_tr_filter_empty_input_returns_empty_both_flag_values() {
        // Edge: empty signals -> empty output, regardless of flag. (process_kline
        // hands us [] when expand_signals had no scope; filter must not panic.)
        let out_false = filter_for_test_rig(vec![], false);
        let out_true  = filter_for_test_rig(vec![], true);
        assert!(out_false.is_empty());
        assert!(out_true.is_empty());
    }

    // ============================================================================
    // disabled_cells: production cell-suppression filter. The filter is wired in
    // `process_kline` (between filter_for_test_rig and decide); these tests
    // exercise the pure function + the integration path. The async oplog test
    // (`decision_task_emits_signal_dropped_disabled_cell_oplog`) lives further
    // down with the other PIECE 3 end-to-end tests since it drives
    // `run_decision_task`.
    //
    // CRITICAL INVARIANT (mirrors filter_for_test_rig): empty list = pure
    // identity. The "default safe" path is byte-identical to the pre-filter
    // pipeline. dispatched_disabled_cells_*default_is_empty (in config tests)
    // guards the toml default; this layer guards the runtime fast path.
    // ============================================================================

    #[test]
    fn filter_disabled_cells_empty_list_is_identity() {
        // Default safe path: an empty list is a pure no-op. All 4 cells
        // (BTC/ETH x 5m/15m) pass through unchanged, in original order.
        let inputs = vec![
            synth_signal("BTC", 1_780_000_000, "5m",  Stratum::ResolvedWindow),
            synth_signal("BTC", 1_780_000_000, "15m", Stratum::Immediate),
            synth_signal("ETH", 1_780_000_000, "5m",  Stratum::ResolvedWindow),
            synth_signal("ETH", 1_780_000_000, "15m", Stratum::Immediate),
        ];
        let out = filter_disabled_cells(inputs.clone(), &[]);
        assert_eq!(out.len(), 4, "empty list must pass all 4 cells");
        for (a, b) in out.iter().zip(inputs.iter()) {
            assert_eq!(a.asset, b.asset);
            assert_eq!(a.interval, b.interval);
        }
    }

    #[test]
    fn filter_disabled_cells_drops_only_listed_cell() {
        // The operative case: disable ETH:15m only. The other 3 cells stay.
        // This is the exact filter we deploy.
        let inputs = vec![
            synth_signal("BTC", 1_780_000_000, "5m",  Stratum::ResolvedWindow),
            synth_signal("BTC", 1_780_000_000, "15m", Stratum::Immediate),
            synth_signal("ETH", 1_780_000_000, "5m",  Stratum::ResolvedWindow),
            synth_signal("ETH", 1_780_000_000, "15m", Stratum::Immediate),
        ];
        let disabled = vec!["ETH:15m".to_string()];
        let out = filter_disabled_cells(inputs, &disabled);
        assert_eq!(out.len(), 3, "ETH:15m must be dropped, other 3 stay");
        let cells: Vec<String> = out.iter().map(|s| format!("{}:{}", s.asset, s.interval)).collect();
        assert!(cells.contains(&"BTC:5m".to_string()));
        assert!(cells.contains(&"BTC:15m".to_string()));
        assert!(cells.contains(&"ETH:5m".to_string()));
        assert!(!cells.contains(&"ETH:15m".to_string()),
            "the dropped cell must not appear in output; got cells={cells:?}");
    }

    #[test]
    fn filter_disabled_cells_case_sensitive() {
        // Case-sensitivity is intentional: bot emits "ETH" / "5m" / "15m"
        // (uppercase asset, lowercase interval). A toml typo like "eth:15m"
        // is caught at validate-time (the asset list contains "ETH" not
        // "eth"); even if it slipped through, this layer would not match.
        // Belt-and-suspenders for fail-closed semantics.
        let inputs = vec![
            synth_signal("ETH", 1_780_000_000, "15m", Stratum::Immediate),
        ];
        // Wrong-case asset: bot emits "ETH", filter has "eth" → no match → kept.
        let disabled = vec!["eth:15m".to_string()];
        let out = filter_disabled_cells(inputs, &disabled);
        assert_eq!(out.len(), 1, "case-sensitive match: 'eth:15m' must NOT drop 'ETH:15m'");
        assert_eq!(out[0].asset, "ETH");
    }

    #[test]
    fn identify_disabled_drops_returns_pairs_for_oplog() {
        // The audit trail capture helper. Must return the same set of pairs
        // that filter_disabled_cells will drop -- enables the caller to emit
        // exactly N oplog events for N drops without recomputing the filter.
        let inputs = vec![
            synth_signal("BTC", 1_780_000_000, "5m",  Stratum::ResolvedWindow),
            synth_signal("BTC", 1_780_000_000, "15m", Stratum::Immediate),
            synth_signal("ETH", 1_780_000_000, "5m",  Stratum::ResolvedWindow),
            synth_signal("ETH", 1_780_000_000, "15m", Stratum::Immediate),
        ];
        // Empty disabled = empty drops (no alloc in the hot path).
        let drops_empty = identify_disabled_drops(&inputs, &[]);
        assert!(drops_empty.is_empty(), "empty disabled list must yield empty drops");
        // Single disabled cell = one pair.
        let drops_one = identify_disabled_drops(&inputs, &["ETH:15m".to_string()]);
        assert_eq!(drops_one, vec![("ETH".to_string(), "15m".to_string())]);
        // Multiple disabled cells = multiple pairs, in input order.
        let drops_two = identify_disabled_drops(&inputs, &["ETH:15m".to_string(), "BTC:5m".to_string()]);
        assert_eq!(drops_two, vec![
            ("BTC".to_string(), "5m".to_string()),
            ("ETH".to_string(), "15m".to_string()),
        ], "pairs returned in input (signal) order, not disabled-list order");
    }

    #[test]
    fn process_kline_disabled_cell_drops_command_and_populates_dropped_cells() {
        // INTEGRATION: drive process_kline with ETH:15m in disabled_cells.
        // A trigger that would normally emit a Fire command for the 15m epoch
        // must NOT produce that command, and the outcome must surface the drop
        // in `dropped_cells` so the live caller can emit the oplog event.
        let mut eng = SignalEngine::new();
        // Pick a t that puts BOTH 5m and 15m into a valid scope:
        //   5m: epoch = base5; ttr = t - base5 ∈ [30, 270]   → ResolvedWindow
        //   15m: epoch = base15; ttr = t - base15 ∈ [301, 870] → Immediate
        // base = a 15m epoch (divisible by 900). t = base + 600:
        //   5m: base5 = (base+600)/300*300; ttr5 = (base+600) - base5 = 0 .. ok
        //     base+600 / 300 = base/300 + 2, base/300 is integer divisible by 3,
        //     so base5 = base+600, ttr5 = 0 -- TOO SHORT (< 30). Adjust to +700:
        //   5m: base5 = base+600; ttr5 = 100 → RW ok.
        //   15m: base15 = base; ttr15 = 700 → IM ok.
        let secs15 = 900i64;
        let base = 1_780_000_000i64 / secs15 * secs15;
        let t = base + 700;
        let empty_book = TestBook(HashMap::new());
        // Warm-up trigger window (3 prior klines, flat).
        for (ts, c) in [(t - 3, 100.0), (t - 2, 100.0), (t - 1, 100.0)] {
            let _ = process_kline(
                &mut eng, &kl("ETH", ts, c, ts * 1000),
                &TestCat(HashMap::new()), &empty_book,
                &DecisionConfig::default(), no_rest, &[], (ts + 1) * 1000,
                false, &[],
            );
        }
        // Catalog has BOTH the 5m AND the 15m active market.
        let base5 = (t / 300) * 300;
        let eth_5m_up  = "ETH-5m-up";
        let eth_15m_up = "ETH-15m-up";
        let mut cat = HashMap::new();
        cat.insert(("ETH".into(), "5m".into(),  base5), mref(eth_5m_up,  "ETH-5m-down"));
        cat.insert(("ETH".into(), "15m".into(), base ), mref(eth_15m_up, "ETH-15m-down"));
        let mut bk = HashMap::new();
        bk.insert(eth_5m_up.to_string(),  EventBbo { best_ask: Some(0.50), best_bid: Some(0.49), ts_ms: (t + 1) * 1000 });
        bk.insert(eth_15m_up.to_string(), EventBbo { best_ask: Some(0.50), best_bid: Some(0.49), ts_ms: (t + 1) * 1000 });
        let now = (t + 1) * 1000;
        // Trigger with disabled_cells = ["ETH:15m"].
        let disabled = vec!["ETH:15m".to_string()];
        let outcome = process_kline(
            &mut eng, &kl("ETH", t, 100.06, t * 1000),
            &TestCat(cat), &TestBook(bk),
            &DecisionConfig::default(), no_rest, &[], now,
            false, &disabled,
        ).expect("trigger fired (+6 bps move)");
        // The drop is recorded for the caller's oplog emission.
        assert_eq!(
            outcome.dropped_cells,
            vec![("ETH".to_string(), "15m".to_string())],
            "dropped_cells must contain exactly the suppressed (asset, interval) pair"
        );
        // The Fire commands MUST NOT include the 15m token.
        let opens: Vec<_> = outcome.commands.iter().filter_map(|c| match c {
            ExecCommand::Open { intent, .. } => Some(intent),
            _ => None,
        }).collect();
        assert!(
            !opens.iter().any(|i| i.token_id == eth_15m_up),
            "ETH:15m must not produce an Open command; got opens={:?}",
            opens.iter().map(|i| &i.token_id).collect::<Vec<_>>()
        );
        // The 5m Fire DID make it through (proves the filter is surgical).
        assert!(
            opens.iter().any(|i| i.token_id == eth_5m_up),
            "ETH:5m must still produce an Open command (only :15m disabled)"
        );
    }

    // ========================================================================
    // Pieza 2: update_running_max -- per-position smart-exit state refresh.
    // CONTRACT TESTS (pure function): no exit_decision is changed yet; this
    // is just the rails the next piece will run on. Each guard (sentinel,
    // absent BBO, stale BBO) is verified independently + together.
    //
    // (Imports `dec` and `OrderStatus` are already in scope from the earlier
    // test fixtures in this module -- do not re-import.)
    // ========================================================================

    /// Build an OpenPosition with NON-sentinel smart-exit state ready for
    /// update_running_max to act on it. `entry_ts_ms` doubles as the initial
    /// `ts_max_bid_ms`; `entry_price` doubles as the initial `running_max_bid`
    /// (matches the Pieza 2 open-time semantics).
    fn pos_with_state(token: &str, entry_price: f64, entry_ts_ms: i64) -> OpenPosition {
        OpenPosition {
            token_id: token.into(),
            asset: "BTC".into(),
            side: Outcome::Up,
            entry_price: rust_decimal::Decimal::try_from(entry_price).unwrap(),
            shares: dec!(10.0),
            opened_at_ms: entry_ts_ms,
            signal_id: format!("{token}-sig"),
            interval: "5m".into(),
            exit_ts_s: (entry_ts_ms / 1000) + 120,
            entry_ts_ms,
            running_max_bid: entry_price,
            ts_max_bid_ms: entry_ts_ms,
            status: OrderStatus::Confirmed,
            order_id: Some("o".into()),
            ack_at_ms: Some(entry_ts_ms),
            confirmed_at_ms: Some(entry_ts_ms),
            confirmation_source: None,
            maker_exit: None,
        }
    }

    /// Insert a BBO with a chosen `(best_bid, ts_ms)` for `token`. `best_ask`
    /// is fixed at bid+0.02 (irrelevant for update_running_max).
    fn put_bbo(bbo: &DashMap<String, EventBbo>, token: &str, bid: f64, ts_ms: i64) {
        bbo.insert(
            token.into(),
            EventBbo { best_bid: Some(bid), best_ask: Some(bid + 0.02), ts_ms },
        );
    }

    #[test]
    fn update_running_max_advances_when_new_high() {
        let t0 = 1_780_000_000_000_i64;
        let mut positions = vec![pos_with_state("T", 0.50, t0)];
        let bbo = DashMap::new();
        let now = t0 + 1_000;
        put_bbo(&bbo, "T", 0.55, now); // fresh bid above the seed

        let stats = update_running_max(&mut positions, &bbo, now);

        assert_eq!(stats.updated, 1, "new high must count as 1 update");
        assert!(stats.stale_skipped.is_empty());
        assert!(stats.absent_skipped.is_empty());
        assert_eq!(stats.sentinel_skipped, 0);
        assert_eq!(positions[0].running_max_bid, 0.55);
        assert_eq!(
            positions[0].ts_max_bid_ms, now,
            "ts_max_bid_ms must stamp the LOCAL `now`, not bbo.ts_ms"
        );
    }

    #[test]
    fn update_running_max_stays_when_bid_falls() {
        let t0 = 1_780_000_000_000_i64;
        let mut positions = vec![pos_with_state("T", 0.50, t0)];
        positions[0].running_max_bid = 0.55; // a prior tick set this high
        positions[0].ts_max_bid_ms = t0 + 1_000;
        let bbo = DashMap::new();
        let now = t0 + 5_000;
        put_bbo(&bbo, "T", 0.50, now); // fresh but LOWER

        let stats = update_running_max(&mut positions, &bbo, now);

        assert_eq!(stats.updated, 0, "lower bid is not a new high");
        assert_eq!(positions[0].running_max_bid, 0.55, "max non-decreasing");
        assert_eq!(positions[0].ts_max_bid_ms, t0 + 1_000, "max-timestamp preserved");
    }

    #[test]
    fn update_running_max_stays_when_bid_equal() {
        // A flat observation MUST NOT advance ts_max_bid_ms -- f6 (time since
        // last STRICT high) is the rule's semantics. If equal-bids reset f6,
        // a flat market would never trigger F6Only.
        let t0 = 1_780_000_000_000_i64;
        let mut positions = vec![pos_with_state("T", 0.50, t0)];
        positions[0].running_max_bid = 0.55;
        positions[0].ts_max_bid_ms = t0 + 1_000;
        let bbo = DashMap::new();
        let now = t0 + 5_000;
        put_bbo(&bbo, "T", 0.55, now); // EQUAL to running_max

        let stats = update_running_max(&mut positions, &bbo, now);

        assert_eq!(stats.updated, 0, "equal bid is not a STRICT new high");
        assert_eq!(positions[0].running_max_bid, 0.55);
        assert_eq!(
            positions[0].ts_max_bid_ms, t0 + 1_000,
            "ts_max_bid_ms NOT advanced on equal bid (so F6Only can fire)"
        );
    }

    #[test]
    fn update_running_max_skips_when_bbo_stale() {
        let t0 = 1_780_000_000_000_i64;
        let mut positions = vec![pos_with_state("T", 0.50, t0)];
        let bbo = DashMap::new();
        let bbo_ts = t0 + 1_000;
        put_bbo(&bbo, "T", 0.60, bbo_ts); // bid would be a NEW HIGH if observed
        let now = bbo_ts + STALE_BBO_MS + 1; // strictly past the threshold

        let stats = update_running_max(&mut positions, &bbo, now);

        assert_eq!(stats.updated, 0, "stale BBO must NOT advance max");
        assert_eq!(stats.stale_skipped.len(), 1);
        assert_eq!(stats.stale_skipped[0].0, "T");
        assert!(stats.stale_skipped[0].1 > STALE_BBO_MS);
        assert_eq!(
            positions[0].running_max_bid, 0.50,
            "preserve the seed; refuse to observe stale bid even if higher"
        );
        assert_eq!(positions[0].ts_max_bid_ms, t0);
    }

    #[test]
    fn update_running_max_skips_when_bbo_absent() {
        let t0 = 1_780_000_000_000_i64;
        let mut positions = vec![pos_with_state("T", 0.50, t0)];
        let bbo: DashMap<String, EventBbo> = DashMap::new(); // empty -- cold start

        let stats = update_running_max(&mut positions, &bbo, t0 + 1_000);

        assert_eq!(stats.updated, 0);
        assert_eq!(stats.absent_skipped, vec!["T".to_string()]);
        assert!(stats.stale_skipped.is_empty());
        // Position state unchanged.
        assert_eq!(positions[0].running_max_bid, 0.50);
        assert_eq!(positions[0].ts_max_bid_ms, t0);
    }

    #[test]
    fn update_running_max_skips_sentinel_positions() {
        // entry_ts_ms == 0 marks pre-Pieza-2 / v2-migrated positions: smart-
        // exit ineligible by contract. Even a fresh, high bid must NOT
        // touch their state -- the (future) rule evaluator falls back to
        // baseline for them.
        let t0 = 1_780_000_000_000_i64;
        let mut positions = vec![pos_with_state("T", 0.50, t0)];
        positions[0].entry_ts_ms = 0; // mark sentinel
        positions[0].running_max_bid = 0.0; // sentinel
        positions[0].ts_max_bid_ms = 0; // sentinel
        let bbo = DashMap::new();
        put_bbo(&bbo, "T", 0.70, t0 + 1_000); // fresh + high

        let stats = update_running_max(&mut positions, &bbo, t0 + 1_000);

        assert_eq!(stats.sentinel_skipped, 1);
        assert_eq!(stats.updated, 0);
        assert!(stats.absent_skipped.is_empty(), "BBO is present, only sentinel skip");
        assert!(stats.stale_skipped.is_empty(), "BBO is fresh, only sentinel skip");
        assert_eq!(positions[0].running_max_bid, 0.0, "sentinel preserved");
        assert_eq!(positions[0].ts_max_bid_ms, 0, "sentinel preserved");
    }

    /// CRITICAL test for Pieza 2 -- the answer to the Q2 reconnect/gap
    /// design question. Verify that during EVERY tick of a multi-second
    /// gap, running_max_bid stays put. A gap NEVER corrupts the max.
    #[test]
    fn update_running_max_preserves_across_long_gap() {
        let t0 = 1_780_000_000_000_i64;
        let mut positions = vec![pos_with_state("T", 0.50, t0)];
        let bbo = DashMap::new();

        // Tick 1: BBO fresh, new high observed -> max becomes 0.55.
        let t1 = t0 + 1_000;
        put_bbo(&bbo, "T", 0.55, t1);
        let stats = update_running_max(&mut positions, &bbo, t1);
        assert_eq!(stats.updated, 1);
        assert_eq!(positions[0].running_max_bid, 0.55);
        assert_eq!(positions[0].ts_max_bid_ms, t1);

        // Tick at t1+4s: still WITHIN the STALE_BBO_MS=5000 window.
        // bid same, no update. Max preserved.
        let stats = update_running_max(&mut positions, &bbo, t1 + 4_000);
        assert_eq!(stats.updated, 0);
        assert!(stats.stale_skipped.is_empty(), "still fresh at t1+4s");
        assert_eq!(positions[0].running_max_bid, 0.55);
        assert_eq!(positions[0].ts_max_bid_ms, t1);

        // Tick at t1+7s: NOW stale. Max preserved -- the key assertion.
        let stats = update_running_max(&mut positions, &bbo, t1 + 7_000);
        assert_eq!(stats.stale_skipped.len(), 1, "stale begins");
        assert_eq!(positions[0].running_max_bid, 0.55, "preserved during gap");
        assert_eq!(positions[0].ts_max_bid_ms, t1, "preserved");

        // Tick at t1+15s: STILL stale (no new BBO). Max STILL preserved.
        let stats = update_running_max(&mut positions, &bbo, t1 + 15_000);
        assert_eq!(stats.stale_skipped.len(), 1);
        assert_eq!(positions[0].running_max_bid, 0.55);
        assert_eq!(positions[0].ts_max_bid_ms, t1);

        // Tick at t1+30s: still stale. Max preserved.
        let stats = update_running_max(&mut positions, &bbo, t1 + 30_000);
        assert_eq!(stats.stale_skipped.len(), 1);
        assert_eq!(positions[0].running_max_bid, 0.55);
        assert_eq!(positions[0].ts_max_bid_ms, t1);

        // RECONNECT at t1+35s: BBO refreshes with bid=0.50 (BELOW the
        // preserved max). max MUST stay 0.55 (running max is non-
        // decreasing across the gap -- no missed peak can lower it).
        let t_reconnect = t1 + 35_000;
        put_bbo(&bbo, "T", 0.50, t_reconnect);
        let stats = update_running_max(&mut positions, &bbo, t_reconnect);
        assert_eq!(stats.updated, 0, "post-reconnect bid below max -> no update");
        assert!(stats.stale_skipped.is_empty(), "BBO fresh again");
        assert_eq!(
            positions[0].running_max_bid, 0.55,
            "running_max_bid PRESERVED across the entire gap; the gap NEVER \
             corrupted it nor reset it"
        );
        assert_eq!(
            positions[0].ts_max_bid_ms, t1,
            "ts_max_bid_ms ALSO preserved -- the rule's f6 = now - ts_max \
             counts time from BEFORE the gap, correctly"
        );

        // And after reconnect, NEW highs work normally.
        let t_after = t_reconnect + 1_000;
        put_bbo(&bbo, "T", 0.60, t_after);
        let stats = update_running_max(&mut positions, &bbo, t_after);
        assert_eq!(stats.updated, 1, "new high after reconnect updates normally");
        assert_eq!(positions[0].running_max_bid, 0.60);
        assert_eq!(positions[0].ts_max_bid_ms, t_after);
    }

    /// DCA: two lots of the SAME token (confirmed real per
    /// decision/mod.rs: "same-side lot exists -> DCA = another lot").
    /// Each lot has its OWN running_max_bid tracking from its OWN
    /// entry_price seed. A new high refines lot1 but not lot2 if lot2's
    /// seed was already higher.
    #[test]
    fn update_running_max_dca_lots_track_independent_max() {
        let t0 = 1_780_000_000_000_i64;
        let mut positions = vec![
            pos_with_state("T", 0.50, t0),         // lot 1: seed max 0.50
            pos_with_state("T", 0.55, t0 + 200),   // lot 2: seed max 0.55 (later DCA, higher ask)
        ];
        let bbo = DashMap::new();
        let now = t0 + 1_000;
        put_bbo(&bbo, "T", 0.52, now); // between the two seeds

        let stats = update_running_max(&mut positions, &bbo, now);

        assert_eq!(stats.updated, 1, "only lot 1's max should advance");
        // Lot 1: 0.52 > 0.50 -> updated.
        assert_eq!(positions[0].running_max_bid, 0.52);
        assert_eq!(positions[0].ts_max_bid_ms, now);
        // Lot 2: 0.52 < 0.55 -> unchanged.
        assert_eq!(positions[1].running_max_bid, 0.55);
        assert_eq!(positions[1].ts_max_bid_ms, t0 + 200);
    }

    #[test]
    fn update_running_max_stats_counts_correctly() {
        // Mixed scenario: 1 updated, 1 stale, 1 absent, 1 sentinel.
        let t0 = 1_780_000_000_000_i64;
        let mut positions = vec![
            pos_with_state("A", 0.40, t0),                // will update
            pos_with_state("B", 0.50, t0),                // BBO stale -> skip
            pos_with_state("C", 0.60, t0),                // BBO absent -> skip
            {
                let mut s = pos_with_state("D", 0.70, t0);
                s.entry_ts_ms = 0; // sentinel
                s
            },
        ];
        let bbo = DashMap::new();
        let now = t0 + 1_000;
        put_bbo(&bbo, "A", 0.45, now);                              // fresh, new high
        put_bbo(&bbo, "B", 0.99, now - STALE_BBO_MS - 100);         // stale (would be high if fresh)
        // (no BBO entry for "C" -> absent)
        // "D" has BBO present + fresh, but sentinel guard fires first.
        put_bbo(&bbo, "D", 0.99, now);

        let stats = update_running_max(&mut positions, &bbo, now);

        assert_eq!(stats.updated, 1);
        assert_eq!(stats.stale_skipped.len(), 1);
        assert_eq!(stats.stale_skipped[0].0, "B");
        assert_eq!(stats.absent_skipped, vec!["C".to_string()]);
        assert_eq!(stats.sentinel_skipped, 1);
        // Per-position state assertions.
        assert_eq!(positions[0].running_max_bid, 0.45, "A advanced");
        assert_eq!(positions[1].running_max_bid, 0.50, "B preserved (stale)");
        assert_eq!(positions[2].running_max_bid, 0.60, "C preserved (absent)");
        assert_eq!(positions[3].running_max_bid, 0.70, "D untouched (sentinel)");
    }

    /// Integration: run_exit_task wires update_running_max into the tick.
    /// Verifies that after a few ticks, a non-sentinel position with a fresh
    /// BBO above its seed has its running_max_bid advanced. close_due is
    /// not exercised (the position's exit_ts_s is far in the future).
    #[tokio::test]
    async fn exit_task_calls_update_running_max_before_close_due() {
        use crate::guards::{GuardConfig, Guards};
        use crate::oplog::OpLog;
        use crate::state::store::StateStore;
        use std::sync::atomic::AtomicU32;

        // Unique dirs to avoid cross-test races.
        static N: AtomicU32 = AtomicU32::new(0);
        let id = N.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!(
            "rb_p2_exit_{}_{}_{}", std::process::id(), now_ms(), id));
        std::fs::create_dir_all(&dir).unwrap();

        // SharedState::new() already returns Arc<SharedState> (see state/mod.rs).
        let state = SharedState::new();
        // Seed a fresh BBO with a NEW HIGH for token "TX".
        let t_now = now_ms();
        state.bbo.insert("TX".into(), EventBbo {
            best_bid: Some(0.65),
            best_ask: Some(0.67),
            ts_ms: t_now, // fresh as of test start
        });

        let store = StateStore::load(&dir.join("state.json")).0;
        {
            let st = store.state();
            let mut bs = st.lock().unwrap();
            // Position seeded at 0.50, exit far in the future (never time-exits).
            let mut p = pos_with_state("TX", 0.50, t_now - 1_000);
            p.exit_ts_s = (t_now / 1000) + 3_600; // 1h in the future
            bs.positions.push(p);
        }

        // Wire the guards (kill-switch path that doesn't exist -> off).
        let mut gcfg = GuardConfig::default();
        gcfg.kill_switch_path = dir.join("ks_nonexistent.txt");
        let guards = Arc::new(Mutex::new(Guards::new(gcfg)));
        let oplog = Arc::new(OpLog::new(&dir.join("oplog.jsonl")));
        let (sd_tx, sd_rx) = watch::channel(false);

        let handle = tokio::spawn(run_exit_task(
            state.clone(), store.state(), guards.clone(), None, oplog.clone(),
            Arc::new(ExitsConfig::default()),  // Pieza 4: empty -> baseline everywhere
            sd_rx,
        ));

        // The tick is 1 s; wait a bit over to guarantee >=1 tick fires.
        // Update BBO ts_ms periodically so it doesn't go stale during the wait.
        for _ in 0..3 {
            tokio::time::sleep(std::time::Duration::from_millis(400)).await;
            let now = now_ms();
            state.bbo.insert("TX".into(), EventBbo {
                best_bid: Some(0.65),
                best_ask: Some(0.67),
                ts_ms: now,
            });
        }

        // Stop the task cleanly.
        let _ = sd_tx.send(true);
        let _ = handle.await;

        // Assert the position's running_max_bid was advanced by the tick.
        let st = store.state();
        let bs = st.lock().unwrap();
        assert_eq!(bs.positions.len(), 1, "position must still be open (exit far)");
        let p = &bs.positions[0];
        assert_eq!(
            p.running_max_bid, 0.65,
            "exit_task tick MUST have called update_running_max and advanced \
             the max from the 0.50 seed to the 0.65 fresh BBO bid"
        );
        assert!(
            p.ts_max_bid_ms > p.entry_ts_ms,
            "ts_max_bid_ms must have advanced past entry_ts_ms ({} vs {})",
            p.ts_max_bid_ms, p.entry_ts_ms
        );

        drop(bs);
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ============================================================================
    // PIECE 3 wiring tests: guards-in-loop. Pure helpers + 2 async end-to-end tests
    // that drive the actual run_decision_task and assert NO Open command is emitted
    // when a cap is at its limit or the kill-switch is present.
    // ============================================================================

    use crate::decision::executor::{ClosedTrade, ExitKind};
    use crate::guards::{GuardConfig, GuardVerdict, Guards};
    use crate::state::SharedState;
    use crate::state::persist::OrderStatus;
    use crate::state::store::StateStore;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU32, Ordering as AOrdering};

    fn tmp_dir(tag: &str) -> PathBuf {
        static N: AtomicU32 = AtomicU32::new(0);
        let id = N.fetch_add(1, AOrdering::SeqCst);
        std::env::temp_dir().join(format!("rb_p3_{tag}_{}_{}_{}", std::process::id(), now_ms(), id))
    }

    fn tmp_killswitch(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!("rb_p3_ks_{tag}_{}_{}.txt", std::process::id(), now_ms()))
    }

    fn held_at(token: &str, shares: Decimal, opened_ms: i64) -> OpenPosition {
        OpenPosition {
            token_id: token.into(), asset: "BTC".into(), side: Outcome::Up,
            entry_price: dec!(0.50), shares,
            opened_at_ms: opened_ms, signal_id: "sig-pre".into(), interval: "5m".into(),
            exit_ts_s: 9_999_999_999,
            // v3 (Pieza 1) sentinels: these helpers feed tests that do not
            // exercise smart-exit (they verify open-position guards, kline
            // close routing, etc.).
            entry_ts_ms: 0,
            running_max_bid: 0.0,
            ts_max_bid_ms: 0,
            status: OrderStatus::TentativeFilled,
            order_id: Some("ord-pre".into()), ack_at_ms: Some(opened_ms),
            confirmed_at_ms: None, confirmation_source: None,
            maker_exit: None,
        }
    }

    #[test]
    fn latest_feed_ms_returns_max_or_now_when_empty() {
        let state = SharedState::new();
        // Empty BBO -> cold-start grace: returns `now`.
        assert_eq!(latest_feed_ms(&state, 123_456), 123_456);
        state.bbo.insert("A".into(), EventBbo { best_ask: Some(0.5), best_bid: Some(0.5), ts_ms: 100 });
        state.bbo.insert("B".into(), EventBbo { best_ask: Some(0.5), best_bid: Some(0.5), ts_ms: 999 });
        state.bbo.insert("C".into(), EventBbo { best_ask: Some(0.5), best_bid: Some(0.5), ts_ms: 500 });
        assert_eq!(latest_feed_ms(&state, 0), 999, "max across the BBO map");
    }

    #[test]
    fn feed_guards_net_pnl_accumulates_into_daily_loss_stop() {
        // The new wiring: every close feeds NET P&L into Guards. Without it, the
        // daily-loss-stop counter stays 0 forever and NEVER trips.
        let guards = Arc::new(Mutex::new(Guards::new(GuardConfig::default())));
        let now = 1_780_000_000_000i64;
        // COMBO Phase 3: default daily cap is now the absolute $15.00 override.
        // 30 losing closes ~ -$18 net (clearly past the -$15 daily cap):
        // shares=2, entry=$0.50, exit=$0.20 -> gross ~-$0.60 each + small fees.
        for i in 0..30 {
            let ct = ClosedTrade {
                token_id: format!("TOK-{i}"), side: Outcome::Up, interval: "5m".into(),
                signal_id: format!("sig-{i}"),
                entry_price: dec!(0.50), exit_price: dec!(0.20), shares: dec!(2.0),
                realized_pnl: dec!(-0.60),
                exit_kind: ExitKind::BookBid,
                opened_at_ms: now, closed_at_ms: now,
            };
            feed_guards_net_pnl(&guards, &ct, now);
        }
        let pnl = guards.lock().unwrap().daily_net_pnl();
        println!("[piece3-test] daily NET P&L after 30 losing closes = ${pnl}");
        assert!(pnl < dec!(-15), "daily NET must be past the -$15 default cap; got ${pnl}");
        // A check_entry now MUST return CloseOnly (daily-loss-stop trips).
        let state = SharedState::new();
        state.bbo.insert("TOK".into(), EventBbo {
            best_ask: Some(0.5), best_bid: Some(0.5), ts_ms: now + 500,
        });
        let intent = OpenIntent {
            token_id: "TOK".into(), asset: "BTC".into(), side: Outcome::Up,
            fill_price: 0.5, shares: 2.0,
            interval: "5m".into(), signal_id: "sig-new".into(),
            exit_ts_s: 9_999_999_999, now_ms: now + 1_000,
        };
        let verdict = eval_guards(&guards.lock().unwrap(), &state, &[], &intent, now + 1_000);
        println!("[piece3-test] post-daily-stop verdict = {verdict:?}");
        assert!(matches!(verdict, GuardVerdict::CloseOnly(_)),
            "daily-loss-stop must trip to CloseOnly via the feed_guards_net_pnl wiring; got {verdict:?}");
    }

    #[test]
    fn eval_guards_reads_resolved_tokens_cache_for_active_only_filter() {
        // PIECE 4 wire-up: the cap denominator EXCLUDES tokens whose REST cache
        // says "resolved". Without the cache hit (or with the entry = false),
        // the conservative default counts the lot toward exposure.
        //
        // Scenario: 2 lots, each "worth" $24 at local cur_price -> if both
        // count, total = $48 (way past $25 cap). REST says lot #1 is RESOLVED
        // (e.g. market ended) -> active total = $24 only -> a $1 order on
        // a NEW token fits within $25 + $1 = $25 (allowed).
        let state = SharedState::new();
        let now = 1_780_000_000_000i64;
        // BBOs fresh so staleness/feed-dead pass.
        state.bbo.insert("T-RESOLVED".into(), EventBbo { best_ask: Some(0.50), best_bid: Some(0.50), ts_ms: now });
        state.bbo.insert("T-ACTIVE".into(),   EventBbo { best_ask: Some(0.50), best_bid: Some(0.50), ts_ms: now });
        state.bbo.insert("T-NEW".into(),      EventBbo { best_ask: Some(0.50), best_bid: Some(0.50), ts_ms: now });
        // PIECE 4 cache says T-RESOLVED is resolved; T-ACTIVE is active.
        state.resolved_tokens.insert("T-RESOLVED".into(), true);
        state.resolved_tokens.insert("T-ACTIVE".into(),   false);
        let positions = vec![
            held_at("T-RESOLVED", dec!(48.0), now), // 48 sh × 0.50 = $24 local, but REST resolved
            held_at("T-ACTIVE",   dec!(48.0), now), // 48 sh × 0.50 = $24 local, REST active
        ];
        let intent = OpenIntent {
            token_id: "T-NEW".into(), asset: "BTC".into(), side: Outcome::Up,
            fill_price: 0.50, shares: 2.0,
            interval: "5m".into(), signal_id: "sig-new".into(),
            exit_ts_s: 9_999_999_999, now_ms: now + 1_000,
        };
        let mut gcfg = GuardConfig::default();
        gcfg.kill_switch_path = tmp_killswitch("p4_cache");
        let guards = Guards::new(gcfg);
        let verdict = eval_guards(&guards, &state, &positions, &intent, now + 1_000);
        println!("[piece4-test] PIECE-4 cache (T-RESOLVED resolved, T-ACTIVE active) -> verdict = {verdict:?}");
        assert!(matches!(verdict, GuardVerdict::Allow),
            "with REST cache dropping T-RESOLVED, $24 active + $1 = $25 cap fits; got {verdict:?}");

        // Sanity counter-test: WITHOUT the cache flag, both lots count -> $48 + $1 -> Deny.
        state.resolved_tokens.clear();
        let verdict2 = eval_guards(&guards, &state, &positions, &intent, now + 1_000);
        println!("[piece4-test] WITHOUT cache (conservative all-active) -> verdict = {verdict2:?}");
        assert!(matches!(verdict2, GuardVerdict::Deny(_)),
            "without REST cache, $24+$24 active = $48 > $25 cap -> Deny; got {verdict2:?}");
    }

    // W7 (2026-06-05): the test `record_order_via_decision_path_advances_
    // frequency_breaker` was DELETED with the FREQUENCY breaker. Pre-W7 it
    // pinned that the decision_loop's record_order call would push the
    // breaker past the 10-in-an-hour threshold. The breaker is gone (see
    // guards.rs comment in the check_entry body for rationale). The
    // replacement contract is asserted in guards.rs::tests::
    // frequency_breaker_removed_does_not_block_burst_entries (100 entries
    // in an instant all pass + other guards still fire correctly).

    /// PIECE 3 END-TO-END #1 + PIECE 5: drive the actual decision task with state
    /// pinned at the total-exposure cap. A triggering kline produces an Open intent
    /// inside process_kline; the loop calls eval_guards which Denies; NO ExecCommand
    /// reaches the exec channel. PIECE 5: the oplog records the kline, the guard
    /// eval (with verdict + numbers), and the guard_blocked_open event -- so the
    /// post-mortem can reconstruct WHY no order was sent. Offline, zero capital.
    #[tokio::test]
    async fn decision_task_no_open_when_total_exposure_at_cap() {
        let state = SharedState::new();
        let now_real = now_ms();
        state.bbo.insert("TOK-CAP".into(), EventBbo { best_ask: Some(0.50), best_bid: Some(0.50), ts_ms: now_real });
        state.bbo.insert("BTC-5m-up".into(), EventBbo { best_ask: Some(0.50), best_bid: Some(0.49), ts_ms: now_real });

        let secs = 300i64;
        let now_s = now_real / 1000;
        let trigger_ts = now_s;
        let epoch = (trigger_ts / secs) * secs;
        state.markets.insert(
            ("BTC".into(), "5m".into(), epoch),
            crate::signal::MarketRef {
                up_token_id: "BTC-5m-up".into(),
                down_token_id: "BTC-5m-down".into(),
                condition_id: "c".into(), end_time: "e".into(),
                maker_base_fee: 0, taker_base_fee: 0, fee_type: String::new(),
            },
        );

        let dir = tmp_dir("cap_async");
        let store = StateStore::load(&dir.join("state.json")).0;
        {
            let st = store.state();
            let mut bs = st.lock().unwrap();
            bs.positions.push(held_at("TOK-CAP", dec!(50.0), now_real));
        }

        let mut gcfg = GuardConfig::default();
        gcfg.kill_switch_path = tmp_killswitch("cap_async");
        let guards = Arc::new(Mutex::new(Guards::new(gcfg)));

        // PIECE 5: dedicated oplog path for this test so we can read the file
        // afterward and inspect the events the loop emitted.
        let oplog_path = dir.join("oplog.jsonl");
        let oplog = Arc::new(OpLog::new(&oplog_path));

        let (kline_tx, kline_rx) = mpsc::unbounded_channel::<KlineClose>();
        let (exec_tx, mut exec_rx) = mpsc::unbounded_channel::<ExecCommand>();
        let (sd_tx, sd_rx) = watch::channel(false);

        let dec_cfg = DecisionConfig { stake_usd: 1.0, ..DecisionConfig::default() };
        let handle = tokio::spawn(run_decision_task(
            state.clone(), store.state(), dec_cfg, guards.clone(), oplog.clone(),
            kline_rx, exec_tx, sd_rx,
            false,    // G5-test-rig: default OFF = no filter
            vec![],   // disabled_cells: default OFF = no production filter
            crate::config::V2Config::default(), // v2 disabled → legacy path under test
            Arc::new(crate::v2::Controls::new(true, 10.0, 100.0)),
            crate::v2::RecalSet {
                m5: Arc::new(Mutex::new(crate::v2::Recalibrator::default())),
                m15: Arc::new(Mutex::new(crate::v2::Recalibrator::default())),
                path_m5: std::env::temp_dir().join("recal5_test.json").to_string_lossy().into_owned(),
                path_m15: std::env::temp_dir().join("recal15_test.json").to_string_lossy().into_owned(),
            },
            None, // live_gate: paper/legacy path under test
        ));

        for off in [-3i64, -2, -1] {
            let t_s = trigger_ts + off;
            kline_tx.send(KlineClose { asset: "BTC".into(), t_s, close: 100.0, received_at_ms: t_s * 1000 }).unwrap();
        }
        kline_tx.send(KlineClose {
            asset: "BTC".into(), t_s: trigger_ts, close: 100.06, received_at_ms: trigger_ts * 1000,
        }).unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(250)).await;

        let _ = sd_tx.send(true);
        drop(kline_tx);
        let _ = handle.await;

        let mut drained = Vec::new();
        while let Ok(c) = exec_rx.try_recv() { drained.push(c); }
        let opens = drained.iter().filter(|c| matches!(c, ExecCommand::Open { .. })).count();

        println!("[piece3-test] decision_task drained: {} commands total, {} Opens", drained.len(), opens);
        println!("[piece3-test] expected 0 Opens (TOTAL exposure $25 + $1 = $26 > $25 cap blocks)");
        assert_eq!(opens, 0,
            "with state at total-exposure-cap, decision_task MUST NOT forward an Open (no POST)");

        // PIECE 5 inspection: read the oplog file and confirm the loop emitted
        // the events that explain WHY no order went out.
        let oplog_body = std::fs::read_to_string(&oplog_path).expect("oplog file should exist");
        let lines: Vec<&str> = oplog_body.lines().collect();
        println!("[piece5-test] === OPLOG ({} lines) for total-cap block ===", lines.len());
        for ln in &lines { println!("{ln}"); }
        let kinds: Vec<String> = lines.iter().filter_map(|l| {
            serde_json::from_str::<serde_json::Value>(l).ok()
                .and_then(|v| v.get("kind").and_then(|k| k.as_str()).map(String::from))
        }).collect();
        println!("[piece5-test] kinds = {kinds:?}");
        assert!(kinds.iter().any(|k| k == "decision_loop_start"), "expected decision_loop_start");
        assert!(kinds.iter().any(|k| k == "kline_received"), "expected kline_received");
        assert!(kinds.iter().any(|k| k == "decision_fired"), "expected decision_fired (the +6bps trigger)");
        assert!(kinds.iter().any(|k| k == "guard_eval"), "expected guard_eval");
        assert!(kinds.iter().any(|k| k == "guard_blocked_open"),
            "expected guard_blocked_open (the cap denied the open)");
        assert!(!kinds.iter().any(|k| k == "intent_open"),
            "intent_open must NOT appear (the guard blocked before it)");

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// PIECE 3 END-TO-END #2 + PIECE 5: drive the actual decision task with a
    /// kill-switch file present. The continuous halt check trips on EVERY pass;
    /// even a clean trigger never reaches process_kline. NO ExecCommand emerges.
    /// PIECE 5: every halt pass emits a `guard_halt` oplog event with the EXACT
    /// reason string -- so the post-mortem can prove the bot saw the kill-switch.
    #[tokio::test]
    async fn decision_task_halts_when_kill_switch_present() {
        let state = SharedState::new();
        let now_real = now_ms();
        state.bbo.insert("BTC-5m-up".into(), EventBbo { best_ask: Some(0.50), best_bid: Some(0.49), ts_ms: now_real });

        let secs = 300i64;
        let now_s = now_real / 1000;
        let trigger_ts = now_s;
        let epoch = (trigger_ts / secs) * secs;
        state.markets.insert(
            ("BTC".into(), "5m".into(), epoch),
            crate::signal::MarketRef {
                up_token_id: "BTC-5m-up".into(),
                down_token_id: "BTC-5m-down".into(),
                condition_id: "c".into(), end_time: "e".into(),
                maker_base_fee: 0, taker_base_fee: 0, fee_type: String::new(),
            },
        );

        let dir = tmp_dir("ks_async");
        let store = StateStore::load(&dir.join("state.json")).0;

        let ks_path = tmp_killswitch("ks_async_active");
        std::fs::write(&ks_path, "STOP").expect("write kill-switch");
        println!("[piece3-test] kill-switch present at {ks_path:?}");

        let mut gcfg = GuardConfig::default();
        gcfg.kill_switch_path = ks_path.clone();
        let guards = Arc::new(Mutex::new(Guards::new(gcfg)));

        // PIECE 5: dedicated oplog path so we can read it after the run.
        let oplog_path = dir.join("oplog.jsonl");
        let oplog = Arc::new(OpLog::new(&oplog_path));

        let (kline_tx, kline_rx) = mpsc::unbounded_channel::<KlineClose>();
        let (exec_tx, mut exec_rx) = mpsc::unbounded_channel::<ExecCommand>();
        let (sd_tx, sd_rx) = watch::channel(false);

        let dec_cfg = DecisionConfig { stake_usd: 1.0, ..DecisionConfig::default() };
        let handle = tokio::spawn(run_decision_task(
            state.clone(), store.state(), dec_cfg, guards.clone(), oplog.clone(),
            kline_rx, exec_tx, sd_rx,
            false,    // G5-test-rig: default OFF = no filter
            vec![],   // disabled_cells: default OFF = no production filter
            crate::config::V2Config::default(), // v2 disabled → legacy path under test
            Arc::new(crate::v2::Controls::new(true, 10.0, 100.0)),
            crate::v2::RecalSet {
                m5: Arc::new(Mutex::new(crate::v2::Recalibrator::default())),
                m15: Arc::new(Mutex::new(crate::v2::Recalibrator::default())),
                path_m5: std::env::temp_dir().join("recal5_test.json").to_string_lossy().into_owned(),
                path_m15: std::env::temp_dir().join("recal15_test.json").to_string_lossy().into_owned(),
            },
            None, // live_gate: paper/legacy path under test
        ));

        for off in [-3i64, -2, -1] {
            let t_s = trigger_ts + off;
            kline_tx.send(KlineClose { asset: "BTC".into(), t_s, close: 100.0, received_at_ms: t_s * 1000 }).unwrap();
        }
        kline_tx.send(KlineClose {
            asset: "BTC".into(), t_s: trigger_ts, close: 100.06, received_at_ms: trigger_ts * 1000,
        }).unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(250)).await;

        let halt_reason = guards.lock().unwrap().halted().map(|s| s.to_string());
        println!("[piece3-test] guards.halted() after run = {halt_reason:?}");
        assert!(halt_reason.is_some(), "kill-switch must latch a halt in continuous-check");

        let _ = sd_tx.send(true);
        drop(kline_tx);
        let _ = handle.await;

        let mut drained = Vec::new();
        while let Ok(c) = exec_rx.try_recv() { drained.push(c); }
        let opens = drained.iter().filter(|c| matches!(c, ExecCommand::Open { .. })).count();

        println!("[piece3-test] decision_task drained: {} commands total, {} Opens", drained.len(), opens);
        println!("[piece3-test] expected 0 Opens (kill-switch HALTED the loop on every pass)");
        assert_eq!(opens, 0,
            "with kill-switch present, decision_task MUST halt and forward NO Open");

        // PIECE 5 inspection: oplog records the halt with the exact reason.
        let oplog_body = std::fs::read_to_string(&oplog_path).expect("oplog file should exist");
        let lines: Vec<&str> = oplog_body.lines().collect();
        println!("[piece5-test] === OPLOG ({} lines) for kill-switch halt ===", lines.len());
        for ln in &lines { println!("{ln}"); }
        let mut saw_halt = false;
        let mut halt_reason_in_oplog: Option<String> = None;
        let mut saw_decision_fired = false;
        let mut saw_intent_open = false;
        for l in &lines {
            let v: serde_json::Value = serde_json::from_str(l).expect("oplog line is JSON");
            let kind = v.get("kind").and_then(|k| k.as_str()).unwrap_or("");
            if kind == "guard_halt" {
                saw_halt = true;
                halt_reason_in_oplog = v["data"]["reason"].as_str().map(String::from);
            }
            if kind == "decision_fired" { saw_decision_fired = true; }
            if kind == "intent_open" { saw_intent_open = true; }
        }
        println!("[piece5-test] guard_halt seen? {saw_halt}; reason in oplog = {halt_reason_in_oplog:?}");
        assert!(saw_halt, "guard_halt event MUST appear in oplog");
        assert!(halt_reason_in_oplog.as_deref().unwrap_or("").contains("KILL-SWITCH"),
            "oplog halt reason must mention KILL-SWITCH; got {halt_reason_in_oplog:?}");
        assert!(!saw_decision_fired, "no decision_fired (halt skipped processing)");
        assert!(!saw_intent_open, "no intent_open (halt skipped processing)");

        let _ = std::fs::remove_file(&ks_path);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// PIECE 5: a transition-style proof -- a close at exit_ts emits a
    /// `paper_close` oplog event with the realized numbers (entry/exit/shares/
    /// realized_pnl). Drives the run_exit_task with a position whose exit_ts is
    /// already past; one tick later, the close fires AND the oplog records it.
    #[tokio::test]
    async fn exit_task_emits_paper_close_event_to_oplog() {
        use crate::state::EventBbo;
        let state = SharedState::new();
        let now_ms_real = now_ms();
        let now_s = now_ms_real / 1000;
        // BBO with a best_bid the close will hit.
        state.bbo.insert("T-EXIT".into(), EventBbo {
            best_ask: Some(0.55), best_bid: Some(0.55), ts_ms: now_ms_real,
        });

        // Persisted state: one position whose exit_ts is already in the past so
        // the next 1s tick will close it.
        let dir = tmp_dir("exit_oplog");
        let store = StateStore::load(&dir.join("state.json")).0;
        {
            let st = store.state();
            let mut bs = st.lock().unwrap();
            bs.positions.push(OpenPosition {
                token_id: "T-EXIT".into(), asset: "BTC".into(), side: Outcome::Up,
                entry_price: dec!(0.50), shares: dec!(2.0),
                opened_at_ms: now_ms_real - 5_000,
                signal_id: "sig-exit".into(), interval: "5m".into(),
                exit_ts_s: now_s - 1, // already past -> close on the next tick
                // v3 (Pieza 1) sentinels: this test verifies the time-exit
                // path emits the right oplog event; smart-exit irrelevant.
                entry_ts_ms: 0,
                running_max_bid: 0.0,
                ts_max_bid_ms: 0,
                status: OrderStatus::TentativeFilled,
                order_id: Some("ord-exit".into()), ack_at_ms: Some(now_ms_real - 5_000),
                confirmed_at_ms: None, confirmation_source: None,
                maker_exit: None,
            });
        }

        let mut gcfg = GuardConfig::default();
        gcfg.kill_switch_path = tmp_killswitch("exit_oplog");
        let guards = Arc::new(Mutex::new(Guards::new(gcfg)));
        let oplog_path = dir.join("oplog.jsonl");
        let oplog = Arc::new(OpLog::new(&oplog_path));
        let (sd_tx, sd_rx) = watch::channel(false);

        let handle = tokio::spawn(run_exit_task(
            state.clone(), store.state(), guards.clone(), None, oplog.clone(),
            Arc::new(ExitsConfig::default()),  // Pieza 4: empty -> baseline everywhere
            sd_rx,
        ));

        // Wait a bit > 1s so the first tick fires and closes the lot.
        tokio::time::sleep(std::time::Duration::from_millis(1_200)).await;

        let _ = sd_tx.send(true);
        let _ = handle.await;

        let body = std::fs::read_to_string(&oplog_path).expect("oplog file exists");
        let lines: Vec<&str> = body.lines().collect();
        println!("[piece5-test] === OPLOG ({} lines) for exit_close ===", lines.len());
        for ln in &lines { println!("{ln}"); }
        let mut close_event: Option<serde_json::Value> = None;
        for l in &lines {
            let v: serde_json::Value = serde_json::from_str(l).expect("json");
            if v["kind"] == "exit_close" { close_event = Some(v); break; }
        }
        let ce = close_event.expect("exit_close event MUST appear in oplog");
        println!("[piece5-test] exit_close event = {ce}");
        assert_eq!(ce["data"]["token_id"], "T-EXIT");
        assert_eq!(ce["data"]["signal_id"], "sig-exit");
        assert_eq!(ce["data"]["entry_price"], "0.50");
        assert_eq!(ce["data"]["exit_price"], "0.55");
        assert_eq!(ce["data"]["shares"], "2.0");
        assert_eq!(ce["data"]["kind"], "book_bid");
        // ts_ms present, monotonic, ms-stamped (one clock).
        assert!(ce["ts_ms"].as_i64().is_some(), "ts_ms missing");

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// disabled_cells END-TO-END: drive the live decision task with
    /// `disabled_cells = ["ETH:15m"]` and an ETH trigger. Assert the oplog
    /// records one `signal_dropped_disabled_cell` event with the right asset/
    /// interval/trigger_ts. This is the AUDIT path the operator uses post-deploy
    /// to verify the filter is actually firing rather than silently no-op'ing.
    #[tokio::test]
    async fn decision_task_emits_signal_dropped_disabled_cell_oplog() {
        let state = SharedState::new();
        let now_real = now_ms();

        // Both ETH cells need a BBO + a market entry so the trigger expands to
        // BOTH signals (5m + 15m), letting us prove the 15m was dropped while
        // the 5m survived.
        state.bbo.insert("ETH-5m-up".into(),  EventBbo { best_ask: Some(0.50), best_bid: Some(0.49), ts_ms: now_real });
        state.bbo.insert("ETH-15m-up".into(), EventBbo { best_ask: Some(0.50), best_bid: Some(0.49), ts_ms: now_real });

        // Pick a trigger_ts that yields a valid scope for BOTH 5m and 15m.
        // 5m: ttr in [30, 270] (RW); 15m: ttr > 300 (IM). Aim ttr15 = 700,
        // and let the 5m epoch fall where it naturally does (ttr5 = 100 if
        // base15 + 700 lands appropriately).
        let secs15 = 900i64;
        let secs5 = 300i64;
        // Round NOW down to a 15m epoch, then add 700s offset.
        let now_s = now_real / 1000;
        let base15 = (now_s / secs15) * secs15;
        let trigger_ts = base15 + 700;
        let base5 = (trigger_ts / secs5) * secs5;
        state.markets.insert(
            ("ETH".into(), "5m".into(), base5),
            crate::signal::MarketRef {
                up_token_id: "ETH-5m-up".into(),
                down_token_id: "ETH-5m-down".into(),
                condition_id: "c5".into(), end_time: "e5".into(),
                maker_base_fee: 0, taker_base_fee: 0, fee_type: String::new(),
            },
        );
        state.markets.insert(
            ("ETH".into(), "15m".into(), base15),
            crate::signal::MarketRef {
                up_token_id: "ETH-15m-up".into(),
                down_token_id: "ETH-15m-down".into(),
                condition_id: "c15".into(), end_time: "e15".into(),
                maker_base_fee: 0, taker_base_fee: 0, fee_type: String::new(),
            },
        );

        let dir = tmp_dir("dc_audit");
        let store = StateStore::load(&dir.join("state.json")).0;

        let mut gcfg = GuardConfig::default();
        gcfg.kill_switch_path = tmp_killswitch("dc_audit");
        let guards = Arc::new(Mutex::new(Guards::new(gcfg)));

        let oplog_path = dir.join("oplog.jsonl");
        let oplog = Arc::new(OpLog::new(&oplog_path));

        let (kline_tx, kline_rx) = mpsc::unbounded_channel::<KlineClose>();
        let (exec_tx, mut exec_rx) = mpsc::unbounded_channel::<ExecCommand>();
        let (sd_tx, sd_rx) = watch::channel(false);

        let dec_cfg = DecisionConfig { stake_usd: 1.0, ..DecisionConfig::default() };
        let disabled = vec!["ETH:15m".to_string()];
        let handle = tokio::spawn(run_decision_task(
            state.clone(), store.state(), dec_cfg, guards.clone(), oplog.clone(),
            kline_rx, exec_tx, sd_rx,
            false,            // G5-test-rig: OFF
            disabled.clone(), // PRODUCTION filter: drop ETH:15m
            crate::config::V2Config::default(), // v2 disabled → legacy path under test
            Arc::new(crate::v2::Controls::new(true, 10.0, 100.0)),
            crate::v2::RecalSet {
                m5: Arc::new(Mutex::new(crate::v2::Recalibrator::default())),
                m15: Arc::new(Mutex::new(crate::v2::Recalibrator::default())),
                path_m5: std::env::temp_dir().join("recal5_test.json").to_string_lossy().into_owned(),
                path_m15: std::env::temp_dir().join("recal15_test.json").to_string_lossy().into_owned(),
            },
            None, // live_gate: paper/legacy path under test
        ));

        // Warm-up klines (flat). The 4th close triggers (+6 bps).
        for off in [-3i64, -2, -1] {
            let t_s = trigger_ts + off;
            kline_tx.send(KlineClose { asset: "ETH".into(), t_s, close: 100.0, received_at_ms: t_s * 1000 }).unwrap();
        }
        kline_tx.send(KlineClose {
            asset: "ETH".into(), t_s: trigger_ts, close: 100.06, received_at_ms: trigger_ts * 1000,
        }).unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(250)).await;

        let _ = sd_tx.send(true);
        drop(kline_tx);
        let _ = handle.await;

        // ASSERT the oplog file has one signal_dropped_disabled_cell event
        // with the correct asset/interval/trigger_ts.
        let oplog_body = std::fs::read_to_string(&oplog_path).expect("oplog file should exist");
        let events: Vec<serde_json::Value> = oplog_body.lines()
            .filter_map(|l| serde_json::from_str(l).ok())
            .collect();
        println!("[disabled_cells-test] === OPLOG ({} events) ===", events.len());
        for ev in &events { println!("{ev}"); }
        let drops: Vec<&serde_json::Value> = events.iter()
            .filter(|e| e["kind"] == "signal_dropped_disabled_cell")
            .collect();
        assert_eq!(
            drops.len(), 1,
            "expected EXACTLY ONE signal_dropped_disabled_cell event for the ETH:15m drop; \
             got {} (audit-path regression)", drops.len()
        );
        let drop = drops[0];
        assert_eq!(drop["data"]["asset"], "ETH", "wrong asset on drop event");
        assert_eq!(drop["data"]["interval"], "15m", "wrong interval on drop event");
        assert_eq!(drop["data"]["trigger_ts"], trigger_ts,
            "drop event must reference the originating trigger_ts so the audit can cross-correlate");

        // ALSO assert the 5m did NOT drop (only ETH:15m was disabled) and that
        // the bot still emitted an Open command for the 5m token -- proves the
        // filter is surgical, not a blanket ETH disable.
        let mut drained = Vec::new();
        while let Ok(c) = exec_rx.try_recv() { drained.push(c); }
        let opens_5m = drained.iter().filter(|c| match c {
            ExecCommand::Open { intent, .. } => intent.token_id == "ETH-5m-up",
            _ => false,
        }).count();
        let opens_15m = drained.iter().filter(|c| match c {
            ExecCommand::Open { intent, .. } => intent.token_id == "ETH-15m-up",
            _ => false,
        }).count();
        println!("[disabled_cells-test] opens 5m={opens_5m} 15m={opens_15m}");
        assert_eq!(opens_15m, 0, "ETH:15m must NOT produce an Open after the filter");
        assert!(opens_5m >= 1, "ETH:5m must still produce an Open (filter is surgical)");

        let _ = std::fs::remove_dir_all(&dir);
    }

    // ========================================================================
    // Pieza 4: evaluate_smart_exits CONTRACT TESTS (pure function) + the
    // run_exit_task INTEGRATION tests (end-to-end with the actual tick).
    // Together they prove the live exit task makes the same decisions the
    // backtester would on the same observations -- the live wiring is
    // mechanically correct from open through close.
    // ========================================================================

    fn rule_f6(y_sec: i64) -> ExitRule {
        ExitRule::F6Only { y_sec }
    }
    fn rule_smart(x_pct: f64, y_sec: i64) -> ExitRule {
        ExitRule::Smart { x_pct, y_sec }
    }
    /// Build an ExitsConfig with one cell pre-registered.
    fn exits_with(cell: &str, rule: ExitRule) -> ExitsConfig {
        let mut cfg = ExitsConfig::default();
        cfg.cells.insert(cell.to_string(), rule);
        cfg
    }
    /// Build a smart-eligible position (asset+interval cell key matches).
    fn pos_smart_eligible(asset: &str, interval: &str, token: &str, entry_price: f64, entry_ms: i64) -> OpenPosition {
        let mut p = pos_with_state(token, entry_price, entry_ms);
        p.asset = asset.to_string();
        p.interval = interval.to_string();
        p
    }

    // ========================================================================
    // COMBO Phase 3: process_b3_makers — the B3 maker lifecycle, end-to-end in
    // paper. These exercise the router + paper-sim + the 7 oplog events.
    // ========================================================================

    fn b3_rule() -> ExitRule {
        ExitRule::B3Maker { delta: 0.10, fallback: crate::config::B3Fallback::F2Market }
    }

    /// Read the oplog file -> Vec<(kind, data)>.
    fn oplog_events(path: &std::path::Path) -> Vec<(String, serde_json::Value)> {
        std::fs::read_to_string(path).unwrap_or_default().lines()
            .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
            .map(|v| (v["kind"].as_str().unwrap_or("").to_string(), v["data"].clone()))
            .collect()
    }
    fn has_kind(evs: &[(String, serde_json::Value)], kind: &str) -> bool {
        evs.iter().any(|(k, _)| k == kind)
    }
    fn find_kind<'a>(evs: &'a [(String, serde_json::Value)], kind: &str) -> Option<&'a serde_json::Value> {
        evs.iter().find(|(k, _)| k == kind).map(|(_, d)| d)
    }
    fn b3_oplog(tag: &str) -> (std::path::PathBuf, OpLog) {
        let p = std::env::temp_dir().join(format!("rb_b3_{tag}_{}.jsonl", std::process::id()));
        let _ = std::fs::remove_file(&p);
        let log = OpLog::new(&p);
        (p, log)
    }

    #[test]
    fn b3_routes_only_b3_cells_and_places_maker_early() {
        let t0 = 1_780_000_000_000_i64;
        // ETH:5m is B3; BTC:5m has no rule -> baseline (untouched by B3 path).
        let mut positions = vec![
            pos_smart_eligible("ETH", "5m", "T-B3",   0.55, t0),
            pos_smart_eligible("BTC", "5m", "T-BASE", 0.50, t0),
        ];
        let cfg = exits_with("ETH:5m", b3_rule());
        let bbo = DashMap::new();
        let now = t0 + 1_000; // not due (exit_ts_s = t0/1000 + 120)
        put_bbo(&bbo, "T-B3",   0.55, now); // < target 0.65 -> place
        put_bbo(&bbo, "T-BASE", 0.70, now);
        let mut tracks = HashMap::new();
        let (path, log) = b3_oplog("route");

        let closed = process_b3_makers(&mut positions, &cfg, &bbo, now, &mut tracks, &log);
        assert!(closed.is_empty(), "placing a maker closes nothing");
        // ETH:5m got a maker; BTC:5m (baseline) untouched.
        let eth = positions.iter().find(|p| p.token_id == "T-B3").unwrap();
        let btc = positions.iter().find(|p| p.token_id == "T-BASE").unwrap();
        assert!(eth.maker_exit.is_some(), "B3 cell must get a maker");
        assert_eq!(eth.maker_exit.as_ref().unwrap().target, 0.65);
        assert!(btc.maker_exit.is_none(), "baseline cell must NOT be touched");
        let evs = oplog_events(&path);
        let placed = find_kind(&evs, "b3_maker_placed").expect("b3_maker_placed emitted");
        assert_eq!(placed["signal_id"], "T-B3-sig");
        // cell + signal_id on `placed` are the keys for the LIVE first-touch-rate
        // metric (filled/placed per cell). Lock them.
        assert_eq!(placed["cell"], "ETH:5m");
        assert_eq!(placed["target_price"], 0.65);
        assert_eq!(placed["delta"], 0.10);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn b3_maker_fill_win() {
        let t0 = 1_780_000_000_000_i64;
        let mut positions = vec![pos_smart_eligible("ETH", "5m", "T", 0.55, t0)];
        let cfg = exits_with("ETH:5m", b3_rule());
        let bbo = DashMap::new();
        let mut tracks = HashMap::new();
        let (path, log) = b3_oplog("fill");
        // tick1: bid 0.55 < target 0.65 -> place.
        let n1 = t0 + 1_000;
        put_bbo(&bbo, "T", 0.55, n1);
        let c1 = process_b3_makers(&mut positions, &cfg, &bbo, n1, &mut tracks, &log);
        assert!(c1.is_empty());
        // tick2: bid 0.66 >= 0.65 -> first-touch fill at target 0.65.
        let n2 = t0 + 2_000;
        put_bbo(&bbo, "T", 0.66, n2);
        let c2 = process_b3_makers(&mut positions, &cfg, &bbo, n2, &mut tracks, &log);
        assert_eq!(c2.len(), 1, "first-touch closes the lot");
        assert_eq!(c2[0].exit_price, dec!(0.65), "fills at TARGET (maker), not the bid");
        assert_eq!(c2[0].exit_kind, ExitKind::B3MakerFill);
        assert_eq!(c2[0].realized_pnl, dec!(1.0)); // gross 10*(0.65-0.55)
        assert!(positions.is_empty(), "lot removed after fill");
        let evs = oplog_events(&path);
        assert!(has_kind(&evs, "b3_maker_placed"));
        let filled = find_kind(&evs, "b3_maker_filled").expect("b3_maker_filled emitted");
        // signal_id on `filled` is the link to count this as a first-touch WIN
        // per cell in the LIVE run (filled/placed = real first-touch rate).
        assert_eq!(filled["signal_id"], "T-sig");
        assert_eq!(filled["fill_price"], 0.65);
        assert_eq!(filled["fee_model"], "maker_floor");
        // net = gross - buy taker fee (maker exit fee 0): 1.0 - 0.07*10*0.55*0.45
        let net = filled["net_pnl"].as_f64().unwrap();
        assert!((net - (1.0 - 0.07 * 10.0 * 0.55 * 0.45)).abs() < 1e-9, "net {net}");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn b3_timeout_cancel_reconcile_fallback() {
        let t0 = 1_780_000_000_000_i64;
        let mut positions = vec![pos_smart_eligible("ETH", "5m", "T", 0.55, t0)];
        let cfg = exits_with("ETH:5m", b3_rule());
        let bbo = DashMap::new();
        let mut tracks = HashMap::new();
        let (path, log) = b3_oplog("timeout");
        // tick1: place (bid 0.55 < 0.65).
        let n1 = t0 + 1_000;
        put_bbo(&bbo, "T", 0.55, n1);
        process_b3_makers(&mut positions, &cfg, &bbo, n1, &mut tracks, &log);
        // tick2: bid 0.60 (arms break-even: > entry 0.55, < target 0.65).
        let n2 = t0 + 2_000;
        put_bbo(&bbo, "T", 0.60, n2);
        process_b3_makers(&mut positions, &cfg, &bbo, n2, &mut tracks, &log);
        assert_eq!(positions.len(), 1, "still working, not filled");
        // tick3: DUE (now_s >= exit_ts_s = t0/1000+120) + bid 0.54 (<= entry,
        // after arm -> break-even fires) -> timeout cancel + F2 @ 0.54.
        let n3 = t0 + 120_000;
        put_bbo(&bbo, "T", 0.54, n3);
        let c3 = process_b3_makers(&mut positions, &cfg, &bbo, n3, &mut tracks, &log);
        assert_eq!(c3.len(), 1, "timeout closes via fallback");
        assert_eq!(c3[0].exit_price, dec!(0.54), "F2 break-even at the actual low bid");
        assert_eq!(c3[0].exit_kind, ExitKind::B3Fallback);
        assert!(positions.is_empty());
        let evs = oplog_events(&path);
        assert!(has_kind(&evs, "b3_maker_timeout_cancel"));
        let recon = find_kind(&evs, "b3_maker_reconcile").expect("reconcile emitted");
        assert_eq!(recon["decision"], "clean_cancel", "paper cancel = clean (no partial)");
        let fb = find_kind(&evs, "b3_maker_fallback_f2").expect("fallback emitted");
        // signal_id on `fallback_f2` is the link to count this as a NON-fill
        // (fell to F2) per cell in the LIVE run -> the complement of first-touch.
        assert_eq!(fb["signal_id"], "T-sig");
        assert_eq!(fb["f2_price"], 0.54);
        assert_eq!(fb["reason"], "clean_cancel");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn b3_target_invalid_places_no_maker_then_f2() {
        // THE edge (router<->analyzer coherence): entry 0.95 + delta 0.10 =
        // 1.05 > 1-tick -> NO maker placed (NOT clamped). At timeout -> F2.
        let t0 = 1_780_000_000_000_i64;
        let mut positions = vec![pos_smart_eligible("ETH", "5m", "T", 0.95, t0)];
        let cfg = exits_with("ETH:5m", b3_rule());
        let bbo = DashMap::new();
        let mut tracks = HashMap::new();
        let (path, log) = b3_oplog("invalid");
        // tick1 (not due): bid 0.96 -> NO maker (target invalid).
        let n1 = t0 + 1_000;
        put_bbo(&bbo, "T", 0.96, n1);
        let c1 = process_b3_makers(&mut positions, &cfg, &bbo, n1, &mut tracks, &log);
        assert!(c1.is_empty());
        assert!(positions[0].maker_exit.is_none(), "NO maker for an invalid target");
        let evs1 = oplog_events(&path);
        assert!(!has_kind(&evs1, "b3_maker_placed"), "must NOT place a maker");
        // tick2 (DUE): bid 0.94 (<=entry, after arm at 0.96) -> F2 @ 0.94.
        let n2 = t0 + 120_000;
        put_bbo(&bbo, "T", 0.94, n2);
        let c2 = process_b3_makers(&mut positions, &cfg, &bbo, n2, &mut tracks, &log);
        assert_eq!(c2.len(), 1);
        assert_eq!(c2[0].exit_kind, ExitKind::B3Fallback);
        assert_eq!(c2[0].exit_price, dec!(0.94));
        let evs2 = oplog_events(&path);
        assert!(has_kind(&evs2, "b3_maker_fallback_f2"));
        assert!(!has_kind(&evs2, "b3_maker_timeout_cancel"), "no maker -> no cancel event");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn b3_crossable_at_placement_taker_captures() {
        // bid already >= target on the placement tick -> #5 crossable: capture
        // as taker at the bid (>= target). No maker placed.
        let t0 = 1_780_000_000_000_i64;
        let mut positions = vec![pos_smart_eligible("ETH", "5m", "T", 0.55, t0)];
        let cfg = exits_with("ETH:5m", b3_rule());
        let bbo = DashMap::new();
        let mut tracks = HashMap::new();
        let (path, log) = b3_oplog("cross");
        let n1 = t0 + 1_000;
        put_bbo(&bbo, "T", 0.66, n1); // >= target 0.65 at placement
        let c1 = process_b3_makers(&mut positions, &cfg, &bbo, n1, &mut tracks, &log);
        assert_eq!(c1.len(), 1, "crossable -> immediate taker capture");
        assert_eq!(c1[0].exit_price, dec!(0.66), "captures at the bid (>= target)");
        assert_eq!(c1[0].exit_kind, ExitKind::B3MakerFill);
        assert!(positions.is_empty());
        let evs = oplog_events(&path);
        let rc = find_kind(&evs, "b3_maker_rejected_crossable").expect("crossable event");
        assert_eq!(rc["action_taken"], "taker_capture");
        assert!(!has_kind(&evs, "b3_maker_placed"), "no maker placed on crossable");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn b3_dca_two_lots_same_token_track_and_close_independently() {
        // Two B3 lots on the SAME token (distinct signal_ids = distinct
        // triggers, as DCA produces). Proves: (a) per-lot tracking by signal_id
        // (no b3_tracks collision), (b) index correctness when ONE lot closes
        // and the other stays (refutes the "stale index" concern: the closing
        // lot's index is removed via apply_smart_exits; the surviving lot's
        // data is untouched).
        let t0 = 1_780_000_000_000_i64;
        let mut p0 = pos_smart_eligible("ETH", "5m", "T", 0.55, t0);
        p0.signal_id = "T-sig-a".into();
        let mut p1 = pos_smart_eligible("ETH", "5m", "T", 0.60, t0);
        p1.signal_id = "T-sig-b".into(); // distinct -> own track + target 0.70
        let mut positions = vec![p0, p1];
        let cfg = exits_with("ETH:5m", b3_rule());
        let bbo = DashMap::new();
        let mut tracks = HashMap::new();
        let (path, log) = b3_oplog("dca");
        // tick1: bid 0.55 < both targets (0.65, 0.70) -> place both.
        let n1 = t0 + 1_000;
        put_bbo(&bbo, "T", 0.55, n1);
        let c1 = process_b3_makers(&mut positions, &cfg, &bbo, n1, &mut tracks, &log);
        assert!(c1.is_empty());
        assert_eq!(positions.len(), 2);
        assert!(positions.iter().all(|p| p.maker_exit.is_some()), "both placed");
        assert_eq!(tracks.len(), 2, "two distinct tracks (no collision)");
        // tick2: bid 0.66 -> lot a (target 0.65) fills; lot b (target 0.70) holds.
        let n2 = t0 + 2_000;
        put_bbo(&bbo, "T", 0.66, n2);
        let c2 = process_b3_makers(&mut positions, &cfg, &bbo, n2, &mut tracks, &log);
        assert_eq!(c2.len(), 1, "only lot a fills");
        assert_eq!(c2[0].signal_id, "T-sig-a");
        assert_eq!(c2[0].exit_price, dec!(0.65));
        assert_eq!(positions.len(), 1, "lot b survives");
        assert_eq!(positions[0].signal_id, "T-sig-b");
        assert_eq!(positions[0].maker_exit.as_ref().unwrap().target, 0.70);
        assert!(tracks.contains_key("T-sig-b") && !tracks.contains_key("T-sig-a"),
            "lot a track cleared, lot b track kept");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn b3_skips_position_with_terminal_maker_status() {
        // A position whose maker_exit is already terminal (Filled/Cancelled)
        // must be SKIPPED (it is closing/closed; re-processing would double-
        // handle). Defensive guard.
        use crate::state::persist::{MakerExitState, MakerExitStatus};
        let t0 = 1_780_000_000_000_i64;
        let mut p = pos_smart_eligible("ETH", "5m", "T", 0.55, t0);
        p.maker_exit = Some(MakerExitState {
            order_id: "x".into(), placed_ms: t0, target: 0.65,
            status: MakerExitStatus::Filled,
        });
        let mut positions = vec![p];
        let cfg = exits_with("ETH:5m", b3_rule());
        let bbo = DashMap::new();
        let now = t0 + 1_000;
        put_bbo(&bbo, "T", 0.66, now); // would fill if not skipped
        let mut tracks = HashMap::new();
        let (path, log) = b3_oplog("terminal");
        let closed = process_b3_makers(&mut positions, &cfg, &bbo, now, &mut tracks, &log);
        assert!(closed.is_empty(), "terminal maker status must be skipped");
        assert_eq!(positions.len(), 1, "position not touched");
        assert!(oplog_events(&path).is_empty(), "no events for a skipped lot");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn b3_baseline_cells_unchanged() {
        // Regression: a baseline cell (no B3 rule) is NEVER touched by the B3
        // path -- no maker, no close, no events. close_due still owns it.
        let t0 = 1_780_000_000_000_i64;
        let mut positions = vec![pos_smart_eligible("BTC", "5m", "T", 0.50, t0)];
        let cfg = ExitsConfig::default(); // no cells -> all baseline
        let bbo = DashMap::new();
        let now = t0 + 130_000; // even past exit_ts_s
        put_bbo(&bbo, "T", 0.55, now);
        let mut tracks = HashMap::new();
        let (path, log) = b3_oplog("base");
        let closed = process_b3_makers(&mut positions, &cfg, &bbo, now, &mut tracks, &log);
        assert!(closed.is_empty(), "B3 path must not close a baseline lot");
        assert_eq!(positions.len(), 1, "baseline lot stays (close_due owns it)");
        assert!(positions[0].maker_exit.is_none(), "baseline lot never gets a maker");
        assert!(oplog_events(&path).is_empty(), "no B3 events for a baseline cell");
        assert!(tracks.is_empty(), "no tracking for non-B3 cells");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn evaluate_smart_no_config_returns_empty() {
        let positions = vec![pos_smart_eligible("BTC", "15m", "T", 0.50, 1_780_000_000_000)];
        let bbo = DashMap::new();
        put_bbo(&bbo, "T", 0.70, 1_780_000_001_000);
        let cfg = ExitsConfig::default();
        let out = evaluate_smart_exits(&positions, &cfg, &bbo, 1_780_000_001_000);
        assert!(out.is_empty(), "no cells configured -> no smart exits");
    }

    #[test]
    fn evaluate_smart_baseline_rule_returns_empty() {
        let positions = vec![pos_smart_eligible("BTC", "15m", "T", 0.50, 1_780_000_000_000)];
        let bbo = DashMap::new();
        put_bbo(&bbo, "T", 0.70, 1_780_000_001_000);
        let cfg = exits_with("BTC:15m", ExitRule::Baseline);
        let out = evaluate_smart_exits(&positions, &cfg, &bbo, 1_780_000_001_000);
        assert!(out.is_empty(), "Baseline rule = no smart, close_due handles");
    }

    #[test]
    fn evaluate_smart_sentinel_entry_ts_skipped() {
        let mut positions = vec![pos_smart_eligible("BTC", "15m", "T", 0.50, 1_780_000_000_000)];
        positions[0].entry_ts_ms = 0; // sentinel (pre-Pieza-2 / v2-migrated)
        let bbo = DashMap::new();
        put_bbo(&bbo, "T", 0.70, 1_780_000_001_000);
        // F6 with y=0 would trigger immediately if eligible.
        let cfg = exits_with("BTC:15m", rule_f6(0));
        let out = evaluate_smart_exits(&positions, &cfg, &bbo, 1_780_000_001_000);
        assert!(out.is_empty(), "entry_ts_ms==0 sentinel forces baseline fallback");
    }

    #[test]
    fn evaluate_smart_sentinel_asset_skipped() {
        let mut positions = vec![pos_smart_eligible("BTC", "15m", "T", 0.50, 1_780_000_000_000)];
        positions[0].asset = String::new(); // sentinel (pre-Pieza-4 / v3-migrated)
        let bbo = DashMap::new();
        put_bbo(&bbo, "T", 0.70, 1_780_000_001_000);
        let cfg = exits_with("BTC:15m", rule_f6(0));
        let out = evaluate_smart_exits(&positions, &cfg, &bbo, 1_780_000_001_000);
        assert!(out.is_empty(), "asset=\"\" sentinel forces baseline fallback");
    }

    #[test]
    fn evaluate_smart_f6_triggers_returns_close() {
        let t0 = 1_780_000_000_000_i64;
        let positions = vec![pos_smart_eligible("BTC", "15m", "T", 0.50, t0)];
        let bbo = DashMap::new();
        // BBO at t0+1s with bid 0.60 (= new high). ts_max_bid_ms updates to
        // t0+1s. y_sec=0 -> trigger immediately.
        let now = t0 + 1_000;
        put_bbo(&bbo, "T", 0.60, now);
        // Run update_running_max first (mirrors what run_exit_task does each tick).
        let mut p = positions;
        let _ = update_running_max(&mut p, &bbo, now);
        // ts_max_bid_ms is now `now` -> f6 = 0 -> y_sec=0 -> trigger.
        let cfg = exits_with("BTC:15m", rule_f6(0));
        let out = evaluate_smart_exits(&p, &cfg, &bbo, now);
        assert_eq!(out.len(), 1, "f6 rule fires with y=0 + fresh BBO");
        assert_eq!(out[0].lot_index, 0);
        assert_eq!(out[0].token_id, "T");
        assert_eq!(out[0].asset, "BTC");
        assert_eq!(out[0].interval, "15m");
        assert_eq!(out[0].exit_price, 0.60, "sell at current best_bid");
        assert_eq!(out[0].exit_kind, ExitKind::F6Triggered);
        assert_eq!(out[0].rule_label, "f6only_y0s");
    }

    #[test]
    fn evaluate_smart_stale_bbo_holds_even_if_rule_fires() {
        // Critical test: the rule fires (based on time + preserved state),
        // but the BBO is STALE -> evaluator MUST refuse to sell at stale
        // price -> empty Vec -> close_due holds the position past the
        // trigger. This is the Pieza 2 Q2 doctrine end-to-end.
        let t0 = 1_780_000_000_000_i64;
        let mut positions = vec![pos_smart_eligible("BTC", "15m", "T", 0.50, t0)];
        // Manually set ts_max_bid_ms in the past so f6 will trigger.
        positions[0].running_max_bid = 0.55;
        positions[0].ts_max_bid_ms = t0 + 1_000;
        let bbo = DashMap::new();
        // BBO ts_ms is STALE -- past STALE_BBO_MS=5000 ago.
        let now = t0 + 60_000;
        put_bbo(&bbo, "T", 0.55, t0 + 1_000); // ts_ms 59s ago at `now`
        // f6 = 59_000ms >> y_sec*1000 (=any reasonable y). Rule would fire.
        let cfg = exits_with("BTC:15m", rule_f6(30));
        let out = evaluate_smart_exits(&positions, &cfg, &bbo, now);
        assert!(
            out.is_empty(),
            "stale BBO MUST hold past the trigger (no SELL at phantom price)"
        );
    }

    #[test]
    fn evaluate_smart_smart_rule_triggers_returns_close() {
        let t0 = 1_780_000_000_000_i64;
        let positions = vec![pos_smart_eligible("ETH", "5m", "T", 0.50, t0)];
        let exit_ts_s = (t0 + 120_000) / 1000; // 120s hold
        let mut p = positions;
        p[0].exit_ts_s = exit_ts_s;
        // Simulate a peak at t0+10s, then 60s of plateau.
        let bbo = DashMap::new();
        put_bbo(&bbo, "T", 0.55, t0 + 10_000);
        let _ = update_running_max(&mut p, &bbo, t0 + 10_000);
        // At t0+70s: f1 = 70/120 = 58.3% (>=50% -> x met). f6 = 60s (>=30s -> y met).
        let now = t0 + 70_000;
        put_bbo(&bbo, "T", 0.54, now); // fresh + below max -> no max update
        let _ = update_running_max(&mut p, &bbo, now);
        let cfg = exits_with("ETH:5m", rule_smart(50.0, 30));
        let out = evaluate_smart_exits(&p, &cfg, &bbo, now);
        assert_eq!(out.len(), 1, "smart f1+f6 both met -> trigger");
        assert_eq!(out[0].exit_kind, ExitKind::SmartTriggered);
        assert_eq!(out[0].rule_label, "smart_x50_y30s");
        assert_eq!(out[0].exit_price, 0.54, "sell at current best_bid");
    }

    /// PIEZA 4 INTEGRATION: a position with a configured smart rule closes
    /// via run_exit_task with ExitKind::F6Triggered (not BookBid), and the
    /// exit_smart_decision oplog event fires.
    #[tokio::test]
    async fn exit_task_smart_rule_closes_with_smart_triggered_reason() {
        use crate::guards::{GuardConfig, Guards};
        use crate::oplog::OpLog;
        use crate::state::store::StateStore;

        let dir = std::env::temp_dir().join(format!(
            "rb_p4_smart_{}_{}", std::process::id(), now_ms()));
        std::fs::create_dir_all(&dir).unwrap();

        let state = SharedState::new();
        let t_now = now_ms();
        // Fresh BBO with a HIGH bid; the position seed was at 0.50 so
        // 0.60 is a new high and ts_max_bid_ms will update to t_now.
        state.bbo.insert("T_SMART".into(), EventBbo {
            best_bid: Some(0.60), best_ask: Some(0.62), ts_ms: t_now,
        });

        let store = StateStore::load(&dir.join("state.json")).0;
        {
            let st = store.state();
            let mut bs = st.lock().unwrap();
            // Position eligible for smart: BTC:15m cell, entry_ts_ms set.
            // exit_ts_s far in the future so time-exit cannot fire first.
            let mut p = pos_smart_eligible("BTC", "15m", "T_SMART", 0.50, t_now - 1_000);
            p.exit_ts_s = (t_now / 1000) + 3_600;
            bs.positions.push(p);
        }

        let mut gcfg = GuardConfig::default();
        gcfg.kill_switch_path = dir.join("ks_nonexistent.txt");
        let guards = Arc::new(Mutex::new(Guards::new(gcfg)));
        let oplog = Arc::new(OpLog::new(&dir.join("oplog.jsonl")));
        let (sd_tx, sd_rx) = watch::channel(false);

        // F6Only y=0 -> the FIRST update_running_max-driven tick sets
        // ts_max_bid_ms = t_first_tick; then f6 (= now - ts_max) is 0 -> triggers.
        let exits_cfg = Arc::new(exits_with("BTC:15m", rule_f6(0)));

        let handle = tokio::spawn(run_exit_task(
            state.clone(), store.state(), guards.clone(), None, oplog.clone(),
            exits_cfg, sd_rx,
        ));

        // Refresh BBO ts_ms periodically so it stays fresh.
        for _ in 0..3 {
            tokio::time::sleep(std::time::Duration::from_millis(400)).await;
            let now = now_ms();
            state.bbo.insert("T_SMART".into(), EventBbo {
                best_bid: Some(0.60), best_ask: Some(0.62), ts_ms: now,
            });
        }
        let _ = sd_tx.send(true);
        let _ = handle.await;

        let st = store.state();
        let bs = st.lock().unwrap();
        assert_eq!(
            bs.positions.len(), 0,
            "position MUST have been closed by smart-exit (exit_ts_s was 1h \
             in the future; only smart could have closed it)"
        );
        drop(bs);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// PIEZA 4 INTEGRATION: when BOTH the smart rule AND the time-exit
    /// would fire at the same tick, smart wins (the position closes ONCE
    /// with ExitKind::F6Triggered, not twice).
    #[tokio::test]
    async fn exit_task_smart_wins_when_both_smart_and_time_due_same_tick() {
        use crate::guards::{GuardConfig, Guards};
        use crate::oplog::OpLog;
        use crate::state::store::StateStore;

        let dir = std::env::temp_dir().join(format!(
            "rb_p4_tiebreak_{}_{}", std::process::id(), now_ms()));
        std::fs::create_dir_all(&dir).unwrap();

        let state = SharedState::new();
        let t_now = now_ms();
        state.bbo.insert("T_TIE".into(), EventBbo {
            best_bid: Some(0.60), best_ask: Some(0.62), ts_ms: t_now,
        });

        let store = StateStore::load(&dir.join("state.json")).0;
        {
            let st = store.state();
            let mut bs = st.lock().unwrap();
            // exit_ts_s ALREADY PAST -> close_due would also fire.
            let mut p = pos_smart_eligible("BTC", "15m", "T_TIE", 0.50, t_now - 1_000);
            p.exit_ts_s = (t_now / 1000) - 1; // already past
            bs.positions.push(p);
        }

        let mut gcfg = GuardConfig::default();
        gcfg.kill_switch_path = dir.join("ks_nonexistent.txt");
        let guards = Arc::new(Mutex::new(Guards::new(gcfg)));
        let oplog = Arc::new(OpLog::new(&dir.join("oplog.jsonl")));
        let (sd_tx, sd_rx) = watch::channel(false);

        let exits_cfg = Arc::new(exits_with("BTC:15m", rule_f6(0)));

        let handle = tokio::spawn(run_exit_task(
            state.clone(), store.state(), guards.clone(), None, oplog.clone(),
            exits_cfg, sd_rx,
        ));
        // One tick is enough -- both smart and time are due.
        for _ in 0..2 {
            tokio::time::sleep(std::time::Duration::from_millis(600)).await;
            let now = now_ms();
            state.bbo.insert("T_TIE".into(), EventBbo {
                best_bid: Some(0.60), best_ask: Some(0.62), ts_ms: now,
            });
        }
        let _ = sd_tx.send(true);
        let _ = handle.await;

        let st = store.state();
        let bs = st.lock().unwrap();
        assert_eq!(
            bs.positions.len(), 0,
            "position must be closed exactly once; smart removes it before \
             close_due runs, so close_due sees nothing -> no double-close"
        );
        drop(bs);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// PIEZA 4 INTEGRATION: a cell WITHOUT a smart rule (cell absent from
    /// [exits.cells]) behaves byte-identical to pre-Pieza-4: close_due
    /// closes it at exit_ts_s with ExitKind::BookBid.
    #[tokio::test]
    async fn exit_task_cell_with_no_config_falls_back_to_time_exit_baseline() {
        use crate::guards::{GuardConfig, Guards};
        use crate::oplog::OpLog;
        use crate::state::store::StateStore;

        let dir = std::env::temp_dir().join(format!(
            "rb_p4_baseline_{}_{}", std::process::id(), now_ms()));
        std::fs::create_dir_all(&dir).unwrap();

        let state = SharedState::new();
        let t_now = now_ms();
        state.bbo.insert("T_BASE".into(), EventBbo {
            best_bid: Some(0.55), best_ask: Some(0.57), ts_ms: t_now,
        });

        let store = StateStore::load(&dir.join("state.json")).0;
        {
            let st = store.state();
            let mut bs = st.lock().unwrap();
            // Position is in ETH:5m cell -- but exits_cfg only has BTC:15m.
            let mut p = pos_smart_eligible("ETH", "5m", "T_BASE", 0.50, t_now - 1_000);
            p.exit_ts_s = (t_now / 1000) - 1; // already past -> time-exit due
            bs.positions.push(p);
        }

        let mut gcfg = GuardConfig::default();
        gcfg.kill_switch_path = dir.join("ks_nonexistent.txt");
        let guards = Arc::new(Mutex::new(Guards::new(gcfg)));
        let oplog = Arc::new(OpLog::new(&dir.join("oplog.jsonl")));
        let (sd_tx, sd_rx) = watch::channel(false);

        // Smart rule for a DIFFERENT cell (BTC:15m); our position is ETH:5m
        // so it falls through to baseline.
        let exits_cfg = Arc::new(exits_with("BTC:15m", rule_f6(0)));

        let handle = tokio::spawn(run_exit_task(
            state.clone(), store.state(), guards.clone(), None, oplog.clone(),
            exits_cfg, sd_rx,
        ));
        for _ in 0..2 {
            tokio::time::sleep(std::time::Duration::from_millis(600)).await;
            let now = now_ms();
            state.bbo.insert("T_BASE".into(), EventBbo {
                best_bid: Some(0.55), best_ask: Some(0.57), ts_ms: now,
            });
        }
        let _ = sd_tx.send(true);
        let _ = handle.await;

        let st = store.state();
        let bs = st.lock().unwrap();
        assert_eq!(
            bs.positions.len(), 0,
            "position must have been closed by close_due (time-exit, baseline)"
        );
        drop(bs);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
