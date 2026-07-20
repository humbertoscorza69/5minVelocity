//! Typed configuration loaded from `bot.toml`.
//!
//! Phase 0: the full schema is declared and validated, but most fields are not
//! yet consumed by real logic (the 9 tasks are stubs). They get wired into real
//! logic in later phases.
//!
//! IMPORTANT: this struct holds NO secrets. Credentials live in `.env` and are
//! read separately via `dotenvy`. That invariant is what makes `--dry-run`
//! config dumping safe.

// Rust's dead-code analysis ignores derived Debug/Clone, so fields only reached
// via the logged `Debug` impl still warn until real logic reads them. Suppress
// that Phase 0 noise here; remove as fields get wired in later phases.
#![allow(dead_code)]

use std::collections::HashMap;
use std::path::Path;

use anyhow::Context;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub mode: ModeConfig,
    pub markets: MarketsConfig,
    pub stakes: StakesConfig,
    pub filters: FiltersConfig,
    pub gates: GatesConfig,
    pub rules: RulesConfig,
    pub connections: ConnectionsConfig,
    pub logging: LoggingConfig,
    pub paths: PathsConfig,
    /// Per-cell exit-rule selection. Absent (or empty) means every cell uses
    /// Baseline (time-exit), which is byte-identical to pre-Pieza-1 behavior.
    /// Pieza 1 only adds the SCHEMA + per-position state; the run_exit_task
    /// is unchanged. The wiring that consumes this lives in later pieces.
    #[serde(default)]
    pub exits: ExitsConfig,
    /// v2 strategy (5minSnip port): vol-normalized z edge-gate + depth-aware
    /// edge-proportional sizing + rolling recalibration. Absent → disabled
    /// (the bot runs the original 5bps+band strategy, byte-identical). Opt-in
    /// via `[v2] enabled = true`. See `crate::v2`.
    #[serde(default)]
    pub v2: V2Config,
    /// Live dashboard HTTP server. Absent → disabled. Bound to 127.0.0.1 by
    /// default (reachable only via SSH tunnel). See `crate::dashboard`.
    #[serde(default)]
    pub dashboard: DashboardConfig,
}

/// Dashboard HTTP server config. Bind to localhost + tunnel the port.
#[derive(Debug, Clone, Deserialize)]
pub struct DashboardConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "d_dash_bind")]
    pub bind: String,
    #[serde(default = "d_dash_port")]
    pub port: u16,
}

fn d_dash_bind() -> String { "127.0.0.1".to_string() }
fn d_dash_port() -> u16 { 8787 }

impl Default for DashboardConfig {
    fn default() -> Self {
        Self { enabled: false, bind: d_dash_bind(), port: d_dash_port() }
    }
}

/// v2 strategy configuration. All fields have serde defaults so an absent
/// `[v2]` block parses to `V2Config::default()` (disabled). When `enabled`, the
/// decision loop runs the parallel `decide_v2` path instead of the 5bps trigger.
#[derive(Debug, Clone, Deserialize)]
pub struct V2Config {
    /// Master switch. Default false = original strategy (no behavior change).
    #[serde(default)]
    pub enabled: bool,
    /// Entry gate: take iff `edge = pcal(z) - ask - fee >= edge_min`.
    #[serde(default = "d_edge_min")]
    pub edge_min: f64,
    /// Skip when rolling vol (bps/s) exceeds this (high-vol = negative-EV regime).
    #[serde(default = "d_vol_cap")]
    pub vol_cap: f64,
    /// Light disp/vel floor (drop genuine coin-flip spikes).
    #[serde(default = "d_dvr_floor")]
    pub dvr_floor: f64,
    /// Reference edge at which we stake `stakes.base_usdc` (linear edge sizing).
    #[serde(default = "d_edge_ref")]
    pub edge_ref: f64,
    /// Min vol-normalized displacement `z` to enter. Below this the move is in the
    /// calibration's near-zero region (win prob ~0.5) and is NOT predictive live --
    /// the key fix for the bot firing on noise at window open. 0.45 ~= the 0.68
    /// May-win-rate knot (the backtested edge region).
    #[serde(default = "d_z_min")]
    pub z_min: f64,
    /// Min seconds-to-settle for an entry (avoid the last-N-seconds chop).
    #[serde(default = "d_v2_min_ttl")]
    pub min_ttl_s: i64,
    /// MAX seconds-to-settle for a 5m entry (skip the first minute of the window).
    /// Live + recorder both show early-window entries (ttl>~240s) are a −EV drag:
    /// small displacement rides a noisy z, the 60s vol still spans the prior window,
    /// and 4+ min of remaining time gives the move max room to revert. The recorder
    /// backtests that validated the strategy never held a ttl>180s trade, so this
    /// closes an un-validated envelope. `0` = off (pre-July-2 behavior).
    #[serde(default = "d_v2_max_ttl")]
    pub max_ttl_s: i64,
    /// Rolling-vol lookback (seconds) for z. 60s is right for the 5m horizon; the
    /// 15m override uses 120s (a 60s window is too twitchy for a 900s market).
    #[serde(default = "d_vol_lb_5m")]
    pub vol_lookback_s: i64,
    /// Frozen-tape gate (5m): skip an entry if the Binance price hasn't ticked in
    /// this many seconds — the triggering move is stale and the book has caught up.
    /// Validated on 26 recorder days (frozen entries −0.04/$1, monotone in staleness,
    /// self-throttling by regime). `0` = off. 3 = the validated setting.
    #[serde(default = "d_frozen_tape")]
    pub frozen_tape_secs: i64,
    /// BOOK-UNMOVED entry gate: skip an entry unless the PM book's mid has NOT
    /// moved in the ~3s before the decision (mid_move_3s == 0). With the
    /// frozen-tape gate (Binance side) this is the literal lag detector — spot
    /// moved, book hasn't. Entries where the book had ALREADY repriced bled
    /// -$0.21/trade (84% of the Jul 4-5 loss; 64 trades, live fills); unmoved
    /// entries were breakeven on dead tape and +0.65/$1 idealized on normal
    /// recorder tape (n=2,341, twice-replicated). Skips ONLY affirmative movement
    /// (mid_move_3s > 0); an unobservable book (no mid history in the 3s window)
    /// passes rather than being silently starved. `true` = on (default). The 15m
    /// override lives in [v2.i15m]; both default ON.
    #[serde(default = "d_true")]
    pub book_unmoved_gate: bool,
    /// RE-ENTRY after a band-stop (Handoff #3 feature A). When a position is closed
    /// by the bid-band invalidation stop, the market becomes eligible for ONE more
    /// entry (max 2/market total), EITHER side, awarded FCFS to the first fresh
    /// signal clearing the full gate stack (edge>=0.06, z>=0.45 side-signed for the
    /// new side, ttl>=30, frozen-tape, book-unmoved on the new token). Validated:
    /// opposite-side +0.144/$1 (CI [0.098,0.191], all windows/IS/OOS/LOWO), same-
    /// side +0.132/$1; EV concentrates ~10-30s after a hi-band stop (we sold >=0.50,
    /// contra sits cheap). NOT the killed instant-flip: median re-entry is 33s out
    /// and must clear a fresh gate. Expected +23.5% volume, ~+26% $/day. Default ON.
    #[serde(default = "d_true")]
    pub reentry_enabled: bool,
    /// Independent per-side re-entry toggles (Handoff #4 Decision 3 kill-rule).
    /// `reentry_enabled` is the master; these gate the two sides separately so the
    /// pre-registered rule "if same-side cumulative net < -$15 at n=100, disable
    /// SAME-side only" is a config flip, not a redeploy. Same-side is the weaker
    /// leg (live −0.356/$1 at n=17 vs recorder +0.132 at n=380 — watch, don't
    /// panic); opposite is the validated winner (+0.144/$1). Both default ON.
    #[serde(default = "d_true")]
    pub reentry_same_enabled: bool,
    #[serde(default = "d_true")]
    pub reentry_opposite_enabled: bool,
    /// SIZING TIERS (Order #5 Part A). Burst = max side-aligned 1s/3s Binance
    /// return (bps) over entry-5s..entry (already logged as burst_bps). stake =
    /// base × burst_mult, where burst_bps < lo → ×1, [lo,hi) → mult_lo, ≥ hi →
    /// mult_hi. Pre-registered +26% $/day, EV/std +16%, no drawdown increase;
    /// paper-confirmed above model twice. A2: tick_age_s == 0 stacks ×tickage_mult
    /// multiplicatively. Total capped at stake_mult_cap (× base). All in config so
    /// the ladder can retune without a redeploy. Defaults 3/8 bps, ×2/×3, ×1.25,
    /// cap ×3 → stakes $1.05 / $2.10 / $2.63 / $3.15 at base $1.05.
    #[serde(default = "d_burst_lo")]
    pub burst_lo_bps: f64,
    #[serde(default = "d_burst_hi")]
    pub burst_hi_bps: f64,
    #[serde(default = "d_burst_mult_lo")]
    pub burst_mult_lo: f64,
    #[serde(default = "d_burst_mult_hi")]
    pub burst_mult_hi: f64,
    #[serde(default = "d_tickage_mult")]
    pub tickage_mult: f64,
    #[serde(default = "d_stake_mult_cap")]
    pub stake_mult_cap: f64,
    /// 5m win-prob calibration knots (Order #6 A4: moved from constants to config so
    /// a validated refit ships as config + recal reset, not a code deploy). Default =
    /// the frozen May curve; bot_v2.toml carries the refit (cal_w re-based to the
    /// honest scale, cal_z unchanged) which is why edge_min drops to 0.01 — 0.06 was
    /// calibrated against the inflated curve. Ship all three together (knots without
    /// the edge_min re-base halve volume and lose money — tested).
    #[serde(default = "d_cal_z")]
    pub cal_z: Vec<f64>,
    #[serde(default = "d_cal_w")]
    pub cal_w: Vec<f64>,
    /// Regime canary (Order #7 Part C) kill switch. `false` = always GREEN (no
    /// de-risk/halt). Thresholds are the validated defaults in CanaryConfig
    /// (n=30, AMBER<0.50, RED<0.45, vol ratio 2.0/floor 2.0bpm, 60m cooldown).
    #[serde(default = "d_true")]
    pub canary_enabled: bool,
    /// Order #9 C3: strict book-gate. When true, an entry whose PM book is
    /// UNOBSERVABLE (no/thin mid-ring coverage — typically a WS-gap window) is
    /// BLOCKED instead of passed. Default false = today's behavior + C2 logging
    /// only. Do NOT enable until the item-E study reads the measured rates.
    #[serde(default)]
    pub book_gate_strict: bool,
    /// Order #8 D entry floors (5m), both 0.0 = OFF. See v2::floor_reject. Guard the
    /// dead-tape z-explosion; sized on recorder data + flipped via config later.
    #[serde(default)]
    pub disp_floor_bps: f64,
    #[serde(default)]
    pub vol60_floor: f64,
    /// Order #9 B: ask floor (5m), default 0.30 = ON — envelope conformity with every
    /// validated study (ask ∈ [0.30, 0.97]). 0 = disabled. See v2::ask_out_of_band.
    #[serde(default = "d_min_ask")]
    pub min_ask: f64,
    /// Rolling recalibration window (closed trades retained).
    #[serde(default = "d_recal_capacity")]
    pub recal_capacity: usize,
    /// Recalibration warmup (samples before any de-bias is applied).
    #[serde(default = "d_recal_warmup")]
    pub recal_warmup: usize,
    /// Path for the persisted recalibrator state (survives restarts).
    #[serde(default = "d_recal_path")]
    pub recal_path: String,
    /// Evaluate the entry on sub-second Binance aggTrades (freshest price) instead
    /// of only on the 1s bar close. Recovers entry latency. Default false (1s).
    #[serde(default)]
    pub tick_driven: bool,
    /// Min ms between tick-driven evaluations (throttle the aggTrade firehose).
    #[serde(default = "d_tick_throttle")]
    pub tick_throttle_ms: i64,
    /// SIGNAL-INVALIDATION STOP (touch): while a position is live, exit the instant
    /// side-signed displacement-from-window-open reverts to <= 0 (the thesis is
    /// dead). 5m sells for real at the bid (when ARMED); 15m is PAPER-LOGGED only
    /// (both assets) to gather evidence before graduating. Backtest: hold +0.04 ->
    /// stop +0.12 EV/$1 honest, ~3x, variance -40%, 28/28 recorder days positive;
    /// breakeven fill haircut 8-11c. Default false (inert) — arm consciously.
    #[serde(default)]
    pub inval_stop_enabled: bool,
    /// DRY-RUN the invalidation stop: when true, would-fires are computed + logged
    /// (oplog "inval_stop" with dry_run=true) but NO sell is ever sent — even for
    /// 5m while armed. This is the safe first phase: run it dry for ~1 day, confirm
    /// the trigger fires on ~55% of positions like the backtest, THEN set false to
    /// go live. Default true so enabling the stop can't sell until you opt in.
    #[serde(default = "d_true")]
    pub inval_stop_dry_run: bool,
    /// BID-BAND conditional stop. The invalidation stop's entire edge is selling
    /// a reversal to a STALE bid; that premium exists only when the bid is in an
    /// overpay band (>= stop_bid_hi OR <= stop_bid_lo). In the fair mid-band
    /// (lo, hi) there is nothing to harvest and triggers whipsaw, so HOLD. The
    /// evaluation is CONTINUOUS: a suppressed trigger is NOT deduped, so if the
    /// bid later enters a band while displacement stays <= 0 the stop fires then.
    /// Validated (dynstop_study, 26 recorder days post-frozen-gate): statistical
    /// tie with the unconditional stop (100% normal-day edge retained) AND +$21
    /// on the Jul 3-4 dead-vol live replay (median dead-day stop bid 0.40 = dead
    /// center of the worthless band). Defaults 0.50 / 0.30 (smooth plateau; the
    /// <= lo leg is load-bearing). Same bands for 5m and 15m so the 15m paper set
    /// stays comparable. Set stop_bid_hi = 1.0 AND stop_bid_lo = 0.0 to disable
    /// the band (unconditional stop = fire on every crossing).
    #[serde(default = "d_stop_bid_hi")]
    pub stop_bid_hi: f64,
    #[serde(default = "d_stop_bid_lo")]
    pub stop_bid_lo: f64,
    /// 15-minute market as its OWN strategy (late-entry, higher z_min, price cap,
    /// its own recalibrator). Absent/disabled = 5m-only (no behavior change).
    #[serde(default)]
    pub i15m: Interval15mCfg,
}

fn d_stop_bid_hi() -> f64 { 0.50 }
fn d_stop_bid_lo() -> f64 { 0.30 }
fn d_min_ask() -> f64 { 0.30 }
fn d_cal_z() -> Vec<f64> { crate::v2::CAL_Z.to_vec() }
fn d_cal_w() -> Vec<f64> { crate::v2::CAL_W.to_vec() }
fn d_cal_z_15m() -> Vec<f64> { crate::v2::CAL_Z_15M.to_vec() }
fn d_cal_w_15m() -> Vec<f64> { crate::v2::CAL_W_15M.to_vec() }
fn d_burst_lo() -> f64 { 3.0 }
fn d_burst_hi() -> f64 { 8.0 }
fn d_burst_mult_lo() -> f64 { 2.0 }
fn d_burst_mult_hi() -> f64 { 3.0 }
fn d_tickage_mult() -> f64 { 1.25 }
fn d_stake_mult_cap() -> f64 { 3.0 }
fn d_edge_min() -> f64 { 0.04 }
fn d_vol_cap() -> f64 { 1.0 }
fn d_dvr_floor() -> f64 { 0.2 }
fn d_edge_ref() -> f64 { 0.08 }
fn d_z_min() -> f64 { 0.45 }
fn d_v2_min_ttl() -> i64 { 30 }
fn d_v2_max_ttl() -> i64 { 0 } // 0 = off (opt-in via config); set 240 for the 5m late gate
fn d_true() -> bool { true }
fn d_vol_lb_5m() -> i64 { 60 }
fn d_frozen_tape() -> i64 { 0 } // 0 = off (opt-in via config); 3 = validated 5m setting
fn d_recal_capacity() -> usize { 300 }
fn d_recal_warmup() -> usize { 50 }
fn d_recal_path() -> String { "data/v2/recal.json".to_string() }
fn d_tick_throttle() -> i64 { 200 }

// 15-minute overrides. Defaults encode the validated 15m spec (interval_15m_study):
// LATE entries only (last 6 min), z_min 0.80, price cap 0.70, own recal file.
fn d_i15m_z_min() -> f64 { 0.70 }   // formula-v2: vol120 makes low-z honest (edge gate selects)
fn d_i15m_edge_min() -> f64 { 0.06 }
fn d_i15m_max_ask() -> f64 { 0.70 }
fn d_i15m_late_ttl() -> i64 { 540 } // formula-v2: widened window (works under vol120)
fn d_i15m_vol_lb() -> i64 { 120 }   // horizon-matched vol lookback
fn d_i15m_recal_path() -> String { "data/v2/recal_15m.json".to_string() }

/// Per-interval override for the 15-minute market. Only the fields that differ from
/// the 5m base live here; sizing (`stakes`) and vol/dvr/edge_ref are shared unless
/// set. `enabled=false` (default) keeps the bot 5m-only.
#[derive(Debug, Clone, Deserialize)]
pub struct Interval15mCfg {
    /// Trade 15m at all. Default false — must be turned on explicitly.
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "d_i15m_z_min")]
    pub z_min: f64,
    #[serde(default = "d_i15m_edge_min")]
    pub edge_min: f64,
    /// Skip when `ask > max_ask` (quality cap). `>=1.0` disables the cap.
    #[serde(default = "d_i15m_max_ask")]
    pub max_ask: f64,
    /// Order #9 B: ask floor (15m), default 0.30 = ON.
    #[serde(default = "d_min_ask")]
    pub min_ask: f64,
    /// Only enter when `ttl <= late_entry_max_ttl_s` (the late-entry edge). `0` = off.
    #[serde(default = "d_i15m_late_ttl")]
    pub late_entry_max_ttl_s: i64,
    /// Own rolling recalibrator (separate from 5m).
    #[serde(default = "d_i15m_recal_path")]
    pub recal_path: String,
    #[serde(default = "d_recal_capacity")]
    pub recal_capacity: usize,
    #[serde(default = "d_recal_warmup")]
    pub recal_warmup: usize,
    /// Shared with 5m unless overridden.
    #[serde(default = "d_vol_cap")]
    pub vol_cap: f64,
    #[serde(default = "d_dvr_floor")]
    pub dvr_floor: f64,
    #[serde(default = "d_edge_ref")]
    pub edge_ref: f64,
    #[serde(default = "d_v2_min_ttl")]
    pub min_ttl_s: i64,
    /// Horizon-matched vol lookback (seconds). 120 for 15m (vs 60 for 5m).
    #[serde(default = "d_i15m_vol_lb")]
    pub vol_lookback_s: i64,
    /// BOOK-UNMOVED entry gate for 15m (see V2Config.book_unmoved_gate). Applied
    /// so the 15m paper dataset stays comparable; default ON, flag independently.
    #[serde(default = "d_true")]
    pub book_unmoved_gate: bool,
    /// 15m calibration knots (A4: moved to config for parity). UNTOUCHED by the 5m
    /// refit — defaults are the current 15m curve; leave them out of the toml.
    #[serde(default = "d_cal_z_15m")]
    pub cal_z: Vec<f64>,
    #[serde(default = "d_cal_w_15m")]
    pub cal_w: Vec<f64>,
    /// Order #11 B entry floors (15m), both 0.0 = OFF. See v2::floor_reject. Own
    /// scale from 5m (15m tape has more room, less noise) — sized on recorder data,
    /// wired to config so the 15m taker cell is guardable independently.
    #[serde(default)]
    pub disp_floor_bps: f64,
    #[serde(default)]
    pub vol60_floor: f64,
}

impl Default for Interval15mCfg {
    fn default() -> Self {
        Self {
            enabled: false,
            z_min: d_i15m_z_min(),
            edge_min: d_i15m_edge_min(),
            max_ask: d_i15m_max_ask(),
            min_ask: d_min_ask(),
            late_entry_max_ttl_s: d_i15m_late_ttl(),
            recal_path: d_i15m_recal_path(),
            recal_capacity: d_recal_capacity(),
            recal_warmup: d_recal_warmup(),
            vol_cap: d_vol_cap(),
            dvr_floor: d_dvr_floor(),
            edge_ref: d_edge_ref(),
            min_ttl_s: d_v2_min_ttl(),
            vol_lookback_s: d_i15m_vol_lb(),
            book_unmoved_gate: true,
            cal_z: d_cal_z_15m(),
            cal_w: d_cal_w_15m(),
            disp_floor_bps: 0.0,
            vol60_floor: 0.0,
        }
    }
}

impl V2Config {
    /// Build the 5-minute strategy (byte-identical to the pre-15m behavior) with the
    /// given current recal de-bias.
    #[must_use]
    pub fn strat_5m(&self, recal_bias: f64) -> crate::v2::IntervalStrat {
        crate::v2::IntervalStrat {
            enabled: self.enabled,
            z_min: self.z_min,
            edge_min: self.edge_min,
            vol_cap: self.vol_cap,
            dvr_floor: self.dvr_floor,
            edge_ref: self.edge_ref,
            min_ttl_s: self.min_ttl_s,
            late_entry_max_ttl_s: self.max_ttl_s, // 5m late gate (0 = off)
            max_ask: 0.0,            // 5m: no price cap (its live edge includes cheap)
            min_ask: self.min_ask,   // Order #9 B: 0.30 floor (envelope conformity)
            cal_z: self.cal_z.clone(),
            cal_w: self.cal_w.clone(),
            recal_bias,
            base_usd: 0.0, // set per-tick from Controls in the decision loop
            max_pos_usd: 0.0,
            vol_lookback_s: self.vol_lookback_s,
            frozen_tape_secs: self.frozen_tape_secs, // 5m: validated
            disp_floor_bps: self.disp_floor_bps,
            vol60_floor: self.vol60_floor,
        }
    }
}

impl Interval15mCfg {
    /// Build the 15-minute strategy with the given current recal de-bias. Gated by
    /// BOTH the master `v2.enabled` and this section's `enabled`.
    #[must_use]
    pub fn strat(&self, master_enabled: bool, recal_bias: f64) -> crate::v2::IntervalStrat {
        crate::v2::IntervalStrat {
            enabled: master_enabled && self.enabled,
            z_min: self.z_min,
            edge_min: self.edge_min,
            vol_cap: self.vol_cap,
            dvr_floor: self.dvr_floor,
            edge_ref: self.edge_ref,
            min_ttl_s: self.min_ttl_s,
            late_entry_max_ttl_s: self.late_entry_max_ttl_s,
            max_ask: self.max_ask,
            min_ask: self.min_ask,
            cal_z: self.cal_z.clone(),
            cal_w: self.cal_w.clone(),
            recal_bias,
            base_usd: 0.0, // set per-tick from Controls in the decision loop
            max_pos_usd: 0.0,
            vol_lookback_s: self.vol_lookback_s,
            frozen_tape_secs: 0, // 15m: not significant in validation → off
            disp_floor_bps: self.disp_floor_bps, // Order #11 B: now config-wired
            vol60_floor: self.vol60_floor,
        }
    }
}

impl Default for V2Config {
    fn default() -> Self {
        Self {
            enabled: false,
            edge_min: d_edge_min(),
            vol_cap: d_vol_cap(),
            dvr_floor: d_dvr_floor(),
            edge_ref: d_edge_ref(),
            z_min: d_z_min(),
            min_ttl_s: d_v2_min_ttl(),
            max_ttl_s: d_v2_max_ttl(),
            vol_lookback_s: d_vol_lb_5m(),
            frozen_tape_secs: d_frozen_tape(),
            recal_capacity: d_recal_capacity(),
            recal_warmup: d_recal_warmup(),
            recal_path: d_recal_path(),
            tick_driven: false,
            tick_throttle_ms: d_tick_throttle(),
            inval_stop_enabled: false,
            inval_stop_dry_run: true,
            stop_bid_hi: d_stop_bid_hi(),
            stop_bid_lo: d_stop_bid_lo(),
            book_unmoved_gate: true,
            reentry_enabled: true,
            reentry_same_enabled: true,
            reentry_opposite_enabled: true,
            burst_lo_bps: d_burst_lo(),
            burst_hi_bps: d_burst_hi(),
            burst_mult_lo: d_burst_mult_lo(),
            burst_mult_hi: d_burst_mult_hi(),
            tickage_mult: d_tickage_mult(),
            stake_mult_cap: d_stake_mult_cap(),
            cal_z: d_cal_z(),
            cal_w: d_cal_w(),
            canary_enabled: true,
            book_gate_strict: false,
            disp_floor_bps: 0.0,
            vol60_floor: 0.0,
            min_ask: d_min_ask(),
            i15m: Interval15mCfg::default(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ModeConfig {
    /// "paper" | "live". The CLI `--mode` flag overrides this at startup.
    pub default: String,
    /// When true, the first-ever live run requires interactive confirmation.
    pub live_confirmation_required: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MarketsConfig {
    /// e.g. ["5m", "15m"].
    pub intervals: Vec<String>,
    /// e.g. ["BTC", "ETH"].
    pub assets: Vec<String>,
    /// Polymarket CLOB token ids (asset_ids) to subscribe to on the public
    /// `market` channel. Phase 1 is strictly WS-only, so the token universe is
    /// sourced from config here (no REST discovery yet).
    ///
    /// CAVEAT: 5m/15m markets roll continuously, so these go stale within
    /// minutes — refresh them right before a live verify / baseline run.
    #[serde(default)]
    pub polymarket_token_ids: Vec<String>,
    /// Future epochs (beyond the current one) the discovery task lists per
    /// asset×interval. Mirrors the Python recorder's `lookahead` (=2).
    #[serde(default = "default_discovery_lookahead")]
    pub discovery_lookahead: i64,
    /// Seconds between market-discovery REST refreshes (recorder uses 60).
    #[serde(default = "default_discovery_refresh_secs")]
    pub discovery_refresh_secs: u64,
    /// Cells (`<ASSET>:<INTERVAL>`) to DROP at signal time (post-`expand_signals`,
    /// pre-`decide`). Empty (default) = no filter = full bot (backwards-compat).
    ///
    /// Rationale: data-driven cell suppression. The 10-day TP backtest (5/6-5/16,
    /// 1949 positions, `data/derived/backtest_tp_v1`) identified `ETH:15m` as the
    /// only chronically deficitario cell: 519 trades, 51% win, -$0.23/trade,
    /// -$118.90 total drag. Dropping it lifts baseline P&L from $1242 -> $1361
    /// (+9.6%) without touching the other 3 profitable cells (BTC_5m +$2.05,
    /// BTC_15m +$0.53, ETH_5m +$0.60 stay on).
    ///
    /// REVERSIBLE: set to `[]` (or remove the line) to re-enable every cell. No
    /// recompile. The filter has a no-op fast path when the list is empty.
    ///
    /// VALIDATION (fail-closed at startup):
    ///   * each entry MUST be `<ASSET>:<INTERVAL>` (one colon, exactly two parts)
    ///   * `<ASSET>` MUST be in `markets.assets`
    ///   * `<INTERVAL>` MUST be in `markets.intervals`
    /// A typo (`ETH-15m`, `DOGE:15m`, `ETH:1h`) refuses to boot -- silent NOOP
    /// would be the worst outcome (you'd think the filter is on while every
    /// signal sneaks through).
    #[serde(default)]
    pub disabled_cells: Vec<String>,
}

fn default_discovery_lookahead() -> i64 {
    2
}

fn default_discovery_refresh_secs() -> u64 {
    60
}

#[derive(Debug, Clone, Deserialize)]
pub struct StakesConfig {
    pub base_usdc: f64,
    /// Hard-capped to <= 100.0 by an `assert!` in `main` before any task spawns.
    pub max_position_usdc: f64,
    pub max_open_positions: u32,
    pub paper_initial_usdc: f64,
    /// PIECE 6 (D3.5): hard ceiling on TRADES (BUY+SELL closed) per session
    /// in `--mode live`. D4 = 1 (one trade then shutdown); D5 may raise it.
    /// Default = usize::MAX (no cap) for paper / shadow. Counted on close.
    #[serde(default = "default_max_trades_per_session")]
    pub max_trades_per_session: usize,
}

fn default_max_trades_per_session() -> usize {
    usize::MAX
}

#[derive(Debug, Clone, Deserialize)]
pub struct FiltersConfig {
    pub h2_enabled: bool,
    pub h2_lower: f64,
    pub h2_upper: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GatesConfig {
    pub book_freshness_ms: u64,
    pub balance_min_usdc: f64,
    pub phase_c_threshold_c: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RulesConfig {
    pub regla_c_enabled: bool,
    pub dca_enabled: bool,
    /// Max ACCEPTABLE slippage (USD price units) on a FOK entry: if the projected
    /// fill is worse than the send-time quote by more than this, ABORT the order
    /// (the edge no longer justifies it). Separate from the FOK worst-price limit
    /// (a hard cap); this is the strategy's "how much slippage is still worth it".
    /// Phase 6 D2. Default 0.02 (2 cents / ~2 ticks at tick 0.01).
    #[serde(default = "default_max_slippage")]
    pub max_slippage: f64,
}

fn default_max_slippage() -> f64 {
    0.02
}

#[derive(Debug, Clone, Deserialize)]
pub struct ConnectionsConfig {
    pub binance_ws_url: String,
    pub polymarket_ws_url: String,
    pub polymarket_rest_url: String,
    pub reconnect_max_attempts_per_60s: u32,
    /// Max seconds to wait for a WS connect (TCP + handshake) before treating the
    /// attempt as lost. Bounds a hung connect to a dead/silent peer so the
    /// reconnect backoff + storm guard + alerting still fire — instead of blocking
    /// forever and leaving the bot silently dead while reporting healthy.
    #[serde(default = "default_connect_timeout_s")]
    pub connect_timeout_s: u64,
    /// Polymarket Gamma API base (REST market discovery). The dynamic
    /// market-discovery task lists active 5m/15m up/down markets here.
    #[serde(default = "default_gamma_url")]
    pub polymarket_gamma_url: String,
}

fn default_connect_timeout_s() -> u64 {
    10
}

fn default_gamma_url() -> String {
    "https://gamma-api.polymarket.com".to_string()
}

#[derive(Debug, Clone, Deserialize)]
pub struct LoggingConfig {
    /// Default level filter, e.g. "info". Overridable via `--log-level` / RUST_LOG.
    pub level: String,
    /// Directory where rotating structured JSON logs are written.
    pub trace_path: String,
    /// Rotation cadence. Phase 0 supports "daily" (anything else falls back to daily).
    pub rotation: String,
    /// Enable the WS-message event recorder that writes `paths.timestamps_file`.
    ///
    /// Default = `true` (preserves the pre-opt-out behavior byte-identical, for
    /// any existing toml that omits the line). The OPERATIVE production toml
    /// flips this to `false` because the recorder has NO consumer: the Phase 1
    /// ±2% baseline gate it was built for CLOSED on 2026-04-23, and the planned
    /// Binance→Polymarket lag analysis was never implemented. Left on it grows
    /// ~20 GB/day in production — pure disk filler.
    ///
    /// When false, `events::spawn` does NOT open the file, does NOT create the
    /// parent directory, and returns a no-op logger; the configured path is
    /// untouched (a zero-byte file appearing would be misleading).
    #[serde(default = "default_event_logger_enabled")]
    pub event_logger_enabled: bool,
}

fn default_event_logger_enabled() -> bool {
    // Default preserves the pre-opt-out behavior: any old bot.toml that
    // doesn't mention `event_logger_enabled` keeps recording, exactly as
    // before this change. The OPERATIVE production toml flips this to false.
    true
}

#[derive(Debug, Clone, Deserialize)]
pub struct PathsConfig {
    pub state_file: String,
    pub timestamps_file: String,
    pub bot_log_file: String,
    pub alert_dir: String,
}

/// Per-cell exit-rule configuration. The map key is `"<ASSET>:<INTERVAL>"`
/// (matches the format used by `markets.disabled_cells`). Cells NOT present
/// in `cells` default to [`ExitRule::Baseline`] (time-based exit) — that is
/// the byte-identical pre-Pieza-1 behavior.
///
/// Pieza 1 only validates the schema; the live exit task is unchanged and
/// does not yet read these values.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct ExitsConfig {
    /// Per-cell rule map. Empty is the production default.
    #[serde(default)]
    pub cells: HashMap<String, ExitRule>,
}

/// One exit rule attached to a cell. Internally tagged on `kind` so the TOML
/// is `{ kind = "...", ...params }`. Unknown kinds are rejected by serde at
/// load time (fail-closed -- the bot refuses to boot rather than silently
/// fall back to baseline on a typo).
///
/// Parameter semantics mirror `backtest_tp.rs::ExitVariant` (the G15
/// hypothesis-generation backtester). The pure-logic refactor that ensures
/// they share their predicate arithmetic lives in a later piece.
#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ExitRule {
    /// Time-based exit at `exit_ts_s` (current production behavior).
    Baseline,
    /// Sell when `now - ts_max_bid_ms >= y_sec * 1000` (time since the running
    /// max bid). `y_sec` MUST be > 0 (validated at config load).
    #[serde(rename = "f6_only")]
    F6Only { y_sec: i64 },
    /// Sell when BOTH conditions hold: `(elapsed / hold_duration) * 100 >=
    /// x_pct` (f1) AND `now - ts_max_bid_ms >= y_sec * 1000` (f6). `x_pct`
    /// MUST be in (0.0, 100.0]; `y_sec` MUST be > 0.
    Smart { x_pct: f64, y_sec: i64 },
    /// B3 maker first-touch (COMBO Phase 1, 2026-06-08). Post a maker limit
    /// order (GTC + post_only) at `entry_price + delta`, clamped to [0,1]. If
    /// the target is never touched within the hold window, exit via `fallback`.
    /// Pre-registered for ETH:5m and BTC:15m (the latter as a hypothesis to
    /// validate on fresh data); see the prereg note. The live exit task does
    /// NOT yet consume this variant — Phase 1 is SCHEMA ONLY (mirrors how
    /// F6Only / Smart landed). Parameter semantics mirror the Python analyzer's
    /// `b3_first_touch_pnl` (scripts/analyze_exits_pf_audit.py).
    #[serde(rename = "b3_maker")]
    B3Maker {
        /// Target offset from entry. MUST be in (0.0, 1.0): `delta <= 0` would
        /// make `target <= entry` (no profit possible); `delta >= 1` would push
        /// `target` past $1, infeasible on Polymarket binary markets (price is
        /// a probability in [0,1]). Pre-registered value: 0.10.
        delta: f64,
        /// What to do if the maker target is never touched before exit_ts_ms.
        /// Closed enum (see `B3Fallback`) — each fallback has distinct PnL
        /// semantics the live exit task must understand, so adding one is a
        /// deliberate schema change, not a free-form string.
        fallback: B3Fallback,
    },
}

/// Fallback for [`ExitRule::B3Maker`] when the maker target is never touched
/// within the hold window. Closed enum: a new fallback is a schema change
/// (intentional — each has distinct fill + fee semantics). Serde renders it as
/// a flat snake_case string in TOML (`fallback = "f2_market"`), matching the
/// Python analyzer's `apply_fallback()` keys.
#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum B3Fallback {
    /// Break-even market stop: if the bid crosses `entry` from above during the
    /// hold window, sell at the bid (taker fee); otherwise time-exit at
    /// exit_ts_ms (taker). Mirrors `apply_fallback('F2_market')` in the Python
    /// analyzer byte-for-byte. The ONLY honest break-even stop (F2_maker was
    /// removed there as unrealistic).
    F2Market,
}

impl Config {
    /// Read and parse `bot.toml` from `path`.
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("reading config file: {}", path.display()))?;
        let cfg: Config = toml::from_str(&raw)
            .with_context(|| format!("parsing TOML config: {}", path.display()))?;
        Ok(cfg)
    }

    /// Soft validation of value ranges and invariants. Returns a descriptive
    /// error on the first violation. This is separate from the hard
    /// `max_position_usdc <= 100.0` guard, which `main` enforces with `assert!`.
    pub fn validate(&self) -> anyhow::Result<()> {
        let mode = self.mode.default.as_str();
        anyhow::ensure!(
            matches!(mode, "paper" | "live"),
            "mode.default must be 'paper' or 'live', got '{mode}'"
        );

        anyhow::ensure!(self.stakes.base_usdc > 0.0, "stakes.base_usdc must be > 0");
        anyhow::ensure!(
            self.stakes.max_position_usdc >= self.stakes.base_usdc,
            "stakes.max_position_usdc ({}) must be >= base_usdc ({})",
            self.stakes.max_position_usdc,
            self.stakes.base_usdc
        );
        anyhow::ensure!(
            self.stakes.max_open_positions >= 1,
            "stakes.max_open_positions must be >= 1"
        );
        anyhow::ensure!(
            self.stakes.paper_initial_usdc > 0.0,
            "stakes.paper_initial_usdc must be > 0"
        );

        anyhow::ensure!(
            (0.0..=1.0).contains(&self.filters.h2_lower),
            "filters.h2_lower must be in [0, 1]"
        );
        anyhow::ensure!(
            (0.0..=1.0).contains(&self.filters.h2_upper),
            "filters.h2_upper must be in [0, 1]"
        );
        anyhow::ensure!(
            self.filters.h2_lower < self.filters.h2_upper,
            "filters.h2_lower ({}) must be < h2_upper ({})",
            self.filters.h2_lower,
            self.filters.h2_upper
        );

        anyhow::ensure!(
            !self.markets.intervals.is_empty(),
            "markets.intervals must not be empty"
        );
        anyhow::ensure!(
            !self.markets.assets.is_empty(),
            "markets.assets must not be empty"
        );

        anyhow::ensure!(
            self.gates.book_freshness_ms > 0,
            "gates.book_freshness_ms must be > 0"
        );
        anyhow::ensure!(
            self.gates.balance_min_usdc >= 0.0,
            "gates.balance_min_usdc must be >= 0"
        );

        anyhow::ensure!(
            self.connections.reconnect_max_attempts_per_60s >= 1,
            "connections.reconnect_max_attempts_per_60s must be >= 1"
        );
        anyhow::ensure!(
            self.connections.connect_timeout_s >= 1,
            "connections.connect_timeout_s must be >= 1"
        );
        anyhow::ensure!(
            !self.connections.polymarket_gamma_url.is_empty(),
            "connections.polymarket_gamma_url must not be empty"
        );
        anyhow::ensure!(
            self.markets.discovery_lookahead >= 0,
            "markets.discovery_lookahead must be >= 0"
        );
        anyhow::ensure!(
            self.markets.discovery_refresh_secs >= 1,
            "markets.discovery_refresh_secs must be >= 1"
        );

        // exits.cells: per-cell exit-rule selection. Pieza 1 validates the SHAPE
        // (cell key format + parameter ranges) but no live code reads it yet.
        // Fail-closed: invalid params / unknown asset|interval refuse to boot --
        // the operator must know they typo'd before any live tick.
        for (cell, rule) in &self.exits.cells {
            let parts: Vec<&str> = cell.split(':').collect();
            anyhow::ensure!(
                parts.len() == 2,
                "exits.cells: '{cell}' must be '<ASSET>:<INTERVAL>' \
                 (one colon, exactly two parts); got {} parts",
                parts.len()
            );
            let (asset, interval) = (parts[0], parts[1]);
            anyhow::ensure!(
                !asset.is_empty() && !interval.is_empty(),
                "exits.cells: '{cell}' has an empty asset or interval half"
            );
            anyhow::ensure!(
                self.markets.assets.iter().any(|a| a == asset),
                "exits.cells: unknown asset '{asset}' in '{cell}' \
                 (not in markets.assets = {:?})",
                self.markets.assets
            );
            anyhow::ensure!(
                self.markets.intervals.iter().any(|i| i == interval),
                "exits.cells: unknown interval '{interval}' in '{cell}' \
                 (not in markets.intervals = {:?})",
                self.markets.intervals
            );
            match rule {
                ExitRule::Baseline => {}
                ExitRule::F6Only { y_sec } => {
                    anyhow::ensure!(
                        *y_sec > 0,
                        "exits.cells['{cell}']: f6_only.y_sec must be > 0, got {y_sec}"
                    );
                }
                ExitRule::Smart { x_pct, y_sec } => {
                    anyhow::ensure!(
                        x_pct.is_finite() && *x_pct > 0.0 && *x_pct <= 100.0,
                        "exits.cells['{cell}']: smart.x_pct must be in (0.0, 100.0], got {x_pct}"
                    );
                    anyhow::ensure!(
                        *y_sec > 0,
                        "exits.cells['{cell}']: smart.y_sec must be > 0, got {y_sec}"
                    );
                }
                ExitRule::B3Maker { delta, fallback } => {
                    anyhow::ensure!(
                        delta.is_finite() && *delta > 0.0 && *delta < 1.0,
                        "exits.cells['{cell}']: b3_maker.delta must be in (0.0, 1.0), got {delta}"
                    );
                    // `fallback` is a closed enum — serde rejects unknown
                    // strings at parse time (before validate). No range check
                    // needed; binding it documents that it's intentionally
                    // consumed here so a future fallback variant forces a
                    // compile-time revisit of this arm.
                    match fallback {
                        B3Fallback::F2Market => {}
                    }
                }
            }
        }

        // markets.disabled_cells: each entry MUST be `<ASSET>:<INTERVAL>` and both
        // halves MUST be members of the configured `assets` / `intervals` lists.
        // Fail-closed: a typo refuses to boot (silent NOOP is the worst outcome
        // -- you'd believe the filter is on while every signal sneaks through).
        for cell in &self.markets.disabled_cells {
            let parts: Vec<&str> = cell.split(':').collect();
            anyhow::ensure!(
                parts.len() == 2,
                "markets.disabled_cells: '{cell}' must be '<ASSET>:<INTERVAL>' \
                 (one colon, exactly two parts); got {} parts",
                parts.len()
            );
            let (asset, interval) = (parts[0], parts[1]);
            anyhow::ensure!(
                !asset.is_empty() && !interval.is_empty(),
                "markets.disabled_cells: '{cell}' has an empty asset or interval half"
            );
            anyhow::ensure!(
                self.markets.assets.iter().any(|a| a == asset),
                "markets.disabled_cells: unknown asset '{asset}' in '{cell}' \
                 (not in markets.assets = {:?})",
                self.markets.assets
            );
            anyhow::ensure!(
                self.markets.intervals.iter().any(|i| i == interval),
                "markets.disabled_cells: unknown interval '{interval}' in '{cell}' \
                 (not in markets.intervals = {:?})",
                self.markets.intervals
            );
        }

        // CROSS-LAYER COHERENCE (COMBO Phase 1, 2026-06-08): a cell cannot be
        // BOTH retired (markets.disabled_cells = no position ever opens) AND
        // assigned an exit rule (exits.cells = how to close an open position).
        // Those two states are contradictory: an exit rule on a cell that never
        // opens is dead config, and — worse — if the two layers ever drift
        // (someone retires a cell in one layer but leaves it active in the
        // other) the bot could trade a cell the operator believes is retired.
        // That silent contradiction is exactly the class of bug the C:-vs-D:
        // watchdog path drift was. Fail LOUD at boot instead. The fix is
        // explicit: pick ONE layer per cell (retire it, OR give it an exit
        // rule — not both).
        //
        // NOTE: this REVERSES the prior decision (the old
        // `exits_cell_in_both_disabled_and_exits_allowed` test permitted dual
        // presence on the rationale "the filter drops the signal so the rule is
        // harmless"). The new stance: harmless-but-contradictory config is a
        // latent footgun; a noisy boot error beats a cell trading by mistake.
        for disabled in &self.markets.disabled_cells {
            anyhow::ensure!(
                !self.exits.cells.contains_key(disabled),
                "cell '{disabled}' is in markets.disabled_cells AND exits.cells \
                 -- a cell cannot be both retired (never opens) and have an exit \
                 rule (how to close). Remove it from one: retire it (keep in \
                 disabled_cells, drop from exits.cells) OR operate it (drop from \
                 disabled_cells, keep the exit rule)."
            );
        }

        // v2 strategy: only range-check when enabled (disabled = dead config,
        // the defaults are always valid anyway).
        if self.v2.enabled {
            anyhow::ensure!(
                self.v2.edge_min.is_finite() && self.v2.edge_min > 0.0 && self.v2.edge_min < 1.0,
                "v2.edge_min must be in (0.0, 1.0), got {}",
                self.v2.edge_min
            );
            anyhow::ensure!(self.v2.vol_cap > 0.0, "v2.vol_cap must be > 0");
            anyhow::ensure!(self.v2.dvr_floor >= 0.0, "v2.dvr_floor must be >= 0");
            anyhow::ensure!(self.v2.edge_ref > 0.0, "v2.edge_ref must be > 0");
            anyhow::ensure!(self.v2.min_ttl_s > 0, "v2.min_ttl_s must be > 0");
            anyhow::ensure!(self.v2.recal_capacity >= 1, "v2.recal_capacity must be >= 1");
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    /// Repo-resolved path to the operative `config/bot.toml`. `cargo test` runs
    /// with cwd = the package root (`rust_bot/`), so the path is relative.
    fn operative_toml_path() -> PathBuf {
        PathBuf::from("config/bot.toml")
    }

    /// PIECE G1 (anti-deriva): the operative `bot.toml` MUST keep `regla_c_enabled
    /// = false`. The baseline strategy (`paper_trade_lagarb_directional.py`) does
    /// NOT use REGLA C; the `_opposite_closes.py` variant does. This bot replicates
    /// the BASELINE -- so flipping the flag here silently switches the bot to a
    /// non-baseline variant. Live paper 5/25-5/29 @ $10 stake measured the
    /// regla_c-close cohort at WR 32.3%, mean -$0.74/trade, n=524 (net -$387.04)
    /// -- a drag, NOT an improvement. Additionally, REGLA C caused the D5 Phantom
    /// SELL (close fired 43 s post-BUY, before /positions could settle on-chain).
    ///
    /// This test FAILS if `regla_c_enabled` flips back to `true` in the operative
    /// toml -- the exact deriva that went undetected from the initial commit
    /// (905be47) until G1.
    #[test]
    fn bot_toml_baseline_has_regla_c_off() {
        let path = operative_toml_path();
        let cfg = Config::load(&path).expect("operative bot.toml must load");
        assert!(
            !cfg.rules.regla_c_enabled,
            "BASELINE INVARIANT VIOLATED: operative bot.toml has regla_c_enabled = true. \
             The baseline strategy (paper_trade_lagarb_directional.py) does NOT include \
             REGLA C; the opposite_closes variant does. Live paper 5/25-5/29 showed the \
             regla_c-close cohort at WR 32.3% / -$0.74/trade (net -$387), AND it caused \
             the D5 Phantom SELL. If you intentionally want to run the opposite_closes \
             variant, this test is the place to acknowledge it -- DO NOT just flip the \
             flag silently. See bot.toml [rules] comment for the full audit trail."
        );
    }

    /// Sanity: the code default also says false. If somebody changes the default
    /// in code, this test stays green only because we ALSO assert the toml above.
    /// Belt-and-suspenders: a future deriva needs to flip BOTH to slip through.
    #[test]
    fn decision_config_default_has_regla_c_off() {
        let dc = crate::decision::DecisionConfig::default();
        assert!(
            !dc.regla_c_enabled,
            "DecisionConfig::default() must keep regla_c_enabled = false (baseline). \
             Code default + bot.toml are belt-and-suspenders; if you change one, the \
             other must be intentional too."
        );
    }

    // ========================================================================
    // markets.disabled_cells: parse + validation tests. The filter is wired in
    // trading_loop::filter_disabled_cells; this layer only guards the SHAPE.
    // ========================================================================

    /// Minimal stand-alone toml so we don't need to bundle a fake bot.toml on
    /// disk. We only test `[markets]` shape here; other sections must parse too,
    /// so include the minimal valid superset.
    fn minimal_toml(disabled_cells_line: &str) -> String {
        minimal_toml_with_exits(disabled_cells_line, "")
    }

    /// Extension of `minimal_toml` that also appends an `[exits.cells]` block
    /// verbatim. Used by the exits.* tests below; existing tests keep calling
    /// `minimal_toml` and get an empty exits section (= default everywhere).
    fn minimal_toml_with_exits(disabled_cells_line: &str, exits_block: &str) -> String {
        format!(r#"
[mode]
default = "paper"
live_confirmation_required = true

[markets]
intervals = ["5m", "15m"]
assets = ["BTC", "ETH"]
polymarket_token_ids = []
discovery_lookahead = 2
discovery_refresh_secs = 60
{disabled_cells_line}

[stakes]
base_usdc = 1.05
max_position_usdc = 5.0
max_open_positions = 3
paper_initial_usdc = 100.0

[filters]
h2_enabled = true
h2_lower = 0.10
h2_upper = 0.90

[gates]
book_freshness_ms = 500
balance_min_usdc = 2.0
phase_c_threshold_c = 0.10

[rules]
regla_c_enabled = false
dca_enabled = false

[connections]
binance_ws_url = "ws://x"
polymarket_ws_url = "ws://x"
polymarket_rest_url = "https://x"
reconnect_max_attempts_per_60s = 5

[logging]
level = "info"
trace_path = "logs"
rotation = "daily"

[paths]
state_file = "s"
timestamps_file = "t"
bot_log_file = "b"
alert_dir = "a"

{exits_block}
"#)
    }

    /// When the `disabled_cells` line is OMITTED, the field MUST default to
    /// empty -- i.e. full bot, no filter, byte-identical to pre-change behavior.
    /// This is the backwards-compatibility guarantee.
    #[test]
    fn markets_disabled_cells_default_is_empty_when_omitted() {
        let raw = minimal_toml("");
        let cfg: Config = toml::from_str(&raw).expect("toml without disabled_cells must parse");
        cfg.validate().expect("default empty disabled_cells must validate");
        assert!(
            cfg.markets.disabled_cells.is_empty(),
            "missing `disabled_cells` line must default to [], got {:?}",
            cfg.markets.disabled_cells,
        );
    }

    /// Happy path: the operative cell `ETH:15m` parses + validates cleanly.
    #[test]
    fn markets_disabled_cells_eth_15m_parses_and_validates() {
        let raw = minimal_toml(r#"disabled_cells = ["ETH:15m"]"#);
        let cfg: Config = toml::from_str(&raw).expect("parse");
        cfg.validate().expect("validate the operative cell");
        assert_eq!(cfg.markets.disabled_cells, vec!["ETH:15m".to_string()]);
    }

    /// Bad format rejected: missing colon, wrong separator, or extra parts.
    /// One asserted variant per line so failure messages pinpoint which form
    /// regressed.
    #[test]
    fn markets_disabled_cells_bad_format_rejected() {
        // 1) wrong separator -- one part, no colon
        let raw = minimal_toml(r#"disabled_cells = ["ETH-15m"]"#);
        let err = toml::from_str::<Config>(&raw).unwrap().validate().unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("'<ASSET>:<INTERVAL>'") && msg.contains("ETH-15m"),
            "bad-separator error must mention the format + the bad cell; got: {msg}"
        );

        // 2) only the asset, no interval
        let raw = minimal_toml(r#"disabled_cells = ["ETH"]"#);
        let err = toml::from_str::<Config>(&raw).unwrap().validate().unwrap_err();
        assert!(format!("{err}").contains("'<ASSET>:<INTERVAL>'"));

        // 3) three parts (extra colon)
        let raw = minimal_toml(r#"disabled_cells = ["ETH:15m:extra"]"#);
        let err = toml::from_str::<Config>(&raw).unwrap().validate().unwrap_err();
        assert!(format!("{err}").contains("'<ASSET>:<INTERVAL>'"));

        // 4) empty asset half
        let raw = minimal_toml(r#"disabled_cells = [":15m"]"#);
        let err = toml::from_str::<Config>(&raw).unwrap().validate().unwrap_err();
        assert!(format!("{err}").contains("empty asset or interval half"));

        // 5) empty interval half
        let raw = minimal_toml(r#"disabled_cells = ["ETH:"]"#);
        let err = toml::from_str::<Config>(&raw).unwrap().validate().unwrap_err();
        assert!(format!("{err}").contains("empty asset or interval half"));
    }

    /// An asset NOT in `markets.assets` is rejected -- prevents silent NOOP
    /// from a typo (e.g. `DOGE:15m` when the bot only trades BTC/ETH).
    #[test]
    fn markets_disabled_cells_unknown_asset_rejected() {
        let raw = minimal_toml(r#"disabled_cells = ["DOGE:15m"]"#);
        let err = toml::from_str::<Config>(&raw).unwrap().validate().unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("unknown asset 'DOGE'") && msg.contains("markets.assets"),
            "unknown-asset error must name the bad asset + reference the assets list; got: {msg}"
        );
    }

    /// An interval NOT in `markets.intervals` is rejected -- prevents typo NOOP
    /// (e.g. `ETH:1h` when only 5m/15m are configured).
    #[test]
    fn markets_disabled_cells_unknown_interval_rejected() {
        let raw = minimal_toml(r#"disabled_cells = ["ETH:1h"]"#);
        let err = toml::from_str::<Config>(&raw).unwrap().validate().unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("unknown interval '1h'") && msg.contains("markets.intervals"),
            "unknown-interval error must name the bad interval + reference the intervals list; got: {msg}"
        );
    }

    // ========================================================================
    // logging.event_logger_enabled: opt-out flag tests.
    // ========================================================================

    /// Backwards compatibility: omitting the field MUST default to `true`
    /// (preserves the pre-opt-out behavior for any toml that doesn't mention
    /// it). The OPERATIVE production toml flips it to false explicitly, but
    /// that's handled in `bot_toml_event_logger_disabled` below.
    #[test]
    fn logging_event_logger_enabled_default_is_true_when_omitted() {
        let raw = minimal_toml("");
        let cfg: Config = toml::from_str(&raw).expect("parse without event_logger_enabled");
        assert!(
            cfg.logging.event_logger_enabled,
            "default must be true (pre-opt-out behavior preserved); got {}",
            cfg.logging.event_logger_enabled
        );
    }

    // ========================================================================
    // exits.cells: Pieza 1 schema tests. The live exit task does NOT yet read
    // these values -- the wiring lands in later pieces. These tests pin the
    // SHAPE (parser + fail-closed validation) so when the wiring is added,
    // misconfiguration cannot silently slip through.
    // ========================================================================

    #[test]
    fn exits_default_is_empty() {
        let raw = minimal_toml("");
        let cfg: Config = toml::from_str(&raw).expect("toml without [exits] must parse");
        cfg.validate().expect("empty exits must validate");
        assert!(
            cfg.exits.cells.is_empty(),
            "missing [exits.cells] must default to empty map, got {:?}",
            cfg.exits.cells
        );
    }

    #[test]
    fn exits_baseline_explicit() {
        let raw = minimal_toml_with_exits(
            "",
            r#"[exits.cells]
"BTC:5m" = { kind = "baseline" }"#,
        );
        let cfg: Config = toml::from_str(&raw).expect("parse");
        cfg.validate().expect("baseline must validate");
        assert_eq!(cfg.exits.cells.get("BTC:5m"), Some(&ExitRule::Baseline));
    }

    #[test]
    fn exits_f6_only_valid() {
        let raw = minimal_toml_with_exits(
            "",
            r#"[exits.cells]
"BTC:15m" = { kind = "f6_only", y_sec = 120 }"#,
        );
        let cfg: Config = toml::from_str(&raw).expect("parse");
        cfg.validate().expect("f6_only y=120 must validate");
        assert_eq!(
            cfg.exits.cells.get("BTC:15m"),
            Some(&ExitRule::F6Only { y_sec: 120 })
        );
    }

    #[test]
    fn exits_smart_valid() {
        let raw = minimal_toml_with_exits(
            "",
            r#"[exits.cells]
"ETH:5m" = { kind = "smart", x_pct = 70.0, y_sec = 30 }"#,
        );
        let cfg: Config = toml::from_str(&raw).expect("parse");
        cfg.validate().expect("smart x=70 y=30 must validate");
        assert_eq!(
            cfg.exits.cells.get("ETH:5m"),
            Some(&ExitRule::Smart {
                x_pct: 70.0,
                y_sec: 30
            })
        );
    }

    #[test]
    fn exits_b3_maker_valid() {
        let raw = minimal_toml_with_exits(
            "",
            r#"[exits.cells]
"ETH:5m" = { kind = "b3_maker", delta = 0.10, fallback = "f2_market" }"#,
        );
        let cfg: Config = toml::from_str(&raw).expect("parse");
        cfg.validate().expect("b3_maker delta=0.10 f2_market must validate");
        assert_eq!(
            cfg.exits.cells.get("ETH:5m"),
            Some(&ExitRule::B3Maker {
                delta: 0.10,
                fallback: B3Fallback::F2Market,
            })
        );
    }

    #[test]
    fn exits_b3_maker_delta_zero_rejected() {
        let raw = minimal_toml_with_exits(
            "",
            r#"[exits.cells]
"ETH:5m" = { kind = "b3_maker", delta = 0.0, fallback = "f2_market" }"#,
        );
        let err = toml::from_str::<Config>(&raw).unwrap().validate().unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("b3_maker.delta") && msg.contains("(0.0, 1.0)"),
            "delta=0 error must name the field + range; got: {msg}"
        );
    }

    #[test]
    fn exits_b3_maker_delta_one_rejected() {
        let raw = minimal_toml_with_exits(
            "",
            r#"[exits.cells]
"ETH:5m" = { kind = "b3_maker", delta = 1.0, fallback = "f2_market" }"#,
        );
        let err = toml::from_str::<Config>(&raw).unwrap().validate().unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("b3_maker.delta") && msg.contains("(0.0, 1.0)"),
            "delta=1.0 error must name the field + range; got: {msg}"
        );
    }

    #[test]
    fn exits_b3_maker_delta_negative_rejected() {
        let raw = minimal_toml_with_exits(
            "",
            r#"[exits.cells]
"ETH:5m" = { kind = "b3_maker", delta = -0.05, fallback = "f2_market" }"#,
        );
        let err = toml::from_str::<Config>(&raw).unwrap().validate().unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("b3_maker.delta") && msg.contains("(0.0, 1.0)"),
            "negative delta error must name the field + range; got: {msg}"
        );
    }

    #[test]
    fn exits_b3_maker_fallback_unknown_rejected() {
        // Unknown fallback string is a SERDE-layer reject (closed enum), at
        // toml::from_str -- before validate. Either way the bot won't boot.
        let raw = minimal_toml_with_exits(
            "",
            r#"[exits.cells]
"ETH:5m" = { kind = "b3_maker", delta = 0.10, fallback = "f5_teleport" }"#,
        );
        let err = toml::from_str::<Config>(&raw)
            .err()
            .expect("unknown fallback must fail at parse");
        let msg = format!("{err}");
        assert!(
            msg.contains("f5_teleport") || msg.contains("unknown variant"),
            "unknown-fallback error must reference the bad value or 'unknown variant'; got: {msg}"
        );
    }

    #[test]
    fn exits_b3_maker_fallback_omitted_rejected() {
        // `fallback` has NO serde default (explicit is mandatory). Omitting it
        // is a parse-layer reject (missing field).
        let raw = minimal_toml_with_exits(
            "",
            r#"[exits.cells]
"ETH:5m" = { kind = "b3_maker", delta = 0.10 }"#,
        );
        let err = toml::from_str::<Config>(&raw)
            .err()
            .expect("missing fallback must fail at parse");
        let msg = format!("{err}");
        assert!(
            msg.contains("fallback") || msg.contains("missing field"),
            "missing-fallback error must reference the field or 'missing field'; got: {msg}"
        );
    }

    #[test]
    fn exits_unknown_kind_rejected() {
        // Unknown `kind` is a serde-layer reject (happens at toml::from_str,
        // before validate). Either way, the bot does NOT boot with a typo.
        let raw = minimal_toml_with_exits(
            "",
            r#"[exits.cells]
"BTC:5m" = { kind = "frobnicate", y_sec = 10 }"#,
        );
        let err = toml::from_str::<Config>(&raw)
            .err()
            .expect("unknown kind must fail at parse");
        let msg = format!("{err}");
        assert!(
            msg.contains("frobnicate") || msg.contains("unknown variant"),
            "unknown-kind error must reference the bad value or 'unknown variant'; got: {msg}"
        );
    }

    #[test]
    fn exits_f6_zero_y_rejected() {
        let raw = minimal_toml_with_exits(
            "",
            r#"[exits.cells]
"BTC:15m" = { kind = "f6_only", y_sec = 0 }"#,
        );
        let err = toml::from_str::<Config>(&raw)
            .unwrap()
            .validate()
            .unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("f6_only.y_sec") && msg.contains("> 0"),
            "zero-y_sec error must name the field and the constraint; got: {msg}"
        );
    }

    #[test]
    fn exits_smart_negative_x_rejected() {
        let raw = minimal_toml_with_exits(
            "",
            r#"[exits.cells]
"ETH:5m" = { kind = "smart", x_pct = -1.0, y_sec = 30 }"#,
        );
        let err = toml::from_str::<Config>(&raw)
            .unwrap()
            .validate()
            .unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("smart.x_pct") && msg.contains("(0.0, 100.0]"),
            "negative-x_pct error must name the field + the range; got: {msg}"
        );
    }

    #[test]
    fn exits_smart_x_above_100_rejected() {
        let raw = minimal_toml_with_exits(
            "",
            r#"[exits.cells]
"ETH:5m" = { kind = "smart", x_pct = 110.0, y_sec = 30 }"#,
        );
        let err = toml::from_str::<Config>(&raw)
            .unwrap()
            .validate()
            .unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("smart.x_pct") && msg.contains("(0.0, 100.0]"),
            "x_pct>100 error must name the field + the range; got: {msg}"
        );
    }

    #[test]
    fn exits_unknown_asset_rejected() {
        // Same fail-closed guard as disabled_cells: typo'd asset refuses boot.
        let raw = minimal_toml_with_exits(
            "",
            r#"[exits.cells]
"DOGE:5m" = { kind = "f6_only", y_sec = 60 }"#,
        );
        let err = toml::from_str::<Config>(&raw)
            .unwrap()
            .validate()
            .unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("unknown asset 'DOGE'") && msg.contains("exits.cells"),
            "unknown-asset error must name the bad asset + reference exits.cells; got: {msg}"
        );
    }

    #[test]
    fn exits_unknown_interval_rejected() {
        let raw = minimal_toml_with_exits(
            "",
            r#"[exits.cells]
"BTC:4h" = { kind = "smart", x_pct = 70.0, y_sec = 30 }"#,
        );
        let err = toml::from_str::<Config>(&raw)
            .unwrap()
            .validate()
            .unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("unknown interval '4h'") && msg.contains("exits.cells"),
            "unknown-interval error must name the bad interval + reference exits.cells; got: {msg}"
        );
    }

    #[test]
    fn exits_cell_in_both_disabled_and_exits_rejected() {
        // COMBO Phase 1 (2026-06-08) REVERSED the prior decision: a cell in
        // BOTH `disabled_cells` and `exits.cells` is now a fail-closed boot
        // error. Rationale: the two layers are contradictory (retired = never
        // opens; exit rule = how to close an open position), and if they ever
        // drift the bot could trade a cell the operator believes is retired --
        // the same silent-contradiction class as the C:-vs-D: watchdog path
        // drift. A noisy boot error beats a cell trading by mistake.
        let raw = minimal_toml_with_exits(
            r#"disabled_cells = ["ETH:15m"]"#,
            r#"[exits.cells]
"ETH:15m" = { kind = "f6_only", y_sec = 60 }"#,
        );
        let err = toml::from_str::<Config>(&raw)
            .expect("parse (the contradiction is a VALIDATE error, not a parse error)")
            .validate()
            .unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("ETH:15m")
                && msg.contains("disabled_cells")
                && msg.contains("exits.cells"),
            "dual-presence error must name the cell + both layers; got: {msg}"
        );
    }

    #[test]
    fn exits_disjoint_layers_pass() {
        // The CORRECT pattern: one cell retired (disabled_cells only), a
        // DIFFERENT cell operated (exits.cells only). No overlap -> validates.
        // This is the shape of the pre-registered MIX strategy: ETH:15m
        // retired, ETH:5m / BTC:15m given B3 rules, BTC:5m baseline (absent).
        let raw = minimal_toml_with_exits(
            r#"disabled_cells = ["ETH:15m"]"#,
            r#"[exits.cells]
"ETH:5m"  = { kind = "b3_maker", delta = 0.10, fallback = "f2_market" }
"BTC:15m" = { kind = "b3_maker", delta = 0.10, fallback = "f2_market" }"#,
        );
        let cfg: Config = toml::from_str(&raw).expect("parse");
        cfg.validate().expect("disjoint layers must validate");
        assert!(cfg.markets.disabled_cells.contains(&"ETH:15m".to_string()));
        assert!(cfg.exits.cells.contains_key("ETH:5m"));
        assert!(cfg.exits.cells.contains_key("BTC:15m"));
        assert!(!cfg.exits.cells.contains_key("ETH:15m"));
    }

    #[test]
    fn bot_toml_operative_loads_with_exits_section() {
        // The operative bot.toml must parse + validate. [exits.cells] is
        // non-empty -- the per-cell pinning happens in
        // `bot_toml_operative_exit_cells_match_b3_prereg` below; here we only
        // assert parse + validate.
        let path = operative_toml_path();
        let cfg = Config::load(&path).expect("operative bot.toml must load");
        cfg.validate().expect("operative bot.toml must validate");
    }

    /// ANTI-DERIVA (mirror of `bot_toml_baseline_has_regla_c_off` and
    /// `bot_toml_event_logger_disabled`): pins the operative `[exits.cells]`
    /// block to EXACTLY the COMBO B3 pre-registration. Any change to the cell
    /// list, kind, or parameters without updating this test fails the build --
    /// the test exists precisely because these values affect real capital
    /// decisions per cell.
    ///
    /// FLIP (2026-06-08, justified): this test PREVIOUSLY pinned the G15
    /// hypotheses (BTC:15m F6Only y120, ETH:5m Smart x70 y30, commit 00d3ad1).
    /// The COMBO investigation this cycle SUPERSEDED G15: the per-cell B3
    /// strategy was validated mechanically on May (exploration + validation +
    /// peeked tercer) via the Python analyzer, and the user decided to deploy
    /// it live. The G15 smart/f6 rules are replaced by B3 maker first-touch on
    /// the two cells where B3 won. This is NOT silent deriva -- it is the
    /// deliberate strategy change recorded in memory/prereg_mix5m_2026_06_08.md,
    /// pinned here so the NEXT change must again be deliberate.
    ///
    /// COMBO B3 pre-registration (the operative values):
    ///   * BTC:5m  -> Baseline (time-exit). NOT listed in [exits.cells].
    ///                B3 cuts winners short in the fast 5m cell.
    ///   * ETH:5m  -> B3Maker { delta: 0.10, fallback: F2Market }. Confirmed
    ///                across all 3 datasets.
    ///   * BTC:15m -> B3Maker { delta: 0.10, fallback: F2Market }. HYPOTHESIS
    ///                (robust 6/6 combos; confirm on fresh data).
    ///   * ETH:15m -> RETIRED via `markets.disabled_cells`; NOT in [exits.cells]
    ///                (the cell never opens, so no exit rule could fire).
    ///
    /// If a future investigation overrides these, update BOTH the operative
    /// bot.toml AND this test in the same commit so the deriva is impossible.
    #[test]
    fn bot_toml_operative_exit_cells_match_b3_prereg() {
        let path = operative_toml_path();
        let cfg = Config::load(&path).expect("operative bot.toml must load");
        cfg.validate().expect("operative bot.toml must validate");

        // Exactly 2 cells configured (BTC:5m + ETH:15m are intentionally
        // absent: baseline + retired respectively).
        assert_eq!(
            cfg.exits.cells.len(), 2,
            "B3 prereg pins EXACTLY 2 cells in [exits.cells] (ETH:5m + BTC:15m); \
             got {} cells: {:?}",
            cfg.exits.cells.len(), cfg.exits.cells.keys().collect::<Vec<_>>()
        );

        // ETH:5m -> B3Maker { delta: 0.10, fallback: F2Market } (confirmed cell)
        assert_eq!(
            cfg.exits.cells.get("ETH:5m"),
            Some(&ExitRule::B3Maker { delta: 0.10, fallback: B3Fallback::F2Market }),
            "B3 prereg ETH:5m is B3Maker delta=0.10 f2_market; got {:?}. \
             Changing this affects capital -- update the prereg first.",
            cfg.exits.cells.get("ETH:5m")
        );

        // BTC:15m -> B3Maker { delta: 0.10, fallback: F2Market } (hypothesis)
        assert_eq!(
            cfg.exits.cells.get("BTC:15m"),
            Some(&ExitRule::B3Maker { delta: 0.10, fallback: B3Fallback::F2Market }),
            "B3 prereg BTC:15m is B3Maker delta=0.10 f2_market (hypothesis); \
             got {:?}. Changing this affects capital.",
            cfg.exits.cells.get("BTC:15m")
        );

        // BTC:5m intentionally ABSENT -> falls through to Baseline time-exit.
        // B3 cuts winners short in the fast 5m cell; baseline preserves tails.
        assert!(
            !cfg.exits.cells.contains_key("BTC:5m"),
            "BTC:5m must NOT have an exit rule (prereg: baseline preserves the \
             big winners B3 would cut). Found: {:?}",
            cfg.exits.cells.get("BTC:5m")
        );

        // ETH:15m intentionally ABSENT -> RETIRED at signal time (disabled_cells).
        assert!(
            !cfg.exits.cells.contains_key("ETH:15m"),
            "ETH:15m must NOT have an exit rule -- it's RETIRED via \
             markets.disabled_cells. A rule here would be dead code (the cell \
             never opens) AND would trip the Phase-1 cross-layer guard. \
             Found: {:?}",
            cfg.exits.cells.get("ETH:15m")
        );

        // And confirm ETH:15m IS actually in disabled_cells (defense in depth:
        // if someone removed it from disabled_cells, the absence-of-rule here
        // would silently let ETH:15m open with baseline time-exit. We require
        // ETH:15m to be either disabled-at-signal OR explicitly ruled here.).
        assert!(
            cfg.markets.disabled_cells.contains(&"ETH:15m".to_string()),
            "ETH:15m must be in markets.disabled_cells; got {:?}. The two \
             invariants together (disabled at signal + absent from exits) \
             ensure ETH:15m cannot trade.",
            cfg.markets.disabled_cells
        );
    }

    /// PIECE G16 (anti-deriva, mirror of `bot_toml_baseline_has_regla_c_off`):
    /// the OPERATIVE `bot.toml` MUST keep `event_logger_enabled = false`. The
    /// recorder has NO consumer: the Phase 1 ±2% baseline gate that justified
    /// it CLOSED on 2026-04-23, and the planned Binance→Polymarket lag analysis
    /// was never implemented. Left on it writes ~20 GB/day to disk in
    /// production — pure disk-filler that already filled the VPS once.
    ///
    /// This test FAILS if the operative toml is reverted to (or silently
    /// reverts to) `true` — including by removing the line (default = true).
    /// If somebody one day wires up a real consumer and wants the recorder
    /// back, this is the place to acknowledge that — DO NOT just flip the
    /// flag silently. See `[logging]` comment in bot.toml for the audit trail.
    #[test]
    fn bot_toml_event_logger_disabled() {
        let path = operative_toml_path();
        let cfg = Config::load(&path).expect("operative bot.toml must load");
        assert!(
            !cfg.logging.event_logger_enabled,
            "OPERATIONAL INVARIANT VIOLATED: operative bot.toml has \
             event_logger_enabled = true (or omits the line, which defaults to \
             true). The event logger has NO consumer (Phase 1 baseline gate \
             closed 2026-04-23; lag analysis never built) and writes ~20 GB/day \
             of timestamps.jsonl that nobody reads. It already filled the VPS \
             disk once. Keep it OFF in production unless you are wiring a real \
             consumer — and if so, acknowledge it here, do not flip silently. \
             See bot.toml [logging] comment for the full audit trail."
        );
    }
}
