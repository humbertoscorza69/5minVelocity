//! v2 strategy core — the validated edge-gate brain, ported from the 5minSnip
//! research (Python `paper_bot.py` + the walk-forward study `live_model.py`).
//!
//! WHAT THIS IS (and what it deliberately is NOT):
//!   * It is the PURE strategy math: vol-normalized displacement `z`, the frozen
//!     win-probability calibration `pcal(z)`, the expected-edge formula, the
//!     depth-aware edge-proportional sizing rule, and the self-healing rolling
//!     recalibration. All deterministic, all unit-tested in isolation.
//!   * It is NOT wired into the decision loop here — that lands when the Binance
//!     price-history feed is threaded into `decision::decide`. Keeping the math
//!     standalone lets it be tested without the async stack.
//!
//! PROVENANCE (do not "tune" these without re-running the walk-forward):
//!   * CAL_Z / CAL_W: frozen May-calibrated win-prob curve (paper_bot.py).
//!   * FEE_RATE 0.07: Polymarket taker fee = rate * p * (1-p).
//!   * EDGE_MIN 0.04: OOS-validated entry gate (edge = pcal(z) - ask - fee).
//!   * VOL_CAP 1.0 bps/s: high-vol regime is NEGATIVE-EV (a risk filter, not a
//!     discrimination claim) — skip it.
//!   * DVR_FLOOR 0.2: drop only the genuine coin-flip spikes; dvr did NOT durably
//!     beat z OOS, so this is a light floor, not a core feature.
//! The walk-forward verdict (live_model.py) was: SELECTION is near ceiling; the
//! durable levers are SIZING + CALIBRATION. This module encodes exactly that.

#![allow(dead_code)] // wired into the decision loop in a follow-up step

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

/// Polymarket taker fee rate: `fee = FEE_RATE * ask * (1 - ask)`.
pub const FEE_RATE: f64 = 0.07;
/// Entry gate: take the trade iff `edge >= EDGE_MIN`.
pub const EDGE_MIN: f64 = 0.04;
/// Skip when rolling vol exceeds this (bps/s) — high-vol is negative-EV.
pub const VOL_CAP: f64 = 1.0;
/// Light disp/vel floor: drop genuine coin-flip spikes only.
pub const DVR_FLOOR: f64 = 0.2;
/// Minimum vol-normalized displacement to enter. Below this the move is in the
/// calibration's unreliable near-zero region (win prob ~0.5-0.59) and live results
/// showed it is NOT predictive -- so require a REAL displacement. 0.45 targets the
/// CAL_Z knot whose May win rate is ~0.68 (the backtested edge region).
pub const Z_MIN: f64 = 0.45;

/// Frozen win-probability calibration (May, paper_bot.py). `pcal` linearly
/// interpolates `CAL_W` over `CAL_Z`, clamped at the endpoints.
pub const CAL_Z: [f64; 8] = [0.14, 0.45, 0.80, 1.24, 1.74, 2.46, 3.82, 10.49];
pub const CAL_W: [f64; 8] = [0.593, 0.680, 0.780, 0.833, 0.877, 0.923, 0.951, 0.964];

/// 15-MINUTE win-probability calibration (its own curve — 15m settles differently
/// than 5m). Fit from the June recorder win-by-z (interval_15m_study, section D):
///   z[0.45,0.7)=0.603  z[0.7,1.0)=0.703  z[1.0,1.5)=0.761  z[1.5,2.5)=0.782.
/// Knots placed at each bucket's mid; the noisy z>=2.5 tail (0.727, n=172) is held
/// flat at 0.782 to keep the curve monotone. Below 0.45 interpolates toward 0.5
/// (the 15m z_min gate is 0.80, so that region is never actually traded — it only
/// exists for interpolation continuity). The 15m rolling recalibrator (its own
/// instance) self-heals any residual bias, exactly as the 5m one does.
pub const CAL_Z_15M: [f64; 4] = [0.57, 0.83, 1.20, 1.90];
pub const CAL_W_15M: [f64; 4] = [0.603, 0.703, 0.761, 0.782];

/// Win probability for a vol-normalized displacement `z` under an arbitrary
/// calibration curve (`cal_z` ascending knots, `cal_w` win rates). Linear
/// interpolation; below the first knot interpolates toward (0.0, 0.5) so a non-move
/// is ~0.5; clamped to the last knot above the range. See `pcal` for the 5m curve.
#[must_use]
pub fn pcal_with(z: f64, cal_z: &[f64], cal_w: &[f64]) -> f64 {
    let n = cal_z.len();
    if n == 0 || cal_w.len() != n {
        return 0.5;
    }
    // DRIFTLESS BASELINE: zero vol-normalized displacement => 50/50 (see the 5m
    // note below — near-zero z must score ~0.5 so noise never clears the edge gate).
    if z <= 0.0 {
        return 0.5;
    }
    if z < cal_z[0] {
        let t = z / cal_z[0];
        return 0.5 + t * (cal_w[0] - 0.5);
    }
    if z >= cal_z[n - 1] {
        return cal_w[n - 1];
    }
    for i in 0..n - 1 {
        if z < cal_z[i + 1] {
            let (z0, z1) = (cal_z[i], cal_z[i + 1]);
            let (w0, w1) = (cal_w[i], cal_w[i + 1]);
            let t = (z - z0) / (z1 - z0);
            return w0 + t * (w1 - w0);
        }
    }
    cal_w[n - 1]
}

/// Win probability for a vol-normalized displacement `z` under the frozen 5-MINUTE
/// May calibration. Linear interpolation of `CAL_W` over `CAL_Z`.
///
/// DRIFTLESS BASELINE (fix): zero vol-normalized displacement => 50/50. The May
/// knots start at z=0.14 (0.593); the old code CLAMPED everything below 0.14 to
/// 0.593, so near-zero displacement (z~0.02-0.13, i.e. the window just opened and
/// nothing moved) was scored ~0.59 win and cleared the edge gate -> the bot fired
/// on noise every window. We now interpolate the [0, 0.14) region down to
/// (0.0, 0.5) so a non-move is correctly ~0.5 (negative edge -> no trade).
#[must_use]
pub fn pcal(z: f64) -> f64 {
    pcal_with(z, &CAL_Z, &CAL_W)
}

/// Rolling realized volatility in bps-per-second: population std of consecutive
/// 1s log-returns over `closes`, scaled by 1e4. Mirrors the Python
/// `np.std(np.diff(np.log(c))) * 1e4` (ddof=0). Returns `None` with < 2 points or
/// a non-positive close.
#[must_use]
pub fn vol_bps(closes: &[f64]) -> Option<f64> {
    if closes.len() < 2 {
        return None;
    }
    let mut rets = Vec::with_capacity(closes.len() - 1);
    for w in closes.windows(2) {
        if w[0] <= 0.0 || w[1] <= 0.0 {
            return None;
        }
        rets.push((w[1] / w[0]).ln());
    }
    let n = rets.len() as f64;
    let mean = rets.iter().sum::<f64>() / n;
    let var = rets.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / n; // population (ddof=0)
    let v = var.sqrt() * 1e4;
    if v.is_finite() && v > 0.0 { Some(v) } else { None }
}

/// Vol-normalized displacement `z = disp_bps / (vol_bps * sqrt(ttl_s))`.
/// `disp_bps` is the side-signed displacement-from-window-open in bps; `ttl_s` is
/// seconds to settlement. Returns `None` for degenerate inputs.
#[must_use]
pub fn zscore(disp_bps: f64, vol_bps: f64, ttl_s: f64) -> Option<f64> {
    if vol_bps <= 0.0 || ttl_s <= 0.0 {
        return None;
    }
    let z = disp_bps / (vol_bps * ttl_s.sqrt());
    z.is_finite().then_some(z)
}

/// Side-signed displacement-from-open in bps. `up` = true for the Up side.
#[must_use]
pub fn disp_bps(open: f64, cur: f64, up: bool) -> f64 {
    if open <= 0.0 {
        return 0.0;
    }
    let raw = (cur / open - 1.0) * 1e4;
    if up { raw } else { -raw }
}

/// disp/vel ratio (sustained vs spiky). `vel_bps` is the side-signed short-window
/// velocity in bps; floored at 0.5 to avoid blow-up (mirrors the Python).
#[must_use]
pub fn dvr(disp_bps: f64, vel_bps: f64) -> f64 {
    disp_bps / vel_bps.abs().max(0.5)
}

/// Polymarket taker fee for a fill at `ask`.
#[must_use]
pub fn fee(ask: f64) -> f64 {
    FEE_RATE * ask * (1.0 - ask)
}

/// Expected edge = `p - ask - fee(ask)`, where `p` is the (recalibrated) win prob.
#[must_use]
pub fn edge(p: f64, ask: f64) -> f64 {
    p - ask - fee(ask)
}

/// The full entry features for one candidate, computed at decision time.
#[derive(Debug, Clone, Copy)]
pub struct Features {
    pub disp_bps: f64,
    pub vel_bps: f64,
    pub vol_bps: f64,
    pub ttl_s: f64,
    pub z: f64,
    pub dvr: f64,
    pub ask: f64,
    /// (Recalibrated) win probability and resulting edge.
    pub p: f64,
    pub edge: f64,
}

/// The entry decision: take it or not, with the reason if not.
#[derive(Debug, Clone, PartialEq)]
pub enum Gate {
    Take,
    Skip(&'static str),
}

/// Apply the v2 entry gate to a candidate's features. Order of checks mirrors the
/// research: vol cap (regime), dvr floor (coin-flip), then the edge gate.
#[must_use]
pub fn gate(f: &Features) -> Gate {
    if !f.vol_bps.is_finite() || f.vol_bps <= 0.0 {
        return Gate::Skip("no_vol");
    }
    if f.vol_bps > VOL_CAP {
        return Gate::Skip("vol_cap");
    }
    if f.disp_bps <= 0.0 {
        return Gate::Skip("no_disp"); // displacement must favor our side
    }
    if f.z < Z_MIN {
        return Gate::Skip("z_min"); // displacement too small to be predictive
    }
    if f.dvr < DVR_FLOOR {
        return Gate::Skip("dvr_floor");
    }
    if f.edge < EDGE_MIN {
        return Gate::Skip("edge_min");
    }
    Gate::Take
}

/// Depth-aware edge-proportional sizing. The biggest validated lever (~2x total,
/// +29% Sharpe OOS) — but bounded by what the book actually offers so we never
/// oversize. Returns USDC notional to stake (0 if edge <= 0).
///
/// `base_usd`   — stake at the reference edge.
/// `edge_ref`   — the edge at which we stake `base_usd` (linear scaling around it).
/// `min_usd`    — exchange/strategy minimum (e.g. 1.05); below this → skip upstream.
/// `max_pos_usd`— hard per-position cap.
/// `depth_usd`  — USDC available at/through the ask (from the live book). The stake
///                is capped here so a FOK can actually fill.
#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn edge_stake(
    edge: f64,
    base_usd: f64,
    edge_ref: f64,
    min_usd: f64,
    max_pos_usd: f64,
    depth_usd: f64,
) -> f64 {
    if edge <= 0.0 || edge_ref <= 0.0 {
        return 0.0;
    }
    let raw = base_usd * (edge / edge_ref);
    let capped = raw.clamp(min_usd, max_pos_usd);
    // Never stake more than the book can fill.
    let stake = capped.min(depth_usd.max(0.0));
    // Quantize to whole cents. Polymarket market-BUY USDC amounts must have <= 2
    // decimals; edge-scaled stakes like 2.4375 are rejected ("max accuracy of 2
    // decimals") -- this silently killed ~84% of orders once base != max made
    // stakes non-cent. FLOOR (never round up) so we can't breach max_position by a
    // cent. Doing it here means guards, logs, paper, and the live order all agree.
    (stake * 100.0).floor() / 100.0
}

/// Fractional-Kelly stake (alternative to edge-proportional). `b = (1-ask)/ask`,
/// `f* = p - (1-p)/b`; stake = `bankroll * kelly_frac * f*`, depth+max capped.
/// Edge-proportional ≈ Kelly in our study; this is provided for A/B.
#[must_use]
pub fn kelly_stake(
    p: f64,
    ask: f64,
    bankroll_usd: f64,
    kelly_frac: f64,
    max_pos_usd: f64,
    depth_usd: f64,
) -> f64 {
    if ask <= 0.0 || ask >= 1.0 {
        return 0.0;
    }
    let b = (1.0 - ask) / ask;
    let f_star = p - (1.0 - p) / b;
    if f_star <= 0.0 {
        return 0.0;
    }
    let raw = bankroll_usd * kelly_frac * f_star;
    raw.clamp(0.0, max_pos_usd).min(depth_usd.max(0.0))
}

/// Self-healing rolling recalibration. Tracks recent (predicted, realized) on
/// closed positions and applies a scalar de-bias so the bot stays calibrated
/// through regime shifts. The walk-forward showed a scalar de-bias drives the
/// optimism from +0.046 to +0.007 with AUC preserved — isotonic added nothing,
/// so a scalar is the right tool.
///
/// `bias = mean(predicted - realized)` over the rolling window; the recalibrated
/// probability is `p_adj = (pcal(z) - bias).clamp(0, 1)`. Serializable so it
/// survives restarts.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Recalibrator {
    /// Rolling (predicted_p, realized_win) pairs, newest at the back.
    window: VecDeque<(f64, f64)>,
    /// Max pairs retained (e.g. ~last 300 closed trades).
    capacity: usize,
    /// Minimum samples before any de-bias is applied (avoid early noise).
    warmup: usize,
}

impl Recalibrator {
    #[must_use]
    pub fn new(capacity: usize, warmup: usize) -> Self {
        Self { window: VecDeque::with_capacity(capacity), capacity: capacity.max(1), warmup }
    }

    /// Record a closed trade: the probability we predicted at entry and whether it won.
    pub fn record(&mut self, predicted_p: f64, won: bool) {
        self.window.push_back((predicted_p, if won { 1.0 } else { 0.0 }));
        while self.window.len() > self.capacity {
            self.window.pop_front();
        }
    }

    /// Current scalar de-bias `= mean(predicted - realized)`; 0 before warmup.
    #[must_use]
    pub fn bias(&self) -> f64 {
        if self.window.len() < self.warmup || self.window.is_empty() {
            return 0.0;
        }
        let n = self.window.len() as f64;
        let sum: f64 = self.window.iter().map(|(p, w)| p - w).sum();
        sum / n
    }

    /// Recalibrated win probability: `pcal(z)` shifted by the current bias.
    #[must_use]
    pub fn apply(&self, raw_p: f64) -> f64 {
        (raw_p - self.bias()).clamp(0.0, 1.0)
    }

    #[must_use]
    pub fn samples(&self) -> usize {
        self.window.len()
    }
}

impl Default for Recalibrator {
    fn default() -> Self {
        // ~last 300 trades, apply only after 50 (a few green/red days of data).
        Self::new(300, 50)
    }
}

/// Per-asset Binance 1s-close history — the feed v2 needs that `SharedState`
/// doesn't keep (it stores only the latest tick). Owned by the decision task and
/// updated on every finalized kline, alongside the `SignalEngine`. Deep enough to
/// look back to a 15m window's open (900s) plus margin for the vol lookback.
#[derive(Debug, Default)]
pub struct PriceHistory {
    /// asset → ring of `(open_second, close)`, ascending in time, capped at `cap`.
    by_asset: HashMap<String, VecDeque<(i64, f64)>>,
    cap: usize,
}

impl PriceHistory {
    /// `cap` = max 1s bars retained per asset. 1024 covers a 15m window (900s) +
    /// the 60s vol lookback + margin.
    #[must_use]
    pub fn new(cap: usize) -> Self {
        Self { by_asset: HashMap::new(), cap: cap.max(2) }
    }

    /// Ingest one finalized 1s kline. Update-in-place on a duplicate second, else
    /// append and evict the oldest past `cap` (same discipline as `SignalEngine`).
    pub fn push(&mut self, asset: &str, sec: i64, close: f64) {
        let q = self.by_asset.entry(asset.to_string()).or_default();
        match q.back_mut() {
            Some(back) if back.0 == sec => *back = (sec, close),
            _ => {
                q.push_back((sec, close));
                while q.len() > self.cap {
                    q.pop_front();
                }
            }
        }
    }

    /// Close of the latest bar at or before `sec` (tolerates gaps), or `None`.
    #[must_use]
    pub fn close_at(&self, asset: &str, sec: i64) -> Option<f64> {
        let q = self.by_asset.get(asset)?;
        let mut last = None;
        for &(t, c) in q.iter() {
            if t <= sec {
                last = Some(c);
            } else {
                break;
            }
        }
        last
    }

    /// Closes in `[from_sec, to_sec]` inclusive, ascending — for the vol window.
    #[must_use]
    pub fn closes_in(&self, asset: &str, from_sec: i64, to_sec: i64) -> Vec<f64> {
        match self.by_asset.get(asset) {
            None => Vec::new(),
            Some(q) => q
                .iter()
                .filter(|(t, _)| *t >= from_sec && *t <= to_sec)
                .map(|(_, c)| *c)
                .collect(),
        }
    }

    /// Rolling vol (bps/s) over the last `lookback_s` seconds ending at `now_sec`.
    #[must_use]
    pub fn vol_bps(&self, asset: &str, now_sec: i64, lookback_s: i64) -> Option<f64> {
        let closes = self.closes_in(asset, now_sec - lookback_s, now_sec);
        vol_bps(&closes)
    }

    /// Side-signed velocity (bps) over `window_s` ending at `now_sec`. `up` selects
    /// the side sign. Mirrors the Python 2s rolling-return velocity.
    #[must_use]
    pub fn vel_bps(&self, asset: &str, now_sec: i64, window_s: i64, up: bool) -> Option<f64> {
        let cur = self.close_at(asset, now_sec)?;
        let prev = self.close_at(asset, now_sec - window_s)?;
        if prev <= 0.0 {
            return None;
        }
        let raw = (cur / prev - 1.0) * 1e4;
        Some(if up { raw } else { -raw })
    }

    /// Compute the full v2 [`Features`] for one candidate, or `None` if the history
    /// can't price it (missing open / vol). `epoch_sec` is the window-open second,
    /// `now_sec` the decision second, `ttl_s` seconds to settlement, `ask` the PM
    /// best ask, `recal_bias` the current recalibration de-bias.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn features(
        &self,
        asset: &str,
        epoch_sec: i64,
        now_sec: i64,
        ttl_s: f64,
        up: bool,
        ask: f64,
        recal_bias: f64,
        cal_z: &[f64],
        cal_w: &[f64],
        vol_lookback_s: i64,
    ) -> Option<Features> {
        let open = self.close_at(asset, epoch_sec - 1)?; // close just before window open
        let cur = self.close_at(asset, now_sec)?;
        let vol = self.vol_bps(asset, now_sec, vol_lookback_s)?;
        let vel = self.vel_bps(asset, now_sec, 2, up).unwrap_or(0.0);
        let d = disp_bps(open, cur, up);
        let z = zscore(d, vol, ttl_s)?;
        let p = (pcal_with(z, cal_z, cal_w) - recal_bias).clamp(0.0, 1.0);
        Some(Features {
            disp_bps: d,
            vel_bps: vel,
            vol_bps: vol,
            ttl_s,
            z,
            dvr: dvr(d, vel),
            ask,
            p,
            edge: edge(p, ask),
        })
    }
}

/// Live, operator-adjustable runtime controls — shared between the decision loop
/// (reads them on the hot path) and the dashboard (writes them via the control
/// endpoint). Lock-free (atomics): stakes are stored as milli-USD integers.
///
/// This is how you run the real-money test WITHOUT editing config + restarting:
/// set `base_usd` to 1.05, watch it for a day, then bump it from the dashboard.
/// `trading_enabled=false` PAUSES new entries (open positions still exit/redeem).
#[derive(Debug)]
pub struct Controls {
    trading_enabled: AtomicBool,
    base_usd_milli: AtomicU64,      // 5m stake
    max_pos_milli: AtomicU64,       // 5m cap
    base_usd_15m_milli: AtomicU64,  // 15m stake (independent)
    max_pos_15m_milli: AtomicU64,   // 15m cap
    inval_stop_on: AtomicBool,      // invalidation stop master switch (dashboard)
    inval_stop_dry: AtomicBool,     // stop DRY-RUN (log would-fires, no sell)
}

impl Controls {
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        trading_enabled: bool,
        base_usd: f64,
        max_pos_usd: f64,
        base_usd_15m: f64,
        max_pos_15m: f64,
        inval_stop_on: bool,
        inval_stop_dry: bool,
    ) -> Self {
        let m = |v: f64| AtomicU64::new((v.max(0.0) * 1000.0) as u64);
        Self {
            trading_enabled: AtomicBool::new(trading_enabled),
            base_usd_milli: m(base_usd),
            max_pos_milli: m(max_pos_usd),
            base_usd_15m_milli: m(base_usd_15m),
            max_pos_15m_milli: m(max_pos_15m),
            inval_stop_on: AtomicBool::new(inval_stop_on),
            inval_stop_dry: AtomicBool::new(inval_stop_dry),
        }
    }
    #[must_use]
    pub fn enabled(&self) -> bool {
        self.trading_enabled.load(Ordering::Relaxed)
    }
    pub fn set_enabled(&self, v: bool) {
        self.trading_enabled.store(v, Ordering::Relaxed);
    }
    #[must_use]
    pub fn base_usd(&self) -> f64 {
        self.base_usd_milli.load(Ordering::Relaxed) as f64 / 1000.0
    }
    /// Clamp to a sane floor (>= $0.10) so a fat-finger can't set $0 / negative.
    pub fn set_base_usd(&self, v: f64) {
        self.base_usd_milli.store((v.clamp(0.10, 10_000.0) * 1000.0) as u64, Ordering::Relaxed);
    }
    #[must_use]
    pub fn max_pos_usd(&self) -> f64 {
        self.max_pos_milli.load(Ordering::Relaxed) as f64 / 1000.0
    }
    pub fn set_max_pos_usd(&self, v: f64) {
        self.max_pos_milli.store((v.clamp(0.10, 10_000.0) * 1000.0) as u64, Ordering::Relaxed);
    }
    // --- 15m sizing (independent of 5m) ---
    #[must_use]
    pub fn base_usd_15m(&self) -> f64 {
        self.base_usd_15m_milli.load(Ordering::Relaxed) as f64 / 1000.0
    }
    pub fn set_base_usd_15m(&self, v: f64) {
        self.base_usd_15m_milli.store((v.clamp(0.10, 10_000.0) * 1000.0) as u64, Ordering::Relaxed);
    }
    #[must_use]
    pub fn max_pos_15m(&self) -> f64 {
        self.max_pos_15m_milli.load(Ordering::Relaxed) as f64 / 1000.0
    }
    pub fn set_max_pos_15m(&self, v: f64) {
        self.max_pos_15m_milli.store((v.clamp(0.10, 10_000.0) * 1000.0) as u64, Ordering::Relaxed);
    }
    /// Per-interval stake/cap (15m uses its own; everything else = 5m).
    #[must_use]
    pub fn base_usd_for(&self, interval: &str) -> f64 {
        if interval == "15m" { self.base_usd_15m() } else { self.base_usd() }
    }
    #[must_use]
    pub fn max_pos_for(&self, interval: &str) -> f64 {
        if interval == "15m" { self.max_pos_15m() } else { self.max_pos_usd() }
    }
    // --- invalidation stop (dashboard live control) ---
    #[must_use]
    pub fn inval_stop_on(&self) -> bool {
        self.inval_stop_on.load(Ordering::Relaxed)
    }
    pub fn set_inval_stop_on(&self, v: bool) {
        self.inval_stop_on.store(v, Ordering::Relaxed);
    }
    #[must_use]
    pub fn inval_stop_dry(&self) -> bool {
        self.inval_stop_dry.load(Ordering::Relaxed)
    }
    pub fn set_inval_stop_dry(&self, v: bool) {
        self.inval_stop_dry.store(v, Ordering::Relaxed);
    }

    /// Serializable snapshot of every operator-adjustable control, so dashboard
    /// changes SURVIVE a restart (systemd auto-restart, reboot, redeploy). Without
    /// this, a crash at 3am silently reverts stakes + the invalidation stop to the
    /// config defaults.
    #[must_use]
    pub fn snapshot(&self) -> ControlsSnapshot {
        ControlsSnapshot {
            trading_enabled: self.enabled(),
            base_usd: self.base_usd(),
            max_pos: self.max_pos_usd(),
            base_usd_15m: self.base_usd_15m(),
            max_pos_15m: self.max_pos_15m(),
            inval_stop_on: self.inval_stop_on(),
            inval_stop_dry: self.inval_stop_dry(),
        }
    }

    /// Apply a persisted snapshot over the current controls (startup restore).
    pub fn apply_snapshot(&self, s: &ControlsSnapshot) {
        self.set_enabled(s.trading_enabled);
        self.set_base_usd(s.base_usd);
        self.set_max_pos_usd(s.max_pos);
        self.set_base_usd_15m(s.base_usd_15m);
        self.set_max_pos_15m(s.max_pos_15m);
        self.set_inval_stop_on(s.inval_stop_on);
        self.set_inval_stop_dry(s.inval_stop_dry);
    }

    /// Persist the current controls to `path` (best-effort; creates parent dirs).
    /// Called by the dashboard after every control change.
    pub fn save(&self, path: &str) {
        if let Some(dir) = std::path::Path::new(path).parent() {
            if !dir.as_os_str().is_empty() {
                let _ = std::fs::create_dir_all(dir);
            }
        }
        if let Ok(json) = serde_json::to_string(&self.snapshot()) {
            let _ = std::fs::write(path, json);
        }
    }
}

/// Persisted operator controls (see `Controls::snapshot`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControlsSnapshot {
    pub trading_enabled: bool,
    pub base_usd: f64,
    pub max_pos: f64,
    pub base_usd_15m: f64,
    pub max_pos_15m: f64,
    pub inval_stop_on: bool,
    pub inval_stop_dry: bool,
}

/// Load a persisted controls snapshot from `path` (None if missing/unparseable).
#[must_use]
pub fn load_controls(path: &str) -> Option<ControlsSnapshot> {
    let s = std::fs::read_to_string(path).ok()?;
    serde_json::from_str(&s).ok()
}

/// Load the persisted recalibrator from `path`, or a fresh one (with the given
/// capacity/warmup) if the file is missing or unparseable. Never fails — a bad
/// file falls back to a cold recalibrator (frozen pcal), the safe default.
#[must_use]
pub fn load_recal(path: &str, capacity: usize, warmup: usize) -> Recalibrator {
    match std::fs::read_to_string(path) {
        Ok(s) => serde_json::from_str::<Recalibrator>(&s).unwrap_or_else(|_| Recalibrator::new(capacity, warmup)),
        Err(_) => Recalibrator::new(capacity, warmup),
    }
}

/// Persist the recalibrator to `path` (creating parent dirs). Best-effort.
pub fn save_recal(r: &Recalibrator, path: &str) -> std::io::Result<()> {
    if let Some(dir) = std::path::Path::new(path).parent() {
        if !dir.as_os_str().is_empty() {
            std::fs::create_dir_all(dir)?;
        }
    }
    let json = serde_json::to_string(r)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    std::fs::write(path, json)
}

/// Resolved per-interval strategy parameters for the decision loop. Built once per
/// kline from `V2Config` (5m base) and the optional 15m override, so `process_kline_v2`
/// can apply DIFFERENT gates + calibration per interval without touching the pure fn.
/// The 5m strat is byte-for-byte the old behavior; 15m carries its own curve, recal
/// bias, late-entry gate (`late_entry_max_ttl_s`), and price cap (`max_ask`).
#[derive(Debug, Clone)]
pub struct IntervalStrat {
    /// Trade this interval at all (lets 15m be toggled independently of 5m).
    pub enabled: bool,
    pub z_min: f64,
    pub edge_min: f64,
    pub vol_cap: f64,
    pub dvr_floor: f64,
    pub edge_ref: f64,
    pub min_ttl_s: i64,
    /// Only enter when `ttl <= late_entry_max_ttl_s` (the 15m late-entry edge).
    /// `0` = no late gate (5m).
    pub late_entry_max_ttl_s: i64,
    /// Skip when `ask > max_ask` (quality cap). `0` (or `>=1`) = no cap.
    pub max_ask: f64,
    /// Win-prob calibration knots for this interval.
    pub cal_z: Vec<f64>,
    pub cal_w: Vec<f64>,
    /// Current rolling-recalibrator de-bias for this interval.
    pub recal_bias: f64,
    /// Per-interval stake + position cap (dashboard-adjustable, independent 5m/15m).
    pub base_usd: f64,
    pub max_pos_usd: f64,
    /// Rolling-vol lookback (seconds) for z. Matched to the horizon: 60s for 5m,
    /// 120s for 15m (a 60s window is too twitchy for a 900s market → mis-scaled z).
    pub vol_lookback_s: i64,
}

/// Per-interval rolling recalibrators. 5m and 15m have different base win rates, so
/// a shared recal would cross-contaminate — each interval self-heals independently.
#[derive(Clone)]
pub struct RecalSet {
    pub m5: std::sync::Arc<std::sync::Mutex<Recalibrator>>,
    pub m15: std::sync::Arc<std::sync::Mutex<Recalibrator>>,
    pub path_m5: String,
    pub path_m15: String,
}

impl RecalSet {
    /// The recalibrator for an interval string (anything != "15m" → 5m).
    #[must_use]
    pub fn for_interval(&self, interval: &str) -> &std::sync::Arc<std::sync::Mutex<Recalibrator>> {
        if interval == "15m" { &self.m15 } else { &self.m5 }
    }

    /// Persist both recalibrators to their paths (best-effort; logs nothing here).
    pub fn save_both(&self) {
        if let Ok(r) = self.m5.lock() {
            let _ = save_recal(&r, &self.path_m5);
        }
        if let Ok(r) = self.m15.lock() {
            let _ = save_recal(&r, &self.path_m15);
        }
    }

    /// Current de-bias for each interval (locks briefly; 0.0 if poisoned).
    #[must_use]
    pub fn bias(&self, interval: &str) -> f64 {
        self.for_interval(interval).lock().map(|r| r.bias()).unwrap_or(0.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn approx(a: f64, b: f64, eps: f64) -> bool {
        (a - b).abs() < eps
    }

    #[test]
    fn pcal_endpoints_and_monotonic() {
        // DRIFTLESS BASELINE: z<=0 => 0.5 (a non-move is a coin flip), not 0.593.
        assert!(approx(pcal(-1.0), 0.5, 1e-12));
        assert!(approx(pcal(0.0), 0.5, 1e-12));
        // [0, 0.14) interpolates (0,0.5) -> (0.14,0.593): midpoint ~0.5465.
        assert!(approx(pcal(0.07), 0.5 + 0.5 * (0.593 - 0.5), 1e-9));
        assert!(approx(pcal(0.14), 0.593, 1e-9)); // first knot
        assert!(approx(pcal(10.49), 0.964, 1e-9)); // last knot
        assert!(approx(pcal(99.0), CAL_W[7], 1e-12)); // clamp high
        // strictly increasing across the knots
        let mut prev = pcal(-5.0);
        for z in [0.0, 0.07, 0.3, 0.6, 1.0, 1.5, 2.0, 3.0, 5.0, 11.0] {
            let p = pcal(z);
            assert!(p >= prev - 1e-12, "pcal must be non-decreasing at z={z}");
            prev = p;
        }
    }

    #[test]
    fn pcal_midpoint_interpolates() {
        // midpoint between knots 0 (0.14,0.593) and 1 (0.45,0.680)
        let zmid = (0.14 + 0.45) / 2.0;
        let want = (0.593 + 0.680) / 2.0;
        assert!(approx(pcal(zmid), want, 1e-9));
    }

    #[test]
    fn vol_of_constant_series_is_none() {
        assert_eq!(vol_bps(&[100.0, 100.0, 100.0]), None); // zero variance -> not > 0
        assert_eq!(vol_bps(&[100.0]), None); // too short
    }

    #[test]
    fn vol_is_positive_for_moving_series() {
        let v = vol_bps(&[100.0, 100.1, 100.0, 100.2, 100.1]).unwrap();
        assert!(v > 0.0 && v.is_finite());
    }

    #[test]
    fn z_and_disp_signs() {
        // Up side, price up 10 bps from open -> positive disp.
        let d = disp_bps(100.0, 100.1, true);
        assert!(approx(d, 10.0, 1e-6));
        // Down side, same move -> negative disp (move against us).
        let d2 = disp_bps(100.0, 100.1, false);
        assert!(approx(d2, -10.0, 1e-6));
        // z = disp / (vol * sqrt(ttl))
        let z = zscore(10.0, 2.0, 100.0).unwrap();
        assert!(approx(z, 10.0 / (2.0 * 10.0), 1e-9)); // = 0.5
        assert_eq!(zscore(10.0, 0.0, 100.0), None);
        assert_eq!(zscore(10.0, 2.0, 0.0), None);
    }

    #[test]
    fn edge_and_fee() {
        let ask = 0.60;
        let f = fee(ask);
        assert!(approx(f, 0.07 * 0.60 * 0.40, 1e-12));
        // p above ask+fee -> positive edge
        assert!(edge(0.78, ask) > 0.0);
        assert!(edge(0.50, ask) < 0.0);
    }

    #[test]
    fn gate_rejects_high_vol_low_dvr_low_edge() {
        let base = Features {
            disp_bps: 5.0, vel_bps: 3.0, vol_bps: 0.4, ttl_s: 120.0,
            z: 1.5, dvr: 1.6, ask: 0.60, p: 0.80, edge: 0.80 - 0.60 - fee(0.60),
        };
        assert_eq!(gate(&base), Gate::Take);
        // high vol
        let mut hv = base; hv.vol_bps = 1.5;
        assert_eq!(gate(&hv), Gate::Skip("vol_cap"));
        // coin-flip spike
        let mut sp = base; sp.dvr = 0.1;
        assert_eq!(gate(&sp), Gate::Skip("dvr_floor"));
        // edge too thin
        let mut te = base; te.edge = 0.01;
        assert_eq!(gate(&te), Gate::Skip("edge_min"));
        // displacement against us
        let mut nd = base; nd.disp_bps = -1.0;
        assert_eq!(gate(&nd), Gate::Skip("no_disp"));
    }

    #[test]
    fn edge_stake_scales_and_caps() {
        // at edge_ref, stake == base (depth permitting)
        let s = edge_stake(0.08, 10.0, 0.08, 1.05, 50.0, 1000.0);
        assert!(approx(s, 10.0, 1e-9));
        // double edge -> ~double stake
        let s2 = edge_stake(0.16, 10.0, 0.08, 1.05, 50.0, 1000.0);
        assert!(approx(s2, 20.0, 1e-9));
        // capped by max_pos
        let s3 = edge_stake(1.0, 10.0, 0.08, 1.05, 50.0, 1000.0);
        assert!(approx(s3, 50.0, 1e-9));
        // capped by depth (book only offers $7)
        let s4 = edge_stake(0.16, 10.0, 0.08, 1.05, 50.0, 7.0);
        assert!(approx(s4, 7.0, 1e-9));
        // no edge -> no stake
        assert_eq!(edge_stake(0.0, 10.0, 0.08, 1.05, 50.0, 1000.0), 0.0);
    }

    #[test]
    fn edge_stake_always_whole_cents() {
        // REGRESSION: differential sizing produced amounts like 2.4375 → Polymarket
        // rejects market-buy USDC with >2 decimals. edge_stake must FLOOR to cents
        // for every input, and never exceed max_pos.
        for &(edge, base, eref, maxp) in &[
            (0.13, 2.0, 0.08, 4.0), // -> raw 3.25, clean
            (0.10, 2.0, 0.08, 4.0), // -> raw 2.5, clean
            (0.075, 2.0, 0.08, 4.0), // -> raw 1.875 -> floor 1.87
            (0.19, 3.0, 0.08, 4.0), // -> raw 7.125 -> clamp 4.00
            (0.061, 2.13, 0.08, 4.0), // messy on purpose
        ] {
            let s = edge_stake(edge, base, eref, 1.05, maxp, maxp);
            let cents = (s * 100.0).round();
            assert!((s * 100.0 - cents).abs() < 1e-9, "stake {s} is not whole cents");
            assert!(s <= maxp + 1e-9, "stake {s} breached max_pos {maxp}");
        }
    }

    #[test]
    fn kelly_stake_positive_only_with_edge() {
        // p > ask -> positive fraction
        let s = kelly_stake(0.80, 0.60, 100.0, 0.25, 50.0, 1000.0);
        assert!(s > 0.0);
        // p below break-even -> zero
        assert_eq!(kelly_stake(0.50, 0.60, 100.0, 0.25, 50.0, 1000.0), 0.0);
        // depth cap
        let s2 = kelly_stake(0.95, 0.60, 100.0, 0.25, 50.0, 3.0);
        assert!(approx(s2, 3.0, 1e-9));
    }

    #[test]
    fn recalibrator_debiases_optimism() {
        let mut r = Recalibrator::new(300, 50);
        assert_eq!(r.bias(), 0.0); // warmup
        // Simulate optimism: predicted 0.75 but only 0.65 realized.
        for i in 0..200 {
            r.record(0.75, i % 100 < 65); // ~65% win
        }
        let bias = r.bias();
        assert!(bias > 0.05 && bias < 0.12, "bias ~0.10, got {bias}");
        // Applying the de-bias pulls a 0.75 prediction down toward realized.
        let adj = r.apply(0.75);
        assert!(adj < 0.70 && adj > 0.60, "adj ~0.65, got {adj}");
    }

    #[test]
    fn recalibrator_rolls_off_old_samples() {
        let mut r = Recalibrator::new(10, 1);
        for _ in 0..10 { r.record(0.9, true); } // perfectly calibrated-ish (pred .9, win 1)
        assert!(r.bias() < 0.0); // predicted below realized -> negative bias
        for _ in 0..10 { r.record(0.9, false); } // now all losses, window replaced
        assert_eq!(r.samples(), 10);
        assert!(r.bias() > 0.0); // predicted .9 but realized 0 -> positive bias
    }

    #[test]
    fn price_history_lookup_and_eviction() {
        let mut h = PriceHistory::new(4);
        for (t, c) in [(100, 1.0), (101, 2.0), (102, 3.0), (103, 4.0), (104, 5.0)] {
            h.push("BTC", t, c);
        }
        // cap=4 → oldest (100) evicted.
        assert_eq!(h.close_at("BTC", 100), None);
        assert_eq!(h.close_at("BTC", 104), Some(5.0));
        // gap tolerance: latest <= 103 is t=103.
        assert_eq!(h.close_at("BTC", 103), Some(4.0));
        // update-in-place on duplicate second.
        h.push("BTC", 104, 5.5);
        assert_eq!(h.close_at("BTC", 104), Some(5.5));
        assert_eq!(h.close_at("ETH", 104), None);
    }

    #[test]
    fn price_history_builds_features_and_gates() {
        let mut h = PriceHistory::new(1024);
        // Window opens at epoch=1000 (open price = close at 999). Build a gentle
        // sustained up-move so disp>0, modest vol, dvr healthy.
        let open = 100.0;
        h.push("BTC", 999, open);
        // Net up-drift WITH return variance (a perfectly steady ramp has zero
        // return-variance → vol=0 → correctly None; real prices always wiggle).
        for t in 1000..=1080 {
            let i = (t - 1000) as f64;
            let px = open + 0.01 * i + 0.02 * (i * 1.3).sin(); // drift + wiggle
            h.push("BTC", t, px);
        }
        let now = 1080;
        let ttl = 120.0;
        let f = h
            .features("BTC", 1000, now, ttl, /*up=*/ true, /*ask=*/ 0.60, /*bias=*/ 0.0, &CAL_Z, &CAL_W, 60)
            .expect("features computable");
        assert!(f.disp_bps > 0.0, "up-move → positive displacement");
        assert!(f.vol_bps > 0.0 && f.vol_bps.is_finite());
        assert!(f.z > 0.0);
        assert!((f.p - pcal(f.z)).abs() < 1e-12); // bias=0 → p == pcal(z)
        // Cold start: window-open is BEFORE any history (earliest bar = 999) →
        // can't price the open → None (the correct skip).
        assert!(h.features("BTC", 800, now, ttl, true, 0.60, 0.0, &CAL_Z, &CAL_W, 60).is_none());
    }
}
