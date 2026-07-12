//! Order #7 Part C — the regime canary.
//!
//! The Jul 11 live burn was a chop-regime tail event (BTC 1m|ret| tripled while
//! trend-efficiency collapsed to 0.43): displacement kept firing and kept
//! reversing, the single worst regime for a displacement-continuation strategy. A
//! $10 bankroll traded at full cadence through it cannot survive the variance.
//! This module de-risks (AMBER) or halts (RED) entries per asset during chop.
//!
//! TWO ARMS feed one per-asset GREEN → AMBER → RED state:
//!   * Arm 1 — rolling HOLD-to-settle win rate over the last `n_window` 5m
//!     positions (the same settle label the recal feed uses, INCLUDING the
//!     counterfactual outcome of stopped positions — realized WR is distorted by
//!     stops). < wr_amber → AMBER, < wr_red → RED. Only Arm 1 can force RED.
//!   * Arm 2 — vol ACCELERATION: trailing-10m / trailing-60m mean |1m return|.
//!     ratio ≥ trig AND 10m level ≥ floor → force AMBER (that asset) while the
//!     condition holds + a hold-over. The RATIO is the signal, not the level (last
//!     night 22h averaged only 2.2 bp/min — a fixed level halt would have missed).
//!
//! AMBER = cap stake_mult to 1.0 (kill the multipliers) + suspend re-entries;
//! entries continue. RED = halt ALL new entries for the asset (5m + 15m — chop is
//! asset-level); open positions are managed normally; blocked signals are logged
//! as shadow intents so the auditor can measure what the halt saved or cost.
//!
//! RESUME (v1 fallback, no shadow-settlement scoring): after `red_cooldown_ms` in
//! RED, resume to AMBER probation; the next `resume_probation_n` settles promote to
//! GREEN if their hold-WR ≥ `resume_probation_wr`, else back to RED.

use std::collections::{HashMap, VecDeque};

use serde_json::{json, Value};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CanaryState {
    Green,
    Amber,
    Red,
}

impl CanaryState {
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            CanaryState::Green => "green",
            CanaryState::Amber => "amber",
            CanaryState::Red => "red",
        }
    }
    /// AMBER or RED both suppress the sizing multipliers + re-entries.
    #[must_use]
    pub fn derisked(&self) -> bool {
        !matches!(self, CanaryState::Green)
    }
    #[must_use]
    pub fn halted(&self) -> bool {
        matches!(self, CanaryState::Red)
    }
}

#[derive(Clone, Debug)]
pub struct CanaryConfig {
    pub enabled: bool,
    pub n_window: usize,
    pub wr_amber: f64,
    pub wr_red: f64,
    pub vol_ratio_trig: f64,
    pub vol_floor_bpm: f64,
    pub red_cooldown_ms: i64,
    pub resume_probation_n: usize,
    pub resume_probation_wr: f64,
    pub vol_amber_hold_ms: i64,
}

impl Default for CanaryConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            n_window: 30,
            wr_amber: 0.50,
            wr_red: 0.45,
            vol_ratio_trig: 2.0,
            vol_floor_bpm: 2.0,
            red_cooldown_ms: 60 * 60 * 1000,
            resume_probation_n: 10,
            resume_probation_wr: 0.50,
            vol_amber_hold_ms: 10 * 60 * 1000,
        }
    }
}

#[derive(Clone, Debug)]
struct Asset {
    hold: VecDeque<bool>, // last n_window hold-to-settle outcomes
    state: CanaryState,
    since_ms: i64,
    red_since_ms: i64,
    // Resume probation after a RED cooldown.
    probation: bool,
    prob_n: usize,
    prob_w: usize,
    // Arm 2 vol.
    vol_10m: f64,
    vol_60m: f64,
    vol_amber_until_ms: i64,
}

impl Asset {
    fn new(now_ms: i64) -> Self {
        Self {
            hold: VecDeque::new(),
            state: CanaryState::Green,
            since_ms: now_ms,
            red_since_ms: 0,
            probation: false,
            prob_n: 0,
            prob_w: 0,
            vol_10m: 0.0,
            vol_60m: 0.0,
            vol_amber_until_ms: 0,
        }
    }
    fn wr(&self, n_window: usize) -> Option<f64> {
        if self.hold.len() >= n_window {
            let w = self.hold.iter().filter(|b| **b).count();
            Some(w as f64 / self.hold.len() as f64)
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub struct Canary {
    cfg: CanaryConfig,
    assets: HashMap<String, Asset>,
}

/// A state change, returned so the caller can emit an oplog event.
pub struct Transition {
    pub asset: String,
    pub from: CanaryState,
    pub to: CanaryState,
    pub wr: Option<f64>,
    pub n: usize,
    pub vol_ratio: f64,
    pub reason: &'static str,
}

impl Canary {
    #[must_use]
    pub fn new(cfg: CanaryConfig) -> Self {
        Self { cfg, assets: HashMap::new() }
    }

    fn asset_mut(&mut self, asset: &str, now_ms: i64) -> &mut Asset {
        self.assets.entry(asset.to_string()).or_insert_with(|| Asset::new(now_ms))
    }

    /// Current state for an asset (GREEN if the canary is disabled or unseen).
    #[must_use]
    pub fn state(&self, asset: &str) -> CanaryState {
        if !self.cfg.enabled {
            return CanaryState::Green;
        }
        self.assets.get(asset).map(|a| a.state).unwrap_or(CanaryState::Green)
    }

    /// Record a settled 5m HOLD-to-settle outcome (won at settlement, including the
    /// counterfactual outcome of a stopped position). Drives Arm 1. Returns a
    /// Transition iff the state changed.
    pub fn record_hold(&mut self, asset: &str, won: bool, now_ms: i64) -> Option<Transition> {
        if !self.cfg.enabled {
            return None;
        }
        let (n_window, wr_amber, wr_red, cooldown, prob_n_target, prob_wr) = (
            self.cfg.n_window, self.cfg.wr_amber, self.cfg.wr_red, self.cfg.red_cooldown_ms,
            self.cfg.resume_probation_n, self.cfg.resume_probation_wr,
        );
        let a = self.asset_mut(asset, now_ms);
        a.hold.push_back(won);
        while a.hold.len() > n_window {
            a.hold.pop_front();
        }
        if a.probation {
            a.prob_n += 1;
            if won {
                a.prob_w += 1;
            }
        }
        Self::recompute(asset, a, now_ms, n_window, wr_amber, wr_red, cooldown, prob_n_target, prob_wr)
    }

    /// Update Arm 2 vol (trailing 10m + 60m mean |1m return|, bps/min). Latches
    /// AMBER for `vol_amber_hold_ms` when ratio ≥ trig AND 10m ≥ floor.
    pub fn update_vol(&mut self, asset: &str, vol_10m: f64, vol_60m: f64, now_ms: i64) -> Option<Transition> {
        if !self.cfg.enabled {
            return None;
        }
        let (n_window, wr_amber, wr_red, cooldown, prob_n_target, prob_wr, ratio_trig, floor, hold_ms) = (
            self.cfg.n_window, self.cfg.wr_amber, self.cfg.wr_red, self.cfg.red_cooldown_ms,
            self.cfg.resume_probation_n, self.cfg.resume_probation_wr, self.cfg.vol_ratio_trig,
            self.cfg.vol_floor_bpm, self.cfg.vol_amber_hold_ms,
        );
        let a = self.asset_mut(asset, now_ms);
        a.vol_10m = vol_10m;
        a.vol_60m = vol_60m;
        let ratio = if vol_60m > 1e-9 { vol_10m / vol_60m } else { 0.0 };
        if ratio >= ratio_trig && vol_10m >= floor {
            a.vol_amber_until_ms = now_ms + hold_ms; // (re)latch: condition + hold-over
        }
        Self::recompute(asset, a, now_ms, n_window, wr_amber, wr_red, cooldown, prob_n_target, prob_wr)
    }

    /// Time-driven re-evaluation (call periodically so a RED cooldown expires and a
    /// vol latch releases even when no settle/vol update arrives).
    pub fn tick(&mut self, asset: &str, now_ms: i64) -> Option<Transition> {
        if !self.cfg.enabled || !self.assets.contains_key(asset) {
            return None;
        }
        let (n_window, wr_amber, wr_red, cooldown, prob_n_target, prob_wr) = (
            self.cfg.n_window, self.cfg.wr_amber, self.cfg.wr_red, self.cfg.red_cooldown_ms,
            self.cfg.resume_probation_n, self.cfg.resume_probation_wr,
        );
        let a = self.assets.get_mut(asset).unwrap();
        Self::recompute(asset, a, now_ms, n_window, wr_amber, wr_red, cooldown, prob_n_target, prob_wr)
    }

    #[allow(clippy::too_many_arguments)]
    fn recompute(
        asset: &str, a: &mut Asset, now_ms: i64, n_window: usize, wr_amber: f64, wr_red: f64,
        cooldown: i64, prob_n_target: usize, prob_wr: f64,
    ) -> Option<Transition> {
        let from = a.state;
        let wr = a.wr(n_window);
        let vol_amber = now_ms < a.vol_amber_until_ms;

        // Arm 1 verdict from the rolling window (None until the window fills).
        let arm1 = match wr {
            Some(w) if w < wr_red => CanaryState::Red,
            Some(w) if w < wr_amber => CanaryState::Amber,
            _ => CanaryState::Green,
        };

        let new = if a.probation {
            // Resume probation: evaluate after prob_n_target fresh settles.
            if a.prob_n >= prob_n_target {
                a.probation = false;
                let pw = a.prob_w as f64 / a.prob_n.max(1) as f64;
                a.prob_n = 0;
                a.prob_w = 0;
                if pw >= prob_wr {
                    CanaryState::Green
                } else {
                    a.red_since_ms = now_ms;
                    CanaryState::Red
                }
            } else {
                // Still probating → hold AMBER (capped, re-entries off). Arm 1 does
                // not force RED here; the fresh settles decide.
                CanaryState::Amber
            }
        } else if a.state == CanaryState::Red {
            // In RED: stay until the cooldown expires, then resume to AMBER probation.
            if now_ms - a.red_since_ms >= cooldown {
                a.probation = true;
                a.prob_n = 0;
                a.prob_w = 0;
                CanaryState::Amber
            } else {
                CanaryState::Red
            }
        } else {
            // Normal operation: Arm 1 drives; Arm 2 can bump GREEN→AMBER.
            match arm1 {
                CanaryState::Red => {
                    a.red_since_ms = now_ms;
                    CanaryState::Red
                }
                CanaryState::Amber => CanaryState::Amber,
                CanaryState::Green => {
                    if vol_amber {
                        CanaryState::Amber
                    } else {
                        CanaryState::Green
                    }
                }
            }
        };
        // Arm 2 can force AMBER even out of probation/normal-green (precedence: any
        // RED wins; else AMBER wins over GREEN).
        let new = if new == CanaryState::Green && vol_amber { CanaryState::Amber } else { new };

        if new != from {
            a.state = new;
            a.since_ms = now_ms;
            let ratio = if a.vol_60m > 1e-9 { a.vol_10m / a.vol_60m } else { 0.0 };
            let reason = if new == CanaryState::Red {
                "holdwr_red"
            } else if vol_amber && new == CanaryState::Amber && wr.map(|w| w >= wr_amber).unwrap_or(true) {
                "vol_accel"
            } else if new == CanaryState::Amber {
                "holdwr_amber_or_resume"
            } else {
                "recovered"
            };
            Some(Transition {
                asset: asset.to_string(),
                from,
                to: new,
                wr,
                n: a.hold.len(),
                vol_ratio: ratio,
                reason,
            })
        } else {
            None
        }
    }

    /// Per-asset telemetry for stats.json / the dashboard banner.
    #[must_use]
    pub fn snapshot(&self) -> Value {
        let mut out = serde_json::Map::new();
        let mut worst = CanaryState::Green;
        for (asset, a) in &self.assets {
            if a.state == CanaryState::Red {
                worst = CanaryState::Red;
            } else if a.state == CanaryState::Amber && worst != CanaryState::Red {
                worst = CanaryState::Amber;
            }
            let ratio = if a.vol_60m > 1e-9 { a.vol_10m / a.vol_60m } else { 0.0 };
            out.insert(asset.clone(), json!({
                "state": a.state.as_str(),
                "wr30": a.wr(self.cfg.n_window),
                "n30": a.hold.len(),
                "vol10m": a.vol_10m,
                "vol60m": a.vol_60m,
                "vol_ratio": ratio,
                "since_ms": a.since_ms,
            }));
        }
        json!({ "enabled": self.cfg.enabled, "state": worst.as_str(), "assets": Value::Object(out) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> CanaryConfig {
        CanaryConfig::default()
    }

    // Fill the window with a given win rate.
    fn feed(c: &mut Canary, asset: &str, wins: usize, total: usize, t0: i64) {
        for i in 0..total {
            c.record_hold(asset, i < wins, t0 + i as i64);
        }
    }

    #[test]
    fn green_until_window_fills() {
        let mut c = Canary::new(cfg());
        // 29 dead-loss settles: still GREEN (n < 30, not enough to halt).
        feed(&mut c, "BTC", 0, 29, 0);
        assert_eq!(c.state("BTC"), CanaryState::Green);
    }

    #[test]
    fn amber_then_red_at_thresholds() {
        let mut c = Canary::new(cfg());
        // 30 settles at 48% WR → in [0.45, 0.50) → AMBER.
        feed(&mut c, "BTC", 14, 30, 0); // 14/30 = 0.467
        assert_eq!(c.state("BTC"), CanaryState::Amber);
        // Push WR below 0.45 → RED.
        let mut c2 = Canary::new(cfg());
        feed(&mut c2, "BTC", 12, 30, 0); // 12/30 = 0.40
        assert_eq!(c2.state("BTC"), CanaryState::Red);
    }

    #[test]
    fn green_when_healthy() {
        let mut c = Canary::new(cfg());
        feed(&mut c, "BTC", 20, 30, 0); // 0.667
        assert_eq!(c.state("BTC"), CanaryState::Green);
    }

    #[test]
    fn red_holds_through_cooldown_then_resumes_amber() {
        let mut c = Canary::new(cfg());
        feed(&mut c, "BTC", 12, 30, 0); // RED
        assert_eq!(c.state("BTC"), CanaryState::Red);
        // Before cooldown: still RED.
        c.tick("BTC", 30 * 60 * 1000);
        assert_eq!(c.state("BTC"), CanaryState::Red);
        // After 60 min: resume to AMBER (probation).
        c.tick("BTC", 61 * 60 * 1000);
        assert_eq!(c.state("BTC"), CanaryState::Amber);
    }

    #[test]
    fn resume_probation_promotes_to_green_on_good_run() {
        let mut c = Canary::new(cfg());
        feed(&mut c, "BTC", 12, 30, 0); // RED
        c.tick("BTC", 61 * 60 * 1000); // → AMBER probation
        assert_eq!(c.state("BTC"), CanaryState::Amber);
        // 10 fresh settles at 60% → promote to GREEN.
        let base = 62 * 60 * 1000;
        for i in 0..10 {
            c.record_hold("BTC", i < 6, base + i as i64);
        }
        assert_eq!(c.state("BTC"), CanaryState::Green);
    }

    #[test]
    fn resume_probation_falls_back_to_red_on_bad_run() {
        let mut c = Canary::new(cfg());
        feed(&mut c, "BTC", 12, 30, 0);
        c.tick("BTC", 61 * 60 * 1000);
        let base = 62 * 60 * 1000;
        for i in 0..10 {
            c.record_hold("BTC", i < 3, base + i as i64); // 30% → RED again
        }
        assert_eq!(c.state("BTC"), CanaryState::Red);
    }

    #[test]
    fn vol_acceleration_forces_amber_and_only_arm1_reds() {
        let mut c = Canary::new(cfg());
        feed(&mut c, "BTC", 25, 30, 0); // healthy → GREEN
        assert_eq!(c.state("BTC"), CanaryState::Green);
        // ratio 3.6/1.0 = 3.6 >= 2.0 AND 10m 3.6 >= 2.0 → AMBER.
        c.update_vol("BTC", 3.6, 1.0, 1_000);
        assert_eq!(c.state("BTC"), CanaryState::Amber);
        // Vol alone never RED.
        assert_ne!(c.state("BTC"), CanaryState::Red);
        // Latch releases after the hold-over → back to GREEN (window still healthy).
        c.tick("BTC", 1_000 + 11 * 60 * 1000);
        assert_eq!(c.state("BTC"), CanaryState::Green);
    }

    #[test]
    fn disabled_is_always_green() {
        let mut cf = cfg();
        cf.enabled = false;
        let mut c = Canary::new(cf);
        feed(&mut c, "BTC", 0, 40, 0);
        assert_eq!(c.state("BTC"), CanaryState::Green);
    }

    #[test]
    fn per_asset_isolation() {
        let mut c = Canary::new(cfg());
        feed(&mut c, "BTC", 12, 30, 0); // BTC RED
        feed(&mut c, "ETH", 22, 30, 0); // ETH GREEN
        assert_eq!(c.state("BTC"), CanaryState::Red);
        assert_eq!(c.state("ETH"), CanaryState::Green);
    }
}
