# Pre-registration — Volatility-Oracle Ceiling (the Kronos feasibility gate)

Registered 2026-07-26, BEFORE any result was computed. Author: auditor session.

## Why this experiment exists

The operator asked whether [Kronos](https://github.com/shiyu-coder/Kronos) (an
open-source K-line foundation model, AAAI 2026, MIT) could help this audit.
Kronos's only claim that touches our decision problem is **volatility
forecasting** (9% lower MAE than the strongest of 25 baselines incl. GARCH).
Direction forecasting is already tombstoned here five ways over (entry-feature
ML, momentum extrapolation, loser-filter, judge-alignment, PM flow tape), so a
price-direction framing is not eligible for re-testing without new framing.

The volatility framing IS new, and it is load-bearing, because our entire win
probability is a volatility computation:

    z = disp_bps / (vol60 · sqrt(ttl))          # v2.rs::zscore
    p = pcal(z)                                 # v2.rs::pcal, piecewise-linear

`disp_bps` and `ttl` are OBSERVED exactly at decision time. `vol60` — the
trailing-60s realised vol used as an estimate of the volatility of the
*remaining* window — is the single forecast input in the whole model. If that
estimate is materially wrong, every downstream number (p, edge, the gate, and
any future sizing rule) inherits the error.

So the question is not "is Kronos good?" but **"how much money is available to
ANY better volatility forecast?"** That ceiling is measurable today without
Kronos, without a GPU, and without writing a line of Rust.

## Design

Population: the **floored paper population** — entries after the Order #11
restart (ts >= 2026-07-20 22:03 UTC), which is the population the audition is
being scored on and the only one a future sizing rule would apply to. 5m and 15m
scored separately (they have different curves, floors, and vol lookbacks).
Photo-finish rows are retained but reported as a separate cut, because Binance
labels flip ~20% there.

Oracle construction. Replace the trailing estimate `vol60` with the *realised*
volatility of the remaining window, computed from Binance 1s closes with the
identical estimator the bot uses (`v2.rs::vol_bps`: population std of 1s log
returns × 1e4):

  - `sigma_fwd_full`  — realised vol over [signal_ts, resolution). LOOSE ceiling:
    it is partly entangled with the outcome, since a hard reversion through zero
    contributes its own volatility. Deliberately generous.
  - `sigma_fwd_early` — realised vol over the FIRST HALF of the remaining
    window only. Much less entangled with the terminal move; the honest ceiling.

Then `z_oracle = disp_bps / (sigma_fwd · sqrt(ttl))`, `p_oracle = pcal(z_oracle)`
with the deployed per-interval knots, and `edge_oracle = p_oracle − ask − fee`.

The oracle CHEATS (it has perfect foresight of forward vol). Every real
forecaster, Kronos included, must land strictly below it. That asymmetry is the
point: a failing oracle is a decisive kill for the whole family; a passing
oracle is only permission to continue.

## Pre-registered two-branch verdict

Primary money metric: mean hold-basis EV per $1 staked on the re-gated
population, matched-volume, with a 10k-resample day-clustered bootstrap CI.

- **PASS** — the oracle lifts EV/$1 by **>= +0.02** over the deployed gate on the
  floored paper population, with bootstrap CI excluding 0, AND improves win-prob
  discrimination (lower Brier, higher AUC) on `sigma_fwd_early`.
  → Volatility forecasting is a real lever. ONLY THEN proceed to stage 2:
  measure how much of that ceiling a cheap estimator captures (vol120, EWMA,
  HAR-RV on 1s bars, GARCH). Kronos's addressable market is the residual after
  the cheap estimators, not the whole ceiling.

- **FAIL** — the oracle lift is **< +0.02/$1**, or the CI includes 0.
  → No volatility forecaster can pay for itself here, because a cheating one
  cannot. Tombstone "better volatility forecast" as an entry/sizing lever and
  decline Kronos on measured grounds. Do not re-test without a genuinely new
  framing.

NO in-between tuning. I will not sweep thresholds, drop cells, or re-cut the
population after seeing the result. If the result lands between branches by some
metric I did not name, it is a FAIL and I report the ambiguity.

## What this experiment does NOT test

- Kronos as a *photo-finish* classifier (P(|final move| < 2bps)) — a different
  target, registered separately if this passes or if the stickiness study
  motivates it.
- Kronos for the maker book's cancel logic or for hourly markets.
- Any cross-sectional/RankIC use, which is irrelevant to a 2-asset book.
