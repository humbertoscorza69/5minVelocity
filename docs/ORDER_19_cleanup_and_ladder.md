# Order #19 — Retire the variants, dry the stop, and step onto the ladder

Small build. The value is in what gets switched OFF, and in not shipping sizing yet.

Author: auditor session, 2026-08-06. Evidence: `livelogs/paperlogs_20260805_2341.tar.gz`,
9.35 days post-arming (Jul 27 15:11 → Aug 5 23:40), 2,234 V0 trades.

---

## Part A — Kill V1 and V2. The A/B is decided.

| arm | settled/day | $/day | EV/$1 | PF | days + |
|---|---|---|---|---|---|
| **V0** | 238.8 | **+$19.38** | **+0.0773** | **1.28** | **10/10** |
| V1 | 505.9 | +$14.65 | +0.0276 | 1.12 | 8/10 |
| V2 | 273.7 | +$14.13 | +0.0492 | 1.17 | 9/10 |

V0 hold-only, stripped of its stop for a like-for-like read: **+$18.38/day** — still ahead
of both. Kill rates (V1 12.1%, V2 15.1%) are *under* the 25% leg, so kills are not the
mechanism; the burst population simply has a far worse per-trade edge and ~2× volume does
not compensate.

Set `[v2.variants] enabled = false`. Leave the code in place — the ShadowBook machinery,
the isolation proof and the log contract are all reusable for the sizing A/B, which is
the next thing that will need them.

## Part B — Dry the invalidation stop. Its registered gauge has failed.

The re-registered rule was *"rolling net dEV/stop > 0 over trailing 500."* Measured over
4,523 post-arming stops:

| window | dEV/stop | saved / whipsawed |
|---|---|---|
| first 500 | +0.0169 | 286 / 214 (1.34) |
| middle 3,523 | +0.0032 | 1,284 / 2,239 |
| **last 500** | **−0.0204** | **181 / 319 (0.57)** |

Over the whole window the stop contributed **+$9.35 total** (~+$1/day) while producing 13%
of exits and their churn. The bot's own banner already says *"stop bleeding vs hold,
consider disarm."*

Set `inval_stop_dry_run = true` — **and fix it in `controls.json`**, which is currently
overriding config (`config true → control false`). That silent override already ran this
stop through an entire audition against a registered decision to dry it; it must not
happen a third time. Keep the stop *evaluating and logging* in dry mode so the gauge
continues.

## Part C — Fix the asleep telemetry regression

`asleep` is null on **79% of intents** (banner says 75%). This was fixed once in `45599ca`
and has come back. It is improving across the window (0.866 → 0.679 null by day) which
suggests a warmup or ring-coverage issue rather than a hard break. Low priority against
A and B, but it blocks the asleep experiment permanently if left.

## Part D — What NOT to build yet: sizing

The sizing study found a real effect — vol-conditioned edge sizing gives **+$4.56/day over
flat, paired day-clustered CI [+2.02, +7.71], the best Sortino of seven rules (10.21 vs
flat's 9.70), 10/10 positive days and a worst day that is still positive.**

**It nonetheless FAILED its pre-registered bar** (it exceeds flat's max drawdown: $19.56 vs
$15.58), the sealed-block leg was never evaluated because 10 days cannot support the
protocol, and the compounding figures are a variance demonstration rather than a forecast.

Do not implement a sizing mode in this order. It needs 25–30 days and a re-registration
with a properly specified risk constraint. What it does **not** need is new logging — `p`,
`ask`, `z`, `disp_bps` and `ttl_s` are already on every intent, and `vol60` is recoverable
from the identity `z = disp/(vol·√ttl)`, so the study runs offline on what the bot already
writes.

---

## The step-by-step the operator asked for

**1. Clean up (this order).** Variants off, stop dry, controls.json corrected. Deploy and
confirm: no `variants_armed` line, `inval_stop` events all `dry_run=true`, V0's intent rate
unchanged (~239/day), recal files still moving normally.

**2. Verify 24–48h in paper.** One clean stretch on the cleaned config. Nothing to tune —
this is a "did we break anything" window, not an experiment.

**3. Step onto the funding ladder: $40–60, flat $1.05, LIVE.**
The re-arm gate from the restart protocol is met: ≥3 paper days on the current build (we
have 10), net positive (+$181.30), invariant clean, canary cycling. The one deviation is
the 5m recal bias at **−0.079**, outside the ±0.06 band — but *negative*, meaning the curve
**under**-predicts and the population is winning more than the model says. That is the
conservative direction; it costs volume, not money.
$50 against a daily sd of ~$5–7 at $1 stakes is ~7–10× — it clears the 4–5× rule.
Registered kill line: **−$13 in a UTC day → disarm and export.**

**4. Accumulate 25–30 live days.** This is the part that cannot be shortened, and going
live *improves* it: the sizing study then runs on real fills instead of paper ones. Every
paper→live transition in this project has disappointed, so measuring sizing on live data
is strictly better than measuring it on paper and hoping.

**5. Then, and only then, the sizing A/B.** Re-register with the risk constraint specified
as drawdown-per-unit-return or a %-of-bankroll ceiling (my absolute "≤ flat's maxDD" bar
demanded a free lunch and was mis-specified). Run VOLCONDEDGE against FLAT using the same
ShadowBook machinery this order is switching off — one process, two virtual portfolios,
V0 untouched. Ship only if it passes including the sealed block.

## What NOT to touch

`recal.json` / `recal_15m.json`, the floors, knots, `edge_min`, `z_min`, `vol_lookback_s`,
V0's entry path. The 5m curve is currently mis-set in the conservative direction and a
refit is a *candidate*, not this order — bias swung from +0.032 to −0.079 in six days and
chasing a six-day swing is how the curve got mis-set last time.
