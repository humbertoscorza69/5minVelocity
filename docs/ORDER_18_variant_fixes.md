# Order #18 — Variant A/B: exit parity, shadow recal persistence, per-variant dashboard

**Do NOT restart the clock.** The run is valid and continues. Read the corrections first.

Author: auditor session, 2026-07-30. Source: `livelogs/paperlogs_20260730_1447.tar.gz`,
2.98 days post-arming (2026-07-27 15:11 → 2026-07-30 14:45 UTC). The dev has not seen
these logs, so every number, event name and field below is quoted from them.

---

## Corrections to what I told the operator an hour ago

I made two wrong calls reading these logs. Both are retracted here so nothing is built
on them.

**WRONG #1 — "the FOK kill model is not applied to V1/V2".** It is. I searched
`variant_fok` for `v1`/`v2` rows, found none, and concluded kills were missing. In fact
`variant_fok` is V0's *counterfactual* channel by design (`trading_loop.rs:1550`, guarded
by `if cand.v0_admitted`), while V1/V2 kills are applied at `trading_loop.rs:1573`
(`if fok.killed { sb.record_kill(&day); … continue; }`) and logged on `v2_intent_open`
with `killed:true`. Measured, post-arming:

| arm | intents | killed | kill rate | mean slip |
|---|---|---|---|---|
| V0 (counterfactual) | 754 | 24 | **3.2%** | 0.0015 |
| V1 | 2194 | 144 | **6.6%** | 0.0062 |
| V2 | 1309 | 110 | **8.4%** | 0.0075 |

All well under the 25% hard-FAIL leg. The kill machinery works. **No action needed.**

**WRONG #2 — "both bugs bias toward the challengers".** The exit asymmetry biases the
other way. `stop_dev` (n=334 post-arming) sums `dev = +17.443`, so V0's band-stop **added
+$17.44** in this window. V0 gets a profitable exit that V1/V2 do not, so correcting the
asymmetry makes the variants look *better*, not worse.

## The one real bug: shadows have no exit policy

Confirmed three ways: 100.00% of `variant_pnl` rows resolve at exactly 0.0 or 1.0
(2155/1075 — every shadow position ran to settlement); there is no `sb.` call in any
stop/close path; and `shadow.rs` has no bid/band/stop capability at all. The stop tags
only `"variant":"v0"` (`trading_loop.rs:1026`).

Order #16 specified *"exits identical across all three"*. V0 runs hold+band-stop; V1/V2
run hold-only.

### Fix, but do not restart the clock

Give `ShadowBook` the deployed band-stop: while a shadow position is open, exit at the
bid when side-signed displacement ≤ 0 **and** bid ≥ 0.50 or ≤ 0.30. Same thresholds as
V0, same per-second evaluation, writing a `variant_pnl` row with the realised exit price
rather than a 0/1 settlement. Emit a shadow `stop_dev` carrying `dev` (stop-vs-hold) so
the same counterfactual exists for every arm.

**The clock keeps running because I can already score the current window
apples-to-apples without this fix:** `stop_dev.dev` gives V0's exact hold counterfactual,
so V0 can be stripped back to hold-only and compared to the shadows' hold-only numbers.
That is a complete comparison of the entry gates, which is what the experiment is for.
Once the fix lands we additionally get the full-policy comparison. Restarting would cost
three days and buy nothing.

## Second issue: shadow state is not persisting

The `shadow/` directory in the export is **empty**. `ShadowBook::new` takes a recal path
and `shadow.rs` exposes `recal_path()` / `recal_bias()` / `recal_samples()`, but nothing
is on disk. Two consequences: shadow recal and ledgers do not survive a restart (there
were 5 `variants_armed` events in this window, so restarts happen), and severance 5
("shadow state in its own files") is unverifiable.

Persist each `ShadowBook` (positions, ledger, day stats, recal window) to
`data/v2/shadow/shadow_v1.json` / `shadow_v2.json` on the same cadence V0's state is
written, and reload on start. Keep the existing refusal to accept a protected path.

## Third: per-variant dashboard (operator request)

Extend the existing segmented filter (the All/5m/15m pattern already in `dashboard.rs`)
with a variant dimension: **All / V0 / V1 / V2**. Per selection recompute net P&L,
EV/$1, win rate, PF, Sortino, entries/day, **kill rate**, mean ask, photo-finish share,
the cumulative curve, open positions and recent trades.

Add a comparison strip showing `V1−V0` and `V2−V0` on net $/day and kill rate, and show
**both** V0 baselines side by side — `net_v0_actual` and `net_v0_killadj` — so the
dual-baseline rule is visible while the run is live rather than only at scoring. Now that
`stop_dev.dev` exists per arm, also show V0 hold-only, since that is the like-for-like
figure against hold-only shadows until the stop fix lands.

Read-only. No per-variant controls — nothing that can accidentally arm or disarm an arm.

## Where the run actually stands (interim, 2.98 of 7 days — NOT a verdict)

Like-for-like, hold-only (V0 stripped of its stop via `stop_dev`):

| arm | settled | /day | net | $/day | EV/$1 | kill | vs V0 |
|---|---|---|---|---|---|---|---|
| **V0 hold-only** | 661 | 221.7 | +$21.94 | **+$7.36** | +0.0567 | 3.2% | — |
| **V1** | 2039 | 683.8 | +$70.81 | **+$23.74** | +0.0331 | 6.6% | **+222%** |
| **V2** | 1191 | 399.4 | +$67.52 | **+$22.64** | +0.0540 | 8.4% | **+208%** |

(V0's *realised* figure including its stop is +$39.38 = +$13.21/day; that is what the
dashboard shows and what the audition sees.)

Both variants clear the +50% leg by a wide margin on three days. **Three caveats keep
this from being a result:**

1. **V1's gain is one day.** Jul 28 alone is +$62.63 of its +$70.81 total (88%), and V1
   was **negative on both of the last two days**. Positive 2/4 days against a
   pre-registered bar of ≥5/7.
2. **V2 is the more robust arm so far** — positive 4/4 days, best day 54% of total, and a
   *higher* EV/$1 (+0.0540 vs V1's +0.0331). V1 buys volume at worse per-dollar edge,
   which is what the tournament predicted, but the tournament also said the burst arm's
   edge was participation not selection.
3. It is 3 days of 7. Do not act on it.

## What NOT to touch

`recal.json` / `recal_15m.json`, the floors, knots, `edge_min`, `z_min`,
`vol_lookback_s`, V0's stop behaviour or entry path, `controls.json`, and the live-arming
path. V0 must stay byte-identical; the `ee879c3` isolation proof must stay green.

## Separate flag for the operator, not the dev

Both auditions now show **deteriorating tails** while passing pooled: 5m n=300 bias
+0.0317 with thirds +0.002/+0.013/**+0.080**; 15m n=192 bias +0.0177 with thirds
−0.103/+0.045/**+0.111**. The 15m trailing third has printed above 0.10 on three
consecutive exports (+0.136, +0.141, +0.111). The pooled passes are being propped up by
older samples while the recent regime over-predicts. **Treat both auditions as
unresolved, and do not re-fund on the pooled number.**
