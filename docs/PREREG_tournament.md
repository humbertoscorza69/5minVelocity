# Pre-registration — Strategy Tournament on the D: recorder (~51 days)

Registered 2026-07-27, BEFORE any tournament result was computed. Auditor session.

## Why this is dangerous and how the design defends against it

This project has never run a systematic parameter optimisation, for a good reason:
with hundreds of configs over ~51 days, **the best-of-N config will look excellent by
chance alone**. Every previous blow-up in this ledger traces to a number that was true
of one population and false of the next. So the design below is built to measure the
*selection procedure*, not to crown a winner.

Four defences, all fixed in advance:

1. **Final holdout, touched exactly once.** The most recent ~20% of days are sealed.
   No config is selected, tuned, filtered, or even *inspected* on them. One evaluation
   at the very end, reported whatever it says.
2. **Rotating-block CV on the remaining 80%.** K=5 contiguous day-blocks. For each
   block: select the winner on the other four (IS), score it on the held-out block.
   The mean held-out score is the honest expectation of the *procedure*. The gap
   between mean-IS and mean-OOS is the overfitting tax, and I will report it explicitly.
3. **Permutation Monte Carlo.** Re-run the ENTIRE selection procedure on data whose
   outcomes have been shuffled within day (destroying edge, preserving structure and
   volume). This yields the distribution of "best-config OOS score under no edge" —
   i.e. it prices the multiple-comparison burden exactly rather than by rule of thumb.
   A real result must beat that null.
4. **Robustness constraint inside selection.** A config may only win if it is positive
   in >= 4 of 5 IS blocks. Fragile knife-edge cells are disqualified by construction —
   the max-ttl gate incident is the standing example of why.

## Fill realism — the primary metric is the pessimistic one

The decision table carries the per-second BBO forward-filled on *receipt* time. That
makes two fill models computable:

- **Idealised:** buy at the ask visible at the decision second.
- **Delayed (+1s):** buy at the ask visible one second later.

Every prior study in this ledger that reported idealised numbers had to be halved to
match live. **The delayed fill is the PRIMARY metric.** Idealised is reported only as
an upper bound.

Exits are simulated on the same per-second series: hold-to-settle, or the deployed
band-stop (sell at the bid when side-signed disp <= 0 and bid >= 0.50 or <= 0.30).

## Metrics

- **Primary / selection criterion:** mean daily net P&L at a fixed $1.05 stake,
  delayed fills, both P&L pipelines summed (survivors + stop closes).
- Secondary, reported never selected on: EV/$1, win rate, Sortino on daily P&L,
  max drawdown, trades/day, photo-finish share.
- Fees: entry `0.07 * ask * (1-ask)` per share, taker only. Sells and $1 redemptions
  are fee-free (verified live).
- Labels: Binance 1s closes, ties -> Up. Photo finishes (|final| < 2bps) flip ~20% vs
  Chainlink, so any config whose edge concentrates in the pf cohort is suspect by
  construction and I will check that explicitly for the winner.

## Pre-registered two-branch verdict

- **PASS** — the selected config's mean rotating-block OOS daily P&L exceeds the
  deployed config's by a margin that (a) survives the permutation null at p < 0.05,
  (b) holds on the sealed final holdout, and (c) was positive in >= 4/5 IS blocks.
  -> Write it as a dev order, re-based (a gate change re-scales z and requires the
  Order-#6 treatment), to ship only after the audition closes.
- **FAIL** — any of the three conditions misses.
  -> Report the deployed config as at its frontier and publish the null. No "close
  enough", no re-cutting the population, no widening the grid to find a winner.

NO in-between tuning. If the winner lands between branches I report FAIL and say so.

## Scope of the search

Entry-gate family only (the exit family is 8-for-8 settled and is not up for
re-litigation): z_min, edge_min, disp_floor, vol60_floor, min/max ask, min/max ttl,
vol lookback, frozen-tape seconds, book-unmoved threshold, per interval and asset.

Plus ONE genuinely new arm, motivated by fresh evidence rather than a grid: the
**burst trigger** (|1s/3s Binance return| >= X), which the legacy baseline bot uses and
which the Jul-27 head-to-head showed reaches a population the current bot never trades
(101% of the baseline's P&L sits in windows the current bot skipped, day-clustered CI
[+0.020, +0.180]). This is tested as an *additional* entry path, not a replacement.

## What is NOT eligible

Anything in the graveyard without a new framing: take-profit / reprice / bid-level
stops (8 refutations), entry-feature ML, momentum extrapolation, Platt/isotonic recal,
favourite-longshot, penny/near-certainty buying, book-noise reversion, cross-asset
sympathy, PM retail flow tape. If the grid surfaces one of these it is reported as a
grid artifact, not a discovery.
