# Phase 2 — Filtered Strategy Search

**Goal: find any profitable configuration. Result: NONE survives honest treatment.**
Method: May = in-sample selection, June = out-of-sample; conservative (exact-binomial / Wilson) expectancy bounds; execution realism (fresh-quote requirement age ≤ 2 s, +1 tick slippage); label-noise controls.

## Families tested

### F1 — Distance-to-strike (raw bps lead, and z = lead / σ√(time-left))
The filter has real signal: favorite win rate rises monotonically with z (74% at z≤0 → 98.2% at z>5). But the ask rises with it — the market already prices the lead. EV by z bucket (May, W=10–60, mid≥0.90) is negative in **every** bucket.
Eleven cells passed conservative IS selection (concentrated at W=5s / P=0.85 and W=90s / z≥4); most replicated OOS at the raw quote. Under execution honesty they die:

| Cell | raw May/June | fresh ≤2s | fresh + 1-tick slip |
|---|---|---|---|
| W=5 mid≥.85 z≥3 | +0.99% / +1.52% | +0.41% / +0.61% | **−0.38% / −0.20%** |
| W=15 mid≥.90 lead≥8 | +1.31% / +1.19% | +1.08% / +0.67% | +0.53% / **−0.22%** |
| W=90 mid≥.92 z≥4 | +1.64% / +2.14% | unchanged | +0.80% / +1.10% (zero-loss cell) |

A third to half of the raw "edge" comes from stale quotes (unexecutable prices); one tick of slippage removes the rest. The W=90 survivor is a 342/342 zero-loss cell whose own conservative bound is negative — and its neighbors expose it: identical filter at W=45/60/120 shows **10 losses in 858 pooled trades (1.17%) vs 0.72% break-even → −0.44%/share observed, −1.25% conservative**. Escalating to z≥5/6 still cannot clear the 99.4%+ break-even win rates.

### F2 — Book imbalance (June-only; May has no depth)
Sign flips between cuts (REST-snapshot bucketing favored high-imbalance; decision-time split favors low-imbalance) and between June halves. Not robust, no IS/OOS support possible. Discard.

### F3 — Combos (z + spread ≤ 1 tick + non-negative momentum)
Same fate as F1: positive raw, dead after fresh+slip.

### F4 — Photo-finish underdog (buy the ~$0.06 side when |move| ≈ 0 but favorite ≥ 0.90)
The most instructive failure. May showed +12%/share (n=240) with beautiful monotonic structure. **It was label noise**: splitting May by label confidence, the clean-label subset is **−2.7%/share (wr 2.9%)** while the ambiguous-label subset shows "98% underdog wins" — i.e., the Binance-based May labeler systematically mislabels exactly these markets (the order book tracks the Chainlink resolution feed better than Binance does). June (authoritative labels): +5.9%/share at |bps|≤2 — but n=40 (5 wins), CI spans zero. **Unproven; the only honest verdict is "interesting, needs months of Chainlink-true data."** The monotonic win-rate structure (33% → 6% as |move| grows 0→3bps, asks flat ~0.06–0.07) is real in June and is the one place a future edge might live.

### F5 — Maker variants (favorite + z filter; underdog photo-finish maker)
Adverse selection survives every filter: filled favorite bids win 84–94% (need ~bid price ≈ 90–95%+), EV per posted order negative in all 24 May cells. Underdog maker (June): fills win 0–14%, EV negative in 7 of 8 cells. Discard.

## Why nothing works

The favorite's late price is calibrated; the *cost* side (half-spread + fee + slippage ≈ 1–1.5¢) exceeds any residual mispricing (≤ 0.5¢). Filters move you along the price-probability curve but cannot manufacture a gap between them. At deep-favorite prices the required win rates (98.5–99.5%) are simultaneously (a) not achieved per observed loss rates and (b) not certifiable with any feasible sample size.

## If you still want to pursue this

1. **Record Chainlink data-stream prices directly** (the actual resolution source). This both fixes labels and creates the genuinely interesting signal: book-vs-oracle divergence in the final seconds (32 June markets resolved against the final book consensus).
2. Collect 3–6 months of photo-finish underdog samples (≈ 5–10/day qualify) and re-test F4 against authoritative labels only.
3. Any live attempt must beat: 1 tick slippage, 0.07·p(1−p) fee, ~0.5–1 s feed latency, and queue position. Paper-trade against real fills before sizing anything.

Artifacts: `data/decisions.parquet` (528k decision rows with all features), `data/filter_sweep.csv`, `data/f4_underdog.csv`, `data/f5_maker_z.csv`, `data/f2_imbalance.csv`; scripts `binance_features.py`, `build_decision_table.py`, `filter_strategies.py`, `confound_checks.py`, `final_cells.py`.
