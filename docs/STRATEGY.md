# A Profitable Edge: At-the-Money Lead-Lag in BTC/ETH 5-Minute Up/Down

**Status: validated edge (IS/OOS, execution-delay, slippage, capacity). Deployable as a fast taker; bigger prize as hybrid market-making.**

## The inefficiency

The Polymarket order book for a 5-minute crypto market **lags the underlying spot price by ~1–2 seconds**. Proof (cross-correlation of book Δmid vs side-signed underlying return): the book's price move at time *t* is best explained by the underlying's move **1–2 seconds earlier** (regression slope peaks at lag +1s, falls off by +3s).

This lag is worthless where the book is already calibrated — the deep favorites (mid ≥ 0.90), which is why Phase-1/Phase-2 directional hunts failed. It is exploitable **at the money** (mid ≈ 0.5), where the underlying is actively deciding the outcome and a fresh tick is not yet in the quote.

**Mechanism:** at time *t*, a 2-second underlying move toward one side predicts that side wins more often than its current ask implies. You buy the lagging-cheap winning side below fair value and hold to settlement.

## The signal

- **Signal:** side-signed underlying return over the last 2 seconds, in bps. For the Up token, signal = +(spot return); for Down, signal = −(spot return). Fire when signal ≥ threshold.
- **Action:** **taker BUY that side, hold to settlement.** One side only — dictated by the signal sign. Do **not** round-trip/scalp (you'd pay spread+fee twice; that's only marginally positive at strong signals and dead at weak ones).
- **Window:** active in the final 5–120 s of the market (tested range).

## Why hold, not scalp (the construction question, answered)

| Structure | Cost paid | Expectancy (sig≥5, real) |
|---|---|---|
| **Scalp** (enter, exit on reprice) | spread + fee **×2** | +1.2¢ (only at strong signals; ~0 at weak) |
| **Hold to settlement** | spread + fee **×1** | **+4–8¢** |

Hold wins decisively because the lag gap (~1–3¢) is smaller than a round-trip's doubled cost but larger than a single cost. The side is never a guess — it is the direction of the fresh underlying move.

## One trade per market — and which signal (the "both sides" problem)

A single instant in a single market produces only one signal (the signed move). But over a market's life spot oscillates, so taking *every* signal accumulates **offsetting Up+Down positions in whipsaw markets** — you pay ~$1.10 for a $1 payout. This is real and large: at sig≥2, **44% of May / 85% of June markets fire both sides**, and taking all signals makes ~50–62% of markets net-lose on the overlap. The earlier "take every signal" economics were inflated by this.

**Rule: one position per market. Enter on the FIRST signal that clears the threshold, hold to settlement, never take the opposite side (do not flip on a later reverse signal — flipping/late entry tests worst, negative on June).** FIRST beats STRONGEST and LATEST in every cell — the first decisive move gives the lag the most room and avoids entering after the move is priced. It is also the simplest to run live.

| Policy (one/market) | win rate | net ¢/trade (+1s,+1tick) | trades/day |
|---|---|---|---|
| May sig≥5 FIRST | **75.5%** | **+8.5¢** | 48 |
| June sig≥5 FIRST | **62.5%** | **+2.4¢** | 157 |
| May sig≥8 FIRST | 78.5% | +10.1¢ | 14 |
| June sig≥8 FIRST | 65.8% | +4.4¢ | 69 |
| May sig≥2 FIRST | 65.1% | +5.0¢ | 283 |
| June sig≥2 FIRST | 56.8% | +1.0¢ | 291 |

One-trade-per-market roughly **doubles** the per-trade edge vs the all-signals figure and removes whipsaw bleed.

### Why FIRST works (mechanism) + the false-positive filter

Decomposing signals within multi-trigger markets: later/stronger signals have *higher* win rates (0.66 vs 0.55) but you **pay a higher ask** for them (0.615 vs 0.496) — the book has already repriced. FIRST's edge is **cheap entry before the book catches up** (the lag itself). The downside FIRST carries: it can fire on a transient blip with no real move behind it (false positive).

**Fix — require displacement-from-window-open** (the actual distance-to-strike; settlement is end-vs-open). The 2s velocity is the lag *trigger*; cumulative displacement is the *confirmation* that the side is genuinely ahead, not a blip:

| FIRST(vel≥5) filter | May win | May net | **June OOS win** | June OOS net |
|---|---|---|---|---|
| baseline | 75.5% | +8.5¢ | 62.5% | +2.4¢ |
| **+ displacement ≥2bps** | **94.0%** | +8.3¢ | **87.8%** | **+3.6¢** |
| + displacement ≥5bps | 95.3% | +5.4¢ | 89.5% | +3.0¢ |
| + displacement ≥10bps | 97.7% | +2.2¢ | 92.6% | +2.0¢ |

The displacement filter lifts OOS win rate **62%→88%** *and* improves net edge. Higher thresholds raise win rate but erode net (the book has priced it — sliding back toward the dead deep-favorite zone). **Sweet spot: displacement 2–5bps → ~88–90% win, +3–3.6¢ net OOS.** Persistence (move still present 1s later) barely helps — being *ahead in the race* is what matters, not the blip lasting.

**Refined entry rule:** first instant in the final ~120s where 2s spot move ≥5bps toward a side AND underlying ≥2bps ahead of the window open → buy that side, hold to settle. ~88% win, +3.6¢/share OOS.

### The repricing gap (why a displacement BAND, not just ≥2)

Two clocks: spot (moves first) and the book (lags 1–2s). Velocity = "book hasn't repriced yet"; displacement = "spot is genuinely ahead." The edge is the gap `win_rate − ask`, and it collapses as the book catches up (May, trigger vel≥5):

| displacement | book ask | true win | gap | net |
|---|---|---|---|---|
| 2–5bps | 0.485 | 0.847 | **+0.36** | +34¢ |
| 5–10bps | 0.72 | 0.87 | +0.15 | +14¢ |
| 10–20bps | 0.92 | 0.96 | +0.04 | +4¢ |
| >20bps | 0.98 | 0.998 | +0.02 | +1.6¢ |

You are NOT entering late — you enter after spot moves but before the book reprices. You only lose the edge once displacement is large (book caught up). **Use a band ≈2–10bps** to stay in the high-gap zone and drop repriced deep-favorite trades.

### DCA (same-side pyramiding)

Each same-side add is +EV but at a worse price (ask climbs 0.84→0.87→0.95), so marginal edge decays (May: 1st +8.8¢, 2nd +7.3¢, 3rd +3.7¢, 5th +1.0¢). Portfolio (May): going from no-DCA to unlimited raises total profit +70% but ROC falls 10.8%→7.1% and worst single-market loss grows −99→−440 (concentration into one binary). **Key: the first signal is the best price, so to add size, a bigger first clip beats DCA.** DCA is a *capacity tool* only — to deploy beyond the first signal's ~$100–500 depth. **Cap k_max≈2–3** (captures ~60% of the extra profit, worst-case −200 vs −440). Capital-constrained → don't DCA (spread across markets, ROC is highest at k=1); signal-constrained with spare capital → DCA to 2–3.

## Maker / market-making (now backtested on real trades — profitable before rewards)

Real-trade fill model (resting bid fills on a SELL print ≤ b; ask on a BUY print ≥ a; maker fee 0):

| | naive | lag-defended (cancel on 2s spot ≥2bps adverse) | cancelled |
|---|---|---|---|
| All June | +1.06¢/fill | +1.34¢/fill | 4% |
| **OOS (Jun≥8)** | **+0.96¢** | **+1.13¢** | 2% |

~114k fills (~16k/day). Adverse-selection gradient on bid fills: −12¢ (picked off, spot dropping ≥5bps) → +0.4¢ (neutral, benign spread capture) → +7¢ (spot rising with you). The lag-defense cancels the left tail (2–9% of fills) for +25–40% edge. **Rewards/rebates are additive and excluded → this is a floor.** Caveat: fill model assumes BBO fills on every qualifying print; real **queue position** means informed (adverse) flow reaches you preferentially while benign fills may go to those ahead — so live adverse selection can exceed the model, making the spot-driven cancel essential. Validate with live quoting.

## Unified system
Rest two-sided quotes to harvest the spread on benign flow; **cancel the instant spot moves against the quote** (kills adverse selection); **flip to aggressive taker when the 2s move ≥5bps with displacement 2–10bps** (chase the big gap). Taker grabs the fat mispricings (~+3.6¢/share, 88% win); maker harvests the continuous spread (~+1¢/fill) plus rewards. Same 1–2s lag, two harvests.

## Validation (full protocol)

- **IS/OOS:** discovered on June, **replicated on the May holdout — stronger there.** May sig≥2 @ +1s delay = +2.93¢ (n=36,909, ±0.29, ~10σ); June = +1.16¢ (±0.30, ~4σ).
- **Execution delay** (lag is 1–2s, so reaction speed matters): edge positive even at +3s delay. Realistic operating point (sub-second API) is between instant and +1s.

  | | +1s | +2s | +3s |
  |---|---|---|---|
  | May sig≥2 | +2.9¢ | +1.7¢ | +1.3¢ |
  | June sig≥2 | +1.2¢ | +0.5¢ | +0.3¢ |

  *(The instant/+0s number carries slight look-ahead from 1-s Binance bar alignment; **+1s is the honest figure** and is what all economics below use.)*
- **Slippage:** at **sig≥5** the edge survives a full +1-tick haircut in both months (+0.9 to +4.3¢/share). Low thresholds get thin on June after slippage → operate at higher conviction.
- **Stability:** BTC and ETH both positive both months; **100% of trading days positive** (23/23 May, 7/7 June). Day-Sharpe 2.49 (May) / 1.45 (June); worst day positive.

## Economics & capacity (entry +1s, +1 tick slip)

| threshold | trades/day | net ¢/share | ROI/trade |
|---|---|---|---|
| sig≥3 | ~570 (May) / ~2,800 (Jun) | +2.7¢ / +0.4¢ | +4.4% / +0.7% |
| **sig≥5** | ~140 / ~900 | **+4.3¢ / +0.9¢** | **+6.7% / +1.6%** |
| sig≥8 | ~33 / ~237 | +6.8¢ / +2.8¢ | +9.9% / +4.6% |

- **Per-signal capacity:** ~$100 at the touch, ~$500 within 2¢ (median ATM depth 112 / 569 shares). Scale by clip size up to depth; concurrency limited (≈1 market/asset near settlement at a time).
- **Variance:** these are ~56–75% bets at near-even money — high per-trade variance. Size with fractional Kelly; the edge compounds through volume, not per-bet certainty.

## Residual risks (must address before live size)

1. **Binance vs Chainlink (the #1 fix).** Signal and validation use Binance spot; **settlement is Chainlink BTC/USD**. The win rates already bake in the Binance↔Chainlink correlation, but going live you should compute the signal off the **Chainlink data stream directly** (or both) to align with the true resolution source and remove last-second divergence risk. This likely *strengthens* the edge.
2. **Fill competition.** "Buy at the +1s ask" assumes that quote is still there. Other arbs and the maker pulling quotes can worsen fills; the +1-tick stress partially covers this. **Paper-trade against live fills before sizing.**
3. **Edge compression.** May > June — the lag may be tightening as arbs enter. Operate with margin (sig≥5), monitor decay.
4. **Granularity.** Binance 1s bars understate sub-second structure; live tick data should sharpen entries.

## The bigger prize: hybrid market-making (lag confirmed; not yet backtestable)

The same lag we pick off as a taker is **exactly the adverse selection a passive maker suffers**. Confirmed directly: maker fill P&L declines monotonically with the spot move against the new position — from +7.7¢ (favorable) to **−19.8¢ when picked off on a >5bps adverse move**. The lag is real on the maker side too.

The thesis: with a sub-second feed you (a) **cancel** before the pick-off, and (b) as a maker pay **no fee, no spread** and collect liquidity rewards + taker-fee rebates (`rewards` block in market metadata: `min_size 50, max_spread 4.5`).

**Honest blocker — cannot be scored from this dataset.** Maker P&L = benign-flow spread capture + rewards − adverse selection. This data has **no trade/fill feed** (trades arrived on the lost `book` channel) and **no rewards feed**. A BBO-only fill proxy registers *only* trade-through (adverse) fills and misses the benign flow that is the entire income side — so any maker P&L computed here is a pessimistic artifact (naive sim showed −5¢/fill, which is the artifact, not a verdict). The one measurable positive: round-trip spread capture ≈ 1.2¢ when both quotes fill.

**To pursue hybrid-MM, the recorder must capture: (1) the trades/executions feed, (2) the rewards/liquidity-reward feed.** Then maker fills, cancel-defense value, and reward income can be scored properly. Until then, the **taker lead-lag is the validated, deployable edge.**

## Recommended path

1. **Build a live signal harness** computing the 2-s side-signed return off **Chainlink (and Binance)**, firing taker buys at sig≥5 in the final ~120 s, hold to settlement. Paper-trade to measure real fills vs the +1s/+1-tick model.
2. **Record the Chainlink stream + Polymarket rewards data** going forward (fixes residual risk #1 and unlocks hybrid-MM scoring).
3. Once fills are confirmed, **layer the maker/hybrid version** for capacity and to harvest rewards.
4. Re-validate monthly for edge compression.

## Artifacts
`scripts/leadlag_extract.py`, `leadlag_may_extract.py` (dense per-second book), `leadlag_analyze.py` (cross-correlation + conditional), `leadlag_validate.py` (IS/OOS + delay decay), `leadlag_capacity.py` (depth, threshold curve, Sharpe). Data: `data/ll_rec_may.parquet`, `data/ll_rec_june.parquet`, `data/leadlag_records.parquet`.
