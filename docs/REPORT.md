# Can You Profit by Buying the Apparent Winner of Polymarket BTC/ETH 5-Minute Up/Down Markets Just Before Settlement?

**Research report — 2026-06-12**
**Verdict: NO for the raw concept. The hypothesis is FALSE for taker execution and FALSE for maker execution. The favorite's price is well-calibrated; the entire apparent "edge" is consumed by spread + fee, and maker fills suffer fatal adverse selection.**

---

## 1. Dataset audit

### Sources

| Source | Coverage | Content | Role |
|---|---|---|---|
| `polymarket_recorder_2026-06-04_to_06-11.tar.zst` (20 GB) | Jun 4 20:56Z → Jun 11 ~02:30Z (Jun 7 missing — recorder down; Jun 11 partial — archive truncated) | websocket `price_change` events (667M rows after filtering, each carrying exchange-computed best bid/ask per token), REST full-depth book snapshots (~72 s cadence/token), tick-size changes, watchdog logs | **Out-of-sample** + all depth/execution analysis |
| `bbo_2026-05-*.parquet` (23 files) | May 6 11:30Z → May 30 00:00Z | best bid/ask change stream per token, exchange timestamps | **In-sample** (parameter discovery) |
| Polymarket CLOB API | — | question, slug, settlement time, **authoritative winner flags** for all 3,569 June markets | ground truth (June) |
| Binance 1s klines (BTC/ETHUSDT) | full May + June period | underlying price | May winner labels + failure analysis |

### Markets

| Series | June markets | May windows | Total |
|---|---|---|---|
| btc-5m | 1,334 | 5,965 | ~7,300 |
| eth-5m | 1,333 | 5,933 | ~7,265 |
| btc-15m | 451 | 1,995 | ~2,446 |
| eth-15m | 451 | 1,997 | ~2,448 |

Up/Down outcome balance 48–50%. Settlement: Chainlink BTC/USD (resp. ETH/USD) at window end ≥ window start → Up wins. Taker fee `0.07·p·(1−p)` per share; makers pay nothing.

### Data-quality findings

- The June archive is **truncated**: the websocket full-book channel (`book`), `markets`, and `market_resolved` channels are lost. Recovered: complete `price_change` streams for Jun 4–6, 8–10, partial Jun 11.
- **Full-depth reconstruction from `price_change` deltas alone is invalid** — trade executions arrive as book refreshes on the lost channel. Validated: delta-replay match vs REST truth decays from ~90% to ~0% over a token's life. Consequently all prices in this study use the **exchange-computed best bid/ask embedded in every event**, and all depth statements use **REST snapshots** (153k snapshots).
- Quote staleness check (REST truth vs last embedded quote): mean signed ask error ≈ 0 in every time-to-settle bucket (slightly *pessimistic* in the final 5 s) — backtest entry prices are not systematically optimistic.
- Recorder latency: median 0.47 s, p99 1.2 s (a live system would act on ~0.5 s-old quotes).

### May winner labels (no API ground truth available)

Labeling procedure validated against June's 2,871 markets with known winners:

| Labeler | Error rate (June) |
|---|---|
| Binance window-move sign | 1.46% |
| Final-second book mid | 1.88% |
| **Binance, cross-checked with book (agreement)** | **0.36%** |

May: 96.5% of 15,890 windows labeled confidently (both sources agree); the 3.5% disagreements are photo-finishes. A **worst-case sensitivity** (every ambiguous market counted as a loss) is reported below and does not change any conclusion.

---

## 2. Core finding: the favorite's price *is* its win probability

Calibration of the favorite's mid vs realized win rate (May, pooled 5m, n per cell 300–7,400):

| Offset | mid 0.80–0.90 | mid 0.90–0.95 | mid 0.95–0.98 | mid 0.98–0.995 |
|---|---|---|---|---|
| 120 s | wr .857 / mid .852 | .927 / .927 | .971 / .965 | .982 / .985 |
| 60 s | .851 / .854 | .917 / .928 | .963 / .967 | .983 / .985 |
| 30 s | .846 / .854 | .918 / .929 | .969 / .966 | .981 / .985 |
| 10 s | .866 / .855 | .917 / .928 | .962 / .967 | .986 / .985 |

Deviation is within ±1pp everywhere 10–120 s out (at 1 s the quoted mid lags the collapse and *over*states deep favorites that are mid-reversal). There is **no informational edge at the mid**: knowing "the favorite trades at 0.95" tells you it wins ~95% of the time — exactly what you pay for, before costs.

**Answer to RQ4 (earliest reliable moment):** reliability is a continuous function of price, not of time — the market is equally calibrated at every tested offset. There is no time at which the outcome is "more certain than priced."

---

## 3. Parameter sweep (taker at best ask, fee included)

8 windows × 10 thresholds × 4 series + pooled, May (IS) and June (OOS) separately. 320 5m-cells per month.

**May in-sample, pooled BTC+ETH 5m — expectancy per share (%):** *(all cells negative except micro-n P=0.99)*

| W\P | 0.80 | 0.85 | 0.90 | 0.95 | 0.97 | 0.98 | 0.99 |
|---|---|---|---|---|---|---|---|
| 5s | −1.62 | −1.78 | −1.71 | −1.09 | −0.96 | −0.97 | +0.11 |
| 10s | −1.18 | −1.26 | −1.25 | −0.90 | −0.65 | −0.48 | +0.11 |
| 15s | −1.16 | −1.00 | −0.80 | −0.73 | −0.28 | −0.21 | +0.14 |
| 30s | −1.41 | −1.36 | −1.13 | −0.74 | −0.96 | −0.98 | +0.18 |
| 60s | −1.42 | −1.54 | −1.29 | −0.92 | −1.04 | −0.77 | +0.23 |
| 120s | −0.74 | −0.63 | −0.64 | −0.37 | −0.80 | −0.91 | n/a |

n per cell: 435–6,471. The P=0.99 cells (n = 82–143, zero observed losses) flip to **−2.5% to −4.2%** under a Wilson lower-bound on win rate — with a 0.99 entry, a single loss per ~700 trades erases the edge; samples of ~100 cannot establish that.

June out-of-sample shows the same picture scattered around zero (−1.7% to +0.9%), with the positive cells (W=10–30) inside one standard error and **opposite in sign to May's same cells** — month-to-month noise, not edge.

### Where the money goes (May, pooled 5m)

| Cell | win rate | avg mid | avg ask | half-spread | fee | EV vs mid | **EV net** |
|---|---|---|---|---|---|---|---|
| W=10, P=0.90 | .9603 | .9646 | .9709 | .0062 | .0019 | −0.43% | **−1.25%** |
| W=10, P=0.95 | .9746 | .9767 | .9824 | .0058 | .0012 | −0.21% | **−0.90%** |
| W=30, P=0.95 | .9751 | .9763 | .9813 | .0050 | .0013 | −0.12% | **−0.74%** |
| W=120, P=0.90 | .9498 | .9478 | .9531 | .0053 | .0031 | +0.19% | **−0.64%** |

The mid is fair (±0.4pp). You lose ~0.5–0.6¢ of half-spread plus 0.1–0.3¢ of fee on every share. **Break-even win rates (= avg cost) sit 1–2pp above realized win rates in every cell.**

### IS/OOS protocol result

Selection on May only (pre-registered rule: n ≥ 50, conservative Wilson-bound EV > 0). Survivors: **zero 5-minute configs**. Four 15-minute configs survived (best: eth-15m W=45 P=0.90, +0.20% conservative); on June OOS, three of four went **negative** (−0.4% to −0.8%/share observed) and the fourth (77 trades, all wins) still fails its own conservative bound. With ~320 cells examined, this survival pattern is exactly what multiple-testing luck produces. **No configuration replicates.**

### Risk profile of representative cells (May, observed)

| Cell | n | win rate | expectancy | max DD (shares) | max consec losses |
|---|---|---|---|---|---|
| btc-5m W=10 P=0.95 | 1,263 | 97.1% | −1.2% | 17.0 | 2 |
| btc-5m W=30 P=0.90 | 2,070 | 95.5% | −1.4% | 30.0 | 2 |
| eth-5m W=10 P=0.98 | 748 | 98.9% | −0.25% | 4.8 | 1 |

Even the least-bad cells: long streaks of +2¢ wins punctuated by −95¢ losses, drawdowns of 17–35 share-units per 1-share staking, and negative drift.

---

## 4. Maker execution: adverse selection is fatal

Posting at the favorite's best bid (no fee, May 5m, conservative fill = ask traded strictly through the level):

| Window | fill rate | win rate **when filled** | win rate when not filled | avg P&L per filled share |
|---|---|---|---|---|
| 5 s | 3–4% | 84–87% | 99.8% | −10% to −13% |
| 30 s | 12–16% | 90–92% | 99.97% | −6% to −7% |
| 120 s | 36% | 93–94% | 100% | −2% to −4% |

A resting bid at 0.95+ fills **only when the favorite is collapsing**. The orders you want filled never fill; the fills you get are the losers. Optimistic fill assumptions (level touched) shrink but never flip these numbers. Queue position only worsens it — the conservative model already assumes you are last in queue.

---

## 5. Failure-case analysis (the most important section)

847 markets had a favorite at mid ≥ 0.90 inside the last 120 s that **lost** (712 in 5m, 135 in 15m — ~5% of qualifying 5m markets).

**Anatomy of the 712 5m failures:**
- **69% were coin-flips in disguise**: underlying lead at t−60 s was < 5 bps. At a 5 bps lead, BTC 1-minute volatility regularly closes the gap; the book was pricing momentum, not a safe lead.
- **61% were photo-finishes**: final window move within ±2 bps of the open. These are inherently unpredictable from price alone.
- **20% were genuine late reversals**: > 10 bps adverse underlying move inside the final 60 s.
- Doomed favorites still looked strong late: 45% still had mid ≥ 0.90 at t−10 s; median mid at t−1 s among failures was **0.985** — the book often dies confidently wrong.
- Median flip (mid crossing 0.5) happened 102 s before settle, but **10% flipped in the last 10 seconds**, and **32 markets (4.5%) never flipped at all** — the book favored the loser at the final tick and the Chainlink print disagreed (oracle-vs-spot divergence: Chainlink BTC/USD vs the order flow traders watch).
- Order-book structure at entry was unremarkable in failures — normal spreads (1 tick) and depth. **There is no book-shape tell**; the risk lives in the underlying's distance-to-strike, which the price already encodes.

---

## 6. Execution feasibility

- **Liquidity is not the constraint.** REST depth for 5m favorites in the final 30 s: median 200–290 shares at best ask (BTC), 38–46 (ETH), 500–670 within 2¢ (BTC); spreads stay at 1 tick (0.01) with no book collapse — books stay two-sided into the final 5 s (`no_ask` ≈ 0%).
- A $100–$500 taker clip is realistically fillable at the quoted ask throughout the final 2 minutes. Slippage beyond the half-spread is minor for small size — and irrelevant, because the economics are negative *at* the best ask.
- Maker fills are realistic only via adverse selection (§4).
- Live latency budget (~0.5–1 s feed delay + order RTT) only degrades the already-negative taker EV, since late quotes in collapsing markets are stale-optimistic.

## 7. Statistical confidence

- May 5m sample: 1,300–6,500 trades per cell → standard errors of 0.1–0.5%/share; the −0.6% to −1.7% expectancies are 2–10 SE below zero. **The negative result is high-confidence.**
- The few positive cells fail exact-binomial scrutiny (zero-loss small-n) and fail OOS replication.
- Label noise bounded: worst-case May relabeling moves cells by −0.2 to −0.6% (more negative); June results use exchange-authoritative winners.
- Caveats: 31 days, one market regime (BTC ~$61k, June realized vol moderate); BTC/ETH same-window trades are correlated (effective n lower than nominal); June 7 + most of June 11 missing.

## 8. Recommendation

**Do not deploy.** The raw concept — buy the apparent winner late — has no edge: the market's late-stage prices are well-calibrated, and both execution paths lose:

| Path | Expected value |
|---|---|
| Taker at ask, any (W, P) cell, 5m | −0.4% to −1.8% per share (fee ~0.1–0.3¢, half-spread ~0.5–0.6¢) |
| Taker, 15m | no cell survives IS selection + OOS replication |
| Maker at bid | −2% to −13% per *filled* share (adverse selection) |

The strategy's true description is **selling deep out-of-the-money optionality on 1-minute BTC variance at a price below fair value, plus paying fees** — a structurally negative trade.

## 9. Future work (not implemented, per scope)

The only candidate signals that showed promise and could, in principle, overcome the ~1% cost hurdle:

1. **Underlying distance-to-strike filter** (strongest): 69% of failures had < 5 bps leads. Condition entry on |Binance move since window open| > X bps — this removes most losses, but also most trades; whether the survivors clear costs needs testing.
2. **Book-imbalance filter**: favorites with above-median total-book bid/ask imbalance won 3–9pp more often than below-median, consistently across series and offsets — the one order-book feature with signal.
3. **BTC velocity/momentum in final seconds** — interacts with (1); failures show adverse drift already in progress at entry.
4. **Oracle-divergence guard**: track Chainlink stream directly (32 failures lost on the oracle print against the book).
5. **Maker-at-deep-discount** (e.g., bid 0.90 on a 0.97 favorite): converts adverse selection into a deliberate "catch the panic" trade — entirely different strategy, untested here.
6. Fee-tier optimization (taker rebate program) shaves ≤ 0.2¢ — insufficient alone.

Items 1+2 combined are worth one focused follow-up study; everything else is secondary.

---

### Artifacts

All intermediate data in `data/` (checkpoints, favorites, sweeps, calibration, failures, depth, staleness), reproducible via `scripts/run_chain.py` + `scripts/extract_failures.py` + `scripts/conservative_selection.py` + `scripts/report_stats.py`.
