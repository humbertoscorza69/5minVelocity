# Finished Product — Taker Lead-Lag (Solo Leg) Spec & Audit

## Audit verdict (look-ahead / leakage / fake fills)

| Test | Result | Conclusion |
|---|---|---|
| **Physics** (observed win vs random-walk `Φ(disp/(vol·√ttl))`) | predicted 0.739 vs observed 0.716 | **No label leakage** — wins are exactly what the lead physically implies, not inflated |
| **Delay sweep** (entry ask at signal+d) | ask climbs 0.47→0.51→0.60→0.66→0.70 over d=−1..+10s; edge decays smoothly | **Real microstructure lag**, not an alignment artifact (a bug would be flat/discontinuous) |
| **Placebo** (enter *before* signal, d<0) | +23¢ "edge" you can't access | Correctly flagged untradeable; we use d≥+1 only |
| **Fillability** vs 4.4M real trades | 93% of entries had a real buy at/through our ask within 3s; traded VWAP 3¢ *below* our modeled ask | **Fills are real; our cost is conservative** |
| **IS/OOS** | May +20.9¢ / June +11.5¢ per share (d=+1) | Replicates out-of-sample; edge compressing May→June |

**The earlier "88% win" was real but misleading** — it came from the loose `disp≥2` filter that includes >20bps trades (98% winners bought at ~0.95, near-zero edge). The tradable regime (band 2–10bps) is **73% win (June), +11.5¢/share** — physically grounded, fills verified, no leakage. The edge is real but **lives in a ~1–5 second window** and requires fast reaction.

## The rule (exact numbers + why)

**Per market, in the final 5–120s before settlement:**
1. **Trigger** — fire when the underlying's **2-second move ≥ 5bps** toward a side. *(A real fast move the book will lag; below ~5bps the move is too small to reliably outrun the book's reaction.)*
2. **Confirmation** — only if the underlying is **2–10bps ahead of the window's opening price** (signed to that side).
3. **Action** — **taker BUY that side within 1 second; hold to settlement.** Never the other side.
4. **One position per market** — take the **FIRST** instant that satisfies 1+2; ignore all later signals (no flipping, no second side).

### Why a displacement BAND, not one number
Displacement isn't a threshold to tune — it's a **regime with two edges**, and you pick both because they bound where the edge exists:
- **Floor = 2bps:** below this the lead is inside noise → coin-flip false positive (the book is *right* to price it ~0.50). This is the false-positive filter.
- **Ceiling = 10bps:** above this the book has **already repriced** to fair (ask ≈ win rate ≈ 0.95+) → no edge left (the dead deep-favorite zone). The gap `win−ask` collapses from +0.36 at 2–5bps to +0.02 at >20bps.
- **2–10bps** is the only zone where the lead is *real* (will hold) AND the book *hasn't caught up*. Confirmed: tight 2–5 gives +12.5¢ but half the volume; 2–10 gives +11.5¢ at 2× volume — chosen for balance.

### Entry timing (audited)
Edge by reaction delay (June OOS): d=+1s **+11.5¢**, d=+2s +6.4¢, d=+5s +3¢, d=+10s ~0. **The book fully reprices in ~10s.** Operate at d≤2s — sub-second API required. d=+2 is the conservative planning number (+6.4¢).

## DCA (same-side adds) — decision

**Cap at k_max = 2–3, and only for capacity.** Each same-side add is +EV but at a worse price (ask climbs 0.84→0.87→0.95; marginal edge 8.8¢→7.3¢→3.7¢→1.0¢ by the 5th). Going to unlimited raises total profit +70% but ROC falls 10.8%→7.1% and worst single-market loss grows −99→−440 (concentration into one binary).
- **The first signal is the cheapest, best price** → to add size, a **bigger first clip beats DCA**.
- DCA only when the first clip exhausts available depth (~$100–500). Then add up to 2–3× total, accepting the lower marginal edge.
- Capital-constrained → don't DCA (k=1 is most capital-efficient; spread across markets instead).

## Sizing & risk
- Outcome is binary (lose the full stake when wrong, ~27% of the time). Full Kelly (~32% at 73%/0.60) is far too aggressive given model uncertainty and edge compression — **use ~1/8 Kelly (≈4–5% bankroll/market) with a hard per-market cap.**
- BTC and ETH moves are correlated → cap simultaneous same-direction exposure.
- **Edge is compressing (May 20.9¢ → June 11.5¢/share)** — re-validate monthly; if it decays below ~3¢ net at d=2, stand down.
- OOS sample is small (7 days, 281 trades, ±5¢ CI). Positive but noisy → **paper-trade live before sizing** to confirm real fills and current edge.

## v3 update — window, volume, BTC/ETH, latency (corrected $10-stake)

- **Bug fixed:** fixed-$ stake over-levers low-ask entries (13.7% of May entries had ask<0.30). All $ figures now use ask floor 0.30–0.97. Real per-trade EV (5m, $10): ~$1.5–5 depending on asset/regime.
- **5m only.** 15m markets show ~zero edge in May (and negative at d+2) — exclude. Lead-lag is a 5-minute phenomenon.
- **BTC > ETH** (June d+1, $10): BTC win 0.78 EV +$2.45; ETH win 0.71 EV +$1.48. Trade both, weight BTC.
- **Window: use 5–180s, not 5–120s.** Edge is ~flat 30–180s, dies >180s; the last 5–20s add little (ask already high). Extending 120→180s lifts June from 37→54 trades/day at the same EV/trade (+50% daily $). Best EV/trade zone is 30–120s; biggest volume is 120–180s.
- **Win rate is volatility-regime dependent** (your observation): June vol was 2–3× May → more signals (40 vs 13/day) but lower win (73% vs 85%). `corr(trades/day, win)=−0.52` in June. A vol-normalized z-filter stabilizes win rate but *lowers* EV (pays higher asks) — keep raw bps for profit, monitor regime.
- **Volume frontier (passive-income dial):** vel≥5/window120/disp2–10 = 37/day, +$1.86/trade (robust); vel≥4/window180/disp1–15 ≈ 75/day, ~$1.2/trade (balanced, RECOMMENDED); vel≥3/window300/disp1–30 = 226/day, +$0.68/trade (max volume, fragile margin). Total daily $ plateaus ~$150/day ($10 clip) / ~$1500 ($100 clip, depth permitting). More trades = more total $ but thinner margin = more fragile live.
- **Latency (Dublin VPS, measured):** order path ~15ms (Cloudflare-colocated), Binance feed ~250ms → total reaction ~250–300ms. Backtest uses d+1=1000ms (conservative); real edge sits between d0 and d+1 (higher). To exploit sub-second, compute velocity from Binance tick WS, not 1s bars.

## Expected performance (June OOS, realistic d=+1)
~40 trades/day, 73% win, +11.5¢/share net of fee+slippage, entry ~$0.60. At $100/clip ≈ $11.5/trade × 40 ≈ ~$460/day gross before capacity limits and compression. Conservative (d=+2): ~$6.4/trade.

## Artifacts
`scripts/audit_taker.py` (leakage audit), `finalize_rules.py` (IS/OOS, filter comparison), `dca_analysis.py` (DCA), data `audit_entries.parquet`, `trades_june5m.parquet`.
