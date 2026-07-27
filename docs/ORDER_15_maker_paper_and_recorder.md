# Order #15 — Maker-book paper bot (VPS) + up/down universe recorder (operator PC)

Ships **after Order #14**. Two independent parts; A is the build, B is nearly free and
feeds A. Neither may touch the taker bot's trading path.

Author: auditor session, 2026-07-27.

---

## Why now

The taker cell is at its calibrated frontier — both auditions pass (5m bias −0.028 at
n=300, 15m −0.006 at n=140), it earns ~+$13/day at $1.05 flat, and this session tested
the entry side four more ways (vol-estimator swap, photo-finish gate, photo-finish
sizing, foundation-model forecasting) with every one coming back priced. More signal
mining is not where money is.

The maker book is the largest unexploited finding in the ledger and is *structurally*
the other side of every inefficiency the taker studies keep finding: **+0.455¢/share
OOS** on 6 held-out days (80% of the +0.567 in-sample), **+0.739¢/share rebate-
inclusive**. It has never been run. This order runs it in paper, instrumented so the
numbers extrapolate to live.

---

# PART A — Maker paper bot (runs on the VPS)

## A0. Non-negotiable design principle: log inputs, not conclusions

**A maker paper fill is a model, not an observation.** Every number this produces
depends on a queue-position assumption we cannot verify offline unless the raw inputs
are preserved. The previous auditor's study caches were lost and the exact P&L
convention became unrecoverable — that must not happen again.

So: every virtual fill must be reconstructible from the log alone, without re-running
the bot. Log the book snapshot, the queue estimate and its inputs, the prints that
consumed it, and the timestamps — then the fill model can be re-scored offline under
different assumptions without collecting new data. If you have to choose between
logging more and computing more, log more.

## A1. Frozen config — do NOT re-optimise

These came from an IS/OOS-validated study. Selecting new parameters on this paper run
would be selecting on the validation set.

| Parameter | Value | Source |
|---|---|---|
| Asset | **BTC only** | ETH asks were negative on *every* OOS day (−0.615¢/sh) |
| Side | **Asks only** | asks +0.57¢/sh vs bids +0.06; 87% of taker prints are BUYS |
| Placement | **Join BBO, back of queue** | pessimistic; inside-quoting earns *less* (+0.25 vs +0.35) and is possible only 13.4% of the time |
| Price band | **0.10 – 0.90** | 0.70–0.90 was toxic in-sample; band chosen IS, held OOS |
| Nominal size S | **50 shares** | the validated unit; log per-share economics so it rescales |
| Lag defense | **NAIVE — none** | defense *inverted* the result (+0.35 → +0.25): cancel-rejoin resets queue position and refreshes capacity straight into continuation toxicity |
| Hours | **ALL hours, no gate** | differs from the live probe spec deliberately — see A2 |
| Interval | 5m and 15m both, tagged | 15m is untested for MM; tag and slice offline |

**Hours:** the live probe spec restricts to 14–21 UTC (edge concentrates at +0.8–1.5¢/sh;
hours 0, 2, 7, 10–11, 22–23 are flat/negative). In **paper there is no risk and data is
the product**, so run all 24h and slice offline. Do not gate hours in the bot.

## A2. Inventory cap — enforce it even in paper

The measured tail is the reason this is capped: worst single 5-minute window
**−$1,956** at S=50 full coverage, p5 −$924, ~50% of windows negative. A trending
window accumulates ~2.7k shares across ~54 requote-clips because capacity refreshes on
every requote.

Enforce `max_net_inventory_shares` (default **150**, matching the probe spec). When
inventory would breach it, **stop posting new asks** — do not hedge, do not cross the
spread to flatten. Log every bind. An uncapped paper run measures a strategy we would
never deploy, which makes its Sortino meaningless.

## A3. The fill model

State per resting virtual order: `(token, price_level, size_remaining,
queue_ahead_shares, posted_ts_ms, book_seq_at_post)`.

1. **On post:** read the current book at that level. `queue_ahead = displayed size at
   our level` (we join the back). Log the full level and the book snapshot age.
2. **On each trade print** at a price ≤ our ask level (a buy sweeping up through us),
   consume in order: first `queue_ahead` (decrement), then our `size_remaining`
   (that portion is OUR fill).
3. **On book size decrease at our level not explained by prints:** these are cancels.
   We cannot observe whether they sat ahead of or behind us. **Pessimistic assumption:
   assume they were BEHIND us — do not improve `queue_ahead`.** Log the amount so the
   optimistic variant can be scored offline.
4. **On BBO move away:** the order is no longer at BBO. Naive config = leave it resting
   (no cancel), and log that it is now stale-priced.
5. **Partial fills are normal** — track `size_remaining`, never all-or-nothing.

**Latency realism (important, and cheap):** median BBO lifetime is **25 ms**, and the
spread is pinned at 1 tick 94.3% of the time. So "join the BBO" is itself a latency-
sensitive act. Log, per post: the book snapshot's age at decision, the decision→post
delay, and whether the level still existed at post time. This is what tells us how
often a *real* order would simply have missed. Do not try to correct for it in the
bot — measure it.

## A4. Position and P&L convention

A filled ask means **we sold a YES share at price p — we are short that outcome**. If
it resolves YES we pay $1; otherwise we keep the premium. Note 95.2% of fills on this
venue are complement-mint matches.

- **Primary P&L = mark to settlement.** These are binaries that settle; the taker
  book's hold-to-settle convention is the proven one, and redemption/settlement is
  fee-free.
- Also log marks at **t+1s, t+5s, t+30s, t+60s** after each fill. This is the adverse-
  selection curve and it is the single most important diagnostic for whether we are
  being picked off. Compute it from the book mid, not our own quote.
- **Maker rebate:** 20% of taker fees paid on filled maker volume, ≈**0.35¢/share at
  p=0.5**, fee-curve weighted. Model it explicitly as a separate logged field —
  never fold it into gross. It is a third of a 1-tick capture, a kicker not a business,
  and it is only verifiable live.
- Liquidity Rewards (resting-depth program) pay **$0** for these markets — every
  sampled 5m/15m BTC/ETH market has `rewards.rates = NULL` and zero crypto 5m/15m
  markets appear in the platform rewards list. Do not model them. Re-check
  `rewards.rates` occasionally (one API call).

## A5. Metrics — the operator asked for everything, so:

Emit a `maker_metrics` rollup per hour and per UTC day, plus a full per-fill and
per-post record. Analysis must be possible from the logs alone.

**Per post:** ts, token, asset, interval, level, our size, queue_ahead, book age at
decision, decision→post ms, mid, spread, book depth both sides, ttl to settlement,
inventory at post, whether inventory cap was binding.

**Per fill:** all of the above plus fill ts, filled size, time-to-fill, fraction of our
order filled, queue consumed by prints vs by cancels, gross ¢/share, modeled rebate,
net ¢/share, marks at +1/+5/+30/+60s, settlement outcome and final P&L.

**Per hour / per day:** posts, fills, **fill rate (fills/posts and shares filled/shares
posted)**, mean and full distribution of ¢/share (p5/p25/p50/p75/p95), gross P&L, rebate,
net P&L, **Sortino** (downside deviation, per-fill and per-5-min-window), Sharpe,
max drawdown, worst 5-min window, p5 window, inventory distribution and time at cap,
adverse-selection curve, cancels-ahead volume, count of posts whose level had vanished
by post time.

Judge against the model: **+0.455¢/share OOS, +0.739¢/share rebate-inclusive.**
Realised materially below that means the queue model was optimistic — which is the
result we most need to know before risking money.

## A6. Isolation

Run as a **separate process/binary** from the taker bot, with its own config, its own
log directory, and its own PM WS subscription. It must be impossible for a maker-side
panic, memory leak, or WS storm to disturb the taker bot or the running audition.
Share no mutable state. If VPS resources are tight, say so before shipping — the taker
audition has priority.

## A7. What this run CANNOT establish — state these limits in the write-up

- Whether **our presence perturbs the flow** we are trying to harvest (unknowable in paper).
- **True queue priority** and at-price competition — we estimate, the exchange knows.
- **Cancels ahead vs behind** — structurally unobservable from the public feed.
- **Order rejections / min-size rules** — never exercised by a virtual order.
- **Rebate accrual** — modeled, verifiable only from live receipts.

## A8. Counterparty logging (cheap, do it now)

The fill-attribution decoder already exists (contract
`0xe111180000d2663c0091e4f400237545b87b996b`) and has never been deployed. Wire it to
log counterparty wallet per fill. Identity is not knowable pre-trade (anonymous book)
but is knowable seconds post-fill via the tx receipt, and it feeds the future
quote/cancel logic. ~438 maker wallets, top-10 = 35% of volume.

---

# PART B — Up/down universe recorder (operator PC, not the VPS)

## B1. Discovery-driven, NOT hard-coded to hourly

I could not confirm hourly up/down markets exist. The Gamma listing returned
inconsistent data (stale epochs ~Dec 2025 on "active" markets, broken pagination past
offset 2500). What I *did* confirm is more interesting:

**Active up/down markets now exist for `btc`, `eth`, `sol` and `xrp` at 5m** — e.g.
`sol-updown-5m-<epoch>`, `xrp-updown-5m-<epoch>`. **SOL and XRP appear in no dataset we
have**; the June inventory was btc+eth only.

So the recorder must **enumerate whatever exists** in the up/down family — every asset,
every interval — and record it all. If hourly markets exist it captures them; if they
never launch, it still captures the SOL/XRP breadth and full-depth books, both of which
are new. Log the discovered universe daily so we can see it change.

## B2. Record the BOOK channel — the hard-won lesson

**Full-depth book reconstruction from `price_change` deltas alone is INVALID.** Trade
executions arrive as full-book refreshes on the `book` channel; delta-replay
accumulates phantom levels (validated: match rate decays ~90% → 0% over a token's life).
The June archive lost the `book` channel and that permanently crippled it.

Record, per market: **`book` (full snapshots — mandatory)**, `markets` (self-contained:
asset/interval/epoch/up_token_id/down_token_id/slug — no API needed), `best_bid_ask`,
`price_change`, and `last_trade_price` if available.

Plus **synced Binance 1s klines** for every discovered asset (btcusdt, ethusdt, solusdt,
xrpusdt) — 1s is the granularity that reproduces the bot's `vol60` bit-for-bit and the
only one fine enough for a 30–240s horizon.

## B3. Apply the Order #14 lesson from day one

We just lost 45 hours to a feed that died while reporting healthy. The recorder must
not be able to do that silently:

- Per-channel **message counters + last-message timestamp**, flushed to a heartbeat file
  every 30 s.
- **Staleness watchdog**: if any subscribed channel goes quiet beyond a threshold,
  reconnect and write a `gap` record (channel, start, end, duration).
- A **gap log** is a first-class output. An analysis that silently sits on missing hours
  is worse than no analysis — that is what voided the weekend exam.
- Survive PC sleep/restart: resume cleanly, and always record the gap.

## B4. Storage

`price_change` is the firehose — the June recorder produced ~58 GB/day uncompressed for
btc+eth at 5m+15m alone, and we are adding two assets. Requirements: **zstd**
compression, daily rotation, per-day directories mirroring the proven layout
(`polymarket/{book,best_bid_ask,price_change,markets}.jsonl.zst` +
`binance/<sym>_kline_1s`).

Set a **disk cap with oldest-day eviction and a warning at 80%**. Measure the actual
byte rate in the first hour and report it before committing to a retention policy — do
not guess. `book` and `markets` are small and must never be evicted; `price_change` is
the evictable one.

## B5. Why this is not just option value

The full-depth book data this captures is exactly what Part A's queue model needs to be
validated and improved offline, and it is the substrate for the still-unmined L2
book-shape work. Part B pays for Part A.

---

## Tests

- Fill model: unit tests for queue consumption (prints only), cancel handling
  (pessimistic — queue_ahead unchanged), partial fills, and inventory-cap binding.
- A replay test: feed a recorded book+print sequence and assert the fill sequence is
  deterministic and reproducible **from the logs alone**.
- Inventory cap: never exceeded across a synthetic trending window.
- Recorder: simulated disconnect produces exactly one `gap` record with correct bounds.
- Isolation: maker process crash leaves the taker bot running and unaffected.
- Existing suite stays green (534 tests as of `6858deb`).

## Deploy

Part A: separate unit, paper only, no live arming path enabled in this order. Part B:
operator PC, run for at least one full week including a weekend before first analysis.

**Do not touch:** the taker bot's trading path, `recal.json` / `recal_15m.json` (the
audition is mid-flight — 15m is at n=140 and needs one more clean stretch), floors,
knots, `edge_min`, `z_min`, `vol_lookback_s`, or the invalidation stop's behaviour.

## Success criteria (registered before the run)

- **Part A:** ≥1 week and ≥2,000 virtual fills. Judge realised net ¢/share against the
  **+0.455 OOS / +0.739 rebate-inclusive** model. Report fill rate, the adverse-selection
  curve, Sortino, and the worst 5-min window. A result materially below model means the
  queue assumption was optimistic — that is a *successful* paper run, because it is the
  cheapest possible way to learn it.
- **Part B:** ≥7 days recorded with a complete gap log, the discovered universe
  enumerated daily, and `book` coverage sufficient for depth reconstruction.

Scaling to more assets/campaigns comes only after Part A's numbers are in.
