# Order #16 — Three-variant paper A/B (one process, N virtual portfolios)

Runs alongside Order #15 Part B. Answers: does the tournament's burst/union finding
survive contact with a live feed, and by how much?

Author: auditor session, 2026-07-27. Evidence: docs/PREREG_tournament.md and the
51-day tournament (5m: sealed holdout +$56.41/day vs deployed +$14.11, permutation
p=0.0000; 15m: +$16.75 vs +$2.20, p=0.0000; same config won 5/5 folds on 5m).

---

## Design: virtual portfolios, not parallel bots

Do **not** run three bot processes. Run **one** paper process with three independent
virtual portfolios sharing one feed, one book state and one decision loop. Each
portfolio has its own entry gate, its own positions, its own P&L ledger and its own
recal instance.

This matters for the comparison, not just for resources: variants then see the
*identical* tick, the *identical* book snapshot and the *identical* decision latency,
so any P&L difference is attributable to the gate alone. Three separate processes
would each have their own WS jitter and the difference would be partly noise.

`v2_intent_open` and every P&L row gain a `variant` field. Everything else about the
logging stays as-is.

## The three variants

| | gate | rationale |
|---|---|---|
| **V0 CONTROL** | exactly today's deployed config, untouched | the incumbent; also the sanity check that the harness reproduces known behaviour |
| **V1 UNION-2** | V0 **OR** first second with `max(\|1s\|,\|3s\| side-signed Binance return) >= 2bps`, no z/edge/disp/vol/book-unmoved/frozen gate, no ask cap | the tournament winner, unmodified — testing it as selected, not a softened version |
| **V2 UNION-3-CAPPED** | V0 **OR** burst `>= 3bps` **and** `ask <= 0.75` | risk-tempered: trims the loosest burst tier and the near-certainty tail. Degrades more gracefully if live fills disappoint |

One entry per market per variant (variants may disagree about which second, and that is
the point). Exits identical across all three: hold-to-settle with the deployed band-stop.
Stakes flat $1.05 everywhere — **no sizing tiers in this experiment**, they are a
separate axis and would confound the comparison.

## THE critical addition: model FOK kills

The paper bot currently fills at the quoted ask. That is exactly the assumption the
burst arm needs to be *wrong* about, so a naive paper A/B would simply replay my
backtest and teach us nothing new.

At fill time (decision + the real observed latency), compare the current ask to the
quoted ask:

- if `ask_now <= quote + max_slippage` → fill at `ask_now`
- if `ask_now > quote + max_slippage` → **record a KILL, no position**

Use the deployed `max_slippage = 0.04`. Log per intent: `quote_ask`, `fill_ask`,
`slip`, `killed` (bool), `latency_ms`. This is the measurement the whole experiment
exists for — my data says the burst population's ask decays **+3.33c/s and moves
against us 60% of the time**, versus +1.35c/s and 31% for the deployed population. If
that translates into a materially higher kill rate, V1's backtested advantage shrinks
or inverts, and we will see it in week one for free.

## Metrics per variant (daily + rolling)

Entries/day, kill rate, realised WR, EV/$1, net $/day, PF, **Sortino on daily P&L**,
max drawdown, mean ask, photo-finish share, mean slip, quote-moved-away rate, and the
stop's saved/whipsawed split. Plus the paired view: on markets where two variants both
entered, which second each took and the P&L difference.

## Pre-registered decision rule (fixed before the run)

After **7 full days** with the feed healthy (Order #14 must be deployed first — a
repeat of the 45-hour blind window voids this experiment exactly as it voided the
weekend exam):

- **V1 or V2 wins** if its net $/day beats V0 by >= +50% AND its kill rate is under
  25% AND it is positive on >= 5 of 7 days. → promote to the live candidate, re-based
  (a new entry path changes the traded population and needs the Order-#6 treatment
  plus a recal reset).
- **Inconclusive** if the winner is ahead but misses any leg → extend to 14 days, once.
- **FAIL** if V1 and V2 do not beat V0, or kill rates exceed 25%. → the tournament
  result was a recorder artifact; record it as such and stop.

No mid-run gate tuning. If a variant is obviously broken (zero entries, invariant
alerts) fix the bug and restart the clock rather than adjusting the gate.

## What NOT to touch

The audition (`recal.json` / `recal_15m.json` — 15m is mid-verdict at n=140), the
floors, knots, `edge_min`, `z_min`, `vol_lookback_s`, the stop's behaviour, and the
taker bot's live-arming path. V0 must be byte-identical to what runs today.

## Sequencing

1. Order #14 (feed watchdog) — already in progress, blocks everything.
2. Order #15 Part B (recorder, operator PC) — also unblocks the maker fill model, below.
3. This order (variant A/B) — one week.
4. Order #15 Part A (maker paper bot) — after Part B confirms the trade-print source.

---

## Appendix — answers to the dev's two blockers

**Q2, trade prints — SUPERSEDED BY LIVE MEASUREMENT. Read this version.**

My original claim ("no trade-print channel exists") was **wrong**, and the reasoning
was bad: I inferred absence from the operator's recorder DIRECTORY LIST, which only
shows what that recorder subscribed to, not what the API emits. `last_trade_price`
does exist on the live feed and carries `price`, `size`, `side`, `timestamp` and
`transaction_hash` — confirmed by the dev (941 events / 90s) and reproduced
independently (478 / 180s).

What survives, and is load-bearing: `price_change` **is** a level-update feed, not
prints. Over 297,884 consecutive updates at the same (token, price, side) the size
**increased 51.4%** of the time, decreased 48.3%, and was exactly `0` in 0.9%. A traded
quantity can never make a level grow and "traded zero" is meaningless, so `size` is the
new resting depth. Draining queue off it would manufacture fills from quote churn.

**But `last_trade_price` is NOT a complete print feed either.** Matched by
`transaction_hash` against `data-api /trades` over the same window, after waiting 300s
for the indexer: REST 898 unique hashes / 18,912 size vs WS 273 / 5,258 —
**WS covers 30.2% of prints and ~28% of volume**, with 627 REST hashes the WS never
sent. It has last-price semantics: consecutive fills at one price collapse.

METHOD WARNING: an immediate REST pull (no indexer wait) showed 16 rows and an apparent
93.8% WS coverage — the exact opposite conclusion. The data-api indexer lags minutes.

**Therefore REST `/trades` remains the fill-scoring path for Part A**, as originally
specified. Draining queue off WS prints alone would consume ~30% of real volume, so the
simulated queue would advance too slowly and the bot would UNDERSTATE fill rate — the
core metric of the whole business case. WS `last_trade_price` stays useful as a
low-latency hint, and its `transaction_hash` gives A8 counterparty logging for free,
but it is not the fill driver. Prints arrive by poll; a **paper** fill need not be
determined in real time, so spec it as a reconciliation loop, not a hot path.

**Q3, the A3.2 fill direction: the dev is right and the order was wrong.** A resting
ask at 0.60 is lifted by a buy printing at 0.60 *or above*; a 0.55 print cannot touch
it. `>=` is correct. Keep the implementation and the `prints_below_our_level_never_fill_us`
test; the order text is the error, not the code.

**Q1, VPS resources:** cannot be measured from here, and the recommendation stands
independently — run Part B first. It is on the operator's PC, it carries no VPS risk,
it is what Part A's queue model needs for offline validation, and its first hour
answers both the byte-rate question and the print-source question above. Part A then
ships on verified inputs rather than on anyone's recollection.
