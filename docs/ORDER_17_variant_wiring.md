# Order #17 — Complete the #16 variant A/B and arm it on the VPS

> ## SCOPE CUT (2026-07-27) — MINIMUM TO ARM
>
> Operator wants the clock started. Re-scoped: **the bot LOGS, the auditor SCORES.**
> Anything that is arithmetic over logged fields does not belong in the decision loop —
> it is offline analysis, and putting it in the bot only adds hot-path work during a
> mid-flight audition. Same principle as the maker bot's A0.
>
> **BUILD (blocking the clock):**
> - **B** — candidate → `burst_bps_at` → `admits()` → `ShadowBook::open` / `record_kill`,
>   with each variant applying its **own** dedup/re-entry against its own book.
> - **C** — FOK kill at fill time (epsilon fix already in), **plus V0's counterfactual
>   `killed` computed and logged without acting on it.**
> - **D** — `variant` tag on every intent, P&L row and stop/close event. V0 tagged
>   `"v0"`, never blank.
> - **E1** — per-tick decision latency (p50/p95/p99) tagged with the enabled flag.
>   Keep: it is ~20 lines and it is the only thing that tells us whether the 25% kill
>   leg is partly measuring our own overhead.
> - The six severances from Part A, each tested.
> - Part F isolation proof stays green (already landed, `ee879c3`).
>
> **DROPPED from the bot — I compute these from the export instead:**
> - **E2** `kill_rate{v1} − kill_rate{v0}` — pure arithmetic over the logged `killed`
>   field.
> - **The daily rollup**, including `net_v0_actual` / `net_v0_killadj`. The bot does not
>   need to know the verdict; it needs to log `killed` and P&L per row. The dual-baseline
>   rule already landed as code in `065e6e3` and can stay there unused by the loop.
> - `suppressed_by_v0_hold{variant}` — **keep only if it is genuinely ~10 lines.** If it
>   costs more than that, drop it; I will bound the bias offline from the decision table
>   instead. It is a caveat-sizing diagnostic, not a gate.
>
> **Required log fields per intent (this is the contract that makes offline scoring
> possible — get these right and nothing else in the bot matters for scoring):**
> `variant`, `quote_ask`, `fill_ask`, `slip`, `killed`, `latency_ms`, plus the existing
> feature set. On every P&L row: `variant`.
>
> Everything below this box stands as written; the boxed list is what gates gate 3.

Finishes what `ee879c3` started. The candidate sink, variant gates, FOK kill model and
decision rule are built and tested; this order is the plumbing that feeds them, the two
instrumentation points that protect the headline metric, and the arming procedure.

Author: auditor session, 2026-07-27. Governing pre-registration: `docs/ORDER_16_variant_ab.md`
and `docs/DECISIONS_maker_and_variants.md` (both committed before the run they govern).

---

## Resolved first: the emission-point scope boundary — ACCEPT, and measure it

The dev flagged that emission sits *after* the one-entry-per-market check, so no
candidate is produced for a market V0 already holds, and asked whether that is
acceptable. **It is.** The reasoning, so it is not re-litigated:

V1's entire mechanism is that its gate is *looser*, so it fires **earlier** than the
full stack — typically well before V0 enters. The suppressed window only begins at
V0's entry. So the blocked region is candidates *after* V0 entered, which is close to
disjoint from where V1's edge actually lives. And when V0 does admit, the candidate is
emitted with `v0_admitted = true`, so V1 ⊇ V0 still holds by construction.

The genuinely uncovered case is a variant wanting a re-entry in a window where V0 holds
(or where V0 stopped out and re-entered). That **under-counts variant entries**, which
biases *against* V1/V2. A conservative bias on the challenger is the safe direction —
it cannot manufacture a false positive.

**But do not assume it is small — measure it.** Add a counter: every tick, if a variant
arm's admission condition is met on a market where emission was suppressed by V0's
hold, increment `suppressed_by_v0_hold{variant}` and log it daily. If that count is a
material fraction of variant entries, we know the comparison understates V1/V2 and by
roughly how much. Cheap, and it converts a documented caveat into a measured one.

## Part A — shadow instantiation

Config block, default OFF:

```toml
[v2.variants]
enabled = false          # master switch; false = today's behaviour exactly
v1_burst_bps = 2.0       # V1: V0 OR burst >= this, no other gate, no ask cap
v2_burst_bps = 3.0       # V2: V0 OR (burst >= this AND ask <= v2_max_ask)
v2_max_ask   = 0.75
state_dir    = "data/v2/shadow"   # per-variant state, NOT state.json
```

One `ShadowBook` per variant: own positions, own P&L ledger, own recal instance, own
state file (`shadow_v1.json`, `shadow_v2.json`). Sharing nothing mutable with
`bs.positions` or the recal set.

**The six severances from the isolation checklist, each individually testable:**

1. Shadow opens consume **no** guard budget — not `max_open_positions`, `stake_cap`,
   total exposure, per-token cap, or `daily_loss_cap`.
2. Shadow settles **never** feed the canary.
3. Shadow settlement uses its own map, **not** `state.v2_settled`.
4. The accounting invariant is **variant-scoped** (a noisy invariant is a dead one, and
   it is the only thing that catches booking bugs).
5. Shadow state in its own files, never inside `state.json`.
6. Shadow recal **never** writes `recal.json` / `recal_15m.json`.

## Part B — candidate → arm → shadow

Per emitted candidate, per variant:

1. Compute burst from the **same definition the deployed loop uses** (`max(|1s|,|3s|`
   side-signed Binance return) — reuse the function, do not re-derive it.
2. `admits(variant, candidate, burst)` — already built and tested.
3. Apply the variant's **own** one-entry-per-market and re-entry rules against its own
   `ShadowBook`, not against `bs.positions`.
4. On admission → FOK kill check (Part C) → `ShadowBook::open` or `record_kill`.

## Part C — FOK kill at fill time

At fill time compare the current ask to the quoted ask:

- `fill_ask <= quote_ask + max_slippage + EPS` → fill at `fill_ask`
- otherwise → `record_kill`, **no position**

`max_slippage = 0.04`, with the epsilon fix already in (`0.64 − 0.60` evaluates to
`0.04000000000000001`; a bare `>` silently inflates the kill rate, which is both the
headline measurement and a hard-FAIL leg).

**AMENDED — the original wording conflicted with DO-NOT-TOUCH and the dev was right to
stop.** "V0 subject to the identical kill model" and "V0 byte-identical to what runs
today" cannot both hold literally: applying kills to V0 would change its traded
population, which feeds the recal, which is the 15m audition sitting mid-verdict at
n=140. The audition is the harder constraint.

**Resolution: V0's kill is a LOGGED COUNTERFACTUAL. V0 still takes the position.**
Compute `killed`/`slip` for V0 exactly as for V1/V2, log it, and do not act on it. V0's
behaviour, its P&L pipeline and its recal feed are untouched, and Part F keeps proving
a property that still holds.

**But the asymmetry this creates is not a footnote — it attacks the WIN condition.**
V1/V2 lose the P&L of killed entries; V0 would not. V0's kill rate will be low (its
population's ask decays ~1.35c/s, moving against us 31% of the time) while V1's will be
high (~3.33c/s, 60%). Scoring an unadjusted V0 against a killed V1 could produce a
**false negative on the +50% leg** — the primary metric. A conservative bias that can
flip the verdict is not acceptable as a silent default.

**So symmetry is recovered in SCORING, not in behaviour.** Because `killed` is logged
for V0, compute both:

- `net_v0_actual` — what V0 really booked (what the dashboard and the audition see).
- `net_v0_killadj` — V0's net with the P&L of counterfactually-killed entries removed
  (i.e. treated as no-position). This is the like-for-like figure.

**Pre-registered scoring, amended:** the +50% leg is judged against
**`net_v0_killadj`**, and `net_v0_actual` is reported as a sensitivity. Each figure
carries a known bias in a *known direction*: unadjusted V0 biases against the
challenger; kill-adjusted V0 biases slightly toward it, because it does not model V0
re-entering after a kill (the H1 preemption externality). The truth is between them.

**If the two figures disagree on the verdict, the result is INCONCLUSIVE, not a win.**
That makes the ambiguity a defined outcome rather than a judgement call made after
seeing the numbers.

Log per intent: `quote_ask`, `fill_ask`, `slip`, `killed`, `latency_ms` — for all three
arms including V0.

## Part D — variant tagging

`variant` field on `v2_intent_open`, every P&L row, and every stop/close event. V0 rows
keep tag `"v0"` — do not leave them untagged, or a later analyst has to infer scope
from absence, which is exactly the ambiguity `skipped_channels` was added to prevent.

## Part E — the two instrumentation points that protect the metric

**E1 — per-tick decision latency, variants on vs off.** The variant machinery does
extra work per tick. If that cost is material it raises kill rates for *all* arms, and
the absolute 25% hard-FAIL leg would be partly measuring our own overhead. ~24
candidates/tick should be negligible — but that is a prediction, not a measurement. Log
p50/p95/p99 tick latency and report it with the results.

**E2 — kill rate against V0's own baseline.** Report `kill_rate{v1} − kill_rate{v0}`
alongside the absolute figure. All three arms at 20% is a different world from V1 alone
at 20%, and the pre-registered 25% leg should be read with that context. This does not
change the registered rule — it adds the diagnostic needed to interpret it honestly.

## Part F — the isolation proof gates arming

The byte-identical test must be green in the same commit, in the refined form already
agreed: **sink present and written to, variants disabled**, asserting V0's intents,
positions, P&L rows and recal samples are identical to a run with the sink absent —
plus the non-vacuity guard that the fixture actually fires. That guard already caught a
vacuously-passing test once; keep it on every future variant test.

## Tests

- Each of the six severances, individually: a shadow open does not move the guard
  budget / canary / `v2_settled` / invariant / `state.json` / recal files.
- Kill model at the exact tolerance boundary (the float case) and either side of it.
- `admits()` superset property: V1 ⊇ V0 and V2 ⊇ V0 on a randomised candidate set.
- Per-variant dedup: a variant holding a market does not re-enter it; V0 holding a
  market does not block a *variant's* first entry in a different market.
- `suppressed_by_v0_hold` increments exactly when expected.
- Full suite green (595 as of `ee879c3`, plus the taker's 586).

---

## Operator run procedure (VPS)

**Gate 1 — Order #14 verified armed.** Not "deployed" — *observed*. This must pass
before the clock starts; a 7-day pre-registered window that ran on a dead feed is void
exactly as the Jul 25–26 weekend exam was.

```bash
journalctl -u velocitybot --since "24 hours ago" --no-pager | grep -E "watchdog armed|feed_watchdog|controls_override|feed_dead"
```

Expect present: `binance feed watchdog armed feed_dead_ms=60000 idle_timeout_s=30`,
`task started: feed_watchdog`, `controls_override … inval_stop_dry`. Expect **absent**:
any `feed_dead`.

**Gate 2 — deploy, still disabled.**

```bash
cd ~/5minVelocity && git pull && cd rust_bot && cargo build --release && cargo test --release 2>&1 | tail -5
```

```bash
sudo systemctl restart velocitybot && sleep 30 && journalctl -u velocitybot --since "2 min ago" --no-pager | grep -E "watchdog armed|variants|bn_kline_age_s"
```

With `variants.enabled = false` this is a **no-behaviour-change deploy**. Confirm
intents still flow at the normal rate and `bn_kline_age_s` is low before proceeding.

**Gate 3 — arm the variants.** Edit `config/bot_v2.toml`, set
`[v2.variants] enabled = true`, restart, then confirm all three arms are live:

```bash
sudo systemctl restart velocitybot && sleep 60 && journalctl -u velocitybot --since "2 min ago" --no-pager | grep -E "variant=|shadow|kill|tick_latency"
```

You should see intents tagged `v0`, `v1`, `v2`. **The clock starts here** — note the
UTC timestamp; it is the start of the pre-registered 7 days.

**Daily check (does not require stopping anything):**

```bash
journalctl -u velocitybot --since "24 hours ago" --no-pager | grep -E "variant_daily|feed_dead|invariant" | tail -20
```

## DO NOT TOUCH

`recal.json` / `recal_15m.json` — the 15m audition is mid-verdict at **n=140** and this
deploy must not disturb it. Also unchanged: the floors, knots, `edge_min`, `z_min`,
`vol_lookback_s`, the stop's behaviour, `controls.json`, and the taker bot's live-arming
path. V0 must remain byte-identical to what runs today, and Part F is what proves it.

## Scoring (restated so it cannot drift)

After **7 full days with the feed healthy throughout**:

- **WIN** — net $/day beats **`net_v0_killadj`** by ≥ +50% **AND** kill rate < 25%
  **AND** positive on ≥ 5 of 7 days **AND** the verdict is unchanged when scored
  against `net_v0_actual`. → promote as a re-based live candidate (a new entry path
  changes the traded population and needs the Order-#6 treatment plus a recal reset).
- **INCONCLUSIVE** — ahead but misses a leg, **or** the kill-adjusted and unadjusted
  V0 baselines disagree on the verdict → extend to 14 days, **once**.
- **FAIL** — does not beat V0 on either baseline, or kill rate ≥ 25%. → the tournament
  result was a recorder artifact. Record it and stop.

No mid-run gate tuning. If an arm is obviously broken (zero entries, invariant alerts),
fix the bug and **restart the clock** rather than adjusting the gate.
