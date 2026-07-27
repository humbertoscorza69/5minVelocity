# Decisions — maker paper bot (#15 A) and variant A/B (#16), run concurrently

Auditor session, 2026-07-27. Settles the open design choices so both can start without
waiting on each other, and without either contaminating the other.

---

## The one constraint that matters

The #16 A/B's headline metric is the **FOK kill rate**, and its hard-FAIL leg is
`kill rate >= 25%`. Kill rate is a latency-sensitive quantity. So anything that adds
CPU, network or WS load to the VPS during the 7-day window can raise kill rates and
fail V1 — the latency-sensitive burst arm — **for the wrong reason**.

Therefore: **the maker paper bot does not share the VPS during the A/B week.** That is
the only sequencing constraint. Everything else runs in parallel.

---

## DECISION 1 — the maker paper bot runs as a REPLAY, not a live process

This supersedes Order #15's implicit "live paper on the VPS" framing.

Run the fill model as a **deterministic replay over the recorder's accumulating data**
on the operator's PC, not as a live process anywhere.

Why this is better, not merely cheaper:

- **Zero resource competition** with the A/B — the constraint above dissolves.
- **Re-scorable.** Order #15's A0 principle is "log inputs, not conclusions". A replay
  IS that principle executed: when the queue assumption changes (e.g. once the
  cancel-vs-trade decomposition from `price_change` lands), we re-run the whole week
  in minutes instead of collecting another week.
- **Reproducible.** The dev already has a replay determinism test; a live paper run is
  not reproducible and cannot be re-scored.
- **A paper maker has no order lifecycle to exercise.** Nothing is posted, so live
  running only proves plumbing — which a short smoke test covers just as well.

What replay does NOT test, and must be stated in the write-up: WS reconnect behaviour
under load, and any latency effect on our own quote placement. Both are live-only and
belong to the eventual $50–100 probe, not to this measurement.

**Start condition:** as soon as the recorder has ~1 day of data. Re-run nightly as
more accumulates. Do not wait for the full week to run it once.

## DECISION 2 — maker config, frozen

Not up for re-optimisation; these came from the IS/OOS-validated study and selecting
new values on this run would be selecting on the validation set.

| | value |
|---|---|
| Asset / side | **BTC only, asks only** (ETH asks were negative every OOS day) |
| Placement | join BBO, **back of queue** (pessimistic) |
| Band | 0.10 – 0.90 |
| Size S | 50 shares nominal; log per-share so it rescales |
| Defense | **naive — none** (defense inverted the result, +0.35 → +0.25) |
| Hours | **all 24h** in replay; slice offline |
| Inventory cap | **150 shares**, enforced against inventory + all resting |
| Cancels | **pessimistic** (assume behind us), amount logged for offline re-scoring |
| Print source | **REST `/trades`** (WS `last_trade_price` covers only 30.2% — measured) |

**Judgment bar:** >= 2,000 modelled fills. Score realised net ¢/share against
**+0.455 OOS / +0.739 rebate-inclusive**. A result materially *below* model is a
SUCCESSFUL run — it means the queue assumption was optimistic, which is the cheapest
possible thing to learn before funding a probe.

## DECISION 3 — variants, frozen

| | gate |
|---|---|
| **V0** | today's deployed stack, consumed as a verdict, never re-implemented |
| **V1** | V0 **OR** burst >= 2bps alone — the tournament winner **unmodified**, no ask cap |
| **V2** | V0 **OR** (burst >= 3bps **AND** ask <= 0.75) |

- Flat **$1.05** everywhere. **No sizing tiers** — separate axis, would confound.
- Exits identical across variants: hold-to-settle + deployed band-stop.
- One entry per market **per variant**; re-entry logic **per variant**.
- FOK kill model with the epsilon fix; log `quote_ask`, `fill_ask`, `slip`, `killed`,
  `latency_ms` per intent.
- V1 is deliberately the raw winner. If a softened version is what survives, we want
  that to be a *finding*, not a thing we assumed at the start.

**Decision rule (pre-registered, unchanged):** 7 full days, feed healthy. WIN =
beats V0 by >= +50% net $/day AND kill rate < 25% AND positive on >= 5/7 days.
INCONCLUSIVE = ahead but misses a leg → extend to 14 days once. FAIL = doesn't beat
V0, or kill rate >= 25%.

## DECISION 4 — what runs where, concurrently

| where | what | starts |
|---|---|---|
| **Operator PC** | universe recorder (#15 B) | **tonight**, independent of everything |
| **Operator PC** | maker replay (#15 A) | once ~1 day of data exists; nightly thereafter |
| **VPS** | variant A/B (#16) | after #14 is *verified armed* — not merely deployed |
| **VPS** | maker paper bot | **not during the A/B week** |

The recorder is the long pole — wall-clock collection that cannot be compressed — so
it starting tonight is the highest-value action regardless of everything else.

## DECISION 5 — the isolation proof gates the #16 start

The byte-identical test (drive the loop over one synthetic event sequence twice,
variants off then on, assert V0's intents / positions / P&L rows / recal samples are
identical) must pass **before** the 7-day clock starts. It is what makes V0 provably
untouched rather than carefully untouched, and it protects the audition, which is
mid-verdict at n=140.

---

## Amendment note

Order #15 Part A's "live paper bot on the VPS" is superseded by Decision 1. The
metrics, config, fill model, inventory cap and judgment bar in Order #15 all stand
unchanged — only the execution venue and mode change, from live-on-VPS to
replay-on-PC. The eventual live probe ($50–100, BTC asks, 14–21 UTC, S=5–10) is
unaffected and still comes after this measurement.
