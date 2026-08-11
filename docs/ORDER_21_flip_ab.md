# Order #21 — Fresh variant A/B: the decoupled FLIP, plus a clean re-read on the stop

Runs in paper. Two open questions, one week, one process, matched tape.

Author: auditor session, 2026-08-11. Ships after Order #20 (the 30s-TWAP label fix).
Evidence: `scripts/revert_and_flip_study.py` over 11.9M 5m book-seconds / 30,442 markets
/ 65 recorder days.

---

## The finding this tests

The gate fires on **both sides of the same market in 9.0% of markets** — 1,312 of 14,510,
about **20/day** — with a median **89 seconds** between the two signals. On those markets:

| | HOLD the original | FLIP (close A at bid, open B) |
|---|---|---|
| EV/$1 | **−0.5372** | **+0.1734** |

A swing of **~+0.71/$1**. Those markets are already inside our ~220/day, so held they
embed roughly −$11.6/day; flipped, roughly +$3.7/day.

Three sub-findings that shape the design:

1. **"Keep A, add B" LOSES** — EV −0.266 / −0.329. The original must be **closed**.
   Holding both is a hedge pair, and hedge pairs have now lost money in two independent
   tests.
2. **82% of the cell would also have been reachable via the band-stop** (stop fires, then
   opposite re-entry); **18% is a cell nothing currently touches**, because the band-stop
   is deliberately suppressed when the bid sits in the fair (0.30, 0.50) zone.
3. **The stop does not earn its keep through this option.** Its own dEV is −$0.0204/stop
   on the trailing 500 at ~484 stops/day; the option is worth a few dollars a day and we
   currently capture ~26% of it (174 lifetime re-entries ≈ 4.4/day against ~20/day
   available). So the correct structure is to **decouple** the flip from the stop rather
   than re-arm a failing stop to harvest a side effect.

**Why this is not the dead flip.** The instant flip at the invalidation crossing is dead
three times over (priced within 2s). The post-stop opposite re-entry is validated
(+0.144/$1). This is the untested middle: a **fully-gated** opposite signal, arriving on
its own schedule, with **no stop required**.

## The three arms

| | definition |
|---|---|
| **V0 — control** | exactly today's config, byte-identical: stop DRY, no flip |
| **V1 — FLIP** | V0's entries, plus: when a **fully-gated** opposite signal fires on a market V1 holds → **close the original at the bid, open the opposite**. Stop stays DRY. |
| **V2 — STOP** | V0's entries with the band-stop **ARMED**, no flip |

V1 isolates the flip. V2 re-reads the stop's own value on the same tape, which is the
other open question — its gauge failed on the trailing 500 but that was one stretch, and
`stop_dev` is logged either way so we get the counterfactual for free.

Flat **$1.05** everywhere. No sizing tiers — separate axis, would confound. One entry per
market per arm; V1's flip is an *exit plus a new entry*, not a second concurrent position.

## What V1 needs that may not exist yet

A **shadow exit-at-bid path** — the ability to close a shadow position mid-window at the
live bid and book it. This is the same machinery Order #18 specified for the shadow
band-stop. If that landed, reuse it. If it did not, it is the one real build here, and V2
needs it too.

Log per flip: `flip_fired`, the original's entry ask, the exit bid, seconds held, the new
leg's ask, and both legs' P&L separately. The exit leg and the entry leg must be
attributable independently or the result is uninterpretable.

## Reset — and one that is genuinely required

Order #20 changes the settlement label from `close[res-1]` to `mean(close[res-30..res-1])`.
That is a change to the **definition of the target**, so every accumulated recal sample was
measured against a different quantity.

**Reset both `recal.json` and `recal_15m.json` on the Order #20 deploy.** Same discipline
as Order #6 (a curve change resets the window). This does restart the audition clock —
accept that; a window that mixes two label definitions is worse than a short one. Note the
5m bias was −0.0899 and out of band anyway; 15m was +0.0263 and passing.

Fresh shadow books for all arms. Note the UTC timestamp at the first armed restart — that
is the start of the pre-registered week.

## Pre-registered decision rule (fixed before the run)

After **7 full days** with the feed healthy (`feed_dead` must be 0 throughout — a repeat
of the 45-hour blind window voids this exactly as it voided the weekend exam):

**The paired test is the primary one, and it is available because all arms see the same
tape.** On the markets where the flip fired, compare V1's realised P&L against V0's
outcome on those *same* markets.

- **FLIP WINS** — V1 beats V0 by **≥ +30%** net $/day, **and** the paired flip-market
  comparison is positive with a day-clustered CI excluding 0, **and** V1 is positive on
  ≥ 5 of 7 days, **and** the flip fired ≥ 10/day. → write it as an entry-path order.
- **STOP WINS** — same bars applied to V2 vs V0 independently.
- **INCONCLUSIVE** — ahead but misses a leg → extend to 14 days, once.
- **FAIL** — does not clear the bars. → the recorder finding was an artifact; record and
  stop.

Both arms are judged **independently against V0**. If both win, they are tested together
in a later run — not merged on the assumption that the effects add.

**Expectation to hold yourself to:** the recorder says the flip is worth ~+$15/day gross,
~$6–7/day after this project's standing 2–2.4× recorder-to-live haircut. **My record on
recorder findings this session is 0 for 1** — the tournament predicted V1 at 3–4× V0 with
permutation p=0.0000 and a sealed holdout, and live delivered 0.76×. If the flip comes in
near zero, that is the expected outcome of a recorder result, not a surprise.

## What NOT to touch

The floors, knots, `edge_min`, `z_min`, `vol_lookback_s`, `min_ask`, `max_ttl_s`, V0's
entry path, `controls.json` beyond the arming flags. V0 must remain byte-identical, and
the `ee879c3` isolation proof must stay green — including the non-vacuity guard, which has
already caught one silently-passing test in this codebase.

## Sequence

1. Order #20 (TWAP label) + reset both recal files.
2. 24h paper burn-in on V0 alone — confirm intent rate ~220/day and the new label is
   flowing (settlement decisions should now differ from the raw close on ~3–4% of windows).
3. Arm V1 and V2. Note the UTC timestamp. Seven days, untouched.
4. Export. I score it against the bars above.

Funding stays paused until this reads out — that is the right order of operations, since
the flip changes what the strategy *is*, and sizing a bankroll to a strategy that is about
to change is wasted work.
