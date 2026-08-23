"""Is the MAKER seat profitable on the 5m BTC up/down contract?

Everything else we tested died in the taker seat: the reactive push snipe
(book reprices as fast as the signal improves), the pre-push filters (the ask
already charges more in thin hours), and the post-settlement spot reversal
(0.3 bps against 4.5 bps of fee). The one number that came back positive was the
mirror of the taker's loss -- whoever posts the ask collects it.

The paper says the same thing structurally: the liquidity trader's loss "is a
transfer to the prediction-market maker and the manipulator", and makers "quote
passively and end each cycle flat" while almost none of the loss lands on them.

This measures the GROSS rent directly, per token, across the whole cycle. We know
every settlement, so for any quote at any moment:

    sell edge = ask  - P(token wins)        (post the offer, get lifted)
    buy  edge = P(token wins) - bid         (post the bid, get hit)

Both are computed on EVERY quote, which deliberately assumes fills arrive
independently of information. That is the optimistic case: it is an upper bound
on the rent, and the gap between it and reality is adverse selection, which
needs trade prints we do not have recorded. Read the output accordingly -- if the
gross rent is thin, the strategy is dead regardless; if it is fat, adverse
selection becomes the next thing to measure.

The paper also predicts WHERE the rent should break down: makers face "direct
exposure to a flipping settlement" near the close, so the term structure by
time-to-close is the load-bearing output here, not the pooled average.

Usage: python scripts/maker_study.py 2026-07
"""
import os
import statistics
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from push_pm_backtest import PM, load_markets, zst_lines  # noqa: E402

import json  # noqa: E402

CYCLE_S = 300
# quote snapshots through the cycle, in seconds since the open
OFFSETS = (30, 60, 90, 120, 150, 180, 210, 240, 260, 270, 280, 290)
# maker_bot's calibrated rebate: 20% of taker fees on filled maker volume
TAKER_FEE_RATE = 0.07
REBATE_SHARE = 0.20
rebate = lambda p: REBATE_SHARE * TAKER_FEE_RATE * min(p, 1.0 - p)


def load_ladder(day, tok2ep):
    """-> {token: {off: (bid, ask), 'final': (bid, ask)}} in one pass."""
    p = os.path.join(PM, "best_bid_ask", f"{day}.jsonl.zst")
    if not os.path.exists(p):
        return {}
    q = {}
    for line in zst_lines(p):
        try:
            pl = json.loads(line)["payload"]
            tok = pl["asset_id"]
        except Exception:
            continue
        ep = tok2ep.get(tok)
        if ep is None:
            continue
        try:
            ts = int(pl["timestamp"]) // 1000
            quote = (float(pl["best_bid"]), float(pl["best_ask"]))
        except (KeyError, TypeError, ValueError):
            continue
        d = q.setdefault(tok, {})
        rel = ts - ep
        for off in OFFSETS:
            if rel <= off:
                d[off] = quote
        if rel >= CYCLE_S + 5:
            d["final"] = quote
    return q


def main():
    month = sys.argv[1] if len(sys.argv) > 1 else "2026-07"
    days = sorted({f[:10] for f in os.listdir(os.path.join(PM, "best_bid_ask"))
                   if f.startswith(month)})
    rows = []          # (offset, bid, ask, won)
    drift = []         # per-token mid path
    for day in days:
        mk = load_markets(day)
        if not mk:
            continue
        tok2ep = {}
        for ep, m in mk.items():
            if m["up"]:
                tok2ep[m["up"]] = ep
            if m["down"]:
                tok2ep[m["down"]] = ep
        q = load_ladder(day, tok2ep)
        for ep, m in mk.items():
            fu = q.get(m["up"], {}).get("final")
            fd = q.get(m["down"], {}).get("final")
            if not fu or not fd:
                continue
            if fu[0] >= 0.9 and fd[0] <= 0.1:
                win = {"up": 1.0, "down": 0.0}
            elif fd[0] >= 0.9 and fu[0] <= 0.1:
                win = {"up": 0.0, "down": 1.0}
            else:
                continue
            # mid path, for the adverse-selection scale below
            for side in ("up", "down"):
                d = q.get(m[side], {})
                mids = {o: (d[o][0] + d[o][1]) / 2 for o in OFFSETS if d.get(o)}
                if mids:
                    drift.append(mids)
            for side in ("up", "down"):
                d = q.get(m[side], {})
                for off in OFFSETS:
                    qt = d.get(off)
                    if not qt:
                        continue
                    b, a = qt
                    if not (0.0 <= b < a <= 1.0):
                        continue
                    rows.append((off, b, a, win[side]))
        print(f"  {day}: {len(rows)} quote-observations", flush=True)

    if not rows:
        print("no data"); return
    print(f"\n=== {month}: {len(rows)} quote-observations "
          f"({len(rows)//len(OFFSETS)} token-cycles) ===")
    print("GROSS rent, fills assumed information-independent (upper bound).")
    print("sell = post the offer and get lifted;  buy = post the bid and get hit.\n")
    print(f"{'TTM':>6} {'n':>7} {'bid':>6} {'ask':>6} {'spr':>6} {'P(win)':>7} "
          f"{'sell edge':>10} {'buy edge':>9} {'2-sided/sh':>11} {'+rebate':>9}")
    for off in OFFSETS:
        g = [r for r in rows if r[0] == off]
        if len(g) < 200:
            continue
        b = statistics.mean(r[1] for r in g)
        a = statistics.mean(r[2] for r in g)
        w = statistics.mean(r[3] for r in g)
        sell = statistics.mean(r[2] - r[3] for r in g)
        buy = statistics.mean(r[3] - r[1] for r in g)
        two = (sell + buy) / 2.0
        reb = statistics.mean(rebate((r[1] + r[2]) / 2) for r in g)
        print(f"  T-{CYCLE_S-off:>3}s {len(g):>7} {b:6.3f} {a:6.3f} {a-b:6.3f} "
              f"{w:7.1%} {sell:+10.4f} {buy:+9.4f} {two:+11.4f} {two+reb:+9.4f}")

    print()
    print("=== ADVERSE SELECTION SCALE: how far does the mid move? ===")
    print("The maker collects half the spread (0.54c) only if fills are random.")
    print("It is gone the moment fills lean toward whoever is right. So: how big")
    print("is the move a maker is exposed to, and how skewed may fills be before")
    print("the half-spread is wiped out?")
    print()
    HALF = 0.0054
    hdr = "{:>8} {:>8} {:>7} {:>13} {:>24}".format(
        "from", "to", "n", "mean |d mid|", "max tolerable skew")
    print(hdr)
    for f_, t_ in [(210,240),(240,270),(270,290),(150,210),(60,150),(30,290)]:
        d = [abs(m[t_] - m[f_]) for m in drift if f_ in m and t_ in m]
        if len(d) < 200:
            continue
        mu = statistics.mean(d)
        print("  T-{:>3}s  T-{:>3}s {:>7} {:>13.4f} {:>23.1%}".format(
              CYCLE_S-f_, CYCLE_S-t_, len(d), mu, (HALF/mu if mu else 0)))
    print()
    print("\n--- the same, split by how far the quote is from even ---")
    print("(a maker's exposure is largest near 0.5; the paper says that is exactly")
    print(" where a flipping settlement hurts them)\n")
    print(f"{'mid band':>12} {'n':>7} {'spr':>6} {'sell edge':>10} {'buy edge':>9} {'2-sided':>9}")
    for lo, hi in ((0.0, 0.1), (0.1, 0.25), (0.25, 0.4), (0.4, 0.6),
                   (0.6, 0.75), (0.75, 0.9), (0.9, 1.01)):
        g = [r for r in rows if lo <= (r[1] + r[2]) / 2 < hi]
        if len(g) < 200:
            continue
        sell = statistics.mean(r[2] - r[3] for r in g)
        buy = statistics.mean(r[3] - r[1] for r in g)
        spr = statistics.mean(r[2] - r[1] for r in g)
        print(f"  {lo:.2f}-{hi:.2f} {len(g):>9} {spr:6.3f} {sell:+10.4f} "
              f"{buy:+9.4f} {(sell+buy)/2:+9.4f}")


if __name__ == "__main__":
    main()
