"""Does buying the TAIL pay? Measured on 49,517 on-chain-resolved tokens.

THE OPERATOR'S ARGUMENT. The paper reports that a settlement push flips a
90-100% favourite 34% of the time. If a near-certain loser trades at 1-2c and
flips anywhere near that often, the payoff is ~50:1 and the strategy is
overwhelmingly profitable even at a low hit rate.

The 34% is CONDITIONAL on a push occurring, and pushes are the top decile of
cycles, so the unconditional flip rate is nearer 0.10*34% + 0.90*1% = 4.3%. That
is still hugely profitable at a 1c entry (breakeven ~1.1%), so the argument does
NOT die on the conditioning. It lives or dies on two things we can measure
directly instead of assuming:

  1. WHAT DOES THE TAIL COST? Polymarket's tick is 1c, so the cheapest possible
     entry is 1c. If the book never actually quotes the tail that cheap near the
     close, the trade does not exist at the price the argument needs.
  2. WHAT DOES THE TAIL PAY? For every token quoted at ask X shortly before the
     close, how often did it ACTUALLY win, on-chain?

Breakeven at ask a is a + fee(a), fee = 0.07*a*(1-a). At 2c that is 2.14%.

No modelling, no proxy label: `true_labels.pkl` is Polymarket's own resolution,
validated at 100.00% (24,756 of 24,757 up/down pairs have exactly one winner).

Usage: python scripts/tail_study.py
"""
import json
import os
import pickle
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from push_pm_backtest import PM, zst_lines  # noqa: E402

CYCLE = {"5m": 300, "15m": 900, "1h": 3600, "4h": 14400}
OFFSETS = (60, 30, 15, 5)          # seconds BEFORE the close
FEE = lambda a: 0.07 * a * (1 - a)


def lines(path):
    if path.endswith(".zst"):
        yield from zst_lines(path)
    else:
        with open(path, encoding="utf-8", errors="ignore") as fh:
            yield from fh


def main():
    labels = pickle.load(open("scripts/_true_labels.pkl", "rb"))
    print(f"resolved tokens: {len(labels)}")
    end = {}
    for tok, d in labels.items():
        span = CYCLE.get(d["interval"])
        if span:
            end[tok] = d["epoch"] + span

    d = os.path.join(PM, "best_bid_ask")
    files = {f[:10]: os.path.join(d, f) for f in os.listdir(d)
             if f.endswith((".jsonl", ".jsonl.zst"))}
    # per token: last ask at each offset before the close
    quotes = defaultdict(dict)
    for day in sorted(files):
        n = 0
        for line in lines(files[day]):
            try:
                pl = json.loads(line)["payload"]
                tok = pl["asset_id"]
            except Exception:
                continue
            e = end.get(tok)
            if e is None:
                continue
            try:
                ts = int(pl["timestamp"]) // 1000
                ask = float(pl["best_ask"])
                bid = float(pl["best_bid"])
            except (KeyError, TypeError, ValueError):
                continue
            for off in OFFSETS:
                if ts <= e - off:
                    quotes[tok][off] = (bid, ask)
            n += 1
        print(f"  {day}: {n} quotes", flush=True)

    print(f"\ntokens with a quote: {len(quotes)}")
    for off in OFFSETS:
        rows = [(q[off][1], labels[t]["won"]) for t, q in quotes.items()
                if off in q and t in labels]
        rows = [(a, w) for a, w in rows if 0.0 < a < 1.0]
        if len(rows) < 500:
            continue
        print(f"\n=== T-{off}s before the close: {len(rows)} quoted tokens ===")
        print(f"{'ask':>10} {'n':>8} {'share':>8} {'ACTUAL WR':>11} "
              f"{'breakeven':>10} {'edge':>9} {'ROI':>9}")
        buckets = [(0.005, 0.015), (0.015, 0.025), (0.025, 0.035),
                   (0.035, 0.055), (0.055, 0.105), (0.105, 0.205)]
        for lo, hi in buckets:
            g = [(a, w) for a, w in rows if lo <= a < hi]
            if len(g) < 50:
                continue
            ask = sum(a for a, _ in g) / len(g)
            wr = sum(w for _, w in g) / len(g)
            be = ask + FEE(ask)
            # buy 1 share at its own ask; win pays 1
            pnl = sum((1.0 - a - FEE(a)) if w else -(a + FEE(a)) for a, w in g)
            cost = sum(a + FEE(a) for a, _ in g)
            print(f"{lo:.3f}-{hi:.3f} {len(g):>8} {len(g)/len(rows):>7.1%} "
                  f"{wr:>11.2%} {be:>10.2%} {wr-be:>+9.2%} {pnl/cost:>+9.1%}")


if __name__ == "__main__":
    main()
