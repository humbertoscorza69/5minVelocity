"""The tail-follow test, done at proper scale and at the independent unit.

WHAT WENT WRONG BEFORE. The first pass reported +7.27% ROI on 107,240 "trades"
which were really 254 tokens counted ~428 times each. Repeating one outcome does
not create information. At the token level n was 254 and every delay came back
within one standard error of zero -- but 254 is one day of markets, far too thin
to settle a question about a 1-in-10 event.

WHAT IS DIFFERENT HERE. ~2,500 markets sampled evenly across the whole recorded
span (rather than one block), which should give roughly ten times the distinct
tokens. Two tests, kept separate because they are different claims:

  A. FOLLOW ANY TAIL BUY. Someone buys at 1-10c; we buy the same token at the
     ask, `delay` seconds later. This is the "the tail flips often enough" claim.

  B. FOLLOW THE IDENTIFIED WALLETS' TAIL BUYS. Wallets are selected for profit
     on the EARLY half and tested only on the LATE half, so the selection cannot
     leak. This is the "follow the manipulators" claim.

In both, ONE ENTRY PER TOKEN -- the first qualifying signal. That is the
independent unit, and the sample size that goes in the significance test.

The bar is the same as everywhere else: the win rate must beat ask + fee.

Usage: python scripts/tail_wide_analysis.py
"""
import json
import os
import pickle
import statistics
import sys
from collections import defaultdict
from math import sqrt

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from push_pm_backtest import PM, zst_lines  # noqa: E402

import datetime  # noqa: E402

FEE = lambda p: 0.07 * p * (1 - p)
LO, HI = 0.01, 0.10


def ask_paths(tokens, days):
    out = defaultdict(list)
    for day in days:
        p = None
        for ext in (".jsonl.zst", ".jsonl"):
            q = os.path.join(PM, "best_bid_ask", day + ext)
            if os.path.exists(q):
                p = q
                break
        if p is None:
            continue
        src = zst_lines(p) if p.endswith(".zst") else open(p, encoding="utf-8", errors="ignore")
        for line in src:
            try:
                pl = json.loads(line)["payload"]
                tok = pl["asset_id"]
            except Exception:
                continue
            if tok not in tokens:
                continue
            try:
                ts = int(pl["timestamp"]) // 1000
                a = float(pl["best_ask"])
            except (KeyError, TypeError, ValueError):
                continue
            out[tok].append((ts, a))
    for t in out:
        out[t].sort()
    return out


def score(entries, lab, label):
    """entries: {token: (ts, ask)} -- one per token. Report with significance."""
    rows = [(a, lab[t]["won"]) for t, (_, a) in entries.items() if 0.0 < a < 1.0]
    if len(rows) < 40:
        print(f"  {label:<34} n={len(rows)} (too few)")
        return
    n = len(rows)
    k = sum(1 for _, w in rows if w)
    ma = statistics.mean(a for a, _ in rows)
    be = ma + FEE(ma)
    net = sum((1 - a - FEE(a)) if w else -(a + FEE(a)) for a, w in rows)
    cost = sum(a + FEE(a) for a, _ in rows)
    se = sqrt(max(be * (1 - be) / n, 1e-12))
    print(f"  {label:<34} n={n:<6} ask={ma:.4f} WR={k/n:>6.2%} "
          f"be={be:>6.2%} ROI={net/cost:>+8.2%} z={(k/n-be)/se:>+6.2f}")


def main():
    data = pickle.load(open("scripts/_tail_wide.pkl", "rb"))
    lab = pickle.load(open("scripts/_true_labels.pkl", "rb"))
    eps = sorted(data)
    print(f"markets: {len(eps):,}   trades kept: {sum(len(v) for v in data.values()):,}")
    mid = eps[len(eps) // 2]

    # ---- wallet selection on the EARLY half only ---------------------------
    prof = defaultdict(lambda: [0.0, 0])
    for ep in eps:
        if ep >= mid:
            continue
        for tok, ts, p, s, side, w in data[ep]:
            L = lab.get(tok)
            if L is None:
                continue
            g = s * (1 - p) if L["won"] else -s * p
            if side != "BUY":
                g = -g
            e = prof[w]
            e[0] += g
            e[1] += 1
    top = {a for _, a in sorted(((v[0], a) for a, v in prof.items() if v[1] >= 20),
                                reverse=True)[:100]}
    print(f"wallets scored on early half: {len(prof):,}   top-100 selected")

    # ---- collect FIRST tail-buy signal per token, on the LATE half ---------
    sig_any, sig_top = {}, {}
    for ep in eps:
        if ep < mid:
            continue
        for tok, ts, p, s, side, w in data[ep]:
            if side != "BUY" or not (LO <= p < HI) or tok not in lab:
                continue
            if tok not in sig_any or ts < sig_any[tok]:
                sig_any[tok] = ts
            if w in top and (tok not in sig_top or ts < sig_top[tok]):
                sig_top[tok] = ts
    print(f"tokens with ANY tail buy (late half): {len(sig_any):,}")
    print(f"tokens with a TOP-WALLET tail buy   : {len(sig_top):,}")

    days = sorted({datetime.datetime.fromtimestamp(e, datetime.UTC).strftime("%Y-%m-%d")
                   for e in eps if e >= mid})
    paths = ask_paths(set(sig_any) | set(sig_top), days)
    print(f"ask paths reconstructed: {len(paths):,}\n")

    for name, sig in (("A. follow ANY tail buy", sig_any),
                      ("B. follow TOP-WALLET tail buys", sig_top)):
        print(f"{name}")
        for delay in (0, 2, 5, 10, 20):
            ent = {}
            for tok, ts in sig.items():
                pp = paths.get(tok)
                if not pp:
                    continue
                a = None
                for qt, qa in pp:
                    if qt <= ts + delay:
                        a = qa
                    else:
                        break
                if a is not None:
                    ent[tok] = (ts, a)
            score(ent, lab, f"buy at ask, +{delay}s")
        print()


if __name__ == "__main__":
    main()
