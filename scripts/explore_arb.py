"""Free-money check: does Up + Down ever cost less than the $1 it must pay?

Every up/down market is a complementary pair. Exactly one side pays $1. So
holding BOTH sides is a risk-free $1, whatever happens -- no prediction, no
settlement risk, no exposure to the manipulation the paper documents.

    cost      = ask_up + ask_down + fee(ask_up) + fee(ask_down)
    payout    = 1.00  (guaranteed, one side always wins)
    arbitrage = cost < 1.00

The fee is 0.07*p*(1-p) per share, which VANISHES at extreme prices: a 2c/97c
pair pays 0.34c of fee, a 50/50 pair pays 3.5c. So if this exists anywhere it
will be in the tails, late in a decided cycle -- exactly where the book is
thinnest and quotes go stale.

The mirror also matters: bid_up + bid_down > 1 means someone will PAY more than
$1 for a package worth exactly $1, which is the same trade from the sell side
(mint a complete set for $1, sell both halves).

This scans every recorded quote pair, tracking the last known quote on each side
and re-evaluating whenever either moves.

Usage: python scripts/explore_arb.py
"""
import json
import os
import pickle
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from push_pm_backtest import PM, zst_lines  # noqa: E402

CY = {"5m": 300, "15m": 900, "1h": 3600, "4h": 14400}
FEE = lambda p: 0.07 * p * (1 - p)


def lines(path):
    if path.endswith(".zst"):
        yield from zst_lines(path)
    else:
        with open(path, encoding="utf-8", errors="ignore") as fh:
            yield from fh


def main():
    lab = pickle.load(open("scripts/_true_labels.pkl", "rb"))
    # pair the two legs of each market
    pair = {}
    for tok, d in lab.items():
        key = (d["epoch"], d["asset"], d["interval"])
        pair.setdefault(key, {})[d["side"]] = tok
    mate, meta = {}, {}
    for key, d in pair.items():
        if len(d) == 2:
            u, dn = d["up"], d["down"]
            mate[u] = dn
            mate[dn] = u
            span = CY.get(key[2], 0)
            meta[u] = meta[dn] = (key[0] + span, key[1], key[2])
    print(f"complete up/down pairs: {len(mate)//2}")

    qd = os.path.join(PM, "best_bid_ask")
    files = {f[:10]: os.path.join(qd, f) for f in os.listdir(qd)
             if f.endswith((".jsonl", ".jsonl.zst"))}

    best_buy = []      # (edge, ts_to_close, asset, interval, a_up, a_dn, day)
    best_sell = []
    n_buy = n_sell = n_obs = 0
    hist_buy = defaultdict(int)
    hist_sell = defaultdict(int)

    for day in sorted(files):
        last = {}
        for line in lines(files[day]):
            try:
                pl = json.loads(line)["payload"]
                tok = pl["asset_id"]
            except Exception:
                continue
            m = mate.get(tok)
            if m is None:
                continue
            try:
                ts = int(pl["timestamp"]) // 1000
                bid = float(pl["best_bid"])
                ask = float(pl["best_ask"])
            except (KeyError, TypeError, ValueError):
                continue
            last[tok] = (ts, bid, ask)
            o = last.get(m)
            if o is None:
                continue
            # both legs must be quoted close together in time to be tradeable
            if abs(ts - o[0]) > 2:
                continue
            n_obs += 1
            end, asset, iv = meta[tok]
            ttc = end - ts
            if not (0 < ttc <= CY.get(iv, 0)):
                continue

            cost = ask + o[2] + FEE(ask) + FEE(o[2])
            if 0.0 < ask < 1.0 and 0.0 < o[2] < 1.0 and cost < 1.0:
                n_buy += 1
                hist_buy[round((1.0 - cost) * 100, 1)] += 1
                best_buy.append((1.0 - cost, ttc, asset, iv, ask, o[2], day))

            rev = bid + o[1] - FEE(bid) - FEE(o[1])
            if bid > 0.0 and o[1] > 0.0 and rev > 1.0:
                n_sell += 1
                hist_sell[round((rev - 1.0) * 100, 1)] += 1
                best_sell.append((rev - 1.0, ttc, asset, iv, bid, o[1], day))
        print(f"  {day}: obs={n_obs:,} buy_arb={n_buy:,} sell_arb={n_sell:,}", flush=True)

    print(f"\n=== paired observations: {n_obs:,} ===")
    print(f"BUY-SIDE arb (ask_up + ask_dn + fees < $1.00): {n_buy:,} "
          f"({n_buy/max(n_obs,1):.4%})")
    print(f"SELL-SIDE arb (bid_up + bid_dn - fees > $1.00): {n_sell:,} "
          f"({n_sell/max(n_obs,1):.4%})")

    for name, arr, hist in (("BUY", best_buy, hist_buy), ("SELL", best_sell, hist_sell)):
        if not arr:
            continue
        arr.sort(reverse=True)
        print(f"\n--- {name} side: edge distribution (cents per $1 package) ---")
        for c in sorted(hist, reverse=True)[:12]:
            print(f"   {c:>6.1f}c : {hist[c]:,}")
        print(f"  top 10 by edge:")
        for e, ttc, a, iv, x, y, day in arr[:10]:
            print(f"   {e*100:6.2f}c  T-{ttc:>4}s  {a:>4} {iv:<4} "
                  f"legs {x:.3f}/{y:.3f}  {day}")


if __name__ == "__main__":
    main()
