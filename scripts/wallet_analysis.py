"""Find the manipulators, then test whether following them is actually tradeable.

The paper identifies manipulators by realised profit: 821 wallets take $8.2M in
pushed cycles while breaking even elsewhere. We reproduce that identification on
our own sample, then ask the only question that matters for us.

BEING ABLE TO NAME THEM IS NOT AN EDGE. Every other signal we tested failed for
the same reason -- it arrived no earlier than the price. So the test here is not
"do these wallets win" (the paper already says yes). It is:

  1. WHEN do they trade? If they position early, before the final-50s push, their
     fills are genuinely leading information.
  2. Does their direction predict the outcome BEYOND the price at that moment?
     If the book already reflects their buying, following them buys nothing.
  3. Would following them have paid, after the spread and fee?

P&L convention per trade, using the on-chain resolution:
  BUY  side that won  -> +size*(1-price)      lost -> -size*price
  SELL side that won  -> -size*(1-price)      lost -> +size*price

Usage: python scripts/wallet_analysis.py
"""
import pickle
import statistics
from collections import defaultdict

FEE = lambda p: 0.07 * p * (1 - p)
CYCLE = 300


def pnl(trade, won):
    p = float(trade["price"]); s = float(trade["size"])
    gain = s * (1 - p) if won else -s * p
    return gain if trade["side"].upper() == "BUY" else -gain


def main():
    data = pickle.load(open("scripts/_wallet_trades.pkl", "rb"))
    lab = pickle.load(open("scripts/_true_labels.pkl", "rb"))
    print(f"markets: {len(data)}   trades: {sum(len(v['trades']) for v in data.values()):,}")

    # ---- wallet P&L split by cycle type -------------------------------------
    w = defaultdict(lambda: {"manip": [0.0, 0], "control": [0.0, 0]})
    for ep, v in data.items():
        for t in v["trades"]:
            tok = t.get("asset")
            L = lab.get(tok)
            if L is None:
                continue
            g = pnl(t, L["won"])
            e = w[t["proxyWallet"]][v["tag"]]
            e[0] += g
            e[1] += 1
    print(f"distinct wallets: {len(w):,}")

    # the paper's criterion: profits where the manipulation is, flat elsewhere
    cands = []
    for addr, d in w.items():
        mp, mn = d["manip"]
        cp, cn = d["control"]
        if mn >= 10 and mp > 0:
            cands.append((mp, mn, cp, cn, addr))
    cands.sort(reverse=True)
    print(f"\nwallets active in >=10 manipulated-cycle trades AND profitable there: {len(cands):,}")
    print(f"{'wallet':>14} {'manip $':>11} {'n':>6} {'control $':>11} {'n':>6}")
    for mp, mn, cp, cn, a in cands[:15]:
        print(f"{a[:12]+'..':>14} {mp:>+11.2f} {mn:>6} {cp:>+11.2f} {cn:>6}")

    tot_m = sum(d["manip"][0] for d in w.values())
    tot_c = sum(d["control"][0] for d in w.values())
    print(f"\nALL wallets pooled: manip {tot_m:+,.0f}   control {tot_c:+,.0f}")
    print("  (should be ~0 both -- every dollar won is a dollar lost by someone)")

    # ---- WHEN do the top wallets trade? -------------------------------------
    top = {a for _, _, _, _, a in cands[:50]}
    print("\n=== TIMING: where in the cycle does each group trade? ===")
    print("(the push lands in the final 50s; positioning BEFORE it is what would")
    print(" make a wallet a leading indicator rather than an echo)")
    print(f"{'group':>12} {'trades':>9} {'median TTC':>12} {'% in last 50s':>15} {'% first half':>13}")
    for lab_, sel in (("top wallets", lambda a: a in top), ("everyone else", lambda a: a not in top)):
        ttc = []
        for ep, v in data.items():
            if v["tag"] != "manip":
                continue
            end = ep + CYCLE
            for t in v["trades"]:
                if not sel(t["proxyWallet"]):
                    continue
                try:
                    ts = int(t["timestamp"])
                except (TypeError, ValueError):
                    continue
                r = end - ts
                if 0 <= r <= CYCLE:
                    ttc.append(r)
        if len(ttc) < 50:
            continue
        print(f"{lab_:>12} {len(ttc):>9,} {statistics.median(ttc):>11.0f}s "
              f"{sum(1 for x in ttc if x <= 50)/len(ttc):>14.1%} "
              f"{sum(1 for x in ttc if x > CYCLE/2)/len(ttc):>12.1%}")

    # ---- would following them have paid? ------------------------------------
    print("\n=== FOLLOW TEST: copy a top wallet's BUY, at the price THEY paid ===")
    print("(their own fill price is the most generous assumption available: no")
    print(" slippage, no latency, no queue. If it does not pay here, it never will)")
    for cut in (50, 100, 200, CYCLE):
        n = win = 0
        net = cost = 0.0
        for ep, v in data.items():
            end = ep + CYCLE
            for t in v["trades"]:
                if t["proxyWallet"] not in top or t["side"].upper() != "BUY":
                    continue
                try:
                    ts = int(t["timestamp"])
                except (TypeError, ValueError):
                    continue
                if not (0 <= end - ts <= cut):
                    continue
                L = lab.get(t.get("asset"))
                if L is None:
                    continue
                p = float(t["price"]); s = float(t["size"])
                if not (0.0 < p < 1.0):
                    continue
                n += 1
                win += L["won"]
                net += (s * (1 - p) - s * FEE(p)) if L["won"] else (-s * p - s * FEE(p))
                cost += s * p
        if n >= 30:
            print(f"  entries within T-{cut:>3}s: n={n:<6} WR={win/n:>6.1%} "
                  f"net=${net:>+10.2f} ROI={net/cost:>+7.2%}")


if __name__ == "__main__":
    main()
