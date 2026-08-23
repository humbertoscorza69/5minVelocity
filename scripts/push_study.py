"""Settlement-manipulation study on our own data (Dai, Jia & Yu 2026).

Tests the two Binance-only strategies from the paper in ONE pass over aggTrades:

  B) UNDERDOG SNIPE. The paper classifies a cycle as manipulated when
     PushIntensity = |net taker flow, final 10s| / median(|net flow| per body bin)
     is at or above 16.11 (its 90th pct; median cycle ~0.9). In those cycles a push
     against the favoured side flips the outcome 34% of the time vs 1% otherwise.
     We reproduce the flip rate on our own tape, and crucially also compute a
     REAL-TIME variant that only sees bins 25-27 (i.e. is known by T-20s), because
     the paper's classifier peeks at the final 10s and is therefore untradeable.

  C) POST-SETTLEMENT REVERSAL. The paper: "within ten seconds the price reverts, by
     about a quarter in the near-even cycles and a tenth in the others." A push is
     transitory; real information persists. We measure the reversion directly.

Binance aggTrades CSV: id, price, qty, first, last, ts_us, was_buyer_maker, best_match
`was_buyer_maker == True` means the BUYER was the maker, so the TAKER SOLD.
Net taker flow = (taker buy notional) - (taker sell notional).

Usage: python scripts/push_study.py [BTCUSDT] [max_days]
"""
import io
import os
import statistics
import sys
import zipfile

SRC = r"D:\polycrypto\aggtrades"
CYCLE_S = 300          # the 5-minute contract
BIN_S = 10             # the paper's bin width; bin 29 is the final 10s
NBINS = CYCLE_S // BIN_S
BODY_END = 25          # bins 0..24 = "body" (the paper's pre-ramp denominator)
RT_LO, RT_HI = 25, 28  # real-time detector sees bins 25,26,27 only -> known by T-20s


def load_day(path):
    """-> {cycle: {'flow':[..30], 'first':[..], 'last':[..]}} for one day's zip."""
    cycles = {}
    z = zipfile.ZipFile(path)
    with z.open(z.namelist()[0]) as fh:
        for line in io.TextIOWrapper(fh, newline=""):
            f = line.split(",")
            if len(f) < 7:
                continue
            try:
                px = float(f[1]); qty = float(f[2]); ts = int(f[5])
            except ValueError:
                continue  # header row
            sec = ts // 1_000_000
            cyc = sec // CYCLE_S
            b = (sec % CYCLE_S) // BIN_S
            c = cycles.get(cyc)
            if c is None:
                c = cycles[cyc] = {
                    "flow": [0.0] * NBINS,
                    "first": [None] * NBINS,
                    "last": [None] * NBINS,
                }
            notional = px * qty
            # f[6] is "True" when the buyer was the maker => the taker was a seller.
            c["flow"][b] += -notional if f[6][0] in "Tt1" else notional
            if c["first"][b] is None:
                c["first"][b] = px
            c["last"][b] = px
    return cycles


def cycle_metrics(c, nxt):
    """Per-cycle features. Returns None when the cycle is too sparse to judge."""
    flow, first, last = c["flow"], c["first"], c["last"]
    body = [abs(flow[i]) for i in range(BODY_END) if first[i] is not None]
    if len(body) < 15:
        return None
    denom = statistics.median(body)
    if denom <= 0:
        return None

    op = next((p for p in first if p is not None), None)
    cl = next((p for p in reversed(last) if p is not None), None)
    # price at T-50s = the state of the world before the push window opens
    t50 = next((first[i] for i in range(RT_LO, NBINS) if first[i] is not None), None)
    if op is None or cl is None or t50 is None or op <= 0:
        return None

    push = abs(flow[NBINS - 1]) / denom                    # paper's PushIntensity
    rt = abs(sum(flow[RT_LO:RT_HI])) / denom               # tradeable variant
    rt_dir = 1 if sum(flow[RT_LO:RT_HI]) > 0 else -1

    # Did the outcome differ from what the pre-push state implied?
    pre_up = t50 >= op
    fin_up = cl >= op
    flipped = pre_up != fin_up

    # Reversal: bin-29 return vs the next 10s (bin 0 of the following cycle).
    r29 = None
    b28 = last[NBINS - 2] if last[NBINS - 2] is not None else None
    if b28 and b28 > 0:
        r29 = (cl / b28 - 1) * 1e4
    rpost = None
    if nxt is not None:
        p_next = next((p for p in nxt["last"][:1] if p is not None), None)
        if p_next and cl > 0:
            rpost = (p_next / cl - 1) * 1e4

    return {
        "push": push, "rt": rt, "rt_dir": rt_dir, "flipped": flipped,
        "margin_bps": abs(t50 / op - 1) * 1e4,   # how decided it looked at T-50s
        "r29": r29, "rpost": rpost,
        "pushed_against": (rt_dir > 0) != pre_up,  # flow opposing the leading side
    }


def pct(xs, q):
    s = sorted(xs)
    return s[min(len(s) - 1, int(q * len(s)))]


def main():
    sym = sys.argv[1] if len(sys.argv) > 1 else "BTCUSDT"
    maxd = int(sys.argv[2]) if len(sys.argv) > 2 else 99
    start = int(sys.argv[3]) if len(sys.argv) > 3 else 0
    files = sorted(f for f in os.listdir(SRC)
                   if f.startswith(sym) and f.endswith(".zip") and len(f.split("-")[-1]) == 6)
    rows = []
    for fn in files[start:start + maxd]:
        cyc = load_day(os.path.join(SRC, fn))
        keys = sorted(cyc)
        for i, k in enumerate(keys):
            m = cycle_metrics(cyc[k], cyc.get(k + 1))
            if m:
                rows.append(m)
        print(f"  {fn}: {len(keys)} cycles -> {len(rows)} usable cumulative", flush=True)
    if not rows:
        print("no data"); return

    print(f"\n=== {sym}: {len(rows)} cycles ===")
    pv = [r["push"] for r in rows]
    print(f"PushIntensity: median {statistics.median(pv):.2f}  p90 {pct(pv,0.90):.2f}  "
          f"p99 {pct(pv,0.99):.2f}   (paper: median ~0.9, p90 16.11)")

    # --- B) flip rate by PushIntensity decile, paper classifier vs real-time ---
    for label, key in (("PAPER PushIntensity (peeks at final 10s)", "push"),
                       ("REAL-TIME (bins 25-27, known by T-20s)", "rt")):
        print(f"\n--- flip rate by decile: {label} ---")
        vals = sorted(rows, key=lambda r: r[key])
        n = len(vals) // 10
        print(f"{'decile':>7} {'n':>6} {'cutoff':>9} {'flip%':>7} {'flip% |near-even':>17}")
        for d in range(10):
            grp = vals[d * n:(d + 1) * n] if d < 9 else vals[9 * n:]
            if not grp:
                continue
            fl = sum(g["flipped"] for g in grp) / len(grp)
            near = [g for g in grp if g["margin_bps"] < 3.0]
            fn_ = (sum(g["flipped"] for g in near) / len(near)) if near else float("nan")
            print(f"{d+1:>7} {len(grp):>6} {grp[0][key]:9.2f} {fl:7.1%} {fn_:17.1%}")

    # --- B2) THE TABLE THAT DECIDES IT -----------------------------------------
    # We have no Polymarket quotes here, but we do not need them. If the book is
    # efficient, the underdog's ask ~= the BASE flip rate for how decided the cycle
    # looks at T-50s. So the tradeable edge is the flip rate when the real-time
    # detector fires MINUS the base rate at the same margin -- priced fairly, paid
    # for by us only when we are wrong about the push.
    print("\n--- B2) real-time edge by how decided the cycle looks at T-50s ---")
    print("    ask_proxy = base flip rate (what an efficient book charges)")
    print("    edge = flip%(detector fires) - ask_proxy - fee(ask_proxy)")
    buckets = [(0, 2), (2, 5), (5, 10), (10, 20), (20, 1e9)]
    rt_cut = pct([r["rt"] for r in rows], 0.90)
    print(f"    detector fires when rt >= {rt_cut:.1f} (top decile)\n")
    print(f"{'margin bps':>12} {'n_fire':>7} {'n_base':>7} {'ask_proxy':>10} "
          f"{'flip|fire':>10} {'edge':>8} {'ret/cost':>9}")
    for lo, hi in buckets:
        grp = [r for r in rows if lo <= r["margin_bps"] < hi]
        fire = [r for r in grp if r["rt"] >= rt_cut]
        base = [r for r in grp if r["rt"] < rt_cut]
        if len(fire) < 30 or len(base) < 30:
            continue
        ask = sum(r["flipped"] for r in base) / len(base)
        win = sum(r["flipped"] for r in fire) / len(fire)
        fee = 0.07 * ask * (1 - ask)
        edge = win - ask - fee
        cost = ask + fee
        tag = f"{lo}-{hi:.0f}" if hi < 1e9 else f"{lo}+"
        print(f"{tag:>12} {len(fire):>7} {len(base):>7} {ask:10.1%} "
              f"{win:10.1%} {edge:+8.3f} {edge/cost if cost else 0:8.0%}")

    # --- C) post-settlement reversal ---
    print("\n--- post-settlement reversal (bps): does the final-10s move revert? ---")
    vals = sorted(rows, key=lambda r: r["push"])
    n = len(vals) // 10
    print(f"{'decile':>7} {'n':>6} {'mean r29':>10} {'mean rpost':>11} {'revert frac':>12}")
    for d in (0, 4, 8, 9):
        grp = [g for g in (vals[d*n:(d+1)*n] if d < 9 else vals[9*n:])
               if g["r29"] is not None and g["rpost"] is not None]
        if not grp:
            continue
        m29 = statistics.mean(abs(g["r29"]) for g in grp)
        # signed against the push: negative means the move gave back
        rev = statistics.mean(-g["rpost"] * (1 if g["r29"] > 0 else -1) for g in grp)
        print(f"{d+1:>7} {len(grp):>6} {m29:10.2f} {rev:11.3f} {rev/m29 if m29 else 0:12.1%}")


if __name__ == "__main__":
    main()
