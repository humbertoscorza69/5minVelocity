"""Would EXITING beat holding to settlement? Measured on our own real live trades.

The post-mortem found the loss is concentrated where the settlement label is
unreliable: trades resolving on a move under ~6 bps lost $332, while 8+ bps
trades earned +$272. Binance's mid sits ~2.5 bps from the Chainlink oracle, so
under ~6 bps our price source genuinely cannot tell who won -- and the 0-2 bps
bucket lost 61.5% of cost at a 26% win rate.

That suggests the operator's idea: stop playing the settlement lottery. If a
position marks up during the window, sell it into the book instead of holding to
a resolution we cannot predict.

This reconstructs, for every REAL trade we took, the full Polymarket bid path
from entry to settlement, and asks:

  * how far did it run in our favour (MFE) and against us (MAE)?
  * how often was a profitable exit available at all?
  * would a take-profit rule have beaten holding?
  * does the answer differ in the unreliable zone vs the trustworthy one?

We bought at `fill_price`, so an exit SELLS INTO THE BID -- the pessimistic and
correct convention. Settlement P&L uses the CORRECTED net from the recorder (the
payout that actually happened), never the optimistic booking.

Usage: python scripts/exit_path_study.py
"""
import json
import os
import statistics
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from push_pm_backtest import PM, zst_lines  # noqa: E402

BUNDLE = (r"C:\Users\tico_\AppData\Local\Temp\claude"
          r"\C--Users-tico--Fable-5minSnip\1e2b4f23-f27a-4d64-a819-753bc75ba264"
          r"\scratchpad\pm\postmortem_20260831_1926")
FEE = lambda p: 0.07 * p * (1 - p)
# session start of the funded live run (2026-08-23 13:20:39 UTC)
LIVE_START_S = 1787225000


def load_trades():
    """Our real v0 entries, joined to the corrected settlement P&L."""
    trades = {}
    with open(os.path.join(BUNDLE, "events.jsonl"), encoding="utf-8", errors="ignore") as fh:
        for line in fh:
            if '"v2_intent_open"' not in line:
                continue
            try:
                d = json.loads(line)["data"]
            except Exception:
                continue
            if d.get("variant") != "v0" or not d.get("token_id"):
                continue
            sid = d.get("signal_id") or ""
            parts = sid.split("-")
            if len(parts) < 2 or not parts[1].isdigit():
                continue
            trades[d["token_id"]] = {
                "entry_s": int(parts[1]),
                "exit_s": d.get("exit_ts_s"),
                "fill": d.get("fill_price") or d.get("ask"),
                "shares": d.get("shares", 0.0),
                "disp_bps": abs(d.get("disp_bps") or 0.0),
                "z": d.get("z"),
                "interval": "15m" if "-15m-" in sid else "5m",
            }
    # SETTLEMENT displacement (what the post-mortem binned on). The entry `disp_bps`
    # is a different quantity -- how far price had moved when we ENTERED -- and
    # conflating the two mislabels every zone.
    with open(os.path.join(BUNDLE, "events.jsonl"), encoding="utf-8", errors="ignore") as fh:
        for line in fh:
            if '"v2_settle"' not in line:
                continue
            try:
                d = json.loads(line)["data"]
            except Exception:
                continue
            if d.get("token_id") in trades:
                trades[d["token_id"]]["settle_disp"] = abs(d.get("disp_twap_bps") or 0.0)

    # corrected settlement truth
    for line in open(os.path.join(BUNDLE, "pnl_recorded.jsonl"), encoding="utf-8",
                     errors="ignore"):
        try:
            v = json.loads(line)
        except Exception:
            continue
        t = v.get("token_id")
        if t not in trades:
            continue
        if v["kind"] == "recorded":
            trades[t]["settle_net"] = v["net_pnl"]
            trades[t]["settle_booked"] = v["net_pnl"]
        elif v["kind"] == "corrected":
            trades[t]["settle_net"] = v.get("true_net", v.get("net_pnl"))
    # LIVE SESSION ONLY. events.jsonl spans the paper era too, and July's paper book
    # was strongly positive -- including it would report a profitable "hold" baseline
    # that has nothing to do with the funded run.
    return {t: d for t, d in trades.items()
            if d.get("settle_net") is not None and d.get("fill") and d.get("exit_s")
            and d["entry_s"] >= LIVE_START_S and d.get("settle_disp") is not None}


def load_paths(trades):
    """bid path per token, entry -> settlement, from the recorder."""
    want = {}
    for t, d in trades.items():
        want[t] = (d["entry_s"], d["exit_s"])
    days = sorted(f[:10] for f in os.listdir(os.path.join(PM, "best_bid_ask"))
                  if f.startswith("2026-08"))
    paths = {t: [] for t in trades}
    for day in days:
        p = os.path.join(PM, "best_bid_ask", f"{day}.jsonl.zst")
        if not os.path.exists(p):
            p = os.path.join(PM, "best_bid_ask", f"{day}.jsonl")
            if not os.path.exists(p):
                continue
            src = (l for l in open(p, encoding="utf-8", errors="ignore"))
        else:
            src = zst_lines(p)
        hit = 0
        for line in src:
            try:
                pl = json.loads(line)["payload"]
                tok = pl["asset_id"]
            except Exception:
                continue
            w = want.get(tok)
            if w is None:
                continue
            try:
                ts = int(pl["timestamp"]) // 1000
                bid = float(pl["best_bid"])
            except (KeyError, TypeError, ValueError):
                continue
            if w[0] <= ts <= w[1]:
                paths[tok].append((ts, bid))
                hit += 1
        print(f"  {day}: {hit} quotes matched", flush=True)
    for t in paths:
        paths[t].sort()
    return paths


def main():
    trades = load_trades()
    print(f"real v0 trades with a corrected settlement: {len(trades)}")
    paths = load_paths(trades)

    rows = []
    for t, d in trades.items():
        path = paths.get(t) or []
        if len(path) < 3:
            continue
        fill = d["fill"]
        bids = [b for _, b in path]
        mfe = max(bids) - fill          # best exit available, in $/share
        mae = min(bids) - fill
        # settlement P&L per share, from the CORRECTED net
        set_ps = d["settle_net"] / d["shares"] if d["shares"] else 0.0
        rows.append({**d, "mfe": mfe, "mae": mae, "set_ps": set_ps,
                     "n_q": len(path), "path": path, "fill": fill})
    print(f"reconstructed paths for {len(rows)} trades\n")
    if not rows:
        return

    print("=== how the position behaved BEFORE settlement (per share, $) ===")
    print(f"{'zone':>14} {'n':>5} {'mean MFE':>9} {'mean MAE':>9} "
          f"{'MFE>2c':>7} {'MFE>5c':>7} {'settle/sh':>10}")
    zones = [("0-2 bps", 0, 2), ("2-6 bps", 2, 6), ("6-8 bps", 6, 8),
             ("8+ bps", 8, 1e9), ("ALL", 0, 1e9)]
    for lab, lo, hi in zones:
        g = [r for r in rows if lo <= r["settle_disp"] < hi]
        if len(g) < 20:
            continue
        print(f"{lab:>14} {len(g):>5} {statistics.mean(r['mfe'] for r in g):+9.4f} "
              f"{statistics.mean(r['mae'] for r in g):+9.4f} "
              f"{sum(1 for r in g if r['mfe'] > 0.02)/len(g):>6.1%} "
              f"{sum(1 for r in g if r['mfe'] > 0.05)/len(g):>6.1%} "
              f"{statistics.mean(r['set_ps'] for r in g):+10.4f}")

    print("\n=== TAKE-PROFIT: sell into the bid at +Xc, else hold to settlement ===")
    print("(exit price = bid, so this is what we could actually have got)")
    print(f"{'rule':>12} {'n exits':>8} {'exit rate':>10} {'total $':>10} "
          f"{'vs hold':>9} {'ROI':>8}")
    hold_total = sum(r["set_ps"] * r["shares"] for r in rows)
    cost = sum(r["fill"] * r["shares"] for r in rows)
    print(f"{'HOLD (real)':>12} {'-':>8} {'-':>10} {hold_total:+10.2f} "
          f"{'-':>9} {hold_total/cost:+8.1%}")
    for tp in (0.02, 0.03, 0.05, 0.08, 0.12, 0.20):
        tot, n_ex = 0.0, 0
        for r in rows:
            target = r["fill"] + tp
            hit = next((b for _, b in r["path"] if b >= target), None)
            if hit is not None:
                # sell into the bid; taker fee on the way out
                tot += (hit - r["fill"] - FEE(hit)) * r["shares"]
                n_ex += 1
            else:
                tot += r["set_ps"] * r["shares"]
        print(f"{'+' + f'{tp*100:.0f}c':>12} {n_ex:>8} {n_ex/len(rows):>9.1%} "
              f"{tot:+10.2f} {tot-hold_total:+9.2f} {tot/cost:+8.1%}")

    ambiguity_exit(rows)
    late_mark(rows)
    entry_selection(rows)
    print("\n=== the same, restricted to the UNRELIABLE zone (<6 bps) ===")
    U = [r for r in rows if r["settle_disp"] < 6]
    if len(U) > 20:
        hu = sum(r["set_ps"] * r["shares"] for r in U)
        cu = sum(r["fill"] * r["shares"] for r in U)
        print(f"{'HOLD (real)':>12} {'-':>8} {'-':>10} {hu:+10.2f} {'-':>9} {hu/cu:+8.1%}")
        for tp in (0.02, 0.03, 0.05, 0.08):
            tot, n_ex = 0.0, 0
            for r in U:
                target = r["fill"] + tp
                hit = next((b for _, b in r["path"] if b >= target), None)
                if hit is not None:
                    tot += (hit - r["fill"] - FEE(hit)) * r["shares"]
                    n_ex += 1
                else:
                    tot += r["set_ps"] * r["shares"]
            print(f"{'+' + f'{tp*100:.0f}c':>12} {n_ex:>8} {n_ex/len(U):>9.1%} "
                  f"{tot:+10.2f} {tot-hu:+9.2f} {tot/cu:+8.1%}")


def entry_selection(rows):
    """Could we have known at ENTRY which trades were already dead?

    The exit study says the book decides by T-60s: 41% of our positions are marked
    under 0.10 and 58% over 0.95, with almost nothing between. Exiting cannot help
    because there is no ambiguity left to trade out of. So the only remaining lever
    is selection -- do the features we ALREADY compute at entry separate the two?
    """
    print()
    print("=== ENTRY FEATURES vs realised P&L per share (live book) ===")
    def show(key, edges, label):
        print()
        print(f"-- {label} --")
        print("{:>14} {:>6} {:>10} {:>9} {:>9}".format("bucket","n","settle/sh","ROI","WR"))
        for lo,hi in edges:
            g=[r for r in rows if r.get(key) is not None and lo<=r[key]<hi]
            if len(g)<25: continue
            net=sum(r["settle_net"] for r in g)
            cost=sum(r["fill"]*r["shares"] for r in g)
            wr=sum(1 for r in g if r["settle_net"]>0)/len(g)
            print("{:>14} {:>6} {:>+10.4f} {:>+9.1%} {:>9.1%}".format(
                  f"{lo:g}-{hi:g}", len(g), net/sum(r["shares"] for r in g), net/cost, wr))
    show("z",[(0,0.5),(0.5,0.7),(0.7,1.0),(1.0,1.5),(1.5,99)],"entry z (vol-normalised displacement)")
    show("disp_bps",[(0,4),(4,6),(6,8),(8,12),(12,20),(20,999)],"entry disp_bps")
    show("fill",[(0.3,0.5),(0.5,0.6),(0.6,0.7),(0.7,0.85),(0.85,1.0)],"entry ask paid")
    print()
    print("-- by interval --")
    for iv in ("5m","15m"):
        g=[r for r in rows if r["interval"]==iv]
        if len(g)<25: continue
        net=sum(r["settle_net"] for r in g); cost=sum(r["fill"]*r["shares"] for r in g)
        print("{:>14} {:>6} {:>+10.4f} {:>+9.1%} {:>9.1%}".format(
              iv,len(g),net/sum(r["shares"] for r in g),net/cost,
              sum(1 for r in g if r["settle_net"]>0)/len(g)))


def late_mark(rows):
    """What does the mark shortly before the close tell us about the payout?

    The band exit almost never triggered, which is itself the finding: by T-60s the
    book has usually already moved to near 0 or 1. So the question is not "is it
    ambiguous" but "is it against us" -- and whether selling a losing position into
    the bid beats letting it settle at zero.
    """
    print()
    print("=== the mark at T-60s vs what actually settled (per share) ===")
    print()
    print("{:>12} {:>6} {:>10} {:>11} {:>11}".format(
          "mark @T-60s", "n", "share", "settle/sh", "exit/sh"))
    buckets=[(0.0,0.10),(0.10,0.25),(0.25,0.45),(0.45,0.60),(0.60,0.80),(0.80,0.95),(0.95,1.01)]
    marks=[]
    for r in rows:
        t_at=r["exit_s"]-60; m=None
        for ts,b in r["path"]:
            if ts<=t_at: m=b
            else: break
        if m is not None: marks.append((m,r))
    for lo,hi in buckets:
        g=[(m,r) for m,r in marks if lo<=m<hi]
        if len(g)<15: continue
        sp=statistics.mean(r["set_ps"] for _,r in g)
        ep=statistics.mean(m-r["fill"]-FEE(m) for m,r in g)
        print("{:>12} {:>6} {:>9.1%} {:>+11.4f} {:>+11.4f}".format(
              f"{lo:.2f}-{hi:.2f}", len(g), len(g)/len(marks), sp, ep))
    print()
    print("=== STOP-LOSS: if the mark at T-60s is below X, sell into the bid ===")
    hold=sum(r["set_ps"]*r["shares"] for r in rows)
    cost=sum(r["fill"]*r["shares"] for r in rows)
    print("{:>10} {:>8} {:>8} {:>10} {:>10} {:>8}".format(
          "cut","n exits","rate","total $","vs hold","ROI"))
    print("{:>10} {:>8} {:>8} {:>10.2f} {:>10} {:>7.1%}".format(
          "HOLD",0,"-",hold,"-",hold/cost))
    for x in (0.10,0.20,0.30,0.40,0.50):
        tot,n=0.0,0
        for m,r in marks:
            if m<x:
                tot+=(m-r["fill"]-FEE(m))*r["shares"]; n+=1
            else:
                tot+=r["set_ps"]*r["shares"]
        for r in rows[len(marks):]: pass
        print("{:>10} {:>8} {:>8.1%} {:>10.2f} {:>+10.2f} {:>7.1%}".format(
              f"<{x:.2f}",n,n/len(marks),tot,tot-hold,tot/cost))


def ambiguity_exit(rows):
    """Exit only the AMBIGUOUS positions, late, and hold the decided ones.

    A flat take-profit fails because it caps the 8+ bps winners that carry the
    book. But settlement risk is not uniform: a position marked near 0.5 with
    seconds to go is the coin flip the oracle decides, while one marked 0.90 is
    already settled in all but name.

    We cannot know the settlement displacement at entry -- but we CAN read the
    mark near the close, and that is a live proxy for the same thing. So: at
    T-`cut` seconds, if the bid sits inside `band`, sell into the bid; otherwise
    hold to settlement. This is the operator's exit idea, aimed only at the
    trades where holding is a lottery ticket.
    """
    print()
    print("=== AMBIGUITY EXIT: sell late only when the mark is still a coin flip ===")
    print("   (hold everything already decided -- that is where the edge lives)")
    print()
    hold = sum(r["set_ps"] * r["shares"] for r in rows)
    cost = sum(r["fill"] * r["shares"] for r in rows)
    hdr = "{:>7} {:>14} {:>8} {:>9} {:>10} {:>10} {:>9}".format(
        "T-cut", "exit band", "n exits", "rate", "total $", "vs hold", "ROI")
    print(hdr)
    print("{:>7} {:>14} {:>8} {:>9} {:>10.2f} {:>10} {:>8.1%}".format(
        "-", "HOLD (real)", 0, "-", hold, "-", hold / cost))
    for cut in (90, 60, 30):
        for lo, hi in ((0.30, 0.70), (0.35, 0.75), (0.40, 0.80), (0.25, 0.85)):
            tot, n_ex = 0.0, 0
            for r in rows:
                t_at = r["exit_s"] - cut
                mark = None
                for ts, b in r["path"]:
                    if ts <= t_at:
                        mark = b
                    else:
                        break
                if mark is not None and lo <= mark <= hi:
                    tot += (mark - r["fill"] - FEE(mark)) * r["shares"]
                    n_ex += 1
                else:
                    tot += r["set_ps"] * r["shares"]
            print("{:>7} {:>14} {:>8} {:>9.1%} {:>10.2f} {:>+10.2f} {:>8.1%}".format(
                f"{cut}s", f"{lo:.2f}-{hi:.2f}", n_ex, n_ex / len(rows),
                tot, tot - hold, tot / cost))


if __name__ == "__main__":
    main()
