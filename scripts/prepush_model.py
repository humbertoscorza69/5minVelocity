"""Can the push be predicted BEFORE it happens?

The causal sweep in push_pm_backtest.py killed the reactive snipe: the book
reprices at the same rate the push detector improves, so every entry from T-40s
to T-10s loses. Reacting to the push is too late by construction -- the maker
sees the same Binance tape and quotes against it.

The paper's Table 8 says the pushed cycles are identifiable from features that
exist LONG before the push:
  * 56% land in Asia/overnight hours (vs 40% of normal cycles)
  * 44% on weekends (vs 27%); only 23% in the deep EU-US overlap (vs 35%)
  * they are QUIETER away from the close, not busier -- lower body-bin flow
  * they concentrate where the outcome is still undecided

Hour, weekday, body flow and margin are all knowable at T-60s, while the ask is
still un-repriced. If a pre-push model can select cycles whose flip probability
exceeds the T-50s ask, that is an edge the reactive detector cannot have --
because it is priced off information the maker has not yet reacted to.

The bar is NOT accuracy. It is beating the posted ask at the moment we buy.

Usage: python scripts/prepush_model.py 2026-07 [train_month]
"""
import datetime as dt
import os
import statistics
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from push_pm_backtest import (  # noqa: E402
    BIN_S, BODY_END, CYCLE_S, FEE, NBINS, RT_LO, PM,
    binance_cycles, load_markets, load_quotes,
)

ENTRY = 250   # T-50s: before the push window, so before the repricing


def build(month):
    """-> list of rows with pre-push features, the T-50s ask, and the outcome."""
    bn = binance_cycles(month)
    days = sorted({f[:10] for f in os.listdir(os.path.join(PM, "best_bid_ask"))
                   if f.startswith(month)})
    rows = []
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
        q = load_quotes(day, tok2ep)
        for ep, m in mk.items():
            b = bn.get(ep)
            if not b or not m["up"] or not m["down"]:
                continue
            fu = q.get(m["up"], {}).get("final")
            fd = q.get(m["down"], {}).get("final")
            if not fu or not fd:
                continue
            if fu[0] >= 0.9 and fd[0] <= 0.1:
                outcome = "up"
            elif fd[0] >= 0.9 and fu[0] <= 0.1:
                outcome = "down"
            else:
                continue
            under = "down" if b["pre_up"] else "up"
            e = q.get(m[under], {}).get(f"e{ENTRY}")
            if not e or not (0.0 < e[1] < 1.0):
                continue
            t = dt.datetime.fromtimestamp(ep, dt.timezone.utc)
            body = [abs(x) for x in b["flow"][:BODY_END]]
            rows.append({
                "ask": e[1],
                "won": outcome == under,
                # --- features knowable at T-50s -------------------------------
                "hour": t.hour,
                "weekend": t.weekday() >= 5,
                "overnight": 0 <= t.hour < 8,          # Asia/overnight, paper's bucket
                "overlap": 13 <= t.hour < 21,          # deep EU-US overlap
                "body_med": statistics.median(body) if body else 0.0,
                "margin": b["margin_bps"],
            })
    return rows


def roi(rows):
    if not rows:
        return 0.0, 0.0, 0
    pnl = sum((1.0 - r["ask"] - FEE(r["ask"])) if r["won"] else -(r["ask"] + FEE(r["ask"]))
              for r in rows)
    cost = sum(r["ask"] + FEE(r["ask"]) for r in rows)
    return (pnl / cost if cost else 0.0), pnl, len(rows)


def report(rows, label):
    r, pnl, n = roi(rows)
    if n < 30:
        print(f"  {label:<38} n={n:<5} (too few)")
        return
    ask = statistics.mean(x["ask"] for x in rows)
    wr = sum(x["won"] for x in rows) / n
    print(f"  {label:<38} n={n:<5} ask={ask:5.3f} WR={wr:6.1%} "
          f"be={ask+FEE(ask):5.3f} net=${pnl:+7.2f} ROI={r:+7.1%}")


def main():
    month = sys.argv[1] if len(sys.argv) > 1 else "2026-07"
    print(f"building {month} ...", flush=True)
    rows = build(month)
    print(f"{len(rows)} cycles with a T-50s quote and a resolution\n")

    print(f"=== buy the underdog at T-50s (ask is NOT yet repriced) ===")
    report(rows, "ALL cycles (no selection)")

    print("\n=== the paper's Table 8 filters, applied BEFORE the push ===")
    med_body = statistics.median(r["body_med"] for r in rows)
    report([r for r in rows if r["overnight"]], "overnight (00-08 UTC)")
    report([r for r in rows if r["weekend"]], "weekend")
    report([r for r in rows if r["overlap"]], "EU-US overlap (13-21 UTC)  [control]")
    report([r for r in rows if r["body_med"] < med_body], "quiet body (below median flow)")
    report([r for r in rows if r["overnight"] or r["weekend"]], "overnight OR weekend")
    report([r for r in rows if (r["overnight"] or r["weekend"]) and r["body_med"] < med_body],
           "thin-liquidity AND quiet body")

    print("\n=== same, restricted to still-undecided cycles (margin < 5 bps) ===")
    nd = [r for r in rows if r["margin"] < 5.0]
    report(nd, "margin<5 : all")
    report([r for r in nd if r["overnight"] or r["weekend"]], "margin<5 : overnight OR weekend")
    report([r for r in nd if (r["overnight"] or r["weekend"]) and r["body_med"] < med_body],
           "margin<5 : thin AND quiet")

    print("\n=== by hour of day (is the ask mispriced at any hour?) ===")
    print(f"  {'hour':>5} {'n':>5} {'ask':>6} {'WR':>7} {'ROI':>8}")
    for h in range(24):
        g = [r for r in rows if r["hour"] == h]
        if len(g) < 40:
            continue
        r_, _, n = roi(g)
        print(f"  {h:>5} {n:>5} {statistics.mean(x['ask'] for x in g):6.3f} "
              f"{sum(x['won'] for x in g)/n:7.1%} {r_:+8.1%}")


if __name__ == "__main__":
    main()
