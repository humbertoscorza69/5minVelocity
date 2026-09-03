"""How large can a taker order get before slippage eats the edge?

Usage: python scripts/depth_capacity_study.py <day> [<day> ...]

Walks the recorder's full-depth `book` snapshots and, for each one, computes the
VWAP an aggressive BUY would actually pay for a range of order sizes — i.e. what it
costs to sweep past the top of book. Reported in cents of slippage versus the touch,
and against the strategy's measured edge so the answer is "how big before it hurts",
not just "how deep is the book".

Also broken out by UTC hour, because Asia and New York are not the same book.
"""
import io
import json
import os
import sys
from collections import defaultdict

import numpy as np
import pandas as pd
import zstandard as zstd

BASE = r"D:\polycrypto\live_l2"
SIZES = [5, 10, 25, 50, 100, 200, 400, 800]


def _open(path):
    if os.path.exists(path):
        return io.TextIOWrapper(zstd.ZstdDecompressor().stream_reader(open(path, "rb")),
                                encoding="utf8", errors="replace")
    raw = path[:-4]
    return open(raw, encoding="utf8", errors="replace") if os.path.exists(raw) else None


def sweep_vwap(asks, size):
    """VWAP paid to buy `size` shares sweeping up the ask ladder. None if book too thin."""
    filled = 0.0
    cost = 0.0
    for p, s in asks:
        take = min(s, size - filled)
        cost += take * p
        filled += take
        if filled >= size - 1e-9:
            return cost / size
    return None


def main(days):
    rows = []
    for day in days:
        fh = _open(rf"{BASE}\polymarket\book\{day}.jsonl.zst")
        if fh is None:
            print(f"{day}: no book file"); continue
        n = 0
        with fh:
            for line in fh:
                if '"asset_id"' not in line:
                    continue
                try:
                    r = json.loads(line); p = r["payload"]
                    a = sorted(((float(x["price"]), float(x["size"])) for x in p.get("asks", [])),
                               key=lambda t: t[0])
                    if not a:
                        continue
                    touch, touch_sz = a[0]
                    if not (0.05 < touch < 0.95):     # the band we actually trade
                        continue
                    hr = pd.Timestamp(r["received_at"]).hour
                    rec = dict(day=day, hour=hr, touch=touch, touch_sz=touch_sz,
                               depth3=sum(s for pp, s in a if pp <= touch + 0.03))
                    for S in SIZES:
                        v = sweep_vwap(a, S)
                        rec[f"slip{S}"] = (v - touch) * 100 if v is not None else np.nan
                    rows.append(rec)
                    n += 1
                except Exception:
                    continue
        print(f"{day}: {n:,} snapshots", flush=True)
    d = pd.DataFrame(rows)
    if d.empty:
        print("no data"); return

    print(f"\n=== TOP-OF-BOOK DEPTH (ask side, {len(d):,} snapshots) ===")
    print(f"  shares AT the touch : p10 {d.touch_sz.quantile(.1):>7.0f}  p50 {d.touch_sz.median():>7.0f}"
          f"  p90 {d.touch_sz.quantile(.9):>7.0f}")
    print(f"  shares within 3c    : p10 {d.depth3.quantile(.1):>7.0f}  p50 {d.depth3.median():>7.0f}"
          f"  p90 {d.depth3.quantile(.9):>7.0f}")

    print(f"\n=== SLIPPAGE vs ORDER SIZE (cents above the touch, aggressive buy) ===")
    print(f"{'shares':>8}{'$ @0.64':>10}{'p50':>8}{'p75':>8}{'p90':>8}{'unfillable':>12}")
    for S in SIZES:
        c = d[f"slip{S}"]
        print(f"{S:>8}{S*0.64:>10.0f}{c.median():>8.2f}{c.quantile(.75):>8.2f}"
              f"{c.quantile(.9):>8.2f}{c.isna().mean()*100:>11.1f}%")

    print(f"\n=== WHAT IT COSTS AS A SHARE OF THE EDGE ===")
    print("  measured EV/$1 = +0.0753, i.e. ~4.8c of edge per share at a 0.64 ask")
    for S in SIZES:
        med = d[f"slip{S}"].median()
        if np.isfinite(med):
            print(f"    {S:>4} sh (${S*0.64:>4.0f}): median slip {med:>5.2f}c = {med/4.8*100:>5.1f}% of edge")

    print(f"\n=== BY UTC HOUR (median slip for a 100-share / ~$64 order) ===")
    h = d.groupby("hour").agg(n=("touch", "size"), touch_sz=("touch_sz", "median"),
                              slip100=("slip100", "median"), slip400=("slip400", "median"))
    print(h.round(2).to_string())
    return d


if __name__ == "__main__":
    main(sys.argv[1:])
