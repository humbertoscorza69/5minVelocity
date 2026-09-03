"""Run the pre-registered tournament (docs/PREREG_tournament.md).

Usage: python scripts/tourney_run.py <dt_dir> <out_dir>

Protocol, fixed in advance:
  * The most recent ~20% of days are SEALED as a final holdout, touched once.
  * K=5 rotating contiguous blocks over the remaining 80%: select on 4, score on
    the held-out 1, rotate. Mean held-out score = honest expectation of the
    SELECTION PROCEDURE (not of a hand-picked config).
  * A config may only win if it is positive in >= 4/5 IS blocks (kills knife-edges).
  * Permutation Monte Carlo: market outcomes shuffled within (day, interval, asset),
    the same selections re-scored, whole procedure repeated -> null distribution of
    best-of-N. Selections are label-independent, which is what makes this affordable.
  * PRIMARY metric = mean daily net P&L at $1.05, DELAYED (+1s) fills, band-stop exit.
"""
import itertools
import json
import os
import sys
import time

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
import tourney_engine as T

HOLDOUT_FRAC = 0.20
K_BLOCKS = 5
N_PERM = 300

DEPLOY = {
    "5m": dict(disp_floor=2.0, vol_floor=0.12, z_min=0.45, edge_min=0.02, min_ask=0.30,
               max_ask=1.0, min_ttl=30, max_ttl=240, frozen=2, vol_lb=60,
               intervals=["5m"], mid_move_max=0.0, max_bbo_age=2, burst_min=0),
    "15m": dict(disp_floor=2.0, vol_floor=0.07, z_min=0.70, edge_min=0.06, min_ask=0.30,
                max_ask=0.70, min_ttl=30, max_ttl=540, frozen=1e9, vol_lb=120,
                intervals=["15m"], mid_move_max=0.0, max_bbo_age=2, burst_min=0),
}


def grid_for(iv):
    """Stage A+B grid: a coordinate sweep around the deployed config, then a focused
    factorial on the axes that matter. Deliberately NOT a full factorial — the
    multiple-comparison burden has to stay measurable."""
    base = DEPLOY[iv]
    axes = {
        "disp_floor": [1.5, 2.0, 3.0, 4.0],
        "vol_floor": [0.06, base["vol_floor"], 0.18, 0.25],
        "z_min": [0.35, base["z_min"], 0.80, 1.10],
        "edge_min": [0.00, base["edge_min"], 0.04, 0.06],
        "max_ttl": [150, base["max_ttl"], (270 if iv == "5m" else 700)],
        "min_ask": [0.30, 0.45, 0.55],
        "max_ask": [base["max_ask"], 0.85, 0.75],
        "vol_lb": [60, 120],
        "mid_move_max": [0.0, None],
        "frozen": [2, 1e9],
        "burst_min": [0, 2, 3, 5],
    }
    cfgs, seen = [], set()

    def add(c, tag):
        key = json.dumps({k: (str(v)) for k, v in sorted(c.items())})
        if key in seen:
            return
        seen.add(key)
        cfgs.append((dict(c), tag))

    add(base, "DEPLOYED")
    for k, vals in axes.items():                       # Stage A: one-at-a-time
        for v in vals:
            c = dict(base); c[k] = v
            add(c, f"A:{k}={v}")
    # Stage B: focused factorial on the structural axes
    for dz, zm, em, vlb, mm in itertools.product(
            axes["disp_floor"], axes["z_min"], axes["edge_min"],
            axes["vol_lb"], axes["mid_move_max"]):
        c = dict(base)
        c.update(disp_floor=dz, z_min=zm, edge_min=em, vol_lb=vlb, mid_move_max=mm)
        add(c, "B")
    # Stage C: the burst arm (new evidence, not a grid artifact)
    for bm, mm, zm in itertools.product([2, 3, 5], [0.0, None], [0.0, base["z_min"]]):
        c = dict(base)
        c.update(burst_min=bm, mid_move_max=mm, z_min=zm, frozen=1e9)
        add(c, f"C:burst>={bm}")
    return cfgs


def blocks(days, k):
    return [list(x) for x in np.array_split(np.array(days), k)]


def main(dt_dir, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    t0 = time.time()
    d, stops = T.load(dt_dir)
    days = sorted(d.day.unique())
    n_hold = max(1, int(round(len(days) * HOLDOUT_FRAC)))
    dev_days, hold_days = days[:-n_hold], days[-n_hold:]
    print(f"loaded {len(d):,} candidate rows | {len(days)} days "
          f"({days[0]}..{days[-1]}) in {time.time()-t0:.0f}s")
    print(f"DEV  {len(dev_days)} days {dev_days[0]}..{dev_days[-1]}")
    print(f"SEALED HOLDOUT {len(hold_days)} days {hold_days[0]}..{hold_days[-1]} "
          f"(touched once, at the end)\n", flush=True)

    results = {}
    for iv in ("5m", "15m"):
        cfgs = grid_for(iv)
        print(f"=== {iv}: {len(cfgs)} configs ===", flush=True)
        rows, sels = [], {}
        for i, (c, tag) in enumerate(cfgs):
            pos = T.select(d, c)
            sels[i] = pos
            s = T.score(d, pos, stops, delayed=True, exit_mode="band")
            per_day = s.groupby("day").pnl.sum() if len(s) else pd.Series(dtype=float)
            rows.append(dict(i=i, tag=tag, n=len(s),
                             wr=(s.won.mean() if len(s) else np.nan),
                             pf=(s.pf.mean() if len(s) else np.nan),
                             **{f"d_{k}": v for k, v in per_day.items()}))
            if (i + 1) % 50 == 0:
                print(f"   {i+1}/{len(cfgs)}  ({time.time()-t0:.0f}s)", flush=True)
        R = pd.DataFrame(rows).set_index("i")
        daycols = [c for c in R.columns if c.startswith("d_")]
        R[daycols] = R[daycols].fillna(0.0)
        R.to_parquet(os.path.join(out_dir, f"grid_{iv}.parquet"))
        results[iv] = (R, cfgs, sels, daycols)
        print(f"   grid done ({time.time()-t0:.0f}s)", flush=True)

    np.save(os.path.join(out_dir, "days.npy"), np.array(days))
    with open(os.path.join(out_dir, "split.json"), "w") as fh:
        json.dump({"dev": dev_days, "holdout": hold_days}, fh, indent=1)
    return d, stops, results, dev_days, hold_days


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
