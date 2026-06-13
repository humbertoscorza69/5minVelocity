"""Validate delta-based book reconstruction against independent REST snapshots.

Picks N random tokens, replays their event stream, and at each rest_book
snapshot time compares top-of-book and depth (with f32-aware tolerance).
"""
import json
import random

import duckdb
import numpy as np
import pandas as pd

DATA = "C:/Users/tico_/Fable/5minSnip/data"
N_TOKENS = 60
TOL = 5e-4  # half a tick of slack for f32 storage

rb = pd.read_parquet(DATA + "/restbook.parquet")
with open(DATA + "/filtered/token_index.json") as f:
    tokidx = json.load(f)

random.seed(7)
cands = rb.tok.unique().tolist()
sample = random.sample(cands, min(N_TOKENS, len(cands)))

con = duckdb.connect()
con.execute("SET threads=8")

results = []
for ti in sample:
    ev = con.execute(f"""
        SELECT ts, lvl_price, lvl_size, side FROM
        read_parquet('{DATA}/events_sorted.parquet') WHERE tok={ti} ORDER BY ts
    """).df()
    if not len(ev):
        continue
    snaps = rb[rb.tok == ti].sort_values("ts")
    bid = {}
    ask = {}
    j = 0
    evts = ev.itertuples(index=False)
    rows = list(ev.itertuples(index=False))
    for s in snaps.itertuples(index=False):
        # replay events up to snapshot exchange ts
        while j < len(rows) and rows[j].ts <= s.ts:
            r = rows[j]
            p = round(float(r.lvl_price), 3)
            d = bid if r.side == 1 else ask
            if r.lvl_size <= 0:
                d.pop(p, None)
            else:
                d[p] = float(r.lvl_size)
            j += 1
        if j == 0:
            continue
        rbb = max(bid) if bid else np.nan
        rba = min(ask) if ask else np.nan
        # snapshot top levels
        sbb, sba = s.bp0, s.ap0
        ok_bb = (np.isnan(rbb) and np.isnan(sbb)) or abs(rbb - sbb) < TOL
        ok_ba = (np.isnan(rba) and np.isnan(sba)) or abs(rba - sba) < TOL
        # depth check at best (sizes)
        sz_ok = True
        if ok_bb and not np.isnan(sbb):
            sz_ok &= abs(bid.get(round(sbb, 3), 0) - s.bs0) < max(1.0, 0.01 * s.bs0)
        if ok_ba and not np.isnan(sba):
            sz_ok &= abs(ask.get(round(sba, 3), 0) - s.as0) < max(1.0, 0.01 * s.as0)
        results.append({"tok": ti, "ts": s.ts, "ok_bb": ok_bb, "ok_ba": ok_ba,
                        "ok_sz": bool(sz_ok),
                        "n_ev_applied": j})

r = pd.DataFrame(results)
r.to_csv(DATA + "/book_validation.csv", index=False)
print("snapshot comparisons:", len(r))
print("best-bid match rate:", r.ok_bb.mean())
print("best-ask match rate:", r.ok_ba.mean())
print("best-size match rate:", r.ok_sz.mean())
print("all-match:", (r.ok_bb & r.ok_ba & r.ok_sz).mean())
