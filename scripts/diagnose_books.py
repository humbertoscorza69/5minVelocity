"""Disentangle reconstruction error vs timing jitter.

A) For each REST snapshot: compare snapshot best vs the EVENT-EMBEDDED best of
   the last event <= snapshot ts (tests WS/REST consistency, no reconstruction).
B) My reconstructed best vs event-embedded best at every event (tol=1e-3),
   per token; also as function of position in stream.
C) Like the original validation but allowing a +/-2s alignment window.
"""
import json
import random

import duckdb
import numpy as np
import pandas as pd

DATA = "C:/Users/tico_/Fable/5minSnip/data"
rb = pd.read_parquet(DATA + "/restbook.parquet")

random.seed(7)
sample = random.sample(rb.tok.unique().tolist(), 40)
con = duckdb.connect()
con.execute("SET threads=6")

a_match = []
b_rates = []
c_match = []
for ti in sample:
    ev = con.execute(f"""
        SELECT ts, lvl_price, lvl_size, side, bb, ba FROM
        read_parquet('{DATA}/events_sorted.parquet') WHERE tok={ti} ORDER BY ts
    """).df()
    if len(ev) < 10:
        continue
    snaps = rb[rb.tok == ti].sort_values("ts")
    ts_arr = ev.ts.values
    # --- A: WS embedded best vs REST best ---
    for s in snaps.itertuples(index=False):
        j = np.searchsorted(ts_arr, s.ts, side="right") - 1
        if j < 0:
            continue
        ebb, eba = ev.bb.iloc[j], ev.ba.iloc[j]
        ok = (abs(ebb - s.bp0) < 5e-4) and (abs(eba - s.ap0) < 5e-4)
        a_match.append(bool(ok))
    # --- B: reconstruction vs embedded best each event ---
    bid = np.zeros(1001)
    ask = np.zeros(1001)
    rbi, rai = -1, 1005
    okc = 0
    n = 0
    match_seq = []
    lp = (ev.lvl_price.values * 1000).round().astype(int)
    ls = ev.lvl_size.values
    sd = ev.side.values
    ebb = ev.bb.values
    eba = ev.ba.values
    for i in range(len(ev)):
        p, s_, si = lp[i], ls[i], sd[i]
        if si == 1:
            if s_ > 0:
                bid[p] = s_
                rbi = max(rbi, p)
            else:
                bid[p] = 0
                if p == rbi:
                    while rbi >= 0 and bid[rbi] == 0:
                        rbi -= 1
        else:
            if s_ > 0:
                ask[p] = s_
                rai = min(rai, p)
            else:
                ask[p] = 0
                if p == rai:
                    while rai <= 1000 and ask[rai] == 0:
                        rai += 1
                    if rai > 1000:
                        rai = 1005
        ok = True
        if ebb[i] == ebb[i] and rbi >= 0:
            ok &= abs(rbi / 1000 - ebb[i]) < 5e-4
        elif rbi < 0 and ebb[i] == ebb[i]:
            ok = False
        if eba[i] == eba[i] and rai <= 1000:
            ok &= abs(rai / 1000 - eba[i]) < 5e-4
        elif rai > 1000 and eba[i] == eba[i]:
            ok = False
        okc += ok
        n += 1
        match_seq.append(ok)
    b_rates.append({"tok": ti, "n": n, "match": okc / n,
                    "match_first1k": float(np.mean(match_seq[:1000])),
                    "match_last1k": float(np.mean(match_seq[-1000:]))})
print("A) WS-embedded vs REST snapshot best match:",
      float(np.mean(a_match)), f"(n={len(a_match)})")
bdf = pd.DataFrame(b_rates)
print("B) reconstruction vs embedded best, per-token match rates:")
print(bdf.describe().loc[["mean", "50%", "min", "max"]].to_string())
print(bdf.sort_values("match").head(8).to_string())
