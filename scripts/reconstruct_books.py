"""Lean checkpoint extraction from the (tok, ts)-sorted event stream.

Depth reconstruction from price_change deltas was shown invalid (trade
executions arrive on the lost 'book' channel), so this walk records only:
  - reported best bid/ask (exchange-computed, embedded in every event)
  - quote age at checkpoint
  - future extremes (min reported ask / max reported bid) from checkpoint
    until settlement  -> maker-fill proxy & reversal detection
Depth analysis is done separately from REST snapshots (analyze_book.py).
"""
import json
import os
import time

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

DATA = r"C:\Users\tico_\Fable\5minSnip\data"
SORTED = os.path.join(DATA, "events_sorted.parquet")
TOKENS = os.path.join(DATA, "tokens_updown.json")
TOKIDX = os.path.join(DATA, "filtered", "token_index.json")
OUT = os.path.join(DATA, "checkpoints.parquet")

with open(TOKENS) as f:
    tokmeta = json.load(f)
with open(TOKIDX) as f:
    tokidx = json.load(f)
idx2meta = {}
for t, i in tokidx.items():
    m = tokmeta.get(t)
    if m:
        idx2meta[i] = (m["asset"], m["interval"], int(m["settle_ts"]),
                       1 if m["winner"] else 0, m["outcome"])

CHK_5M = [300, 240, 180, 120, 90, 60, 45, 30, 15, 10, 5, 2, 1, 0]
CHK_15M = [900, 600, 450] + CHK_5M

COLS = ["tok", "off", "bb_rep", "ba_rep", "age_ms", "n_events",
        "min_ask_after", "max_bid_after"]
out = {k: [] for k in COLS}

cur_tok = -1
have_meta = False
settle_ms = 0
chks = []
chk_i = 0
last_bb = np.nan
last_ba = np.nan
last_ev_ts = 0
nev = 0
total_ev = 0
pending = []      # [tok, off, bb, ba, age, nev]
trk_min = []
trk_max = []

def finish_token():
    global chk_i
    if not have_meta:
        return
    while chk_i < len(chks):
        off = chks[chk_i]
        ts_chk = settle_ms - off * 1000
        pending.append([cur_tok, off, last_bb, last_ba,
                        int(ts_chk - last_ev_ts) if last_ev_ts else -1, nev])
        trk_min.append(np.nan)
        trk_max.append(np.nan)
        chk_i += 1
    for row, mn, mx in zip(pending, trk_min, trk_max):
        for k, v in zip(COLS, row + [mn, mx]):
            out[k].append(v)
    pending.clear()
    trk_min.clear()
    trk_max.clear()

t0 = time.time()
pf = pq.ParquetFile(SORTED)
for batch in pf.iter_batches(batch_size=2_000_000,
                             columns=["tok", "ts", "bb", "ba"]):
    tok_a = batch.column("tok").to_numpy()
    ts_a = batch.column("ts").to_numpy()
    bb_a = batch.column("bb").to_numpy().astype(np.float64)
    ba_a = batch.column("ba").to_numpy().astype(np.float64)
    for i in range(len(tok_a)):
        ti = tok_a[i]
        if ti != cur_tok:
            finish_token()
            cur_tok = ti
            m = idx2meta.get(ti)
            have_meta = m is not None
            nev = 0
            last_bb = np.nan
            last_ba = np.nan
            last_ev_ts = 0
            chk_i = 0
            if have_meta:
                settle_ms = m[2] * 1000
                chks = sorted(CHK_15M if m[1] == "15m" else CHK_5M, reverse=True)
            else:
                chks = []
        if not have_meta:
            continue
        t = ts_a[i]
        while chk_i < len(chks) and t > settle_ms - chks[chk_i] * 1000:
            off = chks[chk_i]
            ts_chk = settle_ms - off * 1000
            pending.append([cur_tok, off, last_bb, last_ba,
                            int(ts_chk - last_ev_ts) if last_ev_ts else -1, nev])
            trk_min.append(np.nan)
            trk_max.append(np.nan)
            chk_i += 1
        nev += 1
        total_ev += 1
        last_ev_ts = t
        b = bb_a[i]
        a = ba_a[i]
        last_bb = b
        last_ba = a
        if trk_min and t <= settle_ms:
            if a == a:
                for j in range(len(trk_min)):
                    if not trk_min[j] <= a:
                        trk_min[j] = a
            if b == b:
                for j in range(len(trk_max)):
                    if not trk_max[j] >= b:
                        trk_max[j] = b
    if (total_ev // 2_000_000) % 10 == 0:
        print(f"{total_ev/1e6:.0f}M events, {time.time()-t0:.0f}s", flush=True)
finish_token()
print(f"walk done {time.time()-t0:.0f}s, events={total_ev:,}")

tok_arr = out["tok"]
assets, intervals, settles, winners, outcomes = [], [], [], [], []
for ti in tok_arr:
    a, iv, st, w, oc = idx2meta[ti]
    assets.append(a); intervals.append(iv); settles.append(st)
    winners.append(w); outcomes.append(oc)

table = {
    "tok": pa.array([int(x) for x in tok_arr], pa.int32()),
    "asset": pa.array(assets, pa.string()),
    "interval": pa.array(intervals, pa.string()),
    "settle_ts": pa.array(settles, pa.int64()),
    "winner": pa.array(winners, pa.int8()),
    "outcome": pa.array(outcomes, pa.string()),
}
for k in COLS:
    if k == "tok":
        continue
    if k in ("off", "age_ms", "n_events"):
        table[k] = pa.array([int(x) for x in out[k]], pa.int64())
    else:
        table[k] = pa.array([float(x) for x in out[k]], pa.float64())
pq.write_table(pa.table(table), OUT, compression="zstd")
print("checkpoint rows:", len(tok_arr))
