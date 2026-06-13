"""Filter price_change files down to up/down-market tokens -> parquet.

One process per day-file (run with the day file as argv[1]).
Emits one row per price_changes entry for tracked tokens:
  tok(i32 idx), ts(i64 exchange ms), recv(i64 recorder ms),
  lvl_price(f32), lvl_size(f32), side(i8 1=BUY/0=SELL), bb(f32), ba(f32)
"""
import io
import json
import os
import sys
import time
from datetime import datetime, timezone

import orjson
import pyarrow as pa
import pyarrow.parquet as pq
import zstandard as zstd

TOKENS = r"C:\Users\tico_\Fable\5minSnip\data\tokens_updown.json"
OUTDIR = r"C:\Users\tico_\Fable\5minSnip\data\filtered"
os.makedirs(OUTDIR, exist_ok=True)

src = sys.argv[1]
day = os.path.basename(src).split(".")[0]
dest = os.path.join(OUTDIR, f"{day}.parquet")
idx_dest = os.path.join(OUTDIR, "token_index.json")

with open(TOKENS) as f:
    tokmeta = json.load(f)
tokidx = {t: i for i, t in enumerate(sorted(tokmeta))}
if not os.path.exists(idx_dest):
    with open(idx_dest, "w") as f:
        json.dump({t: i for t, i in tokidx.items()}, f)

schema = pa.schema([
    ("tok", pa.int32()), ("ts", pa.int64()), ("recv", pa.int64()),
    ("lvl_price", pa.float32()), ("lvl_size", pa.float32()),
    ("side", pa.int8()), ("bb", pa.float32()), ("ba", pa.float32()),
])

def opener(path):
    if path.endswith(".zst"):
        dctx = zstd.ZstdDecompressor()
        fh = open(path, "rb")
        return io.TextIOWrapper(io.BufferedReader(dctx.stream_reader(fh), 32 * 1024 * 1024), encoding="utf-8")
    return open(path, encoding="utf-8", buffering=32 * 1024 * 1024)

cols = {k: [] for k in ("tok", "ts", "recv", "lvl_price", "lvl_size", "side", "bb", "ba")}
writer = pq.ParquetWriter(dest, schema, compression="zstd")
FLUSH = 8_000_000

def flush():
    global cols
    if not cols["tok"]:
        return
    table = pa.table({k: pa.array(cols[k], schema.field(k).type) for k in cols})
    writer.write_table(table)
    cols = {k: [] for k in cols}

t0 = time.time()
n_lines = 0
n_rows = 0
bad = 0
with opener(src) as r:
    for line in r:
        n_lines += 1
        if n_lines % 5_000_000 == 0:
            print(f"{day}: {n_lines/1e6:.0f}M lines, {n_rows/1e6:.1f}M rows, {time.time()-t0:.0f}s", flush=True)
        try:
            rec = orjson.loads(line)
        except Exception:
            bad += 1
            continue
        p = rec.get("payload")
        if not p:
            continue
        changes = p.get("price_changes")
        if not changes:
            continue
        ts = int(p["timestamp"])
        recv = rec.get("received_at")
        # "2026-06-04T20:56:02.380209+00:00" -> epoch ms
        recv_ms = int(datetime.fromisoformat(recv).timestamp() * 1000)
        for c in changes:
            ti = tokidx.get(c["asset_id"])
            if ti is None:
                continue
            try:
                cols["tok"].append(ti)
                cols["ts"].append(ts)
                cols["recv"].append(recv_ms)
                cols["lvl_price"].append(float(c["price"]))
                cols["lvl_size"].append(float(c["size"]))
                cols["side"].append(1 if c.get("side") == "BUY" else 0)
                cols["bb"].append(float(c.get("best_bid") or "nan"))
                cols["ba"].append(float(c.get("best_ask") or "nan"))
                n_rows += 1
            except Exception:
                bad += 1
        if len(cols["tok"]) >= FLUSH:
            flush()
flush()
writer.close()
print(f"{day} DONE: {n_lines:,} lines -> {n_rows:,} rows, bad={bad}, {time.time()-t0:.0f}s", flush=True)
