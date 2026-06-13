"""Convert rest_book snapshots (full depth) to parquet for updown tokens.

Per snapshot row: tok idx, recv ms, exchange ts ms, top-10 bid/ask price+size,
plus aggregate depth (total bid/ask notional, share count).
"""
import glob
import io
import json
import os
from datetime import datetime

import orjson
import pyarrow as pa
import pyarrow.parquet as pq
import zstandard as zstd

RAW = r"C:\Users\tico_\Fable\5minSnip\data\raw\rest_book"
TOKENS = r"C:\Users\tico_\Fable\5minSnip\data\filtered\token_index.json"
OUT = r"C:\Users\tico_\Fable\5minSnip\data\restbook.parquet"

with open(TOKENS) as f:
    tokidx = json.load(f)

N = 10
fields = [("tok", pa.int32()), ("recv", pa.int64()), ("ts", pa.int64()),
          ("bid_total_sh", pa.float32()), ("ask_total_sh", pa.float32()),
          ("bid_total_usd", pa.float32()), ("ask_total_usd", pa.float32())]
for i in range(N):
    fields += [(f"bp{i}", pa.float32()), (f"bs{i}", pa.float32())]
for i in range(N):
    fields += [(f"ap{i}", pa.float32()), (f"as{i}", pa.float32())]
schema = pa.schema(fields)

def lines(path):
    if path.endswith(".zst"):
        dctx = zstd.ZstdDecompressor()
        with open(path, "rb") as fh:
            with io.TextIOWrapper(io.BufferedReader(dctx.stream_reader(fh), 8 * 1024 * 1024), encoding="utf-8") as r:
                yield from r
    else:
        with open(path, encoding="utf-8") as fh:
            yield from fh

cols = {f[0]: [] for f in fields}
n = 0
skipped = 0
for path in sorted(glob.glob(os.path.join(RAW, "*"))):
    for line in lines(path):
        line = line.strip()
        if not line:
            continue
        try:
            rec = orjson.loads(line)
        except Exception:
            skipped += 1
            continue
        b = rec.get("book") or {}
        ti = tokidx.get(b.get("asset_id", ""))
        if ti is None:
            skipped += 1
            continue
        bids = b.get("bids") or []   # ascending price (observed)
        asks = b.get("asks") or []
        # normalize: bids sorted desc by price, asks asc
        bids = sorted(((float(x["price"]), float(x["size"])) for x in bids), reverse=True)
        asks = sorted(((float(x["price"]), float(x["size"])) for x in asks))
        cols["tok"].append(ti)
        cols["recv"].append(int(datetime.fromisoformat(rec["received_at"]).timestamp() * 1000))
        cols["ts"].append(int(b.get("timestamp") or 0))
        cols["bid_total_sh"].append(sum(s for _, s in bids))
        cols["ask_total_sh"].append(sum(s for _, s in asks))
        cols["bid_total_usd"].append(sum(p * s for p, s in bids))
        cols["ask_total_usd"].append(sum(p * s for p, s in asks))
        for i in range(N):
            cols[f"bp{i}"].append(bids[i][0] if i < len(bids) else float("nan"))
            cols[f"bs{i}"].append(bids[i][1] if i < len(bids) else float("nan"))
        for i in range(N):
            cols[f"ap{i}"].append(asks[i][0] if i < len(asks) else float("nan"))
            cols[f"as{i}"].append(asks[i][1] if i < len(asks) else float("nan"))
        n += 1
    print(path, n, flush=True)

table = pa.table({k: pa.array(v, schema.field(k).type) for k, v in cols.items()})
pq.write_table(table, OUT, compression="zstd")
print("rows:", n, "skipped:", skipped)
