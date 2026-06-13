"""Collect unique condition_ids / token_ids and snapshot stats from rest_book files."""
import glob
import io
import json
import os
from collections import defaultdict

import orjson
import zstandard as zstd

RAW = r"C:\Users\tico_\Fable\5minSnip\data\raw\rest_book"
OUT = r"C:\Users\tico_\Fable\5minSnip\data\universe_rest_book.json"

markets = defaultdict(lambda: {"tokens": set(), "n_snaps": 0,
                               "first": None, "last": None})

def lines(path):
    if path.endswith(".zst"):
        dctx = zstd.ZstdDecompressor()
        with open(path, "rb") as fh:
            with io.TextIOWrapper(dctx.stream_reader(fh), encoding="utf-8") as r:
                yield from r
    else:
        with open(path, encoding="utf-8") as fh:
            yield from fh

total = 0
for path in sorted(glob.glob(os.path.join(RAW, "*"))):
    for line in lines(path):
        line = line.strip()
        if not line:
            continue
        try:
            rec = orjson.loads(line)
        except Exception:
            continue  # truncated tail line
        total += 1
        b = rec.get("book") or {}
        m = b.get("market")
        if not m:
            continue
        e = markets[m]
        e["tokens"].add(b.get("asset_id"))
        e["n_snaps"] += 1
        ts = rec.get("received_at")
        if e["first"] is None or ts < e["first"]:
            e["first"] = ts
        if e["last"] is None or ts > e["last"]:
            e["last"] = ts
    print(path, "->", total, "lines cum,", len(markets), "markets", flush=True)

out = {m: {"tokens": sorted(v["tokens"]), "n_snaps": v["n_snaps"],
           "first": v["first"], "last": v["last"]}
       for m, v in markets.items()}
with open(OUT, "w") as f:
    json.dump(out, f)
print("markets:", len(out), "total snapshot lines:", total)
