import json, io, collections
import zstandard as zstd

uni = set(json.load(open(r"C:\Users\tico_\Fable\5minSnip\data\universe_rest_book.json")))
f = r"C:\Users\tico_\Fable\5minSnip\data\raw\price_change\2026-06-04.jsonl.zst"
dctx = zstd.ZstdDecompressor()
unknown = collections.Counter()
total = 0
with open(f, "rb") as fh:
    r = io.TextIOWrapper(io.BufferedReader(dctx.stream_reader(fh), 16 * 1024 * 1024), encoding="utf-8")
    for line in r:
        total += 1
        i = line.find('"market":"')
        if i < 0:
            continue
        cid = line[i + 10:i + 76]
        if cid not in uni:
            unknown[cid] += 1
print("total lines:", total, "unknown cids:", len(unknown), "unknown events:", sum(unknown.values()))
for c, n in unknown.most_common(5):
    print(c, n)
