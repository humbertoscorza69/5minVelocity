"""Fetch historical trades for June 5m markets from Polymarket data-api.
Output: data/trades_june5m.parquet  (conditionId, asset, side, size, price, ts)
"""
import json
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

DATA = r"C:\Users\tico_\Fable\5minSnip\data"
H = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}

meta = json.load(open(DATA + r"\meta_summary.json"))["markets"]
cids = [c for c, m in meta.items() if m["interval"] == "5m"]
print("June 5m markets:", len(cids))

def get(url):
    for a in range(4):
        try:
            return json.load(urllib.request.urlopen(
                urllib.request.Request(url, headers=H), timeout=30))
        except Exception:
            time.sleep(1.5 * (a + 1))
    return []

def fetch_market(cid):
    out = []
    off = 0
    while True:
        d = get(f"https://data-api.polymarket.com/trades?market={cid}"
                f"&limit=1000&offset={off}")
        if not d:
            break
        for t in d:
            out.append((t["conditionId"], t["asset"], t["side"],
                        float(t["size"]), float(t["price"]), int(t["timestamp"])))
        if len(d) < 1000:
            break
        off += 1000
        if off > 20000:
            break
    return out

rows = []
t0 = time.time()
done = 0
with ThreadPoolExecutor(max_workers=12) as ex:
    futs = {ex.submit(fetch_market, c): c for c in cids}
    for f in as_completed(futs):
        rows.extend(f.result())
        done += 1
        if done % 200 == 0:
            print(f"{done}/{len(cids)} markets, {len(rows):,} trades, "
                  f"{time.time()-t0:.0f}s", flush=True)

df = pd.DataFrame(rows, columns=["cid", "asset", "side", "size", "price", "ts"])
df.to_parquet(DATA + r"\trades_june5m.parquet")
print("DONE trades:", len(df), "markets:", df.cid.nunique(), f"{time.time()-t0:.0f}s")
