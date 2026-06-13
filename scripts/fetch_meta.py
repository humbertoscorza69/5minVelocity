"""Resolve all condition_ids seen in rest_book via the CLOB API."""
import json
import time
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed

UNIVERSE = r"C:\Users\tico_\Fable\5minSnip\data\universe_rest_book.json"
OUT = r"C:\Users\tico_\Fable\5minSnip\data\market_meta.jsonl"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/126 Safari/537.36",
           "Accept": "application/json"}

with open(UNIVERSE) as f:
    universe = json.load(f)
cids = sorted(universe.keys())

done = set()
try:
    with open(OUT) as f:
        for line in f:
            try:
                done.add(json.loads(line)["condition_id"])
            except Exception:
                pass
except FileNotFoundError:
    pass
todo = [c for c in cids if c not in done]
print(f"{len(cids)} total, {len(done)} done, {len(todo)} to fetch")

KEEP = ["condition_id", "question", "market_slug", "end_date_iso",
        "accepting_order_timestamp", "minimum_tick_size", "minimum_order_size",
        "maker_base_fee", "taker_base_fee", "closed", "tokens", "tags",
        "neg_risk", "is_50_50_outcome"]

def fetch(cid):
    url = "https://clob.polymarket.com/markets/" + cid
    for attempt in range(5):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=30) as r:
                data = json.load(r)
            return {k: data.get(k) for k in KEEP}
        except urllib.error.HTTPError as e:
            if e.code in (404, 400):
                return {"condition_id": cid, "error": e.code}
            time.sleep(2 ** attempt)
        except Exception:
            time.sleep(2 ** attempt)
    return {"condition_id": cid, "error": "failed"}

n = 0
t0 = time.time()
with open(OUT, "a", encoding="utf-8") as out:
    with ThreadPoolExecutor(max_workers=12) as ex:
        futs = {ex.submit(fetch, c): c for c in todo}
        for fut in as_completed(futs):
            rec = fut.result()
            out.write(json.dumps(rec) + "\n")
            n += 1
            if n % 200 == 0:
                out.flush()
                print(f"{n}/{len(todo)} ({time.time()-t0:.0f}s)", flush=True)
print(f"done {n} in {time.time()-t0:.0f}s")
