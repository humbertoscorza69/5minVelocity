"""Classify fetched market metadata: identify up/down series, build token map."""
import json
import re
import collections

META = r"C:\Users\tico_\Fable\5minSnip\data\market_meta.jsonl"
TOKENS_OUT = r"C:\Users\tico_\Fable\5minSnip\data\tokens_updown.json"
SUMMARY_OUT = r"C:\Users\tico_\Fable\5minSnip\data\meta_summary.json"

INTERVALS = {"5m": 300, "15m": 900, "1h": 3600, "4h": 14400, "1d": 86400}

rows = []
with open(META, encoding="utf-8") as f:
    for line in f:
        rows.append(json.loads(line))

by_series = collections.Counter()
errors = 0
non_updown = []
tokens = {}
markets = {}

pat = re.compile(r"^([a-z0-9]+)-updown-([0-9a-z]+)-(\d+)$")

for m in rows:
    if m.get("error"):
        errors += 1
        continue
    slug = m.get("market_slug") or ""
    mt = pat.match(slug)
    if not mt:
        non_updown.append(slug or m.get("question"))
        by_series["NON_UPDOWN"] += 1
        continue
    asset, interval, ts = mt.group(1), mt.group(2), int(mt.group(3))
    series = f"{asset}-{interval}"
    by_series[series] += 1
    iv = INTERVALS.get(interval)
    settle = ts + iv if iv else None
    toks = m.get("tokens") or []
    winner_outcome = None
    for t in toks:
        if t.get("winner"):
            winner_outcome = t.get("outcome")
    markets[m["condition_id"]] = {
        "slug": slug, "asset": asset, "interval": interval,
        "window_start": ts, "settle_ts": settle,
        "question": m.get("question"),
        "winner": winner_outcome, "closed": m.get("closed"),
        "maker_fee": m.get("maker_base_fee"), "taker_fee": m.get("taker_base_fee"),
        "tick": m.get("minimum_tick_size"),
        "tokens": {t["token_id"]: t["outcome"] for t in toks},
    }
    for t in toks:
        tokens[t["token_id"]] = {
            "cid": m["condition_id"], "outcome": t.get("outcome"),
            "winner": bool(t.get("winner")), "asset": asset,
            "interval": interval, "window_start": ts, "settle_ts": settle,
        }

print("fetched:", len(rows), "errors:", errors)
print("series counts:", dict(by_series))
print("sample non-updown:", non_updown[:15])

# winner availability
no_winner = sum(1 for v in markets.values() if v["winner"] is None)
print("updown markets:", len(markets), "without winner flag:", no_winner)

with open(TOKENS_OUT, "w") as f:
    json.dump(tokens, f)
with open(SUMMARY_OUT, "w") as f:
    json.dump({"series": dict(by_series), "markets": markets}, f)
print("saved", len(tokens), "tokens,", len(markets), "updown markets")
