"""Dataset audit: market coverage by day/series, winner balance, restbook cadence."""
import json
import collections
from datetime import datetime, timezone

import duckdb

DATA = r"C:\Users\tico_\Fable\5minSnip\data"

with open(DATA + r"\meta_summary.json") as f:
    summ = json.load(f)
markets = summ["markets"]

# coverage by UTC day and series
cov = collections.Counter()
winners = collections.Counter()
for cid, m in markets.items():
    day = datetime.fromtimestamp(m["window_start"], tz=timezone.utc).strftime("%m-%d")
    series = f'{m["asset"]}-{m["interval"]}'
    cov[(day, series)] += 1
    winners[(series, m["winner"])] += 1

days = sorted({d for d, _ in cov})
series = sorted({s for _, s in cov})
print("Markets per UTC day:")
print("day   " + "".join(f"{s:>10}" for s in series))
for d in days:
    print(f"{d}  " + "".join(f"{cov.get((d, s), 0):>10}" for s in series))

print("\nWinner balance:")
for s in series:
    up = winners.get((s, "Up"), 0)
    dn = winners.get((s, "Down"), 0)
    print(f"  {s}: Up={up} Down={dn} ({100*up/max(1,up+dn):.1f}% Up)")

# settle-time alignment check: window_start % 300 == 0?
mis = [m["slug"] for m in markets.values()
       if m["interval"] == "5m" and m["window_start"] % 300 != 0]
print("\n5m markets with non-aligned window_start:", len(mis), mis[:3])

# restbook cadence
PARQ = (DATA + r"\restbook.parquet").replace("\\", "/")
con = duckdb.connect()
r = con.execute(f"""
WITH d AS (
  SELECT tok, recv, recv - LAG(recv) OVER (PARTITION BY tok ORDER BY recv) AS gap
  FROM read_parquet('{PARQ}')
)
SELECT count(*), median(gap), avg(gap),
       quantile_cont(gap, 0.9), max(gap)
FROM d WHERE gap IS NOT NULL
""").fetchall()
print("\nrest_book snapshot gaps ms (n, median, avg, p90, max):", r)

# snapshots per token
r2 = con.execute(f"""
SELECT median(c), min(c), max(c) FROM (
  SELECT tok, count(*) c FROM read_parquet('{PARQ}') GROUP BY tok)
""").fetchall()
print("rest_book snapshots per token (median, min, max):", r2)
