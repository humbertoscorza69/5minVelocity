"""Extract dense per-second book mid for June 5m tokens, final 200s, joined
with token meta. Output: data/ll_booksec.parquet
  tok, asset, side, winner, settle_ms, ttl (int sec to settle),
  bb, ba (last quote in that second), nq (quotes that second)
"""
import json
import time

import duckdb

DATA = "C:/Users/tico_/Fable/5minSnip/data"

# token meta (June, 5m) from checkpoints
con = duckdb.connect()
con.execute("SET threads=12")
con.execute("SET memory_limit='12GB'")
con.execute(f"SET temp_directory='{DATA}/tmp_duckdb'")

con.execute(f"""
CREATE TEMP TABLE meta AS
SELECT DISTINCT tok, asset, interval, outcome AS side, winner,
       settle_ts*1000 AS settle_ms
FROM read_parquet('{DATA}/checkpoints.parquet')
WHERE interval='5m'
""")
print("meta tokens:", con.execute("SELECT count(*) FROM meta").fetchone())

t0 = time.time()
con.execute(f"""
COPY (
  SELECT e.tok, m.asset, lower(m.side) AS side, m.winner, m.settle_ms,
         CAST(floor((m.settle_ms - e.ts)/1000) AS INT) AS ttl,
         arg_max(e.bb, e.ts) AS bb,
         arg_max(e.ba, e.ts) AS ba,
         count(*) AS nq
  FROM read_parquet('{DATA}/events_sorted.parquet') e
  JOIN meta m ON e.tok = m.tok
  WHERE e.ts >= m.settle_ms - 200000 AND e.ts <= m.settle_ms
  GROUP BY e.tok, m.asset, m.side, m.winner, m.settle_ms, ttl
) TO '{DATA}/ll_booksec.parquet' (FORMAT PARQUET, COMPRESSION ZSTD)
""")
print(f"extract {time.time()-t0:.0f}s rows:",
      con.execute(f"SELECT count(*) FROM read_parquet('{DATA}/ll_booksec.parquet')").fetchone())
