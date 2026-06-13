"""Build checkpoint table from May BBO parquet.

For each token and offset W: last quote at/before T = settle-W, plus
min best_ask / max best_bid from that row through settlement (fill proxy).
Output: data/may_checkpoints.parquet
"""
import time

import duckdb

DATA = "C:/Users/tico_/Fable/5minSnip/data"
G = "C:/Users/tico_/Fable/5minSnip/bbo_2026-05-*.parquet"

OFFSETS = [300, 240, 180, 120, 90, 60, 45, 30, 15, 10, 5, 2, 1, 0]

con = duckdb.connect()
con.execute("SET memory_limit='8GB'")
con.execute("SET threads=8")
con.execute(f"SET temp_directory='{DATA}/tmp_duckdb'")

t0 = time.time()
con.execute(f"""
CREATE TEMP TABLE q AS
SELECT token_id, asset, interval, epoch, side,
       ts_exch_ms, best_bid, best_ask,
       (epoch + CASE interval WHEN '5m' THEN 300 ELSE 900 END) * 1000 AS settle_ms,
       min(best_ask) OVER w AS min_ask_after,
       max(best_bid) OVER w AS max_bid_after
FROM read_parquet('{G}')
WHERE ts_exch_ms <= (epoch + CASE interval WHEN '5m' THEN 300 ELSE 900 END) * 1000
WINDOW w AS (PARTITION BY token_id ORDER BY ts_exch_ms DESC
             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
""")
print(f"temp table {time.time()-t0:.0f}s rows:",
      con.execute("SELECT count(*) FROM q").fetchone())

parts = []
for W in OFFSETS:
    parts.append(f"""
    SELECT *, {W} AS off FROM (
      SELECT token_id, asset, interval, epoch, side, settle_ms,
             best_bid AS bbid, best_ask AS bask,
             min_ask_after, max_bid_after,
             settle_ms - {W}*1000 - ts_exch_ms AS age_ms,
             row_number() OVER (PARTITION BY token_id ORDER BY ts_exch_ms DESC) rn
      FROM q WHERE ts_exch_ms <= settle_ms - {W}*1000
    ) WHERE rn = 1
    """)
sql = " UNION ALL ".join(parts)
t0 = time.time()
con.execute(f"""
COPY (SELECT token_id, asset, interval, epoch, side, settle_ms, off,
             bbid, bask, min_ask_after, max_bid_after, age_ms
      FROM ({sql}))
TO '{DATA}/may_checkpoints.parquet' (FORMAT PARQUET, COMPRESSION ZSTD)
""")
print(f"checkpoints {time.time()-t0:.0f}s rows:",
      con.execute(f"SELECT count(*) FROM read_parquet('{DATA}/may_checkpoints.parquet')").fetchone())
