"""Per-second book mid for 15m tokens (final 200s), June (events_sorted) + May (bbo)."""
import time
import duckdb

DATA = "C:/Users/tico_/Fable/5minSnip/data"
ROOT = "C:/Users/tico_/Fable/5minSnip"
con = duckdb.connect()
con.execute("SET threads=12"); con.execute("SET memory_limit='12GB'")
con.execute(f"SET temp_directory='{DATA}/tmp_duckdb'")

# June 15m
con.execute(f"""
CREATE TEMP TABLE meta AS
SELECT DISTINCT tok, asset, outcome AS side, winner, settle_ts*1000 AS settle_ms
FROM read_parquet('{DATA}/checkpoints.parquet') WHERE interval='15m'
""")
t0 = time.time()
con.execute(f"""
COPY (
  SELECT e.tok, m.asset, lower(m.side) AS side, m.winner, m.settle_ms,
         CAST(floor((m.settle_ms - e.ts)/1000) AS INT) AS ttl,
         arg_max(e.bb, e.ts) AS bb, arg_max(e.ba, e.ts) AS ba
  FROM read_parquet('{DATA}/events_sorted.parquet') e JOIN meta m ON e.tok=m.tok
  WHERE e.ts >= m.settle_ms - 200000 AND e.ts <= m.settle_ms
  GROUP BY e.tok, m.asset, m.side, m.winner, m.settle_ms, ttl
) TO '{DATA}/ll_booksec15_june.parquet' (FORMAT PARQUET, COMPRESSION ZSTD)
""")
print("june 15m rows:", con.execute(f"SELECT count(*) FROM read_parquet('{DATA}/ll_booksec15_june.parquet')").fetchone(), f"{time.time()-t0:.0f}s")

# May 15m
con.execute(f"""
CREATE TEMP TABLE w AS
SELECT lower(asset) AS asset, interval, epoch, winner_up
FROM read_parquet('{DATA}/may_winners.parquet')
""")
t0 = time.time()
con.execute(f"""
COPY (
  WITH q AS (
    SELECT token_id AS tok, lower(asset) AS asset, interval, epoch, side,
           ts_exch_ms AS ts, best_bid AS bb, best_ask AS ba,
           (epoch+900)*1000 AS settle_ms
    FROM read_parquet('{ROOT}/bbo_2026-05-*.parquet') WHERE interval='15m'
  )
  SELECT q.tok, q.asset, q.side,
         CASE WHEN (q.side='up')=(w.winner_up) THEN 1 ELSE 0 END AS winner,
         q.settle_ms, CAST(floor((q.settle_ms - q.ts)/1000) AS INT) AS ttl,
         arg_max(q.bb,q.ts) AS bb, arg_max(q.ba,q.ts) AS ba
  FROM q JOIN w ON q.asset=w.asset AND q.interval=w.interval AND q.epoch=w.epoch
  WHERE q.ts >= q.settle_ms-200000 AND q.ts <= q.settle_ms AND w.winner_up IS NOT NULL
  GROUP BY q.tok,q.asset,q.side,winner,q.settle_ms,ttl
) TO '{DATA}/ll_booksec15_may.parquet' (FORMAT PARQUET, COMPRESSION ZSTD)
""")
print("may 15m rows:", con.execute(f"SELECT count(*) FROM read_parquet('{DATA}/ll_booksec15_may.parquet')").fetchone(), f"{time.time()-t0:.0f}s")
