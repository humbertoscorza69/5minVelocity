"""Audit May BBO parquet files: coverage, intervals, market counts."""
import glob

import duckdb

con = duckdb.connect()
con.execute("SET threads=12")
G = "C:/Users/tico_/Fable/5minSnip/bbo_2026-05-*.parquet"

print(con.execute(f"""
SELECT asset, interval, count(DISTINCT epoch) AS n_windows,
       count(DISTINCT token_id) AS n_tokens, count(*) AS rows,
       to_timestamp(min(epoch)) AS first_w, to_timestamp(max(epoch)) AS last_w
FROM read_parquet('{G}')
GROUP BY asset, interval ORDER BY asset, interval
""").df().to_string())

print(con.execute(f"""
SELECT strftime(to_timestamp(epoch), '%m-%d') AS day,
       count(DISTINCT CASE WHEN asset='BTC' THEN epoch END) AS btc_5m,
       count(DISTINCT CASE WHEN asset='ETH' THEN epoch END) AS eth_5m
FROM read_parquet('{G}') WHERE interval='5m'
GROUP BY 1 ORDER BY 1
""").df().to_string())

# events near settlement availability: rows in final 120s per window (sample day)
print(con.execute(f"""
SELECT count(*) AS rows_last120s, count(DISTINCT epoch) AS windows
FROM read_parquet('C:/Users/tico_/Fable/5minSnip/bbo_2026-05-15.parquet')
WHERE interval='5m' AND ts_exch_ms >= (epoch + 180) * 1000
""").df().to_string())

# latency
print(con.execute(f"""
SELECT median(received_at_us/1000 - ts_exch_ms) AS med_ms,
       quantile_cont(received_at_us/1000 - ts_exch_ms, 0.99) AS p99_ms
FROM read_parquet('C:/Users/tico_/Fable/5minSnip/bbo_2026-05-15.parquet')
""").df().to_string())
