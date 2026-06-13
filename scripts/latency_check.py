import duckdb
con = duckdb.connect()
con.execute("SET threads=4")
r = con.execute("""
SELECT count(*), median(recv-ts), avg(recv-ts),
       quantile_cont(recv-ts, 0.99), min(recv-ts), max(recv-ts)
FROM read_parquet('C:/Users/tico_/Fable/5minSnip/data/filtered/2026-06-09.parquet')
""").fetchone()
print("recv-ts ms (n, median, avg, p99, min, max):", r)
