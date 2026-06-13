"""Globally sort filtered events by (tok, ts, file order) using DuckDB."""
import duckdb
import time

con = duckdb.connect()
con.execute("SET memory_limit='12GB'")
con.execute("SET threads=12")
con.execute("SET temp_directory='C:/Users/tico_/Fable/5minSnip/data/tmp_duckdb'")
con.execute("SET preserve_insertion_order=false")

t0 = time.time()
con.execute("""
COPY (
  SELECT tok, ts, lvl_price, lvl_size, side, bb, ba
  FROM read_parquet('C:/Users/tico_/Fable/5minSnip/data/filtered/2026-*.parquet',
                    filename=true, file_row_number=true)
  ORDER BY tok, ts, filename, file_row_number
) TO 'C:/Users/tico_/Fable/5minSnip/data/events_sorted.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 4000000);
""")
print(f"sorted in {time.time()-t0:.0f}s")
print(con.execute("SELECT COUNT(*) FROM read_parquet('C:/Users/tico_/Fable/5minSnip/data/events_sorted.parquet')").fetchone())
