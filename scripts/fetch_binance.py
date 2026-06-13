"""Fetch Binance 1s klines for given symbol/time windows (for failure analysis).

Usage: python fetch_binance.py <symbol> <start_ms> <end_ms> <out_csv>
"""
import json
import sys
import time
import urllib.request

def fetch(symbol, start_ms, end_ms):
    out = []
    cur = start_ms
    while cur < end_ms:
        url = (f"https://data-api.binance.vision/api/v3/klines?symbol={symbol}"
               f"&interval=1s&startTime={cur}&endTime={end_ms}&limit=1000")
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=30) as r:
            data = json.load(r)
        if not data:
            break
        out.extend(data)
        cur = data[-1][6] + 1
        if len(data) < 1000:
            break
        time.sleep(0.15)
    return out

if __name__ == "__main__":
    symbol, s, e, dest = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), sys.argv[4]
    rows = fetch(symbol, s, e)
    with open(dest, "w") as f:
        f.write("open_time,open,high,low,close,volume\n")
        for r in rows:
            f.write(f"{r[0]},{r[1]},{r[2]},{r[3]},{r[4]},{r[5]}\n")
    print(len(rows), "klines ->", dest)
