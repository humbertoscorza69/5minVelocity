"""Download Binance 1m klines (public REST, no key) into a parquet cache.

Usage: python scripts/binance_klines_dl.py <out_parquet> <start_unix_s> <end_unix_s>

Used to label Up/Down window outcomes independently of the bot's own bookings:
a window [epoch, epoch+interval) opens at kline[epoch].open and finishes at
kline[epoch+interval-60].close. Ties resolve Up (the project convention).
Settlement is really Chainlink; this is the standard Binance proxy, which flips
on roughly 20% of photo finishes (|final move| < 2bps).
"""
import sys
import time

import pandas as pd
import requests

URL = "https://api.binance.com/api/v3/klines"


def fetch(symbol, start_ms, end_ms):
    out = []
    cur = start_ms
    while cur < end_ms:
        r = requests.get(
            URL,
            params={
                "symbol": symbol,
                "interval": "1m",
                "startTime": cur,
                "endTime": end_ms,
                "limit": 1000,
            },
            timeout=30,
        )
        r.raise_for_status()
        rows = r.json()
        if not rows:
            break
        out.extend(rows)
        cur = rows[-1][0] + 60_000
        time.sleep(0.12)
    df = pd.DataFrame(
        out,
        columns=[
            "open_ms", "open", "high", "low", "close", "volume", "close_ms",
            "qav", "trades", "tbb", "tbq", "ignore",
        ],
    )
    for c in ("open", "high", "low", "close"):
        df[c] = df[c].astype(float)
    df["open_s"] = df.open_ms // 1000
    df["symbol"] = symbol
    return df[["symbol", "open_s", "open", "high", "low", "close", "trades"]]


def main(out_path, start_s, end_s):
    frames = [fetch(sym, int(start_s) * 1000, int(end_s) * 1000)
              for sym in ("BTCUSDT", "ETHUSDT")]
    df = pd.concat(frames, ignore_index=True).drop_duplicates(["symbol", "open_s"])
    df.to_parquet(out_path, index=False)
    for sym, g in df.groupby("symbol"):
        print(f"{sym}: n={len(g)} "
              f"{pd.to_datetime(g.open_s.min(), unit='s')} -> "
              f"{pd.to_datetime(g.open_s.max(), unit='s')}")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3])
