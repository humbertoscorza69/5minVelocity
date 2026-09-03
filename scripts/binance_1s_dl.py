"""Download Binance 1-SECOND klines into a parquet cache.

Usage: python scripts/binance_1s_dl.py <out_parquet> <start_unix_s> <end_unix_s>

1s closes are what the bot's PriceHistory ring holds, so this is the only
granularity that lets an offline study reproduce the deployed vol60 / disp / z
exactly, and the only one fine enough to measure REALISED forward volatility
over a 30-240s remaining window.
"""
import sys
import time

import pandas as pd
import requests

URL = "https://api.binance.com/api/v3/klines"
LIMIT = 1000


def fetch(symbol, start_ms, end_ms):
    out = []
    cur = start_ms
    while cur < end_ms:
        for attempt in range(5):
            try:
                r = requests.get(
                    URL,
                    params={
                        "symbol": symbol,
                        "interval": "1s",
                        "startTime": cur,
                        "endTime": end_ms,
                        "limit": LIMIT,
                    },
                    timeout=30,
                )
                r.raise_for_status()
                rows = r.json()
                break
            except Exception as exc:
                if attempt == 4:
                    raise
                print(f"  retry {symbol} @{cur}: {exc}")
                time.sleep(2 * (attempt + 1))
        if not rows:
            cur += LIMIT * 1000
            continue
        out.extend([(row[0] // 1000, float(row[1]), float(row[4])) for row in rows])
        cur = rows[-1][0] + 1000
        time.sleep(0.06)
    df = pd.DataFrame(out, columns=["open_s", "open", "close"])
    df["symbol"] = symbol
    return df.drop_duplicates("open_s")


def main(out_path, start_s, end_s):
    frames = []
    for sym in ("BTCUSDT", "ETHUSDT"):
        t0 = time.time()
        df = fetch(sym, int(start_s) * 1000, int(end_s) * 1000)
        print(f"{sym}: n={len(df)} in {time.time()-t0:.0f}s "
              f"{pd.to_datetime(df.open_s.min(), unit='s')} -> "
              f"{pd.to_datetime(df.open_s.max(), unit='s')}", flush=True)
        frames.append(df)
    pd.concat(frames, ignore_index=True).to_parquet(out_path, index=False)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3])
