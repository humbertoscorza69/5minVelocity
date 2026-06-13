"""Download Binance daily 1s kline zips for BTCUSDT/ETHUSDT, May 6-30 + June 4-12,
extract to per-day parquet of (open_time_ms, close)."""
import io
import os
import urllib.request
import zipfile
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta

import pandas as pd

OUT = r"C:\Users\tico_\Fable\5minSnip\data\binance"
os.makedirs(OUT, exist_ok=True)

days = []
d = date(2026, 5, 6)
while d <= date(2026, 5, 30):
    days.append(d); d += timedelta(days=1)
d = date(2026, 6, 4)
while d <= date(2026, 6, 12):
    days.append(d); d += timedelta(days=1)

def grab(args):
    sym, day = args
    dest = os.path.join(OUT, f"{sym}_{day}.parquet")
    if os.path.exists(dest):
        return f"skip {sym} {day}"
    url = (f"https://data.binance.vision/data/spot/daily/klines/{sym}/1s/"
           f"{sym}-1s-{day}.zip")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=120) as r:
            buf = io.BytesIO(r.read())
        with zipfile.ZipFile(buf) as z:
            name = z.namelist()[0]
            df = pd.read_csv(z.open(name), header=None, usecols=[0, 4],
                             names=["open_time", "close"])
        # some dumps use microsecond timestamps
        if df.open_time.iloc[0] > 10 ** 14:
            df["open_time"] = df.open_time // 1000
        df.to_parquet(dest)
        return f"ok {sym} {day} {len(df)}"
    except Exception as e:
        return f"ERR {sym} {day} {e!r}"

jobs = [(s, d) for s in ("BTCUSDT", "ETHUSDT") for d in days]
with ThreadPoolExecutor(max_workers=8) as ex:
    for res in ex.map(grab, jobs):
        print(res, flush=True)
print("DONE")
