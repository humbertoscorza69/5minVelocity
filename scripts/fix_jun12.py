import sys, os
import pandas as pd
sys.path.insert(0, os.path.dirname(__file__))
from fetch_binance import fetch

# 2026-06-12 00:00:00 UTC = 1781222400; cover 23:45 Jun 11 -> 00:30 Jun 12
start = (1781222400 - 900) * 1000
end = (1781222400 + 1800) * 1000
for sym in ("BTCUSDT", "ETHUSDT"):
    rows = fetch(sym, start, end)
    df = pd.DataFrame({"open_time": [r[0] for r in rows],
                       "close": [float(r[4]) for r in rows]})
    if len(df) and df.open_time.iloc[0] > 10 ** 14:
        df["open_time"] = df.open_time // 1000
    dest = rf"C:\Users\tico_\Fable\5minSnip\data\binance\{sym}_2026-06-12.parquet"
    df.to_parquet(dest)
    print(sym, len(df))
