"""Underlying (Binance 1s) features for every market window.

Per market: ref price at window start, price at each offset before settle,
pre-window realized vol (std of 1s log returns over the prior 15 min, in bps).
Output: data/binance_features.parquet  (one row per cid_key)
"""
import glob
import os

import numpy as np
import pandas as pd

DATA = r"C:\Users\tico_\Fable\5minSnip\data"
OFFS = [300, 240, 180, 120, 90, 60, 45, 30, 15, 10, 5, 2, 1, 0]

def load_sym(sym):
    files = sorted(glob.glob(os.path.join(DATA, "binance", f"{sym}_*.parquet")))
    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    df = df.drop_duplicates("open_time").sort_values("open_time")
    return df.open_time.values, df.close.values.astype(float)

series = {"btc": load_sym("BTCUSDT"), "eth": load_sym("ETHUSDT")}

fav = pd.read_parquet(DATA + r"\favorites_all.parquet")
mkts = fav[["cid_key", "series", "settle_ts", "month"]].drop_duplicates("cid_key")
mkts["asset"] = mkts.series.str.split("-").str[0]
mkts["interval"] = mkts.series.str.split("-").str[1]
mkts["iv_s"] = mkts.interval.map({"5m": 300, "15m": 900})

rows = []
for asset, g in mkts.groupby("asset"):
    ot, cl = series[asset]
    # px_at(t_ms): close of last kline with open_time <= t-1000 (second ending at t)
    def px_at(t_ms):
        i = np.searchsorted(ot, t_ms - 999, side="right") - 1
        if i < 0:
            return np.nan
        if t_ms - ot[i] > 16000:  # gap > 15s
            return np.nan
        return cl[i]
    logc = np.log(cl)
    for r in g.itertuples(index=False):
        settle_ms = r.settle_ts * 1000
        start_ms = settle_ms - r.iv_s * 1000
        ref = px_at(start_ms + 1000)
        feat = {"cid_key": r.cid_key, "ref": ref}
        # pre-window vol: std of 1s logrets over [start-900s, start]
        i0 = np.searchsorted(ot, start_ms - 900_000)
        i1 = np.searchsorted(ot, start_ms)
        if i1 - i0 > 300:
            rets = np.diff(logc[i0:i1])
            feat["vol_1s_bps"] = float(np.std(rets) * 1e4)
        else:
            feat["vol_1s_bps"] = np.nan
        for off in OFFS:
            px = px_at(settle_ms - off * 1000)
            feat[f"px_{off}"] = px
            feat[f"bps_{off}"] = (px / ref - 1) * 1e4 if ref == ref else np.nan
        rows.append(feat)

bf = pd.DataFrame(rows)
bf.to_parquet(DATA + r"\binance_features.parquet")
print("markets:", len(bf), "nan ref:", int(bf.ref.isna().sum()),
      "median vol_1s_bps:", float(bf.vol_1s_bps.median()))
