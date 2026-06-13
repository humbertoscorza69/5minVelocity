"""Deep-dive every failure: markets where the favorite had mid >= 0.90 at any
tested window yet LOST.

Per failure documents: time of signal, price levels at offsets, when (if ever)
the lead flipped, underlying move (Binance 1s, local data), reversal magnitude.
Outputs: data/failure_cases.csv, data/failure_paths.parquet
"""
import glob
import os

import duckdb
import numpy as np
import pandas as pd

DATA = "C:/Users/tico_/Fable/5minSnip/data"
ROOT = "C:/Users/tico_/Fable/5minSnip"

fav = pd.read_parquet(DATA + "/favorites_all.parquet")
WINDOWS = [5, 10, 15, 30, 45, 60, 90, 120]
sig = fav[fav.off.isin(WINDOWS) & (fav["mid"] >= 0.90) & (fav.winner == 0)]
fails = sig.groupby("cid_key").agg(
    month=("month", "first"), series=("series", "first"),
    settle_ts=("settle_ts", "first"), tid=("tid", "first"),
    fav_side=("fav_side", "first"), confident=("confident", "first"),
    max_mid=("mid", "max"), earliest_W=("off", "max"), latest_W=("off", "min"),
).reset_index()
print("failure markets:", len(fails), "by month:",
      fails.month.value_counts().to_dict())

# mids at all offsets for these markets (pivot from favorites_all)
allof = fav[fav.cid_key.isin(fails.cid_key)]
piv = allof.pivot_table(index="cid_key", columns="off", values="mid",
                        aggfunc="first")
piv.columns = [f"mid_{int(c)}" for c in piv.columns]
fails = fails.merge(piv.reset_index(), on="cid_key", how="left")

con = duckdb.connect()
con.execute("SET threads=8")
con.execute("SET memory_limit='8GB'")

# ---- quote paths: June (events_sorted by tok) ----
paths = []
jf = fails[fails.month == "june"]
if len(jf):
    toks = ",".join(jf.tid.tolist())
    q = con.execute(f"""
        SELECT tok, ts, bb, ba FROM read_parquet('{DATA}/events_sorted.parquet')
        WHERE tok IN ({toks}) ORDER BY tok, ts
    """).df()
    q["tid"] = q.tok.astype(str)
    tid2key = dict(zip(jf.tid, jf.cid_key))
    tid2settle = dict(zip(jf.tid, jf.settle_ts))
    q["cid_key"] = q.tid.map(tid2key)
    q["settle_ms"] = q.tid.map(tid2settle) * 1000
    q = q[(q.ts >= q.settle_ms - 200_000) & (q.ts <= q.settle_ms + 1000)]
    paths.append(q[["cid_key", "ts", "bb", "ba", "settle_ms"]])

mf = fails[fails.month == "may"]
if len(mf):
    tids = ",".join(f"'{t}'" for t in mf.tid.tolist())
    q = con.execute(f"""
        SELECT token_id, ts_exch_ms AS ts, best_bid AS bb, best_ask AS ba
        FROM read_parquet('{ROOT}/bbo_2026-05-*.parquet')
        WHERE token_id IN ({tids}) ORDER BY token_id, ts
    """).df()
    tid2key = dict(zip(mf.tid, mf.cid_key))
    tid2settle = dict(zip(mf.tid, mf.settle_ts))
    q["cid_key"] = q.token_id.map(tid2key)
    q["settle_ms"] = q.token_id.map(tid2settle) * 1000
    q = q[(q.ts >= q.settle_ms - 200_000) & (q.ts <= q.settle_ms + 1000)]
    paths.append(q[["cid_key", "ts", "bb", "ba", "settle_ms"]])

pp = pd.concat(paths, ignore_index=True)
pp.to_parquet(DATA + "/failure_paths.parquet")

# flip time: first time fav mid < 0.5 within last 200s
pp["mid"] = (pp.bb + pp.ba) / 2
flips = (pp[pp["mid"] < 0.5].groupby("cid_key")
         .apply(lambda g: (g.settle_ms.iloc[0] - g.ts.min()) / 1000.0,
                include_groups=False)
         .rename("flip_s_before_settle"))
fails = fails.merge(flips.reset_index(), on="cid_key", how="left")

# ---- Binance underlying ----
def load_sym(sym):
    files = sorted(glob.glob(os.path.join(DATA, "binance", f"{sym}_*.parquet")))
    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    df = df.drop_duplicates("open_time").sort_values("open_time")
    return df.set_index("open_time")["close"]

closes = {"btc": load_sym("BTCUSDT"), "eth": load_sym("ETHUSDT")}

def px_at(s, ms, lookback=15):
    for k in range(1, lookback + 1):
        v = s.get(ms - 1000 * k)
        if v is not None and v == v:
            return float(v)
    return np.nan

rows = []
for _, r in fails.iterrows():
    asset, interval = r.series.split("-")
    iv = 300 if interval == "5m" else 900
    settle_ms = int(r.settle_ts) * 1000
    start_ms = settle_ms - iv * 1000
    s = closes[asset]
    ref = px_at(s, start_ms + 1000)  # close of first second of window
    fin = px_at(s, settle_ms)
    feats = {"binance_ref": ref, "binance_final": fin,
             "bps_final": (fin / ref - 1) * 1e4 if ref == ref else np.nan}
    for off in [120, 60, 30, 10, 5]:
        px = px_at(s, settle_ms - off * 1000)
        feats[f"bps_t{off}"] = (px / ref - 1) * 1e4 if ref == ref else np.nan
    rows.append(feats)
fails = pd.concat([fails.reset_index(drop=True), pd.DataFrame(rows)], axis=1)
fails["reversal_bps_last60"] = fails.bps_final - fails.bps_t60
fails.to_csv(DATA + "/failure_cases.csv", index=False)
print("failure cases saved:", len(fails))
print(fails.groupby(["month", "series"]).size().to_string())
