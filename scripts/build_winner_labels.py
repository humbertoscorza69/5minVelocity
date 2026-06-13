"""Label winners for May markets; validate the labeling procedure on June.

Labelers:
  L_bin : Up wins iff Binance close(end) >= close(start)   (Chainlink proxy)
  L_bbo : Up wins iff Up-token mid at off=0 > 0.5          (market conviction)
  L_mix : L_bbo when |binance move| < amb_bps OR sources disagree -> per rule below

Validation on June (authoritative CLOB winner flags) selects the final rule and
quantifies residual label error for May.

Outputs:
  data/binance_window_moves.parquet  (per asset/interval/epoch: ref, fin, bps)
  data/june_label_validation.csv
  data/may_winners.parquet           (epoch-level winner labels + confidence)
"""
import glob
import json
import os

import duckdb
import numpy as np
import pandas as pd

DATA = r"C:\Users\tico_\Fable\5minSnip\data"
BIN = os.path.join(DATA, "binance")

# ---------- binance per-second close series ----------
def load_sym(sym):
    files = sorted(glob.glob(os.path.join(BIN, f"{sym}_*.parquet")))
    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    df = df.drop_duplicates("open_time").sort_values("open_time")
    return df.set_index("open_time")["close"]

closes = {"btc": load_sym("BTCUSDT"), "eth": load_sym("ETHUSDT")}
print({k: (len(v), v.index.min(), v.index.max()) for k, v in closes.items()})

def px_at(series, boundary_ms, lookback=15):
    """Close of the 1s kline ending at boundary (open_time = boundary-1000)."""
    for k in range(1, lookback + 1):
        v = series.get(boundary_ms - 1000 * k)
        if v is not None and v == v:
            return float(v)
    return np.nan

# ---------- enumerate all windows (May from bbo files, June from meta) ----------
con = duckdb.connect()
may = con.execute("""
SELECT DISTINCT lower(asset) AS asset, interval, epoch
FROM read_parquet('C:/Users/tico_/Fable/5minSnip/bbo_2026-05-*.parquet')
""").df()
with open(os.path.join(DATA, "meta_summary.json")) as f:
    summ = json.load(f)
june = pd.DataFrame([
    {"asset": m["asset"], "interval": m["interval"], "epoch": m["window_start"],
     "winner_true": m["winner"]}
    for m in summ["markets"].values()])

frames = []
for label, df in [("may", may), ("june", june)]:
    df = df.copy()
    df["month"] = label
    frames.append(df)
allw = pd.concat(frames, ignore_index=True)
iv_s = allw.interval.map({"5m": 300, "15m": 900})
allw["start_ms"] = allw.epoch * 1000
allw["end_ms"] = (allw.epoch + iv_s) * 1000

refs, fins = [], []
for _, r in allw.iterrows():
    s = closes[r.asset]
    refs.append(px_at(s, r.start_ms))
    fins.append(px_at(s, r.end_ms))
allw["ref"] = refs
allw["fin"] = fins
allw["bps"] = (allw.fin / allw.ref - 1) * 1e4
allw["up_bin"] = (allw.fin >= allw.ref)
allw.to_parquet(os.path.join(DATA, "binance_window_moves.parquet"))
print("windows labeled with binance:", len(allw),
      "nan ref/fin:", int(allw.ref.isna().sum()), int(allw.fin.isna().sum()))

# ---------- BBO label at off=0 ----------
# June: from reconstruction checkpoints (Up token row, off=0)
cp = pd.read_parquet(os.path.join(DATA, "checkpoints.parquet"))
cp0 = cp[(cp.off == 0) & (cp.outcome == "Up")].copy()
cp0["mid0"] = (cp0.bb_rep + cp0.ba_rep) / 2
cp1 = cp[(cp.off == 1) & (cp.outcome == "Up")].copy()
cp1["mid1"] = (cp1.bb_rep + cp1.ba_rep) / 2
iv_j = cp0.interval.map({"5m": 300, "15m": 900})
cp0["epoch"] = cp0.settle_ts - iv_j
june_bbo = cp0[["asset", "interval", "epoch", "mid0"]].merge(
    cp1.assign(epoch=cp1.settle_ts - cp1.interval.map({"5m": 300, "15m": 900}))[
        ["asset", "interval", "epoch", "mid1"]],
    on=["asset", "interval", "epoch"], how="outer")

# May: from may_checkpoints (side='up', off=0)
mc = pd.read_parquet(os.path.join(DATA, "may_checkpoints.parquet"))
m0 = mc[(mc.off == 0) & (mc.side == "up")].copy()
m0["mid0"] = (m0.bbid + m0.bask) / 2
m1 = mc[(mc.off == 1) & (mc.side == "up")].copy()
m1["mid1"] = (m1.bbid + m1.bask) / 2
m0["asset"] = m0.asset.str.lower()
m1["asset"] = m1.asset.str.lower()
may_bbo = m0[["asset", "interval", "epoch", "mid0"]].merge(
    m1[["asset", "interval", "epoch", "mid1"]],
    on=["asset", "interval", "epoch"], how="outer")

# ---------- June validation ----------
jv = allw[allw.month == "june"].merge(june_bbo, on=["asset", "interval", "epoch"],
                                      how="left")
jv["true_up"] = jv.winner_true == "Up"
res = []
def evaluate(name, pred, valid):
    v = jv[valid & pred.notna()] if hasattr(pred, "notna") else jv[valid]
    p = pred[v.index].astype(bool)
    err = (p != v.true_up)
    res.append({"labeler": name, "n": len(v), "errors": int(err.sum()),
                "err_rate": float(err.mean()) if len(v) else np.nan})

evaluate("L_bin", jv.up_bin, jv.ref.notna() & jv.fin.notna())
evaluate("L_bbo_mid0", jv.mid0 > 0.5, jv.mid0.notna())
evaluate("L_bbo_mid1", jv.mid1 > 0.5, jv.mid1.notna())
# mixed: bbo when |bps| < 2 else binance
mix = jv.up_bin.where(jv.bps.abs() >= 2, jv.mid0 > 0.5)
evaluate("L_mix_2bps", mix, jv.ref.notna() & jv.mid0.notna())
mix1 = jv.up_bin.where(jv.bps.abs() >= 1, jv.mid0 > 0.5)
evaluate("L_mix_1bps", mix1, jv.ref.notna() & jv.mid0.notna())
# agreement-only labeling (drop disagreements)
agree = (jv.up_bin == (jv.mid0 > 0.5))
v = jv[jv.ref.notna() & jv.mid0.notna() & agree]
res.append({"labeler": "agree_only", "n": len(v),
            "errors": int((v.up_bin != v.true_up).sum()),
            "err_rate": float((v.up_bin != v.true_up).mean()) if len(v) else np.nan})
val = pd.DataFrame(res)
val.to_csv(os.path.join(DATA, "june_label_validation.csv"), index=False)
print(val.to_string())

# disagreement stats by |bps|
jv["disagree"] = (jv.up_bin != (jv.mid0 > 0.5))
print("\nJune disagreement rate by |bps| bucket:")
print(jv.groupby(pd.cut(jv.bps.abs(), [0, 0.5, 1, 2, 5, 1000]), observed=True)
        .agg(n=("disagree", "size"), disagree=("disagree", "mean")).to_string())

# ---------- May labels ----------
mv = allw[allw.month == "may"].merge(may_bbo, on=["asset", "interval", "epoch"],
                                     how="left")
mv["up_bbo"] = mv.mid0 > 0.5
mv["agree"] = mv.up_bin == mv.up_bbo
mv["winner_up"] = mv.up_bin  # primary: binance; confidence fields included
mv["confident"] = mv.agree & mv.ref.notna() & mv.mid0.notna()
mv.to_parquet(os.path.join(DATA, "may_winners.parquet"))
print("\nMay windows:", len(mv), "confident:", int(mv.confident.sum()),
      "disagree:", int((~mv.agree & mv.mid0.notna() & mv.ref.notna()).sum()),
      "missing bbo:", int(mv.mid0.isna().sum()),
      "missing bin:", int(mv.ref.isna().sum()))
