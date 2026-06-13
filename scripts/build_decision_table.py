"""Master decision table: BOTH sides of every market at every offset, with
underlying features and (June) REST book imbalance.

Output: data/decisions.parquet
  month, series, cid_key, off, is_fav, side, mid, bbid, bask, age_ms,
  min_ask_after, max_bid_after, winner, confident, settle_ts,
  fav_mid (per cid/off), lead_bps (signed toward this token), z,
  vol_1s_bps, spread, mom60 (fav mid change from off+60), imb, imb_age_s
"""
import json

import numpy as np
import pandas as pd

DATA = r"C:\Users\tico_\Fable\5minSnip\data"

# ---------- June: both sides ----------
cp = pd.read_parquet(DATA + r"\checkpoints.parquet")
cp["mid"] = (cp.bb_rep + cp.ba_rep) / 2
cp["series"] = cp.asset + "-" + cp.interval
cp["cid_key"] = cp.series + "-" + cp.settle_ts.astype(str)
cp["month"] = "june"
cp["confident"] = True
cp["side"] = cp.outcome.str.lower()
june = cp.rename(columns={"bb_rep": "bbid", "ba_rep": "bask"})[
    ["month", "series", "cid_key", "off", "side", "mid", "bbid", "bask",
     "age_ms", "min_ask_after", "max_bid_after", "winner", "confident",
     "settle_ts", "tok"]].copy()
june["tid"] = june.tok.astype(str)
june = june.drop(columns=["tok"])

# ---------- May: both sides ----------
mc = pd.read_parquet(DATA + r"\may_checkpoints.parquet")
mw = pd.read_parquet(DATA + r"\may_winners.parquet")
mc["asset"] = mc.asset.str.lower()
mc["mid"] = (mc.bbid + mc.bask) / 2
mc["series"] = mc.asset + "-" + mc.interval
mc["settle_ts"] = mc.settle_ms // 1000
mc["cid_key"] = mc.series + "-" + mc.settle_ts.astype(str)
mw["winner_side"] = np.where(mw.winner_up, "up", "down")
mc = mc.merge(mw[["asset", "interval", "epoch", "winner_side", "confident"]],
              on=["asset", "interval", "epoch"], how="left")
mc = mc[mc.winner_side.notna()]
mc["winner"] = (mc.side == mc.winner_side).astype(int)
mc["month"] = "may"
may = mc.rename(columns={"token_id": "tid"})[
    ["month", "series", "cid_key", "off", "side", "mid", "bbid", "bask",
     "age_ms", "min_ask_after", "max_bid_after", "winner", "confident",
     "settle_ts", "tid"]].copy()

d = pd.concat([june, may], ignore_index=True)
d = d.dropna(subset=["mid"])

# favorite flag and fav_mid per (cid_key, off)
d["fav_mid"] = d.groupby(["cid_key", "off"])["mid"].transform("max")
d["is_fav"] = (d["mid"] >= d.fav_mid - 1e-12) & (d["mid"] > 0.5)
d["spread"] = d.bask - d.bbid

# momentum of the favorite: fav side mid now minus same-side mid at off+60
piv = d.pivot_table(index=["cid_key", "side"], columns="off", values="mid",
                    aggfunc="first")
mom = {}
for off in [5, 10, 15, 30, 45, 60, 90, 120]:
    base = off + 60
    if base in piv.columns and off in piv.columns:
        mom[off] = (piv[off] - piv[base]).rename(f"m{off}")
momdf = pd.DataFrame(mom).reset_index()
momdf = momdf.melt(id_vars=["cid_key", "side"], var_name="off",
                   value_name="mom60")
momdf["off"] = momdf.off.astype(int)
d = d.merge(momdf, on=["cid_key", "side", "off"], how="left")

# ---------- underlying features ----------
bf = pd.read_parquet(DATA + r"\binance_features.parquet")
keep = ["cid_key", "vol_1s_bps"] + [f"bps_{o}" for o in
                                    [120, 90, 60, 45, 30, 15, 10, 5, 2, 1, 0, 180, 240, 300]]
d = d.merge(bf[keep], on="cid_key", how="left")
offs = d.off.values
bps_at = np.full(len(d), np.nan)
for o in [300, 240, 180, 120, 90, 60, 45, 30, 15, 10, 5, 2, 1, 0]:
    m = offs == o
    bps_at[m] = d.loc[m, f"bps_{o}"].values
d["bps_at_off"] = bps_at
sign = np.where(d.side == "up", 1.0, -1.0)
d["lead_bps"] = sign * d.bps_at_off          # >0: underlying favors THIS token
wsafe = np.maximum(d.off.values, 1)
d["z"] = d.lead_bps / (d.vol_1s_bps * np.sqrt(wsafe))
d = d.drop(columns=[c for c in d.columns if c.startswith("bps_") and c != "bps_at_off"])

# ---------- June REST imbalance (favorite token, last snapshot <= T) ----------
rb = pd.read_parquet(DATA + r"\restbook.parquet")[
    ["tok", "ts", "bid_total_usd", "ask_total_usd"]].sort_values(["tok", "ts"])
rb["imb"] = (rb.bid_total_usd - rb.ask_total_usd) / \
            (rb.bid_total_usd + rb.ask_total_usd + 1e-9)
jmask = d.month == "june"
jd = d[jmask]
dec_ms = (jd.settle_ts - 0).values * 1000 - jd.off.values * 1000
toks = jd.tid.astype(int).values
imb = np.full(len(jd), np.nan)
imb_age = np.full(len(jd), np.nan)
for tok, grp in rb.groupby("tok"):
    sel = np.where(toks == tok)[0]
    if not len(sel):
        continue
    ts_arr = grp.ts.values
    iv = grp.imb.values
    pos = np.searchsorted(ts_arr, dec_ms[sel], side="right") - 1
    ok = pos >= 0
    imb[sel[ok]] = iv[pos[ok]]
    imb_age[sel[ok]] = (dec_ms[sel[ok]] - ts_arr[pos[ok]]) / 1000.0
d.loc[jmask, "imb"] = imb
d.loc[jmask, "imb_age_s"] = imb_age

d.to_parquet(DATA + r"\decisions.parquet")
print("decision rows:", len(d), "| june:", int(jmask.sum()),
      "| with imb:", int(np.isfinite(imb).sum()),
      "| z available:", int(d.z.notna().sum()))
