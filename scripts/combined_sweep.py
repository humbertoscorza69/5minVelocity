"""Combined May+June parameter sweep, calibration, and maker analysis.

Favorites normalized across sources:
  June: reconstruction checkpoints + authoritative CLOB winners
  May : BBO checkpoints + binance/BBO-agreement winner labels
Outputs:
  data/favorites_all.parquet
  data/sweep_combined.csv   (subsets: june / may / all / may_worstcase)
  data/calibration_combined.csv
  data/maker_combined.csv
"""
import math

import numpy as np
import pandas as pd

DATA = r"C:\Users\tico_\Fable\5minSnip\data"
FEE_RATE = 0.07
WINDOWS = [5, 10, 15, 30, 45, 60, 90, 120]
THRESHOLDS = [0.80, 0.82, 0.85, 0.87, 0.90, 0.92, 0.95, 0.97, 0.98, 0.99]

def wilson(k, n, z=1.96):
    if n == 0:
        return (np.nan, np.nan)
    p = k / n
    d = 1 + z * z / n
    c = p + z * z / (2 * n)
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return ((c - h) / d, (c + h) / d)

# ---------- June favorites ----------
jf = pd.read_parquet(DATA + r"\favorites.parquet")
jf = jf.rename(columns={})
jf["month"] = "june"
jf["confident"] = True
jf["series"] = jf.asset + "-" + jf.interval
jf["tid"] = jf.tok.astype(str)
jf["fav_side"] = jf.outcome.str.lower()
june = jf[["series", "cid_key", "off", "mid", "bbid", "bask",
           "min_ask_after", "max_bid_after", "winner", "confident",
           "settle_ts", "month", "age_ms", "tid", "fav_side"]].copy()

# ---------- May favorites ----------
mc = pd.read_parquet(DATA + r"\may_checkpoints.parquet")
mw = pd.read_parquet(DATA + r"\may_winners.parquet")
mc["asset"] = mc.asset.str.lower()
mc["mid"] = (mc.bbid + mc.bask) / 2
mc["series"] = mc.asset + "-" + mc.interval
mc["cid_key"] = mc.series + "-" + (mc.settle_ms // 1000).astype(str)
mw["winner_side"] = np.where(mw.winner_up, "up", "down")
key = ["asset", "interval", "epoch"]
mc = mc.merge(mw[key + ["winner_side", "confident", "agree", "up_bin", "mid0"]],
              on=key, how="left")
mc = mc[mc.winner_side.notna()]
mc["winner"] = (mc.side == mc.winner_side).astype(int)
mc["settle_ts"] = mc.settle_ms // 1000
fav_may = (mc.dropna(subset=["mid"])
             .sort_values("mid", ascending=False)
             .drop_duplicates(["cid_key", "off"], keep="first"))
fav_may["month"] = "may"
fav_may["tid"] = fav_may.token_id
fav_may["fav_side"] = fav_may.side
may = fav_may[["series", "cid_key", "off", "mid", "bbid", "bask",
               "min_ask_after", "max_bid_after", "winner", "confident",
               "settle_ts", "month", "age_ms", "tid", "fav_side"]].copy()

fav = pd.concat([june, may], ignore_index=True)
fav.to_parquet(DATA + r"\favorites_all.parquet")
print("favorites:", fav.groupby("month").size().to_dict())

def fee(p):
    return FEE_RATE * p * (1 - p)

def run_sweep(df, label):
    out = []
    for series, gs in df.groupby("series"):
        for W in WINDOWS:
            g0 = gs[gs.off == W]
            for P in THRESHOLDS:
                g = g0[(g0["mid"] >= P) & (g0.bask < 1.0) & g0.bask.notna()]
                n = len(g)
                if n == 0:
                    out.append({"subset": label, "series": series, "W": W,
                                "P": P, "n": 0})
                    continue
                price = g.bask.values
                f = fee(price)
                win = g.winner.values.astype(bool)
                pnl = np.where(win, 1 - price - f, -(price + f))
                cost = price + f
                roc = pnl / cost
                o = np.argsort(g.settle_ts.values, kind="stable")
                cum = np.cumsum(pnl[o])
                peak = np.maximum.accumulate(np.concatenate([[0], cum]))[1:]
                mdd = float(np.max(peak - cum))
                maxcl = cl = 0
                for w_ in win[o]:
                    cl = 0 if w_ else cl + 1
                    maxcl = max(maxcl, cl)
                k = int(win.sum())
                wlo, whi = wilson(k, n)
                sd = float(pnl.std(ddof=1)) if n > 1 else np.nan
                exp_sh = float(pnl.mean())
                out.append({
                    "subset": label, "series": series, "W": W, "P": P, "n": n,
                    "wins": k, "losses": n - k, "win_rate": k / n,
                    "wr_lo": wlo, "wr_hi": whi,
                    "avg_entry": float(price.mean()),
                    "avg_win": float(pnl[win].mean()) if k else np.nan,
                    "avg_loss": float(-pnl[~win].mean()) if k < n else np.nan,
                    "expectancy_per_share": exp_sh,
                    "exp_ci_lo": exp_sh - 1.96 * sd / math.sqrt(n) if n > 1 else np.nan,
                    "exp_ci_hi": exp_sh + 1.96 * sd / math.sqrt(n) if n > 1 else np.nan,
                    "mean_roc": float(roc.mean()),
                    "total_pnl_per_share": float(pnl.sum()),
                    "max_drawdown_sh": mdd, "max_consec_losses": maxcl,
                    "breakeven_wr": float(cost.mean()),
                    "t_stat": exp_sh / (sd / math.sqrt(n)) if n > 1 and sd > 0 else np.nan,
                })
    return out

res = []
res += run_sweep(fav[fav.month == "june"], "june")
res += run_sweep(fav[fav.month == "may"], "may")
res += run_sweep(fav, "all")
# worst case: every non-confident May label counted as a loss
wc = fav.copy()
wc.loc[(wc.month == "may") & (~wc.confident.astype(bool)), "winner"] = 0
res += run_sweep(wc[wc.month == "may"], "may_worstcase")
sweep = pd.DataFrame(res)
sweep.to_csv(DATA + r"\sweep_combined.csv", index=False)
print("sweep rows:", len(sweep))

# ---------- IS/OOS protocol ----------
# In-sample = MAY only. Selection rule (pre-registered):
#   n >= 50, exp_ci_lo > 0 (95% CI excludes zero), rank by t_stat per series.
# Selected configs are then evaluated on JUNE (out-of-sample).
is_ = sweep[(sweep.subset == "may") & (sweep.n >= 50)].copy()
sel = is_[is_.exp_ci_lo > 0].sort_values("t_stat", ascending=False)
sel_top = sel.groupby("series").head(3)
oos = sweep[sweep.subset == "june"]
rows = []
for _, r in sel_top.iterrows():
    o = oos[(oos.series == r.series) & (oos.W == r.W) & (oos.P == r.P)]
    o = o.iloc[0] if len(o) else None
    rows.append({
        "series": r.series, "W": r.W, "P": r.P,
        "IS_n": r.n, "IS_wr": r.win_rate, "IS_exp": r.expectancy_per_share,
        "IS_ci": (r.exp_ci_lo, r.exp_ci_hi), "IS_t": r.t_stat,
        "IS_roc": r.mean_roc,
        "OOS_n": o.n if o is not None else 0,
        "OOS_wr": o.win_rate if o is not None else np.nan,
        "OOS_exp": o.expectancy_per_share if o is not None else np.nan,
        "OOS_ci": (o.exp_ci_lo, o.exp_ci_hi) if o is not None else np.nan,
        "OOS_roc": o.mean_roc if o is not None else np.nan,
    })
pd.DataFrame(rows).to_csv(DATA + r"\is_oos_validation.csv", index=False)
print("IS-selected configs:", len(sel), "top per series -> OOS saved")

# trades for failure analysis (full table at P=0.90 reference)
trades = []
for (series, W), g0 in fav.groupby(["series", "off"]):
    g = g0[(g0["mid"] >= 0.90) & (g0.bask < 1.0) & g0.bask.notna()].copy()
    g["W"] = W
    trades.append(g)
pd.concat(trades, ignore_index=True).to_parquet(DATA + r"\trades_p90_all.parquet")

# ---------- calibration ----------
bins = [0.5, 0.6, 0.7, 0.8, 0.85, 0.9, 0.92, 0.95, 0.97, 0.98, 0.99, 0.995, 1.0001]
cal = []
for (month, series, off), g in fav.groupby(["month", "series", "off"]):
    g = g[g["mid"].notna() & (g["mid"] >= 0.5)]
    b = pd.cut(g["mid"], bins, right=False)
    agg = g.groupby(b, observed=True).agg(n=("winner", "size"),
                                          wins=("winner", "sum"),
                                          avg_mid=("mid", "mean"))
    for iv, r in agg.iterrows():
        lo, hi = wilson(r.wins, r.n)
        cal.append({"month": month, "series": series, "off": off,
                    "bucket": str(iv), "n": int(r.n), "wins": int(r.wins),
                    "win_rate": r.wins / r.n if r.n else np.nan,
                    "avg_mid": r.avg_mid, "wr_lo": lo, "wr_hi": hi})
pd.DataFrame(cal).to_csv(DATA + r"\calibration_combined.csv", index=False)

# ---------- maker ----------
mres = []
for (month, series), gs in fav.groupby(["month", "series"]):
    for W in WINDOWS:
        g0 = gs[gs.off == W]
        for P in [0.90, 0.95, 0.97]:
            g = g0[(g0["mid"] >= P) & g0.bbid.notna()]
            n = len(g)
            if n == 0:
                continue
            bidp = g.bbid.values
            win = g.winner.values.astype(bool)
            for label, filled in [
                ("optimistic", g.min_ask_after.values <= bidp + 1e-9),
                ("conservative", g.min_ask_after.values < bidp - 1e-9)]:
                nf = int(filled.sum())
                row = {"month": month, "series": series, "W": W, "P": P,
                       "mode": label, "n_signals": n, "n_filled": nf}
                if nf:
                    pnl_f = np.where(win[filled], 1 - bidp[filled], -bidp[filled])
                    row.update({
                        "fill_rate": nf / n,
                        "wr_filled": float(win[filled].mean()),
                        "wr_unfilled": float(win[~filled].mean()) if nf < n else np.nan,
                        "avg_pnl_filled": float(pnl_f.mean()),
                        "ev_per_posted": float(pnl_f.sum() / n),
                    })
                mres.append(row)
pd.DataFrame(mres).to_csv(DATA + r"\maker_combined.csv", index=False)
print("done")
