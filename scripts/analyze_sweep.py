"""Parameter sweep + calibration + maker analysis from checkpoints.parquet.

Decision at offset W seconds before settlement:
  favorite = token with higher mid (reported best bid/ask at last event <= T)
  signal: mid_fav >= threshold P, tradeable: ask_fav < 1.0
  taker entry at ask_fav, fee = FEE_RATE * p * (1-p)  (taker, crypto markets)
Outputs:
  data/sweep_results.csv      per (series, W, P) metrics
  data/sweep_trades.parquet   every simulated trade (for failure analysis)
  data/calibration.csv        P(win | favorite mid bucket, offset)
  data/maker_results.csv      maker-at-bid fill proxy results
"""
import json
import math
import numpy as np
import pandas as pd

DATA = r"C:\Users\tico_\Fable\5minSnip\data"
FEE_RATE = 0.07

cp = pd.read_parquet(DATA + r"\checkpoints.parquet")
print("checkpoint rows:", len(cp), "tokens:", cp.tok.nunique())

# canonical top-of-book: reported best (exchange truth)
cp["bbid"] = cp.bb_rep
cp["bask"] = cp.ba_rep
cp["mid"] = (cp.bbid + cp.bask) / 2
cp["series"] = cp.asset + "-" + cp.interval
cp["cid_key"] = cp.asset + "-" + cp.interval + "-" + cp.settle_ts.astype(str)

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

def fee(p, rate=FEE_RATE):
    return rate * p * (1 - p)

# ---- build per-market-per-offset favorite table ----
rows = []
for off, g in cp.groupby("off"):
    fav = (g.dropna(subset=["mid"])
            .sort_values("mid", ascending=False)
            .drop_duplicates("cid_key", keep="first")
            .reset_index(drop=True))
    fav["off"] = off
    rows.append(fav)
fav = pd.concat(rows, ignore_index=True)
fav.to_parquet(DATA + r"\favorites.parquet")
print("favorite rows:", len(fav))

# ---- calibration ----
bins = [0.5, 0.6, 0.7, 0.8, 0.85, 0.9, 0.92, 0.95, 0.97, 0.98, 0.99, 0.995, 1.0001]
cal = []
for (series, off), g in fav.groupby(["series", "off"]):
    g = g.dropna(subset=["mid"])
    g = g[g["mid"] >= 0.5]
    b = pd.cut(g["mid"], bins, right=False)
    agg = g.groupby(b, observed=True).agg(n=("winner", "size"), wins=("winner", "sum"),
                                          avg_mid=("mid", "mean"))
    for iv, r in agg.iterrows():
        lo, hi = wilson(r.wins, r.n)
        cal.append({"series": series, "off": off, "bucket": str(iv),
                    "n": int(r.n), "wins": int(r.wins),
                    "win_rate": r.wins / r.n if r.n else np.nan,
                    "avg_mid": r.avg_mid, "wr_lo": lo, "wr_hi": hi})
pd.DataFrame(cal).to_csv(DATA + r"\calibration.csv", index=False)

# ---- taker sweep ----
res = []
trades_all = []
for series, gs in fav.groupby("series"):
    for W in WINDOWS:
        g0 = gs[gs.off == W]
        for P in THRESHOLDS:
            g = g0[(g0["mid"] >= P) & (g0.bask < 1.0) & g0.bask.notna()].copy()
            n = len(g)
            skipped_untradeable = int(((g0["mid"] >= P) & ~((g0.bask < 1.0) & g0.bask.notna())).sum())
            if n == 0:
                res.append({"series": series, "W": W, "P": P, "n": 0,
                            "skipped_untradeable": skipped_untradeable})
                continue
            price = g.bask.values
            f = fee(price)
            win = g.winner.values.astype(bool)
            pnl = np.where(win, 1 - price - f, -(price + f))
            cost = price + f
            roc = pnl / cost
            g = g.assign(entry_price=price, fee=f, pnl=pnl, roc=roc, W=W, P=P)
            trades_all.append(g)
            # sequence stats
            o = np.argsort(g.settle_ts.values, kind="stable")
            pnl_seq = pnl[o]
            cum = np.cumsum(pnl_seq)
            peak = np.maximum.accumulate(np.concatenate([[0], cum]))[1:]
            mdd = float(np.max(peak - cum)) if n else 0.0
            # consecutive losses
            maxcl = cl = 0
            for w_ in win[o]:
                cl = 0 if w_ else cl + 1
                maxcl = max(maxcl, cl)
            k = int(win.sum())
            wlo, whi = wilson(k, n)
            avg_win = float(pnl[win].mean()) if k else np.nan
            avg_loss = float(-pnl[~win].mean()) if k < n else np.nan
            exp_sh = float(pnl.mean())
            sd = float(pnl.std(ddof=1)) if n > 1 else np.nan
            res.append({
                "series": series, "W": W, "P": P, "n": n,
                "skipped_untradeable": skipped_untradeable,
                "wins": k, "losses": n - k,
                "win_rate": k / n, "wr_lo": wlo, "wr_hi": whi,
                "avg_entry": float(price.mean()),
                "avg_win": avg_win, "avg_loss": avg_loss,
                "expectancy_per_share": exp_sh,
                "exp_ci_lo": exp_sh - 1.96 * sd / math.sqrt(n) if n > 1 else np.nan,
                "exp_ci_hi": exp_sh + 1.96 * sd / math.sqrt(n) if n > 1 else np.nan,
                "mean_roc": float(roc.mean()),
                "total_pnl_per_share": float(pnl.sum()),
                "max_drawdown_sh": mdd,
                "max_consec_losses": maxcl,
                "breakeven_wr": float(cost.mean()),
                "t_stat": exp_sh / (sd / math.sqrt(n)) if n > 1 and sd > 0 else np.nan,
            })
sweep = pd.DataFrame(res)
sweep.to_csv(DATA + r"\sweep_results.csv", index=False)
trades = pd.concat(trades_all, ignore_index=True) if trades_all else pd.DataFrame()
trades.to_parquet(DATA + r"\sweep_trades.parquet")
print("sweep rows:", len(sweep), "trade rows:", len(trades))

# ---- maker analysis: post at best bid of favorite at T ----
mres = []
for series, gs in fav.groupby("series"):
    for W in WINDOWS:
        g0 = gs[gs.off == W]
        for P in [0.90, 0.95, 0.97]:
            g = g0[(g0["mid"] >= P) & g0.bbid.notna()].copy()
            n = len(g)
            if n == 0:
                continue
            bidp = g.bbid.values
            filled_opt = g.min_ask_after.values <= bidp + 1e-9
            filled_con = g.min_ask_after.values < bidp - 1e-9
            win = g.winner.values.astype(bool)
            for label, filled in [("optimistic", filled_opt), ("conservative", filled_con)]:
                nf = int(filled.sum())
                if nf == 0:
                    mres.append({"series": series, "W": W, "P": P, "mode": label,
                                 "n_signals": n, "n_filled": 0})
                    continue
                pnl_f = np.where(win[filled], 1 - bidp[filled], -bidp[filled])
                ev_posted = float(np.sum(pnl_f) / n)
                mres.append({
                    "series": series, "W": W, "P": P, "mode": label,
                    "n_signals": n, "n_filled": nf, "fill_rate": nf / n,
                    "wr_filled": float(win[filled].mean()),
                    "wr_unfilled": float(win[~filled].mean()) if nf < n else np.nan,
                    "avg_pnl_filled": float(pnl_f.mean()),
                    "ev_per_posted": ev_posted,
                })
pd.DataFrame(mres).to_csv(DATA + r"\maker_results.csv", index=False)
print("maker rows:", len(mres))
print("done")
