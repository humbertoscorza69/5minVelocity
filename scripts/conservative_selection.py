"""Re-do IS selection with statistically honest criteria.

Conservative expectancy per share:
  EV_cons = wr_lo * avg_win_observed - (1 - wr_lo) * (avg_entry + fee)
  (Wilson 95% lower bound on win rate; losses assumed total.)
Also pools btc+eth 5m (and 15m) per (W, P) for larger n.
Selection on MAY only; OOS evaluation on June for any survivor.
"""
import math

import numpy as np
import pandas as pd

DATA = r"C:\Users\tico_\Fable\5minSnip\data"
FEE = 0.07

def wilson_lo(k, n, z=1.96):
    if n == 0:
        return np.nan
    p = k / n
    d = 1 + z * z / n
    c = p + z * z / (2 * n)
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return (c - h) / d

fav = pd.read_parquet(DATA + r"\favorites_all.parquet")
fav["grp"] = fav.series.str.split("-").str[1]  # 5m / 15m
WINDOWS = [5, 10, 15, 30, 45, 60, 90, 120]
THRESH = [0.80, 0.82, 0.85, 0.87, 0.90, 0.92, 0.95, 0.97, 0.98, 0.99]

def table(df, label):
    rows = []
    for scope, gscope in [("pooled-" + g, d) for g, d in df.groupby("grp")] + \
                         [(s, d) for s, d in df.groupby("series")]:
        for W in WINDOWS:
            g0 = gscope[gscope.off == W]
            for P in THRESH:
                g = g0[(g0["mid"] >= P) & (g0.bask < 1.0) & g0.bask.notna()]
                n = len(g)
                if n < 30:
                    continue
                price = g.bask.values
                fee = FEE * price * (1 - price)
                win = g.winner.values.astype(bool)
                k = int(win.sum())
                pnl = np.where(win, 1 - price - fee, -(price + fee))
                wlo = wilson_lo(k, n)
                avg_win = float((1 - price - fee)[win].mean()) if k else np.nan
                worst_loss = float((price + fee).mean())
                ev_cons = wlo * avg_win - (1 - wlo) * worst_loss
                rows.append({"subset": label, "scope": scope, "W": W, "P": P,
                             "n": n, "wins": k, "win_rate": k / n,
                             "wr_lo": wlo, "exp_obs": float(pnl.mean()),
                             "ev_cons": ev_cons,
                             "roc_obs": float((pnl / (price + fee)).mean())})
    return pd.DataFrame(rows)

may = table(fav[fav.month == "may"], "may")
june = table(fav[fav.month == "june"], "june")
out = pd.concat([may, june])
out.to_csv(DATA + r"\sweep_conservative.csv", index=False)

sel = may[may.ev_cons > 0].sort_values("ev_cons", ascending=False)
print("MAY configs with POSITIVE conservative EV:", len(sel))
if len(sel):
    print(sel.head(20).to_string(index=False))

print("\nBest MAY configs by observed expectancy (top 12):")
print(may.sort_values("exp_obs", ascending=False).head(12).to_string(index=False))

print("\nPooled-5m May grid (observed exp per share, fee-adjusted):")
g = may[may.scope == "pooled-5m"].pivot_table(index="W", columns="P",
                                              values="exp_obs")
print((g * 100).round(3).to_string())
print("\nPooled-5m May grid: conservative EV:")
g2 = may[may.scope == "pooled-5m"].pivot_table(index="W", columns="P",
                                               values="ev_cons")
print((g2 * 100).round(3).to_string())
print("\nPooled-5m May n per cell:")
g3 = may[may.scope == "pooled-5m"].pivot_table(index="W", columns="P", values="n")
print(g3.to_string())
