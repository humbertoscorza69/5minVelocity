"""Displacement mechanism (repricing gap) + DCA (same-side pyramiding) analysis.

Builder carries tok, side, mkt, ttl, vel2 (2s spot move signed to token),
disp (spot displacement from window open, signed to token), a1 (ask +1s), win.

PART 1: repricing gap by displacement bucket (book ask vs true win rate).
PART 2: marginal edge of the 1st/2nd/3rd... SAME-SIDE qualifying entry.
PART 3: DCA portfolio (k_max adds) vs one-per-market: net, ROC, risk, drawdown.
IS=May, OOS=June.
"""
import glob
import os

import numpy as np
import pandas as pd

pd.set_option("display.width", 250)
DATA = r"C:\Users\tico_\Fable\5minSnip\data"
FEE = 0.07

def load_sym(sym):
    files = sorted(glob.glob(os.path.join(DATA, "binance", f"{sym}_*.parquet")))
    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    df = df.drop_duplicates("open_time").sort_values("open_time")
    return df.open_time.values.astype(np.int64), df.close.values.astype(float)
B = {"btc": load_sym("BTCUSDT"), "eth": load_sym("ETHUSDT")}

def px(asset, sec):
    ot, cl = B[asset]
    i = np.searchsorted(ot, sec * 1000, side="right") - 1
    v = np.where(i >= 0, cl[np.clip(i, 0, len(cl) - 1)], np.nan)
    gap = sec * 1000 - ot[np.clip(i, 0, len(cl) - 1)]
    return np.where((i >= 0) & (gap <= 5000), v, np.nan)

def build(path):
    bs = pd.read_parquet(path)
    bs = bs[(bs.ba < 1.0) & (bs.bb > 0) & (bs.ba >= bs.bb)]
    rows = {k: [] for k in ["mkt", "tok", "side", "asset", "day", "ttl",
                            "vel2", "disp", "a1", "win"]}
    for tok, g in bs.groupby("tok", sort=False):
        g = g.sort_values("ttl", ascending=False)
        asset = g.asset.iloc[0]; side = g.side.iloc[0]
        winner = int(g.winner.iloc[0]); settle_ms = int(g.settle_ms.iloc[0])
        mkt = f"{asset}_{settle_ms}"
        day = pd.to_datetime(settle_ms, unit="ms").strftime("%m-%d")
        ttl = g.ttl.values; tmax = int(ttl.max()); grid = np.arange(tmax, -1, -1)
        ba = pd.Series(g.ba.values, index=ttl).reindex(grid).ffill().values
        sec = (settle_ms // 1000) - grid
        upx = px(asset, sec); sgn = 1.0 if side == "up" else -1.0
        open_px = px(asset, np.array([settle_ms // 1000 - 300]))[0]
        n = len(grid); vel2 = np.full(n, np.nan)
        vel2[2:] = sgn * (upx[2:] / upx[:-2] - 1) * 1e4
        disp = sgn * (upx / open_px - 1) * 1e4 if np.isfinite(open_px) else np.full(n, np.nan)
        for i in range(n):
            t = grid[i]
            if not (5 <= t <= 120) or not np.isfinite(vel2[i]):
                continue
            rows["mkt"].append(mkt); rows["tok"].append(int(tok))
            rows["side"].append(side); rows["asset"].append(asset)
            rows["day"].append(day); rows["ttl"].append(int(t))
            rows["vel2"].append(vel2[i])
            rows["disp"].append(disp[i] if np.isfinite(disp[i]) else np.nan)
            rows["a1"].append(ba[i + 1] if i + 1 < n else np.nan)
            rows["win"].append(winner)
    return pd.DataFrame(rows)

mr = build(DATA + r"\ll_booksec_may.parquet"); mr["month"] = "may"
jr = build(DATA + r"\ll_booksec.parquet"); jr["month"] = "june"
both = pd.concat([mr, jr], ignore_index=True)
both = both[both.a1.notna() & (both.a1 < 1.0) & both.disp.notna()]

def fee(a):
    return FEE * a * (1 - a)

print("=" * 95)
print("PART 1 — REPRICING GAP by displacement (trigger vel2>=5). book ask vs TRUE win.")
print("  edge_gap = win_rate - ask  (how far the book lags the truth)")
for month in ["may", "june"]:
    d = both[(both.month == month) & (both.vel2 >= 5)]
    d = d.assign(db=pd.cut(d.disp, [-1e9, 0, 2, 5, 10, 20, 1e9]))
    g = d.groupby("db", observed=True).agg(
        n=("win", "size"), ask=("a1", "mean"), win=("win", "mean"))
    g["edge_gap"] = g.win - g.ask
    g["net_c"] = (g.win - g.ask - fee(g.ask)) * 100
    print(f"\n  {month}:")
    print(g.to_string())

print("\n" + "=" * 95)
print("PART 2 — MARGINAL EDGE of the k-th SAME-SIDE entry (vel2>=5 & disp>=2)")
def first_side_entries(df, V, D):
    """Per market: pick side from earliest qualifier; return same-side qualifiers
       ordered in time with an 'k' index."""
    q = df[(df.vel2 >= V) & (df.disp >= D)].copy()
    q = q.sort_values(["mkt", "ttl"], ascending=[True, False])
    recs = []
    for mkt, g in q.groupby("mkt", sort=False):
        side_tok = g.iloc[0].tok          # earliest qualifier's token = committed side
        gg = g[g.tok == side_tok]
        for k, (_, r) in enumerate(gg.iterrows(), 1):
            recs.append({**r.to_dict(), "k": k})
    return pd.DataFrame(recs)

for month, df in [("may", mr), ("june", jr)]:
    fe = first_side_entries(df[df.a1.notna() & (df.a1 < 1.0) & df.disp.notna()], 5, 2)
    print(f"\n  {month}: marginal entry by ordinal k (same side):")
    g = fe.groupby("k").agg(n=("win", "size"), ask=("a1", "mean"),
                            win=("win", "mean"))
    g["net_c"] = (g.win - g.ask - fee(g.ask)) * 100
    print(g[g.index <= 8].to_string())

print("\n" + "=" * 95)
print("PART 3 — DCA PORTFOLIO: cap adds at k_max. 1 share/entry. (vel2>=5 & disp>=2)")
def dca_portfolio(df, V, D, kmax):
    fe = first_side_entries(df[df.a1.notna() & (df.a1 < 1.0) & df.disp.notna()], V, D)
    fe = fe[fe.k <= kmax]
    fe["cost"] = fe.a1 + fee(fe.a1.values)
    fe["pnl"] = fe.win - fe.cost
    permkt = fe.groupby("mkt").agg(entries=("k", "max"), net=("pnl", "sum"),
                                   cap=("cost", "sum"))
    return permkt

for month, df in [("may", mr), ("june", jr)]:
    print(f"\n  {month}:")
    print(f"   {'k_max':>6}{'markets':>8}{'avg_ent':>8}{'tot_net':>9}"
          f"{'ROC':>8}{'net/mkt':>9}{'std/mkt':>9}{'Sharpe':>8}"
          f"{'worst':>8}{'loss%':>7}")
    for kmax in [1, 2, 3, 5, 99]:
        pm = dca_portfolio(df, 5, 2, kmax)
        net = pm.net.values
        roc = pm.net.sum() / pm.cap.sum()
        sharpe = net.mean() / net.std() if net.std() > 0 else np.nan
        print(f"   {kmax:>6}{len(pm):>8}{pm.entries.mean():>8.2f}"
              f"{pm.net.sum()*100:>+9.0f}{roc:>+8.3f}{net.mean()*100:>+9.2f}"
              f"{net.std()*100:>9.2f}{sharpe:>8.3f}{net.min()*100:>+8.0f}"
              f"{(net<0).mean()*100:>7.1f}")
print("\n(tot_net & worst in cents per 1-share clip; ROC=net/capital deployed)")
