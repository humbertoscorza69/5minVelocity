"""AUDIT of the taker lead-lag strategy for look-ahead / leakage / fake fills.
June only (authoritative CLOB winners, independent of the Binance signal).

Checks:
 A) CAUSALITY/DELAY sweep: net edge entering at ask[signal_sec + d] for
    d = -5..+10s. d<0 = entering BEFORE the signal existed (placebo -> if edge
    appears here, there is leakage). Real microstructure lag => edge is max at
    d~0-1 and DECAYS smoothly; an alignment artifact would be flat/discontinuous.
 B) FILLABILITY vs REAL TRADES: did real prints occur at/through our entry ask
    near entry time? Compare a1 to traded VWAP in [entry, entry+3s].
 C) PHYSICS: observed win vs random-walk P = Phi(disp/(vol*sqrt(ttl))).
    Observed >> physics (with no momentum) would flag label leakage.
 D) WINNER-SHUFFLE placebo: randomize winners -> edge must vanish (~ -cost).
 E) SANITY: entry ttl distribution, spread at entry, ask source.
"""
import glob
import json
import os

import numpy as np
import pandas as pd
from scipy.stats import norm

pd.set_option("display.width", 240)
DATA = r"C:\Users\tico_\Fable\5minSnip\data"
FEE = 0.07
V, DLO, DHI = 5.0, 2.0, 10.0   # candidate rule: vel>=5, displacement band 2-10

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

def local_vol(asset, sec, lookback=60):
    ot, cl = B[asset]
    i = np.searchsorted(ot, sec * 1000, side="right") - 1
    if i < lookback + 1:
        return np.nan
    seg = cl[i - lookback:i]
    r = np.diff(np.log(seg))
    return np.std(r) * 1e4

bs = pd.read_parquet(DATA + r"\ll_booksec.parquet")
bs = bs[(bs.ba < 1.0) & (bs.bb > 0) & (bs.ba >= bs.bb)]
tokidx = json.load(open(DATA + r"\filtered\token_index.json"))
idx2tid = {v: k for k, v in tokidx.items()}

# build per-market FIRST entry under candidate rule, plus ask series around entry
DELTAS = list(range(-5, 11))
entries = []
ask_by_d = {dd: [] for dd in DELTAS}
for tok, g in bs.groupby("tok", sort=False):
    g = g.sort_values("ttl", ascending=False)
    asset = g.asset.iloc[0]; side = g.side.iloc[0]
    winner = int(g.winner.iloc[0]); settle_ms = int(g.settle_ms.iloc[0])
    settle_sec = settle_ms // 1000
    ttl = g.ttl.values; tmax = int(ttl.max()); grid = np.arange(tmax, -1, -1)
    ba = pd.Series(g.ba.values, index=ttl).reindex(grid).ffill()
    bb = pd.Series(g.bb.values, index=ttl).reindex(grid).ffill()
    ba_by_ttl = ba.to_dict(); bb_by_ttl = bb.to_dict()
    sec = settle_sec - grid
    upx = px(asset, sec); sgn = 1.0 if side == "up" else -1.0
    open_px = px(asset, np.array([settle_sec - 300]))[0]
    if not np.isfinite(open_px):
        continue
    n = len(grid); vel2 = np.full(n, np.nan)
    vel2[2:] = sgn * (upx[2:] / upx[:-2] - 1) * 1e4
    disp = sgn * (upx / open_px - 1) * 1e4
    # first qualifying instant
    for i in range(n):
        t = grid[i]
        if not (5 <= t <= 120) or not np.isfinite(vel2[i]) or not np.isfinite(disp[i]):
            continue
        if vel2[i] >= V and DLO <= disp[i] <= DHI:
            entries.append({"tok": tok, "token_id": idx2tid[tok], "asset": asset,
                            "side": side, "ttl": t, "entry_sec": int(sec[i]),
                            "settle_sec": settle_sec, "disp": disp[i],
                            "vel2": vel2[i], "win": winner,
                            "vol": local_vol(asset, int(sec[i]))})
            for dd in DELTAS:
                te = t - dd            # ttl at entry+dd seconds
                ask_by_d[dd].append(ba_by_ttl.get(te, np.nan))
            break
E = pd.DataFrame(entries)
for dd in DELTAS:
    E[f"ask_d{dd}"] = ask_by_d[dd]
print(f"candidate-rule FIRST entries (June): {len(E)}  "
      f"({len(E)/E.settle_sec.nunique() if E.settle_sec.nunique() else 0:.1f} per settle-sec)")
print(f"entries/day ~ {len(E)/7:.0f}")

def netc(ask, win):
    ask = np.asarray(ask, float)
    fee = FEE * ask * (1 - ask)
    return (win - ask - fee) * 100

print("\n" + "=" * 80)
print("A) CAUSALITY/DELAY SWEEP (d<0 = enter BEFORE signal = placebo/leak test)")
for dd in DELTAS:
    a = E[f"ask_d{dd}"].values
    m = np.isfinite(a)
    if m.sum() < 50:
        continue
    nc = netc(a[m], E.win.values[m])
    flag = "  <-- placebo (must NOT be tradable)" if dd < 0 else ""
    print(f"  d={dd:+d}s: net={nc.mean():+6.2f}c  win={E.win.values[m].mean():.3f}  "
          f"ask={np.nanmean(a[m]):.3f}  n={m.sum()}{flag}")

print("\n" + "=" * 80)
print("C) PHYSICS: observed win vs random-walk Phi(disp/(vol*sqrt(ttl)))")
ev = E[E.vol.notna() & (E.vol > 0)].copy()
ev["z_phys"] = ev.disp / (ev.vol * np.sqrt(ev.ttl))
ev["p_phys"] = norm.cdf(ev.z_phys)
ev["zb"] = pd.cut(ev.z_phys, [-5, 0, 0.25, 0.5, 1, 2, 5])
g = ev.groupby("zb", observed=True).agg(n=("win", "size"),
                                        pred=("p_phys", "mean"),
                                        obs=("win", "mean"))
print(g.to_string())
print(f"  overall: predicted={ev.p_phys.mean():.3f}  observed={ev.win.mean():.3f}")

print("\n" + "=" * 80)
print("D) WINNER-SHUFFLE placebo (randomize wins -> edge must vanish):")
rng = np.random.default_rng(0)
for _ in range(3):
    shuf = rng.permutation(E.win.values)
    print(f"  shuffled net @ d=1: {netc(E.ask_d1.values, shuf).mean():+.2f}c "
          f"(real: {netc(E.ask_d1.values, E.win.values).mean():+.2f}c)")

print("\n" + "=" * 80)
print("E) SANITY: entry ttl & spread")
# spread at entry
sp = []
for _, r in E.iterrows():
    sp.append(r.ask_d0)
print(f"  entry ttl: median={E.ttl.median():.0f} p10={E.ttl.quantile(.1):.0f} "
      f"p90={E.ttl.quantile(.9):.0f}")
print(f"  entry ask: median={E.ask_d1.median():.3f} "
      f"p10={E.ask_d1.quantile(.1):.3f} p90={E.ask_d1.quantile(.9):.3f}")
E.to_parquet(DATA + r"\audit_entries.parquet")

print("\n" + "=" * 80)
print("B) FILLABILITY vs REAL TRADES (did prints occur at/through entry ask?)")
tr = pd.read_parquet(DATA + r"\trades_june5m.parquet").rename(columns={"asset": "token_id"})
tr = tr.sort_values("ts")
fill_ok = 0; checked = 0; better = 0; vwap_diff = []
buys = tr[tr.side == "BUY"]
grp = {k: v for k, v in buys.groupby("token_id")}
for _, r in E.iterrows():
    g = grp.get(r.token_id)
    if g is None:
        continue
    w = g[(g.ts >= r.entry_sec) & (g.ts <= r.entry_sec + 3)]
    if not len(w):
        continue
    checked += 1
    # could we have bought at ~a1? a real BUY at price >= our ask means ask liq existed there
    a1 = r.ask_d1
    if (w.price <= a1 + 0.011).any():     # a buy executed at/below our ask+1tick
        fill_ok += 1
    vw = (w.price * w["size"]).sum() / w["size"].sum()
    vwap_diff.append(vw - a1)
    if vw <= a1 + 1e-9:
        better += 1
print(f"  entries with BUY prints in [entry,entry+3s]: {checked}/{len(E)}")
if checked:
    print(f"  fraction where a real buy printed at <= our ask+1tick: {fill_ok/checked:.2%}")
    print(f"  fraction where traded VWAP <= our entry ask: {better/checked:.2%}")
    print(f"  mean(traded VWAP - our ask): {np.mean(vwap_diff)*100:+.2f}c "
          f"(>0 => market paid MORE than we modeled = our fill is conservative)")
