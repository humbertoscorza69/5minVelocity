"""Hard scrutiny of the two surviving families.

A) F4 underdog:
   - June-only (authoritative winners), pooled windows, B/A grid
   - May split by label confidence; worst-case (every non-confident = loss)
   - day-by-day P&L stability (May)
B) F1 short-horizon favorite (W=5, P=0.85, z grid):
   - quote age at entry, slippage haircut (+1 tick / +2 ticks)
   - per-asset and per-day breakdown, June daily
   - capacity: trades/day
"""
import math

import numpy as np
import pandas as pd

pd.set_option("display.width", 260)
DATA = r"C:\Users\tico_\Fable\5minSnip\data"
FEE = 0.07

d = pd.read_parquet(DATA + r"\decisions.parquet")
d = d[d.series.str.split("-").str[1] == "5m"]
d["day"] = pd.to_datetime(d.settle_ts, unit="s").dt.strftime("%m-%d")

def wilson_lo(k, n, z=1.96):
    if n == 0:
        return np.nan
    p = k / n
    den = 1 + z * z / n
    c = p + z * z / (2 * n)
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return (c - h) / den

def met(g, slip=0.0):
    n = len(g)
    if n == 0:
        return None
    price = np.minimum(g.bask.values + slip, 0.999)
    fee = FEE * price * (1 - price)
    win = g.winner.values.astype(bool)
    k = int(win.sum())
    pnl = np.where(win, 1 - price - fee, -(price + fee))
    return {"n": n, "wins": k, "wr": k / n, "wr_lo": wilson_lo(k, n),
            "avg_ask": float(price.mean()), "exp": float(pnl.mean()),
            "roc": float((pnl / (price + fee)).mean()), "tot": float(pnl.sum())}

print("=" * 100)
print("A) F4 UNDERDOG — JUNE ONLY (authoritative winners)")
ud = d[~d.is_fav & d.bask.notna() & (d.bask > 0) & (d.bask < 0.5) &
       (d.fav_mid >= 0.90) & d.bps_at_off.notna()]
uj = ud[ud.month == "june"]
for Wset, lab in [([5], "W=5"), ([5, 10, 15], "W=5,10,15"),
                  ([5, 10, 15, 30, 45, 60], "W=5..60")]:
    for B in [0.5, 1, 2, 3]:
        g = uj[uj.off.isin(Wset) & (uj.bps_at_off.abs() <= B) & (uj.bask <= 0.30)]
        # one trade per market per window-set: dedup by (cid_key) keep smallest W
        g = g.sort_values("off").drop_duplicates("cid_key", keep="first")
        m = met(g)
        if m:
            print(f"  {lab:>12} |bps|<={B}: n={m['n']:>4} wr={m['wr']:.3f} "
                  f"avg_ask={m['avg_ask']:.4f} exp={m['exp']:+.4f} "
                  f"roc={m['roc']:+.3f} total={m['tot']:+.2f}")

print("\nJune underdog trades by side (W<=15, |bps|<=2, ask<=0.30):")
g = uj[uj.off.isin([5, 10, 15]) & (uj.bps_at_off.abs() <= 2) & (uj.bask <= 0.30)]
g = g.sort_values("off").drop_duplicates("cid_key", keep="first")
for side, gs in g.groupby("side"):
    m = met(gs)
    print(f"  {side}: n={m['n']} wr={m['wr']:.3f} exp={m['exp']:+.4f}")

print("\nA2) F4 May by label confidence (W=5, |bps|<=1, ask<=0.30):")
um = ud[(ud.month == "may") & (ud.off == 5) & (ud.bps_at_off.abs() <= 1) &
        (ud.bask <= 0.30)]
for conf, gs in um.groupby(um.confident.astype(bool)):
    m = met(gs)
    print(f"  confident={conf}: n={m['n']} wr={m['wr']:.3f} "
          f"avg_ask={m['avg_ask']:.4f} exp={m['exp']:+.4f}")
wc = um.copy()
wc.loc[~wc.confident.astype(bool), "winner"] = 0
m = met(wc)
print(f"  WORST-CASE (non-confident=loss): n={m['n']} wr={m['wr']:.3f} "
      f"exp={m['exp']:+.4f}")
bc = um.copy()
bc.loc[~bc.confident.astype(bool), "winner"] = 1
m = met(bc)
print(f"  BEST-CASE  (non-confident=win) : n={m['n']} wr={m['wr']:.3f} "
      f"exp={m['exp']:+.4f}")

print("\nA3) F4 May day-by-day (W=5, |bps|<=2, ask<=0.30):")
um2 = ud[(ud.month == "may") & (ud.off == 5) & (ud.bps_at_off.abs() <= 2) &
         (ud.bask <= 0.30)]
dd = um2.groupby("day").apply(
    lambda g: pd.Series(met(g)), include_groups=False)
print(dd[["n", "wr", "exp", "tot"]].to_string())

print()
print("=" * 100)
print("B) F1 SHORT-HORIZON FAVORITE (W=5, P=0.85)")
f1 = d[d.is_fav & (d.off == 5) & (d["mid"] >= 0.85) & (d.bask < 1.0) &
       d.bask.notna() & d.z.notna()]
print("\nB1) quote age at entry (ms), May, z>=1:")
g = f1[(f1.month == "may") & (f1.z >= 1)]
print(g.age_ms.describe(percentiles=[.5, .9, .99]).to_string())

print("\nB2) slippage haircuts (entry at ask + k ticks), z grid, May -> June:")
for Z in [1, 2, 3]:
    for slip in [0.0, 0.01, 0.02]:
        mm = met(f1[(f1.month == "may") & (f1.z >= Z)], slip)
        mj = met(f1[(f1.month == "june") & (f1.z >= Z)], slip)
        print(f"  z>={Z} slip={slip:.2f}: MAY exp={mm['exp']:+.4f} (n={mm['n']}) | "
              f"JUNE exp={mj['exp']:+.4f} (n={mj['n']})")

print("\nB3) per-asset (May, z>=1):")
for a, gs in f1[(f1.month == "may") & (f1.z >= 1)].groupby(
        f1.series.str.split("-").str[0]):
    m = met(gs)
    print(f"  {a}: n={m['n']} wr={m['wr']:.4f} exp={m['exp']:+.4f}")

print("\nB4) day-by-day (z>=1, both months):")
g = f1[f1.z >= 1]
dd = g.groupby(["month", "day"]).apply(
    lambda x: pd.Series(met(x)), include_groups=False)
print(dd[["n", "wr", "exp", "tot"]].to_string())

print("\nB5) stale-quote sensitivity: drop entries with age_ms > 2000 (May/June, z>=1):")
for month in ["may", "june"]:
    g0 = f1[(f1.month == month) & (f1.z >= 1)]
    m_all = met(g0)
    m_fresh = met(g0[g0.age_ms <= 2000])
    print(f"  {month}: all n={m_all['n']} exp={m_all['exp']:+.4f} | "
          f"fresh n={m_fresh['n']} exp={m_fresh['exp']:+.4f}")
