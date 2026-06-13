"""Execution-honest evaluation of the last surviving cells.

Conditions: fresh quote (age<=2s), slippage 0 / +1 tick, per asset.
Cells:
  C1: W=5,  mid>=0.85, z>=3
  C2: W=15, mid>=0.90, lead>=8bps
  C3: W=90, mid>=0.92, z>=4
  C4: W=15, mid>=0.85, z>=3   (combo of C1/C2 geometry)
Plus capacity stats and a pooled May+June bootstrap CI for the winner.
"""
import numpy as np
import pandas as pd

pd.set_option("display.width", 240)
DATA = r"C:\Users\tico_\Fable\5minSnip\data"
FEE = 0.07
rng = np.random.default_rng(11)

d = pd.read_parquet(DATA + r"\decisions.parquet")
d = d[d.series.str.split("-").str[1] == "5m"]
d["asset"] = d.series.str.split("-").str[0]
d["day"] = pd.to_datetime(d.settle_ts, unit="s").dt.strftime("%m-%d")

def met(g, slip=0.0):
    n = len(g)
    if n == 0:
        return {"n": 0}
    price = np.minimum(g.bask.values + slip, 0.999)
    fee = FEE * price * (1 - price)
    win = g.winner.values.astype(bool)
    pnl = np.where(win, 1 - price - fee, -(price + fee))
    return {"n": n, "wr": win.mean(), "avg_ask": price.mean(),
            "exp": pnl.mean(), "tot": pnl.sum(),
            "roc": (pnl / (price + fee)).mean()}

CELLS = {
    "C1 W=5 mid>=.85 z>=3":  (d.off == 5) & (d["mid"] >= 0.85) & (d.z >= 3),
    "C2 W=15 mid>=.90 lead>=8": (d.off == 15) & (d["mid"] >= 0.90) & (d.lead_bps >= 8),
    "C3 W=90 mid>=.92 z>=4": (d.off == 90) & (d["mid"] >= 0.92) & (d.z >= 4),
    "C4 W=15 mid>=.85 z>=3": (d.off == 15) & (d["mid"] >= 0.85) & (d.z >= 3),
}
base = d.is_fav & (d.bask < 1.0) & d.bask.notna()

print(f"{'cell':<28}{'variant':<26}{'MAY n':>7}{'exp':>9}{'JUNE n':>8}{'exp':>9}")
for name, cmask in CELLS.items():
    for vlab, extra, slip in [
        ("raw", base, 0.0),
        ("fresh<=2s", base & (d.age_ms <= 2000), 0.0),
        ("fresh + 1 tick slip", base & (d.age_ms <= 2000), 0.01),
        ("fresh+slip, BTC only", base & (d.age_ms <= 2000) & (d.asset == "btc"), 0.01),
    ]:
        g = d[cmask & extra]
        mm = met(g[g.month == "may"], slip)
        mj = met(g[g.month == "june"], slip)
        print(f"{name:<28}{vlab:<26}{mm['n']:>7}{mm.get('exp', float('nan')):>+9.4f}"
              f"{mj['n']:>8}{mj.get('exp', float('nan')):>+9.4f}")
    print()

# Winner candidate: pooled fresh+slip variant of best cell -> bootstrap
print("Bootstrap 95% CI (10k resamples) for selected variants, May+June pooled:")
for name, cmask in CELLS.items():
    g = d[cmask & base & (d.age_ms <= 2000)]
    if not len(g):
        continue
    price = np.minimum(g.bask.values + 0.01, 0.999)
    fee = FEE * price * (1 - price)
    win = g.winner.values.astype(bool)
    pnl = np.where(win, 1 - price - fee, -(price + fee))
    boots = np.array([pnl[rng.integers(0, len(pnl), len(pnl))].mean()
                      for _ in range(10_000)])
    print(f"  {name}: n={len(g)} exp={pnl.mean():+.4f} "
          f"CI=[{np.quantile(boots, 0.025):+.4f}, {np.quantile(boots, 0.975):+.4f}]")

# capacity
print("\nCapacity (trades/day, May, fresh, slip variant of each cell):")
for name, cmask in CELLS.items():
    g = d[cmask & base & (d.age_ms <= 2000) & (d.month == "may")]
    tpd = g.groupby("day").size()
    print(f"  {name}: mean {tpd.mean():.1f}/day (max {tpd.max()})")
