"""Capacity + cost stress for the at-the-money lead-lag hold strategy.

1) At-the-money depth near settlement (REST, June): shares fillable.
2) Threshold curve: trades/day, exp/trade (d=1), total cents/day, at +1 tick slip.
3) Daily-P&L Sharpe.
"""
import numpy as np
import pandas as pd

pd.set_option("display.width", 240)
DATA = r"C:\Users\tico_\Fable\5minSnip\data"
FEE = 0.07

# ---- 1) at-the-money depth ----
rb = pd.read_parquet(DATA + r"\restbook.parquet")
rb["mid"] = (rb.bp0 + rb.ap0) / 2
atm = rb[(rb.mid >= 0.35) & (rb.mid <= 0.65) & rb.ap0.notna()]
ap = atm[[f"ap{i}" for i in range(10)]].values
asz = atm[[f"as{i}" for i in range(10)]].values
best = ap[:, [0]]
within2 = np.where(np.isnan(asz), 0, asz * (ap <= best + 0.02 + 1e-9)).sum(1)
print("AT-THE-MONEY ask depth (mid 0.35-0.65), shares = $ at $1 payout:")
print(f"  best-level size: median {np.nanmedian(asz[:,0]):.0f}  "
      f"p25 {np.nanpercentile(asz[:,0],25):.0f}  p75 {np.nanpercentile(asz[:,0],75):.0f}")
print(f"  within 2c of ask: median {np.median(within2):.0f}  "
      f"p25 {np.percentile(within2,25):.0f}")
print(f"  (n={len(atm)} ATM snapshots)")

# ---- 2) threshold curve ----
def curve(df, label, ndays, slip):
    print(f"\n{label} (entry +1s, +{slip*100:.0f} tick slip, {ndays} days):")
    print(f"  {'sig>=':>6}{'trades/day':>11}{'win':>7}{'ask':>7}"
          f"{'exp/trade(c)':>14}{'cents/day':>11}{'ROI/trade':>10}")
    for s in [1, 1.5, 2, 3, 5, 8]:
        sub = df[(df.sig >= s) & df.a1.notna() & (df.a1 < 1.0)]
        if len(sub) < 30:
            continue
        ask = np.minimum(sub.a1.values + slip, 0.999)
        fee = FEE * ask * (1 - ask)
        pnl = sub.win.values - ask - fee
        tpd = len(sub) / ndays
        print(f"  {s:>6}{tpd:>11.0f}{sub.win.mean():>7.3f}{ask.mean():>7.3f}"
              f"{pnl.mean()*100:>+14.2f}{pnl.mean()*100*tpd:>+11.1f}"
              f"{pnl.mean()/ (ask.mean()):>+10.3f}")

mr = pd.read_parquet(DATA + r"\ll_rec_may.parquet")
jr = pd.read_parquet(DATA + r"\ll_rec_june.parquet")
curve(mr, "MAY", mr.day.nunique(), 0.0)
curve(mr, "MAY", mr.day.nunique(), 0.01)
curve(jr, "JUNE", jr.day.nunique(), 0.0)
curve(jr, "JUNE", jr.day.nunique(), 0.01)

# ---- 3) daily Sharpe (per-trade pnl aggregated to daily mean, sig>=2, d=1) ----
print("\n" + "=" * 70)
print("Daily mean-P&L Sharpe (sig>=2, d=1, no slip):")
for label, df in [("may", mr), ("june", jr)]:
    sub = df[(df.sig >= 2) & df.a1.notna()]
    ask = sub.a1.values
    fee = FEE * ask * (1 - ask)
    sub = sub.assign(pnl=sub.win.values - ask - fee)
    daily = sub.groupby("day").pnl.mean()
    sharpe = daily.mean() / daily.std()
    print(f"  {label}: daily mean {daily.mean()*100:+.2f}c, "
          f"std {daily.std()*100:.2f}c, day-Sharpe {sharpe:.2f}, "
          f"n_days {len(daily)}, worst day {daily.min()*100:+.2f}c")

# annualized rough: trades*edge
print("\nRough scale (May, sig>=2, d=1, +1tick): ")
sub = mr[(mr.sig >= 2) & mr.a1.notna()]
ask = np.minimum(sub.a1.values + 0.01, 0.999)
fee = FEE * ask * (1 - ask)
pnl = sub.win.values - ask - fee
print(f"  {len(sub)} trades over {mr.day.nunique()} days = "
      f"{len(sub)/mr.day.nunique():.0f}/day; net {pnl.mean()*100:+.2f}c/share; "
      f"at $100/clip -> ${pnl.mean()*100:.2f}/trade, "
      f"${pnl.mean()*100*len(sub)/mr.day.nunique():.0f}/day gross of capacity limits")
