"""Final summary statistics for the research report."""
import numpy as np
import pandas as pd

pd.set_option("display.width", 250)
DATA = r"C:\Users\tico_\Fable\5minSnip\data"
FEE = 0.07

fav = pd.read_parquet(DATA + r"\favorites_all.parquet")
fav["grp"] = fav.series.str.split("-").str[1]
f5 = fav[fav.grp == "5m"]

print("=" * 70)
print("1) WHERE THE MONEY GOES: decomposition at reference cells (May, 5m pooled)")
for W, P in [(10, 0.90), (10, 0.95), (30, 0.95), (60, 0.95), (120, 0.90)]:
    g = f5[(f5.month == "may") & (f5.off == W) & (f5["mid"] >= P) &
           (f5.bask < 1.0) & f5.bask.notna()]
    if not len(g):
        continue
    mid = g["mid"].values
    ask = g.bask.values
    fee = FEE * ask * (1 - ask)
    wr = g.winner.mean()
    print(f"W={W:>3} P={P}: n={len(g):>5} avg_mid={mid.mean():.4f} "
          f"win_rate={wr:.4f} (mid calib err={wr - mid.mean():+.4f}) | "
          f"avg_ask={ask.mean():.4f} half_spread={(ask - mid).mean():.4f} "
          f"avg_fee={fee.mean():.4f} | EV_vs_mid={wr - mid.mean():+.4f} "
          f"EV_vs_ask={wr - ask.mean():+.4f} EV_net={wr - (ask + fee).mean():+.4f}")

print()
print("=" * 70)
print("2) CALIBRATION: P(win) vs favorite mid, pooled 5m, by offset (May)")
bins = [0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.98, 0.995, 1.0001]
g = f5[(f5.month == "may") & f5["mid"].notna() & (f5["mid"] >= 0.5)]
for off in [120, 60, 30, 10, 5, 1]:
    go = g[g.off == off]
    b = pd.cut(go["mid"], bins, right=False)
    agg = go.groupby(b, observed=True).agg(n=("winner", "size"),
                                           wr=("winner", "mean"),
                                           avg_mid=("mid", "mean"))
    line = f"  off={off:>3}s: "
    for iv, r in agg.iterrows():
        line += f"[{iv.left:.2f}: wr={r.wr:.3f} vs mid={r.avg_mid:.3f} n={int(r.n)}] "
    print(line)

print()
print("=" * 70)
print("3) FAILURE ANATOMY (favorite mid>=0.90 at some window, lost; both months)")
fc = pd.read_csv(DATA + r"\failure_cases.csv")
fc5 = fc[fc.series.str.contains("5m")]
print(f"5m failures: {len(fc5)} | 15m failures: {len(fc) - len(fc5)}")
print("\nWhen did the doomed favorite still look strong? (5m failures)")
for c in ["mid_120", "mid_60", "mid_30", "mid_10", "mid_5", "mid_1"]:
    v = fc5[c].dropna()
    print(f"  {c:>8}: median={v.median():.3f} p25={v.quantile(.25):.3f} "
          f"p75={v.quantile(.75):.3f} frac>=0.9={float((v >= 0.9).mean()):.2f}")
fl = fc5.flip_s_before_settle.dropna()
print(f"\nflip time before settle (s): n_with_flip={len(fl)}/{len(fc5)} "
      f"median={fl.median():.1f} p10={fl.quantile(.1):.1f} p90={fl.quantile(.9):.1f}")
print(f"never flipped before settle (won on the print): {len(fc5) - len(fl)}")
rb = fc5.reversal_bps_last60.dropna()
print(f"\nunderlying reversal in final 60s (bps): median={rb.median():.2f} "
      f"p10={rb.quantile(.1):.2f} p90={rb.quantile(.9):.2f}")
print(f"|move at t-60 vs ref| (bps) for failures: "
      f"median={fc5.bps_t60.abs().median():.2f}")
amb = fc5[fc5.bps_t60.abs() < 5]
print(f"failures where lead at t-60 was <5bps: {len(amb)} ({len(amb)/len(fc5):.0%})")

print()
print("=" * 70)
print("4) EXECUTION: REST depth near settlement (5m favorites)")
bd = pd.read_csv(DATA + r"\book_depth_by_ttl.csv")
b5 = bd[bd.series.str.contains("5m")]
cols = ["series", "ttl_bin", "n_books", "spread_med", "ask_at_best_med",
        "ask_2c_med", "no_ask_frac", "n_hc", "hc_ask_at_best_med", "hc_spread_med"]
print(b5[cols].to_string(index=False))

print()
print("5) STALENESS BIAS (REST truth vs last event-embedded quote)")
st = pd.read_csv(DATA + r"\staleness_bias.csv")
print(st.to_string(index=False))

print()
print("6) IMBALANCE (REST total book usd, favorites>=0.8): wr high vs low imb")
im = pd.read_csv(DATA + r"\imbalance_results.csv")
im5 = im[im.series.str.contains("5m")]
print(im5.to_string(index=False))

print()
print("7) LABEL-NOISE SENSITIVITY: May worst-case (all ambiguous = losses)")
swc = pd.read_csv(DATA + r"\sweep_combined.csv")
wc = swc[(swc.subset == "may_worstcase") & (swc.series.isin(["btc-5m", "eth-5m"]))
         & swc.P.isin([0.90, 0.95]) & swc.W.isin([10, 30, 60])]
print(wc[["series", "W", "P", "n", "win_rate", "expectancy_per_share"]].to_string(index=False))

print()
print("8) DRAWDOWN / STREAKS at representative cells (May, observed)")
sw = swc[(swc.subset == "may") & swc.series.isin(["btc-5m", "eth-5m"])
         & swc.P.isin([0.90, 0.95, 0.98]) & swc.W.isin([10, 30, 60])]
print(sw[["series", "W", "P", "n", "win_rate", "expectancy_per_share",
          "max_drawdown_sh", "max_consec_losses", "breakeven_wr"]].to_string(index=False))
