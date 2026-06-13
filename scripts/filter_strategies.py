"""Filtered strategy search. Selection on May IS only; June = OOS.

Families:
  F1a taker favorite + raw lead filter (lead_bps >= L)
  F1b taker favorite + vol-normalized lead (z >= Z)
  F3  F1b + spread<=0.01 + non-negative favorite momentum
  F4  photo-finish underdog: |move| small but favorite priced >= P -> buy underdog
  F5  maker-at-bid favorite + z filter (fill proxy, conservative)
  F2  (June-only, exploratory) book-imbalance filter, day-split validation
"""
import math

import numpy as np
import pandas as pd

pd.set_option("display.width", 260)
DATA = r"C:\Users\tico_\Fable\5minSnip\data"
FEE = 0.07

d = pd.read_parquet(DATA + r"\decisions.parquet")
d["grp"] = d.series.str.split("-").str[1]
d5 = d[d.grp == "5m"]

def wilson_lo(k, n, z=1.96):
    if n == 0:
        return np.nan
    p = k / n
    den = 1 + z * z / n
    c = p + z * z / (2 * n)
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return (c - h) / den

def taker_metrics(g):
    n = len(g)
    if n == 0:
        return None
    price = g.bask.values
    fee = FEE * price * (1 - price)
    win = g.winner.values.astype(bool)
    k = int(win.sum())
    pnl = np.where(win, 1 - price - fee, -(price + fee))
    wlo = wilson_lo(k, n)
    avg_win_v = float((1 - price - fee)[win].mean()) if k else 0.0
    worst = float((price + fee).mean())
    return {"n": n, "wins": k, "wr": k / n, "wr_lo": wlo,
            "avg_entry": float(price.mean()),
            "exp": float(pnl.mean()),
            "ev_cons": wlo * avg_win_v - (1 - wlo) * worst,
            "roc": float((pnl / (price + fee)).mean()),
            "tot": float(pnl.sum())}

WINDOWS = [5, 10, 15, 30, 45, 60, 90, 120]

def sweep_family(df, conds, name):
    """conds: list of (label, mask_fn(sub)) applied on top of (W,P) cells."""
    rows = []
    for month in ["may", "june"]:
        dm = df[df.month == month]
        for W in WINDOWS:
            g0 = dm[(dm.off == W) & dm.is_fav & (dm.bask < 1.0) & dm.bask.notna()]
            for P in [0.85, 0.90, 0.92, 0.95, 0.97, 0.98]:
                gP = g0[g0["mid"] >= P]
                for label, fn in conds:
                    m = taker_metrics(gP[fn(gP)])
                    if m:
                        rows.append({"family": name, "month": month, "W": W,
                                     "P": P, "filter": label, **m})
    return pd.DataFrame(rows)

# ---------- F1a / F1b / F3 ----------
f1a = sweep_family(d5, [(f"lead>={L}", lambda g, L=L: g.lead_bps >= L)
                        for L in [0, 3, 5, 8, 12, 20]], "F1a_lead")
f1b = sweep_family(d5, [(f"z>={Z}", lambda g, Z=Z: g.z >= Z)
                        for Z in [0.5, 1, 1.5, 2, 3, 4]], "F1b_z")
f3 = sweep_family(d5, [
    (f"z>={Z}&spr<=1c&mom>=0",
     lambda g, Z=Z: (g.z >= Z) & (g.spread <= 0.011) & (g.mom60.fillna(0) >= 0))
    for Z in [1, 2, 3]], "F3_combo")

allf = pd.concat([f1a, f1b, f3], ignore_index=True)
allf.to_csv(DATA + r"\filter_sweep.csv", index=False)

may_sel = allf[(allf.month == "may") & (allf.n >= 100) & (allf.ev_cons > 0)]
print("=" * 100)
print(f"F1/F3 May IS cells with conservative EV>0 (n>=100): {len(may_sel)}")
if len(may_sel):
    top = may_sel.sort_values("ev_cons", ascending=False).head(15)
    print(top[["family", "W", "P", "filter", "n", "wr", "avg_entry", "exp",
               "ev_cons"]].to_string(index=False))
    print("\n--- June OOS of those cells ---")
    for _, r in top.iterrows():
        o = allf[(allf.month == "june") & (allf.family == r.family) &
                 (allf.W == r.W) & (allf.P == r.P) & (allf["filter"] == r["filter"])]
        if len(o):
            o = o.iloc[0]
            print(f"{r.family} W={r.W} P={r.P} {r['filter']}: "
                  f"IS exp={r.exp:+.4f} (n={r.n}) -> OOS exp={o.exp:+.4f} "
                  f"(n={o.n}, wr={o.wr:.4f})")

# structure check: EV vs z bucket (pooled over W in 10..60, P>=0.90, May)
print("\nEV by z bucket (May, fav, W 10-60, mid>=0.90):")
gg = d5[(d5.month == "may") & d5.is_fav & d5.off.isin([10, 15, 30, 45, 60]) &
        (d5["mid"] >= 0.90) & (d5.bask < 1.0) & d5.bask.notna() & d5.z.notna()]
gg = gg.assign(zb=pd.cut(gg.z, [-np.inf, 0, 1, 2, 3, 5, np.inf]))
for zb, g in gg.groupby("zb", observed=True):
    m = taker_metrics(g)
    print(f"  z {str(zb):>14}: n={m['n']:>5} wr={m['wr']:.4f} "
          f"avg_ask={m['avg_entry']:.4f} exp={m['exp']:+.4f}")

# ---------- F4 underdog ----------
print("\n" + "=" * 100)
print("F4 photo-finish underdog (buy NON-favorite when |move| tiny, fav rich)")
ud = d5[~d5.is_fav & d5.bask.notna() & (d5.bask > 0) & (d5.bask < 0.5)]
rows = []
for month in ["may", "june"]:
    dm = ud[ud.month == month]
    for W in WINDOWS:
        g0 = dm[dm.off == W]
        for P in [0.90, 0.95]:
            for B in [0.5, 1, 2, 3]:
                for A in [0.08, 0.12, 0.20, 0.30]:
                    g = g0[(g0.fav_mid >= P) &
                           (g0.bps_at_off.abs() <= B) & (g0.bask <= A)]
                    m = taker_metrics(g)
                    if m and m["n"] >= 5:
                        rows.append({"month": month, "W": W, "P": P, "B": B,
                                     "A": A, **m})
f4 = pd.DataFrame(rows)
f4.to_csv(DATA + r"\f4_underdog.csv", index=False)
sel4 = f4[(f4.month == "may") & (f4.n >= 60) & (f4.ev_cons > 0)]
print(f"May IS cells with conservative EV>0 (n>=60): {len(sel4)}")
if len(sel4):
    top4 = sel4.sort_values("ev_cons", ascending=False).head(15)
    print(top4[["W", "P", "B", "A", "n", "wr", "avg_entry", "exp", "ev_cons"]]
          .to_string(index=False))
    print("\n--- June OOS ---")
    for _, r in top4.iterrows():
        o = f4[(f4.month == "june") & (f4.W == r.W) & (f4.P == r.P) &
               (f4.B == r.B) & (f4.A == r.A)]
        if len(o):
            o = o.iloc[0]
            print(f"W={r.W} P={r.P} B={r.B} A={r.A}: IS exp={r.exp:+.4f} "
                  f"(n={r.n}) -> OOS exp={o.exp:+.4f} (n={o.n}, wr={o.wr:.4f})")
# structure: underdog wr vs |move|, May
print("\nUnderdog win rate by |move| bucket (May, fav_mid>=0.90, W=30/60, ask<=0.20):")
gu = ud[(ud.month == "may") & (ud.fav_mid >= 0.90) & ud.off.isin([30, 60]) &
        (ud.bask <= 0.20) & ud.bps_at_off.notna()]
gu = gu.assign(bb=pd.cut(gu.bps_at_off.abs(), [0, 0.5, 1, 2, 3, 5, 10, 1000]))
for bb, g in gu.groupby("bb", observed=True):
    m = taker_metrics(g)
    if m:
        print(f"  |bps| {str(bb):>12}: n={m['n']:>5} wr={m['wr']:.4f} "
              f"avg_ask={m['avg_entry']:.4f} exp={m['exp']:+.4f}")
# tie-rule asymmetry
print("\nUnderdog by side (May, same filter, |bps|<=1):")
for side, g in gu[gu.bps_at_off.abs() <= 1].groupby("side"):
    m = taker_metrics(g)
    if m:
        print(f"  {side}: n={m['n']} wr={m['wr']:.4f} avg_ask={m['avg_entry']:.4f} "
              f"exp={m['exp']:+.4f}")

# ---------- F5 maker + z ----------
print("\n" + "=" * 100)
print("F5 maker join-bid on favorite with z filter (conservative fill)")
rows = []
for month in ["may", "june"]:
    dm = d5[(d5.month == month) & d5.is_fav & d5.bbid.notna()]
    for W in [10, 30, 60, 120]:
        for P in [0.90, 0.95]:
            for Z in [1, 2, 3]:
                g = dm[(dm.off == W) & (dm["mid"] >= P) & (dm.z >= Z)]
                n = len(g)
                if n < 50:
                    continue
                bidp = g.bbid.values
                win = g.winner.values.astype(bool)
                filled = g.min_ask_after.values < bidp - 1e-9
                nf = int(filled.sum())
                pnl_f = np.where(win[filled], 1 - bidp[filled], -bidp[filled])
                rows.append({"month": month, "W": W, "P": P, "Z": Z,
                             "n_sig": n, "n_fill": nf,
                             "fill_rate": nf / n,
                             "wr_fill": float(win[filled].mean()) if nf else np.nan,
                             "pnl_fill": float(pnl_f.mean()) if nf else np.nan,
                             "ev_posted": float(pnl_f.sum() / n) if nf else 0.0})
f5 = pd.DataFrame(rows)
f5.to_csv(DATA + r"\f5_maker_z.csv", index=False)
print(f5[f5.month == "may"].to_string(index=False))

# ---------- F2 imbalance (June only, day split) ----------
print("\n" + "=" * 100)
print("F2 imbalance (June only; IS=Jun4-8, OOS=Jun9-11)")
dj = d[(d.month == "june") & (d.grp == "5m") & d.is_fav &
       (d.bask < 1.0) & d.bask.notna() & d.imb.notna()]
dj = dj.assign(half=np.where(dj.settle_ts < 1781049600, "junA", "junB"))  # Jun 9 00:00Z
rows = []
for half in ["junA", "junB"]:
    dh = dj[dj.half == half]
    for W in [10, 30, 60, 120]:
        for P in [0.90, 0.95]:
            g0 = dh[(dh.off == W) & (dh["mid"] >= P)]
            for lab, msk in [("imb>0", g0.imb > 0), ("imb<=0", g0.imb <= 0),
                             ("imb>0.5", g0.imb > 0.5)]:
                m = taker_metrics(g0[msk])
                if m and m["n"] >= 20:
                    rows.append({"half": half, "W": W, "P": P, "f": lab, **m})
f2 = pd.DataFrame(rows)
f2.to_csv(DATA + r"\f2_imbalance.csv", index=False)
piv = f2.pivot_table(index=["W", "P", "f"], columns="half", values=["n", "exp", "wr"])
print(piv.round(4).to_string())
print("\nDONE")
