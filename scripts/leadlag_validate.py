"""Validate the at-the-money lead-lag hold strategy:
 - replicate on MAY (true holdout; signal discovered on June)
 - entry-DELAY decay: enter at ask[t+d] for d=0,1,2,3s (execution realism)
 - per-day and per-asset stability
 - net hold expectancy after fee (taker, single cost)
"""
import glob
import os

import numpy as np
import pandas as pd

pd.set_option("display.width", 240)
DATA = r"C:\Users\tico_\Fable\5minSnip\data"
FEE = 0.07

def load_sym(sym):
    files = sorted(glob.glob(os.path.join(DATA, "binance", f"{sym}_*.parquet")))
    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    df = df.drop_duplicates("open_time").sort_values("open_time")
    return df.open_time.values.astype(np.int64), df.close.values.astype(float)
B = {"btc": load_sym("BTCUSDT"), "eth": load_sym("ETHUSDT")}

def px_at(asset, sec_abs):
    ot, cl = B[asset]
    idx = np.searchsorted(ot, sec_abs * 1000, side="right") - 1
    val = np.where(idx >= 0, cl[np.clip(idx, 0, len(cl) - 1)], np.nan)
    gap = sec_abs * 1000 - ot[np.clip(idx, 0, len(cl) - 1)]
    return np.where((idx >= 0) & (gap <= 5000), val, np.nan)

def build_records(path):
    bs = pd.read_parquet(path)
    bs = bs[(bs.ba < 1.0) & (bs.bb > 0) & (bs.ba >= bs.bb)]
    bs["mid"] = (bs.bb + bs.ba) / 2
    out = {k: [] for k in ["sig", "win", "ttl", "spread", "asset", "day",
                           "a0", "a1", "a2", "a3"]}
    for tok, g in bs.groupby("tok", sort=False):
        g = g.sort_values("ttl", ascending=False)
        asset = g.asset.iloc[0]
        side = g.side.iloc[0]
        winner = int(g.winner.iloc[0])
        settle_ms = int(g.settle_ms.iloc[0])
        day = pd.to_datetime(settle_ms, unit="ms").strftime("%m-%d")
        ttl = g.ttl.values
        tmax = int(ttl.max())
        grid = np.arange(tmax, -1, -1)
        mid = pd.Series(g.mid.values, index=ttl).reindex(grid).ffill().values
        ba = pd.Series(g.ba.values, index=ttl).reindex(grid).ffill().values
        bb = pd.Series(g.bb.values, index=ttl).reindex(grid).ffill().values
        sec = (settle_ms // 1000) - grid
        upx = px_at(asset, sec)
        sgn = 1.0 if side == "up" else -1.0
        n = len(mid)
        sig2 = np.full(n, np.nan)
        sig2[2:] = sgn * (upx[2:] / upx[:-2] - 1.0) * 1e4
        for i in range(n):
            t = grid[i]
            if not (5 <= t <= 120):
                continue
            if not np.isfinite(sig2[i]) or not np.isfinite(ba[i]):
                continue
            out["sig"].append(sig2[i]); out["win"].append(winner)
            out["ttl"].append(t); out["spread"].append(ba[i] - bb[i])
            out["asset"].append(asset); out["day"].append(day)
            out["a0"].append(ba[i])
            out["a1"].append(ba[i + 1] if i + 1 < n else np.nan)
            out["a2"].append(ba[i + 2] if i + 2 < n else np.nan)
            out["a3"].append(ba[i + 3] if i + 3 < n else np.nan)
    return pd.DataFrame(out)

def hold_exp(df, askcol, sigmin):
    sub = df[(df.sig >= sigmin) & df[askcol].notna() & (df[askcol] < 1.0)]
    if len(sub) < 20:
        return None
    ask = sub[askcol].values
    fee = FEE * ask * (1 - ask)
    pnl = sub.win.values - ask - fee
    se = pnl.std(ddof=1) / np.sqrt(len(pnl))
    return {"n": len(sub), "win": sub.win.mean(), "ask": ask.mean(),
            "exp_c": pnl.mean() * 100, "ci_c": 1.96 * se * 100}

print("building records...")
jr = build_records(DATA + r"\ll_booksec.parquet"); jr["month"] = "june"
mr = build_records(DATA + r"\ll_booksec_may.parquet"); mr["month"] = "may"
jr.to_parquet(DATA + r"\ll_rec_june.parquet")
mr.to_parquet(DATA + r"\ll_rec_may.parquet")
print("june recs:", len(jr), "may recs:", len(mr))

print("\n" + "=" * 90)
print("REPLICATION + ENTRY DELAY: hold-to-settle expectancy (cents/share, net fee)")
print("d = seconds of execution delay after signal (a0=instant, a1=+1s, ...)")
for month, df in [("MAY (holdout)", mr), ("JUNE (discovery)", jr)]:
    print(f"\n--- {month} ---")
    for sigmin in [2, 5, 10]:
        line = f"  sig>=+{sigmin:>2}bps: "
        for d, col in enumerate(["a0", "a1", "a2", "a3"]):
            r = hold_exp(df, col, sigmin)
            if r:
                line += f"d={d}:{r['exp_c']:+.2f}c(n={r['n']},±{r['ci_c']:.2f}) "
        print(line)

print("\n" + "=" * 90)
print("PER-ASSET (entry d=1, sig>=+3):")
both = pd.concat([mr, jr])
for (month, asset), g in both.groupby(["month", "asset"]):
    r = hold_exp(g, "a1", 3)
    if r:
        print(f"  {month:>4} {asset}: n={r['n']} win={r['win']:.3f} "
              f"ask={r['ask']:.3f} exp={r['exp_c']:+.2f}c (±{r['ci_c']:.2f})")

print("\n" + "=" * 90)
print("PER-DAY P&L (entry d=1, sig>=+3): mean cents/trade and n")
for month, df in [("may", mr), ("june", jr)]:
    print(f"\n--- {month} ---")
    rows = []
    for day, g in df.groupby("day"):
        r = hold_exp(g, "a1", 3)
        if r:
            rows.append((day, r["n"], r["exp_c"]))
    rd = pd.DataFrame(rows, columns=["day", "n", "exp_c"])
    pos = (rd.exp_c > 0).mean()
    print(rd.to_string(index=False))
    print(f"  positive days: {pos:.0%}  | overall mean: {rd.exp_c.mean():+.2f}c")
