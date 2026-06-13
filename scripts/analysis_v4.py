"""Window & volume study (5m, BTC+ETH).
 A) Edge by time-to-settle bucket across FULL 300s window (where does edge live?)
 B) Window extension: first-per-market with entry window [5, W]
 C) Volume frontier: vel threshold x window x disp band -> trades/day, EV,
    daily $ at $10 and $100 stake (depth-capped).
"""
import glob
import os

import numpy as np
import pandas as pd

pd.set_option("display.width", 250)
DATA = r"C:\Users\tico_\Fable\5minSnip\data"
FEE = 0.07
AFLOOR, ACEIL = 0.30, 0.97

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

def build(path, win_secs, ttl_max=295, vel_min=3):
    bs = pd.read_parquet(path)
    bs = bs[(bs.ba < 1.0) & (bs.bb > 0) & (bs.ba >= bs.bb)]
    rec = []
    for tok, g in bs.groupby("tok", sort=False):
        g = g.sort_values("ttl", ascending=False)
        asset = g.asset.iloc[0]; side = g.side.iloc[0]
        winner = int(g.winner.iloc[0]); settle_ms = int(g.settle_ms.iloc[0])
        ss = settle_ms // 1000
        ttl = g.ttl.values; tmax = int(ttl.max()); grid = np.arange(tmax, -1, -1)
        ba = pd.Series(g.ba.values, index=ttl).reindex(grid).ffill().to_dict()
        sec = ss - grid; upx = px(asset, sec); sgn = 1.0 if side == "up" else -1.0
        opx = px(asset, np.array([ss - win_secs]))[0]
        if not np.isfinite(opx):
            continue
        n = len(grid); vel = np.full(n, np.nan); vel[2:] = sgn*(upx[2:]/upx[:-2]-1)*1e4
        disp = sgn*(upx/opx-1)*1e4
        mkt = f"{asset}_{settle_ms}"
        for i in range(n):
            t = grid[i]
            if not (5 <= t <= ttl_max) or not np.isfinite(vel[i]) or not np.isfinite(disp[i]):
                continue
            if vel[i] >= vel_min:
                rec.append({"mkt": mkt, "asset": asset, "ttl": int(t),
                            "disp": disp[i], "vel": vel[i], "win": winner,
                            "a1": ba.get(t-1, np.nan), "a2": ba.get(t-2, np.nan),
                            "day": pd.to_datetime(settle_ms, unit="ms").strftime("%m-%d")})
    return pd.DataFrame(rec)

def evstats(df, col="a1"):
    a = df[col].values.astype(float)
    m = np.isfinite(a) & (a >= AFLOOR) & (a <= ACEIL)
    a = a[m]; w = df.win.values[m]
    if len(a) < 5:
        return None
    sh = 10.0 / a; feed = sh*FEE*a*(1-a)
    pnl = np.where(w == 1, sh*(1-a)-feed, -10.0-feed)
    return dict(n=len(a), win=w.mean(), ask=a.mean(), ev=pnl.mean())

print("Building (vel>=3, full window)...");
mr = build(DATA + r"\ll_booksec_may.parquet", 300); mr["mo"] = "may"
jr = build(DATA + r"\ll_booksec.parquet", 300); jr["mo"] = "june"
DAYS = {"may": 24, "june": 7}

print("\n" + "="*92)
print("A) EDGE BY TIME-TO-SETTLE (all band signals vel>=5 & disp[2,10], $10 stake)")
bucket = [5, 10, 20, 30, 45, 60, 90, 120, 180, 240, 300]
for mo, df in [("may", mr), ("june", jr)]:
    d = df[(df.vel >= 5) & (df.disp >= 2) & (df.disp <= 10)].copy()
    d["tb"] = pd.cut(d.ttl, bucket)
    print(f"\n  {mo}:")
    for tb, g in d.groupby("tb", observed=True):
        s = evstats(g)
        if s:
            print(f"   ttl {str(tb):>10}: n={s['n']:>4} win={s['win']:.3f} "
                  f"ask={s['ask']:.3f} EV/trade=${s['ev']:+.2f}")

print("\n" + "="*92)
print("B) ENTRY WINDOW [5,W]: first-per-market, band vel>=5 disp[2,10], $10")
for mo, df in [("may", mr), ("june", jr)]:
    print(f"\n  {mo}:")
    for W in [60, 120, 180, 240, 295]:
        q = df[(df.vel >= 5) & (df.disp >= 2) & (df.disp <= 10) & (df.ttl <= W) & df.a1.notna()]
        f = q.sort_values("ttl", ascending=False).drop_duplicates("mkt", keep="first")
        s = evstats(f)
        if s:
            print(f"   window 5-{W:>3}s: n={s['n']:>4} ({s['n']/DAYS[mo]:>4.0f}/d) "
                  f"win={s['win']:.3f} EV/trade=${s['ev']:+.2f} "
                  f"daily=${s['ev']*s['n']/DAYS[mo]:+.1f}")

print("\n" + "="*92)
print("C) VOLUME FRONTIER (first-per-market). daily$ at $10 and $100 stake (depth-capped)")
print(f"  {'vel':>4}{'window':>8}{'dispband':>11}{'n/day_may':>10}{'win_may':>8}"
      f"{'ev_may':>8}{'n/day_jun':>10}{'win_jun':>8}{'ev_jun':>8}{'$100/day_jun':>13}")
for vel in [3, 4, 5]:
    for W in [120, 300]:
        for dlo, dhi in [(2, 10), (1, 15), (1, 30)]:
            row = {}
            for mo, df in [("may", mr), ("june", jr)]:
                q = df[(df.vel >= vel) & (df.disp >= dlo) & (df.disp <= dhi) &
                       (df.ttl <= W) & df.a1.notna()]
                f = q.sort_values("ttl", ascending=False).drop_duplicates("mkt", keep="first")
                row[mo] = evstats(f)
            sm, sj = row["may"], row["june"]
            if not sm or not sj:
                continue
            print(f"  {vel:>4}{('5-'+str(W)):>8}{(str(dlo)+'-'+str(dhi)):>11}"
                  f"{sm['n']/DAYS['may']:>10.0f}{sm['win']:>8.3f}{sm['ev']:>+8.2f}"
                  f"{sj['n']/DAYS['june']:>10.0f}{sj['win']:>8.3f}{sj['ev']:>+8.2f}"
                  f"{sj['ev']*10*sj['n']/DAYS['june']:>+13.0f}")
