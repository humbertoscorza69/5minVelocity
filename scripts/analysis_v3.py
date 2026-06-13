"""Corrected $10-stake analysis with ask floor (fixes low-ask leverage blowup).
BTC/ETH split, 5m AND 15m, raw-bps band vs vol-normalized z-filter.
"""
import glob
import os

import numpy as np
import pandas as pd

pd.set_option("display.width", 250)
DATA = r"C:\Users\tico_\Fable\5minSnip\data"
FEE = 0.07
STAKE = 10.0
ASK_FLOOR = 0.30          # don't 'buy the favored side' if book prices it < 0.30
ASK_CEIL = 0.97

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

def local_vol(asset, sec, lb=60):
    ot, cl = B[asset]
    i = np.searchsorted(ot, sec * 1000, side="right") - 1
    if i < lb + 1:
        return np.nan
    return np.std(np.diff(np.log(cl[i - lb:i]))) * 1e4

def build_all(path, win_secs):
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
            if not (5 <= t <= 120) or not np.isfinite(vel[i]) or not np.isfinite(disp[i]):
                continue
            if vel[i] >= 5:
                vol = local_vol(asset, int(sec[i]))
                z = disp[i]/(vol*np.sqrt(t)) if (vol and vol > 0) else np.nan
                rec.append({"mkt": mkt, "asset": asset, "ttl": int(t),
                            "disp": disp[i], "vol": vol, "z": z, "win": winner,
                            "a1": ba.get(t-1, np.nan), "a2": ba.get(t-2, np.nan),
                            "day": pd.to_datetime(settle_ms, unit="ms").strftime("%m-%d")})
    return pd.DataFrame(rec)

def first(df, mask):
    q = df[mask & df.a1.notna()]
    return q.sort_values("ttl", ascending=False).drop_duplicates("mkt", keep="first")

def dstats(df, col):
    a = df[col].values.astype(float)
    m = np.isfinite(a) & (a >= ASK_FLOOR) & (a <= ASK_CEIL)
    a = a[m]; w = df.win.values[m]
    if len(a) < 5:
        return None
    sh = STAKE / a
    feed = sh * FEE * a * (1 - a)
    pnl = np.where(w == 1, sh * (1 - a) - feed, -STAKE - feed)
    return dict(n=len(a), win=w.mean(), ask=a.mean(), ev=pnl.mean(),
                std=pnl.std(), roi=pnl.mean()/STAKE,
                edge_sh=(w.mean() - a.mean() - FEE*a.mean()*(1-a.mean()))*100)

print(f"STAKE=${STAKE}, ask floor {ASK_FLOOR}-{ASK_CEIL}. Band=vel>=5 & disp[2,10].")
builds = {
    ("5m", "may"): build_all(DATA + r"\ll_booksec_may.parquet", 300),
    ("5m", "june"): build_all(DATA + r"\ll_booksec.parquet", 300),
    ("15m", "may"): build_all(DATA + r"\ll_booksec15_may.parquet", 900),
    ("15m", "june"): build_all(DATA + r"\ll_booksec15_june.parquet", 900),
}
DAYS = {"may": 24, "june": 7}

print("\n" + "="*95)
print("ASK DISTRIBUTION of band-rule entries (shows the low-ask outliers):")
for (iv, mo), df in builds.items():
    f = first(df, (df.disp >= 2) & (df.disp <= 10))
    a = f.a1.dropna()
    print(f"  {iv} {mo}: n={len(a)} ask p5={a.quantile(.05):.3f} p25={a.quantile(.25):.3f} "
          f"med={a.median():.3f} | frac<0.30={np.mean(a<0.30):.1%}")

print("\n" + "="*95)
print("BTC vs ETH, 5m & 15m, band rule, $10 stake (ask-floored), d=+1 / d=+2")
for iv in ["5m", "15m"]:
    for mo in ["may", "june"]:
        df = builds[(iv, mo)]; nd = DAYS[mo]
        for asset in ["btc", "eth"]:
            f = first(df, (df.disp >= 2) & (df.disp <= 10) & (df.asset == asset))
            s1 = dstats(f, "a1"); s2 = dstats(f, "a2")
            if not s1:
                continue
            print(f"  {iv:>3} {mo:<4} {asset} d1: n={s1['n']:>4}({s1['n']/nd:>4.0f}/d) "
                  f"win={s1['win']:.3f} ask={s1['ask']:.3f} EV=${s1['ev']:+.2f} "
                  f"day=${s1['ev']*s1['n']/nd:+6.1f} | d2 EV=${s2['ev']:+.2f}")

print("\n" + "="*95)
print("VOL-NORMALIZED z-filter vs raw band (5m+15m pooled, $10, ask-floored, d=+1):")
for zlo, zhi in [(0.4, 1.5), (0.5, 1.5)]:
    for mo in ["may", "june"]:
        df = pd.concat([builds[("5m", mo)], builds[("15m", mo)]])
        f = first(df, (df.z >= zlo) & (df.z <= zhi) & df.z.notna())
        s = dstats(f, "a1")
        if s:
            print(f"  z[{zlo},{zhi}] {mo}: n={s['n']} win={s['win']:.3f} ask={s['ask']:.3f} "
                  f"EV=${s['ev']:+.2f} ROI={s['roi']*100:+.0f}% ({s['n']/DAYS[mo]:.0f}/d)")
    print()
print("raw band[2,10] 5m+15m:")
for mo in ["may", "june"]:
    df = pd.concat([builds[("5m", mo)], builds[("15m", mo)]])
    f = first(df, (df.disp >= 2) & (df.disp <= 10))
    s = dstats(f, "a1")
    print(f"  {mo}: n={s['n']} win={s['win']:.3f} ask={s['ask']:.3f} EV=${s['ev']:+.2f} "
          f"ROI={s['roi']*100:+.0f}% ({s['n']/DAYS[mo]:.0f}/d)")
