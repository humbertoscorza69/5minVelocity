"""Deep-dive: BTC vs ETH, volume<->winrate hypothesis, vol-normalized filter,
all at a $10 stake. 5m markets (ll_booksec). IS=May, OOS=June.
"""
import glob
import os

import numpy as np
import pandas as pd

pd.set_option("display.width", 250)
DATA = r"C:\Users\tico_\Fable\5minSnip\data"
FEE = 0.07
STAKE = 10.0

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
    r = np.diff(np.log(cl[i - lb:i]))
    return np.std(r) * 1e4

def build_all(path, win_secs=300):
    """All vel>=5 candidate rows with features."""
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
                z = disp[i] / (vol*np.sqrt(t)) if (vol and vol > 0) else np.nan
                rec.append({"mkt": mkt, "asset": asset, "ttl": int(t),
                            "disp": disp[i], "vel": vel[i], "vol": vol, "z": z,
                            "win": winner, "a1": ba.get(t-1, np.nan),
                            "a2": ba.get(t-2, np.nan),
                            "day": pd.to_datetime(settle_ms, unit="ms").strftime("%m-%d")})
    return pd.DataFrame(rec)

def first_per_market(df, mask):
    q = df[mask & df.a1.notna() & (df.a1 < 1.0)]
    return q.sort_values("ttl", ascending=False).drop_duplicates("mkt", keep="first")

def dollar_stats(df, askcol):
    a = df[askcol].values.astype(float)
    m = np.isfinite(a) & (a < 1.0) & (a > 0)
    a = a[m]; w = df.win.values[m]
    shares = STAKE / a
    feed = shares * FEE * a * (1 - a)          # $ fee
    pnl = np.where(w == 1, shares*(1-a) - feed, -STAKE - feed)
    return dict(n=len(a), win=w.mean(), ask=a.mean(),
                ev=pnl.mean(), std=pnl.std(), tot=pnl.sum(),
                win_amt=shares.mean()*(1-a.mean()), roi=pnl.mean()/STAKE)

print(f"$ figures use STAKE=${STAKE:.0f} per trade. Band rule = vel>=5 & disp in [2,10].")
mr = build_all(DATA + r"\ll_booksec_may.parquet")
jr = build_all(DATA + r"\ll_booksec.parquet")

print("\n" + "="*92)
print("PART 1 — BTC vs ETH (5m), band rule, $10 stake, entry d=+1 and d=+2")
band = lambda d: (d.disp >= 2) & (d.disp <= 10)
for label, df, nd in [("MAY", mr, 24), ("JUNE", jr, 7)]:
    for asset in ["btc", "eth"]:
        f = first_per_market(df, band(df) & (df.asset == asset))
        for col, dl in [("a1", "d1"), ("a2", "d2")]:
            s = dollar_stats(f, col)
            print(f"  {label:<5}{asset} {dl}: n={s['n']:>4}({s['n']/nd:>4.0f}/d) "
                  f"win={s['win']:.3f} ask={s['ask']:.3f} "
                  f"EV=${s['ev']:+.2f}/trade std=${s['std']:.2f} "
                  f"day=${s['ev']*s['n']/nd:+.1f} ROI={s['roi']*100:+.1f}%")

print("\n" + "="*92)
print("PART 2 — VOLUME vs WIN-RATE hypothesis (does higher daily signal count = lower win?)")
for label, df in [("MAY", mr), ("JUNE", jr)]:
    f = first_per_market(df, band(df))
    daily = f.groupby("day").agg(n=("win", "size"), win=("win", "mean"),
                                 vol=("vol", "mean")).reset_index()
    c_nw = daily.n.corr(daily.win)
    c_vn = daily.vol.corr(daily.n)
    c_vw = daily.vol.corr(daily.win)
    print(f"\n  {label}: corr(trades/day, win)={c_nw:+.2f}  "
          f"corr(vol, trades)={c_vn:+.2f}  corr(vol, win)={c_vw:+.2f}")
    print(daily.to_string(index=False))

print("\n" + "="*92)
print("PART 3 — VOL-NORMALIZED filter (z = disp/(vol*sqrt(ttl))) vs raw-bps band")
print("  Does a z-band give a MORE STABLE win rate across May/June than raw bps?")
for zlo, zhi in [(0.3, 1.2), (0.4, 1.5), (0.5, 2.0)]:
    row = f"  z in [{zlo},{zhi}]:"
    for label, df, nd in [("MAY", mr, 24), ("JUNE", jr, 7)]:
        f = first_per_market(df, (df.z >= zlo) & (df.z <= zhi) & df.z.notna())
        s = dollar_stats(f, "a1")
        row += (f"  {label} n={s['n']:>4} win={s['win']:.3f} "
                f"EV=${s['ev']:+.2f} (d{s['n']/nd:.0f}/d)")
    print(row)
print("\n  raw-bps band [2,10] for reference:")
for label, df, nd in [("MAY", mr, 24), ("JUNE", jr, 7)]:
    f = first_per_market(df, band(df))
    s = dollar_stats(f, "a1")
    print(f"    {label}: win={s['win']:.3f} EV=${s['ev']:+.2f} n={s['n']} ({s['n']/nd:.0f}/d)")
