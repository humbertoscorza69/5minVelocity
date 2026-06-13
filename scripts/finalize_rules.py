"""Finalize the taker ruleset: band rule IS(May)/OOS(June), realistic entry d=1
and conservative d=2, one-trade-per-market FIRST, hold to settle. Also the
loose disp>=2 vs band 2-10 comparison and DCA cap recommendation summary.
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

def px(asset, sec):
    ot, cl = B[asset]
    i = np.searchsorted(ot, sec * 1000, side="right") - 1
    v = np.where(i >= 0, cl[np.clip(i, 0, len(cl) - 1)], np.nan)
    gap = sec * 1000 - ot[np.clip(i, 0, len(cl) - 1)]
    return np.where((i >= 0) & (gap <= 5000), v, np.nan)

def first_entries(path, V, dlo, dhi):
    bs = pd.read_parquet(path)
    bs = bs[(bs.ba < 1.0) & (bs.bb > 0) & (bs.ba >= bs.bb)]
    rec = []
    for tok, g in bs.groupby("tok", sort=False):
        g = g.sort_values("ttl", ascending=False)
        asset = g.asset.iloc[0]; side = g.side.iloc[0]
        winner = int(g.winner.iloc[0]); settle_ms = int(g.settle_ms.iloc[0])
        ss = settle_ms // 1000
        ttl = g.ttl.values; tmax = int(ttl.max()); grid = np.arange(tmax, -1, -1)
        ba = pd.Series(g.ba.values, index=ttl).reindex(grid).ffill()
        bad = ba.to_dict()
        sec = ss - grid; upx = px(asset, sec); sgn = 1.0 if side == "up" else -1.0
        opx = px(asset, np.array([ss - 300]))[0]
        if not np.isfinite(opx):
            continue
        n = len(grid); vel = np.full(n, np.nan); vel[2:] = sgn*(upx[2:]/upx[:-2]-1)*1e4
        disp = sgn*(upx/opx-1)*1e4
        mkt = f"{asset}_{settle_ms}"
        for i in range(n):
            t = grid[i]
            if not (5 <= t <= 120) or not np.isfinite(vel[i]) or not np.isfinite(disp[i]):
                continue
            if vel[i] >= V and dlo <= disp[i] <= dhi:
                rec.append({"mkt": mkt, "tok": int(tok), "ttl": int(t),
                            "win": winner, "a1": bad.get(t - 1, np.nan),
                            "a2": bad.get(t - 2, np.nan), "day": pd.to_datetime(
                                settle_ms, unit="ms").strftime("%m-%d")})
                break
    df = pd.DataFrame(rec)
    # one trade per market: earliest across tokens
    df = df[df.a1.notna() & (df.a1 < 1.0)]
    df = df.sort_values("ttl", ascending=False).drop_duplicates("mkt", keep="first")
    return df

def stats(df, col):
    a = df[col].values
    m = np.isfinite(a) & (a < 1.0)
    a = a[m]; w = df.win.values[m]
    fee = FEE * a * (1 - a)
    pnl = w - a - fee
    se = pnl.std(ddof=1) / np.sqrt(len(pnl))
    return len(a), w.mean(), a.mean(), pnl.mean() * 100, 1.96 * se * 100

print("FINAL RULE: vel2>=5bps AND displacement in [2,10]bps, FIRST/market, hold to settle")
for label, path, days in [("MAY (IS)", DATA + r"\ll_booksec_may.parquet", 24),
                          ("JUNE (OOS)", DATA + r"\ll_booksec.parquet", 7)]:
    df = first_entries(path, 5, 2, 10)
    for col, dlab in [("a1", "d=+1s realistic"), ("a2", "d=+2s conservative")]:
        n, w, a, net, ci = stats(df, col)
        print(f"  {label:<11} {dlab:<20}: n={n:>4} ({n/days:>3.0f}/day) "
              f"win={w:.3f} ask={a:.3f} net={net:+.2f}c (+-{ci:.2f})")

print("\nCompare filters (JUNE OOS, d=+1):")
for lab, dlo, dhi in [("loose disp>=2 (incl repriced)", 2, 1e9),
                      ("BAND disp 2-10 (chosen)", 2, 10),
                      ("tight disp 2-5", 2, 5),
                      ("disp 3-8", 3, 8)]:
    df = first_entries(DATA + r"\ll_booksec.parquet", 5, dlo, dhi)
    n, w, a, net, ci = stats(df, "a1")
    print(f"  {lab:<32}: n={n:>4} win={w:.3f} ask={a:.3f} net={net:+.2f}c")
