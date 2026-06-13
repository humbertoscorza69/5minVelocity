"""Q1: WHY does FIRST signal win? (mechanism decomposition)
Q2: Remove false positives via confirmation filters.

Per-market signal rows carry:
  ttl, vel2 (2s signed spot move = lag trigger), vel2_next (persistence),
  disp (signed cumulative move from WINDOW OPEN = 'real reason'/distance-to-strike),
  a1 (entry ask +1s), win, mkt, asset, day, month.

Entry = one trade/market, FIRST qualifying instant, hold to settle, +1tick slip.
May = IS (tune), June = OOS (confirm).
"""
import glob
import os

import numpy as np
import pandas as pd

pd.set_option("display.width", 250)
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

def build(path):
    bs = pd.read_parquet(path)
    bs = bs[(bs.ba < 1.0) & (bs.bb > 0) & (bs.ba >= bs.bb)]
    rows = {k: [] for k in ["mkt", "asset", "day", "ttl", "vel2", "vel2_next",
                            "disp", "a1", "win", "ordinal"]}
    for tok, g in bs.groupby("tok", sort=False):
        g = g.sort_values("ttl", ascending=False)
        asset = g.asset.iloc[0]; side = g.side.iloc[0]
        winner = int(g.winner.iloc[0]); settle_ms = int(g.settle_ms.iloc[0])
        mkt = f"{asset}_{settle_ms}"
        day = pd.to_datetime(settle_ms, unit="ms").strftime("%m-%d")
        ttl = g.ttl.values; tmax = int(ttl.max())
        grid = np.arange(tmax, -1, -1)
        ba = pd.Series(g.ba.values, index=ttl).reindex(grid).ffill().values
        sec = (settle_ms // 1000) - grid
        upx = px_at(asset, sec); sgn = 1.0 if side == "up" else -1.0
        open_px = px_at(asset, np.array([settle_ms // 1000 - 300]))[0]
        n = len(grid)
        vel2 = np.full(n, np.nan); vel2[2:] = sgn * (upx[2:] / upx[:-2] - 1) * 1e4
        disp = sgn * (upx / open_px - 1) * 1e4 if np.isfinite(open_px) else np.full(n, np.nan)
        ordn = 0
        for i in range(n):
            t = grid[i]
            if not (5 <= t <= 120) or not np.isfinite(vel2[i]):
                continue
            if vel2[i] >= 2:           # count ordinal among qualifying lag triggers
                ordn += 1
            rows["mkt"].append(mkt); rows["asset"].append(asset); rows["day"].append(day)
            rows["ttl"].append(t); rows["vel2"].append(vel2[i])
            rows["vel2_next"].append(vel2[i + 1] if i + 1 < n else np.nan)
            rows["disp"].append(disp[i] if np.isfinite(disp[i]) else np.nan)
            rows["a1"].append(ba[i + 1] if i + 1 < n else np.nan)
            rows["win"].append(winner)
            rows["ordinal"].append(ordn if vel2[i] >= 2 else 0)
    return pd.DataFrame(rows)

def net(sub, slip=0.01):
    ask = np.minimum(sub.a1.values + slip, 0.999)
    fee = FEE * ask * (1 - ask)
    return sub.win.values - ask - fee

mr = build(DATA + r"\ll_booksec_may.parquet"); mr["month"] = "may"
jr = build(DATA + r"\ll_booksec.parquet"); jr["month"] = "june"
mr.to_parquet(DATA + r"\ll_filt_may.parquet"); jr.to_parquet(DATA + r"\ll_filt_june.parquet")

print("=" * 95)
print("Q1 MECHANISM — within markets with >=3 lag triggers (vel2>=2), by signal ordinal:")
print("   (shows whether FIRST wins via cheaper ask / more time / higher hit rate)")
for month, df in [("MAY", mr), ("JUNE", jr)]:
    q = df[df.vel2 >= 2].copy()
    cnt = q.groupby("mkt").size()
    multi = q[q.mkt.isin(cnt[cnt >= 3].index)]
    print(f"\n  {month}:")
    print(f"   {'ordinal':>8}{'n':>7}{'mean_ttl':>10}{'entry_ask':>11}{'win':>8}{'net_c':>8}")
    for o in [1, 2, 3]:
        s = multi[multi.ordinal == o]
        if len(s):
            print(f"   {o:>8}{len(s):>7}{s.ttl.mean():>10.0f}{s.a1.mean():>11.3f}"
                  f"{s.win.mean():>8.3f}{net(s).mean()*100:>+8.2f}")
    # strongest & latest within these multi markets
    strong = multi.loc[multi.groupby("mkt").vel2.idxmax()]
    latest = multi.loc[multi.groupby("mkt").ttl.idxmin()]
    first = multi[multi.ordinal == 1]
    for nm, s in [("FIRST", first), ("STRONGEST", strong), ("LATEST", latest)]:
        print(f"   {nm:>9}{len(s):>7}{s.ttl.mean():>10.0f}{s.a1.mean():>11.3f}"
              f"{s.win.mean():>8.3f}{net(s).mean()*100:>+8.2f}")

print("\n" + "=" * 95)
print("Q2 FALSE-POSITIVE FILTERS on FIRST(vel2>=V), one trade/market, IS=May OOS=June")
def first_trade(df, V, persist=None, dispmin=None):
    q = df[df.vel2 >= V].copy()
    if persist is not None:
        q = q[q.vel2_next >= persist]
    if dispmin is not None:
        q = q[q.disp >= dispmin]
    q = q[q.a1.notna() & (q.a1 < 1.0)]
    # first qualifying per market
    f = q.sort_values("ttl", ascending=False).groupby("mkt").head(1)
    return f

def rep(label, V, persist=None, dispmin=None):
    out = f"  {label:<34}"
    for month, df, nd in [("MAY", mr, mr.day.nunique()), ("JUNE", jr, jr.day.nunique())]:
        f = first_trade(df, V, persist, dispmin)
        if len(f) < 20:
            out += f" | {month}: n<20"
            continue
        p = net(f)
        out += (f" | {month} n={len(f):>4} win={f.win.mean():.3f} "
                f"net={p.mean()*100:+.2f}c ({len(f)/nd:.0f}/d)")
    print(out)

for V in [5]:
    print(f"\n--- trigger vel2>={V} ---")
    rep(f"baseline FIRST", V)
    rep(f"+persist vel2_next>=0", V, persist=0)
    rep(f"+persist vel2_next>={V*0.5}", V, persist=V * 0.5)
    rep(f"+disp>=2bps", V, dispmin=2)
    rep(f"+disp>=5bps", V, dispmin=5)
    rep(f"+disp>=10bps", V, dispmin=10)
    rep(f"+persist>=0 & disp>=5", V, persist=0, dispmin=5)
    rep(f"+persist>={V*0.5} & disp>=10", V, persist=V * 0.5, dispmin=10)
for V in [2]:
    print(f"\n--- trigger vel2>={V} (higher volume base) ---")
    rep(f"baseline FIRST", V)
    rep(f"+persist>=0", V, persist=0)
    rep(f"+disp>=5", V, dispmin=5)
    rep(f"+disp>=10", V, dispmin=10)
    rep(f"+persist>=0 & disp>=10", V, persist=0, dispmin=10)
