"""Realistic one-trade-per-market backtest + the 'both sides' bleed.

Answers:
 - win rate per policy/threshold
 - how often markets whipsaw (fire signals on BOTH sides)
 - cost of taking ALL signals (overlap bleed) vs ONE per market
 - which single signal to take: FIRST (earliest>=thr), STRONGEST (max|sig|),
   LATEST (closest to settle)
Entry at +1s (a1), +1 tick slippage. Hold to settlement.
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

def build(path):
    bs = pd.read_parquet(path)
    bs = bs[(bs.ba < 1.0) & (bs.bb > 0) & (bs.ba >= bs.bb)]
    bs["mid"] = (bs.bb + bs.ba) / 2
    rows = {k: [] for k in ["mkt", "asset", "day", "side", "ttl", "sig",
                            "win", "a1"]}
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
        n = len(grid); sig2 = np.full(n, np.nan)
        sig2[2:] = sgn * (upx[2:] / upx[:-2] - 1.0) * 1e4
        for i in range(n):
            t = grid[i]
            if not (5 <= t <= 120) or not np.isfinite(sig2[i]):
                continue
            a1 = ba[i + 1] if i + 1 < n else np.nan
            rows["mkt"].append(mkt); rows["asset"].append(asset)
            rows["day"].append(day); rows["side"].append(side)
            rows["ttl"].append(t); rows["sig"].append(sig2[i])
            rows["win"].append(winner); rows["a1"].append(a1)
    return pd.DataFrame(rows)

def net(ask, win, slip=0.01):
    ask = np.minimum(ask + slip, 0.999)
    fee = FEE * ask * (1 - ask)
    return win - ask - fee

for month, path in [("MAY", DATA + r"\ll_booksec_may.parquet"),
                    ("JUNE", DATA + r"\ll_booksec.parquet")]:
    df = build(path)
    df = df[df.a1.notna() & (df.a1 < 1.0)]
    ndays = df.day.nunique()
    print("=" * 92)
    print(f"{month}  ({ndays} days, {df.mkt.nunique()} markets)")
    for thr in [2, 5, 8]:
        q = df[df.sig >= thr]                    # favored-side buy signals
        if len(q) < 30:
            continue
        # whipsaw: markets that fired qualifying signals on BOTH sides over life
        sides_per_mkt = q.groupby("mkt").side.nunique()
        whip = (sides_per_mkt >= 2).mean()
        sig_per_mkt = q.groupby("mkt").size()

        # TAKE-ALL: every signal independent (old method) + per-market overlap bleed
        all_pnl = net(q.a1.values, q.win.values)
        permkt_sum = q.assign(p=all_pnl).groupby("mkt").p.sum()
        neg_mkt = (permkt_sum < 0).mean()

        # ONE-PER-MARKET policies
        def pol(pick):
            idx = pick(q)
            sub = q.loc[idx]
            p = net(sub.a1.values, sub.win.values)
            return len(sub), sub.win.mean(), p.mean()
        first = pol(lambda x: x.sort_values("ttl", ascending=False)
                    .groupby("mkt").head(1).index)
        strong = pol(lambda x: x.sort_values("sig", ascending=False)
                     .groupby("mkt").head(1).index)
        latest = pol(lambda x: x.sort_values("ttl", ascending=True)
                     .groupby("mkt").head(1).index)

        print(f"\n  sig>={thr}bps: {len(q)} signals, {sig_per_mkt.mean():.2f}/mkt, "
              f"whipsaw markets(both sides)={whip:.1%}")
        print(f"    TAKE-ALL  : n={len(q):>5} win={q.win.mean():.3f} "
              f"net/trade={all_pnl.mean()*100:+.2f}c | "
              f"markets with negative total={neg_mkt:.1%}")
        for nm, (n, w, e) in [("FIRST   ", first), ("STRONGEST", strong),
                              ("LATEST  ", latest)]:
            print(f"    {nm}: n={n:>5} win={w:.3f} net/trade={e*100:+.2f}c "
                  f"({n/ndays:.0f}/day)")
