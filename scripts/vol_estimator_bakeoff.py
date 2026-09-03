"""Stage 2 of the vol-oracle study: what do CHEAP forward-vol estimators capture?

Usage: python scripts/vol_estimator_bakeoff.py <entries_parquet> <klines_1s_parquet>

Stage 1 (vol_oracle_study.py) established that perfect forward-vol foresight is
worth ~+0.08 EV/$1 on the floored paper population. That is the ceiling for ANY
volatility forecaster. This script asks how much of it is already reachable with
estimators that need no model, no GPU and no new data -- longer lookbacks, EWMA,
the current window's own tape, and intraday seasonality.

Whatever these leave on the table is the ONLY addressable market for a learned
forecaster (e.g. Kronos). Every estimator here is causal: it sees only closes at
or before signal_ts. The seasonal profile is fit on the first half of the period
and evaluated on the second half.
"""
import sys

import numpy as np
import pandas as pd

from vol_oracle_study import (CAL_Z_5M, CAL_W_5M, CAL_Z_15M, CAL_W_15M,
                              FEE_COEF, PAPER_START, pcal_with, vol_bps)


def ewma_vol(rets, halflife):
    """EWMA of squared 1s log returns -> bps/sqrt(s), causal."""
    if len(rets) < 2:
        return np.nan
    lam = 0.5 ** (1.0 / halflife)
    w = lam ** np.arange(len(rets) - 1, -1, -1)
    var = np.sum(w * rets ** 2) / np.sum(w)
    return np.sqrt(var) * 1e4


def main(entries_path, k1s_path):
    d = pd.read_parquet(entries_path)
    d = d[(d.ts_s >= PAPER_START) & (d.disp_bps.abs() >= 2.0)].copy()
    k = pd.read_parquet(k1s_path)
    k["asset"] = k.symbol.str.replace("USDT", "", regex=False)
    series = {a: g.set_index("open_s")["close"].sort_index()
              for a, g in k.groupby("asset")}

    recs = []
    for r in d.itertuples():
        s = series.get(r.asset)
        t0, res, ep = int(r.signal_ts), int(r.resolution), int(r.epoch)
        hist = s.loc[t0 - 600: t0].to_numpy()
        rets = np.diff(np.log(hist)) if len(hist) > 1 else np.array([])
        row = {
            "vol60": vol_bps(hist[-61:]),
            "vol120": vol_bps(hist[-121:]),
            "vol300": vol_bps(hist[-301:]),
            "vol600": vol_bps(hist),
            "ew30": ewma_vol(rets, 30),
            "ew60": ewma_vol(rets, 60),
            "ew120": ewma_vol(rets, 120),
            "vol_win": vol_bps(s.loc[ep: t0].to_numpy()),
            "sig_early": vol_bps(s.loc[t0: t0 + max(2, (res - t0) // 2)].to_numpy()),
            "sig_full": vol_bps(s.loc[t0: res].to_numpy()),
        }
        row["blend"] = np.sqrt(np.nanmean([row["vol60"] ** 2, row["vol300"] ** 2]))
        recs.append(row)
    d = pd.concat([d.reset_index(drop=True), pd.DataFrame(recs)], axis=1)
    d["dt"] = pd.to_datetime(d.ts_s, unit="s", utc=True)
    d["day"] = d.dt.dt.date
    d = d[d.sig_early.notna() & d.won.notna() & d.vol60.notna()].copy()

    # Intraday seasonality: median vol300 per 30-min bucket, fit on first half only.
    d["slot"] = d.dt.dt.hour * 2 + (d.dt.dt.minute >= 30).astype(int)
    mid = d.ts_s.quantile(0.5)
    prof = d[d.ts_s <= mid].groupby(["asset", "slot"]).vol300.median()
    gmean = d[d.ts_s <= mid].groupby("asset").vol300.median()
    d["seasonal"] = [
        prof.get((a, s), np.nan) if not np.isnan(prof.get((a, s), np.nan))
        else gmean.get(a, np.nan)
        for a, s in zip(d.asset, d.slot)
    ]
    d["seas_x_recent"] = np.sqrt(d.seasonal * d.vol60)
    ev_half = d[d.ts_s > mid]

    CANDS = ["vol60", "vol120", "vol300", "vol600", "ew30", "ew60", "ew120",
             "vol_win", "blend", "seasonal", "seas_x_recent"]

    print(f"=== FORECAST ACCURACY vs realised sig_early  (n={len(d)}) ===")
    print("target = realised per-second vol over the first half of the remaining window")
    print(f"{'estimator':>15} {'corr(log)':>10} {'MAE':>8} {'vs vol60':>9} {'bias':>8}")
    for c in CANDS:
        m = d[c].notna() & (d[c] > 0)
        sub = d[m]
        corr = np.corrcoef(np.log(sub[c]), np.log(sub.sig_early))[0, 1]
        mae = (sub[c] - sub.sig_early).abs().mean()
        base = (sub.vol60 - sub.sig_early).abs().mean()
        print(f"{c:>15} {corr:>10.4f} {mae:>8.4f} {mae/base:>8.3f}x "
              f"{(sub[c]/sub.sig_early).median():>8.3f}")

    print(f"\n=== MONEY: refusal test, same protocol as stage 1 (n={len(d)}) ===")
    print("lift = EV/$1 of the kept set minus EV/$1 of the full population")
    base_ev = d["hold_ev_$1"].mean()
    print(f"baseline EV/$1 = {base_ev:+.4f}\n")
    rng = np.random.default_rng(0)
    days = d.day.unique()
    grp = {x: d[d.day == x] for x in days}
    iv = d.interval.to_numpy()

    def lift_for(col, emin, frame=None):
        f = d if frame is None else frame
        z = f.disp_bps / (f[col] * np.sqrt(f.ttl_s))
        p = np.where(f.interval.to_numpy() == "5m",
                     pcal_with(z.to_numpy(), CAL_Z_5M, CAL_W_5M),
                     pcal_with(z.to_numpy(), CAL_Z_15M, CAL_W_15M))
        edge = p - f.ask - FEE_COEF * f.ask * (1 - f.ask)
        keep = edge >= emin
        if keep.sum() < 25:
            return None, None, None
        return (f.loc[keep, "hold_ev_$1"].mean() - f["hold_ev_$1"].mean(),
                int(keep.sum()), len(f))

    print(f"{'estimator':>15} {'emin':>6} {'kept':>6} {'lift':>9} {'CI(day-clustered)':>24}")
    for c in ["vol60", "vol120", "vol300", "ew30", "ew120", "vol_win", "blend",
              "seasonal", "seas_x_recent", "sig_early", "sig_full"]:
        for emin in (0.02, 0.04):
            lift, kept, _ = lift_for(c, emin)
            if lift is None:
                continue
            bs = []
            for _ in range(2000):
                s = pd.concat([grp[x] for x in rng.choice(days, len(days), replace=True)])
                l, _, _ = lift_for(c, emin, s)
                if l is not None:
                    bs.append(l)
            lo, hi = np.percentile(bs, [2.5, 97.5])
            tag = "  <- ORACLE" if c.startswith("sig_") else ""
            print(f"{c:>15} {emin:>6.2f} {kept:>6} {lift:>+9.4f}   "
                  f"[{lo:+.4f}, {hi:+.4f}]{tag}")
    return d


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
