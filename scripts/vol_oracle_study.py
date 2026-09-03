"""Volatility-oracle ceiling: how much money is available to ANY better vol forecast?

Usage: python scripts/vol_oracle_study.py <entries_parquet> <klines_1s_parquet>

Pre-registered in docs/PREREG_vol_oracle.md. See that file for the two-branch
verdict; this script must not be edited to change the bars after seeing output.

The bot's win probability is p = pcal(z), z = disp_bps / (vol60 * sqrt(ttl)).
disp_bps and ttl are OBSERVED exactly at decision time, so vol60 -- a trailing
estimate of the volatility of the REMAINING window -- is the only forecast input
in the model. Replacing it with the realised forward volatility gives the ceiling
for any volatility forecaster (Kronos, GARCH, HAR, a better lookback).

The oracle cheats, so a real forecaster must land strictly below it: a failing
oracle kills the whole family, a passing oracle is only permission to continue.
"""
import sys

import numpy as np
import pandas as pd
from sklearn.metrics import roc_auc_score

CAL_Z_5M = [0.14, 0.45, 0.80, 1.24, 1.74, 2.46, 3.82, 10.49]
CAL_W_5M = [0.536, 0.615, 0.664, 0.737, 0.767, 0.767, 0.767, 0.767]
CAL_Z_15M = [0.57, 0.83, 1.20, 1.90]
CAL_W_15M = [0.603, 0.703, 0.761, 0.782]
LOOKBACK = {"5m": 60, "15m": 120}
FEE_COEF = 0.07
PAPER_START = 1784584000  # Order #11 paper restart


def pcal_with(z, cal_z, cal_w):
    """Faithful port of v2.rs::pcal_with (anchored at (0, 0.5), clamped above)."""
    z = np.asarray(z, dtype=float)
    out = np.full(z.shape, cal_w[-1], dtype=float)
    out[z <= 0] = 0.5
    lo = (z > 0) & (z < cal_z[0])
    out[lo] = 0.5 + (z[lo] / cal_z[0]) * (cal_w[0] - 0.5)
    for i in range(len(cal_z) - 1):
        m = (z >= cal_z[i]) & (z < cal_z[i + 1])
        t = (z[m] - cal_z[i]) / (cal_z[i + 1] - cal_z[i])
        out[m] = cal_w[i] + t * (cal_w[i + 1] - cal_w[i])
    return out


def vol_bps(closes):
    """Faithful port of v2.rs::vol_bps: population std of 1s log returns * 1e4."""
    if len(closes) < 2 or np.any(closes <= 0):
        return np.nan
    r = np.diff(np.log(closes))
    return r.std(ddof=0) * 1e4


def main(entries_path, k1s_path):
    d = pd.read_parquet(entries_path)
    d = d[(d.ts_s >= PAPER_START) & (d.disp_bps.abs() >= 2.0)].copy()
    d["vol_impl"] = d.disp_bps / (d.z * np.sqrt(d.ttl_s))

    k = pd.read_parquet(k1s_path)
    k["asset"] = k.symbol.str.replace("USDT", "", regex=False)
    series = {a: g.set_index("open_s")["close"].sort_index() for a, g in k.groupby("asset")}

    rows = []
    for r in d.itertuples():
        s = series.get(r.asset)
        if s is None:
            rows.append((np.nan,) * 3)
            continue
        t0, res = int(r.signal_ts), int(r.resolution)
        lb = LOOKBACK[r.interval]
        back = s.loc[t0 - lb: t0].to_numpy()
        fwd = s.loc[t0: res].to_numpy()
        half = s.loc[t0: t0 + max(2, (res - t0) // 2)].to_numpy()
        rows.append((vol_bps(back), vol_bps(fwd), vol_bps(half)))
    d[["vol_recon", "sig_fwd", "sig_early"]] = pd.DataFrame(rows, index=d.index)

    print("=== INTEGRITY: reconstructed vol60 vs the bot's own (backed out of logs) ===")
    ok = d.vol_recon.notna() & d.vol_impl.notna()
    rel = (d.vol_recon[ok] / d.vol_impl[ok])
    print(f"  n={ok.sum()}  ratio recon/bot: median={rel.median():.4f} "
          f"p10={rel.quantile(.1):.3f} p90={rel.quantile(.9):.3f} "
          f"corr={np.corrcoef(d.vol_recon[ok], d.vol_impl[ok])[0,1]:.4f}")

    d = d[d.sig_fwd.notna() & d.sig_early.notna() & d.won.notna()].copy()
    d["won_i"] = d.won.astype(int)
    d["day"] = pd.to_datetime(d.ts_s, unit="s", utc=True).dt.date

    def p_of(z, iv):
        return np.where(iv == "5m",
                        pcal_with(z, CAL_Z_5M, CAL_W_5M),
                        pcal_with(z, CAL_Z_15M, CAL_W_15M))

    iv = d.interval.to_numpy()
    d["p_dep"] = p_of(d.z.to_numpy(), iv)
    for tag, col in (("full", "sig_fwd"), ("early", "sig_early")):
        z_o = d.disp_bps / (d[col] * np.sqrt(d.ttl_s))
        d[f"z_or_{tag}"] = z_o
        d[f"p_or_{tag}"] = p_of(z_o.to_numpy(), iv)

    print(f"\n=== DISCRIMINATION (floored paper population, n={len(d)}) ===")
    print(f"{'model':>22} {'AUC':>8} {'Brier':>8}")
    for name, col in (("deployed  pcal(z|vol60)", "p_dep"),
                      ("oracle-full", "p_or_full"),
                      ("oracle-early (honest)", "p_or_early")):
        auc = roc_auc_score(d.won_i, d[col])
        brier = ((d[col] - d.won_i) ** 2).mean()
        print(f"{name:>22} {auc:>8.4f} {brier:>8.4f}")

    print("\n=== MONEY (refusal test: drop entries the oracle re-scores below the gate) ===")
    print("NOTE one-sided: logs contain only entries the DEPLOYED gate accepted, so the")
    print("oracle's 'would have added' entries are unobservable. This is the refuse half.")
    rng = np.random.default_rng(0)
    days = d.day.unique()
    grp = {x: d[d.day == x] for x in days}
    base = d["hold_ev_$1"].mean()
    print(f"\nbaseline EV/$1 = {base:+.4f}  (n={len(d)})")
    print(f"{'oracle':>14} {'edge_min':>9} {'n kept':>7} {'EV/$1':>9} {'lift':>9} {'day-clustered CI':>26}")
    for tag in ("full", "early"):
        for emin in (0.02, 0.04, 0.06):
            edge = d[f"p_or_{tag}"] - d.ask - FEE_COEF * d.ask * (1 - d.ask)
            keep = edge >= emin
            if keep.sum() < 30:
                continue
            lift = d.loc[keep, "hold_ev_$1"].mean() - base
            bs = []
            for _ in range(4000):
                s = pd.concat([grp[x] for x in rng.choice(days, len(days), replace=True)])
                e = s[f"p_or_{tag}"] - s.ask - FEE_COEF * s.ask * (1 - s.ask)
                kk = e >= emin
                if kk.sum() > 10:
                    bs.append(s.loc[kk, "hold_ev_$1"].mean() - s["hold_ev_$1"].mean())
            lo, hi = np.percentile(bs, [2.5, 97.5])
            print(f"{tag:>14} {emin:>9.2f} {keep.sum():>7} "
                  f"{d.loc[keep,'hold_ev_$1'].mean():>+9.4f} {lift:>+9.4f} "
                  f"   [{lo:+.4f}, {hi:+.4f}]")
    return d


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
