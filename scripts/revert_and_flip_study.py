"""Two untested cells: (A) a fully-gated OPPOSITE signal while still holding, and
(B) mean-reversion entries (VWAP extension / Donchian position / contra-at-extreme-z).

Usage: python scripts/revert_and_flip_study.py <tw_dir>

Both are scored on the only admissible target in a calibrated book: the RESIDUAL,
WR minus ask. The books here price close-vs-open to within ~0.005 at every level, so
any signal that merely predicts direction is already in the price by construction.

(A) is distinct from two things already settled: the INSTANT flip at the invalidation
crossing is dead 3x (priced within 2s), and the fully-gated opposite AFTER a band-stop
is validated (+0.144/$1). The untested cell is a fully-gated opposite signal arriving
while the original position is still open and no stop has fired.
"""
import glob
import os
import sys

import numpy as np
import pandas as pd

FEE = 0.07
# deployed 5m gate
G = dict(disp=2.0, vol=0.12, z=0.45, emin=0.02, amin=0.30, ttl_lo=30, ttl_hi=240)
CAL_Z = [0.14, 0.45, 0.80, 1.24, 1.74, 2.46, 3.82, 10.49]
CAL_W = [0.536, 0.615, 0.664, 0.737, 0.767, 0.767, 0.767, 0.767]


def pcal(z):
    z = np.asarray(z, float)
    out = np.full(z.shape, CAL_W[-1])
    out[z <= 0] = 0.5
    lo = (z > 0) & (z < CAL_Z[0])
    out[lo] = 0.5 + (z[lo] / CAL_Z[0]) * (CAL_W[0] - 0.5)
    for i in range(len(CAL_Z) - 1):
        m = (z >= CAL_Z[i]) & (z < CAL_Z[i + 1])
        out[m] = CAL_W[i] + (z[m] - CAL_Z[i]) / (CAL_Z[i + 1] - CAL_Z[i]) * (CAL_W[i + 1] - CAL_W[i])
    return out


def load(tw_dir):
    d = pd.concat([pd.read_parquet(f) for f in sorted(glob.glob(os.path.join(tw_dir, "tw_*.parquet")))],
                  ignore_index=True)
    d = d.drop_duplicates(["asset", "interval", "epoch", "side", "sec"])
    d = d[(d.interval == "5m") & (d.bbo_age <= 2) & d.ask.notna() &
          (d.ask > 0.02) & (d.ask < 0.98)].copy()
    d = d.sort_values(["asset", "epoch", "side", "sec"]).reset_index(drop=True)
    d["mkey"] = d.asset + "|" + d.epoch.astype(str)
    d["skey"] = d.mkey + "|" + d.side
    N = 300.0
    e = N - d.R
    # running VWAP of the window so far, recovered from the frozen-TWAP identity
    d["vwap_disp"] = np.where(e > 0, (N * d.twap_disp - d.R * d.disp_close) / e, 0.0)
    d["ext_vwap"] = d.disp_close - d.vwap_disp        # how far above its own VWAP this side is
    g = d.groupby("skey", sort=False).disp_close
    d["run_max"] = g.cummax()
    d["run_min"] = g.cummin()
    rng = (d.run_max - d.run_min).replace(0, np.nan)
    d["donch"] = ((d.disp_close - d.run_min) / rng).fillna(0.5)   # 1 = at window high
    d["p"] = pcal(d.z_close.to_numpy())
    d["cost"] = d.ask + FEE * d.ask * (1 - d.ask)
    d["edge"] = d.p - d.cost
    d["gate"] = ((d.disp_close >= G["disp"]) & (d.vol60 >= G["vol"]) & (d.z_close >= G["z"]) &
                 (d.edge >= G["emin"]) & (d.ask >= G["amin"]) &
                 (d.ttl >= G["ttl_lo"]) & (d.ttl <= G["ttl_hi"]))
    return d


def ev(sub, wincol="won_close"):
    a = np.clip(sub.ask.to_numpy(), .01, .99)
    sh = 1.05 / a
    return (sub[wincol].to_numpy() * sh - 1.05 - FEE * a * (1 - a) * sh) / 1.05


def test_a(d):
    print("=" * 74)
    print("TEST A — a fully-gated OPPOSITE signal arriving while we still hold")
    print("=" * 74)
    fires = d[d.gate].groupby("skey").first().reset_index()
    fires["mkey"] = fires.skey.str.rsplit("|", n=1).str[0]
    fires["side"] = fires.skey.str.rsplit("|", n=1).str[1]
    first = fires.sort_values("sec").groupby("mkey").first()          # the entry we actually take
    both = fires.groupby("mkey").size()
    two = both[both > 1].index
    print(f"  markets where the gate fires at all      : {len(both):,}")
    print(f"  markets where BOTH sides fire (untested) : {len(two):,}  = {len(two)/len(both)*100:.1f}%")
    if len(two) == 0:
        return
    f2 = fires[fires.mkey.isin(two)].sort_values("sec")
    a1 = f2.groupby("mkey").first()      # original entry
    a2 = f2.groupby("mkey").last()       # the opposite signal
    gap = (a2.sec - a1.sec)
    print(f"  seconds between them: p25 {gap.quantile(.25):.0f}  median {gap.median():.0f}  p75 {gap.quantile(.75):.0f}")
    hold = ev(a1); flip_leg = ev(a2)
    # FLIP = close the first at its bid at t2, then take the second
    a1b = a1.copy()
    exit_bid = a2.bid.reindex(a1.index).to_numpy()
    sh1 = 1.05 / np.clip(a1.ask.to_numpy(), .01, .99)
    fee1 = FEE * a1.ask.to_numpy() * (1 - a1.ask.to_numpy()) * sh1
    close_pnl = (exit_bid * sh1 - 1.05 - fee1) / 1.05
    print(f"\n  HOLD the original          EV/$1 {hold.mean():+.4f}   n={len(a1)}")
    print(f"  FLIP (sell A at bid, buy B) EV/$1 {(close_pnl + flip_leg).mean():+.4f}"
          f"   [exit {close_pnl.mean():+.4f} + new leg {flip_leg.mean():+.4f}]")
    print(f"  the OPPOSITE leg alone      EV/$1 {flip_leg.mean():+.4f}   WR {a2.won_close.mean():.4f}"
          f"   ask {a2.ask.mean():.3f}")
    print(f"\n  -> the opposite leg must beat HOLD ({hold.mean():+.4f}) to be worth taking.")


def test_b(d):
    print("\n" + "=" * 74)
    print("TEST B — MEAN REVERSION, scored as WR minus ask (the residual)")
    print("=" * 74)
    base = d[(d.ttl >= 30) & (d.ttl <= 240)]
    print(f"  population: {len(base):,} book-seconds\n")
    for name, col, bins in [
        ("contra depth  (disp_close, negative = this side is behind)", "disp_close",
         [-1e9, -20, -10, -5, -2, 0, 2, 5, 10, 20, 1e9]),
        ("VWAP extension (disp - running VWAP)", "ext_vwap",
         [-1e9, -10, -5, -2, 0, 2, 5, 10, 1e9]),
        ("Donchian position in window range (1 = at high)", "donch",
         [-.01, .1, .25, .5, .75, .9, 1.01]),
    ]:
        b = pd.cut(base[col], bins)
        t = base.groupby(b, observed=True).agg(n=("ask", "size"), ask=("ask", "mean"),
                                               WR=("won_close", "mean"))
        t["WR_minus_ask"] = (t.WR - t.ask).round(4)
        t["share"] = (t.n / len(base)).round(3)
        print(f"--- {name}")
        print(t[["n", "share", "ask", "WR", "WR_minus_ask"]].round(4).to_string())
        print()


if __name__ == "__main__":
    d = load(sys.argv[1])
    print(f"loaded {len(d):,} 5m book-seconds, {d.mkey.nunique():,} markets\n")
    test_a(d)
    test_b(d)
