"""Maker-IN entry for the lag-arb signal — pre-registered in docs/PREREG_maker_in_entry.md.

Usage: python scripts/maker_in_study.py <dt_dir> <out_dir>

Swaps EXECUTION, not selection: takes the deployed gate's own signals and asks what
would have happened if, instead of lifting the ask, we had rested a bid and held to
settlement. Entry fee disappears; the maker rebate is added; the question is whether
fill rate and adverse selection eat the difference.

A resting bid at P is treated as filled if the best ask comes down to <= P (touch) or
< P (through, pessimistic) within the patience window, with an early cancel when the
signal invalidates.
"""
import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
import tourney_engine as T

FEE = 0.07
STAKE = 1.05
REBATE_PER_SHARE = 0.0035   # 20% of taker fees on filled maker volume, ~0.35c at p~0.5
TICK = 0.01
PATIENCE = (5, 15, 30, 60)

DEPLOY = dict(disp_floor=2.0, vol_floor=0.12, z_min=0.45, edge_min=0.02, min_ask=0.30,
              max_ask=1.0, min_ttl=30, max_ttl=240, frozen=2, vol_lb=60,
              intervals=["5m"], mid_move_max=0.0, max_bbo_age=2, burst_min=0)


def build_paths(full, sig):
    """For each signal, the forward per-second (ask, bid, disp) path on its own token."""
    idx = {}
    for k, g in full.groupby("skey", sort=False):
        idx[k] = (g.sec.to_numpy(), g.ask.to_numpy(), g.bid.to_numpy(), g.disp.to_numpy())
    return idx


def simulate(sig, paths, price_fn, patience, strict):
    """Returns (filled, fill_price, secs_to_fill, cancelled_by_invalidation)."""
    out = np.zeros((len(sig), 4), dtype=float)
    sk = sig.skey.to_numpy()
    sc = sig.sec.to_numpy()
    px = price_fn(sig)
    for i in range(len(sig)):
        p = px[i]
        e = paths.get(sk[i])
        if e is None or not np.isfinite(p):
            continue
        secs, asks, bids, disps = e
        j0 = np.searchsorted(secs, sc[i] + 1)
        j1 = np.searchsorted(secs, sc[i] + patience, side="right")
        if j1 <= j0:
            continue
        w_ask = asks[j0:j1]
        w_disp = disps[j0:j1]
        # early cancel: the signal invalidated before we were filled
        bad = np.flatnonzero(w_disp <= 0)
        hit = np.flatnonzero(w_ask <= p if not strict else w_ask < p)
        if len(hit) == 0:
            out[i] = (0, np.nan, np.nan, 1 if len(bad) else 0)
            continue
        first_hit = hit[0]
        if len(bad) and bad[0] < first_hit:
            out[i] = (0, np.nan, np.nan, 1)      # cancelled before the fill arrived
            continue
        out[i] = (1, p, float(secs[j0 + first_hit] - sc[i]), 0)
    return out


def econ(filled, fill_px, won, rebate=True):
    """Maker economics: no taker fee, plus rebate. Returns per-entry net P&L (NaN if unfilled)."""
    shares = np.where(filled > 0, STAKE / np.clip(fill_px, 0.01, 0.99), np.nan)
    reb = REBATE_PER_SHARE * shares if rebate else 0.0
    return won * shares - STAKE + reb


def main(dt_dir, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    full_d, stops = T.load(dt_dir)
    # full per-second table for forward paths (T.load already de-duplicates + keys)
    d = full_d
    mpd = d[d.interval == "5m"].groupby("day").mkey.nunique()
    days = sorted(mpd[mpd >= mpd.median() * 0.85].index)
    d = d[d.day.isin(days)].reset_index(drop=True)
    pos = T.select(d, DEPLOY)
    sig = d.iloc[pos].reset_index(drop=True)
    print(f"days={len(days)}  deployed signals={len(sig)}  ({len(sig)/len(days):.1f}/day)")

    paths = build_paths(d, sig)
    won = sig.won.to_numpy().astype(float)

    # taker baseline on the SAME signals (delayed fill, the honest one)
    t_ask = np.clip(sig.ask_next.to_numpy().astype(float), 0.01, 0.99)
    t_sh = STAKE / t_ask
    t_fee = FEE * t_ask * (1 - t_ask) * t_sh
    t_pnl = won * t_sh - STAKE - t_fee
    print(f"\nTAKER baseline (same signals, +1s fill): n={len(sig)} "
          f"WR={won.mean():.4f} EV/$1={(t_pnl/STAKE).mean():+.4f} "
          f"net=${t_pnl.sum():+.2f} (${t_pnl.sum()/len(days):+.2f}/day)")

    price_fns = {
        "at_bid":  lambda s: s.bid.to_numpy().astype(float),
        "bid+1":   lambda s: s.bid.to_numpy().astype(float) + TICK,
        "mid":     lambda s: np.round((s.bid.to_numpy() + s.ask.to_numpy()) / 2 / TICK) * TICK,
        "ask-1":   lambda s: s.ask.to_numpy().astype(float) - TICK,
    }
    rows = []
    for strict in (False, True):
        for arm, fn in price_fns.items():
            for pat in PATIENCE:
                r = simulate(sig, paths, fn, pat, strict)
                filled, fpx, tsec, canc = r[:, 0], r[:, 1], r[:, 2], r[:, 3]
                m = filled > 0
                if m.sum() < 30:
                    continue
                pnl = econ(filled[m], fpx[m], won[m])
                rows.append(dict(
                    mode=("through" if strict else "touch"), arm=arm, patience=pat,
                    n=int(m.sum()), fill_rate=float(m.mean()),
                    wr_filled=float(won[m].mean()), wr_signal=float(won.mean()),
                    adverse=float(won[m].mean() - won.mean()),
                    ev=float((pnl / STAKE).mean()), net=float(pnl.sum()),
                    per_day=float(pnl.sum() / len(days)),
                    fill_px=float(fpx[m].mean()), secs=float(np.nanmean(tsec[m])),
                    cancelled=float(canc.mean()),
                    taker_ev=float((t_pnl / STAKE).mean()),
                    taker_perday=float(t_pnl.sum() / len(days)),
                ))
    R = pd.DataFrame(rows)
    R.to_parquet(os.path.join(out_dir, "maker_in.parquet"), index=False)
    pd.set_option("display.width", 250, "display.max_columns", 30)
    for mode in ("touch", "through"):
        print(f"\n=== {mode.upper()} fills ===")
        s = R[R["mode"] == mode].sort_values("per_day", ascending=False)
        print(s[["arm", "patience", "n", "fill_rate", "wr_filled", "adverse", "fill_px",
                 "secs", "ev", "per_day", "taker_perday"]].round(4).to_string(index=False))
    return R, sig, paths, days


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
