"""Tournament engine: evaluate strategy configs over the decision tables.

Pre-registered in docs/PREREG_tournament.md.

Design note that makes the Monte Carlo affordable: a config's ENTRY SELECTION
(which market-second it takes) depends only on features, never on outcomes. So we
compute selections once, then re-score the same selections under permuted labels
to get the null distribution of best-of-N. That prices the multiple-comparison
burden exactly, at ~1/N the cost of re-running the grid per permutation.

Fills: PRIMARY is the delayed (+1s) ask — every idealised number in this project's
history had to be halved to match live. Exits: hold-to-settle or the deployed
band-stop, simulated on the same per-second bid/disp series.
"""
import glob
import os

import numpy as np
import pandas as pd

FEE = 0.07
STAKE = 1.05
CAL = {
    "5m": ([0.14, 0.45, 0.80, 1.24, 1.74, 2.46, 3.82, 10.49],
           [0.536, 0.615, 0.664, 0.737, 0.767, 0.767, 0.767, 0.767]),
    "15m": ([0.57, 0.83, 1.20, 1.90], [0.603, 0.703, 0.761, 0.782]),
}
# Loosest bounds any config in the grid can use — pre-filter to keep the table small.
PRE = dict(disp=1.5, vol60=0.04, ask_lo=0.25, ask_hi=0.96, ttl_lo=20, ttl_hi=560)


def pcal(z, iv_arr):
    out = np.full(len(z), np.nan)
    for iv, (cz, cw) in CAL.items():
        m = iv_arr == iv
        if not m.any():
            continue
        zz = z[m]
        o = np.full(len(zz), cw[-1])
        o[zz <= 0] = 0.5
        lo = (zz > 0) & (zz < cz[0])
        o[lo] = 0.5 + (zz[lo] / cz[0]) * (cw[0] - 0.5)
        for i in range(len(cz) - 1):
            s = (zz >= cz[i]) & (zz < cz[i + 1])
            o[s] = cw[i] + (zz[s] - cz[i]) / (cz[i + 1] - cz[i]) * (cw[i + 1] - cw[i])
        out[m] = o
    return out


def load(dt_dir, days=None):
    """Returns (candidates, stops). The stop index MUST be built from the full table:
    the band-stop fires on disp <= 0 rows, which the candidate pre-filter removes."""
    frames = []
    for p in sorted(glob.glob(os.path.join(dt_dir, "dt_*.parquet"))):
        day = os.path.basename(p)[3:13]
        if days is not None and day not in days:
            continue
        d = pd.read_parquet(p)
        d["day"] = day
        frames.append(d)
    full = pd.concat(frames, ignore_index=True)
    # Each day-file also ingests the PREVIOUS day's markets (the builder loads both
    # days of the markets/kline channels so late-opening windows are complete). Left
    # alone that double-counts every overlapping market and double-books its P&L.
    # Assign each market to the UTC day of its own epoch, then de-duplicate.
    full["day"] = pd.to_datetime(full.epoch, unit="s", utc=True).dt.strftime("%Y-%m-%d")
    full = full.drop_duplicates(["asset", "interval", "epoch", "side", "sec"])
    full["skey"] = (full.asset + "|" + full.interval + "|" + full.epoch.astype(str) +
                    "|" + full.side)
    stops = stop_index(full)
    d = full
    d = d[(d.disp >= PRE["disp"]) & (d.vol60 >= PRE["vol60"]) &
          (d.ask >= PRE["ask_lo"]) & (d.ask <= PRE["ask_hi"]) &
          (d.ttl >= PRE["ttl_lo"]) & (d.ttl <= PRE["ttl_hi"]) &
          d.ask.notna() & d.bid.notna()].copy()
    d = d.sort_values(["day", "asset", "interval", "epoch", "side", "sec"]).reset_index(drop=True)
    # market key (one entry per market, either side — matches the deployed rule)
    d["mkey"] = (d.asset + "|" + d.interval + "|" + d.epoch.astype(str))
    for lb in (60, 120):
        v = d[f"vol{lb}"].to_numpy()
        z = np.where(v > 0, d.disp.to_numpy() / (v * np.sqrt(d.ttl.to_numpy())), np.nan)
        d[f"z{lb}"] = z
        d[f"p{lb}"] = pcal(z, d.interval.to_numpy())
        d[f"edge{lb}"] = d[f"p{lb}"] - d.ask - FEE * d.ask * (1 - d.ask)
    # delayed fill: the ask one second later on the same token
    g = d.groupby("skey", sort=False)
    d["ask_next"] = g.ask.shift(-1).fillna(d.ask)
    d["ask_next2"] = g.ask.shift(-2).fillna(d.ask_next)
    # Precompute, per candidate row, the bid at the FIRST band-stop-eligible second
    # after it. Turns scoring into pure vectorised arithmetic, which is what makes
    # a 300-shuffle permutation null affordable.
    sb = np.full(len(d), np.nan)
    sk_all = d.skey.to_numpy()
    sec_all = d.sec.to_numpy()
    order = np.argsort(sk_all, kind="stable")
    for k, grp in pd.Series(order).groupby(sk_all[order]):
        e = stops.get(k)
        if e is None:
            continue
        idx = grp.to_numpy()
        j = np.searchsorted(e[0], sec_all[idx] + 1)
        hit = j < len(e[0])
        sb[idx[hit]] = e[1][j[hit]]
    d["stop_bid"] = sb
    # bid 120s later on the same token — the legacy baseline's exit
    bid_at = full.set_index(["skey", "sec"]).bid
    d["bid_t120"] = bid_at.reindex(
        pd.MultiIndex.from_arrays([d.skey, d.sec + 120])).to_numpy()
    return d, stops


def stop_index(d):
    """Per (market,side): the seconds at which the band-stop WOULD fire, and the bid there.
    Condition depends only on the second, so an entry at t exits at the first such second > t."""
    elig = d[(d.disp <= 0) & ((d.bid >= 0.50) | (d.bid <= 0.30))]
    idx = {}
    for k, g in elig.groupby("skey", sort=False):
        idx[k] = (g.sec.to_numpy(), g.bid.to_numpy())
    return idx


def select(d, cfg):
    """Return row positions of the entries this config takes (first clear per market).

    A cfg may carry `union`: a list of sub-configs. A row qualifies if ANY sub-config
    accepts it — an additional entry PATH, not a replacement gate. The one-entry-per-
    market rule still applies afterwards, so a union can only change WHICH second is
    taken and add markets that no single arm would have entered.
    """
    if cfg.get("union"):
        masks = [_mask(d, c) for c in cfg["union"]]
        m = masks[0]
        for x in masks[1:]:
            m = m | x
        return _first_per_market(d, m)
    return _first_per_market(d, _mask(d, cfg))


def _first_per_market(d, m):
    pos = np.flatnonzero(m.to_numpy())
    if len(pos) == 0:
        return pos
    mk = d.mkey.to_numpy()[pos]
    first = np.r_[True, mk[1:] != mk[:-1]]
    return pos[first]


def _mask(d, cfg):
    lb = cfg.get("vol_lb", 60)
    m = ((d.disp >= cfg["disp_floor"]) &
         (d[f"vol{lb}"] >= cfg["vol_floor"]) &
         (d[f"z{lb}"] >= cfg["z_min"]) &
         (d[f"edge{lb}"] >= cfg["edge_min"]) &
         (d.ask >= cfg["min_ask"]) & (d.ask <= cfg["max_ask"]) &
         (d.ttl >= cfg["min_ttl"]) & (d.ttl <= cfg["max_ttl"]) &
         (d.tick_age <= cfg["frozen"]) &
         (d.bbo_age <= cfg.get("max_bbo_age", 2)))
    if cfg.get("mid_move_max") is not None:
        m &= (d.mid_move3.isna() | (d.mid_move3 <= cfg["mid_move_max"]))
    if cfg.get("burst_min", 0) > 0:
        m &= (np.maximum(d.burst1, d.burst3) >= cfg["burst_min"])
    if cfg.get("intervals"):
        m &= d.interval.isin(cfg["intervals"])
    if cfg.get("assets"):
        m &= d.asset.isin(cfg["assets"])
    return m


def score(d, pos, stops, delayed=True, exit_mode="band", bidmap=None, won_override=None):
    """P&L per selected entry.

    exit_mode: 'hold'  = hold to settlement (payout 1/0)
               'band'  = deployed invalidation stop with the 0.30/0.50 bid band
               't120'  = the legacy baseline's exit: sell at the bid 120s after entry
    won_override: array of outcomes aligned to `pos`, used by the permutation null.
    """
    if len(pos) == 0:
        return pd.DataFrame(columns=["day", "pnl", "won", "ask", "pf", "interval"])
    sub = d.iloc[pos]
    col = {0: "ask", 1: "ask_next", 2: "ask_next2"}[int(delayed)]
    ask = sub[col].to_numpy().astype(float)
    ask = np.clip(ask, 0.01, 0.99)
    shares = STAKE / ask
    fee = FEE * ask * (1 - ask) * shares
    won = (sub.won.to_numpy().astype(float) if won_override is None
           else np.asarray(won_override, dtype=float))
    pnl = won * shares - STAKE - fee
    if exit_mode == "band":
        sb = sub.stop_bid.to_numpy().astype(float)
        ok = np.isfinite(sb)
        pnl[ok] = sb[ok] * shares[ok] - STAKE - fee[ok]
    elif exit_mode == "t120":
        b = sub.bid_t120.to_numpy().astype(float)
        ok = np.isfinite(b)
        pnl[ok] = b[ok] * shares[ok] - STAKE - fee[ok]
    return pd.DataFrame({"day": sub.day.to_numpy(), "pnl": pnl,
                         "won": won, "ask": ask, "pf": sub.pf.to_numpy(),
                         "interval": sub.interval.to_numpy()})


def daily(d, pos, stops, **kw):
    s = score(d, pos, stops, **kw)
    if s.empty:
        return pd.Series(dtype=float)
    return s.groupby("day").pnl.sum()
