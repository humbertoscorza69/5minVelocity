"""Replay the Binance-side gate stack over 1s klines to count qualifying markets/day.

Usage: python scripts/gate_replay.py <klines_1s_parquet>

Scores the weekend dead-tape exam WITHOUT the bot's logs. The deployed gate stack
splits into a Binance-side half we can reproduce exactly offline (disp floor,
vol60 floor, z_min, ttl window) and a book-side half we cannot (edge/ask,
book-unmoved, frozen-tape). So this is an UPPER BOUND on entries: every market
counted here still had to clear the book-side gates to become a real intent.

The bound is calibrated against days whose true intent count is known from the
logs, which turns it into a projection rather than a raw ceiling.
"""
import sys

import numpy as np
import pandas as pd

# Deployed gates, read from rust_bot/config/bot_v2.toml.
CFG = {
    "5m":  dict(iv=300, lookback=60,  vol_floor=0.12, z_min=0.45,
                disp_floor=2.0, min_ttl=30, max_ttl=240),
    "15m": dict(iv=900, lookback=120, vol_floor=0.07, z_min=0.70,
                disp_floor=2.0, min_ttl=30, max_ttl=540),
}


def qualifying_markets(closes, opens, idx, cfg):
    """Count windows in which at least one second clears every Binance-side gate."""
    logc = np.log(closes)
    ret = np.diff(logc, prepend=logc[0])
    ret[0] = 0.0
    lb = cfg["lookback"]
    s = pd.Series(ret)
    # population std (ddof=0) of the trailing `lb` 1s log returns, x1e4 -> v2.rs::vol_bps
    vol = s.rolling(lb).std(ddof=0).to_numpy() * 1e4

    iv = cfg["iv"]
    starts = idx[idx % iv == 0]
    qualified = []
    for ep in starts:
        i0 = np.searchsorted(idx, ep)
        if i0 >= len(idx) or idx[i0] != ep:
            continue
        res = ep + iv
        t_lo, t_hi = res - cfg["max_ttl"], res - cfg["min_ttl"]
        a = np.searchsorted(idx, t_lo)
        b = np.searchsorted(idx, t_hi, side="right")
        if b <= a or b > len(idx):
            continue
        w_open = opens[i0]
        disp = (closes[a:b] / w_open - 1.0) * 1e4
        ttl = (res - idx[a:b]).astype(float)
        v = vol[a:b]
        ad = np.abs(disp)
        with np.errstate(divide="ignore", invalid="ignore"):
            z = ad / (v * np.sqrt(ttl))
        ok = (ad >= cfg["disp_floor"]) & (v >= cfg["vol_floor"]) & (z >= cfg["z_min"])
        qualified.append((ep, bool(np.nanmax(ok)) if len(ok) else False))
    return pd.DataFrame(qualified, columns=["epoch", "ok"])


def main(k1s_path):
    k = pd.read_parquet(k1s_path)
    k["asset"] = k.symbol.str.replace("USDT", "", regex=False)
    out = []
    for asset, g in k.groupby("asset"):
        g = g.sort_values("open_s")
        idx = g.open_s.to_numpy()
        closes = g.close.to_numpy()
        opens = g.open.to_numpy()
        for iv, cfg in CFG.items():
            q = qualifying_markets(closes, opens, idx, cfg)
            q["asset"], q["interval"] = asset, iv
            out.append(q)
    q = pd.concat(out, ignore_index=True)
    q["day"] = pd.to_datetime(q.epoch, unit="s", utc=True).dt.date
    q["dow"] = pd.to_datetime(q.epoch, unit="s", utc=True).dt.day_name().str[:3]

    t = q.groupby(["day", "dow", "interval"]).ok.agg(["sum", "size"]).reset_index()
    t["rate"] = (t["sum"] / t["size"]).round(3)
    piv = t.pivot_table(index=["day", "dow"], columns="interval",
                        values="sum", aggfunc="sum")
    piv["TOTAL"] = piv.sum(axis=1)
    print("=== Windows clearing the Binance-side gate stack (both assets) ===")
    print("upper bound on daily intents; book-side gates can only reduce it\n")
    print(piv.to_string())
    return q


if __name__ == "__main__":
    main(sys.argv[1])
