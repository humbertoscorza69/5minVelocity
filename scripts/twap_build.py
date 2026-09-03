"""Decision table on the CORRECT settlement statistic: TWAP-vs-open, not close-vs-open.

Usage: python scripts/twap_build.py <YYYY-MM-DD> <out_dir>

Polymarket resolves these markets on the Chainlink BTC/USD 30s-TWAP stream:
  "Up if the TWAP of the time range is >= the price at the beginning of that range."
Every prior table in this project used close-vs-open, which disagrees on 16.2% of
windows (28.5% in the 0-1bps bucket). This rebuilds the whole thing correctly.

The two new quantities, both causal:

  twap_disp  the TWAP displacement if the price FROZE at its current value:
             ((S_sofar + R*P) / N - open) / open * 1e4
             i.e. where settlement lands if nothing further happens.

  z_twap     twap_disp normalised by the sd of the remaining contribution. For a
             driftless walk the future mean over R seconds has sd ~ sigma*sqrt(R/3),
             and it enters the final average with weight R/N, so
                 sd(final TWAP) ~ sigma * R^1.5 / (N * sqrt(3))
             This is the TWAP analogue of z, and it explodes as R -> 0 because the
             settlement becomes progressively DETERMINED — which is the whole point.
"""
import io
import json
import os
import sys

import numpy as np
import pandas as pd
import zstandard as zstd

BASE = r"D:\polycrypto\live_l2"
IV_S = {"5m": 300, "15m": 900}


def _open(path):
    if os.path.exists(path):
        return io.TextIOWrapper(zstd.ZstdDecompressor().stream_reader(open(path, "rb")),
                                encoding="utf8", errors="replace")
    raw = path[:-4]
    return open(raw, encoding="utf8", errors="replace") if os.path.exists(raw) else None


def _prev(day):
    return (pd.Timestamp(day) - pd.Timedelta(days=1)).strftime("%Y-%m-%d")


def load_markets(day):
    out = {}
    for d in (_prev(day), day):
        fh = _open(rf"{BASE}\polymarket\markets\{d}.jsonl.zst")
        if fh is None:
            continue
        with fh:
            for line in fh:
                try:
                    m = json.loads(line)["market"]
                except Exception:
                    continue
                for side, key in (("Up", "up_token_id"), ("Down", "down_token_id")):
                    if m.get(key):
                        out[str(m[key])] = (m["asset"], m["interval"], int(m["epoch"]), side)
    return out


def load_bbo(day, tokens):
    rows = []
    for d in (_prev(day), day):
        fh = _open(rf"{BASE}\polymarket\best_bid_ask\{d}.jsonl.zst")
        if fh is None:
            continue
        with fh:
            for line in fh:
                if '"best_bid_ask"' not in line:
                    continue
                try:
                    r = json.loads(line); p = r["payload"]
                    if p["asset_id"] not in tokens:
                        continue
                    rows.append((p["asset_id"], pd.Timestamp(r["received_at"]).value // 10**9,
                                 float(p["best_bid"]), float(p["best_ask"])))
                except Exception:
                    continue
    return pd.DataFrame(rows, columns=["token_id", "sec", "bid", "ask"])


def load_klines(day, asset):
    sym = {"BTC": "btcusdt", "ETH": "ethusdt"}[asset]
    rows = []
    for d in (_prev(day), day):
        fh = _open(rf"{BASE}\binance\{sym}_kline_1s\{d}.jsonl.zst")
        if fh is None:
            continue
        with fh:
            for line in fh:
                try:
                    k = json.loads(line)["payload"]["data"]["k"]
                    rows.append((int(k["t"]) // 1000, float(k["c"])))
                except Exception:
                    continue
    df = pd.DataFrame(rows, columns=["sec", "close"])
    return df.drop_duplicates("sec").sort_values("sec").reset_index(drop=True)


def vol_series(close, win):
    r = pd.Series(np.log(close)).diff()
    return (r.rolling(win).std(ddof=0) * 1e4).to_numpy()


def build_day(day, out_dir):
    tokens = load_markets(day)
    if not tokens:
        print(f"{day}: no markets"); return
    bbo = load_bbo(day, set(tokens))
    if bbo.empty:
        print(f"{day}: no bbo"); return

    px, vol60, cums, sidx = {}, {}, {}, {}
    for a in ("BTC", "ETH"):
        k = load_klines(day, a)
        if k.empty:
            continue
        c = k.close.to_numpy()
        px[a] = c
        vol60[a] = vol_series(c, 60)
        cums[a] = np.concatenate([[0.0], np.cumsum(c)])     # prefix sums for O(1) TWAP
        sidx[a] = {s: i for i, s in enumerate(k.sec.to_numpy())}

    mk = {}
    for tid, (a, iv, ep, side) in tokens.items():
        mk.setdefault((a, iv, ep), {})[side] = tid
    per_tok = {t: g.sort_values("sec") for t, g in bbo.groupby("token_id")}

    rows = []
    for (asset, iv, epoch), sides in mk.items():
        if asset not in px or "Up" not in sides or "Down" not in sides:
            continue
        N = IV_S[iv]; res = epoch + N
        si = sidx[asset]
        i_ep, i_res = si.get(epoch), si.get(res)
        if i_ep is None or i_res is None or (i_res - i_ep) != N:
            continue
        c = px[asset]; C = cums[asset]
        open_px = c[i_ep]
        twap_final = (C[i_res] - C[i_ep]) / N                     # settlement statistic
        close_px = c[i_res - 1]
        mv_twap = (twap_final - open_px) / open_px * 1e4
        mv_close = (close_px - open_px) / open_px * 1e4

        lo, hi = epoch + 10, res - 30
        grid = np.arange(lo, hi + 1)
        gi = np.array([si.get(s, -1) for s in grid])
        ok = gi >= 0
        grid, gi = grid[ok], gi[ok]
        if len(grid) == 0:
            continue

        cur = c[gi]
        elapsed = (grid - epoch + 1).astype(float)                # seconds already in the average
        R = N - elapsed                                           # seconds still to come
        S_sofar = C[gi + 1] - C[i_ep]
        twap_frozen = (S_sofar + R * cur) / N                     # settlement if price froze now
        twap_disp = (twap_frozen - open_px) / open_px * 1e4
        v = vol60[asset][gi]
        sd_rem = np.where(R > 0, v * np.power(R, 1.5) / (N * np.sqrt(3.0)), 1e-9)
        disp_close = (cur / open_px - 1.0) * 1e4                  # the OLD signal, for comparison

        q = {}
        for side, tid in sides.items():
            g = per_tok.get(tid)
            if g is None or g.empty:
                q[side] = None; continue
            gg = g.drop_duplicates("sec", keep="last")
            idx = pd.Index(grid)
            ask = pd.Series(gg.ask.to_numpy(), index=gg.sec.to_numpy())
            bid = pd.Series(gg.bid.to_numpy(), index=gg.sec.to_numpy())
            ts = pd.Series(gg.sec.to_numpy(), index=gg.sec.to_numpy())
            f = lambda s: s.reindex(idx.union(s.index)).sort_index().ffill().reindex(idx).to_numpy()
            q[side] = (f(ask), f(bid), f(ts))
        if q.get("Up") is None or q.get("Down") is None:
            continue

        for side in ("Up", "Down"):
            sgn = 1.0 if side == "Up" else -1.0
            a_, b_, t_ = q[side]
            rows.append(pd.DataFrame(dict(
                asset=asset, interval=iv, epoch=epoch, side=side, sec=grid,
                ttl=(res - grid).astype(np.float32), R=R.astype(np.float32),
                twap_disp=(twap_disp * sgn).astype(np.float32),
                z_twap=(twap_disp * sgn / sd_rem).astype(np.float32),
                disp_close=(disp_close * sgn).astype(np.float32),
                z_close=(disp_close * sgn / np.maximum(v * np.sqrt(res - grid), 1e-9)).astype(np.float32),
                vol60=v.astype(np.float32),
                ask=a_.astype(np.float32), bid=b_.astype(np.float32),
                bbo_age=(grid - t_).astype(np.float32),
                mv_twap=np.float32(mv_twap), mv_close=np.float32(mv_close),
                won_twap=np.float32(1.0 if ((mv_twap >= 0) == (side == "Up")) else 0.0),
                won_close=np.float32(1.0 if ((mv_close >= 0) == (side == "Up")) else 0.0),
            )))
    if not rows:
        print(f"{day}: no rows"); return
    df = pd.concat(rows, ignore_index=True)
    os.makedirs(out_dir, exist_ok=True)
    p = os.path.join(out_dir, f"tw_{day}.parquet")
    df.to_parquet(p, index=False)
    print(f"{day}: rows={len(df):,} mkts={df.groupby(['asset','interval','epoch']).ngroups} -> {p}",
          flush=True)


if __name__ == "__main__":
    build_day(sys.argv[1], sys.argv[2])
