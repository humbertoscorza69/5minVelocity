"""Build a per-(market, second) decision table from the D:\polycrypto recorder.

Usage: python scripts/tourney_build.py <YYYY-MM-DD> <out_dir>

One row per (market, candidate entry second). Every column is causal — computed
only from data a bot would have had at that second — so any strategy config is a
vectorised filter over this table plus a first-clear groupby.

Realism notes baked in:
  * BBO is forward-filled from `best_bid_ask` on RECEIVED_AT, not exchange time,
    so a stale quote stays stale exactly as the bot would have seen it, and
    `bbo_age_s` records how stale.
  * Binance 1s closes come from the recorder's own stream (what the bot saw),
    not a REST backfill.
  * vol/z/disp reproduce v2.rs::vol_bps / zscore / disp_bps exactly (population
    std of 1s log returns x 1e4).
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
PHOTO_FINISH_BPS = 2.0


def _open(path):
    if os.path.exists(path):
        return io.TextIOWrapper(
            zstd.ZstdDecompressor().stream_reader(open(path, "rb")),
            encoding="utf8", errors="replace")
    raw = path[:-4] if path.endswith(".zst") else path
    if os.path.exists(raw):
        return open(raw, encoding="utf8", errors="replace")
    return None


def load_markets(day):
    """token_id -> (asset, interval, epoch, side). The `markets` channel is
    self-contained, so no API call is ever needed."""
    out = {}
    for d in (day, _prev(day)):  # windows opened late the previous day
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
                    tid = m.get(key)
                    if tid:
                        out[str(tid)] = (m["asset"], m["interval"], int(m["epoch"]), side)
    return out


def _prev(day):
    return (pd.Timestamp(day) - pd.Timedelta(days=1)).strftime("%Y-%m-%d")


def load_bbo(day, tokens):
    """Per-token BBO events keyed on local receipt time (what we actually knew)."""
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
                    r = json.loads(line)
                    p = r["payload"]
                    tid = p["asset_id"]
                    if tid not in tokens:
                        continue
                    rows.append((tid,
                                 pd.Timestamp(r["received_at"]).value // 10**9,
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
                    rows.append((int(k["t"]) // 1000, float(k["c"]), int(k["n"])))
                except Exception:
                    continue
    df = pd.DataFrame(rows, columns=["sec", "close", "trades"])
    return df.drop_duplicates("sec").sort_values("sec").reset_index(drop=True)


def vol_series(close, win):
    """Population std (ddof=0) of trailing `win` 1s log returns, x1e4 — v2.rs::vol_bps."""
    r = pd.Series(np.log(close)).diff()
    return (r.rolling(win).std(ddof=0) * 1e4).to_numpy()


def build_day(day, out_dir):
    tokens = load_markets(day)
    if not tokens:
        print(f"{day}: no markets")
        return None
    bbo = load_bbo(day, set(tokens))
    if bbo.empty:
        print(f"{day}: no bbo")
        return None

    kl = {a: load_klines(day, a) for a in ("BTC", "ETH")}
    px, vol60, vol120, tickage, sec_idx = {}, {}, {}, {}, {}
    for a, k in kl.items():
        if k.empty:
            continue
        c = k.close.to_numpy()
        px[a] = c
        vol60[a] = vol_series(c, 60)
        vol120[a] = vol_series(c, 120)
        chg = np.r_[True, np.diff(c) != 0]
        age = np.zeros(len(c))
        last = 0
        for i in range(len(c)):
            if chg[i]:
                last = i
            age[i] = i - last
        tickage[a] = age
        sec_idx[a] = {s: i for i, s in enumerate(k.sec.to_numpy())}

    # market -> its two token ids
    mk = {}
    for tid, (a, iv, ep, side) in tokens.items():
        mk.setdefault((a, iv, ep), {})[side] = tid

    bbo = bbo.sort_values("sec")
    per_tok = {t: g for t, g in bbo.groupby("token_id")}

    rows = []
    for (asset, iv, epoch), sides in mk.items():
        if asset not in px or "Up" not in sides or "Down" not in sides:
            continue
        res = epoch + IV_S[iv]
        si = sec_idx[asset]
        if epoch - 1 not in si or res - 1 not in si:
            continue
        w_open = px[asset][si[epoch - 1]]
        w_final = px[asset][si[res - 1]]
        move = (w_final - w_open) / w_open * 1e4

        lo, hi = epoch + 10, res - 30
        grid = np.arange(lo, hi + 1)
        gi = np.array([si.get(s, -1) for s in grid])
        ok = gi >= 0
        grid, gi = grid[ok], gi[ok]
        if len(grid) == 0:
            continue

        cur = px[asset][gi]
        disp_up = (cur / w_open - 1.0) * 1e4
        v60 = vol60[asset][gi]
        v120 = vol120[asset][gi]
        ta = tickage[asset][gi]
        b1 = (cur / px[asset][np.maximum(gi - 1, 0)] - 1.0) * 1e4
        b3 = (cur / px[asset][np.maximum(gi - 3, 0)] - 1.0) * 1e4

        q = {}
        for side, tid in sides.items():
            g = per_tok.get(tid)
            if g is None or g.empty:
                q[side] = None
                continue
            gg = g.drop_duplicates("sec", keep="last")
            s = pd.Series(gg.ask.to_numpy(), index=gg.sec.to_numpy())
            bd = pd.Series(gg.bid.to_numpy(), index=gg.sec.to_numpy())
            idx = pd.Index(grid)
            q[side] = (s.reindex(idx.union(s.index)).sort_index().ffill().reindex(idx).to_numpy(),
                       bd.reindex(idx.union(bd.index)).sort_index().ffill().reindex(idx).to_numpy(),
                       pd.Series(gg.sec.to_numpy(), index=gg.sec.to_numpy())
                         .reindex(idx.union(gg.sec)).sort_index().ffill().reindex(idx).to_numpy())
        if q.get("Up") is None or q.get("Down") is None:
            continue

        for side in ("Up", "Down"):
            ask, bid, qts = q[side]
            sgn = 1.0 if side == "Up" else -1.0
            mid_up = (q["Up"][0] + q["Up"][1]) / 2.0
            mid3 = np.r_[[np.nan] * 3, mid_up[:-3]]
            rows.append(pd.DataFrame(dict(
                asset=asset, interval=iv, epoch=epoch, side=side, sec=grid,
                ttl=(res - grid).astype(np.float32),
                disp=(disp_up * sgn).astype(np.float32),
                vol60=v60.astype(np.float32), vol120=v120.astype(np.float32),
                tick_age=ta.astype(np.float32),
                burst1=(b1 * sgn).astype(np.float32), burst3=(b3 * sgn).astype(np.float32),
                ask=ask.astype(np.float32), bid=bid.astype(np.float32),
                bbo_age=(grid - qts).astype(np.float32),
                mid_move3=np.abs(mid_up - mid3).astype(np.float32),
                move_bps=np.float32(move),
                won=np.float32(1.0 if ((move >= 0) == (side == "Up")) else 0.0),
            )))
    if not rows:
        print(f"{day}: no rows")
        return None
    df = pd.concat(rows, ignore_index=True)
    df["pf"] = (np.abs(df.move_bps) < PHOTO_FINISH_BPS)
    os.makedirs(out_dir, exist_ok=True)
    p = os.path.join(out_dir, f"dt_{day}.parquet")
    df.to_parquet(p, index=False)
    print(f"{day}: rows={len(df):,} markets={df.groupby(['asset','interval','epoch']).ngroups} -> {p}")
    return p


if __name__ == "__main__":
    build_day(sys.argv[1], sys.argv[2])
