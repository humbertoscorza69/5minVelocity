"""SIGNAL-INVALIDATION STOP vs HOLD-TO-SETTLE.

Question: does exiting (SELL) a position when the side-signed displacement-from-
window-open crosses back to <= band (0 / -1bps / -2bps) beat holding to settle?
This is a THESIS-INVALIDATION exit, not a price/bid stop or take-profit (those
were already refuted). Exit fill is HONEST: cross the full spread to the BID at
exit second (taker sell, no PM fee), ffill last bid; if no bid ever, fall through
to settlement.

Entry population mirrors production live gate (max_ask_study.py / stop_relative_15m.py):
  side-signed disp>0, z = disp_bps/(vol_bps*sqrt(ttl_s)),
  z>=0.45 (5m) / z>=0.80 (15m), edge = pcal(z)-ask-0.07*ask*(1-ask) >= 0.06,
  ttl<=240 for 5m (shipped max-ttl gate). One entry per market (first sec clearing gate).

Reports 5m and 15m separately, split by asset (BTC/ETH) and temporal IS/OOS half.
Caches per-entry post-entry disp+bid paths to parquet so re-runs are instant.
"""
import io, math, os
import numpy as np, pandas as pd, orjson, zstandard as zstd

DATA = r"C:\Users\tico_\Fable\5minSnip\data"
PM = r"D:\polycrypto\live_l2\polymarket"; BN = r"D:\polycrypto\live_l2\binance"
FEE = 0.07; EDGE_MIN = 0.06; DAYS = [f"2026-06-{d:02d}" for d in range(18, 30)]


def lines(p):
    if p.endswith(".zst"):
        with open(p, "rb") as f:
            for ln in io.TextIOWrapper(io.BufferedReader(zstd.ZstdDecompressor().stream_reader(f), 1 << 24), encoding="utf-8"):
                yield ln
    else:
        with open(p, encoding="utf-8", buffering=1 << 24) as f:
            for ln in f:
                yield ln


def find(b, d):
    for e in (".jsonl.zst", ".jsonl"):
        q = os.path.join(b, d + e)
        if os.path.exists(q):
            return q


def build_pcal():
    sig = pd.read_parquet(DATA + r"\strat_signals.parquet")
    may = sig[(sig.month == "may") & (sig.is_first == 1)]
    zb = np.array([-1, 0, .3, .6, 1, 1.5, 2, 3, 5, 100]); mids = []; ws = []
    for lo, hi in zip(zb[:-1], zb[1:]):
        s = may[(may.z >= lo) & (may.z < hi)]
        if len(s) >= 20:
            mids.append(s.z.mean()); ws.append(s.win.mean())
    mids = np.array(mids); ws = np.array(ws)
    return lambda z: 0.5 if z <= 0 else float(np.interp(z, mids, ws))


def load_binance():
    B = {}
    for a, sym in (("btc", "btcusdt"), ("eth", "ethusdt")):
        d = {}
        for day in DAYS:
            p = find(os.path.join(BN, f"{sym}_kline_1s"), day)
            if not p:
                continue
            for ln in lines(p):
                try:
                    k = orjson.loads(ln)["payload"]["data"]["k"]; d[int(k["t"]) // 1000] = float(k["c"])
                except Exception:
                    continue
        secs = np.array(sorted(d)); B[a] = (secs, np.array([d[s] for s in secs]))
    return B


def reconstruct(interval, win_s, zmin, ttl_max):
    """Reconstruct entries + full post-entry paths. Returns tidy DataFrame,
    one row PER post-entry second, columns: mkt id, asset, half, z, ask (entry),
    win, sec_since_entry (0=entry), disp (bps at that sec), bid (at that sec)."""
    cache = DATA + rf"\inval_paths_{interval}.parquet"
    if os.path.exists(cache):
        return pd.read_parquet(cache)
    pcal = build_pcal(); B = load_binance()

    def cl(a, sec):
        secs, c = B[a]; i = np.searchsorted(secs, sec, side="right") - 1
        return c[i] if (i >= 0 and sec - secs[i] <= 5) else np.nan

    def vol(a, sec, lb=60):
        secs, c = B[a]; i = np.searchsorted(secs, sec, side="right") - 1
        return np.std(np.diff(np.log(c[i - lb:i]))) * 1e4 if i >= lb + 1 else np.nan

    tokmap = {}
    for day in DAYS:
        p = find(os.path.join(PM, "markets"), day)
        if not p:
            continue
        for ln in lines(p):
            try:
                m = orjson.loads(ln).get("market") or {}
            except Exception:
                continue
            if m.get("interval") != interval:
                continue
            a = str(m.get("asset", "")).lower(); ep = m.get("epoch")
            if a not in ("btc", "eth") or ep is None:
                continue
            s = (ep + win_s) * 1000
            if m.get("up_token_id"):
                tokmap[m["up_token_id"]] = (a, "up", s, ep)
            if m.get("down_token_id"):
                tokmap[m["down_token_id"]] = (a, "down", s, ep)

    book = {}
    for day in DAYS:
        p = find(os.path.join(PM, "best_bid_ask"), day)
        if not p:
            continue
        for ln in lines(p):
            try:
                pp = orjson.loads(ln)["payload"]
            except Exception:
                continue
            info = tokmap.get(pp.get("asset_id"))
            if info is None:
                continue
            s = info[2]; ts = int(pp["timestamp"])
            if ts < s - (win_s + 5) * 1000 or ts > s:
                continue
            try:
                book.setdefault(pp["asset_id"], {})[ts // 1000] = (float(pp["best_bid"]), float(pp["best_ask"]))
            except Exception:
                continue
        print(f"[{interval}] parsed", day)

    rows = []
    for tid, (a, side, settle_ms, ep) in tokmap.items():
        ss = settle_ms // 1000
        grid = np.arange(win_s, -1, -1)          # ttl remaining at each absolute sec
        absec = ss - grid                        # absolute unix second
        ser = pd.Series(book.get(tid, {}))
        bb = ser.reindex(absec).map(lambda x: x[0] if isinstance(x, tuple) else np.nan).ffill().values
        ba = ser.reindex(absec).map(lambda x: x[1] if isinstance(x, tuple) else np.nan).ffill().values
        op = cl(a, ep - 1); fin = cl(a, ss - 1)
        if not (np.isfinite(op) and np.isfinite(fin)):
            continue
        win = int((side == "up") == (fin >= op))
        spot = np.array([cl(a, s) for s in absec]); sgn = 1.0 if side == "up" else -1.0
        disp = sgn * (spot / op - 1) * 1e4       # side-signed displacement in bps
        n = len(grid)
        ent = None
        for i in range(n):
            t = grid[i]
            if not (5 <= t <= win_s * 0.8) or not np.isfinite(disp[i]) or disp[i] <= 0 or not np.isfinite(spot[i]):
                continue
            vv = vol(a, int(absec[i]))
            if not vv or vv <= 0:
                continue
            A = ba[i + 1] if i + 1 < n else ba[i]     # taker ask you actually paid
            if not np.isfinite(A) or not (0.30 <= A <= 0.97):
                continue
            z = disp[i] / (vv * math.sqrt(t))
            if z < zmin or pcal(z) - A - FEE * A * (1 - A) < EDGE_MIN:
                continue
            ent = (i, t, A, z)
            break
        if ent is None:
            continue
        i0, ttl0, A, z = ent
        # emit full post-entry path (entry sec .. settlement sec)
        # half split: temporal by day index across recorder window
        day_idx = (absec[i0] - (pd.Timestamp("2026-06-18").timestamp())) / 86400.0
        half = "IS" if day_idx < 6 else "OOS"
        for j in range(i0, n):
            rows.append((tid, a, half, z, A, win, ttl0, grid[j], int(absec[j] - absec[i0]),
                         disp[j] if np.isfinite(disp[j]) else np.nan,
                         bb[j] if np.isfinite(bb[j]) else np.nan))
    df = pd.DataFrame(rows, columns=["tid", "asset", "half", "z", "ask", "win",
                                     "ttl0", "ttl", "sec", "disp", "bid"])
    df.to_parquet(cache)
    print(f"[{interval}] saved {cache}  entries={df.tid.nunique()} rows={len(df)}")
    return df


def evaluate(df, interval, ttl_gate=True, ttl_cap=240):
    """df: tidy post-entry paths. Apply optional 5m ttl<=cap gate at entry.
    Compute hold-to-settle and invalidation-stop variants."""
    ents = df.groupby("tid")
    recs = []
    for tid, g in ents:
        g = g.sort_values("sec")
        r0 = g.iloc[0]
        if ttl_gate and interval == "5m" and r0.ttl0 > ttl_cap:
            continue
        recs.append((tid, g))
    print(f"\n{'='*70}\n{interval.upper()}  entries={len(recs)}  ttl_gate={ttl_gate and interval=='5m'}")

    ask = np.array([g.iloc[0].ask for _, g in recs])
    win = np.array([g.iloc[0].win for _, g in recs])
    asset = np.array([g.iloc[0].asset for _, g in recs])
    half = np.array([g.iloc[0].half for _, g in recs])
    ttl0 = np.array([g.iloc[0].ttl0 for _, g in recs])
    sh = 1.0 / ask; fee = FEE * (1 - ask)
    hold = np.where(win == 1, sh - 1 - fee, -1.0)

    def stop_pnl(band, persist, first_half_only):
        """Return per-entry pnl under invalidation stop.
        band: exit when disp <= band (bps). persist: require 2 consecutive secs.
        first_half_only: only allow stop while ttl > ttl0/2 remaining."""
        out = hold.copy(); stopped = np.zeros(len(recs), bool)
        for k, (_, g) in enumerate(recs):
            secs = g.sec.values; disp = g.disp.values; bid = g.bid.values; ttl = g.ttl.values
            below = disp <= band
            # skip entry second itself (sec==0 by construction disp>0, but guard)
            trig = None
            for m in range(1, len(secs)):
                if not np.isfinite(disp[m]):
                    continue
                if first_half_only and ttl[m] <= ttl0[k] / 2.0:
                    break
                if below[m] and (not persist or (m + 1 < len(secs) and below[m + 1])):
                    trig = m
                    break
            if trig is None:
                continue
            # honest fill: bid at trigger sec, ffilled already; if still nan -> hold
            b = bid[trig]
            if not np.isfinite(b):
                # search backward for last known bid
                bb = bid[:trig + 1]; bb = bb[np.isfinite(bb)]
                if len(bb) == 0:
                    continue  # cannot exit -> settlement
                b = bb[-1]
            out[k] = sh[k] * b - 1.0        # sell: proceeds = shares*bid; cost basis already 1; no PM fee on sell
            stopped[k] = True
        return out, stopped

    def summ(pnl, label, stopped=None):
        ev = pnl.mean(); tot = pnl.sum(); wr = (pnl > 0).mean()
        shp = ev / pnl.std() if pnl.std() > 0 else float("nan")
        extra = ""
        if stopped is not None:
            n_st = stopped.sum()
            whip = (stopped & (win == 1)).sum()      # winners stopped
            cut = (stopped & (win == 0)).sum()       # losers cut
            # net $ = (loser dollars saved) - (winner dollars lost) vs hold
            d = pnl - hold
            saved = d[stopped & (win == 0)].sum()
            lost = d[stopped & (win == 1)].sum()
            extra = (f"  stopped={n_st}({n_st/len(pnl):.0%}) cutL={cut} whipW={whip} "
                     f"net$(saved+lost)={d[stopped].sum():+.2f}")
        print(f"  {label:<34} EV/$1={ev:+.4f} tot={tot:+7.1f} win={wr:.1%} Sharpe={shp:+.3f}{extra}")
        return ev, pnl.std()

    print(f"  n={len(recs)} baseWin={win.mean():.1%}")
    ev_h, sd_h = summ(hold, "HOLD-to-settle")
    best = None
    for band in (0.0, -1.0, -2.0):
        for persist in (False, True):
            for fh in (False, True):
                pnl, st = stop_pnl(band, persist, fh)
                lbl = f"stop disp<={band:+.0f} {'persist' if persist else 'touch'} {'1stHalf' if fh else 'anytime'}"
                ev, sd = summ(pnl, lbl, st)
                d_ev = ev - ev_h; d_sd = sd - sd_h
                if best is None or ev > best[0]:
                    best = (ev, lbl, d_ev, d_sd)
    print(f"  >> best vs hold: {best[1]}  dEV/$1={best[2]:+.4f}  dStd={best[3]:+.4f}")

    # asset + half split for the disp<=0 touch anytime variant (the canonical one)
    pnl0, st0 = stop_pnl(0.0, False, False)
    print("  --- disp<=0 touch anytime, split by asset & half (dEV vs hold) ---")
    for key, mask in [("BTC", asset == "btc"), ("ETH", asset == "eth"),
                      ("IS", half == "IS"), ("OOS", half == "OOS")]:
        if mask.sum() < 10:
            print(f"    {key:<5} n={mask.sum():4d}  (too small)"); continue
        dev = pnl0[mask].mean() - hold[mask].mean()
        print(f"    {key:<5} n={mask.sum():4d}  holdEV={hold[mask].mean():+.4f}  stopEV={pnl0[mask].mean():+.4f}  dEV={dev:+.4f}")


if __name__ == "__main__":
    for interval, win_s, zmin, ttl_max in [("5m", 300, 0.45, 240), ("15m", 900, 0.80, None)]:
        df = reconstruct(interval, win_s, zmin, ttl_max)
        evaluate(df, interval, ttl_gate=True)
        if interval == "5m":
            print("\n### 5m WITHOUT ttl<=240 gate (is stop a substitute for the ttl gate?) ###")
            evaluate(df, interval, ttl_gate=False)
