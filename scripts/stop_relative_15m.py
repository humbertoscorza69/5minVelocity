"""15m stop-loss done RIGHT: RELATIVE stop (exit if bid <= entry_ask - X), not an
absolute level. And split by ENTRY PRICE -- does a stop even help cheap entries
(user's point: a stop makes little sense when you got in at 0.35)?
NOTE: BACKTEST. Live has shown adverse-selection artifacts on the cheap dimension,
so treat cheap-entry conclusions as provisional until live-validated.
"""
import io, math, os
import numpy as np, pandas as pd, orjson, zstandard as zstd
DATA = r"C:\Users\tico_\Fable\5minSnip\data"; PM = r"D:\polycrypto\live_l2\polymarket"; BN = r"D:\polycrypto\live_l2\binance"
FEE = 0.07; DAYS = [f"2026-06-{d:02d}" for d in range(18, 30)]; WIN_S = 900; EDGE_MIN = 0.06
OUT = DATA + r"\entries_15m_stop.parquet"

def lines(p):
    if p.endswith(".zst"):
        with open(p, "rb") as f:
            for ln in io.TextIOWrapper(io.BufferedReader(zstd.ZstdDecompressor().stream_reader(f), 1 << 24), encoding="utf-8"): yield ln
    else:
        with open(p, encoding="utf-8", buffering=1 << 24) as f:
            for ln in f: yield ln
def find(b, d):
    for e in (".jsonl.zst", ".jsonl"):
        q = os.path.join(b, d + e)
        if os.path.exists(q): return q

if os.path.exists(OUT):
    E = pd.read_parquet(OUT); print("cached", len(E))
else:
    sig = pd.read_parquet(DATA + r"\strat_signals.parquet"); may = sig[(sig.month == "may") & (sig.is_first == 1)]
    zb = np.array([-1, 0, .3, .6, 1, 1.5, 2, 3, 5, 100]); mids = []; ws = []
    for lo, hi in zip(zb[:-1], zb[1:]):
        s = may[(may.z >= lo) & (may.z < hi)]
        if len(s) >= 20: mids.append(s.z.mean()); ws.append(s.win.mean())
    mids = np.array(mids); ws = np.array(ws)
    def pcal(z): return 0.5 if z <= 0 else float(np.interp(z, mids, ws))
    tokmap = {}
    for day in DAYS:
        p = find(os.path.join(PM, "markets"), day)
        if not p: continue
        for ln in lines(p):
            try: m = orjson.loads(ln).get("market") or {}
            except: continue
            if m.get("interval") != "15m": continue
            a = str(m.get("asset", "")).lower(); ep = m.get("epoch")
            if a not in ("btc", "eth") or ep is None: continue
            s = (ep + WIN_S) * 1000
            if m.get("up_token_id"): tokmap[m["up_token_id"]] = (a, "up", s, ep)
            if m.get("down_token_id"): tokmap[m["down_token_id"]] = (a, "down", s, ep)
    B = {}
    for a, sym in (("btc", "btcusdt"), ("eth", "ethusdt")):
        d = {}
        for day in DAYS:
            p = find(os.path.join(BN, f"{sym}_kline_1s"), day)
            if not p: continue
            for ln in lines(p):
                try: k = orjson.loads(ln)["payload"]["data"]["k"]; d[int(k["t"]) // 1000] = float(k["c"])
                except: continue
        secs = np.array(sorted(d)); B[a] = (secs, np.array([d[s] for s in secs]))
    def cl(a, sec):
        secs, c = B[a]; i = np.searchsorted(secs, sec, side="right") - 1
        return c[i] if (i >= 0 and sec - secs[i] <= 5) else np.nan
    def vol(a, sec, lb=60):
        secs, c = B[a]; i = np.searchsorted(secs, sec, side="right") - 1
        return np.std(np.diff(np.log(c[i - lb:i]))) * 1e4 if i >= lb + 1 else np.nan
    book = {}
    for day in DAYS:
        p = find(os.path.join(PM, "best_bid_ask"), day)
        if not p: continue
        for ln in lines(p):
            try: pp = orjson.loads(ln)["payload"]
            except: continue
            info = tokmap.get(pp.get("asset_id"))
            if info is None: continue
            s = info[2]; ts = int(pp["timestamp"])
            if ts < s - (WIN_S + 5) * 1000 or ts > s: continue
            try: book.setdefault(pp["asset_id"], {})[ts // 1000] = (float(pp["best_bid"]), float(pp["best_ask"]))
            except: continue
        print("parsed", day)
    R = []
    for tid, (a, side, settle_ms, ep) in tokmap.items():
        ss = settle_ms // 1000
        grid = np.arange(WIN_S, -1, -1); absec = ss - grid; ser = pd.Series(book.get(tid, {}))
        bb = ser.reindex(absec).map(lambda x: x[0] if isinstance(x, tuple) else np.nan).ffill().values
        ba = ser.reindex(absec).map(lambda x: x[1] if isinstance(x, tuple) else np.nan).ffill().values
        op = cl(a, ep - 1); fin = cl(a, ss - 1)
        if not (np.isfinite(op) and np.isfinite(fin)): continue
        win = int((side == "up") == (fin >= op)); spot = np.array([cl(a, s) for s in absec]); sgn = 1.0 if side == "up" else -1.0
        disp = sgn * (spot / op - 1) * 1e4
        bfill = np.where(np.isfinite(bb), bb, 2.0); sufmin = np.minimum.accumulate(bfill[::-1])[::-1]
        n = len(grid)
        for i in range(n):
            t = grid[i]
            if not (5 <= t <= WIN_S * 0.8) or not np.isfinite(disp[i]) or disp[i] <= 0 or not np.isfinite(spot[i]): continue
            vv = vol(a, int(absec[i]))
            if not vv or vv <= 0: continue
            A = ba[i + 1] if i + 1 < n else ba[i]
            if not np.isfinite(A) or not (0.30 <= A <= 0.97): continue
            z = disp[i] / (vv * math.sqrt(t))
            if z < 0.45 or pcal(z) - A - FEE * A * (1 - A) < EDGE_MIN: continue
            R.append({"sec_in": int(absec[i] - ep), "ask": A, "win": win, "minbid": sufmin[i + 1] if i + 1 < n else 2.0}); break
    E = pd.DataFrame(R); E.to_parquet(OUT); print("saved", len(E))

E = E[E.sec_in >= 540].copy()   # late entries (the good 15m zone)
ask = E.ask.values; win = E.win.values; minb = E.minbid.values
sh = 1.0 / ask; fee = 0.07 * (1 - ask)
hold = np.where(win == 1, sh - 1 - fee, -1.0)
print(f"\n15m LATE (n={len(E)}, win={win.mean():.0%}) HOLD: EV/$1={hold.mean():+.3f} total={hold.sum():+.0f}")
print("\nRELATIVE stop (exit if bid <= entry_ask - X):")
for X in [0.10, 0.15, 0.20, 0.25]:
    lvl = ask - X
    stopped = minb <= lvl
    pnl = np.where(stopped, sh * lvl - 1 - fee, hold)   # exit at ask-X
    whip = (stopped & (win == 1)).sum() / max(1, stopped.sum())
    print(f"  stop entry-{X:.2f}: stopped={stopped.mean():.0%} whipsaw={whip:.0%} EV/$1={pnl.mean():+.3f} total={pnl.sum():+.0f}")
print("\nDoes the stop help by ENTRY PRICE? (relative stop entry-0.15)")
X = 0.15
for lo, hi in [(0.30, 0.50), (0.50, 0.60), (0.60, 0.70), (0.70, 1.0)]:
    m = (ask >= lo) & (ask < hi)
    if m.sum() < 5: continue
    h = hold[m]
    lvl = ask[m] - X; st = minb[m] <= lvl
    p = np.where(st, sh[m] * lvl - 1 - fee[m], h)
    print(f"  entry {lo:.2f}-{hi:.2f} n={m.sum():4d}: HOLD EV={h.mean():+.3f} -> STOP EV={p.mean():+.3f}  (stop {'HELPS' if p.mean()>h.mean() else 'HURTS'})")
