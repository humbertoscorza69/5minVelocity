"""Make 15m better: (A) does ENTRY TIMING within the 15m window matter (more room
to pick the side)? (B) 15m-specific z_min sweep. (C) stop-loss on 15m (more room to
cut losers over 15 min?). (D) 15m win-by-z curve (for its OWN recal/pcal).
"""
import io, math, os
import numpy as np, pandas as pd, orjson, zstandard as zstd
DATA = r"C:\Users\tico_\Fable\5minSnip\data"; PM = r"D:\polycrypto\live_l2\polymarket"; BN = r"D:\polycrypto\live_l2\binance"
FEE = 0.07; DAYS = [f"2026-06-{d:02d}" for d in range(18, 30)]
WIN_S = 900; EDGE_MIN = 0.06

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
        if z < 0.45: continue
        if pcal(z) - A - FEE * A * (1 - A) < EDGE_MIN: continue
        R.append({"sec_in": absec[i] - ep, "z": z, "ask": A, "win": win,
                  "minbid": sufmin[i + 1] if i + 1 < n else 2.0}); break
E = pd.DataFrame(R)
E["net1"] = np.where(E.win == 1, 1.0 / E.ask - 1 - 0.07 * (1 - E.ask), -1.0)
print(f"\n15m entries: {len(E)} win={E.win.mean():.1%} EV/$1={E.net1.mean():+.3f}")

print("\n(A) ENTRY TIMING -- win/EV by seconds INTO the 15m window (more room to pick side?):")
for lo, hi in [(0, 60), (60, 180), (180, 360), (360, 540), (540, 720)]:
    g = E[(E.sec_in >= lo) & (E.sec_in < hi)]
    if len(g): print(f"  {lo:>3}-{hi:<3}s in: n={len(g):4d} win={g.win.mean():.0%} EV/$1={g.net1.mean():+.3f}")

print("\n(B) 15m z_min sweep:")
for zt in [0.45, 0.6, 0.8, 1.0, 1.5]:
    g = E[E.z >= zt]
    if len(g): print(f"  z>={zt:.2f}: n={len(g):4d} win={g.win.mean():.0%} EV/$1={g.net1.mean():+.3f} total={g.net1.sum():+.0f}")

print("\n(C) STOP-LOSS on 15m (hold vs exit when bid<=L):")
sh = 1.0 / E.ask.values; fee = 0.07 * (1 - E.ask.values); win = E.win.values; minb = E.minbid.values; ask = E.ask.values
hold = np.where(win == 1, sh - 1 - fee, -1.0)
print(f"  HOLD: win={win.mean():.0%} EV/$1={hold.mean():+.3f} Sharpe={hold.mean()/hold.std():+.3f} total={hold.sum():+.0f}")
for L in [0.45, 0.40, 0.35, 0.30, 0.25]:
    stopped = minb <= L
    pnl = np.where(stopped, sh * L - 1 - fee, np.where(win == 1, sh - 1 - fee, -1.0))
    whip = (stopped & (win == 1)).sum() / max(1, stopped.sum())
    print(f"  stop@{L:.2f}: stopped={stopped.mean():.0%} whipsaw={whip:.0%} EV/$1={pnl.mean():+.3f} Sharpe={pnl.mean()/pnl.std():+.3f} total={pnl.sum():+.0f}")

print("\n(D) 15m WIN-BY-Z (its own calibration curve, vs 5m):")
for lo, hi in [(0.45, 0.7), (0.7, 1.0), (1.0, 1.5), (1.5, 2.5), (2.5, 99)]:
    g = E[(E.z >= lo) & (E.z < hi)]
    if len(g): print(f"  z [{lo},{hi}): n={len(g):4d} win={g.win.mean():.3f}")
