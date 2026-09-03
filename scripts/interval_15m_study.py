"""15m BTC/ETH backtest: does the z-lag edge exist at the 15-minute interval too?
Same signal (z = disp/(vol*sqrt(ttl)), enter first z>=0.45 with edge>=0.06, hold
to settle). Compare win-by-z + z>=0.45 EV vs the 5m baseline. 15m window=900s.
pcal is the 5m May curve (proxy; the RAW win-by-z is what matters for 'edge exists?').
"""
import io, math, os
import numpy as np, pandas as pd, orjson, zstandard as zstd
DATA = r"C:\Users\tico_\Fable\5minSnip\data"; PM = r"D:\polycrypto\live_l2\polymarket"; BN = r"D:\polycrypto\live_l2\binance"
FEE = 0.07; DAYS = [f"2026-06-{d:02d}" for d in range(18, 30)]
INTERVAL, WIN_S = "15m", 900
ZMIN, EDGE_MIN = 0.45, 0.06

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
        if m.get("interval") != INTERVAL: continue
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
        try: book.setdefault(pp["asset_id"], {})[ts // 1000] = float(pp["best_ask"])
        except: continue
    print("parsed", day)

R = []
GRID = WIN_S  # seconds before settle to scan back over
for tid, (a, side, settle_ms, ep) in tokmap.items():
    ss = settle_ms // 1000
    grid = np.arange(GRID, -1, -1); absec = ss - grid; ser = pd.Series(book.get(tid, {}))
    ba = ser.reindex(absec).ffill().values
    op = cl(a, ep - 1); fin = cl(a, ss - 1)
    if not (np.isfinite(op) and np.isfinite(fin)): continue
    win = int((side == "up") == (fin >= op)); spot = np.array([cl(a, s) for s in absec]); sgn = 1.0 if side == "up" else -1.0
    disp = sgn * (spot / op - 1) * 1e4
    n = len(grid)
    for i in range(n):
        t = grid[i]
        if not (5 <= t <= WIN_S * 0.6) or not np.isfinite(disp[i]) or disp[i] <= 0 or not np.isfinite(spot[i]): continue
        vv = vol(a, int(absec[i]))
        if not vv or vv <= 0: continue
        A = ba[i + 1] if i + 1 < n else ba[i]
        if not np.isfinite(A) or not (0.30 <= A <= 0.97): continue
        z = disp[i] / (vv * math.sqrt(t))
        if z < ZMIN: continue
        if pcal(z) - A - FEE * A * (1 - A) < EDGE_MIN: continue
        sh = 1.0 / A; net1 = (sh - 1 - sh * FEE * A * (1 - A)) if win else -1.0
        R.append({"z": z, "ask": A, "win": win, "net1": net1}); break
E = pd.DataFrame(R)
print(f"\n=== {INTERVAL} BTC/ETH ===")
print(f"entries (z>={ZMIN}, edge>={EDGE_MIN}): {len(E)}  win={E.win.mean():.1%}  EV/$1={E.net1.mean():+.3f}  total={E.net1.sum():+.1f}")
if len(E):
    print("\nwin rate by z bucket (does displacement predict at 15m?):")
    for lo, hi in [(0.45, 0.8), (0.8, 1.5), (1.5, 99)]:
        g = E[(E.z >= lo) & (E.z < hi)]
        if len(g): print(f"  z [{lo},{hi}): n={len(g):4d} win={g.win.mean():.0%} EV/$1={g.net1.mean():+.3f}")
    print("\n(compare: 5m baseline was ~68-77% win, EV ~+0.14/$1 idealized)")
PY