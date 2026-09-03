"""Stop-loss study: does cutting losers mid-trade beat hold-to-settle?

User's idea: avg loss (-$1.08) >> avg win (+$0.61); if we exit losers early we
shrink the losses and EV improves. The risk: winners often DIP below entry before
recovering (give-back study: 80% of losers were green at some point => symmetric,
many winners are temporarily red). A stop may whipsaw out of those winners.

For each entry (z>=0.45, edge>=0.06) we track the MIN bid after entry. Stop at an
absolute bid level L: if bid ever <= L, exit there (sell, no taker fee); else hold
to settle. Compare EV/win/total vs HOLD, and report the WHIPSAW rate (stopped
positions that would have WON).
"""
import io, math, os
import numpy as np, pandas as pd, orjson, zstandard as zstd
DATA = r"C:\Users\tico_\Fable\5minSnip\data"; PM = r"D:\polycrypto\live_l2\polymarket"; BN = r"D:\polycrypto\live_l2\binance"
FEE, STAKE = 0.07, 10.0; DAYS = [f"2026-06-{d:02d}" for d in range(18, 30)]
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
        if m.get("interval") != "5m": continue
        a = str(m.get("asset", "")).lower(); ep = m.get("epoch")
        if a not in ("btc", "eth") or ep is None: continue
        s = (ep + 300) * 1000
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
        if ts < s - 205000 or ts > s: continue
        try: book.setdefault(pp["asset_id"], {})[ts // 1000] = (float(pp["best_bid"]), float(pp["best_ask"]))
        except: continue
    print("parsed", day)

R = []
for tid, sm_ in book.items():
    a, side, settle_ms, ep = tokmap[tid]; ss = settle_ms // 1000
    grid = np.arange(200, -1, -1); absec = ss - grid; ser = pd.Series(sm_)
    bb = ser.reindex(absec).map(lambda x: x[0] if isinstance(x, tuple) else np.nan).ffill().values
    ba = ser.reindex(absec).map(lambda x: x[1] if isinstance(x, tuple) else np.nan).ffill().values
    op = cl(a, ep - 1); fin = cl(a, ss - 1)
    if not (np.isfinite(op) and np.isfinite(fin)): continue
    win = int((side == "up") == (fin >= op)); spot = np.array([cl(a, s) for s in absec]); sgn = 1.0 if side == "up" else -1.0
    n = len(grid); disp = sgn * (spot / op - 1) * 1e4
    # suffix-MIN of bid from i to settle (worst drawdown available to a stop)
    bfill = np.where(np.isfinite(bb), bb, 2.0)
    sufmin = np.minimum.accumulate(bfill[::-1])[::-1]
    for i in range(n):
        t = grid[i]
        if not (5 <= t <= 180) or not np.isfinite(disp[i]) or disp[i] <= 0 or not np.isfinite(spot[i]): continue
        vv = vol(a, int(absec[i]))
        if not vv or vv <= 0: continue
        A = ba[i + 1] if i + 1 < n else ba[i]
        if not np.isfinite(A) or not (0.30 <= A <= 0.97): continue
        z = disp[i] / (vv * math.sqrt(t))
        if z < ZMIN: continue
        if pcal(z) - A - FEE * A * (1 - A) < EDGE_MIN: continue
        mn = sufmin[i + 1] if i + 1 < n else 2.0
        R.append({"ask": A, "win": win, "minbid": mn}); break
E = pd.DataFrame(R)
print(f"\nentries: {len(E)}  win={E.win.mean():.1%}")
ask = E.ask.values; win = E.win.values; sh = STAKE / ask; efee = sh * FEE * ask * (1 - ask); minb = E.minbid.values

hold = sh * win - STAKE - efee
print(f"\nHOLD baseline: win={win.mean():.3f} EV/trade=${hold.mean():+.3f} Sharpe={hold.mean()/hold.std():+.3f} TOTAL=${hold.sum():+.0f}")
print(f"\n{'stop@bid':>9} {'stopped%':>8} {'whipsaw%':>8} {'win%':>6} {'EV/trade':>9} {'Sharpe':>7} {'TOTAL':>8}")
for L in [0.45, 0.40, 0.35, 0.30, 0.25, 0.20]:
    stopped = minb <= L
    # exit at L (sell, no taker fee): net = sh*(L - ask) ; else hold
    pnl = np.where(stopped, sh * (L - ask), sh * win - STAKE - efee)
    won = np.where(stopped, 0.0, win.astype(float))  # a stop is a (small) loss
    whip = (stopped & (win == 1)).sum() / max(1, stopped.sum())  # stopped-but-would-have-won
    print(f"{L:>9.2f} {stopped.mean():>7.0%} {whip:>7.0%} {won.mean():>5.0%} {pnl.mean():>+9.3f} {pnl.mean()/pnl.std():>+7.3f} {pnl.sum():>+8.0f}")
