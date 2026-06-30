"""Give-back / take-profit study (current gate: z>=0.45, edge>=0.06).

The user's observation: "most times we lose, we were in profit at some point."
Tests, on the recorder (Jun 18-29):
  1) GIVE-BACK rate: of LOSERS, how often did the bid rise above entry (we were
     in profit) before settling to 0 — and by how much (max favorable excursion).
  2) TAKE-PROFIT exits: if we exit (maker, locked) the first time bid >= entry+TP,
     else hold to settle — does total P&L / win / Sharpe beat pure HOLD?
Reuses exit_study's book+kline reconstruction.
"""
import io, math, os
import numpy as np, pandas as pd, orjson, zstandard as zstd
pd.set_option("display.width", 240)
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

# frozen May calibration with the pcal(0)=0.5 anchor (matches the shipped fix)
sig = pd.read_parquet(DATA + r"\strat_signals.parquet"); may = sig[(sig.month == "may") & (sig.is_first == 1)]
zb = np.array([-1, 0, .3, .6, 1, 1.5, 2, 3, 5, 100]); mids = []; ws = []
for lo, hi in zip(zb[:-1], zb[1:]):
    s = may[(may.z >= lo) & (may.z < hi)]
    if len(s) >= 20: mids.append(s.z.mean()); ws.append(s.win.mean())
mids = np.array(mids); ws = np.array(ws)
def pcal(z):
    if z <= 0: return 0.5
    return float(np.interp(z, mids, ws))

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
    n = len(grid); vel = np.full(n, np.nan); vel[2:] = sgn * (spot[2:] / spot[:-2] - 1) * 1e4; disp = sgn * (spot / op - 1) * 1e4
    sufbid = np.maximum.accumulate(np.where(np.isfinite(bb), bb, -1)[::-1])[::-1]  # max bid from i to settle
    for i in range(n):
        t = grid[i]
        if not (5 <= t <= 180) or not np.isfinite(disp[i]) or disp[i] <= 0 or not np.isfinite(spot[i]): continue
        vv = vol(a, int(absec[i]))
        if not vv or vv <= 0: continue
        A = ba[i + 1] if i + 1 < n else ba[i]
        if not np.isfinite(A) or not (0.30 <= A <= 0.97): continue
        z = disp[i] / (vv * math.sqrt(t))
        if z < ZMIN: continue                       # CURRENT gate
        edge = pcal(z) - A - FEE * A * (1 - A)
        if edge < EDGE_MIN: continue
        fmax = sufbid[i + 1] if i + 1 < n else -1   # best bid achievable AFTER entry
        R.append({"a": a, "z": z, "edge": edge, "ask": A, "win": win, "fmax": fmax})
        break
E = pd.DataFrame(R)
print(f"\nentries (z>={ZMIN}, edge>={EDGE_MIN}): {len(E)}   win={E.win.mean():.1%}")
ask = E.ask.values; win = E.win.values; sh = STAKE / ask; efee = sh * FEE * ask * (1 - ask); fmax = E.fmax.values

# ---- 1) GIVE-BACK: of LOSERS, how often were we in profit (bid rose above entry) ----
losers = E[E.win == 0]
for thr in [0.0, 0.05, 0.10, 0.20]:
    inprof = (losers.fmax >= losers.ask + thr).mean()
    print(f"  losers that reached bid >= entry+{thr:.2f}: {inprof:.1%}")
mfe_los = (losers.fmax - losers.ask).clip(lower=-1)
print(f"  loser max-favorable-excursion (bid-entry): median={mfe_los.median():+.3f}  mean={mfe_los.mean():+.3f}")
mfe_win = (E[E.win == 1].fmax - E[E.win == 1].ask)
print(f"  winner MFE: median={mfe_win.median():+.3f}")

def stats(name, pnl, won):
    ev = pnl.mean(); sd = pnl.std()
    print(f"  {name:<22} win={won.mean():.3f} EV/trade=${ev:+.3f} Sharpe={ev/sd:+.3f} TOTAL=${pnl.sum():+.0f}")

print("\n=== HOLD vs TAKE-PROFIT (exit maker at entry+TP if bid ever reaches it, else hold) ===")
hold = sh * win - STAKE - efee
stats("HOLD to settle", hold, win.astype(float))
for tp in [0.05, 0.10, 0.15, 0.20, 0.30]:
    S = ask + tp
    filled = (fmax >= S - 1e-9) & (S < 1.0)
    pnl = np.where(filled, sh * (S - ask) - efee, sh * win - STAKE - efee)  # locked TP (maker, no exit fee) else hold
    won = np.where(filled, 1.0, win.astype(float))
    print(f"   TP=+{tp:.2f} fill_rate={filled.mean():.0%}", end="")
    ev = pnl.mean(); print(f"  win={won.mean():.3f} EV/trade=${ev:+.3f} Sharpe={ev/pnl.std():+.3f} TOTAL=${pnl.sum():+.0f}")

# ---- 2) SELL-AT-EXTREME (maker) viability: does the bid actually reach the level? ----
print("\n=== SELL-AT-EXTREME (maker) — fill rate = does bid ever reach the level? ===")
W = E[E.win == 1]; L = E[E.win == 0]
for lvl in [0.90, 0.95, 0.97, 0.99]:
    wfill = (W.fmax >= lvl).mean(); lfill = (L.fmax >= lvl).mean()
    print(f"  level {lvl:.2f}: WINNERS reach it {wfill:.0%}  |  LOSERS reach it {lfill:.0%}")
print("\n=== P&L: maker SELL at level (fill->lock level; else HOLD to settle) ===")
holdp = sh * win - STAKE - efee
print(f"  HOLD baseline: win={win.mean():.3f} EV/trade=${holdp.mean():+.3f} TOTAL=${holdp.sum():+.0f}")
for lvl in [0.90, 0.95, 0.97, 0.99]:
    filled = (fmax >= lvl) & (lvl < 1.0)
    pnl = np.where(filled, sh * (lvl - ask) - efee, sh * win - STAKE - efee)
    won = np.where(filled, 1.0, win.astype(float))
    redeem_needed = (~filled).mean()  # fraction still needing redeem fallback
    print(f"  SELL@{lvl:.2f} fill={filled.mean():.0%} (redeem-still-needed={redeem_needed:.0%})  win={won.mean():.3f} EV/trade=${pnl.mean():+.3f} TOTAL=${pnl.sum():+.0f}")
