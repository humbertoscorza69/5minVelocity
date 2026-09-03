"""Tick-data backtest: can a DENOISED tick signal beat the 1s-bar baseline (~70%)?

We have the Polymarket recorder (markets + book) for June; pair it with Binance
aggTrades (tick) downloaded from data.binance.vision. For each 5m market we
reconstruct a fine (100ms) price grid from ticks, then test several entry
formulas head-to-head on the SAME settlement outcomes + the SAME PM asks:

  F0 1s-baseline   : z from 1s bars, first sec z>=0.45  (control ~70%)
  F1 tick-raw      : z from instantaneous 100ms price   (expect ~live 57%)
  F2 tick-EMA      : z from EMA-smoothed tick price (tau ~1s)  [denoise]
  F3 tick-persist  : z>=thr AND still z>=thr K*100ms later     [confirm]
  F4 tick-volwin   : z with vol over a longer (e.g. 120s) tick window

Win/lose = Binance open(epoch) vs close(epoch+300). EV per $1 from the PM ask.
"""
import io, math, os, zipfile, csv as _csv
import numpy as np, pandas as pd, orjson, zstandard as zstd
DATA = r"C:\Users\tico_\Fable\5minSnip\data"; PM = r"D:\polycrypto\live_l2\polymarket"
TICKS = r"D:\polycrypto\aggtrades"
FEE = 0.07; DAYS = [f"2026-06-{d:02d}" for d in range(23, 30)]  # daily tick files available
ZMIN, EDGE_MIN, GRID_MS = 0.45, 0.06, 100

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

# ---- frozen May calibration (pcal(0)=0.5 anchored) ----
sig = pd.read_parquet(DATA + r"\strat_signals.parquet"); may = sig[(sig.month == "may") & (sig.is_first == 1)]
zb = np.array([-1, 0, .3, .6, 1, 1.5, 2, 3, 5, 100]); mids = []; ws = []
for lo, hi in zip(zb[:-1], zb[1:]):
    s = may[(may.z >= lo) & (may.z < hi)]
    if len(s) >= 20: mids.append(s.z.mean()); ws.append(s.win.mean())
mids = np.array(mids); ws = np.array(ws)
def pcal(z): return 0.5 if z <= 0 else float(np.interp(z, mids, ws))

# ---- load Binance ticks -> per-asset 100ms last-price grid over the window range ----
# Determine the unix-second range of the recorder days.
import datetime as dt
day0 = dt.datetime(2026, 6, 23, tzinfo=dt.timezone.utc); day1 = dt.datetime(2026, 6, 30, tzinfo=dt.timezone.utc)
T0 = int(day0.timestamp()); T1 = int(day1.timestamp())
NG = (T1 - T0) * 1000 // GRID_MS
def load_ticks(sym):
    """Stream the DAILY aggTrades zips -> a 100ms last-price grid (float32), T0..T1."""
    grid = np.full(NG, np.nan, dtype=np.float32)
    for day in DAYS:
        zp = os.path.join(TICKS, f"{sym}-aggTrades-{day}.zip")
        if not os.path.exists(zp):
            print("MISSING", zp); continue
        with zipfile.ZipFile(zp) as z:
            name = z.namelist()[0]
            with z.open(name) as f:
                rdr = _csv.reader(io.TextIOWrapper(f, encoding="utf-8"))
                for row in rdr:
                    # cols: aggId, price, qty, first, last, ts, isBuyerMaker, isBestMatch
                    try:
                        px = float(row[1]); ts = int(row[5])
                    except (ValueError, IndexError):
                        continue
                    if ts > 1e14: ts //= 1000     # microseconds -> ms
                    gi = (ts - T0 * 1000) // GRID_MS
                    if 0 <= gi < NG: grid[gi] = px
    # forward-fill gaps
    last = np.nan
    for i in range(NG):
        if np.isnan(grid[i]): grid[i] = last
        else: last = grid[i]
    return grid

print("loading ticks (this is the slow part)...")
G = {a: load_ticks(s) for a, s in (("btc", "BTCUSDT"), ("eth", "ETHUSDT"))}
print("ticks loaded:", {a: int(np.isfinite(v).sum()) for a, v in G.items()})
def gpx(a, ms):
    gi = (ms - T0 * 1000) // GRID_MS
    return G[a][gi] if 0 <= gi < NG else np.nan

# ---- PM markets + book ----
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

# helpers on the 100ms grid
def disp_at(a, ms, op, sgn):  # side-signed displacement bps
    px = gpx(a, ms); return np.nan if not np.isfinite(px) else sgn * (px / op - 1) * 1e4
def vol_grid(a, ms, win_s):   # bps/s vol from 100ms grid over win_s seconds
    gi = (ms - T0 * 1000) // GRID_MS; k = win_s * 1000 // GRID_MS
    if gi - k < 0: return np.nan
    seg = G[a][gi - k:gi]
    seg = seg[np.isfinite(seg)]
    if len(seg) < 10: return np.nan
    r = np.diff(np.log(seg))
    return np.std(r) * 1e4 * math.sqrt(1000 / GRID_MS)  # scale to per-second

results = {f: {"w": [], "n1": []} for f in ["F0_1s", "F1_tickraw", "F2_ema", "F3_persist", "F4_volwin"]}
def ema_series(a, ep, ss, tau_ms):
    gi0 = (ep * 1000 - T0 * 1000) // GRID_MS; gi1 = (ss * 1000 - T0 * 1000) // GRID_MS
    seg = G[a][gi0:gi1].copy()
    if len(seg) == 0 or not np.isfinite(seg).any(): return None
    alpha = GRID_MS / tau_ms; out = np.empty_like(seg); e = seg[0]
    for i, x in enumerate(seg):
        if np.isfinite(x): e = e + alpha * (x - e)
        out[i] = e
    return out

# Load PM book once per token (for the ask), reuse exit_study reconstruction
book = {}
for day in DAYS:
    bp = find(os.path.join(PM, "best_bid_ask"), day)
    if not bp: continue
    for ln in lines(bp):
        try: pp = orjson.loads(ln)["payload"]
        except: continue
        info = tokmap.get(pp.get("asset_id"))
        if info is None: continue
        s = info[2]; ts = int(pp["timestamp"])
        if ts < s - 205000 or ts > s: continue
        try: book.setdefault(pp["asset_id"], {})[ts // 1000] = float(pp["best_ask"])
        except: continue
    print("parsed book", day)

def ask_at(tid, sec):
    b = book.get(tid)
    if not b: return np.nan
    # nearest prior second within 5s
    for d in range(0, 6):
        if sec - d in b: return b[sec - d]
    return np.nan

for tid, (a, side, settle_ms, ep) in tokmap.items():
    ss = ep + 300; sgn = 1.0 if side == "up" else -1.0
    op = gpx(a, ep * 1000); fin = gpx(a, (ss - 1) * 1000)
    if not (np.isfinite(op) and np.isfinite(fin)): continue
    win = int((side == "up") == (fin >= op))
    ema = ema_series(a, ep, ss, 1000)  # 1s-tau EMA over the window

    def try_enter(price_fn, vol_win, persist_k=0, step_ms=1000):
        for ms in range((ep + 5) * 1000, (ep + 180) * 1000, step_ms):
            t = ms // 1000
            d = price_fn(ms)
            if not np.isfinite(d) or d <= 0: continue
            vv = vol_grid(a, ms, vol_win)
            if not vv or vv <= 0: continue
            z = d / (vv * math.sqrt(max(1, ss - t)))
            if z < ZMIN: continue
            if persist_k:  # confirmation: still z>=ZMIN persist_k*100ms later
                d2 = price_fn(ms + persist_k * GRID_MS)
                if not np.isfinite(d2): continue
                vv2 = vol_grid(a, ms + persist_k * GRID_MS, vol_win)
                z2 = d2 / (vv2 * math.sqrt(max(1, ss - t))) if vv2 and vv2 > 0 else 0
                if z2 < ZMIN: continue
            A = ask_at(tid, t + 1)
            if not np.isfinite(A) or not (0.30 <= A <= 0.97): continue
            if pcal(z) - A - FEE * A * (1 - A) < EDGE_MIN: continue
            sh = 1.0 / A; net1 = (sh - 1 - sh * FEE * A * (1 - A)) if win else -1.0
            return win, net1
        return None

    # F0 1s baseline: 1-second steps (the validated path / control)
    r = try_enter(lambda ms: disp_at(a, (ms // 1000) * 1000, op, sgn), 60, step_ms=1000)
    if r: results["F0_1s"]["w"].append(r[0]); results["F0_1s"]["n1"].append(r[1])
    # F1 tick-raw SUB-SECOND (100ms steps): reproduces live tick-driven
    r = try_enter(lambda ms: disp_at(a, ms, op, sgn), 60, step_ms=100)
    if r: results["F1_tickraw"]["w"].append(r[0]); results["F1_tickraw"]["n1"].append(r[1])
    # F2 EMA-smoothed price disp, sub-second
    def ema_disp(ms):
        gi = (ms - ep * 1000) // GRID_MS
        if ema is None or not (0 <= gi < len(ema)): return np.nan
        return sgn * (ema[gi] / op - 1) * 1e4
    r = try_enter(ema_disp, 60, step_ms=100)
    if r: results["F2_ema"]["w"].append(r[0]); results["F2_ema"]["n1"].append(r[1])
    # F3 persistence sub-second: z>=ZMIN AND still z>=ZMIN 1s (10*100ms) later
    r = try_enter(lambda ms: disp_at(a, ms, op, sgn), 60, persist_k=10, step_ms=100)
    if r: results["F3_persist"]["w"].append(r[0]); results["F3_persist"]["n1"].append(r[1])
    # F4 longer vol window (120s), sub-second
    r = try_enter(lambda ms: disp_at(a, ms, op, sgn), 120, step_ms=100)
    if r: results["F4_volwin"]["w"].append(r[0]); results["F4_volwin"]["n1"].append(r[1])

print(f"\n{'formula':<14} {'entries':>7} {'win%':>6} {'EV/$1':>7} {'TOTAL/$1':>9}")
for f, d in results.items():
    n = len(d["w"])
    if n: print(f"{f:<14} {n:>7} {np.mean(d['w']):>5.0%} {np.mean(d['n1']):>+7.3f} {np.sum(d['n1']):>+9.1f}")
