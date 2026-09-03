"""Platt vs scalar recalibration — walk-forward OOS study for the lead-lag bot.

QUESTION 1 (recalibration): The bot maps vol-normalized displacement z -> pcal(z) -> win prob,
then de-biases with a SCALAR: p_adj = clamp(pcal - bias), bias = mean(pred-realized) over a
rolling window. A live autopsy says the miss is a SHAPE error (both tails run ~9pts under model).
Test a 2-param ONLINE PLATT layer: p_adj = sigmoid(a + b*logit(pcal)), (a,b) refit by logistic
regression on the rolling window each settlement. Compare walk-forward OOS:
  M0 raw pcal | M1 scalar bias (production) | M2 Platt | (M3 isotonic, reference only).
Metrics: Brier, log-loss, reliability error, and — the decider — GATED-SET realized EV
(trades still clearing edge>=0.06 after recal), per interval (5m / 15m).

QUESTION 2 (edge anti-predictivity): does computed edge (pcal-ask-fee) anti-predict realized PnL
at the top? Tercile the gated live trades by edge, report realized EV/$1 and total PnL. Go/no-go
for edge-proportional sizing.

Data: live_trades_enriched.parquet (638 settled live trades, has stored production `p`=pcal),
recorder_trades_enriched.parquet (2319 recorder trades, pcal rebuilt from strat_signals May curve).
fee = 0.07*ask*(1-ask); win pays $1/share. WALK-FORWARD, no look-ahead.
"""
import os, math, numpy as np, pandas as pd

BASE = r"C:\Users\tico_\AppData\Local\Temp\claude\C--Users-tico--Fable-5minSnip\69a6d861-3c7f-49e1-9dcb-334f916f35ed\scratchpad"
DATA = r"C:\Users\tico_\Fable\5minSnip\data"
FEE = 0.07
EDGE_MIN = 0.06
EPS = 1e-6

def logit(p):
    p = np.clip(p, EPS, 1 - EPS)
    return np.log(p / (1 - p))

def sigmoid(x):
    return 1.0 / (1.0 + np.exp(-np.clip(x, -30, 30)))

def fee_of(ask):
    return FEE * ask * (1 - ask)

def net1(win, ask):
    # realized EV per $1 staked: win -> 1/ask - 1 - fee ; loss -> -1
    return np.where(win, 1.0 / ask - 1.0 - fee_of(ask), -1.0)

# ---- Platt fit: 2-param logistic regression of realized outcome on logit(pcal) ----
def fit_platt(x_logit, y, iters=100, l2=1e-3, clamp_b=None):
    """Newton IRLS for a + b*x. l2 ridge for stability on small windows."""
    a, b = 0.0, 1.0
    x = x_logit
    for _ in range(iters):
        eta = a + b * x
        p = sigmoid(eta)
        w = np.clip(p * (1 - p), 1e-6, None)
        # gradient of -loglik + l2 ridge on (a,b)
        r = p - y
        g0 = r.sum() + l2 * a
        g1 = (r * x).sum() + l2 * b
        # Hessian
        h00 = w.sum() + l2
        h01 = (w * x).sum()
        h11 = (w * x * x).sum() + l2
        det = h00 * h11 - h01 * h01
        if abs(det) < 1e-12:
            break
        da = (h11 * g0 - h01 * g1) / det
        db = (-h01 * g0 + h00 * g1) / det
        a -= da; b -= db
        if abs(da) < 1e-8 and abs(db) < 1e-8:
            break
    if clamp_b is not None:
        b = min(max(b, clamp_b[0]), clamp_b[1])
    return a, b

# ---- isotonic (reference only, PAV) ----
def fit_isotonic(x, y):
    order = np.argsort(x)
    xs, ys = x[order], y[order].astype(float)
    # pool adjacent violators
    w = np.ones_like(ys)
    vals = ys.copy(); wt = w.copy(); idx = [[i] for i in range(len(ys))]
    i = 0
    blocks = [[v, ww, [xs[k]]] for k, (v, ww) in enumerate(zip(vals, wt))]
    # simple PAV
    yb = list(ys); wb = list(w); xb = list(xs)
    merged = True
    means = list(ys); weights = list(w); xr = [[xx] for xx in xs]
    while merged:
        merged = False
        j = 0
        while j < len(means) - 1:
            if means[j] > means[j + 1] + 1e-12:
                nm = (means[j] * weights[j] + means[j + 1] * weights[j + 1]) / (weights[j] + weights[j + 1])
                means[j] = nm; weights[j] += weights[j + 1]; xr[j] += xr[j + 1]
                del means[j + 1]; del weights[j + 1]; del xr[j + 1]
                merged = True
            else:
                j += 1
    # build step function: x threshold = max x in block -> mean
    thr = np.array([max(b) for b in xr]); mv = np.array(means)
    def predict(xq):
        i = np.searchsorted(thr, xq, side="left")
        i = np.clip(i, 0, len(mv) - 1)
        return mv[i]
    return predict

# ---- reliability curve error (weighted mean |realized-pred| over p buckets) ----
def reliability_error(pred, y, edges=(0.5, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 1.01)):
    err = 0.0; n = len(pred)
    if n == 0:
        return np.nan
    for lo, hi in zip(edges[:-1], edges[1:]):
        m = (pred >= lo) & (pred < hi)
        if m.sum() >= 3:
            err += m.sum() / n * abs(y[m].mean() - pred[m].mean())
    return err

def brier(pred, y):
    return np.mean((pred - y) ** 2) if len(pred) else np.nan

def logloss(pred, y):
    p = np.clip(pred, EPS, 1 - EPS)
    return -np.mean(y * np.log(p) + (1 - y) * np.log(1 - p)) if len(pred) else np.nan

# ============================================================
# Walk-forward engine
# ============================================================
def walk_forward(df, window, warmup, clamp_b=None):
    """df sorted by time; columns: pcal, ask, won(0/1). Returns per-model OOS preds and gated EV.
    Each row t: fit on rows [max(0,t-window), t) (strictly past), predict row t.
    Warmup: until >=warmup past samples, use raw pcal for all models."""
    df = df.reset_index(drop=True)
    pcal = df["pcal"].values.astype(float)
    ask = df["ask"].values.astype(float)
    y = df["won"].values.astype(float)
    n = len(df)
    lp = logit(pcal)
    out = {m: np.full(n, np.nan) for m in ["M0", "M1", "M2", "M3"]}
    ab_hist = []
    for t in range(n):
        lo = max(0, t - window)
        past_y = y[lo:t]; past_pcal = pcal[lo:t]; past_lp = lp[lo:t]
        out["M0"][t] = pcal[t]
        if len(past_y) < warmup:
            out["M1"][t] = pcal[t]; out["M2"][t] = pcal[t]; out["M3"][t] = pcal[t]
            continue
        # M1 scalar bias = mean(pred - realized) over past
        bias = np.mean(past_pcal - past_y)
        out["M1"][t] = np.clip(pcal[t] - bias, 0.01, 0.99)
        # M2 Platt
        a, b = fit_platt(past_lp, past_y, clamp_b=clamp_b)
        out["M2"][t] = sigmoid(a + b * lp[t])
        ab_hist.append((a, b))
        # M3 isotonic (reference)
        try:
            iso = fit_isotonic(past_pcal, past_y)
            out["M3"][t] = float(iso(pcal[t]))
        except Exception:
            out["M3"][t] = pcal[t]
    valid = ~np.isnan(out["M1"])  # rows where recal was active (post-warmup)
    return out, valid, ask, y, ab_hist

def eval_models(df, window, warmup, clamp_b=None, label=""):
    out, valid, ask, y, ab = walk_forward(df, window, warmup, clamp_b)
    rows = []
    for m in ["M0", "M1", "M2", "M3"]:
        pred = out[m][valid]; yy = y[valid]; aa = ask[valid]
        # gated set: keep trades whose recalibrated edge still >= EDGE_MIN
        edge = pred - aa - fee_of(aa)
        keep = edge >= EDGE_MIN
        gset_y = yy[keep]; gset_ask = aa[keep]
        gn = keep.sum()
        ev = net1(gset_y, gset_ask).mean() if gn else np.nan
        tot = net1(gset_y, gset_ask).sum() if gn else 0.0
        rows.append(dict(model=m, brier=brier(pred, yy), logloss=logloss(pred, yy),
                         relerr=reliability_error(pred, yy), gated_n=int(gn),
                         gated_win=(gset_y.mean() if gn else np.nan), gated_ev=ev, gated_tot=tot))
    if ab:
        bs = np.array([x[1] for x in ab]); a_s = np.array([x[0] for x in ab])
        platt_stats = dict(b_med=np.median(bs), b_min=bs.min(), b_max=bs.max(),
                           a_med=np.median(a_s))
    else:
        platt_stats = {}
    return pd.DataFrame(rows), platt_stats, int(valid.sum())

# per-p-bucket realized vs predicted, to see if Platt fixes tails w/o harming middle
def bucket_table(df, window, warmup, clamp_b=None):
    out, valid, ask, y, ab = walk_forward(df, window, warmup, clamp_b)
    yy = y[valid]
    res = {}
    buckets = [(0.5, 0.65), (0.65, 0.70), (0.70, 0.80), (0.80, 1.01)]
    for m in ["M0", "M1", "M2"]:
        pred = out[m][valid]
        rows = []
        for lo, hi in buckets:
            mm = (pred >= lo) & (pred < hi)
            if mm.sum() >= 3:
                rows.append((f"{lo:.2f}-{hi:.2f}", int(mm.sum()), pred[mm].mean(), yy[mm].mean()))
            else:
                rows.append((f"{lo:.2f}-{hi:.2f}", int(mm.sum()), np.nan, np.nan))
        res[m] = rows
    return res

# ============================================================
# Load data
# ============================================================
lv = pd.read_parquet(os.path.join(BASE, "live_trades_enriched.parquet"))
lv = lv.sort_values("entry_ts").reset_index(drop=True)
lv["pcal"] = lv["p"].astype(float)  # stored PRODUCTION pcal
lv["won"] = lv["won"].astype(int)

# recorder: rebuild pcal from strat_signals May curve (documented method)
sig = pd.read_parquet(os.path.join(DATA, "strat_signals.parquet"))
may = sig[(sig.month == "may") & (sig.is_first == 1)]
zb = np.array([-1, 0, .3, .6, 1, 1.5, 2, 3, 5, 100]); mids = []; ws = []
for lo, hi in zip(zb[:-1], zb[1:]):
    s = may[(may.z >= lo) & (may.z < hi)]
    if len(s) >= 20:
        mids.append(s.z.mean()); ws.append(s.win.mean())
mids = np.array(mids); ws = np.array(ws)
def pcal_z(z):
    return 0.5 if z <= 0 else float(np.interp(z, mids, ws))
rc = pd.read_parquet(os.path.join(BASE, "recorder_trades_enriched.parquet"))
rc = rc.sort_values("ts").reset_index(drop=True)
rc["pcal"] = rc["z"].apply(pcal_z)
rc["won"] = rc["win"].astype(int)
# recorder gated the same way the bot would: recompute edge, keep edge>=EDGE_MIN
rc["edge_rebuilt"] = rc["pcal"] - rc["ask"] - fee_of(rc["ask"].values)

print("=" * 78)
print("SAMPLE SIZES")
print(f"  LIVE  5m : {(lv.interval=='5m').sum()}   15m : {(lv.interval=='15m').sum()}   (Jun30-Jul02, 2 days)")
print(f"  RECORDER : {len(rc)}  (ttl {rc.ttl.min()}-{rc.ttl.max()}s -> short-TTL, NOT 15m; pcal rebuilt)")
print(f"  live edge min={lv.edge.min():.3f} -> live set is ALREADY gated at edge>=0.06")

# ============================================================
# Q1: recalibration walk-forward
# ============================================================
def run_block(df, name, windows=(200, 300), warmup=200):
    print("\n" + "=" * 78)
    print(f"WALK-FORWARD RECALIBRATION — {name}  (n={len(df)})")
    for w in windows:
        eff_warm = min(warmup, w)
        print(f"\n  window={w}, warmup={eff_warm}")
        tbl, ps, nvalid = eval_models(df[["pcal", "ask", "won"]], w, eff_warm)
        if nvalid == 0:
            print("    (too few post-warmup rows to evaluate)")
            continue
        print(f"    OOS rows evaluated: {nvalid}")
        print(f"    {'model':<5}{'brier':>8}{'logloss':>9}{'relerr':>8}{'gN':>6}{'gWin':>7}{'gEV/$1':>9}{'gTot$':>9}")
        for _, r in tbl.iterrows():
            gw = f"{r.gated_win:.1%}" if r.gated_n else "  -"
            ev = f"{r.gated_ev:+.4f}" if r.gated_n else "   -"
            print(f"    {r.model:<5}{r.brier:>8.4f}{r.logloss:>9.4f}{r.relerr:>8.4f}{r.gated_n:>6}{gw:>7}{ev:>9}{r.gated_tot:>+9.2f}")
        if ps:
            print(f"    Platt b: median={ps['b_med']:.2f} range[{ps['b_min']:.2f},{ps['b_max']:.2f}]  a median={ps['a_med']:.2f}")
    # bucket table at window=300 (or largest feasible)
    w = max(windows)
    res = bucket_table(df[["pcal", "ask", "won"]], w, min(warmup, w))
    print(f"\n  PER-BUCKET realized vs predicted (window={w}) — does Platt fix tails w/o harming middle?")
    print(f"    {'bucket':>10} | {'M0 pred/real':>16} | {'M1 pred/real':>16} | {'M2 pred/real':>16}")
    for i in range(4):
        parts = []
        lab = None
        for m in ["M0", "M1", "M2"]:
            b = res[m][i]; lab = b[0]
            if not np.isnan(b[2]):
                parts.append(f"{b[2]:.2f}/{b[3]:.2f} n{b[1]}")
            else:
                parts.append(f"  -  n{b[1]}")
        print(f"    {lab:>10} | {parts[0]:>16} | {parts[1]:>16} | {parts[2]:>16}")

run_block(lv[lv.interval == "5m"].reset_index(drop=True), "LIVE 5m")
run_block(rc.reset_index(drop=True), "RECORDER (short-TTL, pcal rebuilt)")

# 15m: report sample only
n15 = (lv.interval == "15m").sum()
print("\n" + "=" * 78)
print(f"LIVE 15m: n={n15} — TOO SMALL for a walk-forward (need >= warmup ~200). "
      f"Recorder is NOT 15m (ttl<=180s), so NO usable 15m calibration sample exists live.")

# Platt with clamped b, live 5m, for shipping-safety
print("\n" + "=" * 78)
print("SHIP-SAFETY: Platt with clamp b in [0.5, 2.0], LIVE 5m, window=300")
tbl, ps, nv = eval_models(lv[lv.interval == "5m"][["pcal", "ask", "won"]], 300, 200, clamp_b=(0.5, 2.0))
print(f"    {'model':<5}{'brier':>8}{'logloss':>9}{'relerr':>8}{'gN':>6}{'gEV/$1':>9}{'gTot$':>9}")
for _, r in tbl.iterrows():
    ev = f"{r.gated_ev:+.4f}" if r.gated_n else "   -"
    print(f"    {r.model:<5}{r.brier:>8.4f}{r.logloss:>9.4f}{r.relerr:>8.4f}{r.gated_n:>6}{ev:>9}{r.gated_tot:>+9.2f}")
if ps:
    print(f"    Platt b(clamped): median={ps['b_med']:.2f} range[{ps['b_min']:.2f},{ps['b_max']:.2f}]")

# ============================================================
# Q2: edge anti-predictivity (go/no-go for edge-proportional sizing)
# ============================================================
def edge_terciles(df, edge_col, win_col, ask_col, label, pnl_col=None):
    d = df.copy()
    q = d[edge_col].quantile([1/3, 2/3]).values
    d["terc"] = np.where(d[edge_col] <= q[0], "LOW", np.where(d[edge_col] <= q[1], "MID", "HIGH"))
    print(f"\n  {label} (n={len(d)}):")
    print(f"    {'terc':<5}{'n':>5}{'edge_range':>18}{'win%':>7}{'modelEV/$1':>11}", end="")
    if pnl_col:
        print(f"{'realEV/$1':>11}{'realTot$':>10}", end="")
    print()
    for t in ["LOW", "MID", "HIGH"]:
        g = d[d.terc == t]
        n1 = net1(g[win_col].values.astype(bool), g[ask_col].values)
        er = f"[{g[edge_col].min():.3f},{g[edge_col].max():.3f}]"
        line = f"    {t:<5}{len(g):>5}{er:>18}{g[win_col].mean():>6.1%}{n1.mean():>+11.4f}"
        if pnl_col:
            line += f"{(g[pnl_col].sum()/g['stake_usd'].sum()):>+11.4f}{g[pnl_col].sum():>+10.2f}"
        print(line)

print("\n" + "=" * 78)
print("Q2: EDGE ANTI-PREDICTIVITY — go/no-go for edge-proportional sizing")
lv5 = lv[lv.interval == "5m"].copy()
edge_terciles(lv5, "edge", "won", "ask", "LIVE 5m (actual net_pnl booked)", pnl_col="net_pnl")
edge_terciles(rc, "edge_rebuilt", "won", "ask", "RECORDER (short-TTL, model EV only)")

print("\nDONE.")
