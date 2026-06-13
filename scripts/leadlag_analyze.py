"""Lead-lag core analysis (June 5m).

For each token, build per-second book-mid and underlying (Binance) series over
the final window, then:

A) Cross-correlation of d_mid[t] with side-signed underlying return at lag L
   (L>0 = underlying leads, book lags).
B) Conditional on a 2s underlying signal at time t (side-signed bps):
   - book reprice that FOLLOWS: mid[t+h]-mid[t] for h=1,2,5  (scalp gap, cents)
   - eventual outcome (this token wins?)  (hold edge)
   - entry ask & spread at t (costs)
   Bucketed by signal magnitude; restricted to ttl in [5,120], valid quotes.
C) Scalp vs hold P&L per trade after costs.
"""
import glob
import os

import numpy as np
import pandas as pd

pd.set_option("display.width", 240)
DATA = r"C:\Users\tico_\Fable\5minSnip\data"
FEE = 0.07

# ---- binance dense ----
def load_sym(sym):
    files = sorted(glob.glob(os.path.join(DATA, "binance", f"{sym}_*.parquet")))
    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    df = df.drop_duplicates("open_time").sort_values("open_time")
    return df.open_time.values.astype(np.int64), df.close.values.astype(float)
B = {"btc": load_sym("BTCUSDT"), "eth": load_sym("ETHUSDT")}

def px_series(asset, sec_abs):
    ot, cl = B[asset]
    idx = np.searchsorted(ot, sec_abs * 1000, side="right") - 1
    out = np.where(idx >= 0, cl[np.clip(idx, 0, len(cl) - 1)], np.nan)
    # invalidate if kline too old (>5s gap)
    gap = sec_abs * 1000 - ot[np.clip(idx, 0, len(cl) - 1)]
    out = np.where((idx >= 0) & (gap <= 5000), out, np.nan)
    return out

bs = pd.read_parquet(DATA + r"\ll_booksec.parquet")
bs = bs[(bs.ba < 1.0) & (bs.bb > 0) & (bs.ba >= bs.bb)]
bs["mid"] = (bs.bb + bs.ba) / 2

# cross-correlation accumulators
LAGS = list(range(-4, 6))
cc_num = {L: 0.0 for L in LAGS}
cc_dn = 0.0
cc_dd = {L: 0.0 for L in LAGS}
cc_n = {L: 0 for L in LAGS}

# conditional records
rec_sig = []      # 2s signed signal bps
rec_g1, rec_g2, rec_g5 = [], [], []   # mid[t+h]-mid[t]
rec_win, rec_ask, rec_spread, rec_ttl, rec_mid = [], [], [], [], []

for tok, g in bs.groupby("tok", sort=False):
    g = g.sort_values("ttl", ascending=False)   # ttl high->low = time forward
    asset = g.asset.iloc[0]
    side = g.side.iloc[0]
    winner = int(g.winner.iloc[0])
    settle_ms = int(g.settle_ms.iloc[0])
    ttl = g.ttl.values
    # build contiguous second grid from max ttl..0
    tmax = int(ttl.max())
    grid_ttl = np.arange(tmax, -1, -1)
    mid = pd.Series(g.mid.values, index=ttl).reindex(grid_ttl).ffill().values
    ba = pd.Series(g.ba.values, index=ttl).reindex(grid_ttl).ffill().values
    bb = pd.Series(g.bb.values, index=ttl).reindex(grid_ttl).ffill().values
    sec_abs = (settle_ms // 1000) - grid_ttl
    upx = px_series(asset, sec_abs)
    sgn = 1.0 if side == "up" else -1.0
    uret = np.full_like(mid, np.nan)
    uret[1:] = sgn * (upx[1:] / upx[:-1] - 1.0) * 1e4   # side-signed bps, 1s
    dmid = np.full_like(mid, np.nan)
    dmid[1:] = (mid[1:] - mid[:-1])
    # cross-correlation (use overlapping valid)
    for L in LAGS:
        if L >= 0:
            a = dmid[L:]
            b = uret[:len(uret) - L] if L > 0 else uret
        else:
            a = dmid[:L]
            b = uret[-L:]
        m = np.isfinite(a) & np.isfinite(b)
        if m.sum() > 5:
            cc_num[L] += np.sum(a[m] * b[m])
            cc_dd[L] += np.sum(b[m] ** 2)
            cc_n[L] += m.sum()
    # conditional signal: 2s signed return ending at t
    n = len(mid)
    sig2 = np.full(n, np.nan)
    sig2[2:] = sgn * (upx[2:] / upx[:-2] - 1.0) * 1e4
    for i in range(n):
        t = grid_ttl[i]
        if not (5 <= t <= 120):
            continue
        if not np.isfinite(sig2[i]) or not np.isfinite(mid[i]):
            continue
        # future mids
        def fut(h):
            j = i + h
            return mid[j] if j < n and np.isfinite(mid[j]) else np.nan
        rec_sig.append(sig2[i])
        rec_g1.append(fut(1) - mid[i])
        rec_g2.append(fut(2) - mid[i])
        rec_g5.append(fut(5) - mid[i])
        rec_win.append(winner)
        rec_ask.append(ba[i])
        rec_spread.append(ba[i] - bb[i])
        rec_ttl.append(t)
        rec_mid.append(mid[i])

# ---- A) cross-correlation ----
print("=" * 90)
print("A) Cross-correlation: corr(book Δmid[t], side-signed underlying ret[t-L])")
print("   L>0 => underlying LEADS, book LAGS by L seconds")
# normalize by std of dmid pooled
dd_all = np.array(rec_g1)  # proxy not used; recompute properly below
for L in LAGS:
    if cc_n[L] > 100 and cc_dd[L] > 0:
        # cov / (sqrt(var_u)*sqrt(var_d)) approximated; report cov-normalized slope
        slope = cc_num[L] / cc_dd[L]   # regression d_mid ~ uret
        print(f"   L={L:+d}s: slope(Δmid per bp)={slope*1e4:+.5f} cents/bp  n={cc_n[L]}")

df = pd.DataFrame({"sig": rec_sig, "g1": rec_g1, "g2": rec_g2, "g5": rec_g5,
                   "win": rec_win, "ask": rec_ask, "spread": rec_spread,
                   "ttl": rec_ttl, "mid": rec_mid})
df.to_parquet(DATA + r"\leadlag_records.parquet")
print("\nconditional records:", len(df))

# ---- B) conditional gap & outcome by signal bucket ----
print("=" * 90)
print("B) Conditional on 2s signed underlying signal (bps):")
bins = [-1e9, -10, -5, -2, -1, -0.3, 0.3, 1, 2, 5, 10, 1e9]
df["sb"] = pd.cut(df.sig, bins)
agg = df.groupby("sb", observed=True).agg(
    n=("sig", "size"), sig_mean=("sig", "mean"),
    gap1_c=("g1", lambda x: np.nanmean(x) * 100),
    gap2_c=("g2", lambda x: np.nanmean(x) * 100),
    gap5_c=("g5", lambda x: np.nanmean(x) * 100),
    win_rate=("win", "mean"), ask=("ask", "mean"), spread=("spread", "mean"),
    mid=("mid", "mean"))
print(agg.to_string())

print("\nInterpretation columns: gapN_c = book mid move (cents) over next N s after signal")

# ---- C) scalp vs hold P&L ----
print("=" * 90)
print("C) Trade economics per signal bucket (strong signals only):")
print("   SCALP: buy ask[t], sell mid[t+2] proxy -> gap2 - 2*halfspread - 2*fee")
print("   HOLD : buy ask[t], hold to settle -> win - ask - fee")
strong = df[df.sig.abs() >= 2].copy()
for lab, sub in [("sig>=+2 (buy this side)", df[df.sig >= 2]),
                 ("sig>=+5", df[df.sig >= 5]),
                 ("sig>=+10", df[df.sig >= 10])]:
    if len(sub) < 20:
        continue
    ask = sub.ask.values
    fee = FEE * ask * (1 - ask)
    half = sub.spread.values / 2
    gap2 = np.nan_to_num(sub.g2.values)
    scalp = gap2 - 2 * half - 2 * fee
    hold = sub.win.values - ask - fee
    print(f"  {lab}: n={len(sub)} avg_ask={ask.mean():.3f} "
          f"gap2={gap2.mean()*100:+.3f}c half_spread={half.mean()*100:.3f}c "
          f"fee={fee.mean()*100:.3f}c | SCALP_exp={scalp.mean()*100:+.3f}c "
          f"HOLD_exp={hold.mean()*100:+.3f}c (win={sub.win.mean():.3f})")
