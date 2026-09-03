"""Position-sizing study — pre-registered in docs/PREREG_sizing.md.

Usage: python scripts/sizing_study.py <scratch_dir>

Re-weights the SAME realised live-paper trade sequence under competing sizing rules, so
every comparison is paired. Two normalisations are always reported:
  * matched MEAN stake     -> isolates allocation skill from simply betting more
  * matched PEAK exposure  -> isolates it from simply taking more risk
plus a compounding-bankroll mode, which is the only mode in which the Kelly fraction
lambda actually means anything (under mean-matching lambda cancels exactly).

Judged on geometric growth SUBJECT TO drawdown, not on total P&L: this book has killed
three bankrolls.
"""
import os
import sys

import numpy as np
import pandas as pd

FEE = 0.07
PM_MIN = 1.00          # Polymarket minimum order — a real constraint, not a modelling choice
START_BANKROLL = 50.0  # the ladder's re-fund size


def load(scratch):
    L = lambda n: pd.read_parquet(os.path.join(scratch, f"a_{n}.parquet"))
    ARM = pd.Timestamp("2026-07-27 15:11", tz="UTC").timestamp()
    io = L("v2_intent_open")
    io = io[(io.ts_s >= ARM) & (io.variant.isna() | (io.variant == "v0"))].copy()
    pr, pc = L("pnl_recorder_recorded"), L("paper_close")
    pr, pc = pr[pr.ts_s >= ARM], pc[pc.ts_s >= ARM]
    pc["realized_pnl"] = pd.to_numeric(pc.realized_pnl, errors="coerce")
    pr["net_pnl"] = pd.to_numeric(pr.net_pnl, errors="coerce")
    pnl = pd.concat([pr[["token_id", "net_pnl"]],
                     pc[["token_id", "realized_pnl"]].rename(columns={"realized_pnl": "net_pnl"})]
                    ).groupby("token_id").net_pnl.sum()
    d = io.set_index("token_id").join(pnl, how="inner").reset_index().sort_values("ts_s")
    d["dt"] = pd.to_datetime(d.ts_s, unit="s", utc=True)
    d["day"] = d.dt.dt.date
    d["c"] = d.ask + FEE * d.ask * (1 - d.ask)
    d["f_star"] = ((d.p - d.c) / (1 - d.c)).clip(lower=0)
    # vol60 is not logged directly; back it out of the identity z = disp/(vol*sqrt(ttl))
    d["vol60"] = (d.disp_bps / (d.z * np.sqrt(d.ttl_s))).replace([np.inf, -np.inf], np.nan)
    d["vol60"] = d.vol60.fillna(d.vol60.median())
    d["r"] = d.net_pnl / d.stake_usd            # realised return per $1 staked
    d["exit_ts"] = d.exit_ts_s
    return d.reset_index(drop=True)


# ---------------------------------------------------------------- sizing rules (weights)
def weights(d, rule, prior_vol=None):
    if rule == "FLAT":
        return np.ones(len(d))
    if rule == "KELLY":
        return d.f_star.to_numpy()
    if rule == "EDGE":                     # the deployed sizer's own shape
        return d.stake_raw.to_numpy() if "stake_raw" in d else d.f_star.to_numpy()
    if rule == "VOLSCALE":                 # ORB-style: inverse underlying vol
        v = d.vol60.to_numpy() if "vol60" in d else None
        if v is None:
            z = np.where(d.z.to_numpy() != 0, d.disp_bps / d.z, np.nan)
            v = z / np.sqrt(d.ttl_s.to_numpy())
        return 1.0 / np.clip(v, 1e-6, None)
    if rule == "VOLMANAGED":               # Moreira-Muir: inverse STRATEGY return variance
        return 1.0 / np.clip(prior_vol ** 2, 1e-6, None)
    if rule == "VOLTARGET":                # Harvey et al: inverse strategy vol
        return 1.0 / np.clip(prior_vol, 1e-6, None)
    if rule == "VOLCONDEDGE":              # this project's registered follow-up
        # same z wins 7-14pts LESS in low vol60 (volcal_study). Penalise the edge there.
        v = d.vol60.to_numpy()
        adj = np.clip((v - 0.10) / 0.30, -1.0, 1.0)      # -1 dead tape .. +1 lively
        p_adj = np.clip(d.p.to_numpy() + 0.10 * adj, 0.01, 0.99)
        return np.clip((p_adj - d.c.to_numpy()) / (1 - d.c.to_numpy()), 0, None)
    raise ValueError(rule)


def strategy_vol(d, halflife_days=2.0):
    """Trailing realised vol of the STRATEGY's own daily P&L — what Moreira-Muir scale by.
    Causal: only days strictly before the trade's day are used."""
    daily = d.groupby("day").apply(lambda g: (g.r * g.stake_usd).sum(), include_groups=False)
    days = list(daily.index)
    prior = {}
    for i, dy in enumerate(days):
        hist = daily.iloc[:i]
        prior[dy] = hist.ewm(halflife=halflife_days).std().iloc[-1] if len(hist) >= 3 else np.nan
    med = np.nanmedian(list(prior.values()))
    return d.day.map(lambda x: prior.get(x) if np.isfinite(prior.get(x, np.nan)) else med).to_numpy()


# ---------------------------------------------------------------- normalisation + metrics
def stakes_matched_mean(w, target_mean, cap_mult=None):
    w = np.nan_to_num(np.asarray(w, float), nan=0.0)
    if w.sum() <= 0:
        return None
    s = w / w.mean() * target_mean
    if cap_mult is not None:
        s = np.minimum(s, target_mean * cap_mult)
    s = np.maximum(s, PM_MIN)
    s = s / s.mean() * target_mean         # RE-normalise AFTER clipping — the $1 floor
    return np.maximum(s, PM_MIN)           #   otherwise silently raises mean stake


def peak_exposure(d, stakes):
    ev = []
    for t0, t1, s in zip(d.ts_s.to_numpy(), d.exit_ts.to_numpy(), stakes):
        ev.append((t0, s)); ev.append((t1, -s))
    ev.sort()
    cur = pk = 0.0
    for _, x in ev:
        cur += x; pk = max(pk, cur)
    return pk


def metrics(d, stakes):
    pnl = d.r.to_numpy() * stakes
    # TRADE-level equity curve: with 10 all-positive days a daily drawdown is degenerate
    # (it reads 0.00 and hides the intraday hole the operator would actually live through)
    eq = np.cumsum(pnl)
    dd_trade = float(np.maximum.accumulate(eq).max() and (np.maximum.accumulate(eq) - eq).max())
    daily = pd.Series(pnl).groupby(d.day.to_numpy()).sum()
    downside = daily[daily < 0]
    sortino = (daily.mean() / downside.std(ddof=1) * np.sqrt(len(daily))
               if len(downside) > 1 else np.nan)
    # trade-level downside deviation — defined even when every DAY is green
    r_t = pd.Series(pnl)
    dn_t = r_t[r_t < 0]
    sortino_t = (r_t.mean() / dn_t.std(ddof=1) * np.sqrt(len(r_t))
                 if len(dn_t) > 1 else np.nan)
    return dict(total=pnl.sum(), per_day=daily.mean(), maxdd=dd_trade, sortino=sortino_t,
                sortino_day=sortino, worst=daily.min(), pos_days=(daily > 0).sum(),
                n_days=len(daily), mean_stake=stakes.mean(),
                peak_exp=peak_exposure(d, stakes))


def compound(d, w, lam, cap_frac=0.05, bankroll=START_BANKROLL):
    """The only mode in which lambda means anything: bankroll evolves, stake = lam*f*B."""
    b = bankroll
    path = []
    for wi, ri in zip(np.nan_to_num(w), d.r.to_numpy()):
        s = min(lam * wi * b, cap_frac * b)
        s = max(s, PM_MIN) if b > PM_MIN else 0.0
        s = min(s, b)
        b += s * ri
        path.append(b)
        if b <= PM_MIN:
            break
    p = np.array(path)
    peak = np.maximum.accumulate(p)
    return dict(final=p[-1], growth=(p[-1] / bankroll) ** (1 / max(len(p), 1)) - 1,
                maxdd_pct=float(((peak - p) / peak).max()), ruined=bool(p[-1] <= PM_MIN),
                n=len(p))


def main(scratch):
    d = load(scratch)
    d["prior_vol"] = strategy_vol(d)
    base = d.stake_usd.mean()
    print(f"trades={len(d)}  days={d.day.nunique()}  FLAT mean stake ${base:.3f}  "
          f"realised total ${(d.r*d.stake_usd).sum():+.2f}\n")

    RULES = ["FLAT", "KELLY", "EDGE", "VOLSCALE", "VOLMANAGED", "VOLTARGET", "VOLCONDEDGE"]
    print("=== MATCHED MEAN STAKE (isolates allocation shape; lambda cancels here) ===")
    print(f"{'rule':<13}{'total$':>9}{'$/day':>8}{'maxDD':>8}{'Sortino':>9}{'worst':>8}"
          f"{'pos/d':>7}{'meanStk':>9}{'peakExp':>9}")
    res = {}
    for r in RULES:
        w = weights(d, r, d.prior_vol.to_numpy())
        s = stakes_matched_mean(w, base, cap_mult=3.0)
        if s is None:
            continue
        m = metrics(d, s); res[r] = (s, m)
        print(f"{r:<13}{m['total']:>+9.2f}{m['per_day']:>+8.2f}{m['maxdd']:>8.2f}"
              f"{m['sortino']:>9.2f}{m['worst']:>+8.2f}{m['pos_days']}/{m['n_days']:<4}"
              f"{m['mean_stake']:>9.3f}{m['peak_exp']:>9.2f}")

    print("\n=== PAIRED day-clustered CI on (rule - FLAT) $/day, matched mean stake ===")
    rng = np.random.default_rng(17)
    fl = d.r.to_numpy() * res["FLAT"][0]
    days = d.day.to_numpy(); ud = np.unique(days)
    for r in RULES[1:]:
        if r not in res:
            continue
        pn = d.r.to_numpy() * res[r][0]
        bs = []
        for _ in range(4000):
            pick = rng.choice(ud, len(ud), replace=True)
            idx = np.concatenate([np.where(days == x)[0] for x in pick])
            bs.append((pn[idx].sum() - fl[idx].sum()) / len(ud))
        lo, hi = np.percentile(bs, [2.5, 97.5])
        pt = (pn.sum() - fl.sum()) / len(ud)
        print(f"  {r:<13} {pt:+7.2f}/day   CI[{lo:+.2f},{hi:+.2f}]{'  *' if lo > 0 else ''}")

    print(f"\n=== COMPOUNDING (${START_BANKROLL:.0f} bankroll, 5% per-trade cap, $1 PM floor) ===")
    print(f"{'rule':<13}{'lambda':>8}{'final$':>10}{'maxDD%':>9}{'ruined':>8}")
    for r in ("KELLY", "VOLCONDEDGE", "EDGE"):
        w = weights(d, r, d.prior_vol.to_numpy())
        for lam in (0.05, 0.125, 0.25, 0.5):
            c = compound(d, w, lam)
            print(f"{r:<13}{lam:>8.3f}{c['final']:>10.2f}{c['maxdd_pct']*100:>8.1f}%"
                  f"{str(c['ruined']):>8}")
    fl_c = compound(d, np.ones(len(d)) / base * 1.0, 1.0)
    print(f"{'FLAT($1.05)':<13}{'—':>8}{START_BANKROLL + (d.r*base).sum():>10.2f}"
          f"{res['FLAT'][1]['maxdd']/START_BANKROLL*100:>8.1f}%{'False':>8}")
    return d, res


if __name__ == "__main__":
    main(sys.argv[1])
