"""L2 book shape as a RESIDUAL predictor — pre-registered in docs/PREREG_book_shape.md.

Usage: python scripts/book_shape_study.py <dt_dir> <bk_dir> <out_dir>

The books are calibrated (WR - ask within +/-0.014 at every price level), so a feature
that predicts DIRECTION is already in the price. The only admissible target is the
residual: does book shape predict the outcome CONDITIONAL ON THE ASK? Baseline is
p_hat = ask; the challenger must beat it out of sample.
"""
import glob
import os
import sys

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import log_loss, roc_auc_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

sys.path.insert(0, os.path.dirname(__file__))
import tourney_engine as T
from tourney_build import load_markets

DEPLOY = dict(disp_floor=2.0, vol_floor=0.12, z_min=0.45, edge_min=0.02, min_ask=0.30,
              max_ask=1.0, min_ttl=30, max_ttl=240, frozen=2, vol_lb=60,
              intervals=["5m"], mid_move_max=0.0, max_bbo_age=2, burst_min=0)
BK = ["bk_micro", "bk_mid", "bk_imb", "bk_imb3", "bk_imb10", "bk_spread_ticks",
      "bk_bidsz", "bk_asksz", "bk_depth3", "bk_depth10", "bk_levels"]
CTRL = ["z60", "disp", "vol60", "ttl", "burst1", "burst3", "tick_age"]
MAX_BK_AGE = 60          # a snapshot older than this is not information
STAKE, FEE = 1.05, 0.07


def build_signals(dt_dir, bk_dir, cache):
    if os.path.exists(cache):
        return pd.read_parquet(cache)
    d, _ = T.load(dt_dir)
    mpd = d[d.interval == "5m"].groupby("day").mkey.nunique()
    days = sorted(mpd[mpd >= mpd.median() * 0.85].index)
    d = d[d.day.isin(days)].reset_index(drop=True)
    sig = d.iloc[T.select(d, DEPLOY)].reset_index(drop=True)
    print(f"deployed signals: {len(sig)} over {len(days)} days", flush=True)

    out = []
    for day, g in sig.groupby("day"):
        p = os.path.join(bk_dir, f"bk_{day}.parquet")
        if not os.path.exists(p):
            continue
        bk = pd.read_parquet(p)
        tok = load_markets(day)
        mp = pd.DataFrame([(k,) + v for k, v in tok.items()],
                          columns=["token_id", "asset", "interval", "epoch", "side"])
        bk = bk.merge(mp, on="token_id", how="inner")
        if bk.empty:
            continue
        bk["bk_sec"] = bk.sec
        key = ["asset", "interval", "epoch", "side"]
        j = pd.merge_asof(g.sort_values("sec"),
                          bk.sort_values("sec")[["sec", "bk_sec"] + BK + key],
                          on="sec", by=key, direction="backward",
                          tolerance=MAX_BK_AGE)
        out.append(j)
    s = pd.concat(out, ignore_index=True)
    s["bk_age"] = s.sec - s.bk_sec
    s.to_parquet(cache, index=False)
    return s


def money(sub, keep):
    ask = np.clip(sub.ask_next.to_numpy().astype(float), 0.01, 0.99)
    sh = STAKE / ask
    pnl = sub.won.to_numpy().astype(float) * sh - STAKE - FEE * ask * (1 - ask) * sh
    return pnl[keep], pnl


def main(dt_dir, bk_dir, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    s = build_signals(dt_dir, bk_dir, os.path.join(out_dir, "sig_bk.parquet"))
    print(f"signals={len(s)}  with fresh book (<= {MAX_BK_AGE}s): {s.bk_micro.notna().mean():.3f}")
    s = s[s.bk_micro.notna()].copy()
    s["micro_minus_ask"] = s.bk_micro - s.ask
    s["micro_minus_mid"] = s.bk_micro - s.bk_mid
    feats = BK + ["micro_minus_ask", "micro_minus_mid", "bk_age", "ask"] + CTRL
    s = s.dropna(subset=["won", "ask"]).reset_index(drop=True)
    days = sorted(s.day.unique())
    nh = max(1, int(round(len(days) * 0.20)))
    dev, hold = days[:-nh], days[-nh:]
    print(f"usable signals={len(s)}  dev {len(dev)}d  SEALED holdout {len(hold)}d "
          f"({hold[0]}..{hold[-1]})\n")

    y = s.won.to_numpy().astype(int)
    X = s[feats].astype(float).fillna(-1).to_numpy()
    isdev = s.day.isin(dev).to_numpy()

    # ---- rotating-block CV on dev: does ask + book beat ask alone? ----
    blocks = np.array_split(np.array(dev), 5)
    rows = []
    for i, te_days in enumerate(blocks):
        te = s.day.isin(te_days).to_numpy() & isdev
        tr = isdev & ~te
        if te.sum() < 50 or tr.sum() < 200:
            continue
        base_p = s.ask.to_numpy()[te]                      # BASELINE = the market price
        for name, mdl in (("logistic", make_pipeline(StandardScaler(),
                                                     LogisticRegression(max_iter=3000))),
                          ("hgb", HistGradientBoostingClassifier(max_iter=150,
                                                                 random_state=0))):
            mdl.fit(X[tr], y[tr])
            p = mdl.predict_proba(X[te])[:, 1]
            rows.append(dict(fold=i, model=name, n=int(te.sum()),
                             ll_model=log_loss(y[te], np.clip(p, 1e-6, 1 - 1e-6)),
                             ll_ask=log_loss(y[te], np.clip(base_p, 1e-6, 1 - 1e-6)),
                             br_model=float(((p - y[te]) ** 2).mean()),
                             br_ask=float(((base_p - y[te]) ** 2).mean()),
                             auc_model=roc_auc_score(y[te], p),
                             auc_ask=roc_auc_score(y[te], base_p)))
    R = pd.DataFrame(rows)
    print("=== ROTATING-BLOCK CV: challenger vs the ASK as predictor ===")
    agg = R.groupby("model").agg(n=("n", "sum"), ll_model=("ll_model", "mean"),
                                 ll_ask=("ll_ask", "mean"), br_model=("br_model", "mean"),
                                 br_ask=("br_ask", "mean"), auc_model=("auc_model", "mean"),
                                 auc_ask=("auc_ask", "mean"))
    agg["ll_gain"] = agg.ll_ask - agg.ll_model      # positive = model better
    agg["br_gain"] = agg.br_ask - agg.br_model
    print(agg.round(5).to_string())
    print("\n  (ll_gain / br_gain > 0 means book features beat the market price OOS)")

    # ---- money test on the sealed holdout, model fit on ALL dev ----
    print("\n=== SEALED HOLDOUT money test (fit on dev only, one evaluation) ===")
    hm = s.day.isin(hold).to_numpy()
    mdl = HistGradientBoostingClassifier(max_iter=150, random_state=0).fit(X[isdev], y[isdev])
    p = mdl.predict_proba(X[hm])[:, 1]
    sub = s[hm]
    edge = p - sub.ask.to_numpy()
    _, all_pnl = money(sub, np.ones(len(sub), bool))
    nd = len(hold)
    print(f"  take ALL signals : n={len(sub)}  EV/$1={(all_pnl/STAKE).mean():+.4f}  "
          f"${all_pnl.sum()/nd:+.2f}/day")
    for q in (0.0, 0.01, 0.02, 0.03):
        k = edge >= q
        if k.sum() < 30:
            continue
        print(f"  model_edge>={q:.2f}   n={int(k.sum()):5d}  EV/$1={(all_pnl[k]/STAKE).mean():+.4f}  "
              f"${all_pnl[k].sum()/nd:+.2f}/day")
    print(f"\n  holdout AUC: model {roc_auc_score(sub.won.astype(int), p):.4f} "
          f"vs ask {roc_auc_score(sub.won.astype(int), sub.ask):.4f}")
    return s, R


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3])
