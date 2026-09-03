"""Gradient-boosted search for anything that beats the Polymarket price.

WHAT THIS IS NOT. It is not an AUC contest. A model can score 0.95 AUC here by
simply rediscovering the ask -- the price already contains almost all of the
predictable variance, which is exactly why the PM-only residual came back empty.

THE ONLY QUESTION THAT MATTERS: at the moment we would trade, does the model's
probability beat the posted ask by more than the fee? Everything is scored as
realised P&L against on-chain outcomes, never as classification accuracy.

DISCIPLINE:
  * TIME-ORDERED split. Train strictly earlier than test -- no shuffling, which
    would leak the regime across the W33 structural break and manufacture an
    edge that never existed live.
  * The ask is given to the model as a feature. If it cannot improve on its own
    input, there is nothing here.
  * Reported per WEEK, because we already know the edge was +18 to +31pp through
    W32 and ~0 after. A model that only "works" pooled is fitting the dead era.

Usage: python scripts/model_search.py [decide_s]
"""
import datetime
import pickle
import sys
from collections import defaultdict

import numpy as np

DECIDE_S = int(sys.argv[1]) if len(sys.argv) > 1 else 60
FEE = lambda a: 0.07 * a * (1 - a)


def load():
    pm = pickle.load(open(f"scripts/_feat_{DECIDE_S}.pkl", "rb"))
    try:
        bn = pickle.load(open(f"scripts/_bnfeat_{DECIDE_S}.pkl", "rb"))
    except FileNotFoundError:
        bn = {}
    lab = pickle.load(open("scripts/_true_labels.pkl", "rb"))
    rows = []
    for r in pm:
        b = bn.get(r["tok"])
        if not b:
            continue
        d = dict(r)
        # the Binance view is signed for the UP token; flip it for DOWN so the
        # feature always means "toward the side this token pays on"
        side = lab[r["tok"]]["side"]
        sgn = 1.0 if side == "up" else -1.0
        for k, v in b.items():
            d[k] = (v * sgn) if k.startswith(("disp", "mom", "ofi", "flow", "z")) else v
        d["ts"] = r["epoch"]
        rows.append(d)
    rows.sort(key=lambda x: x["ts"])
    return rows


FEATS = ["ask", "bid", "spread", "mid", "n_quotes", "quote_age",
         "mom15", "mom30", "mom60", "mom120", "pm_vol", "pm_range", "drift",
         "disp_bps", "vol_bps", "z", "ttl_s", "rng_bps", "pos_in_rng",
         "push_intensity"]
for h in (10, 30, 60, 120):
    FEATS += [f"mom{h}", f"ofi{h}", f"flow{h}"]
FEATS = list(dict.fromkeys(FEATS))


def week(ts):
    return datetime.datetime.fromtimestamp(ts, datetime.UTC).strftime("%Y-W%V")


def main():
    rows = load()
    print(f"rows with BOTH PM and Binance features: {len(rows):,}")
    if len(rows) < 2000:
        print("not enough joined data"); return
    ws = sorted({week(r["ts"]) for r in rows})
    print(f"weeks: {ws[0]} .. {ws[-1]}")
    X = np.array([[float(r.get(f, 0.0) or 0.0) for f in FEATS] for r in rows])
    y = np.array([1.0 if r["won"] else 0.0 for r in rows])
    ask = np.array([r["ask"] for r in rows])
    wk = np.array([week(r["ts"]) for r in rows])

    import xgboost as xgb
    # WALK-FORWARD: for each test week, train only on everything strictly before.
    print()
    print("WALK-FORWARD. Train on all prior weeks, test on the next. Trade a token")
    print("when model_p exceeds ask + fee + margin. Scored on on-chain outcomes.")
    print()
    print("{:>10} {:>7} {:>7} {:>8} {:>8} {:>10} {:>9}".format(
        "test week", "n", "trades", "WR", "mean ask", "net $", "ROI"))
    MARGIN = 0.03
    tot = cost = 0.0
    ntr = 0
    for i in range(3, len(ws)):
        te = ws[i]
        tr_mask = np.isin(wk, ws[:i])
        te_mask = wk == te
        if tr_mask.sum() < 2000 or te_mask.sum() < 100:
            continue
        m = xgb.XGBClassifier(
            n_estimators=300, max_depth=4, learning_rate=0.05,
            subsample=0.8, colsample_bytree=0.8, reg_lambda=2.0,
            eval_metric="logloss", verbosity=0)
        m.fit(X[tr_mask], y[tr_mask])
        p = m.predict_proba(X[te_mask])[:, 1]
        a = ask[te_mask]
        yy = y[te_mask]
        take = p > (a + np.array([FEE(x) for x in a]) + MARGIN)
        n = int(take.sum())
        if n == 0:
            print("{:>10} {:>7} {:>7} {:>8} {:>8} {:>10} {:>9}".format(
                te, int(te_mask.sum()), 0, "-", "-", "-", "-"))
            continue
        aa = a[take]; yt = yy[take]
        pnl = float(np.sum(np.where(yt > 0, 1.0 - aa - np.array([FEE(x) for x in aa]),
                                    -(aa + np.array([FEE(x) for x in aa])))))
        c = float(np.sum(aa + np.array([FEE(x) for x in aa])))
        tot += pnl; cost += c; ntr += n
        print("{:>10} {:>7} {:>7} {:>8.1%} {:>8.3f} {:>+10.2f} {:>+9.1%}".format(
            te, int(te_mask.sum()), n, float(yt.mean()), float(aa.mean()), pnl, pnl / c))
    print()
    if cost:
        print(f"POOLED OUT-OF-SAMPLE: {ntr:,} trades, net ${tot:+,.2f} on "
              f"${cost:,.0f} = {tot/cost:+.2%}")
    # what did the model lean on?
    m = xgb.XGBClassifier(n_estimators=300, max_depth=4, learning_rate=0.05,
                          subsample=0.8, colsample_bytree=0.8, reg_lambda=2.0,
                          eval_metric="logloss", verbosity=0)
    m.fit(X, y)
    imp = sorted(zip(FEATS, m.feature_importances_), key=lambda t: -t[1])[:12]
    print("\ntop features (full-sample fit, for interpretation only):")
    for f, v in imp:
        print(f"   {f:<16} {v:.4f}")


if __name__ == "__main__":
    main()
