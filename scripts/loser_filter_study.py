"""Loser-filter research: can a MULTIVARIATE model identify losers at entry that
the z-signal alone misses? Honest OUT-OF-SAMPLE test (train early days, test late).

If a full-feature model (logistic + gradient boosting) beats z-alone on the TEST
AUC *and* filtering by it improves TEST EV, we have a real loser-filter. If not,
selection is at ceiling and the stop-loss (cut mid-trade) is the only lever.
"""
import numpy as np, pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import roc_auc_score

E = pd.read_parquet(r"C:\Users\tico_\Fable\5minSnip\data\live_full.parquet")
E = E[E.z >= 0.45].sort_values("ts").reset_index(drop=True)   # entered population, time-ordered
FEATS = ["ttl", "vel", "disp", "vol", "z", "edge", "dvr", "pm_premove", "accel", "liq_aligned", "liq_total", "hour", "ask"]
for f in FEATS: E[f] = E[f].fillna(E[f].median())
y = E.win.values.astype(int)

# temporal split: first 60% train, last 40% test (no lookahead)
cut = int(len(E) * 0.60)
tr, te = slice(0, cut), slice(cut, len(E))
print(f"train n={cut} (win {y[tr].mean():.1%})   test n={len(E)-cut} (win {y[te].mean():.1%})")

def ev_per_dollar(mask):
    a = E.ask.values[te][mask]; w = y[te][mask]
    if len(a) == 0: return 0.0, 0, 0.0
    fee = 0.07 * a * (1 - a)
    net = np.where(w == 1, 1.0 / a - 1 - fee, -1.0)
    return net.mean(), len(a), w.mean()

# ---- baselines ----
auc_z = roc_auc_score(y[te], E.z.values[te])
print(f"\nTEST AUC  z-alone:            {auc_z:.3f}")

# logistic on ALL features
sc = StandardScaler().fit(E[FEATS].values[tr])
Xtr, Xte = sc.transform(E[FEATS].values[tr]), sc.transform(E[FEATS].values[te])
lr = LogisticRegression(max_iter=1000, C=0.5).fit(Xtr, y[tr])
p_lr = lr.predict_proba(Xte)[:, 1]
print(f"TEST AUC  logistic (all feat): {roc_auc_score(y[te], p_lr):.3f}")

# gradient boosting (shallow, regularized -> resist overfit)
gb = GradientBoostingClassifier(n_estimators=120, max_depth=2, learning_rate=0.03,
                                subsample=0.8, min_samples_leaf=40).fit(E[FEATS].values[tr], y[tr])
p_gb = gb.predict_proba(E[FEATS].values[te])[:, 1]
print(f"TEST AUC  gradient boosting:   {roc_auc_score(y[te], p_gb):.3f}")

# also train-AUC of GB to see overfit gap
print(f"(GB train AUC {roc_auc_score(y[tr], gb.predict_proba(E[FEATS].values[tr])[:,1]):.3f} -- big gap vs test = overfit)")

print("\nGB feature importances:")
for f, imp in sorted(zip(FEATS, gb.feature_importances_), key=lambda x: -x[1]):
    print(f"  {f:<12} {imp:.3f}")

# ---- does FILTERING by the model improve EV on test? drop the lowest-prob quantile ----
base_ev, base_n, base_w = ev_per_dollar(np.ones(len(y[te]), bool))
print(f"\nKEEP-ALL (test): n={base_n} win={base_w:.1%} EV/$1={base_ev:+.3f}")
print("Filter = drop bottom X% by model P(win); does EV of KEPT rise above keep-all?")
for name, p in [("logistic", p_lr), ("gbm", p_gb)]:
    print(f"  --- {name} ---")
    for drop in [0.2, 0.35, 0.5]:
        thr = np.quantile(p, drop)
        keep = p >= thr
        ev, n, w = ev_per_dollar(keep)
        print(f"    drop {drop:.0%}: keep n={n:3d} win={w:.1%} EV/$1={ev:+.3f}  (keep-all {base_ev:+.3f})")
