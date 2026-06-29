"""Walk-forward fair-value model: TRAIN on May, TEST on June (live recorder).
Predicts P(win) from [z, dvr, vol, ttl, ask]; compares vs the pcal(z) baseline on
OOS AUC + calibration; then measures P&L of model-based sizing/selection vs the
current flat edge-gate. All trained-May / tested-June (no leakage).
"""
import numpy as np, pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import roc_auc_score, brier_score_loss
DATA=r"C:\Users\tico_\Fable\5minSnip\data"; FEE=0.07
# ---- TRAIN set: May first-signals ----
S=pd.read_parquet(DATA+r"\strat_signals.parquet")
may=S[(S.month=="may")&(S.is_first==1)].copy()
may["dvr"]=may.disp/np.maximum(may.vel.abs(),0.5)
# baseline pcal(z) fit on May
zb=np.array([-1,0,.3,.6,1,1.5,2,3,5,100]); mids=[];ws=[]
for lo,hi in zip(zb[:-1],zb[1:]):
    s=may[(may.z>=lo)&(may.z<hi)]
    if len(s)>=20: mids.append(s.z.mean()); ws.append(s.win.mean())
mids=np.array(mids); ws=np.array(ws); pcal=lambda z:np.interp(z,mids,ws)
may["edge"]=pcal(may.z.values)-may.ask-FEE*may.ask*(1-may.ask)
may=may[may.edge>=0.04]                          # match deployment population
# ---- TEST set: June live recorder (already edge>=0.04) ----
te=pd.read_parquet(DATA+r"\live_full.parquet").copy()
FEATS=["z","dvr","vol","ttl","ask"]
Xtr=may[FEATS].values; ytr=may.win.values; Xte=te[FEATS].values; yte=te.win.values
mu=Xtr.mean(0); sd=Xtr.std(0)+1e-9
lr=LogisticRegression(max_iter=2000).fit((Xtr-mu)/sd,ytr)
hgb=HistGradientBoostingClassifier(max_depth=3,max_iter=200,learning_rate=0.05,
     min_samples_leaf=80,l2_regularization=1.0).fit(Xtr,ytr)
p_base=pcal(te.z.values); p_lr=lr.predict_proba((Xte-mu)/sd)[:,1]; p_hgb=hgb.predict_proba(Xte)[:,1]
print("TRAIN May n=%d (win %.3f)  TEST June n=%d (win %.3f)"%(len(may),ytr.mean(),len(te),yte.mean()))
print("\n=== OOS (June) discrimination ===")
for lab,p in [("baseline pcal(z)",p_base),("logistic",p_lr),("HGB",p_hgb)]:
    print(f"  {lab:<16} AUC={roc_auc_score(yte,p):.3f}  Brier={brier_score_loss(yte,p):.4f}")
# ---- calibration of best model on June ----
best=p_hgb if roc_auc_score(yte,p_hgb)>=roc_auc_score(yte,p_lr) else p_lr
bestname="HGB" if best is p_hgb else "logistic"
print(f"\n=== calibration on June ({bestname}) — does predicted match realized? ===")
te["p"]=best; te["pb"]=pd.qcut(te.p,6,duplicates="drop")
for b,g in te.groupby("pb",observed=True):
    print(f"  pred~{g.p.mean():.3f}  realized={g.win.mean():.3f}  n={len(g)}")
print(f"  baseline pcal mean pred={p_base.mean():.3f} vs realized={yte.mean():.3f} (optimism={p_base.mean()-yte.mean():+.3f})")
print(f"  model    mean pred={best.mean():.3f} vs realized={yte.mean():.3f} (optimism={best.mean()-yte.mean():+.3f})")
# ---- P&L on June: flat baseline vs model sizing/selection ----
a=te.ask.values; hold=te.hold.values
def port(w):
    w=np.clip(w,0,None);
    if w.sum()==0: return 0,0
    w=w/w.mean(); pnl=hold*w; return pnl.sum(), pnl.mean()/(pnl.std()+1e-9)
em=best-a-FEE*a*(1-a)                              # model edge
print("\n=== P&L on June (within edge>=0.04 set) ===")
print(f"  FLAT (current)            total=${hold.sum():.0f}  win={yte.mean():.3f}  EV/std={hold.mean()/hold.std():.3f}")
t,s=port(em);                                       print(f"  model-edge sized          total=${t:.0f}  EV/std={s:.3f}")
keep=best>=np.quantile(best,0.15)                   # drop worst 15% by model p
h2=hold[keep]; print(f"  model-select (drop wrst15%) total=${h2.sum():.0f}  win={yte[keep].mean():.3f}  EV/std={h2.mean()/h2.std():.3f}  n={keep.sum()}")
em2=em.copy(); em2[~keep]=0; t,s=port(em2);          print(f"  select + size (combined)  total=${t:.0f}  EV/std={s:.3f}")
import joblib, os
joblib.dump({"lr":lr,"hgb":hgb,"mu":mu,"sd":sd,"feats":FEATS,"pcal_mids":mids,"pcal_ws":ws}, DATA+r"\fairvalue_model.pkl")
print("\nsaved fairvalue_model.pkl  (feats:",FEATS,")")
