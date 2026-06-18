"""How material is the both-sides hedging issue? For edge>=0.04 (June), how often
does a market fire BOTH sides (opposite), and what do those hedged markets do?"""
import numpy as np, pandas as pd
from scipy.stats import norm
DATA = r"C:\Users\tico_\Fable\5minSnip\data"
FEE = 0.07
sig = pd.read_parquet(DATA + r"\strat_signals.parquet")
# recompute edge (pcal fit on may first-signals), same as strat_compare
may = sig[(sig.month=="may")&(sig.is_first==1)]
zb=np.array([-1,0,.3,.6,1,1.5,2,3,5,100]); mids=[]; ws=[]
for lo,hi in zip(zb[:-1],zb[1:]):
    s=may[(may.z>=lo)&(may.z<hi)]
    if len(s)>=20: mids.append(s.z.mean()); ws.append(s.win.mean())
sig["pcal"]=np.interp(sig.z.values,np.array(mids),np.array(ws))
sig["edge"]=sig.pcal-sig.ask-FEE*sig.ask*(1-sig.ask)

d = sig[(sig.month=="june")&(sig.edge>=0.04)].copy()
# within a market, winning-side trades have win=1, losing-side win=0.
# a market that fired BOTH sides => has both win values present.
g = d.groupby("mkt")
mixed = g.win.nunique()  # 2 => both sides taken
nmk = g.ngroups
n_mixed = (mixed==2).sum()
print(f"June edge>=0.04 BOTH-sides: {len(d)} trades over {nmk} markets")
print(f"  markets that fired BOTH (opposite) sides: {n_mixed} ({n_mixed/nmk:.0%})")
# pnl from single-side vs mixed (hedged) markets
mk_mixed = mixed[mixed==2].index
single = d[~d.mkt.isin(mk_mixed)]
hedged = d[d.mkt.isin(mk_mixed)]
print(f"  single-side markets: trades={len(single)} pnl=${single.hold.sum():+.0f} "
      f"ev=${single.hold.mean():+.3f}")
print(f"  hedged (both-side) markets: trades={len(hedged)} pnl=${hedged.hold.sum():+.0f} "
      f"ev=${hedged.hold.mean():+.3f}")
# also: how many trades/market on average
print(f"  avg trades per market: {len(d)/nmk:.2f}; "
      f"same-side multi (DCA) share approx = {(len(d)-nmk-n_mixed)/len(d):.0%}")
