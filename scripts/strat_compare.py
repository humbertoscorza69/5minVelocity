"""Head-to-head backtest of strategy variants on identical markets.
Builds ALL lag signals (loose) with features + hold-to-settle P&L + stop-out P&L
(exit when underlying crosses back through the window open). Then scores variants:
 entry filter (basic-lag / vel+z / edge-gate) x one-side-per-market vs both-sides
 x exit (hold vs stop-out). IS=May, OOS=June. $10 stake.
Calibration p_cal(z) fit on MAY only (no leakage).
"""
import glob, os
import numpy as np
import pandas as pd
from scipy.stats import norm
pd.set_option("display.width", 250)
DATA = r"C:\Users\tico_\Fable\5minSnip\data"
FEE, STAKE = 0.07, 10.0
CACHE = DATA + r"\strat_signals.parquet"

def load_sym(s):
    fs = sorted(glob.glob(os.path.join(DATA, "binance", f"{s}_*.parquet")))
    df = pd.concat([pd.read_parquet(f) for f in fs], ignore_index=True)
    return (df.drop_duplicates("open_time").sort_values("open_time").open_time.values.astype(np.int64),
            df.close.values.astype(float))
B = {"btc": load_sym("BTCUSDT"), "eth": load_sym("ETHUSDT")}

def px_series(a, secs):
    ot, cl = B[a]; i = np.searchsorted(ot, secs*1000, side="right")-1
    v = np.where(i >= 0, cl[np.clip(i,0,len(cl)-1)], np.nan)
    g = secs*1000 - ot[np.clip(i,0,len(cl)-1)]
    return np.where((i>=0)&(g<=5000), v, np.nan)

def lvol(a, sec, lb=60):
    ot, cl = B[a]; i = np.searchsorted(ot, sec*1000, side="right")-1
    if i < lb+1: return np.nan
    return np.std(np.diff(np.log(cl[i-lb:i])))*1e4

def build(path, month):
    bs = pd.read_parquet(path); bs = bs[(bs.ba<1)&(bs.bb>0)&(bs.ba>=bs.bb)]
    rows=[]
    for tok,g in bs.groupby("tok",sort=False):
        g=g.sort_values("ttl",ascending=False)
        a=g.asset.iloc[0]; side=g.side.iloc[0]; win=int(g.winner.iloc[0])
        sm=int(g.settle_ms.iloc[0]); ss=sm//1000; mkt=f"{a}_{sm}"
        ttl=g.ttl.values; grid=np.arange(int(ttl.max()),-1,-1); n=len(grid)
        ba=pd.Series(g.ba.values,index=ttl).reindex(grid).ffill().values
        bb=pd.Series(g.bb.values,index=ttl).reindex(grid).ffill().values
        sec=ss-grid; spot=px_series(a,sec)
        opx=px_series(a,np.array([ss-300]))[0]
        if not np.isfinite(opx): continue
        sgn=1.0 if side=="up" else -1.0
        vel=np.full(n,np.nan); vel[2:]=sgn*(spot[2:]/spot[:-2]-1)*1e4
        disp=sgn*(spot/opx-1)*1e4
        # next index (forward = larger i) where spot crosses back through open
        nxt=np.full(n,-1)
        cur=-1
        for i in range(n-1,-1,-1):
            nxt[i]=cur
            adverse = (spot[i] < opx) if side=="up" else (spot[i] > opx)
            if adverse: cur=i
        first_done=False
        for i in range(n):
            t=grid[i]
            if not(5<=t<=180) or not np.isfinite(vel[i]) or not np.isfinite(disp[i]): continue
            if vel[i]<2 or disp[i]<=0: continue
            a1=ba[i+1] if i+1<n else ba[i]
            if not np.isfinite(a1) or not(0.30<=a1<=0.99): continue
            vol=lvol(a,int(sec[i]))
            if not vol or vol<=0: continue
            z=disp[i]/(vol*np.sqrt(t))
            sh=STAKE/a1; efee=sh*FEE*a1*(1-a1)
            hold=sh*win-STAKE-efee
            ej=nxt[i]
            if ej>=0 and np.isfinite(bb[ej]):
                be=bb[ej]; xfee=sh*FEE*be*(1-be)
                stop=sh*be-STAKE-efee-xfee
            else:
                stop=hold
            rows.append((mkt,a,t,vel[i],disp[i],vol,z,a1,win,int(not first_done),
                         hold,stop,sm,month))
            first_done=True
    return rows

if os.path.exists(CACHE):
    sig=pd.read_parquet(CACHE); print("cached signals:",len(sig))
else:
    print("building all signals (forward stop-out walk)...")
    rows=build(DATA+r"\ll_booksec_may.parquet","may")+build(DATA+r"\ll_booksec.parquet","june")
    sig=pd.DataFrame(rows,columns=["mkt","asset","ttl","vel","disp","vol","z","ask",
                                   "win","is_first","hold","stop","settle_ts","month"])
    sig.to_parquet(CACHE); print("signals:",len(sig))

# calibration p_cal(z) fit on MAY first-signals
may=sig[(sig.month=="may")&(sig.is_first==1)]
zb=np.array([-1,0,.3,.6,1,1.5,2,3,5,100])
mids=[]; ws=[]
for lo,hi in zip(zb[:-1],zb[1:]):
    s=may[(may.z>=lo)&(may.z<hi)]
    if len(s)>=20: mids.append(s.z.mean()); ws.append(s.win.mean())
mids=np.array(mids); ws=np.array(ws)
def p_cal(z): return np.interp(z, mids, ws)
sig["pcal"]=p_cal(sig.z.values)
sig["edge"]=sig.pcal - sig.ask - FEE*sig.ask*(1-sig.ask)

def metr(d, col="hold"):
    if len(d)<10: return None
    p=d[col].values
    days=pd.to_datetime(d.settle_ts,unit="ms").dt.strftime("%m-%d")
    daily=pd.Series(p).groupby(days.values).sum()
    return dict(n=len(d),win=d.win.mean(),ev=p.mean(),tot=p.sum(),
                posdays=(daily>0).mean(),ndays=len(daily))

def show(name,d,col="hold"):
    m=metr(d,col)
    if m: print(f"  {name:<34} n={m['n']:>4} win={m['win']:.3f} "
                f"ev=${m['ev']:+.3f} tot=${m['tot']:+.0f} posdays={m['posdays']:.0%}")

VARIANTS={
 "BASIC-lag (vel>=4)": lambda d:d[(d.vel>=4)&(d.ask<=0.97)],
 "VEL+Z (vel4,z>=.5,ask<=.9)": lambda d:d[(d.vel>=4)&(d.z>=0.5)&(d.ask<=0.90)],
 "EDGE>=0.04": lambda d:d[d.edge>=0.04],
 "EDGE>=0.08": lambda d:d[d.edge>=0.08],
}
for month in ["may","june"]:
    print(f"\n===== {month.upper()} — ONE trade/market (is_first), HOLD-to-settle =====")
    base=sig[(sig.month==month)&(sig.is_first==1)]
    for name,f in VARIANTS.items(): show(name,f(base),"hold")

print("\n===== ONE-SIDE vs BOTH-SIDES (EDGE>=0.04, hold) =====")
for month in ["may","june"]:
    one=sig[(sig.month==month)&(sig.is_first==1)&(sig.edge>=0.04)]
    both=sig[(sig.month==month)&(sig.edge>=0.04)]   # every qualifying signal
    print(f"  {month}: ONE  ",metr(one,"hold"))
    print(f"  {month}: BOTH ",metr(both,"hold"))

print("\n===== HOLD vs STOP-OUT exit (EDGE>=0.04, one/market) =====")
for month in ["may","june"]:
    d=sig[(sig.month==month)&(sig.is_first==1)&(sig.edge>=0.04)]
    print(f"  {month}: HOLD ",metr(d,"hold"))
    print(f"  {month}: STOP ",metr(d,"stop"))
