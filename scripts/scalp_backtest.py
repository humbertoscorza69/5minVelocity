"""Maker-exit lag-scalp backtest (June, uses real trades feed for maker fills).
Entry: taker buy at ask A (signal+1s), one side/market, edge-gated.
Exit:  post maker SELL at S=A+TP (no fee). Fills if a real BUY trade prints >= S
       before settle. If unfilled by settle -> hold to settlement.
Compare fixed & dynamic TP vs HOLD baseline. Also measure realized reprice.
$10 stake. Maker exit = no exit fee; taker entry pays fee.
"""
import glob, json, os
import numpy as np
import pandas as pd
from scipy.stats import norm
pd.set_option("display.width", 240)
DATA = r"C:\Users\tico_\Fable\5minSnip\data"
FEE, STAKE = 0.07, 10.0

def load_sym(s):
    fs = sorted(glob.glob(os.path.join(DATA, "binance", f"{s}_*.parquet")))
    df = pd.concat([pd.read_parquet(f) for f in fs], ignore_index=True)
    return (df.drop_duplicates("open_time").sort_values("open_time").open_time.values.astype(np.int64),
            df.close.values.astype(float))
B = {"btc": load_sym("BTCUSDT"), "eth": load_sym("ETHUSDT")}
def px(a, sec):
    ot, cl = B[a]; i = np.searchsorted(ot, sec*1000, side="right")-1
    v = np.where(i>=0, cl[np.clip(i,0,len(cl)-1)], np.nan)
    g = sec*1000-ot[np.clip(i,0,len(cl)-1)]; return np.where((i>=0)&(g<=5000),v,np.nan)
def lvol(a, sec, lb=60):
    ot, cl = B[a]; i = np.searchsorted(ot, sec*1000, side="right")-1
    if i<lb+1: return np.nan
    return np.std(np.diff(np.log(cl[i-lb:i])))*1e4

# calibration from May (strat_signals)
sig = pd.read_parquet(DATA+r"\strat_signals.parquet")
may = sig[(sig.month=="may")&(sig.is_first==1)]
zb=np.array([-1,0,.3,.6,1,1.5,2,3,5,100]); mids=[]; ws=[]
for lo,hi in zip(zb[:-1],zb[1:]):
    s=may[(may.z>=lo)&(may.z<hi)]
    if len(s)>=20: mids.append(s.z.mean()); ws.append(s.win.mean())
mids=np.array(mids); ws=np.array(ws)
def pcal(z): return np.interp(z, mids, ws)

tokidx = json.load(open(DATA+r"\filtered\token_index.json"))
idx2tid = {v:k for k,v in tokidx.items()}

# Build June one-side first-per-market edge-gated entries with token_id
bs = pd.read_parquet(DATA+r"\ll_booksec.parquet"); bs=bs[(bs.ba<1)&(bs.bb>0)&(bs.ba>=bs.bb)]
ent=[]
for tok,g in bs.groupby("tok",sort=False):
    g=g.sort_values("ttl",ascending=False)
    a=g.asset.iloc[0]; side=g.side.iloc[0]; win=int(g.winner.iloc[0])
    sm=int(g.settle_ms.iloc[0]); ss=sm//1000; mkt=f"{a}_{sm}"
    ttl=g.ttl.values; grid=np.arange(int(ttl.max()),-1,-1); n=len(grid)
    ba=pd.Series(g.ba.values,index=ttl).reindex(grid).ffill().values
    sec=ss-grid; spot=px(a,sec); opx=px(a,np.array([ss-300]))[0]
    if not np.isfinite(opx): continue
    sgn=1.0 if side=="up" else -1.0
    vel=np.full(n,np.nan); vel[2:]=sgn*(spot[2:]/spot[:-2]-1)*1e4
    disp=sgn*(spot/opx-1)*1e4
    for i in range(n):
        t=grid[i]
        if not(5<=t<=180) or not np.isfinite(vel[i]) or vel[i]<2 or disp[i]<=0: continue
        A=ba[i+1] if i+1<n else ba[i]
        if not np.isfinite(A) or not(0.30<=A<=0.99): continue
        vol=lvol(a,int(sec[i]))
        if not vol or vol<=0: continue
        z=disp[i]/(vol*np.sqrt(t)); edge=pcal(z)-A-FEE*A*(1-A)
        if edge>=0.04:
            ent.append({"mkt":mkt,"tid":idx2tid[tok],"asset":a,"A":A,"win":win,
                        "entry_sec":int(sec[i]),"settle_sec":ss,"z":z,"pcal":float(pcal(z)),
                        "is_first":int(not any(e["mkt"]==mkt for e in ent[-6:]))})
Eall=pd.DataFrame(ent)
# one-side = first edge-signal per market
Efirst=Eall.sort_values("entry_sec").drop_duplicates("mkt",keep="first")
print("June edge>=0.04: all signals(both-sides)=",len(Eall)," one-side=",len(Efirst))
E=Efirst

# trades grouped by token
tr=pd.read_parquet(DATA+r"\trades_june5m.parquet").rename(columns={"asset":"tid"})
trbuy=tr[tr.side=="BUY"].sort_values("ts")
grp={k:(v.ts.values,v.price.values) for k,v in trbuy.groupby("tid")}

def maker_exit(row, S):
    """First BUY trade >= S after entry, before settle. Return fill price S or None."""
    g=grp.get(row.tid)
    if g is None: return None
    ts,pr=g
    m=(ts>row.entry_sec)&(ts<=row.settle_sec)&(pr>=S-1e-9)
    return S if m.any() else None

def run(tp_fn, label, df=None):
    df = E if df is None else df
    fills=0; pnl=[]
    for row in df.itertuples():
        A=row.A; sh=STAKE/A; efee=sh*FEE*A*(1-A)
        S=tp_fn(row)
        if S is not None and S<1.0 and maker_exit(row,S):
            fills+=1; pnl.append(sh*(S-A)-efee)            # maker exit: no exit fee
        else:
            pnl.append(sh*row.win - STAKE - efee)          # unfilled -> hold to settle
    pnl=np.array(pnl)
    print(f"  {label:<30} n={len(df):>4} fill={fills/len(df):.0%} ev=${pnl.mean():+.3f} "
          f"tot=${pnl.sum():+.0f} std=${pnl.std():.2f}")

# realized reprice: max BUY-trade price within 10s after entry vs A
rep=[]
for row in E.itertuples():
    g=grp.get(row.tid)
    if g is None: rep.append(np.nan); continue
    ts,pr=g; m=(ts>row.entry_sec)&(ts<=row.entry_sec+10)
    rep.append(pr[m].max()-row.A if m.any() else np.nan)
E["rep10"]=rep
print("\nrealized reprice in 10s after entry (book BUY prints - A):")
print(f"  median={np.nanmedian(E.rep10):.3f} p25={np.nanpercentile(E.rep10,25):.3f} "
      f"p75={np.nanpercentile(E.rep10,75):.3f} frac>0.05={np.nanmean(E.rep10>0.05):.0%}")

print("\nHOLD baseline (no scalp):")
hold=np.array([ (STAKE/r.A)*r.win - STAKE - (STAKE/r.A)*FEE*r.A*(1-r.A) for r in E.itertuples()])
print(f"  HOLD                       ev=${hold.mean():+.3f} tot=${hold.sum():+.0f} std=${hold.std():.2f}")

print("\nMAKER-EXIT SCALP — ONE side (first/market):")
for f in [0.5,0.7]:
    run(lambda r,f=f: r.A+f*(r.pcal-r.A), f"dynamic TP={f}x edge", E)

print("\nMAKER-EXIT SCALP — BOTH sides (every edge signal):")
nd = 7
for f in [0.5,0.7]:
    run(lambda r,f=f: r.A+f*(r.pcal-r.A), f"dynamic TP={f}x edge", Eall)
print(f"  (both-sides ~{len(Eall)/nd:.0f} trades/day vs one-side ~{len(E)/nd:.0f}/day)")
