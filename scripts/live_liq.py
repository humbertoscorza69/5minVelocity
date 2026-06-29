"""Integrated parse: recorder (Jun18-29) -> full feature matrix WITH timestamps
(z,edge,dvr,vol,pm_premove,ttl,accel,hour,asset) + win/hold/stop, joined with
Coinalyze liquidation flow. Tests: does liq-flow IN the signal direction predict
win (continuation vs reversal tail)? Saves data/live_full.parquet (model base).
"""
import io, math, os, datetime as dt
import numpy as np, pandas as pd, orjson, zstandard as zstd
pd.set_option("display.width",240)
DATA=r"C:\Users\tico_\Fable\5minSnip\data"; PM=r"D:\polycrypto\live_l2\polymarket"; BN=r"D:\polycrypto\live_l2\binance"
FEE,STAKE=0.07,10.0; DAYS=[f"2026-06-{d:02d}" for d in range(18,30)]
def lines(p):
    if p.endswith(".zst"):
        with open(p,"rb") as f:
            for ln in io.TextIOWrapper(io.BufferedReader(zstd.ZstdDecompressor().stream_reader(f),1<<24),encoding="utf-8"): yield ln
    else:
        with open(p,encoding="utf-8",buffering=1<<24) as f:
            for ln in f: yield ln
def find(b,d):
    for e in (".jsonl.zst",".jsonl"):
        q=os.path.join(b,d+e)
        if os.path.exists(q): return q
sig=pd.read_parquet(DATA+r"\strat_signals.parquet"); may=sig[(sig.month=="may")&(sig.is_first==1)]
zb=np.array([-1,0,.3,.6,1,1.5,2,3,5,100]); mids=[]; ws=[]
for lo,hi in zip(zb[:-1],zb[1:]):
    s=may[(may.z>=lo)&(may.z<hi)]
    if len(s)>=20: mids.append(s.z.mean()); ws.append(s.win.mean())
mids=np.array(mids); ws=np.array(ws); pcal=lambda z:np.interp(z,mids,ws)
# liquidation tables: per-asset minute -> (long$, short$)
LQ={}
liq=pd.read_parquet(DATA+r"\coinalyze_liq.parquet")
for a in ("btc","eth"):
    d=liq[liq.asset==a]; LQ[a]=(d.t.values.astype(np.int64), d.l.values.astype(float), d.s.values.astype(float))
def liqflow(a,sec,sgn,win_min=3):
    ts,l,s=LQ[a]; m=(sec//60)*60; i1=np.searchsorted(ts,m,side="right"); i0=np.searchsorted(ts,m-win_min*60,side="left")
    ll=l[i0:i1].sum(); ss=s[i0:i1].sum()
    return sgn*(ss-ll), ss+ll   # aligned (dir-signed), total magnitude
tokmap={}
for day in DAYS:
    p=find(os.path.join(PM,"markets"),day)
    if not p: continue
    for ln in lines(p):
        try: m=orjson.loads(ln).get("market") or {}
        except: continue
        if m.get("interval")!="5m": continue
        a=str(m.get("asset","")).lower(); ep=m.get("epoch")
        if a not in("btc","eth") or ep is None: continue
        s=(ep+300)*1000
        if m.get("up_token_id"): tokmap[m["up_token_id"]]=(a,"up",s,ep)
        if m.get("down_token_id"): tokmap[m["down_token_id"]]=(a,"down",s,ep)
B={}
for a,sym in (("btc","btcusdt"),("eth","ethusdt")):
    d={}
    for day in DAYS:
        p=find(os.path.join(BN,f"{sym}_kline_1s"),day)
        if not p: continue
        for ln in lines(p):
            try: k=orjson.loads(ln)["payload"]["data"]["k"]; d[int(k["t"])//1000]=float(k["c"])
            except: continue
    secs=np.array(sorted(d)); B[a]=(secs,np.array([d[s] for s in secs]))
def cl(a,sec):
    secs,c=B[a]; i=np.searchsorted(secs,sec,side="right")-1
    return c[i] if (i>=0 and sec-secs[i]<=5) else np.nan
def vol(a,sec,lb=60):
    secs,c=B[a]; i=np.searchsorted(secs,sec,side="right")-1
    return np.std(np.diff(np.log(c[i-lb:i])))*1e4 if i>=lb+1 else np.nan
book={}
for day in DAYS:
    p=find(os.path.join(PM,"best_bid_ask"),day)
    if not p: continue
    for ln in lines(p):
        try: pp=orjson.loads(ln)["payload"]
        except: continue
        info=tokmap.get(pp.get("asset_id"))
        if info is None: continue
        s=info[2]; ts=int(pp["timestamp"])
        if ts<s-205000 or ts>s: continue
        try: book.setdefault(pp["asset_id"],{})[ts//1000]=(float(pp["best_bid"]),float(pp["best_ask"]))
        except: continue
    print("parsed",day)
R=[]
for tid,sm_ in book.items():
    a,side,settle_ms,ep=tokmap[tid]; ss=settle_ms//1000
    grid=np.arange(200,-1,-1); absec=ss-grid; ser=pd.Series(sm_)
    bb=ser.reindex(absec).map(lambda x:x[0] if isinstance(x,tuple) else np.nan).ffill().values
    ba=ser.reindex(absec).map(lambda x:x[1] if isinstance(x,tuple) else np.nan).ffill().values
    op=cl(a,ep-1); fin=cl(a,ss-1)
    if not(np.isfinite(op) and np.isfinite(fin)): continue
    win=int((side=="up")==(fin>=op)); spot=np.array([cl(a,s) for s in absec]); sgn=1.0 if side=="up" else -1.0
    n=len(grid); vel=np.full(n,np.nan); vel[2:]=sgn*(spot[2:]/spot[:-2]-1)*1e4; disp=sgn*(spot/op-1)*1e4
    mid=(bb+ba)/2
    for i in range(n):
        t=grid[i]
        if not(5<=t<=180) or i<6 or not np.isfinite(vel[i]) or vel[i]<2 or disp[i]<=0 or not np.isfinite(spot[i]): continue
        vv=vol(a,int(absec[i]))
        if not vv or vv<=0: continue
        A=ba[i+1] if i+1<n else ba[i]
        if not np.isfinite(A) or not(0.30<=A<=0.97): continue
        z=disp[i]/(vv*math.sqrt(t)); edge=pcal(z)-A-FEE*A*(1-A)
        if edge<0.04: break
        dvr=disp[i]/max(abs(vel[i]),0.5); pmpm=(mid[i]-mid[i-3]) if np.isfinite(mid[i-3]) else 0.0
        la,lt=liqflow(a,int(absec[i]),sgn)
        R.append({"asset":a,"ts":int(absec[i]),"ttl":t,"vel":vel[i],"disp":disp[i],"vol":vv,"z":z,
                  "edge":edge,"dvr":dvr,"pm_premove":pmpm,"accel":vel[i]-vel[i-3],
                  "liq_aligned":la,"liq_total":lt,"hour":dt.datetime.utcfromtimestamp(int(absec[i])).hour,
                  "ask":A,"win":win,"hold":(STAKE/A)*win-STAKE-(STAKE/A)*FEE*A*(1-A)})
        break
E=pd.DataFrame(R); E.to_parquet(DATA+r"\live_full.parquet")
print("\nentries=%d  base win=%.3f ev=$%.3f"%(len(E),E.win.mean(),E.hold.mean()))
print("liq coverage: %.0f%% of entries have liq>0 in last 3min"%(100*(E.liq_total>0).mean()))

def qtab(col,q,lab):
    try: E["b"]=pd.qcut(E[col],q,duplicates="drop")
    except: print("  (skip",col,")"); return
    g=E.groupby("b",observed=True).agg(n=("win","size"),win=("win","mean"),ev=("hold","mean"),mu=(col,"mean"))
    print(f"\n=== {lab}  (corr win={np.corrcoef(E[col],E.win)[0,1]:+.3f}) ===")
    for b,r in g.iterrows(): print(f"  {col}~{r.mu:>+12.1f} n={int(r.n):>4} win={r.win:.3f} ev=${r.ev:+.3f}")
qtab("liq_aligned",5,"LIQ ALIGNED (dir-signed $, last 3min) -- continuation signal")
qtab("liq_total",4,"LIQ TOTAL magnitude ($, last 3min)")
# does aligned liq help WITHIN spiky (low-dvr) trades? (rescue the reversal tail)
sp=E[E.dvr<0.5]
if len(sp)>40:
    print("\n=== within SPIKY (dvr<0.5): split by aligned-liq sign ===")
    for lab,m in [("liq with move (aligned>0)",sp.liq_aligned>0),("liq against/none (<=0)",sp.liq_aligned<=0)]:
        d=sp[m]; print(f"  {lab:<26} n={len(d):>4} win={d.win.mean():.3f} ev=${d.hold.mean():+.3f}")
