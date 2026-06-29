"""Feature-mining on live recorder (Jun18-29) to find NEW predictors of win/loss
beyond z/edge. Tests: PM book-staleness (ask already moving?), move shape
(accel, run-length, disp/vel ratio), spread, time-of-day. Reports win/EV by
bucket + point-biserial corr with outcome. edge>=0.04 (new base).
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
        # NEW FEATURES
        accel = vel[i]-vel[i-3]                              # 2s-vel change
        runlen=0
        for j in range(i,0,-1):
            if sgn*(spot[j]-spot[j-1])>0: runlen+=1
            else: break
        dvr = disp[i]/max(abs(vel[i]),0.5)                  # sustained vs spiky
        spread = ba[i]-bb[i]
        pm_premove = (mid[i]-mid[i-3]) if np.isfinite(mid[i-3]) else 0.0  # PM book already moving?
        hour = dt.datetime.utcfromtimestamp(int(absec[i])).hour
        R.append({"win":win,"edge":edge,"vol":vv,"ttl":t,"accel":accel,"runlen":runlen,
                  "dvr":dvr,"spread":spread,"pm_premove":pm_premove,"hour":hour,
                  "ask":A,"hold":(STAKE/A)*win-STAKE-(STAKE/A)*FEE*A*(1-A)})
        break
E=pd.DataFrame(R); print("\nentries:",len(E),"baseline win=%.3f ev=$%.3f"%(E.win.mean(),E.hold.mean()))
E.to_parquet(DATA+r"\live_features.parquet")

def qtab(col,q=5,lab=None):
    try: E["b"]=pd.qcut(E[col],q,duplicates="drop")
    except: return
    g=E.groupby("b",observed=True).agg(n=("win","size"),win=("win","mean"),ev=("hold","mean"),mu=(col,"mean"))
    c=np.corrcoef(E[col],E.win)[0,1]
    print(f"\n=== {lab or col}  (corr with win={c:+.3f}) ===")
    for b,r in g.iterrows(): print(f"  {col}~{r.mu:+8.3f}  n={int(r.n):>4} win={r.win:.3f} ev=${r.ev:+.3f}")
for col,lab in [("pm_premove","PM book pre-move (ask already climbing=late)"),
                ("accel","acceleration (2s-vel change)"),
                ("runlen","run length (consecutive favorable secs)"),
                ("dvr","disp/vel ratio (sustained vs spiky)"),
                ("spread","PM spread"),("vol","vol (control)")]:
    qtab(col,5,lab)
print("\n=== by HOUR (UTC) ===")
g=E.groupby("hour").agg(n=("win","size"),win=("win","mean"),ev=("hold","mean"))
for h,r in g.iterrows():
    if r.n>=15: print(f"  {h:02d}h n={int(r.n):>4} win={r.win:.3f} ev=${r.ev:+.3f}")
