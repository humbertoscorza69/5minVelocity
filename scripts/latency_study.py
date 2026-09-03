"""Entry-latency study: how much edge do we lose waiting for the 1s bar close?
Uses Binance aggTRADES (tick, Jun26-28) to find the TRUE sub-second move time t0,
and the recorder's sub-second Polymarket best_bid_ask to measure the favored
side's ask at t0 vs at our actual 1s-bar entry second T. The ask climb in
[t0 -> T] is the edge eroded by the 1s-bar latency. Also prints the average
sub-second ask reprice curve around entry.
"""
import io, os, math
import numpy as np, pandas as pd, orjson, zstandard as zstd
pd.set_option("display.width",240)
DATA=r"C:\Users\tico_\Fable\5minSnip\data"; PM=r"D:\polycrypto\live_l2\polymarket"; BN=r"D:\polycrypto\live_l2\binance"
AGG=r"D:\polycrypto\aggtrades"; FEE=0.07
DAYS=[f"2026-06-{d:02d}" for d in range(18,29)]   # all days with downloaded ticks
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
# frozen May cal
sig=pd.read_parquet(DATA+r"\strat_signals.parquet"); may=sig[(sig.month=="may")&(sig.is_first==1)]
zb=np.array([-1,0,.3,.6,1,1.5,2,3,5,100]); mids=[];ws=[]
for lo,hi in zip(zb[:-1],zb[1:]):
    s=may[(may.z>=lo)&(may.z<hi)]
    if len(s)>=20: mids.append(s.z.mean()); ws.append(s.win.mean())
mids=np.array(mids); ws=np.array(ws); pcal=lambda z:np.interp(z,mids,ws)
# TICKS
TICK={}
for a,sym in (("btc","BTCUSDT"),("eth","ETHUSDT")):
    fr=[]
    for d in [f"2026-06-{x:02d}" for x in range(18,29)]:
        zp=os.path.join(AGG,f"{sym}-aggTrades-{d}.zip")
        if os.path.exists(zp):
            try: fr.append(pd.read_csv(zp,header=None,usecols=[1,5],names=["price","ms"]))
            except Exception as e: print("tick read fail",zp,e)
    if fr:
        t=pd.concat(fr).sort_values("ms")
        mss=t.ms.values.astype(np.int64)
        if mss.size and mss.max()>10**14:   # Binance aggTrades now in MICROSECONDS -> ms
            mss=mss//1000
        TICK[a]=(mss, t.price.values.astype(float))
        print(f"ticks {a}: {len(t):,} rows  {mss.min()}..{mss.max()} (ms)")
def tprice(a,ms):
    arr,px=TICK.get(a,(None,None))
    if arr is None: return np.nan
    i=np.searchsorted(arr,ms,side="right")-1
    return px[i] if i>=0 else np.nan
# markets
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
# 1s klines (for entry reconstruction)
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
# best_bid_ask: keep RAW events per token (ms-sorted) for sub-second lookup
raw={}
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
        try: raw.setdefault(pp["asset_id"],[]).append((ts,float(pp["best_bid"]),float(pp["best_ask"])))
        except: continue
    print("parsed",day)
RAW={}
for tok,ev in raw.items():
    ev.sort(); a=np.array(ev); RAW[tok]=(a[:,0].astype(np.int64),a[:,1],a[:,2])  # ms,bid,ask
def ask_at(tok,ms):
    d=RAW.get(tok)
    if d is None: return np.nan
    msa,bid,ask=d; i=np.searchsorted(msa,ms,side="right")-1
    return ask[i] if i>=0 else np.nan
# reconstruct entries (edge>=0.04) at 1s-bar-close second T, like the bot
ent=[]
for tid,d in RAW.items():
    a,side,settle_ms,ep=tokmap[tid]; ss=settle_ms//1000
    grid=np.arange(200,-1,-1); absec=ss-grid
    op=cl(a,ep-1)
    if not np.isfinite(op): continue
    spot=np.array([cl(a,s) for s in absec]); sgn=1.0 if side=="up" else -1.0
    n=len(grid); vel=np.full(n,np.nan); vel[2:]=sgn*(spot[2:]/spot[:-2]-1)*1e4; disp=sgn*(spot/op-1)*1e4
    # per-second ask via ffill of raw at absec
    ser=pd.Series({m//1000:ask for m,_,ask in zip(*d)}) if False else None
    for i in range(n):
        t=grid[i]
        if not(5<=t<=180) or not np.isfinite(vel[i]) or vel[i]<2 or disp[i]<=0 or not np.isfinite(spot[i]): continue
        vv=vol(a,int(absec[i]))
        if not vv or vv<=0: continue
        T=int(absec[i]); askT=ask_at(tid, T*1000+999)  # ask available by end of second T
        if not np.isfinite(askT) or not(0.30<=askT<=0.97): continue
        z=disp[i]/(vv*math.sqrt(t)); edge=pcal(z)-askT-FEE*askT*(1-askT)
        if edge<0.04: break
        ent.append({"a":a,"side":side,"tok":tid,"T":T,"askT":askT,"op":op,"z":z,"edge":edge})
        break
E=pd.DataFrame(ent); print("\nentries on tick-days:",len(E))
if not len(E): raise SystemExit("no entries (tick days may lack recorder overlap)")

# find true sub-second move time t0 from TICKS, in [T-2.5s, T], first |2s tick-return|>=5bps matching side
def find_t0(a,side,T):
    arr,px=TICK.get(a,(None,None))
    if arr is None: return None
    lo,hi=(T*1000-2500), (T*1000+200)
    i0=np.searchsorted(arr,lo,side="left"); i1=np.searchsorted(arr,hi,side="right")
    sgn=1.0 if side=="up" else -1.0
    for j in range(i0,i1):
        ms=arr[j]; p_now=px[j]
        k=np.searchsorted(arr,ms-2000,side="right")-1
        if k<0: continue
        ret=sgn*(p_now/px[k]-1)*1e4
        if ret>=5.0: return ms
    return None

lat=[]; er=[]; eg=[]
for r in E.itertuples():
    t0=find_t0(r.a,r.side,r.T)
    if t0 is None: continue
    ask_t0=ask_at(r.tok,t0)
    if not np.isfinite(ask_t0): continue
    lat.append(r.T*1000 - t0)               # ms the bar-close entry was late vs the true move
    er.append(r.askT - ask_t0)              # ask cents eroded by the latency
    eg.append((r.edge, r.edge + (r.askT-ask_t0)))  # (edge now, edge if entered at t0)
lat=np.array(lat); er=np.array(er)
print(f"\nmatched moves with tick t0: {len(lat)} / {len(E)}")
if len(lat):
    print(f"  latency (true move -> 1s-bar entry):  mean={lat.mean():.0f}ms  median={np.median(lat):.0f}ms  p90={np.percentile(lat,90):.0f}ms")
    print(f"  ask EROSION in that gap:              mean={er.mean()*100:+.2f}c  median={np.median(er)*100:+.2f}c  (positive = ask rose = edge lost)")
    print(f"  edge: at 1s-bar entry mean={np.mean([a for a,_ in eg]):.4f}  vs if entered at t0 mean={np.mean([b for _,b in eg]):.4f}")
# sub-second ask reprice curve around entry (recorder only, robust)
print("\n=== avg favored-side ASK around entry second T (sub-second, recorder) ===")
offs=[-2000,-1500,-1000,-500,0,500,1000,1500,2000]
for o in offs:
    vals=[ask_at(r.tok, r.T*1000+o) for r in E.itertuples()]
    vals=[v for v in vals if np.isfinite(v)]
    if vals: print(f"  T{o:+5d}ms  ask={np.mean(vals):.4f}  n={len(vals)}")
