"""OOS variant tests (June 13-18, untouched): proves two things with data.
 (1) MAKER-IN adverse selection: post buy at bid; does it fill on winners or losers?
 (2) HOLD + STOP-OUT: ride winners to settle, cut reversers (exit at bid when spot
     crosses back through open). Does it beat plain hold OOS?
Edge>=0.04, one side/market, frozen May calibration, Binance-proxy winners.
"""
import io, math, os
import numpy as np, pandas as pd, orjson, zstandard as zstd
pd.set_option("display.width",240)
DATA=r"C:\Users\tico_\Fable\5minSnip\data"; PM=r"D:\polycrypto\live_l2\polymarket"; BN=r"D:\polycrypto\live_l2\binance"
FEE,STAKE=0.07,10.0; DAYS=[f"2026-06-{d:02d}" for d in range(13,19)]; MK_WAIT=30
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
def cl_at(a,sec):
    secs,cl=B[a]; i=np.searchsorted(secs,sec,side="right")-1
    return cl[i] if (i>=0 and sec-secs[i]<=5) else np.nan
def vol_at(a,sec,lb=60):
    secs,cl=B[a]; i=np.searchsorted(secs,sec,side="right")-1
    return np.std(np.diff(np.log(cl[i-lb:i])))*1e4 if i>=lb+1 else np.nan
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
H=[]; STOP=[]; MKwin=[]; MKfill=0; MKn=0
for tid,sm_ in book.items():
    a,side,settle_ms,ep=tokmap[tid]; ss=settle_ms//1000
    grid=np.arange(200,-1,-1); absec=ss-grid; ser=pd.Series(sm_)
    bb=ser.reindex(absec).map(lambda x:x[0] if isinstance(x,tuple) else np.nan).ffill().values
    ba=ser.reindex(absec).map(lambda x:x[1] if isinstance(x,tuple) else np.nan).ffill().values
    op=cl_at(a,ep-1); fin=cl_at(a,ss-1)
    if not(np.isfinite(op) and np.isfinite(fin)): continue
    win=int((side=="up")==(fin>=op)); spot=np.array([cl_at(a,s) for s in absec]); sgn=1.0 if side=="up" else -1.0
    n=len(grid); vel=np.full(n,np.nan); vel[2:]=sgn*(spot[2:]/spot[:-2]-1)*1e4; disp=sgn*(spot/op-1)*1e4
    # next index where spot crosses back through open (against side)
    nxt=np.full(n,-1); cur=-1
    for i in range(n-1,-1,-1):
        nxt[i]=cur
        if (spot[i]<op) if side=="up" else (spot[i]>op): cur=i
    for i in range(n):
        t=grid[i]
        if not(5<=t<=180) or not np.isfinite(vel[i]) or vel[i]<2 or disp[i]<=0 or not np.isfinite(spot[i]): continue
        vol=vol_at(a,int(absec[i]))
        if not vol or vol<=0: continue
        A=ba[i+1] if i+1<n else ba[i]; Bd=bb[i+1] if i+1<n else bb[i]
        if not np.isfinite(A) or not(0.30<=A<=0.99): continue
        z=disp[i]/(vol*math.sqrt(t)); edge=pcal(z)-A-FEE*A*(1-A)
        if edge<0.04: break
        sh=STAKE/A; efee=sh*FEE*A*(1-A)
        H.append(sh*win-STAKE-efee)
        ej=nxt[i]
        if ej>=0 and ej>i and np.isfinite(bb[ej]):
            be=bb[ej]; STOP.append(sh*be-STAKE-efee-sh*FEE*be*(1-be))
        else: STOP.append(sh*win-STAKE-efee)
        # maker-in: post buy at Bd; fill if ask drops to <=Bd within MK_WAIT s
        MKn+=1
        w=ba[i+1:i+1+MK_WAIT]
        if w.size and np.nanmin(w)<=Bd+1e-9:
            MKfill+=1; MKwin.append(win)   # filled -> would hold; track outcome
        break
H=np.array(H); STOP=np.array(STOP)
print(f"\nOOS edge>=0.04 one-side: n={len(H)}")
print(f"  HOLD            ev=${H.mean():+.3f} win-implied  tot=${H.sum():+.0f} std=${H.std():.2f}")
print(f"  HOLD+STOP-OUT   ev=${STOP.mean():+.3f}            tot=${STOP.sum():+.0f} std=${STOP.std():.2f}")
print(f"\nMAKER-IN (post buy at bid, wait {MK_WAIT}s):")
print(f"  fill rate={MKfill/MKn:.0%}  win-rate OF FILLED={np.mean(MKwin):.3f}  (taker entries win ~0.71)")
print("  -> if filled-win << 0.71, maker entry is adversely selected (fills the losers)")
