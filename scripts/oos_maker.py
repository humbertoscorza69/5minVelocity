"""OOS maker-scalp test on D:\\polycrypto June 13-18 (untouched).
Taker buy at ask A (signal+1s). Post maker SELL at S=A+TP (no fee). Fill modeled
from REAL recorded book: filled if best_bid later rises to >= S before settle.
Unfilled -> hold to settle. One-side & both-sides, fixed & dynamic TP. Compare to
hold-to-settle. Frozen May calibration. Binance-proxy winners (independent).
"""
import io, math, os
import numpy as np
import pandas as pd
import orjson, zstandard as zstd
pd.set_option("display.width", 240)
DATA=r"C:\Users\tico_\Fable\5minSnip\data"; PM=r"D:\polycrypto\live_l2\polymarket"; BN=r"D:\polycrypto\live_l2\binance"
FEE,STAKE=0.07,10.0; DAYS=[f"2026-06-{d:02d}" for d in range(13,19)]

def lines(p):
    if p.endswith(".zst"):
        with open(p,"rb") as f:
            for ln in io.TextIOWrapper(io.BufferedReader(zstd.ZstdDecompressor().stream_reader(f),1<<24),encoding="utf-8"): yield ln
    else:
        with open(p,encoding="utf-8",buffering=1<<24) as f:
            for ln in f: yield ln
def find(base,day):
    for e in (".jsonl.zst",".jsonl"):
        q=os.path.join(base,day+e)
        if os.path.exists(q): return q

# frozen May calibration
sig=pd.read_parquet(DATA+r"\strat_signals.parquet"); may=sig[(sig.month=="may")&(sig.is_first==1)]
zb=np.array([-1,0,.3,.6,1,1.5,2,3,5,100]); mids=[]; ws=[]
for lo,hi in zip(zb[:-1],zb[1:]):
    s=may[(may.z>=lo)&(may.z<hi)]
    if len(s)>=20: mids.append(s.z.mean()); ws.append(s.win.mean())
mids=np.array(mids); ws=np.array(ws)
pcal=lambda z: np.interp(z,mids,ws)

tokmap={}
for day in DAYS:
    p=find(os.path.join(PM,"markets"),day)
    if not p: continue
    for ln in lines(p):
        try: m=orjson.loads(ln).get("market") or {}
        except: continue
        if m.get("interval")!="5m": continue
        a=str(m.get("asset","")).lower(); ep=m.get("epoch")
        if a not in ("btc","eth") or ep is None: continue
        s=(ep+300)*1000
        if m.get("up_token_id"): tokmap[m["up_token_id"]]=(a,"up",s,ep)
        if m.get("down_token_id"): tokmap[m["down_token_id"]]=(a,"down",s,ep)
print("5m tokens:",len(tokmap))

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
    if i<lb+1: return np.nan
    return np.std(np.diff(np.log(cl[i-lb:i])))*1e4
print("binance loaded")

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
    print("parsed",day,"tokens",len(book))

rows=[]
for tid,sm_ in book.items():
    a,side,settle_ms,ep=tokmap[tid]; ss=settle_ms//1000
    grid=np.arange(200,-1,-1); absec=ss-grid
    ser=pd.Series(sm_)
    bb=ser.reindex(absec).map(lambda x:x[0] if isinstance(x,tuple) else np.nan).ffill().values
    ba=ser.reindex(absec).map(lambda x:x[1] if isinstance(x,tuple) else np.nan).ffill().values
    op=cl_at(a,ep-1); fin=cl_at(a,ss-1)
    if not(np.isfinite(op) and np.isfinite(fin)): continue
    win=int((side=="up")==(fin>=op))
    spot=np.array([cl_at(a,s) for s in absec]); sgn=1.0 if side=="up" else -1.0
    n=len(grid); vel=np.full(n,np.nan); vel[2:]=sgn*(spot[2:]/spot[:-2]-1)*1e4
    disp=sgn*(spot/op-1)*1e4
    sufmax_bb=np.maximum.accumulate(bb[::-1])[::-1]   # suffix max of best bid
    first=True
    for i in range(n):
        t=grid[i]
        if not(5<=t<=180) or not np.isfinite(vel[i]) or vel[i]<2 or disp[i]<=0 or not np.isfinite(spot[i]): continue
        vol=vol_at(a,int(absec[i]))
        if not vol or vol<=0: continue
        A=ba[i+1] if i+1<n else ba[i]
        if not np.isfinite(A) or not(0.30<=A<=0.99): continue
        z=disp[i]/(vol*math.sqrt(t)); pc=float(pcal(z)); edge=pc-A-FEE*A*(1-A)
        fmax=sufmax_bb[i+1] if i+1<n else np.nan        # max best-bid from entry+1..settle
        rep10=(np.nanmax(bb[i+1:i+11])-A) if i+1<n else np.nan
        rows.append((f"{a}_{settle_ms}",a,t,vel[i],z,pc,edge,A,win,fmax,rep10,int(first)))
        first=False
E=pd.DataFrame(rows,columns=["mkt","asset","ttl","vel","z","pcal","edge","A","win","fmax","rep10","is_first"])
print("\nOOS signals:",len(E)," (first/market:",int(E.is_first.sum()),")")
E=E[E.edge>=0.04].copy()
print("edge>=0.04:",len(E))

print("\nrealized 10s reprice (best_bid_max - A):  median=%.3f  frac>0.05=%.0f%%"%(
    np.nanmedian(E.rep10),100*np.nanmean(E.rep10>0.05)))

def hold(d):
    a=d.A.values; sh=STAKE/a; fee=sh*FEE*a*(1-a); p=sh*d.win.values-STAKE-fee
    return len(d),d.win.mean(),p.mean(),p.sum(),p.std()
def scalp(d,tpf):
    a=d.A.values; sh=STAKE/a; fee=sh*FEE*a*(1-a)
    S=tpf(d); filled=(d.fmax.values>=S-1e-9)&(S<1.0)
    p=np.where(filled, sh*(S-a)-fee, sh*d.win.values-STAKE-fee)
    return len(d),filled.mean(),p.mean(),p.sum(),p.std()

E1=E[E.is_first==1]
print("\n=== OOS June13-18, edge>=0.04 ===")
for lbl,d in [("ONE-side",E1),("BOTH-sides",E)]:
    n,w,ev,tot,sd=hold(d)
    print(f"  HOLD  {lbl:<10} n={n:>4} win={w:.3f} ev=${ev:+.3f} tot=${tot:+.0f} std=${sd:.2f}")
    for f in [0.5,0.7]:
        n,fl,ev,tot,sd=scalp(d, lambda x,f=f: x.A.values+f*(x.pcal.values-x.A.values))
        print(f"  SCALP {lbl:<10} TP={f}x  n={n:>4} fill={fl:.0%} ev=${ev:+.3f} tot=${tot:+.0f} std=${sd:.2f}")
print(f"\n(both-sides ~{len(E)/6:.0f} trades/day over 6 days)")
