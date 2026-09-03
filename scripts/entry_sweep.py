"""Entry-selection sweep: is there a way to raise WIN RATE without cutting total?
For each market, collect ALL seconds where the v2 gate passes, then compare
selection policies: FIRST signal (current) vs LATE (lowest ttl) vs MAX-Z vs
z-floor (only trade markets that reach z>=X). Report win, EV, std, Sharpe,
total — flat AND edge-weighted — to see the win-rate/volume tradeoff and
whether sizing compensates. Recorder Jun18-29, frozen May cal.
"""
import io, math, os
import numpy as np, pandas as pd, orjson, zstandard as zstd
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
zb=np.array([-1,0,.3,.6,1,1.5,2,3,5,100]); mids=[];ws=[]
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
# per token: collect ALL qualifying seconds (ttl, ask, z, edge) + win
TOK=[]
for tid,sm_ in book.items():
    a,side,settle_ms,ep=tokmap[tid]; ss=settle_ms//1000
    grid=np.arange(200,-1,-1); absec=ss-grid; ser=pd.Series(sm_)
    ba=ser.reindex(absec).map(lambda x:x[1] if isinstance(x,tuple) else np.nan).ffill().values
    op=cl(a,ep-1); fin=cl(a,ss-1)
    if not(np.isfinite(op) and np.isfinite(fin)): continue
    win=int((side=="up")==(fin>=op)); spot=np.array([cl(a,s) for s in absec]); sgn=1.0 if side=="up" else -1.0
    n=len(grid); vel=np.full(n,np.nan); vel[2:]=sgn*(spot[2:]/spot[:-2]-1)*1e4; disp=sgn*(spot/op-1)*1e4
    quals=[]
    for i in range(n):
        t=grid[i]
        if not(5<=t<=180) or not np.isfinite(vel[i]) or vel[i]<2 or disp[i]<=0 or not np.isfinite(spot[i]): continue
        vv=vol(a,int(absec[i]))
        if not vv or vv<=0 or vv>1.0: continue
        A=ba[i+1] if i+1<n else ba[i]
        if not np.isfinite(A) or not(0.30<=A<=0.97): continue
        z=disp[i]/(vv*math.sqrt(t)); edge=pcal(z)-A-FEE*A*(1-A)
        if edge<0.04: continue
        quals.append((t,A,z,edge))
    if quals: TOK.append((win, quals))
print(f"\nmarkets with >=1 qualifying entry: {len(TOK)}")

def evalpolicy(name, pick):
    rows=[]
    for win,quals in TOK:
        q=pick(quals)
        if q is None: continue
        t,A,z,edge=q; sh=STAKE/A; pnl=sh*win-STAKE-sh*FEE*A*(1-A)
        rows.append((pnl,win,edge,A,z,t))
    if not rows: print(f"  {name:<16} (none)"); return
    pnl=np.array([r[0] for r in rows]); wn=np.array([r[1] for r in rows]); eg=np.array([r[2] for r in rows])
    # edge-weighted total at same avg capital
    w=np.clip(eg,0,None); w=w/w.mean(); tot_w=(pnl*w).sum()
    print(f"  {name:<16} n={len(rows):>4} win={wn.mean():.3f} EV=${pnl.mean():+.3f} std=${pnl.std():.2f} "
          f"Sharpe={pnl.mean()/pnl.std():+.3f} tot(flat)=${pnl.sum():+.0f} tot(edgeWtd)=${tot_w:+.0f}")

print("\n=== entry-selection policies ===")
evalpolicy("FIRST(current)", lambda q: max(q, key=lambda x:x[0]))        # highest ttl = earliest
evalpolicy("LATE(min ttl>=30)", lambda q: min([x for x in q if x[0]>=30], key=lambda x:x[0], default=None) if any(x[0]>=30 for x in q) else None)
evalpolicy("MAX-Z", lambda q: max(q, key=lambda x:x[2]))
for zf in (1.0,1.5,2.0):
    evalpolicy(f"z>= {zf} (first)", (lambda zf: lambda q: next((x for x in sorted(q,key=lambda y:-y[0]) if x[2]>=zf), None))(zf))
for ef in (0.06,0.10):
    evalpolicy(f"edge>= {ef} (first)", (lambda ef: lambda q: next((x for x in sorted(q,key=lambda y:-y[0]) if x[3]>=ef), None))(ef))
