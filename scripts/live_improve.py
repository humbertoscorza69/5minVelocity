"""Mine the LIVE period (Jun18-29, recorder data) for loss-reduction ideas.
Reconstructs edge-gate entries (frozen May cal) with full features + post-entry
path, then: (1) what separates wins vs losses, (2) win/EV by vol & ttl & edge,
(3) HOLD vs STOP-OUT overall + per day (red-day savings). edge>=0.04 to match bot.
"""
import io, math, os
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
# frozen May calibration
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
print("tokens",len(tokmap))
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
    nxt=np.full(n,-1); cur=-1
    for i in range(n-1,-1,-1):
        nxt[i]=cur
        if (spot[i]<op) if side=="up" else (spot[i]>op): cur=i
    for i in range(n):
        t=grid[i]
        if not(5<=t<=180) or not np.isfinite(vel[i]) or vel[i]<2 or disp[i]<=0 or not np.isfinite(spot[i]): continue
        vv=vol(a,int(absec[i]))
        if not vv or vv<=0: continue
        A=ba[i+1] if i+1<n else ba[i]
        if not np.isfinite(A) or not(0.30<=A<=0.97): continue
        z=disp[i]/(vv*math.sqrt(t)); edge=pcal(z)-A-FEE*A*(1-A)
        if edge<0.04: break
        sh=STAKE/A; efee=sh*FEE*A*(1-A); hold=sh*win-STAKE-efee
        ej=nxt[i]
        if ej>=0 and ej>i and np.isfinite(bb[ej]):
            be=bb[ej]; stop=sh*be-STAKE-efee-sh*FEE*be*(1-be)
        else: stop=hold
        R.append({"asset":a,"ttl":t,"vel":vel[i],"disp":disp[i],"vol":vv,"z":z,"edge":edge,
                  "ask":A,"win":win,"hold":hold,"stop":stop,"day":int(ss/86400)})
        break
E=pd.DataFrame(R); print("\nlive-period edge>=0.04 entries:",len(E),"  win=%.3f hold_ev=$%.3f"%(E.win.mean(),E.hold.mean()))

print("\n=== WINS vs LOSSES: mean feature ===")
for f in ["vol","ttl","disp","vel","z","edge","ask"]:
    print(f"  {f:>5}: win={E[E.win==1][f].mean():.3f}  loss={E[E.win==0][f].mean():.3f}")

def tab(col,bins,lab):
    E["b"]=pd.cut(E[col],bins)
    g=E.groupby("b",observed=True).agg(n=("win","size"),win=("win","mean"),ev=("hold","mean"))
    print(f"\n=== win/EV by {lab} ===")
    for b,r in g.iterrows(): print(f"  {str(b):>14} n={int(r.n):>4} win={r.win:.2f} ev=${r.ev:+.3f}")
tab("vol",[0,0.3,0.5,0.7,1.0,1.5,10],"VOL bucket (bps/s)")
tab("ttl",[5,15,30,60,120,180],"TTL bucket (s)")
tab("edge",[0.04,0.06,0.10,0.15,1],"EDGE bucket")

print("\n=== HOLD vs STOP-OUT (overall) ===")
print(f"  HOLD  ev=$%.3f tot=$%.0f std=$%.2f"%(E.hold.mean(),E.hold.sum(),E.hold.std()))
print(f"  STOP  ev=$%.3f tot=$%.0f std=$%.2f"%(E.stop.mean(),E.stop.sum(),E.stop.std()))
print("\n=== per day: HOLD vs STOP (red-day savings) ===")
g=E.groupby("day").agg(n=("win","size"),win=("win","mean"),hold=("hold","sum"),stop=("stop","sum"))
for d,r in g.iterrows(): print(f"  day {d} n={int(r.n):>3} win={r.win:.2f} HOLD=$%+.0f STOP=$%+.0f"%(r.hold,r.stop))

E=E.drop(columns=[c for c in ["b"] if c in E.columns])
try: E.to_parquet(DATA+r"\live_entries.parquet")
except Exception as ex: print("parquet skip:",ex)
print("\n=== COMBINED: vol cap x exit (hold vs stop) ===")
for vc in [99,1.2,1.0,0.8]:
    d=E[E.vol<vc]
    print(f" vol<{vc}: n={len(d)} win={d.win.mean():.3f} | HOLD ev=${d.hold.mean():.3f} tot=${d.hold.sum():.0f} std=${d.hold.std():.2f} | STOP ev=${d.stop.mean():.3f} tot=${d.stop.sum():.0f} std=${d.stop.std():.2f}"
          .replace("$-","-$"))
# worst-day comparison under each policy
print("\n=== worst single-day P&L under each policy ===")
for lbl,vc,col in [("HOLD all",99,"hold"),("STOP all",99,"stop"),("HOLD vol<1.0",1.0,"hold"),("STOP vol<1.0",1.0,"stop")]:
    d=E[E.vol<vc]; g=d.groupby("day")[col].sum()
    print(f"  {lbl:<14} worst day=${g.min():+.0f}  total=${g.sum():+.0f}  green days={int((g>0).sum())}/{len(g)}")
