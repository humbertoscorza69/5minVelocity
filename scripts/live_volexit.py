"""Test the VOL-CONDITIONAL EXIT hypothesis on live recorder data (Jun18-29):
 low-vol trades -> HOLD to settle; high-vol trades -> maker TAKE-PROFIT scalp
 (post sell at A+TP, fill if best_bid later reaches it; else hold).
Compares: all-hold | all-stop | vol-split(hold low / scalp high). And isolates
the high-vol subset: hold vs scalp (does scalp beat the coin-flip hold?).
edge>=0.04, frozen May cal, Binance-proxy winners.
"""
import io, math, os
import numpy as np, pandas as pd, orjson, zstandard as zstd
pd.set_option("display.width",240)
DATA=r"C:\Users\tico_\Fable\5minSnip\data"; PM=r"D:\polycrypto\live_l2\polymarket"; BN=r"D:\polycrypto\live_l2\binance"
FEE,STAKE=0.07,10.0; DAYS=[f"2026-06-{d:02d}" for d in range(18,30)]; VC=1.0
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
    sufbid=np.maximum.accumulate(np.where(np.isfinite(bb),bb,-1)[::-1])[::-1]  # max future best-bid
    for i in range(n):
        t=grid[i]
        if not(5<=t<=180) or not np.isfinite(vel[i]) or vel[i]<2 or disp[i]<=0 or not np.isfinite(spot[i]): continue
        vv=vol(a,int(absec[i]))
        if not vv or vv<=0: continue
        A=ba[i+1] if i+1<n else ba[i]
        if not np.isfinite(A) or not(0.30<=A<=0.97): continue
        z=disp[i]/(vv*math.sqrt(t)); edge=pcal(z)-A-FEE*A*(1-A)
        if edge<0.04: break
        fmax=sufbid[i+1] if i+1<n else -1
        R.append({"asset":a,"vol":vv,"edge":edge,"ask":A,"win":win,"fmax":fmax,"pcal":float(pcal(z)),"day":int(ss/86400)})
        break
E=pd.DataFrame(R); print("\nentries:",len(E))

def hold_pnl(d):
    a=d.ask.values; sh=STAKE/a; return sh*d.win.values-STAKE-sh*FEE*a*(1-a)
def scalp_pnl(d,tp):
    a=d.ask.values; sh=STAKE/a; efee=sh*FEE*a*(1-a)
    S=a+tp if np.isscalar(tp) else tp
    filled=(d.fmax.values>=S-1e-9)&(S<1.0)
    return np.where(filled, sh*(S-a)-efee, sh*d.win.values-STAKE-efee), filled.mean()

hi=E[E.vol>=VC]; lo=E[E.vol<VC]
print(f"\nhigh-vol (>= {VC}): n={len(hi)}  low-vol: n={len(lo)}")
print("\n=== HIGH-VOL subset: HOLD vs maker-TP SCALP (does scalp beat the coin-flip?) ===")
hp=hold_pnl(hi); print(f"  HOLD    ev=${hp.mean():+.3f} tot=${hp.sum():+.0f} win={hi.win.mean():.3f}")
for tp in [0.03,0.05,0.08]:
    sp,fr=scalp_pnl(hi,tp); print(f"  scalp TP={tp}  fill={fr:.0%} ev=${sp.mean():+.3f} tot=${sp.sum():+.0f}")
sp,fr=scalp_pnl(hi, hi.ask.values+0.5*(hi.pcal.values-hi.ask.values)); print(f"  scalp TP=0.5xedge fill={fr:.0%} ev=${sp.mean():+.3f} tot=${sp.sum():+.0f}")

print("\n=== WHOLE BOOK: policy comparison (total over all entries) ===")
allhold=hold_pnl(E)
print(f"  ALL-HOLD                 ev=${allhold.mean():+.3f} tot=${allhold.sum():+.0f} std=${allhold.std():.2f}")
# vol-split: low->hold, high->scalp
for tp in [0.03,0.05]:
    lo_p=hold_pnl(lo); hi_p,_=scalp_pnl(hi,tp)
    comb=np.concatenate([lo_p,hi_p])
    print(f"  SPLIT(lo=hold,hi=scalpTP{tp}) ev=${comb.mean():+.3f} tot=${comb.sum():+.0f} std=${comb.std():.2f}")
# vol-cap only (drop high)
print(f"  VOL-CAP only (drop hi)   ev=${hold_pnl(lo).mean():+.3f} tot=${hold_pnl(lo).sum():+.0f} std=${hold_pnl(lo).std():.2f} n={len(lo)}")
