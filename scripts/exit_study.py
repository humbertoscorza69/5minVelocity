"""Exit study: HOLD-to-resolution vs REPRICE-EXIT (maker TP at fair value).
The question: we reliably capture the LAG (book reprices to our side); do we gain
by harvesting that reprice as a maker and sidestepping the 5-min settlement
reversal risk? Judge on win rate, variance, Sharpe (EV/std) AND Kelly-sized
growth — because higher win rate + lower variance lets you size up.
Recorder Jun18-29, edge>=0.04, frozen May cal, Binance-proxy winners.
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
    sufbid=np.maximum.accumulate(np.where(np.isfinite(bb),bb,-1)[::-1])[::-1]
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
        R.append({"a":a,"vol":vv,"z":z,"edge":edge,"ask":A,"win":win,"fair":float(pcal(z)),"fmax":fmax})
        break
E=pd.DataFrame(R); print("\nentries:",len(E))
a=E.ask.values; win=E.win.values; sh=STAKE/a; efee=sh*FEE*a*(1-a); fair=E.fair.values; fmax=E.fmax.values

def summary(name, pnl, won):
    ev=pnl.mean(); sd=pnl.std(); wr=won.mean()
    # Kelly-sized terminal wealth: per-trade return on $1 staked = pnl/STAKE;
    # f = clip(p - (1-p)/b, 0,1) with b=avg odds; size = 0.25*f of bankroll.
    r=pnl/STAKE
    print(f"  {name:<26} n={len(pnl):>4} win={wr:.3f} EV=${ev:+.3f} std=${sd:.2f} Sharpe(EV/std)={ev/sd:+.3f} tot=${pnl.sum():+.0f}")
    return ev/sd, r, wr

def kelly_growth(r, wr, label):
    # fractional-Kelly on the empirical win rate + realized per-$ returns
    # approximate odds from mean win return; sequential 1/4-Kelly, cap 20%/trade
    wins=r[r>0]; loss=r[r<=0]
    b = wins.mean()/abs(loss.mean()) if len(loss) and loss.mean()!=0 else 1.0
    f = max(0.0, wr - (1-wr)/b); f=min(f*0.25, 0.20)
    bk=1.0
    for x in r: bk*=(1+f*x)
    print(f"    {label}: kelly_frac={f:.3f}  terminal_bankroll=x{bk:.2f}")

print("\n=== EXIT POLICIES (flat $10) ===")
hold_pnl=sh*win-STAKE-efee
s_hold,r_hold,wr_hold=summary("HOLD to resolution", hold_pnl, win.astype(float))

for lab,S in [("reprice@fair (pcal)",fair),
              ("reprice@halfway",a+0.5*(fair-a)),
              ("reprice@+0.03",a+0.03)]:
    filled=(fmax>=S-1e-9)&(S<1.0)
    pnl=np.where(filled, sh*(S-a)-efee, sh*win-STAKE-efee)   # maker exit (no exit fee) else hold
    won=np.where(filled, 1.0, win.astype(float))             # a filled reprice = a "win" (locked profit)
    print(f"   fill_rate={filled.mean():.1%}")
    summary(lab, pnl, won)

print("\n=== KELLY-SIZED GROWTH (can higher win-rate size up to beat hold?) ===")
kelly_growth(r_hold, wr_hold, "HOLD")
for lab,S in [("reprice@fair",fair),("reprice@halfway",a+0.5*(fair-a))]:
    filled=(fmax>=S-1e-9)&(S<1.0)
    pnl=np.where(filled, sh*(S-a)-efee, sh*win-STAKE-efee); won=np.where(filled,1.0,win.astype(float))
    kelly_growth(pnl/STAKE, won.mean(), lab)
