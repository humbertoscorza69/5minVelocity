"""TRUE OUT-OF-SAMPLE TEST on D:\\polycrypto live recorder, June 13-18 (never touched).
Signal = Binance-derived (vel/disp/vol/z). Price paid = recorded Polymarket ask
(best_bid_ask). Label = Binance final-vs-open (independent of the PM book).
Calibration p_cal(z) is FROZEN from May (no refit). All causal: signal at t,
ask at t+1s, outcome at settle.

Outputs:
  - strategy EV OOS (edge-gate / vel+z / basic) vs in-sample  -> overfitting check
  - calibration: realized win vs Phi(z) and vs p_cal               -> leakage check
  - delay sweep (enter at signal-1..+3 s)                          -> look-ahead check
"""
import glob, io, math, os
import numpy as np
import pandas as pd
import orjson
import zstandard as zstd
from scipy.stats import norm
pd.set_option("display.width", 240)

DATA = r"C:\Users\tico_\Fable\5minSnip\data"
PM = r"D:\polycrypto\live_l2\polymarket"
BN = r"D:\polycrypto\live_l2\binance"
FEE, STAKE = 0.07, 10.0
DAYS = [f"2026-06-{d:02d}" for d in range(13, 19)]   # 13..18 OOS

def lines(path):
    if path.endswith(".zst"):
        with open(path, "rb") as f:
            r = io.BufferedReader(zstd.ZstdDecompressor().stream_reader(f), 1<<24)
            for ln in io.TextIOWrapper(r, encoding="utf-8"):
                yield ln
    else:
        with open(path, encoding="utf-8", buffering=1<<24) as f:
            for ln in f:
                yield ln

def find(base, day):
    for ext in (".jsonl.zst", ".jsonl"):
        p = os.path.join(base, day+ext)
        if os.path.exists(p): return p
    return None

# ---------- 1. May calibration (FROZEN) ----------
sig = pd.read_parquet(DATA+r"\strat_signals.parquet")
may = sig[(sig.month=="may")&(sig.is_first==1)]
zb=np.array([-1,0,.3,.6,1,1.5,2,3,5,100]); mids=[]; ws=[]
for lo,hi in zip(zb[:-1],zb[1:]):
    s=may[(may.z>=lo)&(may.z<hi)]
    if len(s)>=20: mids.append(s.z.mean()); ws.append(s.win.mean())
mids=np.array(mids); ws=np.array(ws)
def pcal(z): return np.interp(z, mids, ws)
print("frozen May calibration nodes z=",np.round(mids,2)," win=",np.round(ws,3))

# ---------- 2. market map (5m) ----------
tokmap={}
for day in DAYS:
    p=find(os.path.join(PM,"markets"),day)
    if not p: continue
    for ln in lines(p):
        try: rec=orjson.loads(ln)
        except: continue
        m=rec.get("market") or {}
        if m.get("interval")!="5m": continue
        a=str(m.get("asset","")).lower(); ep=m.get("epoch")
        if a not in ("btc","eth") or ep is None: continue
        s=(ep+300)*1000
        if m.get("up_token_id"): tokmap[m["up_token_id"]]=(a,"up",s,ep)
        if m.get("down_token_id"): tokmap[m["down_token_id"]]=(a,"down",s,ep)
print("5m tokens mapped:",len(tokmap))

# ---------- 3. binance spot ----------
B={}
for a,sym in (("btc","btcusdt"),("eth","ethusdt")):
    d={}
    for day in DAYS:
        p=find(os.path.join(BN,f"{sym}_kline_1s"),day)
        if not p: continue
        for ln in lines(p):
            try: k=orjson.loads(ln)["payload"]["data"]["k"]
            except: continue
            d[int(k["t"])//1000]=float(k["c"])
    secs=np.array(sorted(d)); cl=np.array([d[s] for s in secs])
    B[a]=(secs,cl)
    print(f"binance {a}: {len(secs)} secs {secs.min()}..{secs.max()}")

def cl_at(a,sec):
    secs,cl=B[a]; i=np.searchsorted(secs,sec,side="right")-1
    if i<0: return np.nan
    if sec-secs[i]>5: return np.nan
    return cl[i]
def vol_at(a,sec,lb=60):
    secs,cl=B[a]; i=np.searchsorted(secs,sec,side="right")-1
    if i<lb+1: return np.nan
    seg=cl[i-lb:i]
    return np.std(np.diff(np.log(seg)))*1e4

# ---------- 4. best_bid_ask -> per token per sec ----------
book={}
for day in DAYS:
    p=find(os.path.join(PM,"best_bid_ask"),day)
    if not p: continue
    nseen=0
    for ln in lines(p):
        nseen+=1
        try: pp=orjson.loads(ln)["payload"]
        except: continue
        tid=pp.get("asset_id")
        info=tokmap.get(tid)
        if info is None: continue
        s=info[2]; ts=int(pp["timestamp"])
        if ts<s-205000 or ts>s: continue
        try: bb=float(pp["best_bid"]); ba=float(pp["best_ask"])
        except: continue
        book.setdefault(tid,{})[ts//1000]=(bb,ba)
    print(f"  parsed {day} ({nseen:,} lines), tokens with book so far={len(book)}")

# ---------- 5. build signals (causal) ----------
rows=[]
for tid,secmap in book.items():
    a,side,settle_ms,ep=tokmap[tid]; ss=settle_ms//1000
    grid=np.arange(200,-1,-1); absec=ss-grid
    ser=pd.Series({s:v for s,v in secmap.items()})
    bb=ser.reindex(absec).map(lambda x:x[0] if isinstance(x,tuple) else np.nan).ffill().values
    ba=ser.reindex(absec).map(lambda x:x[1] if isinstance(x,tuple) else np.nan).ffill().values
    openpx=cl_at(a,ep-1); finalpx=cl_at(a,ss-1)
    if not(np.isfinite(openpx) and np.isfinite(finalpx)): continue
    up_win = finalpx>=openpx
    win = int((side=="up")==up_win)
    spot=np.array([cl_at(a,s) for s in absec])
    sgn=1.0 if side=="up" else -1.0
    n=len(grid); vel=np.full(n,np.nan); vel[2:]=sgn*(spot[2:]/spot[:-2]-1)*1e4
    disp=sgn*(spot/openpx-1)*1e4
    for i in range(n):
        t=grid[i]
        if not(5<=t<=180) or not np.isfinite(vel[i]) or vel[i]<2 or disp[i]<=0: continue
        if not np.isfinite(spot[i]): continue
        vol=vol_at(a,int(absec[i]))
        if not vol or vol<=0: continue
        z=disp[i]/(vol*math.sqrt(t))
        a_d={d:(ba[i+d] if 0<=i+d<n else np.nan) for d in (-1,1,2,3)}
        rows.append((f"{a}_{settle_ms}",a,t,vel[i],disp[i],vol,z,win,
                     a_d[1],a_d[2],a_d[3],a_d[-1]))
        break  # one side / first signal per market
E=pd.DataFrame(rows,columns=["mkt","asset","ttl","vel","disp","vol","z","win",
                             "a1","a2","a3","a_1"])
E["pcal"]=pcal(E.z.values); E["edge"]=E.pcal-E.a1-FEE*E.a1*(1-E.a1)
print(f"\nOOS first-signal entries (Jun13-18): {len(E)}")

def hold_ev(d,col="a1"):
    d=d[d[col].notna()&(d[col]<1.0)&(d[col]>0)]
    if len(d)<10: return None
    a=d[col].values; sh=STAKE/a; fee=sh*FEE*a*(1-a)
    pnl=sh*d.win.values-STAKE-fee
    return len(d),d.win.mean(),a.mean(),pnl.mean(),pnl.sum()

print("\n===== OOS STRATEGY (hold-to-settle, $10), Jun13-18 =====")
for name,f in [("BASIC vel>=4",E[(E.vel>=4)&(E.a1<=0.97)]),
               ("VEL+Z (vel4,z>=.5,ask<=.9)",E[(E.vel>=4)&(E.z>=0.5)&(E.a1<=0.90)]),
               ("EDGE>=0.04",E[E.edge>=0.04]),
               ("EDGE>=0.08",E[E.edge>=0.08])]:
    r=hold_ev(f)
    if r: print(f"  {name:<28} n={r[0]:>4} win={r[1]:.3f} ask={r[2]:.3f} ev=${r[3]:+.3f} tot=${r[4]:+.0f}")

print("\n===== LEAKAGE CHECK 1: calibration (realized win vs model) =====")
g=E.copy(); g["pb"]=pd.cut(g.pcal,[0,.6,.7,.8,.9,.97,1.001])
agg=g.groupby("pb",observed=True).agg(n=("win","size"),
    pred_phi=("z",lambda z:norm.cdf(z).mean()),pred_cal=("pcal","mean"),
    realized=("win","mean"))
print(agg.to_string())

print("\n===== LEAKAGE CHECK 2: delay sweep (EDGE>=0.04) =====")
print("  enter at signal+d s. d=-1 = BEFORE signal (placebo, must be untradeable)")
ed=E[E.edge>=0.04]
for d,col in [(-1,"a_1"),(1,"a1"),(2,"a2"),(3,"a3")]:
    r=hold_ev(ed,col)
    if r: print(f"  d={d:+d}: n={r[0]} win={r[1]:.3f} ask={r[2]:.3f} ev=${r[3]:+.3f}")
