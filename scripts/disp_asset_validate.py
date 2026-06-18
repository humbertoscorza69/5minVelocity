"""Validate the live finding (BTC edge at low disp, ETH at high disp) against the
large backtest. First-per-market entry like the bot: vel>=4, ttl 5-180, ask in
[0.45,0.97]. Bucket by asset x disp, and also by vol-normalized z=disp/(vol*sqrt(ttl)).
$10-stake EV. May+June 5m.
"""
import glob, os
import numpy as np
import pandas as pd
pd.set_option("display.width", 240)
DATA = r"C:\Users\tico_\Fable\5minSnip\data"
FEE, STAKE = 0.07, 10.0

def load_sym(sym):
    fs = sorted(glob.glob(os.path.join(DATA, "binance", f"{sym}_*.parquet")))
    df = pd.concat([pd.read_parquet(f) for f in fs], ignore_index=True)
    return (df.drop_duplicates("open_time").sort_values("open_time").open_time.values.astype(np.int64),
            df.close.values.astype(float))
B = {"btc": load_sym("BTCUSDT"), "eth": load_sym("ETHUSDT")}

def px(a, sec):
    ot, cl = B[a]; i = np.searchsorted(ot, sec*1000, side="right")-1
    v = np.where(i >= 0, cl[np.clip(i,0,len(cl)-1)], np.nan)
    gap = sec*1000 - ot[np.clip(i,0,len(cl)-1)]
    return np.where((i>=0)&(gap<=5000), v, np.nan)

def lvol(a, sec, lb=60):
    ot, cl = B[a]; i = np.searchsorted(ot, sec*1000, side="right")-1
    if i < lb+1: return np.nan
    return np.std(np.diff(np.log(cl[i-lb:i])))*1e4

def build(path):
    bs = pd.read_parquet(path); bs = bs[(bs.ba<1)&(bs.bb>0)&(bs.ba>=bs.bb)]
    rec=[]
    for tok,g in bs.groupby("tok",sort=False):
        g=g.sort_values("ttl",ascending=False)
        asset=g.asset.iloc[0]; side=g.side.iloc[0]; win=int(g.winner.iloc[0])
        sm=int(g.settle_ms.iloc[0]); ss=sm//1000
        ttl=g.ttl.values; grid=np.arange(int(ttl.max()),-1,-1)
        ba=pd.Series(g.ba.values,index=ttl).reindex(grid).ffill().to_dict()
        sec=ss-grid; upx=px(asset,sec); sgn=1.0 if side=="up" else -1.0
        opx=px(asset,np.array([ss-300]))[0]
        if not np.isfinite(opx): continue
        n=len(grid); vel=np.full(n,np.nan); vel[2:]=sgn*(upx[2:]/upx[:-2]-1)*1e4
        disp=sgn*(upx/opx-1)*1e4; mkt=f"{asset}_{sm}"
        for i in range(n):
            t=grid[i]
            if not(5<=t<=180) or not np.isfinite(vel[i]) or not np.isfinite(disp[i]): continue
            a1=ba.get(t-1,np.nan)
            if vel[i]>=4 and 0.45<=a1<=0.97:
                vol=lvol(asset,int(sec[i]))
                z=disp[i]/(vol*np.sqrt(t)) if (vol and vol>0) else np.nan
                rec.append({"mkt":mkt,"asset":asset,"disp":disp[i],"z":z,"a1":a1,"win":win})
                break
    df=pd.DataFrame(rec)
    return df.sort_values("mkt")  # first already (break on first qualifying)

def ev(df):
    a=df.a1.values; sh=STAKE/a; fee=sh*FEE*a*(1-a)
    pnl=np.where(df.win.values==1, sh*(1-a)-fee, -STAKE-fee)
    return len(df), df.win.mean(), pnl.mean()

print("Building backtest entries (vel>=4, ask 0.45-0.97, first/market)...")
dfs=[]
for p in [DATA+r"\ll_booksec_may.parquet", DATA+r"\ll_booksec.parquet"]:
    dfs.append(build(p))
alldf=pd.concat(dfs,ignore_index=True)
alldf=alldf[(alldf.disp>=0)]
print("entries:",len(alldf))

print("\n=== RAW DISPLACEMENT x ASSET (backtest) ===")
for asset in ["btc","eth"]:
    print(f"\n{asset}:")
    d=alldf[alldf.asset==asset]
    d=d.assign(b=pd.cut(d.disp,[0,3,4,6,8,12,1e9]))
    for b,g in d.groupby("b",observed=True):
        if len(g)>=10:
            n,w,e=ev(g); print(f"  disp {str(b):>10}: n={n:>4} win={w:.2f} EV=${e:+.3f}")

print("\n=== VOL-NORMALIZED z=disp/(vol*sqrt(ttl)) x ASSET (backtest) ===")
zz=alldf[alldf.z.notna()]
for asset in ["btc","eth"]:
    print(f"\n{asset}:")
    d=zz[zz.asset==asset]
    d=d.assign(b=pd.cut(d.z,[-1,0.3,0.6,1.0,1.5,2.5,1e9]))
    for b,g in d.groupby("b",observed=True):
        if len(g)>=10:
            n,w,e=ev(g); print(f"  z {str(b):>11}: n={n:>4} win={w:.2f} EV=${e:+.3f}")
