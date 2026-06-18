"""Test the 'expected-edge' formula as a unified gate, on the backtest.
edge = Phi(z) - ask - fee, where z = disp/(vol*sqrt(ttl)), Phi = std normal CDF.
If higher predicted-edge buckets have higher REALIZED EV, the formula works and
can replace the vel/z/ask filter stack. May+June 5m, BTC+ETH pooled.
Caches entries to data/edge_entries.parquet for fast re-runs.
"""
import glob, os
import numpy as np
import pandas as pd
from scipy.stats import norm
pd.set_option("display.width", 240)
DATA = r"C:\Users\tico_\Fable\5minSnip\data"
FEE, STAKE = 0.07, 10.0
CACHE = DATA + r"\edge_entries.parquet"

def load_sym(s):
    fs = sorted(glob.glob(os.path.join(DATA, "binance", f"{s}_*.parquet")))
    df = pd.concat([pd.read_parquet(f) for f in fs], ignore_index=True)
    return (df.drop_duplicates("open_time").sort_values("open_time").open_time.values.astype(np.int64),
            df.close.values.astype(float))
B = {"btc": load_sym("BTCUSDT"), "eth": load_sym("ETHUSDT")}

def px(a, sec):
    ot, cl = B[a]; i = np.searchsorted(ot, sec*1000, side="right")-1
    v = np.where(i >= 0, cl[np.clip(i,0,len(cl)-1)], np.nan)
    g = sec*1000 - ot[np.clip(i,0,len(cl)-1)]
    return np.where((i>=0)&(g<=5000), v, np.nan)

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
            if vel[i]>=2 and 0.30<=a1<=0.99 and disp[i]>0:   # loose baseline
                vol=lvol(asset,int(sec[i]))
                if vol and vol>0:
                    z=disp[i]/(vol*np.sqrt(t))
                    rec.append({"mkt":mkt,"asset":asset,"ttl":t,"vel":vel[i],
                                "disp":disp[i],"vol":vol,"z":z,"ask":a1,"win":win})
                break
    return pd.DataFrame(rec)

if os.path.exists(CACHE):
    e = pd.read_parquet(CACHE)
    print("loaded cached entries:", len(e))
else:
    print("building (loose baseline vel>=2)...")
    e = pd.concat([build(DATA+r"\ll_booksec_may.parquet"),
                   build(DATA+r"\ll_booksec.parquet")], ignore_index=True)
    e.to_parquet(CACHE)
    print("entries:", len(e))

e["pred"] = norm.cdf(e.z)                       # model fair win prob
e["fee"] = FEE*e.ask*(1-e.ask)
e["edge"] = e.pred - e.ask - e.fee              # expected per-share edge
sh = STAKE/e.ask.values
e["pnl"] = np.where(e.win==1, sh*(1-e.ask)-sh*e.fee, -STAKE-sh*e.fee)

print("\n=== CALIBRATION: model Phi(z) vs realized win ===")
e["pb"] = pd.cut(e.pred,[0,.55,.65,.75,.85,.92,.97,1.001])
print(e.groupby("pb",observed=True).agg(n=("win","size"),pred=("pred","mean"),
      realized=("win","mean")).to_string())

print("\n=== EDGE-GATE: realized EV by predicted-edge bucket ===")
e["eb"]=pd.cut(e.edge,[-1,-.05,0,.03,.06,.10,.20,1])
g=e.groupby("eb",observed=True).agg(n=("win","size"),ask=("ask","mean"),
   win=("win","mean"),ev=("pnl","mean"))
print(g.to_string())

print("\n=== GATE COMPARISON ($10 stake, all entries) ===")
def stats(d,label):
    if len(d)<10: return
    print(f"  {label:<28} n={len(d):>4} win={d.win.mean():.3f} "
          f"ev/trade=${d.pnl.mean():+.3f} total=${d.pnl.sum():+.1f}")
stats(e, "baseline (vel>=2,disp>0)")
stats(e[(e.vel>=4)&(e.z>=0.5)&(e.ask<=0.90)], "current live gate (vel4,z.5,ask<=.9)")
for m in [0.02,0.04,0.06,0.08]:
    stats(e[e.edge>=m], f"edge>={m}")
for m in [0.04,0.06]:
    stats(e[(e.edge>=m)&(e.ask<=0.90)], f"edge>={m} & ask<=0.90")
