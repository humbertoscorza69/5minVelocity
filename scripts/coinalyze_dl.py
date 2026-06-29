"""Download Coinalyze liquidation + open-interest history (BTC/ETH Binance perp)
for the live period, save to data/ (gitignored). Key from env CK.
"""
import os, time, json, urllib.request, urllib.parse, datetime as dt
import pandas as pd
KEY=os.environ["CK"]; BASE="https://api.coinalyze.net/v1/"
SYMS={"btc":"BTCUSDT_PERP.A","eth":"ETHUSDT_PERP.A"}
DATA=r"C:\Users\tico_\Fable\5minSnip\data"
def epoch(s): return int(dt.datetime.strptime(s,"%Y-%m-%d").replace(tzinfo=dt.timezone.utc).timestamp())
def get(ep,params):
    url=BASE+ep+"?"+urllib.parse.urlencode(params)
    req=urllib.request.Request(url,headers={"api_key":KEY})
    for attempt in range(4):
        try:
            with urllib.request.urlopen(req,timeout=40) as r: return json.load(r)
        except Exception as e:
            print("  retry",attempt,str(e)[:80]); time.sleep(3)
    return []
# pull in 2-day chunks (1min interval) over the live period + a May slice for model training
WINDOWS=[("2026-05-01","2026-05-24"),("2026-06-13","2026-06-30")]
for ep,short in [("liquidation-history","liq"),("open-interest-history","oi")]:
    rows=[]
    for asset,sym in SYMS.items():
        for w0,w1 in WINDOWS:
            f=epoch(w0); tend=epoch(w1)
            while f<tend:
                t=min(f+2*86400,tend)
                d=get(ep,{"symbols":sym,"interval":"1min","from":f,"to":t,"convert_to_usd":"true"})
                for s in d:
                    for h in s.get("history",[]):
                        h["asset"]=asset; rows.append(h)
                f=t; time.sleep(1.7)   # ~35 calls/min < 40 limit
    df=pd.DataFrame(rows)
    print(short, "rows=",len(df), "cols=",list(df.columns) if len(df) else "EMPTY")
    if len(df):
        df=df.drop_duplicates(subset=[c for c in df.columns if c!='asset']+['asset']) if 'asset' in df else df
        df.to_parquet(os.path.join(DATA,f"coinalyze_{short}.parquet"))
        print("  saved", df.asset.value_counts().to_dict(), "t-range", df.t.min(),"-",df.t.max())
