"""Download Binance SPOT aggTrades (tick stream) from data.binance.vision.
Used to study tick-driven entry vs waiting for the 1s bar close. Days chosen to
overlap the recorder period (Jun18-29) so ticks can be cross-referenced against
the bot's actual entry timestamps. Files are large (~300-600MB zipped/day/symbol).
"""
import os, urllib.request, time
OUT=r"D:\polycrypto\aggtrades"; os.makedirs(OUT, exist_ok=True)
BASE="https://data.binance.vision/data/spot/daily/aggTrades"
SYMS=["BTCUSDT","ETHUSDT"]
DAYS=[f"2026-06-{d:02d}" for d in range(18,29)]  # full recorder span Jun18-28
def dl(sym,day):
    fn=f"{sym}-aggTrades-{day}.zip"; url=f"{BASE}/{sym}/{fn}"; out=os.path.join(OUT,fn)
    if os.path.exists(out) and os.path.getsize(out)>0:
        print("  exists",fn,f"{os.path.getsize(out)/1e6:.0f}MB"); return
    try:
        t0=time.time()
        with urllib.request.urlopen(url,timeout=60) as r, open(out,"wb") as f:
            total=0
            while True:
                chunk=r.read(1<<20)
                if not chunk: break
                f.write(chunk); total+=len(chunk)
        print(f"  OK {fn} {total/1e6:.0f}MB in {time.time()-t0:.0f}s")
    except urllib.error.HTTPError as e:
        print(f"  HTTP {e.code} for {fn} (not available yet?)")
    except Exception as e:
        print(f"  FAIL {fn}: {str(e)[:80]}")
def dl_monthly(sym,ym):
    fn=f"{sym}-aggTrades-{ym}.zip"; url=f"https://data.binance.vision/data/spot/monthly/aggTrades/{sym}/{fn}"; out=os.path.join(OUT,fn)
    if os.path.exists(out) and os.path.getsize(out)>0:
        print("  exists",fn,f"{os.path.getsize(out)/1e6:.0f}MB"); return
    try:
        t0=time.time()
        with urllib.request.urlopen(url,timeout=120) as r, open(out,"wb") as f:
            total=0
            while True:
                c=r.read(1<<20)
                if not c: break
                f.write(c); total+=len(c)
        print(f"  OK monthly {fn} {total/1e6:.0f}MB in {time.time()-t0:.0f}s")
    except urllib.error.HTTPError as e: print(f"  HTTP {e.code} monthly {fn}")
    except Exception as e: print(f"  FAIL monthly {fn}: {str(e)[:80]}")
for sym in SYMS:
    print(f"--- monthly {sym} 2026-05 / 2026-06 ---")
    dl_monthly(sym,"2026-05"); dl_monthly(sym,"2026-06")
    for day in DAYS:
        print(f"downloading {sym} {day} ...")
        dl(sym,day)
print("done ->",OUT)
