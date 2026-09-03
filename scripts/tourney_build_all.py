"""Build decision tables for every recorded day. Resumable — skips days already built."""
import os
import sys
import glob
import traceback

sys.path.insert(0, os.path.dirname(__file__))
from tourney_build import build_day, BASE

if __name__ == "__main__":
    out = sys.argv[1]
    os.makedirs(out, exist_ok=True)
    days = sorted({os.path.basename(p).split(".")[0]
                   for p in glob.glob(rf"{BASE}\polymarket\best_bid_ask\*.jsonl*")})
    print(f"{len(days)} recorded days: {days[0]} .. {days[-1]}", flush=True)
    for d in days:
        if os.path.exists(os.path.join(out, f"dt_{d}.parquet")):
            print(f"{d}: cached", flush=True)
            continue
        try:
            build_day(d, out)
        except Exception:
            print(f"{d}: FAILED\n{traceback.format_exc()}", flush=True)
