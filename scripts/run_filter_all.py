"""Run filter_price_change.py for all day files in parallel subprocesses."""
import glob
import os
import subprocess
import sys
import time

RAW = r"C:\Users\tico_\Fable\5minSnip\data\raw\price_change"
SCRIPT = r"C:\Users\tico_\Fable\5minSnip\scripts\filter_price_change.py"

files = sorted(glob.glob(os.path.join(RAW, "*")))
# big files first so they overlap with small ones
files.sort(key=os.path.getsize, reverse=True)
print("files:", [os.path.basename(f) for f in files], flush=True)

procs = []
for f in files:
    p = subprocess.Popen([sys.executable, SCRIPT, f])
    procs.append((f, p))

rc = 0
for f, p in procs:
    code = p.wait()
    print("EXIT", os.path.basename(f), code, flush=True)
    if code != 0:
        rc = 1
sys.exit(rc)
