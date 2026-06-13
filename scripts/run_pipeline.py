"""Run reconstruction -> sweep -> book analysis sequentially."""
import subprocess
import sys
import time

S = r"C:\Users\tico_\Fable\5minSnip\scripts"
steps = ["reconstruct_books.py", "analyze_sweep.py", "analyze_book.py"]
for s in steps:
    t0 = time.time()
    print(f"=== {s} ===", flush=True)
    rc = subprocess.call([sys.executable, f"{S}\\{s}"])
    print(f"=== {s} exit={rc} in {time.time()-t0:.0f}s ===", flush=True)
    if rc != 0:
        sys.exit(rc)
print("PIPELINE COMPLETE")
