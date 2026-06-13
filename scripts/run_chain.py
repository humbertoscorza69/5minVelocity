"""Run the full analysis chain sequentially with logging."""
import subprocess
import sys
import time

S = r"C:\Users\tico_\Fable\5minSnip\scripts"
steps = [
    "reconstruct_books.py",     # June checkpoints (lean, reported BBO)
    "analyze_sweep.py",         # June favorites + June-only sweep
    "may_checkpoints.py",       # May BBO checkpoints
    "build_winner_labels.py",   # binance labels + June validation + May winners
    "combined_sweep.py",        # IS/OOS sweep, calibration, maker
    "analyze_book.py",          # REST depth, staleness bias, imbalance
]
for s in steps:
    t0 = time.time()
    print(f"=== START {s} ===", flush=True)
    rc = subprocess.call([sys.executable, f"{S}\\{s}"])
    print(f"=== END {s} exit={rc} {time.time()-t0:.0f}s ===", flush=True)
    if rc != 0:
        sys.exit(rc)
print("CHAIN COMPLETE")
