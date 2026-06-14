"""Counterfactual on the live paper trades (day 1) to refine the config.
Rows: (ask_fill, slip, won, pnl). disp only known for the last 8 (from bot.log)."""
rows = [
    (0.63,0.0,1,5.614),(0.88,0.07,1,1.2796),(0.97,0.02,1,0.2883),(0.98,0.0,1,0.1901),
    (0.97,0.02,1,0.2883),(0.96,0.0,1,0.3887),(0.9,0.05,1,1.0411),(0.84,0.09,1,1.7928),
    (0.58,0.14,0,-10.294),(0.95,0.0,1,0.4913),(0.96,0.0,1,0.3887),(0.97,0.01,1,0.2883),
    (0.92,0.08,1,0.8136),(0.99,0.0,1,0.094),(0.9,0.0,1,1.0411),(0.72,0.0,0,-10.196),
    (0.98,0.01,1,0.1901),(0.89,0.19,1,1.159),(0.95,0.02,1,0.4913),(0.99,0.01,1,0.094),
    (0.9,0.11,1,1.0411),(0.93,0.08,1,0.7037),(0.89,0.04,1,1.159),(0.87,0.02,1,1.4033),
    (0.98,0.02,1,0.1901),(0.92,0.02,1,0.8136),(0.97,0.0,1,0.2883),(0.88,0.31,1,1.2796),
    (0.8,0.06,0,-10.14),(0.47,0.03,1,10.9056),(0.97,0.0,1,0.2883),(0.98,0.0,1,0.1901),
    (0.94,0.02,1,0.5963),(0.73,0.11,1,3.5096),(0.39,0.01,0,-10.427),(0.59,0.04,0,-10.287),
]

def run(ask_lo, ask_hi, max_slip):
    sel = [r for r in rows if ask_lo <= r[0] <= ask_hi and r[1] <= max_slip + 1e-9]
    n = len(sel)
    if not n:
        return None
    w = sum(r[2] for r in sel)
    p = sum(r[3] for r in sel)
    return n, w / n, p

print("baseline (current would_trade: ask<=0.97, slip<=0.02):")
print("  ", run(0.30, 0.97, 0.02))
print("\nsweep ask_hi x max_slip (ask_lo=0.40):")
print(f"  {'ask_hi':>7}{'slip<=':>8}{'n':>4}{'win':>7}{'pnl':>9}")
for ah in [0.78, 0.85, 0.92, 0.97]:
    for ms in [0.02, 0.04, 0.06, 0.10, 1.0]:
        r = run(0.40, ah, ms)
        if r:
            print(f"  {ah:>7}{ms:>8}{r[0]:>4}{r[1]:>7.2f}{r[2]:>+9.2f}")

# disp filter demo on the 8 rows where disp is known (from bot.log)
disp_rows = [  # (disp, won, pnl)
    (1.1,0,-10.427),(1.4,0,-10.14),(1.4,0,-10.287),
    (3.5,1,10.9056),(8.1,1,0.1901),(10.5,1,0.2883),(11.5,1,0.5963),(11.9,1,3.5096)]
print("\ndisp filter on the 8 disp-known trades:")
for lo in [1, 2, 3]:
    sel = [r for r in disp_rows if r[0] >= lo]
    p = sum(r[2] for r in sel)
    w = sum(r[1] for r in sel) / len(sel) if sel else 0
    print(f"  disp>={lo}: n={len(sel)} win={w:.2f} pnl={p:+.2f}")
