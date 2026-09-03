"""Binance-derived features for every PM market we can align, at a fixed decision time.

WHY BINANCE AND NOT MORE PM. The PM feature matrix showed nothing beyond the
price, which is close to tautological: the book already aggregates what PM
participants know, and the apparent "residual" turned out to be the spread
(mid_up + mid_dn = 1.0195, not 1.000). Any genuine edge must be information the
PM book does not yet contain -- and the only such source we have is the
underlying tape.

The known edge (verified on-chain, +8 to +9pp per z-bucket through week 32) came
from exactly here and died in W33. So this rebuilds the feature side properly and
lets a model search the space, rather than us hand-picking z.

EVERY FEATURE IS CAUSAL. Bin i of the cycle is only readable once the decision
time is past (i+1)*BIN_S. The decision instant is DECIDE_S seconds before the
close, and nothing after it is touched.

Usage: python scripts/build_binance_feat.py [decide_s]
"""
import io
import os
import pickle
import statistics
import sys
import zipfile
from collections import defaultdict

AGG = r"D:\polycrypto\aggtrades"
BIN_S = 10
DECIDE_S = int(sys.argv[1]) if len(sys.argv) > 1 else 60
OUT = f"scripts/_bnfeat_{DECIDE_S}.pkl"
SYMS = {"BTC": "BTCUSDT", "ETH": "ETHUSDT"}


def scan(fn):
    """One pass over a zip -> per-second last price and signed taker flow."""
    px, flow = {}, defaultdict(float)
    z = zipfile.ZipFile(os.path.join(AGG, fn))
    with z.open(z.namelist()[0]) as fh:
        for line in io.TextIOWrapper(fh, newline=""):
            f = line.split(",")
            if len(f) < 7:
                continue
            try:
                p = float(f[1]); q = float(f[2]); ts = int(f[5])
            except ValueError:
                continue
            s = ts // 1_000_000
            px[s] = p
            flow[s] += -p * q if f[6][0] in "Tt1" else p * q
    return px, flow


def feats_for(px, flow, open_s, decide_s, span):
    """Features knowable at `decide_s` for a window that opened at `open_s`."""
    o = None
    for s in range(open_s, open_s + 30):
        if s in px:
            o = px[s]; break
    cur = None
    for s in range(decide_s, decide_s - 30, -1):
        if s in px:
            cur = px[s]; break
    if not o or not cur or o <= 0:
        return None
    series = [px[s] for s in range(open_s, decide_s + 1) if s in px]
    if len(series) < 20:
        return None
    rets = [ (series[i]/series[i-1] - 1) for i in range(1, len(series)) if series[i-1] > 0]
    vol = statistics.pstdev(rets) * 1e4 if len(rets) > 2 else 0.0
    disp = (cur / o - 1) * 1e4
    ttl = (open_s + span) - decide_s
    f = {
        "disp_bps": disp,
        "vol_bps": vol,
        "z": disp / (vol * (ttl ** 0.5)) if vol > 0 and ttl > 0 else 0.0,
        "ttl_s": ttl,
        "rng_bps": (max(series) - min(series)) / o * 1e4,
        "pos_in_rng": ((cur - min(series)) / (max(series) - min(series))
                       if max(series) > min(series) else 0.5),
    }
    # momentum at several horizons, and signed order-flow imbalance
    for h in (10, 30, 60, 120):
        prev = None
        for s in range(decide_s - h, decide_s - h - 20, -1):
            if s in px:
                prev = px[s]; break
        f[f"mom{h}"] = ((cur / prev - 1) * 1e4) if prev else 0.0
        fl = sum(flow.get(s, 0.0) for s in range(decide_s - h, decide_s + 1))
        tot = sum(abs(flow.get(s, 0.0)) for s in range(decide_s - h, decide_s + 1))
        f[f"ofi{h}"] = (fl / tot) if tot > 0 else 0.0
        f[f"flow{h}"] = fl
    # the paper's PushIntensity denominator: typical per-bin flow in the body
    body = [abs(sum(flow.get(s, 0.0) for s in range(open_s + b*BIN_S, open_s + (b+1)*BIN_S)))
            for b in range(0, min(25, (decide_s - open_s)//BIN_S))]
    den = statistics.median(body) if body else 0.0
    late = abs(sum(flow.get(s, 0.0) for s in range(decide_s - 30, decide_s + 1)))
    f["push_intensity"] = (late / den) if den > 0 else 0.0
    return f


def main():
    feat = pickle.load(open(f"scripts/_feat_{DECIDE_S}.pkl", "rb"))
    want = defaultdict(list)     # (asset, day) -> rows
    for r in feat:
        if r["asset"] in SYMS:
            want[(r["asset"], r["day"])].append(r)
    have = {f for f in os.listdir(AGG) if f.endswith(".zip")}
    CY = {"5m": 300, "15m": 900, "1h": 3600, "4h": 14400}

    # Group rows by the FILE that serves them and scan each file ONCE. The first
    # version resolved the monthly fallback per day, which re-read a 361 MB zip
    # thirty-one times and never finished.
    byfile = defaultdict(list)
    for (asset, day), rows in want.items():
        sym = SYMS[asset]
        fn = f"{sym}-aggTrades-{day}.zip"
        if fn not in have:
            fn = f"{sym}-aggTrades-{day[:7]}.zip"
            if fn not in have:
                continue
        byfile[fn].extend(rows)

    out = {}
    for i, (fn, rows) in enumerate(sorted(byfile.items()), 1):
        px, flow = scan(fn)
        n = 0
        for r in rows:
            span = CY.get(r["interval"])
            if not span:
                continue
            d = feats_for(px, flow, r["epoch"], r["epoch"] + span - DECIDE_S, span)
            if d:
                out[r["tok"]] = d
                n += 1
        print(f"  [{i}/{len(byfile)}] {fn}: {n}/{len(rows)} rows", flush=True)
        del px, flow

    with open(OUT, "wb") as fh:
        pickle.dump(out, fh)
    print(f"\n{len(out):,} tokens with Binance features -> {OUT}")


if __name__ == "__main__":
    main()
