"""Build one reusable feature matrix over every on-chain-resolved token.

Rather than re-scan the recorder for each hypothesis, extract everything once:
the full quote path per token, summarised into features that are all knowable at
a fixed decision time, plus the TRUE on-chain outcome.

DECISION TIME. Features are computed at `DECIDE_S` seconds before the close and
use only quotes at or before that instant. Anything later is look-ahead -- the
mistake that made an early version of the push snipe look like +52% ROI.

WHY PM-ONLY FEATURES FIRST. Binance ticks cover only part of the window
(May, Jun 18-29, Jul, and 8 sampled August days), while Polymarket quotes cover
all 76 recorded days. Starting PM-only keeps the sample large; Binance features
get joined in afterwards for the subset where both exist.

The market's own ask is included deliberately. It is the benchmark: any feature
that "predicts" the outcome but only rediscovers what the price already says is
worthless. The question is always whether something beats the ask, not whether
it correlates with the result.

Usage: python scripts/build_features.py [decide_s]
"""
import json
import os
import pickle
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from push_pm_backtest import PM, zst_lines  # noqa: E402

CY = {"5m": 300, "15m": 900, "1h": 3600, "4h": 14400}
DECIDE_S = int(sys.argv[1]) if len(sys.argv) > 1 else 60
LOOKBACKS = (15, 30, 60, 120)
OUT = f"scripts/_feat_{DECIDE_S}.pkl"


def lines(path):
    if path.endswith(".zst"):
        yield from zst_lines(path)
    else:
        with open(path, encoding="utf-8", errors="ignore") as fh:
            yield from fh


def main():
    lab = pickle.load(open("scripts/_true_labels.pkl", "rb"))
    end, span = {}, {}
    for tok, d in lab.items():
        s = CY.get(d["interval"])
        if s:
            end[tok] = d["epoch"] + s
            span[tok] = s
    qd = os.path.join(PM, "best_bid_ask")
    files = {f[:10]: os.path.join(qd, f) for f in os.listdir(qd)
             if f.endswith((".jsonl", ".jsonl.zst"))}

    rows = []
    for day in sorted(files):
        path = {}          # token -> list of (ts, bid, ask) up to the decision point
        for line in lines(files[day]):
            try:
                pl = json.loads(line)["payload"]
                tok = pl["asset_id"]
            except Exception:
                continue
            e = end.get(tok)
            if e is None:
                continue
            try:
                ts = int(pl["timestamp"]) // 1000
                bid = float(pl["best_bid"])
                ask = float(pl["best_ask"])
            except (KeyError, TypeError, ValueError):
                continue
            if ts > e - DECIDE_S:
                continue
            if ts < e - span[tok]:
                continue          # before this market's own window opened
            path.setdefault(tok, []).append((ts, bid, ask))

        n = 0
        for tok, p in path.items():
            if len(p) < 5:
                continue
            p.sort()
            t_now, bid, ask = p[-1]
            if not (0.0 < bid < ask < 1.0):
                continue
            mid = (bid + ask) / 2
            d = lab[tok]
            f = {
                "tok": tok, "won": d["won"], "asset": d["asset"],
                "interval": d["interval"], "epoch": d["epoch"], "day": day,
                "ask": ask, "bid": bid, "spread": ask - bid, "mid": mid,
                "n_quotes": len(p),
                # how far into the cycle the LAST quote sits: staleness
                "quote_age": (end[tok] - DECIDE_S) - t_now,
            }
            # momentum of the PM price itself over several lookbacks
            for lb in LOOKBACKS:
                cut = t_now - lb
                prev = None
                for ts, b, a in p:
                    if ts <= cut:
                        prev = (b + a) / 2
                    else:
                        break
                f[f"mom{lb}"] = (mid - prev) if prev is not None else 0.0
            # realised volatility of the PM mid over the whole visible path
            mids = [(b + a) / 2 for _, b, a in p]
            diffs = [abs(mids[i] - mids[i - 1]) for i in range(1, len(mids))]
            f["pm_vol"] = (sum(diffs) / len(diffs)) if diffs else 0.0
            f["pm_range"] = max(mids) - min(mids)
            f["mid_open"] = mids[0]
            f["drift"] = mid - mids[0]
            rows.append(f)
            n += 1
        print(f"  {day}: {n} tokens", flush=True)

    with open(OUT, "wb") as fh:
        pickle.dump(rows, fh)
    print(f"\n{len(rows):,} rows -> {OUT}")
    if rows:
        print(f"base win rate {sum(r['won'] for r in rows)/len(rows):.2%}")


if __name__ == "__main__":
    main()
