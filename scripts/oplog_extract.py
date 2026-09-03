"""Stream a bot oplog.jsonl export into per-kind parquet caches.

Usage: python scripts/oplog_extract.py <export_dir> <out_dir>

The oplog is lifetime-cumulative (~650MB) and the v2_intent_open schema has
grown over time (burst_bps, tick_age_s, stake_mult, stake_raw, asleep,
mid_move_3s were added by later orders), so this flattens data{} and lets the
caller slice by ts.
"""
import json
import os
import sys
import collections

import pandas as pd

KINDS = {
    "v2_intent_open",
    "pnl_recorder_recorded",
    "v2_recal_update",
    "inval_stop",
    "inval_stop_suppressed",
    "paper_close",
    "paper_open",
    "decision_loop_start",
    "v2_guard_blocked_open",
    "v2_book_moved_skip",
    "v2_reentry_side_off",
    "canary_amber",
    "canary_resume",
    "canary_red",
    "guard_halt",
    "live_open_posted",
    "live_open_rolled_back",
    "live_open_slippage_observed",
    "sizing_clipped",
    "v2_photofinish_book_deferred",
    "settlement_rest_fallback",
    "pnl_corrected",
}


def main(export_dir, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    rows = collections.defaultdict(list)
    counts = collections.Counter()
    path = os.path.join(export_dir, "oplog.jsonl")
    with open(path, encoding="utf8", errors="replace") as fh:
        for line in fh:
            if '"kind"' not in line:
                continue
            try:
                rec = json.loads(line)
            except Exception:
                continue
            kind = rec.get("kind")
            counts[kind] += 1
            if kind not in KINDS:
                continue
            data = rec.get("data") or {}
            if not isinstance(data, dict):
                data = {"value": data}
            flat = dict(data)
            flat["ts_ms"] = rec.get("ts_ms")
            rows[kind].append(flat)

    for kind, recs in rows.items():
        df = pd.DataFrame(recs)
        if "ts_ms" in df.columns:
            df["ts_s"] = df["ts_ms"] / 1000.0
            df = df.sort_values("ts_s")
        out = os.path.join(out_dir, f"{kind}.parquet")
        df.to_parquet(out, index=False)
        print(f"{kind:38s} n={len(df):>8d} -> {out}")
    return counts


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
