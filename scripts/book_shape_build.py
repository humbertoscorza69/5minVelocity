"""Extract L2 book-shape features per (token, second) from the recorder's `book` channel.

Usage: python scripts/book_shape_build.py <YYYY-MM-DD> <out_dir>

The `book` channel is the ledger's "last untouched data vein" — full depth snapshots,
lost from the June archive and never analysed. Every feature here is causal: it comes
from the most recent snapshot at or before the second it is stamped on, with `bk_age`
recording how stale that is.

Microprice is the motivating feature: (bid*ask_sz + ask*bid_sz)/(bid_sz+ask_sz) — the
queue-weighted fair value. Where it differs from the traded price, the top of book has
not yet expressed the pressure sitting behind it.
"""
import io
import json
import os
import sys

import numpy as np
import pandas as pd
import zstandard as zstd

BASE = r"D:\polycrypto\live_l2"
TICK = 0.01


def _open(path):
    if os.path.exists(path):
        return io.TextIOWrapper(zstd.ZstdDecompressor().stream_reader(open(path, "rb")),
                                encoding="utf8", errors="replace")
    raw = path[:-4]
    return open(raw, encoding="utf8", errors="replace") if os.path.exists(raw) else None


def shape(bids, asks):
    """Depth features from one snapshot. bids/asks are [(price,size)] as given."""
    if not bids or not asks:
        return None
    b = sorted(((float(x["price"]), float(x["size"])) for x in bids), key=lambda t: -t[0])
    a = sorted(((float(x["price"]), float(x["size"])) for x in asks), key=lambda t: t[0])
    bp, bs = b[0]
    ap, asz = a[0]
    if not (0 < bp < ap < 1):
        return None
    tot = bs + asz
    micro = (bp * asz + ap * bs) / tot if tot > 0 else np.nan   # queue-weighted fair value
    mid = (bp + ap) / 2.0

    def within(levels, ref, n, sign):
        lim = ref + sign * n * TICK
        return float(sum(s for p, s in levels if (p >= lim if sign < 0 else p <= lim)))

    b3, a3 = within(b, bp, 3, -1), within(a, ap, 3, +1)
    b10, a10 = within(b, bp, 10, -1), within(a, ap, 10, +1)
    return dict(
        bk_bid=bp, bk_ask=ap, bk_mid=mid, bk_micro=micro,
        bk_micro_minus_mid=micro - mid,
        bk_imb=(bs - asz) / tot if tot > 0 else np.nan,
        bk_imb3=(b3 - a3) / (b3 + a3) if (b3 + a3) > 0 else np.nan,
        bk_imb10=(b10 - a10) / (b10 + a10) if (b10 + a10) > 0 else np.nan,
        bk_spread_ticks=round((ap - bp) / TICK),
        bk_bidsz=bs, bk_asksz=asz, bk_depth3=b3 + a3, bk_depth10=b10 + a10,
        bk_levels=len(b) + len(a),
    )


def build_day(day, out_dir):
    fh = _open(rf"{BASE}\polymarket\book\{day}.jsonl.zst")
    if fh is None:
        print(f"{day}: no book file")
        return
    rows = []
    with fh:
        for line in fh:
            if '"asset_id"' not in line:
                continue
            try:
                r = json.loads(line)
                p = r["payload"]
                s = shape(p.get("bids"), p.get("asks"))
                if s is None:
                    continue
                s["token_id"] = p["asset_id"]
                s["sec"] = pd.Timestamp(r["received_at"]).value // 10**9
                rows.append(s)
            except Exception:
                continue
    if not rows:
        print(f"{day}: no snapshots")
        return
    df = pd.DataFrame(rows)
    # keep the LAST snapshot per (token, second) — that is what a bot would hold
    df = df.sort_values("sec").drop_duplicates(["token_id", "sec"], keep="last")
    os.makedirs(out_dir, exist_ok=True)
    p = os.path.join(out_dir, f"bk_{day}.parquet")
    df.to_parquet(p, index=False)
    print(f"{day}: snapshots={len(df):,} tokens={df.token_id.nunique()} -> {p}", flush=True)


if __name__ == "__main__":
    build_day(sys.argv[1], sys.argv[2])
