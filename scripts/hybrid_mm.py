"""Hybrid market-making analysis built on the lead-lag.

Thesis: a passive maker resting at the BBO is adversely selected exactly when
spot moves (the 1-2s lag). A maker watching spot can CANCEL before being picked
off. Maker pays NO fee and earns (not pays) the spread.

We simulate resting quotes (join best bid AND best ask) on each token, fill by
the conservative rule "market trades THROUGH my level within H sec", hold each
acquired position to settlement (maker fee = 0), and compare:
  NAIVE      : always quoting
  DEFENDED   : cancel the side whose new inventory the 2s spot move opposes
Also estimate ROUND-TRIP spread capture (both sides fill -> earn the spread).
Reward/rebate income is additive and NOT included (no feed) -> results are a
floor.
"""
import glob
import os

import numpy as np
import pandas as pd

pd.set_option("display.width", 240)
DATA = r"C:\Users\tico_\Fable\5minSnip\data"
H = 3            # fill look-forward seconds
CANCEL = 2.0     # bps adverse 2s move that triggers cancel

def load_sym(sym):
    files = sorted(glob.glob(os.path.join(DATA, "binance", f"{sym}_*.parquet")))
    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    df = df.drop_duplicates("open_time").sort_values("open_time")
    return df.open_time.values.astype(np.int64), df.close.values.astype(float)
B = {"btc": load_sym("BTCUSDT"), "eth": load_sym("ETHUSDT")}

def px_at(asset, sec_abs):
    ot, cl = B[asset]
    idx = np.searchsorted(ot, sec_abs * 1000, side="right") - 1
    val = np.where(idx >= 0, cl[np.clip(idx, 0, len(cl) - 1)], np.nan)
    gap = sec_abs * 1000 - ot[np.clip(idx, 0, len(cl) - 1)]
    return np.where((idx >= 0) & (gap <= 5000), val, np.nan)

def run(path, label):
    bs = pd.read_parquet(path)
    bs = bs[(bs.ba < 1.0) & (bs.bb > 0) & (bs.ba >= bs.bb)]
    # accumulators
    rec = {k: [] for k in ["pnl", "sig_for_pos", "side_kind", "spread", "mid"]}
    rt_spread = []   # captured spread on round trips
    for tok, g in bs.groupby("tok", sort=False):
        g = g.sort_values("ttl", ascending=False)
        asset = g.asset.iloc[0]; side = g.side.iloc[0]
        winner = int(g.winner.iloc[0]); settle_ms = int(g.settle_ms.iloc[0])
        ttl = g.ttl.values; tmax = int(ttl.max())
        grid = np.arange(tmax, -1, -1)
        bb = pd.Series(g.bb.values, index=ttl).reindex(grid).ffill().values
        ba = pd.Series(g.ba.values, index=ttl).reindex(grid).ffill().values
        sec = (settle_ms // 1000) - grid
        upx = px_at(asset, sec); sgn = 1.0 if side == "up" else -1.0
        n = len(grid)
        sig2 = np.full(n, np.nan)
        sig2[2:] = sgn * (upx[2:] / upx[:-2] - 1.0) * 1e4   # signed toward token
        for i in range(n):
            t = grid[i]
            if not (5 <= t <= 120):
                continue
            hi = min(n, i + 1 + H)
            fut_ba = ba[i + 1:hi]
            fut_bb = bb[i + 1:hi]
            bid_fill = fut_ba.size and np.nanmin(fut_ba) < bb[i] - 1e-9  # bought @ bb
            ask_fill = fut_bb.size and np.nanmax(fut_bb) > ba[i] + 1e-9  # sold  @ ba
            s = sig2[i]
            # BID fill -> long token: pnl = winner - bb ; adverse if s<0
            if bid_fill:
                rec["pnl"].append(winner - bb[i]); rec["sig_for_pos"].append(s)
                rec["side_kind"].append("bid"); rec["spread"].append(ba[i] - bb[i])
                rec["mid"].append((ba[i] + bb[i]) / 2)
            # ASK fill -> short token: pnl = ba - winner ; adverse if s>0
            if ask_fill:
                rec["pnl"].append(ba[i] - winner); rec["sig_for_pos"].append(-s)
                rec["side_kind"].append("ask"); rec["spread"].append(ba[i] - bb[i])
                rec["mid"].append((ba[i] + bb[i]) / 2)
            if bid_fill and ask_fill:
                rt_spread.append(ba[i] - bb[i])
    d = pd.DataFrame(rec)
    print("=" * 90)
    print(f"{label}: {len(d)} maker fills (maker fee=0)")
    # adverse-selection gradient
    d["sb"] = pd.cut(d.sig_for_pos, [-1e9, -5, -2, -1, -0.3, 0.3, 1, 2, 5, 1e9])
    g = d.groupby("sb", observed=True).agg(n=("pnl", "size"),
                                           pnl_c=("pnl", lambda x: x.mean() * 100))
    print("\nMaker fill P&L (cents) by 2s spot move toward the acquired position:")
    print("(sig_for_pos<0 = spot moved AGAINST your new inventory = pick-off)")
    print(g.to_string())
    # policy comparison
    naive = d.pnl.mean() * 100
    defended = d[d.sig_for_pos > -CANCEL].pnl.mean() * 100
    avoided = (d.sig_for_pos <= -CANCEL).mean()
    print(f"\nNAIVE maker  (hold every fill to settle): {naive:+.2f}c/fill (n={len(d)})")
    print(f"DEFENDED maker (cancel if 2s spot move <= -{CANCEL}bps against new "
          f"inventory): {defended:+.2f}c/fill, fills avoided={avoided:.1%}")
    if rt_spread:
        print(f"Round-trip spread capture available: {np.mean(rt_spread)*100:.2f}c "
              f"on {len(rt_spread)} both-sides-fill instants")
    return d

run(DATA + r"\ll_booksec_may.parquet", "MAY")
run(DATA + r"\ll_booksec.parquet", "JUNE")
