"""Join v2_intent_open records to independent Binance-kline window outcomes.

Usage: python scripts/build_entry_table.py <cache_dir> <klines_parquet> <out_parquet>

Population note: this is the INTENT population (every entry the gate cleared).
In paper every intent fills, so intents == fills here; on live exports the
rolled-back / FOK-killed subset must be removed before this is a fill population.

Outcome labelling follows the project convention: a window [epoch, epoch+iv)
opens at kline[epoch].open and finishes at kline[epoch+iv-60].close, ties
resolve Up. That is a Binance proxy for a Chainlink settlement and flips on
roughly 20% of photo finishes (|move| < PHOTO_FINISH_BPS).
"""
import sys

import numpy as np
import pandas as pd

PHOTO_FINISH_BPS = 2.0
FEE_COEF = 0.07
INTERVAL_S = {"5m": 300, "15m": 900}
EXIT_PAD_S = 300  # v2 sets exit_ts_s = resolution + 300


def build(cache_dir, klines_path, out_path):
    d = pd.read_parquet(f"{cache_dir}/v2_intent_open.parquet")

    parts = d.signal_id.str.split("-", expand=True)
    d["asset"] = parts[0]
    d["signal_ts"] = parts[1].astype("int64")
    d["interval"] = parts[2]
    d["side"] = parts[3]

    d["iv_s"] = d.interval.map(INTERVAL_S)
    d["resolution"] = d.exit_ts_s - EXIT_PAD_S
    d["epoch"] = d.resolution - d.iv_s
    d["ttl_calc"] = d.resolution - d.signal_ts

    k = pd.read_parquet(klines_path)
    k["asset"] = k.symbol.str.replace("USDT", "", regex=False)
    op = k.set_index(["asset", "open_s"])["open"]
    cl = k.set_index(["asset", "open_s"])["close"]

    d["w_open"] = op.reindex(pd.MultiIndex.from_arrays([d.asset, d.epoch])).to_numpy()
    d["w_final"] = cl.reindex(
        pd.MultiIndex.from_arrays([d.asset, d.resolution - 60])
    ).to_numpy()

    d["move_bps"] = (d.w_final - d.w_open) / d.w_open * 1e4
    d["abs_move_bps"] = d.move_bps.abs()
    d["photo_finish"] = d.abs_move_bps < PHOTO_FINISH_BPS
    d["label"] = np.where(d.move_bps >= 0, "Up", "Down")  # ties -> Up
    d["won"] = (d.side.str.lower() == d.label.str.lower()).astype("boolean")
    d.loc[d.w_final.isna() | d.w_open.isna(), "won"] = pd.NA

    # Bot's realised accounting, verified exactly against pnl_recorder rows:
    #   shares = stake / fill_price;  fee = 0.07 * ask * (1-ask) * shares
    d["fee"] = FEE_COEF * d.fill_price * (1 - d.fill_price) * d.shares
    d["hold_pnl"] = (
        d.won.astype("float") * d.shares - d.stake_usd - d.fee
    )
    d["hold_ev_$1"] = d.hold_pnl / d.stake_usd

    # Model edge as the gate saw it, and the entry-side signed displacement.
    d["edge"] = d.p - d.ask - FEE_COEF * d.ask * (1 - d.ask)
    d["disp_signed"] = np.where(
        d.side.str.lower() == "up", d.disp_bps, -d.disp_bps
    )
    # Did a reverting window still finish on the side we entered?
    d["stuck"] = d.won

    d = d.sort_values("ts_s").reset_index(drop=True)
    d.to_parquet(out_path, index=False)
    return d


if __name__ == "__main__":
    df = build(sys.argv[1], sys.argv[2], sys.argv[3])
    print(f"rows={len(df)}  labelled={df.won.notna().sum()}")
    print(df.groupby("interval").agg(
        n=("won", "size"),
        wr=("won", "mean"),
        ev=("hold_ev_$1", "mean"),
        pf=("photo_finish", "mean"),
    ).round(4).to_string())
