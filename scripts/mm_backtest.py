"""Realistic market-making backtest from REAL trade prints (June 5m).

Maker joins BBO each second. Fill model from actual taker prints:
  resting BID @ bb fills if a taker SELL prints at price <= bb that second
  resting ASK @ ba fills if a taker BUY  prints at price >= ba that second
Maker pays NO fee. P&L is exact under inventory (linear):
  bid fill -> +(winner - bb) ;  ask fill -> +(ba - winner)
Matched (both sides fill same second) -> earns the spread regardless of outcome.

Policies:
  NAIVE     : always quoting both sides
  DEFENDED  : cancel bid if 2s spot move <= -C (falling); cancel ask if >= +C (rising)
IS/OOS: June split by date (early vs late).
Rewards/rebates NOT included -> results are a floor.
"""
import glob
import json
import os

import numpy as np
import pandas as pd

pd.set_option("display.width", 240)
DATA = r"C:\Users\tico_\Fable\5minSnip\data"
C = 2.0   # cancel threshold bps (2s spot move)

# ---- spot ----
def load_sym(sym):
    files = sorted(glob.glob(os.path.join(DATA, "binance", f"{sym}_*.parquet")))
    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    df = df.drop_duplicates("open_time").sort_values("open_time")
    return df.open_time.values.astype(np.int64), df.close.values.astype(float)
B = {"btc": load_sym("BTCUSDT"), "eth": load_sym("ETHUSDT")}

def px(asset, sec):
    ot, cl = B[asset]
    i = np.searchsorted(ot, sec * 1000, side="right") - 1
    v = np.where(i >= 0, cl[np.clip(i, 0, len(cl) - 1)], np.nan)
    gap = sec * 1000 - ot[np.clip(i, 0, len(cl) - 1)]
    return np.where((i >= 0) & (gap <= 5000), v, np.nan)

# ---- book (per tok per second) + token_id map ----
bs = pd.read_parquet(DATA + r"\ll_booksec.parquet")
bs = bs[(bs.ba < 1.0) & (bs.bb > 0) & (bs.ba >= bs.bb)].copy()
tokidx = json.load(open(DATA + r"\filtered\token_index.json"))
idx2tid = {v: k for k, v in tokidx.items()}
bs["token_id"] = bs.tok.map(idx2tid)
bs["sec"] = bs.settle_ms // 1000 - bs.ttl
# spot vel2 signed toward this token's side
bs["sgn"] = np.where(bs.side == "up", 1.0, -1.0)
for a in ("btc", "eth"):
    m = bs.asset == a
    bs.loc[m, "p0"] = px(a, bs.loc[m, "sec"].values)
    bs.loc[m, "p2"] = px(a, bs.loc[m, "sec"].values - 2)
bs["vel2"] = bs.sgn * (bs.p0 / bs.p2 - 1) * 1e4

# ---- trades ----
tr = pd.read_parquet(DATA + r"\trades_june5m.parquet")
tr = tr.rename(columns={"asset": "token_id"})
tr["ttl"] = None
# join trades to book by (token_id, sec); aggregate per token_id+sec
g = tr.groupby(["token_id", "ts"])
agg = g.agg(min_sell=("price", lambda s: np.nan),  # placeholder
            ).reset_index()
# compute per second: min SELL price, max BUY price
tr_sell = tr[tr.side == "SELL"].groupby(["token_id", "ts"]).price.min().rename("min_sell")
tr_buy = tr[tr.side == "BUY"].groupby(["token_id", "ts"]).price.max().rename("max_buy")
sell_sz = tr[tr.side == "SELL"].groupby(["token_id", "ts"]).size().rename("n_sell")
buy_sz = tr[tr.side == "BUY"].groupby(["token_id", "ts"]).size().rename("n_buy")
flow = pd.concat([tr_sell, tr_buy, sell_sz, buy_sz], axis=1).reset_index()
flow = flow.rename(columns={"ts": "sec"})

d = bs.merge(flow, on=["token_id", "sec"], how="left")
d["day"] = pd.to_datetime(d.settle_ms, unit="ms").dt.strftime("%m-%d")
d = d[(d.ttl >= 5) & (d.ttl <= 120)]

# fills
eps = 1e-9
d["bid_fill"] = d.min_sell.notna() & (d.min_sell <= d.bb + eps)
d["ask_fill"] = d.max_buy.notna() & (d.max_buy >= d.ba - eps)
d["bid_pnl"] = d.winner - d.bb         # if bid filled (bought)
d["ask_pnl"] = d.ba - d.winner         # if ask filled (sold)

def score(df, label):
    bid = df[df.bid_fill]
    ask = df[df.ask_fill]
    # defended: cancel bid if falling (vel2<=-C); cancel ask if rising (vel2>=+C)
    bid_def = bid[bid.vel2 > -C]
    ask_def = ask[ask.vel2 < C]
    matched = df[df.bid_fill & df.ask_fill]
    n_fills = len(bid) + len(ask)
    naive = (bid.bid_pnl.sum() + ask.ask_pnl.sum())
    defend = (bid_def.bid_pnl.sum() + ask_def.ask_pnl.sum())
    print(f"\n{label}: bid_fills={len(bid)} ask_fills={len(ask)} "
          f"matched_secs={len(matched)}")
    print(f"  NAIVE   : {naive/ n_fills*100:+.2f}c/fill over {n_fills} fills "
          f"(bid {bid.bid_pnl.mean()*100:+.2f}c, ask {ask.ask_pnl.mean()*100:+.2f}c)")
    nd = len(bid_def) + len(ask_def)
    print(f"  DEFENDED: {defend/ nd*100:+.2f}c/fill over {nd} fills "
          f"(cancelled {n_fills-nd}, {100*(n_fills-nd)/n_fills:.0f}%) "
          f"[bid {bid_def.bid_pnl.mean()*100:+.2f}c, ask {ask_def.ask_pnl.mean()*100:+.2f}c]")
    print(f"  matched-pair spread capture: {(matched.ba-matched.bb).mean()*100:.2f}c "
          f"x {len(matched)} = realized on benign flow")
    # adverse-selection gradient (bid side)
    bb_ = bid.assign(vb=pd.cut(bid.vel2, [-1e9,-5,-2,-0.5,0.5,2,1e9]))
    gg = bb_.groupby("vb", observed=True).bid_pnl.agg(["size", "mean"])
    print("  bid-fill P&L by spot vel2 (adverse<0):",
          {str(k): f"{v['mean']*100:+.1f}c/n{int(v['size'])}" for k, v in gg.iterrows()})

print("=" * 80)
print("REAL-TRADE MARKET-MAKING BACKTEST (June 5m, maker fee=0, rewards excluded)")
days = sorted(d.day.unique())
cut = days[len(days) // 2]
score(d, "ALL JUNE")
score(d[d.day < cut], f"IS (June < {cut})")
score(d[d.day >= cut], f"OOS (June >= {cut})")
