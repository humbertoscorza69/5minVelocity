"""Order book behavior near settlement.

Sources of truth:
  - restbook.parquet  : full REST depth snapshots (~72s cadence per token)
  - checkpoints.parquet: reported best bid/ask at offsets (event-embedded)

Outputs:
  data/book_depth_by_ttl.csv  depth/spread distributions by time-to-settle
  data/staleness_bias.csv     REST best vs last event-embedded best
  data/imbalance_results.csv  favorite book imbalance vs outcome (REST-based)
"""
import json

import duckdb
import numpy as np
import pandas as pd

DATA = r"C:\Users\tico_\Fable\5minSnip\data"

rb = pd.read_parquet(DATA + r"\restbook.parquet")
with open(DATA + r"\tokens_updown.json") as f:
    tokmeta = json.load(f)
with open(DATA + r"\filtered\token_index.json") as f:
    tokidx = json.load(f)
idx2 = {i: tokmeta[t] for t, i in tokidx.items() if t in tokmeta}

meta = pd.DataFrame.from_dict(idx2, orient="index")
meta.index.name = "tok"
meta = meta.reset_index()[["tok", "asset", "interval", "settle_ts", "winner", "outcome"]]
rb = rb.merge(meta, on="tok", how="inner")
rb["ttl_s"] = rb.settle_ts - rb.ts / 1000.0
rb["mid"] = (rb.bp0 + rb.ap0) / 2
rb["spread"] = rb.ap0 - rb.bp0
rb["series"] = rb.asset + "-" + rb.interval

life = rb.interval.map({"5m": 300, "15m": 900})
rb = rb[(rb.ttl_s >= 0) & (rb.ttl_s <= life + 600)]

# ask depth in shares within bands of best ask (true REST depth)
for c in range(1, 10):
    rb[f"a_within_{c}"] = 0.0
ap = rb[[f"ap{i}" for i in range(10)]].values
asz = rb[[f"as{i}" for i in range(10)]].values
best = ap[:, [0]]
for band, col in [(0.0, "ask_at_best"), (0.01, "ask_1c"), (0.02, "ask_2c"),
                  (0.05, "ask_5c")]:
    mask = (ap <= best + band + 1e-9)
    rb[col] = np.where(np.isnan(asz), 0, asz * mask).sum(axis=1)

bins = [0, 5, 10, 15, 30, 45, 60, 90, 120, 180, 300, 900]
rb["ttl_bin"] = pd.cut(rb.ttl_s, bins, right=True)

rows = []
for (series, b), g in rb.groupby(["series", "ttl_bin"], observed=True):
    # favorite-side stats: book whose mid >= 0.5 (the side you'd buy)
    f = g[g["mid"] >= 0.5]
    if not len(f):
        continue
    rows.append({
        "series": series, "ttl_bin": str(b), "n_books": len(f),
        "spread_med": f.spread.median(), "spread_p90": f.spread.quantile(0.9),
        "ask_at_best_med": f.ask_at_best.median(),
        "ask_1c_med": f.ask_1c.median(), "ask_2c_med": f.ask_2c.median(),
        "ask_5c_med": f.ask_5c.median(),
        "ask_at_best_p10": f.ask_at_best.quantile(0.10),
        "ask_5c_p10": f.ask_5c.quantile(0.10),
        "ask_total_usd_med": f.ask_total_usd.median(),
        "bid_total_usd_med": f.bid_total_usd.median(),
        "no_ask_frac": float(f.ap0.isna().mean()),
        # high-confidence favorites only (mid>=0.9)
        "n_hc": int((f["mid"] >= 0.9).sum()),
        "hc_ask_at_best_med": f.loc[f["mid"] >= 0.9, "ask_at_best"].median(),
        "hc_ask_2c_med": f.loc[f["mid"] >= 0.9, "ask_2c"].median(),
        "hc_spread_med": f.loc[f["mid"] >= 0.9, "spread"].median(),
    })
pd.DataFrame(rows).to_csv(DATA + r"\book_depth_by_ttl.csv", index=False)
print("depth by ttl saved,", len(rows), "rows")

# ---- staleness bias: REST best vs last event-embedded best before snapshot ----
P = DATA.replace("\\", "/")
con = duckdb.connect()
con.execute("SET threads=8")
con.execute("SET memory_limit='10GB'")
st = con.execute(f"""
WITH snaps AS (
  SELECT tok, ts, bp0, ap0 FROM read_parquet('{P}/restbook.parquet')
  WHERE ap0 IS NOT NULL AND bp0 IS NOT NULL
)
SELECT s.tok, s.ts, s.ap0, s.bp0, e.ba, e.bb, s.ts - e.ts AS age_ms
FROM snaps s ASOF JOIN read_parquet('{P}/events_sorted.parquet') e
  ON s.tok = e.tok AND e.ts <= s.ts
""").df()
st["d_ask"] = (st.ap0 - st.ba).round(3)   # >0: true ask worse(higher) than embedded
st["d_bid"] = (st.bp0 - st.bb).round(3)
st = st.merge(meta, on="tok", how="inner")
st["ttl_s"] = st.settle_ts - st.ts / 1000.0
st["ttl_bin"] = pd.cut(st.ttl_s, bins, right=True)
out = []
for b, g in st.groupby("ttl_bin", observed=True):
    out.append({
        "ttl_bin": str(b), "n": len(g),
        "ask_exact_frac": float((g.d_ask.abs() < 5e-4).mean()),
        "ask_worse_frac": float((g.d_ask > 5e-4).mean()),
        "ask_better_frac": float((g.d_ask < -5e-4).mean()),
        "d_ask_mean": float(g.d_ask.mean()), "d_ask_p90": float(g.d_ask.quantile(0.9)),
        "d_ask_med_when_worse": float(g.loc[g.d_ask > 5e-4, "d_ask"].median()) if (g.d_ask > 5e-4).any() else 0.0,
        "age_ms_med": float(g.age_ms.median()),
    })
pd.DataFrame(out).to_csv(DATA + r"\staleness_bias.csv", index=False)
print("staleness bias saved")

# ---- imbalance vs outcome (REST-based, favorite books) ----
rows2 = []
fb = rb[(rb["mid"] >= 0.8)].copy()
fb["imb"] = (fb.bid_total_usd - fb.ask_total_usd) / (fb.bid_total_usd + fb.ask_total_usd + 1e-9)
for (series, b), g in fb.groupby(["series", "ttl_bin"], observed=True):
    if len(g) < 60:
        continue
    med = g.imb.median()
    hi = g[g.imb > med]
    lo = g[g.imb <= med]
    rows2.append({"series": series, "ttl_bin": str(b), "n": len(g),
                  "wr_high_imb": float(hi.winner.mean()),
                  "wr_low_imb": float(lo.winner.mean())})
pd.DataFrame(rows2).to_csv(DATA + r"\imbalance_results.csv", index=False)
print("imbalance saved")
