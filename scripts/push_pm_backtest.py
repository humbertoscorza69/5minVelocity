"""Underdog-snipe backtest against REAL Polymarket quotes and REAL resolutions.

push_study.py established that the real-time push detector predicts flips on the
Binance tape. It could not price the trade: it used a MODELLED ask (the base flip
rate) because no Polymarket quotes were in that dataset. That is the assumption
that decides whether the strategy is real, because the paper says makers shade the
settlement-window quote precisely against this risk -- so the true ask may already
eat the edge.

This joins three sources and removes the assumption:
  * Binance aggTrades  -> PushIntensity / real-time detector / margin at T-50s
  * PM `markets`       -> epoch, up/down token ids, and the REAL resolved_outcome
                          (ground truth: the paper notes Binance and the Chainlink
                          resolution disagree ~15% of the time, so deriving the
                          label from Binance would flatter us)
  * PM `best_bid_ask`  -> the ACTUAL ask we would have paid, as of T-20s

Entry rule under test: at T-20s, when the detector fires AND the flow is pushing
AGAINST the side leading at T-50s, buy the underdog at its posted ask.

Usage: python scripts/push_pm_backtest.py <YYYY-MM> [rt_cut]
"""
import io
import json
import os
import statistics
import sys
import zipfile

import zstandard as zstd

PM = r"D:\polycrypto\live_l2\polymarket"
AGG = r"D:\polycrypto\aggtrades"
CYCLE_S, BIN_S = 300, 10
NBINS = CYCLE_S // BIN_S
BODY_END = 25
RT_LO, RT_HI = 25, 28        # detector sees bins 25-27 only -> known at T-20s
ENTRY_OFFSET_S = 280         # T-20s
FEE = lambda a: 0.07 * a * (1 - a)


def zst_lines(path):
    with open(path, "rb") as f:
        yield from io.TextIOWrapper(zstd.ZstdDecompressor().stream_reader(f))


def load_markets(day, asset="BTC", interval="5m"):
    """-> {epoch: {up, down, outcome}} using the LAST snapshot per epoch."""
    p = os.path.join(PM, "markets", f"{day}.jsonl.zst")
    if not os.path.exists(p):
        return {}
    out = {}
    for line in zst_lines(p):
        try:
            m = json.loads(line)["market"]
        except Exception:
            continue
        if m.get("asset") != asset or m.get("interval") != interval:
            continue
        try:
            ep = int(m["epoch"])
        except (KeyError, TypeError, ValueError):
            continue
        out[ep] = {
            "up": str(m.get("up_token_id") or ""),
            "down": str(m.get("down_token_id") or ""),
            "outcome": (m.get("resolved_outcome") or "").strip().lower(),
        }
    return out


def load_quotes(day, tok2epoch):
    """Two snapshots per token, from one pass over the day's quote feed.

      entry -> last bid/ask at or before that token's own T-20s (what we could pay)
      final -> last bid/ask after the close (the RESOLUTION: the winner settles to
               bid ~0.99/ask 1.0 and the loser to 0/0.01)

    The `markets` feed carries a `resolved_outcome` field but it is empty on every
    snapshot (the recorder only sees markets while they are live), and the
    `market_resolved` feed has single-digit lines per day. Post-close quotes are
    therefore the only complete label source -- and they are Polymarket's OWN
    resolution, not a Binance proxy, which matters because the paper reports the
    two disagree about 15% of the time.
    """
    p = os.path.join(PM, "best_bid_ask", f"{day}.jsonl.zst")
    if not os.path.exists(p):
        return {}
    q = {}
    for line in zst_lines(p):
        try:
            pl = json.loads(line)["payload"]
            tok = pl["asset_id"]
        except Exception:
            continue
        ep = tok2epoch.get(tok)
        if ep is None:
            continue
        try:
            ts = int(pl["timestamp"]) // 1000
            quote = (float(pl["best_bid"]), float(pl["best_ask"]))
        except (KeyError, TypeError, ValueError):
            continue
        d = q.setdefault(tok, {})
        for off in (240, 250, 260, 270, 280, 290):
            if ts <= ep + off:
                d[f"e{off}"] = quote
        if ts >= ep + CYCLE_S + 5:
            d["final"] = quote
    return q


def binance_cycles(month, sym="BTCUSDT"):
    """-> {epoch: {margin_bps, rt, rt_dir, pre_up}} from monthly or daily zips."""
    files = [f for f in os.listdir(AGG)
             if f.startswith(sym) and month in f and f.endswith(".zip")]
    cyc = {}
    for fn in sorted(files):
        z = zipfile.ZipFile(os.path.join(AGG, fn))
        with z.open(z.namelist()[0]) as fh:
            for line in io.TextIOWrapper(fh, newline=""):
                f = line.split(",")
                if len(f) < 7:
                    continue
                try:
                    px = float(f[1]); qty = float(f[2]); ts = int(f[5])
                except ValueError:
                    continue
                sec = ts // 1_000_000
                k = (sec // CYCLE_S) * CYCLE_S
                b = (sec % CYCLE_S) // BIN_S
                c = cyc.get(k)
                if c is None:
                    c = cyc[k] = {"flow": [0.0] * NBINS, "first": [None] * NBINS}
                c["flow"][b] += -px * qty if f[6][0] in "Tt1" else px * qty
                if c["first"][b] is None:
                    c["first"][b] = px
    out = {}
    for k, c in cyc.items():
        body = [abs(c["flow"][i]) for i in range(BODY_END) if c["first"][i] is not None]
        if len(body) < 15:
            continue
        den = statistics.median(body)
        if den <= 0:
            continue
        op = next((p for p in c["first"] if p is not None), None)
        t50 = next((c["first"][i] for i in range(RT_LO, NBINS) if c["first"][i] is not None), None)
        if not op or not t50:
            continue
        out[k] = {"flow": c["flow"], "den": den,
                  "margin_bps": abs(t50 / op - 1) * 1e4, "pre_up": t50 >= op}
    return out


def causal_detector(b, entry_off):
    """Detector using ONLY bins complete at `entry_off` seconds into the cycle.

    This is the whole ballgame. The headline PushIntensity reads bin 29 and the
    T-20s variant reads bins 25-27, so evaluating either at an EARLIER entry is
    look-ahead: at T-50s the flow that triggers the signal has not happened yet.
    Here bin i is only readable once entry_off >= (i+1)*BIN_S.
    """
    hi = min(NBINS, entry_off // BIN_S)
    if hi <= RT_LO:
        return None                     # nothing of the push window is visible yet
    sgn = sum(b["flow"][RT_LO:hi])
    return abs(sgn) / b["den"], (1 if sgn > 0 else -1)


def main():
    NL = chr(10)
    month = sys.argv[1] if len(sys.argv) > 1 else "2026-07"
    print(f"loading Binance {month} ...", flush=True)
    bn = binance_cycles(month)
    print(f"  {len(bn)} Binance cycles", flush=True)

    days = sorted({f[:10] for f in os.listdir(os.path.join(PM, "best_bid_ask"))
                   if f.startswith(month)})
    rows = []
    for day in days:
        mk = load_markets(day)
        if not mk:
            continue
        tok2ep = {}
        for ep, m in mk.items():
            if m["up"]:
                tok2ep[m["up"]] = ep
            if m["down"]:
                tok2ep[m["down"]] = ep
        q = load_quotes(day, tok2ep)
        for ep, m in mk.items():
            b = bn.get(ep)
            if not b or not m["up"] or not m["down"]:
                continue
            fu = q.get(m["up"], {}).get("final")
            fd = q.get(m["down"], {}).get("final")
            if not fu or not fd:
                continue
            if fu[0] >= 0.9 and fd[0] <= 0.1:
                outcome = "up"
            elif fd[0] >= 0.9 and fu[0] <= 0.1:
                outcome = "down"
            else:
                continue
            under = "down" if b["pre_up"] else "up"
            rows.append({"b": b, "under": under, "won": outcome == under,
                         "q": q.get(m[under], {}), "margin": b["margin_bps"]})
        print(f"  {day}: {len(rows)} cumulative", flush=True)

    print(f"{NL}=== {month}: {len(rows)} joined cycles ===")
    print("CAUSAL: at each entry time the detector may read ONLY the bins already")
    print("complete at that moment. Entering earlier means a weaker signal, not a")
    print("cheaper price for the same signal.")
    print(f"{NL}{'entry':>8} {'bins':>7} {'n_fire':>7} {'ask':>7} {'WR':>7} "
          f"{'breakeven':>10} {'ROI':>9}")
    for off in (260, 270, 280, 290):
        det = []
        for r in rows:
            d = causal_detector(r["b"], off)
            e = r["q"].get(f"e{off}")
            if d and e and 0.0 < e[1] < 1.0:
                det.append((d[0], d[1], e[1], r))
        if len(det) < 200:
            continue
        cut = sorted(x[0] for x in det)[int(0.90 * len(det))]
        fire = [x for x in det
                if x[0] >= cut and ((x[1] > 0) != x[3]["b"]["pre_up"])]
        if len(fire) < 20:
            continue
        ask = statistics.mean(x[2] for x in fire)
        wr = sum(x[3]["won"] for x in fire) / len(fire)
        pnl = sum((1.0 - x[2] - FEE(x[2])) if x[3]["won"] else -(x[2] + FEE(x[2]))
                  for x in fire)
        cost = sum(x[2] + FEE(x[2]) for x in fire)
        nb = (min(NBINS, off // BIN_S)) - RT_LO
        print(f"  T-{300-off:>3}s {nb:>7} {len(fire):>7} {ask:7.3f} {wr:7.1%} "
              f"{ask+FEE(ask):10.3f} {pnl/cost if cost else 0:+8.1%}")

    print(f"{NL}--- baseline: buy EVERY underdog at T-20s (are takers profitable at all?) ---")
    allu = [r for r in rows if r["q"].get("e280") and 0.0 < r["q"]["e280"][1] < 1.0]
    a = [r["q"]["e280"][1] for r in allu]
    pnl = sum((1.0 - x - FEE(x)) if r["won"] else -(x + FEE(x))
              for r, x in zip(allu, a))
    cost = sum(x + FEE(x) for x in a)
    print(f"  n={len(allu)}  mean ask={statistics.mean(a):.3f}  "
          f"WR={sum(r['won'] for r in allu)/len(allu):.1%}  "
          f"net=${pnl:+.2f}  ROI={pnl/cost:+.1%}")

    # --- the OTHER SEAT ---------------------------------------------------------
    # Whoever posted that ask is the counterparty to every one of those losing
    # taker buys. Sell the underdog at the ask instead of buying it: collect `ask`
    # now, pay $1 only if the underdog actually wins. Collateral is (1 - ask).
    # Fees are NOT modelled here (Polymarket quotes maker_base_fee separately and
    # it needs its own study), so read this as the GROSS rent, an upper bound.
    print(f"{NL}--- the other seat: SELL every underdog at the posted ask ---")
    gross = sum((x - (1.0 if r["won"] else 0.0)) for r, x in zip(allu, a))
    coll = sum(1.0 - x for x in a)
    print(f"  n={len(allu)}  gross=${gross:+.2f} on ${coll:,.0f} collateral  "
          f"= {gross/coll:+.2%} per cycle turned (fees excluded)")
    wins = sum(1 for r in allu if not r["won"])
    print(f"  the seller is right {wins/len(allu):.1%} of the time "
          f"(mirror of the {sum(r['won'] for r in allu)/len(allu):.1%} taker WR)")


if __name__ == "__main__":
    main()
