"""Rebuild our ENTIRE trade history against Polymarket's OWN resolution.

THE PROBLEM THIS SOLVES. The bot books P&L from a Binance-derived label
(`v2_settled`), and `redemption.rs:588` decides what to collect on-chain using
that SAME label. So the loop is closed: nothing ever checks it. A correction can
only fire when a redeem is actually attempted and pays zero, which happened on
31.6% of live trades. On those, the booked 58.4% win rate became a true 40.8%.
On the other 68% -- never redeemed, never contradicted -- booked and "true"
agree perfectly, because no one ever asked.

That makes every number we have ever quoted suspect, including the calibration
curve and the z-gate, because those were FIT against a label derived from the
same Binance feed the features come from. The model has been predicting its own
label.

THE FIX. Polymarket's resolution is observable without any API call: after a
market closes, the winning token quotes bid ~0.99 / ask 1.00 and the loser
0.00 / 0.01. The recorder captured `best_bid_ask` continuously, so the true
outcome of every market we traded is sitting in data we already own.

This builds token -> TRUE outcome for every recorded day and caches it, so every
downstream study can finally be scored against reality instead of ourselves.

Usage: python scripts/true_labels.py [out.json]
"""
import json
import os
import pickle
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from push_pm_backtest import PM, zst_lines  # noqa: E402

CYCLE = {"5m": 300, "15m": 900, "1h": 3600, "4h": 14400}
# a quote this long after the close is the settled state, not live trading
SETTLE_LAG_S = 5
WIN_BID, LOSE_BID = 0.90, 0.10


def day_files(sub):
    """Recorded days for a channel, handling both .zst and plain .jsonl."""
    d = os.path.join(PM, sub)
    out = {}
    for f in os.listdir(d):
        if f.endswith(".jsonl.zst") or f.endswith(".jsonl"):
            out[f[:10]] = os.path.join(d, f)
    return out


def lines(path):
    if path.endswith(".zst"):
        yield from zst_lines(path)
    else:
        with open(path, encoding="utf-8", errors="ignore") as fh:
            yield from fh


def markets_for_day(path):
    """-> {token: (epoch, end_s, asset, interval, side)} for EVERY asset/interval."""
    out = {}
    for line in lines(path):
        try:
            m = json.loads(line)["market"]
            ep = int(m["epoch"])
        except Exception:
            continue
        iv = (m.get("interval") or "").lower()
        span = CYCLE.get(iv)
        if not span:
            continue
        a = (m.get("asset") or "").upper()
        for key, side in (("up_token_id", "up"), ("down_token_id", "down")):
            tok = m.get(key)
            if tok:
                out[str(tok)] = (ep, ep + span, a, iv, side)
    return out


def resolve_day(bba_path, toks):
    """Last post-close quote per token -> the settled bid."""
    final = {}
    for line in lines(bba_path):
        try:
            pl = json.loads(line)["payload"]
            tok = pl["asset_id"]
        except Exception:
            continue
        meta = toks.get(tok)
        if meta is None:
            continue
        try:
            ts = int(pl["timestamp"]) // 1000
            bid = float(pl["best_bid"])
        except (KeyError, TypeError, ValueError):
            continue
        if ts >= meta[1] + SETTLE_LAG_S:
            final[tok] = bid
    return final


def main():
    out_path = sys.argv[1] if len(sys.argv) > 1 else "scripts/_true_labels.pkl"
    mk_days = day_files("markets")
    bba_days = day_files("best_bid_ask")
    days = sorted(set(mk_days) & set(bba_days))
    print(f"recorded days with both markets and quotes: {len(days)}", flush=True)

    labels = {}          # token -> dict(won, asset, interval, epoch, side, settled_bid)
    pairs = {}           # epoch+asset+interval -> {side: token}
    for day in days:
        toks = markets_for_day(mk_days[day])
        if not toks:
            continue
        final = resolve_day(bba_days[day], toks)
        n_ok = 0
        for tok, bid in final.items():
            ep, end, asset, iv, side = toks[tok]
            if bid >= WIN_BID:
                won = True
            elif bid <= LOSE_BID:
                won = False
            else:
                continue          # still contested: refuse to label
            labels[tok] = {"won": won, "asset": asset, "interval": iv,
                           "epoch": ep, "side": side, "settled_bid": bid}
            pairs.setdefault((ep, asset, iv), {})[side] = tok
            n_ok += 1
        print(f"  {day}: {len(toks)} tokens -> {n_ok} resolved", flush=True)

    # SANITY: in a binary, exactly one side of each pair must win. A pair where
    # both or neither won means the label rule is wrong, and we must know that.
    both = neither = good = 0
    for (_ep, _a, _iv), d in pairs.items():
        if len(d) != 2:
            continue
        w = [labels[t]["won"] for t in d.values()]
        if all(w):
            both += 1
        elif not any(w):
            neither += 1
        else:
            good += 1
    tot = both + neither + good
    print(f"\ncomplete up/down pairs: {tot}")
    if tot:
        print(f"  exactly one winner : {good} ({good/tot:.2%})   <- must be ~100%")
        print(f"  both won           : {both}")
        print(f"  neither won        : {neither}")
    print(f"\nresolved tokens: {len(labels)}")
    with open(out_path, "wb") as fh:
        pickle.dump(labels, fh)
    print(f"cached -> {out_path}")


if __name__ == "__main__":
    main()
