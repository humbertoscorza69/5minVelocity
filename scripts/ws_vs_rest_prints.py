"""Settle whether the WS `last_trade_price` feed is a COMPLETE print source.

Usage: python scripts/ws_vs_rest_prints.py <seconds>

The maker fill model consumes trade prints to drain queue. The validated
queue-aware backtest (+0.455c/share OOS) scored against the REST feed
data-api /trades. The dev found the live WS also emits `last_trade_price`
carrying price/size/side/transaction_hash — but the NAME implies last-price
semantics, which would mean it does not emit one event per fill.

Both feeds carry `transaction_hash`, so completeness is directly measurable:
collect WS prints for N seconds, then pull REST trades for the same markets and
window, and match on hash. Under-counting shows up as REST hashes the WS never
sent.
"""
import asyncio
import json
import sys
import time
from collections import defaultdict

import requests
import websockets

WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
GAMMA = "https://gamma-api.polymarket.com/markets"
REST = "https://data-api.polymarket.com/trades"
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120 Safari/537.36"}


def discover():
    """Current + next few 5m/15m windows, via slug probe (the markets channel is
    authoritative but needs a subscription; slug probing is enough to get tokens)."""
    now = int(time.time())
    out = {}
    for asset in ("btc", "eth", "sol", "xrp"):
        for iv, step in (("5m", 300), ("15m", 900)):
            base = now - (now % step)
            for off in (0, step):
                slug = f"{asset}-updown-{iv}-{base + off}"
                try:
                    r = requests.get(GAMMA, params={"slug": slug}, headers=UA, timeout=15)
                    j = r.json()
                except Exception:
                    continue
                if not isinstance(j, list) or not j:
                    continue
                m = j[0]
                toks = m.get("clobTokenIds")
                if isinstance(toks, str):
                    toks = json.loads(toks)
                cid = m.get("conditionId")
                if toks and cid:
                    out[cid] = dict(slug=slug, tokens=[str(t) for t in toks])
    return out


async def collect(tokens, seconds):
    prints, counts = [], defaultdict(int)
    try:
        async with websockets.connect(WS, ping_interval=15, max_size=None) as ws:
            await ws.send(json.dumps({"assets_ids": tokens, "type": "market",
                                      "custom_feature_enabled": True}))
            end = time.time() + seconds
            while time.time() < end:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=max(1, end - time.time()))
                except asyncio.TimeoutError:
                    break
                try:
                    msg = json.loads(raw)
                except Exception:
                    continue
                for ev in (msg if isinstance(msg, list) else [msg]):
                    if not isinstance(ev, dict):
                        continue
                    et = ev.get("event_type", "?")
                    counts[et] += 1
                    if et == "last_trade_price":
                        prints.append(ev)
    except Exception as exc:
        print(f"  ws error: {exc}")
    return prints, counts


def main(seconds=180):
    mk = discover()
    print(f"discovered {len(mk)} markets")
    tokens = [t for v in mk.values() for t in v["tokens"]]
    print(f"subscribing to {len(tokens)} tokens for {seconds}s ...", flush=True)
    t0 = time.time()
    prints, counts = asyncio.run(collect(tokens, seconds))
    t1 = time.time()
    print(f"\nWS event types received: {dict(counts)}")
    print(f"WS last_trade_price events: {len(prints)}")
    if prints:
        print(f"  sample: {json.dumps(prints[0])[:300]}")
    ws_hash = {p.get("transaction_hash") for p in prints if p.get("transaction_hash")}
    ws_by_tok = defaultdict(int)
    for p in prints:
        ws_by_tok[p.get("asset_id")] += 1

    print("\nfetching REST trades for the same window ...", flush=True)
    rest_hash, rest_n = set(), 0
    for cid, v in mk.items():
        try:
            r = requests.get(REST, params={"market": cid, "limit": 500},
                             headers=UA, timeout=25)
            j = r.json()
        except Exception:
            continue
        if not isinstance(j, list):
            continue
        for tr in j:
            ts = tr.get("timestamp") or tr.get("matchtime") or 0
            try:
                ts = float(ts)
                ts = ts / 1000 if ts > 1e11 else ts
            except Exception:
                continue
            if t0 <= ts <= t1 + 30:
                rest_n += 1
                h = tr.get("transactionHash") or tr.get("transaction_hash")
                if h:
                    rest_hash.add(h)
    print(f"REST trades inside the window: {rest_n} ({len(rest_hash)} hashes)")
    if rest_hash:
        both = ws_hash & rest_hash
        print(f"\n  hashes in BOTH        : {len(both)}")
        print(f"  REST-only (WS MISSED) : {len(rest_hash - ws_hash)}")
        print(f"  WS-only               : {len(ws_hash - rest_hash)}")
        cov = len(both) / len(rest_hash) if rest_hash else float("nan")
        print(f"\n  WS coverage of REST prints: {cov:.1%}")
        print("  -> ~100% means last_trade_price IS a complete print feed and Part A")
        print("     can use it in real time. Materially <100% means it is last-price")
        print("     semantics and REST must remain the fill-scoring path.")
    else:
        print("  (no REST hashes in-window — inconclusive, rerun on livelier tape)")


if __name__ == "__main__":
    main(int(sys.argv[1]) if len(sys.argv) > 1 else 180)
