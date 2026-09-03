"""Pull wallet-level trades for MANIPULATED vs CONTROL cycles.

THE ONE UNTESTED IDEA. Every other angle failed because the information arrived
no earlier than the price. This one might not: the paper's manipulators BUY the
contract and THEN push the spot, so their Polymarket position is established
BEFORE the Binance flow that every detector (ours and the market maker's) reacts
to. If that ordering holds, their wallet activity is a genuinely leading signal,
and it is public on-chain.

METHOD. Classify July BTC 5m cycles by the paper's PushIntensity (final-10s net
taker flow over the median body bin). Take the top decile as manipulated and a
matched random sample as control, then fetch the FULL trade tape for each from
data-api.polymarket.com -- every fill, with the proxyWallet behind it.

Deliberately deep rather than wide: one page covers only ~72 seconds of a cycle,
so shallow sampling would miss exactly the early positioning we are looking for.
Better to have complete tapes for a few hundred cycles than the last minute of
thousands.

Read-only public data. Rate-limited politely.

Usage: python scripts/fetch_wallet_trades.py [n_each]
"""
import json
import os
import pickle
import random
import statistics
import sys
import time
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from push_pm_backtest import PM, binance_cycles, zst_lines  # noqa: E402

API = "https://data-api.polymarket.com/trades"
N_EACH = int(sys.argv[1]) if len(sys.argv) > 1 else 300
MAX_PAGES = 8
OUT = "scripts/_wallet_trades.pkl"


def markets_july():
    """-> {epoch: condition_id} for BTC 5m on the recorded July days."""
    out = {}
    d = os.path.join(PM, "markets")
    for f in sorted(os.listdir(d)):
        if not f.startswith("2026-07"):
            continue
        for line in zst_lines(os.path.join(d, f)) if f.endswith(".zst") else open(
                os.path.join(d, f), encoding="utf-8", errors="ignore"):
            try:
                m = json.loads(line)["market"]
            except Exception:
                continue
            if m.get("asset") == "BTC" and m.get("interval") == "5m" and m.get("condition_id"):
                try:
                    out[int(m["epoch"])] = m["condition_id"]
                except (TypeError, ValueError):
                    pass
    return out


ERRORS = {"http": 0, "other": 0}


def fetch(cid):
    """All trades for one market, paginated.

    The API returns 403 to a request with no User-Agent, and an earlier version
    swallowed that in a bare `except: break` -- so 600 markets came back with
    zero trades and the run REPORTED that as a result. Errors are counted and
    surfaced now; a silent failure that looks like a finding is the worst
    possible outcome.
    """
    rows = []
    for page in range(MAX_PAGES):
        url = f"{API}?market={cid}&takerOnly=false&limit=500&offset={page*500}"
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (research; polymarket settlement study)",
            "Accept": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                d = json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            ERRORS["http"] += 1
            if ERRORS["http"] <= 3:
                print(f"    HTTP {e.code} on {cid[:10]}..", flush=True)
            break
        except Exception as e:
            ERRORS["other"] += 1
            if ERRORS["other"] <= 3:
                print(f"    {type(e).__name__} on {cid[:10]}..", flush=True)
            break
        if not d:
            break
        rows.extend(d)
        if len(d) < 500:
            break
        time.sleep(0.15)
    return rows


def main():
    print("computing PushIntensity for July BTC 5m ...", flush=True)
    bn = binance_cycles("2026-07")
    push = {}
    for ep, c in bn.items():
        den = c["den"]
        if den > 0:
            push[ep] = abs(c["flow"][29]) / den
    mk = markets_july()
    common = [ep for ep in push if ep in mk]
    print(f"  cycles with both Binance flow and a market: {len(common):,}")
    if not common:
        return
    vals = sorted(push[e] for e in common)
    cut = vals[int(0.90 * len(vals))]
    manip = [e for e in common if push[e] >= cut]
    norm = [e for e in common if push[e] < cut]
    print(f"  PushIntensity p90 = {cut:.2f} (paper: 16.11)")
    print(f"  manipulated {len(manip):,}   normal {len(norm):,}")

    random.seed(7)
    pick_m = random.sample(manip, min(N_EACH, len(manip)))
    pick_n = random.sample(norm, min(N_EACH, len(norm)))
    jobs = [(e, "manip") for e in pick_m] + [(e, "control") for e in pick_n]
    random.shuffle(jobs)

    out = {}
    t0 = time.time()
    for i, (ep, tag) in enumerate(jobs, 1):
        rows = fetch(mk[ep])
        out[ep] = {"tag": tag, "push": push[ep], "cid": mk[ep], "trades": rows}
        if i % 25 == 0 or i == len(jobs):
            el = time.time() - t0
            print(f"  [{i}/{len(jobs)}] {sum(len(v['trades']) for v in out.values()):,} trades "
                  f"({el:.0f}s, {el/i:.2f}s per market)", flush=True)
        time.sleep(0.1)

    with open(OUT, "wb") as fh:
        pickle.dump(out, fh)
    if ERRORS["http"] or ERRORS["other"]:
        print(f"  FETCH ERRORS: http={ERRORS['http']} other={ERRORS['other']}")
    tt = sum(len(v["trades"]) for v in out.values())
    ww = len({t["proxyWallet"] for v in out.values() for t in v["trades"]})
    print(f"\n{len(out)} markets, {tt:,} trades, {ww:,} distinct wallets -> {OUT}")


if __name__ == "__main__":
    main()
