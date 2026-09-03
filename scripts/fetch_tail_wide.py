"""Wide fetch of late-cycle trades, so the tail test has a real sample.

THE CRITICISM THAT PROMPTED THIS. The first tail study had 108,675 signals but
only 254 distinct TOKENS -- each outcome counted ~428 times. At the token level,
which is the independent unit, n=254 and nothing was distinguishable from zero.
254 tokens is roughly one day of 5-minute markets: far too thin to conclude
anything about a 1-in-10 event.

WHY THIS IS CHEAP NOW. The API returns trades NEWEST FIRST, and every tail buy
sits in the first two pages: page 1 covers t+298..+370s, page 2 covers
t+292..+298s, and pages 3-8 (t+253..+292s) contained ZERO tail buys in the probe.
So two pages per market captures the whole tail population at a quarter of the
request cost -- which buys roughly 8x the token count.

Only late-cycle trades are kept, and only the fields the test needs, so the
output stays small enough to work with.

Usage: python scripts/fetch_tail_wide.py [n_markets]
"""
import json
import os
import pickle
import sys
import time
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from push_pm_backtest import PM, zst_lines  # noqa: E402

API = "https://data-api.polymarket.com/trades"
PAGES = 2
N = int(sys.argv[1]) if len(sys.argv) > 1 else 2000
OUT = "scripts/_tail_wide.pkl"
ERR = {"http": 0, "other": 0}


def markets():
    """-> {epoch: condition_id} for BTC 5m across every recorded day."""
    out = {}
    d = os.path.join(PM, "markets")
    for f in sorted(os.listdir(d)):
        p = os.path.join(d, f)
        src = zst_lines(p) if f.endswith(".zst") else open(p, encoding="utf-8", errors="ignore")
        for line in src:
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


def fetch(cid):
    rows = []
    for page in range(PAGES):
        url = f"{API}?market={cid}&takerOnly=false&limit=500&offset={page*500}"
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (research; polymarket settlement study)",
            "Accept": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                d = json.loads(r.read().decode())
        except urllib.error.HTTPError:
            ERR["http"] += 1
            break
        except Exception:
            ERR["other"] += 1
            break
        if not d:
            break
        rows.extend(d)
        if len(d) < 500:
            break
    return rows


def main():
    lab = pickle.load(open("scripts/_true_labels.pkl", "rb"))
    resolved = {t for t in lab}
    mk = markets()
    # keep only markets whose BOTH legs we can resolve on-chain
    by_ep = {}
    for tok, d in lab.items():
        if d["asset"] == "BTC" and d["interval"] == "5m":
            by_ep.setdefault(d["epoch"], set()).add(tok)
    eps = sorted(e for e in mk if len(by_ep.get(e, ())) == 2)
    print(f"BTC 5m markets with a condition_id and BOTH legs resolved: {len(eps):,}")
    # spread the sample evenly across the whole recorded span, not one block
    step = max(1, len(eps) // N)
    pick = eps[::step][:N]
    print(f"sampling {len(pick):,} of them (every {step})")

    # CHECKPOINT + RESUME. The first run died at 700/2500 during a session
    # interruption and, because the pickle was only written at the end, lost all
    # of it. Now: load any partial result, skip epochs already fetched, and
    # write the file every 100 markets so a kill costs at most a few minutes.
    out = {}
    if os.path.exists(OUT):
        try:
            out = pickle.load(open(OUT, "rb"))
            print(f"resuming: {len(out):,} markets already fetched", flush=True)
        except Exception:
            out = {}
    todo = [ep for ep in pick if ep not in out]
    t0 = time.time()
    for i, ep in enumerate(todo, 1):
        rows = fetch(mk[ep])
        keep = []
        for t in rows:
            try:
                ts = int(t["timestamp"]); p = float(t["price"]); s = float(t["size"])
            except (KeyError, TypeError, ValueError):
                continue
            if t.get("asset") not in resolved:
                continue
            keep.append((t["asset"], ts, p, s, t["side"].upper(), t["proxyWallet"]))
        out[ep] = keep
        if i % 100 == 0 or i == len(todo):
            with open(OUT + ".tmp", "wb") as fh:
                pickle.dump(out, fh)
            os.replace(OUT + ".tmp", OUT)
            el = time.time() - t0
            print(f"  [{len(out)}/{len(pick)}] {sum(len(v) for v in out.values()):,} kept "
                  f"({el/60:.1f}min, {el/i:.2f}s/mkt, eta {(len(todo)-i)*el/i/60:.0f}min) "
                  f"[checkpointed]", flush=True)
        time.sleep(0.05)

    with open(OUT, "wb") as fh:
        pickle.dump(out, fh)
    tails = sum(1 for v in out.values() for r in v if 0.01 <= r[2] < 0.10 and r[4] == "BUY")
    toks = len({r[0] for v in out.values() for r in v if 0.01 <= r[2] < 0.10 and r[4] == "BUY"})
    print(f"\n{len(out):,} markets, {sum(len(v) for v in out.values()):,} trades")
    print(f"tail buys: {tails:,} across {toks:,} DISTINCT TOKENS  (was 254)")
    if ERR["http"] or ERR["other"]:
        print(f"errors: http={ERR['http']} other={ERR['other']}")


if __name__ == "__main__":
    main()
