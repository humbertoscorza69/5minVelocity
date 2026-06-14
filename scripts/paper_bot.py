#!/usr/bin/env python3
"""
Polymarket BTC/ETH 5-minute Up/Down — LEAD-LAG PAPER-TRADING BOT (no real orders).

Strategy (balanced config from the research; see docs/TAKER_SPEC.md):
  - Watch the CURRENT 5-minute market for BTC and ETH.
  - In the entry window ttl in [5, 180] seconds-to-settle, the FIRST instant that
    BOTH:
        2-second Binance move >= VEL_MIN bps toward a side   (lag trigger)
        displacement-from-window-open in [DISP_LO, DISP_HI] bps for that side
    -> PAPER-BUY that side, hold to settlement. One trade per market. Never flip.
  - Latency is modeled: on signal we record the book ask, then wait
    ORDER_LATENCY_MS and record the ask we would actually have hit -> that is the
    paper fill. Feed latency (Binance exch ts -> our clock) is logged too.

Execution is SIMULATED. This never sends an order. It exists to compare modeled
vs real fills before risking capital.

Run:
  pip install websockets aiohttp
  python scripts/paper_bot.py
Outputs (event-driven, tiny):
  data/paper_log.jsonl    one JSON line per event (signal/fill/skip/settle)
  data/paper_trades.csv   one row per completed (filled+settled) trade
  data/bot.log            rotating status log (30 MB cap total)
Config via env vars (optional): ORDER_LATENCY_MS, STAKE, VEL_MIN, DISP_LO,
  DISP_HI, TTL_MIN, TTL_MAX, ASSETS.
"""
import asyncio
import csv
import json
import logging
import os
import time
from collections import deque
from logging.handlers import RotatingFileHandler

import aiohttp
import websockets

# ----------------------------- CONFIG -----------------------------
DATA_DIR = os.environ.get("PAPER_DATA_DIR", "data")
ASSETS = os.environ.get("ASSETS", "btc,eth").split(",")
STAKE = float(os.environ.get("STAKE", "10"))          # paper $ per trade
VEL_MIN = float(os.environ.get("VEL_MIN", "4"))       # 2s move, bps (balanced)
# Displacement band aligned to the validated backtest sweet spot (disp 2-10bps).
# v1 defaults (DISP_LO=1, ASK_CEIL=0.97) let in coin-flips + razor deep favorites
# and lost money in live day-1 paper trading; tightened to the edge zone.
DISP_LO = float(os.environ.get("DISP_LO", "3"))       # displacement band, bps
DISP_HI = float(os.environ.get("DISP_HI", "12"))
TTL_MIN = float(os.environ.get("TTL_MIN", "5"))       # seconds to settle
TTL_MAX = float(os.environ.get("TTL_MAX", "180"))
ASK_FLOOR = float(os.environ.get("ASK_FLOOR", "0.45"))
ASK_CEIL = float(os.environ.get("ASK_CEIL", "0.90"))
ORDER_LATENCY_MS = int(os.environ.get("ORDER_LATENCY_MS", "300"))
MAX_SLIP = float(os.environ.get("MAX_SLIP", "0.04"))   # max ask drift signal->fill
SETTLE_DELAY = int(os.environ.get("SETTLE_DELAY", "20"))   # score this long after settle
STALE_SETTLE_WARN = int(os.environ.get("STALE_SETTLE_WARN", "120"))  # alarm if unscored
FEE_RATE = 0.07
INTERVAL = 300                                         # 5-minute markets
DECISION_HZ = 0.2                                      # decision loop period (s)

SYM = {"btc": "BTCUSDT", "eth": "ETHUSDT"}
SYM2ASSET = {v.lower(): k for k, v in SYM.items()}
GAMMA = "https://gamma-api.polymarket.com/markets"
CLOB = "https://clob.polymarket.com"
BINANCE_REST = "https://api.binance.com/api/v3/klines"
BINANCE_WS = ("wss://stream.binance.com:9443/stream?streams="
              + "/".join(f"{SYM[a].lower()}@aggTrade" for a in ASSETS))
UA = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}

os.makedirs(DATA_DIR, exist_ok=True)
LOG_JSONL = os.path.join(DATA_DIR, "paper_log.jsonl")
TRADES_CSV = os.path.join(DATA_DIR, "paper_trades.csv")

logger = logging.getLogger("paperbot")
logger.setLevel(logging.INFO)
_h = RotatingFileHandler(os.path.join(DATA_DIR, "bot.log"),
                         maxBytes=10_000_000, backupCount=2)  # 30 MB cap
_h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(_h)
logger.addHandler(logging.StreamHandler())

# ----------------------------- STATE -----------------------------
# per asset: deque of (exch_ms, price)
spot = {a: deque(maxlen=4000) for a in ASSETS}
markets = {}          # key (asset, epoch) -> dict
http = None           # aiohttp session


def now_ms():
    return int(time.time() * 1000)


def log_event(ev: dict):
    ev["t_log"] = now_ms()
    with open(LOG_JSONL, "a") as f:
        f.write(json.dumps(ev) + "\n")


def spot_now(asset):
    d = spot[asset]
    return d[-1] if d else None  # (exch_ms, price)


def spot_at(asset, target_ms):
    """Last price with exch_ms <= target_ms (for 2s velocity)."""
    d = spot[asset]
    best = None
    for ts, px in reversed(d):
        if ts <= target_ms:
            best = (ts, px)
            break
    return best


# ----------------------------- REST helpers -----------------------------
async def jget(url):
    for attempt in range(4):
        try:
            async with http.get(url, headers=UA, timeout=15) as r:
                if r.status == 200:
                    return await r.json()
        except Exception as e:
            if attempt == 3:
                logger.warning(f"GET fail {url[:70]}: {e}")
        await asyncio.sleep(0.5 * (attempt + 1))
    return None


async def discover_market(asset, epoch):
    slug = f"{asset}-updown-5m-{epoch}"
    d = await jget(f"{GAMMA}?slug={slug}")
    if not d:
        return None
    m = d[0]
    try:
        toks = json.loads(m["clobTokenIds"])
        outs = json.loads(m["outcomes"])
    except Exception:
        return None
    # map outcome -> token id
    tok = {outs[i].lower(): toks[i] for i in range(len(toks))}
    if "up" not in tok or "down" not in tok:
        return None
    return {"cid": m.get("conditionId"), "slug": slug,
            "token": tok, "settle": epoch + INTERVAL}


async def fetch_open_px(asset, epoch):
    """Binance 1s close at the window-open second (settlement reference proxy)."""
    url = (f"{BINANCE_REST}?symbol={SYM[asset]}&interval=1s"
           f"&startTime={epoch*1000}&limit=1")
    d = await jget(url)
    try:
        return float(d[0][4])
    except Exception:
        return None


async def fetch_book(token_id):
    d = await jget(f"{CLOB}/book?token_id={token_id}")
    if not d:
        return (None, None)
    bids = [float(x["price"]) for x in d.get("bids", [])]
    asks = [float(x["price"]) for x in d.get("asks", [])]
    return (max(bids) if bids else None, min(asks) if asks else None)


async def fetch_authoritative(cid, slug):
    """Best-effort Polymarket resolution ('Up'/'Down') or None.
    NOTE: for these 5m markets the public APIs publish the winner SLOWLY
    (minutes), so this is only a confirmation — primary scoring uses Binance."""
    g = await jget(f"{GAMMA}?slug={slug}")
    if isinstance(g, list) and g:
        m = g[0]
        if m.get("closed"):
            try:
                pr = json.loads(m["outcomePrices"]) if isinstance(
                    m.get("outcomePrices"), str) else m.get("outcomePrices")
                ou = json.loads(m["outcomes"]) if isinstance(
                    m.get("outcomes"), str) else m.get("outcomes")
                for i, p in enumerate(pr):
                    if float(p) >= 0.99:
                        return ou[i]
            except Exception:
                pass
    d = await jget(f"{CLOB}/markets/{cid}")
    if isinstance(d, dict):
        for t in d.get("tokens", []):
            if t.get("winner"):
                return t.get("outcome")
    return None


# ----------------------------- TASKS -----------------------------
async def binance_ws():
    while True:
        try:
            async with websockets.connect(BINANCE_WS, ping_interval=15,
                                          max_queue=None) as ws:
                logger.info("Binance WS connected")
                async for raw in ws:
                    msg = json.loads(raw)
                    d = msg.get("data") or {}
                    s = d.get("s", "").lower()
                    a = SYM2ASSET.get(s)
                    if a and "p" in d:
                        spot[a].append((int(d["T"]), float(d["p"])))
        except Exception as e:
            logger.warning(f"Binance WS dropped: {e}; reconnecting in 3s")
            await asyncio.sleep(3)


async def market_manager():
    while True:
        try:
            now = int(time.time())
            epoch = now - (now % INTERVAL)
            for a in ASSETS:
                key = (a, epoch)
                if key not in markets:
                    info = await discover_market(a, epoch)
                    if not info:
                        continue
                    info["open_px"] = await fetch_open_px(a, epoch)
                    info.update(asset=a, epoch=epoch, signaled=False,
                                position=None, settled=False)
                    markets[key] = info
                    logger.info(f"market {info['slug']} open_px={info['open_px']} "
                                f"settle={info['settle']}")
            # prune very old, fully-settled markets from memory
            for k in [k for k, m in markets.items()
                      if m.get("settled") and time.time() - m["settle"] > 1200]:
                markets.pop(k, None)
        except Exception as e:
            logger.warning(f"market_manager: {e}")
        await asyncio.sleep(15)


def evaluate(m):
    """Return (side, vel, disp, spot_now_px, spot_2s_px) if signal, else None."""
    a = m["asset"]
    op = m.get("open_px")
    if not op:
        return None
    sn = spot_now(a)
    if not sn:
        return None
    t0, p_now = sn
    s2 = spot_at(a, t0 - 2000)
    if not s2:
        return None
    p2 = s2[1]
    ret2 = (p_now / p2 - 1) * 1e4          # bps over 2s (signed: + = up)
    dispU = (p_now / op - 1) * 1e4         # up-side displacement bps
    if ret2 >= VEL_MIN and DISP_LO <= dispU <= DISP_HI:
        return ("up", ret2, dispU, p_now, p2)
    if -ret2 >= VEL_MIN and DISP_LO <= -dispU <= DISP_HI:
        return ("down", -ret2, -dispU, p_now, p2)
    return None


async def fill_after_latency(m, sig):
    await asyncio.sleep(ORDER_LATENCY_MS / 1000.0)
    token = m["token"][sig["side"]]
    bid, ask = await fetch_book(token)
    asig = sig["ask_at_signal"]
    slip = (ask - asig) if (ask is not None and asig is not None) else None
    in_range = ask is not None and (ASK_FLOOR <= ask <= ASK_CEIL)
    within_slip = slip is not None and slip <= MAX_SLIP
    would_trade = bool(in_range and within_slip)   # the live bot WOULD enter this
    rec = {"type": "fill", "slug": m["slug"], "asset": m["asset"],
           "side": sig["side"], "token": token, "t_fill": now_ms(),
           "ask_at_fill": ask, "bid_at_fill": bid, "ask_at_signal": asig,
           "slip": round(slip, 4) if slip is not None else None,
           "in_range": in_range, "within_slip": within_slip,
           "would_trade": would_trade, "max_slip": MAX_SLIP,
           "latency_ms": ORDER_LATENCY_MS}
    if ask is None:
        rec["filled"] = False
        rec["reason"] = "no_book"
        log_event(rec)
        logger.info(f"NOBOOK {m['slug']} {sig['side']}")
        return
    # Paper-fill ALL signals (so outcomes are recorded for tuning the cap), but
    # tag would_trade. Live P&L = filter would_trade==1. Shadow = the rest.
    shares = STAKE / ask
    fee = shares * FEE_RATE * ask * (1 - ask)
    rec.update(filled=True, fill_price=ask, shares=shares, fee=fee,
               cost=STAKE + fee, disp_bps=sig.get("disp_bps"),
               vel_bps=sig.get("vel_bps"), ttl_s=sig.get("ttl_s"))
    m["position"] = rec
    log_event(rec)
    tag = "TRADE" if would_trade else "SHADOW"
    logger.info(f"{tag} {m['slug']} {sig['side']} @ {ask:.3f} "
                f"slip={(slip if slip is not None else float('nan')):+.3f} "
                f"(cap {MAX_SLIP}) would_trade={would_trade}")


async def decision_loop():
    while True:
        try:
            tnow = time.time()
            for m in list(markets.values()):
                if m["signaled"] or m.get("settled"):
                    continue
                ttl = m["settle"] - tnow
                if not (TTL_MIN <= ttl <= TTL_MAX):
                    continue
                sig = evaluate(m)
                if not sig:
                    continue
                m["signaled"] = True
                side, vel, disp, p_now, p2 = sig
                token = m["token"][side]
                sn = spot_now(m["asset"])
                bid, ask = await fetch_book(token)
                signal = {
                    "type": "signal", "slug": m["slug"], "asset": m["asset"],
                    "side": side, "token": token,
                    "t_signal": now_ms(), "spot_exch_ms": sn[0],
                    "feed_latency_ms": now_ms() - sn[0],
                    "ttl_s": round(ttl, 2), "vel_bps": round(vel, 3),
                    "disp_bps": round(disp, 3), "spot_now": p_now,
                    "spot_2s_ago": p2, "open_px": m["open_px"],
                    "ask_at_signal": ask, "bid_at_signal": bid}
                log_event(signal)
                logger.info(f"SIGNAL {m['slug']} {side} ttl={ttl:.0f}s "
                            f"vel={vel:.1f} disp={disp:.1f} ask={ask} "
                            f"feed_lat={signal['feed_latency_ms']}ms")
                asyncio.create_task(fill_after_latency(m, signal))
        except Exception as e:
            logger.warning(f"decision_loop: {e}")
        await asyncio.sleep(DECISION_HZ)


def write_trade_row(row):
    new = not os.path.exists(TRADES_CSV)
    with open(TRADES_CSV, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(row.keys()))
        if new:
            w.writeheader()
        w.writerow(row)


async def settlement_loop():
    while True:
        try:
            tnow = time.time()
            for m in list(markets.values()):
                if m.get("settled") or m.get("position") is None:
                    continue
                if tnow < m["settle"] + SETTLE_DELAY:
                    continue
                # PRIMARY outcome = Binance final-vs-open (fast, ~settle+seconds)
                op = m.get("open_px")
                fin = await fetch_open_px(m["asset"], m["settle"] - 1)
                if fin is None or op is None:
                    if tnow > m["settle"] + STALE_SETTLE_WARN:
                        logger.warning(f"UNSCORED {m['slug']} "
                                       f"{int(tnow-m['settle'])}s past settle — "
                                       f"no Binance price (possible bug)")
                    continue
                bin_out = "Up" if fin >= op else "Down"
                # best-effort authoritative confirmation (often slow/None)
                auth = await fetch_authoritative(m["cid"], m["slug"])
                outcome = auth or bin_out
                source = "polymarket" if auth else "binance"
                pos = m["position"]
                won = (outcome.lower() == pos["side"])
                pnl = (pos["shares"] if won else 0.0) - pos["cost"]
                row = {
                    "slug": m["slug"], "asset": m["asset"], "side": pos["side"],
                    "disp_bps": pos.get("disp_bps"), "vel_bps": pos.get("vel_bps"),
                    "ttl_s": pos.get("ttl_s"), "fill_price": pos["fill_price"],
                    "shares": round(pos["shares"], 3), "cost": round(pos["cost"], 4),
                    "outcome": outcome, "outcome_source": source,
                    "binance_outcome": bin_out,
                    "auth_outcome": auth if auth else "",
                    "auth_agrees": int(auth == bin_out) if auth else "",
                    "won": int(won), "pnl": round(pnl, 4), "stake": STAKE,
                    "latency_ms": pos["latency_ms"],
                    "ask_at_signal": pos["ask_at_signal"],
                    "ask_at_fill": pos["fill_price"], "slip": pos.get("slip"),
                    "within_slip": int(bool(pos.get("within_slip"))),
                    "would_trade": int(bool(pos.get("would_trade"))),
                    "max_slip": pos.get("max_slip"),
                    "open_px": op, "final_px": fin, "settle_ts": m["settle"]}
                log_event({"type": "settle", **row})
                write_trade_row(row)
                m["settled"] = True
                if auth and auth != bin_out:
                    logger.warning(f"OUTCOME MISMATCH {m['slug']}: "
                                   f"binance={bin_out} polymarket={auth}")
                logger.info(f"SETTLE {m['slug']} {pos['side']} -> {outcome} "
                            f"({source}) won={won} pnl={pnl:+.3f}")
        except Exception as e:
            logger.warning(f"settlement_loop: {e}")
        await asyncio.sleep(10)


async def stats_loop():
    while True:
        await asyncio.sleep(300)
        try:
            n = wins = 0
            pnl = 0.0
            nw = winsw = 0
            pnlw = 0.0
            if os.path.exists(TRADES_CSV):
                with open(TRADES_CSV) as f:
                    for r in csv.DictReader(f):
                        n += 1
                        wins += int(r["won"])
                        pnl += float(r["pnl"])
                        if int(r.get("would_trade", 1)):
                            nw += 1
                            winsw += int(r["won"])
                            pnlw += float(r["pnl"])
            active = sum(1 for m in markets.values() if not m.get("settled"))
            logger.info(
                f"STATS would_trade: n={nw} win_rate={(winsw/nw if nw else 0):.3f} "
                f"pnl={pnlw:+.2f} | ALL(incl shadow): n={n} "
                f"win_rate={(wins/n if n else 0):.3f} pnl={pnl:+.2f} | "
                f"active={active}")
        except Exception as e:
            logger.warning(f"stats_loop: {e}")


async def main():
    global http
    logger.info(f"PAPER BOT start | assets={ASSETS} stake=${STAKE} "
                f"vel>={VEL_MIN} disp[{DISP_LO},{DISP_HI}] ttl[{TTL_MIN},{TTL_MAX}] "
                f"order_latency={ORDER_LATENCY_MS}ms")
    http = aiohttp.ClientSession()
    try:
        await asyncio.gather(binance_ws(), market_manager(), decision_loop(),
                             settlement_loop(), stats_loop())
    finally:
        await http.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("stopped")
