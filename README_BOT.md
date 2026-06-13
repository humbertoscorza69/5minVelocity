# Lead-Lag Paper-Trading Bot

Paper-trades the Polymarket BTC/ETH 5-minute Up/Down lead-lag strategy. **Sends no
real orders** — it logs simulated entries/fills/settlements with full latency
accounting, so we can compare modeled vs real fills before risking capital.
Strategy rationale: `docs/TAKER_SPEC.md`.

## What it does
- Streams Binance BTC/ETH trades (sub-second) for the signal.
- Watches the current 5-min market per asset; in ttl ∈ [5,180]s, the **first**
  instant `2s move ≥ VEL_MIN bps` toward a side AND `displacement-from-open ∈
  [DISP_LO,DISP_HI] bps` → paper-buy that side, hold to settle. One trade/market.
- Models order latency: records the book ask at signal, waits `ORDER_LATENCY_MS`,
  records the ask it would actually have hit = the paper fill. Logs feed latency too.
- Scores each trade against the real Polymarket resolution (and the Binance-implied
  outcome, for comparison).

## Run (Dublin VPS)
```bash
git clone <your-repo> && cd <repo>
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python3 scripts/paper_bot.py            # logs to ./data/
```

### Run 24/7 with systemd (auto-restart)
`/etc/systemd/system/paperbot.service`:
```ini
[Unit]
Description=Polymarket lead-lag paper bot
After=network-online.target

[Service]
WorkingDirectory=/root/<repo>
ExecStart=/root/<repo>/.venv/bin/python3 scripts/paper_bot.py
Restart=always
RestartSec=5
Environment=ORDER_LATENCY_MS=300

[Install]
WantedBy=multi-user.target
```
```bash
sudo systemctl enable --now paperbot
journalctl -u paperbot -f      # live logs
```
Fallback: `nohup python3 scripts/paper_bot.py > /dev/null 2>&1 &`

## Config (env vars)
| var | default | meaning |
|---|---|---|
| `ORDER_LATENCY_MS` | 300 | modeled signal→fill delay (set to your measured reaction) |
| `STAKE` | 10 | paper $ per trade |
| `VEL_MIN` | 4 | 2-second move trigger, bps |
| `DISP_LO`,`DISP_HI` | 1,15 | displacement-from-open band, bps |
| `TTL_MIN`,`TTL_MAX` | 5,180 | entry window, seconds-to-settle |
| `ASSETS` | btc,eth | |
| `ASK_FLOOR`,`ASK_CEIL` | 0.30,0.97 | reject if book ask outside this |
| `MAX_SLIP` | 0.02 | max ask drift signal→fill; beyond this the trade is tagged `shadow` (not counted as a live trade) |

Defaults = the "balanced" config (~75 trades/day backtested).

## Outputs (in `data/`, event-driven — ~0.3 MB/day, safe for a 75 GB VPS)
- `paper_log.jsonl` — one JSON line per event (signal/fill/skip/settle), full timeline.
- `paper_trades.csv` — one row per completed trade (entry + outcome + pnl). **Analyze this.**
- `bot.log` — rotating status log (hard 30 MB cap).

## Pull results back for analysis
```bash
scp root@<vps>:/root/<repo>/data/paper_trades.csv .
```
Key columns: `fill_price, ask_at_signal, ask_at_fill, slip` (latency slippage),
`would_trade` (1 = passed the slip cap + ask range = a real live trade; 0 = shadow,
recorded only to tune the cap), `won, pnl`, `binance_agrees` (did the Binance proxy
match the real Chainlink outcome).

**Live P&L = rows where `would_trade==1`.** Shadow rows let us A/B the `MAX_SLIP`
threshold against real outcomes without guessing.

### Outcome scoring
Polymarket's public APIs publish the winner slowly for these 5m markets (minutes,
sometimes absent), so the bot scores each trade from the **Binance final-vs-open
price** at `settle + SETTLE_DELAY` (default 20s) — fast and ~98.5% in agreement
with the real outcome (higher on our displacement-filtered trades). The authoritative
Polymarket result is recorded as best-effort confirmation when available:
columns `outcome_source` (binance|polymarket), `binance_outcome`, `auth_outcome`,
`auth_agrees`. If a trade is still unscored 2 min after settle, the bot logs an
`UNSCORED ... (possible bug)` warning.

## Notes / next upgrades
- Signal currently uses Binance 1s-ish aggregated ticks; to exploit sub-second
  latency fully, compute velocity from the raw tick stream (already streamed).
- The bot REST-polls the Polymarket book only at signal & fill (your order path is
  ~15–50 ms). For higher fidelity, subscribe to the CLOB market WS.
- Validate ≥1 week before considering real capital. Watch the gap between
  `ask_at_signal` and `ask_at_fill` — that is your real latency cost.
