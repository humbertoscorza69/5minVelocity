# 5minVelocity

Research + live paper-trading bot for a **lead-lag edge in Polymarket BTC/ETH
5-minute Up/Down markets**: the order book lags Binance spot by ~1–2s, so near the
money a fresh underlying move predicts the outcome before the book reprices.

## TL;DR
- **Edge:** when the 2-second Binance move is ≥ VEL_MIN bps toward a side AND the
  underlying is displaced 1–15 bps from the window open, buy that side and hold to
  settlement. One trade per market. Backtested ~73% win / +1–4¢ per share OOS
  (June), validated IS (May), audited for look-ahead. See `docs/TAKER_SPEC.md`.
- **Bot:** `scripts/paper_bot.py` — paper-trades it live (no real orders), models
  latency + slippage, scores from Binance, logs everything compactly. See
  `README_BOT.md`.

## Quick start (paper bot)
```bash
pip install -r requirements.txt
python3 scripts/paper_bot.py        # logs to ./data/
```
Run details, systemd unit, config, and log schema: **`README_BOT.md`**.

## Docs
- `docs/TAKER_SPEC.md` — the deployable strategy spec + leakage audit.
- `docs/STRATEGY.md` — full lead-lag findings (taker + market-making).
- `docs/REPORT.md` — phase-1 research (why the naive "buy the favorite" fails).
- `docs/FILTER_STUDY.md` — phase-2 filter search.

## Layout
- `scripts/paper_bot.py` — the live paper bot (only file needed to run live).
- `scripts/*.py` — research pipeline (extraction, backtests, audits). Reference only.
- `data/` — local data & logs (gitignored; never pushed).

Research data (≈20 GB) is intentionally excluded from the repo.
