# Order #14 — Binance feed watchdog + honest health (BLOCKER)

**Priority: ship before anything else.** The bot has been running blind for 45 hours
and reporting itself healthy. Nothing else in the queue matters until this is fixed;
the paper audition cannot resume and a live re-arm on this build would be dangerous.

Author: auditor session, 2026-07-27. Evidence: `livelogs/paperlogs_20260726_2310.tar.gz`.

---

## Context — what happened

At **2026-07-25 02:26:12 UTC** the Binance WS stopped delivering. The bot did not
notice. As of the export at Jul 26 23:10 it had produced **zero klines and zero
trading decisions for 45 hours** while reporting `healthy=true`.

Receipts (all from the export, reproducible):

| Fact | Value |
|---|---|
| Last `kline_received` | Jul 25 02:26:12 |
| Last `v2_intent_open` | Jul 25 02:16:48 |
| Last **any** `binance_ws` lifecycle event (`ws_lost`/`ws_reconnecting`/`ws_reconnect`) | **Jul 22 22:09:03** |
| `polymarket_ws` events over the same window | continuous, 741 reconnect attempts, ~17/hr |
| `kline_received` per day, Jul 18–24 | 172,800/day (= 2 assets × 86,400 s, full coverage) |
| `kline_received` on Jul 25 | 17,546 (exactly up to 02:26) |
| Journal `stats` at Jul 26 23:10:38 | `bn_connected=true pm_connected=true healthy=true bn_msgs=1584683 bn_klines=205714` |
| `bn_msgs` / `bn_klines` across the whole 45 h (sampled every 20 min) | **frozen at those identical values** |
| `state.json` positions | `[]` — flat since the last settle at 02:20, nothing stranded |

So: the socket was believed connected, the counters were frozen, no error was raised,
no reconnect was attempted, and no alert fired.

## Root cause (code-read, not inferred)

`rust_bot/src/ws/binance.rs:128`

```rust
let end = loop {
    tokio::select! {
        changed = shutdown.changed() => { ... }
        msg = read.next() => match msg {
            Some(Ok(Message::Text(txt))) => handle_text(&txt, state, logger),
            Some(Ok(Message::Ping(payload))) => { let _ = write.send(Message::Pong(payload)).await; }
            Some(Ok(Message::Close(frame))) => break SessionEnd::Lost(...),
            Some(Ok(_)) => {}
            Some(Err(e)) => break SessionEnd::Lost(format!("read error: {e}")),
            None => break SessionEnd::Lost("stream ended".into()),
        }
    }
};
```

There is **no timeout branch**. Every exit path requires the stream to *produce
something* — an error, a close frame, or EOF. On a half-open TCP connection (server
gone, no FIN, no RST — the classic cloud/NAT idle-drop) `read.next()` simply never
resolves. The loop parks forever.

Consequences chain from there:
- `state.binance_connected` is set `true` at `binance.rs:115` and cleared only at
  `:123` / `:151`, both downstream of a `SessionEnd` that never happens → stays `true`.
- `Shared::is_healthy()` (`rust_bot/src/state/mod.rs:274`) is just
  `!self.health_failed.load(...)`, and `health_failed` is only set by the WS supervisor
  on an *observed* session failure → stays `false` → `healthy=true`.
- The dashboard's "feed stale / halted" alert (`rust_bot/src/dashboard.rs:638`) is
  driven by that same `is_healthy()` → never fires.

Note the asymmetry that explains why Polymarket survived and Binance didn't: the PM
client got a 15 s keepalive ping in commit `cc61a53`. Binance never did. Binance's
server normally pings every ~20 s (handled at `binance.rs:138`), so under a healthy
connection there is always traffic — which is precisely why an idle-timeout is both
safe and sufficient here.

---

## Part A — idle-timeout watchdog on the Binance read loop (the fix)

In `rust_bot/src/ws/binance.rs`, add a staleness branch to the `select!`. Track the
instant of the last inbound frame (any frame — text, ping, pong, binary) and break
`SessionEnd::Lost` when it exceeds the threshold.

```rust
let idle_limit = Duration::from_secs(cfg.binance_idle_timeout_s.max(5) as u64);
let mut ticker = tokio::time::interval(Duration::from_secs(1));
let mut last_frame = Instant::now();

let end = loop {
    tokio::select! {
        changed = shutdown.changed() => { ... unchanged ... }
        _ = ticker.tick() => {
            if last_frame.elapsed() >= idle_limit {
                break SessionEnd::Lost(format!(
                    "idle timeout: no frame for {}s", last_frame.elapsed().as_secs()
                ));
            }
        }
        msg = read.next() => {
            last_frame = Instant::now();      // set on EVERY arm below
            match msg { ... unchanged ... }
        }
    }
};
```

Sizing the threshold: normal inter-message gap is well under 1 s (2 symbols × 1 s
klines + aggTrades = ~1.6 M messages per session). Server pings arrive every ~20 s
even if trading were silent. **Default `binance_idle_timeout_s = 30`** — ~30× the
normal gap, comfortably above the ping interval, and 5,400× faster than the 45 h this
incident actually took.

Returning `SessionEnd::Lost` puts it on the existing reconnect path, which is known to
work (285 `binance_ws` `ws_lost` events historically). No new reconnect logic needed.

**Also:** mirror the PM keepalive — send a WS Ping every 15 s. This converts a silent
half-open socket into an observable write error on many failure modes, giving a second
independent detector.

## Part B — health must measure DATA, not the socket flag

`binance_connected` records "we opened a socket and haven't seen it fail." That is not
health. Add a data-liveness signal and make `is_healthy()` depend on it.

1. Add `last_kline_ms: AtomicI64` to `Shared` (`state/mod.rs`), stamped in
   `handle_text` (`binance.rs:~257`) wherever `counters.binance_klines` is incremented.
2. Add `Shared::feed_stale_ms(now) -> i64` and fold it into `is_healthy()`:
   feed is unhealthy when `now - last_kline_ms > binance_feed_dead_ms` (default
   **60_000**; there is precedent in `guards.rs:49 feed_dead_ms: 30_000` for the PM
   price_change feed — this is the Binance-side twin, which never existed).
3. Emit the staleness in the `stats` line and in `/api/stats` so it is visible without
   inference: `bn_kline_age_s`.

Guard interaction to preserve: entries must not fire on a stale ring. Confirm that a
stale feed cannot produce an intent (it currently cannot, because no kline means no
decision tick — but once the watchdog forces reconnects, a *partially* recovered ring
must not be trusted). The existing `frozen_tape_secs=3` gate covers the normal case;
verify it also rejects a post-reconnect cold ring.

## Part C — automated action, not a banner (per the standing rule)

A dashboard pill nobody is looking at did not save us, and the project rule is that a
protection requiring a human will not be executed. So:

1. **Event:** emit `feed_dead` to the oplog on transition into the stale state
   (fields: `ws`, `last_kline_ms`, `age_s`), and `feed_recovered` on exit. These are
   the audit trail; today there is literally no record of a 45-hour outage.
2. **Auto-action:** on `feed_dead`, force `trading_enabled = false` for entries (halt
   new opens; do NOT touch exits or settlement) and set the health pill red. Restore
   automatically on `feed_recovered` plus a warmup of `>= vol_lookback_s` seconds of
   fresh klines, so z/vol are computed on a full ring, never a partial one.
3. **Escalation:** if stale > 10 min, write an `alerts/` file (the existing
   `paths.alert_dir` mechanism) so the outage is visible outside the dashboard.

## Part D — reconcile controls.json with the config (separate, real bug)

`controls.json` in the export reads:

```json
{"trading_enabled":true,"base_usd":1.05,"max_pos":3.15,"base_usd_15m":1.05,
 "max_pos_15m":3.15,"inval_stop_on":true,"inval_stop_dry":false,"reentry_opp_on":true}
```

`inval_stop_dry:false` **silently overrode** Order #12 C, which set
`inval_stop_dry_run = true` in `bot_v2.toml`. The stop has been firing real paper sells
for the entire audition — 2,175 `inval_stop` events, all `dry_run=false`.

Do **not** "fix" this by forcing the config to win. Instead make the override *visible*:

1. At startup and on every `controls.json` reload, log and oplog a
   `controls_override` event for **every** field where the control differs from the
   config value, with both values.
2. Surface a persistent dashboard line: "controls.json overriding: inval_stop_dry
   (config true → control false)".
3. Add the effective (post-override) values to `decision_loop_start`'s payload so any
   future log analysis can read the *actual* running config instead of inferring it.

**On the stop itself, see the recommendation below — do not re-dry it yet.**

---

## Tests

- `binance.rs`: unit test that a session whose stream yields `Pending` forever returns
  `SessionEnd::Lost("idle timeout...")` within ~`idle_timeout_s` (drive with
  `tokio::time::pause()`/`advance()` so it runs instantly).
- `binance.rs`: test that a stream delivering a frame every `idle_limit/2` never times
  out (regression against a too-tight threshold).
- `state/mod.rs`: `is_healthy()` false when `last_kline_ms` is older than
  `binance_feed_dead_ms`, true when fresh; transition emits exactly one `feed_dead`.
- Part C: entries blocked while stale; re-enabled only after warmup seconds of klines.
- Part D: a controls/config divergence emits exactly one `controls_override` per field
  per reload, and none when they agree.
- Full suite must stay green (534 tests as of `6858deb`).

## Deploy

```bash
git pull && cargo build --release && systemctl restart velocitybot
```

No recal reset. **Do not touch** `recal.json` / `recal_15m.json` — the 5m window is a
full n=300 and the 15m window is at n=140 mid-verdict; resetting either destroys the
audition. Floors, knots, `edge_min`, `z_min`, curves: all untouched by this order.

**Verify within 10 minutes of restart:** `bn_kline_age_s` present and < 5 in the stats
line; `kline_received` flowing at ~172,800/day; a `controls_override` event for
`inval_stop_dry`; intents resuming. Then kill the network to the Binance host (or
`iptables -A OUTPUT -d <binance-ip> -j DROP`) and confirm `feed_dead` fires within
~60 s, entries halt, and recovery is clean when the rule is removed.

## What NOT to touch

- The recal windows (see above) — the audition is mid-flight.
- The regime floors (`disp_floor_bps`, `vol60_floor`), `z_min`, `edge_min`, `min_ask`,
  `max_ttl_s`, the calibration knots, `vol_lookback_s`. All out of scope.
- The invalidation stop's *behaviour* — Part D only makes the override visible.
- The Polymarket WS reconnect path. It is noisy (~17/hr) but it works; that is a
  separate ops item and must not be bundled into a blocker fix.

---

## Recommendation attached to this order: do NOT re-dry the stop

Order #12 C dried the stop because its probation read −0.001/stop. Because the control
override meant it never actually went dry, we have an unplanned but clean A/B on the
**floored paper population** — and it reverses the verdict:

| Paper-era stops (n=696) | |
|---|---|
| Realized (stop-sold) | −$322.85 |
| Counterfactual hold | −$387.44 |
| **Stop dEV** | **+$64.59 = +0.0928/stop** |
| Saved / whipsawed | 472 (+$279.26) / 224 (−$214.66) |
| Jul 23 / Jul 24 dEV | +$23.12 / +$33.44 |

It passes the re-registered bar (rolling net dEV/stop > 0) decisively, and it earns
most on the worst day. The old −0.001 was measured on a different, pre-floor
population — the recurring population error this project keeps paying for.

**Caveat that keeps this a recommendation and not a shipped decision:** paper sells
fill at the *model* bid, so this run does not test sell-fill realism. Live history said
fills were pristine (−0.3c haircut), but that measurement is old. Keep the stop armed,
leave `inval_stop_dry_run` alone, and re-register the gauge on the floored population
with realized-vs-model fill as the kill metric once real money is back on.
