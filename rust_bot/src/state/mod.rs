//! Shared, concurrently-accessed runtime state for the ingestion layer.
//!
//! Phase 1 is read-only: the two websocket clients WRITE here, and the stats
//! loop (and, in later phases, the signal/decision engines) READ here. Access is
//! lock-free per key via `DashMap`; counters are plain atomics. There is no
//! trading state in here yet — only market data and ingestion health.

#![allow(dead_code)] // several fields are populated now, consumed in later phases

pub mod persist;
pub mod store;

use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use tokio::sync::mpsc;

use crate::signal::MarketRef;

/// Cheap-to-clone handle to the whole runtime state.
pub type Shared = Arc<SharedState>;

/// The best bid/ask as REPORTED BY THE EVENT (Polymarket semantics): for `book`
/// the top of each side (`asks[-1]`/`bids[-1]`), for `price_change`/`best_bid_ask`
/// the event's own `best_bid`/`best_ask`. This is the SINGLE source of truth for
/// prices — what the Python `state.books[token]` holds for decisions (Q2-Q4 reads
/// best_ask; staleness reads ts_ms; sizing reads best_ask; REGLA C reads best_bid).
///
/// How fresh the BBO must be for smart-exit observations to count. If the
/// last BBO update for a token (`EventBbo.ts_ms`) is older than this many
/// milliseconds vs the local clock, the position's `running_max_bid` is
/// NOT updated for this tick, and the smart-exit evaluator -- once wired
/// in a later piece -- skips the rule entirely (falls back to time-exit).
///
/// 5 s is well past normal WS jitter (Polymarket sends updates every few
/// seconds for active tokens during market hours) but short enough that
/// real gaps are detected promptly. The constant lives here -- not in
/// `trading_loop` -- so tests for any module can reference the same source
/// of truth without a back-channel duplicate. See Pieza 2 plan (Q2:
/// reconnect / gap handling) for the fail-closed rationale.
pub const STALE_BBO_MS: i64 = 5_000;

/// Phase 5 retired the depth-accumulating BookCache (Phase 1) after the Hallazgo-1
/// verification: it diverged ~6% from this event-BBO and had NO consumer of depth
/// (the operational Python uses only the BBO). See DECISIONS.md (Phase 5).
#[derive(Debug, Clone, Copy, Default)]
pub struct EventBbo {
    pub best_ask: Option<f64>,
    pub best_bid: Option<f64>,
    /// Event `timestamp` (Polymarket server ms) of the last update.
    pub ts_ms: i64,
}

/// A FINALIZED Binance kline, emitted by the WS client to the decision loop
/// (Phase 6 D1). `received_at_ms` is the ingest instant = the trade-log `t_signal`.
/// Defined here (the lowest layer) so both the WS client and the trading loop can
/// reference it without a module cycle; re-exported as `trading_loop::KlineClose`.
#[derive(Debug, Clone)]
pub struct KlineClose {
    pub asset: String,
    pub t_s: i64,
    pub close: f64,
    pub received_at_ms: i64,
}

impl EventBbo {
    /// Mid price when both sides are present (matches the old `BookCache::mid`).
    pub fn mid(&self) -> Option<f64> {
        match (self.best_ask, self.best_bid) {
            (Some(a), Some(b)) => Some((a + b) / 2.0),
            _ => None,
        }
    }
}

/// Latest Binance data for one symbol (e.g. `btcusdt`).
#[derive(Debug, Default, Clone)]
pub struct BinanceTick {
    pub symbol: String,
    /// Last aggTrade price.
    pub last_price: f64,
    /// Exchange time (ms) of the last aggTrade.
    pub last_trade_ms: i64,
    /// Close of the most recent 1s kline.
    pub kline_close: f64,
    pub kline_open_ms: i64,
    pub kline_close_ms: i64,
    /// Whether the last kline update was the final tick of its interval.
    pub kline_final: bool,
    /// Local receive time (ms) of the last update of any kind.
    pub updated_ms: i64,
}

/// Monotonic ingestion counters. Feed the ±2% message-count gate and stats.
#[derive(Debug, Default)]
pub struct Counters {
    pub binance_msgs: AtomicU64,
    pub binance_klines: AtomicU64,
    pub binance_aggtrades: AtomicU64,
    pub polymarket_msgs: AtomicU64,
    pub polymarket_book: AtomicU64,
    pub polymarket_price_change: AtomicU64,
    pub polymarket_other: AtomicU64,
    pub parse_errors: AtomicU64,
    pub reconnects: AtomicU64,
    /// Planned WS resubscribes (Polymarket token set changed → reconnect with the
    /// new list). NOT errors; kept distinct from `reconnects`.
    pub resubscribes: AtomicU64,
    /// Market-discovery REST cycles that failed (the previous token set was kept).
    pub discovery_failures: AtomicU64,
    /// Decisions evaluated by the live decision loop (Phase 6 D1).
    pub decisions: AtomicU64,
}

/// Top-level shared state. Wrapped in an `Arc` (see [`Shared`]).
#[derive(Debug)]
pub struct SharedState {
    /// Binance ticks keyed by symbol (lowercase, e.g. `btcusdt`).
    pub binance: DashMap<String, BinanceTick>,
    /// Event-reported BBO per token id (asset_id); see [`EventBbo`]. The single
    /// source of truth for Polymarket prices (decisions, sizing, REGLA C, logging).
    pub bbo: DashMap<String, EventBbo>,
    /// Active up/down markets keyed `(asset, interval, epoch)` — the live
    /// [`crate::signal::MarketCatalog`] source for the decision loop (Phase 6 D1).
    /// Populated by discovery alongside the token set.
    pub markets: DashMap<(String, String, i64), MarketRef>,
    pub counters: Counters,
    pub binance_connected: AtomicBool,
    pub polymarket_connected: AtomicBool,
    /// Latched true when a WS client exhausts its reconnect budget. The process
    /// keeps running (human-in-the-loop) but reports unhealthy.
    pub health_failed: AtomicBool,
    /// Current number of dynamically-discovered Polymarket tokens (gauge, updated
    /// by the market-discovery task).
    pub active_tokens: AtomicU64,
    /// Phase 6 D1: sink for FINALIZED Binance klines → the decision loop. Set once at
    /// startup (after the channel is built). The binance WS `handle_text` emits a
    /// `KlineClose` here on each final 1s bar; `None` until wired (paper/live only).
    pub kline_tx: OnceLock<mpsc::UnboundedSender<KlineClose>>,
    /// Wallet USDC balance in milli-dollars (balance * 1000), refreshed by
    /// `run_positions_refresh` every cycle. `-1` = not yet known (paper mode, or
    /// before the first REST balance fetch). Read by the dashboard.
    pub balance_milli: AtomicI64,
    /// v2 strategy: predicted win-probability at entry, keyed by bet token id.
    /// Written by the v2 decision path on each Open; drained by the positions-
    /// refresh recal feed when the market resolves (→ `(pred, won)` into the
    /// rolling recalibrator). Empty when v2 is disabled.
    pub v2_pred: DashMap<String, f64>,
    /// PIECE 4 (active-only enrichment): per-token RESOLUTION flag derived from
    /// the data-api `/positions` snapshot (redeemable OR cur_price ∈ {0,1} OR
    /// end_date past). Updated periodically by `run_positions_refresh`. Missing
    /// entry = conservative default (treat as active). The exposure-cap guard
    /// reads this map to drop resolved positions from the live cap denominator.
    pub resolved_tokens: DashMap<String, bool>,
    /// Phase 6 #3 (dashboard): optional operational-log sink for the WS reconnect
    /// supervisor, so connection lifecycle (ws_lost / ws_reconnecting / ws_cooldown
    /// / ws_recovered) lands in the oplog the dashboard reads. Set once at startup
    /// in trading modes; `None` in tests and pure-baseline (→ no emission, zero
    /// behavior change). Passive observability only — never read by trading.
    pub oplog: OnceLock<Arc<crate::oplog::OpLog>>,
    pub started_at: Instant,
}

impl SharedState {
    pub fn new() -> Shared {
        Arc::new(Self {
            binance: DashMap::new(),
            bbo: DashMap::new(),
            markets: DashMap::new(),
            counters: Counters::default(),
            binance_connected: AtomicBool::new(false),
            polymarket_connected: AtomicBool::new(false),
            health_failed: AtomicBool::new(false),
            active_tokens: AtomicU64::new(0),
            kline_tx: OnceLock::new(),
            balance_milli: AtomicI64::new(-1),
            v2_pred: DashMap::new(),
            resolved_tokens: DashMap::new(),
            oplog: OnceLock::new(),
            started_at: Instant::now(),
        })
    }

    /// Set the unhealthy flag. Used by the WS supervisor while it is in the slow
    /// (rate-limit / storm cool-down) regime so the dashboard can see a degraded
    /// feed. NOT a permanent latch any more — see [`Self::mark_healthy`].
    pub fn mark_health_failed(&self) {
        self.health_failed.store(true, Ordering::Relaxed);
    }

    /// Clear the unhealthy flag. Called by the WS supervisor when a session
    /// recovers to "stable" (auto-recovery): the bot rode out the outage on its
    /// own, so health returns to true with no human in the loop.
    pub fn mark_healthy(&self) {
        self.health_failed.store(false, Ordering::Relaxed);
    }

    pub fn is_healthy(&self) -> bool {
        !self.health_failed.load(Ordering::Relaxed)
    }

    /// Install the oplog sink the WS supervisor uses for connection-lifecycle
    /// events (idempotent — first set wins). Passive observability for the
    /// dashboard; never affects trading.
    pub fn set_oplog(&self, oplog: Arc<crate::oplog::OpLog>) {
        let _ = self.oplog.set(oplog);
    }

    /// The installed oplog sink, if any (see [`Self::set_oplog`]).
    pub fn oplog(&self) -> Option<&Arc<crate::oplog::OpLog>> {
        self.oplog.get()
    }
}

/// Wall-clock milliseconds since the Unix epoch.
pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
