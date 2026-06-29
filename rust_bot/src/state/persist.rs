//! Phase 3: persistent bot state (positions, recent signals, balance, launch
//! history) + crash-safe snapshot/restore.
//!
//! This is SEPARATE from [`crate::state::SharedState`] (Phase 1 ingestion state:
//! BookCache, Binance ticks, counters). That state is reconstructed from the live
//! feeds at startup and is NOT persisted. THIS state is the trading state that
//! must survive a crash, so it is snapshotted to disk atomically.
//!
//! Snapshot/restore + reconciliation land after the struct review (STOP 1); this
//! file currently defines the serializable state model + its in-memory trimming.

#![allow(dead_code)] // positions/signals are written by the engines in Phase 4-6

use std::collections::VecDeque;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Schema version of the persisted state. Bump when the on-disk shape changes so
/// a future load can migrate rather than silently mis-parse.
///
/// v2 (Phase 5): `OpenPosition` gained `exit_ts_s` (the time-based exit clock).
/// v3 (Pieza 1): `OpenPosition` gained `entry_ts_ms`, `running_max_bid`, and
///     `ts_max_bid_ms` (the per-position state needed by smart-exit rules from
///     the G15 investigation). v2 positions migrate via serde defaults to
///     sentinels (all zeros) so the live exit task -- once the wiring lands in
///     a later piece -- falls back to Baseline for them (no smart exit on
///     pre-migration positions, no premature exit risk).
/// v4 (Pieza 4): `OpenPosition` gained `asset` (e.g. "BTC" / "ETH"). The
///     smart-exit evaluator needs `(asset, interval)` to look up the per-cell
///     rule in `bot.toml [exits.cells]`. v3 positions migrate to `asset = ""`
///     (sentinel). The evaluator treats an empty asset OR `entry_ts_ms == 0`
///     as smart-ineligible -> fall back to Baseline (time-exit). Same
///     fail-closed pattern as v3's sentinels: no risk of smart-exit firing
///     on a position with incomplete state.
/// v5 (COMBO Phase 3 Pieza 3): `OpenPosition` gained `maker_exit:
///     Option<MakerExitState>` — the tracking handle for a B3 maker SELL
///     limit order (place GTC+post_only at entry+delta, cancel-on-timeout,
///     reconcile fill). `None` = no maker order outstanding (baseline cells,
///     or before the exit task places one). v4 positions migrate to `None`
///     via serde default — they have no maker order, exactly the correct
///     state. SCHEMA-ONLY: no consumer yet (the exit-task wiring is Pieza 1).
pub const SCHEMA_VERSION: u32 = 5;

/// In-memory cap for the recent-signals dedup ring.
pub const MAX_RECENT_SIGNALS_MEM: usize = 1000;
/// How many recent signals are persisted (the tail). The full ring is for live
/// dedup; only a window needs to survive a restart.
pub const MAX_RECENT_SIGNALS_PERSIST: usize = 100;
/// How many launch records are kept (for the first-live confirmation defense).
pub const MAX_MODE_HISTORY: usize = 5;

/// Which outcome token a position holds. Polymarket binary markets are Up/Down;
/// the bot only ever BUYS one side (no shorting), so the position side IS the
/// outcome held.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Outcome {
    Up,
    Down,
}

/// Hybrid fill-flow status. An order moves Pending → TentativeFilled (CLOB acked
/// it / returned an orderID) → Confirmed (the fill is corroborated on-chain via
/// `confirmation_source`). `Failed` = rejected; `Unknown` = indeterminate (e.g.
/// the ack was lost) and needs reconciliation / manual escalation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderStatus {
    Pending,
    TentativeFilled,
    Confirmed,
    Failed,
    Unknown,
}

/// Where a fill confirmation came from (populated when status → Confirmed).
///
/// CONFIDENCE / PRECEDENCE (documented for Phase 6 — NO resolution logic yet).
/// When a fill is corroborated by more than one source, Phase 6 resolves it by
/// this confidence ranking (highest first):
///
///   UserWs > OnChain > ClobTrade > DataApiPositions
///
/// * `UserWs` — fastest + most authoritative for fills (the CLOB user channel
///   pushes the fill in real time). Drives the Pending → Confirmed transition.
/// * `OnChain` — most definitive (settlement) but slow (block latency); confirms
///   when UserWs is unavailable.
/// * `ClobTrade` — the CLOB `/trades` endpoint view; a corroborating cross-check.
/// * `DataApiPositions` — has the documented data-api lag (Phase 2); lowest
///   confidence, corroboration only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConfirmationSource {
    /// data-api `/positions` reflected the position (has the documented lag).
    DataApiPositions,
    /// CLOB order/trade status reported matched/confirmed.
    ClobTrade,
    /// An on-chain transaction hash was observed (most definitive, slowest).
    OnChain,
    /// The CLOB user websocket pushed a fill event (fastest, authoritative).
    UserWs,
}

/// Lifecycle of a B3 maker exit order (COMBO Phase 3, schema v5).
/// SCHEMA-ONLY for now; the transitions are driven by the exit task in
/// Pieza 1/4 (place -> reconcile -> timeout-cancel -> fallback). The variant
/// set is kept minimal + extensible; new states are an additive serde change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MakerExitStatus {
    /// Working on the book (GTC + post_only), not yet (fully) filled.
    Placed,
    /// Fully filled at `target` (maker). The position is closing/closed.
    Filled,
    /// Cancelled on timeout with NO fill; the F2_market fallback handles the
    /// full size.
    Cancelled,
    /// Cancelled after a PARTIAL fill: the filled part closed at `target`
    /// (maker), the remainder went to the F2_market fallback (taker).
    PartialThenFallback,
}

/// Tracking state for an outstanding B3 maker SELL limit order (schema v5).
///
/// `Some` on an [`OpenPosition`] means a maker exit is (or was) working at
/// `target`; `None` means no maker exit was placed. The four fields are ONE
/// struct (not four loose `Option`s) on purpose: a maker order either exists
/// with all of (order_id, placed_ms, target, status) or it does not — an
/// `Option<MakerExitState>` makes the inconsistent in-between (e.g. an
/// order_id with no target) UNREPRESENTABLE.
///
/// DISTINCT from [`OpenPosition::order_id`], which is the BUY order id. This
/// is the EXIT maker order. Do not conflate them.
///
/// CONCURRENCY (the rule from the Phase 3 plan): the exit task mutates this
/// under the `store_state` lock, but the place/cancel/reconcile network calls
/// happen with the lock RELEASED (lock -> read snapshot -> unlock -> await
/// SDK -> lock -> write result). The field is plain owned data so it copies
/// out cleanly for that pattern. (Schema-only here; the pattern lands in
/// Pieza 1/4.)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MakerExitState {
    /// CLOB order id of the maker SELL limit (from `PostOrderResponse.order_id`).
    /// The handle used to cancel (`cancel_order`) + reconcile (`Client::order`).
    pub order_id: String,
    /// Wall-clock ms when the maker order was placed.
    pub placed_ms: i64,
    /// The limit price = clamp(entry_price + delta, [tick, 1-tick]); the maker
    /// fills here.
    pub target: f64,
    /// Lifecycle of the maker order.
    pub status: MakerExitStatus,
}

/// An open position, keyed in [`BotState::positions`] by its token id.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpenPosition {
    /// The outcome token id held (also the map key; stored for self-containment).
    pub token_id: String,
    /// Asset ticker (e.g. "BTC", "ETH"). Combined with `interval` it forms the
    /// cell key (`"BTC:5m"`) used to look up the per-cell exit rule in
    /// `bot.toml [exits.cells]`. v4 field; v3 migrations default to "" which
    /// the smart-exit evaluator treats as ineligible (fall back to Baseline).
    #[serde(default)]
    pub asset: String,
    /// Which outcome (Up/Down) this position is long.
    pub side: Outcome,
    /// Average entry price (USDC per share). Decimal — exact, never f64 for money.
    pub entry_price: Decimal,
    /// Shares held. Decimal — exact.
    pub shares: Decimal,
    /// When the position was opened (wall-clock ms).
    pub opened_at_ms: i64,
    /// The signal that triggered the entry (links to [`RecentSignal::signal_id`]).
    pub signal_id: String,
    /// Market interval ("5m" / "15m").
    pub interval: String,
    /// Wall-clock second at which the time-based exit fires: the entry
    /// `trigger_ts` + the stratum hold (RW 120s / IM 300s). The PaperExecutor
    /// closes the position at `best_bid` once `now >= exit_ts_s` (Phase 5).
    pub exit_ts_s: i64,

    // ---- v3 (Pieza 1): smart-exit per-position state ----
    //
    // Sentinel value of 0 in any of these three fields means "pre-migration
    // position (or pre-Pieza-2 open path)": the smart-exit evaluator MUST
    // detect that and fall back to Baseline for this position. The check
    // lives at the rule-evaluation site (a later piece); the state layer
    // just stores the values and provides `#[serde(default)]` so v2 JSON
    // loads cleanly without these fields present.
    /// Wall-clock ms when the position was opened (entry). Used by smart/f1
    /// to compute `(now - entry) / hold_duration`. 0 = sentinel "not set".
    #[serde(default)]
    pub entry_ts_ms: i64,
    /// Running maximum of the position's bid price observed since open. The
    /// 1s exit tick refreshes it from the live BBO. 0.0 = sentinel "not set".
    #[serde(default)]
    pub running_max_bid: f64,
    /// Wall-clock ms when `running_max_bid` was last set (used by f6 to
    /// compute time-since-running-max). 0 = sentinel "not set".
    #[serde(default)]
    pub ts_max_bid_ms: i64,

    // ---- hybrid fill flow ----
    pub status: OrderStatus,
    /// CLOB order id, once the order is acked.
    pub order_id: Option<String>,
    /// When the CLOB acked the order (ms).
    pub ack_at_ms: Option<i64>,
    /// When the fill was confirmed on-chain (ms).
    pub confirmed_at_ms: Option<i64>,
    /// What corroborated the fill (set with `confirmed_at_ms`).
    pub confirmation_source: Option<ConfirmationSource>,

    // ---- v5 (COMBO Phase 3 Pieza 3): B3 maker exit order tracking ----
    /// The outstanding B3 maker SELL limit order, if any. `None` = no maker
    /// exit placed (baseline cells, or before the exit task places one).
    /// v4 positions migrate to `None` via serde default (no maker order =
    /// exactly correct). SCHEMA-ONLY: no consumer yet (Pieza 1 wires it).
    /// See [`MakerExitState`] for the all-or-nothing invariant + the
    /// distinction from `order_id` (the BUY order).
    #[serde(default)]
    pub maker_exit: Option<MakerExitState>,
}

/// A recent decision signal, kept for de-duplication (don't act twice on the same
/// market/offset). The full set of dedup keys firms up in Phase 4-5; this carries
/// enough to identify a `(market, offset, side)` decision.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RecentSignal {
    /// Unique signal id (the dedup primary key).
    pub signal_id: String,
    /// Market slug the signal was for.
    pub market_slug: String,
    /// Token id chosen.
    pub token_id: String,
    /// Outcome chosen.
    pub side: Outcome,
    /// Market interval ("5m" / "15m").
    pub interval: String,
    /// Decision offset (seconds since the market epoch).
    pub offset_s: i64,
    /// When the signal was created (ms).
    pub created_at_ms: i64,
}

/// One bot launch — the first-live confirmation defense reads these.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LaunchRecord {
    /// "paper" | "live".
    pub mode: String,
    /// Bot version at launch (`CARGO_PKG_VERSION`).
    pub version: String,
    /// When this launch happened (ms).
    pub launched_at_ms: i64,
}

/// The full persistent trading state. Serialized to `state.json` (atomically).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BotState {
    /// Persisted-shape version (see [`SCHEMA_VERSION`]).
    pub schema_version: u32,
    /// Open position LOTS — one entry per fire (DCA = multiple lots on the same
    /// token). On-chain Polymarket aggregates these into a single position
    /// (`/positions`: one entry per token, avg price); `Σ lots per token` = that
    /// aggregate, and each lot exits independently at its own `exit_ts_s` (faithful
    /// to the Python `position_close_loop`). See DECISIONS.md (Phase 5).
    pub positions: Vec<OpenPosition>,
    /// Recent signals (newest at the back), for dedup.
    pub recent_signals: VecDeque<RecentSignal>,
    /// Last known collateral balance estimate (USDC). Decimal — exact.
    pub balance_usdc: Decimal,
    /// Last launches (newest at the back), capped at [`MAX_MODE_HISTORY`].
    pub mode_history: VecDeque<LaunchRecord>,
}

impl Default for BotState {
    fn default() -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            positions: Vec::new(),
            recent_signals: VecDeque::new(),
            balance_usdc: Decimal::ZERO,
            mode_history: VecDeque::new(),
        }
    }
}

impl BotState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a signal, trimming the ring to [`MAX_RECENT_SIGNALS_MEM`].
    pub fn push_signal(&mut self, sig: RecentSignal) {
        self.recent_signals.push_back(sig);
        while self.recent_signals.len() > MAX_RECENT_SIGNALS_MEM {
            self.recent_signals.pop_front();
        }
    }

    /// True if this signal id was already seen (the dedup primary key).
    pub fn has_signal(&self, signal_id: &str) -> bool {
        self.recent_signals.iter().any(|s| s.signal_id == signal_id)
    }

    /// Record a launch, trimming history to [`MAX_MODE_HISTORY`].
    pub fn record_launch(&mut self, mode: &str, version: &str, launched_at_ms: i64) {
        self.mode_history.push_back(LaunchRecord {
            mode: mode.to_string(),
            version: version.to_string(),
            launched_at_ms,
        });
        while self.mode_history.len() > MAX_MODE_HISTORY {
            self.mode_history.pop_front();
        }
    }

    /// Has the bot ever launched in live mode? Drives the first-live interactive
    /// confirmation: the FIRST ever live launch must be confirmed by the operator.
    pub fn has_launched_live(&self) -> bool {
        self.mode_history.iter().any(|l| l.mode == "live")
    }

    /// A clone trimmed for persistence: only the last [`MAX_RECENT_SIGNALS_PERSIST`]
    /// signals are written (the in-memory ring is larger for live dedup).
    pub fn for_persist(&self) -> BotState {
        let keep = self.recent_signals.len().saturating_sub(MAX_RECENT_SIGNALS_PERSIST);
        BotState {
            schema_version: self.schema_version,
            positions: self.positions.clone(),
            recent_signals: self.recent_signals.iter().skip(keep).cloned().collect(),
            balance_usdc: self.balance_usdc,
            mode_history: self.mode_history.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn sample_position() -> OpenPosition {
        OpenPosition {
            token_id: "12345".to_string(),
            asset: "BTC".to_string(),
            side: Outcome::Up,
            entry_price: dec!(0.53),
            shares: dec!(12.5),
            opened_at_ms: 1_780_000_000_000,
            signal_id: "sig-1".to_string(),
            interval: "5m".to_string(),
            exit_ts_s: 1_780_000_120,
            // v3 Pieza 1: sentinel state. Production opens (later piece) will
            // populate these; the existing roundtrip test below relies on
            // these being non-default to prove the field IS persisted.
            entry_ts_ms: 1_780_000_000_500,
            running_max_bid: 0.55,
            ts_max_bid_ms: 1_780_000_017_000,
            status: OrderStatus::TentativeFilled,
            order_id: Some("0xorder".to_string()),
            ack_at_ms: Some(1_780_000_000_100),
            confirmed_at_ms: None,
            confirmation_source: None,
            maker_exit: None,
        }
    }

    fn sample_signal(id: &str) -> RecentSignal {
        RecentSignal {
            signal_id: id.to_string(),
            market_slug: "btc-updown-5m-1780000000".to_string(),
            token_id: "12345".to_string(),
            side: Outcome::Up,
            interval: "5m".to_string(),
            offset_s: 60,
            created_at_ms: 1_780_000_000_000,
        }
    }

    #[test]
    fn serde_roundtrip_bit_exact() {
        let mut s = BotState::new();
        s.positions.push(sample_position());
        // add a confirmed position too (exercise the Some(confirmation_source) path)
        let mut p2 = sample_position();
        p2.token_id = "67890".to_string();
        p2.side = Outcome::Down;
        p2.status = OrderStatus::Confirmed;
        p2.confirmed_at_ms = Some(1_780_000_001_000);
        p2.confirmation_source = Some(ConfirmationSource::DataApiPositions);
        s.positions.push(p2);
        s.push_signal(sample_signal("sig-1"));
        s.push_signal(sample_signal("sig-2"));
        s.balance_usdc = dec!(104.513264);
        s.record_launch("paper", "0.1.0", 1_780_000_000_000);

        let json = serde_json::to_string(&s).unwrap();
        let back: BotState = serde_json::from_str(&json).unwrap();
        assert_eq!(s, back, "deserialized state must equal the original");
    }

    #[test]
    fn recent_signals_trim_to_mem_cap() {
        let mut s = BotState::new();
        for i in 0..(MAX_RECENT_SIGNALS_MEM + 250) {
            s.push_signal(sample_signal(&format!("sig-{i}")));
        }
        assert_eq!(s.recent_signals.len(), MAX_RECENT_SIGNALS_MEM);
        // Oldest trimmed; newest retained.
        assert!(s.has_signal(&format!("sig-{}", MAX_RECENT_SIGNALS_MEM + 249)));
        assert!(!s.has_signal("sig-0"));
    }

    #[test]
    fn for_persist_keeps_only_last_100_signals() {
        let mut s = BotState::new();
        for i in 0..500 {
            s.push_signal(sample_signal(&format!("sig-{i}")));
        }
        let p = s.for_persist();
        assert_eq!(p.recent_signals.len(), MAX_RECENT_SIGNALS_PERSIST);
        // The persisted tail is the newest 100.
        assert_eq!(p.recent_signals.back().unwrap().signal_id, "sig-499");
        assert_eq!(p.recent_signals.front().unwrap().signal_id, "sig-400");
    }

    #[test]
    fn mode_history_caps_and_first_live_detection() {
        let mut s = BotState::new();
        assert!(!s.has_launched_live());
        for i in 0..7 {
            s.record_launch("paper", "0.1.0", 1_780_000_000_000 + i);
        }
        assert_eq!(s.mode_history.len(), MAX_MODE_HISTORY);
        assert!(!s.has_launched_live());
        s.record_launch("live", "0.1.0", 1_780_000_010_000);
        assert!(s.has_launched_live());
    }

    // ========================================================================
    // v3 (Pieza 1) schema tests: the 3 new OpenPosition fields serialize
    // round-trip and -- critically -- default to sentinel zeros when missing
    // from old JSON (the v2 -> v3 migration relies on this).
    // ========================================================================

    #[test]
    fn position_v3_serde_roundtrip() {
        // sample_position() sets the 3 new fields to non-default values; if
        // serde silently dropped them, the round-trip would compare unequal.
        let p = sample_position();
        assert_ne!(p.entry_ts_ms, 0, "fixture must use non-sentinel value");
        assert_ne!(p.running_max_bid, 0.0, "fixture must use non-sentinel value");
        assert_ne!(p.ts_max_bid_ms, 0, "fixture must use non-sentinel value");

        let json = serde_json::to_string(&p).expect("serialize");
        // The JSON output must mention the 3 new keys explicitly.
        assert!(json.contains("\"entry_ts_ms\""), "missing entry_ts_ms in JSON: {json}");
        assert!(json.contains("\"running_max_bid\""), "missing running_max_bid in JSON: {json}");
        assert!(json.contains("\"ts_max_bid_ms\""), "missing ts_max_bid_ms in JSON: {json}");

        let back: OpenPosition = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(p, back, "round-trip must preserve all v3 fields");
    }

    // ========================================================================
    // v4 (Pieza 4) schema tests: the new OpenPosition.asset field serializes
    // round-trip and -- critically -- defaults to empty string when missing
    // from a v3-shape JSON (the v3 -> v4 migration relies on this).
    // ========================================================================

    #[test]
    fn position_v4_serde_roundtrip() {
        let p = sample_position();
        assert_eq!(p.asset, "BTC", "fixture must use non-default asset");
        let json = serde_json::to_string(&p).expect("serialize");
        assert!(json.contains("\"asset\""), "missing asset key in JSON: {json}");
        let back: OpenPosition = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(p, back, "round-trip must preserve asset");
    }

    #[test]
    fn position_v4_asset_defaults_when_missing() {
        // v3-shape JSON (no `asset`) MUST deserialize into v4 OpenPosition
        // with asset = "" (sentinel: smart-exit ineligible).
        let v3_json = r#"{
            "token_id": "12345",
            "side": "up",
            "entry_price": "0.53",
            "shares": "12.5",
            "opened_at_ms": 1780000000000,
            "signal_id": "sig-1",
            "interval": "5m",
            "exit_ts_s": 1780000120,
            "entry_ts_ms": 1780000000500,
            "running_max_bid": 0.55,
            "ts_max_bid_ms": 1780000017000,
            "status": "tentative_filled",
            "order_id": "0xorder",
            "ack_at_ms": 1780000000100,
            "confirmed_at_ms": null,
            "confirmation_source": null
        }"#;
        let p: OpenPosition = serde_json::from_str(v3_json)
            .expect("v3-shape JSON must deserialize into v4 via serde defaults");
        assert_eq!(p.asset, "", "missing asset must default to empty string");
        // The v3 fields carried over unchanged.
        assert_eq!(p.entry_ts_ms, 1_780_000_000_500);
        assert_eq!(p.running_max_bid, 0.55);
        assert_eq!(p.ts_max_bid_ms, 1_780_000_017_000);
        assert_eq!(p.exit_ts_s, 1_780_000_120);
    }

    #[test]
    fn position_v3_defaults_when_missing() {
        // A v2-shape JSON (no entry_ts_ms / running_max_bid / ts_max_bid_ms)
        // MUST deserialize cleanly with the 3 new fields populated as
        // sentinels (0 / 0.0 / 0). This is the contract the migration relies
        // on; if a refactor drops `#[serde(default)]` on these fields, old
        // state.json files would refuse to load -- breaking restart.
        let v2_json = r#"{
            "token_id": "12345",
            "side": "up",
            "entry_price": "0.53",
            "shares": "12.5",
            "opened_at_ms": 1780000000000,
            "signal_id": "sig-1",
            "interval": "5m",
            "exit_ts_s": 1780000120,
            "status": "tentative_filled",
            "order_id": "0xorder",
            "ack_at_ms": 1780000000100,
            "confirmed_at_ms": null,
            "confirmation_source": null
        }"#;
        let p: OpenPosition = serde_json::from_str(v2_json)
            .expect("v2-shape JSON must deserialize into v3 OpenPosition via serde defaults");
        assert_eq!(p.entry_ts_ms, 0, "missing field must default to 0 (sentinel)");
        assert_eq!(p.running_max_bid, 0.0, "missing field must default to 0.0 (sentinel)");
        assert_eq!(p.ts_max_bid_ms, 0, "missing field must default to 0 (sentinel)");
        // The rest of the v2 data carries over unchanged.
        assert_eq!(p.token_id, "12345");
        assert_eq!(p.exit_ts_s, 1_780_000_120);
    }
}
