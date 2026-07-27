//! rust_bot â€” Polymarket lag-arbitrage trading bot (Rust port).
//!
//! Phase 1 = read-only websocket ingestion. The Binance and Polymarket clients
//! stream market data into shared state under a reconnect supervisor; the other
//! seven tasks remain placeholders that gain real logic in later phases. Still
//! no REST, no signing, no orders.

mod backtest_tp;
mod bbo_dump;
mod canary;
mod capa_b;
mod config;
mod dashboard;
mod b3_live;
mod b3_maker;
mod decision;
mod discovery;
mod events;
mod exec;
mod exit_rules;
mod feed_watchdog;
mod guards;
mod hold_recovery;
mod idempotency;
mod live_backend;
mod live_executor;
mod live_test;
mod logging;
mod oplog;
mod pnl_recorder;
mod redemption;
mod relayer;
mod rest;
mod sign_parity;
mod signal;
mod state;
mod tasks;
mod trade_log;
mod trading_loop;
mod v2;
mod variants;
mod window_logger;
mod ws;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use crate::config::Config;

/// Hard upper bound on position size. Enforced with `assert!` at startup before
/// any task spawns â€” no config or env value may exceed it.
const MAX_POSITION_USDC_HARD_CAP: f64 = 100.0;

/// Credential env vars. In Phase 0 these are presence-checked only (to confirm
/// `.env` loads) and never read into the program beyond a boolean â€” values are
/// NEVER logged or used.
const CREDENTIAL_KEYS: [&str; 5] = [
    "POLYMARKET_PRIVATE_KEY",
    "POLYMARKET_API_KEY",
    "POLYMARKET_API_SECRET",
    "POLYMARKET_API_PASSPHRASE",
    "POLYMARKET_FUNDER_ADDRESS",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum Mode {
    Paper,
    Live,
}

impl Mode {
    fn as_str(self) -> &'static str {
        match self {
            Mode::Paper => "paper",
            Mode::Live => "live",
        }
    }
}

#[derive(Debug, Parser)]
#[command(name = "rust_bot", version, about = "Polymarket lag-arb bot (Rust)")]
struct Cli {
    /// Trading mode; overrides [mode].default in the config.
    #[arg(long, value_enum)]
    mode: Option<Mode>,

    /// Path to the TOML config file.
    #[arg(long, default_value = "config/bot.toml")]
    config: PathBuf,

    /// Parse + validate config, log effective settings (no secrets), then exit 0.
    #[arg(long)]
    dry_run: bool,

    /// Phase 2 probe: build the read-only REST client, query every endpoint once
    /// against live Polymarket (read-only), log the results, then exit 0.
    #[arg(long)]
    rest_check: bool,

    /// With --rest-check, probe THIS token id instead of a discovered one. Used to
    /// cross-validate REST price vs the WS BookCache for the same token.
    #[arg(long)]
    rest_token: Option<String>,

    /// G7.2 probe: fire EXACTLY ONE `GET /relay-payload` against the real Polymarket
    /// relayer with the production env-var creds, log the FULL response (status, body,
    /// URL hit, header NAMES — never values), then exit 0. Pure diagnostic of the call
    /// the auto-redeem task fails on. Does NOT sign, does NOT submit, does NOT redeem,
    /// does NOT spawn the live trader. Safe to run with the bot stopped on the VPS to
    /// isolate the wire-format reason behind the 100%% relayer failure.
    #[arg(long)]
    redeem_check: bool,

    /// G7.4-C0 probe: list the Builder API Keys associated with the authenticated CLOB
    /// account. Pure read-only diagnostic (`GET /auth/builder-api-key` on the CLOB host
    /// using existing L2 CLOB credentials). Logs key UUIDs + created_at/revoked_at
    /// timestamps -- NEVER any secret/passphrase (the LIST endpoint upstream does not
    /// return those; they are only returned at creation time). Used to decide whether
    /// the operator needs to CREATE a new builder key (Step C1) before the relayer's
    /// POST /submit HMAC implementation (Step C2) can be wired. Safe to run anytime.
    #[arg(long)]
    builder_creds_list: bool,

    /// Backtest-TP: offline simulator that re-prices the bot's time-exit strategy
    /// against alternative take-profit (TP) thresholds, using the recorder's HISTORIC
    /// depth (44+ bid/ask levels with sizes). Reconstructs the FULL book per-token
    /// from book snapshots + price_change deltas, computes the EXECUTABLE bid
    /// for each position's shares at every event, and evaluates ALL exit variants
    /// (Baseline, FirstTouch TP, Peak) on the SAME entries -- guarantees a fair
    /// comparison. Writes per-variant metrics + per-trade detail to --bt-out-dir.
    /// Pure offline, ZERO network, ZERO capital.
    #[arg(long)]
    backtest_tp: bool,

    /// With --backtest-tp: start date YYYY-MM-DD (inclusive).
    #[arg(long, default_value = "2026-05-06")]
    bt_start_date: String,

    /// With --backtest-tp: end date YYYY-MM-DD (inclusive).
    #[arg(long, default_value = "2026-05-16")]
    bt_end_date: String,

    /// With --backtest-tp: comma-separated variants. `0`=Baseline (time-exit),
    /// positive int = FirstTouch TP percentage, `peak`=Peak (theoretical max).
    /// Default: `0,5,10,15,20,25,30,40,50,peak` (full curve).
    #[arg(long, default_value = "0,5,10,15,20,25,30,40,50,peak")]
    bt_variants: String,

    /// With --backtest-tp: output directory for per-variant + summary JSON.
    #[arg(long, default_value = "data/derived/backtest_tp")]
    bt_out_dir: PathBuf,

    /// With --backtest-tp: recorder data root (= where `live_l2/` lives).
    #[arg(long, default_value = "data")]
    bt_data_root: PathBuf,

    /// With --backtest-tp: phase guard (out-of-sample discipline). `exploration`
    /// (default) forbids touching dates >= VALIDATION_CUTOFF_DATE (= 2026-05-17)
    /// -- aborts at startup if --bt-end-date reaches into the validation set, so
    /// a CLI typo during Fase 1/2 cannot silently contaminate Fase 3.
    /// `validation` allows any dates BUT prints a loud banner + writes a
    /// `validation_seal_broken.txt` marker in --bt-out-dir (the one-shot Fase 3
    /// gate is logged in the audit trail).
    #[arg(long, default_value = "exploration")]
    bt_phase: String,

    /// With --backtest-tp: use the legacy load-all-then-sort event source
    /// (the pre-G11 implementation). The default is the G11 streaming k-way
    /// merger which has bounded per-day memory (~250 KB vs ~17 GB on heavy
    /// days). This flag exists ONLY for equivalence comparisons (run BOTH
    /// implementations on the same dates and diff the output JSONLs to
    /// verify byte-exact agreement) and as a fallback if the streaming
    /// merger were ever found to deviate from legacy semantics.
    #[arg(long, default_value = "false")]
    bt_legacy_inmemory: bool,

    /// G12 BACKTESTER HEALTH CHECK: scan recorder file structure (line counts,
    /// recv_ms monotonicity, gaps > 60s) across `--bt-start-date` ..
    /// `--bt-end-date`. SAFE on validation data: only the `received_at`
    /// JSON field is extracted per line; price/size/token/asset_id/payload
    /// are NOT read. The phase guard does NOT apply (this is a structural
    /// scan, not a backtest). NO `validation_seal_broken.txt` is ever
    /// written. Use this BEFORE Fase 3 to verify the validation set is
    /// healthy (0-1 OOO per stream per day expected) without contaminating
    /// the seal.
    #[arg(long)]
    bt_health_check: bool,

    /// G13 FASE 2: per-feature predictive analysis (AUC + Mann-Whitney U +
    /// Bonferroni) of the 12 fixed candidate features against the "should
    /// have sold by now?" oracle label. Causal by type (CausalSlice). Runs
    /// 4-layer sanity (oracle / random / canary_strong / canary_moderate)
    /// BEFORE the 12 real features; aborts if a sanity layer's AUC is out
    /// of band. Respects --bt-phase guard (exploration | validation).
    #[arg(long)]
    bt_phase2: bool,

    /// G14 FASE 3: CSV of specific YYYY-MM-DD dates to process. OVERRIDES
    /// --bt-start-date / --bt-end-date when non-empty. Used to validate the
    /// frozen smart exit rule on a non-contiguous set of clean validation
    /// days (5/17, 5/21, 5/23, 5/24 -- the rest excluded by the G12 health
    /// check). The phase guard ALSO checks every entry in this list against
    /// VALIDATION_CUTOFF_DATE -- you cannot bypass the seal by hiding
    /// validation dates inside an exploration range.
    #[arg(long, value_delimiter = ',', default_value = "")]
    bt_include_dates: Vec<String>,

    /// PIECE W8: backtest one-or-more ENTRY FILTERS over a date range and emit a
    /// cross-variant comparison table. Each filter produces a separate
    /// `summary_<label>.json` + `trades_<label>.jsonl` in --bt-out-dir + a
    /// single `compare_entry_filters.csv` with one row per (filter x cell)
    /// for at-a-glance hypothesis review. ALL filters use ExitVariant::Baseline
    /// (time-exit) so the only variable is the entry filter. Uses --bt-data-root,
    /// --bt-start-date, --bt-end-date, --bt-phase from the existing --backtest-tp
    /// CLI args. Burned-data discipline applies (the date range is QUEMADA;
    /// validation is a separate later pass on fresh recorder data).
    #[arg(long)]
    backtest_entry_filters: bool,

    /// W9-Pieza1 EXITS-TRACE AUDIT: a self-contained pass with a0 entry filter +
    /// Baseline (time_exit) exit, emitting `trades_exits_trace.jsonl` with a
    /// per-trade RICH row (trigger_ret_bps, maker/taker fees from markets log,
    /// fixed-offset bids at 5/15/30/60/120s, max/min in window, high/low water
    /// mark traces). Used by the Python analyzer to simulate B1 (sell post-
    /// repricing fast at fixed offset / first-touch level) and B3 (limit/maker
    /// at target = f(entry_price, |bps|)) without re-running the backtester.
    /// Honors --bt-phase: validation dates require --bt-phase validation.
    #[arg(long)]
    backtest_exits_trace: bool,

    /// With --backtest-entry-filters: comma-separated filter labels. Default
    /// runs ALL 15 hypotheses (A/B/C from W8 + D-family DCA filters from W9).
    /// Supported labels:
    ///   a0  = Baseline (accept every Fire; reproduces current behavior)
    ///   a1  = BPS threshold 6 ; a2 = 7 ; a3 = 8
    ///   b1  = NoOpposite (skip Fire if opposite-side position exists)
    ///   b2  = REGLA C (cfg.regla_c_enabled = true, close-and-open)
    ///   c1  = AsymmetricBps min=5 opp=8 ; c2 = opp=7 ; c3 = opp=10
    ///   d0  = DcaUnlimited (= a0 baseline; kept distinct for the D-family table)
    ///   d1  = DcaImprovingPrice (accept lot only if entry < MIN of prior same-side)
    ///   d2a = DcaConfirmingUnderlying (Binance kline close continued in our dir)
    ///   d2b = DcaConfirmingAsk (entry > MAX of prior same-side; mirror of d1)
    ///   d3  = NoDca (one lot per market-side)
    ///   d4  = DcaCap{max=3} (cap DCA at 3 lots per market-side)
    ///   split_dca = per-cell composition: D0 in 5m + D4 in 15m (W9 hypothesis)
    #[arg(long, default_value = "a0,a1,a2,a3,b1,b2,c1,c2,c3,d0,d1,d2a,d2b,d3,d4")]
    bt_entry_filters: String,

    /// COMBO Phase A gate: run the FULL production strategy (b1 entry, per-cell
    /// exits: BTC_5m baseline, ETH_5m + BTC_15m B3 d=0.10 F2, ETH_15m retired)
    /// over a b1 JSONL using the REAL Rust B3 math (b3_first_touch_sim). Prints
    /// per-cell gross/net + maker lifecycle + the analyzer-coherence numbers.
    /// Read-only, no network. Pair with --b3-replay-input.
    #[arg(long)]
    b3_strategy_replay: bool,

    /// With --b3-strategy-replay: path to the b1 trades_exits_trace.jsonl.
    #[arg(long, default_value = "")]
    b3_replay_input: String,

    /// With --b3-strategy-replay: also write a per-position dump
    /// (<input>.b3dump.jsonl) for the coherence diff vs the Python analyzer.
    /// Off by default (the gate run only needs the aggregate report).
    #[arg(long)]
    b3_replay_dump: bool,

    /// With --backtest-exits-trace: SINGLE entry-filter label (default a0 =
    /// Baseline, accept every Fire). Combined with the offline Python analyzer,
    /// this lets COMBO A/B/C be measured: A and C use a0 (both sides); B uses
    /// b1 (NoOpposite, one side). The exit rule is FIXED to Baseline at this
    /// layer — alternate exits (B3 maker first-touch, MIX-5m) are applied
    /// downstream in scripts/analyze_exits_pf_audit.py on the emitted JSONL.
    /// Supports the same labels as --bt-entry-filters but ONE at a time.
    /// Regression: with a0, bit-identical to the pre-COMBO output.
    #[arg(long, default_value = "a0")]
    bt_exits_trace_filter: String,

    /// G8 escape hatch: at startup, manually RESET `Guards::daily_net_pnl` to zero
    /// before any task runs. Use when the operator knows the counter is "dirty"
    /// (e.g. a partial catch-up inflated today's number with stale losses, or the
    /// operator wants to start the trading day clean after an incident). Logs an
    /// explicit AUDIT line at startup naming the operator action so the reset is
    /// traceable in the file log. Default OFF -- not triggered by any automatic
    /// path. Per-day reset already happens at midnight UTC inside `record_net_pnl`;
    /// this flag is for non-midnight manual resets only.
    #[arg(long)]
    guard_reset_daily_pnl: bool,

    /// G7.4-C1 action: CREATE a new Builder API Key on the authenticated CLOB account
    /// (`POST /auth/builder-api-key`). Without `--builder-creds-confirm`, runs in
    /// DRY-RUN mode (prints exactly what it WOULD do but creates nothing). With
    /// `--builder-creds-confirm`, it actually creates the key and emits the new
    /// (key, secret, passphrase) triple to STDERR -- ONE-SHOT (the secret + passphrase
    /// are returned only at creation, never again). Does NOT move capital and does NOT
    /// modify trading state; it does change account state (adds one builder key).
    /// Output is to STDERR (not via tracing) so the secrets do NOT enter any file log.
    #[arg(long)]
    builder_creds_create: bool,

    /// Companion to `--builder-creds-create`: explicit opt-in to actually create the
    /// key (sin esto, --builder-creds-create solo hace dry-run). Mirrors the
    /// `--live-test-execute` pattern: action flag + execute gate, both required.
    #[arg(long)]
    builder_creds_confirm: bool,

    /// Confirm the FIRST-ever live launch. Required once before trading real
    /// capital (the state mode_history first-live defense).
    #[arg(long)]
    confirm_live: bool,

    /// Log level filter override, e.g. "info", "debug", "rust_bot=trace".
    #[arg(long)]
    log_level: Option<String>,

    /// Run ingestion for N seconds then shut down gracefully, instead of waiting
    /// for Ctrl-C. Used for timed validation runs (e.g. the Phase 5 BBO check).
    #[arg(long)]
    run_secs: Option<u64>,

    /// Phase 4 Capa A: replay historical recorder klines through the signal engine
    /// and write the signal sequence to jsonl. Offline parity harness â€” needs no
    /// config, .env, network, or state; branches before all of that and exits.
    #[arg(long)]
    replay_signals: bool,

    /// With --replay-signals: recorder data root (holds live_l2/binance/... and
    /// live_l2/polymarket/markets/...).
    #[arg(long, default_value = "data")]
    replay_data_root: PathBuf,

    /// With --replay-signals: the date to replay (YYYY-MM-DD).
    #[arg(long, default_value = "2026-05-28")]
    replay_date: String,

    /// With --replay-signals: output jsonl path
    /// (default: <data-root>/derived/capa_a/rust_signals_<date>.jsonl).
    #[arg(long)]
    replay_out: Option<PathBuf>,

    /// Phase 5 verification: replay recorder book/price_change/best_bid_ask through
    /// the live handler and dump the per-event BBO (to diff vs the Python). Offline.
    #[arg(long)]
    bbo_dump: bool,

    /// With --bbo-dump: the date to read (YYYY-MM-DD). Uses --replay-data-root.
    #[arg(long, default_value = "2026-05-28")]
    bbo_dump_date: String,

    /// With --bbo-dump: max qualifying events per stream (bounds the read).
    #[arg(long, default_value_t = 300_000)]
    bbo_dump_max: u64,

    /// With --bbo-dump: output jsonl path
    /// (default: <data-root>/derived/capa_a/rust_bbo_<date>.jsonl).
    #[arg(long)]
    bbo_dump_out: Option<PathBuf>,

    /// Phase 5 Capa B: replay recorder klines+book through the decision engine and
    /// emit per-trigger decisions + per-fire trades (to diff vs the Python oracle).
    #[arg(long)]
    capa_b: bool,

    /// With --capa-b: the date (YYYY-MM-DD). Uses --replay-data-root.
    #[arg(long, default_value = "2026-05-28")]
    capa_b_date: String,

    /// With --capa-b: window start/end hour (UTC) to bound the replay.
    #[arg(long, default_value_t = 0)]
    capa_b_start_hour: u32,
    #[arg(long, default_value_t = 24)]
    capa_b_end_hour: u32,

    /// With --capa-b: enable REGLA C (opposite-closes variant). Default OFF =
    /// baseline (the operative variant).
    #[arg(long)]
    capa_b_regla_c: bool,

    /// With --capa-b: run the Phase 6 D1 PARITY gate (direct `decide` vs the
    /// `trading_loop::process_kline` seam) over the tape, instead of emitting files.
    #[arg(long)]
    d1_parity: bool,

    /// Phase 6 Sub-paso A: OFFLINE signing-parity harness. Builds + signs a CTF
    /// Exchange V2 order with a FIXED TEST KEY and pinned inputs, dumps the
    /// EIP-712 hash + signature + post body for byte-diff vs the Python SDK. ZERO
    /// network, ZERO orders.
    #[arg(long)]
    sign_parity: bool,

    /// With --sign-parity: output dir (default data/derived/sign_parity).
    #[arg(long)]
    sign_parity_out: Option<PathBuf>,

    /// Phase 6 Sub-paso A: isolated LiveExecutor round-trip test. Default = READ-ONLY
    /// preflight (auth + balance + candidate + plan + cap check, ZERO orders).
    #[arg(long)]
    live_test: bool,

    /// With --live-test: actually place the real BUY+SELL (one token, FOK, cap $5).
    /// Gated: requires explicit opt-in after reviewing the preflight.
    #[arg(long)]
    live_test_execute: bool,

    /// With --live-test --live-test-execute: WATCH MODE. Loop the read-only
    /// discovery (â‰¤30 min) until a balanced window appears, fire EXACTLY ONE
    /// autonomous order, then HARD-STOP. Autonomy is only over timing; every
    /// capital guard stays strict.
    #[arg(long)]
    live_test_watch: bool,

    /// Phase 6 D2: run the paper decision loop in PAPER + LIVE-SHADOW mode — the
    /// LiveExecutor builds+signs the real order it WOULD send (no POST) on each fire,
    /// reporting the signed hash + projected slippage. Needs .env creds (read-only +
    /// local sign). ZERO capital — never posts. Requires --mode paper.
    #[arg(long)]
    d2_shadow: bool,

    /// Phase 6 D2: DETERMINISTIC shadow self-test. Forces ONE order through the real
    /// build+sign path on a live token and prints what WOULD be sent (signed hash +
    /// slippage) — NEVER posts (shadow hardcoded). Read-only REST + local sign, ZERO
    /// capital. The on-demand D2 gate (doesn't wait for a probabilistic trigger).
    #[arg(long)]
    d2_shadow_selftest: bool,

    /// Phase B: DETERMINISTIC maker SIGNING self-test (the compressed-shadow gate
    /// before the first real maker). Builds + signs a real GTC + post_only MAKER
    /// limit SELL on a live token and prints what WOULD be sent — NEVER posts
    /// (shadow hardcoded). Read-only REST + local sign, ZERO capital, no LIVE_ARMED.
    /// Confirms the maker build-validation + signing BEFORE any real maker post.
    #[arg(long)]
    maker_sign_selftest: bool,

    /// Phase B: READ-ONLY self-test of the LiveMakerBackend->SDK boundary for
    /// list_open_orders (the highest-risk read; it feeds the reconcile). Lists
    /// YOUR real open orders + prints the parsed rows. No post/cancel, zero
    /// capital, no LIVE_ARMED. Run before the ARM.
    #[arg(long)]
    maker_list_orders_selftest: bool,

    /// Phase B: READ-ONLY self-test of poll_order/order(id) (status +
    /// size_matched -- the fields that govern no-oversell). Fetches ONE real
    /// order by id (--maker-poll-order-id) + prints the mapped result.
    #[arg(long)]
    maker_poll_selftest: bool,

    /// With --maker-poll-selftest: the order id to fetch (read-only).
    #[arg(long, default_value = "")]
    maker_poll_order_id: String,

    /// G5-test-rig — TEST MODE ONLY. Filters triggers post-`expand_signals` to ONLY
    /// 15m IM (interval=="15m" AND stratum==Immediate). 15m IM trades ALWAYS close
    /// by SELL in window (never past-close), so this isolates the SELL path from
    /// the redeem path for controlled live testing. Discards ~50% of the normal
    /// trade flow (all 5m and all 15m RW). DEFAULT OFF = byte-identical to the
    /// pre-G5-test-rig bot. Banner WARN at startup when enabled so it never goes
    /// unnoticed in production.
    #[arg(long)]
    restrict_15m_im: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // --- Phase 4 Capa A: offline signal replay. Self-contained â€” no config/.env/
    //     network/state. Branch before any of that so the harness stays pure.
    if cli.replay_signals {
        return signal::replay::run_cli(
            &cli.replay_data_root,
            &cli.replay_date,
            cli.replay_out.as_deref(),
        );
    }
    if cli.bbo_dump {
        return bbo_dump::run_cli(
            &cli.replay_data_root,
            &cli.bbo_dump_date,
            cli.bbo_dump_out.as_deref(),
            cli.bbo_dump_max,
        );
    }
    if cli.capa_b {
        if cli.d1_parity {
            return capa_b::run_d1_parity(
                &cli.replay_data_root,
                &cli.capa_b_date,
                cli.capa_b_start_hour,
                cli.capa_b_end_hour,
            );
        }
        return capa_b::run_cli(
            &cli.replay_data_root,
            &cli.capa_b_date,
            cli.capa_b_start_hour,
            cli.capa_b_end_hour,
            cli.capa_b_regla_c,
            None,
        );
    }
    // Phase 6 Sub-paso A: offline signing-parity (no config/.env/network/orders).
    if cli.sign_parity {
        return sign_parity::run_cli(cli.sign_parity_out.as_deref()).await;
    }
    // Backtest-TP: offline simulator (no config/.env/network/orders). Pure
    // historical replay of the recorder data with depth-aware exit policy
    // comparison. ZERO capital touched.
    // G12 health check FIRST (before backtest_tp dispatcher) so the phase
    // guard is bypassed entirely for this seal-safe structural scan.
    if cli.bt_health_check {
        return backtest_tp::run_health_check(
            &cli.bt_data_root,
            &cli.bt_start_date,
            &cli.bt_end_date,
        );
    }
    // G13 Phase 2: predictive feature analysis. Respects --bt-phase guard.
    if cli.bt_phase2 {
        let phase = backtest_tp::BtPhase::parse(&cli.bt_phase)
            .with_context(|| "parsing --bt-phase")?;
        return backtest_tp::run_phase2(
            &cli.bt_data_root,
            &cli.bt_start_date,
            &cli.bt_end_date,
            &cli.bt_out_dir,
            phase,
        );
    }
    if cli.backtest_tp {
        let phase = backtest_tp::BtPhase::parse(&cli.bt_phase)
            .with_context(|| "parsing --bt-phase")?;
        let variants = backtest_tp::parse_variants(&cli.bt_variants)
            .with_context(|| "parsing --bt-variants")?;
        // G14: filter empty strings out of include_dates (clap with
        // default_value="" + value_delimiter="," may emit a single "" if
        // the flag is absent or empty).
        let include_dates: Vec<String> = cli.bt_include_dates
            .iter()
            .filter(|s| !s.trim().is_empty())
            .cloned()
            .collect();
        return backtest_tp::run_backtest_tp(
            &cli.bt_data_root,
            &cli.bt_start_date,
            &cli.bt_end_date,
            &variants,
            &cli.bt_out_dir,
            phase,
            cli.bt_legacy_inmemory,
            &include_dates,
        );
    }
    // PIECE W8: entry-filter hypothesis-generation backtester.
    if cli.backtest_entry_filters {
        let phase = backtest_tp::BtPhase::parse(&cli.bt_phase)
            .with_context(|| format!("parsing --bt-phase: '{}'", cli.bt_phase))?;
        let filters = backtest_tp::parse_entry_filter_labels(&cli.bt_entry_filters)
            .with_context(|| "parsing --bt-entry-filters")?;
        return backtest_tp::run_backtest_entry_filters(
            &cli.bt_data_root,
            &cli.bt_start_date,
            &cli.bt_end_date,
            &filters,
            &cli.bt_out_dir,
            phase,
        );
    }
    // COMBO Phase A gate: full-strategy paper replay over a b1 JSONL using the
    // real Rust B3 math. Read-only, no network, no config/.env needed.
    if cli.b3_strategy_replay {
        anyhow::ensure!(!cli.b3_replay_input.is_empty(), "--b3-strategy-replay requires --b3-replay-input <jsonl>");
        return b3_maker::run_strategy_replay(std::path::Path::new(&cli.b3_replay_input), cli.b3_replay_dump);
    }
    // PIECE W9-Pieza1: exits-trace audit. Self-contained; entry filter is
    // parameterized via --bt-exits-trace-filter (default a0 = Baseline). Exit
    // is fixed to Baseline at this layer (alternate exits are applied offline
    // by scripts/analyze_exits_pf_audit.py on the emitted JSONL). Emits per-
    // trade enriched JSONL (trigger_ret_bps, fees, fixed-offset bids, HWM/LWM,
    // extremes). Bit-identical to pre-COMBO output when filter = a0.
    if cli.backtest_exits_trace {
        let phase = backtest_tp::BtPhase::parse(&cli.bt_phase)
            .with_context(|| format!("parsing --bt-phase: '{}'", cli.bt_phase))?;
        // Reuse the entry-filter parser (Vec) and require a SINGLE label.
        let mut filters = backtest_tp::parse_entry_filter_labels(&cli.bt_exits_trace_filter)
            .with_context(|| "parsing --bt-exits-trace-filter")?;
        if filters.len() != 1 {
            anyhow::bail!(
                "--bt-exits-trace-filter must be a SINGLE label, got {} ({})",
                filters.len(), cli.bt_exits_trace_filter,
            );
        }
        let entry_filter = filters.remove(0);
        return backtest_tp::run_backtest_exits_trace(
            &cli.bt_data_root,
            &cli.bt_start_date,
            &cli.bt_end_date,
            &cli.bt_out_dir,
            phase,
            &entry_filter,
        );
    }
    // Phase 6 Sub-paso A: isolated LiveExecutor. Preflight is read-only; real orders
    // only with --live-test-execute. Self-contained (own .env load), so branch early.
    if cli.live_test {
        return live_test::run_cli(cli.live_test_execute, cli.live_test_watch, None).await;
    }

    // --- Config (before tracing: tracing needs the logging paths/level) ------
    let config = Config::load(&cli.config)
        .with_context(|| format!("loading config from {}", cli.config.display()))?;

    // --- Tracing (JSON file + stdout). Guard must outlive the program. -------
    let _log_guard = logging::init(&config, cli.log_level.as_deref())
        .context("initializing tracing")?;

    // Phase 6 D2: deterministic shadow self-test. Forces ONE order through the real
    // build+sign path on a live token and prints what WOULD be sent — NEVER posts
    // (shadow hardcoded in live_executor). Read-only REST + local sign, ZERO capital.
    if cli.maker_sign_selftest {
        return live_executor::run_maker_sign_selftest(&config).await;
    }
    if cli.maker_list_orders_selftest {
        return live_executor::run_maker_list_orders_selftest(&config).await;
    }
    if cli.maker_poll_selftest {
        anyhow::ensure!(!cli.maker_poll_order_id.is_empty(), "--maker-poll-selftest requires --maker-poll-order-id <id>");
        return live_executor::run_maker_poll_selftest(&config, &cli.maker_poll_order_id).await;
    }
    if cli.d2_shadow_selftest {
        return live_executor::run_shadow_selftest(&config, config.stakes.base_usdc).await;
    }

    let effective_mode = cli
        .mode
        .map(Mode::as_str)
        .unwrap_or(config.mode.default.as_str());
    info!(
        version = env!("CARGO_PKG_VERSION"),
        config_path = %cli.config.display(),
        mode = effective_mode,
        dry_run = cli.dry_run,
        "rust_bot starting (Phase 1: WS ingestion)"
    );

    // --- .env: confirm it loads; presence-check credentials (never values) ---
    match dotenvy::dotenv() {
        Ok(path) => info!(env_file = %path.display(), "loaded .env"),
        Err(e) if e.not_found() => {
            warn!("no .env file found (ok for Phase 0; credentials are not used)")
        }
        Err(e) => warn!(error = %e, "failed to load .env (continuing; credentials not used in Phase 0)"),
    }
    check_credentials_present();

    // --- Validation ----------------------------------------------------------
    if let Err(e) = config.validate() {
        error!(error = %e, "config validation failed");
        return Err(e);
    }
    info!("config validated");

    // --- Hard guard (before any spawn) ---------------------------------------
    assert!(
        config.stakes.max_position_usdc <= MAX_POSITION_USDC_HARD_CAP,
        "HARD GUARD: stakes.max_position_usdc ({}) exceeds hard cap ({}). Refusing to start.",
        config.stakes.max_position_usdc,
        MAX_POSITION_USDC_HARD_CAP
    );
    info!(
        max_position_usdc = config.stakes.max_position_usdc,
        hard_cap = MAX_POSITION_USDC_HARD_CAP,
        "hard guard passed"
    );

    // --- Dry run: dump effective config (no secrets) and exit 0 --------------
    if cli.dry_run {
        info!("dry-run: effective configuration follows (secrets come from .env and are NOT shown)");
        info!("{config:#?}");
        info!("dry-run complete; exiting 0");
        return Ok(());
    }

    // --- TLS: install the rustls crypto provider (aws-lc-rs) so every wss
    //     ClientConfig builds deterministically, independent of feature
    //     unification across the dependency tree. Err just means it's already
    //     installed, which is fine.
    if rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .is_err()
    {
        debug!("rustls aws-lc-rs crypto provider already installed");
    }

    // --- Phase 2: read-only REST probe â€” query every endpoint once, then exit -
    if cli.rest_check {
        rest_check(&config, cli.rest_token.clone()).await?;
        info!("rest-check complete; exiting 0");
        return Ok(());
    }

    // --- G7.2: read-only relayer probe -- single GET /relay-payload, then exit ---
    // The auto-redeem task fails 100% on this exact call in production. This
    // diagnostic isolates ONE call (no signing, no submitting) so the operator
    // can see the exact HTTP failure (status + body + URL hit) without operating.
    if cli.redeem_check {
        redeem_check().await?;
        info!("redeem-check complete; exiting 0");
        return Ok(());
    }

    // --- G7.4-C0: read-only CLOB probe -- list Builder API Keys, then exit ------
    // Pure diagnostic. Uses existing CLOB L2 creds to GET /auth/builder-api-key.
    // Logs UUIDs + timestamps only -- secret/passphrase are NEVER returned by LIST
    // (those are only available at creation time -- Step C1). Decides whether
    // Step C1 (create) is needed before C2 (HMAC implementation for POST /submit).
    if cli.builder_creds_list {
        builder_creds_list(&config).await?;
        info!("builder-creds-list complete; exiting 0");
        return Ok(());
    }

    // --- G7.4-C1: CREATE a Builder API Key on the CLOB account, then exit -------
    // GATED: without --builder-creds-confirm, runs as a dry-run that explains
    // what it WOULD do. With --builder-creds-confirm, actually POSTs and emits
    // the new (key, secret, passphrase) to STDERR ONE-SHOT. Does NOT move
    // capital, but DOES add one builder key to the operator's account.
    if cli.builder_creds_create {
        builder_creds_create(&config, cli.builder_creds_confirm).await?;
        info!("builder-creds-create complete; exiting 0");
        return Ok(());
    }

    // --- Phase 3: load persistent state + launch gate ------------------------
    // Crash recovery: state.json â†’ .bak fallback â†’ empty. The launch gate refuses
    // --live when state was lost (corrupt) or on an unconfirmed first-ever live run.
    let (state_store, load_outcome) =
        state::store::StateStore::load(Path::new(&config.paths.state_file));
    let state_store = Arc::new(state_store);
    {
        let shared = state_store.state();
        let mut bs = shared.lock().expect("state mutex poisoned");
        bs.record_launch(effective_mode, env!("CARGO_PKG_VERSION"), state::now_ms());
        info!(
            recovery = ?load_outcome.source,
            positions = bs.positions.len(),
            recent_signals = bs.recent_signals.len(),
            "persistent state loaded"
        );
        for e in &load_outcome.errors {
            warn!(detail = %e, "state recovery note");
        }
        if load_outcome.lost_state() {
            ws::write_alert(
                &config.paths.alert_dir,
                "state",
                "lost_state",
                "state.json and .bak both unrecoverable; started with empty state",
            );
        }
        match state::store::evaluate_launch(&load_outcome, &bs, effective_mode, cli.confirm_live) {
            state::store::LaunchDecision::Allow => {}
            state::store::LaunchDecision::RefuseLostState => {
                error!(
                    "REFUSING --live: persisted state was lost (corrupt state.json + .bak). \
                     Restore a backup or start --mode paper; investigate before trading capital."
                );
                anyhow::bail!("refuse live: persisted state lost");
            }
            state::store::LaunchDecision::RefuseFirstLiveUnconfirmed => {
                error!(
                    "REFUSING --live: this is the FIRST live launch. Re-run with --confirm-live \
                     to confirm trading real capital."
                );
                anyhow::bail!("refuse live: first-live unconfirmed");
            }
        }
    }
    // Persist the launch record immediately (event-driven; the task also does 5s).
    if let Err(e) = state_store.snapshot() {
        warn!(error = %e, "initial state snapshot failed");
    }
    // On-chain reconciliation is a live-startup concern (full live path is Phase 6).
    if effective_mode == "live"
        && let Err(e) = reconcile_on_chain(&config, &state_store).await
    {
        warn!(error = %e, "on-chain reconciliation failed at startup");
    }

    // --- Shared state, event recorder, and the shutdown signal ---------------
    let state = state::SharedState::new();
    // v2 tick-driven entry: push the config flags into shared state so the Binance
    // WS emits sub-second decision triggers (throttled) when enabled.
    state.tick_driven.store(config.v2.tick_driven, std::sync::atomic::Ordering::Relaxed);
    state.tick_throttle_ms.store(config.v2.tick_throttle_ms.max(1), std::sync::atomic::Ordering::Relaxed);
    // Order #7 C: configure the regime canary with the operator's kill switch
    // (thresholds are the validated CanaryConfig defaults).
    *state.canary.lock().expect("canary mutex") = canary::Canary::new(canary::CanaryConfig {
        enabled: config.v2.canary_enabled,
        ..canary::CanaryConfig::default()
    });
    info!(canary_enabled = config.v2.canary_enabled, "regime canary configured");
    // ORDER #14 B: arm the Binance feed-liveness guard. Until this is set the check
    // is disabled (0), which is what keeps pure-WS/ingestion tests unaffected — so
    // this line is what makes health measure DATA in every real run.
    state.feed_dead_ms.store(
        config.connections.binance_feed_dead_ms,
        std::sync::atomic::Ordering::Relaxed,
    );
    info!(
        feed_dead_ms = config.connections.binance_feed_dead_ms,
        idle_timeout_s = config.connections.binance_idle_timeout_s,
        "binance feed watchdog armed (Order #14)"
    );
    if config.v2.tick_driven {
        info!(throttle_ms = config.v2.tick_throttle_ms,
            "v2 TICK-DRIVEN entry ENABLED — decisions fire on sub-second aggTrades");
    }
    // #3 (dashboard): in trading modes, give the WS reconnect supervisor an oplog
    // sink so connection lifecycle (ws_lost / ws_reconnecting / ws_cooldown /
    // ws_recovered) lands in data/live/oplog.jsonl for the passive Connection/
    // Health panel. Same append-only file the trading tasks use; safe to share.
    // Passive observability only — never read by trading.
    if live_backend::trading_tasks_enabled(effective_mode) {
        state.set_oplog(std::sync::Arc::new(oplog::OpLog::default_path()));
    }
    let (event_logger, event_handle) = events::spawn(
        Path::new(&config.paths.timestamps_file),
        config.logging.event_logger_enabled,
    )
    .context("opening event log")?;
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    // --- Dynamic market discovery -------------------------------------------
    // Publishes the active 5m/15m token set (seeded from config, then refreshed
    // via REST every discovery_refresh_secs). The Polymarket WS client follows it.
    let (token_tx, token_rx) =
        tokio::sync::watch::channel(Arc::new(config.markets.polymarket_token_ids.clone()));
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .context("building market-discovery HTTP client")?;
    let token_source = Arc::new(discovery::RestTokenSource::new(
        http,
        config.markets.assets.clone(),
        config.markets.intervals.clone(),
        config.markets.discovery_lookahead,
        config.connections.polymarket_gamma_url.clone(),
        config.connections.polymarket_rest_url.clone(),
    ).with_market_sink(state.clone())); // D1: also populate the live MarketCatalog
    let discovery_task = tokio::spawn(discovery::run_discovery(
        token_source,
        token_tx,
        state.clone(),
        config.paths.alert_dir.clone(),
        Duration::from_secs(config.markets.discovery_refresh_secs),
        shutdown_rx.clone(),
    ));

    // --- Phase 1 ingestion: real read-only WS clients ------------------------
    let binance = tokio::spawn(ws::binance::run(
        state.clone(),
        config.clone(),
        shutdown_rx.clone(),
        event_logger.clone(),
    ));
    let polymarket = tokio::spawn(ws::polymarket::run(
        state.clone(),
        config.clone(),
        shutdown_rx.clone(),
        event_logger.clone(),
        token_rx,
    ));
    let stats = tokio::spawn(ws::stats_loop(state.clone(), shutdown_rx.clone()));

    // --- Phase 3: periodic + final-on-shutdown state snapshot ----------------
    let persist_task = tokio::spawn(state::store::run_persist(
        state_store.clone(),
        Duration::from_secs(5),
        shutdown_rx.clone(),
    ));

    // --- Phase 6 D1: the decision→execution→exit loop (PAPER backend) ----------
    // Gated to paper mode: the SAME plumbing carries the Live backend in D2+. Zero
    // capital. Binance final klines (via state.kline_tx) → decision → guards →
    // exec command → PaperExecutor; a 1s exit task closes lots at +hold.
    let trading_tasks: Vec<JoinHandle<()>> = if live_backend::trading_tasks_enabled(effective_mode) {
        let (kline_tx, kline_rx) =
            tokio::sync::mpsc::unbounded_channel::<trading_loop::KlineClose>();
        if state.kline_tx.set(kline_tx).is_err() {
            warn!("kline_tx already set (unexpected)");
        }
        let (exec_tx, exec_rx) =
            tokio::sync::mpsc::unbounded_channel::<trading_loop::ExecCommand>();
        let dec_cfg = decision::DecisionConfig {
            stake_usd: config.stakes.base_usdc,
            regla_c_enabled: config.rules.regla_c_enabled,
            ..decision::DecisionConfig::default()
        };
        // PIECE 3: shared guards instance -- decision task evaluates pre-entry,
        // execution + exit tasks feed NET P&L into the daily-loss-stop accumulator,
        // decision task records every accepted Open into the frequency breaker.
        let mut guard_cfg = guards::GuardConfig::default();
        // v2 uses edge-proportional sizing ($base..$max_position), which the legacy
        // $1.05 stake_cap would block. When v2 is enabled, derive the guard caps
        // from config so the validated sizing is permitted (still bounded).
        if config.v2.enabled {
            let maxp = rust_decimal::Decimal::try_from(config.stakes.max_position_usdc)
                .unwrap_or(guard_cfg.stake_cap);
            let total = rust_decimal::Decimal::try_from(
                config.stakes.max_position_usdc * config.stakes.max_open_positions as f64,
            )
            .unwrap_or(guard_cfg.total_exposure_cap);
            guard_cfg.stake_cap = maxp; // single order up to max_position
            guard_cfg.per_token_cap = maxp; // one entry per market → per-token = one lot
            guard_cfg.total_exposure_cap = total; // max_open_positions * max_position
            guard_cfg.hard_cap = total.max(guard_cfg.hard_cap);
            guard_cfg.daily_loss_cap_usdc = None; // derive from stake_cap*stakes (paper-generous)
            info!(stake_cap = %guard_cfg.stake_cap, per_token_cap = %guard_cfg.per_token_cap,
                total_cap = %guard_cfg.total_exposure_cap, hard_cap = %guard_cfg.hard_cap,
                daily_loss_cap = %guard_cfg.daily_loss_cap(),
                "v2: guard caps derived from config (legacy $1.05 stake_cap overridden)");
        }
        // Capture the gate-file paths before guard_cfg is moved into Guards — the
        // dashboard's ARM/KILL buttons write/remove these same files.
        let kill_switch_path_str = guard_cfg.kill_switch_path.display().to_string();
        let live_armed_path_str = guards::LIVE_ARMED_PATH.to_string();
        // STARTUP kill-switch check: a present file at launch is a deliberate halt;
        // log it prominently and let the continuous-check latch it on the first pass.
        if guards::kill_switch_active(&guard_cfg.kill_switch_path) {
            warn!(
                path = %guard_cfg.kill_switch_path.display(),
                "STARTUP: KILL-SWITCH file is present -- guards will halt the decision loop on its first pass"
            );
        }
        let guards_shared: std::sync::Arc<std::sync::Mutex<guards::Guards>> =
            std::sync::Arc::new(std::sync::Mutex::new(guards::Guards::new(guard_cfg)));
        // PIECE 5: shared oplog instance -- every task (decision/execution/exit/refresh)
        // appends operational events to this single firehose (one wall clock,
        // ms-stamped, append-only). Enables full per-trade temporal reconstruction.
        let oplog_shared: std::sync::Arc<oplog::OpLog> =
            std::sync::Arc::new(oplog::OpLog::default_path());
        // G8: manual operator-triggered reset of the daily counter. Placed AFTER
        // oplog_shared so the reset emits an audit oplog event. The warn! line
        // names it explicitly so the manual action is traceable in the file log
        // and the oplog (two independent sinks). Per-day midnight UTC reset is
        // automatic inside record_net_pnl; this flag is the non-midnight
        // escape hatch (e.g. for cleaning a counter dirtied by a partial
        // catch-up or a buggy earlier run).
        if cli.guard_reset_daily_pnl {
            if let Ok(mut g) = guards_shared.lock() {
                g.reset_daily_pnl();
            }
            warn!(
                "AUDIT: --guard-reset-daily-pnl flag present at startup -- \
                 daily_net_pnl reset to 0 by manual operator action. \
                 The daily-loss-stop accumulator starts this run at zero regardless \
                 of any prior intra-day losses. Subsequent record_net_pnl events \
                 accumulate from zero."
            );
            oplog_shared.sys(
                "guard_reset_daily_pnl",
                serde_json::json!({
                    "source": "cli_flag",
                    "ts_ms": state::now_ms(),
                }),
            );
        }
        // D2 SHADOW / PIECE 6 LIVE: build a real RestClient + signer when EITHER
        // --d2-shadow is set OR --mode live. Read-only + local-sign requires
        // .env creds. Without either flag → pure paper (D1).
        let need_rest = cli.d2_shadow || effective_mode == "live";
        let rest_ctx: Option<trading_loop::ShadowCtx> = if need_rest {
            match build_shadow_ctx(&config).await {
                Ok(sc) => {
                    info!(max_slippage = config.rules.max_slippage,
                        d2_shadow = cli.d2_shadow, live = effective_mode == "live",
                        "REST + signer built (shadow or live)");
                    Some(sc)
                }
                Err(e) => {
                    warn!(error = %e, "REST + signer build failed; falling back to paper-only");
                    None
                }
            }
        } else {
            None
        };
        // CONNECTION WARMUP: the first live order cold-started ~40s (TLS/DNS +
        // connect-timeout retries to clob.polymarket.com), which let duplicate
        // entries pile up and killed the lag-edge. Pay that cost ONCE here, before
        // any order, with a throwaway authenticated CLOB call. The 60s balance
        // fetch in positions_refresh then keeps the pooled connection warm
        // (reqwest idle timeout > 60s). Non-fatal if it fails.
        if let Some(sc) = &rest_ctx {
            let t0 = std::time::Instant::now();
            info!("warming CLOB connection (throwaway balance call) to avoid first-order cold-start...");
            match sc.rest.get_balance().await {
                Ok(b) => info!(
                    balance_usdc = b.balance_usdc,
                    warm_ms = t0.elapsed().as_millis() as u64,
                    "CLOB connection warm — first real order will not cold-start"
                ),
                Err(e) => warn!(error = %e,
                    "CLOB warmup failed (non-fatal); first order may still stall"),
            }
        }
        let shadow_ctx: Option<trading_loop::ShadowCtx> = if cli.d2_shadow {
            rest_ctx.clone()
        } else {
            None
        };
        // PIECE 6 (D3.5) LIVE backend: real POST path, gated by LIVE_ARMED +
        // max_trades_per_session. Built ONLY when --mode live AND REST built.
        // Construction does NOT post anything (the gate refuses until armed).
        let live_backend: Option<std::sync::Arc<live_backend::LiveBackend>> =
            if effective_mode == "live" {
                rest_ctx.as_ref().map(|sc| {
                    info!(
                        max_trades_per_session = config.stakes.max_trades_per_session,
                        live_armed_path = guards::LIVE_ARMED_PATH,
                        intent_log = idempotency::INTENT_LOG,
                        "PIECE 6: LIVE backend constructed (POST gated by LIVE_ARMED + max_trades)"
                    );
                    std::sync::Arc::new(live_backend::LiveBackend {
                        rest: sc.rest.clone(),
                        pk: sc.pk.clone(),
                        max_slippage: sc.max_slippage,
                        intent_log: std::path::PathBuf::from(idempotency::INTENT_LOG),
                        live_armed_path: std::path::PathBuf::from(guards::LIVE_ARMED_PATH),
                        max_trades_per_session: config.stakes.max_trades_per_session,
                        trades_opened: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                        trades_closed: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                        shutdown_tx: std::sync::Arc::new(shutdown_tx.clone()),
                    })
                })
            } else {
                None
            };
        // G5-test-rig: banner WARN if the test flag is on. Emitted BEFORE
        // the routine info! line so it can't be missed in a busy startup log.
        if cli.restrict_15m_im {
            warn!("");
            warn!("====================================================================");
            warn!("  TEST MODE ACTIVE: --restrict-15m-im");
            warn!("  Solo procesara triggers 15m IM (los que SIEMPRE cierran por SELL");
            warn!("  en ventana). Descarta ~50% del flujo (5m + 15m RW).");
            warn!("  NO usar en produccion normal -- esto es para probar el SELL");
            warn!("  aislado del redeem en un test controlado.");
            warn!("====================================================================");
            warn!("");
            oplog_shared.sys("test_mode_restrict_15m_im_active", serde_json::json!({
                "warning": "TEST MODE -- only 15m IM triggers will be processed",
                "discards": "all 5m + all 15m RW triggers",
                "rationale": "isolates the SELL path from the redeem path",
            }));
        }
        // disabled_cells: production cell-suppression. Audit-banner at startup
        // (parallels the restrict_15m_im banner) AND an oplog event so EVERY
        // run records WHICH cells were off -- important for cross-correlating
        // results later ("did this run have ETH:15m disabled?").
        if !config.markets.disabled_cells.is_empty() {
            warn!("");
            warn!("====================================================================");
            warn!("  PRODUCTION FILTER ACTIVE: disabled_cells");
            warn!("  Cells = {:?}", config.markets.disabled_cells);
            warn!("  Signals matching these (asset:interval) will be DROPPED after");
            warn!("  expand_signals, before decide. Each drop emits a");
            warn!("  signal_dropped_disabled_cell oplog event (cross-correlate via");
            warn!("  trigger_ts). Rationale = 10-day TP backtest 5/6-5/16 identified");
            warn!("  ETH:15m as the only chronically deficitario cell. REVERSIBLE:");
            warn!("  clear bot.toml [markets] disabled_cells and restart.");
            warn!("====================================================================");
            warn!("");
            oplog_shared.sys("disabled_cells_active", serde_json::json!({
                "cells": config.markets.disabled_cells,
                "rationale": "10-day TP backtest 5/6-5/16 (data/derived/backtest_tp_v1) \
                              identified ETH:15m as the only chronically deficitario cell \
                              (519 trades, 51% win, -$0.23/trade, -$118.90 total drag).",
                "reversible": "clear bot.toml [markets] disabled_cells and restart",
            }));
        }
        info!(
            mode = effective_mode,
            stake_usd = config.stakes.base_usdc,
            regla_c = config.rules.regla_c_enabled,
            shadow = shadow_ctx.is_some(),
            live = live_backend.is_some(),
            restrict_15m_im = cli.restrict_15m_im,
            "trading loop: spawning decision/execution/exit tasks"
        );
        // PIECE 4: when a RestClient is available (shadow path or live), spawn the
        // periodic positions-refresh task. It populates state.resolved_tokens so
        // eval_guards' active-only filter is precise (drops resolved lots from the
        // cap denominator). No RestClient = conservative fallback (empty cache,
        // every lot counts) -- equivalent to the pre-piece-4 status quo.
        let refresh_rest: Option<std::sync::Arc<rest::RestClient>> =
            rest_ctx.as_ref().map(|sc| sc.rest.clone());
        // v2 rolling recalibrator: loaded from disk (survives restarts), shared
        // between the decision task (reads bias) and positions_refresh (feeds it).
        // Inert when v2 is disabled. Banner when active so it's unmissable.
        if config.v2.enabled {
            warn!("");
            warn!("====================================================================");
            warn!("  v2 STRATEGY ACTIVE (5minSnip port)");
            warn!("  z edge-gate (edge>={}) + depth-aware edge sizing + rolling recal", config.v2.edge_min);
            warn!("  vol_cap={} dvr_floor={} edge_ref={} base=${} max=${}",
                config.v2.vol_cap, config.v2.dvr_floor, config.v2.edge_ref,
                config.stakes.base_usdc, config.stakes.max_position_usdc);
            warn!("  REPLACES the 5bps trigger strategy. paper default; LIVE_ARMED gates live.");
            warn!("====================================================================");
            warn!("");
        }
        // Per-interval rolling recalibrators (5m + 15m), each loaded from its own
        // file. 5m is the original; 15m has its own so the two never cross-bias.
        if config.v2.enabled && config.v2.i15m.enabled {
            warn!("  15m MARKET ACTIVE: z_min={} max_ask={} late<=~{}s edge>={} recal={}",
                config.v2.i15m.z_min, config.v2.i15m.max_ask,
                config.v2.i15m.late_entry_max_ttl_s, config.v2.i15m.edge_min,
                config.v2.i15m.recal_path);
        }
        let recal_shared = v2::RecalSet {
            m5: std::sync::Arc::new(std::sync::Mutex::new(v2::load_recal(
                &config.v2.recal_path,
                config.v2.recal_capacity,
                config.v2.recal_warmup,
            ))),
            m15: std::sync::Arc::new(std::sync::Mutex::new(v2::load_recal(
                &config.v2.i15m.recal_path,
                config.v2.i15m.recal_capacity,
                config.v2.i15m.recal_warmup,
            ))),
            path_m5: config.v2.recal_path.clone(),
            path_m15: config.v2.i15m.recal_path.clone(),
        };
        // Live operator controls (dashboard-adjustable): stake + on/off. Start
        // with trading ENABLED and the config stakes; the dashboard can change
        // both at runtime without a restart. (LIVE_ARMED still gates live POSTs.)
        let controls_shared: std::sync::Arc<v2::Controls> = std::sync::Arc::new(
            v2::Controls::new(
                true,
                config.stakes.base_usdc,
                config.stakes.max_position_usdc,
                config.stakes.base_usdc,            // 15m stake defaults = 5m (adjust on dashboard)
                config.stakes.max_position_usdc,
                config.v2.inval_stop_enabled,       // stop master switch from config
                config.v2.inval_stop_dry_run,       // stop dry-run from config
                config.v2.reentry_opposite_enabled, // Order #13 D: opp re-entry from config
            ),
        );
        // Restore persisted operator controls (stakes, stop) so dashboard settings
        // SURVIVE restarts (systemd auto-restart / reboot / redeploy). Config values
        // above are only the first-boot fallback.
        let controls_path = "data/v2/controls.json".to_string();
        // ORDER #14 D: what the CONFIG asked for, captured before the persisted file
        // is applied over it. Used to make any override visible (never to win).
        let config_controls = controls_shared.snapshot();
        if let Some(snap) = v2::load_controls(&controls_path) {
            controls_shared.apply_snapshot(&snap);
            info!(?snap, "controls: restored persisted operator settings from {controls_path}");
            // ORDER #14 D: controls.json legitimately outranks config (operator
            // settings must survive restarts) — but silently. `inval_stop_dry:false`
            // overrode Order #12 C's config `true` for the entire audition and the
            // stop fired real paper sells while every log said it was dry. Precedence
            // is UNCHANGED; the divergence is now shouted, per field, with both values.
            let overrides = v2::control_overrides(&config_controls, &snap);
            for o in &overrides {
                warn!(
                    field = o.field, config = %o.config, control = %o.control,
                    "controls_override: controls.json is OVERRIDING the config file"
                );
                oplog_shared.sys("controls_override", serde_json::json!({
                    "field": o.field, "config": o.config, "control": o.control,
                    "source": controls_path,
                }));
            }
            if overrides.is_empty() {
                info!("controls: persisted settings agree with config (no overrides)");
            }
        }
        // The values actually in force after the restore — logged once so any future
        // analysis reads the REAL running config instead of inferring it from the toml.
        let effective_controls = controls_shared.snapshot();
        info!(?effective_controls, "controls: EFFECTIVE values in force");
        let mut handles: Vec<JoinHandle<()>> = vec![
            tokio::spawn(trading_loop::run_decision_task(
                state.clone(),
                state_store.state(),
                dec_cfg,
                guards_shared.clone(),
                oplog_shared.clone(),
                kline_rx,
                exec_tx,
                shutdown_rx.clone(),
                cli.restrict_15m_im,                  // G5-test-rig: default false; true via CLI flag
                config.markets.disabled_cells.clone(), // PRODUCTION filter (from bot.toml [markets])
                config.v2.clone(),                     // v2 strategy config (enabled=false → legacy path)
                controls_shared.clone(),               // live operator controls (stake / on-off)
                recal_shared.clone(),
                // live-arm gate: in --mode live, only enter when ARMED (disarmed = idle)
                if effective_mode == "live" { Some(live_armed_path_str.clone()) } else { None },
            )),
            tokio::spawn(trading_loop::run_execution_task(
                state_store.state(),
                exec_rx,
                shadow_ctx,
                live_backend.clone(),
                guards_shared.clone(),
                oplog_shared.clone(),
                shutdown_rx.clone(),
            )),
            tokio::spawn(trading_loop::run_exit_task(
                state.clone(),
                state_store.state(),
                guards_shared.clone(),
                live_backend.clone(),
                oplog_shared.clone(),
                // Pieza 4: pass the per-cell exit-rule config. Empty (default)
                // = every cell uses Baseline (time-exit) = pre-Pieza-4 byte-
                // identical behavior. Operator activates per-cell rules by
                // editing bot.toml `[exits.cells]`.
                Arc::new(config.exits.clone()),
                shutdown_rx.clone(),
            )),
        ];
        // Live dashboard HTTP server (read-only; bound to localhost → tunnel the
        // port). Spawned in any trading mode; reads in-memory state + the oplog.
        if config.dashboard.enabled {
            info!(bind = %config.dashboard.bind, port = config.dashboard.port,
                "spawning dashboard — tunnel with: ssh -N -L {p}:127.0.0.1:{p} user@vps then open http://localhost:{p}",
                p = config.dashboard.port);
            handles.push(tokio::spawn(dashboard::run_dashboard(
                state.clone(),
                state_store.state(),
                recal_shared.clone(),
                controls_shared.clone(),
                controls_path.clone(),
                effective_mode.to_string(),
                live_armed_path_str.clone(),
                kill_switch_path_str.clone(),
                oplog::OPLOG_PATH.to_string(),
                config.dashboard.bind.clone(),
                config.dashboard.port,
                state::now_ms(),
                config.v2.stake_mult_cap, // Order #13 A: sizing-clip WARN threshold
                config_controls.clone(),  // Order #14 D: config-vs-controls divergence
                shutdown_rx.clone(),
            )));
        }
        // Shared with the redemption task so winners recorded at redeem-time and
        // losers recorded at poll-time use the SAME recorder set (idempotent).
        // G8: build the shared PnlRecorder UNCONDITIONALLY (from the persisted file;
        // EMPTY on load failure). Settlement booking runs in PAPER too (via
        // run_settlement_booking below), and both modes MUST share ONE recorder set
        // for idempotency, so this can't live inside the live-only REST block.
        let pnl_recorder_shared = std::sync::Arc::new(std::sync::Mutex::new(
            match pnl_recorder::PnlRecorder::load(pnl_recorder::DEFAULT_PNL_RECORDED_LOG) {
                Ok(r) => {
                    info!(loaded = r.len(),
                        "pnl_recorder: persisted set loaded -- these token_ids are already accounted for");
                    r
                }
                Err(e) => {
                    warn!(error = %e,
                        "pnl_recorder: failed to load persisted set; starting EMPTY (will re-snapshot backlog)");
                    pnl_recorder::PnlRecorder::load(
                        std::env::temp_dir().join(format!("_empty_pnl_{}", state::now_ms()))
                    ).expect("load missing file returns empty")
                }
            },
        ));
        let mut pnl_recorder_for_redeem: Option<
            std::sync::Arc<std::sync::Mutex<pnl_recorder::PnlRecorder>>,
        > = None;
        if let Some(rest_arc) = refresh_rest.clone() {
            info!("PIECE 4: spawning positions_refresh (60s period) -- active-only cap reads REST snapshot + G8 P&L hook");
            pnl_recorder_for_redeem = Some(pnl_recorder_shared.clone());

            // G8: initial snapshot pass BEFORE positions_refresh ticks. Catalogs
            // the backlog of already-resolved positions WITHOUT recording P&L.
            // This is the catch-up control: today's daily counter starts at 0,
            // unaffected by losses from prior days.
            {
                let recorder_for_snapshot = pnl_recorder_shared.clone();
                let bs_for_snapshot = state_store.state();
                let guards_for_snapshot = guards_shared.clone();
                let oplog_for_snapshot = oplog_shared.clone();
                let rest_for_snapshot = rest_arc.clone();
                let mut recorder_lock = recorder_for_snapshot.lock().expect("recorder mutex");
                if let Err(e) = pnl_recorder::snapshot_existing_resolved(
                    rest_for_snapshot.as_ref(),
                    &bs_for_snapshot,
                    &mut recorder_lock,
                    &guards_for_snapshot,
                    oplog_for_snapshot.as_ref(),
                ).await {
                    warn!(error = %e,
                        "pnl_recorder: snapshot_existing_resolved failed -- backlog NOT marked. \
                         The next positions_refresh tick MAY register stale P&L into today's counter. \
                         Investigate /positions failure, then consider --guard-reset-daily-pnl on next restart.");
                }
            }

            handles.push(tokio::spawn(trading_loop::run_positions_refresh(
                rest_arc,
                state.clone(),
                oplog_shared.clone(),
                Duration::from_secs(60),
                shutdown_rx.clone(),
                state_store.state(),
                guards_shared.clone(),
                pnl_recorder_shared.clone(),
                recal_shared.clone(),          // v2 per-interval recalibrators (fed on resolution)
            )));
        } else {
            // PAPER: no Polymarket REST -> positions_refresh (which holds the
            // settlement booking) is NOT spawned. Run the standalone
            // settlement_booking task so held-to-settle positions still book P&L
            // from Binance close (+ the ring-gap REST fallback). Without this, paper
            // only ever books stops and held-to-settle positions pile up unbooked
            // (the accounting invariant ALERTs on them). Mode-independent: bs +
            // Binance only, no on-chain /positions or redemption coupling.
            info!("spawning settlement_booking task (paper P&L path — books held-to-settle from Binance close)");
            handles.push(tokio::spawn(trading_loop::run_settlement_booking(
                state.clone(),
                state_store.state(),
                guards_shared.clone(),
                pnl_recorder_shared.clone(),
                recal_shared.clone(),
                oplog_shared.clone(),
                Duration::from_secs(30),
                shutdown_rx.clone(),
            )));
        }
        // ORDER #14 C: feed watchdog — BOTH modes. Halts new entries while the
        // Binance feed is dead and through the post-recovery warmup, records the
        // outage in the oplog, and escalates to an alert file past 10 min. Warmup is
        // the LARGEST vol lookback across intervals so both 5m and 15m see a full
        // ring before they are allowed to compute z again.
        {
            let warmup_s = config.v2.vol_lookback_s.max(config.v2.i15m.vol_lookback_s);
            info!(warmup_s, "spawning feed_watchdog task (Order #14 C)");
            handles.push(tokio::spawn(feed_watchdog::run_feed_watchdog(
                state.clone(),
                oplog_shared.clone(),
                config.paths.alert_dir.clone(),
                warmup_s,
                Duration::from_secs(5),
                shutdown_rx.clone(),
            )));
        }
        // G5-wire: auto-redeem task. ONLY in live mode AND ONLY if the
        // builder creds are configured in .env. Graceful degradation if not:
        // the bot keeps running normally and the operator can claim manually
        // via the UI until they configure the Builder API Key triple
        // (POLYMARKET_BUILDER_KEY + _SECRET + _PASSPHRASE), derivable via
        // `--builder-creds-create --builder-creds-confirm` (G7.4-C1).
        //
        // NOT GATED BY LIVE_ARMED: auto-redeem is cleanup of EXISTING resolved
        // positions, not new exposure. Disarming the bot (rm LIVE_ARMED.txt)
        // means "no new orders" -- it should NOT mean "leave winnings stranded".
        // The task runs whenever the bot is up, in live mode, with creds set.
        if effective_mode == "live" {
            if let Some(sc) = rest_ctx.as_ref() {
                match redemption::setup_from_env(|k| std::env::var(k).ok()) {
                    redemption::RedemptionSetup::Enabled { relayer, builder_code } => {
                        let cfg = redemption::RedemptionConfig::from_defaults(
                            sc.pk.clone(),
                            sc.rest.funder(),
                            builder_code,
                        );
                        info!(
                            poll_interval_secs = cfg.poll_interval.as_secs(),
                            proxy_wallet = %cfg.proxy_wallet,
                            intent_log = %cfg.intent_log_path.display(),
                            redemptions_log = %cfg.redemptions_log_path.display(),
                            "G5: AUTO-REDEEM ENABLED -- resolved positions claimed via Polymarket Relayer (gas-free, NOT gated by LIVE_ARMED)"
                        );
                        oplog_shared.sys("redemption_task_enabled", serde_json::json!({
                            "poll_interval_secs": cfg.poll_interval.as_secs(),
                            "proxy_wallet": format!("{:#x}", cfg.proxy_wallet),
                        }));
                        handles.push(tokio::spawn(redemption::run_redemption_task(
                            sc.rest.clone(),
                            relayer,
                            cfg,
                            oplog_shared.clone(),
                            shutdown_rx.clone(),
                            // P&L: record WINNERS at redeem-time (they vanish from
                            // /positions before the poll-based recorder can see them).
                            state_store.state(),
                            guards_shared.clone(),
                            pnl_recorder_for_redeem.clone(),
                            // shared state: skip redeeming known losers (quota saver).
                            state.clone(),
                        )));
                    }
                    redemption::RedemptionSetup::Disabled { reason } => {
                        warn!(%reason,
                            "G5: AUTO-REDEEM DISABLED -- the bot runs normally; resolved positions will NOT be auto-claimed. Set the relayer creds in .env to enable; manual UI claim remains available.");
                        oplog_shared.sys("redemption_task_disabled", serde_json::json!({
                            "reason": reason,
                        }));
                    }
                }
            }
        }
        handles
    } else {
        Vec::new()
    };

    // --- Remaining placeholder tasks (real logic arrives in later phases) ----
    let stub_handles = [
        (
            "polymarket_user_ws",
            tokio::spawn(tasks::polymarket_user_ws()),
        ),
        ("balance_refresh", tokio::spawn(tasks::balance_refresh())),
    ];
    info!(
        ws_tasks = 2,
        discovery_tasks = 1,
        persist_tasks = 1,
        stub_tasks = stub_handles.len(),
        "all tasks spawned; press Ctrl-C to stop"
    );

    // --- Wait for Ctrl-C (or the timed run window), then shut down gracefully -
    match cli.run_secs {
        Some(secs) => {
            info!(run_secs = secs, "timed run; will stop after the window or on Ctrl-C");
            tokio::select! {
                r = tokio::signal::ctrl_c() => {
                    r.context("listening for Ctrl-C")?;
                    info!("shutdown signal received (Ctrl-C); stopping ingestion");
                }
                _ = tokio::time::sleep(Duration::from_secs(secs)) => {
                    info!(run_secs = secs, "run window elapsed; stopping ingestion");
                }
            }
        }
        None => {
            tokio::signal::ctrl_c()
                .await
                .context("listening for Ctrl-C")?;
            info!("shutdown signal received; stopping ingestion");
        }
    }
    let _ = shutdown_tx.send(true);

    // The WS clients and stats loop watch the shutdown signal; give them a brief
    // window to break out of their loops and (for the WS clients) flush.
    join_or_abort("binance_ws", binance).await;
    join_or_abort("polymarket_ws", polymarket).await;
    join_or_abort("market_discovery", discovery_task).await;
    join_or_abort("state_persist", persist_task).await;
    join_or_abort("stats", stats).await;
    // D1 trading loop tasks (paper mode only) — they watch the shutdown signal.
    for (i, h) in trading_tasks.into_iter().enumerate() {
        join_or_abort("trading_loop", h).await;
        let _ = i;
    }

    // The placeholder tasks don't observe shutdown yet â€” abort them.
    for (name, handle) in stub_handles {
        handle.abort();
        debug!(task = name, "aborted stub task");
    }

    // Drop our logger handle so the writer task sees all senders gone, flushes
    // the tail of the JSONL, and exits.
    drop(event_logger);
    join_or_abort("event_logger", event_handle).await;

    info!("rust_bot stopped cleanly");
    Ok(())
}

/// Await a task for up to 5s during shutdown; abort it if it overruns so the
/// process can exit promptly.
async fn join_or_abort(name: &str, mut handle: JoinHandle<()>) {
    tokio::select! {
        res = &mut handle => match res {
            Ok(()) => debug!(task = name, "stopped"),
            Err(e) => warn!(task = name, error = %e, "task join error"),
        },
        _ = tokio::time::sleep(Duration::from_secs(5)) => {
            warn!(task = name, "join timed out on shutdown; aborting");
            handle.abort();
        }
    }
}

/// Log whether each expected credential is present in the environment. Logs the
/// key NAME and a boolean only â€” never the value.
fn check_credentials_present() {
    for key in CREDENTIAL_KEYS {
        let present = std::env::var(key).map(|v| !v.is_empty()).unwrap_or(false);
        if present {
            info!(key, "credential present (not used in Phase 0)");
        } else {
            warn!(key, "credential missing or empty (ok for Phase 0)");
        }
    }
}

/// Phase 2 read-only probe: build the REST client and query every endpoint once
/// against live Polymarket, logging the results. Strictly read-only â€” no orders.
async fn rest_check(config: &Config, token_override: Option<String>) -> anyhow::Result<()> {
    use crate::discovery::{RestTokenSource, TokenSource};

    let getenv = |k: &str| std::env::var(k).map_err(|_| anyhow::anyhow!("missing env {k}"));
    let pk = getenv("POLYMARKET_PRIVATE_KEY")?;
    let ak = getenv("POLYMARKET_API_KEY")?;
    let asec = getenv("POLYMARKET_API_SECRET")?;
    let apass = getenv("POLYMARKET_API_PASSPHRASE")?;
    let funder = getenv("POLYMARKET_FUNDER_ADDRESS")?;

    let timeout = Duration::from_secs(config.connections.connect_timeout_s.max(10));
    let client = rest::RestClient::connect(
        &config.connections.polymarket_rest_url,
        "https://data-api.polymarket.com",
        &pk,
        &ak,
        &asec,
        &apass,
        &funder,
        timeout,
    )
    .await
    .context("building REST client")?;
    info!("rest-check: authenticated CLOB client built (POLY_PROXY / sig_type=1)");

    match client.get_balance().await {
        Ok(b) => info!(
            balance_usdc = b.balance_usdc,
            balance_raw = b.balance_raw,
            allowance_contracts = b.allowance_contracts,
            "rest-check: get_balance"
        ),
        Err(e) => warn!(error = %e, "rest-check: get_balance FAILED"),
    }

    match client.get_positions().await {
        Ok(ps) => {
            info!(count = ps.len(), "rest-check: get_positions");
            for p in ps.iter().take(5) {
                info!(token = %p.token_id, size = p.size, avg_price = p.avg_price,
                      cur_price = p.cur_price, "rest-check:   position");
            }
        }
        Err(e) => warn!(error = %e, "rest-check: get_positions FAILED"),
    }

    // Probe the per-token endpoints on either the caller-specified token or a
    // freshly-discovered one.
    let token = match token_override {
        Some(t) => {
            info!(token = %t, "rest-check: probing caller-specified token");
            t
        }
        None => {
            let http = reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .context("rest-check http client")?;
            let source = RestTokenSource::new(
                http,
                config.markets.assets.clone(),
                config.markets.intervals.clone(),
                config.markets.discovery_lookahead,
                config.connections.polymarket_gamma_url.clone(),
                config.connections.polymarket_rest_url.clone(),
            );
            let tokens = source.discover().await.context("discovering tokens")?;
            match tokens.first().cloned() {
                Some(t) => {
                    info!(token = %t, discovered = tokens.len(), "rest-check: sample token");
                    t
                }
                None => {
                    warn!("rest-check: discovery returned no tokens; skipping per-token probes");
                    return Ok(());
                }
            }
        }
    };

    let ask = client.get_price(&token, rest::Side::Sell).await;
    let bid = client.get_price(&token, rest::Side::Buy).await;
    let mid = client.get_midpoint(&token).await;
    let tick = client.get_tick_size(&token).await;
    let nr = client.get_neg_risk(&token).await;
    let cond = client.get_market_by_token(&token).await;
    let book = client.get_book(&token).await;
    info!(ask = ?ask, bid = ?bid, midpoint = ?mid, tick_size = ?tick,
          neg_risk = ?nr, condition_id = ?cond, "rest-check: per-token endpoints");
    info!(book = ?book, "rest-check: /book (prices UNRELIABLE per documented bug)");
    Ok(())
}

/// G7.2 read-only relayer probe -- the ISOLATED diagnostic for the 100% redeem
/// failure observed in production. Fires EXACTLY ONE call: the same
/// `GET /relay-payload?address=<EOA>&type=PROXY` that
/// `relayer::build_redeem_submit_body` issues at step 1. NO headers (the
/// endpoint is PUBLIC upstream -- G7.4-A removed the wrong-by-inference
/// `RELAYER_API_KEY` pair). Uses the same `setup_from_env` the production task
/// uses, so the env-gate is the same path the live bot would take. Logs the
/// FULL anyhow chain on failure (status + body + URL) via `format!("{e:#}")`
/// so the operator sees the wire-level reason without operating.
///
/// Does NOT: sign, build/encode meta-tx, submit, redeem, spawn the trader.
/// May safely run with the bot stopped on the VPS.
///
/// SECRETS POLICY (enforced by construction, verified by review):
///   * Reads `POLYMARKET_BUILDER_KEY` / `_SECRET` / `_PASSPHRASE` /
///     `_PRIVATE_KEY` from `.env` via `std::env::var` (already loaded by
///     `dotenvy::dotenv()` upstream in `main`). The BUILDER triple is used
///     to construct the RelayerClient via setup_from_env (so this probe
///     mirrors the live path exactly), but is NOT sent over the wire by
///     /relay-payload itself (PUBLIC endpoint).
///   * NEVER logs the value of any credential. Logs only:
///       - the env var NAMES + a boolean "is set / non-empty".
///       - the EOA address derived from the private key (PUBLIC, appears in any
///         on-chain tx the EOA signs; the URL embeds it as `address=`).
///       - the HTTP URL + HEADER NAMES (no values).
///       - the relayer's response body (an error string from the relayer; the
///         relayer does not echo back our creds in its error bodies).
async fn redeem_check() -> anyhow::Result<()> {
    use crate::redemption::{
        ENV_BUILDER_CODE, ENV_BUILDER_KEY, ENV_BUILDER_PASSPHRASE, ENV_BUILDER_SECRET,
        RedemptionSetup, setup_from_env,
    };
    use crate::relayer::{
        DEFAULT_RELAYER_URL, build_get_relay_payload_url, signer_from_key,
    };
    // `PrivateKeySigner::address()` is an inherent method (NOT via Signer trait),
    // so no trait import is needed here.

    info!("=== redeem-check: read-only diagnostic of relayer /relay-payload ===");
    info!(
        "This call does NOT sign, submit, redeem, or move capital -- one GET only. \
         If the bot is running on this host, redeem-check does not interfere with it."
    );

    // 1) Env-var presence (boolean only -- NEVER logs the values).
    //    Trim-then-is_empty mirrors setup_from_env's check so "key set to spaces"
    //    is reported the same way the production task treats it.
    //
    //    G7.4-C2 note: /relay-payload itself is PUBLIC and needs none of the
    //    BUILDER creds. We still gate on setup_from_env() here so this probe
    //    rejects the same way the live task would -- the BUILDER triple is
    //    required at startup (for POST /submit later in the pipeline).
    let is_set = |k: &str| {
        std::env::var(k)
            .map(|v| !v.trim().is_empty())
            .unwrap_or(false)
    };
    let builder_key_set = is_set(ENV_BUILDER_KEY);
    let builder_secret_set = is_set(ENV_BUILDER_SECRET);
    let builder_passphrase_set = is_set(ENV_BUILDER_PASSPHRASE);
    let builder_code_set = is_set(ENV_BUILDER_CODE);
    let privkey_set = is_set("POLYMARKET_PRIVATE_KEY");
    info!(
        builder_key_set,
        builder_secret_set,
        builder_passphrase_set,
        builder_code_set,  // optional (attribution metadata in body, not auth)
        private_key_set = privkey_set,  // needed to derive the EOA for `address=` param
        "redeem-check: env-var presence (booleans only; values NEVER logged)"
    );

    // 2) Build the relayer the SAME way the production task does. If the env
    //    vars are missing/empty, the disabled reason here matches the one the
    //    real task would log on startup.
    let setup = setup_from_env(|k| std::env::var(k).ok());
    let relayer = match setup {
        RedemptionSetup::Enabled { relayer, builder_code: _ } => {
            info!("redeem-check: setup_from_env -> Enabled (RelayerClient built)");
            relayer
        }
        RedemptionSetup::Disabled { reason } => {
            warn!(%reason, "redeem-check: setup_from_env -> Disabled");
            anyhow::bail!("relayer setup disabled (no probe to send): {reason}");
        }
    };

    // 3) Derive the EOA from POLYMARKET_PRIVATE_KEY. We need its ADDRESS for the
    //    `address=` query param -- the relayer scopes nonce + relay assignment by
    //    EOA. We do NOT sign anything; just call `signer.address()`.
    let privkey = std::env::var("POLYMARKET_PRIVATE_KEY")
        .context("POLYMARKET_PRIVATE_KEY missing in .env -- required to derive the EOA")?;
    let signer = signer_from_key(privkey.trim())
        .context("could not build signer from POLYMARKET_PRIVATE_KEY (value NEVER logged)")?;
    let eoa = signer.address();
    info!(
        eoa = %eoa,
        "redeem-check: derived EOA from POLYMARKET_PRIVATE_KEY (address is PUBLIC; the key value is not logged)"
    );

    // 4) Log the exact GET we'll fire. Built through `build_get_relay_payload_url`
    //    (the SAME pure helper RelayerClient::get_relay_payload uses), so the
    //    logged URL is byte-for-byte what the production task sends -- the
    //    operator can copy it into curl/Postman to cross-check independently.
    //    G7.4-A: path is `/relay-payload?address=...` (was wrongly `/getRelayPayload?from=...`,
    //    which caused the 100% 404 surfaced by this very diagnostic in G7.2).
    let url = build_get_relay_payload_url(DEFAULT_RELAYER_URL, eoa);
    info!(url, "redeem-check: target URL (built via the SAME pure helper the real client uses)");
    info!(
        "redeem-check: request carries NO auth headers (per G7.4-A: /relay-payload is a PUBLIC \
         endpoint upstream -- see Polymarket/builder-relayer-client src/client.ts:150-156)"
    );

    // 5) Fire the probe. This is identical to the call that fails in
    //    `relayer::build_redeem_submit_body` step 1 today.
    info!("redeem-check: dispatching GET ...");
    match relayer.get_relay_payload(eoa).await {
        Ok(payload) => {
            // SUCCESS shape: the relayer accepted our auth + returned a valid
            // payload. If this succeeds but production redeem still fails, the
            // issue is downstream (build/sign/submit), NOT auth or URL.
            info!(
                relay_address = %payload.address,
                nonce = %payload.nonce,
                "redeem-check: SUCCESS -- relayer accepted auth + returned a valid payload. \
                 If production redeem STILL fails, the issue is downstream of get_relay_payload \
                 (encoding / signing / submit / poll), NOT auth or URL."
            );
            Ok(())
        }
        Err(e) => {
            // FAILURE shape: this is THE error the production task hits 100% of the
            // time. The G7.1-diag fix to `format!("{e:#}")` makes the full anyhow
            // chain visible -- status code + body + URL + the reqwest root cause.
            let chain = format!("{e:#}");
            error!(
                error_chain = %chain,
                "redeem-check: FAILED -- this is the exact error production redeem hits today"
            );
            // Bail with the chain so the process exit message AND stdout both carry it.
            anyhow::bail!("redeem-check probe failed: {chain}");
        }
    }
}

/// G7.4-C0 read-only CLOB probe -- list this account's Builder API Keys.
///
/// Authenticates against the CLOB using the SAME credentials the bot already uses
/// for trading (POLYMARKET_API_KEY / _SECRET / _PASSPHRASE / _PRIVATE_KEY /
/// _FUNDER_ADDRESS), then GETs `https://clob-v2.polymarket.com/auth/builder-api-key`
/// via the SDK's `clob::Client::builder_api_keys()`.
///
/// Decides Step C1: if the response shows ZERO active builder keys, the operator
/// must CREATE one (Step C1) before the relayer's POST `/submit` HMAC (Step C2)
/// can be wired. If a key already exists AND the operator still has its
/// secret+passphrase saved somewhere from a prior creation, no new key is needed.
///
/// SECRETS POLICY:
///   * READS POLYMARKET_API_KEY / _SECRET / _PASSPHRASE / _PRIVATE_KEY /
///     _FUNDER_ADDRESS from the already-loaded `.env`. NEVER logs the values.
///   * The `/auth/builder-api-key` LIST endpoint upstream returns ONLY
///     `{key (UUID), createdAt, revokedAt}` per `BuilderApiKeyResponse`. It does
///     NOT include any secret or passphrase -- those are returned ONLY by the
///     CREATE endpoint (Step C1), at the moment of creation, never again.
///   * If the operator pasted a `POLYMARKET_BUILDER_KEY` UUID into `.env`, the
///     probe cross-checks whether it matches one of the returned UUIDs (so the
///     operator can tell which key they had in mind).
async fn builder_creds_list(config: &Config) -> anyhow::Result<()> {
    info!("=== builder-creds-list: read-only diagnostic of CLOB /auth/builder-api-key ===");
    info!(
        "This call does NOT create, revoke, or modify any builder credentials -- one GET only. \
         If the bot is running on this host, builder-creds-list does not interfere with it."
    );

    let is_set = |k: &str| {
        std::env::var(k)
            .map(|v| !v.trim().is_empty())
            .unwrap_or(false)
    };
    let private_key_set = is_set("POLYMARKET_PRIVATE_KEY");
    let api_key_set = is_set("POLYMARKET_API_KEY");
    let api_secret_set = is_set("POLYMARKET_API_SECRET");
    let api_passphrase_set = is_set("POLYMARKET_API_PASSPHRASE");
    let funder_set = is_set("POLYMARKET_FUNDER_ADDRESS");
    let builder_key_hint_set = is_set("POLYMARKET_BUILDER_KEY");
    info!(
        private_key_set,
        api_key_set,
        api_secret_set,
        api_passphrase_set,
        funder_set,
        builder_key_hint_set, // optional -- only used to cross-check against listed UUIDs
        "builder-creds-list: env-var presence (booleans only; values NEVER logged)"
    );

    let getenv = |k: &str| std::env::var(k).map_err(|_| anyhow::anyhow!("missing env {k}"));

    info!("builder-creds-list: authenticating against the CLOB (clob-v2.polymarket.com) ...");
    let client = rest::RestClient::connect(
        &config.connections.polymarket_rest_url,
        "https://data-api.polymarket.com",
        &getenv("POLYMARKET_PRIVATE_KEY")?,
        &getenv("POLYMARKET_API_KEY")?,
        &getenv("POLYMARKET_API_SECRET")?,
        &getenv("POLYMARKET_API_PASSPHRASE")?,
        &getenv("POLYMARKET_FUNDER_ADDRESS")?,
        Duration::from_secs(30),
    )
    .await
    .context("building authenticated CLOB client")?;
    info!("builder-creds-list: CLOB client authenticated (POLY_PROXY / sig_type=1)");

    // The cross-check UUID, only if the operator already pasted one. Stored as a
    // lowercase string so the comparison is case-insensitive against the UUID
    // serialization the SDK returns.
    let hinted_uuid = std::env::var("POLYMARKET_BUILDER_KEY")
        .ok()
        .map(|s| s.trim().to_ascii_lowercase())
        .filter(|s| !s.is_empty());

    info!("builder-creds-list: dispatching GET /auth/builder-api-key ...");
    let keys = client
        .clob()
        .builder_api_keys()
        .await
        .map_err(|e| anyhow::anyhow!("builder_api_keys SDK call failed: {e}"))
        .context("listing builder API keys")?;

    info!(
        count = keys.len(),
        "builder-creds-list: account currently has this many Builder API Key entries (active + revoked)"
    );

    if keys.is_empty() {
        warn!(
            "builder-creds-list: NO Builder API Keys on this account. \
             To unblock the relayer's POST /submit (Step C2), Step C1 must create one \
             (CLI tool to be added next, gated by --confirm)."
        );
        return Ok(());
    }

    let mut active = 0usize;
    let mut revoked = 0usize;
    let mut hint_matched = false;
    for (i, k) in keys.iter().enumerate() {
        let key_str = k.key.to_string();
        let key_lower = key_str.to_ascii_lowercase();
        let is_revoked = k.revoked_at.is_some();
        if is_revoked {
            revoked += 1;
        } else {
            active += 1;
        }
        let matches_hint = hinted_uuid.as_deref().is_some_and(|h| h == key_lower);
        if matches_hint {
            hint_matched = true;
        }
        info!(
            index = i,
            key_uuid = %key_str,
            // chrono's Display for DateTime<Utc> renders ISO-8601 UTC; safe to log.
            created_at = %k.created_at.map(|t| t.to_rfc3339()).unwrap_or_else(|| "unknown".to_string()),
            revoked_at = %k.revoked_at.map(|t| t.to_rfc3339()).unwrap_or_else(|| "null".to_string()),
            status = if is_revoked { "REVOKED" } else { "ACTIVE" },
            matches_env_hint = matches_hint,
            "builder-creds-list: key entry"
        );
    }

    info!(
        active,
        revoked,
        "builder-creds-list: summary"
    );

    // Cross-check + advice tailored to what we found.
    match (hinted_uuid.as_deref(), hint_matched, active) {
        (Some(_), true, _) => {
            info!(
                "builder-creds-list: POLYMARKET_BUILDER_KEY MATCHES one of the listed UUIDs. \
                 If you ALSO have its secret + passphrase saved (from when you originally created it), \
                 add them to .env as POLYMARKET_BUILDER_SECRET + POLYMARKET_BUILDER_PASSPHRASE \
                 (names TBC -- we'll wire whatever you pick in Step C2). \
                 If you do NOT have the secret + passphrase, you must REVOKE this key and CREATE \
                 a new one (Step C1) to get a fresh triple."
            );
        }
        (Some(_), false, _) => {
            warn!(
                "builder-creds-list: POLYMARKET_BUILDER_KEY DOES NOT MATCH any UUID returned by the API. \
                 Possible causes: typo, the key was revoked but the env-var wasn't updated, or the key \
                 belongs to a different account. Verify the value or proceed to Step C1 (create fresh)."
            );
        }
        (None, _, 0) => {
            warn!(
                "builder-creds-list: all listed keys are REVOKED. Step C1 (create fresh) is required \
                 before Step C2 can be wired."
            );
        }
        (None, _, n) => {
            // n >= 1 (the only remaining case after (None,_,0) above).
            info!(
                "builder-creds-list: no POLYMARKET_BUILDER_KEY hint in .env. {n} ACTIVE key(s) found \
                 on the account. If you have the secret+passphrase of one of them from a past creation, \
                 add the triple to .env; otherwise Step C1 (create fresh) is needed.",
                n = n
            );
        }
    }

    Ok(())
}

/// G7.4-C1 action -- CREATE a new Builder API Key on the authenticated CLOB account.
///
/// Two modes, gated by `confirmed` (= the `--builder-creds-confirm` flag):
///   * `confirmed == false` -> DRY-RUN. Prints exactly what it would do (env-vars
///     it would read, endpoint it would hit, env-var names where the operator
///     should paste the result). Does NOT touch the network at all. Default if
///     the operator only passes `--builder-creds-create`.
///   * `confirmed == true` -> EXECUTE. Authenticates against the CLOB, calls the
///     SDK's `create_builder_api_key()` (`POST /auth/builder-api-key`), and on
///     success emits the new `(key, secret, passphrase)` triple to STDERR as
///     three KEY=VALUE lines plus framing banner. The operator copies those
///     three lines verbatim into `.env`.
///
/// SECRETS POLICY:
///   * The DRY-RUN path NEVER touches creds (zero network, zero output of values).
///   * The EXECUTE path emits secret + passphrase to **STDERR via `eprintln!`**,
///     NOT via the `tracing` macros. The tracing pipeline writes structured logs
///     to a file on disk; bypassing it ensures the secret never reaches the file
///     log. The `info!` lines are about STATUS only (never values).
///   * The operator is warned explicitly that piping stderr to a file
///     (`2>>log.txt`, systemd journal, etc) would defeat this protection -- they
///     are responsible for an interactive terminal during this one-shot command.
///   * `Credentials::secret()` + `passphrase()` return `SecretString`; we call
///     `expose_secret()` only at the eprintln! boundary, never earlier.
async fn builder_creds_create(config: &Config, confirmed: bool) -> anyhow::Result<()> {
    info!("=== builder-creds-create: CREATE Builder API Key (POST /auth/builder-api-key) ===");

    if !confirmed {
        warn!(
            "DRY-RUN MODE -- --builder-creds-confirm was NOT passed. NO key will be created, \
             no network call is made. Re-run with --builder-creds-confirm to actually create."
        );
        info!("With --builder-creds-confirm, this command will:");
        info!("  1. Authenticate against clob-v2.polymarket.com using your existing CLOB L2 creds");
        info!("     (POLYMARKET_API_KEY / _SECRET / _PASSPHRASE / _PRIVATE_KEY / _FUNDER_ADDRESS).");
        info!("  2. POST /auth/builder-api-key (creates a FRESH Builder API Key on your account).");
        info!("  3. Print the new (key, secret, passphrase) triple to STDERR -- ONE-SHOT,");
        info!("     not via tracing (so secrets do NOT enter the file log).");
        info!("  4. Exit. You copy the 3 emitted KEY=VALUE lines verbatim into .env.");
        info!("");
        info!("Env-var names the new key will use (matches Step C2 HMAC code):");
        info!("    POLYMARKET_BUILDER_KEY=<the new uuid>");
        info!("    POLYMARKET_BUILDER_SECRET=<the new secret>");
        info!("    POLYMARKET_BUILDER_PASSPHRASE=<the new passphrase>");
        info!("");
        info!(
            "Account state per --builder-creds-list (last run): 3 ACTIVE keys. This command is \
             ADDITIVE: existing keys (active or revoked) are NOT modified, the new key is a 4th \
             sibling. Revoking older keys (--builder-creds-revoke <uuid>, future tool) is \
             optional hygiene, not required for the new key to work."
        );
        info!("");
        warn!(
            "BEFORE you run with --builder-creds-confirm: open `.env` in an editor and have it \
             ready. The terminal MUST be interactive (NO `2>>file`, NO `tee`, NO captured \
             systemd journal) -- the secret is emitted to STDERR exactly once."
        );
        return Ok(());
    }

    // CONFIRMED EXECUTION PATH ------------------------------------------------

    let is_set = |k: &str| {
        std::env::var(k)
            .map(|v| !v.trim().is_empty())
            .unwrap_or(false)
    };
    info!(
        private_key_set = is_set("POLYMARKET_PRIVATE_KEY"),
        api_key_set = is_set("POLYMARKET_API_KEY"),
        api_secret_set = is_set("POLYMARKET_API_SECRET"),
        api_passphrase_set = is_set("POLYMARKET_API_PASSPHRASE"),
        funder_set = is_set("POLYMARKET_FUNDER_ADDRESS"),
        "builder-creds-create: env-var presence (booleans only; values NEVER logged)"
    );

    let getenv = |k: &str| std::env::var(k).map_err(|_| anyhow::anyhow!("missing env {k}"));

    info!("builder-creds-create: authenticating against the CLOB (clob-v2.polymarket.com) ...");
    let client = rest::RestClient::connect(
        &config.connections.polymarket_rest_url,
        "https://data-api.polymarket.com",
        &getenv("POLYMARKET_PRIVATE_KEY")?,
        &getenv("POLYMARKET_API_KEY")?,
        &getenv("POLYMARKET_API_SECRET")?,
        &getenv("POLYMARKET_API_PASSPHRASE")?,
        &getenv("POLYMARKET_FUNDER_ADDRESS")?,
        Duration::from_secs(30),
    )
    .await
    .context("building authenticated CLOB client")?;
    info!("builder-creds-create: CLOB client authenticated (POLY_PROXY / sig_type=1)");

    info!("builder-creds-create: dispatching POST /auth/builder-api-key ...");
    let creds = client
        .clob()
        .create_builder_api_key()
        .await
        .map_err(|e| anyhow::anyhow!("create_builder_api_key SDK call failed: {e}"))
        .context("creating builder API key")?;

    // Critical: import ExposeSecret only at the call boundary, NOT module-level,
    // to minimize the scope where secret values can be extracted from the
    // SecretString wrapper. `Credentials::secret()` / `passphrase()` return
    // `&SecretString`; `.expose_secret()` is the explicit opt-out.
    use polymarket_client_sdk_v2::auth::ExposeSecret;

    info!(
        key_uuid = %creds.key(),
        "builder-creds-create: SUCCESS -- new Builder API Key created on the account. \
         The UUID is also logged above (it is the PUBLIC part). Secret + passphrase \
         follow on STDERR ONLY -- copy them now."
    );

    // Emit to STDERR via eprintln! -- NOT via tracing -- so the values never
    // enter the file log. Big visual delimiters so the operator cannot miss
    // the boundary of the secret region.
    eprintln!();
    eprintln!("########################################################################");
    eprintln!("#  NEW BUILDER API KEY -- COPY THESE 3 LINES TO .env RIGHT NOW.        #");
    eprintln!("#  The secret + passphrase are returned ONLY here, ONLY this once.     #");
    eprintln!("#  If you lose them, revoke this key and create another one.           #");
    eprintln!("########################################################################");
    eprintln!();
    eprintln!("POLYMARKET_BUILDER_KEY={}", creds.key());
    eprintln!("POLYMARKET_BUILDER_SECRET={}", creds.secret().expose_secret());
    eprintln!(
        "POLYMARKET_BUILDER_PASSPHRASE={}",
        creds.passphrase().expose_secret()
    );
    eprintln!();
    eprintln!("########################################################################");
    eprintln!("#  END NEW BUILDER API KEY. Paste the 3 lines above into .env, save.   #");
    eprintln!("########################################################################");
    eprintln!();

    info!(
        "builder-creds-create: emission complete. Open .env, paste the 3 lines above, save. \
         Then proceed to Step C2 (HMAC wiring for POST /submit)."
    );

    Ok(())
}

/// Phase 3: at live startup, reconcile cached positions against on-chain reality
/// (REST `get_positions`). Divergences beyond a lag-tolerant threshold are
/// alerted for manual review. Runs only for `--mode live` (the full live path is
/// Phase 6); paper never reconciles.
async fn reconcile_on_chain(config: &Config, store: &state::store::StateStore) -> anyhow::Result<()> {
    use rust_decimal::Decimal;

    let getenv = |k: &str| std::env::var(k).map_err(|_| anyhow::anyhow!("missing env {k}"));
    let client = rest::RestClient::connect(
        &config.connections.polymarket_rest_url,
        "https://data-api.polymarket.com",
        &getenv("POLYMARKET_PRIVATE_KEY")?,
        &getenv("POLYMARKET_API_KEY")?,
        &getenv("POLYMARKET_API_SECRET")?,
        &getenv("POLYMARKET_API_PASSPHRASE")?,
        &getenv("POLYMARKET_FUNDER_ADDRESS")?,
        Duration::from_secs(config.connections.connect_timeout_s.max(10)),
    )
    .await
    .context("REST client for reconciliation")?;

    let onchain_raw = client.get_positions().await.context("get_positions")?;
    // data-api size is f64; convert to Decimal for the (lag-tolerant) comparison.
    let onchain: Vec<(String, Decimal)> = onchain_raw
        .iter()
        .map(|p| (p.token_id.clone(), Decimal::try_from(p.size).unwrap_or(Decimal::ZERO)))
        .collect();
    let cached = state_store_positions(store);
    // 0.01-share threshold tolerates the documented data-api lag (Phase 2).
    let divs = state::store::reconcile(&cached, &onchain, Decimal::new(1, 2));
    if divs.is_empty() {
        info!(cached = cached.len(), onchain = onchain.len(), "reconciliation clean");
    } else {
        for d in &divs {
            warn!(
                token = %d.token_id, kind = ?d.kind,
                cached = %d.cached_shares, onchain = %d.onchain_shares,
                "reconciliation divergence"
            );
        }
        ws::write_alert(
            &config.paths.alert_dir,
            "state",
            "reconcile_divergence",
            &format!("{} position divergence(s) vs on-chain", divs.len()),
        );
    }
    Ok(())
}

/// Snapshot the cached position lots out from under the lock (avoids holding the
/// lock across the async reconcile).
fn state_store_positions(
    store: &state::store::StateStore,
) -> Vec<state::persist::OpenPosition> {
    store.state().lock().expect("state mutex poisoned").positions.clone()
}

/// Phase 6 D2: build the SHADOW context (authenticated RestClient + the private key)
/// for the LiveExecutor build+sign-no-POST path. Read-only REST + LOCAL signing;
/// ZERO capital — the shadow path never calls `post_order`. Mirrors the reconcile
/// cred pattern. The pk string is carried (not a typed signer; the signer is rebuilt
/// locally where its type can be inferred) and is NEVER logged.
async fn build_shadow_ctx(config: &Config) -> anyhow::Result<trading_loop::ShadowCtx> {
    let getenv = |k: &str| std::env::var(k).map_err(|_| anyhow::anyhow!("missing env {k}"));
    let pk = getenv("POLYMARKET_PRIVATE_KEY")?;
    let rest = rest::RestClient::connect(
        &config.connections.polymarket_rest_url,
        "https://data-api.polymarket.com",
        &pk,
        &getenv("POLYMARKET_API_KEY")?,
        &getenv("POLYMARKET_API_SECRET")?,
        &getenv("POLYMARKET_API_PASSPHRASE")?,
        &getenv("POLYMARKET_FUNDER_ADDRESS")?,
        Duration::from_secs(config.connections.connect_timeout_s.max(10)),
    )
    .await
    .context("D2 shadow: REST client")?;
    Ok(trading_loop::ShadowCtx {
        rest: Arc::new(rest),
        pk,
        max_slippage: config.rules.max_slippage,
    })
}
