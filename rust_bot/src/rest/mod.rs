//! Phase 2: read-only Polymarket REST client.
//!
//! Thin wrapper over `polymarket_client_sdk_v2`'s CLOB (authenticated) + Data
//! (public) clients. Provides the read-only queries the bot needs: balance,
//! positions, price/midpoint, tick size, neg-risk, market lookup, and (with a
//! loud caveat) the order book.
//!
//! Strictly READ-ONLY: this module exposes NO order placement. Orders arrive in
//! Phase 6 with their own signing + idempotency path — see the warning on
//! [`RestClient::retrying`].
//!
//! Auth replicates the Python `live_execution_test` (validated against real
//! capital): POLY_PROXY (`SignatureType::Proxy`, sig_type=1), funder = proxy
//! address, L2 HMAC from the `.env` API credentials.

#![allow(dead_code)] // most accessors are consumed by the signal/decision engines (Phase 4-5)

use std::future::Future;
use std::str::FromStr;
use std::time::Duration;

use anyhow::{Context, anyhow};
use polymarket_client_sdk_v2::auth::state::Authenticated;
use polymarket_client_sdk_v2::auth::{Credentials, LocalSigner, Normal, Signer, Uuid};
use polymarket_client_sdk_v2::clob::types::request::{
    BalanceAllowanceRequest, MidpointRequest, OrderBookSummaryRequest, PriceRequest,
};
use polymarket_client_sdk_v2::clob::types::{AssetType, SignatureType};
pub use polymarket_client_sdk_v2::clob::types::Side;
use polymarket_client_sdk_v2::data::types::request::PositionsRequest;
use polymarket_client_sdk_v2::types::{Address, Decimal, U256};
use polymarket_client_sdk_v2::{POLYGON, Result as SdkResult, clob, data};
use tracing::debug;

/// Authenticated CLOB client (POLY_PROXY / L2). The market-data methods live on
/// the SDK's `impl<S: State> Client<S>`, so this one client serves BOTH the
/// public price/book/tick endpoints AND the authenticated balance endpoint.
pub type Clob = clob::Client<Authenticated<Normal>>;

/// Collateral (pUSD/USDC) balance + allowance snapshot.
#[derive(Debug, Clone)]
pub struct BalanceInfo {
    /// Balance in USDC dollars (raw 6-decimal units scaled by 1e6).
    pub balance_usdc: f64,
    /// Raw balance value as returned by the API (pre-scaling), for cross-checks.
    pub balance_raw: f64,
    /// Number of spender contracts with an allowance set.
    pub allowance_contracts: usize,
}

/// Activity kind we care about for P&L (from data-api `/activity`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActivityKind {
    /// A BUY/SELL of outcome tokens (we record cost from BUYs).
    Trade,
    /// Redeeming a resolved position for collateral (payout > 0 = win, 0 = loss).
    Redeem,
    /// Split / merge / reward / etc. — ignored for P&L.
    Other,
}

/// One on-chain activity row (simplified from data-api `/activity`). The feed is
/// the GROUND TRUTH for realized P&L and never drops resolved markets, so it is
/// the reliable basis for booking settlements that `/positions` loses.
#[derive(Debug, Clone)]
pub struct ActivityRow {
    /// CTF condition_id, `"0x"`-prefixed lowercase hex. May be "" for some kinds.
    pub condition_id: String,
    /// Outcome token id (decimal string), when present (Trade rows carry it).
    pub token_id: Option<String>,
    pub kind: ActivityKind,
    /// For a Trade: true if BUY (cost), false if SELL.
    pub is_buy: bool,
    /// USDC value of the activity (payout for Redeem, notional for Trade).
    pub usdc: f64,
    /// Unix seconds.
    pub ts: i64,
}

/// One open position (from data-api `/positions`).
///
/// PIECE 4 enrichment: `redeemable` + `end_date` are now carried through from the
/// SDK so the active-only filter can be precise (not just the curPrice in {0,1}
/// conservative proxy). Mirrors what `check_positions.py` parses.
///
/// G5 enrichment: `condition_id` carries the CTF condition identifier (the
/// market's unique on-chain id) so the redemption task can dispatch
/// `redeemPositions(USDC, 0x0, condition_id, [1,2])` via the Polymarket Relayer.
/// Stored as a `"0x"`-prefixed lowercase hex string (32 bytes / 64 hex chars).
#[derive(Debug, Clone)]
pub struct PositionInfo {
    pub token_id: String,
    pub size: f64,
    pub avg_price: f64,
    pub cur_price: f64,
    pub cash_pnl: f64,
    /// True when the position can be redeemed (market resolved, outcome decided).
    /// A resolved-pending-redemption position is NOT open exposure.
    pub redeemable: bool,
    /// Market end / resolution date (UTC date, no time). `None` when the data-api
    /// doesn't report one. Past = market already ended -> resolved.
    pub end_date: Option<polymarket_client_sdk_v2::types::NaiveDate>,
    /// G5: CTF condition_id of the market, as `"0x"`-prefixed lowercase hex
    /// (32 bytes). Required to dispatch the redeem via the Polymarket Relayer.
    pub condition_id: String,
}

impl PositionInfo {
    /// Is this position RESOLVED (not open exposure)? Mirrors `check_positions.py`:
    ///   redeemable -> resolved
    ///   curPrice <= 0.001 or >= 0.999 -> outcome decided -> resolved
    ///   end_date < today_utc -> market ended -> resolved
    /// `today_utc` is a date (no time-of-day); for the live poller pass
    /// `chrono::Utc::now().date_naive()`. PURE so the resolution decision is
    /// fully testable offline.
    #[must_use]
    pub fn is_resolved_at(&self, today_utc: polymarket_client_sdk_v2::types::NaiveDate) -> (bool, &'static str) {
        if self.redeemable {
            return (true, "redeemable");
        }
        if self.cur_price <= 0.001 || self.cur_price >= 0.999 {
            return (true, "cur_price_extreme");
        }
        if let Some(end) = self.end_date {
            if end < today_utc {
                return (true, "end_in_past");
            }
        }
        (false, "active")
    }
}

/// Order-book summary. PRICES HERE ARE NOT RELIABLE — see [`RestClient::get_book`].
#[derive(Debug, Clone)]
pub struct BookSummary {
    pub bid_levels: usize,
    pub ask_levels: usize,
    /// Best bid price — UNRELIABLE (see `get_book` caveat). Do NOT use for decisions.
    pub best_bid: Option<f64>,
    /// Best ask price — UNRELIABLE (see `get_book` caveat). Do NOT use for decisions.
    pub best_ask: Option<f64>,
}

/// Read-only REST client. Cheap to clone is NOT assumed; wrap in `Arc` to share.
pub struct RestClient {
    clob: Clob,
    data: data::Client,
    funder: Address,
    timeout: Duration,
    retries: u32,
}

impl RestClient {
    /// Build + authenticate. Supplying credentials makes `authenticate()` a local
    /// operation (no network round-trip; it only bakes the L2 creds in).
    #[allow(clippy::too_many_arguments)]
    pub async fn connect(
        clob_host: &str,
        data_host: &str,
        private_key: &str,
        api_key: &str,
        api_secret: &str,
        api_passphrase: &str,
        funder_address: &str,
        timeout: Duration,
    ) -> anyhow::Result<Self> {
        // Signer MUST carry a chain id (authenticate() rejects an unset chain).
        let signer = LocalSigner::from_str(private_key)
            .context("parsing POLYMARKET_PRIVATE_KEY")?
            .with_chain_id(Some(POLYGON));
        let api_key_uuid =
            Uuid::from_str(api_key).context("POLYMARKET_API_KEY is not a valid UUID")?;
        let creds = Credentials::new(
            api_key_uuid,
            api_secret.to_string(),
            api_passphrase.to_string(),
        );
        let funder = Address::from_str(funder_address.trim())
            .context("parsing POLYMARKET_FUNDER_ADDRESS")?;

        let unauth = clob::Client::new(clob_host, clob::Config::default())
            .map_err(|e| anyhow!("building CLOB client: {e}"))?;
        let clob = unauth
            .authentication_builder(&signer)
            .credentials(creds)
            .funder(funder)
            .signature_type(SignatureType::Proxy) // sig_type=1, POLY_PROXY
            .authenticate()
            .await
            .map_err(|e| anyhow!("CLOB authenticate failed: {e}"))?;

        let data = data::Client::new(data_host).map_err(|e| anyhow!("building data client: {e}"))?;

        Ok(Self {
            clob,
            data,
            funder,
            timeout,
            retries: 4,
        })
    }

    /// Run a read-only SDK call with a bounded timeout + bounded retry/backoff.
    ///
    /// The SDK's HTTP client sets no request timeout, so we bound each attempt
    /// here; transient failures (timeout / 5xx / 429 / dropped connection) are
    /// retried with exponential backoff, mirroring the Python `get_json` policy.
    ///
    /// ⚠️ PHASE 6 WARNING — DO NOT REUSE THIS FOR ORDER PLACEMENT. Blind retry is
    /// safe ONLY because every call here is an idempotent read (GET). A retried
    /// WRITE (order POST) can double-submit if the first attempt reached the
    /// server but its response was lost → DOUBLE FILL with real capital. Order
    /// writes (Phase 6) need idempotency (client order id / order salt) and must
    /// NOT be wrapped in this retry.
    async fn retrying<T, F, Fut>(&self, what: &str, mut make: F) -> anyhow::Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = SdkResult<T>>,
    {
        let mut last = String::new();
        for attempt in 0..self.retries {
            match tokio::time::timeout(self.timeout, make()).await {
                Ok(Ok(value)) => return Ok(value),
                Ok(Err(e)) => last = e.to_string(),
                Err(_) => last = format!("timeout after {:?}", self.timeout),
            }
            if attempt + 1 < self.retries {
                let backoff = Duration::from_millis(250u64.saturating_mul(1u64 << attempt));
                debug!(call = what, attempt, error = %last, "REST retry");
                tokio::time::sleep(backoff).await;
            }
        }
        Err(anyhow!("{what}: failed after {} attempts: {last}", self.retries))
    }

    /// Collateral balance + allowance. Requires L2 auth.
    pub async fn get_balance(&self) -> anyhow::Result<BalanceInfo> {
        let resp = self
            .retrying("get_balance", || {
                self.clob.balance_allowance(
                    BalanceAllowanceRequest::builder()
                        .asset_type(AssetType::Collateral)
                        .build(),
                )
            })
            .await?;
        // Polymarket returns the balance in raw USDC units (6 decimals), matching
        // the Python script's `float(balance) / 1e6`.
        let raw = dec_to_f64(resp.balance);
        Ok(BalanceInfo {
            balance_usdc: raw / 1e6,
            balance_raw: raw,
            allowance_contracts: resp.allowances.len(),
        })
    }

    /// Open positions for the funder/proxy wallet (public data-api). NOTE: data-api
    /// lags on-chain state by a few seconds — do not treat as instantaneous.
    pub async fn get_positions(&self) -> anyhow::Result<Vec<PositionInfo>> {
        let req = PositionsRequest::builder()
            .user(self.funder)
            .size_threshold(Decimal::new(1, 2)) // 0.01
            .build();
        let positions = self
            .retrying("get_positions", || self.data.positions(&req))
            .await?;
        Ok(positions
            .into_iter()
            .map(|p| PositionInfo {
                token_id: p.asset.to_string(),
                size: dec_to_f64(p.size),
                avg_price: dec_to_f64(p.avg_price),
                cur_price: dec_to_f64(p.cur_price),
                cash_pnl: dec_to_f64(p.cash_pnl),
                redeemable: p.redeemable,
                end_date: p.end_date,
                // G5: B256 -> "0x..." lowercase hex (32 bytes = 64 hex chars).
                condition_id: format!("{:#x}", p.condition_id),
            })
            .collect())
    }

    /// On-chain activity (BUY trades + REDEEMs) for the funder/proxy wallet, from
    /// the public data-api `/activity`. This is the GROUND TRUTH for realized P&L
    /// (the same source as a Polymarket history CSV export) and — unlike
    /// `/positions` — does NOT drop resolved markets, so it is the reliable basis
    /// for booking settlements. Newest first. `limit` is clamped to [1, 500].
    pub async fn get_activity(&self, limit: i32) -> anyhow::Result<Vec<ActivityRow>> {
        // We hit the public data-api directly (not the SDK's typed client) and parse
        // leniently: the SDK's strict B256/U256 field types reject some live rows
        // with "invalid string length", killing the whole batch. We only need a few
        // fields, so a tolerant serde_json::Value parse is both simpler and robust.
        let limit = limit.clamp(1, 500);
        let user = format!("{:#x}", self.funder);
        let url = format!(
            "https://data-api.polymarket.com/activity?user={user}&limit={limit}&type=TRADE,REDEEM&sortDirection=DESC"
        );
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .map_err(|e| anyhow!("activity client build: {e}"))?;
        let resp = client.get(&url).send().await.map_err(|e| anyhow!("activity GET: {e}"))?;
        let status = resp.status();
        let body: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| anyhow!("activity json (status {status}): {e}"))?;
        let arr = body.as_array().cloned().unwrap_or_default();
        // number-or-string -> f64
        fn num(v: &serde_json::Value) -> Option<f64> {
            v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
        }
        Ok(arr
            .iter()
            .map(|v| {
                let typ = v.get("type").and_then(|x| x.as_str()).unwrap_or("");
                let kind = match typ.to_ascii_uppercase().as_str() {
                    "TRADE" => ActivityKind::Trade,
                    "REDEEM" => ActivityKind::Redeem,
                    _ => ActivityKind::Other,
                };
                let side = v.get("side").and_then(|x| x.as_str()).unwrap_or("");
                ActivityRow {
                    condition_id: v
                        .get("conditionId")
                        .and_then(|x| x.as_str())
                        .unwrap_or("")
                        .to_ascii_lowercase(),
                    token_id: v
                        .get("asset")
                        .and_then(|x| x.as_str())
                        .filter(|s| !s.is_empty())
                        .map(|s| s.to_string()),
                    kind,
                    is_buy: side.eq_ignore_ascii_case("BUY"),
                    usdc: v.get("usdcSize").and_then(num).unwrap_or(0.0),
                    ts: v.get("timestamp").and_then(|x| x.as_i64()).unwrap_or(0),
                }
            })
            .collect())
    }

    /// Reliable price for a token. `Side::Sell` = ask, `Side::Buy` = bid. This is
    /// the source of truth for price (NOT `/book` — see [`Self::get_book`]).
    pub async fn get_price(&self, token_id: &str, side: Side) -> anyhow::Result<f64> {
        let req = PriceRequest::builder()
            .token_id(parse_token(token_id)?)
            .side(side)
            .build();
        let resp = self.retrying("get_price", || self.clob.price(&req)).await?;
        Ok(dec_to_f64(resp.price))
    }

    /// Midpoint price for a token (reliable; the (bid+ask)/2 from the CLOB).
    pub async fn get_midpoint(&self, token_id: &str) -> anyhow::Result<f64> {
        let req = MidpointRequest::builder()
            .token_id(parse_token(token_id)?)
            .build();
        let resp = self
            .retrying("get_midpoint", || self.clob.midpoint(&req))
            .await?;
        Ok(dec_to_f64(resp.mid))
    }

    /// Order book via the REST `/book` endpoint.
    ///
    /// ⚠️ CAVEAT: the Polymarket REST `/book` endpoint returns GHOST levels
    /// (0.99 ask / 0.01 bid) for active markets — a documented bug
    /// (py-clob-client issue #180). The prices returned here are therefore NOT
    /// reliable and MUST NOT drive any price decision. Use [`Self::get_price`] /
    /// [`Self::get_midpoint`] for prices; this exists only for depth diagnostics.
    pub async fn get_book(&self, token_id: &str) -> anyhow::Result<BookSummary> {
        let req = OrderBookSummaryRequest::builder()
            .token_id(parse_token(token_id)?)
            .build();
        let book = self.retrying("get_book", || self.clob.order_book(&req)).await?;
        // Best bid = highest bid price; best ask = lowest ask price.
        let best_bid = book
            .bids
            .iter()
            .map(|l| l.price)
            .max()
            .map(dec_to_f64);
        let best_ask = book
            .asks
            .iter()
            .map(|l| l.price)
            .min()
            .map(dec_to_f64);
        Ok(BookSummary {
            bid_levels: book.bids.len(),
            ask_levels: book.asks.len(),
            best_bid,
            best_ask,
        })
    }

    /// Minimum tick size for a token (e.g. 0.01 / 0.001). Needed in Phase 6 to
    /// validate order prices.
    pub async fn get_tick_size(&self, token_id: &str) -> anyhow::Result<f64> {
        let tok = parse_token(token_id)?;
        let resp = self
            .retrying("get_tick_size", || self.clob.tick_size(tok))
            .await?;
        Ok(dec_to_f64(resp.minimum_tick_size.as_decimal()))
    }

    /// Whether a token's market is a neg-risk market (affects the exchange address
    /// + approvals in Phase 6).
    pub async fn get_neg_risk(&self, token_id: &str) -> anyhow::Result<bool> {
        let tok = parse_token(token_id)?;
        let resp = self
            .retrying("get_neg_risk", || self.clob.neg_risk(tok))
            .await?;
        Ok(resp.neg_risk)
    }

    /// Resolve a token id to its market condition id (+ the paired token). This is
    /// the targeted "market lookup" Phase 2 needs; full market enumeration
    /// (`clob.markets`) is available but the dynamic discovery already covers the
    /// 5m/15m universe, so we do not duplicate it here.
    pub async fn get_market_by_token(&self, token_id: &str) -> anyhow::Result<String> {
        let tok = parse_token(token_id)?;
        let resp = self
            .retrying("get_market_by_token", || self.clob.market_by_token(tok))
            .await?;
        Ok(resp.condition_id.to_string())
    }

    /// Phase 6 — the authenticated CLOB client, for ORDER PLACEMENT
    /// (`market_order()` / `sign()` / `post_order()`). The read methods above stay
    /// read-only; this exposes the underlying client so the LiveExecutor can place
    /// signed orders. Order writes MUST NOT be wrapped in `retrying` (blind retry
    /// of a write can double-fill — see the warning on that method).
    pub fn clob(&self) -> &Clob {
        &self.clob
    }

    /// The funder/proxy wallet address (for `/positions` and reporting).
    pub fn funder(&self) -> Address {
        self.funder
    }
}

/// Parse a decimal token-id string into the SDK's `U256`. Rejects empty input
/// explicitly — `U256::from_str("")` yields 0, which would silently query the
/// wrong (zero) token.
fn parse_token(token_id: &str) -> anyhow::Result<U256> {
    let t = token_id.trim();
    if t.is_empty() {
        return Err(anyhow!("empty token_id"));
    }
    U256::from_str(t).map_err(|e| anyhow!("invalid token_id {token_id}: {e}"))
}

/// Convert a `rust_decimal::Decimal` to `f64` via its decimal string (lossless
/// enough for prices/sizes; avoids a direct rust_decimal dependency).
fn dec_to_f64(d: Decimal) -> f64 {
    d.to_string().parse().unwrap_or(f64::NAN)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_token_accepts_decimal_token_id() {
        // A real Polymarket token id (78-digit decimal).
        let t = "57223448503580739992718262906156795047377038597301526307144722036523261855081";
        assert!(parse_token(t).is_ok());
        assert!(parse_token("  123  ").is_ok());
        assert!(parse_token("0xnothex").is_err());
        assert!(parse_token("").is_err());
    }

    #[test]
    fn dec_to_f64_roundtrips_prices() {
        assert!((dec_to_f64(Decimal::new(535, 3)) - 0.535).abs() < 1e-9); // 0.535
        assert!((dec_to_f64(Decimal::new(1, 2)) - 0.01).abs() < 1e-9); // 0.01
        assert!((dec_to_f64(Decimal::new(104_000_000, 0)) - 104_000_000.0).abs() < 1.0);
    }
}
