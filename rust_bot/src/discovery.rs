//! Dynamic Polymarket market discovery (REST).
//!
//! Replicates the Python recorder's `current_market_rows`: every refresh it lists
//! the active up/down token ids for the configured assets × intervals (the
//! current epoch plus `lookahead` future epochs) and publishes that set so the
//! Polymarket WS client follows markets as they roll. Markets fall out of the set
//! implicitly — once an epoch is older than the current one it is no longer
//! listed, so its tokens drop and its book is pruned on the next reconnect.
//!
//! Still ingestion-only: this lists markets and updates the subscription set. It
//! makes NO trading decisions and places NO orders.

#![allow(dead_code)]

use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use anyhow::{Context, anyhow};
use serde_json::Value;
use tokio::sync::watch;
use tracing::{info, warn};

use crate::state::Shared;
use crate::ws::write_alert;

/// Interval label → seconds. Mirrors the Python `INTERVAL_SECONDS`.
pub fn interval_seconds(interval: &str) -> Option<i64> {
    match interval {
        "5m" => Some(300),
        "15m" => Some(900),
        _ => None,
    }
}

/// Polymarket up/down market slug, e.g. `btc-updown-5m-1780012500`.
pub fn slug_for(asset: &str, interval: &str, epoch: i64) -> String {
    format!("{}-updown-{}-{}", asset.to_lowercase(), interval, epoch)
}

/// Diff two token sets → (added, removed). Pure; used to log each roll.
pub fn reconcile(old: &[String], new: &[String]) -> (Vec<String>, Vec<String>) {
    let oldset: BTreeSet<&str> = old.iter().map(String::as_str).collect();
    let newset: BTreeSet<&str> = new.iter().map(String::as_str).collect();
    let added = newset.difference(&oldset).map(|s| s.to_string()).collect();
    let removed = oldset.difference(&newset).map(|s| s.to_string()).collect();
    (added, removed)
}

/// Drop event-BBO entries whose token id is no longer in `keep` (markets that
/// rolled off). Thread-safe via `DashMap::retain`.
pub fn prune_books(state: &Shared, keep: &[String]) {
    let keepset: BTreeSet<&str> = keep.iter().map(String::as_str).collect();
    state.bbo.retain(|k, _| keepset.contains(k.as_str()));
}

/// Boxed, `Send` future so the trait is object-safe and the discovery task can be
/// generic over real REST vs. a test fake while still being `tokio::spawn`-able.
pub type DiscoverFuture<'a> = Pin<Box<dyn Future<Output = anyhow::Result<Vec<String>>> + Send + 'a>>;

/// Something that can list the currently-active token ids.
pub trait TokenSource: Send + Sync {
    fn discover(&self) -> DiscoverFuture<'_>;
}

/// REST-backed discovery against the Polymarket Gamma + CLOB APIs.
pub struct RestTokenSource {
    client: reqwest::Client,
    assets: Vec<String>,
    intervals: Vec<String>,
    lookahead: i64,
    gamma_base: String,
    clob_base: String,
    /// Optional sink: when set, `discover` also populates `state.markets` (the live
    /// MarketCatalog for the Phase 6 decision loop) keyed `(asset, interval, epoch)`.
    market_sink: Option<crate::state::Shared>,
}

impl RestTokenSource {
    pub fn new(
        client: reqwest::Client,
        assets: Vec<String>,
        intervals: Vec<String>,
        lookahead: i64,
        gamma_base: String,
        clob_base: String,
    ) -> Self {
        Self {
            client,
            assets,
            intervals,
            lookahead,
            gamma_base: gamma_base.trim_end_matches('/').to_string(),
            clob_base: clob_base.trim_end_matches('/').to_string(),
            market_sink: None,
        }
    }

    /// Also populate `state.markets` (the live catalog) on each discovery cycle.
    #[must_use]
    pub fn with_market_sink(mut self, state: crate::state::Shared) -> Self {
        self.market_sink = Some(state);
        self
    }

    /// GET with the recorder's `get_json` policy: 404 → `Ok(None)`; 429/5xx and
    /// transport errors → retry with exponential backoff (4 attempts); a clean
    /// success → `Ok(Some(json))`; otherwise → `Err`.
    async fn get_json(&self, url: &str) -> anyhow::Result<Option<Value>> {
        let mut last_err: Option<String> = None;
        for attempt in 0..4u32 {
            match self.client.get(url).send().await {
                Ok(resp) => {
                    let code = resp.status();
                    if code.as_u16() == 404 {
                        return Ok(None);
                    } else if matches!(code.as_u16(), 429 | 500 | 502 | 503 | 504) {
                        last_err = Some(format!("http {code}"));
                    } else if code.is_success() {
                        return resp.json::<Value>().await.map(Some).context("decode json");
                    } else {
                        return Err(anyhow!("http {code} for {url}"));
                    }
                }
                Err(e) => last_err = Some(e.to_string()),
            }
            if attempt < 3 {
                tokio::time::sleep(Duration::from_millis(250 * (1u64 << attempt))).await;
            }
        }
        Err(anyhow!(last_err.unwrap_or_else(|| "request failed".to_string())))
    }

    /// CLOB server time in epoch seconds.
    async fn clob_now(&self) -> anyhow::Result<i64> {
        let url = format!("{}/time", self.clob_base);
        let v = self.get_json(&url).await?.ok_or_else(|| anyhow!("clob /time returned 404"))?;
        if let Some(n) = v.as_i64() {
            return Ok(n);
        }
        if let Some(n) = v.as_str().and_then(|s| s.parse::<i64>().ok()) {
            return Ok(n);
        }
        Err(anyhow!("unexpected clob /time payload: {v}"))
    }

    /// Extract the Up/Down token ids from a Gamma event payload.
    fn tokens_from_event(event: &Value) -> Vec<String> {
        let Some(market) = event
            .get("markets")
            .and_then(Value::as_array)
            .and_then(|m| m.first())
        else {
            return Vec::new();
        };
        let outcomes = parse_json_str_array(market.get("outcomes"));
        let token_ids = parse_json_str_array(market.get("clobTokenIds"));
        let mut out = Vec::new();
        for want in ["Up", "Down"] {
            if let Some(idx) = outcomes.iter().position(|o| o == want)
                && let Some(tok) = token_ids.get(idx)
                && !tok.is_empty()
            {
                out.push(tok.clone());
            }
        }
        out
    }

    /// Extract a [`MarketRef`] (up + down tokens + ids) from a Gamma event payload,
    /// for the live catalog. `None` unless BOTH outcomes resolve to non-empty tokens.
    fn market_from_event(event: &Value) -> Option<crate::signal::MarketRef> {
        let market = event.get("markets").and_then(Value::as_array).and_then(|m| m.first())?;
        let outcomes = parse_json_str_array(market.get("outcomes"));
        let token_ids = parse_json_str_array(market.get("clobTokenIds"));
        let pick = |want: &str| -> Option<String> {
            let idx = outcomes.iter().position(|o| o == want)?;
            token_ids.get(idx).filter(|t| !t.is_empty()).cloned()
        };
        let up = pick("Up")?;
        let down = pick("Down")?;
        Some(crate::signal::MarketRef {
            up_token_id: up,
            down_token_id: down,
            condition_id: market.get("conditionId").and_then(Value::as_str).unwrap_or("").to_string(),
            end_time: market.get("endDate").and_then(Value::as_str).unwrap_or("").to_string(),
            // W9-Pieza1: live discovery REST path does NOT expose fee fields
            // (the markets log recorder does, but the live discovery /markets
            // REST endpoint surfaces a different schema). Default to 0/"" -- the
            // exits_trace audit only consumes the replay catalog, never live.
            maker_base_fee: 0,
            taker_base_fee: 0,
            fee_type: String::new(),
        })
    }
}

impl TokenSource for RestTokenSource {
    fn discover(&self) -> DiscoverFuture<'_> {
        Box::pin(async move {
            let now_ts = self.clob_now().await.context("clob /time")?;
            let mut set: BTreeSet<String> = BTreeSet::new();
            for asset in &self.assets {
                for interval in &self.intervals {
                    let Some(step) = interval_seconds(interval) else {
                        continue;
                    };
                    let current = (now_ts / step) * step;
                    for k in 0..=self.lookahead {
                        let epoch = current + k * step;
                        let slug = slug_for(asset, interval, epoch);
                        let url = format!("{}/events/slug/{}", self.gamma_base, slug);
                        if let Some(event) = self.get_json(&url).await? {
                            for tok in Self::tokens_from_event(&event) {
                                set.insert(tok);
                            }
                            // Populate the live catalog (Phase 6 decision loop), if wired.
                            if let Some(state) = &self.market_sink
                                && let Some(m) = Self::market_from_event(&event)
                            {
                                state.markets.insert((asset.clone(), interval.clone(), epoch), m);
                            }
                        }
                    }
                }
            }
            Ok(set.into_iter().collect())
        })
    }
}

/// Parse a `clobTokenIds` / `outcomes` field that may be a JSON array OR a
/// JSON-encoded string (the Gamma API returns these as strings). Mirrors the
/// Python `json_field`.
fn parse_json_str_array(v: Option<&Value>) -> Vec<String> {
    match v {
        Some(Value::Array(arr)) => arr.iter().filter_map(value_to_string).collect(),
        Some(Value::String(s)) => serde_json::from_str::<Vec<Value>>(s)
            .map(|a| a.iter().filter_map(value_to_string).collect())
            .unwrap_or_default(),
        _ => Vec::new(),
    }
}

fn value_to_string(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

/// Discovery loop: every `refresh`, list active tokens and publish the set on
/// `token_tx`. On REST failure it KEEPS the current set (never goes dark), counts
/// the failure, and alerts (rate-limited). Honors shutdown.
pub async fn run_discovery(
    source: Arc<dyn TokenSource>,
    token_tx: watch::Sender<Arc<Vec<String>>>,
    state: Shared,
    alert_dir: String,
    refresh: Duration,
    mut shutdown: watch::Receiver<bool>,
) {
    info!(period_secs = refresh.as_secs(), "task started: market_discovery");
    let mut iv = tokio::time::interval(refresh);
    iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut consecutive_failures: u64 = 0;

    loop {
        tokio::select! {
            _ = iv.tick() => {
                match source.discover().await {
                    Ok(mut new) => {
                        consecutive_failures = 0;
                        new.sort();
                        new.dedup();
                        state.active_tokens.store(new.len() as u64, Ordering::Relaxed);
                        let current = token_tx.borrow().clone();
                        if new != *current {
                            let (added, removed) = reconcile(&current, &new);
                            info!(
                                added = added.len(),
                                removed = removed.len(),
                                total = new.len(),
                                "market discovery: token set changed"
                            );
                            if !added.is_empty() {
                                info!(tokens = ?added, "market discovery: added");
                            }
                            if !removed.is_empty() {
                                info!(tokens = ?removed, "market discovery: removed");
                            }
                            // Only fails if every receiver dropped (WS task gone).
                            let _ = token_tx.send(Arc::new(new));
                        }
                    }
                    Err(e) => {
                        consecutive_failures += 1;
                        state.counters.discovery_failures.fetch_add(1, Ordering::Relaxed);
                        warn!(
                            error = %e,
                            consecutive = consecutive_failures,
                            "market discovery REST failed; keeping current token set"
                        );
                        // Alert on the first failure, then every 10th, to flag a
                        // sustained outage without spamming on a transient blip.
                        if consecutive_failures == 1 || consecutive_failures.is_multiple_of(10) {
                            write_alert(&alert_dir, "market_discovery", "rest_failure", &e.to_string());
                        }
                    }
                }
            }
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    info!("market_discovery: shutdown");
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{EventBbo, SharedState, now_ms};
    use std::sync::atomic::{AtomicU32, AtomicUsize};
    use std::time::Instant;
    use tokio::time::timeout;

    fn unique_dir(tag: &str) -> String {
        static N: AtomicU32 = AtomicU32::new(0);
        let id = N.fetch_add(1, Ordering::SeqCst);
        std::env::temp_dir()
            .join(format!("rb_disc_{tag}_{}_{}", now_ms(), id))
            .to_string_lossy()
            .to_string()
    }

    #[test]
    fn slug_matches_recorder_format() {
        assert_eq!(slug_for("BTC", "5m", 1780012500), "btc-updown-5m-1780012500");
        assert_eq!(slug_for("eth", "15m", 1780011900), "eth-updown-15m-1780011900");
    }

    #[test]
    fn reconcile_reports_added_and_removed() {
        let (added, removed) = reconcile(
            &["a".into(), "b".into()],
            &["b".into(), "c".into()],
        );
        assert_eq!(added, vec!["c".to_string()]);
        assert_eq!(removed, vec!["a".to_string()]);
    }

    #[test]
    fn prune_books_drops_rolled_off_markets() {
        let state = SharedState::new();
        for k in ["a", "b", "c"] {
            state.bbo.insert(k.to_string(), EventBbo::default());
        }
        prune_books(&state, &["b".to_string()]);
        assert_eq!(state.bbo.len(), 1);
        assert!(state.bbo.contains_key("b"));
        assert!(!state.bbo.contains_key("a"));
    }

    #[test]
    fn tokens_from_event_parses_up_down() {
        // Gamma returns outcomes/clobTokenIds as JSON-encoded strings.
        let event = serde_json::json!({
            "markets": [{
                "outcomes": "[\"Up\", \"Down\"]",
                "clobTokenIds": "[\"111\", \"222\"]"
            }]
        });
        assert_eq!(
            RestTokenSource::tokens_from_event(&event),
            vec!["111".to_string(), "222".to_string()]
        );
    }

    /// Scripted fake: each step returns a token set or an error; the last step
    /// repeats once the script is exhausted.
    #[derive(Clone)]
    enum Step {
        Ok(Vec<String>),
        Err(String),
    }

    struct FakeSource {
        steps: Vec<Step>,
        idx: AtomicUsize,
    }

    impl FakeSource {
        fn new(steps: Vec<Step>) -> Self {
            Self { steps, idx: AtomicUsize::new(0) }
        }
    }

    impl TokenSource for FakeSource {
        fn discover(&self) -> DiscoverFuture<'_> {
            let i = self.idx.fetch_add(1, Ordering::SeqCst).min(self.steps.len() - 1);
            let step = self.steps[i].clone();
            Box::pin(async move {
                match step {
                    Step::Ok(v) => Ok(v),
                    Step::Err(e) => Err(anyhow!(e)),
                }
            })
        }
    }

    fn v(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    /// Discovery publishes the initial set, then each subsequent change (new
    /// markets appear, dead markets drop).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn discovery_publishes_added_and_removed() {
        let fake = Arc::new(FakeSource::new(vec![
            Step::Ok(v(&["a", "b"])),
            Step::Ok(v(&["b", "c"])), // 'a' rolled off, 'c' appeared
            Step::Ok(v(&["b"])),      // 'c' rolled off
        ]));
        let (tx, mut rx) = watch::channel(Arc::new(Vec::<String>::new()));
        let (_sd_tx, sd_rx) = watch::channel(false);
        let state = SharedState::new();
        let dir = unique_dir("ok");

        let h = tokio::spawn(run_discovery(
            fake,
            tx,
            state.clone(),
            dir.clone(),
            Duration::from_millis(10),
            sd_rx,
        ));

        timeout(Duration::from_secs(20), rx.changed()).await.unwrap().unwrap();
        assert_eq!(rx.borrow().clone().as_ref(), &v(&["a", "b"]));
        timeout(Duration::from_secs(20), rx.changed()).await.unwrap().unwrap();
        assert_eq!(rx.borrow().clone().as_ref(), &v(&["b", "c"]));
        timeout(Duration::from_secs(20), rx.changed()).await.unwrap().unwrap();
        assert_eq!(rx.borrow().clone().as_ref(), &v(&["b"]));
        assert_eq!(state.active_tokens.load(Ordering::Relaxed), 1);

        _sd_tx.send(true).unwrap();
        let _ = timeout(Duration::from_secs(2), h).await;
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// When the discovery REST fails, the task keeps the last good set, counts the
    /// failure, writes an alert, and does NOT crash or clear the set.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn discovery_keeps_set_and_alerts_on_failure() {
        let fake = Arc::new(FakeSource::new(vec![
            Step::Ok(v(&["a", "b"])),
            Step::Err("boom".to_string()), // repeats forever
        ]));
        let (tx, mut rx) = watch::channel(Arc::new(Vec::<String>::new()));
        let (sd_tx, sd_rx) = watch::channel(false);
        let state = SharedState::new();
        let dir = unique_dir("fail");

        let h = tokio::spawn(run_discovery(
            fake,
            tx,
            state.clone(),
            dir.clone(),
            Duration::from_millis(10),
            sd_rx,
        ));

        // First cycle publishes the good set.
        timeout(Duration::from_secs(20), rx.changed()).await.unwrap().unwrap();
        assert_eq!(rx.borrow().clone().as_ref(), &v(&["a", "b"]));

        // Subsequent cycles fail. Wait for the failure to register AND its alert
        // file to LAND: the alert is written just after the counter bumps, so
        // gating on the counter alone races the file write (under a saturated
        // parallel scheduler the dir read can fall in that gap). Generous
        // deadline against task starvation. (Timing-robustness, not behavior.)
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let failed = state.counters.discovery_failures.load(Ordering::Relaxed) >= 1;
            let alerts = std::fs::read_dir(&dir).map(|d| d.count()).unwrap_or(0);
            if (failed && alerts >= 1) || Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(state.counters.discovery_failures.load(Ordering::Relaxed) >= 1);
        // Set unchanged (still the last good set), bot did not go dark.
        assert_eq!(rx.borrow().clone().as_ref(), &v(&["a", "b"]));
        let alerts = std::fs::read_dir(&dir).map(|d| d.count()).unwrap_or(0);
        assert!(alerts >= 1, "expected a discovery rest_failure alert in {dir}");

        sd_tx.send(true).unwrap();
        let _ = timeout(Duration::from_secs(2), h).await;
        let _ = std::fs::remove_dir_all(&dir);
    }
}
