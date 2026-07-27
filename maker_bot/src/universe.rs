//! Order #15 B1 — discovery-driven enumeration of the up/down family.
//!
//! NOT hard-coded to any interval. The Gamma listing could not be trusted (stale
//! epochs on "active" markets, broken pagination past offset 2500), so we enumerate
//! by *probing slugs*, which is deterministic and needs no pagination. What that
//! confirmed: active up/down markets now exist for `btc`, `eth`, `sol` AND `xrp` at
//! 5m — and sol/xrp appear in no dataset we hold (the June inventory was btc+eth
//! only). So the candidate set is deliberately wide: if hourly markets exist we
//! capture them, and if they never launch we still capture the sol/xrp breadth.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Assets to probe. sol/xrp are the new breadth; keep btc/eth for continuity with
/// the existing archive.
pub const DEFAULT_ASSETS: &[&str] = &["btc", "eth", "sol", "xrp"];

/// Intervals to probe. 1h is included precisely BECAUSE we could not confirm hourly
/// markets exist — probing is how we find out, and a miss costs one 404.
pub const DEFAULT_INTERVALS: &[&str] = &["5m", "15m", "1h"];

/// Polymarket up/down slug, e.g. `btc-updown-5m-1780012500`.
#[must_use]
pub fn slug_for(asset: &str, interval: &str, epoch: i64) -> String {
    format!("{}-updown-{}-{}", asset.to_lowercase(), interval, epoch)
}

/// Seconds per interval label. `None` for anything unrecognised (so a typo in config
/// is skipped loudly rather than probed forever).
#[must_use]
pub fn interval_seconds(interval: &str) -> Option<i64> {
    match interval {
        "1m" => Some(60),
        "5m" => Some(300),
        "15m" => Some(900),
        "30m" => Some(1_800),
        "1h" => Some(3_600),
        _ => None,
    }
}

/// A discovered market — deliberately SELF-CONTAINED (order B2): asset, interval,
/// epoch, both token ids and the slug, so downstream analysis never needs an API
/// call to interpret the recording.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketRef {
    pub asset: String,
    pub interval: String,
    pub epoch: i64,
    pub slug: String,
    pub up_token_id: String,
    pub down_token_id: String,
    pub condition_id: String,
}

impl MarketRef {
    #[must_use]
    pub fn tokens(&self) -> [&str; 2] {
        [self.up_token_id.as_str(), self.down_token_id.as_str()]
    }
}

/// Parse a `clobTokenIds` / `outcomes` field that may be a JSON array OR a
/// JSON-encoded string — the Gamma API returns these as strings.
#[must_use]
pub fn parse_json_str_array(v: Option<&Value>) -> Vec<String> {
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

/// Build a [`MarketRef`] from a Gamma `/events/slug/<slug>` payload.
///
/// Up/Down ordering is taken from the `outcomes` array rather than assumed, because
/// assuming index 0 == "Up" is exactly the kind of silent mis-attribution that
/// poisons a dataset months later. Returns `None` if the payload cannot be resolved
/// unambiguously — a skipped market is recoverable, a mislabelled one is not.
#[must_use]
pub fn market_from_event(asset: &str, interval: &str, epoch: i64, event: &Value) -> Option<MarketRef> {
    let markets = event.get("markets").and_then(Value::as_array)?;
    let m = markets.first()?;
    let tokens = parse_json_str_array(m.get("clobTokenIds"));
    let outcomes = parse_json_str_array(m.get("outcomes"));
    if tokens.len() < 2 || outcomes.len() < 2 {
        return None;
    }
    let up_idx = outcomes.iter().position(|o| o.eq_ignore_ascii_case("up"))?;
    let down_idx = outcomes.iter().position(|o| o.eq_ignore_ascii_case("down"))?;
    Some(MarketRef {
        asset: asset.to_lowercase(),
        interval: interval.to_string(),
        epoch,
        slug: slug_for(asset, interval, epoch),
        up_token_id: tokens.get(up_idx)?.clone(),
        down_token_id: tokens.get(down_idx)?.clone(),
        condition_id: m.get("conditionId").and_then(Value::as_str).unwrap_or_default().to_string(),
    })
}

/// The (asset, interval, epoch) triples to probe for a given wall-clock second.
/// `lookahead` is how many future windows to reach for; 0 = current window only.
#[must_use]
pub fn probe_plan(
    now_ts: i64,
    assets: &[String],
    intervals: &[String],
    lookahead: i64,
) -> Vec<(String, String, i64)> {
    let mut out = Vec::new();
    for asset in assets {
        for interval in intervals {
            let Some(step) = interval_seconds(interval) else { continue };
            let current = (now_ts / step) * step;
            for k in 0..=lookahead {
                out.push((asset.clone(), interval.clone(), current + k * step));
            }
        }
    }
    out
}

/// Daily snapshot of what discovery actually found — B1 asks that the universe be
/// logged daily "so we can see it change", which is the only way a silently
/// vanishing asset is ever noticed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UniverseSnapshot {
    pub ts_ms: i64,
    pub day: String,
    pub markets: Vec<MarketRef>,
    /// Probed but absent — the evidence for "hourly markets do not exist", which is
    /// otherwise indistinguishable from "we forgot to look".
    pub probed_missing: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn slug_matches_the_recorder_format() {
        assert_eq!(slug_for("BTC", "5m", 1_780_012_500), "btc-updown-5m-1780012500");
        assert_eq!(slug_for("sol", "15m", 1_780_011_900), "sol-updown-15m-1780011900");
        assert_eq!(slug_for("XRP", "1h", 1_780_009_200), "xrp-updown-1h-1780009200");
    }

    #[test]
    fn unknown_intervals_are_skipped_not_probed() {
        assert_eq!(interval_seconds("5m"), Some(300));
        assert_eq!(interval_seconds("1h"), Some(3_600));
        assert_eq!(interval_seconds("7q"), None, "a config typo must not become a probe loop");
    }

    /// The plan must cover the whole family — every asset × every interval — which is
    /// the point of B1 (the June inventory was btc+eth only and missed sol/xrp).
    #[test]
    fn probe_plan_covers_every_asset_and_interval() {
        let assets: Vec<String> = DEFAULT_ASSETS.iter().map(|s| s.to_string()).collect();
        let intervals: Vec<String> = DEFAULT_INTERVALS.iter().map(|s| s.to_string()).collect();
        let plan = probe_plan(1_780_012_507, &assets, &intervals, 1);
        // 4 assets × 3 intervals × 2 windows.
        assert_eq!(plan.len(), 24);
        assert!(plan.iter().any(|(a, i, _)| a == "sol" && i == "5m"), "sol must be probed");
        assert!(plan.iter().any(|(a, i, _)| a == "xrp" && i == "5m"), "xrp must be probed");
        assert!(plan.iter().any(|(_, i, _)| i == "1h"), "hourly must be probed to be disproved");
        // Epochs are floored to the interval boundary.
        let five = plan.iter().find(|(a, i, _)| a == "btc" && i == "5m").unwrap();
        assert_eq!(five.2 % 300, 0, "epoch aligns to the interval grid");
        assert_eq!(five.2, 1_780_012_500);
    }

    /// Up/Down must come from `outcomes`, never from array position.
    #[test]
    fn market_parses_up_down_by_label_not_index() {
        // Deliberately REVERSED: outcomes[0] = "Down".
        let ev = json!({
            "markets": [{
                "clobTokenIds": "[\"tok_down\",\"tok_up\"]",
                "outcomes": "[\"Down\",\"Up\"]",
                "conditionId": "0xcid"
            }]
        });
        let m = market_from_event("btc", "5m", 1_780_012_500, &ev).expect("parses");
        assert_eq!(m.up_token_id, "tok_up", "Up must follow the LABEL, not index 0");
        assert_eq!(m.down_token_id, "tok_down");
        assert_eq!(m.slug, "btc-updown-5m-1780012500");
        assert_eq!(m.condition_id, "0xcid");
        assert_eq!(m.tokens(), ["tok_up", "tok_down"]);
    }

    /// Ambiguous or short payloads are SKIPPED. A skipped market is recoverable; a
    /// mislabelled one silently poisons months of data.
    #[test]
    fn ambiguous_payloads_are_skipped() {
        let no_markets = json!({ "markets": [] });
        assert!(market_from_event("btc", "5m", 1, &no_markets).is_none());
        let one_token = json!({ "markets": [{ "clobTokenIds": "[\"a\"]", "outcomes": "[\"Up\"]" }] });
        assert!(market_from_event("btc", "5m", 1, &one_token).is_none());
        let no_up = json!({
            "markets": [{ "clobTokenIds": "[\"a\",\"b\"]", "outcomes": "[\"Yes\",\"No\"]" }]
        });
        assert!(market_from_event("btc", "5m", 1, &no_up).is_none(), "no Up/Down labels → skip");
    }

    #[test]
    fn json_str_array_accepts_both_shapes() {
        let as_str = json!("[\"a\",\"b\"]");
        let as_arr = json!(["a", "b"]);
        assert_eq!(parse_json_str_array(Some(&as_str)), vec!["a", "b"]);
        assert_eq!(parse_json_str_array(Some(&as_arr)), vec!["a", "b"]);
        assert!(parse_json_str_array(None).is_empty());
    }
}
