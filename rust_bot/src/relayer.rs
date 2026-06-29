//! Phase 6 G5 -- Polymarket Relayer API client (auto-redeem).
//!
//! THE RELAYER: a Polymarket-operated HTTP service that takes a signed meta-tx
//! from us, submits it on-chain via Polymarket's GSN Relay Hub, and pays the
//! gas. The whole point of this module is to NOT pay gas, NOT manage MATIC
//! balances, and NOT manually construct proxy meta-txs against contract ABIs
//! we'd have to reverse-engineer from Polygonscan.
//!
//! ON-CHAIN FLOW (the relayer does this; we only sign + POST):
//!   bot signs meta-tx -> POST /submit -> Polymarket relayer's hot wallet -->
//!     RelayHub (0xD216153c..F494) `.relayCall(from, recipient=PROXY_FACTORY,
//!                                              encodedFunction, fees, gas,
//!                                              nonce, signature, approvalData)`
//!     -> PROXY_FACTORY (0xaB45c5..4052) `.proxy([{typeCode=1 (DelegateCall),
//!                                                  to=CTF_COLL_ADAPTER,
//!                                                  value=0, data=redeem_call}])`
//!     -> (delegatecall in user's proxy storage) ->
//!        CTF_COLL_ADAPTER (0xada100db..fce1f)`.redeemPositions(USDC, 0x0,
//!                                                                condId, [1,2])`
//!     -> CTF burns the proxy's outcome token + Adapter wraps the USDC.e -> pUSD
//!        -> proxy receives the proceeds.
//!
//! SPEC SOURCE: every constant + the createRelayHash byte layout + the proxy
//! multicall ABI + the typeCode=1 (DelegateCall via Ctf Collateral Adapter)
//! convention were ALL verified byte-by-byte against the user's REAL on-chain
//! claim tx `0x12c12c301689a5eb7d6873aa2665eef0cc02b28a5104dc555504b6d30b58a9a1`
//! (the manual claim done before G5). The SDK's `ctf::redeem_positions` example
//! shows the direct-EOA path which DOES NOT work for POLY_PROXY users (their
//! tokens live in the proxy wallet, not the EOA), so we implement the proxy
//! meta-tx path here from scratch.
//!
//! AUTH: simple 2-header pair `RELAYER_API_KEY` + `RELAYER_API_KEY_ADDRESS`
//! (NO HMAC, NO timestamp, NO passphrase) -- this is the "Relayer API Key" tier
//! shown in the Polymarket settings page. The fancier "Builder API Key" tier
//! (HMAC-SHA256 with POLY_BUILDER_*) is available but unnecessary for redeem.

#![allow(dead_code)] // wired in by main.rs's live-mode spawn of run_redemption_task

use std::str::FromStr;

use alloy::primitives::{Address, B256, Bytes, U256, address, keccak256};
use alloy::sol;
use alloy::sol_types::SolCall;
use anyhow::{Context, Result, anyhow};
use polymarket_client_sdk_v2::POLYGON;
use polymarket_client_sdk_v2::auth::{LocalSigner, Signer};
use reqwest::Client as HttpClient;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

// ============================================================================
// ON-CHAIN CONSTANTS -- all verified against the user's claim tx + the SDK.
// DO NOT change these without re-verifying on Polygonscan first.
// ============================================================================

/// Default Polygon mainnet relayer host. Overridable via config for tests.
pub const DEFAULT_RELAYER_URL: &str = "https://relayer-v2.polymarket.com";

/// GSN Relay Hub on Polygon. Visible as "Interacted With (To)" in the user's
/// claim tx. The bot does NOT call this directly; it appears in createRelayHash
/// and in the request `signatureParams.relayHub` field.
pub const RELAY_HUB: Address = address!("D216153c06E857cD7f72665E0aF1d7D82172F494");

/// Polymarket Proxy Factory. The GSN `recipient` of relayCall (input data [1]
/// of the user's claim tx == this address exactly). Also the `to` field of the
/// /submit body.
pub const PROXY_FACTORY: Address = address!("aB45c5A4B0c941a2F231C04C3f49182e1A254052");

/// Polymarket "Ctf Collateral Adapter". The DelegateCall target of the inner
/// proxy multicall (= where the proxy executes the redeem in its own storage
/// context so msg.sender to the underlying CTF is the proxy, not the EOA or
/// the relayer). The adapter wraps the USDC.e -> pUSD conversion. Decoded
/// from the user's claim tx encodedFunction.
pub const CTF_COLL_ADAPTER: Address = address!("ada100db00ca00073811820692005400218fce1f");

/// USDC on Polygon (the collateral token, first arg of redeemPositions).
/// From SDK CONFIG[POLYGON].collateral + verified vs user's claim tx.
pub const USDC: Address = address!("c011a7E12a19f7B1f670d46F03B03f3342E82DFB");

/// typeCode in ProxyCall: 0 = Call, 1 = DelegateCall. Polymarket's redeem
/// path uses DelegateCall through the Ctf Collateral Adapter (user's claim tx
/// shows typeCode=1 in the decoded encodedFunction).
pub const TYPECODE_DELEGATECALL: u8 = 1;

/// Default gas limit for the redeem multicall meta-tx. The relayer caps it on
/// their side; this just needs to be big enough for the on-chain execution.
/// 3M is what the magic-proxy-builder-example uses for typical redeems.
pub const DEFAULT_GAS_LIMIT: u64 = 3_000_000;

// ============================================================================
// ABI types via alloy `sol!` macro -- deterministic Solidity-compatible encoding.
// ============================================================================

sol! {
    /// One sub-call inside the proxy multicall. typeCode 0=Call, 1=DelegateCall.
    /// Exactly matches the Polymarket Proxy Factory's `proxy(Call[])` signature.
    struct ProxyCall {
        uint8 typeCode;
        address to;
        uint256 value;
        bytes data;
    }

    /// The outer call to PROXY_FACTORY. Selector = 0x34ee9791 (verified vs user tx).
    function proxy(ProxyCall[] calls) external;

    /// The CTF redeemPositions function. Same signature whether called on the
    /// standard CTF (0x4D97DCd..) or via the Ctf Collateral Adapter (which
    /// re-exports it with the same selector = 0x01b7037c, verified vs user tx).
    function redeemPositions(
        address collateralToken,
        bytes32 parentCollectionId,
        bytes32 conditionId,
        uint256[] indexSets
    ) external;
}

// ============================================================================
// PURE ENCODING HELPERS (deterministic, testable byte-for-byte vs the user tx).
// ============================================================================

/// Build the inner `redeemPositions(USDC, 0x0, conditionId, [1,2])` calldata.
/// For binary markets the indexSets are [1, 2] (= YES and NO partition).
#[must_use]
pub fn encode_redeem_inner_data(condition_id: B256) -> Vec<u8> {
    let call = redeemPositionsCall {
        collateralToken: USDC,
        parentCollectionId: B256::ZERO,
        conditionId: condition_id,
        indexSets: vec![U256::from(1u64), U256::from(2u64)],
    };
    call.abi_encode()
}

/// Build the outer `proxy([{typeCode:1, to:CTF_COLL_ADAPTER, value:0, data:inner}])`
/// calldata that goes in the /submit body's `data` field.
#[must_use]
pub fn encode_proxy_multicall_redeem(condition_id: B256) -> Vec<u8> {
    let inner = encode_redeem_inner_data(condition_id);
    let arg = ProxyCall {
        typeCode: TYPECODE_DELEGATECALL,
        to: CTF_COLL_ADAPTER,
        value: U256::ZERO,
        data: Bytes::from(inner),
    };
    let outer = proxyCall { calls: vec![arg] };
    outer.abi_encode()
}

/// Compute the EIP-191-prefixed message hash that the EOA signs. Verbatim from
/// magic-proxy-builder-example/utils/relay.ts `createRelayHash`:
///   keccak256(concat(
///     "rlx:"           (4 bytes ASCII, 0x726c783a),
///     from             (20 bytes),
///     to=PROXY_FACTORY (20 bytes),
///     data             (variable, the proxy multicall calldata),
///     uint256(relayerFee)  (32 bytes BE),
///     uint256(gasPrice)    (32 bytes BE),
///     uint256(gasLimit)    (32 bytes BE),
///     uint256(nonce)       (32 bytes BE),
///     relayHub         (20 bytes),
///     relay            (20 bytes),
///   ))
#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn create_relay_hash(
    from: Address,
    to: Address,
    data: &[u8],
    relayer_fee: U256,
    gas_price: U256,
    gas_limit: U256,
    nonce: U256,
    relay_hub: Address,
    relay: Address,
) -> B256 {
    let mut buf = Vec::with_capacity(4 + 20 + 20 + data.len() + 32 * 4 + 20 + 20);
    buf.extend_from_slice(b"rlx:");
    buf.extend_from_slice(from.as_slice());
    buf.extend_from_slice(to.as_slice());
    buf.extend_from_slice(data);
    buf.extend_from_slice(&relayer_fee.to_be_bytes::<32>());
    buf.extend_from_slice(&gas_price.to_be_bytes::<32>());
    buf.extend_from_slice(&gas_limit.to_be_bytes::<32>());
    buf.extend_from_slice(&nonce.to_be_bytes::<32>());
    buf.extend_from_slice(relay_hub.as_slice());
    buf.extend_from_slice(relay.as_slice());
    keccak256(buf.as_slice())
}

// ============================================================================
// HTTP URL BUILDERS (pure -- testable byte-for-byte vs the upstream TS spec).
// ============================================================================
//
// Why these are split out as pure helpers: the production 404 (G7.2 diag) was
// caused by URL paths/queries inferred-by-guess in `RelayerClient::*` -- the
// `MockRelayer` test double never saw the real URL, so no test could catch it.
// Each URL the real client builds now goes through one of these pure builders
// AND has a gold test asserting the exact wire-format string, anchored to the
// official TS upstream `Polymarket/builder-relayer-client`.

/// Build the GET URL for the relayer's `/relay-payload` endpoint, which returns
/// the EOA's current `{address: relay, nonce}` so the meta-tx hash can be
/// computed + signed locally.
///
/// SPEC SOURCE (verified byte-for-byte in G7.3 against
/// https://github.com/Polymarket/builder-relayer-client):
///   - path:  `/relay-payload`  (kebab-case; see `src/endpoints.ts:2`
///     `export const GET_RELAY_PAYLOAD = "/relay-payload"`)
///   - query: `?address=<EOA-lowercase>&type=PROXY`  (see `src/client.ts:150-156`
///     `getRelayPayload(signerAddress, signerType)` passes
///     `{address: signerAddress, type: signerType}` as params; `signerType` for
///     proxy users is `TransactionType.PROXY = "PROXY"`)
///
/// PRE-G7.4-A this was `/getRelayPayload?from=...&type=PROXY` (camelCase path,
/// `from=` instead of `address=`), guessed from inferred spec. That URL returned
/// HTTP 404 in production, which is what `--redeem-check` (G7.2) surfaced.
#[must_use]
pub fn build_get_relay_payload_url(base_url: &str, eoa: Address) -> String {
    // `{:#x}` formats with `0x` prefix + lowercase hex. The TS upstream passes
    // the raw checksum string (mixed case); the server is case-insensitive on
    // the EOA per ERC-55 -- lowercase is the safe normalization.
    format!("{base_url}/relay-payload?address={eoa:#x}&type=PROXY")
}

/// Build the GET URL for the relayer's `/transaction` endpoint, which polls
/// the on-chain state of a previously-submitted meta-tx by its `transactionID`
/// (the UUID the relayer returned from POST `/submit`).
///
/// SPEC SOURCE (verified byte-for-byte in G7.3 against
/// https://github.com/Polymarket/builder-relayer-client):
///   - path:  `/transaction`  (see `src/endpoints.ts:3`
///     `export const GET_TRANSACTION = "/transaction"`)
///   - query: `?id=<transactionID>`  (see `src/client.ts:158-164`
///     `getTransaction(transactionId)` passes `{id: transactionId}` as params;
///     ALL lowercase `id`, NOT `transactionID`)
///
/// PRE-G7.4-B this was `?transactionID=...` (the param name from the response
/// body, NOT the query-param name the server expects). The server would have
/// 404'd or returned empty exactly like the pre-G7.4-A `/getRelayPayload` bug.
/// Caught only by inspection because we never reached the poll in production
/// (the build-body step 404'd first).
#[must_use]
pub fn build_get_transaction_url(base_url: &str, transaction_id: &str) -> String {
    format!("{base_url}/transaction?id={transaction_id}")
}

// ============================================================================
// REQUEST / RESPONSE TYPES (matches POST /submit body + responses).
// ============================================================================

#[derive(Debug, Clone, Serialize)]
pub struct SignatureParams {
    #[serde(rename = "gasPrice")]
    pub gas_price: String,
    #[serde(rename = "gasLimit")]
    pub gas_limit: String,
    #[serde(rename = "relayerFee")]
    pub relayer_fee: String,
    #[serde(rename = "relayHub")]
    pub relay_hub: String,
    pub relay: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct SubmitBody {
    pub from: String,
    pub to: String,
    #[serde(rename = "proxyWallet")]
    pub proxy_wallet: String,
    pub data: String,
    pub nonce: String,
    pub signature: String,
    #[serde(rename = "signatureParams")]
    pub signature_params: SignatureParams,
    #[serde(rename = "type")]
    pub kind: String,
    pub metadata: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RelayPayload {
    pub address: String,
    pub nonce: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SubmitResponse {
    #[serde(rename = "transactionID")]
    pub transaction_id: String,
    pub state: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TransactionStatus {
    pub state: String,
    #[serde(rename = "transactionHash", default)]
    pub transaction_hash: Option<String>,
    /// G5.1: error message string from the relayer (when state is failed). The
    /// EXACT field name Polymarket's relayer uses for this is TBD by first real
    /// failure observation -- we accept several common aliases so we capture
    /// whatever they send. `is_already_redeemed_signal` inspects this string
    /// alongside `state` to detect idempotent-failure cases.
    #[serde(alias = "errorMessage", alias = "error_message", alias = "message", alias = "error", default)]
    pub error_message: Option<String>,
}

impl TransactionStatus {
    /// On-chain success: the relayer landed the tx and it executed without revert.
    #[must_use]
    pub fn is_mined(&self) -> bool {
        matches!(self.state.as_str(), "STATE_MINED" | "STATE_SUCCESS")
            && self.transaction_hash.is_some()
    }
    /// Definitive failure: relayer rejected or chain reverted.
    #[must_use]
    pub fn is_failed(&self) -> bool {
        matches!(self.state.as_str(), "STATE_FAILED" | "STATE_ERROR")
    }
    /// Still in flight: relayer queued or pending mining.
    #[must_use]
    pub fn is_pending(&self) -> bool {
        !self.is_mined() && !self.is_failed()
    }
}

/// G7.4-B: sentinel `state` value used when the relayer's `/transaction`
/// endpoint returns an EMPTY ARRAY -- i.e. the relayer hasn't seen our
/// `transactionID` yet (typical for the first few hundred ms after a successful
/// POST `/submit`, before propagation into the queue's read model). Maps to a
/// PENDING `TransactionStatus` so `submit_and_wait`'s poll loop keeps trying;
/// the distinctive string is what tells the operator in the oplog "the relayer
/// returned nothing" vs "the relayer says STATE_NEW".
///
/// NOT one of the relayer's real `STATE_*` enum values
/// (STATE_NEW/EXECUTED/MINED/INVALID/CONFIRMED/FAILED per TS `src/types.ts:120-127`)
/// -- prefixed to be obviously synthesized.
pub const STATE_NOT_FOUND_SENTINEL: &str = "STATE_NOT_FOUND_LOCAL_SENTINEL";

/// G7.4-B: pure helper that takes the relayer's `/transaction` array response
/// and either returns the first element (matching TS `pollUntilState`'s
/// `txns[0]` at `src/client.ts:490`) or, if the array is empty, a synthesized
/// PENDING `TransactionStatus` with the `STATE_NOT_FOUND_SENTINEL` state.
///
/// Rationale for the sentinel-not-None approach: keeps the `RelayerApi` trait
/// signature `Result<TransactionStatus>` (one return, simpler) instead of
/// rippling `Option` through the trait, the MockRelayer, and the 8 existing
/// tests. The downstream `submit_and_wait` loop already treats every non-MINED
/// non-FAILED status as "keep polling" -- the sentinel fits that contract
/// exactly, AND surfaces a diagnostic-useful state string in the warn log if
/// the poll eventually times out.
#[must_use]
pub fn first_or_pending(mut arr: Vec<TransactionStatus>) -> TransactionStatus {
    arr.drain(..).next().unwrap_or_else(|| TransactionStatus {
        state: STATE_NOT_FOUND_SENTINEL.to_string(),
        transaction_hash: None,
        error_message: None,
    })
}

/// G5.1: returns true if a failure signal indicates the position was ALREADY
/// redeemed on-chain (== treat as idempotent success, NOT retry).
///
/// This is the defense against the crash window in `run_redemption_task`: if
/// the bot mined a redeem but crashed before recording it to the tracker, the
/// restart would re-attempt and the relayer/chain would reject the duplicate.
/// Without this check we'd loop on the rejection until /positions updates.
///
/// CONSERVATIVE WIDE NET: the EXACT wording Polymarket's relayer returns for
/// this case isn't documented and we have NOT observed one yet -- the list
/// below covers all plausible variants. If a future failure shows a different
/// wording, ADD the substring; do not change the semantics (failure-unless-
/// proven-idempotent is the safe default; a false negative just retries an
/// extra time per the backoff, while a false positive would silently lose a
/// real failure into the tracker as "redeemed" when it wasn't).
///
/// Substrings are checked CASE-INSENSITIVE against both `state` and
/// `error_message` (if present).
#[must_use]
pub fn is_already_redeemed_signal(state: &str, error_message: Option<&str>) -> bool {
    // Keep this list ordered most-likely-first for quick exit. All needles are
    // lowercase; the haystack is lowercased before checking.
    const NEEDLES: &[&str] = &[
        "already redeemed",
        "already claimed",
        "alreadyredeemed",
        "alreadyclaimed",
        "already-redeemed",
        "already-claimed",
        "result already known", // CTF revert on duplicate redeem
        "position already redeemed",
        "redeem already executed",
        "state_already_redeemed", // hypothetical relayer-level state
    ];
    let state_l = state.to_ascii_lowercase();
    if NEEDLES.iter().any(|n| state_l.contains(n)) {
        return true;
    }
    if let Some(msg) = error_message {
        let m = msg.to_ascii_lowercase();
        if NEEDLES.iter().any(|n| m.contains(n)) {
            return true;
        }
    }
    false
}

/// G5.1: classification of one redeem submission's outcome (the unit the task
/// dispatches on). Separates "store success in tracker" from "back off and
/// retry" cleanly + makes the dispatch trivially testable.
#[derive(Debug, Clone, PartialEq)]
pub enum RedeemOutcome {
    /// Mined on-chain. Persist the real tx_hash to the tracker, clear failures.
    Success {
        tx_hash: String,
        transaction_id: String,
    },
    /// Failed BUT the failure signal indicates the redeem was already done
    /// (e.g. previous mined-but-not-recorded attempt). Persist with a sentinel
    /// `tx_hash` so the tracker treats it as done; clear failures. Idempotent.
    AlreadyRedeemed {
        transaction_id: String,
        detected_via: &'static str,
    },
    /// Definitive on-chain failure (relayer/chain rejected). Increment failure
    /// counter (apply backoff). The position MAY become non-redeemable later
    /// via /positions update; if so the natural pruning removes it.
    PermanentFail { reason: String },
    /// Transient HTTP/network/parse error (no on-chain effect). Increment
    /// failure counter (same backoff as PermanentFail -- the distinction is
    /// only for logging clarity).
    TransientFail { reason: String },
}

/// G5.1: classify the result of submit_and_wait into a RedeemOutcome.
/// Pure -- no I/O, no side effects.
pub fn classify_outcome(
    result: std::result::Result<(SubmitResponse, TransactionStatus), String>,
) -> RedeemOutcome {
    match result {
        Ok((submitted, status)) => {
            if status.is_mined() {
                return RedeemOutcome::Success {
                    tx_hash: status.transaction_hash.clone().unwrap_or_default(),
                    transaction_id: submitted.transaction_id,
                };
            }
            if is_already_redeemed_signal(&status.state, status.error_message.as_deref()) {
                return RedeemOutcome::AlreadyRedeemed {
                    transaction_id: submitted.transaction_id,
                    detected_via: "status",
                };
            }
            if status.is_failed() {
                return RedeemOutcome::PermanentFail {
                    reason: format!(
                        "state={} msg={:?}",
                        status.state,
                        status.error_message.as_deref().unwrap_or("")
                    ),
                };
            }
            // Non-terminal state at timeout -- treat as transient (retry).
            RedeemOutcome::TransientFail {
                reason: format!("non-terminal state at timeout: {}", status.state),
            }
        }
        Err(e) => {
            if is_already_redeemed_signal("", Some(&e)) {
                return RedeemOutcome::AlreadyRedeemed {
                    transaction_id: String::new(),
                    detected_via: "submit_error",
                };
            }
            RedeemOutcome::TransientFail { reason: e }
        }
    }
}

/// G5.1: sentinel tx_hash recorded to the tracker when the redeem was detected
/// as already-done (we don't have the original on-chain tx hash to record).
/// A string starting with "already-redeemed:" is distinctive enough that grep
/// over `redemptions.jsonl` identifies these reconciliations vs real mines.
pub const ALREADY_REDEEMED_SENTINEL_PREFIX: &str = "already-redeemed:";

// ============================================================================
// G7.4-C2: BUILDER HMAC AUTH for POST /submit
// ============================================================================
//
// The Polymarket relayer authenticates POST /submit (and a few other
// endpoints) with a 4-header pair built from a "Builder API Key" triple
// (key, secret, passphrase) issued separately from the CLOB L2 creds.
// GET /relay-payload + GET /transaction are PUBLIC (no auth) -- those went
// without headers in G7.4-A/B.
//
// SPEC SOURCE -- verified byte-for-byte in G7.3 against TWO independent
// upstreams (both arrived at the same algorithm + header names):
//
//   1. @polymarket/builder-signing-sdk@0.0.8 (TS, downloaded from npm,
//      dist/signing/hmac.js + dist/signer.js):
//        message      = timestamp + method + requestPath + (body || "")
//        secretBytes  = Buffer.from(secret, "base64")
//        sig          = HMAC-SHA256(secretBytes, message)
//        sigUrlSafe   = base64(sig).replace('+','-').replace('/','_')
//        headers      = { POLY_BUILDER_API_KEY, POLY_BUILDER_PASSPHRASE,
//                         POLY_BUILDER_SIGNATURE, POLY_BUILDER_TIMESTAMP }
//
//   2. polymarket_client_sdk_v2 (Rust SDK, our own crate), src/auth.rs:
//        - fn to_message     -> "{timestamp}{method}{path}{body}"  (line 261-267)
//        - fn hmac           -> base64_url_safe_decode(secret) ->
//                               HMAC-SHA256 -> base64_url_safe_encode (line 278-285)
//        - L2 CLOB uses 5 POLY_* headers. The Builder variant adds the
//          POLY_BUILDER_* names instead -- same algorithm, different header
//          names + a different set of creds.
//
// We don't reuse the SDK's `auth::hmac` because it is `pub(crate)`. The
// algorithm is ~15 lines and tested below against the SDK's own gold vector
// (`g7_4_c_hmac_matches_sdk_gold_vector`) -- so a future SDK auth refactor
// cannot drift our HMAC silently.

/// Header name for the builder API key UUID.
/// Source: builder-signing-sdk dist/signer.js:18.
pub const POLY_BUILDER_API_KEY: &str = "POLY_BUILDER_API_KEY";
/// Header name for the builder passphrase (echoes the cred verbatim).
pub const POLY_BUILDER_PASSPHRASE: &str = "POLY_BUILDER_PASSPHRASE";
/// Header name for the URL-safe base64 HMAC-SHA256 signature.
pub const POLY_BUILDER_SIGNATURE: &str = "POLY_BUILDER_SIGNATURE";
/// Header name for the Unix-seconds timestamp included in the HMAC message.
pub const POLY_BUILDER_TIMESTAMP: &str = "POLY_BUILDER_TIMESTAMP";

/// Builder API Key triple, exactly as returned by the CLOB's
/// `POST /auth/builder-api-key` endpoint (= the `Credentials` struct from
/// `polymarket_client_sdk_v2::auth`). The operator obtains these via the
/// `--builder-creds-create --builder-creds-confirm` CLI tool (G7.4-C1) and
/// pastes them into `.env`; `setup_from_env` (redemption.rs) reads them.
///
/// FIELDS:
///   * `key`        -- UUID string. PUBLIC. Sent verbatim as POLY_BUILDER_API_KEY.
///   * `secret`     -- base64-encoded random bytes. SECRET. Used to HMAC the
///                     request; never sent on the wire directly.
///   * `passphrase` -- short string. SECRET-ish (less critical than secret).
///                     Sent verbatim as POLY_BUILDER_PASSPHRASE.
///
/// `Debug` is implemented manually to REDACT secret + passphrase -- accidental
/// `format!("{creds:?}")` cannot leak them into the file log. `Clone` is
/// derived intentionally so the value can be stored in `RelayerClient` and
/// reused across `submit()` calls without re-reading env.
#[derive(Clone)]
pub struct BuilderCreds {
    pub key: String,
    pub secret: String,
    pub passphrase: String,
}

impl std::fmt::Debug for BuilderCreds {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The key (UUID) IS public; secret + passphrase are NOT. Show length-only
        // for the redacted fields so the operator can sanity-check "is it set?"
        // without exposing the value.
        f.debug_struct("BuilderCreds")
            .field("key", &self.key)
            .field("secret", &format_args!("<redacted, {} chars>", self.secret.len()))
            .field("passphrase", &format_args!("<redacted, {} chars>", self.passphrase.len()))
            .finish()
    }
}

/// Build the HMAC message that POLY_BUILDER_SIGNATURE signs:
///   `format!("{timestamp_secs}{method}{path}{body}")`
///
/// Matches the TS upstream + the Rust SDK's `auth::to_message` exactly. Pure /
/// no-IO so it can be unit-tested against the SDK gold vector.
///
/// `body` is the JSON-serialized request body. Pass `""` for GET requests
/// (the TS upstream's `if (body !== undefined) message += body` short-circuits
/// to no-op for absent bodies; passing the empty string is equivalent).
#[must_use]
pub fn build_hmac_message(
    timestamp_secs: i64,
    method: &str,
    path: &str,
    body: &str,
) -> String {
    format!("{timestamp_secs}{method}{path}{body}")
}

/// Compute the URL-safe base64 HMAC-SHA256 signature for a builder request.
///
/// Steps (matching `polymarket_client_sdk_v2::auth::hmac` line 278-285):
///   1. base64-URL-safe-decode `secret_b64` into raw key bytes.
///   2. HMAC-SHA256 over `message.as_bytes()` keyed with those bytes.
///   3. base64-URL-safe-encode the digest (32 bytes -> 44 chars including `=` pad).
///
/// Errors:
///   * `secret_b64` is not valid URL-safe base64.
///   * HMAC initialization fails (only on impossibly-empty keys with this lib).
pub fn compute_builder_hmac(secret_b64: &str, message: &str) -> Result<String> {
    use base64::Engine as _;
    use base64::engine::general_purpose::URL_SAFE;
    use hmac::{Hmac, Mac as _};
    use sha2::Sha256;

    let decoded_secret = URL_SAFE
        .decode(secret_b64)
        .context("base64-decoding builder secret (URL_SAFE expected)")?;
    let mut mac = <Hmac<Sha256>>::new_from_slice(&decoded_secret)
        .map_err(|e| anyhow!("HMAC init failed: {e}"))?;
    mac.update(message.as_bytes());
    let result = mac.finalize().into_bytes();
    Ok(URL_SAFE.encode(result))
}

/// One key-value pair representing a header to attach to the outgoing
/// HTTP request. Returning a `Vec` (not a `HeaderMap`) keeps the helper
/// pure / framework-agnostic and easy to test by string equality.
pub type AuthHeader = (&'static str, String);

/// Build the 4 POLY_BUILDER_* headers for an authenticated request to the
/// relayer. Pure / no-IO -- the caller (RelayerClient::submit) supplies the
/// current timestamp and request shape.
///
/// Caller responsibilities:
///   * `path` MUST be the URL path (e.g. `/submit`), NOT a full URL.
///   * `body` MUST be the EXACT bytes that will be sent on the wire
///     (post-`serde_json::to_string`). The HMAC covers the request body
///     verbatim -- any mutation between sign and send invalidates the sig.
///   * `timestamp_secs` MUST be Unix seconds (NOT millis). The relayer
///     server compares against its own clock with some tolerance window.
pub fn build_submit_auth_headers(
    creds: &BuilderCreds,
    timestamp_secs: i64,
    method: &str,
    path: &str,
    body: &str,
) -> Result<Vec<AuthHeader>> {
    let message = build_hmac_message(timestamp_secs, method, path, body);
    let signature = compute_builder_hmac(&creds.secret, &message)?;
    Ok(vec![
        (POLY_BUILDER_API_KEY, creds.key.clone()),
        (POLY_BUILDER_PASSPHRASE, creds.passphrase.clone()),
        (POLY_BUILDER_SIGNATURE, signature),
        (POLY_BUILDER_TIMESTAMP, timestamp_secs.to_string()),
    ])
}

// ============================================================================
// HTTP CLIENT (the real impl + a trait for tests to mock).
// ============================================================================

/// Trait over the 3 relayer endpoints, so tests can mock without HTTP.
#[async_trait::async_trait]
pub trait RelayerApi: Send + Sync {
    async fn get_relay_payload(&self, eoa: Address) -> Result<RelayPayload>;
    async fn submit(&self, body: &SubmitBody) -> Result<SubmitResponse>;
    async fn poll_transaction(&self, transaction_id: &str) -> Result<TransactionStatus>;
}

/// The real HTTP client.
///
/// Carries:
///   * the base URL (`DEFAULT_RELAYER_URL` unless overridden via `with_base_url`);
///   * a reqwest `Client`;
///   * the Builder API Key triple (G7.4-C2): used only by `submit()` to compute
///     the 4 POLY_BUILDER_* headers per request. `get_relay_payload` and
///     `poll_transaction` are PUBLIC endpoints upstream and do NOT use these
///     creds (G7.4-A + G7.4-B).
///
/// The pre-G7.4-C2 shape had `api_key` + `api_key_address` (the "Relayer API
/// Key" pair shown in Polymarket settings), which `--redeem-check` proved is
/// NOT what the relayer endpoint expects. Those fields are now removed: the
/// authenticated header set comes from the Builder API Key creds derived via
/// `--builder-creds-create` (Step C1).
pub struct RelayerClient {
    http: HttpClient,
    creds: BuilderCreds,
    base_url: String,
}

impl RelayerClient {
    #[must_use]
    pub fn new(creds: BuilderCreds) -> Self {
        Self {
            http: HttpClient::new(),
            creds,
            base_url: DEFAULT_RELAYER_URL.to_string(),
        }
    }

    #[must_use]
    pub fn with_base_url(mut self, url: String) -> Self {
        self.base_url = url;
        self
    }
}

#[async_trait::async_trait]
impl RelayerApi for RelayerClient {
    async fn get_relay_payload(&self, eoa: Address) -> Result<RelayPayload> {
        // G7.4-A: URL build delegated to `build_get_relay_payload_url` (pure +
        // gold-tested). Path corrected from inferred `/getRelayPayload?from=` to
        // the upstream-verified `/relay-payload?address=` (this was the 100%
        // production 404, see G7.2 `--redeem-check` output).
        let url = build_get_relay_payload_url(&self.base_url, eoa);
        debug!(url, "relayer: GET /relay-payload");
        // G7.4-A: NO auth headers. Per TS upstream `src/client.ts:150-156`,
        // `getRelayPayload` is a PUBLIC endpoint and goes through `this.send()`
        // directly (not `this.sendAuthedRequest()`). The previous
        // `RELAYER_API_KEY` + `RELAYER_API_KEY_ADDRESS` pair was guessed from
        // the UI's settings page label and is not what the endpoint expects --
        // builder HMAC auth (separate from these keys) only attaches to POST
        // `/submit` + GET `/transactions`. Sending unused-here headers also
        // leaks the cred values into more TLS streams than necessary.
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .context("relay-payload request failed")?;
        let status = resp.status();
        let text = resp.text().await.context("relay-payload body read")?;
        if !status.is_success() {
            anyhow::bail!("relay-payload {status}: {text}");
        }
        serde_json::from_str(&text).with_context(|| format!("relay-payload JSON parse: {text}"))
    }

    async fn submit(&self, body: &SubmitBody) -> Result<SubmitResponse> {
        // G7.4-C2: HMAC-authenticated POST. The 4 POLY_BUILDER_* headers are
        // computed against the EXACT body bytes that will be sent on the wire
        // (any mutation between sign-time and send-time invalidates the sig).
        // Path string MUST be the URL path (`/submit`), NOT the full URL --
        // the upstream HMAC algorithm is path-based (see TS
        // `buildHmacSignature(secret, ts, method, requestPath, body)`).
        let url = format!("{}/submit", self.base_url);
        let body_str = serde_json::to_string(body).context("submit body serialize")?;
        // Wall-clock Unix seconds. The relayer server compares against its own
        // clock with a tolerance window (TS upstream uses
        // `Math.floor(Date.now() / 1000)` -- identical to this).
        let now_secs = crate::state::now_ms() / 1000;
        let headers = build_submit_auth_headers(
            &self.creds,
            now_secs,
            "POST",
            "/submit",
            &body_str,
        )
        .context("building POLY_BUILDER_* HMAC headers")?;
        debug!(
            url,
            timestamp_secs = now_secs,
            "relayer: POST /submit (4 POLY_BUILDER_* headers attached)"
        );
        let mut req = self
            .http
            .post(&url)
            .header("Content-Type", "application/json")
            .body(body_str);
        for (name, value) in headers {
            req = req.header(name, value);
        }
        let resp = req
            .send()
            .await
            .context("submit request failed")?;
        let status = resp.status();
        let text = resp.text().await.context("submit body read")?;
        if !status.is_success() {
            anyhow::bail!("submit {status}: {text}");
        }
        serde_json::from_str(&text).with_context(|| format!("submit JSON parse: {text}"))
    }

    async fn poll_transaction(&self, transaction_id: &str) -> Result<TransactionStatus> {
        // G7.4-B: URL build delegated to `build_get_transaction_url` (pure +
        // gold-tested). Query param corrected from inferred `transactionID=` to
        // the upstream-verified `id=` (TS `src/client.ts:158-164`).
        let url = build_get_transaction_url(&self.base_url, transaction_id);
        debug!(url, "relayer: GET /transaction");
        // G7.4-B: NO auth headers. Per TS upstream, `getTransaction` goes through
        // `this.send()` directly (not `this.sendAuthedRequest()`) -- same
        // public-endpoint pattern as `getRelayPayload`. Builder HMAC auth (when
        // we add it in Paso C) only attaches to POST `/submit` and
        // GET `/transactions` (plural -- a separate authenticated endpoint).
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .context("transaction request failed")?;
        let status = resp.status();
        let text = resp.text().await.context("transaction body read")?;
        if !status.is_success() {
            anyhow::bail!("transaction {status}: {text}");
        }
        // G7.4-B: relayer returns `Vec<TransactionStatus>` (array), NOT a single
        // object. TS `pollUntilState` (src/client.ts:488-498) reads
        // `const txns = await this.getTransaction(...); if (txns.length > 0) {
        // const txn = txns[0]; ... }`. Empty array = "relayer hasn't propagated
        // the tx yet, keep polling" (NOT an error). `first_or_pending` mirrors
        // this exactly: take [0] if present, else a synthesized pending sentinel
        // that flows through `submit_and_wait`'s loop without crashing.
        let arr: Vec<TransactionStatus> = serde_json::from_str(&text)
            .with_context(|| format!("transaction JSON parse (expected array): {text}"))?;
        Ok(first_or_pending(arr))
    }
}

// ============================================================================
// HIGH-LEVEL SUBMIT-BODY BUILDER (fetches relay payload + signs + builds JSON).
// ============================================================================

/// Build a fully-signed /submit body for a redeem of `condition_id` from
/// `proxy_wallet`. The EOA signer (= owner of the proxy) signs the meta-tx hash.
///
/// Steps:
///   1. fetch nonce + relay address (per-proxy, per-relayer state)
///   2. ABI-encode the proxy multicall calldata
///   3. compute the EIP-191 message hash (createRelayHash)
///   4. EOA signs that hash via personal_sign (LocalSigner::sign_message)
///   5. assemble the body with all fields populated
pub async fn build_redeem_submit_body<S: Signer + Sync>(
    relayer: &dyn RelayerApi,
    signer: &S,
    proxy_wallet: Address,
    condition_id: B256,
    builder_code: &str,
    gas_limit: u64,
) -> Result<SubmitBody> {
    let from = signer.address();
    let payload = relayer
        .get_relay_payload(from)
        .await
        .context("getRelayPayload for redeem")?;
    let relay_address = Address::from_str(payload.address.trim())
        .with_context(|| format!("relay address parse: {}", payload.address))?;
    let nonce = U256::from_str(payload.nonce.trim())
        .with_context(|| format!("nonce parse: {}", payload.nonce))?;

    let data_bytes = encode_proxy_multicall_redeem(condition_id);
    let gas_limit_u = U256::from(gas_limit);

    let hash = create_relay_hash(
        from,
        PROXY_FACTORY,
        &data_bytes,
        U256::ZERO,
        U256::ZERO,
        gas_limit_u,
        nonce,
        RELAY_HUB,
        relay_address,
    );

    let signature = signer
        .sign_message(hash.as_slice())
        .await
        .map_err(|e| anyhow!("EIP-191 personal_sign failed: {e}"))?;
    let sig_bytes = signature.as_bytes();

    Ok(SubmitBody {
        from: format!("{from:#x}"),
        to: format!("{PROXY_FACTORY:#x}"),
        proxy_wallet: format!("{proxy_wallet:#x}"),
        data: format!("0x{}", alloy::hex::encode(&data_bytes)),
        nonce: payload.nonce,
        signature: format!("0x{}", alloy::hex::encode(sig_bytes)),
        signature_params: SignatureParams {
            gas_price: "0".to_string(),
            gas_limit: gas_limit.to_string(),
            relayer_fee: "0".to_string(),
            relay_hub: format!("{RELAY_HUB:#x}"),
            relay: payload.address,
        },
        kind: "PROXY".to_string(),
        metadata: builder_code.to_string(),
    })
}

/// Build a PrivateKeySigner from a raw private-key hex string, chain-set to
/// Polygon. The return type is alloy's concrete `PrivateKeySigner` (=
/// `LocalSigner<SigningKey>`); callers pass it as `&S` to functions generic
/// over `S: Signer`.
pub fn signer_from_key(
    private_key: &str,
) -> Result<alloy::signers::local::PrivateKeySigner> {
    let s = LocalSigner::from_str(private_key).map_err(|e| anyhow!("signer parse: {e}"))?;
    Ok(s.with_chain_id(Some(POLYGON)))
}

/// Convenience: log a sanity line about a built body (for the test/diagnostic path).
pub fn summary_for_log(body: &SubmitBody) -> String {
    format!(
        "from={} to={} proxyWallet={} type={} data_bytes={} nonce={} sig_bytes={} meta={}",
        body.from,
        body.to,
        body.proxy_wallet,
        body.kind,
        (body.data.len() - 2) / 2,
        body.nonce,
        (body.signature.len() - 2) / 2,
        body.metadata
    )
}

/// One-shot: submit a redeem and poll until mined/failed/timeout. Returns the
/// final TransactionStatus (with tx_hash on success).
///
/// Caller controls the poll cadence + timeout via `poll_interval` and
/// `max_polls` (so the redemption task can choose how patient to be).
pub async fn submit_and_wait(
    relayer: &dyn RelayerApi,
    body: &SubmitBody,
    poll_interval: std::time::Duration,
    max_polls: u32,
) -> Result<(SubmitResponse, TransactionStatus)> {
    let submitted = relayer.submit(body).await.context("submit failed")?;
    info!(
        transaction_id = %submitted.transaction_id,
        state = %submitted.state,
        "relayer: /submit accepted"
    );
    for attempt in 1..=max_polls {
        tokio::time::sleep(poll_interval).await;
        match relayer.poll_transaction(&submitted.transaction_id).await {
            Ok(status) => {
                debug!(attempt, state = %status.state, "relayer: poll");
                if status.is_mined() || status.is_failed() {
                    return Ok((submitted, status));
                }
            }
            Err(e) => {
                warn!(attempt, error = %e, "relayer: poll error (will retry)");
            }
        }
    }
    anyhow::bail!(
        "submit_and_wait: did not reach terminal state after {max_polls} polls (last submitted transactionID = {})",
        submitted.transaction_id
    );
}

// ============================================================================
// TESTS -- all pure / mocked. Zero capital touched. Zero real HTTP.
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::b256;
    use std::sync::Mutex as StdMutex;

    // ---------- Pure: ABI encoding gold tests ----------

    /// Real conditionId from the user's claim tx (decodificada del encodedFunction).
    const USER_TX_CONDITION_ID: B256 =
        b256!("0x642b9702238dcb905f85da5548e3799d906c67b1fed3dcae234224c76f20ecf0");

    #[test]
    fn g5_redeem_selector_is_0x01b7037c() {
        // The user's claim tx shows selector 0x01b7037c for the inner call.
        // sol! macro derives the selector from the function signature, so this
        // catches accidental signature drift.
        assert_eq!(
            redeemPositionsCall::SELECTOR,
            [0x01, 0xb7, 0x03, 0x7c],
            "redeemPositions selector must match the on-chain tx exactly"
        );
    }

    #[test]
    fn g5_proxy_selector_is_0x34ee9791() {
        // The user's claim tx shows selector 0x34ee9791 for the outer proxy() call.
        assert_eq!(
            proxyCall::SELECTOR,
            [0x34, 0xee, 0x97, 0x91],
            "proxy(Call[]) selector must match the on-chain tx exactly"
        );
    }

    #[test]
    fn g5_encode_redeem_inner_starts_with_selector_then_args() {
        let encoded = encode_redeem_inner_data(USER_TX_CONDITION_ID);
        // Length: 4 (selector) + 32 (USDC padded) + 32 (parentColl) + 32 (condId)
        // + 32 (offset to indexSets) + 32 (len=2) + 32 (=1) + 32 (=2) = 4 + 32*7 = 228.
        // This MATCHES the `data length = 0xe4 = 228` field we read out of the
        // user's claim tx -- byte-for-byte gold reference.
        assert_eq!(encoded.len(), 228,
            "inner redeem calldata must be 228 bytes (= 0xe4, matches user tx)");
        assert_eq!(&encoded[0..4], &[0x01, 0xb7, 0x03, 0x7c], "selector");
        // USDC address is padded into 32 bytes (12 zero bytes + 20 address bytes).
        let mut usdc_padded = [0u8; 32];
        usdc_padded[12..].copy_from_slice(USDC.as_slice());
        assert_eq!(&encoded[4..36], &usdc_padded, "arg0 = USDC address (padded)");
        // parentCollectionId = bytes32 zeros.
        assert_eq!(&encoded[36..68], &[0u8; 32], "arg1 = parentCollectionId zero");
        // conditionId = the user's exact 32-byte value.
        assert_eq!(&encoded[68..100], USER_TX_CONDITION_ID.as_slice(),
            "arg2 = conditionId verbatim");
        // arg3 = uint256[] is dynamic; offset(0x80) + length(2) + values(1, 2).
        let mut offset_bytes = [0u8; 32];
        offset_bytes[31] = 0x80;
        assert_eq!(&encoded[100..132], &offset_bytes, "arg3 offset = 0x80");
        // (lengths + values verified by total byte count + sentinel match above.)
    }

    #[test]
    fn g5_encode_proxy_multicall_redeem_full_shape() {
        let encoded = encode_proxy_multicall_redeem(USER_TX_CONDITION_ID);
        // Outer layout:
        //   4   selector (0x34ee9791)
        //   32  offset to calls[] = 0x20
        //   32  calls.length = 1
        //   32  offset to calls[0] (relative to array head) = 0x20
        //   32  typeCode = 1
        //   32  to = CTF_COLL_ADAPTER (padded)
        //   32  value = 0
        //   32  offset to data within calls[0] = 0x80
        //   32  data.length = 228 (0xe4)
        //   256 data padded to multiple of 32 (228 -> 256)
        // Total: 4 + 32*8 + 256 = 516 bytes.
        assert_eq!(encoded.len(), 516,
            "proxy multicall calldata must be 516 bytes");
        assert_eq!(&encoded[0..4], &[0x34, 0xee, 0x97, 0x91], "outer selector");
        // typeCode at offset 4 + 32*3 = 100; uint8 right-padded to 32 bytes.
        let mut typecode_word = [0u8; 32];
        typecode_word[31] = TYPECODE_DELEGATECALL;
        assert_eq!(&encoded[100..132], &typecode_word,
            "typeCode = 1 (DelegateCall) -- the Polymarket convention for redeem");
        // Inner `to` at offset 132: CTF_COLL_ADAPTER padded.
        let mut to_padded = [0u8; 32];
        to_padded[12..].copy_from_slice(CTF_COLL_ADAPTER.as_slice());
        assert_eq!(&encoded[132..164], &to_padded,
            "inner to = Ctf Collateral Adapter (NOT direct CTF -- the Polymarket helper)");
    }

    // ---------- Pure: createRelayHash ----------

    #[test]
    fn g5_create_relay_hash_is_deterministic_and_stable() {
        let from = address!("e5a9fb008d4eec3e3386a52e0f326ca3f98fafe0");
        let data = encode_proxy_multicall_redeem(USER_TX_CONDITION_ID);
        let relay = address!("A7BE1729F709955d7E0081cf07806e9F806daE26");
        let h1 = create_relay_hash(
            from, PROXY_FACTORY, &data,
            U256::ZERO, U256::ZERO, U256::from(3_000_000u64), U256::from(7u64),
            RELAY_HUB, relay,
        );
        let h2 = create_relay_hash(
            from, PROXY_FACTORY, &data,
            U256::ZERO, U256::ZERO, U256::from(3_000_000u64), U256::from(7u64),
            RELAY_HUB, relay,
        );
        assert_eq!(h1, h2, "createRelayHash must be deterministic");
        // Changing ANY field must change the hash.
        let h_diff_nonce = create_relay_hash(
            from, PROXY_FACTORY, &data,
            U256::ZERO, U256::ZERO, U256::from(3_000_000u64), U256::from(8u64),
            RELAY_HUB, relay,
        );
        assert_ne!(h1, h_diff_nonce, "changing nonce must change the hash");
    }

    #[test]
    fn g5_create_relay_hash_starts_with_rlx_prefix_in_concat() {
        // Sanity: the hash differs if we ever forget the "rlx:" prefix. We can't
        // observe it directly post-keccak, but we can confirm the input layout
        // makes "rlx:" load-bearing by constructing the buffer manually and
        // checking the keccak of the buffer-with-prefix == our function output.
        let from = address!("e5a9fb008d4eec3e3386a52e0f326ca3f98fafe0");
        let to = PROXY_FACTORY;
        let data = encode_proxy_multicall_redeem(USER_TX_CONDITION_ID);
        let relay = address!("A7BE1729F709955d7E0081cf07806e9F806daE26");
        let nonce = U256::from(42u64);
        let gas_limit = U256::from(DEFAULT_GAS_LIMIT);

        let h_via_fn =
            create_relay_hash(from, to, &data, U256::ZERO, U256::ZERO, gas_limit, nonce, RELAY_HUB, relay);

        let mut buf = Vec::new();
        buf.extend_from_slice(b"rlx:");
        buf.extend_from_slice(from.as_slice());
        buf.extend_from_slice(to.as_slice());
        buf.extend_from_slice(&data);
        buf.extend_from_slice(&U256::ZERO.to_be_bytes::<32>());
        buf.extend_from_slice(&U256::ZERO.to_be_bytes::<32>());
        buf.extend_from_slice(&gas_limit.to_be_bytes::<32>());
        buf.extend_from_slice(&nonce.to_be_bytes::<32>());
        buf.extend_from_slice(RELAY_HUB.as_slice());
        buf.extend_from_slice(relay.as_slice());
        let h_manual = keccak256(buf.as_slice());

        assert_eq!(h_via_fn, h_manual, "function output must match manual concat");
    }

    // ---------- TransactionStatus state classification ----------

    #[test]
    fn g5_transaction_status_classifies_states_correctly() {
        let mined = TransactionStatus {
            state: "STATE_MINED".to_string(),
            transaction_hash: Some("0xabc".to_string()),
            error_message: None,
        };
        assert!(mined.is_mined() && !mined.is_failed() && !mined.is_pending());

        let mined_no_hash = TransactionStatus {
            state: "STATE_MINED".to_string(),
            transaction_hash: None,
            error_message: None,
        };
        // Mined without a tx hash is suspicious; treat as pending so we keep polling.
        assert!(!mined_no_hash.is_mined() && mined_no_hash.is_pending());

        let failed = TransactionStatus {
            state: "STATE_FAILED".to_string(),
            transaction_hash: None,
            error_message: None,
        };
        assert!(!failed.is_mined() && failed.is_failed() && !failed.is_pending());

        let new = TransactionStatus {
            state: "STATE_NEW".to_string(),
            transaction_hash: None,
            error_message: None,
        };
        assert!(new.is_pending());
    }

    // ---------- Mock RelayerApi + integration: build_redeem_submit_body ----------

    struct MockRelayer {
        relay_addr: Address,
        nonce: u64,
        submits: StdMutex<Vec<SubmitBody>>,
    }

    impl MockRelayer {
        fn new(relay_addr: Address, nonce: u64) -> Self {
            Self { relay_addr, nonce, submits: StdMutex::new(Vec::new()) }
        }
        fn last_submit(&self) -> SubmitBody {
            self.submits.lock().unwrap().last().cloned().expect("at least one submit")
        }
    }

    #[async_trait::async_trait]
    impl RelayerApi for MockRelayer {
        async fn get_relay_payload(&self, _eoa: Address) -> Result<RelayPayload> {
            Ok(RelayPayload {
                address: format!("{:#x}", self.relay_addr),
                nonce: self.nonce.to_string(),
            })
        }
        async fn submit(&self, body: &SubmitBody) -> Result<SubmitResponse> {
            self.submits.lock().unwrap().push(body.clone());
            Ok(SubmitResponse {
                transaction_id: "mock-uuid-1".to_string(),
                state: "STATE_NEW".to_string(),
            })
        }
        async fn poll_transaction(&self, _id: &str) -> Result<TransactionStatus> {
            Ok(TransactionStatus {
                state: "STATE_MINED".to_string(),
                transaction_hash: Some("0xmocktx".to_string()),
                error_message: None,
            })
        }
    }

    /// Well-known anvil/hardhat test key #0 (NEVER used for live capital).
    /// Public; appears in countless examples. The derived EOA is
    /// 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266.
    const TEST_PRIVKEY: &str =
        "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";

    #[tokio::test(flavor = "current_thread")]
    async fn g5_build_redeem_submit_body_assembles_valid_json_shape() {
        let signer = signer_from_key(TEST_PRIVKEY).expect("test signer");
        let relay_addr = address!("A7BE1729F709955d7E0081cf07806e9F806daE26");
        let mock = MockRelayer::new(relay_addr, 42);
        let proxy = address!("425671d3dd8e33618fe4518f8ae05de0adf7caae");
        let builder_code =
            "0xda7a9e16bb66a5ce7741fa24c929bc080727ce3b321df54c6631bcfe8b667b80";

        let body = build_redeem_submit_body(
            &mock,
            &signer,
            proxy,
            USER_TX_CONDITION_ID,
            builder_code,
            DEFAULT_GAS_LIMIT,
        )
        .await
        .expect("body built");

        // DIAGNOSTIC: print the assembled body so --nocapture shows EXACTLY what
        // the bot will POST in production. The signer is the test key, so no
        // secret leaks.
        println!(
            "[G5-test] ASSEMBLED /submit body (test-key signed -- NEVER live):\n{}",
            serde_json::to_string_pretty(&body).unwrap()
        );
        println!("[G5-test] one-line summary: {}", summary_for_log(&body));

        // Structural assertions.
        assert_eq!(body.to, format!("{PROXY_FACTORY:#x}"), "to must be PROXY_FACTORY");
        assert_eq!(body.proxy_wallet, format!("{proxy:#x}"));
        assert_eq!(body.kind, "PROXY");
        assert_eq!(body.metadata, builder_code);
        assert_eq!(body.signature_params.gas_price, "0");
        assert_eq!(body.signature_params.relayer_fee, "0");
        assert_eq!(body.signature_params.gas_limit, DEFAULT_GAS_LIMIT.to_string());
        assert_eq!(body.signature_params.relay_hub, format!("{RELAY_HUB:#x}"));
        assert_eq!(body.signature_params.relay, format!("{relay_addr:#x}"));
        assert_eq!(body.nonce, "42");

        // The data field hex matches our pure encoder output for this conditionId.
        let expected_data = encode_proxy_multicall_redeem(USER_TX_CONDITION_ID);
        assert_eq!(
            body.data,
            format!("0x{}", alloy::hex::encode(&expected_data)),
            "body.data must equal the pure encoder output"
        );

        // Signature is 65 bytes (130 hex chars + "0x").
        assert_eq!(body.signature.len(), 2 + 130, "signature must be 65 bytes hex");
        assert!(body.signature.starts_with("0x"));
    }

    // ---------- G5.1: is_already_redeemed_signal ----------

    #[test]
    fn g5_1_already_redeemed_signal_matches_common_wordings() {
        // Case 1: state alone.
        assert!(is_already_redeemed_signal("STATE_ALREADY_REDEEMED", None),
            "state field carrying the signal");
        assert!(is_already_redeemed_signal("already redeemed", None));
        // Case 2: error message alone.
        assert!(is_already_redeemed_signal("STATE_FAILED", Some("Position already redeemed")));
        assert!(is_already_redeemed_signal("STATE_FAILED", Some("ALREADY CLAIMED")),
            "case-insensitive match");
        assert!(is_already_redeemed_signal("STATE_FAILED", Some("execution reverted: result already known")));
        assert!(is_already_redeemed_signal("STATE_FAILED", Some("alreadyRedeemed()")));
        assert!(is_already_redeemed_signal("STATE_FAILED", Some("already-redeemed")));
        // Case 3: generic revert -- NOT matched (safer to retry than false-positive into tracker).
        assert!(!is_already_redeemed_signal("STATE_FAILED", Some("execution reverted")));
        assert!(!is_already_redeemed_signal("STATE_FAILED", Some("out of gas")));
        assert!(!is_already_redeemed_signal("STATE_FAILED", None));
        assert!(!is_already_redeemed_signal("STATE_FAILED", Some("")));
        // Case 4: success states -- NOT matched (they go through Success path).
        assert!(!is_already_redeemed_signal("STATE_MINED", None));
        assert!(!is_already_redeemed_signal("STATE_NEW", None));
    }

    // ---------- G5.1: classify_outcome ----------

    fn mock_submit(id: &str) -> SubmitResponse {
        SubmitResponse {
            transaction_id: id.to_string(),
            state: "STATE_NEW".to_string(),
        }
    }

    fn mock_status(state: &str, tx: Option<&str>, err: Option<&str>) -> TransactionStatus {
        TransactionStatus {
            state: state.to_string(),
            transaction_hash: tx.map(String::from),
            error_message: err.map(String::from),
        }
    }

    #[test]
    fn g5_1_classify_outcome_mined_is_success() {
        let r = Ok((mock_submit("uuid-1"), mock_status("STATE_MINED", Some("0xtx"), None)));
        match classify_outcome(r) {
            RedeemOutcome::Success { tx_hash, transaction_id } => {
                assert_eq!(tx_hash, "0xtx");
                assert_eq!(transaction_id, "uuid-1");
            }
            o => panic!("expected Success, got {o:?}"),
        }
    }

    #[test]
    fn g5_1_classify_outcome_failed_with_already_redeemed_is_idempotent() {
        let r = Ok((
            mock_submit("uuid-2"),
            mock_status("STATE_FAILED", None, Some("Position already redeemed")),
        ));
        match classify_outcome(r) {
            RedeemOutcome::AlreadyRedeemed { transaction_id, detected_via } => {
                assert_eq!(transaction_id, "uuid-2");
                assert_eq!(detected_via, "status");
            }
            o => panic!("expected AlreadyRedeemed, got {o:?}"),
        }
    }

    #[test]
    fn g5_1_classify_outcome_failed_real_error_is_permanent_fail() {
        let r = Ok((
            mock_submit("uuid-3"),
            mock_status("STATE_FAILED", None, Some("execution reverted: out of gas")),
        ));
        match classify_outcome(r) {
            RedeemOutcome::PermanentFail { reason } => {
                assert!(reason.contains("STATE_FAILED"));
                assert!(reason.contains("out of gas"));
            }
            o => panic!("expected PermanentFail, got {o:?}"),
        }
    }

    #[test]
    fn g5_1_classify_outcome_pending_at_timeout_is_transient() {
        let r = Ok((mock_submit("uuid-4"), mock_status("STATE_NEW", None, None)));
        match classify_outcome(r) {
            RedeemOutcome::TransientFail { reason } => {
                assert!(reason.contains("non-terminal"));
            }
            o => panic!("expected TransientFail, got {o:?}"),
        }
    }

    #[test]
    fn g5_1_classify_outcome_submit_error_is_transient() {
        let r = Err::<(SubmitResponse, TransactionStatus), String>(
            "connection timeout".to_string(),
        );
        match classify_outcome(r) {
            RedeemOutcome::TransientFail { reason } => assert_eq!(reason, "connection timeout"),
            o => panic!("expected TransientFail, got {o:?}"),
        }
    }

    #[test]
    fn g5_1_classify_outcome_submit_error_with_already_redeemed_msg_is_idempotent() {
        // Sometimes the relayer rejects at HTTP layer with a body that mentions
        // already-redeemed -- bubble that into AlreadyRedeemed too.
        let r = Err::<(SubmitResponse, TransactionStatus), String>(
            "submit 409: {\"error\":\"already redeemed\"}".to_string(),
        );
        match classify_outcome(r) {
            RedeemOutcome::AlreadyRedeemed { detected_via, .. } => {
                assert_eq!(detected_via, "submit_error");
            }
            o => panic!("expected AlreadyRedeemed, got {o:?}"),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn g5_submit_and_wait_returns_on_first_mined_poll() {
        let signer = signer_from_key(TEST_PRIVKEY).unwrap();
        let mock = MockRelayer::new(
            address!("A7BE1729F709955d7E0081cf07806e9F806daE26"),
            7,
        );
        let body = build_redeem_submit_body(
            &mock,
            &signer,
            address!("425671d3dd8e33618fe4518f8ae05de0adf7caae"),
            USER_TX_CONDITION_ID,
            "",
            DEFAULT_GAS_LIMIT,
        )
        .await
        .unwrap();

        let (submitted, status) =
            submit_and_wait(&mock, &body, std::time::Duration::from_millis(1), 3)
                .await
                .expect("mined on first poll");
        assert_eq!(submitted.transaction_id, "mock-uuid-1");
        assert!(status.is_mined());
        assert_eq!(status.transaction_hash.as_deref(), Some("0xmocktx"));
    }

    // ---------- G7.4-A: WIRE-FORMAT gold test for /relay-payload URL ----------

    /// Anti-regression GOLD: assert the exact bytes of the URL `RelayerClient`
    /// hits for `getRelayPayload`. The MockRelayer hides this -- it returns
    /// `Ok(payload)` from `get_relay_payload` regardless of what URL the real
    /// HTTP client would send. That blind spot is exactly how the
    /// `/getRelayPayload?from=...` -> 404 bug shipped: every mock-based test
    /// passed while production hit a path the server doesn't expose.
    ///
    /// This test bypasses the mock and verifies the PURE URL builder against
    /// the official TS upstream's wire-format (verified G7.3 against
    /// https://github.com/Polymarket/builder-relayer-client):
    ///   - path  = `/relay-payload`  (kebab-case)
    ///   - query = `address=0x<eoa-lowercase>&type=PROXY`
    ///
    /// If anyone reverts the path to `/getRelayPayload`, swaps `address=` back
    /// to `from=`, drops `type=PROXY`, or otherwise drifts from the upstream
    /// contract, this test catches it BEFORE the bot is ever started and
    /// before any HTTP round-trip is made.
    #[test]
    fn g7_4_a_relay_payload_url_matches_upstream_wire_format() {
        // Use the well-known anvil/hardhat key #0 EOA -- public, deterministic,
        // appears in countless test fixtures, NEVER used for live capital.
        // signer_from_key(TEST_PRIVKEY).address() ==
        //   0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266
        let eoa = address!("f39Fd6e51aad88F6F4ce6aB8827279cffFb92266");
        let base = "https://relayer-v2.polymarket.com";

        let url = build_get_relay_payload_url(base, eoa);

        // FULL-STRING GOLD: byte-for-byte assertion. The address is lowercased
        // by `{:#x}` (lossless under ERC-55 since the server is case-insensitive
        // on EOA hex per the TS upstream's pass-through behavior).
        assert_eq!(
            url,
            "https://relayer-v2.polymarket.com/relay-payload?\
             address=0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266&type=PROXY",
            "URL must match the upstream wire-format byte-for-byte"
        );

        // ANTI-REGRESSION SHARDS: each one of these catches a specific past mistake.
        assert!(url.contains("/relay-payload?"),
            "path MUST be `/relay-payload` (kebab-case, per src/endpoints.ts:2 in TS upstream); \
             reverting to `/getRelayPayload` is what caused the 100% 404 in G7.2 prod diag");
        assert!(!url.contains("/getRelayPayload"),
            "negative guard: must NOT use the camelCase `/getRelayPayload` (the inferred-spec \
             path that 404'd in production)");
        assert!(url.contains("address=0x"),
            "query MUST use `address=` (per TS upstream src/client.ts:150-156 \
             `getRelayPayload(signerAddress, signerType)` -> `{{address: signerAddress, type: ...}}`)");
        assert!(!url.contains("from=0x") && !url.contains("from=" ),
            "negative guard: must NOT use `from=` (that was our inferred guess; the upstream \
             uses `address=`)");
        assert!(url.contains("&type=PROXY"),
            "query MUST end with `&type=PROXY` (per TS upstream `TransactionType.PROXY = \"PROXY\"` \
             passed as the `type` query param for proxy-wallet users)");
        // Sanity: no double slashes, no whitespace, no stray fragments.
        assert!(!url.contains("//relay-payload"),
            "no double-slash between base and path (base must not end with /)");
        assert!(!url.contains(' '), "no whitespace in URL");
        assert!(!url.contains('#'), "no URL fragment");
    }

    /// Property test: the URL builder is pure (same input -> same output) and
    /// deterministic. Belt-and-braces in case anyone adds nondeterminism
    /// (timestamp, RNG) to URL construction.
    #[test]
    fn g7_4_a_relay_payload_url_is_pure_and_deterministic() {
        let eoa = address!("e5a9fb008d4eec3e3386a52e0f326ca3f98fafe0");
        let base = "https://relayer-v2.polymarket.com";
        let a = build_get_relay_payload_url(base, eoa);
        let b = build_get_relay_payload_url(base, eoa);
        assert_eq!(a, b, "same input -> same URL");
        // A different EOA yields a different URL (the EOA actually flows into the query).
        let other = address!("0000000000000000000000000000000000000001");
        let c = build_get_relay_payload_url(base, other);
        assert_ne!(a, c, "different EOA must produce a different URL");
    }

    // ---------- G7.4-B: WIRE-FORMAT + ARRAY-PARSING gold tests for /transaction ----------

    /// Anti-regression GOLD: assert the exact bytes of the URL `RelayerClient`
    /// hits for `getTransaction`. Same rationale as G7.4-A: the MockRelayer
    /// never sees the real URL, so reverting `id=` back to `transactionID=`
    /// (the body field name, which a copy-paste mistake could easily reintroduce)
    /// would slip past every existing mock-based test.
    ///
    /// Spec source: `Polymarket/builder-relayer-client` `src/endpoints.ts:3`
    /// + `src/client.ts:158-164` `getTransaction(transactionId)` calls
    /// `this.send(GET_TRANSACTION, GET, {params: {id: transactionId}})`.
    #[test]
    fn g7_4_b_transaction_url_matches_upstream_wire_format() {
        // A realistic UUID-shaped transaction id (the relayer's `/submit` returns
        // a UUID string). Used as-is in the query -- no encoding/escaping needed
        // since UUIDs are URL-safe characters.
        let txid = "550e8400-e29b-41d4-a716-446655440000";
        let base = "https://relayer-v2.polymarket.com";

        let url = build_get_transaction_url(base, txid);

        // FULL-STRING GOLD: byte-for-byte assertion vs the upstream wire-format.
        assert_eq!(
            url,
            "https://relayer-v2.polymarket.com/transaction?id=550e8400-e29b-41d4-a716-446655440000",
            "URL must match the upstream wire-format byte-for-byte"
        );

        // ANTI-REGRESSION SHARDS: each one catches a specific past mistake.
        assert!(url.contains("/transaction?"),
            "path MUST be `/transaction` (per src/endpoints.ts:3 in TS upstream)");
        assert!(url.contains("?id="),
            "query MUST use `id=` (per TS upstream src/client.ts:158-164 \
             `{{id: transactionId}}` params -- NOT `transactionID=`, that's the \
             response BODY field name and was our wrongly-inferred query name)");
        assert!(!url.contains("transactionID="),
            "negative guard: must NOT use camelCase `transactionID=` as a query \
             param (the inferred-spec mistake)");
        // Sanity: no double slashes, no whitespace.
        assert!(!url.contains("//transaction"),
            "no double-slash between base and path");
        assert!(!url.contains(' '), "no whitespace in URL");
        assert!(!url.contains('#'), "no URL fragment");
    }

    /// `first_or_pending` must take the FIRST element of a non-empty array
    /// (matching TS `pollUntilState`'s `txns[0]` at `src/client.ts:490`).
    /// A single-element array is the common case after the relayer has
    /// propagated the tx to its read model.
    #[test]
    fn g7_4_b_parse_transaction_array_takes_first_item() {
        // Real-shaped relayer response: an array with one mined tx.
        let json = r#"[{
            "state": "STATE_MINED",
            "transactionHash": "0xabcd",
            "errorMessage": null
        }]"#;
        let arr: Vec<TransactionStatus> = serde_json::from_str(json)
            .expect("array of one mined status must parse");
        assert_eq!(arr.len(), 1);
        let picked = first_or_pending(arr);
        assert!(picked.is_mined(), "the single mined element must be picked");
        assert_eq!(picked.state, "STATE_MINED");
        assert_eq!(picked.transaction_hash.as_deref(), Some("0xabcd"));
    }

    /// EMPTY ARRAY = "relayer hasn't propagated the tx yet, keep polling"
    /// (per TS `if (txns.length > 0) { ... }` at `src/client.ts:489`). MUST NOT
    /// crash, MUST yield a pending status the `submit_and_wait` loop can see.
    /// This is the CRITICAL anti-crash test: a naive
    /// `serde_json::from_str::<TransactionStatus>("[]")` would fail with
    /// "expected struct, found array" and bring the poll loop down.
    #[test]
    fn g7_4_b_parse_empty_transaction_array_returns_pending_sentinel() {
        let json = "[]";
        let arr: Vec<TransactionStatus> = serde_json::from_str(json)
            .expect("empty array must parse without error");
        assert!(arr.is_empty(), "input is the empty array");
        let picked = first_or_pending(arr);
        // The sentinel is a pending state -- the poll loop treats it as
        // "keep polling", not "give up" or "crash".
        assert!(picked.is_pending(),
            "empty array must yield a PENDING status so the poll loop continues");
        assert!(!picked.is_mined() && !picked.is_failed(),
            "sentinel must not accidentally classify as terminal");
        assert_eq!(picked.state, STATE_NOT_FOUND_SENTINEL,
            "sentinel state string must be the distinctive synthesized value \
             (diagnostic-useful in operator logs)");
        assert!(picked.transaction_hash.is_none(),
            "sentinel must not have a fake tx hash");
        assert!(picked.error_message.is_none(),
            "sentinel must not invent an error message");
    }

    /// Defensive: multi-item array picks [0], matching TS `txns[0]`. The
    /// relayer shouldn't return multiple statuses for one `id` query in
    /// practice, but if it ever does (e.g. retries / reorgs surfaced), we must
    /// not silently take a different element than upstream does.
    #[test]
    fn g7_4_b_parse_multi_item_array_takes_first_not_last() {
        // First element = pending, second = mined. If we accidentally took
        // `last()` we'd report a mined tx; if we take `first()` we correctly
        // report pending (matching upstream).
        let json = r#"[
            {"state": "STATE_NEW", "transactionHash": null},
            {"state": "STATE_MINED", "transactionHash": "0xdeadbeef"}
        ]"#;
        let arr: Vec<TransactionStatus> = serde_json::from_str(json).unwrap();
        assert_eq!(arr.len(), 2);
        let picked = first_or_pending(arr);
        assert_eq!(picked.state, "STATE_NEW",
            "must take element [0] (per TS pollUntilState `txns[0]`), \
             NOT [.last()] or .find(is_mined)");
        assert!(picked.is_pending(),
            "first element was pending; picking it must report pending");
        assert!(!picked.is_mined(),
            "picking last would have falsely reported mined -- regression guard");
    }

    /// Negative parse: a single object (not wrapped in array) must FAIL to
    /// parse as `Vec<TransactionStatus>`. This is the regression-direction
    /// guard: if anyone tries to "simplify" the type back to a single
    /// `TransactionStatus`, the production parse line will start failing
    /// against the array shape -- and this test documents WHY we need Vec.
    #[test]
    fn g7_4_b_parse_single_object_response_fails_as_array() {
        let json_object = r#"{"state": "STATE_MINED", "transactionHash": "0xabc"}"#;
        let attempt: Result<Vec<TransactionStatus>, _> = serde_json::from_str(json_object);
        assert!(attempt.is_err(),
            "a single object MUST NOT deserialize as Vec<TransactionStatus> -- \
             the relayer always returns an array (even of length 1)");
        // Symmetric: as a single object it WOULD parse (so we know the schema
        // itself is fine -- the array wrapping is the actual contract).
        let as_single: Result<TransactionStatus, _> = serde_json::from_str(json_object);
        assert!(as_single.is_ok(),
            "the object schema itself is correct; only the array wrapping differs");
    }

    // ---------- G7.4-C2: BUILDER HMAC AUTH gold tests ----------

    /// GOLD VECTOR -- proves our HMAC matches the Polymarket Rust SDK's own
    /// CLOB L2 auth HMAC (`polymarket_client_sdk_v2::auth::hmac` line 278-285,
    /// gold-tested at `auth.rs:393-414` with this exact same input).
    ///
    /// If we ever drift from the SDK's algorithm (wrong base64 engine, wrong
    /// padding, wrong message order, accidental URL-vs-standard base64 swap,
    /// etc), this test catches it -- and the test failure points directly at
    /// the upstream invariant we broke.
    ///
    /// The Polymarket BUILDER HMAC uses the SAME algorithm (verified via the
    /// builder-signing-sdk@0.0.8 tarball, dist/signing/hmac.js), just with
    /// different HEADER NAMES applied to the output. So this CLOB-derived
    /// vector is authoritative for the BUILDER HMAC too.
    #[test]
    fn g7_4_c_hmac_matches_sdk_gold_vector() {
        // Inputs verbatim from polymarket_client_sdk_v2/src/auth.rs:393-414:
        //   secret    = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
        //   timestamp = 1_000_000
        //   method    = "test-sign"
        //   path      = "/orders"
        //   body      = r#"{"hash":"0x123"}"#
        // Expected: message  = r#"1000000test-sign/orders{"hash":"0x123"}"#
        //           signature = "4gJVbox-R6XlDK4nlaicig0_ANVL1qdcahiL8CXfXLM="
        let secret = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
        let message = build_hmac_message(
            1_000_000,
            "test-sign",
            "/orders",
            r#"{"hash":"0x123"}"#,
        );
        assert_eq!(
            message,
            r#"1000000test-sign/orders{"hash":"0x123"}"#,
            "message format must match SDK auth::to_message byte-for-byte"
        );
        let signature = compute_builder_hmac(secret, &message).expect("hmac succeeds");
        assert_eq!(
            signature,
            "4gJVbox-R6XlDK4nlaicig0_ANVL1qdcahiL8CXfXLM=",
            "HMAC must match SDK gold vector EXACTLY -- any drift here means the \
             algorithm diverged (wrong base64 alphabet / wrong padding / wrong order)"
        );
    }

    /// `build_hmac_message` is pure concatenation in a fixed order. This test
    /// pins the order explicitly (catches accidental swaps like "body before
    /// path" or "method before timestamp" that would still produce *some*
    /// signature but a different one).
    #[test]
    fn g7_4_c_hmac_message_order_is_timestamp_method_path_body() {
        let m = build_hmac_message(42, "POST", "/submit", "{\"a\":1}");
        assert_eq!(m, r#"42POST/submit{"a":1}"#);
        // Empty body -> no body suffix.
        let m_empty = build_hmac_message(42, "GET", "/relay-payload", "");
        assert_eq!(m_empty, "42GET/relay-payload");
        // Different timestamp -> different message.
        let m_other_ts = build_hmac_message(43, "POST", "/submit", "{\"a\":1}");
        assert_ne!(m, m_other_ts);
    }

    /// `build_submit_auth_headers` returns EXACTLY 4 headers with the exact
    /// POLY_BUILDER_* names the relayer's auth middleware expects (per
    /// builder-signing-sdk@0.0.8 dist/signer.js:15-21).
    ///
    /// Reverting a name (e.g. dropping the `BUILDER_` prefix to use the CLOB
    /// L2 `POLY_*` names) silently fails: the request reaches the server but
    /// the middleware doesn't recognize the auth and returns 401. This test
    /// catches that drift at compile-time-ish (= without an HTTP round-trip).
    #[test]
    fn g7_4_c_submit_headers_has_exactly_4_poly_builder_names() {
        let creds = BuilderCreds {
            key: "019aefed-d585-7f24-8343-64feccc151e3".to_string(),
            secret: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_string(),
            passphrase: "test-passphrase".to_string(),
        };
        let headers = build_submit_auth_headers(&creds, 1_000_000, "POST", "/submit", "{}")
            .expect("headers built");
        assert_eq!(headers.len(), 4, "MUST be exactly 4 POLY_BUILDER_* headers");
        let names: Vec<&str> = headers.iter().map(|(n, _)| *n).collect();
        // Order in our impl: API_KEY, PASSPHRASE, SIGNATURE, TIMESTAMP.
        assert_eq!(
            names,
            vec![
                "POLY_BUILDER_API_KEY",
                "POLY_BUILDER_PASSPHRASE",
                "POLY_BUILDER_SIGNATURE",
                "POLY_BUILDER_TIMESTAMP",
            ],
            "header names + order MUST match builder-signing-sdk dist/signer.js exactly"
        );
        // ANTI-REGRESSION SHARDS: no POLY_* (without _BUILDER_), no Bearer,
        // no Authorization, no Content-Type (the caller sets that separately).
        for (name, _) in &headers {
            assert!(name.starts_with("POLY_BUILDER_"),
                "every auth header MUST be prefixed POLY_BUILDER_; got: {name}");
            assert!(!name.eq_ignore_ascii_case("Authorization"),
                "no Bearer-style Authorization header -- builder uses HMAC, not Bearer");
            assert!(!name.eq_ignore_ascii_case("Content-Type"),
                "auth headers MUST NOT include Content-Type (set separately at HTTP call)");
        }
        // And NONE of the pre-G7.4-C names (the inferred-spec mistake).
        for (name, _) in &headers {
            assert_ne!(*name, "RELAYER_API_KEY", "pre-G7.4-C2 name removed");
            assert_ne!(*name, "RELAYER_API_KEY_ADDRESS", "pre-G7.4-C2 name removed");
        }
    }

    /// Header VALUES must reflect the inputs verbatim (API_KEY = creds.key,
    /// PASSPHRASE = creds.passphrase, TIMESTAMP = string of `timestamp_secs`,
    /// SIGNATURE = correct HMAC). Catches "field-name swap" bugs at the
    /// header-building boundary.
    #[test]
    fn g7_4_c_submit_headers_values_reflect_inputs() {
        let creds = BuilderCreds {
            key: "key-uuid-here".to_string(),
            secret: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_string(),
            passphrase: "passphrase-value".to_string(),
        };
        let ts = 1_700_000_000_i64;
        let headers = build_submit_auth_headers(&creds, ts, "POST", "/submit", r#"{"x":1}"#)
            .expect("headers built");
        let map: std::collections::HashMap<&str, String> =
            headers.iter().map(|(n, v)| (*n, v.clone())).collect();
        assert_eq!(map["POLY_BUILDER_API_KEY"], "key-uuid-here");
        assert_eq!(map["POLY_BUILDER_PASSPHRASE"], "passphrase-value");
        assert_eq!(map["POLY_BUILDER_TIMESTAMP"], "1700000000");
        // Signature must match the manual computation -- proves the header builder
        // didn't accidentally HMAC over a different message (e.g. wrong path).
        let expected_msg = format!(r#"{ts}POST/submit{{"x":1}}"#);
        let expected_sig = compute_builder_hmac(&creds.secret, &expected_msg).unwrap();
        assert_eq!(map["POLY_BUILDER_SIGNATURE"], expected_sig);
    }

    /// `BuilderCreds`'s `Debug` impl MUST redact secret + passphrase so an
    /// accidental `format!("{creds:?}")` in a log line cannot leak them into
    /// the file log. This is the second line of defense (the first is "we
    /// don't log creds at all"); the redaction makes the second-order mistake
    /// safe too.
    #[test]
    fn g7_4_c_builder_creds_debug_redacts_secret_and_passphrase() {
        let creds = BuilderCreds {
            key: "019aefed-d585-7f24-8343-64feccc151e3".to_string(),
            secret: "VERY-SECRET-VALUE-DO-NOT-LEAK".to_string(),
            passphrase: "VERY-PASSPHRASE-DO-NOT-LEAK".to_string(),
        };
        let dbg = format!("{creds:?}");
        // Key (UUID) is PUBLIC -- must be present.
        assert!(dbg.contains("019aefed-d585-7f24-8343-64feccc151e3"),
            "key UUID is public -- must appear in Debug; got: {dbg}");
        // Secret + passphrase MUST be redacted.
        assert!(!dbg.contains("VERY-SECRET-VALUE-DO-NOT-LEAK"),
            "secret MUST NOT appear in Debug; got: {dbg}");
        assert!(!dbg.contains("VERY-PASSPHRASE-DO-NOT-LEAK"),
            "passphrase MUST NOT appear in Debug; got: {dbg}");
        // The redaction marker must be obviously a placeholder, not a value.
        assert!(dbg.contains("redacted"),
            "Debug must use a `redacted` placeholder so the operator KNOWS \
             the value was withheld (vs an empty string from a missing field); got: {dbg}");
    }

    /// Invalid base64 secret must fail with a clear error (not panic, not silent
    /// 0-byte HMAC). Guards against the operator pasting a non-base64 value into
    /// POLYMARKET_BUILDER_SECRET (e.g. the raw hex of an entropy source).
    #[test]
    fn g7_4_c_hmac_rejects_invalid_base64_secret() {
        let bad = "not!valid!base64!@#$";
        let result = compute_builder_hmac(bad, "msg");
        assert!(result.is_err(), "invalid base64 secret MUST error");
        let err = format!("{:#}", result.unwrap_err());
        assert!(err.contains("base64") || err.contains("decod"),
            "error message must reference the base64 decode failure; got: {err}");
    }
}
