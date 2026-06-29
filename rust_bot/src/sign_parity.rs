//! Phase 6 Sub-paso A — OFFLINE signing-parity harness (`--sign-parity`).
//!
//! Proves the Rust SDK (`polymarket_client_sdk_v2`) and the Python SDK
//! (`py_clob_client_v2`) produce the SAME signed CTF-Exchange-V2 order for
//! IDENTICAL inputs — the Capa-A/B methodology applied to order signing, the gate
//! that de-risks every real order. It is PURE OFFLINE: ZERO network, ZERO orders.
//!
//! How offline is guaranteed:
//!   * a FIXED THROWAWAY test private key (NOT the real funded wallet) — signature
//!     parity only needs the SAME key on both sides, so the real credential never
//!     enters this path;
//!   * the order struct is built directly (`OrderV2` is pub; Default + field-set),
//!     bypassing `OrderBuilder::build()` (which fetches tick_size/version/book);
//!   * `client.set_neg_risk(token, false)` pre-seeds the only cache `client.sign`
//!     reads, so `sign` (local EIP-712 hash + ECDSA) makes no network call.
//!
//! It writes `params.json` (every pinned input incl. the test key) — the Python
//! side reads it so both build byte-identical orders — and `rust_signed.json`
//! (`{payload, domain, signing_hash, signature, post_body}`). A diff script
//! byte-compares the two sides. SALT: a FIXED constant HERE (reproducibility only);
//! production uses a deterministic-per-order salt (idempotency) — never this fixed
//! one.

use std::borrow::Cow;
use std::path::Path;
use std::str::FromStr;

use alloy::dyn_abi::Eip712Domain;
use alloy::sol_types::SolStruct;
use anyhow::{Context, Result, anyhow};
use polymarket_client_sdk_v2::auth::{Credentials, LocalSigner, Signer, Uuid};
use polymarket_client_sdk_v2::clob::types::{
    OrderPayload, OrderSignature, OrderType, OrderV2, SignableOrder, SignatureType, Side,
};
use polymarket_client_sdk_v2::types::{Address, B256, U256};
use polymarket_client_sdk_v2::{POLYGON, clob, contract_config, derive_proxy_wallet};
use serde_json::json;

// ---- pinned test inputs (identical on both sides; written to params.json) ----
/// Throwaway test key (32×0x11). A VALID secp256k1 key, NOT a funded wallet.
const TEST_PRIVATE_KEY: &str =
    "0x1111111111111111111111111111111111111111111111111111111111111111";
/// A representative outcome-token id (decimal). Value is irrelevant to parity as
/// long as both sides use it; chosen to look like a real token id.
const TOKEN_ID_DEC: &str =
    "71321045679252212594626385532706912750332728571942532289631379312455583992563";
/// Fixed test salt (reproducibility ONLY — production derives salt per order id).
const FIXED_SALT: u64 = 470_111_222_333; // < 2^53
/// Fixed maker/taker integer amounts (6-decimal USDC units). BUY $1 @ 0.50:
/// makerAmount = 1.00 USDC = 1_000_000; takerAmount = 2.00 shares = 2_000_000.
const MAKER_AMOUNT: u128 = 1_000_000;
const TAKER_AMOUNT: u128 = 2_000_000;
/// Fixed V2 timestamp (ms). Pinned so the EIP-712 hash is reproducible.
const TIMESTAMP_MS: u64 = 1_780_000_000_000;
const CLOB_HOST: &str = "https://clob.polymarket.com";

pub async fn run_cli(out_dir: Option<&Path>) -> Result<()> {
    let out = out_dir
        .map(Path::to_path_buf)
        .unwrap_or_else(|| Path::new("data/derived/sign_parity").to_path_buf());
    std::fs::create_dir_all(&out)?;

    // ---- signer (test key) + Proxy funder ----
    let signer = LocalSigner::from_str(TEST_PRIVATE_KEY)
        .context("parsing TEST_PRIVATE_KEY")?
        .with_chain_id(Some(POLYGON));
    let eoa: Address = signer.address();
    let funder = derive_proxy_wallet(eoa, POLYGON)
        .ok_or_else(|| anyhow!("derive_proxy_wallet returned None for chain {POLYGON}"))?;
    let token_id = U256::from_str(TOKEN_ID_DEC).context("parsing TOKEN_ID_DEC")?;

    // ---- offline authenticated CLOB client (dummy creds; authenticate() is local) ----
    let creds = Credentials::new(Uuid::nil(), "test-secret".to_string(), "test-pass".to_string());
    let unauth = clob::Client::new(CLOB_HOST, clob::Config::default())
        .map_err(|e| anyhow!("building CLOB client: {e}"))?;
    let client = unauth
        .authentication_builder(&signer)
        .credentials(creds)
        .funder(funder)
        .signature_type(SignatureType::Proxy) // sig_type=1, POLY_PROXY
        .authenticate()
        .await
        .map_err(|e| anyhow!("CLOB authenticate (local) failed: {e}"))?;
    // Pre-seed the ONLY cache `sign` reads → no network during sign.
    client.set_neg_risk(token_id, false);

    // ---- construct OrderV2 directly (pub, non_exhaustive → Default + field-set) ----
    let mut order = OrderV2::default();
    order.salt = U256::from(FIXED_SALT);
    order.maker = funder; // Proxy: maker = funder (proxy wallet)
    order.signer = eoa; // signer = EOA (the key)
    order.tokenId = token_id;
    order.makerAmount = U256::from(MAKER_AMOUNT);
    order.takerAmount = U256::from(TAKER_AMOUNT);
    order.side = Side::Buy as u8; // 0
    order.signatureType = SignatureType::Proxy as u8; // 1
    order.timestamp = U256::from(TIMESTAMP_MS);
    order.metadata = B256::ZERO;
    order.builder = B256::ZERO;

    let payload = OrderPayload::new(order, U256::ZERO); // expiration out-of-struct = 0
    let signable = SignableOrder::builder()
        .payload(payload)
        .order_type(OrderType::FOK)
        .build();

    // ---- sign (LOCAL: EIP-712 hash + ECDSA; no network thanks to the pre-seed) ----
    let signed = client
        .sign(&signer, signable)
        .await
        .map_err(|e| anyhow!("client.sign failed: {e}"))?;
    let o = signed.order(); // &OrderV2 in the signed payload

    // ---- the domain `sign` used (replicated for the dump; same constants) ----
    let exchange = contract_config(POLYGON, false)
        .and_then(|c| c.exchange_v2)
        .ok_or_else(|| anyhow!("no exchange_v2 in contract_config"))?;
    let domain = Eip712Domain {
        name: Some(Cow::Borrowed("Polymarket CTF Exchange")),
        version: Some(Cow::Borrowed("2")),
        chain_id: Some(U256::from(POLYGON)),
        verifying_contract: Some(exchange),
        salt: None,
    };
    let signing_hash = o.eip712_signing_hash(&domain);

    let signature_hex = match &signed.signature {
        OrderSignature::Ecdsa(s) => format!("0x{}", alloy::hex::encode(s.as_bytes())),
        OrderSignature::Wrapped(w) => w.clone(),
        other => return Err(anyhow!("unexpected OrderSignature variant (SDK changed): {other:?}")),
    };
    let post_body = serde_json::to_value(&signed).context("serializing SignedOrder (post body)")?;

    // ---- params.json (inputs the Python side replays) ----
    let addr_lc = |a: Address| format!("0x{}", alloy::hex::encode(a.as_slice()));
    let params = json!({
        "private_key": TEST_PRIVATE_KEY,
        "chain_id": POLYGON,
        "neg_risk": false,
        "verifying_contract": addr_lc(exchange),
        "token_id": TOKEN_ID_DEC,
        "side": Side::Buy as u8,
        "signature_type": SignatureType::Proxy as u8,
        "maker": addr_lc(funder),
        "signer": addr_lc(eoa),
        "maker_amount": MAKER_AMOUNT.to_string(),
        "taker_amount": TAKER_AMOUNT.to_string(),
        "salt": FIXED_SALT,
        "timestamp_ms": TIMESTAMP_MS,
        "order_type": "FOK",
    });

    let rust_out = json!({
        "payload": {
            "salt": FIXED_SALT.to_string(),
            "maker": addr_lc(o.maker),
            "signer": addr_lc(o.signer),
            "tokenId": o.tokenId.to_string(),
            "makerAmount": o.makerAmount.to_string(),
            "takerAmount": o.takerAmount.to_string(),
            "side": o.side,
            "signatureType": o.signatureType,
            "timestamp": o.timestamp.to_string(),
            "metadata": format!("0x{}", alloy::hex::encode(o.metadata.as_slice())),
            "builder": format!("0x{}", alloy::hex::encode(o.builder.as_slice())),
        },
        "domain": {
            "name": "Polymarket CTF Exchange",
            "version": "2",
            "chainId": POLYGON,
            "verifyingContract": addr_lc(exchange),
        },
        "signing_hash": format!("0x{}", alloy::hex::encode(signing_hash.as_slice())),
        "signature": signature_hex,
        "post_body": post_body,
    });

    let params_path = out.join("params.json");
    let rust_path = out.join("rust_signed.json");
    std::fs::write(&params_path, serde_json::to_string_pretty(&params)?)?;
    std::fs::write(&rust_path, serde_json::to_string_pretty(&rust_out)?)?;

    println!("[sign-parity rust] EOA={} funder(proxy)={}", addr_lc(eoa), addr_lc(funder));
    println!("[sign-parity rust] signing_hash={}", rust_out["signing_hash"]);
    println!("[sign-parity rust] signature={}", rust_out["signature"]);
    println!("[sign-parity rust] wrote {} + {}", params_path.display(), rust_path.display());
    println!("[sign-parity rust] NOTE: offline, no order sent. Test key only.");
    Ok(())
}
