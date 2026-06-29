//! Phase 0 task stubs.
//!
//! The 9 tokio tasks from the approved architecture are declared and spawned
//! here as placeholders: each logs "task started" then loops on a sleep.
//! NO real logic. Channels, WS clients, REST, signing, and state I/O all arrive
//! in later phases; for now the tasks only prove the runtime wires up and stays
//! alive without panicking.
//!
//! Later phases will split these into their real modules (signal_engine,
//! decision_engine, execution, state, …) and give them channel endpoints. The
//! names here mirror those future modules.
//!
//! NOTE: `binance_ws` and `polymarket_ws` graduated to real clients in Phase 1
//! (see `crate::ws`); `market_refresh` graduated to dynamic market discovery
//! (see `crate::discovery`); `state_persist` graduated to the crash-safe snapshot
//! task in Phase 3 (see `crate::state::store`). They no longer live here.

use std::time::Duration;

use tracing::{debug, info};

/// Shared stub body: announce start, then heartbeat forever on `period`.
async fn heartbeat(task: &'static str, period: Duration) {
    info!(task, period_secs = period.as_secs(), "task started");
    let mut tick: u64 = 0;
    loop {
        tokio::time::sleep(period).await;
        tick += 1;
        debug!(task, tick, "heartbeat");
    }
}

// --- Stream ingestion (user channel; market channels are now in crate::ws) --

pub async fn polymarket_user_ws() {
    heartbeat("polymarket_user_ws", Duration::from_secs(15)).await;
}

// --- Engines (signal_engine / decision_engine / execution) graduated to the real
//     decision→execution→exit loop in Phase 6 D1 (see crate::trading_loop). The
//     stubs are gone. --------------------------------------------------------------

// --- Periodic maintenance ---------------------------------------------------

pub async fn balance_refresh() {
    heartbeat("balance_refresh", Duration::from_secs(60)).await;
}
