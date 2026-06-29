//! tracing setup.
//!
//! Two sinks, per the Phase 0 spec ("tracing: JSON a archivo + stdout"):
//! - stdout: human-readable lines for the operator watching the console.
//! - file:   newline-delimited JSON, rotated daily, for machine parsing / shipping.

use anyhow::Context;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, fmt};

use crate::config::Config;

/// Initialize the global tracing subscriber.
///
/// Returns a [`WorkerGuard`] that MUST stay alive for the lifetime of the
/// program. Dropping it flushes and shuts down the background file-writer
/// thread; dropping it early loses buffered file logs. `main` holds it until
/// exit.
///
/// Level precedence: `--log-level` (level_override) > `RUST_LOG` env >
/// `config.logging.level`.
pub fn init(config: &Config, level_override: Option<&str>) -> anyhow::Result<WorkerGuard> {
    let filter = match level_override {
        Some(level) => EnvFilter::try_new(level)
            .with_context(|| format!("invalid --log-level filter: {level}"))?,
        None => EnvFilter::try_from_default_env().or_else(|_| {
            EnvFilter::try_new(&config.logging.level)
        })
        .with_context(|| {
            format!("invalid logging.level filter: {}", config.logging.level)
        })?,
    };

    // Rotating daily JSON file appender. (Phase 0 only implements daily; the
    // config field is plumbed for later cadences.)
    std::fs::create_dir_all(&config.logging.trace_path)
        .with_context(|| format!("creating log dir: {}", config.logging.trace_path))?;
    let file_appender =
        tracing_appender::rolling::daily(&config.logging.trace_path, "rust_bot.jsonl");
    let (file_writer, guard) = tracing_appender::non_blocking(file_appender);

    let file_layer = fmt::layer()
        .json()
        .with_writer(file_writer)
        .with_current_span(true)
        .with_span_list(false)
        .with_ansi(false);

    // ANSI off on stdout too: keeps gate-captured output free of escape codes.
    let stdout_layer = fmt::layer()
        .with_writer(std::io::stdout)
        .with_target(false)
        .with_ansi(false);

    tracing_subscriber::registry()
        .with(filter)
        .with(file_layer)
        .with(stdout_layer)
        .init();

    Ok(guard)
}
