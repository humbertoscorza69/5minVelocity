//! Append-only JSONL recorder for ingested-message timestamps.
//!
//! Every websocket message we ingest is logged here as one JSON line. Phase 1
//! used these records two ways: counting them for the ±2% baseline against the
//! Python recorder (CLOSED on 2026-04-23, see task #18), and (later) computing
//! Binance→Polymarket lag (NEVER IMPLEMENTED — no consumer wired in src/ or
//! scripts/).
//!
//! Because both consumers are gone the file is now write-only and effectively
//! a disk-filler (~20 GB/day in production). The bot supports turning the
//! recorder OFF entirely via `logging.event_logger_enabled = false` in
//! `bot.toml`. The OPERATIVE default in code is `true` (preserves the
//! pre-opt-out behavior byte-identical); the production toml flips it to
//! `false` explicitly.
//!
//! When disabled, `spawn` returns a no-op logger that
//!   * does NOT touch the configured path (no `create_dir_all`, no `OpenOptions`,
//!     no zero-byte file appears),
//!   * accepts `record()` calls as a cheap drop,
//!   * returns a JoinHandle for an already-finished task so the standard
//!     shutdown drain (`drop(logger); join_or_abort(...)`) still works unchanged.
//!
//! The hot path (WS read loops) only does a non-blocking `send` into an
//! unbounded channel; a single dedicated task owns the file and batches writes
//! behind a `BufWriter`, flushing every 500ms and again on shutdown. Unbounded
//! is deliberate: we never want to drop records (it would corrupt the count
//! gate, while it existed), and local-SSD throughput easily keeps up with the
//! message rate.

use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::time::Duration;

use serde::Serialize;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{error, info};

/// One ingested message. `recv_ms` is local wall-clock at receive; `exch_ms` is
/// the exchange-provided timestamp when present.
#[derive(Debug, Serialize)]
pub struct EventRecord {
    pub recv_ms: i64,
    pub source: &'static str,
    pub event_type: String,
    pub key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exch_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub px: Option<f64>,
}

/// Cloneable handle the WS clients use to record events. Dropping all clones
/// signals the writer task to flush and stop.
///
/// When the logger is disabled at spawn time (`enabled=false`), `tx` is `None`
/// and `record()` is a cheap no-op — the 6 call-sites in `ws/binance.rs` and
/// `ws/polymarket.rs` need no change.
#[derive(Clone)]
pub struct EventLogger {
    tx: Option<mpsc::UnboundedSender<EventRecord>>,
}

impl EventLogger {
    /// Non-blocking. No-op when the logger was spawned disabled, or once the
    /// writer task has already stopped.
    pub fn record(&self, rec: EventRecord) {
        if let Some(tx) = &self.tx {
            let _ = tx.send(rec);
        }
    }
}

/// Spawn the event-log writer.
///
/// * `enabled = true` (legacy / default): open `path` (creating parent dirs)
///   for append and spawn the writer task. Behavior is byte-identical to the
///   pre-opt-out implementation.
/// * `enabled = false`: do NOT touch the filesystem at all (no parent dir,
///   no file open). Return a no-op logger plus an already-finished JoinHandle
///   so the caller's shutdown drain works unchanged.
///
/// Returns the logger handle and the task's `JoinHandle` (await it on shutdown
/// after dropping every logger clone, to guarantee a final flush in the
/// enabled case; the disabled handle resolves immediately).
pub fn spawn(path: &Path, enabled: bool) -> anyhow::Result<(EventLogger, JoinHandle<()>)> {
    if !enabled {
        // Disabled: deliberately do NOT call `create_dir_all` or `OpenOptions`.
        // The configured path must remain untouched — no zero-byte file may
        // appear on the VPS just because the writer was meant to be off.
        // The no-op task finishes immediately so `join_or_abort` is trivial.
        let handle = tokio::spawn(async {});
        info!(
            path = %path.display(),
            "event logger DISABLED (logging.event_logger_enabled = false); \
             timestamps file will NOT be opened or created"
        );
        return Ok((EventLogger { tx: None }, handle));
    }

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let file = OpenOptions::new().create(true).append(true).open(path)?;
    let display_path = path.display().to_string();

    let (tx, mut rx) = mpsc::unbounded_channel::<EventRecord>();
    let handle = tokio::spawn(async move {
        let mut buf = BufWriter::new(file);
        let mut flush_iv = tokio::time::interval(Duration::from_millis(500));
        flush_iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut written: u64 = 0;

        loop {
            tokio::select! {
                maybe = rx.recv() => match maybe {
                    Some(rec) => match serde_json::to_string(&rec) {
                        Ok(line) => {
                            if let Err(e) = writeln!(buf, "{line}") {
                                error!(error = %e, "event log write failed");
                            } else {
                                written += 1;
                                if written.is_multiple_of(1024) {
                                    let _ = buf.flush();
                                }
                            }
                        }
                        Err(e) => error!(error = %e, "event serialize failed"),
                    },
                    // All logger clones dropped → drain complete, shut down.
                    None => break,
                },
                _ = flush_iv.tick() => {
                    let _ = buf.flush();
                }
            }
        }

        let _ = buf.flush();
        info!(path = %display_path, records = written, "event logger flushed and stopped");
    });

    Ok((EventLogger { tx: Some(tx) }, handle))
}

#[cfg(test)]
mod tests {
    //! Tests for the opt-out path. The enabled path is exercised end-to-end by
    //! the Phase 1 baseline gate (closed 2026-04-23); we cover its file-creation
    //! shape here too as a regression guard.
    //!
    //! NOTE: we deliberately do NOT use `tempfile` (not a dev-dep here). We use
    //! `std::env::temp_dir()` joined with a deterministic unique suffix (PID +
    //! test name) and clean up after ourselves. No `Date::now`, no randomness.
    use super::*;
    use std::path::PathBuf;

    fn unique_tmp(name: &str) -> PathBuf {
        let pid = std::process::id();
        // No randomness, no clock: pid + test-name is enough for `cargo test`
        // worker isolation. If the path already exists, blow it away first.
        let p = std::env::temp_dir().join(format!("rust_bot_evlog_test_{pid}_{name}"));
        if p.exists() {
            let _ = std::fs::remove_file(&p);
        }
        p
    }

    /// CORE OPT-OUT INVARIANT: with `enabled=false`, the file at `path` must
    /// NOT be created. No `OpenOptions::create(true)` call must reach the
    /// filesystem. A zero-byte `timestamps.jsonl` on the VPS would be
    /// misleading — clean = "the file simply does not exist when disabled".
    #[tokio::test]
    async fn spawn_disabled_does_not_create_file() {
        let path = unique_tmp("disabled_no_file");
        assert!(!path.exists(), "precondition: tmp path must not pre-exist");

        let (logger, handle) = spawn(&path, false).expect("spawn(disabled) must succeed");

        // Drive a record() to prove it's a cheap no-op (no panic, no I/O).
        logger.record(EventRecord {
            recv_ms: 0,
            source: "test",
            event_type: "noop".into(),
            key: "k".into(),
            exch_ms: None,
            px: None,
        });

        drop(logger);
        // The disabled handle is already finished; await must complete promptly.
        handle.await.expect("disabled join must succeed");

        assert!(
            !path.exists(),
            "FAIL: disabled spawn created the file at {} -- the path must \
             remain untouched (no parent dir create, no OpenOptions). A \
             zero-byte file on the VPS is the exact regression this test \
             guards against.",
            path.display()
        );
    }

    /// Even if the parent directory does NOT exist, disabled spawn must still
    /// succeed and create NOTHING. Asymmetry with the enabled path (which calls
    /// `create_dir_all`) is intentional: when off, we touch zero filesystem.
    #[tokio::test]
    async fn spawn_disabled_does_not_create_parent_dir() {
        let parent = unique_tmp("disabled_no_parent_dir");
        let path = parent.join("timestamps.jsonl");
        assert!(!parent.exists(), "precondition: parent must not pre-exist");

        let (logger, handle) = spawn(&path, false)
            .expect("spawn(disabled) must succeed even with non-existent parent");
        drop(logger);
        handle.await.expect("disabled join must succeed");

        assert!(
            !parent.exists(),
            "FAIL: disabled spawn created parent dir {} -- it must touch \
             zero filesystem when off.",
            parent.display()
        );
        assert!(!path.exists(), "FAIL: disabled spawn created the file");
    }

    /// Regression guard for the enabled path: it MUST still create the file
    /// (this is the byte-identical pre-opt-out behavior). If this flips, the
    /// opt-out broke the enabled case.
    #[tokio::test]
    async fn spawn_enabled_creates_file() {
        let path = unique_tmp("enabled_creates_file");
        if path.exists() {
            let _ = std::fs::remove_file(&path);
        }

        let (logger, handle) = spawn(&path, true).expect("spawn(enabled) must succeed");
        drop(logger);
        handle.await.expect("enabled join must succeed");

        assert!(
            path.exists(),
            "FAIL: enabled spawn did NOT create the file at {} -- this is \
             the byte-identical pre-opt-out behavior and must be preserved.",
            path.display()
        );
        let _ = std::fs::remove_file(&path);
    }

    /// record() on a disabled logger must NEVER panic, even after the handle's
    /// task has completed. Defensive: WS clients might still hold a clone of
    /// the logger past shutdown.
    #[tokio::test]
    async fn record_disabled_never_panics() {
        let path = unique_tmp("record_disabled_no_panic");
        let (logger, handle) = spawn(&path, false).expect("spawn(disabled) must succeed");
        handle.await.expect("noop handle joins immediately");

        // Many records after the task is gone — must be silent.
        for i in 0..100 {
            logger.record(EventRecord {
                recv_ms: i,
                source: "test",
                event_type: "x".into(),
                key: "k".into(),
                exch_ms: None,
                px: None,
            });
        }
    }
}
