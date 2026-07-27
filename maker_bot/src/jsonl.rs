//! Append-only JSONL sinks with UTC-daily rotation and optional zstd.
//!
//! Order #15 A0 ("log inputs, not conclusions") and B4 (storage) both land here.
//! Part A writes low-volume per-post/per-fill records uncompressed so they stay
//! greppable; Part B writes the `price_change` firehose compressed — the June
//! recorder produced ~58 GB/day uncompressed for btc+eth at 5m+15m alone and this
//! order adds sol+xrp.
//!
//! Layout mirrors the proven recorder so existing analysis code keeps working:
//!   `<root>/<YYYY-MM-DD>/<subdir>/<name>.jsonl[.zst]`

use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

/// One rotating sink. Rotation is driven by the caller-supplied UTC day string, so
/// the writer needs no clock of its own and stays unit-testable.
pub struct DayWriter {
    root: PathBuf,
    subdir: String,
    name: String,
    compress: bool,
    day: Option<String>,
    sink: Option<Sink>,
    bytes: u64,
    lines: u64,
}

enum Sink {
    Plain(BufWriter<File>),
    Zstd(Box<zstd::Encoder<'static, BufWriter<File>>>),
}

impl Sink {
    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        match self {
            Sink::Plain(w) => w.write_all(buf),
            Sink::Zstd(w) => w.write_all(buf),
        }
    }
    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Sink::Plain(w) => w.flush(),
            Sink::Zstd(w) => w.flush(),
        }
    }
    /// zstd frames MUST be finished or the tail of the file is unreadable — the
    /// exact way a recorder silently loses its last day.
    fn finish(self) -> std::io::Result<()> {
        match self {
            Sink::Plain(mut w) => w.flush(),
            Sink::Zstd(w) => w.finish().map(|mut inner| { let _ = inner.flush(); }),
        }
    }
}

impl DayWriter {
    #[must_use]
    pub fn new(root: impl Into<PathBuf>, subdir: &str, name: &str, compress: bool) -> Self {
        Self {
            root: root.into(),
            subdir: subdir.to_string(),
            name: name.to_string(),
            compress,
            day: None,
            sink: None,
            bytes: 0,
            lines: 0,
        }
    }

    /// Full path this writer uses for a given day.
    #[must_use]
    pub fn path_for(&self, day: &str) -> PathBuf {
        let ext = if self.compress { "jsonl.zst" } else { "jsonl" };
        self.root.join(day).join(&self.subdir).join(format!("{}.{}", self.name, ext))
    }

    /// Bytes handed to the sink (pre-compression) — the honest input rate, which is
    /// what B4 asks be measured before committing to a retention policy.
    #[must_use]
    pub fn bytes(&self) -> u64 {
        self.bytes
    }
    #[must_use]
    pub fn lines(&self) -> u64 {
        self.lines
    }

    /// Append one line, rotating first if `day` changed.
    pub fn write_line(&mut self, day: &str, line: &str) -> std::io::Result<()> {
        if self.day.as_deref() != Some(day) {
            self.rotate(day)?;
        }
        let Some(sink) = self.sink.as_mut() else { return Ok(()) };
        sink.write_all(line.as_bytes())?;
        sink.write_all(b"\n")?;
        self.bytes += line.len() as u64 + 1;
        self.lines += 1;
        Ok(())
    }

    fn rotate(&mut self, day: &str) -> std::io::Result<()> {
        if let Some(old) = self.sink.take() {
            old.finish()?;
        }
        let path = self.path_for(day);
        if let Some(dir) = path.parent() {
            fs::create_dir_all(dir)?;
        }
        let file = File::options().create(true).append(true).open(&path)?;
        let w = BufWriter::new(file);
        self.sink = Some(if self.compress {
            Sink::Zstd(Box::new(zstd::Encoder::new(w, 3)?))
        } else {
            Sink::Plain(w)
        });
        self.day = Some(day.to_string());
        Ok(())
    }

    pub fn flush(&mut self) -> std::io::Result<()> {
        if let Some(s) = self.sink.as_mut() { s.flush() } else { Ok(()) }
    }

    /// DURABILITY CHECKPOINT — finish the current zstd frame and reopen in append
    /// mode, so everything written so far is readable even after a hard kill.
    ///
    /// This exists because a live test killed the recorder with SIGTERM and the whole
    /// day's `.zst` files decoded to NOTHING: `flush()` only pushes bytes, it does not
    /// close the frame, and an unfinished frame is unreadable. zstd files are
    /// concatenations of independent frames, so checkpointing simply appends another
    /// one — decoders read straight through. Loss on an abrupt kill is now bounded by
    /// the checkpoint interval instead of being "the entire day".
    /// No-op for plain sinks (already durable line-by-line).
    pub fn checkpoint(&mut self) -> std::io::Result<()> {
        if !self.compress {
            return self.flush();
        }
        let Some(day) = self.day.clone() else { return Ok(()) };
        if let Some(old) = self.sink.take() {
            old.finish()?;
        }
        // Reopen the SAME path in append mode → a second frame in the same file.
        let path = self.path_for(&day);
        let file = File::options().create(true).append(true).open(&path)?;
        self.sink = Some(Sink::Zstd(Box::new(zstd::Encoder::new(BufWriter::new(file), 3)?)));
        Ok(())
    }

    /// Close cleanly (finishes the zstd frame). Call on shutdown.
    pub fn close(&mut self) -> std::io::Result<()> {
        if let Some(s) = self.sink.take() {
            s.finish()?;
        }
        Ok(())
    }
}

impl Drop for DayWriter {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

/// Total bytes under a directory (recursive). Used by the disk-cap sweep.
pub fn dir_size(path: &Path) -> u64 {
    let Ok(rd) = fs::read_dir(path) else { return 0 };
    let mut total = 0;
    for e in rd.flatten() {
        let Ok(md) = e.metadata() else { continue };
        total += if md.is_dir() { dir_size(&e.path()) } else { md.len() };
    }
    total
}

/// Day directories under `root`, ascending (oldest first). Only `YYYY-MM-DD` names.
pub fn day_dirs(root: &Path) -> Vec<(String, PathBuf)> {
    let Ok(rd) = fs::read_dir(root) else { return Vec::new() };
    let mut days: Vec<(String, PathBuf)> = rd
        .flatten()
        .filter(|e| e.path().is_dir())
        .filter_map(|e| {
            let name = e.file_name().to_string_lossy().to_string();
            let looks_like_day = name.len() == 10
                && name.as_bytes()[4] == b'-'
                && name.as_bytes()[7] == b'-'
                && name.chars().filter(char::is_ascii_digit).count() == 8;
            looks_like_day.then_some((name, e.path()))
        })
        .collect();
    days.sort_by(|a, b| a.0.cmp(&b.0));
    days
}

/// Which files an eviction pass may delete, oldest day first, until usage is back
/// under `target_bytes`.
///
/// B4: `price_change` is the evictable firehose. `book` and `markets` must NEVER be
/// evicted — full snapshots are what make depth reconstruction possible at all, and
/// `markets` is the self-contained index that removes the need for any API call.
#[must_use]
pub fn evictable(name: &str) -> bool {
    !matches!(name, "book" | "markets")
}

/// Result of a disk-usage check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiskState {
    Ok,
    /// Past the warning fraction (B4 asks for 80%) but under the cap.
    Warn,
    /// Over cap — the caller should evict oldest evictable files.
    Evict,
}

#[must_use]
pub fn disk_state(used_bytes: u64, cap_bytes: u64, warn_frac: f64) -> DiskState {
    if cap_bytes == 0 {
        return DiskState::Ok;
    }
    if used_bytes >= cap_bytes {
        DiskState::Evict
    } else if (used_bytes as f64) >= (cap_bytes as f64) * warn_frac {
        DiskState::Warn
    } else {
        DiskState::Ok
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "mb_{tag}_{}_{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ))
    }

    #[test]
    fn writes_and_rotates_per_utc_day() {
        let root = tmp("rot");
        let mut w = DayWriter::new(&root, "polymarket", "book", false);
        w.write_line("2026-07-27", r#"{"a":1}"#).unwrap();
        w.write_line("2026-07-27", r#"{"a":2}"#).unwrap();
        w.write_line("2026-07-28", r#"{"a":3}"#).unwrap(); // rotation
        w.close().unwrap();

        let d1 = fs::read_to_string(root.join("2026-07-27/polymarket/book.jsonl")).unwrap();
        let d2 = fs::read_to_string(root.join("2026-07-28/polymarket/book.jsonl")).unwrap();
        assert_eq!(d1.lines().count(), 2, "day 1 keeps its own lines");
        assert_eq!(d2.lines().count(), 1, "day 2 starts a fresh file");
        assert_eq!(w.lines(), 3);
        let _ = fs::remove_dir_all(&root);
    }

    /// A zstd file must be readable back — i.e. the frame was finished. An unfinished
    /// frame is how a recorder silently loses its most recent day.
    #[test]
    fn zstd_output_round_trips() {
        let root = tmp("zstd");
        let mut w = DayWriter::new(&root, "polymarket", "price_change", true);
        for i in 0..500 {
            w.write_line("2026-07-27", &format!(r#"{{"i":{i}}}"#)).unwrap();
        }
        w.close().unwrap();

        let path = root.join("2026-07-27/polymarket/price_change.jsonl.zst");
        let bytes = fs::read(&path).unwrap();
        let text = zstd::decode_all(&bytes[..]).unwrap();
        let text = String::from_utf8(text).unwrap();
        assert_eq!(text.lines().count(), 500, "every line must survive the round trip");
        assert!(text.contains(r#"{"i":499}"#), "including the LAST one (frame finished)");
        assert!((bytes.len() as u64) < w.bytes(), "compression must actually shrink it");
        let _ = fs::remove_dir_all(&root);
    }

    /// LIVE-CAUGHT REGRESSION: a hard kill must not cost the whole day. After a
    /// checkpoint every written line is readable, and the multi-frame file that
    /// results decodes transparently — verified here rather than assumed, because
    /// this is exactly the failure mode that silently empties an archive.
    #[test]
    fn checkpointed_zstd_survives_an_abrupt_kill() {
        let root = tmp("ckpt");
        let path = {
            let mut w = DayWriter::new(&root, "polymarket", "price_change", true);
            for i in 0..200 {
                w.write_line("2026-07-27", &format!(r#"{{"i":{i}}}"#)).unwrap();
            }
            w.checkpoint().unwrap(); // ← the durability barrier
            for i in 200..300 {
                w.write_line("2026-07-27", &format!(r#"{{"i":{i}}}"#)).unwrap();
            }
            w.checkpoint().unwrap();
            let p = w.path_for("2026-07-27");
            // Simulate SIGKILL: leak the writer so Drop/close never runs.
            std::mem::forget(w);
            p
        };
        let bytes = fs::read(&path).unwrap();
        let text = String::from_utf8(zstd::decode_all(&bytes[..]).unwrap()).unwrap();
        assert_eq!(text.lines().count(), 300, "every checkpointed line must survive a hard kill");
        assert!(text.contains(r#"{"i":299}"#), "including the last line before the kill");
        let _ = fs::remove_dir_all(&root);
    }

    /// B4: `book` and `markets` are never evictable; `price_change` is the firehose
    /// that is.
    #[test]
    fn eviction_policy_protects_book_and_markets() {
        assert!(!evictable("book"), "full snapshots are the irreplaceable channel");
        assert!(!evictable("markets"), "the self-contained index must survive");
        assert!(evictable("price_change"), "the firehose is the evictable one");
        assert!(evictable("best_bid_ask"));
    }

    #[test]
    fn disk_state_warns_at_80_percent_then_evicts() {
        let cap = 1_000u64;
        assert_eq!(disk_state(700, cap, 0.8), DiskState::Ok);
        assert_eq!(disk_state(800, cap, 0.8), DiskState::Warn, "warn at 80%");
        assert_eq!(disk_state(999, cap, 0.8), DiskState::Warn);
        assert_eq!(disk_state(1_000, cap, 0.8), DiskState::Evict, "at cap → evict");
        assert_eq!(disk_state(5_000, 0, 0.8), DiskState::Ok, "cap 0 disables the sweep");
    }

    #[test]
    fn day_dirs_are_sorted_oldest_first_and_ignore_junk() {
        let root = tmp("days");
        for d in ["2026-07-28", "2026-07-26", "2026-07-27", "notaday", "tmp"] {
            fs::create_dir_all(root.join(d)).unwrap();
        }
        let days: Vec<String> = day_dirs(&root).into_iter().map(|(d, _)| d).collect();
        assert_eq!(days, vec!["2026-07-26", "2026-07-27", "2026-07-28"], "oldest first, junk ignored");
        let _ = fs::remove_dir_all(&root);
    }
}
