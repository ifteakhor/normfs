//! Shared setup for the WAL benchmarks.
//!
//! Nothing here is configurable by environment variable. A benchmark whose
//! numbers depend on how it was invoked cannot be compared against a pasted log,
//! so the shape of every case is fixed in code and the one quantity that
//! genuinely varies by machine — how much data it takes to defeat the page cache
//! — is measured rather than asked for.
//!
//! The default payload is [`PAYLOAD`]: a sensor message, the workload the WAL
//! exists for and the size at which per-entry framing decides the file size.
//! [`BIG_PAYLOAD`] is kept alongside it as the contrast, where bytes dominate
//! and framing cannot matter.

#![allow(dead_code)] // each bench binary uses a subset

use std::path::Path;
use std::time::Duration;

use bytes::Bytes;
use normfs_types::{QueueId, QueueIdResolver};
use normfs_wal::{WalHeader, WalSettings, WalStore};
use uintn::UintN;

/// A sensor message. Small records are what high-frequency ingestion produces,
/// and the only size at which per-entry framing is a meaningful fraction of the
/// file.
pub const PAYLOAD: usize = 80;

/// A large sensor block, for contrast: at this size framing is under 0.05% of
/// the entry and the numbers are bandwidth, not per-entry cost.
pub const BIG_PAYLOAD: usize = 12 * 1024;

/// Entries in the small-record cases. Sized to stay inside a 16 MiB L3, so the
/// measurement is per-entry work rather than memory bandwidth.
pub const SMALL_N: u64 = 100_000;

/// Entries in the large-record cases. 400 was too few to resolve a few percent —
/// run-to-run swings of ±0.8 ms swamped the difference.
pub const LARGE_N: u64 = 20_000;

pub const WARM_UP: Duration = Duration::from_secs(3);
pub const MEASUREMENT: Duration = Duration::from_secs(10);

/// Cap on enqueued-but-unwritten bytes during a build. Large enough that the
/// writer still batches — a small window paces the producer off its flush timer
/// and costs orders of magnitude — and small enough to bound a build far larger
/// than RAM.
pub const BUILD_IN_FLIGHT_BYTES: u64 = 256 * 1024 * 1024;
pub const CHECK_EVERY: u64 = 1024;

/// Current on-disk length of a WAL file, or 0 before it exists.
pub fn file_len(path: &Path) -> u64 {
    std::fs::metadata(path).map(|m| m.len()).unwrap_or(0)
}

/// Bytes under `dir` — the size actually produced, not the size asked for.
pub fn dir_size(dir: &Path) -> u64 {
    let mut total = 0;
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            match entry.file_type() {
                Ok(t) if t.is_dir() => total += dir_size(&entry.path()),
                Ok(_) => total += entry.metadata().map(|m| m.len()).unwrap_or(0),
                Err(_) => {}
            }
        }
    }
    total
}

/// Physical memory, which decides how big a file has to be before reading it
/// actually reaches the disk. Falls back to 8 GiB where it cannot be read; that
/// only makes an uncached case smaller, and the case prints the size it used.
pub fn total_ram_bytes() -> u64 {
    #[cfg(target_os = "linux")]
    {
        let mem_total = std::fs::read_to_string("/proc/meminfo")
            .ok()
            .and_then(|text| {
                text.lines().find_map(|line| {
                    line.strip_prefix("MemTotal:")?
                        .split_whitespace()
                        .next()?
                        .parse::<u64>()
                        .ok()
                })
            });
        if let Some(kb) = mem_total {
            return kb * 1024;
        }
    }

    #[cfg(target_os = "macos")]
    {
        let mem_size = std::process::Command::new("sysctl")
            .args(["-n", "hw.memsize"])
            .output()
            .ok()
            .and_then(|out| {
                String::from_utf8_lossy(&out.stdout)
                    .trim()
                    .parse::<u64>()
                    .ok()
            });
        if let Some(bytes) = mem_size {
            return bytes;
        }
    }

    8 << 30
}

/// Free bytes on the filesystem holding `path`. `df -Pk` is POSIX output on both
/// platforms; a parse failure yields `None` rather than a guess.
pub fn free_bytes(path: &Path) -> Option<u64> {
    let out = std::process::Command::new("df")
        .arg("-Pk")
        .arg(path)
        .output()
        .ok()?;
    let text = String::from_utf8_lossy(&out.stdout);
    let available = text.lines().nth(1)?.split_whitespace().nth(3)?;
    available.parse::<u64>().ok().map(|kb| kb * 1024)
}

/// Size for a case that must not fit in the page cache: twice RAM, held to half
/// the free space so the run cannot fill the disk. This is the one quantity no
/// constant can express, since the right value is a property of the machine.
pub fn uncached_bytes(dir: &Path) -> u64 {
    let want = total_ram_bytes().saturating_mul(2);
    match free_bytes(dir) {
        Some(free) => want.min(free / 2),
        None => want,
    }
}

/// Criterion flags that take a separate value, so the value is not mistaken for
/// a case name in [`uncached_requested`].
const VALUED_FLAGS: &[&str] = &[
    "--baseline",
    "--save-baseline",
    "--load-baseline",
    "--sample-size",
    "--measurement-time",
    "--warm-up-time",
    "--profile-time",
    "--nresamples",
    "--noise-threshold",
    "--confidence-level",
    "--significance-level",
    "--output-format",
    "--plotting-backend",
    "--filter",
    "--color",
];

/// Whether the command line asked for the uncached case, which has to write
/// twice this machine's RAM to disk and so is opt-in rather than default. That
/// makes the filter the switch, and no environment variable is needed.
///
/// The word has to appear in the filter itself rather than the filter merely
/// matching the case name: `-- large` selects `large_cached`, and it must not
/// also start a multi-hundred-gigabyte build nobody asked for.
pub fn uncached_requested() -> bool {
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if VALUED_FLAGS.contains(&arg.as_str()) {
            args.next();
        } else if !arg.starts_with('-') && arg.contains("uncached") {
            return true;
        }
    }
    false
}

/// Write `n` entries of `payload` bytes into a single WAL file under `root`,
/// then return a store that can read it back. `name` separates one benchmark's
/// queue from another's. The written/completed receivers are dropped: the read
/// path does not use them.
pub async fn build_file(
    root: &Path,
    name: &str,
    n: u64,
    payload: usize,
) -> (WalStore, QueueId, UintN) {
    let (written_tx, _written_rx) = tokio::sync::mpsc::unbounded_channel();
    let (complete_tx, _complete_rx) = tokio::sync::mpsc::unbounded_channel();
    let store = WalStore::new(root, written_tx, complete_tx);

    let queue_id = QueueIdResolver::new("bench").resolve(name);
    let file_id = UintN::from(1u64);
    let settings = WalSettings {
        max_file_size: 1 << 40, // never rotate: a case must be one file
        write_buffer_size: 8 * 1024 * 1024,
        enable_fsync: false,
        ..Default::default()
    };
    store
        .start_writer(&queue_id, &file_id, WalHeader::default(), settings, None)
        .await
        .unwrap();

    // One shared payload: `Bytes::clone` is a refcount bump, so anything the
    // build retains is the WAL's, not the records'.
    let record = Bytes::from(vec![0xABu8; payload]);
    let wal_path = file_id.to_file_path(queue_id.to_wal_dir(root).to_str().unwrap(), "wal");
    for i in 0..n {
        store
            .enqueue(&queue_id, UintN::from(i), record.clone())
            .unwrap();

        // `enqueue` is synchronous and hands off to a writer task, so an
        // unthrottled loop produces at memory speed while the writer drains at
        // disk speed and the gap stays on the heap. Gating on bytes landed holds
        // it flat.
        if i % CHECK_EVERY == 0 {
            let enqueued = (i + 1) * payload as u64;
            let mut stalled = 0u32;
            while enqueued.saturating_sub(file_len(&wal_path)) > BUILD_IN_FLIGHT_BYTES {
                tokio::time::sleep(Duration::from_millis(2)).await;
                stalled += 1;
                assert!(stalled < 60_000, "writer made no progress for 120 s at {i}");
            }
        }
    }
    store.close().await.unwrap();

    (store, queue_id, file_id)
}
