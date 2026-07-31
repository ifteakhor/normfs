//! Write and scan throughput across record sizes, in records per second.
//!
//! The one benchmark meant to be run against **two revisions of the tree** and
//! compared, which is why it is deliberately plain: no criterion, no shared
//! module, and nothing outside the WAL API that `v0.1.0-beta.1` also has. A
//! plain `fn main` is both a `harness = false` bench here and an example in an
//! older checkout, so the same source measures both releases:
//!
//!   scripts/bench-baseline.sh          # 0.1 and this tree, one after the other
//!   cargo bench -p normfs-wal --bench wal_sweep    # this tree only
//!
//! Records per second, not bytes per second, is the figure being compared. The
//! whole point of the entry format is what it costs to put *one small record*
//! on disk, and a byte rate hides that behind the record size.
//!
//! Sizes are swept rather than fixed because the answer changes shape across
//! them: framing is a fifth of an 80 B entry and a rounding error on a 12 KiB
//! one. Each size gets its own entry count — a fixed byte budget would mean
//! billions of records at the small end — so the dataset size is reported per
//! row rather than implied.
//!
//! Every point is measured [`PASSES`] times and reported as a median with the
//! slowest-over-fastest spread beside it. One pass is not a result here: on a
//! laptop the same point moves by a third between runs, and a single number
//! invites a ratio to be quoted that the next run will not reproduce.

use std::path::Path;
use std::time::{Duration, Instant};

use bytes::Bytes;
use normfs_types::{QueueId, QueueIdResolver};
use normfs_wal::{WalHeader, WalSettings, WalStore};
use tokio::sync::mpsc;
use uintn::UintN;

/// Record sizes to sweep: a minimal record, the sensor messages the WAL is
/// for, the band where checksum implementations trade places, and the 12 KiB
/// block the benchmarks used to default to.
const SIZES: [usize; 7] = [16, 64, 80, 256, 1024, 4096, 12 * 1024];

/// Entries per size, capped two ways: enough records for the rate to mean
/// something, and not more bytes than a run should write for one point.
const MAX_RECORDS: u64 = 20_000_000;
const MAX_BYTES: u64 = 4 << 30;

/// Repeats per point. Each pass rebuilds the dataset, so this measures the
/// variance that matters — whole runs — rather than re-reading one warm file.
const PASSES: usize = 3;

/// Cap on enqueued-but-unwritten bytes. `enqueue` is synchronous and hands off
/// to a writer task, so an unthrottled loop produces at memory speed while the
/// writer drains at disk speed and the gap stays on the heap.
const BUILD_IN_FLIGHT_BYTES: u64 = 256 * 1024 * 1024;
const CHECK_EVERY: u64 = 1024;

fn file_len(path: &Path) -> u64 {
    std::fs::metadata(path).map(|m| m.len()).unwrap_or(0)
}

fn settings() -> WalSettings {
    // The trailing `..Default::default()` is redundant against today's
    // WalSettings and kept on purpose: this file is compiled against other
    // revisions of the struct, and a field added on either side must not stop
    // the comparison from building.
    #[allow(clippy::needless_update)]
    WalSettings {
        // Never rotate: one file per size keeps the scan a single-file scan.
        max_file_size: 1 << 40,
        write_buffer_size: 8 * 1024 * 1024,
        enable_fsync: false,
        // Off on purpose. With them on this measures a compressor over a
        // synthetic block rather than the WAL path.
        encryption_type: normfs_types::EncryptionType::None,
        compression_type: normfs_types::CompressionType::None,
        ..Default::default()
    }
}

struct Row {
    bytes: u64,
    write_secs: f64,
    scan_secs: f64,
}

/// Median, and slowest over fastest. Rates are derived from the median, so the
/// spread is what says whether the leading digits mean anything.
fn median_and_spread(v: &[f64]) -> (f64, f64) {
    let mut s = v.to_vec();
    s.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let spread = if s[0] > 0.0 {
        s[s.len() - 1] / s[0]
    } else {
        1.0
    };
    (s[s.len() / 2], spread)
}

async fn measure(root: &Path, payload: usize, records: u64) -> Row {
    let (written_tx, _written_rx) = mpsc::unbounded_channel();
    let (complete_tx, _complete_rx) = mpsc::unbounded_channel();
    let store = WalStore::new(root, written_tx, complete_tx);

    let queue_id: QueueId = QueueIdResolver::new("bench").resolve("sweep");
    let file_id = UintN::from(1u64);
    store
        .start_writer(&queue_id, &file_id, WalHeader::default(), settings(), None)
        .await
        .unwrap();

    let record = Bytes::from(vec![0xABu8; payload]);
    let wal_path = file_id.to_file_path(queue_id.to_wal_dir(root).to_str().unwrap(), "wal");

    // Timed through `close`: `enqueue` only hands off, so the tail reaches disk
    // during the close and timing the loop alone would report memory speed.
    let start = Instant::now();
    for i in 0..records {
        store
            .enqueue(&queue_id, UintN::from(i), record.clone())
            .unwrap();

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
    let write_secs = start.elapsed().as_secs_f64();

    // Reads and verifies every entry to find the file's last id — the call
    // recovery makes on the way in.
    let start = Instant::now();
    let end = store.get_file_end(&queue_id, &file_id).await.unwrap();
    let scan_secs = start.elapsed().as_secs_f64();
    assert_eq!(
        end,
        Some(UintN::from(records - 1)),
        "{payload} B scan should reach the last entry"
    );

    Row {
        bytes: file_len(&wal_path),
        write_secs,
        scan_secs,
    }
}

#[tokio::main]
async fn main() {
    println!(
        "normfs-wal {} — write and scan by record size\n",
        env!("CARGO_PKG_VERSION")
    );
    println!("median of {PASSES} passes, spread is slowest over fastest\n");
    println!(
        "{:>8} | {:>10} | {:>8} | {:>20} | {:>20} | {:>9}",
        "payload", "records", "on disk", "write M rec/s", "scan M rec/s", "bytes/rec"
    );
    println!(
        "{:->8}-+-{:->10}-+-{:->8}-+-{:->20}-+-{:->20}-+-{:->9}",
        "", "", "", "", "", ""
    );

    for payload in SIZES {
        let records = MAX_RECORDS.min(MAX_BYTES / payload as u64);
        let mut writes = Vec::with_capacity(PASSES);
        let mut scans = Vec::with_capacity(PASSES);
        let mut bytes = 0;

        for _ in 0..PASSES {
            // A fresh directory per pass: the scan must see one file, not a
            // queue that grew, and the build has to be a build.
            let tmp = tempfile::tempdir().unwrap();
            let row = measure(tmp.path(), payload, records).await;
            writes.push(row.write_secs);
            scans.push(row.scan_secs);
            bytes = row.bytes;
            drop(tmp);
        }

        let (write_med, write_spread) = median_and_spread(&writes);
        let (scan_med, scan_spread) = median_and_spread(&scans);
        println!(
            "{:>8} | {:>10} | {:>7.2}G | {:>13.2} {:>5} | {:>13.2} {:>5} | {:>9.1}",
            payload,
            records,
            bytes as f64 / (1u64 << 30) as f64,
            records as f64 / write_med / 1e6,
            format!("{:.2}x", write_spread),
            records as f64 / scan_med / 1e6,
            format!("{:.2}x", scan_spread),
            bytes as f64 / records as f64
        );
    }
}
