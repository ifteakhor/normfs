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
//! The dataset is the same size on every machine: at most 4 GiB per point and
//! at most 20 million records, whichever binds first. It used to be derived
//! from free space, so that a board with a small card could run it unchanged,
//! and that was a mistake. On a Raspberry Pi, where `/tmp` is a tmpfs and free
//! space is therefore RAM, it shrank the dataset twelvefold and measured a scan
//! over 113 ms — while printing a row that looked like every other row. A run
//! that cannot fit the dataset now stops and says why.
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
//!
//! **Points measured in one process are not independent.** Each one writes and
//! deletes gigabytes, and a large-record write is bandwidth-bound, so it reads
//! whatever writeback backlog the points before it left behind. Measured last
//! after six other sizes, the 12 KiB write came out 24 % below the same point
//! measured first — and worse for the *faster* build, which reaches the late
//! points sooner and gives writeback less time to drain. Sizes are therefore
//! taken one per process: pass the record sizes as arguments and the run does
//! only those.
//!
//!   cargo bench -p normfs-wal --bench wal_sweep -- 12288
//!
//! With no arguments it sweeps every size in one process, which is fine for a
//! quick look and not fine for a number anyone will quote.

use std::path::Path;
use std::time::{Duration, Instant};

use bytes::Bytes;
use normfs_types::{QueueId, QueueIdResolver};
use normfs_wal::{WalHeader, WalSettings, WalStore};
use tokio::sync::mpsc;
use uintn::UintN;

/// Record sizes swept when none are named: a minimal record, the sensor
/// messages the WAL is for, the band where checksum implementations trade
/// places, and a large block.
const SIZES: [usize; 7] = [16, 64, 80, 256, 1024, 4096, 12 * 1024];

/// Sizes to measure: the ones named on the command line, or all of them.
fn requested_sizes() -> Vec<usize> {
    let named: Vec<usize> = std::env::args()
        .skip(1)
        .filter_map(|a| a.parse().ok())
        .collect();
    if named.is_empty() {
        SIZES.to_vec()
    } else {
        named
    }
}

/// Entries per size, capped two ways: enough records for the rate to mean
/// something, and not more bytes than the machine should write for one point.
///
/// The record cap is what keeps the small sizes finite — at 16 B the byte
/// budget alone would ask for 1.3 billion entries — so 16 B and 64 B write less
/// than the full budget and the table prints what each row actually wrote.
const MAX_RECORDS: u64 = 250_000_000;

/// Most a point may write, and the free space a run needs before it starts.
///
/// Fixed, not derived from the machine. This benchmark exists to compare two
/// revisions and two machines, and a dataset that quietly resizes itself to fit
/// the disk invalidates both comparisons while still printing a plausible row.
/// The headroom above the budget covers framing, which pushes the file a couple
/// of percent past the payload total; passes run one at a time and delete their
/// directory, so only one dataset is on disk at once.
/// 20 GiB is chosen to be larger than the RAM of the machines this runs on, so
/// the scan reads a file the page cache could not have kept whole. At 4 GiB it
/// read back out of memory on every machine tested and reported that as storage.
/// The required-free figure covers the previous release rather than this one:
/// 0.1 frames each entry in 28 bytes against V1's 5, so it writes about a
/// quarter more for the same records.
const MAX_BYTES: u64 = 20 << 30;
const REQUIRED_FREE: u64 = 30 << 30;

/// Free bytes where the datasets go. `df -Pk` is POSIX output everywhere this
/// runs; a parse failure yields `None` rather than a guess.
fn free_bytes(path: &Path) -> Option<u64> {
    let out = std::process::Command::new("df")
        .arg("-Pk")
        .arg(path)
        .output()
        .ok()?;
    let text = String::from_utf8_lossy(&out.stdout);
    let available = text.lines().nth(1)?.split_whitespace().nth(3)?;
    available.parse::<u64>().ok().map(|kb| kb * 1024)
}

/// Stops the run when the datasets will not fit, rather than measuring a
/// smaller one. The default temporary directory is a tmpfs on several
/// distributions — Debian 13 among them — where free space is RAM and a
/// benchmark that adapts to it silently stops measuring storage at all.
///
/// A failed check is reported and not fatal: not knowing the free space is a
/// reason to warn, not to refuse.
fn require_space(dir: &Path) {
    let Some(free) = free_bytes(dir) else {
        eprintln!(
            "warning: could not read free space at {}; running anyway",
            dir.display()
        );
        return;
    };
    if free >= REQUIRED_FREE {
        return;
    }
    let gib = |b: u64| b as f64 / (1u64 << 30) as f64;
    eprintln!(
        "This benchmark needs {:.1} GiB free at {}, and found {:.1} GiB.",
        gib(REQUIRED_FREE),
        dir.display(),
        gib(free)
    );
    eprintln!();
    eprintln!("That path is a tmpfs on many systems, in which case it is backed by RAM");
    eprintln!("and the result would describe memory rather than storage. Point TMPDIR at");
    eprintln!("real storage and run again:");
    eprintln!();
    eprintln!("    TMPDIR=/path/on/disk {}", program_name());
    std::process::exit(1);
}

fn program_name() -> String {
    std::env::args()
        .next()
        .unwrap_or_else(|| "wal_sweep".to_string())
}

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
    // Kept and drained: these acks are the writer's progress, and the build
    // loop throttles on them.
    let (written_tx, mut written_rx) = mpsc::unbounded_channel();
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
    // Entries the producer may be ahead by. Capped in records as well as bytes:
    // 256 MiB of 16 B records is sixteen million queue entries, and their
    // per-item overhead is the cost, not the payload they point at.
    let max_in_flight = (BUILD_IN_FLIGHT_BYTES / payload as u64).clamp(1024, 2_000_000);
    let mut acked: u64 = 0;

    let start = Instant::now();
    for i in 0..records {
        store
            .enqueue(&queue_id, UintN::from(i), record.clone())
            .unwrap();

        if i % CHECK_EVERY == 0 {
            let mut stalled = 0u32;
            loop {
                // The writer acks the last entry of every flush, which is its
                // progress counted in records — the same unit the producer
                // counts in. Comparing payload bytes against the file's length
                // instead, as this once did, compares different units: framing
                // makes the file grow faster than the payload total, so the
                // difference saturated at zero and the throttle never engaged
                // until the writer was a fixed *fraction* of the run behind.
                // At 0.1's 28 B framing on an 80 B record that fraction is 26 %
                // — seven gigabytes of queue at this dataset size, which is
                // what the OOM killer ended on an 8 GB board.
                while let Ok((_, id)) = written_rx.try_recv() {
                    if let Ok(n) = id.to_u64() {
                        acked = acked.max(n);
                    }
                }
                if i.saturating_sub(acked) <= max_in_flight {
                    break;
                }
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
    require_space(&std::env::temp_dir());

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

    let budget = MAX_BYTES;
    println!(
        "up to {:.2} GiB and {} records per point, {} passes each\n",
        budget as f64 / (1u64 << 30) as f64,
        MAX_RECORDS,
        PASSES
    );

    let sizes = requested_sizes();
    if sizes.len() > 1 {
        println!("warning: {} sizes in one process — later points inherit the writeback backlog of earlier ones. Pass one size per run for numbers to quote.\n", sizes.len());
    }

    for payload in sizes {
        let records = MAX_RECORDS.min(budget / payload as u64).max(1);
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
