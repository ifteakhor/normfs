//! Recovery / scan-time benchmark (metric e), V0 vs V1.
//!
//! Builds one WAL file per case and format, then times
//! `WalStore::get_file_end` — the public call recovery uses to find a file's
//! last entry id. It reads the header and scans every entry, verifying each
//! entry's checksum (xxHash64 for V0, CRC32C for V1) and, for V1, deriving the
//! id positionally. Throughput is reported in entries scanned per second.
//!
//! Warmup / measurement default to 5 s / 30 s, overridable (seconds) with
//! WAL_BENCH_WARMUP / WAL_BENCH_MEASURE.
//!
//! Other knobs:
//!   WAL_BENCH_SCAN_GIB   size of the uncached case, in GiB (default 2)
//!   WAL_BENCH_CACHED_N   entries in the large cached case (default 400)
//!   WAL_BENCH_ORDER      v0v1 (default) or v1v0 — which format is built and
//!                        scanned first; run both to tell a format difference
//!                        apart from a running-order one
//!
//!   cargo bench -p normfs-wal --bench wal_v1_scan
//!   WAL_BENCH_ORDER=v1v0 cargo bench -p normfs-wal --bench wal_v1_scan

use std::hint::black_box;
use std::path::Path;
use std::time::{Duration, Instant};

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use normfs_types::{QueueId, QueueIdResolver};
use normfs_wal::{WalEntryFormat, WalHeader, WalSettings, WalStore};
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use uintn::UintN;

fn env_secs(var: &str, default: u64) -> Duration {
    Duration::from_secs(
        std::env::var(var)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(default),
    )
}
fn warm() -> Duration {
    env_secs("WAL_BENCH_WARMUP", 5)
}
fn meas() -> Duration {
    env_secs("WAL_BENCH_MEASURE", 30)
}

/// Cap on enqueued-but-unwritten bytes. Large enough that the writer still
/// batches — a small window paces the producer off its flush timer and costs
/// orders of magnitude — and small enough to bound a build larger than RAM.
const BUILD_IN_FLIGHT_BYTES: u64 = 256 * 1024 * 1024;
const CHECK_EVERY: u64 = 1024;

/// Current on-disk length of the WAL file, or 0 before it exists.
fn file_len(path: &Path) -> u64 {
    std::fs::metadata(path).map(|m| m.len()).unwrap_or(0)
}

/// Write `n` entries of `payload` bytes to a single WAL file in `format`, then
/// return a store that can read it back. The senders' receivers are dropped
/// once writing is done; the read path does not use them.
async fn build_file(
    root: &Path,
    format: WalEntryFormat,
    n: u64,
    payload: usize,
) -> (WalStore, QueueId, UintN) {
    let (written_tx, _written_rx) = mpsc::unbounded_channel();
    let (complete_tx, _complete_rx) = mpsc::unbounded_channel();
    let store = WalStore::new(root, written_tx, complete_tx);

    let queue_id = QueueIdResolver::new("bench").resolve("scan");
    let file_id = UintN::from(1u64);
    let settings = WalSettings {
        max_file_size: 1 << 40, // 1 TiB: never rotate, the scan must be one file
        write_buffer_size: 8 * 1024 * 1024,
        enable_fsync: false,
        wal_entry_format: format,
        ..Default::default()
    };
    store
        .start_writer(&queue_id, &file_id, WalHeader::default(), settings, None)
        .await
        .unwrap();

    let record = Bytes::from(vec![0xABu8; payload]);
    let wal_path = file_id.to_file_path(queue_id.to_wal_dir(root).to_str().unwrap(), "wal");
    for i in 0..n {
        store
            .enqueue(&queue_id, UintN::from(i), record.clone())
            .unwrap();

        // `enqueue` is synchronous and hands off to a writer task, so an
        // unthrottled loop produces at memory speed while the writer drains at
        // disk speed and the gap stays on the heap — otherwise ~60% of the
        // dataset, which puts a 100 GiB build out of reach. Gating on bytes
        // landed holds it flat at ~1 GiB.
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

/// Total bytes under `dir` — the size actually scanned, not the size asked for.
fn dir_size(dir: &Path) -> u64 {
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

fn bench_recovery_scan(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut g = c.benchmark_group("recovery_scan");
    g.warm_up_time(warm());
    g.measurement_time(meas());
    g.sample_size(20);

    // Two axes decide whether the V1 checksum's chunked fast path matters, and
    // one case cannot cover both:
    //
    //   * entry size — the chunked path needs 768 bytes in one call, so a 64 B
    //     record is checksummed by the serial tail no matter how big the file
    //     is. That case measures framing and iteration, not the fast path.
    //   * working set — three interleaved cursors beat one chain while the data
    //     is in cache and lose to it once the scan is pulling from RAM.
    //
    // The two "cached" cases are sized against a 16 MiB L3 and stay inside it.
    // The uncached one is sized in GiB, not entries: at 147 MB it sat in the
    // page cache and every iteration after the first measured a memory scan.
    let scan_gib: u64 = std::env::var("WAL_BENCH_SCAN_GIB")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2);
    let big_payload = 12 * 1024usize;
    // Sized against V0's wider 28-byte framing, so neither format exceeds it.
    let big_n = (scan_gib << 30) / (big_payload as u64 + 28);

    let large_cached_n: u64 = std::env::var("WAL_BENCH_CACHED_N")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(400);

    // Datasets are built one format after the other, so the second inherits the
    // first's free-space layout and a warmer drive — worth a few percent on a
    // multi-GiB scan, enough to read as a format difference. Run both orders.
    //
    // An unrecognised value aborts: a silent default would hand back a V0-first
    // run labelled as the swap, and these runs take hours.
    let order = std::env::var("WAL_BENCH_ORDER").unwrap_or_else(|_| "v0v1".to_string());
    let formats = match order.replace(['-', '_', ','], "").as_str() {
        "v0v1" => [("v0", WalEntryFormat::V0), ("v1", WalEntryFormat::V1)],
        "v1v0" => [("v1", WalEntryFormat::V1), ("v0", WalEntryFormat::V0)],
        other => panic!("WAL_BENCH_ORDER must be v0v1 or v1v0, got {other:?}"),
    };
    eprintln!(
        "[bench] order={}, scan_gib={scan_gib}, cached_n={large_cached_n}",
        formats.map(|(l, _)| l).join("->")
    );

    let cases = [
        ("small_cached", 100_000u64, 64usize),
        ("large_cached", large_cached_n, big_payload),
        ("large_uncached", big_n, big_payload),
    ];

    for (case, n, payload) in cases {
        g.throughput(Throughput::Elements(n));
        // A multi-GiB scan takes seconds per iteration; 10 is criterion's floor.
        g.sample_size(if n == big_n { 10 } else { 20 });

        for (label, format) in formats {
            let tmp = tempfile::tempdir().unwrap();
            let (store, queue_id, file_id) =
                rt.block_on(build_file(tmp.path(), format, n, payload));
            eprintln!(
                "[bench] {case}/{label}: {n} entries, {:.2} GiB on disk",
                dir_size(tmp.path()) as f64 / (1u64 << 30) as f64
            );

            // Sanity: the scan finds the last entry id (= n - 1 for both formats).
            let end = rt
                .block_on(store.get_file_end(&queue_id, &file_id))
                .unwrap();
            assert_eq!(
                end,
                Some(UintN::from(n - 1)),
                "{case}/{label} scan should reach the last entry"
            );

            g.bench_function(
                BenchmarkId::new("get_file_end", format!("{case}/{label}")),
                |b| {
                    b.iter_custom(|iters| {
                        rt.block_on(async {
                            let start = Instant::now();
                            for _ in 0..iters {
                                black_box(
                                    store.get_file_end(&queue_id, &file_id).await.unwrap(),
                                );
                            }
                            start.elapsed()
                        })
                    });
                },
            );

            drop(store);
            drop(tmp);
        }
    }
    g.finish();
}

criterion_group!(benches, bench_recovery_scan);
criterion_main!(benches);
