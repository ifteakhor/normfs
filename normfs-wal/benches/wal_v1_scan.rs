//! Recovery / scan-time benchmark (metric e).
//!
//! Builds one V1 WAL file per case, then times `WalStore::get_file_end` — the
//! public call recovery uses to find a file's last entry id. It reads the
//! header and scans every entry, verifying each CRC32C and deriving the id
//! positionally. Throughput is reported in entries scanned per second.
//!
//! Warmup / measurement default to 5 s / 30 s, overridable (seconds) with
//! WAL_BENCH_WARMUP / WAL_BENCH_MEASURE.
//!
//! Other knobs:
//!   WAL_BENCH_SCAN_GIB   size of the uncached case, in GiB (default 2)
//!
//!   cargo bench -p normfs-wal --bench wal_v1_scan

use std::hint::black_box;
use std::path::Path;
use std::time::{Duration, Instant};

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
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

/// Entries in the large cached case. 400 was too few to resolve a few percent —
/// run-to-run swings of ±0.8 ms swamped the difference.
const LARGE_CACHED_N: u64 = 20_000;

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
    // 28 B is the widest framing the entry sizes here have used, so the file
    // never exceeds the requested size.
    let big_n = (scan_gib << 30) / (big_payload as u64 + 28);

    eprintln!("[bench] scan_gib={scan_gib}");

    let cases = [
        ("small_cached", 100_000u64, 64usize),
        ("large_cached", LARGE_CACHED_N, big_payload),
        ("large_uncached", big_n, big_payload),
    ];

    for (case, n, payload) in cases {
        g.throughput(Throughput::Elements(n));
        // A multi-GiB scan takes seconds per iteration; 10 is criterion's floor.
        g.sample_size(if n == big_n { 10 } else { 20 });

        let tmp = tempfile::tempdir().unwrap();
        let (store, queue_id, file_id) =
            rt.block_on(build_file(tmp.path(), WalEntryFormat::V1, n, payload));
        eprintln!(
            "[bench] {case}: {n} entries, {:.2} GiB on disk",
            dir_size(tmp.path()) as f64 / (1u64 << 30) as f64
        );

        // Sanity: the scan finds the last entry id.
        let end = rt
            .block_on(store.get_file_end(&queue_id, &file_id))
            .unwrap();
        assert_eq!(
            end,
            Some(UintN::from(n - 1)),
            "{case} scan should reach the last entry"
        );

        g.bench_function(BenchmarkId::new("get_file_end", case), |b| {
            b.iter_custom(|iters| {
                rt.block_on(async {
                    let start = Instant::now();
                    for _ in 0..iters {
                        black_box(store.get_file_end(&queue_id, &file_id).await.unwrap());
                    }
                    start.elapsed()
                })
            });
        });

        drop(store);
        drop(tmp);
    }
    g.finish();
}

criterion_group!(benches, bench_recovery_scan);
criterion_main!(benches);
