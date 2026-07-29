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
//!   cargo bench -p normfs-wal --bench wal_v1_scan

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
    for i in 0..n {
        store.enqueue(&queue_id, UintN::from(i), record.clone()).unwrap();
    }
    store.close().await.unwrap();

    (store, queue_id, file_id)
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

    let cases = [
        ("small_cached", 100_000u64, 64usize),
        ("large_cached", 400u64, big_payload),
        ("large_uncached", big_n, big_payload),
    ];

    for (case, n, payload) in cases {
        g.throughput(Throughput::Elements(n));
        // A multi-GiB scan takes seconds per iteration; 10 is criterion's floor.
        g.sample_size(if n == big_n { 10 } else { 20 });

        for (label, format) in [
            ("v0", WalEntryFormat::V0),
            ("v1", WalEntryFormat::V1),
        ] {
            let tmp = tempfile::tempdir().unwrap();
            let (store, queue_id, file_id) =
                rt.block_on(build_file(tmp.path(), format, n, payload));

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
