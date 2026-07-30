//! Recovery / scan-time benchmark (metric e).
//!
//! Builds one WAL file per case, then times `WalStore::get_file_end` — the
//! public call recovery uses to find a file's last entry id. It reads the header
//! and scans every entry, verifying each CRC32C and deriving the id positionally.
//! Throughput is reported in entries scanned per second.
//!
//!   cargo bench -p normfs-wal --bench wal_v1_scan
//!   cargo bench -p normfs-wal --bench wal_v1_scan -- uncached
//!
//! The second form adds the uncached case, which writes twice this machine's RAM
//! to disk — too costly to run unasked, so it is opt-in rather than the default.

mod common;

use std::hint::black_box;
use std::time::Instant;

use common::{
    build_file, dir_size, uncached_bytes, uncached_requested, BIG_PAYLOAD, LARGE_N, MEASUREMENT,
    PAYLOAD, SMALL_N, WARM_UP,
};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use tokio::runtime::Runtime;
use uintn::UintN;

/// Widest V1 framing: a 5 B record-size varint plus a 4 B CRC32C. Sizing an
/// uncached case with it keeps the file at or under the size asked for.
const MAX_FRAMING: u64 = 9;

fn bench_recovery_scan(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut g = c.benchmark_group("recovery_scan");
    g.warm_up_time(WARM_UP);
    g.measurement_time(MEASUREMENT);

    // Two axes decide what a scan costs, and one case cannot cover both:
    //
    //   * entry size — at 80 B the scan is per-entry work, framing and checksum
    //     setup; the CRC32C chunked path needs 768 bytes in one call, so a small
    //     record is checksummed by the serial tail no matter how big the file is.
    //   * working set — the cached cases stay inside a 16 MiB L3 and measure the
    //     CPU; only a file larger than RAM measures the read path against disk.
    let mut cases = vec![
        ("small_cached", SMALL_N, PAYLOAD),
        ("large_cached", LARGE_N, BIG_PAYLOAD),
    ];

    // Sized against RAM rather than by entry count: at 147 MB the file sat in
    // the page cache and every iteration after the first measured a memory scan.
    if uncached_requested() {
        let tmp = std::env::temp_dir();
        let bytes = uncached_bytes(&tmp);
        eprintln!(
            "[bench] large_uncached: sizing to {:.1} GiB against this machine's RAM",
            bytes as f64 / (1u64 << 30) as f64
        );
        cases.push((
            "large_uncached",
            bytes / (BIG_PAYLOAD as u64 + MAX_FRAMING),
            BIG_PAYLOAD,
        ));
    } else {
        eprintln!("[bench] skipping large_uncached; add `-- uncached` to include it");
    }

    for (case, n, payload) in cases {
        g.throughput(Throughput::Elements(n));
        // A multi-GiB scan takes seconds per iteration; 10 is criterion's floor.
        g.sample_size(if case == "large_uncached" { 10 } else { 20 });

        let tmp = tempfile::tempdir().unwrap();
        let (store, queue_id, file_id) = rt.block_on(build_file(tmp.path(), "scan", n, payload));
        eprintln!(
            "[bench] {case}: {n} entries x {payload} B, {:.2} GiB on disk",
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
