//! Full-range iteration throughput at step 1 and step 2 (metric h).
//!
//! Times `WalStore::read_wal_range` over every entry of a file — the path a
//! subscriber or replay uses, as opposed to `wal_scan`'s `get_file_end`,
//! which only needs the last id. Entries are delivered over a bounded channel to
//! a draining consumer, so the measurement includes the per-entry record copy and
//! channel hand-off that a real reader pays.
//!
//! `step` filters *after* the entry has been framed and checksum-verified: a
//! variable-length frame gives no way to find entry i+2 without decoding i+1. So
//! step 2 halves the records delivered but not the bytes read, and it adds a
//! `UintN` subtract-and-modulo per entry. Whether it is faster at all is the
//! point of measuring it.
//!
//! Both steps run back to back over the same file, so that comparison carries no
//! build-order effect.
//!
//!   cargo bench -p normfs-wal --bench wal_range
//!   cargo bench -p normfs-wal --bench wal_range -- uncached
//!
//! The second form adds the uncached case, which writes twice this machine's RAM
//! to disk — too costly to run unasked, so it is opt-in rather than the default.

mod common;

use std::time::Instant;

use common::{
    build_file, file_len, uncached_bytes, uncached_requested, BIG_PAYLOAD, LARGE_N, MEASUREMENT,
    PAYLOAD, SMALL_N, WARM_UP,
};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use normfs_types::{DataSource, QueueId, ReadEntry};
use normfs_wal::WalStore;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use uintn::UintN;

/// Large enough that the consumer is not the bottleneck, small enough to stay a
/// real hand-off.
const CHANNEL_CAPACITY: usize = 1024;

/// Widest V1 framing: a 5 B record-size varint plus a 4 B CRC32C.
const MAX_FRAMING: u64 = 9;

/// Read every entry from id 0 at `step`, draining on a separate task.
async fn read_all(store: &WalStore, queue_id: &QueueId, file_id: &UintN, step: usize) -> u64 {
    let (tx, mut rx) = mpsc::channel::<ReadEntry>(CHANNEL_CAPACITY);
    let drain = tokio::spawn(async move {
        let mut count = 0u64;
        while rx.recv().await.is_some() {
            count += 1;
        }
        count
    });

    store
        .read_wal_range(
            queue_id,
            file_id,
            &UintN::from(0u64),
            &None,
            step,
            &tx,
            DataSource::DiskWal,
        )
        .await
        .unwrap();

    // The reader holds its own sender; dropping ours ends the drain loop.
    drop(tx);
    drain.await.unwrap()
}

fn bench_range(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut g = c.benchmark_group("range_read");
    g.warm_up_time(WARM_UP);
    g.measurement_time(MEASUREMENT);

    let mut cases = vec![("small", SMALL_N, PAYLOAD), ("large", LARGE_N, BIG_PAYLOAD)];

    // Sized against RAM rather than by entry count: the case only means anything
    // if the file cannot fit in the page cache.
    if uncached_requested() {
        let bytes = uncached_bytes(&std::env::temp_dir());
        eprintln!(
            "[bench] uncached: sizing to {:.1} GiB against this machine's RAM",
            bytes as f64 / (1u64 << 30) as f64
        );
        cases.push((
            "uncached",
            bytes / (BIG_PAYLOAD as u64 + MAX_FRAMING),
            BIG_PAYLOAD,
        ));
    } else {
        eprintln!("[bench] skipping uncached; add `-- uncached` to include it");
    }

    for (case, n, payload) in cases {
        // A multi-GiB read takes seconds per iteration; 10 is criterion's floor.
        g.sample_size(if case == "uncached" { 10 } else { 20 });
        let tmp = tempfile::tempdir().unwrap();
        let (store, queue_id, file_id) = rt.block_on(build_file(tmp.path(), "range", n, payload));
        let wal_path =
            file_id.to_file_path(queue_id.to_wal_dir(tmp.path()).to_str().unwrap(), "wal");

        for step in [1usize, 2] {
            // Every entry is framed and verified regardless of step, so
            // throughput is quoted over entries *scanned*, not delivered —
            // otherwise step 2 would look twice as fast for doing the same work.
            // Deliveries are asserted so a silent filter change shows.
            let delivered = rt.block_on(read_all(&store, &queue_id, &file_id, step));
            let expected = if step == 1 { n } else { n.div_ceil(2) };
            assert_eq!(
                delivered, expected,
                "{case} step {step} should deliver {expected} entries"
            );

            // Reported after a read has succeeded, not straight after `close`:
            // the writer's last flush can land a little later, and measuring too
            // early understates the file by a buffer's worth.
            if step == 1 {
                eprintln!(
                    "[bench] {case}: {n} entries x {payload} B, {:.1} MiB on disk",
                    file_len(&wal_path) as f64 / (1024.0 * 1024.0)
                );
            }

            g.throughput(Throughput::Elements(n));
            g.bench_function(
                BenchmarkId::new("read_wal_range", format!("{case}/step{step}")),
                |b| {
                    b.iter_custom(|iters| {
                        rt.block_on(async {
                            let start = Instant::now();
                            for _ in 0..iters {
                                read_all(&store, &queue_id, &file_id, step).await;
                            }
                            start.elapsed()
                        })
                    });
                },
            );
        }

        drop(store);
        drop(tmp);
    }
    g.finish();
}

criterion_group!(benches, bench_range);
criterion_main!(benches);
