//! Full-range iteration throughput, V0 vs V1, at step 1 and step 2 (metric h).
//!
//! Times `WalStore::read_wal_range` over every entry of a file — the path a
//! subscriber or replay uses, as opposed to `wal_v1_scan`'s `get_file_end`,
//! which only needs the last id. Entries are delivered over a bounded channel
//! to a draining consumer, so the measurement includes the per-entry record copy
//! and channel hand-off that a real reader pays.
//!
//! `step` filters *after* the entry has been framed and checksum-verified: a
//! variable-length frame gives no way to find entry i+2 without decoding i+1.
//! So step 2 halves the records delivered but not the bytes read, and it adds a
//! `UintN` subtract-and-modulo per entry. Whether it is faster at all is the
//! point of measuring it.
//!
//! Both steps run back to back over the same file, so that comparison carries
//! no build-order effect; only V0 against V1 does.
//!
//! Warmup / measurement default to 5 s / 30 s, overridable (seconds) with
//! WAL_BENCH_WARMUP / WAL_BENCH_MEASURE. Sizes are overridable with
//! WAL_BENCH_RANGE_SMALL_N (default 100_000, 64 B records),
//! WAL_BENCH_RANGE_LARGE_N (default 20_000, 12 KiB records) and
//! WAL_BENCH_RANGE_GIB (default 2), which sizes the uncached case in GiB — set
//! it to at least 2x RAM or the file stays in the page cache.
//!
//!   cargo bench -p normfs-wal --bench wal_v1_range
//!   WAL_BENCH_RANGE_GIB=50 cargo bench -p normfs-wal --bench wal_v1_range -- uncached

use std::path::Path;
use std::time::{Duration, Instant};

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use normfs_types::{DataSource, QueueId, QueueIdResolver, ReadEntry};
use normfs_wal::{WalEntryFormat, WalHeader, WalSettings, WalStore};
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use uintn::UintN;

/// Large enough that the consumer is not the bottleneck, small enough to stay
/// a real hand-off.
const CHANNEL_CAPACITY: usize = 1024;

/// Bounds the build backlog; see wal_memory.
const BUILD_IN_FLIGHT_BYTES: u64 = 256 * 1024 * 1024;
const CHECK_EVERY: u64 = 1024;

fn env_secs(var: &str, default: u64) -> Duration {
    Duration::from_secs(
        std::env::var(var)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(default),
    )
}
fn env_u64(var: &str, default: u64) -> u64 {
    std::env::var(var)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn file_len(path: &Path) -> u64 {
    std::fs::metadata(path).map(|m| m.len()).unwrap_or(0)
}

async fn build_file(
    root: &Path,
    format: WalEntryFormat,
    n: u64,
    payload: usize,
) -> (WalStore, QueueId, UintN) {
    let (written_tx, _written_rx) = mpsc::unbounded_channel();
    let (complete_tx, _complete_rx) = mpsc::unbounded_channel();
    let store = WalStore::new(root, written_tx, complete_tx);

    let queue_id = QueueIdResolver::new("bench").resolve("range");
    let file_id = UintN::from(1u64);
    let settings = WalSettings {
        max_file_size: 1 << 40, // never rotate: the read must cover one file
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
    g.warm_up_time(env_secs("WAL_BENCH_WARMUP", 5));
    g.measurement_time(env_secs("WAL_BENCH_MEASURE", 30));
    g.sample_size(20);

    // The uncached case is sized in GiB rather than entries: it only means
    // anything if the file cannot fit in the page cache, so it has to be set
    // against the machine's RAM, not against a fixed entry count.
    let big_payload = 12 * 1024usize;
    let range_gib = env_u64("WAL_BENCH_RANGE_GIB", 2);
    let big_n = (range_gib << 30) / (big_payload as u64 + 28);

    let cases = [
        ("small", env_u64("WAL_BENCH_RANGE_SMALL_N", 100_000), 64usize),
        (
            "large",
            env_u64("WAL_BENCH_RANGE_LARGE_N", 20_000),
            big_payload,
        ),
        ("uncached", big_n, big_payload),
    ];

    for (case, n, payload) in cases {
        // A multi-GiB read takes seconds per iteration; 10 is criterion's floor.
        g.sample_size(if case == "uncached" { 10 } else { 20 });
        for (label, format) in [
            ("v0", WalEntryFormat::V0),
            ("v1", WalEntryFormat::V1),
        ] {
            let tmp = tempfile::tempdir().unwrap();
            let (store, queue_id, file_id) =
                rt.block_on(build_file(tmp.path(), format, n, payload));
            let wal_path =
                file_id.to_file_path(queue_id.to_wal_dir(tmp.path()).to_str().unwrap(), "wal");

            for step in [1usize, 2] {
                // Every entry is framed and verified regardless of step, so
                // throughput is quoted over entries *scanned*, not delivered —
                // otherwise step 2 would look twice as fast for doing the same
                // work. Deliveries are asserted so a silent filter change shows.
                let delivered = rt.block_on(read_all(&store, &queue_id, &file_id, step));
                let expected = if step == 1 { n } else { n.div_ceil(2) };
                assert_eq!(
                    delivered, expected,
                    "{case}/{label} step {step} should deliver {expected} entries"
                );

                // Reported after a read has succeeded, not straight after
                // `close`: the writer's last flush can land a little later, and
                // measuring too early understates the file by a buffer's worth.
                if step == 1 {
                    eprintln!(
                        "[bench] {case}/{label}: {n} entries x {payload} B, {:.1} MiB on disk",
                        file_len(&wal_path) as f64 / (1024.0 * 1024.0)
                    );
                }

                g.throughput(Throughput::Elements(n));
                g.bench_function(
                    BenchmarkId::new("read_wal_range", format!("{case}/{label}/step{step}")),
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
    }
    g.finish();
}

criterion_group!(benches, bench_range);
criterion_main!(benches);
