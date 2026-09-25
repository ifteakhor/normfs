//! Identical-source dev/branch comparison. Run one case per process:
//! `fs_comparison throughput 80`, `throughput 4096`, or `latency 0|4|12`.
//! Scans are deliberately cached; this does not measure cold storage bandwidth.

use bytes::Bytes;
use normfs_types::{CompressionType, EncryptionType, QueueId, QueueIdResolver};
use normfs_wal::{WalHeader, WalSettings, WalStore};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::time::{Duration, Instant};
use tokio::sync::{Barrier, mpsc};
use uintn::UintN;

fn settings(buffer: usize, fsync: bool) -> WalSettings {
    WalSettings {
        max_file_size: 1 << 40,
        write_buffer_size: buffer,
        write_interval: Duration::from_millis(50),
        enable_fsync: fsync,
        compression_type: CompressionType::None,
        encryption_type: EncryptionType::None,
        ..Default::default()
    }
}

async fn ack(rx: &mut mpsc::UnboundedReceiver<(QueueId, UintN)>, queue: &QueueId, id: u64) {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let (q, n) = rx.recv().await.expect("ack channel closed");
            if &q == queue && n.to_u64().unwrap() >= id {
                break;
            }
        }
    })
    .await
    .expect("durable ack timed out");
}

async fn build(
    store: &WalStore,
    q: &QueueId,
    rx: &mut mpsc::UnboundedReceiver<(QueueId, UintN)>,
    payload: usize,
    n: u64,
    fsync: bool,
) {
    store
        .start_writer(
            q,
            &UintN::from(1u64),
            WalHeader::default(),
            settings(1 << 20, fsync),
            None,
        )
        .await
        .unwrap();
    let bytes = Bytes::from(vec![0xAB; payload]);
    let window = (16 * 1024 * 1024 / payload).max(1) as u64;
    for i in 0..n {
        store.enqueue(q, UintN::from(i), bytes.clone()).unwrap();
        if (i + 1) % window == 0 {
            ack(rx, q, i).await;
        }
    }
    store.close_writer(q).await.unwrap();
    if n % window != 0 {
        ack(rx, q, n - 1).await;
    }
}

fn percentile(v: &[f64], p: f64) -> f64 {
    v[((v.len() as f64 * p).ceil() as usize).saturating_sub(1)]
}

#[tokio::main(worker_threads = 8)]
async fn main() {
    let args: Vec<_> = std::env::args().collect();
    assert_eq!(
        args.len(),
        3,
        "usage: fs_comparison throughput BYTES | latency READERS"
    );
    let value: usize = args[2].parse().unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let (done, _done_rx) = mpsc::unbounded_channel();
    let store = Arc::new(WalStore::new(tmp.path(), tx, done));
    let resolver = QueueIdResolver::new("fs-comparison");
    let q = resolver.resolve("scan");
    let file = UintN::from(1u64);
    match args[1].as_str() {
        "throughput" => {
            assert!([80, 4096].contains(&value));
            let n = if value == 80 { 10_000_000 } else { 1_048_576 };
            let start = Instant::now();
            build(&store, &q, &mut rx, value, n, true).await;
            let write = start.elapsed().as_secs_f64();
            assert_eq!(
                store.get_file_end(&q, &file).await.unwrap(),
                Some(UintN::from(n - 1))
            );
            let start = Instant::now();
            for _ in 0..5 {
                assert_eq!(
                    store.get_file_end(&q, &file).await.unwrap(),
                    Some(UintN::from(n - 1))
                );
            }
            let scan = start.elapsed().as_secs_f64() / 5.0;
            println!(
                "RESULT {{\"case\":\"throughput_{value}\",\"records\":{n},\"payload\":{value},\"write_seconds\":{write},\"write_mib_s\":{},\"scan_seconds\":{scan},\"scan_mib_s\":{}}}",
                n as f64 * value as f64 / 1048576.0 / write,
                n as f64 * value as f64 / 1048576.0 / scan
            );
        }
        "latency" => {
            assert!([0, 4, 12].contains(&value));
            let n = 16384;
            build(&store, &q, &mut rx, 4096, n, false).await;
            assert_eq!(
                store.get_file_end(&q, &file).await.unwrap(),
                Some(UintN::from(n - 1))
            );
            let writer = resolver.resolve("commit");
            // A full buffer triggers every record immediately; the flush timer
            // must not hide pool contention behind its 50 ms interval.
            store
                .start_writer(
                    &writer,
                    &file,
                    WalHeader::default(),
                    settings(4096, true),
                    None,
                )
                .await
                .unwrap();
            let stop = Arc::new(AtomicBool::new(false));
            let scans = Arc::new(AtomicU64::new(0));
            let barrier = Arc::new(Barrier::new(value + 1));
            let mut readers = Vec::new();
            for _ in 0..value {
                let (store, q, file, stop, scans, barrier) = (
                    store.clone(),
                    q.clone(),
                    file.clone(),
                    stop.clone(),
                    scans.clone(),
                    barrier.clone(),
                );
                readers.push(tokio::spawn(async move {
                    barrier.wait().await;
                    while !stop.load(Ordering::Relaxed) {
                        assert_eq!(
                            store.get_file_end(&q, &file).await.unwrap(),
                            Some(UintN::from(n - 1))
                        );
                        scans.fetch_add(1, Ordering::Relaxed);
                    }
                }));
            }
            barrier.wait().await;
            let bytes = Bytes::from(vec![0xCD; 4096]);
            for i in 0..20 {
                store
                    .enqueue(&writer, UintN::from(i), bytes.clone())
                    .unwrap();
                ack(&mut rx, &writer, i).await;
            }
            let mut samples = Vec::new();
            let scans_before = scans.load(Ordering::Relaxed);
            let window = Instant::now();
            for i in 20..520 {
                let start = Instant::now();
                store
                    .enqueue(&writer, UintN::from(i), bytes.clone())
                    .unwrap();
                ack(&mut rx, &writer, i).await;
                samples.push(start.elapsed().as_secs_f64() * 1000.0);
            }
            let elapsed = window.elapsed().as_secs_f64();
            let completed = scans.load(Ordering::Relaxed) - scans_before;
            stop.store(true, Ordering::Relaxed);
            for reader in readers {
                reader.await.unwrap();
            }
            samples.sort_by(f64::total_cmp);
            println!(
                "RESULT {{\"case\":\"latency_{value}\",\"readers\":{value},\"commits\":500,\"p50_ms\":{},\"p95_ms\":{},\"p99_ms\":{},\"max_ms\":{},\"reader_scans\":{completed},\"seconds\":{elapsed},\"samples_ms\":{samples:?}}}",
                percentile(&samples, 0.50),
                percentile(&samples, 0.95),
                percentile(&samples, 0.99),
                samples.last().unwrap()
            );
        }
        _ => panic!("unknown case"),
    }
    store.close().await.unwrap();
}
