//! Identical-source migration benchmark: 384 WAL files, 1 MiB payload each,
//! four store workers, no compression/encryption, durable publication.

use bytes::Bytes;
use normfs_crypto::CryptoContext;
use normfs_store::{PersistStore, StoreWriteConfig};
use normfs_types::{CompressionType, EncryptionType, QueueIdResolver};
use normfs_wal::{WalHeader, WalSettings, WalStore};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use uintn::UintN;

#[tokio::main(worker_threads = 8)]
async fn main() {
    let queues: usize = std::env::args()
        .nth(1)
        .expect("queue count")
        .parse()
        .unwrap();
    assert!([1, 12].contains(&queues));
    let tmp = tempfile::tempdir().unwrap();
    let crypto = Arc::new(CryptoContext::open(tmp.path()).unwrap());
    let resolver = QueueIdResolver::new(crypto.instance_id_hex());
    let (written, _written_rx) = mpsc::unbounded_channel();
    let (done, done_rx) = mpsc::unbounded_channel();
    let wal = Arc::new(WalStore::new(tmp.path(), written, done));
    let store = PersistStore::new(
        tmp.path(),
        StoreWriteConfig {
            num_workers: 4,
            verify_signatures: true,
        },
        crypto,
        wal.clone(),
        mpsc::unbounded_channel().0,
    );
    let payload = Bytes::from(vec![0xAB; 4096]);
    let mut expected = Vec::new();
    for i in 0..384 {
        let queue = resolver.resolve(&format!("queue_{}", i % queues));
        let file = UintN::from((i / queues + 1) as u64);
        let before = (i / queues * 256) as u64;
        let header = WalHeader {
            num_entries_before: UintN::from(before),
            ..Default::default()
        };
        let settings = WalSettings {
            max_file_size: 1 << 30,
            write_buffer_size: 2 << 20,
            enable_fsync: false,
            compression_type: CompressionType::None,
            encryption_type: EncryptionType::None,
            ..Default::default()
        };
        wal.start_writer(
            &queue,
            &file,
            header,
            settings,
            before.checked_sub(1).map(UintN::from),
        )
        .await
        .unwrap();
        for id in before..before + 256 {
            wal.enqueue(&queue, UintN::from(id), payload.clone())
                .unwrap();
        }
        wal.close_writer(&queue).await.unwrap();
        expected.push((queue, file));
    }
    let start = Instant::now();
    let mut completed = store.start_writers(done_rx).await;
    let mut received = Vec::new();
    for _ in 0..384 {
        let item = tokio::time::timeout(Duration::from_secs(30), completed.recv())
            .await
            .expect("publication timeout")
            .expect("completion channel closed");
        assert!(expected.contains(&item));
        assert!(!received.contains(&item), "duplicate completion");
        received.push(item);
    }
    // Completion precedes range bookkeeping and WAL removal. Joining workers
    // includes those final operations in the measured migration time.
    store.close().await.unwrap();
    let elapsed = start.elapsed().as_secs_f64();
    assert_eq!(received.len(), expected.len());
    for (q, f) in &expected {
        let raw = store
            .get_store_bytes(q, f)
            .await
            .unwrap()
            .expect("published file");
        let bytes = store.extract_wal_bytes(q, f, raw, true).unwrap();
        assert!(!bytes.is_empty());
    }
    wal.close().await.unwrap();
    println!(
        "RESULT {{\"case\":\"publication_{queues}\",\"files\":384,\"payload_mib\":384,\"seconds\":{elapsed},\"publish_mib_s\":{},\"files_s\":{}}}",
        384.0 / elapsed,
        384.0 / elapsed
    );
}
