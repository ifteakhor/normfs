use std::path::Path;
use std::time::{Duration, Instant};

use bytes::Bytes;
use normfs::NormFS;
use tempfile::TempDir;

fn wal_bytes(dir: &Path) -> u64 {
    let mut total = 0;
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            match entry.file_type() {
                Ok(t) if t.is_dir() => total += wal_bytes(&path),
                Ok(_) if path.extension().is_some_and(|x| x == "wal") => {
                    total += entry.metadata().map(|m| m.len()).unwrap_or(0)
                }
                _ => {}
            }
        }
    }
    total
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lone_record_reaches_disk_without_filling_the_buffer() {
    let _ = env_logger::builder().is_test(true).try_init();

    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    let normfs = NormFS::new(path.clone(), normfs::NormFsSettings::all_active())
        .await
        .unwrap();
    let queue = normfs.resolve("cold");
    normfs.ensure_queue_exists_for_write(&queue).await.unwrap();

    // A first record, given time to settle, so the size below is the one a
    // lone record has to move.
    normfs
        .enqueue(&queue, Bytes::from_static(b"first record"))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    let settled = wal_bytes(&path);
    assert!(settled > 0, "the first record never produced a WAL file");

    normfs
        .enqueue(&queue, Bytes::from_static(b"one record a week"))
        .await
        .unwrap();

    let start = Instant::now();
    let deadline = start + Duration::from_secs(10);
    while wal_bytes(&path) <= settled {
        assert!(
            Instant::now() < deadline,
            "a single small record never reached disk: the WAL is still {settled} bytes, \
             so nothing but a full buffer starts a flush"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    println!("record on disk after {:?}", start.elapsed());

    // Inside the flush interval, so this fails on a design that waits for a
    // full buffer rather than on a slow disk.
    assert!(start.elapsed() < Duration::from_secs(2));
}
