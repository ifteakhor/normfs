use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use normfs::{NormFS, NormFsSettings, ReadPosition};
use tempfile::TempDir;
use uintn::UintN;

const TASKS: usize = 8;
const PER_TASK: usize = 40;
const OVERSIZED: usize = 300 * 1024;

fn payload(task: usize, index: usize) -> Bytes {
    let tag = format!("t{task:02}-r{index:03}-");
    let len = if index % 10 == 3 { OVERSIZED } else { 96 };
    let mut v = vec![b'.'; len];
    v[..tag.len()].copy_from_slice(tag.as_bytes());
    Bytes::from(v)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn records_reach_disk_in_the_order_they_were_accepted() {
    let _ = env_logger::builder().is_test(true).try_init();

    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    let settings = NormFsSettings {
        max_memory_usage: 4 * 1024 * 1024,
        wal_settings: normfs_wal::WalSettings {
            max_file_size: 4 * 1024 * 1024,
            ..Default::default()
        },
        ..Default::default()
    };

    let normfs = Arc::new(NormFS::new(path.clone(), settings.clone()).await.unwrap());
    let queue = normfs.resolve("order");
    normfs.ensure_queue_exists_for_write(&queue).await.unwrap();

    let mut handles = Vec::new();
    for task in 0..TASKS {
        let normfs = Arc::clone(&normfs);
        let queue = queue.clone();
        handles.push(tokio::spawn(async move {
            let mut placed = Vec::new();
            for index in 0..PER_TASK {
                let data = payload(task, index);
                let id = normfs.enqueue(&queue, data.clone()).await.unwrap();
                placed.push((id.to_u64().unwrap(), data));
            }
            placed
        }));
    }

    let mut expected: HashMap<u64, Bytes> = HashMap::new();
    for handle in handles {
        for (id, data) in handle.await.unwrap() {
            assert!(
                expected.insert(id, data).is_none(),
                "id {id} was handed to two records"
            );
        }
    }
    let total = TASKS * PER_TASK;
    assert_eq!(expected.len(), total, "every enqueue must return its own id");

    normfs.close().await.unwrap();
    drop(normfs);

    let normfs = NormFS::new(path.clone(), settings).await.unwrap();
    let queue = normfs.resolve("order");
    normfs.ensure_queue_exists_for_write(&queue).await.unwrap();

    let (tx, mut rx) = tokio::sync::mpsc::channel(64);
    let reader = tokio::spawn(async move {
        normfs
            .read(&queue, ReadPosition::Absolute(UintN::zero()), total as u64, 1, tx)
            .await
    });

    let mut seen = 0usize;
    let mut previous: Option<u64> = None;
    while let Ok(Some(entry)) = tokio::time::timeout(Duration::from_secs(60), rx.recv()).await {
        let id = entry.id.to_u64().unwrap();
        if let Some(previous) = previous {
            assert!(id > previous, "ids came back out of order: {previous} then {id}");
        }
        let want = expected
            .get(&id)
            .unwrap_or_else(|| panic!("read an id nothing was enqueued under: {id}"));
        assert_eq!(
            entry.data.len(),
            want.len(),
            "id {id} came back with a different record's length"
        );
        assert_eq!(entry.data, *want, "id {id} came back holding another record");
        previous = Some(id);
        seen += 1;
    }
    let _ = reader.await;

    assert_eq!(seen, total, "every record enqueued must be readable");
}
