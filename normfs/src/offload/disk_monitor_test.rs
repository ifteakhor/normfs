use super::*;
use normfs_types::QueueIdResolver;
use std::sync::Mutex;

fn write_store_file(root: &Path, queue: &QueueId, id: u64, len: usize) {
    let path = queue.to_store_path(root, &UintN::from(id));
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, vec![0u8; len]).unwrap();
}

fn store_file_exists(root: &Path, queue: &QueueId, id: u64) -> bool {
    queue.to_store_path(root, &UintN::from(id)).exists()
}

#[tokio::test]
async fn the_tracked_size_follows_completions_and_deletions() {
    let temp = tempfile::TempDir::new().unwrap();
    let root = temp.path();
    let queue = QueueIdResolver::new("inst").resolve("cam");
    for id in 1..=4 {
        write_store_file(root, &queue, id, 100);
    }

    let forgotten = Arc::new(Mutex::new(Vec::new()));
    let log = forgotten.clone();
    let forget: ForgetRange =
        Arc::new(move |_: &QueueId, id: &UintN| log.lock().unwrap().push(id.clone()));
    let monitor = DiskMonitor::new(root, None, None, Some(forget))
        .await
        .unwrap();

    let config = DiskMonitorConfig {
        max_size: 250,
        check_interval: Duration::from_secs(60),
        wal_settings: WalSettings {
            max_file_size: 10,
            ..Default::default()
        },
    };
    monitor.add_queue(&queue, config).await.unwrap();

    // 400 bytes over a 250 limit: the two oldest files go, and the range
    // cache hears about each.
    assert!(!store_file_exists(root, &queue, 1));
    assert!(!store_file_exists(root, &queue, 2));
    assert!(store_file_exists(root, &queue, 3));
    assert_eq!(
        *forgotten.lock().unwrap(),
        vec![UintN::from(1u64), UintN::from(2u64)]
    );

    let monitors = monitor.monitors.read().await;
    let queue_monitor = monitors.get(&queue).unwrap();
    assert_eq!(queue_monitor.get_queue_size().await.unwrap(), 200);

    // A file nobody reported is not counted: the directory is not walked.
    write_store_file(root, &queue, 6, 100);
    assert_eq!(queue_monitor.get_queue_size().await.unwrap(), 200);

    // A reported completion is.
    write_store_file(root, &queue, 5, 100);
    queue_monitor.store_file_done(&UintN::from(5u64)).await;
    assert_eq!(queue_monitor.get_queue_size().await.unwrap(), 300);

    queue_monitor.check_and_cleanup(false).await.unwrap();
    assert!(!store_file_exists(root, &queue, 3));
    assert_eq!(queue_monitor.get_queue_size().await.unwrap(), 200);

    // The periodic rescan picks up the unreported file.
    queue_monitor.check_and_cleanup(true).await.unwrap();
    assert!(!store_file_exists(root, &queue, 4));
    assert!(store_file_exists(root, &queue, 5));
    assert!(store_file_exists(root, &queue, 6));
    assert_eq!(queue_monitor.get_queue_size().await.unwrap(), 200);
}
