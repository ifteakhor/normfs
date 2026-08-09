use crate::ack_file_writer::{AckFileWriter, AckFileWriterSettings};

use super::*;
use bytes::Bytes;
use normfs_types::QueueIdResolver;
use std::fs;
use std::path::Path;
use std::time::Duration;
use tempfile::tempdir;
use tokio::sync::mpsc;
use tokio::time::timeout;

async fn read_file_content(path: &Path) -> Vec<u8> {
    fs::read(path).unwrap()
}

#[tokio::test]
async fn test_write_single_entry_and_ack() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test.wal");

    let (ack_sender, mut ack_receiver) = mpsc::unbounded_channel();

    let settings = AckFileWriterSettings {
        max_buffer_size: 1024,
        max_file_size: 10 * 1024,
        write_interval: Duration::from_millis(10),
        fsync: true,
    };

    let mut writer = AckFileWriter::new(&file_path, settings, ack_sender, Bytes::new(), None, 0)
        .await
        .unwrap();

    let resolver = QueueIdResolver::new("test_instance");
    let queue_id = resolver.resolve("queue_1");
    let entry_id = UintN::from(1u64);
    let data = Bytes::from("hello world");

    writer
        .write(queue_id.clone(), entry_id.clone(), data.clone())
        .await;
    writer.close().await.unwrap();

    let file_content = read_file_content(&file_path).await;
    assert_eq!(file_content, data);

    let ack = timeout(Duration::from_secs(1), ack_receiver.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ack, (queue_id, entry_id));
}

#[tokio::test]
async fn test_write_multiple_entries_and_ack() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test.wal");

    let (ack_sender, mut ack_receiver) = mpsc::unbounded_channel();

    let settings = AckFileWriterSettings {
        max_buffer_size: 1024,
        max_file_size: 10 * 1024,
        write_interval: Duration::from_millis(100),
        fsync: true,
    };

    let mut writer = AckFileWriter::new(&file_path, settings, ack_sender, Bytes::new(), None, 0)
        .await
        .unwrap();

    let resolver = QueueIdResolver::new("test_instance");
    let queue_id_1 = resolver.resolve("queue_1");
    let entry_id_1 = UintN::from(1u64);
    let data_1 = Bytes::from("entry 1");

    let queue_id_2 = resolver.resolve("queue_2");
    let entry_id_2 = UintN::from(10u64);
    let data_2 = Bytes::from("entry 2");

    writer
        .write(queue_id_1.clone(), entry_id_1.clone(), data_1.clone())
        .await;
    writer
        .write(queue_id_2.clone(), entry_id_2.clone(), data_2.clone())
        .await;

    writer.close().await.unwrap();

    let mut expected_content = Vec::new();
    expected_content.extend_from_slice(&data_1);
    expected_content.extend_from_slice(&data_2);

    let file_content = read_file_content(&file_path).await;
    assert_eq!(file_content, expected_content);

    let ack = timeout(Duration::from_secs(1), ack_receiver.recv())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(ack, (queue_id_2, entry_id_2));
}

#[tokio::test]
async fn test_buffer_full_triggers_write_and_ack() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test2.wal");

    let (ack_sender, mut ack_receiver) = mpsc::unbounded_channel();

    let settings = AckFileWriterSettings {
        max_buffer_size: 20,
        max_file_size: 10 * 1024,
        write_interval: Duration::from_secs(5), // Long interval to ensure buffer full is the trigger
        fsync: true,
    };

    let mut writer = AckFileWriter::new(&file_path, settings, ack_sender, Bytes::new(), None, 0)
        .await
        .unwrap();

    let resolver = QueueIdResolver::new("test_instance");
    let queue_id_1 = resolver.resolve("queue_1");
    let entry_id_1 = UintN::from(1u64);
    let data_1 = Bytes::from("1234567890");

    let queue_id_2 = resolver.resolve("queue_2");
    let entry_id_2 = UintN::from(2u64);
    let data_2 = Bytes::from("abcdefghij");

    let queue_id_3 = resolver.resolve("queue_3");
    let entry_id_3 = UintN::from(3u64);
    let data_3 = Bytes::from("last_one");

    writer
        .write(queue_id_1.clone(), entry_id_1.clone(), data_1.clone())
        .await;
    writer
        .write(queue_id_2.clone(), entry_id_2.clone(), data_2.clone())
        .await;

    // The first two writes should fill the buffer and trigger a flush
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut expected_content = Vec::new();
    expected_content.extend_from_slice(&data_1);
    expected_content.extend_from_slice(&data_2);

    let file_content = read_file_content(&file_path).await;
    assert_eq!(file_content, expected_content);

    let ack2 = timeout(Duration::from_secs(1), ack_receiver.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ack2, (queue_id_2, entry_id_2));

    // Write one more and close
    writer
        .write(queue_id_3.clone(), entry_id_3.clone(), data_3.clone())
        .await;
    writer.close().await.unwrap();

    expected_content.extend_from_slice(&data_3);
    let file_content = read_file_content(&file_path).await;
    assert_eq!(file_content, expected_content);

    let ack3 = timeout(Duration::from_secs(1), ack_receiver.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ack3, (queue_id_3, entry_id_3));
}

#[tokio::test]
async fn test_writer_with_header() {
    let dir = tempfile::tempdir().unwrap();
    let file_path = dir.path().join("test_writer_with_header.log");

    let settings = AckFileWriterSettings {
        max_buffer_size: 1024,
        max_file_size: 10 * 1024,
        write_interval: Duration::from_millis(100),
        fsync: true,
    };

    let (ack_sender, mut ack_receiver) = mpsc::unbounded_channel();
    let header = Bytes::from_static(b"test header");

    let mut writer = AckFileWriter::new(&file_path, settings, ack_sender, header.clone(), None, 0)
        .await
        .unwrap();

    let resolver = QueueIdResolver::new("test_instance");
    let queue_id = resolver.resolve("test_queue");
    let entry_id = UintN::from(1u64);
    let entry_data = Bytes::from_static(b"test data");
    writer
        .write(queue_id.clone(), entry_id.clone(), entry_data.clone())
        .await;

    writer.close().await.unwrap();

    let file_content = tokio::fs::read(&file_path).await.unwrap();
    assert_eq!(&file_content[..header.len()], header.as_ref());
    assert_eq!(&file_content[header.len()..], entry_data.as_ref());

    let ack = ack_receiver.recv().await.unwrap();
    assert_eq!(ack, (queue_id, entry_id));
}

/// The page-write path: bytes reach the file straight from the pages they were
/// appended into, and the durable watermark moves only once they are there.
#[tokio::test]
async fn pages_reach_the_file_before_the_watermark_moves() {
    use crate::page_pool::PagePool;
    use crate::wal_ring_v1::AppendOutcome;
    use std::sync::Arc;

    let dir = tempdir().unwrap();
    let file_path = dir.path().join("paged.wal");
    let (ack_sender, _ack_receiver) = mpsc::unbounded_channel();

    let pool = Arc::new(PagePool::new(4, 4096, 0));
    let settings = AckFileWriterSettings {
        max_buffer_size: 1024,
        max_file_size: 10 * 1024 * 1024,
        write_interval: Duration::from_millis(10),
        fsync: true,
    };

    let header = Bytes::from_static(b"HDR!");
    let mut writer = AckFileWriter::new(
        &file_path,
        settings,
        ack_sender,
        header.clone(),
        Some(Arc::clone(&pool)),
        0,
    )
    .await
    .unwrap();

    // Standing in for WriterState, which opens this gate once the record that
    // opened the file has been handed over. Until then a flush takes no pages,
    // so the file's first record cannot be overtaken by the ones behind it.
    writer.allow_pool_flush();

    // Nothing is durable before anything has been appended.
    assert_eq!(pool.durable_before(), 0);

    let record = b"a paged record";
    for _ in 0..3 {
        assert!(matches!(
            pool.try_append(record),
            AppendOutcome::Cached(_)
        ));
    }
    let appended_through = pool.next_entry_id();

    // The flush is on a timer; wait for the watermark rather than for a
    // duration, so the test asserts the ordering instead of racing it.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while pool.durable_before() < appended_through {
        assert!(
            tokio::time::Instant::now() < deadline,
            "writer never reported the pages durable"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    writer.close().await.unwrap();

    // Once the watermark has moved, the bytes really are in the file: header
    // first, then each record's payload in append order.
    let content = read_file_content(&file_path).await;
    assert!(content.starts_with(&header), "header should lead the file");
    let occurrences = content
        .windows(record.len())
        .filter(|w| *w == record)
        .count();
    assert_eq!(
        occurrences, 3,
        "every appended record should appear exactly once in the file"
    );
}
