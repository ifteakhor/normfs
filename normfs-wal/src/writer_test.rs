use std::sync::Once;
use std::time::Duration;

use crate::{
    WalEntryFormat, WalSettings, WalStore,
    reader::{
        ReadRangeResult, get_wal_content, get_wal_range, read_wal_bytes_range, read_wal_file_range,
    },
    wal_header::WalHeader,
};
use bytes::Bytes;
use normfs_types::{DataSource, QueueIdResolver};
use tempfile::tempdir;
use tokio::sync::mpsc;
use tokio::time::timeout;
use uintn::UintN;

static INIT: Once = Once::new();

fn init_logger() {
    INIT.call_once(|| {
        env_logger::Builder::from_default_env()
            .filter_level(log::LevelFilter::Debug)
            .init();
    });
}

#[tokio::test]
async fn test_enqueue_and_read() {
    init_logger();
    let tmp_dir = tempdir().unwrap();
    let instance_id = "test_instance";
    let (written_sender, _) = mpsc::unbounded_channel();
    let (wal_complete_sender, _) = mpsc::unbounded_channel();

    let store = WalStore::new(tmp_dir.path(), written_sender, wal_complete_sender);

    let resolver = QueueIdResolver::new(instance_id);
    let queue_id = resolver.resolve("test_queue");
    let file_id = UintN::from(1u64);
    let header = WalHeader::default();
    let settings = WalSettings {
        max_file_size: 1024,
        write_buffer_size: 128,
        enable_fsync: true,
        encryption_type: normfs_types::EncryptionType::Aes,
        compression_type: normfs_types::CompressionType::Zstd,
        wal_entry_format: WalEntryFormat::default(),
    };

    store
        .start_writer(&queue_id, &file_id, header, settings, None)
        .await
        .unwrap();

    let entry_id1 = UintN::from(0u64);
    let data1 = Bytes::from("hello");
    store.enqueue(&queue_id, entry_id1, data1).unwrap();

    let entry_id2 = UintN::from(1u64);
    let data2 = Bytes::from("world");
    store.enqueue(&queue_id, entry_id2, data2).unwrap();

    store.close().await.unwrap();

    let (tx, mut rx) = mpsc::channel(10);
    let result = read_wal_file_range(
        &queue_id.to_wal_dir(tmp_dir.path()),
        &file_id,
        &UintN::from(0u64),
        &Some(UintN::from(1u64)),
        1,
        &tx,
        DataSource::DiskWal,
    )
    .await
    .unwrap();
    drop(tx);

    assert!(matches!(result, ReadRangeResult::Complete));

    let read_entry1 = timeout(Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read_entry1.id, UintN::from(0u64));
    assert_eq!(read_entry1.data, Bytes::from("hello"));
    assert_eq!(read_entry1.source, DataSource::DiskWal);

    let read_entry2 = timeout(Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read_entry2.id, UintN::from(1u64));
    assert_eq!(read_entry2.data, Bytes::from("world"));
    assert_eq!(read_entry2.source, DataSource::DiskWal);
}

#[tokio::test]
async fn test_enqueue_batch_and_read() {
    init_logger();
    let tmp_dir = tempdir().unwrap();
    let instance_id = "test_instance";
    let (written_sender, _) = mpsc::unbounded_channel();
    let (wal_complete_sender, _) = mpsc::unbounded_channel();

    let store = WalStore::new(tmp_dir.path(), written_sender, wal_complete_sender);

    let resolver = QueueIdResolver::new(instance_id);
    let queue_id = resolver.resolve("test_queue");
    let file_id = UintN::from(1u64);
    let header = WalHeader::default();
    let settings = WalSettings {
        max_file_size: 1024,
        write_buffer_size: 128,
        enable_fsync: true,
        encryption_type: normfs_types::EncryptionType::Aes,
        compression_type: normfs_types::CompressionType::Zstd,
        wal_entry_format: WalEntryFormat::default(),
    };

    store
        .start_writer(&queue_id, &file_id, header, settings, None)
        .await
        .unwrap();

    let entries = vec![
        (UintN::from(0u64), Bytes::from("hello")),
        (UintN::from(1u64), Bytes::from("world")),
    ];
    store.enqueue_batch(&queue_id, entries).unwrap();

    store.close().await.unwrap();

    let (tx, mut rx) = mpsc::channel(10);
    let result = read_wal_file_range(
        &queue_id.to_wal_dir(tmp_dir.path()),
        &file_id,
        &UintN::from(0u64),
        &Some(UintN::from(1u64)),
        1,
        &tx,
        DataSource::DiskWal,
    )
    .await
    .unwrap();
    drop(tx);

    assert!(matches!(result, ReadRangeResult::Complete));

    let read_entry1 = timeout(Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read_entry1.id, UintN::from(0u64));
    assert_eq!(read_entry1.data, Bytes::from("hello"));
    assert_eq!(read_entry1.source, DataSource::DiskWal);

    let read_entry2 = timeout(Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read_entry2.id, UintN::from(1u64));
    assert_eq!(read_entry2.data, Bytes::from("world"));
    assert_eq!(read_entry2.source, DataSource::DiskWal);
}

#[tokio::test]
async fn test_size_based_rotation() {
    init_logger();
    let tmp_dir = tempdir().unwrap();
    let instance_id = "test_instance";
    let (written_sender, _) = mpsc::unbounded_channel();
    let (wal_complete_sender, mut wal_complete_receiver) = mpsc::unbounded_channel();

    let store = WalStore::new(tmp_dir.path(), written_sender, wal_complete_sender);

    let resolver = QueueIdResolver::new(instance_id);
    let queue_id = resolver.resolve("test_queue");
    let file_id = UintN::from(1u64);
    let header = WalHeader::default();
    let settings = WalSettings {
        max_file_size: 128,
        write_buffer_size: 64,
        enable_fsync: true,
        encryption_type: normfs_types::EncryptionType::Aes,
        compression_type: normfs_types::CompressionType::Zstd,
        wal_entry_format: WalEntryFormat::default(),
    };

    store
        .start_writer(&queue_id, &file_id, header, settings, None)
        .await
        .unwrap();

    let entry_id1 = UintN::from(0u64);
    let data1 = Bytes::from(vec![0; 64]);
    store.enqueue(&queue_id, entry_id1, data1).unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    let entry_id2 = UintN::from(1u64);
    let data2 = Bytes::from(vec![0; 64]);
    store.enqueue(&queue_id, entry_id2, data2).unwrap();

    store.close().await.unwrap();

    let received = wal_complete_receiver.recv().await.unwrap();
    assert_eq!(received.queue_id, queue_id);
    assert_eq!(received.file_id, file_id);

    let content1 = get_wal_content(&queue_id.to_wal_dir(tmp_dir.path()), &file_id)
        .await
        .unwrap();
    assert_eq!(content1.num_entries, UintN::from(1u64));

    let content2 = get_wal_content(&queue_id.to_wal_dir(tmp_dir.path()), &file_id.increment())
        .await
        .unwrap();
    assert_eq!(content2.num_entries, UintN::from(1u64));
}

/// Pinned to V0: the trigger only exists there. A V0 entry stores its record
/// size in a `data_size_bytes`-wide field, so a record too wide for it forces a
/// new file. V1 frames the size as a varint and has no width to overflow.
#[tokio::test]
async fn test_data_size_based_rotation() {
    init_logger();
    let tmp_dir = tempdir().unwrap();
    let instance_id = "test_instance";
    let (written_sender, _) = mpsc::unbounded_channel();
    let (wal_complete_sender, mut wal_complete_receiver) = mpsc::unbounded_channel();

    let store = WalStore::new(tmp_dir.path(), written_sender, wal_complete_sender);

    let resolver = QueueIdResolver::new(instance_id);
    let queue_id = resolver.resolve("test_queue");
    let file_id = UintN::from(1u64);
    let header = WalHeader {
        data_size_bytes: 1, // Set data size limit to 1 byte
        ..Default::default()
    };
    let settings = WalSettings {
        max_file_size: 1024,
        write_buffer_size: 128,
        enable_fsync: true,
        encryption_type: normfs_types::EncryptionType::Aes,
        compression_type: normfs_types::CompressionType::Zstd,
        wal_entry_format: WalEntryFormat::V0,
    };

    store
        .start_writer(&queue_id, &file_id, header, settings, None)
        .await
        .unwrap();

    let entry_id1 = UintN::from(0u64);
    let data1 = Bytes::from("a"); // 1 byte - fits in data_size_bytes = 1
    store.enqueue(&queue_id, entry_id1, data1).unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    let entry_id2 = UintN::from(1u64);
    let data2 = Bytes::from("data too large for writing into file, data too large for writing into file, data too large for writing into file, data too large for writing into file,
    data too large for writing into file, data too large for writing into file, data too large for writing into file, data too large for writing into file
    data too large for writing into file, data too large for writing into file, data too large for writing into file, data too large for writing into file"); // > 1 byte - triggers rotation
    store.enqueue(&queue_id, entry_id2, data2).unwrap();

    // Wait for the rotation to complete and receive the completion signal
    let received = timeout(Duration::from_secs(5), wal_complete_receiver.recv())
        .await
        .expect("wal_complete_receiver.recv() timed out")
        .unwrap();
    assert_eq!(received.queue_id, queue_id);
    assert_eq!(received.file_id, file_id);

    // Now close the store
    timeout(Duration::from_secs(5), store.close())
        .await
        .expect("store.close() timed out")
        .unwrap();

    let content1 = timeout(
        Duration::from_secs(5),
        get_wal_content(&queue_id.to_wal_dir(tmp_dir.path()), &file_id),
    )
    .await
    .expect("get_wal_content for file_id timed out")
    .unwrap();
    assert_eq!(content1.num_entries, UintN::from(1u64));

    let content2 = timeout(
        Duration::from_secs(5),
        get_wal_content(&queue_id.to_wal_dir(tmp_dir.path()), &file_id.increment()),
    )
    .await
    .expect("get_wal_content for file_id.increment() timed out")
    .unwrap();
    assert_eq!(content2.num_entries, UintN::from(1u64));
    assert_eq!(content2.entries_before, UintN::from(1u64));
}

/// End-to-end V1: the writer emits `[varint][record][crc]` entries with no id,
/// and every reader path derives the id from num_entries_before + index.
#[tokio::test]
async fn test_v1_enqueue_read_and_scan() {
    init_logger();
    let tmp_dir = tempdir().unwrap();
    let (written_sender, _) = mpsc::unbounded_channel();
    let (wal_complete_sender, _) = mpsc::unbounded_channel();
    let store = WalStore::new(tmp_dir.path(), written_sender, wal_complete_sender);

    let resolver = QueueIdResolver::new("test_instance");
    let queue_id = resolver.resolve("v1_queue");
    let file_id = UintN::from(1u64);
    let header = WalHeader::default(); // num_entries_before = 0
    let settings = WalSettings {
        max_file_size: 4096,
        write_buffer_size: 128,
        enable_fsync: true,
        encryption_type: normfs_types::EncryptionType::Aes,
        compression_type: normfs_types::CompressionType::Zstd,
        wal_entry_format: WalEntryFormat::V1,
    };
    store
        .start_writer(&queue_id, &file_id, header, settings, None)
        .await
        .unwrap();

    // Empty and varying-width records; ids must be enqueued as num_entries_before + i.
    let records: Vec<Bytes> = vec![
        Bytes::from_static(b"alpha"),
        Bytes::from_static(b""),
        Bytes::from_static(b"a longer gamma record crossing a two byte varint boundary .............."),
    ];
    for (i, r) in records.iter().enumerate() {
        store
            .enqueue(&queue_id, UintN::from(i as u64), r.clone())
            .unwrap();
    }
    store.close().await.unwrap();

    let wal_dir = queue_id.to_wal_dir(tmp_dir.path());

    // The file is genuinely V1: the header version word (u64 LE) is 1.
    let raw = tokio::fs::read(file_id.to_file_path(wal_dir.to_str().unwrap(), "wal"))
        .await
        .unwrap();
    assert_eq!(raw[0], 1, "new file must carry the V1 header version");

    // read_wal_file_range: ids are 0,1,2 derived from position, data intact.
    let (tx, mut rx) = mpsc::channel(10);
    let result = read_wal_file_range(
        &wal_dir,
        &file_id,
        &UintN::from(0u64),
        &Some(UintN::from(2u64)),
        1,
        &tx,
        DataSource::DiskWal,
    )
    .await
    .unwrap();
    drop(tx);
    assert!(matches!(result, ReadRangeResult::Complete));
    for (i, r) in records.iter().enumerate() {
        let e = timeout(Duration::from_secs(1), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(e.id, UintN::from(i as u64));
        assert_eq!(e.data, *r);
    }

    // get_wal_content + get_wal_range agree on the count and the id range.
    let content = get_wal_content(&wal_dir, &file_id).await.unwrap();
    assert_eq!(content.num_entries, UintN::from(3u64));
    assert_eq!(content.entries_before, UintN::from(0u64));

    let (_, range) = get_wal_range(&wal_dir, &file_id).await.unwrap();
    assert_eq!(range, Some((UintN::from(0u64), UintN::from(2u64))));

    // read_wal_bytes_range over the same content, with a step, hits the V1
    // in-memory path and its zero-copy record slices.
    let (tx2, mut rx2) = mpsc::channel(10);
    read_wal_bytes_range(
        &content.content,
        &UintN::from(0u64),
        &Some(UintN::from(2u64)),
        2, // step: ids 0 and 2
        &tx2,
        DataSource::DiskWal,
    )
    .await
    .unwrap();
    drop(tx2);
    let first = timeout(Duration::from_secs(1), rx2.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.id, UintN::from(0u64));
    assert_eq!(first.data, records[0]);
    let third = timeout(Duration::from_secs(1), rx2.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(third.id, UintN::from(2u64));
    assert_eq!(third.data, records[2]);
}

/// A truncated V1 tail is dropped: the scan stops at the last whole entry so
/// the derived ids of the surviving prefix stay correct.
#[tokio::test]
async fn test_v1_truncated_tail_is_dropped() {
    init_logger();
    let tmp_dir = tempdir().unwrap();
    let (written_sender, _) = mpsc::unbounded_channel();
    let (wal_complete_sender, _) = mpsc::unbounded_channel();
    let store = WalStore::new(tmp_dir.path(), written_sender, wal_complete_sender);

    let resolver = QueueIdResolver::new("test_instance");
    let queue_id = resolver.resolve("v1_trunc");
    let file_id = UintN::from(1u64);
    let settings = WalSettings {
        max_file_size: 4096,
        write_buffer_size: 128,
        enable_fsync: true,
        encryption_type: normfs_types::EncryptionType::Aes,
        compression_type: normfs_types::CompressionType::Zstd,
        wal_entry_format: WalEntryFormat::V1,
    };
    store
        .start_writer(&queue_id, &file_id, WalHeader::default(), settings, None)
        .await
        .unwrap();
    for i in 0..3u64 {
        store
            .enqueue(&queue_id, UintN::from(i), Bytes::from(format!("record-{i}")))
            .unwrap();
    }
    store.close().await.unwrap();

    // Lop 3 bytes off the end, corrupting the last entry's CRC/frame.
    let wal_dir = queue_id.to_wal_dir(tmp_dir.path());
    let path = file_id.to_file_path(wal_dir.to_str().unwrap(), "wal");
    let bytes = tokio::fs::read(&path).await.unwrap();
    tokio::fs::write(&path, &bytes[..bytes.len() - 3]).await.unwrap();

    // Only the first two entries survive; ids 0 and 1.
    let (_, range) = get_wal_range(&wal_dir, &file_id).await.unwrap();
    assert_eq!(range, Some((UintN::from(0u64), UintN::from(1u64))));

    let content = get_wal_content(&wal_dir, &file_id).await.unwrap();
    assert_eq!(content.num_entries, UintN::from(2u64));
}

/// A queue may hold a V0 file and a V1 file side by side; the reader dispatches
/// on each file's header version, so both read back correctly.
#[tokio::test]
async fn test_mixed_v0_and_v1_files_in_one_queue() {
    init_logger();
    let tmp_dir = tempdir().unwrap();
    let (written_sender, _) = mpsc::unbounded_channel();
    let (wal_complete_sender, _) = mpsc::unbounded_channel();
    let store = WalStore::new(tmp_dir.path(), written_sender, wal_complete_sender);

    let resolver = QueueIdResolver::new("test_instance");
    let queue_id = resolver.resolve("mixed_queue");
    let wal_dir = queue_id.to_wal_dir(tmp_dir.path());

    let base = WalSettings {
        max_file_size: 4096,
        write_buffer_size: 128,
        enable_fsync: true,
        encryption_type: normfs_types::EncryptionType::Aes,
        compression_type: normfs_types::CompressionType::Zstd,
        wal_entry_format: WalEntryFormat::V0,
    };

    // File 1: legacy V0, entries 0 and 1.
    let file1 = UintN::from(1u64);
    store
        .start_writer(&queue_id, &file1, WalHeader::default(), base.clone(), None)
        .await
        .unwrap();
    store
        .enqueue(&queue_id, UintN::from(0u64), Bytes::from_static(b"v0-zero"))
        .unwrap();
    store
        .enqueue(&queue_id, UintN::from(1u64), Bytes::from_static(b"v0-one"))
        .unwrap();
    store.close().await.unwrap();

    // File 2: V1, entries 2 and 3 (num_entries_before = 2).
    let file2 = UintN::from(2u64);
    let header2 = WalHeader::new(8, 4, UintN::from(2u64)).unwrap();
    let v1 = WalSettings {
        wal_entry_format: WalEntryFormat::V1,
        ..base
    };
    store
        .start_writer(&queue_id, &file2, header2, v1, Some(UintN::from(1u64)))
        .await
        .unwrap();
    store
        .enqueue(&queue_id, UintN::from(2u64), Bytes::from_static(b"v1-two"))
        .unwrap();
    store
        .enqueue(&queue_id, UintN::from(3u64), Bytes::from_static(b"v1-three"))
        .unwrap();
    store.close().await.unwrap();

    // Both files read back correctly via their own format.
    let raw1 = tokio::fs::read(file1.to_file_path(wal_dir.to_str().unwrap(), "wal"))
        .await
        .unwrap();
    let raw2 = tokio::fs::read(file2.to_file_path(wal_dir.to_str().unwrap(), "wal"))
        .await
        .unwrap();
    assert_eq!(raw1[0], 0, "file 1 must be V0");
    assert_eq!(raw2[0], 1, "file 2 must be V1");

    let (_, range1) = get_wal_range(&wal_dir, &file1).await.unwrap();
    assert_eq!(range1, Some((UintN::from(0u64), UintN::from(1u64))));

    let (_, range2) = get_wal_range(&wal_dir, &file2).await.unwrap();
    assert_eq!(range2, Some((UintN::from(2u64), UintN::from(3u64))));

    // Stream file 2's V1 entries and confirm the derived ids and data.
    let (tx, mut rx) = mpsc::channel(10);
    read_wal_file_range(
        &wal_dir,
        &file2,
        &UintN::from(2u64),
        &Some(UintN::from(3u64)),
        1,
        &tx,
        DataSource::DiskWal,
    )
    .await
    .unwrap();
    drop(tx);
    let e2 = timeout(Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(e2.id, UintN::from(2u64));
    assert_eq!(e2.data, Bytes::from_static(b"v1-two"));
    let e3 = timeout(Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(e3.id, UintN::from(3u64));
    assert_eq!(e3.data, Bytes::from_static(b"v1-three"));
}

// V0 twins of the tests above, which track the current default format.

/// V0 twin of `test_enqueue_and_read`.
#[tokio::test]
async fn test_enqueue_and_read_v0_compat() {
    init_logger();
    let tmp_dir = tempdir().unwrap();
    let instance_id = "test_instance";
    let (written_sender, _) = mpsc::unbounded_channel();
    let (wal_complete_sender, _) = mpsc::unbounded_channel();

    let store = WalStore::new(tmp_dir.path(), written_sender, wal_complete_sender);

    let resolver = QueueIdResolver::new(instance_id);
    let queue_id = resolver.resolve("test_queue");
    let file_id = UintN::from(1u64);
    let header = WalHeader::default();
    let settings = WalSettings {
        max_file_size: 1024,
        write_buffer_size: 128,
        enable_fsync: true,
        encryption_type: normfs_types::EncryptionType::Aes,
        compression_type: normfs_types::CompressionType::Zstd,
        wal_entry_format: WalEntryFormat::V0,
    };

    store
        .start_writer(&queue_id, &file_id, header, settings, None)
        .await
        .unwrap();

    store
        .enqueue(&queue_id, UintN::from(0u64), Bytes::from("hello"))
        .unwrap();
    store
        .enqueue(&queue_id, UintN::from(1u64), Bytes::from("world"))
        .unwrap();

    store.close().await.unwrap();

    let raw = tokio::fs::read(
        file_id.to_file_path(queue_id.to_wal_dir(tmp_dir.path()).to_str().unwrap(), "wal"),
    )
    .await
    .unwrap();
    assert_eq!(raw[0], 0, "compat file must carry the V0 header version");

    let (tx, mut rx) = mpsc::channel(10);
    let result = read_wal_file_range(
        &queue_id.to_wal_dir(tmp_dir.path()),
        &file_id,
        &UintN::from(0u64),
        &Some(UintN::from(1u64)),
        1,
        &tx,
        DataSource::DiskWal,
    )
    .await
    .unwrap();
    drop(tx);

    assert!(matches!(result, ReadRangeResult::Complete));

    let read_entry1 = timeout(Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read_entry1.id, UintN::from(0u64));
    assert_eq!(read_entry1.data, Bytes::from("hello"));
    assert_eq!(read_entry1.source, DataSource::DiskWal);

    let read_entry2 = timeout(Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read_entry2.id, UintN::from(1u64));
    assert_eq!(read_entry2.data, Bytes::from("world"));
    assert_eq!(read_entry2.source, DataSource::DiskWal);
}

/// V0 twin of `test_enqueue_batch_and_read`.
#[tokio::test]
async fn test_enqueue_batch_and_read_v0_compat() {
    init_logger();
    let tmp_dir = tempdir().unwrap();
    let instance_id = "test_instance";
    let (written_sender, _) = mpsc::unbounded_channel();
    let (wal_complete_sender, _) = mpsc::unbounded_channel();

    let store = WalStore::new(tmp_dir.path(), written_sender, wal_complete_sender);

    let resolver = QueueIdResolver::new(instance_id);
    let queue_id = resolver.resolve("test_queue");
    let file_id = UintN::from(1u64);
    let header = WalHeader::default();
    let settings = WalSettings {
        max_file_size: 1024,
        write_buffer_size: 128,
        enable_fsync: true,
        encryption_type: normfs_types::EncryptionType::Aes,
        compression_type: normfs_types::CompressionType::Zstd,
        wal_entry_format: WalEntryFormat::V0,
    };

    store
        .start_writer(&queue_id, &file_id, header, settings, None)
        .await
        .unwrap();

    let entries = vec![
        (UintN::from(0u64), Bytes::from("hello")),
        (UintN::from(1u64), Bytes::from("world")),
    ];
    store.enqueue_batch(&queue_id, entries).unwrap();

    store.close().await.unwrap();

    let (tx, mut rx) = mpsc::channel(10);
    let result = read_wal_file_range(
        &queue_id.to_wal_dir(tmp_dir.path()),
        &file_id,
        &UintN::from(0u64),
        &Some(UintN::from(1u64)),
        1,
        &tx,
        DataSource::DiskWal,
    )
    .await
    .unwrap();
    drop(tx);

    assert!(matches!(result, ReadRangeResult::Complete));

    let read_entry1 = timeout(Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read_entry1.id, UintN::from(0u64));
    assert_eq!(read_entry1.data, Bytes::from("hello"));

    let read_entry2 = timeout(Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read_entry2.id, UintN::from(1u64));
    assert_eq!(read_entry2.data, Bytes::from("world"));
}

/// V0 twin of `test_size_based_rotation`.
#[tokio::test]
async fn test_size_based_rotation_v0_compat() {
    init_logger();
    let tmp_dir = tempdir().unwrap();
    let instance_id = "test_instance";
    let (written_sender, _) = mpsc::unbounded_channel();
    let (wal_complete_sender, mut wal_complete_receiver) = mpsc::unbounded_channel();

    let store = WalStore::new(tmp_dir.path(), written_sender, wal_complete_sender);

    let resolver = QueueIdResolver::new(instance_id);
    let queue_id = resolver.resolve("test_queue");
    let file_id = UintN::from(1u64);
    let header = WalHeader::default();
    let settings = WalSettings {
        max_file_size: 128,
        write_buffer_size: 64,
        enable_fsync: true,
        encryption_type: normfs_types::EncryptionType::Aes,
        compression_type: normfs_types::CompressionType::Zstd,
        wal_entry_format: WalEntryFormat::V0,
    };

    store
        .start_writer(&queue_id, &file_id, header, settings, None)
        .await
        .unwrap();

    store
        .enqueue(&queue_id, UintN::from(0u64), Bytes::from(vec![0; 64]))
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    store
        .enqueue(&queue_id, UintN::from(1u64), Bytes::from(vec![0; 64]))
        .unwrap();

    store.close().await.unwrap();

    let received = wal_complete_receiver.recv().await.unwrap();
    assert_eq!(received.queue_id, queue_id);
    assert_eq!(received.file_id, file_id);

    let content1 = get_wal_content(&queue_id.to_wal_dir(tmp_dir.path()), &file_id)
        .await
        .unwrap();
    assert_eq!(content1.num_entries, UintN::from(1u64));

    let content2 = get_wal_content(&queue_id.to_wal_dir(tmp_dir.path()), &file_id.increment())
        .await
        .unwrap();
    assert_eq!(content2.num_entries, UintN::from(1u64));
}

/// V1 counterpart to `test_data_size_based_rotation`: no width to overflow, so
/// the oversized record stays put and `max_file_size` is the only trigger left.
#[tokio::test]
async fn test_v1_rotates_on_file_size_not_field_width() {
    init_logger();
    let tmp_dir = tempdir().unwrap();
    let (written_sender, _) = mpsc::unbounded_channel();
    let (wal_complete_sender, _) = mpsc::unbounded_channel();

    let store = WalStore::new(tmp_dir.path(), written_sender, wal_complete_sender);

    let resolver = QueueIdResolver::new("test_instance");
    let queue_id = resolver.resolve("test_queue");
    let file_id = UintN::from(1u64);
    let header = WalHeader {
        data_size_bytes: 1,
        ..Default::default()
    };
    let settings = WalSettings {
        max_file_size: 4096, // far above the total written below
        write_buffer_size: 128,
        enable_fsync: true,
        encryption_type: normfs_types::EncryptionType::Aes,
        compression_type: normfs_types::CompressionType::Zstd,
        wal_entry_format: WalEntryFormat::V1,
    };

    store
        .start_writer(&queue_id, &file_id, header, settings, None)
        .await
        .unwrap();

    store
        .enqueue(&queue_id, UintN::from(0u64), Bytes::from("a"))
        .unwrap();
    // 512 bytes does not fit a 1-byte-wide size field. Under V0 this rotates.
    store
        .enqueue(&queue_id, UintN::from(1u64), Bytes::from(vec![b'x'; 512]))
        .unwrap();

    store.close().await.unwrap();

    let wal_dir = queue_id.to_wal_dir(tmp_dir.path());

    let content1 = get_wal_content(&wal_dir, &file_id).await.unwrap();
    assert_eq!(
        content1.num_entries,
        UintN::from(2u64),
        "V1 has no fixed-width size field to overflow, so both entries belong to file 1"
    );

    let file_2 = UintN::from(2u64).to_file_path(wal_dir.to_str().unwrap(), "wal");
    assert!(
        tokio::fs::metadata(&file_2).await.is_err(),
        "no rotation should have happened, but file 2 exists"
    );

    let (tx, mut rx) = mpsc::channel(10);
    read_wal_file_range(
        &wal_dir,
        &file_id,
        &UintN::from(1u64),
        &Some(UintN::from(1u64)),
        1,
        &tx,
        DataSource::DiskWal,
    )
    .await
    .unwrap();
    drop(tx);
    let entry = timeout(Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(entry.id, UintN::from(1u64));
    assert_eq!(entry.data, Bytes::from(vec![b'x'; 512]));
}
