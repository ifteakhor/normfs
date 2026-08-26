//! Closing a queue for good.
//!
//! Close is a promise with three parts: no write is ever accepted again (not
//! even after a restart), everything already written stays readable, and a
//! follower is told "that was the last record" instead of waiting forever.
//! Each test pins one part; together they are what makes the promise safe to
//! rely on.

use std::time::Duration;

use bytes::Bytes;
use normfs::{Error, NormFS, NormFsSettings, ReadPosition};
use tokio::sync::mpsc;
use tokio::time::timeout;
use uintn::UintN;

async fn write_records(fs: &NormFS, queue: &normfs::QueueId, count: u64) {
    for i in 0..count {
        fs.enqueue(queue, Bytes::from(format!("record-{i}")))
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn a_closed_queue_refuses_writes_and_keeps_its_data_readable() {
    let temp = tempfile::TempDir::new().unwrap();
    let fs = NormFS::new(temp.path().to_path_buf(), NormFsSettings::all_active())
        .await
        .unwrap();
    let queue = fs.resolve("doomed");
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
    write_records(&fs, &queue, 5).await;

    fs.close_queue(&queue).await.unwrap();

    let refused = fs.enqueue(&queue, Bytes::from_static(b"late")).await;
    assert!(
        matches!(refused, Err(Error::QueueClosed)),
        "a write after close must be refused as QueueClosed, got {refused:?}"
    );

    let (tx, mut rx) = mpsc::channel(16);
    fs.read(&queue, ReadPosition::Absolute(UintN::zero()), 5, 1, tx)
        .await
        .unwrap();
    for i in 0..5u64 {
        let entry = timeout(Duration::from_secs(5), rx.recv())
            .await
            .unwrap_or_else(|_| panic!("record {i} must stay readable after close"))
            .expect("the stream ended early");
        assert_eq!(entry.data, Bytes::from(format!("record-{i}")));
    }

    fs.close().await.unwrap();
}

#[tokio::test]
async fn a_close_survives_a_restart() {
    let temp = tempfile::TempDir::new().unwrap();
    {
        let fs = NormFS::new(temp.path().to_path_buf(), NormFsSettings::all_active())
            .await
            .unwrap();
        let queue = fs.resolve("doomed");
        fs.ensure_queue_exists_for_write(&queue).await.unwrap();
        write_records(&fs, &queue, 3).await;
        fs.close_queue(&queue).await.unwrap();
        fs.close().await.unwrap();
    }

    let fs = NormFS::new(temp.path().to_path_buf(), NormFsSettings::all_active())
        .await
        .unwrap();
    let queue = fs.resolve("doomed");

    let reopened = fs.ensure_queue_exists_for_write(&queue).await;
    assert!(
        matches!(reopened, Err(Error::QueueClosed)),
        "a restart must not resurrect a closed queue, got {reopened:?}"
    );

    fs.ensure_queue_exists_for_read(&queue).await.unwrap();
    let (tx, mut rx) = mpsc::channel(16);
    fs.read(&queue, ReadPosition::Absolute(UintN::zero()), 3, 1, tx)
        .await
        .unwrap();
    for i in 0..3u64 {
        let entry = timeout(Duration::from_secs(5), rx.recv())
            .await
            .unwrap_or_else(|_| panic!("record {i} must stay readable across the restart"))
            .expect("the stream ended early");
        assert_eq!(entry.data, Bytes::from(format!("record-{i}")));
    }

    fs.close().await.unwrap();
}

#[tokio::test]
async fn a_follow_on_a_closed_queue_ends_at_the_last_record() {
    let temp = tempfile::TempDir::new().unwrap();
    {
        let fs = NormFS::new(temp.path().to_path_buf(), NormFsSettings::all_active())
            .await
            .unwrap();
        let queue = fs.resolve("doomed");
        fs.ensure_queue_exists_for_write(&queue).await.unwrap();
        write_records(&fs, &queue, 3).await;
        fs.close_queue(&queue).await.unwrap();
        fs.close().await.unwrap();
    }

    let fs = NormFS::new(temp.path().to_path_buf(), NormFsSettings::all_active())
        .await
        .unwrap();
    let queue = fs.resolve("doomed");
    fs.ensure_queue_exists_for_read(&queue).await.unwrap();

    // Limit 0 is a follow. On a live queue it would subscribe and wait; on a
    // closed one it must deliver the backlog and end.
    let (tx, mut rx) = mpsc::channel(16);
    fs.read(&queue, ReadPosition::Absolute(UintN::zero()), 0, 1, tx)
        .await
        .unwrap();
    for i in 0..3u64 {
        let entry = timeout(Duration::from_secs(5), rx.recv())
            .await
            .unwrap_or_else(|_| panic!("backlog record {i} never arrived"))
            .expect("the stream ended inside the backlog");
        assert_eq!(entry.id, UintN::from(i));
    }
    let end = timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("the follow must end after the backlog of a closed queue, not wait forever");
    assert!(
        end.is_none(),
        "nothing can follow the last record of a closed queue, got {end:?}"
    );

    fs.close().await.unwrap();
}

#[tokio::test]
async fn a_live_follower_ends_when_the_queue_closes() {
    let temp = tempfile::TempDir::new().unwrap();
    let fs = NormFS::new(temp.path().to_path_buf(), NormFsSettings::all_active())
        .await
        .unwrap();
    let queue = fs.resolve("doomed");
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
    write_records(&fs, &queue, 2).await;

    let (tx, mut rx) = mpsc::channel(16);
    fs.read(&queue, ReadPosition::Absolute(UintN::zero()), 0, 1, tx)
        .await
        .unwrap();
    for i in 0..2u64 {
        let entry = timeout(Duration::from_secs(5), rx.recv())
            .await
            .unwrap_or_else(|_| panic!("record {i} never arrived on the follow"))
            .expect("the stream ended inside the backlog");
        assert_eq!(entry.id, UintN::from(i));
    }

    fs.close_queue(&queue).await.unwrap();

    let end = timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("closing the queue must end the live follow, not leave it waiting");
    assert!(end.is_none(), "no record can arrive after close, got {end:?}");

    fs.close().await.unwrap();
}

#[tokio::test]
async fn an_enqueue_on_a_never_started_queue_is_an_error_not_a_panic() {
    let temp = tempfile::TempDir::new().unwrap();
    let fs = NormFS::new(temp.path().to_path_buf(), NormFsSettings::all_active())
        .await
        .unwrap();
    let queue = fs.resolve("never-started");

    // No ensure call: this is the map-miss path a racing close also takes.
    let refused = fs.enqueue(&queue, Bytes::from_static(b"lost")).await;
    assert!(
        matches!(refused, Err(Error::QueueNotFound)),
        "a record the map cannot place must be refused, got {refused:?}"
    );

    fs.close().await.unwrap();
}

#[tokio::test]
async fn a_tail_follow_on_a_closed_queue_ends_at_the_last_record() {
    let temp = tempfile::TempDir::new().unwrap();
    {
        let fs = NormFS::new(temp.path().to_path_buf(), NormFsSettings::all_active())
            .await
            .unwrap();
        let queue = fs.resolve("doomed");
        fs.ensure_queue_exists_for_write(&queue).await.unwrap();
        write_records(&fs, &queue, 3).await;
        fs.close_queue(&queue).await.unwrap();
        fs.close().await.unwrap();
    }

    let fs = NormFS::new(temp.path().to_path_buf(), NormFsSettings::all_active())
        .await
        .unwrap();
    let queue = fs.resolve("doomed");
    fs.ensure_queue_exists_for_read(&queue).await.unwrap();

    // A follow one record back from the tail: ids 1 and 2, then the end.
    let (tx, mut rx) = mpsc::channel(16);
    fs.read(&queue, ReadPosition::ShiftFromTail(UintN::from(1u64)), 0, 1, tx)
        .await
        .unwrap();
    for expected in [1u64, 2] {
        let entry = timeout(Duration::from_secs(5), rx.recv())
            .await
            .unwrap_or_else(|_| panic!("record {expected} never arrived on the tail follow"))
            .expect("the stream ended inside the backlog");
        assert_eq!(entry.id, UintN::from(expected));
    }
    let end = timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("the tail follow must end after the last record of a closed queue");
    assert!(end.is_none(), "nothing can follow the last record, got {end:?}");

    fs.close().await.unwrap();
}

#[tokio::test]
async fn a_follow_after_an_in_process_close_delivers_the_backlog() {
    let temp = tempfile::TempDir::new().unwrap();
    let fs = NormFS::new(temp.path().to_path_buf(), NormFsSettings::all_active())
        .await
        .unwrap();
    let queue = fs.resolve("doomed");
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
    write_records(&fs, &queue, 3).await;
    fs.close_queue(&queue).await.unwrap();

    // Same process, no restart, no re-ensure: the queue is gone from the
    // memory map and only the closed record knows where the sequence ended.
    let (tx, mut rx) = mpsc::channel(16);
    fs.read(&queue, ReadPosition::Absolute(UintN::zero()), 0, 1, tx)
        .await
        .unwrap();
    for i in 0..3u64 {
        let entry = timeout(Duration::from_secs(5), rx.recv())
            .await
            .unwrap_or_else(|_| panic!("backlog record {i} never arrived"))
            .unwrap_or_else(|| panic!("the stream ended at record {i}, swallowing the backlog"));
        assert_eq!(entry.id, UintN::from(i));
    }
    let end = timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("the follow must end after the backlog");
    assert!(end.is_none());

    fs.close().await.unwrap();
}

#[tokio::test]
async fn a_batch_after_close_is_refused() {
    let temp = tempfile::TempDir::new().unwrap();
    let fs = NormFS::new(temp.path().to_path_buf(), NormFsSettings::all_active())
        .await
        .unwrap();
    let queue = fs.resolve("doomed");
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
    fs.close_queue(&queue).await.unwrap();

    let refused = fs
        .enqueue_batch(&queue, vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")])
        .await;
    assert!(
        matches!(refused, Err(Error::QueueClosed)),
        "a batch after close must be refused, got {refused:?}"
    );

    fs.close().await.unwrap();
}

#[tokio::test]
async fn a_second_close_is_ok_and_a_never_written_queue_closes() {
    let temp = tempfile::TempDir::new().unwrap();
    let fs = NormFS::new(temp.path().to_path_buf(), NormFsSettings::all_active())
        .await
        .unwrap();

    let queue = fs.resolve("silent");
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
    fs.close_queue(&queue).await.unwrap();
    fs.close_queue(&queue)
        .await
        .expect("a second close has nothing left to do and succeeds");

    let refused = fs.enqueue(&queue, Bytes::from_static(b"late")).await;
    assert!(matches!(refused, Err(Error::QueueClosed)));

    fs.close().await.unwrap();
}

#[tokio::test]
async fn a_closed_queues_file_migrates_to_the_store() {
    let temp = tempfile::TempDir::new().unwrap();
    let fs = NormFS::new(temp.path().to_path_buf(), NormFsSettings::all_active())
        .await
        .unwrap();
    let queue = fs.resolve("doomed");
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
    write_records(&fs, &queue, 3).await;
    fs.close_queue(&queue).await.unwrap();

    // Close is this file's only completion: no writer will ever rotate it
    // out, so if close does not migrate it, nothing ever will.
    let wal_dir = temp
        .path()
        .join(fs.get_instance_id())
        .join("doomed")
        .join("wal");
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let wal_files = std::fs::read_dir(&wal_dir)
            .map(|d| d.filter_map(|e| e.ok()).count())
            .unwrap_or(0);
        if wal_files == 0 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the closed queue's final WAL file never migrated to the store"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // And the data survives the migration.
    let (tx, mut rx) = mpsc::channel(16);
    fs.read(&queue, ReadPosition::Absolute(UintN::zero()), 3, 1, tx)
        .await
        .unwrap();
    for i in 0..3u64 {
        let entry = timeout(Duration::from_secs(5), rx.recv())
            .await
            .unwrap_or_else(|_| panic!("record {i} unreadable after migration"))
            .expect("the stream ended early");
        assert_eq!(entry.data, Bytes::from(format!("record-{i}")));
    }

    fs.close().await.unwrap();
}

#[tokio::test]
async fn a_queue_named_like_a_marker_does_not_close_its_parent() {
    let temp = tempfile::TempDir::new().unwrap();
    let fs = NormFS::new(temp.path().to_path_buf(), NormFsSettings::all_active())
        .await
        .unwrap();

    // "parent/closed" is a legitimate queue name; its *directory* must not
    // read as parent's closed *marker*.
    let inner = fs.resolve("parent/closed");
    fs.ensure_queue_exists_for_write(&inner).await.unwrap();
    fs.enqueue(&inner, Bytes::from_static(b"data")).await.unwrap();

    let parent = fs.resolve("parent");
    fs.ensure_queue_exists_for_write(&parent)
        .await
        .expect("a child queue named 'closed' must not close its parent");
    fs.enqueue(&parent, Bytes::from_static(b"alive")).await.unwrap();

    fs.close().await.unwrap();
}

#[tokio::test]
async fn a_zero_offset_tail_follow_on_a_closed_queue_delivers_nothing() {
    let temp = tempfile::TempDir::new().unwrap();
    let fs = NormFS::new(temp.path().to_path_buf(), NormFsSettings::all_active())
        .await
        .unwrap();
    let queue = fs.resolve("doomed");
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
    write_records(&fs, &queue, 3).await;
    fs.close_queue(&queue).await.unwrap();

    // Live semantics for ShiftFromTail(0) + follow are "from the next
    // record"; on a closed queue the next record can never exist.
    let (tx, mut rx) = mpsc::channel(16);
    fs.read(&queue, ReadPosition::ShiftFromTail(UintN::zero()), 0, 1, tx)
        .await
        .unwrap();
    let end = timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("a zero-offset tail follow on a closed queue must end at once");
    assert!(end.is_none(), "nothing follows the tail of a closed queue, got {end:?}");

    fs.close().await.unwrap();
}
