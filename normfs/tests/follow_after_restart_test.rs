//! A follow started after a restart delivers the disk backlog and then stays
//! subscribed. Before this test's fixes it did one or the other: memory
//! subscribed with the backlog silently skipped, and once that was closed the
//! file path delivered the backlog and then terminated the stream.

use std::time::Duration;

use bytes::Bytes;
use normfs::{NormFS, NormFsSettings, ReadPosition};
use uintn::UintN;

#[tokio::test]
async fn a_follow_after_a_restart_gets_the_backlog_and_then_new_entries() {
    let temp = tempfile::TempDir::new().unwrap();
    let path = temp.path().to_path_buf();

    const BACKLOG: u64 = 5;
    {
        let fs = NormFS::new(path.clone(), NormFsSettings::all_active())
            .await
            .unwrap();
        let queue = fs.resolve("follow");
        fs.ensure_queue_exists_for_write(&queue).await.unwrap();
        for i in 0..BACKLOG {
            fs.enqueue(&queue, Bytes::from(format!("record-{i}")))
                .await
                .unwrap();
        }
        fs.close().await.unwrap();
    }

    let fs = NormFS::new(path.clone(), NormFsSettings::all_active())
        .await
        .unwrap();
    let queue = fs.resolve("follow");
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();

    // Limit 0 is a follow: everything from id 0, then whatever comes.
    let (tx, mut rx) = tokio::sync::mpsc::channel(16);
    fs.read(&queue, ReadPosition::Absolute(UintN::zero()), 0, 1, tx)
        .await
        .unwrap();

    for i in 0..BACKLOG {
        let entry = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .unwrap_or_else(|_| panic!("entry {i} of the backlog never arrived"))
            .expect("the stream ended inside the backlog");
        assert_eq!(entry.id, UintN::from(i));
        assert_eq!(entry.data, Bytes::from(format!("record-{i}")));
    }

    // The stream must still be live: a new entry arrives through it.
    let id = fs
        .enqueue(&queue, Bytes::from_static(b"after-the-backlog"))
        .await
        .unwrap();
    assert_eq!(id, UintN::from(BACKLOG));
    let entry = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("the follow delivered its backlog and then went silent instead of staying subscribed")
        .expect("the stream ended after the backlog instead of staying subscribed");
    assert_eq!(entry.id, UintN::from(BACKLOG));
    assert_eq!(entry.data, Bytes::from_static(b"after-the-backlog"));

    fs.close().await.unwrap();
}
