//! A pooled record must not be held twice while it waits for the writer.
//!
//! The pool copies the record's bytes at `enqueue` (into a page, or framed
//! whole when it is wider than one), so the `Bytes` sent to the WAL writer's
//! channel would be a second hold of every record in flight. The writer only
//! ever needs the record's length, which rides in the `Placement` instead.
//!
//! These tests run on tokio's single-threaded scheduler and `enqueue` sends to
//! the channel after its last await, so when it returns the writer task cannot
//! have drained the message yet: a payload riding the channel is still alive
//! and `is_unique` catches it deterministically.

use bytes::Bytes;
use normfs::{NormFS, NormFsSettings};

#[tokio::test]
async fn an_enqueued_record_is_not_held_by_the_writer_channel() {
    let temp = tempfile::TempDir::new().unwrap();
    let fs = NormFS::new(temp.path().to_path_buf(), NormFsSettings::default())
        .await
        .unwrap();
    let queue = fs.resolve("hold");
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();

    let record = Bytes::from(vec![7u8; 1024]);
    fs.enqueue(&queue, record.clone()).await.unwrap();
    assert!(
        record.is_unique(),
        "the caller's Bytes must be the last reference: the pool copied the \
         record at enqueue, so a clone in the writer channel is a double hold"
    );

    // Wider than a page: held whole by the pool, framed once. Same rule.
    let oversize = Bytes::from(vec![9u8; 300 * 1024]);
    fs.enqueue(&queue, oversize.clone()).await.unwrap();
    assert!(
        oversize.is_unique(),
        "an oversized record is framed into the pool's own buffer, so nothing \
         but the caller may still hold the original"
    );

    fs.close().await.unwrap();
}
