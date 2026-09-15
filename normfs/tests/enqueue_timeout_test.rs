//! A wait that runs out refuses the way `try_enqueue` does: no id taken.

use std::time::Duration;

use bytes::Bytes;
use normfs::{Error, NormFS, NormFsSettings};
use uintn::UintN;

/// Four pages, and a flush retry that outlasts the test.
fn settings() -> NormFsSettings {
    let mut settings = NormFsSettings::all_active();
    settings.mem_page_size = 256 * 1024;
    settings.max_memory_usage = 1024 * 1024;
    settings.wal_settings.flush_max_retries = u32::MAX;
    settings.wal_settings.flush_retry_delay = Duration::from_millis(20);
    settings
}

#[tokio::test]
async fn a_wait_that_runs_out_refuses_without_taking_an_id() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let normfs = NormFS::new(path.clone(), settings()).await.expect("normfs");
    let queue = normfs.resolve("bounded");
    normfs
        .ensure_queue_exists_for_write(&queue)
        .await
        .expect("queue");

    let first_file =
        UintN::from(1u64).to_file_path(queue.to_wal_dir(&path).to_str().unwrap(), "wal");
    normfs_wal::fail_flushes(&first_file, u32::MAX);

    // Nothing else holds the gate, so a refusal means the pool is full.
    let block = Bytes::from(vec![0u8; 200 * 1024]);
    let mut last_accepted = None;
    for _ in 0..64 {
        match normfs.try_enqueue(&queue, block.clone()) {
            Ok(id) => last_accepted = Some(id.to_u64().unwrap()),
            Err(Error::WouldBlock) => break,
            Err(e) => panic!("unexpected refusal: {e}"),
        }
    }
    let last_accepted = last_accepted.expect("at least one record was placed");

    let refused = tokio::time::timeout(
        Duration::from_secs(5),
        normfs.enqueue_timeout(&queue, block.clone(), Duration::from_millis(200)),
    )
    .await
    .expect("the bounded wait must end on its own");
    assert!(
        matches!(refused, Err(Error::WouldBlock)),
        "no page can come free while the flush fails; got {refused:?}"
    );

    normfs_wal::heal(&first_file);
    let next = normfs
        .enqueue_timeout(
            &queue,
            Bytes::from_static(b"after"),
            Duration::from_secs(10),
        )
        .await
        .expect("accepted once the healed writer frees a page");
    assert_eq!(
        next.to_u64().unwrap(),
        last_accepted + 1,
        "the refused record must not have taken an id"
    );
}
