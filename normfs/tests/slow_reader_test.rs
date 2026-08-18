//! A client that reads slowly must not stop the queue being written to.
//!
//! Payloads read from memory are not copies: they point into the pages the
//! records were written into, and hold a pin on those pages for as long as the
//! payload lives. A payload lives until the client has been sent it — which for
//! a client that has stopped reading its socket is indefinitely. So the read
//! side can hold the write side's memory, and the only question is how much of
//! it.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use normfs::{NormFS, NormFsSettings, ReadPosition};
use tokio::sync::mpsc;
use uintn::UintN;

/// Enough records to fill the queue's pages several times over, so the writer
/// has to keep reclaiming them while the reader holds what it holds.
const RECORDS: usize = 4000;
const PAYLOAD: usize = 1024;

fn settings() -> NormFsSettings {
    let mut settings = NormFsSettings::default();
    // A small arena, so a handful of records is enough to make the pool turn
    // over rather than never filling in the first place.
    settings.max_memory_usage = 4 * 1024 * 1024;
    settings.wal_settings.write_interval = Duration::from_millis(20);
    settings
}

fn payload(i: usize) -> Bytes {
    let mut v = vec![0u8; PAYLOAD];
    v[..8].copy_from_slice(&(i as u64).to_le_bytes());
    Bytes::from(v)
}

/// A reader that never drains, while the queue keeps being written to.
///
/// The reader's channel has room for one entry and nothing takes it, so its
/// payloads — and the pins on the pages behind them — stay alive for the whole
/// of the write. The writes must still finish.
#[tokio::test]
async fn a_reader_that_never_drains_does_not_stop_the_writes() {
    let temp = tempfile::TempDir::new().unwrap();
    let fs = Arc::new(
        NormFS::new(temp.path().to_path_buf(), settings())
            .await
            .unwrap(),
    );
    let queue = fs.resolve("slow-reader");
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();

    // Something to read.
    for i in 0..200 {
        fs.enqueue(&queue, payload(i)).await.unwrap();
    }

    // A reader that asks for everything and then stops taking it. Held for the
    // whole test: dropping it would release the pins and prove nothing.
    let (tx, _rx) = mpsc::channel(1);
    let reader = {
        let fs = Arc::clone(&fs);
        let queue = queue.clone();
        tokio::spawn(async move {
            let _ = fs
                .read(&queue, ReadPosition::Absolute(UintN::zero()), 200, 1, tx)
                .await;
        })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;

    let started = Instant::now();
    for i in 200..RECORDS {
        let write = fs.enqueue(&queue, payload(i));
        tokio::time::timeout(Duration::from_secs(30), write)
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "write {i} did not complete in 30s while a reader held its payloads: \
                     the read side is holding memory the write side needs"
                )
            })
            .unwrap();
    }
    let elapsed = started.elapsed();

    reader.abort();
    fs.close().await.unwrap();

    // Not a throughput assertion — machines differ — but a queue whose writer is
    // waiting on a reader rather than on the disk does not finish this at all.
    // What keeps it moving is two things together: the reader can hold at most
    // its share of the pages (see a_reader_may_hold_only_its_share_of_the_pool,
    // which is what pins the share itself), and an appender that has to wait
    // asks for a flush rather than waiting for the writer's next tick.
    assert!(
        elapsed < Duration::from_secs(60),
        "{} writes took {elapsed:?} behind a reader that never drained",
        RECORDS - 200
    );
}

/// Everything written behind a stalled reader is still there, in order.
///
/// Back-pressure that drops or reorders is not back-pressure. This is the same
/// scenario as above, read back from a fresh instance.
#[tokio::test]
async fn nothing_written_behind_a_stalled_reader_is_lost_or_reordered() {
    const COUNT: usize = 800;

    let temp = tempfile::TempDir::new().unwrap();
    let path = temp.path().to_path_buf();

    let instance = {
        let fs = Arc::new(NormFS::new(path.clone(), settings()).await.unwrap());
        let queue = fs.resolve("stalled-reader");
        fs.ensure_queue_exists_for_write(&queue).await.unwrap();

        for i in 0..100 {
            fs.enqueue(&queue, payload(i)).await.unwrap();
        }

        let (tx, _rx) = mpsc::channel(1);
        let reader = {
            let fs = Arc::clone(&fs);
            let queue = queue.clone();
            tokio::spawn(async move {
                let _ = fs
                    .read(&queue, ReadPosition::Absolute(UintN::zero()), 100, 1, tx)
                    .await;
            })
        };
        tokio::time::sleep(Duration::from_millis(100)).await;

        for i in 100..COUNT {
            let id = fs.enqueue(&queue, payload(i)).await.unwrap();
            assert_eq!(
                id,
                UintN::from(i as u64),
                "ids must stay dense and in order"
            );
        }

        reader.abort();
        let instance = fs.get_instance_id().to_string();
        fs.close().await.unwrap();
        instance
    };
    let _ = instance;

    let fs = NormFS::new(path, settings()).await.unwrap();
    let queue = fs.resolve("stalled-reader");
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();

    let (tx, mut rx) = mpsc::channel(COUNT + 16);
    fs.read(
        &queue,
        ReadPosition::Absolute(UintN::zero()),
        COUNT as u64,
        1,
        tx,
    )
    .await
    .unwrap();

    let mut got = Vec::with_capacity(COUNT);
    while got.len() < COUNT {
        match tokio::time::timeout(Duration::from_secs(10), rx.recv()).await {
            Ok(Some(entry)) => got.push(entry),
            _ => break,
        }
    }

    assert_eq!(got.len(), COUNT, "every record written must read back");
    for (i, entry) in got.iter().enumerate() {
        assert_eq!(entry.id, UintN::from(i as u64), "id at position {i}");
        assert_eq!(entry.data, payload(i), "payload at position {i}");
    }

    fs.close().await.unwrap();
}
