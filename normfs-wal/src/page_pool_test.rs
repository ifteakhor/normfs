//! Behaviour of the page pool under pressure.
//!
//! The point of these tests is the one thing the old cache got wrong: when
//! every page is occupied by records that are not yet on disk, an appender must
//! *wait*. The previous in-memory store called `reinit` and threw the cache
//! away, which is silent data loss for anything reading from memory.

use std::sync::Arc;
use std::time::Duration;

use crate::page_pool::{PagePool, PoolError};
use crate::wal_ring_v1::AppendOutcome;

// Two small pages, so the pool fills after a handful of records. A 16 B record
// frames to 1 + 16 + 4 = 21 bytes and costs a further 4 for its offset slot, so
// two fit in a 64 B page and the third has to rotate.
const PAGE_SIZE: usize = 64;
const PAGE_COUNT: usize = 2;
const RECORD: [u8; 16] = [0xAB; 16];

fn pool() -> Arc<PagePool> {
    Arc::new(PagePool::new(PAGE_COUNT, PAGE_SIZE, 0))
}

/// Fills every page, returning how many records it took.
fn fill(pool: &PagePool) -> u64 {
    let mut n = 0;
    loop {
        match pool.try_append(&RECORD) {
            AppendOutcome::Cached(_) => n += 1,
            AppendOutcome::Full => return n,
            AppendOutcome::TooLarge => panic!("16 B record should fit a 64 B page"),
        }
    }
}

#[tokio::test]
async fn try_append_reports_full_rather_than_discarding() {
    let pool = pool();
    let n = fill(&pool);
    assert!(n >= 2, "expected at least two records to fit, got {n}");
    assert_eq!(pool.try_append(&RECORD), AppendOutcome::Full);
    // The records that were accepted are still there: nothing was reset to make
    // room, which is what the old cache did.
    assert_eq!(pool.next_entry_id(), n);
}

#[tokio::test]
async fn append_waits_for_a_page_and_resumes_when_one_is_durable() {
    let pool = pool();
    let n = fill(&pool);

    let waiter = {
        let pool = Arc::clone(&pool);
        tokio::spawn(async move { pool.append_at(n, &RECORD).await })
    };

    // Give the task real time on the runtime, then observe that it has not
    // returned: no page can be reclaimed, because nothing is durable yet.
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(
        !waiter.is_finished(),
        "append returned while every page still held records that were not on disk"
    );

    // Reporting the first page durable frees it.
    pool.mark_durable(n);

    tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .expect("append_at should resume once a page is reclaimable")
        .expect("task panicked")
        .expect("append_at should succeed");
    assert_eq!(
        pool.next_entry_id(),
        n + 1,
        "the waiting record keeps the next id in sequence"
    );
}

#[tokio::test]
async fn nothing_accepted_is_ever_lost() {
    let pool = pool();
    // Three times what the pool holds at once, so pages must be recycled.
    let rounds = 3 * PAGE_COUNT as u64;
    let mut ids = Vec::new();

    for _ in 0..rounds {
        loop {
            match pool.try_append(&RECORD) {
                AppendOutcome::Cached(id) => {
                    ids.push(id);
                    break;
                }
                // Standing in for the file writer: everything appended so far
                // is on disk, so its pages may be reused.
                AppendOutcome::Full => pool.mark_durable(pool.next_entry_id()),
                AppendOutcome::TooLarge => unreachable!(),
            }
        }
    }

    // Ids are dense and in order — no gap where a record was dropped.
    let expected: Vec<u64> = (0..rounds).collect();
    assert_eq!(ids, expected);
}

#[tokio::test]
async fn a_record_larger_than_a_page_fails_instead_of_waiting() {
    let pool = pool();
    let huge = vec![0u8; PAGE_SIZE * 2];
    // No amount of waiting would ever make room, so this must not block.
    let outcome = tokio::time::timeout(
        Duration::from_secs(2),
        pool.append_at(pool.next_entry_id(), &huge),
    )
    .await
    .expect("TooLarge must not wait");
    assert_eq!(outcome, Err(PoolError::TooLarge));
}

#[tokio::test]
async fn pending_yields_each_byte_once_and_in_id_order() {
    let pool = pool();
    fill(&pool);

    let first = pool.take_pending();
    assert!(!first.is_empty(), "appended records should be pending a write");
    let total: usize = first.iter().map(|(_, b)| b.len()).sum();
    assert!(total > 0);
    for w in first.windows(2) {
        assert!(
            w[0].0.last_entry_id < w[1].0.last_entry_id,
            "pending runs must come out in id order"
        );
    }

    // Not written yet, so it must come back: an untried write must not lose
    // the run.
    assert_eq!(pool.take_pending(), first, "an uncommitted run must stay pending");

    // The writer commits each run once it is actually on disk.
    for (w, _) in &first {
        pool.commit_written(w.page_index, w.to);
    }

    // Committed once: a second call has nothing to give, so the writer cannot
    // append the same bytes to the file twice.
    assert!(pool.take_pending().is_empty());

    // A further record produces only the new bytes, not the whole page again.
    pool.mark_durable(pool.next_entry_id());
    assert!(matches!(pool.try_append(&RECORD), AppendOutcome::Cached(_)));
    let second = pool.take_pending();
    let second_total: usize = second.iter().map(|(_, b)| b.len()).sum();
    assert!(
        second_total < total,
        "only the newly appended run should be pending, got {second_total} of {total}"
    );
}

#[tokio::test]
async fn durable_watermark_only_moves_forward() {
    let pool = pool();
    fill(&pool);
    pool.mark_durable(2);
    assert_eq!(pool.durable_before(), 2);
    // A late or out-of-order report must not un-declare anything as durable:
    // the C ring would then be free to overwrite a page it had been told to
    // keep.
    pool.mark_durable(1);
    assert_eq!(pool.durable_before(), 2);
}
