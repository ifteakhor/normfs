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
        tokio::spawn(async move { pool.append(&RECORD).await })
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

    let id = tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .expect("append should resume once a page is reclaimable")
        .expect("task panicked")
        .expect("append should succeed");
    assert_eq!(id, n, "the waiting record keeps the next id in sequence");
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
    let outcome = tokio::time::timeout(Duration::from_secs(2), pool.append(&huge))
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

    // Taken once: a second call has nothing to give, so the writer cannot
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

// ---------------------------------------------------------------------------
// Rotation and the page boundary.
//
// A page's bytes go to the file unchanged, so a page whose records belong to
// two different files cannot be written to either: there is no boundary inside
// a page to split at. Two mechanisms keep that from happening, and each of the
// tests below dies if one of them is removed:
//
//   seal_active()   the record that opens a file starts a fresh page
//   accept_below    a flush stops at the end of the file that is open
//
// The pool used here is armed with a small file so rotation happens after a
// couple of records rather than 128 MiB in.
// ---------------------------------------------------------------------------

/// Encoded size of `RECORD` as the pool charges it.
fn record_charge() -> u64 {
    crate::wal_entry_v1::encoded_len(RECORD.len() as u32) as u64
}

/// A pool with room for several records and a file that holds only two.
fn armed_pool(header_len: u64) -> Arc<PagePool> {
    let pool = Arc::new(PagePool::new(4, PAGE_SIZE, 0));
    pool.arm_file_fill(header_len + record_charge() * 2, header_len);
    pool
}

#[tokio::test]
async fn a_page_never_holds_entries_for_two_files() {
    const HEADER: u64 = 16;
    let pool = armed_pool(HEADER);

    // Drive the same sequence the enqueue path drives: charge, seal if the
    // charge says so, then append. Record which file each id was charged to.
    let mut file_of: Vec<(u64, u64)> = Vec::new();
    for _ in 0..8 {
        let id = pool.next_entry_id();
        let decision = pool.charge(record_charge());
        if decision.rotate_before {
            pool.seal_active();
        }
        pool.append_at(id, &RECORD).await.unwrap();
        file_of.push((id, decision.epoch));
    }

    assert!(
        file_of.iter().any(|(_, epoch)| *epoch > 0),
        "the file should have rotated at least once with only two records per file"
    );

    // No page may span two files.
    pool.with_ring(|ring| {
        for k in 0..ring.page_count() {
            let (Some(first), Some(last)) = (ring.page_first_entry_id(k), ring.page_last_entry_id(k))
            else {
                continue;
            };
            let epoch_of = |id: u64| file_of.iter().find(|(i, _)| *i == id).map(|(_, e)| *e);
            assert_eq!(
                epoch_of(first),
                epoch_of(last),
                "page {k} holds entries {first}..={last}, which were charged to different files: \
                 its bytes belong to neither"
            );
        }
    });
}

#[tokio::test]
async fn a_bounded_take_stops_at_the_file_boundary() {
    const HEADER: u64 = 16;
    let pool = Arc::new(PagePool::new(4, PAGE_SIZE, 0));
    pool.arm_file_fill(1 << 20, HEADER);

    // Three records in the file that is open.
    for id in 0..3u64 {
        pool.charge(record_charge());
        pool.append_at(id, &RECORD).await.unwrap();
    }

    // The fourth opens a new file.
    pool.seal_active();
    assert_eq!(pool.accept_below(), Some(3));
    pool.append_at(3, &RECORD).await.unwrap();

    // The old file's flush must stop at entry 2.
    let pending = pool.take_pending();
    assert!(!pending.is_empty(), "the first three entries should be ready");
    let highest = pending.iter().map(|(w, _)| w.last_entry_id).max().unwrap();
    assert_eq!(
        highest, 2,
        "a flush of the old file must not take entry 3, which belongs to the next one"
    );

    // And the watermark it can advance stops there too, so a page holding
    // entry 3 is not handed back for reuse on the strength of the old file's
    // fsync.
    pool.mark_durable(highest + 1);
    assert_eq!(pool.durable_before(), 3);

    // The new file opens; now entry 3 is takeable, exactly once.
    pool.advance_file();
    let second = pool.take_pending();
    let ids: Vec<u64> = second.iter().map(|(w, _)| w.last_entry_id).collect();
    assert_eq!(ids, vec![3], "the new file gets entry 3, and only entry 3");
    assert!(
        pool.take_pending().is_empty(),
        "nothing may be handed out twice"
    );
}

#[tokio::test]
async fn the_first_entry_of_a_file_never_rotates() {
    const HEADER: u64 = 16;

    // A file that holds exactly two records. The record that opens a file must
    // not rotate again on the strength of the space the previous file's records
    // took, or every file would be born empty.
    let pool = Arc::new(PagePool::new(4, PAGE_SIZE, 0));
    pool.arm_file_fill(HEADER + record_charge() * 2, HEADER);
    let epochs: Vec<(bool, u64)> = (0..6)
        .map(|_| {
            let d = pool.charge(record_charge());
            (d.rotate_before, d.epoch)
        })
        .collect();
    assert_eq!(
        epochs,
        vec![
            (false, 0), // opens file 0
            (false, 0), // fills it
            (true, 1),  // does not fit: opens file 1
            (false, 1), // fills it -- the point of the test
            (true, 2),
            (false, 2),
        ],
        "each file must take two records before rotating again"
    );

    // And the guard holds even when a single record is larger than the whole
    // file: it goes into the file it finds rather than rotating into an empty
    // one, which would rotate forever.
    let tiny = Arc::new(PagePool::new(4, PAGE_SIZE, 0));
    tiny.arm_file_fill(HEADER + 4, HEADER);
    let first = tiny.charge(record_charge());
    assert!(
        !first.rotate_before && first.epoch == 0,
        "a record larger than max_file_size still opens the first file rather than skipping it"
    );
    // Each subsequent oversized record gets a file of its own -- one rotation
    // each, never two for the same record.
    for expected in 1..=3u64 {
        let d = tiny.charge(record_charge());
        assert_eq!((d.rotate_before, d.epoch), (true, expected));
    }
}

#[tokio::test]
async fn the_fill_counts_each_entry_exactly_once() {
    const HEADER: u64 = 16;
    let pool = Arc::new(PagePool::new(4, PAGE_SIZE, 0));
    pool.arm_file_fill(1 << 20, HEADER);

    // Asserted as a number rather than inferred from where the file boundary
    // lands: the pooled path ignores AckFileWriter::current_size entirely, so a
    // second, redundant count there would not move any boundary and a test that
    // compared boundaries would pass with the double count still in place.
    for n in 1..=5u64 {
        pool.charge(record_charge());
        assert_eq!(
            pool.fill_used(),
            Some(HEADER + record_charge() * n),
            "after {n} records the file should hold its header plus {n} encoded entries"
        );
    }
}

/// A seal must outlive a wait for a page.
///
/// The discriminating case is an active page that *has room*. A sealed append
/// must refuse that room and rotate — waiting if no page can be reclaimed —
/// while an unsealed one would take it and put the new file's first record on
/// the tail of a page belonging to the old file. If the seal were dropped when
/// the first attempt came back `Full`, the retry would find that room and use
/// it, so this test fails on the wait rather than on the placement.
#[tokio::test]
async fn a_sealed_append_waits_and_still_lands_on_a_fresh_page() {
    // Three pages, each holding two records. Five records leave pages 0 and 1
    // full and page 2 active with room for one more. Nothing is durable, so no
    // page can be reclaimed.
    let pool = Arc::new(PagePool::new(3, PAGE_SIZE, 0));
    for _ in 0..5 {
        assert!(matches!(pool.try_append(&RECORD), AppendOutcome::Cached(_)));
    }
    let n = pool.next_entry_id();
    assert_eq!(n, 5);
    pool.with_ring(|ring| {
        assert_eq!(
            ring.page_len(ring.active_page()),
            1,
            "the active page must have room left, or this proves nothing"
        );
    });

    pool.seal_active();

    let waiter = {
        let pool = pool.clone();
        tokio::spawn(async move { pool.append_at(n, &RECORD).await })
    };
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(
        !waiter.is_finished(),
        "the sealed record must wait for a page of its own rather than take the room \
         left on a page that belongs to the previous file"
    );

    // Free page 0. The record must land on it as the sole occupant.
    pool.mark_durable(2);
    tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .expect("the append should resume once a page is free")
        .expect("task panicked")
        .expect("the record fits a page");

    pool.with_ring(|ring| {
        let active = ring.active_page();
        assert_eq!(
            ring.page_first_entry_id(active),
            Some(n),
            "the record that opens a file must be the first entry on its page"
        );
        assert_eq!(ring.page_len(active), 1, "and the only one on it so far");
    });
}

/// A writer never emits records that were in the pool before it started.
///
/// A queue upgraded from readonly to write keeps its `MemQueue`, and with it a
/// pool that may already hold records -- cached by reads, or left by the
/// previous writer. Those have no entry in the new file and no index to be
/// placed at, and V1's positional ids turn an unaccounted record into every
/// later entry reading back under the wrong id.
#[tokio::test]
async fn arming_a_writer_does_not_adopt_what_the_pool_already_held() {
    let pool = pool();
    fill(&pool);
    assert!(
        !pool.take_pending().is_empty() || pool.next_entry_id() > 0,
        "the pool should be holding records before the writer arms"
    );

    // Put a fresh run in that a previous writer had not taken.
    pool.mark_durable(pool.next_entry_id());
    assert!(matches!(pool.try_append(&RECORD), AppendOutcome::Cached(_)));
    assert!(
        !pool.take_pending().is_empty(),
        "that run is pending a write"
    );
    assert!(matches!(pool.try_append(&RECORD), AppendOutcome::Cached(_)));

    // A writer starts now. Whatever is already there is not its to write.
    pool.arm_file_fill(1 << 20, 16);
    assert!(
        pool.take_pending().is_empty(),
        "a newly armed writer must not adopt records it has no header entry for"
    );

    // What it is told about afterwards still reaches the file.
    let id = pool.next_entry_id();
    pool.charge(record_charge());
    pool.append_at(id, &RECORD).await.unwrap();
    let pending = pool.take_pending();
    assert_eq!(
        pending.iter().map(|(w, _)| w.last_entry_id).max(),
        Some(id),
        "records enqueued after arming are written normally"
    );
}

/// Stepping over a record too large for a page must not wait for a page.
///
/// `skip_to` waits until the pool has nothing left to lose before it re-seeds.
/// Testing that by the durable watermark alone hangs: `reinit` resets the
/// watermark to zero while moving `next_entry_id` forward, so after one
/// oversized record the watermark can never catch up -- and when every record
/// is oversized nothing ever enters a page, so nothing can ever report one
/// durable. An empty pool has nothing to lose and must proceed at once.
#[tokio::test]
async fn every_record_being_oversized_does_not_stall_the_queue() {
    let pool = Arc::new(PagePool::new(PAGE_COUNT, PAGE_SIZE, 0));
    pool.set_drainer();
    pool.arm_file_fill(1 << 20, 16);

    let huge = vec![0u8; PAGE_SIZE * 4];

    // Ten in a row, each stepped over. Nothing reports anything durable,
    // because nothing ever reaches a page.
    let run = async {
        for id in 0..10u64 {
            pool.charge(huge.len() as u64);
            assert_eq!(
                pool.append_at(id, &huge).await,
                Err(PoolError::TooLarge),
                "a record larger than a page is never cached"
            );
            pool.skip_to(id + 1).await;
            assert_eq!(
                pool.next_entry_id(),
                id + 1,
                "the id sequence steps over the record the pool refused"
            );
        }
    };
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("stepping over oversized records must not wait for a page");
}

// ---------------------------------------------------------------------------
// Streaming reads borrow from pages.
//
// `pin_range` hands out payloads that point into the pages the records were
// written into, rather than copies of them. What makes that sound is the pin:
// `normfs_wal_ring_rotate_to` requires `normfs_wal_page_is_reusable`, whose
// first conjunct is `pin_count == 0`, so a page a reader holds cannot be reset
// under it. These tests are that conjunct doing its job.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_pinned_page_is_not_reused_while_a_reader_holds_it() {
    let pool = pool();
    let n = fill(&pool);
    assert!(n >= 2);

    // Borrow entry 0 and report everything durable. Durability alone would
    // make every page reclaimable -- the pin is the only thing holding this
    // one, which is exactly what is being tested.
    let held = pool.pin_range(0, 0);
    assert_eq!(held.len(), 1, "entry 0 should be held in memory");
    let borrowed = held[0].1.clone();
    assert_eq!(&borrowed[..], &RECORD[..], "the payload reads back");
    pool.mark_durable(n);

    // The page holding entry 0 must not be handed out for reuse.
    let pinned_page = pool
        .with_ring(|ring| (0..ring.page_count()).find(|&k| ring.page_pin_count(k) > 0))
        .expect("a page should be pinned");

    for _ in 0..(2 * PAGE_COUNT + 2) {
        let _ = pool.try_append(&RECORD);
        let still_there = pool.with_ring(|ring| ring.page_first_entry_id(pinned_page));
        assert_eq!(
            still_there,
            Some(0),
            "a page a reader is holding was recycled under it"
        );
    }

    // The bytes the reader is looking at are unchanged.
    assert_eq!(&borrowed[..], &RECORD[..]);

    // Dropping the last reader releases it.
    drop(held);
    drop(borrowed);
    assert_eq!(
        pool.with_ring(|ring| ring.page_pin_count(pinned_page)),
        0,
        "dropping the payload must release the pin"
    );
    assert!(
        matches!(pool.try_append(&RECORD), AppendOutcome::Cached(_)),
        "and the page becomes reusable again"
    );
}

#[tokio::test]
async fn every_pin_is_released_even_when_payloads_are_dropped_unread() {
    let pool = pool();
    fill(&pool);

    // A stream that starts and is abandoned: the payloads go out of scope
    // without ever being looked at, and the pins must go with them.
    for _ in 0..5 {
        let batch = pool.pin_range(0, u64::MAX);
        assert!(!batch.is_empty());
        drop(batch);
    }

    let pinned: u32 = pool.with_ring(|ring| {
        (0..ring.page_count())
            .map(|k| ring.page_pin_count(k))
            .sum()
    });
    assert_eq!(pinned, 0, "a dropped payload leaks a pin, and a leaked pin holds the page for good");
}
