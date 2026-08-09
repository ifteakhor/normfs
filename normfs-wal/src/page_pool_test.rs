//! Behaviour of the page pool under pressure.
//!
//! The point of these tests is the one thing the old cache got wrong: when
//! every page is occupied by records that are not yet on disk, an appender must
//! *wait*. The previous in-memory store called `reinit` and threw the cache
//! away, which is silent data loss for anything reading from memory.

use std::sync::Arc;
use std::time::Duration;

use crate::page_pool::{PagePool, Placement, PoolError, RotateHint};
use crate::wal_ring_v1::AppendOutcome;

// Two small pages, so the pool fills after a handful of records. A 16 B record
// frames to 1 + 16 + 4 = 21 bytes and costs a further 4 for its offset slot, so
// two fit in a 64 B page and the third has to rotate.
const PAGE_SIZE: usize = 64;
const PAGE_COUNT: usize = 2;
const RECORD: [u8; 16] = [0xAB; 16];
/// The widest a V1 header gets, which is what the pool is armed with here.
const HEADER: u64 = 16;
/// The widest a V1 header gets, as the pool is armed with it in these tests.

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
        tokio::spawn(async move { pool.place(n, &RECORD).await })
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
        .expect("place should resume once a page is reclaimable")
        .expect("task panicked")
        .expect("place should succeed");
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
        pool.place(pool.next_entry_id(), &huge),
    )
    .await
    .expect("TooLarge must not wait");
    assert_eq!(outcome, Err(PoolError::TooLarge));
}

#[tokio::test]
async fn pending_yields_each_byte_once_and_in_id_order() {
    let pool = pool();
    fill(&pool);

    let first = pool.take_pending(0);
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
    assert_eq!(pool.take_pending(0), first, "an uncommitted run must stay pending");

    // The writer commits each run once it is actually on disk.
    for (w, _) in &first {
        pool.commit_written(w.page_index, w.to);
    }

    // Committed once: a second call has nothing to give, so the writer cannot
    // append the same bytes to the file twice.
    assert!(pool.take_pending(0).is_empty());

    // A further record produces only the new bytes, not the whole page again.
    pool.mark_durable(pool.next_entry_id());
    assert!(matches!(pool.try_append(&RECORD), AppendOutcome::Cached(_)));
    let second = pool.take_pending(0);
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
// two different files could be written to neither: there is no boundary inside
// a page to split at. The file therefore ends where a page ends -- crossing
// max_file_size arms the rotation, and the next page to open carries it out --
// so a page belongs to one file by construction rather than by a seal.
//
// What a flush still needs is to know which pages are its own, because the
// enqueue side runs ahead of it. That is the epoch, stamped on a page as its
// first record lands.
//
// The pool used here is armed with a small file so rotation happens after a
// couple of pages rather than 128 MiB in.
// ---------------------------------------------------------------------------

/// Encoded size of `RECORD` as the pool charges it.
fn record_charge() -> u64 {
    crate::wal_entry_v1::encoded_len(RECORD.len() as u32) as u64
}

/// A pool whose file is smaller than one page, so every page rotates it.
fn armed_pool() -> Arc<PagePool> {
    let pool = Arc::new(PagePool::new(4, PAGE_SIZE, 0));
    pool.arm_file_fill(HEADER + record_charge(), HEADER);
    pool
}

/// Places the next record and frees its page, so the pool never blocks.
async fn place_next(pool: &PagePool) -> (u64, Placement) {
    let id = pool.next_entry_id();
    let placement = pool.place(id, &RECORD).await.unwrap();
    pool.mark_durable(id);
    (id, placement)
}

#[tokio::test]
async fn a_page_never_holds_entries_for_two_files() {
    let pool = armed_pool();

    // Drive what the enqueue path drives, and record which file each id landed
    // in. Freeing as we go, so the pool never blocks on a page.
    let mut file_of: Vec<(u64, u64)> = Vec::new();
    for _ in 0..8 {
        let (id, placement) = place_next(&pool).await;
        file_of.push((id, placement.epoch));
    }

    assert!(
        file_of.iter().any(|(_, epoch)| *epoch > 0),
        "a file smaller than a page should rotate on every page"
    );

    pool.with_ring(|ring| {
        for k in 0..ring.page_count() {
            let (Some(first), Some(last)) =
                (ring.page_first_entry_id(k), ring.page_last_entry_id(k))
            else {
                continue;
            };
            let epoch_of = |id: u64| file_of.iter().find(|(i, _)| *i == id).map(|(_, e)| *e);
            assert_eq!(
                epoch_of(first),
                epoch_of(last),
                "page {k} holds entries {first}..={last}, which belong to different files: \
                 its bytes belong to neither"
            );
        }
    });
}

/// A file is only ever allowed to end where a page ends.
///
/// Crossing `max_file_size` mid-page must not rotate: the record that crosses
/// it stays on the page it is on, and the rotation waits for the next page. A
/// rotation reported anywhere else means a page is about to be split.
#[tokio::test]
async fn a_file_ends_only_where_a_page_ends() {
    let pool = Arc::new(PagePool::new(4, PAGE_SIZE, 0));
    // One record over, so the threshold is crossed in the middle of page 0.
    pool.arm_file_fill(HEADER + record_charge(), HEADER);

    let mut rotations = 0;
    for _ in 0..8 {
        let id = pool.next_entry_id();
        let before = pool.with_ring(|r| r.active_page());
        let placement = pool.place(id, &RECORD).await.unwrap();
        let opened_page = pool.with_ring(|r| r.page_first_entry_id(r.active_page())) == Some(id);
        if placement.rotate == RotateHint::Before {
            rotations += 1;
            assert!(
                opened_page,
                "entry {id} rotated the file without opening a page (active {before} -> {})",
                pool.with_ring(|r| r.active_page())
            );
        }
        pool.mark_durable(id);
    }
    assert!(rotations > 0, "the file should have rotated at least once");
}

/// A flush takes the pages of its own file and no others.
#[tokio::test]
async fn a_flush_takes_only_its_own_files_pages() {
    let pool = armed_pool();

    // Fill until the file rotates, remembering what each file was given.
    let mut of_epoch: Vec<(u64, u64)> = Vec::new();
    while pool.epoch() < 2 {
        let (id, placement) = place_next(&pool).await;
        of_epoch.push((id, placement.epoch));
    }

    for epoch in 0..=pool.epoch() {
        let taken = pool.take_pending(epoch);
        let mut ids: Vec<u64> = taken.iter().map(|(w, _)| w.last_entry_id).collect();
        ids.sort_unstable();
        for id in &ids {
            let charged = of_epoch.iter().find(|(i, _)| i == id).map(|(_, e)| *e);
            assert_eq!(
                charged,
                Some(epoch),
                "file {epoch} was handed entry {id}, which belongs to file {charged:?}"
            );
        }
        // Committed once, so nothing reaches a file twice.
        for (w, _) in &taken {
            pool.commit_written(w.page_index, w.to);
        }
        assert!(
            pool.take_pending(epoch).is_empty(),
            "nothing may be handed out twice"
        );
    }
}

/// A record too large for a page keeps the old pre-record rotation.
///
/// It never enters a page, so there is no page boundary for its file to end at.
/// Without this a queue whose records are all oversized would never rotate.
#[tokio::test]
async fn a_buffered_record_rotates_before_itself() {
    let pool = Arc::new(PagePool::new(4, PAGE_SIZE, 0));
    pool.arm_file_fill(HEADER + record_charge() * 2, HEADER);

    let placements: Vec<(RotateHint, u64)> = (0..6)
        .map(|_| {
            let p = pool.charge_buffered(RECORD.len());
            (p.rotate, p.epoch)
        })
        .collect();
    assert_eq!(
        placements,
        vec![
            (RotateHint::None, 0), // opens file 0
            (RotateHint::None, 0), // fills it
            (RotateHint::Before, 1),
            (RotateHint::None, 1),
            (RotateHint::Before, 2),
            (RotateHint::None, 2),
        ],
        "each file must take two records before rotating again"
    );

    // A record larger than the whole file still opens the file it finds rather
    // than rotating into an empty one, which would rotate forever.
    let tiny = Arc::new(PagePool::new(4, PAGE_SIZE, 0));
    tiny.arm_file_fill(HEADER + 4, HEADER);
    assert_eq!(
        tiny.charge_buffered(RECORD.len()).rotate,
        RotateHint::None,
        "a record larger than max_file_size still opens the first file rather than skipping it"
    );
    for expected in 1..=3u64 {
        let p = tiny.charge_buffered(RECORD.len());
        assert_eq!((p.rotate, p.epoch), (RotateHint::Before, expected));
    }
}

#[tokio::test]
async fn the_fill_counts_each_entry_exactly_once() {
    let pool = Arc::new(PagePool::new(4, PAGE_SIZE, 0));
    pool.arm_file_fill(1 << 20, HEADER);

    // Asserted as a number rather than inferred from where the file boundary
    // lands: the pooled path ignores AckFileWriter::current_size entirely, so a
    // second, redundant count there would not move any boundary and a test that
    // compared boundaries would pass with the double count still in place.
    for n in 1..=5u64 {
        place_next(&pool).await;
        assert_eq!(
            pool.fill_used(),
            Some(HEADER + record_charge() * n),
            "after {n} records the file should hold its header plus {n} encoded entries"
        );
    }
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
        !pool.take_pending(0).is_empty() || pool.next_entry_id() > 0,
        "the pool should be holding records before the writer arms"
    );

    // Put a fresh run in that a previous writer had not taken.
    pool.mark_durable(pool.next_entry_id());
    assert!(matches!(pool.try_append(&RECORD), AppendOutcome::Cached(_)));
    assert!(
        !pool.take_pending(0).is_empty(),
        "that run is pending a write"
    );
    assert!(matches!(pool.try_append(&RECORD), AppendOutcome::Cached(_)));

    // A writer starts now. Whatever is already there is not its to write.
    pool.arm_file_fill(1 << 20, 16);
    assert!(
        pool.take_pending(0).is_empty(),
        "a newly armed writer must not adopt records it has no header entry for"
    );

    // What it is told about afterwards still reaches the file.
    let id = pool.next_entry_id();
    pool.mark_durable(id);
    pool.place(id, &RECORD).await.unwrap();
    let pending = pool.take_pending(0);
    assert_eq!(
        pending.iter().map(|(w, _)| w.last_entry_id).max(),
        Some(id),
        "records enqueued after arming are written normally"
    );
}

/// An unwritten page from a closed file goes to the next file, not nowhere.
///
/// `take_pending` filters on epoch, and filtering on exact equality loses these
/// outright: no writer for a closed file is ever constructed again, so nothing
/// asks for that epoch, while the ring reuses the page as soon as a later fsync
/// moves the watermark past it. The records were acked and end up in no file.
#[tokio::test]
async fn a_page_left_unwritten_by_a_closed_file_still_reaches_one() {
    let pool = armed_pool();

    // File 0 takes a record and never writes it: no commit_written, standing in
    // for a flush that failed every retry before close().
    let id0 = pool.next_entry_id();
    pool.place(id0, &RECORD).await.unwrap();
    let stranded_page = pool.with_ring(|r| r.active_page());
    assert!(
        pool.take_pending(0)
            .iter()
            .any(|(w, _)| w.page_index == stranded_page),
        "file 0 has a run pending on page {stranded_page}, and this test is about it \
         never being written"
    );

    // Enough records to rotate, so file 0 is closed and file 1 is open.
    pool.mark_durable(id0);
    let mut rotated = false;
    while !rotated {
        rotated = place_next(&pool).await.1.rotate == RotateHint::Before;
    }
    let epoch = pool.epoch();
    assert!(epoch > 0);

    let taken = pool.take_pending(epoch);
    let pages: Vec<usize> = taken.iter().map(|(w, _)| w.page_index).collect();
    assert!(
        pages.contains(&stranded_page),
        "the open file must pick up page {stranded_page}, which the closed file left \
         unwritten, or entry {id0} is in no file at all: took pages {pages:?}"
    );
}

/// A record too wide for the V1 frame must not move the file on.
///
/// The writer rejects it before it can rotate anything, so charging it advances
/// the pool's epoch past the writer's for good: every later page is stamped with
/// an epoch no flush asks for, and the queue stops reaching disk entirely.
#[tokio::test]
async fn a_record_too_wide_to_frame_does_not_move_the_epoch() {
    let pool = Arc::new(PagePool::new(4, PAGE_SIZE, 0));
    pool.arm_file_fill(HEADER + record_charge() * 2, HEADER);

    // One real record first, so has_written is set and a rotation is otherwise
    // possible.
    pool.charge_buffered(RECORD.len());
    let before = (pool.epoch(), pool.fill_used());

    let placement = pool.charge_buffered(u32::MAX as usize + 1);
    assert_eq!(placement.rotate, RotateHint::WriterDecides);
    assert_eq!(
        (pool.epoch(), pool.fill_used()),
        before,
        "a record the writer will reject must leave the file accounting untouched"
    );
}

/// A file takes at least one record, however small its maximum.
///
/// The paged path rotates on a page opening, and the first record of a file
/// opens one by definition. Without the `has_written` guard a file below its own
/// header size is closed empty, and two files then claim the same
/// num_entries_before, which recovery uses to order them.
#[tokio::test]
async fn a_file_is_never_closed_before_it_takes_a_record() {
    let pool = Arc::new(PagePool::new(4, PAGE_SIZE, 0));

    // Fill the active page before the writer arms, so the queue's first record
    // has to open a page. This is the readonly-to-write upgrade `arm_file_fill`
    // is written for: the pool already holds records when the writer starts.
    for _ in 0..2 {
        assert!(matches!(pool.try_append(&RECORD), AppendOutcome::Cached(_)));
    }
    assert_eq!(
        pool.with_ring(|r| r.active_page()),
        0,
        "two records fill page 0 and leave it active, per this module's geometry"
    );
    pool.mark_durable(pool.next_entry_id());

    // Smaller than the header, so `used >= max` holds from the moment it arms.
    pool.arm_file_fill(HEADER / 2, HEADER);

    let id = pool.next_entry_id();
    let first = pool.place(id, &RECORD).await.unwrap();
    assert!(
        pool.with_ring(|r| r.page_first_entry_id(r.active_page())) == Some(id),
        "this test only means something if the first record opens a page"
    );
    assert_eq!(
        first.rotate,
        RotateHint::None,
        "the first record of a file must not rotate it away before it holds anything"
    );
    assert_eq!(first.epoch, 0);
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
            assert_eq!(
                pool.place(id, &huge).await,
                Err(PoolError::TooLarge),
                "a record larger than a page is never cached"
            );
            pool.charge_buffered(huge.len());
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
