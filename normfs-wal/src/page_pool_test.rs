//! Behaviour of the page pool under pressure.
//!
//! The point of these tests is the one thing the old cache got wrong: when
//! every page is occupied by records that are not yet on disk, an appender must
//! *wait*. The previous in-memory store called `reinit` and threw the cache
//! away, which is silent data loss for anything reading from memory.

use std::sync::Arc;
use std::time::Duration;

use crate::page_pool::{PagePool, Placement, RotateHint};
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

/// Stands in for the file writer taking responsibility for everything placed so
/// far. `take_pending` hands out nothing above this, so a test that skips it is
/// testing an empty flush.
fn hand_over_all(pool: &PagePool) {
    let next = pool.next_entry_id();
    if next > 0 {
        pool.note_handed_over(next - 1);
    }
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

/// A record larger than a page is held by the pool, not routed round it.
///
/// It used to be handed to the writer's own buffer, and the pool then re-seeded
/// to step its id sequence past it -- a drain and an fsync per record, and a
/// second road to the same file that nothing ordered against the first.
#[tokio::test]
async fn a_record_larger_than_a_page_is_held_rather_than_refused() {
    let pool = pool();
    let huge = vec![0xEE; PAGE_SIZE * 2];

    // No amount of waiting makes a page fit it, so this must not block.
    let placed = tokio::time::timeout(
        Duration::from_secs(2),
        pool.place(pool.next_entry_id(), &huge),
    )
    .await
    .expect("an oversized record must not wait for a page it can never have")
    .expect("an oversized record is held, not refused");

    assert!(
        placed.in_pool,
        "the pool holds it, so the writer must not buffer it again"
    );
    assert_eq!(
        pool.next_entry_id(),
        1,
        "it takes its id like any other record"
    );

    hand_over_all(&pool);
    let pending = pool.take_pending(placed.epoch);
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].0.first_entry_id, 0);
    assert_eq!(pending[0].0.last_entry_id, 0);
    assert_eq!(
        &pending[0].1[pending[0].1.len() - 4 - huge.len()..pending[0].1.len() - 4],
        &huge[..],
        "the framed entry carries the record whole"
    );
}

/// A record wider than the V1 frame has no encoding at all, and no id can save
/// it. `NormFS::enqueue` refuses one before it takes an id; this is the pool
/// refusing to accept one if that guard is ever bypassed.
#[tokio::test]
async fn a_record_too_wide_to_frame_is_refused() {
    // A real 4 GiB record is not allocatable here, so this asserts the shape of
    // the check rather than driving it: encoded_len is None exactly there.
    assert!(crate::page_pool::encoded_len_of(u32::MAX as usize + 1).is_none());
    assert!(crate::page_pool::encoded_len_of(u32::MAX as usize).is_some());
}

#[tokio::test]
async fn pending_yields_each_byte_once_and_in_id_order() {
    let pool = pool();
    fill(&pool);

    assert!(
        pool.take_pending(0).is_empty(),
        "nothing may be written before the writer has taken responsibility for it"
    );

    hand_over_all(&pool);
    let first = pool.take_pending(0);
    assert!(
        !first.is_empty(),
        "appended records should be pending a write"
    );
    let total: usize = first.iter().map(|(_, b)| b.len()).sum();
    assert!(total > 0);
    for w in first.windows(2) {
        assert!(
            w[0].0.last_entry_id < w[1].0.first_entry_id,
            "pending runs must come out in id order and must not overlap"
        );
    }

    // Not written yet, so it must come back: an untried write must not lose
    // the run.
    assert_eq!(
        pool.take_pending(0),
        first,
        "an uncommitted run must stay pending"
    );

    // The writer commits each run once it is actually on disk.
    for (w, _) in &first {
        pool.commit_written(w);
    }

    // Committed once: a second call has nothing to give, so the writer cannot
    // append the same bytes to the file twice.
    assert!(pool.take_pending(0).is_empty());

    // A further record produces only the new bytes, not the whole page again.
    pool.mark_durable(pool.next_entry_id());
    assert!(matches!(pool.try_append(&RECORD), AppendOutcome::Cached(_)));
    hand_over_all(&pool);
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
    // What the writer does when the entry reaches it, in the id order its
    // ordered buffer restores. Without it a flush takes nothing.
    pool.note_handed_over(id);
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
            pool.commit_written(w);
        }
        assert!(
            pool.take_pending(epoch).is_empty(),
            "nothing may be handed out twice"
        );
    }
}

/// A record too large for a page still ends a file.
///
/// It never enters a page, so no page rotation will come along carrying a
/// boundary for it. `skip_entry` empties the active page before stepping the
/// sequence, which is what gives the boundary somewhere to land — and it is why
/// a queue whose records are all oversized still rotates its files rather than
/// growing one for ever.
#[tokio::test]
async fn an_oversized_record_still_ends_a_file() {
    let huge = vec![0x5A; PAGE_SIZE * 2];
    let charge = crate::wal_entry_v1::encoded_len(huge.len() as u32) as u64;

    let pool = Arc::new(PagePool::new(4, PAGE_SIZE, 0));
    pool.arm_file_fill(HEADER + charge * 2, HEADER);

    let mut placements: Vec<(RotateHint, u64)> = Vec::new();
    for _ in 0..6 {
        let id = pool.next_entry_id();
        let p = pool.place(id, &huge).await.expect("held, not refused");
        pool.note_handed_over(id);
        pool.mark_durable(id + 1);
        placements.push((p.rotate, p.epoch));
    }
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
    // than rotating into an empty one, which would rotate for ever.
    let tiny = Arc::new(PagePool::new(4, PAGE_SIZE, 0));
    tiny.arm_file_fill(HEADER + 4, HEADER);
    let id = tiny.next_entry_id();
    let first = tiny.place(id, &huge).await.unwrap();
    tiny.note_handed_over(id);
    tiny.mark_durable(id + 1);
    assert_eq!(
        first.rotate,
        RotateHint::None,
        "a record larger than max_file_size still opens the first file rather than skipping it"
    );
    for expected in 1..=3u64 {
        let id = tiny.next_entry_id();
        let p = tiny.place(id, &huge).await.unwrap();
        tiny.note_handed_over(id);
        tiny.mark_durable(id + 1);
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
    hand_over_all(&pool);
    assert!(
        !pool.take_pending(0).is_empty() || pool.next_entry_id() > 0,
        "the pool should be holding records before the writer arms"
    );

    // Put a fresh run in that a previous writer had not taken.
    pool.mark_durable(pool.next_entry_id());
    assert!(matches!(pool.try_append(&RECORD), AppendOutcome::Cached(_)));
    hand_over_all(&pool);
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

    // What it is told about afterwards still reaches the file. `arm_file_fill`
    // cleared the handover mark with the cursors, so the new writer's own first
    // entry is what re-opens the flush.
    let id = pool.next_entry_id();
    pool.mark_durable(id);
    pool.place(id, &RECORD).await.unwrap();
    pool.note_handed_over(id);
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
    pool.note_handed_over(id0);
    let stranded_page = pool.with_ring(|r| r.active_page());
    assert!(
        pool.take_pending(0)
            .iter()
            .any(|(w, _)| w.first_entry_id == id0),
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
    let ids: Vec<u64> = taken.iter().map(|(w, _)| w.first_entry_id).collect();
    assert!(
        ids.contains(&id0),
        "the open file must pick up the run the closed file left unwritten on page \
         {stranded_page}, or entry {id0} is in no file at all: took runs from {ids:?}"
    );
}

/// A record too wide for the V1 frame must not move the file on.
///
/// There is no encoding for it, so no file can hold it and no id should have
/// been given to it. `NormFS::enqueue` refuses it before that happens; if that
/// guard is ever bypassed the pool must refuse too, and refuse without charging
/// — charging would advance the pool's epoch past the writer's for good, and
/// every later page would be stamped with an epoch no flush asks for.
#[tokio::test]
async fn a_record_too_wide_to_frame_does_not_move_the_epoch() {
    let pool = Arc::new(PagePool::new(4, PAGE_SIZE, 0));
    pool.arm_file_fill(HEADER + record_charge() * 2, HEADER);

    // One real record first, so has_written is set and a rotation is otherwise
    // possible.
    let id = pool.next_entry_id();
    pool.place(id, &RECORD).await.unwrap();
    pool.note_handed_over(id);
    pool.mark_durable(id + 1);
    let before = (pool.epoch(), pool.fill_used(), pool.next_entry_id());

    // The check itself: `encoded_len_of` is what `hold_oversize` consults, and
    // it is `None` exactly at the frame's limit. A 4 GiB buffer is not
    // allocatable in a test, so this pins the boundary rather than driving it.
    assert!(crate::page_pool::encoded_len_of(u32::MAX as usize).is_some());
    assert!(crate::page_pool::encoded_len_of(u32::MAX as usize + 1).is_none());
    assert_eq!(
        (pool.epoch(), pool.fill_used(), pool.next_entry_id()),
        before,
        "consulting the frame limit must not move the file accounting"
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

/// A queue of nothing but oversized records keeps running.
///
/// This used to be the worst case by a distance. Every record went round the
/// pool into the writer's buffer, and the pool then re-seeded to step its id
/// sequence past it — which it could only do once it had nothing left to lose,
/// so every record cost a drain and an fsync of the whole queue. A 1 MiB record
/// against a 256 KiB page is the ordinary way in, and `signature_test` does
/// exactly that 150 times.
///
/// Now the record is held whole and the sequence is stepped in place, so this
/// runs at the speed of the flush rather than one record per fsync.
#[tokio::test]
async fn every_record_being_oversized_does_not_stall_the_queue() {
    let pool = Arc::new(PagePool::new(PAGE_COUNT, PAGE_SIZE, 0));
    pool.set_drainer();
    pool.arm_file_fill(1 << 20, 16);

    let huge = vec![0x11; PAGE_SIZE * 4];

    // Ten in a row. The budget is one page, so all but the first wait for a
    // flush — which the loop performs, exactly as the writer would.
    let run = async {
        for id in 0..10u64 {
            let placed = pool
                .place(id, &huge)
                .await
                .expect("an oversized record is held, not refused");
            assert!(placed.in_pool);
            assert_eq!(
                pool.next_entry_id(),
                id + 1,
                "the id sequence steps over the record no page can hold"
            );

            // What the writer does: take, write, sync, report durable.
            pool.note_handed_over(id);
            let pending = pool.take_pending(placed.epoch);
            assert_eq!(pending.len(), 1, "one held record, one run");
            for (w, _) in &pending {
                pool.commit_written(w);
            }
            pool.mark_durable(id + 1);
        }
    };
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("a queue of oversized records must keep moving");

    assert!(pool.is_empty(), "everything durable is released");
}

/// The pool holds oversized records rather than letting them accumulate.
///
/// A record larger than a page is memory the pages cannot use, so it has to be
/// bounded the same way they are — by making the appender wait for the disk.
/// The first one is always taken however large it is: refusing it would be the
/// discarding this pool exists to avoid.
#[tokio::test]
async fn a_held_oversized_record_makes_the_next_one_wait() {
    let pool = Arc::new(PagePool::new(PAGE_COUNT, PAGE_SIZE, 0));
    pool.set_drainer();
    let huge = vec![0x22; PAGE_SIZE * 4];

    pool.place(0, &huge)
        .await
        .expect("the first is always taken");

    // The second is over budget, so it waits — and only a flush can end that.
    let waited = tokio::time::timeout(Duration::from_millis(150), pool.place(1, &huge)).await;
    assert!(
        waited.is_err(),
        "a second oversized record must wait for the first to reach disk"
    );

    let pool2 = Arc::clone(&pool);
    let resumed = tokio::spawn(async move { pool2.place(1, &huge).await });
    tokio::time::sleep(Duration::from_millis(20)).await;
    pool.note_handed_over(0);
    for (w, _) in &pool.take_pending(0) {
        pool.commit_written(w);
    }
    pool.mark_durable(1);
    tokio::time::timeout(Duration::from_secs(5), resumed)
        .await
        .expect("the wait must end when the held record reaches disk")
        .unwrap()
        .expect("held, not refused");
}

/// Memory answers for an oversized record like any other, until it is released.
///
/// It sits between two pages in the id sequence, so a read that could answer
/// for the ids either side of it but not for the record itself would hand back
/// a run with a hole in it — and a hole is worse than a miss, because the
/// caller cannot see it.
#[tokio::test]
async fn a_held_oversized_record_reads_back_between_its_neighbours() {
    let pool = Arc::new(PagePool::new(4, PAGE_SIZE, 0));
    pool.set_drainer();
    let huge = vec![0x33; PAGE_SIZE * 2];

    pool.place(0, &RECORD).await.unwrap();
    pool.place(1, &huge).await.unwrap();
    pool.place(2, &RECORD).await.unwrap();

    let got = pool.collect_range(0, 2);
    let ids: Vec<u64> = got.iter().map(|(id, _)| *id).collect();
    assert_eq!(ids, vec![0, 1, 2], "the run must have no hole in it");
    assert_eq!(got[1].1, huge, "and the record must come back whole");

    let pinned = Arc::clone(&pool).pin_range(0, 2);
    assert_eq!(
        pinned.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    assert_eq!(&pinned[1].1[..], &huge[..]);

    // Released once durable, and memory then says so rather than answering for
    // its neighbours and not for it.
    hand_over_all(&pool);
    for (w, _) in &pool.take_pending(0) {
        pool.commit_written(w);
    }
    pool.mark_durable(2);
    assert_eq!(
        pool.min_cached_id(),
        Some(2),
        "memory starts above the record it released, not below it"
    );
    assert_eq!(
        pool.collect_range(0, 2)
            .iter()
            .map(|(id, _)| *id)
            .collect::<Vec<_>>(),
        vec![2],
        "the ids either side of a released record are no longer offered from memory"
    );
}

/// Nothing is written before the writer has taken responsibility for it.
///
/// A record's bytes are in its page from the moment `place` returns, which is
/// before the entry has reached the writer at all — the two are joined by an
/// unbounded channel and the append gate is released between them. A flush
/// landing in that window used to write the later record first, and V1 derives
/// ids from position, so that is every entry after it answering under the wrong
/// id rather than one record out of place.
#[tokio::test]
async fn a_flush_never_runs_ahead_of_the_writer() {
    let pool = Arc::new(PagePool::new(4, PAGE_SIZE, 0));
    pool.set_drainer();
    pool.arm_file_fill(1 << 20, HEADER);

    let huge = vec![0x44; PAGE_SIZE * 2];
    pool.place(0, &huge).await.unwrap(); // in flight to the writer
    pool.place(1, &RECORD).await.unwrap(); // already in a page

    assert!(
        pool.take_pending(0).is_empty(),
        "neither record has reached the writer, so neither may be written"
    );

    // The writer takes them in id order, which is what its ordered buffer is
    // for. Entry 1 becomes writable only after entry 0.
    pool.note_handed_over(0);
    let first = pool.take_pending(0);
    assert_eq!(
        first
            .iter()
            .map(|(w, _)| w.last_entry_id)
            .collect::<Vec<_>>(),
        vec![0],
        "only the record the writer has claimed"
    );
    for (w, _) in &first {
        pool.commit_written(w);
    }

    pool.note_handed_over(1);
    let second = pool.take_pending(0);
    assert_eq!(
        second
            .iter()
            .map(|(w, _)| w.first_entry_id)
            .collect::<Vec<_>>(),
        vec![1]
    );
}

/// A page is cut between entries, never inside one.
///
/// The handover bound falls wherever the writer has got to, which is generally
/// part-way through a page. The page's own offset table is what says where the
/// next entry begins, so the cut is exact.
#[tokio::test]
async fn a_page_is_handed_over_one_entry_at_a_time() {
    let pool = Arc::new(PagePool::new(4, 4096, 0));
    pool.set_drainer();
    pool.arm_file_fill(1 << 20, HEADER);

    for id in 0..5u64 {
        pool.place(id, &RECORD).await.unwrap();
    }

    let charge = record_charge() as usize;
    for claimed in 0..5u64 {
        pool.note_handed_over(claimed);
        let taken = pool.take_pending(0);
        let bytes: usize = taken.iter().map(|(_, b)| b.len()).sum();
        assert_eq!(
            bytes,
            charge * (claimed as usize + 1),
            "a flush must stop at the end of entry {claimed}, not part-way into the next"
        );
        assert_eq!(
            taken.iter().map(|(w, _)| w.last_entry_id).max(),
            Some(claimed)
        );
    }
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

    let pinned: u32 =
        pool.with_ring(|ring| (0..ring.page_count()).map(|k| ring.page_pin_count(k)).sum());
    assert_eq!(
        pinned, 0,
        "a dropped payload leaks a pin, and a leaked pin holds the page for good"
    );
}

#[tokio::test]
async fn a_reseed_does_not_reset_a_page_a_reader_is_holding() {
    let pool = pool();
    fill(&pool);

    let borrowed = pool.pin_range(0, 0);
    assert_eq!(borrowed.len(), 1);
    let (_, payload) = borrowed.into_iter().next().unwrap();
    let pinned_page = pool
        .with_ring(|ring| (0..ring.page_count()).find(|&k| ring.page_pin_count(k) > 0))
        .expect("the payload holds a pin");

    assert!(pool.reseed(64), "one page is free, so this can proceed");
    assert_eq!(
        pool.with_ring(|ring| ring.page_pin_count(pinned_page)),
        1,
        "a reseed must carry the pin across, or the page is handed out to be overwritten"
    );

    // Whatever the pool does next, the reader's bytes are its own.
    for _ in 0..64 {
        let _ = pool.try_append(&RECORD);
    }
    assert_eq!(&payload[..], &RECORD[..]);

    drop(payload);
    assert_eq!(
        pool.with_ring(|ring| ring.page_pin_count(pinned_page)),
        0,
        "and the count comes back to zero rather than underflowing"
    );
}

#[tokio::test]
async fn a_read_never_pins_the_last_page() {
    let pool = pool();
    fill(&pool);

    // Everything the pool holds, borrowed at once.
    let held = pool.pin_range(0, u64::MAX);
    assert!(!held.is_empty());

    let unpinned = pool.with_ring(|ring| {
        (0..ring.page_count())
            .filter(|&k| ring.page_pin_count(k) == 0)
            .count()
    });
    assert!(
        unpinned >= 1,
        "a read that pins every page leaves the writer nowhere to append"
    );
}

/// A reader may hold at most its share of the pool, and never the active page.
///
/// One reserved page is enough to prove the pool cannot deadlock, and it was
/// proved. It is not enough to keep it working: with a single page to append
/// into, a queue advances one page per flush however much memory it was given,
/// and the cache degrades to nothing but the stalled reader's pages so every
/// other reader starts missing memory too.
#[tokio::test]
async fn a_reader_may_hold_only_its_share_of_the_pool() {
    let pool = Arc::new(PagePool::new(8, PAGE_SIZE, 0));
    fill(&pool);

    // Everything the pool holds, borrowed at once, by two readers rather than
    // one -- the share is a property of the pool, not of a single call.
    let _first = Arc::clone(&pool).pin_range(0, u64::MAX);
    let _second = Arc::clone(&pool).pin_range(0, u64::MAX);

    let (pinned, active_pinned) = pool.with_ring(|ring| {
        let active = ring.active_page();
        (
            (0..ring.page_count())
                .filter(|&k| ring.page_pin_count(k) > 0)
                .count(),
            ring.page_pin_count(active) > 0,
        )
    });

    assert!(
        pinned <= 4,
        "reads hold {pinned} of 8 pages; half is the share that leaves the writer a \
         working set rather than a single page"
    );
    assert!(
        !active_pinned,
        "the page being appended into must never be pinned: pinning it stops the rotation \
         that would free it"
    );
}

/// A single read cannot claim its whole share in one step.
///
/// `read_full` materialises the entire requested range before sending any of
/// it, so one large read would otherwise take the share and hold it for the
/// length of the send.
#[tokio::test]
async fn one_read_hands_out_a_bounded_number_of_borrowed_payloads() {
    let pool = Arc::new(PagePool::new(8, 4096, 0));
    for id in 0..2000u64 {
        pool.place(id, &RECORD).await.unwrap();
        pool.mark_durable(id);
    }

    let got = Arc::clone(&pool).pin_range(0, u64::MAX);
    assert!(!got.is_empty());
    let pinned: u32 =
        pool.with_ring(|ring| (0..ring.page_count()).map(|k| ring.page_pin_count(k)).sum());
    assert!(
        (pinned as usize) <= 4096,
        "one read borrowed {pinned} payloads; past the bound they must be copied"
    );
}

#[tokio::test]
async fn an_append_makes_progress_while_a_reader_holds_everything() {
    let pool = pool();
    fill(&pool);
    pool.set_drainer();

    // Everything is on disk, so nothing but a pin could hold a page — which is
    // the point: `PIN_RESERVE` keeps one page unpinned, and a page that is both
    // unpinned and durable is one the ring may rotate into.
    pool.mark_durable(pool.next_entry_id());

    // The payloads stay alive for the whole of this test.
    let _held = Arc::clone(&pool).pin_range(0, u64::MAX);

    // An ordinary append first.
    let next = pool.next_entry_id();
    assert!(
        tokio::time::timeout(Duration::from_secs(5), pool.place(next, &RECORD))
            .await
            .is_ok(),
        "a reader holding payloads must not stall an append for ever"
    );

    // And resyncing to an id the pool is not expecting, which has to step the
    // sequence rather than append to it.
    pool.mark_durable(pool.next_entry_id());
    let ahead = pool.next_entry_id() + 3;
    assert!(
        tokio::time::timeout(Duration::from_secs(5), pool.place(ahead, &RECORD))
            .await
            .is_ok(),
        "stepping the sequence must not wait on a pin either"
    );
    assert_eq!(pool.next_entry_id(), ahead + 1);
}
