//! Pages moving between rings over one shared arena.
//!
//! These exercise the migration boundary the C proof stands on: a ring grows
//! only into the free slot directly above it, and gives a page back only once
//! nothing pins it and everything on it is on disk.

use std::sync::Arc;

use crate::wal_arena::{POOL_FREE, WalArena};
use crate::wal_ring_v1::{AppendOutcome, WalRing};

// Big enough for a handful of small records plus their offset slots.
const PAGE: usize = 64;

const RING_A: u64 = 1;
const RING_B: u64 = 2;

/// Slots `[first, first + count)` all read as owned by `who`.
fn assert_owned(arena: &WalArena, first: usize, count: usize, who: u64) {
    for k in first..first + count {
        assert_eq!(
            arena.owner_of(k),
            who,
            "slot {k} should be owned by {who}, arena: {:?}",
            (0..arena.page_count())
                .map(|s| arena.owner_of(s))
                .collect::<Vec<_>>()
        );
    }
}

/// Appends until the ring reports it is out of pages, returning how many
/// records landed.
fn fill(ring: &mut WalRing) -> usize {
    let mut n = 0;
    loop {
        match ring.append(b"abcd") {
            AppendOutcome::Cached(_) => n += 1,
            AppendOutcome::Full => return n,
            other => panic!("unexpected {other:?}"),
        }
    }
}

#[test]
fn reserve_leaves_the_first_range_the_whole_arena_above_it() {
    let arena = Arc::new(WalArena::new(6, PAGE));
    let a = arena.reserve(2, RING_A, 0).expect("room for two pages");

    // Nothing below slot 0 to keep headroom for, so the range starts there and
    // the rest of the arena is room for it to grow into.
    assert_eq!(a.first_slot, 0);
    assert_eq!(a.page_count, 2);
    assert_owned(&arena, 0, 2, RING_A);
    assert_eq!(arena.free_pages(), 4);
}

#[test]
fn reserve_splits_the_spare_room_with_the_range_below() {
    let arena = Arc::new(WalArena::new(6, PAGE));
    let a = arena.reserve(2, RING_A, 0).expect("room for A");
    let b = arena.reserve(2, RING_B, 0).expect("room for B");

    // A holds [0,1]. The gap is [2..5], two spare after B's two, so one slot
    // stays free above A and one above B. Packed back to back instead, A could
    // never grow again.
    assert_eq!((a.first_slot, a.page_count), (0, 2));
    assert_eq!((b.first_slot, b.page_count), (3, 2));
    assert_eq!(arena.owner_of(2), POOL_FREE, "A must keep room to grow");
    assert_eq!(arena.owner_of(5), POOL_FREE, "B must keep room to grow");
}

#[test]
fn a_ring_grows_into_the_free_slot_above_it() {
    let arena = Arc::new(WalArena::new(6, PAGE));
    let range = arena.reserve(2, RING_A, 0).unwrap();
    let mut ring = WalRing::in_arena(&arena, range, RING_A, 0);

    assert_eq!(ring.page_count(), 2);
    assert!(ring.grow(), "slot 2 is free");
    assert_eq!(ring.page_count(), 3);
    assert_owned(&arena, 0, 3, RING_A);
    assert_eq!(arena.free_pages(), 3);
}

#[test]
fn a_ring_will_not_grow_into_another_rings_slot() {
    let arena = Arc::new(WalArena::new(4, PAGE));
    let a = arena.reserve(2, RING_A, 0).unwrap();
    // Take the slot directly above A by hand, so the only thing stopping the
    // grow is that the page belongs to someone else.
    let b = arena.reserve(2, RING_B, 0).unwrap();
    assert_eq!(b.first_slot, 2, "B must sit directly above A for this test");

    let mut ring = WalRing::in_arena(&arena, a, RING_A, 0);
    assert!(!ring.grow(), "slot 2 belongs to B");
    assert_eq!(ring.page_count(), 2);
    assert_owned(&arena, 2, 2, RING_B);
}

#[test]
fn a_ring_at_the_top_of_the_arena_cannot_grow() {
    let arena = Arc::new(WalArena::new(2, PAGE));
    let range = arena.reserve(2, RING_A, 0).unwrap();
    let mut ring = WalRing::in_arena(&arena, range, RING_A, 0);

    assert!(!ring.grow(), "there is no slot 2");
    assert_eq!(ring.page_count(), 2);
}

#[test]
fn shrinking_returns_the_top_page_to_the_arena() {
    let arena = Arc::new(WalArena::new(4, PAGE));
    let range = arena.reserve(3, RING_A, 0).unwrap();
    let mut ring = WalRing::in_arena(&arena, range, RING_A, 0);

    assert!(ring.shrink(1), "an empty top page is reusable");
    assert_eq!(ring.page_count(), 2);
    assert_eq!(
        arena.owner_of(range.first_slot + 2),
        POOL_FREE,
        "the released slot is free for another range to take"
    );
    assert_owned(&arena, range.first_slot, 2, RING_A);
}

#[test]
fn shrinking_stops_at_the_floor() {
    let arena = Arc::new(WalArena::new(4, PAGE));
    let range = arena.reserve(3, RING_A, 0).unwrap();
    let mut ring = WalRing::in_arena(&arena, range, RING_A, 0);

    assert!(ring.shrink(2));
    assert_eq!(ring.page_count(), 2);
    assert!(!ring.shrink(2), "the floor is 2 pages");
    assert_eq!(ring.page_count(), 2);
}

#[test]
fn a_pinned_page_does_not_leave_the_ring_that_holds_it() {
    let arena = Arc::new(WalArena::new(4, PAGE));
    let range = arena.reserve(3, RING_A, 0).unwrap();
    let mut ring = WalRing::in_arena(&arena, range, RING_A, 0);

    // A reader is looking at the top page. `normfs_wal_ring_shrink` requires
    // normfs_wal_page_is_reusable, whose first conjunct is pin_count == 0, so
    // the page cannot be handed to another queue while the read is in flight.
    ring.pin(2);
    assert!(!ring.shrink(1), "a pinned page must not be released");
    assert_owned(&arena, range.first_slot, 3, RING_A);

    ring.unpin(2);
    assert!(ring.shrink(1), "released once the reader is done");
    assert_eq!(arena.owner_of(range.first_slot + 2), POOL_FREE);
}

#[test]
fn a_page_of_records_that_are_not_on_disk_does_not_leave_the_ring() {
    let arena = Arc::new(WalArena::new(4, PAGE));
    let range = arena.reserve(3, RING_A, 0).unwrap();
    let mut ring = WalRing::in_arena(&arena, range, RING_A, 0);

    // Fill every page, then rotate back to the bottom so the top page holds
    // records while something below it is active.
    fill(&mut ring);
    let held = ring.next_entry_id();
    ring.set_essential(held);
    assert!(matches!(ring.append(b"abcd"), AppendOutcome::Cached(_)));

    // Everything up to `held` is durable, so the top page may go.
    assert!(ring.page_count() == 3);
    let top = 2;
    assert!(
        ring.page_last_entry_id(top).is_some(),
        "the top page must hold records for this test to mean anything"
    );

    // Pull the watermark back below the top page's last record: those bytes are
    // no longer known to be on disk, and the page must stay put.
    let top_last = ring.page_last_entry_id(top).unwrap();
    ring.set_essential(top_last);
    assert!(
        !ring.shrink(1),
        "a page whose last record is not below the watermark must not be released"
    );
    assert_owned(&arena, range.first_slot, 3, RING_A);

    ring.set_essential(top_last + 1);
    assert!(ring.shrink(1), "released once the records are on disk");
}

#[test]
fn two_rings_trade_a_page_without_the_total_moving() {
    // The gap this closes: until pages could move, a busy queue could not use
    // memory an idle one had released, however much of it there was.
    let arena = Arc::new(WalArena::new(6, PAGE));
    let a = arena.reserve(2, RING_A, 0).unwrap();
    let b = arena.reserve(2, RING_B, 0).unwrap();
    assert_eq!((a.first_slot, b.first_slot), (0, 3));

    let mut ring_a = WalRing::in_arena(&arena, a, RING_A, 0);
    let ring_b = WalRing::in_arena(&arena, b, RING_B, 0);

    // A takes the one slot it can reach, and stops: slot 3 is B's.
    assert!(ring_a.grow());
    assert!(!ring_a.grow(), "slot 3 belongs to B");
    assert_eq!(ring_a.page_count(), 3);

    // B goes away and hands its whole range back.
    let held = ring_b.min_essential_id();
    drop(ring_b);
    arena.release(b, RING_B, held);
    assert_eq!(
        arena.free_pages(),
        3,
        "B's two pages plus the spare above it"
    );

    // Now A can reach them, one slot at a time.
    assert!(ring_a.grow());
    assert!(ring_a.grow());
    assert!(ring_a.grow());
    assert!(!ring_a.grow(), "the arena is exhausted");

    assert_eq!(ring_a.page_count(), 6);
    assert_owned(&arena, 0, 6, RING_A);
    assert_eq!(
        arena.free_pages(),
        0,
        "every page moved to A, and the arena never grew"
    );
}

#[test]
fn a_taken_slot_does_not_answer_with_its_last_holders_records() {
    // The reason `normfs_wal_ring_grow` requires the page it takes to be empty.
    // seek scans every page in the range, so a slot still carrying another
    // queue's entries would answer under an id of this queue's own.
    let arena = Arc::new(WalArena::new(4, PAGE));

    // A sits below B, so the slot B writes to is the one A will grow into.
    let a = arena.reserve(1, RING_A, 0).unwrap();
    let b = arena.reserve(2, RING_B, 500).unwrap();
    assert_eq!(
        (a.first_slot, b.first_slot),
        (0, 1),
        "A must sit directly below B for this test"
    );

    let mut ring_b = WalRing::in_arena(&arena, b, RING_B, 500);
    assert!(matches!(
        ring_b.append(b"from-b"),
        AppendOutcome::Cached(500)
    ));
    let held = ring_b.next_entry_id();
    ring_b.set_essential(held);
    drop(ring_b);
    arena.release(b, RING_B, held);

    // A grows into the slot B wrote to. The ids overlap on purpose.
    let mut ring_a = WalRing::in_arena(&arena, a, RING_A, 0);
    assert!(ring_a.grow());
    assert_eq!(ring_a.page_count(), 2);

    assert!(matches!(ring_a.append(b"from-a"), AppendOutcome::Cached(0)));
    assert_eq!(ring_a.get(0).unwrap(), b"from-a");
    assert!(
        ring_a.get(500).is_none(),
        "the slot was reset when it was taken, so B's entry 500 is not visible here"
    );
}

#[test]
fn holders_names_who_is_using_the_arena() {
    let arena = Arc::new(WalArena::new(6, PAGE));
    arena.reserve(2, RING_A, 0).unwrap();
    arena.reserve(3, RING_B, 0).unwrap();
    arena.set_label(RING_A, "/inst/busy-queue");
    arena.set_label(RING_B, "/inst/idle-queue");

    let holders = arena.holders();
    assert_eq!(holders.len(), 2);
    assert_eq!(holders[0], ("/inst/idle-queue".to_string(), 3));
    assert_eq!(holders[1], ("/inst/busy-queue".to_string(), 2));
}

#[test]
fn a_dropped_ring_gives_its_reusable_slots_back() {
    let arena = Arc::new(WalArena::new(4, PAGE));
    {
        let range = arena.reserve(3, RING_A, 0).unwrap();
        let mut ring = WalRing::in_arena(&arena, range, RING_A, 0);
        assert!(matches!(ring.append(b"abcd"), AppendOutcome::Cached(_)));
        ring.set_essential(ring.next_entry_id());
        assert_eq!(arena.free_pages(), 1);
    }
    assert_eq!(
        arena.free_pages(),
        4,
        "queue churn must not bleed the arena dry"
    );
}

#[test]
fn a_dropped_ring_leaks_rather_than_releases_unwritten_pages() {
    let arena = Arc::new(WalArena::new(4, PAGE));
    {
        let range = arena.reserve(3, RING_A, 0).unwrap();
        let mut ring = WalRing::in_arena(&arena, range, RING_A, 0);
        // A record accepted and never reported durable: its page must not
        // reach another ring, whatever happens to this one.
        assert!(matches!(ring.append(b"abcd"), AppendOutcome::Cached(_)));
    }
    assert_eq!(
        arena.free_pages(),
        3,
        "two empty pages return; the unwritten one is deliberately leaked"
    );
}
