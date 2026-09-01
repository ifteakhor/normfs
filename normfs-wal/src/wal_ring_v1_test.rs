use crate::wal_ring_v1::{AppendOutcome, WalRing};

// A page big enough for a handful of small records plus their offset slots.
const PAGE: usize = 64;

#[test]
fn append_then_get_roundtrips() {
    let mut ring = WalRing::new(4, PAGE, 0);
    let a = ring.append(b"hello");
    let b = ring.append(b"world");
    assert_eq!(a, AppendOutcome::Cached(0));
    assert_eq!(b, AppendOutcome::Cached(1));
    assert_eq!(ring.get(0).unwrap(), b"hello");
    assert_eq!(ring.get(1).unwrap(), b"world");
}

#[test]
fn ids_are_sequential_from_first() {
    let mut ring = WalRing::new(4, PAGE, 100);
    for i in 0..5u64 {
        assert_eq!(ring.append(b"x"), AppendOutcome::Cached(100 + i));
    }
    assert_eq!(ring.next_entry_id(), 105);
}

#[test]
fn empty_record_roundtrips() {
    let mut ring = WalRing::new(2, PAGE, 0);
    assert_eq!(ring.append(b""), AppendOutcome::Cached(0));
    assert_eq!(ring.get(0).unwrap(), b"");
}

#[test]
fn rotates_across_pages_and_reads_back() {
    // Small pages so a few records force rotations; plenty of pages so no
    // reclaim is needed.
    let mut ring = WalRing::new(8, 32, 0);
    let mut ids = Vec::new();
    for _ in 0..12 {
        match ring.append(b"record") {
            AppendOutcome::Cached(id) => ids.push(id),
            other => panic!("unexpected {:?}", other),
        }
    }
    assert_eq!(ids, (0..12).collect::<Vec<_>>());
    for id in ids {
        assert_eq!(ring.get(id).unwrap(), b"record");
    }
}

#[test]
fn oversized_record_is_not_cached() {
    let mut ring = WalRing::new(2, 32, 0);
    let big = vec![0xABu8; 40]; // bigger than a 32 byte page
    assert_eq!(ring.append(&big), AppendOutcome::TooLarge);
}

#[test]
fn full_when_no_page_reusable() {
    // Two tiny pages; keep appending until every page holds still-essential
    // entries and the active one is full, so rotation cannot find a victim.
    let mut ring = WalRing::new(2, 24, 0);
    let mut outcome = AppendOutcome::Cached(0);
    for _ in 0..100 {
        outcome = ring.append(b"abcd");
        if outcome == AppendOutcome::Full {
            break;
        }
    }
    assert_eq!(outcome, AppendOutcome::Full);
}

#[test]
fn reclaim_after_essential_advances_frees_pages() {
    let mut ring = WalRing::new(2, 24, 0);
    // Fill until full.
    let mut appended = 0u64;
    loop {
        match ring.append(b"abcd") {
            AppendOutcome::Cached(_) => appended += 1,
            AppendOutcome::Full => break,
            other => panic!("unexpected {:?}", other),
        }
    }
    assert!(appended >= 1);
    // Mark everything appended so far as no longer essential; a page becomes
    // reusable and appends succeed again.
    ring.set_essential(ring.next_entry_id());
    assert!(matches!(ring.append(b"abcd"), AppendOutcome::Cached(_)));
}

#[test]
fn seek_missing_id_returns_none() {
    let mut ring = WalRing::new(2, PAGE, 0);
    ring.append(b"only");
    assert!(ring.seek(999).is_none());
    assert!(ring.get(999).is_none());
}

#[test]
fn ring_survives_move() {
    // The C ring holds raw pointers into the heap buffers; moving the value
    // must not invalidate them.
    let mut ring = WalRing::new(2, PAGE, 0);
    ring.append(b"before-move");
    let mut moved = ring; // move
    assert_eq!(moved.get(0).unwrap(), b"before-move");
    assert!(matches!(
        moved.append(b"after-move"),
        AppendOutcome::Cached(1)
    ));
    assert_eq!(moved.get(1).unwrap(), b"after-move");
}
