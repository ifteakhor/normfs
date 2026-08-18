//! Safe wrapper over the verified C V1 WAL paged memory store.
//!
//! Rust owns the memory: one arena of `page_count * page_size` bytes plus the
//! page-descriptor array. The C layer (proven with Frama-C WP) does the
//! appends, offset table, rotation choice and cursor arithmetic; this module
//! sequences them and hands back safe slices.
//!
//! Page `k` is the slice of the arena at `k * page_size`. That is not an
//! implementation detail — it is what the C side's separation argument rests
//! on. With one buffer per page, "no two pages overlap" is an axiom the caller
//! supplies and every mutation has to carry across itself, as a quantifier over
//! every *pair* of pages; the automatic provers do not transport it. As slices
//! of one allocation it is arithmetic on one valid block instead, so the
//! separation clause becomes three flat statements about base pointers that no
//! mutation assigns, and it survives by frame.

use std::os::raw::c_int;
use std::sync::Arc;

use crate::wal_arena::{CWalPool, POOL_FREE, WalArena};

#[repr(C)]
pub(crate) struct CWalPage {
    pub(crate) buf: *mut u8,
    pub(crate) cap: usize,
    pub(crate) used_bytes: usize,
    pub(crate) count: u32,
    pub(crate) page_id: u64,
    pub(crate) first_entry_id: u64,
    pub(crate) last_entry_id: u64,
    pub(crate) pin_count: u32,
    pub(crate) published: c_int,
}

#[repr(C)]
struct CWalRing {
    // Mirrors `struct normfs_wal_ring` field for field, in order. Nothing
    // checks that across the FFI boundary, so a field added on one side and
    // not the other silently misreads every field after it.
    pool: *mut CWalPool,
    first_slot: usize,
    pages: *mut CWalPage,
    arena: *mut u8,
    page_count: usize,
    page_size: usize,
    active: usize,
    next_entry_id: u64,
    next_page_id: u64,
    min_essential_id: u64,
}

#[repr(C)]
struct CRingAppendResult {
    entry_id: u64,
    page_index: usize,
    status: c_int,
}

#[repr(C)]
struct CRingSeekResult {
    page_index: usize,
    index: u32,
    found: c_int,
}

#[repr(C)]
struct CEntryDecodeResult {
    record_offset: usize,
    record_size: usize,
    consumed: usize,
    crc: u32,
    status: c_int,
}

// Ring status codes (see wal_ring.h). NEEDS_ROTATE (1) is handled via the
// catch-all arm, so only these are named.
const RING_OK: c_int = 0;
const RING_TOO_LARGE: c_int = 2;
const ENTRY_OK: c_int = 0;

unsafe extern "C" {
    pub(crate) fn normfs_wal_page_init(
        page: *mut CWalPage,
        buf: *mut u8,
        cap: usize,
        page_id: u64,
        first_entry_id: u64,
    );
    fn normfs_wal_page_offset(page: *mut CWalPage, index: u32) -> u32;
    fn normfs_wal_page_entry_id(page: *mut CWalPage, index: u32) -> u64;
    fn normfs_wal_page_pin(page: *mut CWalPage);
    fn normfs_wal_page_unpin(page: *mut CWalPage);
    fn normfs_wal_page_reusable(page: *const CWalPage, min_essential_id: u64) -> c_int;

    fn normfs_wal_ring_init(
        ring: *mut CWalRing,
        pages: *mut CWalPage,
        arena: *mut u8,
        page_count: usize,
        page_size: usize,
        first_entry_id: u64,
    );
    fn normfs_wal_ring_try_append(
        ring: *mut CWalRing,
        record: *const u8,
        record_size: u32,
    ) -> CRingAppendResult;
    fn normfs_wal_ring_rotate_to(ring: *mut CWalRing, index: usize);
    fn normfs_wal_ring_seek(ring: *mut CWalRing, entry_id: u64) -> CRingSeekResult;
    fn normfs_wal_ring_set_essential(ring: *mut CWalRing, min_essential_id: u64);
    fn normfs_wal_ring_grow(ring: *mut CWalRing, ring_id: u64);
    fn normfs_wal_ring_shrink(ring: *mut CWalRing, ring_id: u64);
    fn normfs_wal_ring_skip_entry(ring: *mut CWalRing);

    fn normfs_wal_entry_v1_decode(buf: *const u8, len: usize) -> CEntryDecodeResult;
}

/// Result of appending a record to the paged store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppendOutcome {
    /// The record was cached and given this entry id.
    Cached(u64),
    /// The record is larger than a page and is not cached; read it from file.
    TooLarge,
    /// Every page is pinned or still essential; the buffer is full.
    Full,
}

/// Where a ring's pages come from.
///
/// `Owned` is the standalone shape: the ring allocates its own arena and holds
/// every page in it for life. `Shared` borrows a contiguous slot range from a
/// process-wide [`WalArena`], which is what lets a busy queue grow into memory
/// an idle one has released.
///
/// Nothing below this enum cares which it is. The C ring caches `pages` and
/// `arena` *based at its own first slot*, so page `k` is at the same offset
/// from those bases either way — which is the whole reason migration did not
/// have to disturb any already-proven function.
enum Backing {
    Owned {
        _arena: Vec<u8>,
        _pages: Vec<CWalPage>,
    },
    Shared(Arc<WalArena>),
}

/// A ring of WAL pages holding V1 entries in memory.
///
/// The raw pointers the C ring holds stay valid even if the `WalRing` value is
/// moved: they point either into this value's own heap allocations, which are
/// never reallocated after construction, or into a shared arena that outlives
/// the ring by way of the `Arc`.
pub struct WalRing {
    backing: Backing,
    ring: Box<CWalRing>,
    page_size: usize,
    /// This ring's id in the shared arena's owner array. Meaningless — and
    /// never used — when the ring owns its pages.
    ring_id: u64,
}

impl WalRing {
    /// Creates a ring of `page_count` pages of `page_size` bytes each, whose
    /// first entry id is `first_entry_id`. `page_count` must be at least 1 and
    /// `page_size` must exceed the minimum entry framing.
    ///
    /// The ring owns its pages outright; it cannot grow or shrink. See
    /// [`WalRing::in_arena`] for a ring that borrows from the shared arena.
    pub fn new(page_count: usize, page_size: usize, first_entry_id: u64) -> Self {
        assert!(page_count >= 1, "ring needs at least one page");
        assert!(
            page_size >= 9,
            "page must hold the smallest entry plus its offset"
        );
        // Preconditions of normfs_wal_page_init and normfs_wal_ring_init: the
        // offset table holds a page offset as a u32, and the page pointers are
        // derived from the arena size.
        assert!(
            page_size <= u32::MAX as usize,
            "page must be addressable by a u32 offset"
        );
        assert!(
            page_count.checked_mul(page_size).is_some(),
            "arena size overflows"
        );

        // One allocation, never reallocated. Page k starts at k * page_size,
        // which is what makes the pages provably disjoint without a pairwise
        // separation axiom.
        let mut arena: Vec<u8> = vec![0u8; page_count * page_size];
        let base = arena.as_mut_ptr();
        let mut pages: Vec<CWalPage> = Vec::with_capacity(page_count);
        for k in 0..page_count {
            let mut page: CWalPage = unsafe { std::mem::zeroed() };
            unsafe {
                normfs_wal_page_init(
                    &mut page,
                    base.add(k * page_size),
                    page_size,
                    k as u64,
                    first_entry_id,
                );
            }
            pages.push(page);
        }

        let mut ring: Box<CWalRing> = Box::new(unsafe { std::mem::zeroed() });
        unsafe {
            normfs_wal_ring_init(
                ring.as_mut(),
                pages.as_mut_ptr(),
                base,
                page_count,
                page_size,
                first_entry_id,
            );
        }

        WalRing {
            backing: Backing::Owned {
                _arena: arena,
                _pages: pages,
            },
            ring,
            page_size,
            ring_id: POOL_FREE,
        }
    }

    /// Creates a ring over the slot range `arena` has already reserved for
    /// `ring_id`.
    ///
    /// The caller reserves the range with [`WalArena::reserve`], which is what
    /// initialises each page descriptor and marks the slots owned. This only
    /// points a `CWalRing` at them.
    ///
    /// ## Why the pool is bound from Rust rather than by a C entry point
    ///
    /// `normfs_wal_ring_init` sets `pool = NULL` — a ring that owns its pages
    /// needs no pool, and that is the shape the standalone tests build. Binding
    /// is then two plain field writes on a `#[repr(C)]` mirror. Adding a
    /// `ring_bind` to C would mean another contract and another proof for an
    /// assignment WP would have nothing to say about, and the trust boundary is
    /// unchanged either way: Rust establishes the preconditions, C proves the
    /// bodies that consume them.
    pub fn in_arena(
        arena: &Arc<WalArena>,
        range: crate::wal_arena::SlotRange,
        ring_id: u64,
        first_entry_id: u64,
    ) -> Self {
        assert!(range.page_count >= 1, "ring needs at least one page");
        assert!(
            range.first_slot + range.page_count <= arena.page_count(),
            "slot range runs past the end of the arena"
        );
        assert!(ring_id != POOL_FREE, "a ring cannot be called FREE");

        let page_size = arena.page_size();
        let mut ring: Box<CWalRing> = Box::new(unsafe { std::mem::zeroed() });
        unsafe {
            normfs_wal_ring_init(
                ring.as_mut(),
                arena.page_ptr(range.first_slot),
                arena.slot_base(range.first_slot),
                range.page_count,
                page_size,
                first_entry_id,
            );
        }
        ring.pool = arena.pool_ptr();
        ring.first_slot = range.first_slot;

        WalRing {
            backing: Backing::Shared(Arc::clone(arena)),
            ring,
            page_size,
            ring_id,
        }
    }

    /// The shared arena behind this ring, if it has one.
    fn arena(&self) -> Option<&Arc<WalArena>> {
        match &self.backing {
            Backing::Owned { .. } => None,
            Backing::Shared(arena) => Some(arena),
        }
    }

    /// Takes the free arena slot directly above this ring's range.
    ///
    /// Returns whether it grew. `false` means the ring owns its pages, the
    /// range already ends at the arena, or the slot above belongs to another
    /// queue — which contiguity makes a real limit, not a transient one: a ring
    /// grows into the slot above it and nowhere else.
    ///
    /// Everything `normfs_wal_ring_grow` requires is established here: the slot
    /// is checked free under the arena's slot lock, and the page descriptor is
    /// re-initialised so it is well-formed, empty and based at the right arena
    /// offset. Emptiness is not bookkeeping — [`WalRing::seek`] scans every page
    /// in range, so a slot still holding the previous holder's records would
    /// answer this ring's seeks with a foreign payload under a colliding id.
    pub fn grow(&mut self) -> bool {
        let Some(arena) = self.arena().map(Arc::clone) else {
            return false;
        };
        let _slots = arena.lock_slots();

        let slot = self.ring.first_slot + self.ring.page_count;
        if slot >= arena.page_count() || arena.owner_of(slot) != POOL_FREE {
            return false;
        }

        unsafe {
            normfs_wal_page_init(
                arena.page_ptr(slot),
                arena.slot_base(slot),
                self.page_size,
                slot as u64,
                self.ring.next_entry_id,
            );
            normfs_wal_ring_grow(self.ring.as_mut(), self.ring_id);
        }
        true
    }

    /// Gives the top page of this ring's range back to the arena.
    ///
    /// Returns whether it shrank. The top page must not be the one being
    /// appended to, the ring never drops below `floor` pages, and — the
    /// durability half — the page must be reusable: nothing pinning it, and
    /// nothing on it below the durable watermark.
    ///
    /// That last check calls `normfs_wal_page_reusable`, whose contract is
    /// `\result != 0 <==> normfs_wal_page_is_reusable(page, m)` and which is
    /// proven, so the guard *is* the predicate `normfs_wal_ring_shrink`
    /// requires rather than a Rust restatement of it that could drift.
    pub fn shrink(&mut self, floor: usize) -> bool {
        let Some(arena) = self.arena().map(Arc::clone) else {
            return false;
        };
        let _slots = arena.lock_slots();

        let count = self.ring.page_count;
        if count <= floor.max(1) || count <= 1 {
            return false;
        }
        let top = count - 1;
        if self.ring.active >= top {
            return false;
        }
        if unsafe { normfs_wal_page_reusable(self.page(top), self.ring.min_essential_id) } == 0 {
            return false;
        }

        unsafe { normfs_wal_ring_shrink(self.ring.as_mut(), self.ring_id) };
        true
    }

    /// This ring's slot range in the shared arena, if it has one.
    pub fn slot_range(&self) -> Option<crate::wal_arena::SlotRange> {
        self.arena().map(|_| crate::wal_arena::SlotRange {
            first_slot: self.ring.first_slot,
            page_count: self.ring.page_count,
        })
    }

    /// This ring's id in the arena's owner array.
    pub fn ring_id(&self) -> u64 {
        self.ring_id
    }

    /// Page `k` of this ring's range. The C ring's `pages` base is already at
    /// `first_slot`, so `k` is a ring-relative index for both backings.
    fn page(&self, k: usize) -> *mut CWalPage {
        debug_assert!(k < self.ring.page_count, "page {k} is outside this ring");
        unsafe { self.ring.pages.add(k) }
    }

    /// Page `k`'s descriptor, for reads.
    ///
    /// Sound for `&self`: the descriptor is only written by this ring, under
    /// whatever lock its owner holds, and the arena's owner array is what says
    /// no other ring may touch it.
    fn page_ref(&self, k: usize) -> &CWalPage {
        unsafe { &*self.page(k) }
    }

    /// Appends a record, rotating into a reusable page if the active one is
    /// full. Records larger than a page are reported as [`AppendOutcome::TooLarge`].
    ///
    /// Rotation prefers an empty page, then the oldest all-reclaimable page, so
    /// the cached entries stay a contiguous id suffix.
    pub fn append(&mut self, record: &[u8]) -> AppendOutcome {
        if record.len() > u32::MAX as usize {
            return AppendOutcome::TooLarge;
        }
        let size = record.len() as u32;
        let ptr = record.as_ptr();

        let first = unsafe { normfs_wal_ring_try_append(self.ring.as_mut(), ptr, size) };
        match first.status {
            RING_OK => return AppendOutcome::Cached(first.entry_id),
            RING_TOO_LARGE => return AppendOutcome::TooLarge,
            _ => {} // NEEDS_ROTATE
        }

        let idx = match self.oldest_reclaimable_page() {
            Some(k) => k,
            None => return AppendOutcome::Full,
        };
        self.rotate_into(idx);

        let second = unsafe { normfs_wal_ring_try_append(self.ring.as_mut(), ptr, size) };
        match second.status {
            RING_OK => AppendOutcome::Cached(second.entry_id),
            RING_TOO_LARGE => AppendOutcome::TooLarge,
            _ => AppendOutcome::Full,
        }
    }

    /// Discards page `index` and makes it active.
    ///
    /// `normfs_wal_ring_rotate_to` requires the page to be reusable — nothing
    /// pinning it, and nothing on it below the durable watermark. That
    /// precondition is the durability theorem at its one point of use, and the
    /// choice of page is made by [`WalRing::oldest_reclaimable_page`], which is
    /// Rust and unproven. So the choice is checked here against
    /// `normfs_wal_page_reusable`, whose contract is
    /// `\result != 0 <==> normfs_wal_page_is_reusable(page, m)` and which is
    /// proven — the check is the proven predicate itself, not a restatement of
    /// it. `assigns \nothing`, so the shared page reference may be handed over
    /// as a raw pointer.
    fn rotate_into(&mut self, index: usize) {
        debug_assert!(index < self.ring.page_count, "rotate target out of range");
        debug_assert!(
            unsafe { normfs_wal_page_reusable(self.page(index), self.ring.min_essential_id) != 0 },
            "rotating into page {index}, which is pinned or holds records that are not yet durable"
        );
        unsafe { normfs_wal_ring_rotate_to(self.ring.as_mut(), index) };
    }

    /// Picks the page to rotate into: an empty page if any, otherwise the
    /// oldest page whose entries are all below the essential id.
    fn oldest_reclaimable_page(&self) -> Option<usize> {
        let min_essential = self.ring.min_essential_id;
        let mut empty: Option<usize> = None;
        let mut oldest: Option<(usize, u64)> = None;
        for k in 0..self.ring.page_count {
            let p = self.page_ref(k);
            if p.pin_count != 0 {
                continue;
            }
            if p.count == 0 {
                empty.get_or_insert(k);
                continue;
            }
            if p.last_entry_id < min_essential
                && oldest.is_none_or(|(_, fid)| p.first_entry_id < fid)
            {
                oldest = Some((k, p.first_entry_id));
            }
        }
        empty.or(oldest.map(|(k, _)| k))
    }

    /// Steps the id sequence over a record no page can hold, keeping every
    /// record already on a page.
    ///
    /// This is what replaces re-seeding. A record wider than a page still takes
    /// an id, and V1 derives every entry id from position, so the gap has to be
    /// in the sequence the pages agree with. Re-seeding put it there by
    /// renumbering from scratch, which discards the pages — including records
    /// that had been accepted and were not yet on disk.
    ///
    /// The active page is emptied first, by rotation, because
    /// `normfs_wal_ring_scalar_wf` ties `next_entry_id` to the active page's
    /// `first_entry_id + count`. That is also what makes the skipped id fall
    /// strictly *between* two pages, so the ids on any one page stay a dense
    /// run and the pages and the skipped records have one id order between
    /// them.
    ///
    /// False when the active page holds records and no page can be rotated
    /// into — the same condition an append reports as `Full`, and the caller
    /// waits on it the same way.
    pub fn skip_entry(&mut self) -> bool {
        if self.page_ref(self.ring.active).count != 0 {
            let Some(idx) = self.oldest_reclaimable_page() else {
                return false;
            };
            self.rotate_into(idx);
        }
        unsafe { normfs_wal_ring_skip_entry(self.ring.as_mut()) };
        true
    }

    /// Where entry `index` of page `page_index` begins, in that page's bytes.
    ///
    /// The offset table the C page keeps is the only thing that can answer
    /// this, and it is what lets a flush stop part-way through a page: the
    /// bytes of entries up to some id end exactly where the next entry begins.
    pub fn page_entry_offset(&self, page_index: usize, index: u32) -> usize {
        unsafe { normfs_wal_page_offset(self.page(page_index), index) as usize }
    }

    /// Resets the ring to empty, with `first_entry_id` as the next id to cache.
    /// Used to resync the cache after an entry could not be cached.
    ///
    /// A pin outlives this. `normfs_wal_page_init` zeroes `pin_count`, so
    /// carrying the counts across is what keeps `normfs_wal_page_is_reusable`
    /// false for a page a reader is still borrowing bytes from, and moving the
    /// active page off a pinned one keeps appends off those bytes.
    ///
    /// The range keeps its size and its place: this discards contents, not
    /// slots. `normfs_wal_ring_init` unbinds the pool — a ring it builds owns
    /// its pages — so a shared ring is re-bound afterwards, or the next
    /// `grow`/`shrink` would dereference a null pool.
    ///
    /// False when every page is pinned, changing nothing.
    pub fn reinit(&mut self, first_entry_id: u64) -> bool {
        let pins: Vec<u32> = (0..self.ring.page_count)
            .map(|k| self.page_ref(k).pin_count)
            .collect();
        let Some(free) = pins.iter().position(|&c| c == 0) else {
            return false;
        };

        let page_size = self.page_size;
        let n = self.ring.page_count;
        let first_slot = self.ring.first_slot;
        let pages_ptr = self.ring.pages;
        let base = self.ring.arena;
        let pool = self.ring.pool;

        for k in 0..n {
            unsafe {
                normfs_wal_page_init(
                    pages_ptr.add(k),
                    base.add(k * page_size),
                    page_size,
                    (first_slot + k) as u64,
                    first_entry_id,
                );
            }
            unsafe { (*self.page(k)).pin_count = pins[k] };
        }
        unsafe {
            normfs_wal_ring_init(
                self.ring.as_mut(),
                pages_ptr,
                base,
                n,
                page_size,
                first_entry_id,
            );
        }
        self.ring.pool = pool;
        self.ring.first_slot = first_slot;
        if self.page_ref(self.ring.active).pin_count != 0 {
            self.rotate_into(free);
        }
        true
    }

    /// The lowest entry id currently cached, or `None` if the ring is empty.
    pub fn min_cached_id(&self) -> Option<u64> {
        (0..self.ring.page_count)
            .map(|k| self.page_ref(k))
            .filter(|p| p.count > 0)
            .map(|p| p.first_entry_id)
            .min()
    }

    /// Whether the ring holds no cached entries.
    pub fn is_empty(&self) -> bool {
        (0..self.ring.page_count).all(|k| self.page_ref(k).count == 0)
    }

    /// All cached records with id in `[start, end]`, in id order.
    pub fn collect_range(&self, start: u64, end: u64) -> Vec<(u64, Vec<u8>)> {
        let mut out: Vec<(u64, Vec<u8>)> = Vec::new();
        for k in 0..self.ring.page_count {
            let p = self.page_ref(k);
            if p.count == 0 {
                continue;
            }
            // Cheap page-level skip using the page's id span; the per-entry id
            // itself is derived by the proven C page codec below.
            let pfirst = p.first_entry_id;
            let plast = p.first_entry_id + p.count as u64 - 1;
            if plast < start || pfirst > end {
                continue;
            }
            let page = self.page(k);
            for index in 0..p.count {
                // id = first_entry_id + index, from the Frama-C-proven
                // normfs_wal_page_entry_id (assigns \nothing, so the shared
                // page reference may be cast to a mutable pointer for FFI).
                let id = unsafe { normfs_wal_page_entry_id(page, index) };
                if id < start || id > end {
                    continue;
                }
                if let Some(rec) = self.record_at(k, index) {
                    out.push((id, rec.to_vec()));
                }
            }
        }
        out.sort_by_key(|(id, _)| *id);
        out
    }

    // Reads the record of entry `index` on page `page_index`. The C calls are
    // proven `assigns \nothing`, so casting the shared page reference to a
    // mutable pointer for FFI is sound.
    fn record_at(&self, page_index: usize, index: u32) -> Option<&[u8]> {
        let page = self.page(page_index);
        let offset = unsafe { normfs_wal_page_offset(page, index) as usize };
        let buffer = self.page_slice(page_index);
        let framed = &buffer[offset..];
        let decoded = unsafe { normfs_wal_entry_v1_decode(framed.as_ptr(), framed.len()) };
        if decoded.status != ENTRY_OK {
            return None;
        }
        let s = offset + decoded.record_offset;
        Some(&buffer[s..s + decoded.record_size])
    }

    /// Advances the reclaim boundary: entries with id `< min_essential_id` may
    /// have their pages reused.
    pub fn set_essential(&mut self, min_essential_id: u64) {
        unsafe { normfs_wal_ring_set_essential(self.ring.as_mut(), min_essential_id) };
    }

    /// Locates the entry with `entry_id`, returning its `(page_index, index)`.
    pub fn seek(&self, entry_id: u64) -> Option<(usize, u32)> {
        let ring = &*self.ring as *const CWalRing as *mut CWalRing;
        let r = unsafe { normfs_wal_ring_seek(ring, entry_id) };
        if r.found != 0 {
            Some((r.page_index, r.index))
        } else {
            None
        }
    }

    /// Returns the record bytes of the entry at `(page_index, index)`.
    pub fn record(&self, page_index: usize, index: u32) -> Option<&[u8]> {
        self.record_at(page_index, index)
    }

    /// Convenience: seek then read the record for `entry_id`.
    pub fn get(&self, entry_id: u64) -> Option<&[u8]> {
        let (page_index, index) = self.seek(entry_id)?;
        self.record(page_index, index)
    }

    /// Pins page `page_index` so it cannot be reclaimed until unpinned.
    pub fn pin(&mut self, page_index: usize) {
        unsafe { normfs_wal_page_pin(self.page(page_index)) };
    }

    /// Releases a pin taken with [`WalRing::pin`].
    pub fn unpin(&mut self, page_index: usize) {
        unsafe { normfs_wal_page_unpin(self.page(page_index)) };
    }

    /// The id that the next appended entry will receive.
    pub fn next_entry_id(&self) -> u64 {
        self.ring.next_entry_id
    }

    /// The id the next rotation will stamp on the page it takes.
    ///
    /// `normfs_wal_ring_rotate_to` increments this on every rotation and
    /// nothing else touches it, so a change across an append is an exact
    /// rotation detector. Comparing the active index instead misses the case
    /// where the ring rotates into the page that was already active — legal
    /// when that page is empty or fully durable — which resets its bytes while
    /// leaving a stale write cursor behind.
    pub fn next_page_id(&self) -> u64 {
        self.ring.next_page_id
    }

    /// Number of pages in the ring.
    ///
    /// Read from the C ring rather than from a `Vec` length: growing and
    /// shrinking move this, and the descriptor array it indexes into belongs to
    /// the whole arena rather than to this ring.
    pub fn page_count(&self) -> usize {
        self.ring.page_count
    }

    /// The index of the page currently being appended to.
    pub fn active_page(&self) -> usize {
        self.ring.active
    }

    /// The framed entry bytes of page `page_index`, in append order.
    ///
    /// This is the page's forward region only — the backward offset table is a
    /// memory-only index and is deliberately excluded, so these bytes are
    /// exactly a run of the V1 entry stream and can go to the file unchanged.
    /// Handing this out is what lets the writer put a page on disk without
    /// copying it into a buffer of its own.
    pub fn page_bytes(&self, page_index: usize) -> &[u8] {
        &self.page_slice(page_index)[..self.page_ref(page_index).used_bytes]
    }

    /// Page `page_index`'s whole slice of the arena.
    ///
    /// Taken from the C ring's arena base, which is already at this ring's
    /// first slot, so `page_index` is ring-relative for a shared ring exactly
    /// as it is for one that owns its pages.
    fn page_slice(&self, page_index: usize) -> &[u8] {
        debug_assert!(
            page_index < self.ring.page_count,
            "page {page_index} is outside this ring"
        );
        // SAFETY: the arena covers page_count * page_size bytes from this base
        // — for a shared ring that is `normfs_wal_ring_layout`, which grow
        // re-establishes — and the backing outlives `self`.
        unsafe {
            std::slice::from_raw_parts(
                self.ring.arena.add(page_index * self.page_size),
                self.page_size,
            )
        }
    }

    /// Entries held by page `page_index`.
    pub fn page_len(&self, page_index: usize) -> u32 {
        self.page_ref(page_index).count
    }

    /// The last entry id on page `page_index`, or `None` if it is empty.
    pub fn page_last_entry_id(&self, page_index: usize) -> Option<u64> {
        let p = self.page_ref(page_index);
        (p.count > 0).then_some(p.last_entry_id)
    }

    /// The first entry id on page `page_index`, or `None` if it is empty.
    pub fn page_first_entry_id(&self, page_index: usize) -> Option<u64> {
        let p = self.page_ref(page_index);
        (p.count > 0).then_some(p.first_entry_id)
    }

    /// How many readers currently hold page `page_index`.
    pub fn page_pin_count(&self, page_index: usize) -> u32 {
        self.page_ref(page_index).pin_count
    }

    /// Whether page `page_index` may be written over: nothing pinning it, and
    /// nothing on it below `min_essential_id`.
    ///
    /// This calls the proven `normfs_wal_page_reusable`, whose contract is
    /// `\result != 0 <==> normfs_wal_page_is_reusable(page, m)`, so callers get
    /// the predicate the C contracts require rather than a Rust restatement of
    /// it that could drift from it. `assigns \nothing`.
    pub fn page_is_reusable(&self, page_index: usize, min_essential_id: u64) -> bool {
        unsafe { normfs_wal_page_reusable(self.page(page_index), min_essential_id) != 0 }
    }

    /// The reclaim boundary the ring is currently holding to.
    pub fn min_essential_id(&self) -> u64 {
        self.ring.min_essential_id
    }

    /// The page size the ring was built with.
    pub fn page_size(&self) -> usize {
        self.page_size
    }
}

// The ring may cross threads under an external lock, and shared (`&self`)
// access only calls C functions proven to write nothing, so concurrent reads
// are safe.
//
// A ring that owns its pages points only at its own heap allocations, which
// nothing else can reach. A shared one points into an arena other rings also
// point into — but only ever at the slots the arena's owner array says are
// this ring's, and the C pool proves that ownership is exclusive: there is one
// owner slot per page, so two rings holding one page is not a representable
// state. The `Arc` keeps the arena alive for at least as long as the ring.
unsafe impl Send for WalRing {}
unsafe impl Sync for WalRing {}
