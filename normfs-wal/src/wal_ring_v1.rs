//! Safe wrapper over the verified C V1 WAL paged memory store.
//!
//! Rust owns the memory: one byte buffer per page plus the page-descriptor
//! array. The C layer (proven with Frama-C WP) does the appends, offset table,
//! rotation choice and cursor arithmetic; this module sequences them and hands
//! back safe slices.

use std::os::raw::c_int;

#[repr(C)]
struct CWalPage {
    buf: *mut u8,
    cap: usize,
    used_bytes: usize,
    count: u32,
    page_id: u64,
    first_entry_id: u64,
    last_entry_id: u64,
    pin_count: u32,
    published: c_int,
}

#[repr(C)]
struct CWalRing {
    pages: *mut CWalPage,
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
    fn normfs_wal_page_init(
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

    fn normfs_wal_ring_init(
        ring: *mut CWalRing,
        pages: *mut CWalPage,
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

/// A ring of WAL pages holding V1 entries in memory.
///
/// The buffers and the descriptor array are heap-allocated `Vec`s, so the raw
/// pointers the C ring holds stay valid even if the `WalRing` value is moved;
/// they are never reallocated after construction.
pub struct WalRing {
    buffers: Vec<Vec<u8>>,
    pages: Vec<CWalPage>,
    ring: Box<CWalRing>,
    page_size: usize,
}

impl WalRing {
    /// Creates a ring of `page_count` pages of `page_size` bytes each, whose
    /// first entry id is `first_entry_id`. `page_count` must be at least 1 and
    /// `page_size` must exceed the minimum entry framing.
    pub fn new(page_count: usize, page_size: usize, first_entry_id: u64) -> Self {
        assert!(page_count >= 1, "ring needs at least one page");
        assert!(page_size >= 9, "page must hold the smallest entry plus its offset");

        let mut buffers: Vec<Vec<u8>> = (0..page_count).map(|_| vec![0u8; page_size]).collect();
        let mut pages: Vec<CWalPage> = Vec::with_capacity(page_count);
        for (k, buffer) in buffers.iter_mut().enumerate() {
            let mut page: CWalPage = unsafe { std::mem::zeroed() };
            unsafe {
                normfs_wal_page_init(
                    &mut page,
                    buffer.as_mut_ptr(),
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
                page_count,
                page_size,
                first_entry_id,
            );
        }

        WalRing {
            buffers,
            pages,
            ring,
            page_size,
        }
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
        unsafe { normfs_wal_ring_rotate_to(self.ring.as_mut(), idx) };

        let second = unsafe { normfs_wal_ring_try_append(self.ring.as_mut(), ptr, size) };
        match second.status {
            RING_OK => AppendOutcome::Cached(second.entry_id),
            RING_TOO_LARGE => AppendOutcome::TooLarge,
            _ => AppendOutcome::Full,
        }
    }

    /// Picks the page to rotate into: an empty page if any, otherwise the
    /// oldest page whose entries are all below the essential id.
    fn oldest_reclaimable_page(&self) -> Option<usize> {
        let min_essential = self.ring.min_essential_id;
        let mut empty: Option<usize> = None;
        let mut oldest: Option<(usize, u64)> = None;
        for (k, p) in self.pages.iter().enumerate() {
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

    /// Resets the ring to empty, with `first_entry_id` as the next id to cache.
    /// Used to resync the cache after an entry could not be cached.
    pub fn reinit(&mut self, first_entry_id: u64) {
        let page_size = self.page_size;
        let n = self.pages.len();
        for k in 0..n {
            let buf = self.buffers[k].as_mut_ptr();
            unsafe {
                normfs_wal_page_init(&mut self.pages[k], buf, page_size, k as u64, first_entry_id);
            }
        }
        let pages_ptr = self.pages.as_mut_ptr();
        unsafe {
            normfs_wal_ring_init(self.ring.as_mut(), pages_ptr, n, page_size, first_entry_id);
        }
    }

    /// The lowest entry id currently cached, or `None` if the ring is empty.
    pub fn min_cached_id(&self) -> Option<u64> {
        self.pages
            .iter()
            .filter(|p| p.count > 0)
            .map(|p| p.first_entry_id)
            .min()
    }

    /// Whether the ring holds no cached entries.
    pub fn is_empty(&self) -> bool {
        self.pages.iter().all(|p| p.count == 0)
    }

    /// All cached records with id in `[start, end]`, in id order.
    pub fn collect_range(&self, start: u64, end: u64) -> Vec<(u64, Vec<u8>)> {
        let mut out: Vec<(u64, Vec<u8>)> = Vec::new();
        for (k, p) in self.pages.iter().enumerate() {
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
            let page = &self.pages[k] as *const CWalPage as *mut CWalPage;
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
        let page = &self.pages[page_index] as *const CWalPage as *mut CWalPage;
        let offset = unsafe { normfs_wal_page_offset(page, index) as usize };
        let buffer = &self.buffers[page_index];
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
        unsafe { normfs_wal_page_pin(&mut self.pages[page_index]) };
    }

    /// Releases a pin taken with [`WalRing::pin`].
    pub fn unpin(&mut self, page_index: usize) {
        unsafe { normfs_wal_page_unpin(&mut self.pages[page_index]) };
    }

    /// The id that the next appended entry will receive.
    pub fn next_entry_id(&self) -> u64 {
        self.ring.next_entry_id
    }

    /// The page size the ring was built with.
    pub fn page_size(&self) -> usize {
        self.page_size
    }
}

// The raw pointers the C ring holds refer only to this value's own heap
// allocations, which are never shared, so the ring may cross threads under an
// external lock. Shared (`&self`) access only calls C functions proven to
// write nothing, so concurrent reads are safe.
unsafe impl Send for WalRing {}
unsafe impl Sync for WalRing {}
