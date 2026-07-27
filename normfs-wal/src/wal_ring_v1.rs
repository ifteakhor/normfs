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
struct CRingReusableResult {
    index: usize,
    found: c_int,
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
    fn normfs_wal_ring_find_reusable(ring: *mut CWalRing) -> CRingReusableResult;
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
            _ => {}
        }

        // Active page is full: find a reusable page and retry once.
        let reusable = unsafe { normfs_wal_ring_find_reusable(self.ring.as_mut()) };
        if reusable.found == 0 {
            return AppendOutcome::Full;
        }
        unsafe { normfs_wal_ring_rotate_to(self.ring.as_mut(), reusable.index) };

        let second = unsafe { normfs_wal_ring_try_append(self.ring.as_mut(), ptr, size) };
        match second.status {
            RING_OK => AppendOutcome::Cached(second.entry_id),
            RING_TOO_LARGE => AppendOutcome::TooLarge,
            _ => AppendOutcome::Full,
        }
    }

    /// Advances the reclaim boundary: entries with id `< min_essential_id` may
    /// have their pages reused.
    pub fn set_essential(&mut self, min_essential_id: u64) {
        unsafe { normfs_wal_ring_set_essential(self.ring.as_mut(), min_essential_id) };
    }

    /// Locates the entry with `entry_id`, returning its `(page_index, index)`.
    pub fn seek(&mut self, entry_id: u64) -> Option<(usize, u32)> {
        let r = unsafe { normfs_wal_ring_seek(self.ring.as_mut(), entry_id) };
        if r.found != 0 {
            Some((r.page_index, r.index))
        } else {
            None
        }
    }

    /// The entry id of the `index`-th entry of page `page_index`.
    pub fn entry_id(&mut self, page_index: usize, index: u32) -> u64 {
        unsafe { normfs_wal_page_entry_id(&mut self.pages[page_index], index) }
    }

    /// Returns the record bytes of the entry at `(page_index, index)`, borrowed
    /// out of the page buffer, or `None` if the entry does not decode.
    pub fn record(&mut self, page_index: usize, index: u32) -> Option<&[u8]> {
        let offset = unsafe {
            normfs_wal_page_offset(&mut self.pages[page_index], index) as usize
        };
        let buffer = &self.buffers[page_index];
        let framed = &buffer[offset..];
        let decoded = unsafe { normfs_wal_entry_v1_decode(framed.as_ptr(), framed.len()) };
        if decoded.status != ENTRY_OK {
            return None;
        }
        let start = offset + decoded.record_offset;
        Some(&buffer[start..start + decoded.record_size])
    }

    /// Convenience: seek then read the record for `entry_id`.
    pub fn get(&mut self, entry_id: u64) -> Option<&[u8]> {
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
// external lock.
unsafe impl Send for WalRing {}
