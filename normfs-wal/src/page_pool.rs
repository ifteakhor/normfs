//! A fixed pool of WAL pages that writers wait on.
//!
//! The pool owns one [`WalRing`] — every byte it will ever use is allocated
//! once, at construction — and hands out entry ids as records are appended into
//! pages. When no page can be reclaimed, an appending task **waits** rather than
//! failing or discarding: the pages still hold records that are not yet on disk,
//! and dropping them is the one thing this store must never do.
//!
//! The waiting lives here and not in C. The C ring reports `NEEDS_ROTATE` and
//! `find_reusable` reports "none", and it never blocks, never allocates and
//! never learns what a file is — which is what keeps it portable down to a
//! microcontroller later.
//!
//! ## Why a page can be reused
//!
//! Reuse is governed entirely by the proven C predicate
//! `normfs_wal_page_is_reusable(p, m)`:
//!
//! ```text
//! p->pin_count == 0 && (p->count == 0 || p->last_entry_id < m)
//! ```
//!
//! and `normfs_wal_ring_rotate_to` requires it. The two conjuncts are two
//! different claims, and this module is what gives each its meaning:
//!
//! * `pin_count` — a reader or a stream is still looking at these bytes.
//! * `min_essential_id` — **the bytes are on disk**. [`PagePool::mark_durable`]
//!   is the only thing that advances it, and its caller may only call it after
//!   an `fsync` has returned.
//!
//! Together those give the property the store exists for: a record that
//! `append` accepted is never overwritten in memory until it has been written
//! *and* synced. The C side proves the "never overwritten" half; this module is
//! responsible for never advancing the watermark on anything less than a
//! completed sync.

use std::sync::Mutex;
use std::time::Duration;

use tokio::sync::Notify;

use crate::wal_ring_v1::{AppendOutcome, WalRing};

/// How long a task may wait for a page before the pool starts reporting who is
/// holding things up. Waiting itself is not an error — a full pool means the
/// disk is behind, and back-pressure is the correct response — but an
/// indefinite wait with no explanation is not debuggable.
const STALL_WARN_AFTER: Duration = Duration::from_secs(5);

/// Why an append could not be satisfied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolError {
    /// The record is larger than a whole page, so no page could ever hold it.
    /// Waiting would never help.
    TooLarge,
}

/// A run of bytes on one page that the file writer has not written yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingWrite {
    pub page_index: usize,
    /// Offset into the page's entry region where the unwritten run starts.
    pub from: usize,
    /// Offset one past its end.
    pub to: usize,
    /// The last entry id covered, so the caller knows what its `fsync` makes
    /// durable.
    pub last_entry_id: u64,
}

impl PendingWrite {
    pub fn len(&self) -> usize {
        self.to - self.from
    }

    pub fn is_empty(&self) -> bool {
        self.from == self.to
    }
}

struct Inner {
    ring: WalRing,
    /// Per page: how many of its bytes the file writer has taken. A page is
    /// appended to while it is being written out, so this is a cursor rather
    /// than a flag — the writer takes the run that appeared since last time.
    written: Vec<usize>,
}

pub struct PagePool {
    inner: Mutex<Inner>,
    /// Set once a file writer is draining this pool. Until then nothing can
    /// report pages durable, so nothing may wait for one to be freed.
    drainer: std::sync::atomic::AtomicBool,
    /// Woken when [`PagePool::mark_durable`] advances the watermark, which is
    /// the only event that can turn a full pool into a pool with room.
    space: Notify,
}

/// Appends into the ring, resetting the write cursor on rotation into a page.
fn append_locked(inner: &mut Inner, record: &[u8]) -> AppendOutcome {
    let before = inner.ring.active_page();
    let outcome = inner.ring.append(record);
    let after = inner.ring.active_page();
    if after != before {
        inner.written[after] = 0;
    }
    outcome
}

impl PagePool {
    /// Allocates `page_count` pages of `page_size` bytes. This is the only
    /// allocation the pool ever performs.
    pub fn new(page_count: usize, page_size: usize, first_entry_id: u64) -> Self {
        PagePool {
            inner: Mutex::new(Inner {
                ring: WalRing::new(page_count, page_size, first_entry_id),
                written: vec![0; page_count],
            }),
            space: Notify::new(),
            drainer: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Whether a file writer is taking pages from this pool, and therefore
    /// whether waiting for a page can ever end.
    pub fn has_drainer(&self) -> bool {
        self.drainer.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Marks that a file writer has taken responsibility for draining this
    /// pool. Callers may wait for a page only after this.
    pub fn set_drainer(&self) {
        self.drainer.store(true, std::sync::atomic::Ordering::Release);
    }

    /// Appends without waiting. `Full` means every page is either pinned by a
    /// reader or still holds records that are not on disk.
    pub fn try_append(&self, record: &[u8]) -> AppendOutcome {
        let mut inner = self.inner.lock().unwrap();
        append_locked(&mut inner, record)
    }

    /// Appends the record that must land on `expected_id`, waiting until a
    /// page can be reclaimed.
    ///
    /// The caller owns the id sequence; the pool follows it. They can only
    /// disagree after a record the pool refused, and re-seeding to catch up
    /// discards pages, so callers must serialise their appends.
    pub async fn append_at(&self, expected_id: u64, record: &[u8]) -> Result<(), PoolError> {
        let mut waited = false;
        loop {
            let woken = self.space.notified();
            {
                let mut inner = self.inner.lock().unwrap();
                if inner.ring.next_entry_id() != expected_id {
                    inner.ring.reinit(expected_id);
                    inner.written.iter_mut().for_each(|w| *w = 0);
                }
                let outcome = append_locked(&mut inner, record);
                match outcome {
                    AppendOutcome::Cached(_) => {
                        if waited {
                            log::debug!(target: "normfs-wal", "page pool: resumed at entry {expected_id}");
                        }
                        return Ok(());
                    }
                    AppendOutcome::TooLarge => return Err(PoolError::TooLarge),
                    AppendOutcome::Full => {}
                }
            }
            waited = true;
            if tokio::time::timeout(STALL_WARN_AFTER, woken).await.is_err() {
                self.warn_stalled();
            }
        }
    }

    /// Steps the id sequence over a record the pool could not hold, without
    /// losing anything it is still holding.
    ///
    /// Re-seeding drops whatever the pages contain, so it waits until the
    /// writer has reported everything durable first. A record too large for a
    /// page is rare, and paying a drain for it is the price of never dropping
    /// one that was already accepted.
    pub async fn skip_to(&self, next_id: u64) {
        loop {
            let woken = self.space.notified();
            {
                let mut inner = self.inner.lock().unwrap();
                if inner.ring.min_essential_id() >= inner.ring.next_entry_id() {
                    inner.ring.reinit(next_id);
                    inner.written.iter_mut().for_each(|w| *w = 0);
                    return;
                }
            }
            if tokio::time::timeout(STALL_WARN_AFTER, woken).await.is_err() {
                log::warn!(
                    target: "normfs-wal",
                    "waiting to step over an oversized record: pool not yet drained"
                );
            }
        }
    }

    /// Reports which page is holding the pool up, so an indefinite wait can be
    /// diagnosed instead of guessed at.
    fn warn_stalled(&self) {
        let inner = self.inner.lock().unwrap();
        let essential = inner.ring.min_essential_id();
        let pinned = (0..inner.ring.page_count())
            .filter(|&k| inner.ring.page_pin_count(k) > 0)
            .count();
        let oldest = (0..inner.ring.page_count())
            .filter_map(|k| inner.ring.page_last_entry_id(k).map(|last| (k, last)))
            .min_by_key(|&(_, last)| last);
        match oldest {
            Some((k, last)) => log::warn!(
                target: "normfs-wal",
                "page pool full for over {}s: {} of {} pages pinned by readers, \
                 durable up to {}, oldest page {} ends at entry {} (pins {})",
                STALL_WARN_AFTER.as_secs(),
                pinned,
                inner.ring.page_count(),
                essential,
                k,
                last,
                inner.ring.page_pin_count(k),
            ),
            None => log::warn!(
                target: "normfs-wal",
                "page pool full for over {}s but every page is empty — this is a bug",
                STALL_WARN_AFTER.as_secs(),
            ),
        }
    }

    /// Byte runs that have been appended but not yet handed to the file writer,
    /// oldest first.
    ///
    /// The bytes are copied out under the lock rather than borrowed: the writer
    /// awaits I/O, and holding the pool locked across that would block every
    /// appender for the duration of a disk write.
    ///
    /// Doesn't mark runs as taken — call [`PagePool::commit_written`] once a
    /// run is actually on disk, or an uncommitted run just comes back here.
    pub fn take_pending(&self) -> Vec<(PendingWrite, Vec<u8>)> {
        let inner = self.inner.lock().unwrap();
        let count = inner.ring.page_count();
        let mut out: Vec<(PendingWrite, Vec<u8>)> = Vec::new();

        for k in 0..count {
            let used = inner.ring.page_bytes(k).len();
            let from = inner.written[k];
            if from >= used || inner.ring.page_len(k) == 0 {
                continue;
            }
            let Some(last_entry_id) = inner.ring.page_last_entry_id(k) else {
                continue;
            };
            let bytes = inner.ring.page_bytes(k)[from..used].to_vec();
            out.push((
                PendingWrite {
                    page_index: k,
                    from,
                    to: used,
                    last_entry_id,
                },
                bytes,
            ));
        }

        out.sort_by_key(|(w, _)| w.last_entry_id);
        out
    }

    /// Marks a run from [`PagePool::take_pending`] as written. Only moves the
    /// cursor forward.
    pub fn commit_written(&self, page_index: usize, up_to: usize) {
        let mut inner = self.inner.lock().unwrap();
        if up_to > inner.written[page_index] {
            inner.written[page_index] = up_to;
        }
    }

    /// Records that every entry below `first_non_durable_id` is on disk, and
    /// wakes whoever is waiting for a page.
    ///
    /// **Only call this after an `fsync` covering those entries has returned.**
    /// Everything the pool promises rests on that: the C ring will hand a page
    /// to be overwritten as soon as this watermark passes its last entry, and it
    /// is right to do so exactly when the bytes are safe.
    pub fn mark_durable(&self, first_non_durable_id: u64) {
        {
            let mut inner = self.inner.lock().unwrap();
            if first_non_durable_id <= inner.ring.min_essential_id() {
                return;
            }
            inner.ring.set_essential(first_non_durable_id);
        }
        self.space.notify_waiters();
    }

    /// The id the next appended record will get.
    pub fn next_entry_id(&self) -> u64 {
        self.inner.lock().unwrap().ring.next_entry_id()
    }

    /// Everything below this id is on disk.
    pub fn durable_before(&self) -> u64 {
        self.inner.lock().unwrap().ring.min_essential_id()
    }

    /// Whether the pool holds no records.
    pub fn is_empty(&self) -> bool {
        self.inner.lock().unwrap().ring.is_empty()
    }

    /// The lowest id still held in memory, or `None` when nothing is.
    pub fn min_cached_id(&self) -> Option<u64> {
        self.inner.lock().unwrap().ring.min_cached_id()
    }

    /// Every held record with id in `[start, end]`, in id order.
    pub fn collect_range(&self, start: u64, end: u64) -> Vec<(u64, Vec<u8>)> {
        self.inner.lock().unwrap().ring.collect_range(start, end)
    }

    /// Restarts the pool empty, numbering from `first_entry_id`.
    ///
    /// This drops whatever the pages held, so it is only for resynchronising a
    /// pool that has fallen out of step with the id sequence — never for making
    /// room. Making room is what waiting is for.
    pub fn reseed(&self, first_entry_id: u64) {
        let mut inner = self.inner.lock().unwrap();
        inner.ring.reinit(first_entry_id);
        inner.written.iter_mut().for_each(|w| *w = 0);
    }
}
