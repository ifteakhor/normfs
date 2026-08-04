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

/// What the enqueue side decided about a record, for the writer to carry out.
///
/// The writer used to decide rotation itself, from `AckFileWriter::can_add`.
/// It cannot any more: the bytes are in a page before the writer sees the
/// entry, so by the time it decided, the record that triggers the rotation had
/// already been flushed into the file it was supposed to start *after*. The
/// decision therefore moves to the one place that runs before the bytes enter a
/// page, and reaches the writer as an instruction rather than a hint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Placement {
    /// The record is already in a page, so its bytes reach the file from there
    /// and must not be buffered again.
    pub in_pool: bool,
    pub rotate: RotateHint,
    /// Which file this record was charged to. A tripwire, not a control input.
    pub epoch: u64,
}

impl Placement {
    /// The placement for a caller that has no pool: the writer decides
    /// rotation exactly as it always did.
    pub fn legacy() -> Self {
        Placement::default()
    }
}

/// Whether the writer must rotate before this record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RotateHint {
    /// No decision was made for this record; the writer makes its own, from
    /// its own accounting. This is the path `WalStore::enqueue` takes, and it
    /// behaves exactly as it did before pages existed.
    #[default]
    WriterDecides,
    /// Rotate, then write. The pool has already sealed the page so this record
    /// starts a fresh one.
    Before,
    /// Do not rotate, whatever the writer's own accounting says.
    None,
}

/// What [`PagePool::charge`] decided about a record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RotateDecision {
    pub rotate_before: bool,
    /// The file the record was charged to, counted from zero.
    pub epoch: u64,
}

/// How full the currently-open WAL file is.
///
/// This lives here rather than in `AckFileWriter` because an `AckFileWriter` is
/// per-file and dies on rotation, while the decision has to be made on the
/// enqueue path, before the bytes enter a page. It is the *only* fill
/// accounting on the pooled path: `AckFileWriter::current_size` is not
/// maintained for pooled records, so the two can never drift.
struct FileFill {
    used: u64,
    max: u64,
    header_len: u64,
    /// Whether this file has been charged an entry yet. A file always takes at
    /// least one record however large it is, or an oversized record would
    /// rotate forever into empty files.
    has_written: bool,
    epoch: u64,
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
    /// Fill of the open file, once a writer has armed it.
    fill: Option<FileFill>,
    /// Set by [`PagePool::seal_active`], cleared by the next append that lands.
    /// It survives a wait for a page on purpose — see `append_locked`.
    seal_pending: bool,
    /// File boundaries drawn but not yet reached by the writer, oldest first.
    /// Each is an exclusive id bound: entries below it belong to the file that
    /// opened before it.
    ///
    /// A queue rather than a single value, because the enqueue side runs ahead
    /// of the writer — the channel between them is unbounded — so several
    /// rotations can be charged before the writer performs the first. With one
    /// slot, the second seal would overwrite the boundary the open file still
    /// needed, and its closing flush would pull the next file's records into
    /// it: the very bug the bound exists to prevent, just harder to see.
    ///
    /// Empty means the open file runs to the end of what the pages hold.
    boundaries: std::collections::VecDeque<u64>,
}

/// Appends under the pool lock, keeping the write cursor and the seal honest.
///
/// Two things have to happen around the append itself and neither is the ring's
/// business:
///
/// * A rotation resets a page's bytes, and with them the file writer's cursor
///   into that page. Rotation is detected by `next_page_id`, which
///   `normfs_wal_ring_rotate_to` increments and nothing else touches. Comparing
///   the active index instead misses the ring rotating into the page that was
///   already active — legal when that page is empty or fully durable — which
///   would leave a stale cursor and silently swallow the new page's first bytes.
/// * The seal clears only once a record has actually landed. If the pool is
///   full, `append_at` waits and comes back here, and the seal must still be in
///   force: a full pool may delay the first record of the next file, but it may
///   never let it share a page with the last record of the previous one.
fn append_locked(inner: &mut Inner, record: &[u8]) -> AppendOutcome {
    let before_page_id = inner.ring.next_page_id();
    let outcome = if inner.seal_pending {
        inner.ring.append_on_fresh_page(record)
    } else {
        inner.ring.append(record)
    };
    if inner.ring.next_page_id() != before_page_id {
        let active = inner.ring.active_page();
        inner.written[active] = 0;
    }
    if matches!(outcome, AppendOutcome::Cached(_)) {
        inner.seal_pending = false;
    }
    outcome
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

impl PagePool {
    /// Allocates `page_count` pages of `page_size` bytes. This is the only
    /// allocation the pool ever performs.
    pub fn new(page_count: usize, page_size: usize, first_entry_id: u64) -> Self {
        PagePool {
            inner: Mutex::new(Inner {
                ring: WalRing::new(page_count, page_size, first_entry_id),
                written: vec![0; page_count],
                fill: None,
                seal_pending: false,
                boundaries: std::collections::VecDeque::new(),
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

    /// Takes over the rotation decision from the writer.
    ///
    /// Called once when the WAL writer starts, beside
    /// [`PagePool::set_drainer`]. Until it is called, [`PagePool::charge`]
    /// declines to decide and the writer keeps deciding for itself, which is
    /// what the unpooled path relies on.
    ///
    /// `header_len` should be the *widest* a V1 header can be, not the current
    /// one's encoded length: `WalHeader::resize` can widen the id and data size
    /// fields when a file rotates, and the enqueue side cannot know the next
    /// header's exact size without duplicating that logic. Over-charging by a
    /// few bytes rotates fractionally early, which is the safe direction
    /// against a cap.
    pub fn arm_file_fill(&self, max_file_size: u64, header_len: u64) {
        let mut inner = self.inner.lock().unwrap();
        inner.fill = Some(FileFill {
            used: header_len,
            max: max_file_size,
            header_len,
            has_written: false,
            epoch: 0,
        });
    }

    /// Charges `entry_len` encoded bytes to the open file, and says whether the
    /// writer must rotate before this record.
    ///
    /// `entry_len` is the *encoded* length — `wal_entry_v1::encoded_len` — the
    /// same number the writer used to pass to `AckFileWriter::can_add`, so the
    /// file boundary lands where it always did.
    ///
    /// The caller must hold whatever serialises appends for this queue, so that
    /// charge, seal and append are one step: two records interleaving between
    /// the charge and the seal would put the boundary on the wrong entry.
    pub fn charge(&self, entry_len: u64) -> RotateDecision {
        let mut inner = self.inner.lock().unwrap();
        let Some(fill) = inner.fill.as_mut() else {
            // Not armed: no writer is taking pages from this pool yet, so
            // there is no file to fill and nothing to decide.
            return RotateDecision {
                rotate_before: false,
                epoch: 0,
            };
        };

        // `has_written` first, and it is why this is a field rather than
        // something derived from `used`: a file takes its first record whatever
        // its size, or a record larger than max_file_size would rotate forever
        // into empty files. This is the same guard the writer applied.
        if fill.has_written && fill.used.saturating_add(entry_len) > fill.max {
            fill.epoch += 1;
            fill.used = fill.header_len.saturating_add(entry_len);
            fill.has_written = true;
            RotateDecision {
                rotate_before: true,
                epoch: fill.epoch,
            }
        } else {
            fill.used = fill.used.saturating_add(entry_len);
            fill.has_written = true;
            RotateDecision {
                rotate_before: false,
                epoch: fill.epoch,
            }
        }
    }

    /// Draws a file boundary here: everything appended so far belongs to the
    /// file that is open, and the next record starts a fresh page.
    ///
    /// Called at enqueue time, when [`PagePool::charge`] says the record about
    /// to be appended has to start a new file. Both halves matter and each is
    /// useless without the other — the seal is what makes the bound in
    /// [`PagePool::take_pending`] exact rather than approximate.
    ///
    /// Cannot fail and does not await. Forcing the rotation now would have to
    /// handle "no page is reclaimable", which means either blocking here — a
    /// second copy of the wait that `append_at` already owns — or failing and
    /// pushing a retry loop onto the caller. Instead the next append rotates,
    /// and if it has to wait for a page first, the seal waits with it.
    pub fn seal_active(&self) {
        let mut inner = self.inner.lock().unwrap();
        // An active page with nothing on it is already fresh; sealing it would
        // rotate away a page for no reason.
        let active = inner.ring.active_page();
        if inner.ring.page_len(active) > 0 {
            inner.seal_pending = true;
        }
        let boundary = inner.ring.next_entry_id();
        inner.boundaries.push_back(boundary);
    }

    /// Reports that the writer has reached the oldest boundary and opened the
    /// next file, so flushes may now run up to the boundary after it.
    ///
    /// Must be called only *after* the new file's writer exists. The old
    /// writer's final flush runs inside `close()` and has to stay bounded, or
    /// it drains the next file's records into the old file — which is the bug
    /// the bound is here to prevent.
    pub fn advance_file(&self) {
        let mut inner = self.inner.lock().unwrap();
        if inner.boundaries.pop_front().is_none() {
            log::error!(
                target: "normfs-wal",
                "the writer rotated with no file boundary drawn for it: the enqueue side did not \
                 seal this rotation, so a page may hold entries for both files"
            );
            debug_assert!(false, "writer rotated without a boundary from the pool");
        }
    }

    /// Bytes charged to the open file so far, including its header. For tests.
    pub fn fill_used(&self) -> Option<u64> {
        self.inner.lock().unwrap().fill.as_ref().map(|f| f.used)
    }

    /// The exclusive id bound a flush will stop at, if one is drawn. For tests.
    pub fn accept_below(&self) -> Option<u64> {
        self.inner.lock().unwrap().boundaries.front().copied()
    }

    /// How many file boundaries have been drawn but not yet reached by the
    /// writer. For tests.
    pub fn pending_boundaries(&self) -> usize {
        self.inner.lock().unwrap().boundaries.len()
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
                match append_locked(&mut inner, record) {
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

    /// Appends, waiting until a page can be reclaimed.
    ///
    /// Returns only once the record is in a page and has an id. It does not
    /// wait for that page to reach disk — [`PagePool::mark_durable`] is what
    /// reports that, later.
    pub async fn append(&self, record: &[u8]) -> Result<u64, PoolError> {
        let mut waited = false;
        loop {
            // Registered before the attempt: a `mark_durable` landing between
            // the attempt and the await must not be missed, or the waiter
            // sleeps until the next one and a quiet queue stalls forever.
            let woken = self.space.notified();

            match self.try_append(record) {
                AppendOutcome::Cached(id) => {
                    if waited {
                        log::debug!(target: "normfs-wal", "page pool: resumed, entry {id} placed");
                    }
                    return Ok(id);
                }
                AppendOutcome::TooLarge => return Err(PoolError::TooLarge),
                AppendOutcome::Full => {}
            }

            waited = true;
            if tokio::time::timeout(STALL_WARN_AFTER, woken).await.is_err() {
                self.warn_stalled();
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
    /// oldest first — and never past the end of the file that is open.
    ///
    /// The bytes are copied out under the lock rather than borrowed: the writer
    /// awaits I/O, and holding the pool locked across that would block every
    /// appender for the duration of a disk write.
    ///
    /// ## The bound
    ///
    /// A flush must not write bytes belonging to the next file into this one.
    /// That is the whole of trap 3: the pool is filled at enqueue time and the
    /// file is closed later, so without a bound the close would drain records
    /// that the rotation had already assigned to the next file, and the reader —
    /// which derives V1 ids positionally — would be skewed by exactly that many
    /// entries.
    ///
    /// The bound is applied per page, and that is *exact* rather than
    /// conservative only because [`PagePool::seal_active`] guarantees no page
    /// holds entries for two files. The two are one mechanism; a page skipped
    /// for exceeding the bound while it starts below the bound means the seal
    /// did not happen, which is why that case is loud rather than silent.
    pub fn take_pending(&self) -> Vec<(PendingWrite, Vec<u8>)> {
        let mut inner = self.inner.lock().unwrap();
        let count = inner.ring.page_count();
        let bound = inner.boundaries.front().copied();
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
            if let Some(bound) = bound
                && last_entry_id >= bound
            {
                // Belongs to the next file. Correct to skip — unless the page
                // also starts before the boundary, in which case it holds
                // entries for two files and the seal failed.
                if inner.ring.page_first_entry_id(k).is_some_and(|f| f < bound) {
                    log::error!(
                        target: "normfs-wal",
                        "page {k} holds entries {:?}..={last_entry_id} across the file boundary \
                         at {bound}: the page was not sealed when the file rotated, so these \
                         bytes cannot be split between the two files",
                        inner.ring.page_first_entry_id(k),
                    );
                    debug_assert!(false, "page {k} straddles the file boundary at {bound}");
                }
                continue;
            }
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
            inner.written[k] = used;
        }

        out.sort_by_key(|(w, _)| w.last_entry_id);
        out
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

    /// Runs `f` against the ring under the pool lock, for reads.
    pub fn with_ring<T>(&self, f: impl FnOnce(&WalRing) -> T) -> T {
        f(&self.inner.lock().unwrap().ring)
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
