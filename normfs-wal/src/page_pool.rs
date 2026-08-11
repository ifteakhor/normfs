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

use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::Notify;

use crate::wal_ring_v1::{AppendOutcome, WalRing};

/// Keeps a page alive for as long as a reader is looking at bytes on it.
///
/// The payload handed out by [`PagePool::pin_range`] is not a copy — it points
/// into the page the record was written into, which is the same memory the WAL
/// file writer takes its bytes from. Nothing else keeps that memory still, so
/// this does: the guard holds a pin for its lifetime and drops it afterwards,
/// including when the read fails or the stream is dropped part-way.
///
/// ## Why the borrow is sound
///
/// A page is reused in exactly one place, `normfs_wal_ring_rotate_to`, and its
/// contract requires `normfs_wal_page_is_reusable`, whose first conjunct is
/// `pin_count == 0`. So a pinned page cannot be reset, and the bytes below
/// `used_bytes` cannot change: appends only ever move that cursor forward, into
/// space this slice does not cover. The page buffers are `Vec<u8>`s allocated
/// once when the ring is built and never reallocated, and the `Arc` keeps the
/// pool — and with it the buffers — alive for at least as long as the guard.
///
/// That is what the pin conjunct was always for. Until now nothing pinned
/// anything, so it read as a precaution against a caller that did not exist;
/// with reads borrowing from pages it carries its intended meaning.
pub struct PageGuard {
    pool: Arc<PagePool>,
    page_index: usize,
    ptr: *const u8,
    len: usize,
}

// The pointer refers to a page buffer owned by the pool, which this guard holds
// an `Arc` to and a pin on, so it stays valid and unwritten for the guard's
// lifetime. Nothing here is mutated, so sharing across threads is safe.
unsafe impl Send for PageGuard {}
unsafe impl Sync for PageGuard {}

impl AsRef<[u8]> for PageGuard {
    fn as_ref(&self) -> &[u8] {
        // SAFETY: as argued on the type -- the pin holds the page against
        // reuse, and the buffer outlives this guard.
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl Drop for PageGuard {
    fn drop(&mut self) {
        self.pool.unpin_page(self.page_index);
    }
}

impl std::fmt::Debug for PageGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PageGuard")
            .field("page_index", &self.page_index)
            .field("len", &self.len)
            .finish()
    }
}

/// How long a task may wait for a page before the pool starts reporting who is
/// holding things up. Waiting itself is not an error — a full pool means the
/// disk is behind, and back-pressure is the correct response — but an
/// indefinite wait with no explanation is not debuggable.
const STALL_WARN_AFTER: Duration = Duration::from_secs(5);

/// Pages a read may never pin, so the writer always has one to append into.
const PIN_RESERVE: usize = 1;

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
    /// Which file this record was charged to, counted from zero.
    ///
    /// A tripwire, not a control input: `check_epoch` compares it against the
    /// writer's own count so a disagreement is greppable rather than silent.
    /// What a flush asks the pool with is the writer's own `file_epoch`.
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
    /// Rotate, then write. This record opened a page, so the file it closes
    /// ends where that page's predecessor ended.
    Before,
    /// Do not rotate, whatever the writer's own accounting says.
    None,
}

/// How full the currently-open WAL file is.
///
/// This lives here rather than in `AckFileWriter` because an `AckFileWriter` is
/// per-file and dies on rotation, while the accounting has to run on the enqueue
/// path. It is the *only* fill accounting on the pooled path:
/// `AckFileWriter::current_size` is not maintained for pooled records, so the
/// two can never drift.
///
/// `max` is a threshold rather than a cap. A file ends at the first page to open
/// after it is crossed, so a file overshoots by at most the tail of one page —
/// and a `max_file_size` below the page size buys nothing.
struct FileFill {
    used: u64,
    max: u64,
    header_len: u64,
    /// Whether this file has been charged an entry yet. Both paths consult it,
    /// so a file takes its first record however large it is: without it a
    /// record bigger than `max` would rotate forever into empty files, and a
    /// file whose max is below its own header would be closed holding nothing.
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
    /// Per page: which file the bytes currently on it belong to.
    ///
    /// This is what keeps a flush inside its own file. The enqueue side runs
    /// ahead of the writer over an unbounded channel, so by the time a writer
    /// flushes, later pages may already hold records for files that are not
    /// open yet. A writer takes only the pages stamped with its own epoch, so
    /// several rotations can be outstanding at once and each file still gets
    /// exactly its own records.
    page_epoch: Vec<u64>,
    /// Bytes appended and not yet reported written. An estimate: it drives the
    /// flush hint, not a decision.
    unwritten: usize,
}

/// What a record of this length costs the file: framing and CRC, not payload.
///
/// `None` for a record wider than the V1 frame, which is a record no file can
/// take. Callers decide what that means for them.
fn encoded_len_of(record_len: usize) -> Option<u64> {
    u32::try_from(record_len)
        .ok()
        .map(|n| crate::wal_entry_v1::encoded_len(n) as u64)
}

/// Charges a record that landed on a page, and stamps that page with the file
/// it now belongs to.
///
/// A file ends where a page ends. The threshold alone does not rotate: the
/// record that crosses it stays where it is, and the file runs on to the end of
/// the page it is on. So a page's records belong to one file by construction —
/// which is what a flush needs, because a page's bytes go to the file whole and
/// there is no boundary inside one to split at.
///
/// The cost is that `max_file_size` is a threshold rather than a cap: a file
/// overshoots it by at most the tail of one page.
fn charge_paged(inner: &mut Inner, entry_len: u64, opened_page: bool) -> Placement {
    let active = inner.ring.active_page();
    let Some(fill) = inner.fill.as_mut() else {
        // Not armed: no writer is taking pages from this pool yet, so there is
        // no file to fill and nothing to decide. Nothing has stamped a page
        // either, so they are all still at the zero this would write.
        return Placement {
            in_pool: true,
            rotate: RotateHint::None,
            epoch: 0,
        };
    };

    // `has_written` guards the first record of a file: it opens a page by
    // definition, and without this a file whose max is below its own header
    // would be closed empty, leaving two files claiming the same
    // num_entries_before.
    let rotate = if opened_page && fill.has_written && fill.used >= fill.max {
        fill.epoch += 1;
        fill.used = fill.header_len.saturating_add(entry_len);
        RotateHint::Before
    } else {
        fill.used = fill.used.saturating_add(entry_len);
        RotateHint::None
    };
    fill.has_written = true;
    let epoch = fill.epoch;
    inner.page_epoch[active] = epoch;

    Placement {
        in_pool: true,
        rotate,
        epoch,
    }
}

/// Appends under the pool lock, keeping the file writer's cursor honest.
///
/// A rotation resets a page's bytes, and with them the cursor into that page.
/// Rotation is detected by `next_page_id`, which `normfs_wal_ring_rotate_to`
/// increments and nothing else touches. Comparing the active index instead
/// misses the ring rotating into the page that was already active, which is
/// legal when that page is empty or fully durable, and would leave a stale
/// cursor that silently swallowed the new page's first bytes.
///
/// Returns whether this append started a fresh page, which is where a file is
/// allowed to end.
fn append_locked(inner: &mut Inner, record: &[u8]) -> (AppendOutcome, bool) {
    let before_page_id = inner.ring.next_page_id();
    let outcome = inner.ring.append(record);
    let rotated = inner.ring.next_page_id() != before_page_id;
    if rotated {
        let active = inner.ring.active_page();
        inner.written[active] = 0;
    }
    (outcome, rotated)
}

pub struct PagePool {
    inner: Mutex<Inner>,
    /// Set once a file writer is draining this pool. Until then nothing can
    /// report pages durable, so nothing may wait for one to be freed.
    drainer: std::sync::atomic::AtomicBool,
    /// Woken when [`PagePool::mark_durable`] advances the watermark, which is
    /// the only event that can turn a full pool into a pool with room.
    space: Notify,
    /// Poked at `flush_watermark` unwritten bytes. Otherwise the writer's
    /// interval tick is the only thing on this path that can start a flush.
    flush: Mutex<Option<Arc<Notify>>>,
    flush_watermark: usize,
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
                page_epoch: vec![0; page_count],
                unwritten: 0,
            }),
            space: Notify::new(),
            drainer: std::sync::atomic::AtomicBool::new(false),
            flush: Mutex::new(None),
            flush_watermark: (page_count * page_size / 2).max(page_size),
        }
    }

    pub fn set_flush_signal(&self, flush: Arc<Notify>) {
        *self.flush.lock().unwrap() = Some(flush);
    }

    fn signal_flush(&self) {
        if let Some(flush) = self.flush.lock().unwrap().as_ref() {
            flush.notify_one();
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

    /// Gives up that responsibility. Waiting for a page nothing drains is a
    /// wait nothing can end.
    pub fn clear_drainer(&self) {
        self.drainer
            .store(false, std::sync::atomic::Ordering::Release);
        self.space.notify_waiters();
    }

    /// Takes over the rotation decision from the writer.
    ///
    /// Called once when the WAL writer starts, beside
    /// [`PagePool::set_drainer`]. Until it is called, [`PagePool::place`]
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

        // Whatever the pages already hold is not this writer's to write. A
        // queue upgraded from readonly to write keeps its `MemQueue`, and so
        // its pool: those records were cached by reads, or written by the
        // previous writer, and either way this one has no entry for them in
        // its file and no index to place them at. Writing them would append
        // records the header does not account for, which V1's positional ids
        // turn into every later entry reading back under the wrong one.
        //
        // So the cursor starts at what is there: this writer emits only the
        // records it is told about.
        for k in 0..inner.ring.page_count() {
            inner.written[k] = inner.ring.page_bytes(k).len();
            inner.page_epoch[k] = 0;
        }
    }

    /// Bytes charged to the open file so far, including its header. For tests.
    pub fn fill_used(&self) -> Option<u64> {
        self.inner.lock().unwrap().fill.as_ref().map(|f| f.used)
    }

    /// The file the next record will be charged to, counted from zero.
    pub fn epoch(&self) -> u64 {
        self.inner.lock().unwrap().fill.as_ref().map_or(0, |f| f.epoch)
    }

    /// Appends without waiting. `Full` means every page is either pinned by a
    /// reader or still holds records that are not on disk.
    pub fn try_append(&self, record: &[u8]) -> AppendOutcome {
        let mut inner = self.inner.lock().unwrap();
        append_locked(&mut inner, record).0
    }

    /// Appends the record that must land on `expected_id`, waiting until a
    /// page can be reclaimed.
    ///
    /// The caller owns the id sequence; the pool follows it. They can only
    /// disagree after a record the pool refused, and re-seeding to catch up
    /// discards pages, so callers must serialise their appends.
    pub async fn place(&self, expected_id: u64, record: &[u8]) -> Result<Placement, PoolError> {
        // Unwrap-free: a record too wide to frame never reaches `charge_paged`,
        // because `append_locked` reports `TooLarge` for it first.
        let entry_len = encoded_len_of(record.len()).unwrap_or(u64::MAX);
        match self.try_place(expected_id, record, entry_len) {
            Ok(Some(placed)) => return Ok(placed),
            Ok(None) => {}
            Err(e) => return Err(e),
        }

        // Registered before the retry re-tests the pool: `notify_waiters`
        // leaves no permit, and `enable()` is what joins the waiter list.
        loop {
            let woken = self.space.notified();
            tokio::pin!(woken);
            woken.as_mut().enable();

            match self.try_place(expected_id, record, entry_len) {
                Ok(Some(placed)) => {
                    log::debug!(target: "normfs-wal", "page pool: resumed at entry {expected_id}");
                    return Ok(placed);
                }
                Ok(None) => {}
                Err(e) => return Err(e),
            }

            if tokio::time::timeout(STALL_WARN_AFTER, woken).await.is_err() {
                self.warn_stalled();
            }
        }
    }

    /// `Ok(None)` means no page can take the record yet.
    fn try_place(
        &self,
        expected_id: u64,
        record: &[u8],
        entry_len: u64,
    ) -> Result<Option<Placement>, PoolError> {
        let over_watermark;
        let placed = {
            let mut inner = self.inner.lock().unwrap();
            if inner.ring.next_entry_id() != expected_id {
                // Every page pinned: nothing to append into either, so wait.
                if !inner.ring.reinit(expected_id) {
                    return Ok(None);
                }
                inner.written.iter_mut().for_each(|w| *w = 0);
                inner.unwritten = 0;
            }
            let (outcome, opened_page) = append_locked(&mut inner, record);
            match outcome {
                AppendOutcome::Cached(_) => {
                    inner.unwritten = inner.unwritten.saturating_add(entry_len as usize);
                    over_watermark = inner.unwritten >= self.flush_watermark;
                    charge_paged(&mut inner, entry_len, opened_page)
                }
                AppendOutcome::TooLarge => return Err(PoolError::TooLarge),
                AppendOutcome::Full => return Ok(None),
            }
        };
        if over_watermark {
            self.signal_flush();
        }
        Ok(Some(placed))
    }

    /// Charges a record the pool refused, which the writer will buffer instead.
    ///
    /// Such a record has no page, so there is no page boundary for the file to
    /// end at and it takes the decision the writer used to take: rotate before
    /// it when it does not fit. Without this a queue whose records are all
    /// larger than a page would never rotate at all, because rotation would be
    /// waiting for a page boundary that never comes.
    pub fn charge_buffered(&self, record_len: usize) -> Placement {
        let Some(entry_len) = encoded_len_of(record_len) else {
            // Wider than the V1 frame, so `WriterState::write` rejects it before
            // it can rotate anything. Charging it would move the file on for a
            // record that never reaches one, and the pool's epoch would outrun
            // the writer's for good.
            return Placement::default();
        };
        let mut inner = self.inner.lock().unwrap();
        let Some(fill) = inner.fill.as_mut() else {
            return Placement::default();
        };
        let rotate = if fill.has_written && fill.used.saturating_add(entry_len) > fill.max {
            fill.epoch += 1;
            fill.used = fill.header_len.saturating_add(entry_len);
            RotateHint::Before
        } else {
            fill.used = fill.used.saturating_add(entry_len);
            RotateHint::None
        };
        fill.has_written = true;
        Placement {
            in_pool: false,
            rotate,
            epoch: fill.epoch,
        }
    }

    /// Steps the id sequence over a record the pool could not hold, without
    /// losing anything it is still holding.
    ///
    /// Re-seeding drops whatever the pages contain, so it waits until there is
    /// nothing left to drop. A record too large for a page is rare, and paying
    /// a drain for it is the price of never losing one that was accepted.
    ///
    /// "Nothing left to drop" is two cases, and only testing the second of them
    /// deadlocks: a pool that holds nothing has nothing to lose, and a pool
    /// whose every record is already durable has nothing to lose either.
    /// `reinit` resets the watermark to zero while moving `next_entry_id`
    /// forward, so after one oversized record the durability test alone can
    /// never become true again — and when every record is oversized, nothing
    /// ever enters a page, so nothing can ever report one durable to make it
    /// true. That is a hang, not a failure. A 1 MiB record against a 256 KiB
    /// page is the ordinary way in.
    pub async fn skip_to(&self, next_id: u64) {
        loop {
            let woken = self.space.notified();
            tokio::pin!(woken);
            woken.as_mut().enable();
            {
                let mut inner = self.inner.lock().unwrap();
                let nothing_to_lose = inner.ring.is_empty()
                    || inner.ring.min_essential_id() >= inner.ring.next_entry_id();
                // A pinned page is something to lose too, and `reinit` reports
                // that by refusing.
                if nothing_to_lose && inner.ring.reinit(next_id) {
                    inner.written.iter_mut().for_each(|w| *w = 0);
                    inner.unwritten = 0;
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
    /// oldest first — and never past the end of the file that is open.
    ///
    /// The bytes are copied out under the lock rather than borrowed: the writer
    /// awaits I/O, and holding the pool locked across that would block every
    /// appender for the duration of a disk write.
    ///
    /// This does not mark the runs as taken — call
    /// [`PagePool::commit_written`] once a run is actually on disk, or an
    /// uncommitted run just comes back here. Advancing the cursor at take time
    /// would drop the bytes of any write that then failed.
    ///
    /// ## Why `epoch`
    ///
    /// A flush must not write bytes belonging to the next file into this one.
    /// That is the whole of trap 3: the pool is filled at enqueue time and the
    /// file is closed later, so an unfiltered close would drain records that the
    /// rotation had already assigned to the next file, and the reader — which
    /// derives V1 ids positionally — would be skewed by exactly that many
    /// entries.
    ///
    /// Each writer passes the file it is writing, and gets the pages stamped
    /// with it. No page carries two epochs, because a file only ever ends where
    /// a page ends, so this is a filter rather than a cut: nothing has to be
    /// split, and there is no straddling case left to detect.
    ///
    /// Older epochs are taken too, and that is deliberate. A page of an earlier
    /// file is normally written out long before its writer closes, so it is
    /// already past its cursor and skipped here. One arrives only when that
    /// file's last flush failed outright, and then the choice is between this
    /// file and no file at all: nothing else will ever ask for that epoch
    /// again, and the ring will reuse the page as soon as a later fsync moves
    /// the watermark past it. Handing it to the open file keeps the bytes.
    pub fn take_pending(&self, epoch: u64) -> Vec<(PendingWrite, Vec<u8>)> {
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
            if inner.page_epoch[k] > epoch {
                continue;
            }
            if inner.page_epoch[k] < epoch {
                log::error!(
                    target: "normfs-wal",
                    "page {k} still holds unwritten entries ..={last_entry_id} for file {}, \
                     which is already closed: its last flush did not complete, so they are \
                     being written into file {epoch} instead of being lost",
                    inner.page_epoch[k],
                );
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
        }

        out.sort_by_key(|(w, _)| w.last_entry_id);
        out
    }

    /// Marks a run from [`PagePool::take_pending`] as written. Only moves the
    /// cursor forward.
    pub fn commit_written(&self, page_index: usize, up_to: usize) {
        let mut inner = self.inner.lock().unwrap();
        if up_to > inner.written[page_index] {
            let taken = up_to - inner.written[page_index];
            inner.written[page_index] = up_to;
            inner.unwritten = inner.unwritten.saturating_sub(taken);
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

    /// Runs `f` against the ring under the pool lock, for reads.
    ///
    /// The delegating accessors below cover what the store needs; this is for
    /// tests that assert on page-level state — which page holds which ids, how
    /// many pages a queue was given — where adding a delegate each time would
    /// widen the API for no caller.
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

    /// Every held record with id in `[start, end]`, in id order, **borrowed
    /// from the pages rather than copied out of them**.
    ///
    /// Each payload is a [`Bytes`] over the page the record was written into,
    /// and holds a pin on that page until it is dropped. See [`PageGuard`] for
    /// why that is sound.
    pub fn pin_range(self: &Arc<Self>, start: u64, end: u64) -> Vec<(u64, Bytes)> {
        let mut found: Vec<(u64, usize, *const u8, usize)> = Vec::new();
        let mut copied: Vec<(u64, Bytes)> = Vec::new();
        {
            let inner = self.inner.lock().unwrap();
            let count = inner.ring.page_count();
            let pinned_now = (0..count).filter(|&k| inner.ring.page_pin_count(k) > 0).count();
            // Past this budget records are copied, not borrowed: a pin lasts as
            // long as the payload, and a pinned page is one no append can use.
            let mut budget = count
                .saturating_sub(PIN_RESERVE)
                .saturating_sub(pinned_now);
            let mut taking: Vec<bool> = vec![false; count];

            for k in 0..count {
                let n = inner.ring.page_len(k);
                let Some(first) = inner.ring.page_first_entry_id(k) else {
                    continue;
                };
                // Cheap page-level skip on the id span before touching entries.
                let last = first + u64::from(n) - 1;
                if last < start || first > end {
                    continue;
                }
                if !taking[k] && inner.ring.page_pin_count(k) == 0 {
                    if budget == 0 {
                        for i in 0..n {
                            let id = first + u64::from(i);
                            if id < start || id > end {
                                continue;
                            }
                            if let Some(rec) = inner.ring.record(k, i) {
                                copied.push((id, Bytes::copy_from_slice(rec)));
                            }
                        }
                        continue;
                    }
                    budget -= 1;
                }
                taking[k] = true;
                for i in 0..n {
                    let id = first + u64::from(i);
                    if id < start || id > end {
                        continue;
                    }
                    if let Some(rec) = inner.ring.record(k, i) {
                        found.push((id, k, rec.as_ptr(), rec.len()));
                    }
                }
            }
            // One pin per payload handed out, so each guard's drop balances
            // exactly one of them.
            let mut inner = inner;
            for &(_, k, _, _) in &found {
                inner.ring.pin(k);
            }
        }
        // The lock is released before any guard exists. A `PageGuard` unpins on
        // drop, which takes this same lock, so one dropped while it were held
        // would deadlock against itself.

        let mut out: Vec<(u64, Bytes)> = found
            .into_iter()
            .map(|(id, page_index, ptr, len)| {
                let guard = PageGuard {
                    pool: Arc::clone(self),
                    page_index,
                    ptr,
                    len,
                };
                (id, Bytes::from_owner(guard))
            })
            .collect();
        out.extend(copied);
        out.sort_by_key(|(id, _)| *id);
        out
    }

    /// Releases one pin taken by [`PagePool::pin_range`]. Called from
    /// [`PageGuard::drop`], and from nowhere else: an unbalanced unpin wraps
    /// the count and pins the page for good.
    fn unpin_page(&self, page_index: usize) {
        let mut inner = self.inner.lock().unwrap();
        inner.ring.unpin(page_index);
        drop(inner);
        // A page whose last reader has gone may now be reclaimable, and an
        // appender may be waiting for exactly that.
        self.space.notify_waiters();
    }

    /// Restarts the pool empty, numbering from `first_entry_id`.
    ///
    /// This drops whatever the pages held, so it is only for resynchronising a
    /// pool that has fallen out of step with the id sequence — never for making
    /// room. Making room is what waiting is for.
    /// False when every page is pinned, in which case nothing was reseeded.
    pub fn reseed(&self, first_entry_id: u64) -> bool {
        let mut inner = self.inner.lock().unwrap();
        if !inner.ring.reinit(first_entry_id) {
            return false;
        }
        inner.written.iter_mut().for_each(|w| *w = 0);
        inner.unwritten = 0;
        true
    }
}
