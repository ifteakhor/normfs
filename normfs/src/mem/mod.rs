use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use tokio::sync::mpsc::Sender;

use bytes::Bytes;
use normfs_types::{DataSource, QueueId, ReadEntry, SubscriberCallback};
use normfs_wal::{AppendOutcome, PagePool};
use uintn::UintN;

// Geometry of the in-memory paged store. A record larger than a page is not
// cached and is served from file; the ring caps a queue's cache at
// MEM_MAX_PAGES pages.
//
// clamp(1, ..) floors every queue at one full page, even below its fair
// share — see the log::warn in start_queue.
const MEM_PAGE_SIZE: usize = 256 * 1024;
const MEM_MAX_PAGES: usize = 64;

fn ring_page_count(max_memory_usage: usize) -> usize {
    (max_memory_usage / MEM_PAGE_SIZE).clamp(1, MEM_MAX_PAGES)
}

// Entry ids are sequential counters that fit u64 for any real queue; the
// fallback is defensive only (cache_append saturates on it, never wraps).
fn id_to_u64(id: &UintN) -> u64 {
    debug_assert!(id.to_u64().is_ok(), "WAL entry id {} does not fit u64", id);
    id.to_u64().unwrap_or(u64::MAX)
}

/// Result of a memory read operation
#[derive(Debug)]
pub struct MemReadResult {
    /// Whether the read was fully satisfied by memory
    pub success: bool,
    /// For negative lookups: the resolved start_id (offset from end)
    pub start_id: Option<UintN>,
    /// For follow/subscribe operations: the subscription ID
    pub subscription_id: Option<usize>,
}

impl MemReadResult {
    pub fn fail() -> Self {
        Self {
            success: false,
            start_id: None,
            subscription_id: None,
        }
    }
}

pub struct MemStore {
    queues: RwLock<HashMap<QueueId, Arc<MemQueue>>>,
    max_memory_usage: usize,
}

struct Inner {
    // The paged store, allocated on first enqueue. It holds a contiguous suffix
    // of recent entries; older acked entries are reclaimed and served from file.
    pool: Option<Arc<PagePool>>,
    // The last enqueued id. It persists beyond the cache, so it is tracked
    // separately from the ring.
    last_id: Option<UintN>,
    last_acked_id: Option<UintN>,
}

struct MemQueue {
    inner: RwLock<Inner>,
    max_memory_usage: usize,
    /// Held across an append so the pool sees ids in order.
    append_gate: tokio::sync::Mutex<()>,
    subscribers: Mutex<HashMap<usize, SubscriberCallback>>,
    next_subscriber_id: Mutex<usize>,
}

impl MemQueue {
    pub fn new(last_id: Option<UintN>, max_memory_usage: usize) -> Self {
        // Allocated here, once, so the pool's footprint is fixed for the life
        // of the queue and the WAL writer can be handed the same one.
        let first_entry_id = last_id.as_ref().map_or(0, |id| id_to_u64(id).wrapping_add(1));
        let pool = Arc::new(PagePool::new(
            ring_page_count(max_memory_usage),
            MEM_PAGE_SIZE,
            first_entry_id,
        ));
        MemQueue {
            inner: RwLock::new(Inner {
                pool: Some(pool),
                last_id,
                last_acked_id: None,
            }),
            max_memory_usage,
            append_gate: tokio::sync::Mutex::new(()),
            subscribers: Mutex::new(HashMap::new()),
            next_subscriber_id: Mutex::new(0),
        }
    }

    // Caches `data` under `id_u64` in the pre-allocated pool.
    //
    // The pool is the id authority: it hands out ids in sequence from where it
    // was seeded, and `last_id` mirrors them. They can only disagree if a
    // record went to the file without being cached, and the pool is re-seeded
    // then so its contents stay a contiguous id suffix.
    //
    // `Full` still declines to cache here rather than waiting. Waiting belongs
    // on the async path (`PagePool::append`), which is what `NormFS::enqueue`
    // will use; this synchronous one cannot block a runtime thread.
    fn cache_append(&self, inner: &mut Inner, id_u64: u64, data: &[u8]) {
        let max_memory = self.max_memory_usage;
        let pool = inner.pool.get_or_insert_with(|| {
            Arc::new(PagePool::new(
                ring_page_count(max_memory),
                MEM_PAGE_SIZE,
                id_u64,
            ))
        });

        if pool.next_entry_id() != id_u64 {
            pool.reseed(id_u64);
        }

        match pool.try_append(data) {
            AppendOutcome::Cached(_) => {}
            AppendOutcome::Full => {
                // Start the cache again at this record and keep it, so what is
                // held stays the newest contiguous run of ids rather than an
                // arbitrary older one.
                pool.reseed(id_u64);
                if !matches!(pool.try_append(data), AppendOutcome::Cached(_)) {
                    // saturating: a wrapping add on id_to_u64's MAX fallback
                    // would resume caching at id 0, colliding with real ids.
                    pool.reseed(id_u64.saturating_add(1));
                }
            }
            AppendOutcome::TooLarge => {
                pool.reseed(id_u64.saturating_add(1));
            }
        }
    }

    /// Enqueues, waiting for a page rather than dropping the cache when the
    /// pool is full. The synchronous [`MemQueue::enqueue`] stays for callers
    /// that cannot await.
    pub async fn enqueue_awaiting(&self, data: Bytes) -> (UintN, bool) {
        let _gate = self.append_gate.lock().await;

        let (id, pool, subscribers_data) = {
            let mut inner = self.inner.write().unwrap();
            let id = inner
                .last_id
                .as_ref()
                .map_or(UintN::zero(), |id| id.increment());
            let subscribers_data = if self.subscribers.lock().unwrap().is_empty() {
                None
            } else {
                Some(data.clone())
            };
            inner.last_id = Some(id.clone());
            (id, inner.pool.clone(), subscribers_data)
        };

        // Waiting is only safe once the WAL writer drains this pool: the wait
        // ends when a flush reports pages durable, and nothing reports that
        // until the writer is started with Some(pool). Until then this uses the
        // non-blocking cache behaviour, or a full pool would hang the caller
        // forever with nothing able to free it.
        let mut cached = false;
        if let Some(pool) = pool {
            if pool.has_drainer() {
                match pool.append_at(id_to_u64(&id), &data).await {
                    Ok(()) => cached = true,
                    Err(_) => {
                        // Too large for a page: step over this id without
                        // dropping what the pool holds, and let the file writer
                        // buffer this one record the old way.
                        pool.skip_to(id_to_u64(&id).wrapping_add(1)).await;
                    }
                }
            } else {
                let mut inner = self.inner.write().unwrap();
                self.cache_append(&mut inner, id_to_u64(&id), &data);
            }
        }


        log::debug!(target: "normfs-mem", "Enqueued entry - ID: {}, Data size: {} bytes", id, data.len());

        if let Some(data) = subscribers_data {
            self.notify_subscribers(&[(id.clone(), data)]);
        }

        (id, cached)
    }

    pub fn enqueue(&self, data: Bytes) -> UintN {
        let mut inner = self.inner.write().unwrap();
        let id = inner
            .last_id
            .as_ref()
            .map_or(UintN::zero(), |id| id.increment());

        let subscribers_data = if self.subscribers.lock().unwrap().is_empty() {
            None
        } else {
            Some(data.clone())
        };

        self.cache_append(&mut inner, id_to_u64(&id), &data);
        inner.last_id = Some(id.clone());

        log::debug!(target: "normfs-mem", "Enqueued entry - ID: {}, Data size: {} bytes", id, data.len());

        drop(inner);

        if let Some(data) = subscribers_data {
            self.notify_subscribers(&[(id.clone(), data)]);
        }

        id
    }

    pub fn enqueue_batch(&self, entries: Vec<Bytes>) -> Vec<UintN> {
        if entries.is_empty() {
            return Vec::new();
        }

        let mut inner = self.inner.write().unwrap();
        let mut ids = Vec::with_capacity(entries.len());
        let mut next_id = inner
            .last_id
            .as_ref()
            .map_or(UintN::zero(), |id| id.increment());

        let has_subscribers = !self.subscribers.lock().unwrap().is_empty();
        let mut entries_with_ids = if has_subscribers {
            Vec::with_capacity(entries.len())
        } else {
            Vec::new()
        };

        for data in entries {
            ids.push(next_id.clone());
            if has_subscribers {
                entries_with_ids.push((next_id.clone(), data.clone()));
            }
            self.cache_append(&mut inner, id_to_u64(&next_id), &data);
            next_id = next_id.increment();
        }

        if let Some(last_id) = ids.last() {
            inner.last_id = Some(last_id.clone());
        }

        drop(inner);

        if has_subscribers {
            self.notify_subscribers(&entries_with_ids);
        }

        ids
    }

    pub fn get_last_id(&self) -> Option<UintN> {
        self.inner.read().unwrap().last_id.clone()
    }

    pub fn ack(&self, id: &UintN) {
        let mut inner = self.inner.write().unwrap();
        if inner.last_acked_id.as_ref().is_none_or(|last| id > last) {
            log::debug!(target: "normfs-mem", "Acknowledging entry - ID: {}", id);
            inner.last_acked_id = Some(id.clone());
            // Deliberately does not free pages. A page may only be reused
            // once its records are on disk, and that is reported by the WAL
            // writer through PagePool::mark_durable. Letting a consumer ack
            // advance the same watermark would hand a page back to be
            // overwritten while its records were still only in memory.
        }
    }

    pub async fn read_full(
        &self,
        start_id: UintN,
        end_id: UintN,
        step: usize,
        target_chan: &Sender<ReadEntry>,
    ) -> MemReadResult {
        // Collect entries to send while holding the lock
        let entries_to_send: Vec<(UintN, Bytes)> = {
            let inner = self.inner.read().unwrap();

            // Check queue's last_id first - if start_id is beyond it, we're done
            let mem_last_id = match &inner.last_id {
                Some(id) => id,
                None => {
                    // Queue exists but is empty (no entries ever enqueued)
                    // Return success with 0 entries
                    return MemReadResult {
                        success: true,
                        start_id: None,
                        subscription_id: None,
                    };
                }
            };

            // If requesting entries beyond what exists, complete with no entries
            if start_id > *mem_last_id {
                return MemReadResult {
                    success: true,
                    start_id: None,
                    subscription_id: None,
                };
            }

            // Check if entries are actually loaded in memory
            let ring = match &inner.pool {
                Some(ring) if !ring.is_empty() => ring,
                _ => return MemReadResult::fail(), // Not in memory, read from files
            };

            let mem_start_id = match ring.min_cached_id() {
                Some(m) => UintN::from(m),
                None => return MemReadResult::fail(),
            };

            // If start_id is before what's in memory, need to read from files
            if start_id < mem_start_id {
                return MemReadResult::fail();
            }

            let mut current_id = start_id.clone();
            let mut results = Vec::new();

            for (id_u64, data) in ring.collect_range(id_to_u64(&start_id), id_to_u64(&end_id)) {
                let id = UintN::from(id_u64);
                if id > end_id {
                    break;
                }

                while current_id < id {
                    current_id = current_id.step_by(step);
                    if current_id > end_id {
                        break;
                    }
                }

                if current_id == id {
                    results.push((id.clone(), Bytes::from(data)));
                    current_id = current_id.step_by(step);
                }
            }

            results
        };

        for (id, data) in entries_to_send {
            if target_chan
                .send(ReadEntry::new(id, data, DataSource::Memory))
                .await
                .is_err()
            {
                break;
            }
        }

        MemReadResult {
            success: true,
            start_id: None,
            subscription_id: None,
        }
    }

    pub async fn read_full_negative(
        &self,
        offset: UintN,
        step: usize,
        limit: u64,
        target_chan: &Sender<ReadEntry>,
    ) -> MemReadResult {
        // Collect entries to send while holding the lock
        let (entries_to_send, start_id) = {
            let inner = self.inner.read().unwrap();

            let last_id = if let Some(id) = &inner.last_id {
                id
            } else {
                // Queue exists but is empty (no entries ever enqueued)
                // Return success with 0 entries and start_id = 0
                return MemReadResult {
                    success: true,
                    start_id: Some(UintN::zero()),
                    subscription_id: None,
                };
            };

            // Calculate start_id = last_id - offset (before first_id check,
            // so we can return it even when memory has no entries yet)
            let start_id = if offset > *last_id {
                UintN::zero()
            } else {
                last_id.sub(&offset).unwrap_or(UintN::zero())
            };

            let ring = match &inner.pool {
                Some(ring) if !ring.is_empty() => ring,
                _ => {
                    // Nothing in memory yet; return start_id for file fallback.
                    return MemReadResult {
                        success: false,
                        start_id: Some(start_id),
                        subscription_id: None,
                    };
                }
            };

            let mem_start_id = match ring.min_cached_id() {
                Some(m) => UintN::from(m),
                None => {
                    return MemReadResult {
                        success: false,
                        start_id: Some(start_id),
                        subscription_id: None,
                    };
                }
            };

            if start_id < mem_start_id {
                return MemReadResult {
                    success: false,
                    start_id: Some(start_id),
                    subscription_id: None,
                };
            }

            let last = inner.last_id.as_ref().map(id_to_u64).unwrap_or(u64::MAX);
            let mut current_id = start_id.clone();
            let mut entries = Vec::new();
            let mut count = 0u64;

            for (id_u64, data) in ring.collect_range(id_to_u64(&start_id), last) {
                if limit > 0 && count >= limit {
                    break;
                }
                let id = UintN::from(id_u64);
                while current_id < id {
                    current_id = current_id.step_by(step);
                }
                if current_id == id {
                    entries.push((id.clone(), Bytes::from(data)));
                    current_id = current_id.step_by(step);
                    count += 1;
                }
            }

            (entries, start_id)
        };

        if entries_to_send.is_empty() {
            return MemReadResult {
                success: false,
                start_id: Some(start_id),
                subscription_id: None,
            };
        }

        for (id, data) in entries_to_send {
            if target_chan
                .send(ReadEntry::new(id, data, DataSource::Memory))
                .await
                .is_err()
            {
                return MemReadResult {
                    success: false,
                    start_id: Some(start_id),
                    subscription_id: None,
                };
            }
        }

        MemReadResult {
            success: true,
            start_id: Some(start_id),
            subscription_id: None,
        }
    }

    pub async fn follow_full(
        self: &Arc<Self>,
        from_id: &UintN,
        start_id: UintN,
        step: usize,
        target_chan: &Sender<ReadEntry>,
    ) -> MemReadResult {
        // Read all existing entries from start_id onwards
        let (entries_to_send, last_sent_id) = {
            let inner = self.inner.read().unwrap();

            match &inner.pool {
                Some(ring) if !ring.is_empty() => {
                    if let Some(mem_start) = ring.min_cached_id() {
                        if start_id < UintN::from(mem_start) {
                            return MemReadResult::fail();
                        }
                    }
                    let last = inner.last_id.as_ref().map(id_to_u64).unwrap_or(u64::MAX);
                    let mut current_id = start_id.clone();
                    let mut entries = Vec::new();
                    for (id_u64, data) in ring.collect_range(id_to_u64(&start_id), last) {
                        let id = UintN::from(id_u64);
                        while current_id < id {
                            current_id = current_id.step_by(step);
                        }
                        if current_id == id {
                            entries.push((id.clone(), Bytes::from(data)));
                            current_id = current_id.step_by(step);
                        }
                    }
                    let last_id = entries.last().map(|(id, _)| id.clone());
                    (entries, last_id)
                }
                _ => (Vec::new(), None),
            }
        };

        for (id, data) in entries_to_send {
            if target_chan
                .send(ReadEntry::new(id, data, DataSource::Memory))
                .await
                .is_err()
            {
                return MemReadResult::fail();
            }
        }

        let target_chan_clone = target_chan.clone();
        let from_id_clone = from_id.clone();
        let last_sent_id_clone = last_sent_id.clone();

        let mut next_id = self.next_subscriber_id.lock().unwrap();
        let subscription_id = *next_id;
        *next_id += 1;
        drop(next_id);

        let callback = Box::new(move |entries: &[(UintN, Bytes)]| -> bool {
            for (id, data) in entries {
                if let Some(ref last_id) = last_sent_id_clone {
                    if id <= last_id {
                        continue;
                    }
                }

                if id.in_step(&from_id_clone, step)
                    && target_chan_clone
                        .try_send(ReadEntry::new(id.clone(), data.clone(), DataSource::Memory))
                        .is_err()
                {
                    return false;
                }
            }
            true
        });

        let mut subscribers = self.subscribers.lock().unwrap();
        subscribers.insert(subscription_id, callback);

        log::debug!(target: "normfs-mem", "Started follow_full with subscription {} from last_sent_id: {:?}",
            subscription_id, last_sent_id);

        MemReadResult {
            success: true,
            start_id: None,
            subscription_id: Some(subscription_id),
        }
    }

    pub async fn follow_full_negative(
        self: &Arc<Self>,
        offset: UintN,
        step: usize,
        target_chan: &Sender<ReadEntry>,
    ) -> MemReadResult {
        // Special case: offset 0 means just subscribe from now on, no existing entries
        if offset == UintN::zero() {
            let (last_id, from_id) = {
                let inner = self.inner.read().unwrap();
                let last = inner.last_id.clone();
                let from = last
                    .as_ref()
                    .map(|id| id.add(&UintN::from(1u64)))
                    .unwrap_or(UintN::zero());
                (last, from)
            };

            let target_chan_clone = target_chan.clone();
            let last_id_clone = last_id.clone();
            let from_id_clone = from_id.clone();

            let mut next_id = self.next_subscriber_id.lock().unwrap();
            let subscription_id = *next_id;
            *next_id += 1;
            drop(next_id);

            let callback = Box::new(move |entries: &[(UintN, Bytes)]| -> bool {
                for (id, data) in entries {
                    if let Some(ref last) = last_id_clone {
                        if id <= last {
                            continue;
                        }
                    }

                    if id.in_step(&from_id_clone, step)
                        && target_chan_clone
                            .try_send(ReadEntry::new(id.clone(), data.clone(), DataSource::Memory))
                            .is_err()
                    {
                        return false;
                    }
                }
                true
            });

            let mut subscribers = self.subscribers.lock().unwrap();
            subscribers.insert(subscription_id, callback);

            log::debug!(target: "normfs-mem", "Started follow_full_negative with offset 0 (subscribe only), subscription {}, from_id: {}",
                subscription_id, from_id);

            return MemReadResult {
                success: true,
                start_id: Some(from_id),
                subscription_id: Some(subscription_id),
            };
        }

        // Calculate start_id from last_id
        let (start_id, last_sent_id, entries_to_send) = {
            let inner = self.inner.read().unwrap();

            let last_id = if let Some(id) = &inner.last_id {
                id
            } else {
                // Queue exists but is empty (no entries ever enqueued)
                // Return success with 0 entries and start_id = 0
                return MemReadResult {
                    success: true,
                    start_id: Some(UintN::zero()),
                    subscription_id: None,
                };
            };

            let start_id = if offset > *last_id {
                UintN::zero()
            } else {
                last_id.sub(&offset).unwrap_or(UintN::zero())
            };

            let ring = match &inner.pool {
                Some(ring) if !ring.is_empty() => ring,
                _ => {
                    return MemReadResult {
                        success: false,
                        start_id: Some(start_id),
                        subscription_id: None,
                    };
                }
            };

            if let Some(mem_start) = ring.min_cached_id() {
                if start_id < UintN::from(mem_start) {
                    return MemReadResult {
                        success: false,
                        start_id: Some(start_id),
                        subscription_id: None,
                    };
                }
            }

            let last = inner.last_id.as_ref().map(id_to_u64).unwrap_or(u64::MAX);
            let mut current_id = start_id.clone();
            let mut entries = Vec::new();

            for (id_u64, data) in ring.collect_range(id_to_u64(&start_id), last) {
                let id = UintN::from(id_u64);
                while current_id < id {
                    current_id = current_id.step_by(step);
                }
                if current_id == id {
                    entries.push((id.clone(), Bytes::from(data)));
                    current_id = current_id.step_by(step);
                }
            }

            let last_sent = entries.last().map(|(id, _)| id.clone());
            (start_id, last_sent, entries)
        };

        for (id, data) in entries_to_send {
            if target_chan
                .send(ReadEntry::new(id, data, DataSource::Memory))
                .await
                .is_err()
            {
                return MemReadResult {
                    success: false,
                    start_id: Some(start_id),
                    subscription_id: None,
                };
            }
        }

        let target_chan_clone = target_chan.clone();
        let start_id_clone = start_id.clone();
        let last_sent_id_clone = last_sent_id.clone();

        let mut next_id = self.next_subscriber_id.lock().unwrap();
        let subscription_id = *next_id;
        *next_id += 1;
        drop(next_id);

        let callback = Box::new(move |entries: &[(UintN, Bytes)]| -> bool {
            for (id, data) in entries {
                if let Some(ref last_id) = last_sent_id_clone {
                    if id <= last_id {
                        continue;
                    }
                }

                if id.in_step(&start_id_clone, step)
                    && target_chan_clone
                        .try_send(ReadEntry::new(id.clone(), data.clone(), DataSource::Memory))
                        .is_err()
                {
                    return false;
                }
            }
            true
        });

        let mut subscribers = self.subscribers.lock().unwrap();
        subscribers.insert(subscription_id, callback);

        log::debug!(target: "normfs-mem", "Started follow_full_negative with subscription {} from last_sent_id: {:?}",
            subscription_id, last_sent_id);

        MemReadResult {
            success: true,
            start_id: Some(start_id),
            subscription_id: Some(subscription_id),
        }
    }

    fn notify_subscribers(&self, entries: &[(UintN, Bytes)]) {
        log::debug!(target: "normfs-mem", "Notifying {} subscribers about {} new entries",
            self.subscribers.lock().unwrap().len(),
            entries.len());

        let to_remove = {
            let subscribers = self.subscribers.lock().unwrap();
            let mut to_remove = Vec::new();

            for (id, callback) in subscribers.iter() {
                if !callback(entries) {
                    to_remove.push(*id);
                }
            }

            to_remove
        }; // Lock released here

        // Clean up failed subscriptions without holding lock
        for id in to_remove {
            self.unsubscribe(id);
        }
    }

    pub fn subscribe(&self, callback: SubscriberCallback) -> usize {
        let mut next_id = self.next_subscriber_id.lock().unwrap();
        let subscriber_id = *next_id;
        *next_id += 1;

        let mut subscribers = self.subscribers.lock().unwrap();
        subscribers.insert(subscriber_id, callback);

        log::debug!(target: "normfs-mem", "Added subscription {}", subscriber_id);
        subscriber_id
    }

    pub fn unsubscribe(&self, subscriber_id: usize) {
        let mut subscribers = self.subscribers.lock().unwrap();
        if subscribers.remove(&subscriber_id).is_some() {
            log::debug!(target: "normfs-mem", "Removed subscription {}", subscriber_id);
        }
    }
}

impl MemStore {
    pub fn new(max_memory_usage: usize) -> Self {
        MemStore {
            queues: RwLock::new(HashMap::new()),
            max_memory_usage,
        }
    }

    /// The queue's page pool, so the WAL writer can put those same pages on
    /// disk instead of copying their contents into a buffer of its own.
    #[allow(dead_code)] // used once the pool becomes the writer's source
    pub fn pool(&self, queue: &QueueId) -> Option<Arc<PagePool>> {
        let queues = self.queues.read().unwrap();
        let q = queues.get(queue)?;
        let inner = q.inner.read().unwrap();
        inner.pool.clone()
    }

    pub fn start_queue(&self, queue: &QueueId, last_id: Option<UintN>) {
        let mut queues = self.queues.write().unwrap();
        if !queues.contains_key(queue) {
            let queue_max_memory = self.max_memory_usage / queues.len().max(1);
            // Below MEM_PAGE_SIZE the queue still reserves a full page, so
            // total memory can exceed max_memory_usage with enough queues.
            if queue_max_memory < MEM_PAGE_SIZE {
                log::warn!(target: "normfs-mem",
                    "Queue '{}' fair share ({} bytes) is below the {} byte page \
                     floor; total memory usage may exceed max_memory_usage",
                    queue, queue_max_memory, MEM_PAGE_SIZE);
            }
            log::debug!(target: "normfs-mem", "Starting queue '{}' with last_id: {:?}, max memory: {} bytes",
                queue, last_id, queue_max_memory);
            let new_queue = Arc::new(MemQueue::new(last_id, queue_max_memory));
            queues.insert(queue.clone(), new_queue);
        }
    }

    pub async fn enqueue_awaiting(&self, queue: &QueueId, data: Bytes) -> (UintN, bool) {
        let mem_queue = {
            let queues = self.queues.read().unwrap();
            queues.get(queue).cloned()
        };
        match mem_queue {
            Some(q) => q.enqueue_awaiting(data).await,
            None => (self.enqueue(queue, data), false),
        }
    }

    pub fn enqueue(&self, queue: &QueueId, data: Bytes) -> UintN {
        let queues = self.queues.read().unwrap();
        let mem_queue = queues.get(queue).expect("queue not setup");
        let id = mem_queue.enqueue(data);
        log::debug!(target: "normfs-mem", "Enqueued to queue '{}' - Entry ID: {}", queue, id);
        id
    }

    pub fn enqueue_batch(&self, queue: &QueueId, entries: Vec<Bytes>) -> Vec<UintN> {
        let queues = self.queues.read().unwrap();
        let mem_queue = queues.get(queue).expect("queue not setup");
        let ids = mem_queue.enqueue_batch(entries);
        if let (Some(first), Some(last)) = (ids.first(), ids.last()) {
            log::debug!(target: "normfs-mem", "Enqueued batch to queue '{}' - Count: {}, First ID: {}, Last ID: {}",
                queue, ids.len(), first, last);
        }
        ids
    }

    pub fn get_last_id(&self, queue: &QueueId) -> Option<Option<UintN>> {
        let queues = self.queues.read().unwrap();
        queues.get(queue).map(|q| q.get_last_id())
    }

    pub fn ack(&self, queue: &QueueId, id: &UintN) {
        log::debug!(target: "normfs-mem", "Acknowledging entry in queue '{}' - Entry ID: {}", queue, id);
        let queues = self.queues.read().unwrap();
        if let Some(mem_queue) = queues.get(queue) {
            mem_queue.ack(id);
        }
    }

    pub async fn read_full(
        &self,
        queue: &QueueId,
        start_id: UintN,
        end_id: UintN,
        step: usize,
        target_chan: &Sender<ReadEntry>,
    ) -> MemReadResult {
        log::debug!(target: "normfs-mem", "Reading from queue '{}' - Start ID: {}, End ID: {}",
            queue, start_id, end_id);

        let mem_queue = self.queues.read().unwrap().get(queue).cloned();

        if let Some(mem_queue) = mem_queue {
            let result = mem_queue
                .read_full(start_id, end_id, step, target_chan)
                .await;
            log::debug!(target: "normfs-mem", "Read from queue '{}' completed - Success: {}", queue, result.success);
            result
        } else {
            log::warn!(target: "normfs-mem", "Queue '{}' not found for read", queue);
            MemReadResult::fail()
        }
    }

    pub async fn follow_full(
        &self,
        queue: &QueueId,
        from_id: &UintN,
        start_id: UintN,
        step: usize,
        target_chan: &Sender<ReadEntry>,
    ) -> MemReadResult {
        log::debug!(target: "normfs-mem", "Starting follow_full from queue '{}' - Start ID: {}",
            queue, start_id);

        let mem_queue = self.queues.read().unwrap().get(queue).cloned();

        if let Some(mem_queue) = mem_queue {
            let result = mem_queue
                .follow_full(from_id, start_id, step, target_chan)
                .await;
            log::debug!(target: "normfs-mem", "Follow_full from queue '{}' - Success: {}, Subscription ID: {:?}",
                queue, result.success, result.subscription_id);
            result
        } else {
            log::warn!(target: "normfs-mem", "Queue '{}' not found for follow_full", queue);
            MemReadResult::fail()
        }
    }

    pub async fn read_full_negative(
        &self,
        queue: &QueueId,
        offset: UintN,
        step: usize,
        limit: u64,
        target_chan: &Sender<ReadEntry>,
    ) -> MemReadResult {
        log::debug!(target: "normfs-mem", "Reading negative from queue '{}' - Offset: {}, Limit: {}",
            queue, offset, limit);

        let mem_queue = self.queues.read().unwrap().get(queue).cloned();

        if let Some(mem_queue) = mem_queue {
            let result = mem_queue
                .read_full_negative(offset, step, limit, target_chan)
                .await;
            log::debug!(target: "normfs-mem", "Read negative from queue '{}' completed - Success: {}, Start ID: {:?}",
                queue, result.success, result.start_id);
            result
        } else {
            log::warn!(target: "normfs-mem", "Queue '{}' not found for read_full_negative", queue);
            MemReadResult::fail()
        }
    }

    pub async fn follow_full_negative(
        &self,
        queue: &QueueId,
        offset: UintN,
        step: usize,
        target_chan: &Sender<ReadEntry>,
    ) -> MemReadResult {
        log::debug!(target: "normfs-mem", "Starting follow_full_negative from queue '{}' - Offset: {}",
            queue, offset);

        let mem_queue = self.queues.read().unwrap().get(queue).cloned();

        if let Some(mem_queue) = mem_queue {
            let result = mem_queue
                .follow_full_negative(offset, step, target_chan)
                .await;
            log::debug!(target: "normfs-mem", "Follow_full_negative from queue '{}' - Success: {}, Subscription ID: {:?}, Start ID: {:?}",
                queue, result.success, result.subscription_id, result.start_id);
            result
        } else {
            log::warn!(target: "normfs-mem", "Queue '{}' not found for follow_full_negative", queue);
            MemReadResult::fail()
        }
    }

    pub fn subscribe(&self, queue: &QueueId, callback: SubscriberCallback) -> Option<usize> {
        log::debug!(target: "normfs-mem", "Subscribing to queue '{}'", queue);

        let mem_queue = self.queues.read().unwrap().get(queue).cloned();

        if let Some(mem_queue) = mem_queue {
            let subscriber_id = mem_queue.subscribe(callback);
            Some(subscriber_id)
        } else {
            log::warn!(target: "normfs-mem", "Queue '{}' not found for subscription", queue);
            None
        }
    }

    pub fn unsubscribe(&self, queue: &QueueId, subscriber_id: usize) {
        log::debug!(target: "normfs-mem", "Unsubscribing from queue '{}'", queue);

        let mem_queue = self.queues.read().unwrap().get(queue).cloned();

        if let Some(mem_queue) = mem_queue {
            mem_queue.unsubscribe(subscriber_id);
        } else {
            log::warn!(target: "normfs-mem", "Queue '{}' not found for unsubscription", queue);
        }
    }
}

#[cfg(test)]
mod tests;
