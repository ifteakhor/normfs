//! Peak heap use of the WAL write and recovery-scan paths (metric g).
//!
//! Not a timed benchmark. A counting global allocator records live and peak
//! bytes, and the run reports the peak reached while building a file and while
//! scanning it back. The question it answers is whether either path's memory
//! grows with the dataset: recovery has to scan files far larger than RAM, so a
//! scan that retains per-entry state cannot work at all, and a build whose
//! backlog grows without bound caps how large a file can be produced.
//!
//! Peak *live heap* is deliberately the metric rather than RSS. RSS also counts
//! pages the allocator has freed but not returned to the OS, so it can track
//! dataset size while the program holds almost nothing — the two disagreeing is
//! itself the finding, and only this number says what the code retains.
//!
//! Size defaults to 2 GiB, overridable with WAL_BENCH_MEM_GIB. Use at least
//! 2x RAM to make a scan genuinely uncached.
//!
//!   cargo bench -p normfs-wal --bench wal_memory
//!   WAL_BENCH_MEM_GIB=8 cargo bench -p normfs-wal --bench wal_memory

use std::alloc::{GlobalAlloc, Layout, System};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use normfs_types::{QueueId, QueueIdResolver};
use normfs_wal::{WalEntryFormat, WalHeader, WalSettings, WalStore};
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use uintn::UintN;

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

/// Tracks live bytes and the high-water mark. `Relaxed` throughout: these are
/// statistics, not synchronisation, and the peak is read only after both
/// phases finish.
struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            bump(layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = unsafe { System.realloc(ptr, layout, new_size) };
        if !new_ptr.is_null() {
            if new_size >= layout.size() {
                bump(new_size - layout.size());
            } else {
                LIVE.fetch_sub(layout.size() - new_size, Ordering::Relaxed);
            }
        }
        new_ptr
    }
}

fn bump(size: usize) {
    let live = LIVE.fetch_add(size, Ordering::Relaxed) + size;
    // CAS rather than a store: a concurrent allocation may have published a
    // higher mark, and the peak must never go down.
    let mut peak = PEAK.load(Ordering::Relaxed);
    while live > peak {
        match PEAK.compare_exchange_weak(peak, live, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(current) => peak = current,
        }
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

fn reset_peak() {
    PEAK.store(LIVE.load(Ordering::Relaxed), Ordering::Relaxed);
}

fn peak() -> usize {
    PEAK.load(Ordering::Relaxed)
}

fn live() -> usize {
    LIVE.load(Ordering::Relaxed)
}

fn mib(bytes: usize) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

/// Cap on enqueued-but-unwritten bytes during a build. Large enough that the
/// writer still batches efficiently — a small window paces the producer off the
/// writer's flush timer and costs orders of magnitude of throughput — and small
/// enough that a build far larger than RAM stays bounded.
const BUILD_IN_FLIGHT_BYTES: u64 = 256 * 1024 * 1024;
const CHECK_EVERY: u64 = 1024;

/// Current on-disk length of the WAL file, or 0 before it exists.
fn file_len(path: &Path) -> u64 {
    std::fs::metadata(path).map(|m| m.len()).unwrap_or(0)
}

async fn build_file(
    root: &Path,
    format: WalEntryFormat,
    n: u64,
    payload: usize,
) -> (WalStore, QueueId, UintN) {
    let (written_tx, _written_rx) = mpsc::unbounded_channel();
    let (complete_tx, _complete_rx) = mpsc::unbounded_channel();
    let store = WalStore::new(root, written_tx, complete_tx);

    let queue_id = QueueIdResolver::new("bench").resolve("mem");
    let file_id = UintN::from(1u64);
    let settings = WalSettings {
        max_file_size: 1 << 40, // never rotate: one file per measurement
        write_buffer_size: 8 * 1024 * 1024,
        enable_fsync: false,
        wal_entry_format: format,
        ..Default::default()
    };
    store
        .start_writer(&queue_id, &file_id, WalHeader::default(), settings, None)
        .await
        .unwrap();

    // One shared payload: `Bytes::clone` is a refcount bump, so anything the
    // build retains is the WAL's, not the records'.
    let record = Bytes::from(vec![0xABu8; payload]);
    let wal_path = file_id.to_file_path(queue_id.to_wal_dir(root).to_str().unwrap(), "wal");
    for i in 0..n {
        store
            .enqueue(&queue_id, UintN::from(i), record.clone())
            .unwrap();

        // Gate on bytes actually landed, or the producer runs at memory speed
        // while the writer drains at disk speed and the gap stays on the heap.
        if i % CHECK_EVERY == 0 {
            let enqueued = (i + 1) * payload as u64;
            let mut stalled = 0u32;
            while enqueued.saturating_sub(file_len(&wal_path)) > BUILD_IN_FLIGHT_BYTES {
                tokio::time::sleep(Duration::from_millis(2)).await;
                stalled += 1;
                assert!(stalled < 60_000, "writer made no progress for 120 s at {i}");
            }
        }
    }
    store.close().await.unwrap();

    (store, queue_id, file_id)
}

fn main() {
    let gib: u64 = std::env::var("WAL_BENCH_MEM_GIB")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2);
    let payload = 12 * 1024usize;
    // Matches wal_v1_scan's sizing so the two benches describe the same file.
    let n = (gib << 30) / (payload as u64 + 28);

    let rt = Runtime::new().unwrap();

    println!(
        "== WAL peak live heap, {} GiB dataset, {} entries of {} B ==\n",
        gib, n, payload
    );
    println!(
        "{:>7} | {:>14} | {:>14} | {:>12}",
        "format", "build peak MiB", "scan peak MiB", "on-disk GiB"
    );
    println!("{:->7}-+-{:->14}-+-{:->14}-+-{:->12}", "", "", "", "");

    for (label, format) in [("V1", WalEntryFormat::V1)] {
        let tmp = tempfile::tempdir().unwrap();

        reset_peak();
        let (store, queue_id, file_id) = rt.block_on(build_file(tmp.path(), format, n, payload));
        let build_peak = peak();

        let bytes = std::fs::metadata(
            file_id.to_file_path(queue_id.to_wal_dir(tmp.path()).to_str().unwrap(), "wal"),
        )
        .map(|m| m.len())
        .unwrap_or(0);

        // Measured after the build's allocations have been dropped, so the
        // scan's peak reflects only what the scan itself holds.
        reset_peak();
        let end = rt
            .block_on(store.get_file_end(&queue_id, &file_id))
            .unwrap();
        let scan_peak = peak();
        assert_eq!(
            end,
            Some(UintN::from(n - 1)),
            "{label} scan should reach the last entry"
        );

        println!(
            "{:>7} | {:>14.1} | {:>14.1} | {:>12.2}",
            label,
            mib(build_peak),
            mib(scan_peak),
            bytes as f64 / (1u64 << 30) as f64
        );

        drop(store);
        drop(tmp);
    }

    println!(
        "\nLive heap still held after both formats: {:.1} MiB",
        mib(live())
    );
    println!(
        "Scan peak should be flat in the dataset size — recovery reads files\n\
         larger than RAM. Compare against RSS: if RSS tracks dataset size while\n\
         these stay flat, the growth is allocator retention, not retained data."
    );
}
