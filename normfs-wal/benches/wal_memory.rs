//! Peak heap use of the WAL write and recovery-scan paths (metric g).
//!
//! Not a timed benchmark. A counting global allocator records live and peak
//! bytes, and the run reports the peak reached while building a file and while
//! scanning it back. The question it answers is whether either path's memory
//! grows with the dataset: recovery has to scan files far larger than RAM, so a
//! scan that retains per-entry state cannot work at all, and a build whose
//! backlog grows without bound caps how large a file can be produced.
//!
//! Growth is what matters, so the same shape is run over a 64x range of sizes.
//! One row is a number; a ladder that stays flat is the answer, and it also
//! shows up the occasional 2x blip from the read buffer's growth policy for the
//! noise it is.
//!
//! Peak *live heap* is deliberately the metric rather than RSS. RSS also counts
//! pages the allocator has freed but not returned to the OS, so it can track
//! dataset size while the program holds almost nothing — the two disagreeing is
//! itself the finding, and only this number says what the code retains.
//!
//!   cargo bench -p normfs-wal --bench wal_memory

mod common;

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use common::{build_file, BIG_PAYLOAD, PAYLOAD};
use tokio::runtime::Runtime;
use uintn::UintN;

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

/// Tracks live bytes and the high-water mark. `Relaxed` throughout: these are
/// statistics, not synchronisation, and the peak is read only after a phase
/// finishes.
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

/// The small-record shape from 80 MB to 5 GB, plus one large-record row. A flat
/// scan peak across the ladder is the result; one that tracked the file size
/// would be the bug this looks for.
const CASES: [(usize, u64); 5] = [
    (PAYLOAD, 1_000_000),
    (PAYLOAD, 4_000_000),
    (PAYLOAD, 16_000_000),
    (PAYLOAD, 64_000_000),
    (BIG_PAYLOAD, 40_000),
];

fn main() {
    let rt = Runtime::new().unwrap();

    println!("== WAL peak live heap ==\n");
    println!(
        "{:>9} | {:>10} | {:>14} | {:>14} | {:>11}",
        "payload", "entries", "build peak MiB", "scan peak MiB", "on-disk MiB"
    );
    println!(
        "{:->9}-+-{:->10}-+-{:->14}-+-{:->14}-+-{:->11}",
        "", "", "", "", ""
    );

    for (payload, n) in CASES {
        let tmp = tempfile::tempdir().unwrap();

        reset_peak();
        let (store, queue_id, file_id) = rt.block_on(build_file(tmp.path(), "mem", n, payload));
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
            "{payload} B x {n} scan should reach the last entry"
        );

        println!(
            "{:>9} | {:>10} | {:>14.1} | {:>14.1} | {:>11.1}",
            payload,
            n,
            mib(build_peak),
            mib(scan_peak),
            bytes as f64 / (1024.0 * 1024.0)
        );

        drop(store);
        drop(tmp);
    }

    println!(
        "\nLive heap still held after every case: {:.1} MiB",
        mib(live())
    );
    println!(
        "Both peaks should be flat in the dataset size — recovery reads files\n\
         larger than RAM. Compare against RSS: if RSS tracks dataset size while\n\
         these stay flat, the growth is allocator retention, not retained data."
    );
}
