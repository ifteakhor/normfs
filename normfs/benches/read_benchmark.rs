//! End-to-end read throughput through `NormFS::read`, over the dataset a write
//! benchmark left behind.
//!
//! Reuses that dataset rather than rebuilding it — at full scale the write would
//! dominate — so it checks the dataset's manifest first and refuses one written
//! with a different shape. See `common/mod.rs` for what that shape is.
//!
//!   cargo bench -p normfs --bench write_benchmark   # produces the dataset
//!   cargo bench -p normfs --bench read_benchmark

mod common;

use common::{dir_size, BenchConfig};
use normfs::{NormFS, ReadPosition};
use std::sync::Arc;
use std::time::Instant;
use uintn::UintN;

const PROGRESS_INTERVAL: usize = 10_000;

/// One pass is not reproducible here — spreads of 3x between runs — so several
/// are taken and all reported alongside the median.
const PASSES: usize = 3;

#[tokio::main]
async fn main() {
    env_logger::init();

    if let Err(e) = run_benchmark().await {
        eprintln!("Benchmark failed: {:?}", e);
        std::process::exit(1);
    }
}

async fn run_benchmark() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = BenchConfig::new();
    cfg.print_header("NormFS Read Benchmark");

    // A mismatched dataset would silently measure the wrong thing.
    if let Err(e) = cfg.check_manifest() {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
    println!(
        "Dataset verified, {:.2} GiB on disk",
        dir_size(&cfg.dir) as f64 / (1u64 << 30) as f64
    );
    println!();

    let normfs = NormFS::new(cfg.dir.clone(), cfg.settings()).await?;
    let normfs = Arc::new(normfs);
    let queue_name = normfs.resolve("write_bench_queue");
    normfs.ensure_queue_exists_for_write(&queue_name).await?;

    let n_passes = PASSES;
    let mut pass_secs: Vec<f64> = Vec::with_capacity(n_passes);

    for pass in 1..=n_passes {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<normfs::ReadEntry>(1000);
        let start_offset = UintN::from(0u64);
        let limit = cfg.total_blocks as u64;

        println!("Starting read pass {pass}/{n_passes}...");
        let start_time = Instant::now();
        let mut last_progress_time = start_time;
        let mut last_progress_blocks = 0;

        let normfs_clone = normfs.clone();
        let queue_for_pass = queue_name.clone();
        let read_task = tokio::spawn(async move {
            normfs_clone
                .read(
                    &queue_for_pass,
                    ReadPosition::Absolute(start_offset.clone()),
                    limit,
                    1,
                    tx,
                )
                .await
        });

        let mut entries_read = 0u64;
        let mut expected_id: Option<UintN> = None;
        let mut bytes_read = 0usize;

        while let Some(entry) = rx.recv().await {
            if expected_id.is_none() {
                println!("Queue starts at entry ID: {}", entry.id);
                expected_id = Some(entry.id.clone());
            }

            if let Some(ref exp_id) = expected_id {
                if &entry.id != exp_id {
                    panic!(
                        "Order corruption detected! Expected entry ID: {}, but got: {}",
                        exp_id, entry.id
                    );
                }
            }

            if entry.data.len() != cfg.block_size {
                panic!(
                    "Unexpected data size! Expected: {} bytes, but got: {} bytes for entry ID: {}",
                    cfg.block_size,
                    entry.data.len(),
                    entry.id
                );
            }

            expected_id = Some(entry.id.increment());
            entries_read += 1;
            bytes_read += entry.data.len();

            if entries_read.is_multiple_of(PROGRESS_INTERVAL as u64)
                || entries_read == cfg.total_blocks as u64
            {
                let current_time = Instant::now();
                let total_elapsed = current_time.duration_since(start_time);
                let interval_elapsed = current_time.duration_since(last_progress_time);

                let total_mb = bytes_read as f64 / (1024.0 * 1024.0);
                let progress_pct = entries_read as f64 / cfg.total_blocks as f64 * 100.0;
                let interval_bytes =
                    (entries_read - last_progress_blocks) as usize * cfg.block_size;

                println!(
                "[{:6.2}%] Read: {:.2} GiB | Total speed: {:.2} MB/s | Current speed: {:.2} MB/s | Entries: {} | Elapsed: {:.1}s",
                progress_pct,
                total_mb / 1024.0,
                total_mb / total_elapsed.as_secs_f64(),
                (interval_bytes as f64 / (1024.0 * 1024.0)) / interval_elapsed.as_secs_f64(),
                entries_read,
                total_elapsed.as_secs_f64()
            );

                last_progress_time = current_time;
                last_progress_blocks = entries_read;
            }
        }

        read_task.await??;

        let total_elapsed = start_time.elapsed();
        let total_mb = bytes_read as f64 / (1024.0 * 1024.0);

        // A short read must not pass as throughput over the whole dataset.
        if entries_read != cfg.total_blocks as u64 {
            eprintln!(
                "ERROR: expected {} entries but read {}",
                cfg.total_blocks, entries_read
            );
            std::process::exit(1);
        }

        println!(
            "Pass {pass}/{n_passes}: {:.2} s, {:.2} MB/s, {} entries",
            total_elapsed.as_secs_f64(),
            total_mb / total_elapsed.as_secs_f64(),
            entries_read
        );
        pass_secs.push(total_elapsed.as_secs_f64());
    }

    let total_mb = cfg.total_bytes() as f64 / (1024.0 * 1024.0);
    let mut sorted = pass_secs.clone();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let median = sorted[sorted.len() / 2];

    println!();
    println!("Read benchmark completed!");
    println!("========================");
    println!("Data integrity: all entry IDs in sequential order, all blocks full size");
    println!(
        "Passes: {} | fastest {:.2} s | median {:.2} s | slowest {:.2} s",
        n_passes,
        sorted[0],
        median,
        sorted[sorted.len() - 1]
    );
    // Records per second is the figure that matters at this size: a small
    // record's cost is per-entry work, not bytes moved.
    println!(
        "Median speed: {:.2} M records/s | {:.2} MB/s",
        cfg.total_blocks as f64 / median / 1e6,
        total_mb / median
    );
    // A median over passes disagreeing by multiples is not a throughput figure.
    println!(
        "Spread (slowest/fastest): {:.2}x",
        sorted[sorted.len() - 1] / sorted[0]
    );
    println!();

    println!("Closing NormFS...");
    let normfs = Arc::try_unwrap(normfs).unwrap_or_else(|arc| {
        panic!(
            "Failed to unwrap Arc, {} references remaining",
            Arc::strong_count(&arc)
        );
    });
    normfs.close().await?;

    println!("NormFS closed successfully.");

    Ok(())
}
