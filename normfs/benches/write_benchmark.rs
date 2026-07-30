//! End-to-end write throughput through `NormFS::enqueue`.
//!
//! Size, block size, WAL entry format and directory come from the environment —
//! see `common/mod.rs`. Defaults to 2 GiB; use NORMFS_BENCH_GIB=50 for a full
//! run, and make sure the target filesystem has room for it.
//!
//!   cargo bench -p normfs --bench write_benchmark
//!   NORMFS_BENCH_GIB=50 NORMFS_BENCH_FORMAT=v1 cargo bench -p normfs --bench write_benchmark

mod common;

use bytes::Bytes;
use common::{dir_size, BenchConfig};
use normfs::NormFS;
use std::sync::Arc;
use std::time::Instant;

const PROGRESS_INTERVAL: usize = 10_000;

#[tokio::main]
async fn main() {
    env_logger::init();

    if let Err(e) = run_benchmark().await {
        eprintln!("Benchmark failed: {:?}", e);
        std::process::exit(1);
    }
}

async fn run_benchmark() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = BenchConfig::from_env();
    cfg.print_header("NormFS Write Benchmark");

    // Fresh directory: appending to a previous dataset measures neither it nor
    // this one, and leaves the reader a dataset matching no configuration.
    cfg.reset_dir()?;

    let normfs = NormFS::new(cfg.dir.clone(), cfg.settings()).await?;
    let normfs = Arc::new(normfs);
    let queue_name = normfs.resolve("write_bench_queue");
    normfs.ensure_queue_exists_for_write(&queue_name).await?;

    let block = Bytes::from(vec![0u8; cfg.block_size]);

    println!("Starting write benchmark...");
    let start_time = Instant::now();
    let mut last_progress_time = start_time;
    let mut last_progress_blocks = 0;

    for i in 0..cfg.total_blocks {
        normfs.enqueue(&queue_name, block.clone())?;

        if (i + 1) % PROGRESS_INTERVAL == 0 || i == cfg.total_blocks - 1 {
            let current_time = Instant::now();
            let total_elapsed = current_time.duration_since(start_time);
            let interval_elapsed = current_time.duration_since(last_progress_time);

            let total_bytes_written = (i + 1) * cfg.block_size;
            let interval_bytes = (i + 1 - last_progress_blocks) * cfg.block_size;

            let total_mb = total_bytes_written as f64 / (1024.0 * 1024.0);
            let progress_pct = (i + 1) as f64 / cfg.total_blocks as f64 * 100.0;

            println!(
                "[{:6.2}%] Written: {:.2} GiB | Total speed: {:.2} MB/s | Current speed: {:.2} MB/s | Elapsed: {:.1}s",
                progress_pct,
                total_mb / 1024.0,
                total_mb / total_elapsed.as_secs_f64(),
                (interval_bytes as f64 / (1024.0 * 1024.0)) / interval_elapsed.as_secs_f64(),
                total_elapsed.as_secs_f64()
            );

            last_progress_time = current_time;
            last_progress_blocks = i + 1;
        }
    }

    let enqueue_elapsed = start_time.elapsed();

    println!();
    println!("Closing NormFS...");
    let normfs = Arc::try_unwrap(normfs).unwrap_or_else(|arc| {
        panic!(
            "Failed to unwrap Arc, {} references remaining",
            Arc::strong_count(&arc)
        );
    });
    normfs.close().await?;

    // `enqueue` hands off to a writer task, so the close is where the tail
    // reaches disk. Timing only the loop reports memory speed.
    let total_elapsed = start_time.elapsed();
    let total_mb = cfg.total_bytes() as f64 / (1024.0 * 1024.0);

    println!();
    println!("Write benchmark completed!");
    println!("========================");
    println!(
        "Enqueue time: {:.2} s | including close: {:.2} s",
        enqueue_elapsed.as_secs_f64(),
        total_elapsed.as_secs_f64()
    );
    println!(
        "Average speed: {:.2} MB/s (including close)",
        total_mb / total_elapsed.as_secs_f64()
    );
    println!("Total blocks written: {}", cfg.total_blocks);
    println!(
        "On disk: {:.2} GiB",
        dir_size(&cfg.dir) as f64 / (1u64 << 30) as f64
    );

    // Lets the read benchmark reject a dataset from different settings.
    cfg.write_manifest()?;

    println!();
    println!("Data persisted at: {}", cfg.dir.display());
    println!("Run the read benchmark with the same NORMFS_BENCH_* settings.");

    Ok(())
}
