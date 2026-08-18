//! Multi-queue write throughput under a shared memory cap.
//!
//! Four cold queues write a small burst and go idle; a hot queue writes the
//! real dataset. Run twice, with the hot queue arriving first and last: the
//! order decides how much memory a fixed-share design leaves it.
//!
//!   cargo bench -p normfs --bench multi_queue_benchmark

mod common;

use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use common::BenchConfig;
use normfs::{NormFS, NormFsSettings};

const COLD_QUEUES: usize = 4;
const COLD_RECORDS: usize = 20_000;
const HOT_RECORDS: usize = 8_000_000;
const MEMORY: usize = 8 * 1024 * 1024;

#[tokio::main]
async fn main() {
    env_logger::init();
    let cfg = BenchConfig {
        dir: std::env::temp_dir().join("normfs-multiq-bench"),
        ..BenchConfig::new()
    };
    println!("NormFS Multi-Queue Write Benchmark");
    println!("========================");
    println!(
        "{COLD_QUEUES} cold queues x {COLD_RECORDS} records, 1 hot queue x {HOT_RECORDS} \
         records of {} B, max_memory_usage {} MiB",
        cfg.block_size,
        MEMORY >> 20
    );
    println!("Data directory: {}", cfg.dir.display());
    println!();

    let mut results = Vec::new();
    for hot_first in [false, true] {
        match run(&cfg, hot_first).await {
            Ok(r) => results.push((hot_first, r)),
            Err(e) => {
                eprintln!("failed (hot_first={hot_first}): {e:?}");
                std::process::exit(1);
            }
        }
    }

    println!();
    println!("Hot queue arrives | Hot enqueue |    MB/s | Close+drain | Total");
    println!("------------------+-------------+---------+-------------+--------");
    for (hot_first, r) in &results {
        println!(
            "{:<17} | {:>9.1} s | {:>7.2} | {:>9.1} s | {:>5.1} s",
            if *hot_first { "first" } else { "last (5th)" },
            r.hot_secs,
            r.mb_per_sec,
            r.close_secs,
            r.hot_secs + r.close_secs,
        );
    }
}

struct Run {
    hot_secs: f64,
    mb_per_sec: f64,
    close_secs: f64,
}

async fn run(cfg: &BenchConfig, hot_first: bool) -> Result<Run, Box<dyn std::error::Error>> {
    println!("--- hot queue {} ---", if hot_first { "first" } else { "last" });
    cfg.reset_dir()?;
    cfg.write_manifest()?;

    let settings = NormFsSettings {
        max_memory_usage: MEMORY,
        ..cfg.settings()
    };
    let normfs = Arc::new(NormFS::new(cfg.dir.clone(), settings).await?);
    let block = Bytes::from(vec![0u8; cfg.block_size]);

    let hot = normfs.resolve("hot");
    let cold: Vec<_> = (0..COLD_QUEUES)
        .map(|i| normfs.resolve(&format!("cold_{i}")))
        .collect();

    if hot_first {
        normfs.ensure_queue_exists_for_write(&hot).await?;
        // A burst so the hot queue is visibly busy before the cold ones arrive.
        for _ in 0..COLD_RECORDS {
            normfs.enqueue(&hot, block.clone()).await?;
        }
    }
    for q in &cold {
        normfs.ensure_queue_exists_for_write(q).await?;
        for _ in 0..COLD_RECORDS {
            normfs.enqueue(q, block.clone()).await?;
        }
    }
    if !hot_first {
        normfs.ensure_queue_exists_for_write(&hot).await?;
    }
    // Let the cold bursts reach disk, so an adaptive design has had its chance
    // to take their memory back before the hot phase is timed.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let start = Instant::now();
    for i in 0..HOT_RECORDS {
        normfs.enqueue(&hot, block.clone()).await?;
        if (i + 1) % 500_000 == 0 {
            println!(
                "  {:>5.1}% | {:>6.1}s | {:>6.2} MB/s",
                (i + 1) as f64 / HOT_RECORDS as f64 * 100.0,
                start.elapsed().as_secs_f64(),
                ((i + 1) * cfg.block_size) as f64 / (1024.0 * 1024.0) / start.elapsed().as_secs_f64()
            );
        }
    }
    let hot_secs = start.elapsed().as_secs_f64();

    let close_start = Instant::now();
    normfs.close().await?;
    // Reopen and settle, so both designs are timed to the same finished state.
    {
        let settings = NormFsSettings {
            max_memory_usage: MEMORY,
            ..cfg.settings()
        };
        let normfs = NormFS::new(cfg.dir.clone(), settings).await?;
        let deadline = Instant::now() + std::time::Duration::from_secs(300);
        while common::wal_backlog(&cfg.dir).0 > COLD_QUEUES + 1 {
            if Instant::now() > deadline {
                println!("  settle timed out");
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
        normfs.close().await?;
    }
    let close_secs = close_start.elapsed().as_secs_f64();

    let mb = (HOT_RECORDS * cfg.block_size) as f64 / (1024.0 * 1024.0);
    let r = Run {
        hot_secs,
        mb_per_sec: mb / hot_secs,
        close_secs,
    };
    println!(
        "  hot {:.1} s ({:.2} MB/s), close+settle {:.1} s",
        r.hot_secs, r.mb_per_sec, r.close_secs
    );
    Ok(r)
}
