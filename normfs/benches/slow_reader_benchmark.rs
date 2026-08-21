//! Does a slow reader hold up the writer? Reads pin the pages they borrow from,
//! so a client that consumes slowly takes pages out of the writer's pool. This
//! writes the same dataset with and without readers, at two pool sizes.
//!
//!   cargo bench -p normfs --bench slow_reader_benchmark

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use common::BenchConfig;
use normfs::{NormFS, ReadPosition};
use normfs_types::QueueId;
use uintn::UintN;

/// Entries a reader asks for per round, from the tail backwards.
const BATCH: u64 = 64;

/// How long a reader holds a round, which is how long its pins last.
const HOLD: Duration = Duration::from_millis(50);

const PROGRESS_INTERVAL: usize = 1_000_000;

/// Records per phase. Smaller than the write benchmark's: this runs four.
const RECORDS: usize = 10_000_000;

/// A queue gets 64 pages from the first, the floor of 2 from the second.
const POOLS: [(&str, usize); 2] = [("default 256 MiB", 256 << 20), ("tight 1 MiB", 1 << 20)];

/// Pinned rather than inherited. The question here is what share of a pool a
/// reader can pin, so the pool has to be measured in pages rather than bytes --
/// and the tight case is one page at the production default of 4 MiB, which is
/// below the floor a queue needs to run at all.
const PAGE_SIZE: usize = 256 * 1024;

#[tokio::main]
async fn main() {
    env_logger::init();

    // Its own directory: fewer records than the write benchmark, so a shared
    // one would leave the read benchmarks a dataset half its manifest.
    let cfg = BenchConfig {
        dir: std::env::temp_dir().join("normfs-slow-reader-bench"),
        mem_page_size: PAGE_SIZE,
        ..BenchConfig::new()
    };
    println!("NormFS Slow Reader Benchmark");
    println!("========================");
    println!(
        "Per phase: {} records of {} B ({:.2} GiB), compression {:?}, encryption {:?}",
        RECORDS,
        cfg.block_size,
        (RECORDS * cfg.block_size) as f64 / (1u64 << 30) as f64,
        cfg.compression,
        cfg.encryption
    );
    println!(
        "Readers hold {BATCH} entries from the tail for {} ms per round.",
        HOLD.as_millis()
    );
    println!("Data directory: {}", cfg.dir.display());
    println!();

    let mut results = Vec::new();
    for (pool_name, memory) in POOLS {
        for readers in [0usize, 4] {
            match run(&cfg, memory, readers).await {
                Ok(r) => results.push((pool_name, readers, r)),
                Err(e) => {
                    eprintln!("Benchmark failed on {pool_name} with {readers} reader(s): {e:?}");
                    std::process::exit(1);
                }
            }
        }
    }

    println!();
    println!("Pool            | Readers |  Enqueue  |    MB/s | vs no reader | Rounds read");
    println!("----------------+---------+-----------+---------+--------------+------------");
    for (pool_name, readers, r) in &results {
        let baseline = results
            .iter()
            .find(|(p, n, _)| p == pool_name && *n == 0)
            .map(|(_, _, b)| b.mb_per_sec)
            .unwrap_or(r.mb_per_sec);
        println!(
            "{:<15} | {:>7} | {:>7.1} s | {:>7.2} | {:>11.1}% | {:>11}",
            pool_name,
            readers,
            r.enqueue.as_secs_f64(),
            r.mb_per_sec,
            r.mb_per_sec / baseline * 100.0,
            r.rounds
        );
    }
}

struct Run {
    enqueue: Duration,
    mb_per_sec: f64,
    rounds: u64,
}

async fn run(
    cfg: &BenchConfig,
    memory: usize,
    readers: usize,
) -> Result<Run, Box<dyn std::error::Error>> {
    println!(
        "--- {} MiB pool ({} pages), {readers} slow reader(s) ---",
        memory >> 20,
        memory / PAGE_SIZE
    );
    cfg.reset_dir()?;
    cfg.write_manifest()?;

    let settings = normfs::NormFsSettings {
        max_memory_usage: memory,
        ..cfg.settings()
    };
    let normfs = Arc::new(NormFS::new(cfg.dir.clone(), settings).await?);
    let queue = normfs.resolve("slow_reader_queue");
    normfs.ensure_queue_exists_for_write(&queue).await?;

    // Something to read before a reader starts asking for the tail.
    let block = Bytes::from(vec![0u8; cfg.block_size]);
    for _ in 0..BATCH * 2 {
        normfs.enqueue(&queue, block.clone()).await?;
    }

    let stop = Arc::new(AtomicBool::new(false));
    let rounds = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();
    for _ in 0..readers {
        handles.push(tokio::spawn(slow_reader(
            Arc::clone(&normfs),
            queue.clone(),
            Arc::clone(&stop),
            Arc::clone(&rounds),
        )));
    }

    let start = Instant::now();
    for i in 0..RECORDS {
        normfs.enqueue(&queue, block.clone()).await?;
        if (i + 1) % PROGRESS_INTERVAL == 0 {
            println!(
                "  {:>5.1}% | {:>6.1}s | {:>6.2} MB/s",
                (i + 1) as f64 / RECORDS as f64 * 100.0,
                start.elapsed().as_secs_f64(),
                ((i + 1) * cfg.block_size) as f64 / (1024.0 * 1024.0) / start.elapsed().as_secs_f64()
            );
        }
    }
    let enqueue = start.elapsed();

    stop.store(true, Ordering::Relaxed);
    for handle in handles {
        let _ = handle.await;
    }
    normfs.close().await?;

    let mb = (RECORDS * cfg.block_size) as f64 / (1024.0 * 1024.0);
    let result = Run {
        enqueue,
        mb_per_sec: mb / enqueue.as_secs_f64(),
        rounds: rounds.load(Ordering::Relaxed),
    };
    println!(
        "  enqueue {:.1} s, {:.2} MB/s, {} reader round(s)",
        result.enqueue.as_secs_f64(),
        result.mb_per_sec,
        result.rounds
    );
    Ok(result)
}

async fn slow_reader(
    normfs: Arc<NormFS>,
    queue: QueueId,
    stop: Arc<AtomicBool>,
    rounds: Arc<AtomicU64>,
) {
    while !stop.load(Ordering::Relaxed) {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let read = tokio::spawn({
            let normfs = Arc::clone(&normfs);
            let queue = queue.clone();
            async move {
                normfs
                    .read(&queue, ReadPosition::ShiftFromTail(UintN::from(BATCH)), BATCH, 1, tx)
                    .await
            }
        });

        let mut held = Vec::new();
        while let Some(entry) = rx.recv().await {
            held.push(entry);
            tokio::time::sleep(HOLD / BATCH as u32).await;
        }
        read.abort();
        tokio::time::sleep(HOLD).await;
        drop(held);
        rounds.fetch_add(1, Ordering::Relaxed);
    }
}
