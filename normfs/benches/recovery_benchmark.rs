//! Time to bring an existing data directory back into service — what a restart
//! actually costs.
//!
//! Two phases are timed separately because they are different work and either
//! can dominate:
//!
//!   * `NormFS::new`, which runs the store's recovery;
//!   * `ensure_queue_exists_for_write`, which walks back to the newest WAL file
//!     holding entries and scans it, verifying every entry's checksum and
//!     deriving its id.
//!
//! Reporting one number would hide which of the two moved.
//!
//! That walk stops at the first file with entries, so a restart scans **one**
//! WAL file, not the whole dataset. Recovery cost is therefore bounded by
//! NORMFS_BENCH_WAL_FILE_MIB, and that is the knob to turn here — this
//! benchmark sizes its own dataset to just under one file so the file being
//! scanned is a full one. NORMFS_BENCH_GIB is ignored; a bigger dataset only
//! adds older files that recovery never touches. See `common/mod.rs` for the
//! rest.
//!
//! NORMFS_BENCH_CRASH=1 lops the tail off the newest WAL file before measuring,
//! so recovery has to discard a partial entry — closer to the case a restart is
//! actually for than a clean shutdown is.
//!
//!   cargo bench -p normfs --bench recovery_benchmark
//!   NORMFS_BENCH_WAL_FILE_MIB=1024 NORMFS_BENCH_GIB=2 NORMFS_BENCH_FORMAT=v1 \
//!     cargo bench -p normfs --bench recovery_benchmark

mod common;

use bytes::Bytes;
use common::{dir_size, BenchConfig};
use normfs::NormFS;
use std::path::PathBuf;
use std::time::Instant;

const QUEUE: &str = "recovery_bench_queue";

fn passes() -> usize {
    std::env::var("NORMFS_BENCH_PASSES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3)
        .max(1)
}

fn crash_mode() -> bool {
    matches!(
        std::env::var("NORMFS_BENCH_CRASH").ok().as_deref(),
        Some("1") | Some("true")
    )
}

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
    cfg.print_header("NormFS Recovery Benchmark");

    // Just under one file, so everything lands in a single full WAL file and
    // recovery scans all of it. Writing more would only add older files the
    // backward walk stops before reaching, leaving a near-empty newest file and
    // a recovery time close to zero — which measures nothing.
    let blocks = (cfg.wal_file_bytes as u64 * 95 / 100) / cfg.block_size as u64;
    assert!(
        blocks > 0,
        "NORMFS_BENCH_WAL_FILE_MIB is smaller than one block"
    );
    println!(
        "Sizing the dataset to one WAL file: {} blocks, {:.0} MiB\n",
        blocks,
        (blocks * cfg.block_size as u64) as f64 / (1024.0 * 1024.0)
    );

    // Phase 0, untimed: produce the directory a restart would find.
    println!("Building dataset...");
    cfg.reset_dir()?;
    let build_start = Instant::now();
    {
        let normfs = NormFS::new(cfg.dir.clone(), cfg.settings()).await?;
        let queue = normfs.resolve(QUEUE);
        normfs.ensure_queue_exists_for_write(&queue).await?;
        let block = Bytes::from(vec![0u8; cfg.block_size]);
        for _ in 0..blocks {
            normfs.enqueue(&queue, block.clone())?;
        }
        normfs.close().await?;
    }
    println!(
        "Built {:.2} GiB on disk in {:.1} s across {} WAL file(s)",
        dir_size(&cfg.dir) as f64 / (1u64 << 30) as f64,
        build_start.elapsed().as_secs_f64(),
        count_wal_files(&cfg.dir)
    );

    if crash_mode() {
        match truncate_newest_wal(&cfg.dir) {
            Ok(path) => println!("Crash mode: truncated the tail of {}", path.display()),
            Err(e) => {
                eprintln!("Error: crash mode requested but no WAL file to truncate: {e}");
                std::process::exit(1);
            }
        }
    }
    // Measured before the first pass: opening the queue for write starts a new
    // WAL file, so after a pass the newest file is an empty one and no longer
    // the file recovery actually scanned.
    let scanned = newest_wal_len(&cfg.dir).unwrap_or(0);
    println!();

    let n_passes = passes();
    let mut open_secs: Vec<f64> = Vec::with_capacity(n_passes);
    let mut ready_secs: Vec<f64> = Vec::with_capacity(n_passes);

    for pass in 1..=n_passes {
        // A fresh instance per pass: recovery only happens on the way in, so
        // reusing one would measure nothing after the first.
        let open_start = Instant::now();
        let normfs = NormFS::new(cfg.dir.clone(), cfg.settings()).await?;
        let open = open_start.elapsed().as_secs_f64();

        let queue = normfs.resolve(QUEUE);
        let ready_start = Instant::now();
        normfs.ensure_queue_exists_for_write(&queue).await?;
        let ready = ready_start.elapsed().as_secs_f64();

        normfs.close().await?;

        println!(
            "Pass {pass}/{n_passes}: NormFS::new {:.3} s | queue ready {:.3} s | total {:.3} s",
            open,
            ready,
            open + ready
        );
        open_secs.push(open);
        ready_secs.push(ready);
    }

    // The first pass reads a cold file and the rest read a warm one, so the
    // spread is part of the answer rather than noise to average away.
    println!();
    println!("Recovery benchmark completed!");
    println!("========================");
    report("NormFS::new (store recovery)", &open_secs);
    report("queue ready (WAL scan)", &ready_secs);

    let scanned_mib = scanned as f64 / (1024.0 * 1024.0);
    let ready_median = median(&ready_secs);
    println!(
        "Newest WAL file (the one recovery scans): {:.1} MiB",
        scanned_mib
    );
    if ready_median > 0.0 {
        println!(
            "WAL scan rate: {:.2} MB/s (median)",
            scanned_mib / ready_median
        );
    }
    // A small newest file means the walk had almost nothing to do, so the
    // number above says little about recovery on a real restart.
    if scanned * 2 < cfg.wal_file_bytes as u64 {
        println!(
            "Note: that file is under half of NORMFS_BENCH_WAL_FILE_MIB, so this run \
             understates recovery."
        );
    }

    Ok(())
}

fn median(v: &[f64]) -> f64 {
    let mut s = v.to_vec();
    s.sort_by(|a, b| a.partial_cmp(b).unwrap());
    s[s.len() / 2]
}

fn report(label: &str, v: &[f64]) {
    let mut s = v.to_vec();
    s.sort_by(|a, b| a.partial_cmp(b).unwrap());
    println!(
        "{label}: median {:.3} s | fastest {:.3} s | slowest {:.3} s | spread {:.2}x",
        s[s.len() / 2],
        s[0],
        s[s.len() - 1],
        if s[0] > 0.0 {
            s[s.len() - 1] / s[0]
        } else {
            1.0
        }
    );
}

/// Every `.wal` file under `dir`, newest last by file name length then order —
/// ids are zero-padded per file, so a plain sort puts the newest at the end.
fn wal_files(dir: &std::path::Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    collect_wal(dir, &mut found);
    found.sort();
    found
}

fn collect_wal(dir: &std::path::Path, out: &mut Vec<PathBuf>) {
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            match entry.file_type() {
                Ok(t) if t.is_dir() => collect_wal(&path, out),
                Ok(_) if path.extension().is_some_and(|e| e == "wal") => out.push(path),
                _ => {}
            }
        }
    }
}

fn count_wal_files(dir: &std::path::Path) -> usize {
    wal_files(dir).len()
}

fn newest_wal_len(dir: &std::path::Path) -> Option<u64> {
    wal_files(dir)
        .last()
        .and_then(|p| std::fs::metadata(p).ok())
        .map(|m| m.len())
}

/// Drop the last few bytes of the newest WAL file, leaving a partial entry for
/// recovery to discard.
fn truncate_newest_wal(dir: &std::path::Path) -> Result<PathBuf, String> {
    let path = wal_files(dir)
        .pop()
        .ok_or_else(|| format!("no .wal file under {}", dir.display()))?;
    let bytes = std::fs::read(&path).map_err(|e| e.to_string())?;
    let keep = bytes.len().saturating_sub(7);
    std::fs::write(&path, &bytes[..keep]).map_err(|e| e.to_string())?;
    Ok(path)
}
