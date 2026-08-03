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
//! WAL file, not the whole dataset. Recovery cost is bounded by the WAL rotation
//! size, so this benchmark ignores the configured dataset size and writes just
//! under one file: a bigger dataset only adds older files recovery never touches.
//!
//! Both endings a restart can find are measured. A clean shutdown leaves a whole
//! file; a crash leaves a partial entry at the tail, which recovery has to detect
//! and discard. The second is what recovery is for, so it is not optional here.
//!
//!   cargo bench -p normfs --bench recovery_benchmark

mod common;

use bytes::Bytes;
use common::{dir_size, BenchConfig};
use normfs::NormFS;
use std::path::{Path, PathBuf};
use std::time::Instant;

const QUEUE: &str = "recovery_bench_queue";

/// The first pass reads cold and the rest warm, so several are taken and the
/// spread is reported alongside the median.
const PASSES: usize = 3;

/// A file this short holds only a header, no entries — the backward walk skips
/// it. Rotation leaves one behind every time the queue is opened for write. An
/// empty V1 file is 17 bytes.
const HEADER_ONLY_BYTES: u64 = 64;

/// Widest V1 framing: a 5 B record-size varint plus a 4 B CRC32C. The dataset is
/// sized in framed bytes, not payload bytes — at this record size framing is 6%,
/// and ignoring it overshoots the rotation size and splits the dataset across
/// two files, leaving recovery a near-empty one to scan.
const MAX_FRAMING: u64 = 9;

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
    cfg.print_header("NormFS Recovery Benchmark");

    // Just under one file, so recovery scans a full one. Writing more only adds
    // older files the backward walk never reaches, leaving a near-empty newest
    // file and a recovery time near zero.
    let framed = cfg.block_size as u64 + MAX_FRAMING;
    let blocks = (cfg.wal_file_bytes as u64 * 95 / 100) / framed;
    assert!(
        blocks > 0,
        "the WAL rotation size is smaller than one record"
    );
    println!(
        "Sizing the dataset to one WAL file: {} records, up to {:.0} MiB framed\n",
        blocks,
        (blocks * framed) as f64 / (1024.0 * 1024.0)
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
            normfs.enqueue(&queue, block.clone()).await?;
        }
        normfs.close().await?;
    }
    // Marks the directory as this benchmark's, so the next run may reset it.
    cfg.write_manifest()?;

    println!(
        "Built {:.2} GiB on disk in {:.1} s across {} WAL file(s)",
        dir_size(&cfg.dir) as f64 / (1u64 << 30) as f64,
        build_start.elapsed().as_secs_f64(),
        wal_files(&cfg.dir).len()
    );

    let scanned = newest_wal_with_entries(&cfg.dir)
        .and_then(|p| std::fs::metadata(p).ok())
        .map(|m| m.len())
        .unwrap_or(0);
    println!(
        "Newest WAL file with entries (the one recovery scans): {:.1} MiB\n",
        scanned as f64 / (1024.0 * 1024.0)
    );

    let clean = measure(&cfg, "clean shutdown").await?;

    // Crash last: truncation is destructive, and the clean numbers have to come
    // from an intact file.
    match truncate_newest_wal(&cfg.dir) {
        Ok(path) => println!("\nTruncated the tail of {}", path.display()),
        Err(e) => {
            eprintln!("Error: no WAL file to truncate: {e}");
            std::process::exit(1);
        }
    }
    let crashed = measure(&cfg, "after a crash").await?;

    println!("\nRecovery benchmark completed!");
    println!("========================");
    for (label, (open_secs, ready_secs)) in [("clean", &clean), ("crash", &crashed)] {
        report(&format!("{label}: NormFS::new (store recovery)"), open_secs);
        report(&format!("{label}: queue ready (WAL scan)"), ready_secs);
    }

    let scanned_mib = scanned as f64 / (1024.0 * 1024.0);
    let ready_median = median(&clean.1);
    if ready_median > 0.0 {
        println!(
            "WAL scan rate: {:.2} M records/s | {:.0} MB/s over {:.1} MiB (clean, median)",
            blocks as f64 / ready_median / 1e6,
            scanned_mib / ready_median,
            scanned_mib
        );
    }
    // A small scanned file means the walk had almost nothing to do.
    if scanned * 2 < cfg.wal_file_bytes as u64 {
        println!(
            "Note: that file is under half the WAL rotation size, so this run \
             understates recovery."
        );
    }

    Ok(())
}

/// Open and ready the queue `PASSES` times, timing each phase. Returns the
/// per-pass seconds for `NormFS::new` and for the WAL scan.
async fn measure(
    cfg: &BenchConfig,
    label: &str,
) -> Result<(Vec<f64>, Vec<f64>), Box<dyn std::error::Error>> {
    let mut open_secs = Vec::with_capacity(PASSES);
    let mut ready_secs = Vec::with_capacity(PASSES);

    for pass in 1..=PASSES {
        // Fresh instance per pass: recovery only happens on the way in.
        let open_start = Instant::now();
        let normfs = NormFS::new(cfg.dir.clone(), cfg.settings()).await?;
        let open = open_start.elapsed().as_secs_f64();

        let queue = normfs.resolve(QUEUE);
        let ready_start = Instant::now();
        normfs.ensure_queue_exists_for_write(&queue).await?;
        let ready = ready_start.elapsed().as_secs_f64();

        normfs.close().await?;

        println!(
            "{label} pass {pass}/{PASSES}: NormFS::new {:.3} s | queue ready {:.3} s | total {:.3} s",
            open,
            ready,
            open + ready
        );
        open_secs.push(open);
        ready_secs.push(ready);
    }

    Ok((open_secs, ready_secs))
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

/// Every `.wal` file under `dir`, newest last: ids are zero-padded, so a plain
/// sort orders them.
fn wal_files(dir: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    collect_wal(dir, &mut found);
    found.sort();
    found
}

fn collect_wal(dir: &Path, out: &mut Vec<PathBuf>) {
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

/// The file recovery actually scans. Opening the queue for write starts a fresh
/// file, so after a pass the newest file on disk is an empty one and the
/// backward walk goes past it.
fn newest_wal_with_entries(dir: &Path) -> Option<PathBuf> {
    wal_files(dir).into_iter().rev().find(|p| {
        std::fs::metadata(p)
            .map(|m| m.len() > HEADER_ONLY_BYTES)
            .unwrap_or(false)
    })
}

/// Leave a partial entry at the tail for recovery to discard.
fn truncate_newest_wal(dir: &Path) -> Result<PathBuf, String> {
    let path = newest_wal_with_entries(dir)
        .ok_or_else(|| format!("no .wal file with entries under {}", dir.display()))?;
    let bytes = std::fs::read(&path).map_err(|e| e.to_string())?;
    let keep = bytes.len().saturating_sub(7);
    std::fs::write(&path, &bytes[..keep]).map_err(|e| e.to_string())?;
    Ok(path)
}
