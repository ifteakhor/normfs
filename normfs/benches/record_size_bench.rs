//! Write cost at the two record sizes the rover actually produces: CAN bus
//! frames (~50 B) and camera frames (~1 MiB), measured to a settled state.
//!
//! Run one case per invocation so the peak-RSS figure belongs to that case:
//!
//!   cargo bench -p normfs --bench record_size_bench -- can50 256k
//!   cargo bench -p normfs --bench record_size_bench -- img1m 4m
//!   cargo bench -p normfs --bench record_size_bench -- img1m_paced 16m
//!
//! `img1m_paced` writes at the camera's real 20 frames/s for a fixed stretch
//! and reports CPU as a share of one core; the other two write as fast as the
//! queue accepts.
//!
//! The page size is the second argument, so a sweep is one build rather than
//! one rebuild per point -- which is also what stops a stale binary
//! reproducing a previous point's numbers exactly. It is echoed in the result
//! line, so a pasted log still says what produced it. Omit it for the
//! configured default. Pass a label for the printed line via
//! NORMFS_BENCH_LABEL.
//!
//! A record larger than a page is refused, so `img1m` at a page below ~1 MiB
//! reports its records as refused rather than quietly measuring something
//! else.

mod common;

use bytes::Bytes;
use normfs::{NormFS, NormFsSettings, QueueConfig, QueueSettings};
use normfs_types::{CompressionType, EncryptionType};
use normfs_wal::WalSettings;
use std::time::{Duration, Instant};

const PACED_SECS: u64 = 30;
const PACE_HZ: u64 = 20;

fn cpu_seconds() -> f64 {
    let s = std::fs::read_to_string("/proc/self/stat").unwrap();
    let rest = &s[s.rfind(')').unwrap() + 2..];
    let f: Vec<&str> = rest.split_whitespace().collect();
    let utime: u64 = f[11].parse().unwrap();
    let stime: u64 = f[12].parse().unwrap();
    (utime + stime) as f64 / 100.0
}

fn peak_rss_mib() -> f64 {
    let s = std::fs::read_to_string("/proc/self/status").unwrap();
    let line = s.lines().find(|l| l.starts_with("VmHWM:")).unwrap();
    let kib: f64 = line.split_whitespace().nth(1).unwrap().parse().unwrap();
    kib / 1024.0
}

fn settings(page_size: usize) -> NormFsSettings {
    let queue_config = QueueConfig {
        compression_type: CompressionType::None,
        enable_fsync: true,
        encryption_type: EncryptionType::None,
        ..QueueConfig::active()
    };
    NormFsSettings {
        max_disk_usage_per_queue: None,
        mem_page_size: page_size,
        wal_settings: WalSettings {
            max_file_size: 128 << 20,
            ..Default::default()
        },
        queue_settings: QueueSettings::new(Vec::new(), queue_config)
            .expect("no glob patterns, cannot fail"),
        ..Default::default()
    }
}

/// `64k`, `256k`, `1m`, `4m`, `16m`, or a plain byte count.
fn parse_page_size(arg: &str) -> Option<usize> {
    let (digits, scale) = match arg.as_bytes().last()? {
        b'k' | b'K' => (&arg[..arg.len() - 1], 1024),
        b'm' | b'M' => (&arg[..arg.len() - 1], 1024 * 1024),
        _ => (arg, 1),
    };
    digits.parse::<usize>().ok()?.checked_mul(scale)
}

#[tokio::main]
async fn main() {
    env_logger::init();
    // `cargo bench` hands a bare `--bench` to a harness=false binary. Any
    // other flag is a mistyped page size, and dropping it would run the sweep
    // at the default under the wrong label.
    let mut positional: Vec<String> = Vec::new();
    for a in std::env::args().skip(1) {
        if a == "--bench" {
            continue;
        }
        if a.starts_with('-') {
            eprintln!("unrecognised flag {a}");
            std::process::exit(2);
        }
        positional.push(a);
    }
    let Some(case) = positional.first().cloned() else {
        eprintln!("usage: record_size_bench -- can50|img1m|img1m_paced [page_size]");
        std::process::exit(2);
    };
    let page_size = match positional.get(1) {
        Some(arg) => parse_page_size(arg).unwrap_or_else(|| {
            eprintln!("unrecognised page size {arg}; try 64k, 256k, 1m, 4m, 16m");
            std::process::exit(2);
        }),
        None => NormFsSettings::all_active().mem_page_size,
    };
    let label = std::env::var("NORMFS_BENCH_LABEL").unwrap_or_default();

    let (block_size, count, paced) = match case.as_str() {
        "can50" => (50, 5_000_000, false),
        "img1m" => (1 << 20, 2_000, false),
        "img1m_paced" => (1 << 20, (PACED_SECS * PACE_HZ) as usize, true),
        other => {
            eprintln!("unknown case {other}");
            std::process::exit(2);
        }
    };

    // A record no page can hold is refused at enqueue, so say so here rather
    // than reporting a run of errors as a measurement.
    let cap = normfs_wal::max_record_len(page_size);
    if block_size > cap {
        eprintln!(
            "case {case} writes {block_size} B records, which a {} KiB page cannot hold \
             (largest is {cap} B): this point of the sweep does not exist",
            page_size / 1024
        );
        std::process::exit(2);
    }

    let dir = std::env::temp_dir().join("normfs-record-size-bench");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let normfs = NormFS::new(dir.clone(), settings(page_size)).await.unwrap();
    let queue = normfs.resolve("record_size_bench");
    normfs.ensure_queue_exists_for_write(&queue).await.unwrap();

    let block = Bytes::from(vec![0u8; block_size]);
    let c0 = cpu_seconds();
    let t0 = Instant::now();

    if paced {
        let mut tick = tokio::time::interval(Duration::from_millis(1000 / PACE_HZ));
        for _ in 0..count {
            tick.tick().await;
            normfs.enqueue(&queue, block.clone()).await.unwrap();
        }
    } else {
        for _ in 0..count {
            normfs.enqueue(&queue, block.clone()).await.unwrap();
        }
    }

    let enqueue_wall = t0.elapsed().as_secs_f64();
    normfs.close().await.unwrap();

    // `close()` can leave WAL files unmigrated; the run has not paid its full
    // cost until a reopen drains them.
    {
        let normfs = NormFS::new(dir.clone(), settings(page_size)).await.unwrap();
        let queue = normfs.resolve("record_size_bench");
        normfs.ensure_queue_exists_for_write(&queue).await.unwrap();
        let deadline = Instant::now() + Duration::from_secs(900);
        while common::wal_backlog(&dir).0 > 1 && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        normfs.close().await.unwrap();
    }

    let wall = t0.elapsed().as_secs_f64();
    let cpu = cpu_seconds() - c0;
    let mb = (count as f64 * block_size as f64) / (1024.0 * 1024.0);
    println!(
        "case={case} label={label} page={}KiB records={count} block={block_size}B \
         enqueue={enqueue_wall:.2}s settled={wall:.2}s cpu={cpu:.2}s \
         cpu_pct={:.1} thr={:.2}MB/s rss_peak={:.0}MiB",
        page_size / 1024,
        cpu / wall * 100.0,
        mb / wall,
        peak_rss_mib()
    );
    let _ = std::fs::remove_dir_all(&dir);
}
