//! Shared setup for the end-to-end NormFS throughput benchmarks.
//!
//! These stay manual throughput programs rather than Criterion benches: one
//! pass over a dataset sized against RAM does not fit Criterion's
//! repeat-and-sample model. What they borrow from the WAL benches is that a run
//! is described by the environment instead of by recompiling, and prints its own
//! configuration, so a pasted log says what produced it.
//!
//!   NORMFS_BENCH_GIB          dataset size in GiB (default 2; 50 for a full run)
//!   NORMFS_BENCH_BLOCK_KIB    record size in KiB (default 12)
//!   NORMFS_BENCH_FORMAT       v0 or v1 (default: the WAL crate's own default)
//!   NORMFS_BENCH_COMPRESSION  none, gzip, xz or zstd (default zstd)
//!   NORMFS_BENCH_ENCRYPTION   none or aes (default aes)
//!   NORMFS_BENCH_MAX_QUEUE_GIB  per-queue disk cap in GiB, 0 for none (default 1)
//!   NORMFS_BENCH_DIR          dataset directory (default $TMPDIR/normfs-bench)
//!
//! Compression and encryption are explicit because they are easy to get wrong
//! by omission: a queue's `QueueConfig::default()` is Zstd + AES, so a benchmark
//! that simply takes the defaults is already compressing and encrypting whether
//! or not it says so. Set both to `none` for a raw baseline.
//!
//! The per-queue cap matters for the same reason. Once a dataset exceeds it the
//! queue offloads to the store, so a read then measures some mix of WAL, store
//! and memory that depends on offload timing — comparing two runs across that
//! boundary compares the mix, not the thing under test. A 50 GiB dataset against
//! the 1 GiB default is almost entirely store reads. Set it to 0 to keep
//! everything in the WAL.
//!
//! The read benchmarks reuse the dataset a write benchmark left behind — at
//! full scale rewriting it per run would dominate — so the writer records what
//! it produced and the reader refuses a dataset that does not match what it was
//! asked to read. Without that, a directory left by an earlier run of a
//! different size or format is measured silently.

#![allow(dead_code)] // each bench binary uses a subset

use std::path::PathBuf;

use normfs::{NormFsSettings, QueueConfig, QueueSettings};
use normfs_types::{CompressionType, EncryptionType};
use normfs_wal::{WalEntryFormat, WalSettings};

const MANIFEST: &str = "bench-manifest.txt";

pub struct BenchConfig {
    pub dir: PathBuf,
    pub block_size: usize,
    pub total_blocks: usize,
    pub format: WalEntryFormat,
    pub compression: CompressionType,
    pub encryption: EncryptionType,
    /// `None` means no per-queue cap, so nothing offloads to the store.
    pub max_queue_bytes: Option<u64>,
}

impl BenchConfig {
    pub fn from_env() -> Self {
        let gib = env_u64("NORMFS_BENCH_GIB", 2);
        let block_size = env_u64("NORMFS_BENCH_BLOCK_KIB", 12) as usize * 1024;
        assert!(block_size > 0, "NORMFS_BENCH_BLOCK_KIB must be non-zero");

        // An unrecognised value aborts rather than falling back: a silent
        // default would label a run with a format it did not use.
        let format = match std::env::var("NORMFS_BENCH_FORMAT").ok().as_deref() {
            None => WalEntryFormat::default(),
            Some("v0") | Some("V0") => WalEntryFormat::V0,
            Some("v1") | Some("V1") => WalEntryFormat::V1,
            Some(other) => panic!("NORMFS_BENCH_FORMAT must be v0 or v1, got {other:?}"),
        };

        let compression = match std::env::var("NORMFS_BENCH_COMPRESSION").ok().as_deref() {
            None | Some("zstd") => CompressionType::Zstd,
            Some("none") => CompressionType::None,
            Some("gzip") => CompressionType::Gzip,
            Some("xz") => CompressionType::Xz,
            Some(other) => {
                panic!("NORMFS_BENCH_COMPRESSION must be none, gzip, xz or zstd, got {other:?}")
            }
        };

        let encryption = match std::env::var("NORMFS_BENCH_ENCRYPTION").ok().as_deref() {
            None | Some("aes") => EncryptionType::Aes,
            Some("none") => EncryptionType::None,
            Some(other) => panic!("NORMFS_BENCH_ENCRYPTION must be none or aes, got {other:?}"),
        };

        let max_queue_gib = env_u64("NORMFS_BENCH_MAX_QUEUE_GIB", 1);
        let max_queue_bytes = (max_queue_gib > 0).then(|| max_queue_gib << 30);

        let dir = std::env::var_os("NORMFS_BENCH_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| std::env::temp_dir().join("normfs-bench"));

        Self {
            dir,
            block_size,
            total_blocks: ((gib << 30) / block_size as u64) as usize,
            format,
            compression,
            encryption,
            max_queue_bytes,
        }
    }

    pub fn total_bytes(&self) -> u64 {
        self.total_blocks as u64 * self.block_size as u64
    }

    pub fn settings(&self) -> NormFsSettings {
        // Compression and encryption reach the WAL writer through the queue
        // config, not through `wal_settings` — `NormFS` overwrites those two
        // fields from the queue's config — so they have to be set here.
        let queue_config = QueueConfig {
            compression_type: self.compression,
            enable_fsync: true,
            encryption_type: self.encryption,
        };

        NormFsSettings {
            max_disk_usage_per_queue: self.max_queue_bytes,
            wal_settings: WalSettings {
                wal_entry_format: self.format,
                ..Default::default()
            },
            queue_settings: QueueSettings::new(Vec::new(), queue_config)
                .expect("no glob patterns, cannot fail"),
            ..Default::default()
        }
    }

    pub fn print_header(&self, title: &str) {
        println!("{title}");
        println!("========================");
        println!(
            "Dataset: {:.2} GiB in {} blocks of {} KiB",
            self.total_bytes() as f64 / (1u64 << 30) as f64,
            self.total_blocks,
            self.block_size / 1024
        );
        println!("WAL entry format: {:?}", self.format);
        println!(
            "Compression: {:?} | Encryption: {:?}",
            self.compression, self.encryption
        );
        println!(
            "Per-queue disk cap: {}",
            match self.max_queue_bytes {
                Some(b) => format!(
                    "{:.2} GiB (offloads to store beyond this)",
                    b as f64 / (1u64 << 30) as f64
                ),
                None => "none (stays in the WAL)".to_string(),
            }
        );
        println!("Data directory: {}", self.dir.display());
        println!();
    }

    /// What a reader compares against, so a dataset written with a different
    /// size, block or format is rejected instead of silently measured.
    fn signature(&self) -> String {
        format!(
            "blocks={} block_size={} format={:?} compression={:?} encryption={:?} max_queue={:?}\n",
            self.total_blocks,
            self.block_size,
            self.format,
            self.compression,
            self.encryption,
            self.max_queue_bytes
        )
    }

    pub fn write_manifest(&self) -> std::io::Result<()> {
        std::fs::write(self.dir.join(MANIFEST), self.signature())
    }

    pub fn check_manifest(&self) -> Result<(), String> {
        let path = self.dir.join(MANIFEST);
        let found = std::fs::read_to_string(&path).map_err(|e| {
            format!(
                "cannot read {} ({e}); run the matching write benchmark first",
                path.display()
            )
        })?;
        if found.trim() != self.signature().trim() {
            return Err(format!(
                "dataset does not match this run:\n  on disk: {}\n  wanted:  {}\n\
                 re-run the write benchmark with the same NORMFS_BENCH_* settings",
                found.trim(),
                self.signature().trim()
            ));
        }
        Ok(())
    }

    /// Start from an empty directory — appending to a previous run's data
    /// measures neither run.
    ///
    /// Refuses to delete a directory this benchmark did not write, since
    /// NORMFS_BENCH_DIR points wherever the caller says.
    pub fn reset_dir(&self) -> Result<(), String> {
        if self.dir.exists() {
            let empty = std::fs::read_dir(&self.dir)
                .map(|mut d| d.next().is_none())
                .unwrap_or(false);
            if !self.dir.join(MANIFEST).exists() && !empty {
                return Err(format!(
                    "{} exists, is not empty and has no {MANIFEST}, so it was not \
                     written by this benchmark — refusing to delete it. Remove it \
                     yourself or point NORMFS_BENCH_DIR elsewhere.",
                    self.dir.display()
                ));
            }
            std::fs::remove_dir_all(&self.dir).map_err(|e| e.to_string())?;
        }
        std::fs::create_dir_all(&self.dir).map_err(|e| e.to_string())
    }
}

fn env_u64(var: &str, default: u64) -> u64 {
    match std::env::var(var) {
        Ok(v) => v
            .parse()
            .unwrap_or_else(|_| panic!("{var} must be a number, got {v:?}")),
        Err(_) => default,
    }
}

/// Bytes on disk under `dir`, so a run can report the size it actually produced
/// rather than the size it asked for.
pub fn dir_size(dir: &std::path::Path) -> u64 {
    let mut total = 0;
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            match entry.file_type() {
                Ok(t) if t.is_dir() => total += dir_size(&entry.path()),
                Ok(_) => total += entry.metadata().map(|m| m.len()).unwrap_or(0),
                Err(_) => {}
            }
        }
    }
    total
}
