//! Shared setup for the end-to-end NormFS throughput benchmarks.
//!
//! Manual throughput programs, not Criterion: one pass over a multi-GiB dataset
//! does not fit its repeat-and-sample model.
//!
//! Nothing here is configurable by environment variable. A benchmark whose
//! numbers depend on how it was invoked cannot be compared against a pasted log,
//! so every run has the same shape and prints it.
//!
//! Two choices are worth stating because they decide what is being measured:
//!
//!   * [`BLOCK_SIZE`] is a sensor message. Small records are the workload the
//!     WAL exists for, and the only size at which per-entry framing is a
//!     meaningful fraction of the file.
//!   * Compression and encryption are off. `QueueConfig::default()` is Zstd +
//!     AES, and leaving them on would measure a codec compressing a synthetic
//!     block rather than the WAL path. Turn them on in `settings` to measure
//!     the production stack instead.
//!
//! Nothing offloads to the store: the per-queue cap is disabled, so a read stays
//! in the WAL rather than measuring a WAL/store/memory mix that shifts with
//! offload timing.
//!
//! Read benchmarks reuse the dataset a write left behind, so the writer records
//! what it produced and the reader refuses anything else. Otherwise a directory
//! from an earlier run of a different shape is measured silently.

#![allow(dead_code)] // each bench binary uses a subset

use std::path::PathBuf;

use normfs::{NormFsSettings, QueueConfig, QueueSettings};
use normfs_types::{CompressionType, EncryptionType};
use normfs_wal::WalSettings;

const MANIFEST: &str = "bench-manifest.txt";

/// Record size: a sensor message, and the size every reported figure uses.
const BLOCK_SIZE: usize = 80;

/// Records written. At [`BLOCK_SIZE`] this is a dataset large enough to leave
/// the caches and cross several WAL files, and small enough to build in a run
/// nobody has to plan around.
const TOTAL_BLOCKS: usize = 20_000_000;

/// WAL rotation size. Recovery scans only the newest file holding entries, so
/// this — not the dataset size — bounds what a restart reads.
const WAL_FILE_BYTES: usize = 128 << 20;

pub struct BenchConfig {
    pub dir: PathBuf,
    pub block_size: usize,
    pub total_blocks: usize,
    pub compression: CompressionType,
    pub encryption: EncryptionType,
    /// `None` disables the cap, so nothing offloads to the store.
    pub max_queue_bytes: Option<u64>,
    pub wal_file_bytes: usize,
}

impl BenchConfig {
    pub fn new() -> Self {
        Self {
            dir: std::env::temp_dir().join("normfs-bench"),
            block_size: BLOCK_SIZE,
            total_blocks: TOTAL_BLOCKS,
            compression: CompressionType::None,
            encryption: EncryptionType::None,
            max_queue_bytes: None,
            wal_file_bytes: WAL_FILE_BYTES,
        }
    }

    pub fn total_bytes(&self) -> u64 {
        self.total_blocks as u64 * self.block_size as u64
    }

    pub fn settings(&self) -> NormFsSettings {
        // Compression and encryption reach the writer through the queue config:
        // `NormFS` overwrites those two fields of `wal_settings` from it.
        let queue_config = QueueConfig {
            compression_type: self.compression,
            enable_fsync: true,
            encryption_type: self.encryption,
        };

        NormFsSettings {
            max_disk_usage_per_queue: self.max_queue_bytes,
            wal_settings: WalSettings {
                max_file_size: self.wal_file_bytes,
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
            "Dataset: {:.2} GiB in {} records of {} B",
            self.total_bytes() as f64 / (1u64 << 30) as f64,
            self.total_blocks,
            self.block_size
        );
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
        println!(
            "WAL file size: {:.0} MiB",
            self.wal_file_bytes as f64 / (1024.0 * 1024.0)
        );
        println!("Data directory: {}", self.dir.display());
        println!();
    }

    /// Compared by readers, so a dataset written with different settings is
    /// rejected rather than silently measured.
    fn signature(&self) -> String {
        format!(
            "blocks={} block_size={} compression={:?} encryption={:?} max_queue={:?} wal_file={}\n",
            self.total_blocks,
            self.block_size,
            self.compression,
            self.encryption,
            self.max_queue_bytes,
            self.wal_file_bytes
        )
    }

    pub fn write_manifest(&self) -> std::io::Result<()> {
        std::fs::write(self.dir.join(MANIFEST), self.signature())
    }

    pub fn check_manifest(&self) -> Result<(), String> {
        let path = self.dir.join(MANIFEST);
        let found = std::fs::read_to_string(&path).map_err(|e| {
            format!(
                "cannot read {} ({e}); run the write benchmark first",
                path.display()
            )
        })?;
        if found.trim() != self.signature().trim() {
            return Err(format!(
                "dataset does not match this run:\n  on disk: {}\n  wanted:  {}\n\
                 re-run the write benchmark",
                found.trim(),
                self.signature().trim()
            ));
        }
        Ok(())
    }

    /// Start from an empty directory; appending to a previous run measures
    /// neither. Refuses to delete a directory this benchmark did not write.
    pub fn reset_dir(&self) -> Result<(), String> {
        if self.dir.exists() {
            let empty = std::fs::read_dir(&self.dir)
                .map(|mut d| d.next().is_none())
                .unwrap_or(false);
            if !self.dir.join(MANIFEST).exists() && !empty {
                return Err(format!(
                    "{} exists, is not empty and has no {MANIFEST}, so it was not \
                     written by this benchmark — refusing to delete it. Remove \
                     it yourself.",
                    self.dir.display()
                ));
            }
            std::fs::remove_dir_all(&self.dir).map_err(|e| e.to_string())?;
        }
        std::fs::create_dir_all(&self.dir).map_err(|e| e.to_string())
    }
}

/// Bytes on disk under `dir` — the size produced, not the size asked for.
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

/// Unmigrated WAL files under `dir`, and their bytes: work the run has not finished.
pub fn wal_backlog(dir: &std::path::Path) -> (usize, u64) {
    let mut files = 0;
    let mut bytes = 0;
    fn walk(dir: &std::path::Path, files: &mut usize, bytes: &mut u64) {
        if let Ok(entries) = std::fs::read_dir(dir) {
            for e in entries.flatten() {
                let p = e.path();
                match e.file_type() {
                    Ok(t) if t.is_dir() => walk(&p, files, bytes),
                    Ok(_) if p.extension().is_some_and(|x| x == "wal") => {
                        *files += 1;
                        *bytes += e.metadata().map(|m| m.len()).unwrap_or(0);
                    }
                    _ => {}
                }
            }
        }
    }
    walk(dir, &mut files, &mut bytes);
    (files, bytes)
}
