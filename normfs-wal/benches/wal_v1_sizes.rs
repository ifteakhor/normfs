//! WAL entry framing overhead and file-size tables (metrics a and b).
//!
//! Not a timed benchmark: it encodes representative workloads and reports exact
//! on-disk byte sizes, so per-entry overhead and whole-file size are concrete
//! numbers rather than estimates.
//!
//!   cargo bench -p normfs-wal --bench wal_v1_sizes

mod common;

use bytes::BytesMut;
use common::{BIG_PAYLOAD, PAYLOAD};
use normfs_wal::WalEntryV1;

/// Exact on-disk size of one entry: `[record_size varint][record][crc32c]`.
fn entry_len(record: &[u8]) -> usize {
    let mut buf = BytesMut::new();
    WalEntryV1::new(record).write_to_bytes(&mut buf).unwrap();
    buf.len()
}

fn mib(bytes: usize) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

fn main() {
    // Small payloads dominate the WAL in high-frequency sensor ingestion, which
    // is where the fixed per-entry overhead matters most; 12 KiB is a large
    // block for contrast.
    let payloads = [8usize, PAYLOAD, 256, 1024, BIG_PAYLOAD];

    println!("== WAL entry framing (bytes) ==");
    println!("{:>9} | {:>8} | {:>9}", "payload", "total", "overhead");
    for &p in &payloads {
        let record = vec![0u8; p];
        let total = entry_len(&record);
        println!("{:>9} | {:>8} | {:>9}", p, total, total - p);
    }

    let n = 1_000_000usize;
    println!("\n== WAL file size for {n} entries (entries only) ==");
    println!("{:>9} | {:>11}", "payload", "MiB");
    for &p in &payloads {
        let record = vec![0u8; p];
        // Per-entry width is fixed by the payload, so a uniform workload's file
        // is exactly n times the per-entry size.
        println!("{:>9} | {:>11.1}", p, mib(n * entry_len(&record)));
    }

    println!("\nNote: overhead is a 1-5 B record-size varint plus a 4 B CRC32C.");
}
