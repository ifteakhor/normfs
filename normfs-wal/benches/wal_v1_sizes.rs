//! WAL entry framing overhead and file-size tables, V0 vs V1 (metrics a and b).
//!
//! Not a timed benchmark: it encodes representative workloads and reports exact
//! on-disk byte sizes, so the entry-header shrink (28 B -> 5-9 B) and the file
//! size reduction are concrete numbers rather than estimates.
//!
//!   cargo bench -p normfs-wal --bench wal_v1_sizes

use bytes::BytesMut;
use normfs_wal::{WalEntryHeader, WalEntryV1, WalHeader};
use uintn::UintN;

/// Exact V0 on-disk size of one entry: `[version][id][record_size][xxhash]`
/// header (widths from `header`) plus the raw record.
fn v0_entry_len(entry_id: u64, header: &WalHeader, record: &[u8]) -> usize {
    let mut buf = BytesMut::new();
    WalEntryHeader::new(UintN::from(entry_id), record)
        .write_to_bytes(&mut buf, header)
        .unwrap();
    buf.len() + record.len()
}

/// Exact V1 on-disk size of one entry: `[record_size varint][record][crc32c]`.
fn v1_entry_len(record: &[u8]) -> usize {
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
    let payloads = [8usize, 64, 256, 1024, 12 * 1024];
    let header = WalHeader::default(); // id_size = 4, data_size = 8

    println!("== WAL entry framing, V0 vs V1 (bytes) ==");
    println!(
        "{:>9} | {:>8} | {:>8} | {:>11} | {:>11} | {:>9}",
        "payload", "V0 total", "V1 total", "V0 overhead", "V1 overhead", "saved/entry"
    );
    for &p in &payloads {
        let record = vec![0u8; p];
        let v0 = v0_entry_len(0, &header, &record);
        let v1 = v1_entry_len(&record);
        println!(
            "{:>9} | {:>8} | {:>8} | {:>11} | {:>11} | {:>9}",
            p,
            v0,
            v1,
            v0 - p,
            v1 - p,
            v0 - v1
        );
    }

    let n = 1_000_000usize;
    println!("\n== WAL file size for {n} entries, V0 vs V1 (entries only) ==");
    println!(
        "{:>9} | {:>11} | {:>11} | {:>9}",
        "payload", "V0 (MiB)", "V1 (MiB)", "reduction"
    );
    for &p in &payloads {
        let record = vec![0u8; p];
        // Per-entry widths are fixed by the header, so the whole-file size is
        // exactly n * per-entry size for a uniform payload.
        let v0_total = n * v0_entry_len(0, &header, &record);
        let v1_total = n * v1_entry_len(&record);
        let reduction = 100.0 * (v0_total - v1_total) as f64 / v0_total as f64;
        println!(
            "{:>9} | {:>11.1} | {:>11.1} | {:>8.1}%",
            p,
            mib(v0_total),
            mib(v1_total),
            reduction
        );
    }
    println!(
        "\nNote: V0 header is {} B fixed + id_size({}) + data_size({}) = {} B; \
         V1 overhead is a 1-5 B record-size varint + 4 B CRC32C.",
        normfs_wal::WAL_ENTRY_HEADER_FIXED_OVERHEAD,
        header.id_size_bytes,
        header.data_size_bytes,
        normfs_wal::WAL_ENTRY_HEADER_FIXED_OVERHEAD
            + header.id_size_bytes as usize
            + header.data_size_bytes as usize
    );
}
