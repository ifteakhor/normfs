//! CPU-bound V1 WAL codec benchmarks (metrics c, d, f):
//!   * entry_encode   — write throughput
//!   * entry_iterate  — read/iteration throughput (decode + checksum-verify)
//!   * checksum       — CRC32C against xxHash64 and xxHash32, by input size
//!
//!   cargo bench -p normfs-wal --bench wal_codec

mod common;

use std::hint::black_box;

use bytes::BytesMut;
use common::{BIG_PAYLOAD, MEASUREMENT, PAYLOAD, WARM_UP};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use normfs_wal::{WalEntryV1, crc32c};
use xxhash_rust::{xxh32, xxh64};

/// Payloads spanning the varint-width boundaries, from the smallest useful
/// record up to a large block. PAYLOAD is the workload this is tuned for.
const PAYLOADS: [usize; 5] = [8, PAYLOAD, 256, 1024, BIG_PAYLOAD];

fn pseudo_random(len: usize) -> Vec<u8> {
    let mut state: u32 = 0x9E37_79B9;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            (state & 0xFF) as u8
        })
        .collect()
}

fn bench_encode(c: &mut Criterion) {
    let mut g = c.benchmark_group("entry_encode");
    g.warm_up_time(WARM_UP);
    g.measurement_time(MEASUREMENT);

    for &p in &PAYLOADS {
        let record = pseudo_random(p);
        g.throughput(Throughput::Bytes(p as u64));

        g.bench_with_input(BenchmarkId::new("v1", p), &record, |b, rec| {
            let mut buf = BytesMut::with_capacity(64 + rec.len());
            b.iter(|| {
                buf.clear();
                WalEntryV1::new(rec).write_to_bytes(&mut buf).unwrap();
                black_box(&buf);
            });
        });
    }
    g.finish();
}

fn bench_iterate(c: &mut Criterion) {
    let mut g = c.benchmark_group("entry_iterate");
    g.warm_up_time(WARM_UP);
    g.measurement_time(MEASUREMENT);
    let entries = 2_000u64;

    for &p in &PAYLOADS {
        let record = pseudo_random(p);

        // V1 buffer: [varint][record][crc32c], no id.
        let mut v1buf = BytesMut::new();
        for _ in 0..entries {
            WalEntryV1::new(&record).write_to_bytes(&mut v1buf).unwrap();
        }
        let v1 = v1buf.freeze();

        g.throughput(Throughput::Elements(entries));

        g.bench_with_input(BenchmarkId::new("v1", p), &v1, |b, buf| {
            b.iter(|| {
                let mut cursor = 0usize;
                let mut index = 0u64;
                let mut count = 0u64;
                while cursor < buf.len() {
                    let (_, _id, consumed) =
                        WalEntryV1::iter_next(&buf[cursor..], 0, index).unwrap();
                    cursor += consumed;
                    index += 1;
                    count += 1;
                }
                black_box(count);
            });
        });
    }
    g.finish();
}

fn bench_checksum(c: &mut Criterion) {
    let mut g = c.benchmark_group("checksum");
    g.warm_up_time(WARM_UP);
    g.measurement_time(MEASUREMENT);

    // Dense enough to locate the crossover rather than interpolate across it:
    // CRC32C's chunked path needs 768 bytes in one call, and xxHash is strong
    // in exactly the band below that. 768 and its neighbours are the point.
    for &p in &[
        8usize, 64, PAYLOAD, 128, 256, 512, 768, 1024, 2048, 4096, 65536,
    ] {
        let data = pseudo_random(p);
        g.throughput(Throughput::Bytes(p as u64));

        g.bench_with_input(BenchmarkId::new("crc32c_dispatched", p), &data, |b, d| {
            b.iter(|| black_box(crc32c(0, d)));
        });
        g.bench_with_input(BenchmarkId::new("xxh64", p), &data, |b, d| {
            b.iter(|| black_box(xxh64::xxh64(d, 0)));
        });
        g.bench_with_input(BenchmarkId::new("xxh32", p), &data, |b, d| {
            b.iter(|| black_box(xxh32::xxh32(d, 0)));
        });
    }
    g.finish();
}

criterion_group!(benches, bench_encode, bench_iterate, bench_checksum);
criterion_main!(benches);
