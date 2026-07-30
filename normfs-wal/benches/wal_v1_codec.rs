//! CPU-bound V1 WAL codec benchmarks (metrics c, d, f):
//!   * entry_encode   — write throughput
//!   * entry_iterate  — read/iteration throughput (decode + checksum-verify)
//!   * checksum       — CRC32C fast-path cost vs the portable path and xxHash64
//!
//! Every group warms up and then measures for a fixed window; both default to
//! 5 s / 30 s and are overridable (in seconds) with WAL_BENCH_WARMUP and
//! WAL_BENCH_MEASURE.
//!
//!   cargo bench -p normfs-wal --bench wal_v1_codec
//!   WAL_BENCH_MEASURE=2 WAL_BENCH_WARMUP=1 cargo bench -p normfs-wal --bench wal_v1_codec

use std::hint::black_box;
use std::time::Duration;

use bytes::BytesMut;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use normfs_wal::{crc32c, crc32c_portable, WalEntryV1};
use xxhash_rust::xxh64;

/// Payloads spanning the varint-width boundaries; small sizes expose the
/// per-entry overhead, 12 KiB is a large sensor block.
const PAYLOADS: [usize; 5] = [8, 64, 256, 1024, 12 * 1024];

fn env_secs(var: &str, default: u64) -> Duration {
    Duration::from_secs(
        std::env::var(var)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(default),
    )
}
fn warm() -> Duration {
    env_secs("WAL_BENCH_WARMUP", 5)
}
fn meas() -> Duration {
    env_secs("WAL_BENCH_MEASURE", 30)
}

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
    g.warm_up_time(warm());
    g.measurement_time(meas());

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
    g.warm_up_time(warm());
    g.measurement_time(meas());
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
    g.warm_up_time(warm());
    g.measurement_time(meas());

    // Sizes around the 8-byte word boundary the intrinsic loop uses, up to a
    // large block where the fast path's throughput advantage is clearest.
    for &p in &[8usize, 64, 256, 1024, 4096, 65536] {
        let data = pseudo_random(p);
        g.throughput(Throughput::Bytes(p as u64));

        g.bench_with_input(BenchmarkId::new("crc32c_dispatched", p), &data, |b, d| {
            b.iter(|| black_box(crc32c(0, d)));
        });
        g.bench_with_input(BenchmarkId::new("crc32c_portable", p), &data, |b, d| {
            b.iter(|| black_box(crc32c_portable(0, d)));
        });
        g.bench_with_input(BenchmarkId::new("xxh64", p), &data, |b, d| {
            b.iter(|| black_box(xxh64::xxh64(d, 0)));
        });
    }
    g.finish();
}

criterion_group!(benches, bench_encode, bench_iterate, bench_checksum);
criterion_main!(benches);
