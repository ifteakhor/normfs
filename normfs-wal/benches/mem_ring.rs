//! In-memory WAL cache benchmark: the V1 paged ring (`WalRing`) against a V0
//! baseline that reproduces the pre-V1 store — a flat `Vec<(id, Bytes)>` that
//! keeps every record live (refcounted, never copied) and grows without bound.
//!
//!   * ring_append        — cache one entry
//!   * ring_collect_range — read a contiguous id range back
//!   * ring_seek          — locate one entry by id
//!
//! The ring compacts records into fixed pages (a copy in, a decode out) to stay
//! bounded and reclaimable; the Vec does neither. So the honest reading is: the
//! ring pays a small CPU cost on append/read for bounded memory, and wins seek
//! outright (page arithmetic vs a linear scan).
//!
//! Warmup / measurement default to 5s / 30s, overridable (seconds) with
//! WAL_BENCH_WARMUP / WAL_BENCH_MEASURE.
//!
//!   cargo bench -p normfs-wal --bench mem_ring

use std::hint::black_box;
use std::time::Duration;

use bytes::Bytes;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use normfs_wal::{AppendOutcome, WalRing};

const N: u64 = 8_000;
const PAYLOAD: usize = 64;
const PAGE_SIZE: usize = 256 * 1024;
const PAGE_COUNT: usize = 4; // 1 MiB, comfortably holds N * ~69 B entries

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

fn record() -> Bytes {
    Bytes::from(vec![0xABu8; PAYLOAD])
}

/// A populated ring holding ids 0..N.
fn filled_ring() -> WalRing {
    let mut ring = WalRing::new(PAGE_COUNT, PAGE_SIZE, 0);
    let rec = record();
    for _ in 0..N {
        assert!(matches!(ring.append(rec.as_ref()), AppendOutcome::Cached(_)));
    }
    ring
}

/// The V0 baseline store: ids and refcounted records in a flat Vec.
fn filled_vec() -> Vec<(u64, Bytes)> {
    let rec = record();
    (0..N).map(|id| (id, rec.clone())).collect()
}

fn bench_append(c: &mut Criterion) {
    let mut g = c.benchmark_group("ring_append");
    g.warm_up_time(warm());
    g.measurement_time(meas());
    g.throughput(Throughput::Elements(N));
    let rec = record();

    // reinit resets the ring without reallocating, so the loop measures the
    // append path (frame encode + offset table), not allocation. Each append
    // copies the record into a page — the cost of staying bounded.
    g.bench_function("v1_ring", |b| {
        let mut ring = WalRing::new(PAGE_COUNT, PAGE_SIZE, 0);
        b.iter(|| {
            ring.reinit(0);
            for _ in 0..N {
                black_box(ring.append(rec.as_ref()));
            }
        });
    });

    // The old store just pushed a refcounted Bytes handle — no copy, but every
    // record stays live in memory forever.
    g.bench_function("v0_vec", |b| {
        let mut v: Vec<(u64, Bytes)> = Vec::with_capacity(N as usize);
        b.iter(|| {
            v.clear();
            for id in 0..N {
                v.push((id, rec.clone()));
            }
            black_box(&v);
        });
    });
    g.finish();
}

fn bench_collect_range(c: &mut Criterion) {
    let mut g = c.benchmark_group("ring_collect_range");
    g.warm_up_time(warm());
    g.measurement_time(meas());

    // Read back the middle half of the cache.
    let start = N / 4;
    let end = 3 * N / 4;
    g.throughput(Throughput::Elements(end - start + 1));

    let ring = filled_ring();
    g.bench_function("v1_ring", |b| {
        b.iter(|| black_box(ring.collect_range(start, end)));
    });

    let v = filled_vec();
    g.bench_function("v0_vec", |b| {
        b.iter(|| {
            let out: Vec<(u64, Bytes)> = v
                .iter()
                .filter(|(id, _)| *id >= start && *id <= end)
                .map(|(id, r)| (*id, r.clone()))
                .collect();
            black_box(out)
        });
    });
    g.finish();
}

fn bench_seek(c: &mut Criterion) {
    let mut g = c.benchmark_group("ring_seek");
    g.warm_up_time(warm());
    g.measurement_time(meas());
    let target = N / 2;

    let ring = filled_ring();
    g.bench_function("v1_ring", |b| {
        b.iter(|| black_box(ring.seek(target)));
    });

    let v = filled_vec();
    g.bench_function("v0_vec", |b| {
        b.iter(|| black_box(v.iter().position(|(id, _)| *id == target)));
    });
    g.finish();
}

criterion_group!(benches, bench_append, bench_collect_range, bench_seek);
criterion_main!(benches);
