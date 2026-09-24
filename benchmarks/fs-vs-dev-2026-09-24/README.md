# FS vs dev — Mac

Fresh paired runs on September 24, 2026: **red = dev**, **blue = FS**.
[PNG](fs-results.png) · [raw records](results.jsonl) ·
[summary](summary.json) · [metadata and binary hashes](metadata.json).

Both revisions run on the same Mac under macOS 27.0. Dev is
`e5895b1f2817c63f5f49ade77c137a579b385907`; FS is the working tree at
`6d613c7` with the preexisting WAL proof edits. The metadata preserves the
working-tree status. This comparison uses new runs for **both** versions;
neither the September 21 comparison nor the earlier FS-only run is mixed in.

All **56 runs passed**, retaining **12,000 commit-latency samples**. Every
version/case has four distinct rounds. CPU arithmetic, plotted medians and
min–max ranges were independently checked against the raw records; the PNG
was visually inspected.

| Median measurement | dev | FS |
|---|---:|---:|
| WAL writes, 80 B (MiB/s) | 294.58 | 308.35 |
| WAL writes, 4 KiB (MiB/s) | 1,201.80 | 1,200.28 |
| Commit p99, no readers (ms) | 6.04 | 6.06 |
| Store migration, one queue (MiB/s) | 262.32 | 145.86 |
| Store migration, twelve queues (MiB/s) | 256.64 | 143.40 |

WAL write ranges overlap. Store migration is about 44% slower in FS. CPU
seconds do not show a consistent reduction across cases. One FS no-reader
run had p99 of 20.735 ms; one dev 4 KiB write run fell to 655.29 MiB/s, and
one FS 4 KiB scan run fell to 5,103.88 MiB/s. All are retained. These runs do
not isolate causes of the variability or establish rare-stall probabilities.

Dev was exported with `git archive` into `target/fs-dev-e5895b1` inside this
checkout. No other checkout was modified. The existing standalone driver
was copied there with its saved dev lockfile. WAL harnesses are byte-identical.
Store harness changes are only the three API adaptations in
`benchmarks/fs-comparison-2026-09-21/dev-store-api.patch`: mutable binding,
four-argument constructor, and unit-returning `close`. Both drivers built
successfully with `cargo build --release --locked --offline`. Fault injection
is disabled by using normal library dependencies. The preexisting untracked
driver, harnesses and historical artifacts are not included in this commit.

Each of seven cases runs four times per version, serially. Version order
alternates each round; case order reverses on alternate rounds. Every child
process is preceded by `sync` and a three-second pause. Builds finish before
measurements; unrelated host activity is not controlled. Chart bars show
medians, with min–max whiskers rather than confidence intervals.

Workload sizes, validation and timing boundaries match the
[FS-only run](../fs-local-2026-09-24/README.md): 80 B and 4 KiB WAL records,
five timed cached scans, 500 commit samples per latency run, and migration
of 384 one-MiB WAL payloads. The throughput harness excludes compression,
encryption and the full `NormFS::enqueue` path.

CPU is measured with child-process resource-usage deltas. It includes setup,
warmup, validation and teardown, while throughput and commit latency refer
to individual phases. WAL CPU covers writing and scanning together. The CPU
time chart compares total user plus system seconds; raw records retain both
separately. Utilization divides total CPU seconds by process wall time;
100% means one logical core.
Lower CPU utilization alone does not mean less CPU work: longer waits can
reduce that percentage even when total CPU seconds stay the same.

FS additionally syncs publication directories and durably creates missing
directories. Dev does not provide the same directory durability. The chart
compares the implementations as they are, not equal durability guarantees.
Cached scans do not measure physical disk bandwidth, and these short runs
establish neither a sustained disk ceiling nor minimum CPU cost.

To repeat with both existing builds, choose a new output directory:

```sh
python3 scripts/measure-fs-local.py /tmp/fs-vs-dev-new \
  --dev-binaries target/fs-dev-e5895b1/benchmarks/fs-driver/target/release \
  --dev-revision e5895b1f2817c63f5f49ade77c137a579b385907
```

Matplotlib is required for plotting. `--plot-only` regenerates the PNG from
a completed output directory without rerunning measurements.
