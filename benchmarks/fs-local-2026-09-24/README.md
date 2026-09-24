# Current-checkout FS measurements on Mac

Fresh measurements on 2026-09-24, using the existing standalone driver in
`benchmarks/fs-driver`. The release build passed with `--locked --offline`.
The driver links the libraries as normal dependencies, avoiding the WAL
example's fault-injection dev feature. No production code was changed for
this run. Existing uncommitted harnesses and WAL proof edits were retained;
they are not included in this results commit.

See [the chart](fs-results.png), [raw results](results.jsonl),
[summary](summary.json), and [metadata and binary hashes](metadata.json).
The metadata records the pre-run revision and working-tree state. This is
the current checkout only, not a fresh comparison against dev. The host now
runs macOS 27.0; the earlier September 21 results used macOS 26.0.1.

All 28 runs completed successfully, including 6,000 measured commit latencies.
Raw records were checked for four distinct rounds per case, correct CPU
arithmetic, and agreement with every plotted median and range. The PNG was
visually inspected for readable labels and layout.

Median WAL write rates were 327.50 MiB/s (80 B) and 1,174.03 MiB/s (4 KiB).
Commit p99 without readers was 4.83 ms; store migration was 145.96–146.51
MiB/s. Whole-process CPU ranged from 4.89% of one core in the no-reader commit
case to 822.38% with twelve cached readers. These are different workloads,
not a demonstration of an optimal CPU/throughput tradeoff.

Four serial process runs cover each of seven cases, reversing case order
on alternate rounds. Each process is preceded by `sync` and a three-second
pause. Agent builds and tests were finished before measurement. Other host
activity is not controlled. Bars are medians; whiskers show the full range,
not confidence intervals.

- WAL: 10,000,000 records of 80 B or 1,048,576 records of 4 KiB, a 1 MiB
  write buffer, sync enabled, compression/encryption disabled. Write timing
  includes close and final acknowledgement. One validating scan precedes
  five timed scans. Datasets fit in memory; scans are cached.
- Latency: 500 measured commits after 20 warmup commits, one outstanding
  4 KiB record, with zero, four or twelve concurrent readers scanning a
  64 MiB WAL. The 4 KiB write buffer triggers immediate flushes.
- Store: 384 prebuilt WAL files of 1 MiB payload, four migration workers,
  one or twelve queues. Timing includes worker shutdown, range bookkeeping
  and WAL deletion. Setup and subsequent signature validation are outside
  the migration timer.
- CPU: deltas of `getrusage(RUSAGE_CHILDREN)` immediately around each child
  process, including setup, warmup, scans, validation and cleanup. Utilization
  is `(user + system CPU seconds) / process wall seconds * 100`; 100% means
  one logical core, not the whole machine. CPU cannot be attributed to the
  individual write, scan or migration phase from these measurements.

The harness checks recovered WAL entry IDs, acknowledgement deadlines and
store completion/output validity. A nonzero child exit aborts the runner;
partial results are not charted as a complete run. Neither physical disk
saturation nor a minimum CPU cost follows from this experiment. Those still
need a matched storage baseline, sustained workloads and configuration sweeps.

To repeat in this checkout, with Matplotlib installed in the chosen Python
environment and the existing driver/harness files present:

```sh
cargo build --release --locked --offline --manifest-path benchmarks/fs-driver/Cargo.toml
python3 scripts/measure-fs-local.py /tmp/fs-local-new
```

The output directory must not already exist. To regenerate a chart from
completed measurements, use `--plot-only` with the existing output directory.
