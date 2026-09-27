# Measurements with a minimum I/O duration

**Latest results:** [after read-ahead](READ-AHEAD.md), same protocol, same day.

The September 24 fixed-volume runs could finish cached reads in a fraction of a
second and uncached SSD reads in roughly 7–15 seconds. A fixed byte count was
not an adequate duration requirement. The current binaries and runners require
at least 30 seconds; this run requests **35 seconds per worker per phase**.
Elapsed time is spent executing operations, not padded with a sleep. Setup,
verification, output formatting and cleanup remain outside the measured phase.

The fresh September 25 run started after the user allowed benchmarks to resume.

The six rows in `results-2026-09-25/` belong to the interrupted attempt and are
excluded from these summaries. Its temporary payload was identified by directory
layout and deterministic content before cleanup.

Artifacts:

- FS layer versus Raw I/O: [chart](results-2026-09-25-35s/raw-io-vs-fs.png),
  [table](results-2026-09-25-35s/table.md), [runs](results-2026-09-25-35s/runs.jsonl),
  [metadata](results-2026-09-25-35s/metadata.json),
  [per-run durations](results-2026-09-25-35s/durations.md) and
  [duration audit](results-2026-09-25-35s/duration-audit.json).
- Uncached SSD: [chart](ssd-results-2026-09-25-35s/ssd-throughput.png),
  [table](ssd-results-2026-09-25-35s/table.md), [runs](ssd-results-2026-09-25-35s/runs.jsonl),
  [metadata](ssd-results-2026-09-25-35s/metadata.json),
  [per-phase durations](ssd-results-2026-09-25-35s/durations.md) and
  [duration audit](ssd-results-2026-09-25-35s/duration-audit.json).

## Results

All **66 FS runs and 30 SSD phases** passed the duration and byte-accounting
audit. The shortest worker interval was 35.000000583 seconds for FS and
35.000027708 seconds for SSD. FS wall times were 35.00–35.52 seconds; SSD
wall times were 35.00–76.64 seconds.

The FS layer does not demonstrate a general throughput or CPU advantage over
Raw I/O here. Median write throughput changes range from −14.4% to +5.5%, with
overlapping min–max ranges in every write configuration. Three repetitions
do not establish a small difference as a reliable gain. Cached reads show a
clearer cost: FS throughput is 84.9% lower for 32 KiB/one client, 34.8% lower
for 4 MiB/one client and 31.3% lower for 4 MiB/eight clients. CPU seconds/GiB
also increase for these reads. This measures the overhead of the public async
interface, including worker dispatch and buffering; it does not isolate the
individual causes or assess the value of its correctness guarantees.

The highest SSD **median read** rate is **5.471 GB/s** at 4 MiB/eight clients
(4.695–5.639 GB/s across runs). Sixteen and thirty-two clients are slower and
use more CPU per GiB. The highest **median write** rate is **0.992 GB/s** at
1 MiB/one client, but its 0.485–1.077 GB/s range is wide. These are observed
rates on this APFS volume, not proof of a hardware maximum or minimum CPU cost.

Measured write traffic totals **903.09 GB**: 381.71 GB for FS comparisons and
521.38 GB for SSD. SSD measured reads total **2.204 TB**. FS reads account for
**21.161 TB of logical cached traffic**, not physical SSD reads. These totals
exclude fixture preparation, verification reads, and filesystem/device write
amplification. The write-footprint limits are 64 GiB for FS and 32 GiB for SSD;
the suites ran serially and their temporary payloads were removed on completion.

Verification included the locked offline release build, Rust formatting,
short functional runs of both FS backends and SSD wrapping, rejection of
sub-30-second measurements, resume validation, and the complete duration
audits. Source/binary hashes match the measured artifacts. Both charts were
rendered and visually inspected. Production Rust/C code was unchanged.

## Stop rule and accounting

Each worker starts its own monotonic timer after the start barrier and continues
issuing operations until its active interval reaches the requested duration.
The process records the actual completed operation count and active seconds of
every worker. The runner rejects any worker below the threshold, any wall time
below the threshold, or any byte count inconsistent with completed operations.
Throughput and CPU seconds/GiB use **actual transferred bytes**, which vary
between backends, workers and repetitions. No longer is 4 GiB assumed for reads.
Wall time includes the final operation and thread/task completion; SSD write
wall time also includes the final full flush. The worker duration requirement
must already be met before that flush.

The explicit `--smoke` binary flag permits short functional checks and marks
their results. Measurement runners and duration-aware plots reject such rows.
The regular CLI rejects durations below 30 seconds. All phases run serially;
builds, functional checks and chart rendering are performed outside measurement.
Ordinary host activity is not controlled.

## FS comparison

The same eleven configurations cover append, publication and cached reads with
32 KiB, 1 MiB and 4 MiB blocks and one/eight clients. Three rounds alternate the
backend order and reverse case order on the second round. Raw I/O is red and
the FS layer blue. The NormFS application, WAL and Store are not executed.

Write durability is unchanged: file sync after every append; file sync, rename
and parent-directory sync for every publication. Append files grow and each
publication creates a new file; writes do not wrap and overwrite earlier data.
Each process has a 64 GiB write-footprint cap, divided equally among workers.
Reaching the cap aborts instead of accepting a short run. The runner requires
that cap plus a configurable free reserve (16 GiB by default) on the temporary-file volume. Publication
paths are now constructed as operations are issued on both backends, instead
of precomputing a fixed number of names before timing.

After 45 completed runs, a free-space preflight stopped the series before the
next process started. Free-space readings varied around 79–81 GiB; a retry
with the original 80 GiB threshold also stopped before starting another run.
The remaining runs use a 12 GiB reserve (76 GiB required in total), while the
64 GiB payload cap and benchmark binary remain unchanged. The runner now checks
after its sync/settling pause and retries briefly. `--resume` validates the saved rows,
revision, binary/source hashes and settings before appending missing cases.
The pause and reserve change are recorded in metadata and are outside every
measured interval. No completed measurement is replaced or shortened.

The cached read fixture remains 64 MiB per client. Workers repeatedly read it
for at least 35 seconds, so logical traffic can exceed RAM many times. **Longer
runtime does not turn this into an SSD or cold-cache test.** Every read syscall
still has the same one-MiB cap in both backends. The public FS reader retains its
additional worker dispatch and copy into the caller's buffer.

All write-operation latencies are retained; cached reads sample every 64th
operation per worker to avoid allocating and serializing millions of samples.
Read p99 is therefore a sampled percentile. Each result records sample stride
and count, and the runner checks p50/p99 against those samples. The chart takes
the median of per-run p99s, not a pooled percentile. Full write contents and
lengths are verified after timing; reads verify each client's last block.

Read latency sampling is periodic, not random: the 64-operation stride selects
a fixed subset of fixture offsets (only the first block for 4 MiB requests).
Its p99 describes that subset, not an unbiased estimate across all read offsets.
Throughput and CPU accounting include every completed operation.

## SSD comparison

Each case still uses a 32 GiB fixture, larger than this Mac's 24 GiB RAM, with
`F_NOCACHE` enabled on every file descriptor. Every worker completes at least
one full pass through its share of the fixture **and** at least 35 seconds of
I/O. Offsets wrap at the end of each worker's file for additional passes, so
the disk footprint stays bounded while transferred bytes keep increasing.
Long writes can exceed 35 seconds just to complete their first pass.

Write and read phases independently satisfy the duration rule. Write performs
`F_FULLFSYNC` at the end of each file, inside wall time, with no fallback on
failure. First/middle/last written blocks and file lengths are checked outside
timing; reads check each worker's final block, accounting for wrapped offsets.
Device caches and filesystem metadata caching are not disabled.

All five configurations (1 MiB/one client; 4 MiB/one, eight, sixteen and
thirty-two clients) now appear in each of three rounds, rather than running
16/32 clients in a later separate sweep. The second round reverses their order.
Write precedes read within a case, and a sync plus three-second pause precedes
the next case. Results report measured APFS throughput, not an absolute hardware
ceiling. These bulk rates still use a different sync frequency from the FS test.

## Reproduction

On macOS, from the repository root:

```sh
cargo build --release --locked --manifest-path benchmarks/fs-io/Cargo.toml --features macos-ssd
python3 scripts/benchmark-fs-io.py /tmp/fs-duration-results --minimum-seconds 35 --rounds 3 --reserve-gib 12
python3 scripts/audit-io-duration.py /tmp/fs-duration-results
python3 scripts/plot-fs-io.py /tmp/fs-duration-results
python3 scripts/benchmark-ssd.py /tmp/ssd-duration-results --minimum-seconds 35
python3 scripts/audit-io-duration.py /tmp/ssd-duration-results
python3 scripts/plot-ssd.py /tmp/ssd-duration-results
```

Output directories must not already exist. Matplotlib is required for charts.
Use the same filesystem for the temporary data and intended storage measurement.
The complete matrix includes 66 FS runs and 30 SSD phases, each at least 35
seconds, plus setup and verification. Historical September 24 artifacts remain
unchanged; use their recorded revisions to reproduce their fixed-work protocol.
