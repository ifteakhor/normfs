# Store-migration diagnosis on Mac

This experiment tests whether the store-migration gap is primarily associated
with extra publication barriers, and measures where the FS implementation
spends time. Production sources and their durability policy are unchanged.
Instrumented Rust copies live under `target/fs-diagnosis`; C sources are not
modified by the instrumentation.

## Results — September 25, 2026

Two reruns of the same 36-run protocol on the current checkout, after the
reader's read-ahead, the provisioned-directory cache and the shared directory
sync landed in `normfs-fs`. The `dev` binary is unchanged.

[First rerun](results-2026-09-25/): `Fs::mkdir_all` calls fell from 768 to 2
per run and FS submissions from 1920 to 1154, but one-queue FS throughput fell
from 138.72 to 122.53 MiB/s while every timed operation got cheaper (open
2.66 → 0.10 ms, file sync 11.50 → 4.04 ms, directory sync 7.56 → 3.75 ms) and
20 ms per file moved outside every timer. That is the signature of the four
workers taking turns. The cause is #37's `DiskUsage::publish`, merged the
morning of the 25th and absent from the September 24 build: it held a
per-queue `tokio::sync::Mutex` across the whole publication, so one queue's
landings ran one at a time and never overlapped at the directory barrier.

[Second rerun](results-2026-09-25-shared-lock/): publications hold the shared
side of a per-queue `RwLock` and account through an atomic; rescans and
evictions hold the exclusive side, which keeps the invariant the mutex was
for. Twelve-queue numbers are unchanged, since twelve queues publish into
twelve directories and four workers rarely meet in one.

| Median throughput (MiB/s) | 1 queue | 12 queues |
|---|---:|---:|
| dev, profiled | 245.55 | 243.50 |
| dev + directory sync, profiled | 137.07 | 131.33 |
| FS, profiled | 149.92 | 132.71 |
| dev, timers disabled | 241.67 | 244.60 |
| dev + directory sync, timers disabled | 138.50 | 127.49 |
| FS, timers disabled | 148.20 | 127.00 |

One-queue FS is 22% above the serialized first rerun and 8% above September 24,
and for the first time above dev with the matching barrier in both control
conditions (ranges 143–153 against 134–139). Directory sync per file rose to
10.57 ms because that timer now includes waiting for a sync another worker
runs; file processing fell from 32.51 to 26.62 ms per file. The dev variants
moved by up to 10% between the three series on their own, so the FS-versus-dev
ordering is this host's, not a universal one.

## Results — September 24, 2026

[Chart](results-2026-09-24/fs-diagnosis.png) ·
[raw runs](results-2026-09-24/results.jsonl) ·
[summary](results-2026-09-24/summary.json) ·
[metadata](results-2026-09-24/metadata.json).

All **36 runs passed**: 24 with profiling and 12 with timers disabled.
Each output was validated by the migration harness. Profile counts confirm
384 file-sync operations for every profiled run and 384 directory-sync
operations for each stronger variant. The probe's concurrent-counting test
passed, both release builds passed their locked checks, and the generated
chart was visually inspected. The four FS C implementation files were checked
byte-for-byte against the originals and are unchanged.

| Median throughput (MiB/s) | 1 queue | 12 queues |
|---|---:|---:|
| dev, profiled | 223.18 | 222.01 |
| dev + directory sync, profiled | 118.59 | 125.52 |
| FS, profiled | 138.72 | 133.60 |
| dev, timers disabled | 226.75 | 227.93 |
| dev + directory sync, timers disabled | 131.30 | 129.22 |
| FS, timers disabled | 141.21 | 139.10 |

Adding the publication-directory barrier to dev reproduces a large slowdown
without introducing the FS planner or pool. FS no longer has the throughput
disadvantage when comparing the two stronger variants in this workload.
Controls show the same ordering. Run ranges overlap for some comparisons;
the measurements do not establish a universal FS speedup.

| FS median stage time (summed ms per published file) | 1 queue | 12 queues |
|---|---:|---:|
| Admission wait | 0.001 | 0.001 |
| Queue wait, all FS jobs | 0.284 | 0.157 |
| Open temporary file | 2.659 | 2.680 |
| Write | 0.657 | 0.652 |
| File sync | 11.503 | 12.040 |
| Rename | 0.903 | 0.877 |
| Directory sync | 7.560 | 8.245 |
| Entire store-file processing (overlaps the above) | 28.764 | 29.932 |

Sync operations dominate these recorded FS stage times; admission and queue
waits are small. Directory synchronization can also change the cost of other
operations: dev's async file-sync timing increases from about 8.4 to 12.1 ms
per file with the extra barrier. The effect is not simply the duration of
one isolated extra syscall. This experiment supports publication barriers as
the main explanation for the throughput difference here, rather than pool
saturation. It does not assign an exact fraction of the earlier 44% gap:
this run preprovisions directories and host conditions vary between runs.

An optimization experiment should next test batching directory barriers
while withholding publication acknowledgement until the covering barrier
completes. That would require a protocol/cancellation review and corresponding
proof work; no such production change is part of this diagnosis. Increasing
the worker count is not supported by the measured queue delays.

## Controlled comparison

Three variants use the existing 384-file migration harness with four workers,
1 MiB payloads, and either one or twelve queues:

- **dev:** the original publication sequence from `e5895b1`.
- **dev + directory sync:** the same binary, additionally opening and syncing
  the destination directory in a Tokio blocking task after each rename and
  before publication notification. Errors propagate and fail the benchmark.
- **FS:** the production publication sequence, including its directory barrier.

Both versions precreate all destination directories and the temporary-file
directory, sync the directories from children to parents, and sync the root's
parent **before** the migration timer starts. This removes first-use directory
creation as a confounder. The two stronger variants have matching file and
destination-directory publication barriers for this successful, preprovisioned
workload; this does not claim that their cancellation, failure handling or
crash guarantees are identical in general. No FS barrier is disabled.

The dev intervention uses `std::fs::File::sync_all()` on the open directory;
FS uses its existing syscall shim. The barrier includes opening and closing
that directory. The test is macOS-specific and is not a hardware power-cut test.

Four rounds enable timers; two interleaved control rounds disable them. Each
round rotates/reverses version order and alternates queue-count order. Runs are
serial, preceded by `sync` and three seconds of settling time. Builds and tests
finish before measurements; unrelated host activity remains uncontrolled.
The disabled controls still contain the instrumentation's enabled checks;
they are not pristine production binaries, and two samples cannot quantify
a small profiling overhead reliably.

## Timing boundaries

Only migration is inside the probe window. WAL preparation, directory
provisioning, and post-run signature verification are outside it. Worker
shutdown, range bookkeeping and WAL deletion are inside. CPU counters cover
the whole process, including preparation and verification.

FS measurements distinguish admission wait, time from admission to worker
dispatch, and each publication planner operation. Queue timing covers **all**
FS jobs during migration. Operation timing covers publication plans only,
including syscall work and the immediate planner transition; it is not a
kernel trace. Directory-creation timings cover the existing-directory fast
path in this experiment. Read-WAL and process timers are outer scopes.

Dev write, file-sync and rename timers surround the corresponding async
operations, so they include Tokio scheduling and any buffered-I/O completion.
They must not be interpreted as syscall-only measurements or subtracted
directly from the FS operation timers. The added dev directory-sync timer
runs inside its blocking closure; its queue delay is recorded separately.

All stage values are summed elapsed time across concurrent workers. Dividing
by 384 produces aggregate milliseconds per published file, **not wall-clock
shares**. Outer process/read timers overlap inner timings; do not sum all
reported fields. The counter implementation uses atomic accumulators without
per-operation logging or allocation.

## Reproduce in this checkout

The preparation script requires the existing untracked benchmark driver,
publication/WAL harnesses, saved dev driver lockfile and API adaptation patch
from the September 21 comparison. It makes fresh copies without modifying
those inputs or any other checkout. The copied driver lockfiles acquire only
the local probe dependency; both release builds are then checked with
`--locked --offline`.

```sh
cargo test --offline --locked --manifest-path benchmarks/fs-diagnosis/probe/Cargo.toml
python3 scripts/diagnose-fs.py prepare
python3 scripts/diagnose-fs.py measure
python3 scripts/plot-fs-diagnosis.py benchmarks/fs-diagnosis/results-2026-09-24
```

Preparation and results directories must be new. Plotting needs Matplotlib.
The probe test verifies concurrent counting, resetting between windows and
disabled counters. Every measured migration must validate all outputs, report
384 completed processes, and (for each stronger variant) 384 directory syncs.
The runner fails on a subprocess error or missing/duplicate result record.
