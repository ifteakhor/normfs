# Filesystem layer versus raw I/O

This standalone harness compares direct file operations with the public
`normfs-fs` API. It does not instantiate the NormFS application or depend on
`normfs`, `normfs-wal` or `normfs-store`. There are no WAL_STORE/STORE modes,
codecs, encryption, queue scheduling or application flush loops in these runs.
The red baseline is **Raw I/O**, not a measurement of the `dev` branch.

## Measurements

The [PNG](results-2026-09-24/raw-io-vs-fs.png),
[median table](results-2026-09-24/table.md),
[individual runs](results-2026-09-24/runs.jsonl), and
[host/run metadata](results-2026-09-24/metadata.json) describe the local Mac run.
Throughput is better when higher; CPU seconds per GiB and latency are better
when lower. Error bars show the observed minimum and maximum, not confidence
intervals. Each p99 is computed within one run; the chart reports the median of
those per-run p99s, not a percentile pooled across runs.

The 88 runs completed with verification enabled. In these cases, write
throughput medians range from 7.9% below to 1.3% above Raw I/O. Four-MiB append
with one client is essentially equal (820.22 versus 820.69 MiB/s); eight clients
give 1524.59 versus 1403.64 MiB/s. Four-MiB publication differs by -3.0% with one
client and +0.6% with eight. These small publication differences are within a
large observed spread: both implementations slowed in later rounds. They do
not establish an FS speed advantage or a stable sustained rate.

CPU savings are not general: 32 KiB append uses 3.236 versus 4.501 CPU s/GiB
(about 39% more for FS), while 4 MiB publication has slightly lower FS medians
with overlapping ranges. Cached reads expose larger overhead: FS throughput is
83.6% lower at 32 KiB, and 23.0–29.5% lower at 4 MiB. For the 32 KiB reader, CPU
cost is about 8.8 times the direct baseline. Dispatch through the pool and the
public reader's additional copy are plausible contributors visible in the code;
this benchmark does not profile or separate their individual costs.

The FS layer's bounded scheduling and durability protocols may justify its
cost, but these measurements do not show a general speedup or minimum CPU
usage. They also do not measure those architectural benefits under overload
or failure. The clearest follow-up performance target is the cached read path.

## Workloads and comparison boundary

| Operation | Raw I/O | FS layer | Timed durability barriers |
|---|---|---|---|
| append | `FileExt::write_all_at` | `append_sync_with_inode` | File sync after every block |
| publish | Create exclusive temporary file, write, close, rename | `Fs::publish` | File sync before rename, parent-directory sync after rename |
| read | `FileExt::read_exact_at` | `Fs::open_read` / `ReadFile` | None; fixture creation and sync are outside timing |

Raw I/O uses one dedicated blocking thread per client. The FS layer uses the
default bounded worker pool (eight threads on this host) and one async task per
client. Both run in a process with the same eight-thread Tokio runtime. The raw
workers do not use Tokio for their I/O. Setup creates directories, opens append
files, obtains inodes, and synchronizes fixture paths before the timed barrier.
Raw publication includes the inode and destination metadata checks performed by
the FS publisher. A raw write uses `pwrite`; the FS layer uses a single-vector
`pwritev`. This measures the cost of the real public API, including its admission,
planning, allocation, dispatch and completion work, rather than an identical
sequence of instructions around a syscall.

Both paths request the same sync boundaries. On this Mac, Rust `sync_all` and
the FS syscall shim use `F_FULLFSYNC` with an `fsync` fallback. Publication is a
filesystem protocol here, not a Store migration. Buffered/no-sync writes,
multiple buffers per write, error recovery and cancellation are outside this
measurement.

Reads cycle sequentially through a freshly written 64 MiB fixture per client;
the fixtures fit in RAM. Each read run transfers 4 GiB logically. Both paths cap
each read syscall at 1 MiB. The FS public reader additionally dispatches work
and copies its internal buffer into the caller's buffer. **These are cached
reads, not physical SSD bandwidth or cold-cache measurements.** Some raw read
runs last less than a second, so their small differences should not be treated
as precise sustained-throughput results.

Every backend receives identical deterministic buffers for a paired case.
There are sixteen distinct buffers, reused cyclically. The 32 KiB cases use
synthetic structured sensor text; the 1 MiB and 4 MiB cases use xorshift-generated
entropy. Data generation is outside the timer. These are sizing/payload probes,
not captured production traffic or a study separating payload entropy from size.

Four rounds run serially with alternating backend order and reversed case order
every other round. A system sync and two-second pause precede every process.
No builds or tests run concurrently with the measurements. Process user+system
CPU comes from `getrusage(RUSAGE_SELF)` over the same timed workers as wall time;
setup, verification, result formatting and cleanup are excluded. CPU seconds
per GiB normalizes CPU work by bytes. CPU percentage in the raw results can
exceed 100% because multiple cores may run; lower percentage alone does not
establish greater efficiency. Kernel work outside this process is not counted.

All appended and published bytes, output lengths and absence of temporary files
are verified after timing. For reads, the last returned block of each client is
verified. Operation counts and latency sample counts must agree. The runner
checks the reported p50/p99 against the sorted latency samples before retaining
the per-run aggregates; individual latency samples are not archived.

## Why these block sizes

Read-only inspection of local `norma-core` revision
`a2e81d20f962374fcc0edbc89a9b377c39ab4d01`,
`software/station/bin/station/src/main.rs`, found:

- `ACTIVE_PAGE_SIZE = 4 * 1024 * 1024` at line 61, passed as `mem_page_size`
  at line 332; active is also the queue-rule fallback.
- The queue-settings description at line 210 specifies passive pages of 32 KiB.
- Rules mix sync enabled and disabled, compression modes, and encryption.

The test therefore covers 32 KiB and 4 MiB plus an intermediate 1 MiB size,
with one or eight independent clients. It deliberately omits the application
policies: sync is enabled for every measured write. The station uses a published
NormFS dependency and is not the application being executed by this harness.

## Reproduction

From the repository root, with Cargo on PATH and Matplotlib installed for Python:

```sh
cargo build --release --locked --manifest-path benchmarks/fs-io/Cargo.toml
python3 scripts/benchmark-fs-io.py /tmp/fs-io-results
python3 scripts/plot-fs-io.py /tmp/fs-io-results
```

The output directory must not exist. The binary can also run individual cases:

```sh
benchmarks/fs-io/target/release/fs_io raw append 4194304 1 128 entropy
benchmarks/fs-io/target/release/fs_io fs append 4194304 1 128 entropy
```

This benchmark cannot establish maximum disk bandwidth or minimum possible CPU
usage. It measures overhead relative to a direct blocking baseline for these
filesystem protocols, data sizes, concurrency levels and this host.
