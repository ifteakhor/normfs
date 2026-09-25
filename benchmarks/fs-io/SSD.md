# Uncached SSD throughput on the Mac

**Current results and protocol:** [minimum-duration measurements](DURATION.md).
The historical September 24 results below used fixed transfer volumes and
allowed short reads. Current scripts require at least 30 seconds of active I/O
for every worker in both phases and default to 35 seconds.

This measures bulk filesystem throughput on the internal SSD, without NormFS,
WAL, Store or the FS layer. It complements the [FS overhead comparison](README.md),
but uses a different sync frequency: a full flush at the end of each file,
instead of a flush after each block. The two sets of throughput figures must
not be divided to estimate FS overhead.

Artifacts: [chart](ssd-results-2026-09-24/ssd-throughput.png),
[table](ssd-results-2026-09-24/table.md),
[individual runs](ssd-results-2026-09-24/runs.jsonl),
[metadata](ssd-results-2026-09-24/metadata.json),
[extended concurrency runs](ssd-results-2026-09-24/extended/runs.jsonl) and
[extended metadata](ssd-results-2026-09-24/extended/metadata.json).

## Observed rates

All fifteen write/read pairs completed with successful spot checks. The best
read median was **4.510 GB/s**, using 4 MiB blocks and eight clients (observed
range 3.878–4.564 GB/s). The best write median was **0.572 GB/s**, using 1 MiB
blocks and one client (0.531–0.734 GB/s). The fastest individual runs were
4.700 GB/s for reads with sixteen clients and 0.892 GB/s for writes with eight.
Those single-run maxima are not sustained guarantees.

Sixteen and thirty-two clients did not improve throughput medians over eight,
and increased CPU cost per GiB. The write results vary substantially across
repeats. This run does not identify the cause of that variation or establish
the drive's absolute write ceiling. It reports achieved bulk throughput for
this APFS volume, cache policy, workload and host state. In particular, it
does not measure a short cached-write burst or attribute the write/read
asymmetry to the FS layer, which is absent from this binary.

## Method

- Host: macOS 27.0, internal APPLE SSD AP0512Z, nominal 500 GB, 24 GiB RAM.
  Model/capacity come from `system_profiler SPNVMeDataType`; RAM from
  `sysctl hw.memsize`. Approximately 109 GiB was available before measurement.
- Each case writes 32 GiB of new file data and then reads all 32 GiB. One client
  uses one 32 GiB file; multiple clients split the 32 GiB equally across files.
- Every descriptor has `fcntl(F_NOCACHE, 1)` enabled before any data I/O. Failure
  aborts the run. The installed macOS `fcntl(2)` manual documents this command as
  disabling data caching. The working set also exceeds physical RAM in aggregate.
  Device-internal caches and normal filesystem metadata caching remain possible.
- Write uses blocking `write_all_at` and performs `F_FULLFSYNC` once per file at
  the end, inside the timed interval. A rejected full flush aborts rather than
  silently falling back. Read uses blocking `read_exact_at`. There is no Tokio
  runtime, application scheduler or FS executor in the benchmark binary.
- Each client has one outstanding synchronous call. Blocks are 1 MiB with one
  client and 4 MiB with one, eight, sixteen and thirty-two clients. Multiple
  clients mean concurrent sequential streams, not one contiguous global request
  sequence.
- Three rounds reverse case order on the second round. Every case starts with
  a system sync and three-second pause. Its read always follows its write and
  untimed spot checks; there is no cold-start/thermal reset between phases.
- The 16/32-client sweep follows the complete 1/8-client sweep, rather than
  interleaving all configurations. Its only Rust source change extends the
  accepted client-count list from `[1, 8]` to `[1, 8, 16, 32]`; each phase records
  its source and binary hashes. The I/O algorithm is unchanged. Host/drive state
  can drift between these phases, limiting precise cross-configuration ranking.
- Sixteen distinct deterministic xorshift-filled blocks are generated before
  timing and reused cyclically. No zero-filled, sparse or cloned source is used.
  The temporary files are on the same APFS data volume as this checkout and are
  removed after each case. The runner requires 32 GiB plus a 16 GiB free reserve.
- Worker creation and buffer initialization precede the start barrier. Wall
  time and `getrusage(RUSAGE_SELF)` CPU cover only worker I/O through completion
  and thread join, including the final write flush. They exclude preparation,
  verification, formatting and deletion. CPU outside the process is not counted.
- After writing, file sizes and the full first, middle and last blocks of every
  file are checked outside timing. After reading, each client's final block is
  checked. This is spot verification, not a complete integrity scan of all bytes.
  Every timed syscall must return successfully and supply the requested bytes.
- No builds, tests or other benchmark processes are deliberately run alongside
  the measurement. Ordinary host activity is not controlled.

GB/s is decimal (10^9 bytes/s); MiB/s and GiB use binary units. The plot reports
medians and observed min–max across three runs, not confidence intervals. A
fastest run or configuration is an observed rate, not proof of an absolute
hardware maximum. Filesystem overhead, free space, SSD cache state, temperature,
OS activity, request size and concurrency can all affect results. There is no
random-I/O/IOPS test, raw-device access, exhaustive queue-depth sweep or long
steady-state endurance workload here.

## Reproduction

Use [the current duration-controlled commands](DURATION.md#reproduction).
The historical fixed-volume protocol is available at revision `40388cc`.
