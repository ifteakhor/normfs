# FS performance: what the Mac measurements establish

The available measurements do **not** establish either maximum physical disk
throughput or minimum CPU consumption. They show roughly unchanged WAL
performance versus dev, with slower store migration under stronger directory
durability guarantees.

This assessment uses the existing local
`benchmarks/fs-comparison-2026-09-21/{metadata.json,raw/results.jsonl,publication/results.jsonl}`
artifacts, comparing dev `e5895b1` with FS revision `222c47d`. Those artifacts
were untracked when this note was written and are not included in this
documentation commit. These are historical results, not a new benchmark run.

The recorded host was a MacBook Pro Mac16,8, 12 logical CPUs, 24 GiB RAM,
macOS 26.0.1, internal APFS storage. Release builds used eight Tokio workers
and, on the FS branch, eight FS workers. Values below are medians of four
process runs per case and revision; rates count logical payload bytes.

| Measurement | dev | FS branch |
|---|---:|---:|
| WAL write, 80 B records, sync enabled (MiB/s) | 286.96 | 287.09 |
| WAL write, 4 KiB records, sync enabled (MiB/s) | 678.52 | 701.31 |
| Cached WAL scan, 80 B records (MiB/s) | 4,083.29 | 4,047.44 |
| Cached WAL scan, 4 KiB records (MiB/s) | 7,866.29 | 7,628.84 |
| Commit p50, no readers (ms) | 4.017 | 4.008 |
| Commit p99, no readers (ms) | 6.004 | 6.032 |
| Store migration, one queue (MiB/s) | 197.62 | 120.31 |
| Store migration, twelve queues (MiB/s) | 200.78 | 118.06 |

WAL throughput ranges overlap. The 4 KiB write rate fell from roughly 900 to
470 MiB/s in later rounds on both revisions; these short runs do not establish
a sustained rate. Cached scans fit in RAM and do not measure physical disk
read bandwidth. There is no independent storage-ceiling measurement with
matching write sizes and sync frequency. The WAL harness also bypasses the
full `NormFS::enqueue` path and disables compression and encryption.

Store migration is 39–41% slower. The FS branch syncs the destination directory
after publication and durably creates missing directories. The earlier path
did not provide the same directory guarantees. This is a plausible contributor,
but the whole-branch comparison does not isolate its cost. On macOS the current
FS shim attempts `F_FULLFSYNC`, falling back to `fsync` on failure; the results
do not record which calls took the fallback or test power-cut survival.

The result records contain elapsed time, throughput and latency, but no user
or system CPU time, CPU profiles, or context-switch counts. In the executor,
idle workers block in `Receiver::recv`; admission waits on a semaphore and
syscalls execute off Tokio workers. This avoids a busy polling loop but does
not demonstrate minimum CPU cost under load. Eight workers is a configuration,
not a measured optimum.

To resolve the remaining questions on the same Mac, measure a storage baseline
with matching durability and batching, sustained writes and reads beyond the
cache-resident dataset, and the full application path. Record user/system CPU
seconds per GiB and CPU utilization over the same timed interval, alongside
throughput and commit p99. Sweep worker counts and batch sizes under a fixed
workload and profile the expensive cases, particularly store publication.
Keep durability guarantees fixed when attributing performance differences.

A [September 24 follow-up](../benchmarks/fs-local-2026-09-24/README.md)
adds fresh current-checkout measurements and whole-process CPU accounting on
macOS 27.0. CPU includes setup and validation, so it does not isolate the FS
executor's overhead. That run still does not establish a disk ceiling or a
minimum CPU cost.
