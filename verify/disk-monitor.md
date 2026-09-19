# Disk monitor: proof scope

## Assumed kernel behavior

WP verifies `disk_monitor.c` against the contracts in `disk_monitor_sys.h`.
The kernel and the syscall shim bodies are outside the proof.

`NormfsDiskKernel` assumes that a successful regular-file probe returns the
exact length and that successful unlink removes an existing name. For a
regular file, unlink subtracts that length from `disk_fs_bytes`. Syscalls may
fail; their contracts do not require eventual success.

The model excludes external namespace and file-size changes between probing
and unlinking, including replacement of the file or its parent directories.
The existing pathname-based TOCTOU remains. Bytes denote regular-file lengths
summed over directory entries, not allocated disk blocks. Hard links and open
descriptors can keep data alive after unlink; the native shim test checks this
boundary as well as name disappearance and repeated-unlink failure.

## Proved properties

For `evict_one`, the successful-unlink branch establishes pathname absence.
A returned successful deletion decreases modeled filesystem bytes by the
measured file size and sets `to_free` to `max(0, old_to_free - file_size)`.
An error or stop leaves `to_free` unchanged.

For `evict`, `to_free` never increases. A reported successful deletion of a
positive-size file strictly decreases it; returning no events preserves it.
`event_append` preserves earlier events' validity, sizes, and deletion results.

## Limits

There is no batch-level proof that filesystem bytes decrease by the sum of
reported deletions: unlink's world assignment does not preserve facts about
other paths. The proved batch progress property concerns `to_free`.

Convergence of the running server to the target is not proved. It requires
accurate accounting, enough eligible files, successful removals exceeding new
arrivals, eventual offload completion, and continued monitor execution. Gaps,
protected files, permanent errors, or continuous writes can prevent progress;
zero-size deletions do not reduce the deficit. WP does not verify directory-scan
completeness, Rust synchronization, scheduler fairness, or concurrent WAL writes.
