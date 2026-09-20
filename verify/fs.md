# fs planner: proof scope

## Assumed kernel behavior

WP verifies `normfs-fs/c/src/fs_plan.c` against the contracts in
`normfs/fs_sys.h`. The kernel, the executors and the shim bodies are outside
the proof.

`NormfsFsKernel` is four ghost worlds: the names and the file lengths the
page cache holds, and the names and the synced byte prefixes that would
survive a power cut. The completion functions `normfs_fs_world_*` state what
each finished operation lets the planner conclude: a write grows the cached
length and moves nothing durable; a file fsync makes the cached length the
synced prefix; a rename is atomic in the namespace and moves nothing durable;
a directory fsync makes the durable entry for one name equal the cached one;
ftruncate cuts the cached length and never grows the synced prefix. A failed
write or fsync leaves the synced prefix where it was and the cached length
unknown; a failed directory fsync leaves the durable entry either where it
was or where the cache has it.

Two assumptions carry the model to an executor. An executor reports a step
only after the operation it names has completed with the result it names,
and in the order the planner hands steps out. And when an executor is
configured without fsync it reports the fsync steps done without doing them,
so nothing below applies to it.

The model excludes changes to a plan's paths by anything but the plan
between two of its steps, including replacement of a file or its parent
directory. ext4's `data=ordered` is not assumed: bytes past the synced prefix
are not claimed until the next fsync. Hard links and open descriptors are
not modelled.

## Proved properties

For PUBLISH, every apply step preserves `NORMFS_FS_PUBLISH_STATE` and
satisfies `NORMFS_FS_PUBLISH_STEP`: the durable entry for the target name is
what it was before the step, or the new inode with all of its bytes synced.
Over any sequence of reports from `publish_init`, the durable entry is
therefore the old file, no file, or the whole new file, and at DONE it is
the whole new file. `normfs_fs_publish_done_durable` states that conclusion.
Removing the file fsync from the transition table leaves the synced-prefix
conjunct of the DONE state unprovable; removing the directory fsync leaves
the durable-entry conjunct unprovable.

For APPEND, from a file synced and unwritten past `at`, every step leaves
the synced prefix at `at` or at `at + total`, never between, and the plan is
at DONE exactly when it is the latter. A failed write or fsync is followed by
a truncate back to `at`, after which the cached length is `at` again. That
biconditional is the licence for `PagePool::mark_durable`: the WAL advances
its watermark on DONE and on nothing else. `normfs_fs_append_boundary_holds`
states it.

For CREATE, DONE means the name durably resolves to the created inode with
all of its bytes synced. For REMOVE, DONE means the name is durably absent,
whether the unlink removed it or found it already gone.

The precondition of `append_init` is the caller's obligation: the file is
synced and unwritten past `at`. A finished APPEND leaves the file so at
`at + total`; a restored failure leaves it so at `at`; an unrestored failure
is followed by a RESTORE plan, whose DONE re-establishes it at `at`. The WAL
writer's `flushed_len` is that `at`.

## Limits

The executors are not proved. The thread pool performs each step through a
shim and reports its result; the shim contracts carry errno only, and
`tests/test_fs.c` checks them. An io_uring executor would perform the same
steps from completions, and the order of its completions is its own
obligation.

Directory scans, whole-file reads and the accounting closure a publish runs
after DONE are ordinary Rust on the executor's threads and are not modelled.

A plan is stated for one target name. Nothing here relates two plans, so a
rename over a file another plan is publishing, or a scan racing an eviction,
is outside the proof, as it is outside the disk monitor's.
