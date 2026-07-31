#!/usr/bin/env bash
# Run wal_sweep against a released baseline and against this tree, so the two
# can be compared on one machine.
#
#   scripts/bench-baseline.sh [baseline-ref]     # default: v0.1.0-beta.1
#
# The baseline is checked out into a git worktree and the sweep is dropped into
# its examples/ directory: cargo discovers examples without a manifest entry, so
# an old tree that predates this benchmark still runs it unmodified. That works
# because wal_sweep.rs uses only WAL API the baseline already has.
#
# One record size per process, and each size measured in both orders. Both parts
# matter, and for the same reason: a run inherits whatever writeback backlog the
# run before it left behind, and a large-record write is bandwidth-bound enough
# to notice. Sweeping all seven sizes in one process put the 12 KiB write 24 %
# low and penalised the *faster* build hardest, since that one reaches the late
# points sooner and leaves writeback less time to drain. Fixing that but always
# measuring the baseline first just moves the same bias onto whichever build
# goes second, so each size is run baseline-first and branch-first.
#
# Even so, expect the two orders to disagree at 4 KiB and above: on a laptop
# that comparison is writeback, not code.
set -euo pipefail

REF="${1:-v0.1.0-beta.1}"
ROOT="$(git -C "$(dirname "$0")/.." rev-parse --show-toplevel)"
WORKTREE="${TMPDIR:-/tmp}/normfs-baseline-${REF//\//-}"
SIZES=(16 64 80 256 1024 4096 12288)

# Long enough for the previous size's writeback to drain before the next run is
# timed.
SETTLE=5

cd "$ROOT"

if [ ! -d "$WORKTREE" ]; then
    git worktree add --detach "$WORKTREE" "$REF"
fi
mkdir -p "$WORKTREE/normfs-wal/examples"
cp normfs-wal/benches/wal_sweep.rs "$WORKTREE/normfs-wal/examples/wal_sweep.rs"

# Built up front so no compile lands inside a measurement.
(cd "$WORKTREE" && cargo build --release -q -p normfs-wal --example wal_sweep)
cargo build --release -q -p normfs-wal --bench wal_sweep

HEAD_SHORT="$(git rev-parse --short HEAD)"

run_baseline() {
    sync; sleep "$SETTLE"
    echo "-- baseline $REF"
    (cd "$WORKTREE" && cargo run --release -q -p normfs-wal --example wal_sweep -- "$1")
}

run_branch() {
    sync; sleep "$SETTLE"
    echo "-- this tree $HEAD_SHORT"
    cargo bench -q -p normfs-wal --bench wal_sweep -- "$1"
}

for size in "${SIZES[@]}"; do
    echo "======== record size: ${size} B, baseline first ========"
    run_baseline "$size"
    run_branch "$size"
    echo "======== record size: ${size} B, branch first ========"
    run_branch "$size"
    run_baseline "$size"
    echo
done

echo "Baseline worktree left at $WORKTREE; remove it with:"
echo "  git worktree remove --force $WORKTREE"
