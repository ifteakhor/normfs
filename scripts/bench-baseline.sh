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
set -euo pipefail

REF="${1:-v0.1.0-beta.1}"
ROOT="$(git -C "$(dirname "$0")/.." rev-parse --show-toplevel)"
WORKTREE="${TMPDIR:-/tmp}/normfs-baseline-${REF//\//-}"

cd "$ROOT"

if [ ! -d "$WORKTREE" ]; then
    git worktree add --detach "$WORKTREE" "$REF"
fi
mkdir -p "$WORKTREE/normfs-wal/examples"
cp normfs-wal/benches/wal_sweep.rs "$WORKTREE/normfs-wal/examples/wal_sweep.rs"

echo "=== baseline: $REF ==="
(cd "$WORKTREE" && cargo run --release -q -p normfs-wal --example wal_sweep)

echo
echo "=== this tree: $(git rev-parse --short HEAD) ==="
cargo bench -q -p normfs-wal --bench wal_sweep

echo
echo "Baseline worktree left at $WORKTREE; remove it with:"
echo "  git worktree remove --force $WORKTREE"
