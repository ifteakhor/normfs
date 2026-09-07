#!/usr/bin/env bash
# Sweep listener counts in both modes, two rounds with the order reversed in
# the second, and collect one RESULT line per run into a CSV-ish log.
#
# Usage: ./listeners-sweep.sh <repo-dir> <out-file> [label]
set -u
REPO=${1:?repo dir}
OUT=${2:?output file}
LABEL=${3:-$(hostname)}
COUNTS="1 2 4 8 16 32 64 128 256 512 1000"
export CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-$REPO/target}
export NORMFS_PUBLISHES=${NORMFS_PUBLISHES:-1500}
export NORMFS_PUBLISH_HZ=${NORMFS_PUBLISH_HZ:-100}

cd "$REPO"
cargo build --release -p normfs --bench listeners_bench 2>&1 | tail -1
BIN=$(ls -t "$CARGO_TARGET_DIR"/release/deps/listeners_bench-* | grep -v '\.d$' | head -1)
echo "binary: $BIN" | tee -a "$OUT"

run_one() {
  local mode=$1 n=$2 round=$3
  if grep -q "round=$round RESULT mode=$mode listeners=$n " "$OUT" 2>/dev/null; then
    echo "skip: $mode $n round $round already in $OUT"; return
  fi
  line=$(NORMFS_MODE=$mode NORMFS_LISTENERS=$n "$BIN" 2>/dev/null | grep '^RESULT')
  echo "host=$LABEL round=$round $line" | tee -a "$OUT"
}

for mode in follow poll; do
  for n in $COUNTS; do run_one $mode $n 1; done
done
REV=""; for n in $COUNTS; do REV="$n $REV"; done
for mode in poll follow; do
  for n in $REV; do run_one $mode $n 2; done
done
echo "SWEEP-DONE $LABEL" | tee -a "$OUT"
