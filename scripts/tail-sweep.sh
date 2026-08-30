#!/usr/bin/env bash
# Repeat the original parallel_tail_benchmark (N polling clients, propagation
# latency = time until every client saw a new entry) on two code versions,
# sweeping N, two rounds with the order reversed. One line per run.
#
# Usage: ./tail-sweep.sh <label> <out-file> <bin-dev> <bin-stack>
set -u
LABEL=$1; OUT=$2; BIN_DEV=$3; BIN_STACK=$4
COUNTS="1 2 4 8 16 32 64 128 256 512 1000"
export NORMFS_PUBLISHES=${NORMFS_PUBLISHES:-500}
export NORMFS_CHECK_MS=${NORMFS_CHECK_MS:-1}

run_one() {
  local ver=$1 n=$2 round=$3 bin
  if [ "$ver" = dev ]; then bin=$BIN_DEV; else bin=$BIN_STACK; fi
  if grep -q "version=$ver round=$round RESULT clients=$n " "$OUT" 2>/dev/null; then
    echo "skip $ver $n $round"; return
  fi
  line=$(NORMFS_CLIENTS=$n "$bin" 2>/dev/null | grep '^RESULT')
  echo "host=$LABEL version=$ver round=$round $line" | tee -a "$OUT"
}

for ver in dev stack; do for n in $COUNTS; do run_one $ver $n 1; done; done
REV=""; for n in $COUNTS; do REV="$n $REV"; done
for ver in stack dev; do for n in $REV; do run_one $ver $n 2; done; done
echo "SWEEP-DONE $LABEL" | tee -a "$OUT"
