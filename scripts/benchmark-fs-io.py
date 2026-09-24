#!/usr/bin/env python3
"""Run paired filesystem-only measurements after building the standalone harness."""

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import subprocess
import time

ROOT = Path(__file__).resolve().parents[1]
CASES = [
    ("append", 32768, 1, 256, "structured"),
    ("append", 1048576, 1, 256, "entropy"),
    ("append", 4194304, 1, 128, "entropy"),
    ("append", 4194304, 8, 16, "entropy"),
    ("publish", 32768, 8, 32, "structured"),
    ("publish", 1048576, 8, 32, "entropy"),
    ("publish", 4194304, 1, 128, "entropy"),
    ("publish", 4194304, 8, 16, "entropy"),
    ("read", 32768, 1, 131072, "structured"),
    ("read", 4194304, 1, 1024, "entropy"),
    ("read", 4194304, 8, 128, "entropy"),
]


def command(*args):
    return subprocess.check_output(args, cwd=ROOT, text=True).strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--rounds", type=int, default=4)
    args = parser.parse_args()
    if args.rounds < 2:
        parser.error("at least two rounds are required")
    args.output.mkdir(parents=True, exist_ok=False)
    binary = ROOT / "benchmarks/fs-io/target/release/fs_io"
    metadata = {
        "head": command("git", "rev-parse", "HEAD"),
        "status": command("git", "status", "--short"),
        "platform": platform.platform(),
        "macos": platform.mac_ver()[0],
        "logical_cpus": os.cpu_count(),
        "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
        "source_sha256": hashlib.sha256(
            (ROOT / "benchmarks/fs-io/main.rs").read_bytes()
        ).hexdigest(),
        "rounds": args.rounds,
        "cases": CASES,
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "cpu_scope": "getrusage(RUSAGE_SELF), timed workers only; excludes setup/verification/cleanup",
        "read_fixture": "64 MiB per client, freshly written and synced; cached, no cache eviction",
        "barriers": {"append": "file sync per operation", "publish": "file sync, rename, parent sync per operation"},
    }
    (args.output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    total = args.rounds * len(CASES) * 2
    completed = 0
    with (args.output / "runs.jsonl").open("w") as output:
        for round_index in range(args.rounds):
            cases = list(enumerate(CASES))
            if round_index % 2:
                cases.reverse()
            for case_index, (op, size, workers, count, pattern) in cases:
                backends = ["raw", "fs"]
                if (round_index + case_index) % 2:
                    backends.reverse()
                for backend in backends:
                    os.sync()
                    time.sleep(2)
                    result = subprocess.run(
                        [str(binary), backend, op, str(size), str(workers), str(count), pattern],
                        capture_output=True, text=True, check=True, timeout=300,
                    )
                    lines = [line[7:] for line in result.stdout.splitlines() if line.startswith("RESULT ")]
                    assert len(lines) == 1, result.stdout
                    row = json.loads(lines[0])
                    assert row["verified"] and row["operations"] == workers * count
                    assert (row["backend"], row["operation"], row["block_bytes"], row["workers"], row["pattern"]) == (backend, op, size, workers, pattern)
                    samples = row.pop("samples_ms")
                    assert len(samples) == row["operations"]
                    for key, quantile in [("p50_ms", 0.5), ("p99_ms", 0.99)]:
                        assert row[key] == samples[math.ceil(len(samples) * quantile) - 1]
                    assert math.isclose(row["mib_s"], row["payload_mib"] / row["seconds"])
                    row.update(round=round_index + 1, case=case_index, quantiles_checked=True)
                    output.write(json.dumps(row, sort_keys=True) + "\n")
                    output.flush()
                    completed += 1
                    print(f"{completed}/{total} round={round_index + 1} {backend:3} {op:7} {size//1024:4}KiB x{workers}: {row['mib_s']:.1f} MiB/s, {row['cpu_seconds_per_gib']:.3f} CPU s/GiB", flush=True)
    metadata["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%S%z")
    (args.output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")


if __name__ == "__main__":
    main()
