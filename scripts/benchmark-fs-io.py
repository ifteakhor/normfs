#!/usr/bin/env python3
"""Run paired filesystem-only measurements after building the standalone harness."""

import argparse
import hashlib
import json
import math
import os
import shutil
import tempfile
from pathlib import Path
import platform
import subprocess
import time

ROOT = Path(__file__).resolve().parents[1]
CASES = [
    ("append", 32768, 1, "structured"),
    ("append", 1048576, 1, "entropy"),
    ("append", 4194304, 1, "entropy"),
    ("append", 4194304, 8, "entropy"),
    ("publish", 32768, 8, "structured"),
    ("publish", 1048576, 8, "entropy"),
    ("publish", 4194304, 1, "entropy"),
    ("publish", 4194304, 8, "entropy"),
    ("read", 32768, 1, "structured"),
    ("read", 4194304, 1, "entropy"),
    ("read", 4194304, 8, "entropy"),
]


def command(*args):
    return subprocess.check_output(args, cwd=ROOT, text=True).strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--minimum-seconds", type=float, default=35.0)
    parser.add_argument("--resume", action="store_true", help="Resume a verified partial series with the same binary and settings")
    parser.add_argument("--reserve-gib", type=int, default=16, help="Free-space reserve in addition to the 64 GiB payload cap")
    args = parser.parse_args()
    if args.rounds < 2:
        parser.error("at least two rounds are required")
    if not 30 <= args.minimum_seconds <= 3600:
        parser.error("each measurement must perform I/O for at least 30 seconds (maximum 3600)")
    if not 8 <= args.reserve_gib <= 64:
        parser.error("reserve must be between 8 and 64 GiB")
    assert os.stat(tempfile.gettempdir()).st_dev == ROOT.stat().st_dev
    if not args.resume:
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
        "minimum_seconds": args.minimum_seconds,
        "stop_rule": "each worker executes I/O until its active duration reaches minimum_seconds; no padding",
        "latency_sampling": "every operation for writes; every 64th operation per client for cached reads",
        "max_write_footprint_bytes": 64 * 1024**3,
        "write_reserve_bytes": args.reserve_gib * 1024**3,
        "cases": CASES,
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "cpu_scope": "getrusage(RUSAGE_SELF), timed workers only; excludes setup/verification/cleanup",
        "read_fixture": "64 MiB per client, freshly written and synced; cached, no cache eviction",
        "barriers": {"append": "file sync per operation", "publish": "file sync, rename, parent sync per operation"},
    }
    previous_rows = []
    if args.resume:
        previous = json.loads((args.output / "metadata.json").read_text())
        for key in ["head", "binary_sha256", "source_sha256", "rounds", "minimum_seconds"]:
            assert previous[key] == metadata[key], f"Cannot resume: {key} changed"
        assert previous["cases"] == [list(case) for case in CASES]
        assert not previous.get("finished_at"), "Series is already complete"
        previous_rows = [json.loads(line) for line in (args.output / "runs.jsonl").read_text().splitlines()]
        for row in previous_rows:
            assert 1 <= row["round"] <= args.rounds and 0 <= row["case"] < len(CASES)
            op, size, workers, pattern = CASES[row["case"]]
            assert row["backend"] in ["raw", "fs"]
            assert (row["operation"], row["block_bytes"], row["workers"], row["pattern"]) == (op, size, workers, pattern)
            assert row["verified"] and row["quantiles_checked"] and not row["smoke"]
            assert row["minimum_seconds"] == args.minimum_seconds
            assert row["seconds"] >= args.minimum_seconds and min(row["worker_active_seconds"]) >= args.minimum_seconds
            assert len(row["worker_operations"]) == len(row["worker_active_seconds"]) == workers
            assert row["operations"] == sum(row["worker_operations"])
            assert row["payload_mib"] == row["operations"] * size / 1048576
        metadata = previous
        metadata.setdefault("initial_write_reserve_bytes", metadata.get("write_reserve_bytes", 16 * 1024**3))
        metadata["write_reserve_bytes"] = args.reserve_gib * 1024**3
        metadata.setdefault("resumptions", []).append({
            "at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "completed_runs": len(previous_rows),
            "free_bytes": shutil.disk_usage(tempfile.gettempdir()).free,
            "same_binary_and_workload": True,
            "write_reserve_bytes": args.reserve_gib * 1024**3,
        })
    finished_keys = {(row["round"], row["case"], row["backend"]) for row in previous_rows}
    assert len(finished_keys) == len(previous_rows), "Duplicate rows in partial series"
    (args.output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    total = args.rounds * len(CASES) * 2
    completed = len(previous_rows)
    with (args.output / "runs.jsonl").open("a" if args.resume else "w") as output:
        for round_index in range(args.rounds):
            cases = list(enumerate(CASES))
            if round_index % 2:
                cases.reverse()
            for case_index, (op, size, workers, pattern) in cases:
                backends = ["raw", "fs"]
                if (round_index + case_index) % 2:
                    backends.reverse()
                for backend in backends:
                    if (round_index + 1, case_index, backend) in finished_keys:
                        continue
                    os.sync()
                    time.sleep(2)
                    if op != "read":
                        # APFS can report reclaimed temporary-file space after process exit.
                        for attempt in range(3):
                            if shutil.disk_usage(tempfile.gettempdir()).free >= (64 + args.reserve_gib) * 1024**3:
                                break
                            if attempt < 2:
                                time.sleep(5)
                                os.sync()
                        else:
                            available = shutil.disk_usage(tempfile.gettempdir()).free / 1024**3
                            raise RuntimeError(f"Only {available:.2f} GiB free; need 64 GiB cap plus {args.reserve_gib} GiB reserve; partial series can be resumed")
                    result = subprocess.run(
                        [str(binary), backend, op, str(size), str(workers), str(args.minimum_seconds), pattern],
                        capture_output=True, text=True, check=True, timeout=max(300, args.minimum_seconds * 4 + 120),
                    )
                    lines = [line[7:] for line in result.stdout.splitlines() if line.startswith("RESULT ")]
                    assert len(lines) == 1, result.stdout
                    row = json.loads(lines[0])
                    assert row["verified"] and not row["smoke"]
                    assert row["minimum_seconds"] == args.minimum_seconds
                    assert len(row["worker_operations"]) == len(row["worker_active_seconds"]) == workers
                    assert min(row["worker_active_seconds"]) >= args.minimum_seconds
                    assert row["seconds"] >= args.minimum_seconds
                    assert row["operations"] == sum(row["worker_operations"])
                    assert row["payload_mib"] == row["operations"] * size / 1048576
                    assert (row["backend"], row["operation"], row["block_bytes"], row["workers"], row["pattern"]) == (backend, op, size, workers, pattern)
                    samples = row.pop("samples_ms")
                    assert len(samples) == sum(math.ceil(n / row["latency_sample_stride"]) for n in row["worker_operations"])
                    row["latency_samples"] = len(samples)
                    for key, quantile in [("p50_ms", 0.5), ("p99_ms", 0.99)]:
                        assert row[key] == samples[math.ceil(len(samples) * quantile) - 1]
                    assert math.isclose(row["mib_s"], row["payload_mib"] / row["seconds"])
                    row.update(round=round_index + 1, case=case_index, quantiles_checked=True)
                    output.write(json.dumps(row, sort_keys=True) + "\n")
                    output.flush()
                    completed += 1
                    print(f"{completed}/{total} round={round_index + 1} {backend:3} {op:7} {size//1024:4}KiB x{workers}: {row['seconds']:.2f}s, {row['payload_mib']/1024:.2f} GiB, {row['mib_s']:.1f} MiB/s, {row['cpu_seconds_per_gib']:.3f} CPU s/GiB", flush=True)
    metadata["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%S%z")
    (args.output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")


if __name__ == "__main__":
    main()
