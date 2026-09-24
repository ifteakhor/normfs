#!/usr/bin/env python3
"""Measure macOS uncached bulk file I/O, without NormFS or the FS layer."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import time

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--extended", action="store_true", help="Measure 4 MiB with 16 and 32 clients")
    args = parser.parse_args()
    assert platform.system() == "Darwin", "macOS required"
    args.output.mkdir(parents=True, exist_ok=False)
    binary = ROOT / "benchmarks/fs-io/target/release/ssd_io"
    cases = [(4194304, 16), (4194304, 32)] if args.extended else [(1048576, 1), (4194304, 1), (4194304, 8)]
    metadata = {
        "platform": platform.platform(),
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
        "source_sha256": hashlib.sha256((ROOT / "benchmarks/fs-io/ssd.rs").read_bytes()).hexdigest(),
        "total_bytes_per_phase": 32 * 1024**3,
        "rounds": 3,
        "cases": cases,
        "cache_policy": "F_NOCACHE on every file descriptor; no fallback",
        "write_sync": "F_FULLFSYNC once per file at end of timed write; no fallback",
        "read_order": "read follows write and untimed spot verification, with F_NOCACHE still enabled",
        "temporary_parent": str(args.output.resolve()),
        "free_bytes_before": shutil.disk_usage(args.output).free,
    }
    def save_metadata():
        (args.output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    save_metadata()
    with (args.output / "runs.jsonl").open("w") as output:
        for round_index in range(3):
            order = cases if round_index % 2 == 0 else list(reversed(cases))
            for block, clients in order:
                assert shutil.disk_usage(args.output).free >= 48 * 1024**3, "Need 32 GiB plus 16 GiB reserve"
                os.sync()
                time.sleep(3)
                print(f"round={round_index + 1}/3 block={block//1048576}MiB clients={clients} starting 32GiB write + read", flush=True)
                process = subprocess.run(
                    [str(binary), str(block), str(clients), "32768", str(args.output.resolve())],
                    capture_output=True, text=True, check=True, timeout=600,
                )
                rows = [json.loads(line[7:]) for line in process.stdout.splitlines() if line.startswith("RESULT ")]
                assert len(rows) == 2
                assert {row["operation"] for row in rows} == {"write", "read"}
                for row in rows:
                    assert row["nocache"] and row["spot_checks_passed"] and row["full_sync_on_write"]
                    assert (row["block_bytes"], row["clients"], row["total_bytes"]) == (block, clients, 32 * 1024**3)
                    row["round"] = round_index + 1
                    output.write(json.dumps(row, sort_keys=True) + "\n")
                    output.flush()
                    print(f"  {row['operation']}: {row['gb_s']:.3f} GB/s, {row['seconds']:.2f}s, {row['cpu_seconds_per_gib']:.3f} CPU s/GiB", flush=True)
    metadata["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%S%z")
    metadata["free_bytes_after"] = shutil.disk_usage(args.output).free
    save_metadata()


if __name__ == "__main__":
    main()
