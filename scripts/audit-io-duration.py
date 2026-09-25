#!/usr/bin/env python3
"""Check a complete duration-controlled matrix and report each measured interval."""

import argparse
import json
import math
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results", type=Path)
    args = parser.parse_args()
    metadata = json.loads((args.results / "metadata.json").read_text())
    rows = [json.loads(line) for line in (args.results / "runs.jsonl").read_text().splitlines()]
    minimum = metadata["minimum_seconds"]
    assert minimum >= 30 and metadata.get("finished_at"), "Incomplete or short-duration series"
    fs = "backend" in rows[0]
    expected = set()
    for repeat in range(1, metadata["rounds"] + 1):
        for case_index, case in enumerate(metadata["cases"]):
            if fs:
                op, block, clients, pattern = case
                expected.update((repeat, case_index, backend, op, block, clients, pattern) for backend in ["raw", "fs"])
            else:
                block, clients = case
                expected.update((repeat, op, block, clients) for op in ["write", "read"])
    observed = set()
    table = [
        "| Round | Backend / operation | Block KiB | Clients | Wall seconds | Minimum worker I/O seconds | Logical GiB |",
        "|---:|---|---:|---:|---:|---:|---:|",
    ]
    byte_totals = {"read": 0, "write": 0}
    for row in rows:
        clients = row["workers"] if fs else row["clients"]
        block = row["block_bytes"]
        active = row["worker_active_seconds"]
        operations = row["worker_operations"]
        assert not row["smoke"] and row["minimum_seconds"] == minimum
        assert len(active) == len(operations) == clients
        assert min(active) >= minimum and row["seconds"] >= minimum
        assert all(n > 0 for n in operations)
        transferred = sum(operations) * block
        if fs:
            key = (row["round"], row["case"], row["backend"], row["operation"], block, clients, row["pattern"])
            assert row["verified"] and row["quantiles_checked"]
            assert row["operations"] == sum(operations)
            assert row["payload_mib"] == transferred / 1048576
            stride = 64 if row["operation"] == "read" else 1
            assert row["latency_sample_stride"] == stride
            assert row["latency_samples"] == sum((n + stride - 1) // stride for n in operations)
            assert 0 <= row["p50_ms"] <= row["p99_ms"] <= row["max_ms"]
            cpu_seconds = row["user_seconds"] + row["system_seconds"]
            name = f"{row['backend']} / {row['operation']}"
        else:
            key = (row["round"], row["operation"], block, clients)
            assert row["nocache"] and row["full_sync_on_write"] and row["spot_checks_passed"]
            assert row["fixture_bytes"] == metadata["fixture_bytes"]
            assert all(n * block >= row["fixture_bytes"] // clients for n in operations)
            assert row["total_bytes"] == transferred
            assert math.isclose(row["gb_s"], transferred / 1e9 / row["seconds"])
            cpu_seconds = row["cpu_seconds"]
            name = row["operation"]
        assert key in expected and key not in observed
        observed.add(key)
        assert math.isclose(row["mib_s"], transferred / 1048576 / row["seconds"])
        assert math.isclose(row["cpu_seconds_per_gib"], cpu_seconds / (transferred / 1024**3))
        byte_totals["read" if row["operation"] == "read" else "write"] += transferred
        table.append(f"| {row['round']} | {name} | {block // 1024} | {clients} | {row['seconds']:.6f} | {min(active):.6f} | {transferred / 1024**3:.3f} |")
    assert observed == expected
    audit = {
        "complete": True,
        "measurements": len(rows),
        "required_seconds": minimum,
        "minimum_wall_seconds": min(row["seconds"] for row in rows),
        "maximum_wall_seconds": max(row["seconds"] for row in rows),
        "minimum_worker_io_seconds": min(min(row["worker_active_seconds"]) for row in rows),
        "maximum_worker_io_seconds": max(max(row["worker_active_seconds"]) for row in rows),
        "measured_logical_bytes": byte_totals,
        "byte_scope": "Measured phases only; excludes fixture preparation and verification; FS reads are cached",
    }
    (args.results / "duration-audit.json").write_text(json.dumps(audit, indent=2) + "\n")
    (args.results / "durations.md").write_text("\n".join(table) + "\n")
    print(json.dumps(audit, indent=2))


if __name__ == "__main__":
    main()
