#!/usr/bin/env python3
"""Measure FS drivers, optionally comparing dev, including whole-process CPU cost."""

import argparse
import datetime
import hashlib
import json
import os
import pathlib
import platform
import resource
import statistics
import subprocess
import time


CASES = [
    ("throughput_80", "fs_comparison", ["throughput", "80"]),
    ("throughput_4096", "fs_comparison", ["throughput", "4096"]),
    ("latency_0", "fs_comparison", ["latency", "0"]),
    ("latency_4", "fs_comparison", ["latency", "4"]),
    ("latency_12", "fs_comparison", ["latency", "12"]),
    ("publication_1", "fs_publication", ["1"]),
    ("publication_12", "fs_publication", ["12"]),
]


def measure(output, binaries, rounds, dev_binaries=None, dev_revision=None):
    versions = {"fs": binaries}
    if dev_binaries is not None:
        versions = {"dev": dev_binaries, **versions}
    metadata = {
        "started": datetime.datetime.now().astimezone().isoformat(),
        "revision": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "working_tree_status": subprocess.check_output(["git", "status", "--short"], text=True),
        "os": platform.platform(),
        "macos": platform.mac_ver()[0],
        "logical_cpus": os.cpu_count(),
        "rounds": rounds,
        "dev_revision": dev_revision,
        "versions": list(versions),
        "cpu_scope": "Whole child process, including setup, warmup, validation and teardown; 100% = one core",
        "binary_sha256": {
            version: {
                name: hashlib.sha256((directory / name).read_bytes()).hexdigest()
                for name in ("fs_comparison", "fs_publication")
            }
            for version, directory in versions.items()
        },
    }
    output.mkdir(parents=True, exist_ok=False)
    (output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    with (output / "results.jsonl").open("x") as results:
        for iteration in range(1, rounds + 1):
            cases = CASES if iteration % 2 else list(reversed(CASES))
            ordered_versions = list(versions) if iteration % 2 else list(reversed(versions))
            jobs = [(case, binary, args, version) for case, binary, args in cases for version in ordered_versions]
            for case, binary, args, version in jobs:
                subprocess.run(["sync"], check=True)
                time.sleep(3)
                before = resource.getrusage(resource.RUSAGE_CHILDREN)
                start = time.perf_counter()
                run = subprocess.run(
                    [str(versions[version] / binary), *args], capture_output=True, text=True, timeout=300
                )
                elapsed = time.perf_counter() - start
                after = resource.getrusage(resource.RUSAGE_CHILDREN)
                (output / f"{iteration}-{version}-{case}.log").write_text(run.stdout + run.stderr)
                run.check_returncode()
                records = [line[7:] for line in run.stdout.splitlines() if line.startswith("RESULT ")]
                if len(records) != 1:
                    raise RuntimeError(f"Expected one result: {iteration} {case}")
                row = json.loads(records[0])
                if row["case"] != case:
                    raise RuntimeError(f"Unexpected case: {row['case']}")
                user = after.ru_utime - before.ru_utime
                system = after.ru_stime - before.ru_stime
                row.update(
                    round=iteration, version=version, process_seconds=elapsed, user_seconds=user,
                    system_seconds=system, cpu_percent=100 * (user + system) / elapsed,
                    voluntary_switches=after.ru_nvcsw - before.ru_nvcsw,
                    involuntary_switches=after.ru_nivcsw - before.ru_nivcsw,
                )
                results.write(json.dumps(row) + "\n")
                results.flush()
                print(json.dumps({k: v for k, v in row.items() if k != "samples_ms"}), flush=True)


def plot(output):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    metadata = json.loads((output / "metadata.json").read_text())
    rows = [json.loads(line) for line in (output / "results.jsonl").read_text().splitlines()]
    for row in rows:
        row.setdefault("version", "fs")
        row["cpu_seconds"] = row["user_seconds"] + row["system_seconds"]
    versions = [("dev", "dev", "#D64545"), ("fs", "FS", "#2878C8")]
    versions = [v for v in versions if any(r["version"] == v[0] for r in rows)]
    expected_versions = set(metadata.get("versions", ["fs"]))
    if {v[0] for v in versions} != expected_versions:
        raise RuntimeError("Missing version results")
    summary = []
    plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 10})
    fig, axes = plt.subplots(3, 2, figsize=(16, 13))

    def panel(ax, cases, metric, title, unit, labels):
        width = 0.36 if len(versions) == 2 else 0.6
        for index, (version, label, color) in enumerate(versions):
            groups = [[r for r in rows if r["case"] == case and r["version"] == version] for case in cases]
            expected_rounds = list(range(1, metadata["rounds"] + 1))
            if any(sorted(r["round"] for r in group) != expected_rounds for group in groups):
                raise RuntimeError("Incomplete or duplicate benchmark results")
            values = [[r[metric] for r in group] for group in groups]
            medians = [statistics.median(g) for g in values]
            offset = (index - (len(versions) - 1) / 2) * width
            bars = ax.bar(
                [i + offset for i in range(len(cases))], medians,
                color=color, width=width, label=label,
                yerr=[[m - min(g) for m, g in zip(medians, values)],
                      [max(g) - m for m, g in zip(medians, values)]], capsize=3,
            )
            labels_above = [f"{m:,.0f}" if m >= 100 else f"{m:.2f}" for m in medians]
            if metric == "cpu_percent":
                labels_above = [f"{m:.0f}" if m >= 10 else f"{m:.1f}" for m in medians]
            ax.bar_label(bars, labels=labels_above,
                         padding=7, fontsize=8)
            for case, group, median in zip(cases, values, medians):
                summary.append(dict(case=case, version=version, metric=metric, median=median,
                                    minimum=min(group), maximum=max(group), runs=len(group)))
        ax.set_xticks(range(len(cases)), labels)
        ax.set_title(title, loc="left", weight="bold", pad=15)
        ax.set_ylabel(unit)
        ax.set_ylim(0, ax.get_ylim()[1] * 1.25)
        ax.spines[["top", "right"]].set_visible(False)
        ax.grid(axis="y", alpha=0.18)
        ax.set_axisbelow(True)

    throughput = ["throughput_80", "throughput_4096"]
    latency = ["latency_0", "latency_4", "latency_12"]
    panel(axes[0, 0], throughput, "write_mib_s", "WAL writes", "MiB/s", ["80 B records", "4 KiB records"])
    panel(axes[0, 1], throughput, "scan_mib_s", "Cached WAL scans", "MiB/s", ["80 B records", "4 KiB records"])
    panel(axes[1, 0], latency, "p99_ms", "Commit latency · p99", "Milliseconds", ["No readers", "4 readers", "12 readers"])
    panel(axes[1, 1], ["publication_1", "publication_12"], "publish_mib_s", "Store migration", "MiB/s", ["1 queue", "12 queues"])
    all_cases = [case for case, _, _ in CASES]
    labels = ["WAL\n80 B", "WAL\n4 KiB", "Ack\n0 readers", "Ack\n4 readers", "Ack\n12 readers", "Store\n1 queue", "Store\n12 queues"]
    panel(axes[2, 0], all_cases, "cpu_percent", "CPU utilization", "% · 100% = one core", labels)
    panel(axes[2, 1], all_cases, "cpu_seconds", "CPU time", "User + system seconds", labels)
    fig.suptitle("FS vs dev — Mac" if len(versions) == 2 else "FS — Mac", fontsize=22, weight="bold", y=0.98)
    handles, labels = axes[0, 0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", bbox_to_anchor=(0.5, 0.951), ncol=2, frameon=False, fontsize=13)
    caveat = "Reads are cached."
    if len(versions) == 2:
        caveat += " FS adds directory durability absent in dev."
    fig.text(0.5, 0.025,
             f"Median of {metadata['rounds']} runs; whiskers: min–max. CPU includes setup, validation and cleanup.\n"
             + caveat,
             ha="center", fontsize=10, color="#555555")
    fig.tight_layout(rect=(0.02, 0.075, 0.99, 0.92), h_pad=3.5, w_pad=3)
    fig.savefig(output / "fs-results.png", dpi=160)
    plt.close(fig)
    (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=pathlib.Path)
    parser.add_argument("--binaries", type=pathlib.Path, default=pathlib.Path("benchmarks/fs-driver/target/release"))
    parser.add_argument("--dev-binaries", type=pathlib.Path)
    parser.add_argument("--dev-revision")
    parser.add_argument("--rounds", type=int, default=4)
    parser.add_argument("--plot-only", action="store_true")
    args = parser.parse_args()
    if args.rounds < 1:
        parser.error("--rounds must be positive")
    if not args.plot_only:
        if args.dev_binaries is not None and not args.dev_revision:
            parser.error("--dev-revision is required with --dev-binaries")
        measure(args.output, args.binaries.resolve(), args.rounds,
                args.dev_binaries.resolve() if args.dev_binaries is not None else None,
                args.dev_revision)
    plot(args.output)


if __name__ == "__main__":
    main()
