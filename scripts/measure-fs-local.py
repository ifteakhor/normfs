#!/usr/bin/env python3
"""Measure the current checkout's FS driver, including whole-process CPU cost."""

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


def measure(output, binaries, rounds):
    metadata = {
        "started": datetime.datetime.now().astimezone().isoformat(),
        "revision": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "working_tree_status": subprocess.check_output(["git", "status", "--short"], text=True),
        "os": platform.platform(),
        "macos": platform.mac_ver()[0],
        "logical_cpus": os.cpu_count(),
        "rounds": rounds,
        "cpu_scope": "Whole child process, including setup, warmup, validation and teardown; 100% = one core",
        "binary_sha256": {
            name: hashlib.sha256((binaries / name).read_bytes()).hexdigest()
            for name in ("fs_comparison", "fs_publication")
        },
    }
    output.mkdir(parents=True, exist_ok=False)
    (output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    with (output / "results.jsonl").open("x") as results:
        for iteration in range(1, rounds + 1):
            cases = CASES if iteration % 2 else list(reversed(CASES))
            for case, binary, args in cases:
                subprocess.run(["sync"], check=True)
                time.sleep(3)
                before = resource.getrusage(resource.RUSAGE_CHILDREN)
                start = time.perf_counter()
                run = subprocess.run(
                    [str(binaries / binary), *args], capture_output=True, text=True, timeout=300
                )
                elapsed = time.perf_counter() - start
                after = resource.getrusage(resource.RUSAGE_CHILDREN)
                (output / f"{iteration}-{case}.log").write_text(run.stdout + run.stderr)
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
                    round=iteration, process_seconds=elapsed, user_seconds=user,
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
    summary = []
    plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 10})
    fig, axes = plt.subplots(3, 2, figsize=(14, 13))

    def panel(ax, cases, metric, title, unit, labels):
        groups = [[r[metric] for r in rows if r["case"] == case] for case in cases]
        if any(len(g) != metadata["rounds"] for g in groups):
            raise RuntimeError("Incomplete benchmark results")
        medians = [statistics.median(g) for g in groups]
        bars = ax.bar(
            range(len(cases)), medians, color="#2878C8", width=0.6,
            yerr=[[m - min(g) for m, g in zip(medians, groups)],
                  [max(g) - m for m, g in zip(medians, groups)]], capsize=4,
        )
        ax.bar_label(bars, labels=[f"{m:,.2f}" for m in medians], padding=8, fontsize=9)
        ax.set_xticks(range(len(cases)), labels)
        ax.set_title(title, loc="left", weight="bold", pad=15)
        ax.set_ylabel(unit)
        ax.set_ylim(0, ax.get_ylim()[1] * 1.25)
        ax.spines[["top", "right"]].set_visible(False)
        ax.grid(axis="y", alpha=0.18)
        ax.set_axisbelow(True)
        for case, group, median in zip(cases, groups, medians):
            summary.append(dict(case=case, metric=metric, median=median,
                                minimum=min(group), maximum=max(group), runs=len(group)))

    throughput = ["throughput_80", "throughput_4096"]
    latency = ["latency_0", "latency_4", "latency_12"]
    panel(axes[0, 0], throughput, "write_mib_s", "WAL writes · sync enabled", "Payload MiB/s", ["80 B records", "4 KiB records"])
    panel(axes[0, 1], throughput, "scan_mib_s", "Cached WAL scans · not physical disk reads", "Payload MiB/s", ["80 B records", "4 KiB records"])
    panel(axes[1, 0], latency, "p99_ms", "Commit latency · p99", "Milliseconds", ["No readers", "4 readers", "12 readers"])
    panel(axes[1, 1], ["publication_1", "publication_12"], "publish_mib_s", "Store migration · directory sync enabled", "Payload MiB/s", ["1 queue", "12 queues"])
    all_cases = [case for case, _, _ in CASES]
    labels = ["WAL\n80 B", "WAL\n4 KiB", "Ack\n0 rdr", "Ack\n4 rdr", "Ack\n12 rdr", "Store\n1 q", "Store\n12 q"]
    panel(axes[2, 0], all_cases, "cpu_percent", "Whole-process CPU · 100% = one core", "CPU %", labels)
    ax = axes[2, 1]
    for metric, color, offset, label in [("user_seconds", "#2878C8", -0.18, "User"), ("system_seconds", "#E69F00", 0.18, "System")]:
        groups = [[r[metric] for r in rows if r["case"] == case] for case in all_cases]
        medians = [statistics.median(g) for g in groups]
        ax.bar([i + offset for i in range(len(all_cases))], medians, width=0.36, color=color, label=label,
               yerr=[[m-min(g) for m,g in zip(medians,groups)], [max(g)-m for m,g in zip(medians,groups)]], capsize=3)
        for case, group, median in zip(all_cases, groups, medians):
            summary.append(dict(case=case, metric=metric, median=median, minimum=min(group), maximum=max(group), runs=len(group)))
    ax.set_xticks(range(len(all_cases)), labels)
    ax.set_title("Whole-process CPU time · user and system", loc="left", weight="bold", pad=15)
    ax.set_ylabel("CPU seconds")
    ax.legend(frameon=False)
    ax.spines[["top", "right"]].set_visible(False)
    ax.grid(axis="y", alpha=0.18)
    ax.set_axisbelow(True)
    fig.suptitle("FS layer — current checkout / Mac", fontsize=21, weight="bold", y=0.98)
    fig.text(0.5, 0.943, f"{metadata['started'][:10]} · macOS {metadata['macos']} · {metadata['logical_cpus']} logical CPUs · HEAD {metadata['revision'][:7]} + working tree", ha="center", color="#444444")
    fig.text(0.5, 0.018,
             f"Median of {metadata['rounds']} serial runs per case; whiskers: min–max. Release build; 500 commits per latency run.\n"
             "CPU covers the entire process, including setup, warmup, validation and cleanup. WAL CPU includes writes AND scans.\n"
             "Throughput/latency timers cover their individual phases. Cached data; no claim of disk saturation or minimum CPU.",
             ha="center", fontsize=10, color="#444444")
    fig.tight_layout(rect=(0.02, 0.085, 0.99, 0.92), h_pad=3.5, w_pad=3)
    fig.savefig(output / "fs-results.png", dpi=160)
    plt.close(fig)
    (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=pathlib.Path)
    parser.add_argument("--binaries", type=pathlib.Path, default=pathlib.Path("benchmarks/fs-driver/target/release"))
    parser.add_argument("--rounds", type=int, default=4)
    parser.add_argument("--plot-only", action="store_true")
    args = parser.parse_args()
    if args.rounds < 1:
        parser.error("--rounds must be positive")
    if not args.plot_only:
        measure(args.output, args.binaries.resolve(), args.rounds)
    plot(args.output)


if __name__ == "__main__":
    main()
