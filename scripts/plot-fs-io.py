#!/usr/bin/env python3
"""Plot the filesystem-only paired runs; no application pipeline measurements."""

import argparse
import json
from pathlib import Path
from statistics import median

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Patch

COLORS = {"raw": "#d94b4b", "fs": "#327ac2"}
NAMES = {"raw": "Raw I/O", "fs": "FS layer"}
METRICS = [
    ("mib_s", "Throughput · MiB/s · higher is better ↑"),
    ("cpu_seconds_per_gib", "CPU seconds/GiB · lower is better ↓"),
    ("p99_ms", "p99 latency · ms · lower is better ↓"),
]


def label(row):
    size = row["block_bytes"]
    return f"{size // 1024} KiB" if size < 1048576 else f"{size // 1048576} MiB"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results", type=Path)
    args = parser.parse_args()
    metadata = json.loads((args.results / "metadata.json").read_text())
    runs = [json.loads(line) for line in (args.results / "runs.jsonl").read_text().splitlines()]
    grouped = {}
    for row in runs:
        grouped.setdefault((row["case"], row["backend"]), []).append(row)
    assert len(runs) == len(metadata["cases"]) * 2 * metadata["rounds"]
    assert all(len(rows) == metadata["rounds"] for rows in grouped.values())
    assert all(row["verified"] and row["quantiles_checked"] for row in runs)
    minimum_seconds = metadata.get("minimum_seconds")
    if minimum_seconds is not None:
        assert minimum_seconds >= 30
        assert all(not row["smoke"] and row["seconds"] >= minimum_seconds and min(row["worker_active_seconds"]) >= minimum_seconds for row in runs)
    summary = []
    for (case, backend), rows in sorted(grouped.items()):
        item = {key: rows[0][key] for key in ["case", "backend", "operation", "block_bytes", "workers", "pattern"]}
        for metric in [key for key, _ in METRICS] + ["operations", "payload_mib", "seconds"]:
            values = [row[metric] for row in rows]
            item[metric] = {"median": median(values), "min": min(values), "max": max(values)}
        summary.append(item)
    (args.results / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    indexed = {(row["case"], row["backend"]): row for row in summary}
    fig, axes = plt.subplots(3, 3, figsize=(17, 12))
    for operation_index, operation in enumerate(["append", "publish", "read"]):
        cases = sorted({row["case"] for row in runs if row["operation"] == operation})
        labels = [f"{label(indexed[case, 'raw'])} · {indexed[case, 'raw']['workers']} client(s)" for case in cases]
        for metric_index, (metric, title) in enumerate(METRICS):
            ax = axes[operation_index, metric_index]
            if operation == "read" and metric == "p99_ms" and minimum_seconds is not None:
                title = "Sampled-offset p99 · ms · lower is better ↓"
            maximum = max(indexed[case, backend][metric]["max"] for case in cases for backend in COLORS)
            for backend_index, backend in enumerate(COLORS):
                values = [indexed[case, backend][metric]["median"] for case in cases]
                positions = [i + (backend_index - 0.5) * 0.36 for i in range(len(cases))]
                errors = [
                    [indexed[case, backend][metric]["median"] - indexed[case, backend][metric]["min"] for case in cases],
                    [indexed[case, backend][metric]["max"] - indexed[case, backend][metric]["median"] for case in cases],
                ]
                ax.barh(positions, values, height=0.32, color=COLORS[backend], xerr=errors, error_kw={"elinewidth": 0.8, "capsize": 2})
                for case, y, value in zip(cases, positions, values):
                    end = indexed[case, backend][metric]["max"]
                    number = f"{value:.3f}" if value < 1 else f"{value:,.2f}" if value < 100 else f"{value:,.0f}"
                    ax.text(end + maximum * 0.02, y, number, va="center", fontsize=9)
            ax.set_xlim(0, maximum * 1.28)
            ax.set_yticks(range(len(cases)), labels if metric_index == 0 else [])
            ax.invert_yaxis()
            ax.set_title(title, fontsize=11, loc="left", pad=12)
            ax.grid(axis="x", alpha=0.2)
            ax.set_axisbelow(True)
            for spine in ["top", "right", "left"]:
                ax.spines[spine].set_visible(False)
            ax.tick_params(axis="y", length=0)
            ax.tick_params(axis="x", labelsize=9)
            if metric_index == 0:
                names = {"append": "Append + file sync", "publish": "Create + write + file sync\n+ rename + directory sync", "read": "Cached reads · NOT SSD bandwidth"}
                ax.set_ylabel(names[operation], fontsize=11, labelpad=14)
    fig.suptitle("Raw I/O vs FS layer — filesystem operations only", fontsize=21, x=0.5, y=0.98)
    duration_label = f" · ≥{minimum_seconds:g} s/client" if minimum_seconds is not None else ""
    fig.text(0.5, 0.94, f"Mac · macOS {metadata['macos']} · {metadata['rounds']} paired rounds{duration_label} · medians; whiskers = min–max · equal sync barriers", ha="center", fontsize=12)
    fig.legend(handles=[Patch(color=COLORS[key], label=NAMES[key]) for key in COLORS], loc="upper center", bbox_to_anchor=(0.5, 0.93), ncol=2, frameon=False, fontsize=12)
    read_label = f"≥{minimum_seconds:g} s/client; read p99 sampled every 64 ops" if minimum_seconds is not None else "4 GiB logical traffic/run"
    fig.text(0.5, 0.038, f"Raw I/O: dedicated blocking threads  |  FS layer: public async API + default worker pool\n32 KiB: synthetic structured data; 1/4 MiB: deterministic entropy. No NormFS application, WAL, Store, compression or encryption.\nReads: 64 MiB fixture/client, cached; {read_label}. CPU excludes fixture creation, verification and cleanup.", ha="center", va="center", fontsize=10, linespacing=1.6)
    fig.subplots_adjust(left=0.15, right=0.98, top=0.865, bottom=0.11, wspace=0.17, hspace=0.42)
    fig.savefig(args.results / "raw-io-vs-fs.png", dpi=170, facecolor="white")
    plt.close(fig)
    table = ["| Operation | Block | Clients | Raw MiB/s | FS MiB/s | FS throughput change | Raw CPU s/GiB | FS CPU s/GiB | Raw p99 ms | FS p99 ms |", "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for case in sorted({row["case"] for row in runs}):
        raw, fs = indexed[case, "raw"], indexed[case, "fs"]
        def value(row, metric):
            return row[metric]["median"]
        table.append(f"| {raw['operation']} | {label(raw)} | {raw['workers']} | {value(raw, 'mib_s'):.2f} | {value(fs, 'mib_s'):.2f} | {(value(fs, 'mib_s') / value(raw, 'mib_s') - 1) * 100:+.1f}% | {value(raw, 'cpu_seconds_per_gib'):.3f} | {value(fs, 'cpu_seconds_per_gib'):.3f} | {value(raw, 'p99_ms'):.3f} | {value(fs, 'p99_ms'):.3f} |")
    (args.results / "table.md").write_text("\n".join(table) + "\n")


if __name__ == "__main__":
    main()
