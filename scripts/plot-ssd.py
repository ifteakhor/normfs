#!/usr/bin/env python3
"""Plot uncached bulk SSD throughput, separately from sync-per-operation FS tests."""

import argparse
import json
from pathlib import Path
from statistics import median

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Patch


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results", type=Path)
    parser.add_argument("--extra", type=Path, help="Additional concurrency sweep results")
    args = parser.parse_args()
    rows = [json.loads(line) for line in (args.results / "runs.jsonl").read_text().splitlines()]
    metadata = [json.loads((args.results / "metadata.json").read_text())]
    if args.extra:
        metadata.append(json.loads((args.extra / "metadata.json").read_text()))
        rows += [json.loads(line) for line in (args.extra / "runs.jsonl").read_text().splitlines()]
    assert len(rows) == sum(len(meta["cases"]) * meta["rounds"] * 2 for meta in metadata)
    minimum_seconds = metadata[0].get("minimum_seconds")
    if minimum_seconds is not None:
        assert minimum_seconds >= 30
        assert all(not row["smoke"] and row["seconds"] >= minimum_seconds and min(row["worker_active_seconds"]) >= minimum_seconds for row in rows)
    cases = sorted({(row["block_bytes"], row["clients"]) for row in rows})
    colors = {"write": "#d94b4b", "read": "#327ac2"}
    summary = []
    for block, clients in cases:
        for operation in colors:
            values = [row for row in rows if (row["block_bytes"], row["clients"], row["operation"]) == (block, clients, operation)]
            assert len(values) == 3 and {row["round"] for row in values} == {1, 2, 3}
            assert all(row["spot_checks_passed"] and row["nocache"] for row in values)
            item = {"block_bytes": block, "clients": clients, "operation": operation}
            for metric in ["gb_s", "mib_s", "cpu_seconds_per_gib", "seconds", "total_bytes"]:
                numbers = [row[metric] for row in values]
                item[metric] = {"median": median(numbers), "min": min(numbers), "max": max(numbers)}
            summary.append(item)
    (args.results / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    indexed = {(row["block_bytes"], row["clients"], row["operation"]): row for row in summary}
    fig, axes = plt.subplots(1, 2, figsize=(15, 6))
    for ax, metric, title in zip(axes, ["gb_s", "cpu_seconds_per_gib"], ["Throughput · GB/s · higher is better ↑", "CPU seconds/GiB · lower is better ↓"]):
        for operation_index, operation in enumerate(colors):
            stats = [indexed[block, clients, operation][metric] for block, clients in cases]
            values = [stat["median"] for stat in stats]
            positions = [i + (operation_index - 0.5) * 0.36 for i in range(len(cases))]
            ax.bar(positions, values, width=0.32, color=colors[operation], yerr=[[stat["median"] - stat["min"] for stat in stats], [stat["max"] - stat["median"] for stat in stats]], capsize=4, error_kw={"elinewidth": 1})
            maximum = max(row[metric]["max"] for row in summary)
            for position, stat in zip(positions, stats):
                ax.text(position, stat["max"] + maximum * 0.025, f"{stat['median']:.3f}", ha="center", fontsize=11)
            ax.set_ylim(0, maximum * 1.2)
        ax.set_xticks(range(len(cases)), [f"{block//1048576} MiB\n{clients} client(s)" for block, clients in cases])
        ax.set_title(title, fontsize=12, pad=14)
        ax.grid(axis="y", alpha=0.2)
        ax.set_axisbelow(True)
        ax.spines[["top", "right"]].set_visible(False)
    fig.suptitle("Uncached sequential SSD throughput", fontsize=21, y=0.97)
    workload = f"≥{minimum_seconds:g} s/client/phase · 32 GiB fixture" if minimum_seconds is not None else "32 GiB/run"
    fig.text(0.5, 0.9, f"macOS · APFS · {workload} · 3 rounds · medians; whiskers = min–max", ha="center", fontsize=12)
    fig.legend(handles=[Patch(color=color, label=operation.capitalize()) for operation, color in colors.items()], loc="upper center", bbox_to_anchor=(0.5, 0.88), ncol=2, frameon=False, fontsize=12)
    fig.text(0.5, 0.06, "F_NOCACHE on every file · F_FULLFSYNC at end of each written file · no NormFS or FS layer\nGB/s = decimal 10⁹ bytes/s. Measured filesystem throughput; not a proven hardware maximum.\nDifferent sync frequency from the FS-layer benchmark: those rates are not directly comparable.", ha="center", fontsize=10, linespacing=1.5)
    fig.subplots_adjust(left=0.07, right=0.98, top=0.74, bottom=0.22, wspace=0.23)
    fig.savefig(args.results / "ssd-throughput.png", dpi=170, facecolor="white")
    plt.close(fig)
    table = ["| Block | Clients | Operation | Median GB/s | Min–max GB/s | Median CPU s/GiB |", "|---|---:|---|---:|---:|---:|"]
    for row in summary:
        speed = row["gb_s"]
        table.append(f"| {row['block_bytes']//1048576} MiB | {row['clients']} | {row['operation']} | {speed['median']:.3f} | {speed['min']:.3f}–{speed['max']:.3f} | {row['cpu_seconds_per_gib']['median']:.3f} |")
    (args.results / "table.md").write_text("\n".join(table) + "\n")


if __name__ == "__main__":
    main()
