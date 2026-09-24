#!/usr/bin/env python3
"""Plot the controlled store-migration experiment and FS stage timings."""

import json
from pathlib import Path
import statistics
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

root = Path(sys.argv[1])
rows = [json.loads(line) for line in (root / "results.jsonl").read_text().splitlines()]
versions = [("dev", "dev", "#D64545"), ("dev_dirsync", "dev + directory sync", "#E69F00"), ("fs", "FS", "#2878C8")]
summary = []
for profile, rounds in [(True, [1, 2, 4, 5]), (False, [3, 6])]:
    for version, _, _ in versions:
        for queues in (1, 12):
            group = [r for r in rows if r["version"] == version and r["case"] == f"publication_{queues}" and r["profiled"] == profile]
            assert sorted(r["round"] for r in group) == rounds
            for metric in ("seconds", "publish_mib_s", "user_seconds", "system_seconds"):
                values = [r[metric] for r in group]
                summary.append(dict(version=version, queues=queues, profiled=profile, metric=metric,
                                    median=statistics.median(values), minimum=min(values), maximum=max(values)))
            if profile:
                for metric in group[0]["profile"]:
                    values = [r["profile"][metric]["seconds"] * 1000 / r["files"] for r in group]
                    summary.append(dict(version=version, queues=queues, profiled=True, metric=metric + "_ms_per_file",
                                        median=statistics.median(values), minimum=min(values), maximum=max(values)))
(root / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")

plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 10})
fig, axes = plt.subplots(2, 2, figsize=(14, 10))


def decorate(ax, title, unit):
    ax.set_title(title, loc="left", weight="bold", pad=16)
    ax.set_ylabel(unit)
    ax.set_ylim(0, ax.get_ylim()[1] * 1.22)
    ax.spines[["top", "right"]].set_visible(False)
    ax.grid(axis="y", alpha=0.18)
    ax.set_axisbelow(True)


def bars(ax, groups, positions, width, color, label, stages=False):
    med = [statistics.median(g) for g in groups]
    rects = ax.bar(positions, med, width, label=label, color=color,
                  yerr=[[m-min(g) for m,g in zip(med,groups)], [max(g)-m for m,g in zip(med,groups)]], capsize=3)
    labels = [f"{v:.2f}" for v in med]
    if stages:
        labels = [f"{v:.3f}" if v < 0.01 else f"{v:.1f}" if v >= 10 else f"{v:.2f}" for v in med]
    padding = 18 if stages and label.startswith("12") else 6
    ax.bar_label(rects, labels=labels, padding=padding, fontsize=7 if stages else 9)


for ax, metric, title, unit, profile in [
    (axes[0, 0], "publish_mib_s", "Store migration · ↑ Higher is better", "MiB/s", True),
    (axes[0, 1], "publish_mib_s", "Timers disabled · ↑ Higher is better", "MiB/s", False),
    (axes[1, 1], "cpu", "CPU time · ↓ Lower is better for equal work", "User + system seconds / process", True),
]:
    for i, (version, label, color) in enumerate(versions):
        groups = []
        for queues in (1, 12):
            selected = [r for r in rows if r["version"] == version and r["case"] == f"publication_{queues}" and r["profiled"] == profile]
            groups.append([r["user_seconds"] + r["system_seconds"] if metric == "cpu" else r[metric] for r in selected])
        bars(ax, groups, [x + (i-1)*0.25 for x in range(2)], 0.25, color, label)
    ax.set_xticks([0, 1], ["1 queue", "12 queues"])
    decorate(ax, title, unit)

metrics = ["admission", "queue", "open", "write", "file_sync", "rename", "dir_sync"]
for i, queues in enumerate((1, 12)):
    selected = [r for r in rows if r["version"] == "fs" and r["case"] == f"publication_{queues}" and r["profiled"]]
    groups = [[r["profile"][metric]["seconds"] * 1000 / r["files"] for r in selected] for metric in metrics]
    bars(axes[1, 0], groups, [x+(i-0.5)*0.36 for x in range(len(metrics))], 0.36,
         ["#2878C8", "#83B9E8"][i], f"{queues} queue" + ("s" if queues > 1 else ""), stages=True)
axes[1, 0].set_xticks(range(len(metrics)), ["Admission", "Queue", "Open", "Write", "File\nsync", "Rename", "Directory\nsync"])
axes[1, 0].legend(frameon=False, fontsize=9)
decorate(axes[1, 0], "FS stages · ↓ Lower is better", "Summed elapsed ms / published file")
fig.suptitle("Why store migration slows down — Mac", fontsize=21, weight="bold", y=0.98)
handles, labels = axes[0, 0].get_legend_handles_labels()
fig.legend(handles, labels, loc="upper center", bbox_to_anchor=(0.5, 0.95), ncol=3, frameon=False, fontsize=12)
fig.text(0.5, 0.018,
         "Median of 4 profiled runs; 2 timer-disabled controls. Whiskers: min–max. Directories provisioned before timing.\n"
         "dev + directory sync and FS use file + destination-directory barriers. dev alone has weaker durability.\n"
         "Stage times sum concurrent work and are not wall-clock shares. CPU includes preparation and validation.",
         ha="center", fontsize=10, color="#555555")
fig.tight_layout(rect=(0.01, 0.09, 0.99, 0.91), h_pad=3, w_pad=3)
fig.savefig(root / "fs-diagnosis.png", dpi=160)
