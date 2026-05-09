from __future__ import annotations

import os
import tempfile
from pathlib import Path

_CACHE_ROOT = Path(tempfile.gettempdir()) / "riposte-eval-plot-cache"
_MPL_CACHE = _CACHE_ROOT / "matplotlib"
_XDG_CACHE = _CACHE_ROOT / "xdg"
_FONTCONFIG_CACHE = _XDG_CACHE / "fontconfig"
for _cache_dir in (_MPL_CACHE, _XDG_CACHE, _FONTCONFIG_CACHE):
    _cache_dir.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("MPLCONFIGDIR", str(_MPL_CACHE))
os.environ.setdefault("XDG_CACHE_HOME", str(_XDG_CACHE))

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from .data import (
    aes_reference_summary,
    figures_dir,
    load_aes_speed,
    load_measured_throughput,
    load_metadata,
    throughput_summary,
)


def _save_png(fig: plt.Figure, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=200, bbox_inches="tight")
    plt.close(fig)
    return path


def plot_throughput_over_time(results_dir: str | Path) -> Path:
    df = load_measured_throughput(results_dir)
    stats = throughput_summary(df)
    out = figures_dir(results_dir) / "throughput-over-time.png"
    steady = df.iloc[1:].copy() if len(df) > 1 else df.copy()
    start = steady["log_time"].min()
    steady["elapsed_seconds"] = (steady["log_time"] - start).dt.total_seconds()
    steady["elapsed_minutes"] = steady["elapsed_seconds"] / 60.0

    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax.plot(steady["elapsed_minutes"], steady["rate_req_per_sec"], color="#1f77b4", linewidth=1.8, label="10-second samples")
    ax.axhline(
        stats["steady_mean"],
        color="#2ca02c",
        linestyle=":",
        linewidth=1.8,
        label=f"steady-state mean {stats['steady_mean']:.1f} req/s",
    )
    ax.set_title("Riposte Steady-State Throughput Over Time")
    ax.set_xlabel("Elapsed time (minutes)")
    ax.set_ylabel("Throughput (requests/sec)")
    ax.grid(True, alpha=0.25)
    ax.legend(frameon=False, loc="lower right")
    ax.text(
        0.01,
        0.96,
        f"First warm-up sample excluded. Spread: {stats['steady_min']:.1f}-{stats['steady_max']:.1f} req/s",
        transform=ax.transAxes,
        fontsize=9,
        color="#444444",
        va="top",
    )
    return _save_png(fig, out)


def plot_throughput_distribution(results_dir: str | Path) -> Path:
    df = load_measured_throughput(results_dir)
    stats = throughput_summary(df)
    out = figures_dir(results_dir) / "throughput-distribution.png"

    steady = df.iloc[1:].copy() if len(df) > 1 else df.copy()
    values = steady["rate_req_per_sec"]
    bins = np.linspace(np.floor(values.min()), np.ceil(values.max()), 18)

    fig, ax = plt.subplots(figsize=(7, 4.8))
    ax.hist(values, bins=bins, color="#4c78a8", edgecolor="white")
    ax.axvline(
        stats["steady_mean"],
        color="#2ca02c",
        linestyle=":",
        linewidth=2.2,
        label=f"steady mean {stats['steady_mean']:.1f} req/s",
    )
    ax.axvline(
        stats["mean"],
        color="#d62728",
        linestyle="--",
        linewidth=1.4,
        label=f"all-sample mean {stats['mean']:.1f} req/s",
    )
    ax.set_title("Steady-State Throughput Distribution")
    ax.set_xlabel("Throughput (requests/sec)")
    ax.set_ylabel("10-second samples")
    ax.grid(True, axis="y", alpha=0.25)
    ax.legend(frameon=False, loc="upper right")
    fig.subplots_adjust(bottom=0.22)
    fig.text(
        0.5,
        0.04,
        f"Zoomed to {len(steady)} samples after excluding the first warm-up sample "
        f"({df.loc[0, 'rate_req_per_sec']:.1f} req/s).",
        ha="center",
        fontsize=9,
        color="#444444",
    )
    return _save_png(fig, out)


def plot_aes_reference(results_dir: str | Path) -> Path:
    aes = aes_reference_summary(load_aes_speed(results_dir))
    out = figures_dir(results_dir) / "aes-reference.png"

    fig, ax = plt.subplots(figsize=(6, 4.2))
    ax.bar(aes["server"], aes["gb_per_sec"], color=["#9467bd", "#8c564b"])
    ax.set_title("Idle AES-128-CTR Reference Throughput")
    ax.set_xlabel("Server")
    ax.set_ylabel("GB/sec at 16 KiB block size")
    low = max(0.0, float(aes["gb_per_sec"].min()) - 0.01)
    high = float(aes["gb_per_sec"].max()) + 0.01
    ax.set_ylim(low, high)
    ax.grid(True, axis="y", alpha=0.25)
    for idx, row in aes.iterrows():
        ax.text(idx, row["gb_per_sec"], f"{row['gb_per_sec']:.4f}", ha="center", va="bottom", fontsize=10)
    delta = float(aes["gb_per_sec"].max() - aes["gb_per_sec"].min())
    ax.text(
        0.5,
        -0.22,
        f"Y-axis zoomed to show server-to-server delta ({delta:.4f} GB/s). "
        "Upper-bound crypto reference only.",
        transform=ax.transAxes,
        ha="center",
        fontsize=9,
        color="#444444",
    )
    return _save_png(fig, out)


def write_summary_table(results_dir: str | Path) -> Path:
    df = load_measured_throughput(results_dir)
    stats = throughput_summary(df)
    metadata = load_metadata(results_dir)
    aes = aes_reference_summary(load_aes_speed(results_dir))
    out = figures_dir(results_dir) / "summary-table.md"

    aes_values = ", ".join(
        f"{row.server}: {row.gb_per_sec:.2f} GB/s"
        for row in aes.itertuples(index=False)
    )
    duration = stats["duration_minutes"] if stats["duration_minutes"] is not None else 0.0

    lines = [
        "| Metric | Value |",
        "| --- | ---: |",
        f"| Duration covered by samples | {duration:.2f} min |",
        f"| Samples | {stats['samples']:,} |",
        f"| Mean throughput, all samples | {stats['mean']:.3f} req/s |",
        f"| Stddev, all samples | {stats['stddev']:.3f} req/s |",
        f"| Min throughput | {stats['min']:.3f} req/s |",
        f"| Max throughput | {stats['max']:.3f} req/s |",
        f"| Mean throughput, excluding first sample | {stats['steady_mean']:.3f} req/s |",
        f"| Stddev, excluding first sample | {stats['steady_stddev']:.3f} req/s |",
        f"| Interval requests | {stats['interval_requests']:,} |",
        f"| Final total requests | {stats['final_total']:,} |",
        f"| Instance type | {metadata.get('instance_type', 'unknown')} |",
        f"| Threads per process | {metadata.get('threads', 'unknown')} |",
        f"| Table rows | {metadata.get('table_rows', 'unknown'):,} |",
        f"| Row bytes | {metadata.get('row_bytes', 'unknown')} |",
        f"| Git commit | `{metadata.get('git_commit', 'unknown')}` |",
        f"| AES-128-CTR reference, 16 KiB block | {aes_values} |",
    ]

    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(lines) + "\n")
    return out
