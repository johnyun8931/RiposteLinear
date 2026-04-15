#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <results-dir>" >&2
  exit 1
fi

RESULT_DIR="$1"
CSV="$RESULT_DIR/throughput.csv"
SUMMARY="$RESULT_DIR/throughput-summary.txt"

python3 - "$RESULT_DIR" "$CSV" "$SUMMARY" <<'PY'
import csv
import math
import re
import statistics
import sys
from pathlib import Path

result_dir = Path(sys.argv[1])
csv_path = Path(sys.argv[2])
summary_path = Path(sys.argv[3])

pattern = re.compile(
    r"(?P<time>\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}).*"
    r"Served (?P<count>\d+) requests at (?P<rate>[0-9.]+) reqs/sec "
    r"\[since start: (?P<total>\d+)\]"
)

rows = []

for log_path in sorted(result_dir.rglob("server-*.log")):
    parts = log_path.parts
    phase = "unknown"
    for candidate in ("smoke", "sanity-10m", "measured-30m"):
        if candidate in parts:
            phase = candidate
            break

    remote = "unknown"
    if "remotes" in parts:
        idx = parts.index("remotes")
        if idx + 1 < len(parts):
            remote = parts[idx + 1]

    server = log_path.stem

    with log_path.open(errors="replace") as fh:
        for line in fh:
            match = pattern.search(line)
            if not match:
                continue
            rows.append({
                "phase": phase,
                "remote": remote,
                "server": server,
                "log_time": match.group("time"),
                "interval_requests": int(match.group("count")),
                "rate_req_per_sec": float(match.group("rate")),
                "total_since_start": int(match.group("total")),
                "source_file": str(log_path.relative_to(result_dir)),
            })

csv_path.parent.mkdir(parents=True, exist_ok=True)
with csv_path.open("w", newline="") as fh:
    writer = csv.DictWriter(fh, fieldnames=[
        "phase",
        "remote",
        "server",
        "log_time",
        "interval_requests",
        "rate_req_per_sec",
        "total_since_start",
        "source_file",
    ])
    writer.writeheader()
    writer.writerows(rows)

def summarize(group_rows):
    if not group_rows:
        return None
    values = [row["rate_req_per_sec"] for row in group_rows]
    return {
        "samples": len(values),
        "mean": statistics.fmean(values),
        "stddev": statistics.pstdev(values) if len(values) > 1 else 0.0,
        "min": min(values),
        "max": max(values),
        "interval_requests": sum(row["interval_requests"] for row in group_rows),
        "final_total": max(row["total_since_start"] for row in group_rows),
    }

groups = {}
for row in rows:
    key = (row["phase"], row["server"])
    groups.setdefault(key, []).append(row)

with summary_path.open("w") as fh:
    if not rows:
        fh.write("No throughput samples found.\n")
    for key in sorted(groups):
        stats = summarize(groups[key])
        if stats is None:
            continue
        phase, server = key
        fh.write(
            f"{phase} {server}: "
            f"samples={stats['samples']} "
            f"mean={stats['mean']:.3f} "
            f"stddev={stats['stddev']:.3f} "
            f"min={stats['min']:.3f} "
            f"max={stats['max']:.3f} "
            f"interval_requests={stats['interval_requests']} "
            f"final_total={stats['final_total']}\n"
        )

print(f"wrote {csv_path}")
print(f"wrote {summary_path}")
print(f"parsed {len(rows)} throughput samples")
PY
