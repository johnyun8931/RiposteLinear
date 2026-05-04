#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <results-dir>" >&2
  exit 1
fi

RESULT_DIR="$1"

python3 - "$RESULT_DIR" <<'PY'
import csv
from datetime import datetime
import json
import re
import sys
from pathlib import Path

result_dir = Path(sys.argv[1])
metadata_path = result_dir / "metadata.json"
baseline_csv = result_dir / "baseline-throughput.csv"
sharded_csv = result_dir / "sharded-throughput.csv"
summary_md = result_dir / "comparison-summary.md"

if not metadata_path.exists():
    raise SystemExit(f"missing metadata.json at {metadata_path}")

metadata = json.loads(metadata_path.read_text())
measured_seconds = int(metadata["config"]["measured_epoch_seconds"])
client_exit_grace_seconds = int(metadata["config"].get("client_exit_grace_seconds", 30))
client_concurrency = int(metadata["config"].get("client_concurrency", 16))

pattern = re.compile(
    r"(?P<time>\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}).*"
    r"Served (?P<count>\d+) requests at (?P<rate>[0-9.]+) reqs/sec "
    r"\[since start: (?P<total>\d+)\]"
)

def parse_log_rows(log_path: Path, phase: str, role: str):
    rows = []
    with log_path.open(errors="replace") as fh:
        for line in fh:
            match = pattern.search(line)
            if not match:
                continue
            rows.append({
                "phase": phase,
                "role": role,
                "log_time": match.group("time"),
                "interval_requests": int(match.group("count")),
                "rate_req_per_sec": float(match.group("rate")),
                "total_since_start": int(match.group("total")),
                "source_file": str(log_path.relative_to(result_dir)),
            })
    return rows

def parse_client_log(log_path: Path):
    if not log_path.exists():
        return {
            "exists": False,
            "has_no_active_epoch": False,
            "has_unexpected_eof": False,
            "has_overload": False,
            "last_sent": None,
            "last_sent_time": "",
        }

    sent_pattern = re.compile(r"(?P<time>\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}).*Sent (?P<count>\d+) requests")
    result = {
        "exists": True,
        "has_no_active_epoch": False,
        "has_unexpected_eof": False,
        "has_overload": False,
        "last_sent": None,
        "last_sent_time": "",
    }
    with log_path.open(errors="replace") as fh:
        for line in fh:
            if "No active epoch" in line:
                result["has_no_active_epoch"] = True
            if "unexpected EOF" in line:
                result["has_unexpected_eof"] = True
            if "server overloaded: ready queue full" in line:
                result["has_overload"] = True
            match = sent_pattern.search(line)
            if match:
                result["last_sent"] = int(match.group("count"))
                result["last_sent_time"] = match.group("time")
    return result

def load_phase_status(phase: str):
    status_path = result_dir / "remotes" / "client" / "riposte-eval" / "phases" / phase / "phase-status.json"
    if not status_path.exists():
        return None
    return json.loads(status_path.read_text())

def parse_time(value: str):
    return datetime.strptime(value, "%Y/%m/%d %H:%M:%S")

def sample_interval_seconds(rows):
    times = [parse_time(row["log_time"]) for row in rows]
    deltas = []
    for prev, cur in zip(times, times[1:]):
        delta = int((cur - prev).total_seconds())
        if delta > 0:
            deltas.append(delta)
    if not deltas:
        return 10
    deltas.sort()
    return deltas[len(deltas) // 2]

def role_activity(rows):
    if not rows:
        return {
            "nonzero_samples": 0,
            "first_nonzero_index": None,
            "last_nonzero_index": None,
            "active_until_seconds": 0,
        }
    interval = sample_interval_seconds(rows)
    nonzero = [idx for idx, row in enumerate(rows) if row["interval_requests"] > 0]
    if not nonzero:
        return {
            "nonzero_samples": 0,
            "first_nonzero_index": None,
            "last_nonzero_index": None,
            "active_until_seconds": 0,
        }
    return {
        "nonzero_samples": len(nonzero),
        "first_nonzero_index": nonzero[0],
        "last_nonzero_index": nonzero[-1],
        "active_until_seconds": (nonzero[-1] + 1) * interval,
    }

baseline_rows = []
sharded_rows = []

log_specs = [
    ("baseline-measured", "shard0-leader", result_dir / "remotes" / "shard0-leader" / "riposte-eval" / "phases" / "baseline-measured" / "logs" / "shard0-leader.log"),
    ("sharded-measured", "shard0-leader", result_dir / "remotes" / "shard0-leader" / "riposte-eval" / "phases" / "sharded-measured" / "logs" / "shard0-leader.log"),
    ("sharded-measured", "shard1-leader", result_dir / "remotes" / "shard1-leader" / "riposte-eval" / "phases" / "sharded-measured" / "logs" / "shard1-leader.log"),
]

for phase, role, path in log_specs:
    if not path.exists():
        raise SystemExit(f"missing expected log file: {path}")
    rows = parse_log_rows(path, phase, role)
    if not rows:
        raise SystemExit(f"no throughput samples found in {path}")
    if phase == "baseline-measured":
        baseline_rows.extend(rows)
    else:
        sharded_rows.extend(rows)

def write_csv(path: Path, rows):
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "phase",
                "role",
                "log_time",
                "interval_requests",
                "rate_req_per_sec",
                "total_since_start",
                "source_file",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)

write_csv(baseline_csv, baseline_rows)
write_csv(sharded_csv, sharded_rows)

def final_total(rows, role):
    role_rows = [row for row in rows if row["role"] == role]
    if not role_rows:
        raise SystemExit(f"missing rows for role {role}")
    return max(row["total_since_start"] for row in role_rows)

def rows_for_role(rows, role):
    return [row for row in rows if row["role"] == role]

def evaluate_phase(phase, rows, roles):
    reasons = []
    warnings = []
    client_log_path = result_dir / "remotes" / "client" / "riposte-eval" / "phases" / phase / "logs" / "client.log"
    client_log = parse_client_log(client_log_path)
    status = load_phase_status(phase)

    if status is not None and not status.get("valid", False):
        status_reason = status.get("invalid_reason") or status.get("client_exit_reason") or "phase status marked invalid"
        reasons.append(f"phase status invalid: {status_reason}")
    if status is None:
        warnings.append("phase status file missing")

    if not client_log["exists"]:
        reasons.append("client log missing")
    if client_log["has_unexpected_eof"]:
        reasons.append("client log contains unexpected EOF")
    if client_log["has_overload"]:
        reasons.append("client log contains server overload")
    if not client_log["has_no_active_epoch"]:
        reasons.append("client log does not contain No active epoch")

    threshold = measured_seconds * 0.80
    active_until = 0
    role_activity_rows = {}
    for role in roles:
        activity = role_activity(rows_for_role(rows, role))
        role_activity_rows[role] = activity
        active_until = max(active_until, activity["active_until_seconds"])
    if active_until < threshold:
        reasons.append(f"accepted traffic stopped around {active_until}s, before 80% of the {measured_seconds}s measured epoch")

    return {
        "valid": len(reasons) == 0,
        "reasons": reasons,
        "warnings": warnings,
        "client_log": client_log,
        "status": status,
        "active_until_seconds": active_until,
        "role_activity": role_activity_rows,
    }

baseline_total = final_total(baseline_rows, "shard0-leader")
shard0_total = final_total(sharded_rows, "shard0-leader")
shard1_total = final_total(sharded_rows, "shard1-leader")
sharded_total = shard0_total + shard1_total

baseline_req_per_sec = baseline_total / measured_seconds
sharded_req_per_sec = sharded_total / measured_seconds
delta = sharded_total - baseline_total
delta_pct = 0.0 if baseline_total == 0 else (delta / baseline_total) * 100.0
baseline_eval = evaluate_phase("baseline-measured", baseline_rows, ["shard0-leader"])
sharded_eval = evaluate_phase("sharded-measured", sharded_rows, ["shard0-leader", "shard1-leader"])

if sharded_total > 0:
    shard0_share = shard0_total / sharded_total
    shard1_share = shard1_total / sharded_total
    if min(shard0_share, shard1_share) < 0.25:
        sharded_eval["warnings"].append(
            f"sharded traffic skew: shard0={shard0_share:.1%}, shard1={shard1_share:.1%}"
        )

both_valid = baseline_eval["valid"] and sharded_eval["valid"]
winner = "unavailable"
if both_valid:
    winner = "sharded" if sharded_total > baseline_total else "baseline" if sharded_total < baseline_total else "tie"

def format_reasons(items):
    if not items:
        return "`none`"
    return "; ".join(f"`{item}`" for item in items)

summary_md.write_text(
    "\n".join(
        [
            "# AWS Throughput Comparison",
            "",
            f"- measured epoch seconds: `{measured_seconds}`",
            f"- client exit grace seconds: `{client_exit_grace_seconds}`",
            f"- client concurrency: `{client_concurrency}`",
            f"- baseline valid: `{str(baseline_eval['valid']).lower()}`",
            f"- baseline invalid reasons: {format_reasons(baseline_eval['reasons'])}",
            f"- sharded valid: `{str(sharded_eval['valid']).lower()}`",
            f"- sharded invalid reasons: {format_reasons(sharded_eval['reasons'])}",
            f"- baseline total / req/sec: `{baseline_total}` / `{baseline_req_per_sec:.2f}`",
            f"- shard 0 total: `{shard0_total}`",
            f"- shard 1 total: `{shard1_total}`",
            f"- sharded total / req/sec: `{sharded_total}` / `{sharded_req_per_sec:.2f}`",
            f"- delta: `{delta}` ({delta_pct:.2f}%)",
            f"- winner: `{winner}`",
            "",
            "## Validity Details",
            "",
            f"- baseline active-until estimate: `{baseline_eval['active_until_seconds']}s`",
            f"- sharded active-until estimate: `{sharded_eval['active_until_seconds']}s`",
            f"- baseline warnings: {format_reasons(baseline_eval['warnings'])}",
            f"- sharded warnings: {format_reasons(sharded_eval['warnings'])}",
            "",
            "## Artifacts",
            "",
            f"- [baseline-throughput.csv]({baseline_csv})",
            f"- [sharded-throughput.csv]({sharded_csv})",
            f"- [metadata.json]({metadata_path})",
        ]
    )
    + "\n"
)

print(f"wrote {baseline_csv}")
print(f"wrote {sharded_csv}")
print(f"wrote {summary_md}")
PY
