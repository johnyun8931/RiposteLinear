#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
USER_SERVER_THREADS="${SERVER_THREADS-}"
USER_CLIENT_THREADS="${CLIENT_THREADS-}"
USER_CLIENT_RETRY_OVERLOAD="${CLIENT_RETRY_OVERLOAD-}"
USER_CLIENT_OVERLOAD_BACKOFF_INITIAL_MS="${CLIENT_OVERLOAD_BACKOFF_INITIAL_MS-}"
USER_CLIENT_OVERLOAD_BACKOFF_MAX_MS="${CLIENT_OVERLOAD_BACKOFF_MAX_MS-}"
USER_WARMUP_EPOCH_SECONDS="${WARMUP_EPOCH_SECONDS-}"
USER_MEASURED_EPOCH_SECONDS="${MEASURED_EPOCH_SECONDS-}"
USER_CLIENT_EXIT_GRACE_SECONDS="${CLIENT_EXIT_GRACE_SECONDS-}"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"
load_state

require_cmd python3

SWEEP_ID="${SWEEP_ID:-$(date -u +%Y%m%dT%H%M%SZ)-concurrency-sweep}"
CLIENT_CONCURRENCY_SWEEP="${CLIENT_CONCURRENCY_SWEEP:-1 2 4 8 16}"
SWEEP_SERVER_THREADS="${USER_SERVER_THREADS:-2}"
SWEEP_CLIENT_THREADS="${USER_CLIENT_THREADS:-1}"
SWEEP_CLIENT_RETRY_OVERLOAD="${USER_CLIENT_RETRY_OVERLOAD:-$CLIENT_RETRY_OVERLOAD}"
SWEEP_CLIENT_OVERLOAD_BACKOFF_INITIAL_MS="${USER_CLIENT_OVERLOAD_BACKOFF_INITIAL_MS:-$CLIENT_OVERLOAD_BACKOFF_INITIAL_MS}"
SWEEP_CLIENT_OVERLOAD_BACKOFF_MAX_MS="${USER_CLIENT_OVERLOAD_BACKOFF_MAX_MS:-$CLIENT_OVERLOAD_BACKOFF_MAX_MS}"
SWEEP_WARMUP_SECONDS="${USER_WARMUP_EPOCH_SECONDS:-10}"
SWEEP_MEASURED_SECONDS="${USER_MEASURED_EPOCH_SECONDS:-45}"
SWEEP_CLIENT_EXIT_GRACE_SECONDS="${USER_CLIENT_EXIT_GRACE_SECONDS:-30}"
SWEEP_OUT_DIR="$RESULTS_DIR/$SWEEP_ID"
SUMMARY_TSV="$SWEEP_OUT_DIR/concurrency-summary.tsv"
SUMMARY_MD="$SWEEP_OUT_DIR/concurrency-summary.md"

mkdir -p "$SWEEP_OUT_DIR"

printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
  "client_concurrency" "client_retry_overload" "benchmark_exit_status" "collect_exit_status" "result_id" \
  "baseline_warmup_valid" "baseline_measured_valid" "sharded_warmup_valid" "sharded_measured_valid" \
  "invalid_reasons" "winner" "baseline_total" "sharded_total" "delta" >"$SUMMARY_TSV"

append_summary_row() {
  local result_dir="$1"
  local concurrency="$2"
  local benchmark_status="$3"
  local collect_status="$4"
  local result_id="$5"

  python3 - "$result_dir" "$concurrency" "$SWEEP_CLIENT_RETRY_OVERLOAD" "$benchmark_status" "$collect_status" "$result_id" >>"$SUMMARY_TSV" <<'PY'
import json
import re
import sys
from pathlib import Path

result_dir = Path(sys.argv[1])
concurrency = sys.argv[2]
client_retry_overload = sys.argv[3]
benchmark_status = sys.argv[4]
collect_status = sys.argv[5]
result_id = sys.argv[6]

phases = [
    "baseline-warmup",
    "baseline-measured",
    "sharded-warmup",
    "sharded-measured",
]

valid = {}
reasons = []
for phase in phases:
    path = result_dir / "remotes" / "client" / "riposte-eval" / "phases" / phase / "phase-status.json"
    if not path.exists():
        valid[phase] = "missing"
        continue
    status = json.loads(path.read_text())
    valid[phase] = str(bool(status.get("valid", False))).lower()
    if not status.get("valid", False):
        reason = status.get("client_exit_reason") or status.get("invalid_reason") or "invalid"
        valid_reason = f"{phase}:{reason}"
        if valid_reason not in reasons:
            reasons.append(valid_reason)

winner = ""
baseline_total = ""
sharded_total = ""
delta = ""
summary = result_dir / "comparison-summary.md"
if summary.exists():
    text = summary.read_text()
    match = re.search(r"- winner: `([^`]*)`", text)
    if match:
        winner = match.group(1)
    match = re.search(r"- baseline total / req/sec: `([^`]*)`", text)
    if match:
        baseline_total = match.group(1)
    match = re.search(r"- sharded total / req/sec: `([^`]*)`", text)
    if match:
        sharded_total = match.group(1)
    match = re.search(r"- delta: `([^`]*)`", text)
    if match:
        delta = match.group(1)

row = [
    concurrency,
    client_retry_overload,
    benchmark_status,
    collect_status,
    result_id,
    valid["baseline-warmup"],
    valid["baseline-measured"],
    valid["sharded-warmup"],
    valid["sharded-measured"],
    ";".join(reasons),
    winner,
    baseline_total,
    sharded_total,
    delta,
]
print("\t".join(row))
PY
}

for concurrency in $CLIENT_CONCURRENCY_SWEEP; do
  result_id="${SWEEP_ID}-c${concurrency}"
  result_dir="$RESULTS_DIR/$result_id"
  info "running AWS short benchmark for client concurrency=$concurrency"

  set +e
  RESULT_ID="$result_id" \
  SERVER_THREADS="$SWEEP_SERVER_THREADS" \
  CLIENT_THREADS="$SWEEP_CLIENT_THREADS" \
  CLIENT_CONCURRENCY="$concurrency" \
  CLIENT_RETRY_OVERLOAD="$SWEEP_CLIENT_RETRY_OVERLOAD" \
  CLIENT_OVERLOAD_BACKOFF_INITIAL_MS="$SWEEP_CLIENT_OVERLOAD_BACKOFF_INITIAL_MS" \
  CLIENT_OVERLOAD_BACKOFF_MAX_MS="$SWEEP_CLIENT_OVERLOAD_BACKOFF_MAX_MS" \
  WARMUP_EPOCH_SECONDS="$SWEEP_WARMUP_SECONDS" \
  MEASURED_EPOCH_SECONDS="$SWEEP_MEASURED_SECONDS" \
  CLIENT_EXIT_GRACE_SECONDS="$SWEEP_CLIENT_EXIT_GRACE_SECONDS" \
    "$SCRIPT_DIR/04-run-benchmark.sh"
  benchmark_status=$?

  RESULT_ID="$result_id" "$SCRIPT_DIR/05-collect-logs.sh"
  collect_status=$?
  set -e

  append_summary_row "$result_dir" "$concurrency" "$benchmark_status" "$collect_status" "$result_id"
done

python3 - "$SUMMARY_TSV" >"$SUMMARY_MD" <<'PY'
import csv
import sys

rows = list(csv.DictReader(open(sys.argv[1]), delimiter="\t"))

print("# AWS Client Concurrency Sweep")
print()
print("| client_concurrency | client_retry_overload | benchmark_exit_status | baseline_warmup | baseline_measured | sharded_warmup | sharded_measured | winner | baseline_total | sharded_total | delta | invalid_reasons |")
print("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
for row in rows:
    print(
        f"| {row['client_concurrency']} | {row['client_retry_overload']} | {row['benchmark_exit_status']} | "
        f"{row['baseline_warmup_valid']} | {row['baseline_measured_valid']} | "
        f"{row['sharded_warmup_valid']} | {row['sharded_measured_valid']} | "
        f"{row['winner']} | {row['baseline_total']} | {row['sharded_total']} | "
        f"{row['delta']} | {row['invalid_reasons']} |"
    )
PY

cat <<EOF
AWS client concurrency sweep complete.
  sweep id: $SWEEP_ID
  summary TSV: $SUMMARY_TSV
  summary MD:  $SUMMARY_MD
EOF
