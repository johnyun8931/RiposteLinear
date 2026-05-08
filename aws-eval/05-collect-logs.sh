#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"
load_state
if [[ -f "$BENCHMARK_STATE_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$BENCHMARK_STATE_FILE"
fi

require_cmd scp
require_cmd git
require_cmd python3
require_cmd aws

RESULT_ID="${RESULT_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
OUT_DIR="$RESULTS_DIR/$RESULT_ID"
mkdir -p "$OUT_DIR/remotes" "$OUT_DIR/leader-results"
if public_entry_enabled; then
  capture_nlb_artifacts "$OUT_DIR/aws/nlb"
fi
if sqs_ingestion_enabled; then
  capture_ingestion_artifacts "$OUT_DIR/aws/ingestion"
fi

copy_role_tree() {
  local label="$1"
  local host="$2"
  local dst="$OUT_DIR/remotes/$label"
  mkdir -p "$dst"
  info "copying logs/results from $label"
  copy_remote_tree_if_present "$label" "$host" "$REMOTE_ROOT" "$dst"
}

copy_role_tree "coordinator" "$COORDINATOR_PUBLIC_IP"
copy_role_tree "shard0-leader" "$SHARD0_LEADER_PUBLIC_IP"
copy_role_tree "shard0-follower" "$SHARD0_FOLLOWER_PUBLIC_IP"
copy_role_tree "shard1-leader" "$SHARD1_LEADER_PUBLIC_IP"
copy_role_tree "shard1-follower" "$SHARD1_FOLLOWER_PUBLIC_IP"
copy_role_tree "client" "$CLIENT_PUBLIC_IP"

cp "$STATE_FILE" "$OUT_DIR/state-env.sh"
if [[ -f "$BENCHMARK_STATE_FILE" ]]; then
  cp "$BENCHMARK_STATE_FILE" "$OUT_DIR/benchmark-env.sh"
fi

while IFS= read -r json_file; do
  rel="${json_file#$OUT_DIR/remotes/}"
  role="${rel%%/*}"
  phase="$(basename "$(dirname "$(dirname "$(dirname "$json_file")")")")"
  cp "$json_file" "$OUT_DIR/leader-results/${role}-${phase}-$(basename "$json_file")"
done < <(find "$OUT_DIR/remotes" -type f -name 'epoch-*.json' | sort)

GIT_COMMIT="$(cd "$REPO_ROOT" && git rev-parse HEAD)"
GIT_BRANCH="$(cd "$REPO_ROOT" && git rev-parse --abbrev-ref HEAD)"
AWS_IDENTITY="$(aws_base sts get-caller-identity --output json 2>/dev/null || echo '{}')"

RESULT_ID="$RESULT_ID" \
GIT_COMMIT="$GIT_COMMIT" \
GIT_BRANCH="$GIT_BRANCH" \
AWS_REGION="$AWS_REGION" \
PROJECT_TAG="$PROJECT_TAG" \
RUN_ID="$RUN_ID" \
SELECTED_VPC_ID="$SELECTED_VPC_ID" \
SELECTED_SUBNET_ID="$SELECTED_SUBNET_ID" \
SELECTED_AZ="$SELECTED_AZ" \
AMI_ID="$AMI_ID" \
COORDINATOR_INSTANCE_TYPE="$COORDINATOR_INSTANCE_TYPE" \
SERVER_INSTANCE_TYPE="$SERVER_INSTANCE_TYPE" \
CLIENT_INSTANCE_TYPE="$CLIENT_INSTANCE_TYPE" \
COORDINATOR_PORT="$COORDINATOR_PORT" \
COORDINATOR_STANDBY_PORT="${COORDINATOR_STANDBY_PORT:-8631}" \
SHARD0_LEADER_PORT="$SHARD0_LEADER_PORT" \
SHARD0_FOLLOWER_PORT="$SHARD0_FOLLOWER_PORT" \
SHARD1_LEADER_PORT="$SHARD1_LEADER_PORT" \
SHARD1_FOLLOWER_PORT="$SHARD1_FOLLOWER_PORT" \
SERVER_THREADS="$SERVER_THREADS" \
CLIENT_THREADS="$CLIENT_THREADS" \
CLIENT_CONCURRENCY="$CLIENT_CONCURRENCY" \
CLIENT_RETRY_OVERLOAD="$CLIENT_RETRY_OVERLOAD" \
CLIENT_OVERLOAD_BACKOFF_INITIAL_MS="$CLIENT_OVERLOAD_BACKOFF_INITIAL_MS" \
CLIENT_OVERLOAD_BACKOFF_MAX_MS="$CLIENT_OVERLOAD_BACKOFF_MAX_MS" \
CONTROL_STORE_BACKEND="$CONTROL_STORE_BACKEND" \
DYNAMODB_CONTROL_TABLE="$DYNAMODB_CONTROL_TABLE" \
DYNAMODB_CONTROL_REGION="$(dynamodb_control_region)" \
SESSION_STORE_BACKEND="$SESSION_STORE_BACKEND" \
DYNAMODB_SESSION_TABLE="$(dynamodb_session_table)" \
DYNAMODB_SESSION_REGION="$(dynamodb_session_region)" \
COORDINATOR_HOLDER_ID="$(coordinator_holder_id)" \
COORDINATOR_LEASE_TTL_SECONDS="$COORDINATOR_LEASE_TTL_SECONDS" \
COORDINATOR_LEASE_RENEW_SECONDS="$COORDINATOR_LEASE_RENEW_SECONDS" \
WARMUP_EPOCH_SECONDS="$WARMUP_EPOCH_SECONDS" \
MEASURED_EPOCH_SECONDS="$MEASURED_EPOCH_SECONDS" \
START_EPOCH_RETRY_TIMEOUT="$START_EPOCH_RETRY_TIMEOUT" \
START_EPOCH_RETRY_INTERVAL="$START_EPOCH_RETRY_INTERVAL" \
POST_EPOCH_FLUSH_SECONDS="$POST_EPOCH_FLUSH_SECONDS" \
CLIENT_EXIT_GRACE_SECONDS="$CLIENT_EXIT_GRACE_SECONDS" \
PUBLIC_ENTRY_BACKEND="$PUBLIC_ENTRY_BACKEND" \
PUBLIC_ENTRY_MULTI_COORDINATOR="${PUBLIC_ENTRY_MULTI_COORDINATOR:-0}" \
INGESTION_QUEUE_BACKEND="$INGESTION_QUEUE_BACKEND" \
INGESTION_S3_BUCKET="${INGESTION_S3_BUCKET:-}" \
INGESTION_SQS_SHARD0_QUEUE_URL="${INGESTION_SQS_SHARD0_QUEUE_URL:-}" \
INGESTION_SQS_SHARD1_QUEUE_URL="${INGESTION_SQS_SHARD1_QUEUE_URL:-}" \
NLB_NAME="${NLB_NAME:-}" \
NLB_DNS_NAME="${NLB_DNS_NAME:-}" \
NLB_ARN="${NLB_ARN:-}" \
NLB_TARGET_GROUP_NAME="${NLB_TARGET_GROUP_NAME:-}" \
NLB_TARGET_GROUP_ARN="${NLB_TARGET_GROUP_ARN:-}" \
NLB_LISTENER_ARN="${NLB_LISTENER_ARN:-}" \
COORDINATOR_ID="$COORDINATOR_ID" \
COORDINATOR_PUBLIC_IP="$COORDINATOR_PUBLIC_IP" \
COORDINATOR_PRIVATE_IP="$COORDINATOR_PRIVATE_IP" \
SHARD0_LEADER_ID="$SHARD0_LEADER_ID" \
SHARD0_LEADER_PUBLIC_IP="$SHARD0_LEADER_PUBLIC_IP" \
SHARD0_LEADER_PRIVATE_IP="$SHARD0_LEADER_PRIVATE_IP" \
SHARD0_FOLLOWER_ID="$SHARD0_FOLLOWER_ID" \
SHARD0_FOLLOWER_PUBLIC_IP="$SHARD0_FOLLOWER_PUBLIC_IP" \
SHARD0_FOLLOWER_PRIVATE_IP="$SHARD0_FOLLOWER_PRIVATE_IP" \
SHARD1_LEADER_ID="$SHARD1_LEADER_ID" \
SHARD1_LEADER_PUBLIC_IP="$SHARD1_LEADER_PUBLIC_IP" \
SHARD1_LEADER_PRIVATE_IP="$SHARD1_LEADER_PRIVATE_IP" \
SHARD1_FOLLOWER_ID="$SHARD1_FOLLOWER_ID" \
SHARD1_FOLLOWER_PUBLIC_IP="$SHARD1_FOLLOWER_PUBLIC_IP" \
SHARD1_FOLLOWER_PRIVATE_IP="$SHARD1_FOLLOWER_PRIVATE_IP" \
CLIENT_ID="$CLIENT_ID" \
CLIENT_PUBLIC_IP="$CLIENT_PUBLIC_IP" \
CLIENT_PRIVATE_IP="$CLIENT_PRIVATE_IP" \
python3 - "$OUT_DIR/metadata.json" "$AWS_IDENTITY" <<'PY'
import json
import os
import sys

metadata_path = sys.argv[1]
aws_identity = json.loads(sys.argv[2])

payload = {
    "result_id": os.environ["RESULT_ID"],
    "git_commit": os.environ["GIT_COMMIT"],
    "git_branch": os.environ["GIT_BRANCH"],
    "aws_region": os.environ["AWS_REGION"],
    "aws_identity": aws_identity,
    "project_tag": os.environ["PROJECT_TAG"],
    "run_id": os.environ["RUN_ID"],
    "selected_vpc_id": os.environ["SELECTED_VPC_ID"],
    "selected_subnet_id": os.environ["SELECTED_SUBNET_ID"],
    "selected_az": os.environ["SELECTED_AZ"],
    "ami_id": os.environ["AMI_ID"],
    "instance_types": {
        "coordinator": os.environ["COORDINATOR_INSTANCE_TYPE"],
        "server": os.environ["SERVER_INSTANCE_TYPE"],
        "client": os.environ["CLIENT_INSTANCE_TYPE"],
    },
    "ports": {
        "coordinator": int(os.environ["COORDINATOR_PORT"]),
        "coordinator_standby": int(os.environ["COORDINATOR_STANDBY_PORT"]),
        "shard0_leader": int(os.environ["SHARD0_LEADER_PORT"]),
        "shard0_follower": int(os.environ["SHARD0_FOLLOWER_PORT"]),
        "shard1_leader": int(os.environ["SHARD1_LEADER_PORT"]),
        "shard1_follower": int(os.environ["SHARD1_FOLLOWER_PORT"]),
    },
    "config": {
        "server_threads": int(os.environ["SERVER_THREADS"]),
        "client_threads": int(os.environ["CLIENT_THREADS"]),
        "client_concurrency": int(os.environ["CLIENT_CONCURRENCY"]),
        "client_retry_overload": os.environ["CLIENT_RETRY_OVERLOAD"],
        "client_overload_backoff_initial_ms": int(os.environ["CLIENT_OVERLOAD_BACKOFF_INITIAL_MS"]),
        "client_overload_backoff_max_ms": int(os.environ["CLIENT_OVERLOAD_BACKOFF_MAX_MS"]),
        "control_store_backend": os.environ["CONTROL_STORE_BACKEND"],
        "dynamodb_control_table": os.environ["DYNAMODB_CONTROL_TABLE"],
        "dynamodb_control_region": os.environ["DYNAMODB_CONTROL_REGION"],
        "session_store_backend": os.environ["SESSION_STORE_BACKEND"],
        "dynamodb_session_table": os.environ["DYNAMODB_SESSION_TABLE"],
        "dynamodb_session_region": os.environ["DYNAMODB_SESSION_REGION"],
        "coordinator_holder_id": os.environ["COORDINATOR_HOLDER_ID"],
        "coordinator_lease_ttl_seconds": int(os.environ["COORDINATOR_LEASE_TTL_SECONDS"]),
        "coordinator_lease_renew_seconds": int(os.environ["COORDINATOR_LEASE_RENEW_SECONDS"]),
        "warmup_epoch_seconds": int(os.environ["WARMUP_EPOCH_SECONDS"]),
        "measured_epoch_seconds": int(os.environ["MEASURED_EPOCH_SECONDS"]),
        "start_epoch_retry_timeout": int(os.environ["START_EPOCH_RETRY_TIMEOUT"]),
        "start_epoch_retry_interval": int(os.environ["START_EPOCH_RETRY_INTERVAL"]),
        "post_epoch_flush_seconds": int(os.environ["POST_EPOCH_FLUSH_SECONDS"]),
        "client_exit_grace_seconds": int(os.environ["CLIENT_EXIT_GRACE_SECONDS"]),
        "public_entry_backend": os.environ["PUBLIC_ENTRY_BACKEND"],
        "public_entry_multi_coordinator": os.environ["PUBLIC_ENTRY_MULTI_COORDINATOR"],
        "ingestion_queue_backend": os.environ["INGESTION_QUEUE_BACKEND"],
        "ingestion_s3_bucket": os.environ["INGESTION_S3_BUCKET"],
        "ingestion_sqs_shard0_queue_url": os.environ["INGESTION_SQS_SHARD0_QUEUE_URL"],
        "ingestion_sqs_shard1_queue_url": os.environ["INGESTION_SQS_SHARD1_QUEUE_URL"],
        "nlb": {
            "name": os.environ["NLB_NAME"],
            "dns_name": os.environ["NLB_DNS_NAME"],
            "arn": os.environ["NLB_ARN"],
            "target_group_name": os.environ["NLB_TARGET_GROUP_NAME"],
            "target_group_arn": os.environ["NLB_TARGET_GROUP_ARN"],
            "listener_arn": os.environ["NLB_LISTENER_ARN"],
        },
    },
    "instances": {
        "coordinator": {
            "id": os.environ["COORDINATOR_ID"],
            "public_ip": os.environ["COORDINATOR_PUBLIC_IP"],
            "private_ip": os.environ["COORDINATOR_PRIVATE_IP"],
        },
        "shard0_leader": {
            "id": os.environ["SHARD0_LEADER_ID"],
            "public_ip": os.environ["SHARD0_LEADER_PUBLIC_IP"],
            "private_ip": os.environ["SHARD0_LEADER_PRIVATE_IP"],
        },
        "shard0_follower": {
            "id": os.environ["SHARD0_FOLLOWER_ID"],
            "public_ip": os.environ["SHARD0_FOLLOWER_PUBLIC_IP"],
            "private_ip": os.environ["SHARD0_FOLLOWER_PRIVATE_IP"],
        },
        "shard1_leader": {
            "id": os.environ["SHARD1_LEADER_ID"],
            "public_ip": os.environ["SHARD1_LEADER_PUBLIC_IP"],
            "private_ip": os.environ["SHARD1_LEADER_PRIVATE_IP"],
        },
        "shard1_follower": {
            "id": os.environ["SHARD1_FOLLOWER_ID"],
            "public_ip": os.environ["SHARD1_FOLLOWER_PUBLIC_IP"],
            "private_ip": os.environ["SHARD1_FOLLOWER_PRIVATE_IP"],
        },
        "client": {
            "id": os.environ["CLIENT_ID"],
            "public_ip": os.environ["CLIENT_PUBLIC_IP"],
            "private_ip": os.environ["CLIENT_PRIVATE_IP"],
        },
    },
}

with open(metadata_path, "w") as fh:
    json.dump(payload, fh, indent=2, sort_keys=True)
    fh.write("\n")
PY

BASELINE_MEASURED_LOG="$OUT_DIR/remotes/shard0-leader/riposte-eval/phases/baseline-measured/logs/shard0-leader.log"
SHARDED0_MEASURED_LOG="$OUT_DIR/remotes/shard0-leader/riposte-eval/phases/sharded-measured/logs/shard0-leader.log"
SHARDED1_MEASURED_LOG="$OUT_DIR/remotes/shard1-leader/riposte-eval/phases/sharded-measured/logs/shard1-leader.log"

if [[ -f "$BASELINE_MEASURED_LOG" && -f "$SHARDED0_MEASURED_LOG" && -f "$SHARDED1_MEASURED_LOG" ]]; then
  "$SCRIPT_DIR/parse-throughput.sh" "$OUT_DIR"
else
  python3 - "$OUT_DIR" "$OUT_DIR/comparison-summary.md" <<'PY'
import json
import sys
from pathlib import Path

out_dir = Path(sys.argv[1])
summary_path = Path(sys.argv[2])
phases = [
    "baseline-warmup",
    "baseline-measured",
    "sharded-warmup",
    "sharded-measured",
]

lines = [
    "# AWS Throughput Comparison",
    "",
    "The benchmark did not run all measured phases, so no throughput comparison was parsed.",
    "",
    "| phase | valid | client_exit_reason | invalid_reason |",
    "| --- | --- | --- | --- |",
]

def load_json(path):
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as err:
        return {"valid": False, "invalid_reason": f"invalid_json:{err}"}

for phase in phases:
    status_path = out_dir / "remotes" / "client" / "riposte-eval" / "phases" / phase / "phase-status.json"
    if not status_path.exists():
        lines.append(f"| {phase} | missing |  |  |")
        continue
    status = load_json(status_path)
    lines.append(
        f"| {phase} | {str(bool(status.get('valid', False))).lower()} | "
        f"{status.get('client_exit_reason', '')} | {status.get('invalid_reason', '')} |"
    )

lines.extend([
    "",
    "Inspect copied remote logs and phase-status files under:",
    "",
    "```text",
    f"{out_dir}/remotes/",
    "```",
    "",
])

summary_path.write_text("\n".join(lines))
PY
  echo "warning: measured logs are incomplete; wrote partial-run summary to $OUT_DIR/comparison-summary.md" >&2
fi

cat <<EOF
results collected in $OUT_DIR
  metadata:            $OUT_DIR/metadata.json
  baseline samples:    $OUT_DIR/baseline-throughput.csv
  sharded samples:     $OUT_DIR/sharded-throughput.csv
  comparison summary:  $OUT_DIR/comparison-summary.md
EOF
