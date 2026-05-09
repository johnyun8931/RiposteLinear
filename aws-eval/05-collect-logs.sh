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
require_cmd ssh
require_cmd git
require_cmd python3
require_cmd aws

RESULT_ID="${RESULT_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
OUT_DIR="$RESULTS_DIR/$RESULT_ID"
mkdir -p "$OUT_DIR/remotes" "$OUT_DIR/leader-results"
if public_entry_enabled; then
  capture_nlb_artifacts "$OUT_DIR/aws/nlb"
fi
capture_read_alb_artifacts "$OUT_DIR/aws/read-alb"
if sqs_ingestion_enabled; then
  capture_ingestion_artifacts "$OUT_DIR/aws/ingestion"
fi

collect_readserver_artifacts() {
  local output_dir="$1"
  mkdir -p "$output_dir"
  aws_region autoscaling describe-auto-scaling-groups \
    --auto-scaling-group-names "${READ_SERVER_ASG_NAME:-}" \
    --output json >"$output_dir/asg.json" 2>/dev/null || true
  aws_region ec2 describe-instances \
    --filters Name=tag:Project,Values="$PROJECT_TAG" Name=tag:Role,Values=readserver \
    --output json >"$output_dir/instances.json" || true
  local id public_ip private_ip dst
  while IFS=$'\t' read -r id public_ip private_ip; do
    [[ -n "${id:-}" && "$id" != "None" ]] || continue
    dst="$output_dir/$id"
    mkdir -p "$dst"
    printf '%s\n' "$public_ip" >"$dst/public-ip.txt"
    printf '%s\n' "$private_ip" >"$dst/private-ip.txt"
    if [[ -n "$public_ip" && "$public_ip" != "None" ]]; then
      remote_cmd "$public_ip" "journalctl -u readserver --no-pager -n 1000" >"$dst/readserver-journal.log" 2>"$dst/readserver-journal.err" || true
      remote_cmd "$public_ip" "curl -fsS 'http://127.0.0.1:${READ_SERVER_PORT:-8080}/status'" >"$dst/status.json" 2>"$dst/status.err" || true
    fi
  done < <(aws_region ec2 describe-instances \
    --filters Name=tag:Project,Values="$PROJECT_TAG" Name=tag:Role,Values=readserver Name=instance-state-name,Values=pending,running,stopping,stopped \
    --query 'Reservations[].Instances[].[InstanceId,PublicIpAddress,PrivateIpAddress]' \
    --output text)
}

collect_readserver_artifacts "$OUT_DIR/aws/readservers"

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
READ_SERVER_INSTANCE_TYPE="${READ_SERVER_INSTANCE_TYPE:-}" \
COORDINATOR_PORT="$COORDINATOR_PORT" \
READ_SERVER_PORT="${READ_SERVER_PORT:-8080}" \
READ_ALB_PORT="${READ_ALB_PORT:-80}" \
COORDINATOR_STANDBY_PORT="${COORDINATOR_STANDBY_PORT:-8631}" \
SHARD0_LEADER_PORT="$SHARD0_LEADER_PORT" \
SHARD0_FOLLOWER_PORT="$SHARD0_FOLLOWER_PORT" \
SHARD1_LEADER_PORT="$SHARD1_LEADER_PORT" \
SHARD1_FOLLOWER_PORT="$SHARD1_FOLLOWER_PORT" \
SHARD0_STANDBY_LEADER_PORT="${SHARD0_STANDBY_LEADER_PORT:-8640}" \
SHARD0_STANDBY_FOLLOWER_PORT="${SHARD0_STANDBY_FOLLOWER_PORT:-8641}" \
SHARD1_STANDBY_LEADER_PORT="${SHARD1_STANDBY_LEADER_PORT:-8650}" \
SHARD1_STANDBY_FOLLOWER_PORT="${SHARD1_STANDBY_FOLLOWER_PORT:-8651}" \
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
HOT_STANDBY_INGESTION="${HOT_STANDBY_INGESTION:-0}" \
INGESTION_S3_BUCKET="${INGESTION_S3_BUCKET:-}" \
INGESTION_RECEIVE_BATCH_SIZE="$INGESTION_RECEIVE_BATCH_SIZE" \
INGESTION_SQS_WAIT_SECONDS="$INGESTION_SQS_WAIT_SECONDS" \
INGESTION_SQS_VISIBILITY_TIMEOUT_SECONDS="$INGESTION_SQS_VISIBILITY_TIMEOUT_SECONDS" \
INGESTION_WORKER_ERROR_BACKOFF_MS="$INGESTION_WORKER_ERROR_BACKOFF_MS" \
INGESTION_SQS_SHARD0_QUEUE_URL="${INGESTION_SQS_SHARD0_QUEUE_URL:-}" \
INGESTION_SQS_SHARD1_QUEUE_URL="${INGESTION_SQS_SHARD1_QUEUE_URL:-}" \
INGESTION_SQS_SHARD0_STANDBY_QUEUE_URL="${INGESTION_SQS_SHARD0_STANDBY_QUEUE_URL:-}" \
INGESTION_SQS_SHARD1_STANDBY_QUEUE_URL="${INGESTION_SQS_SHARD1_STANDBY_QUEUE_URL:-}" \
COMPLETED_UPLOAD_LEDGER_BACKEND="$COMPLETED_UPLOAD_LEDGER_BACKEND" \
COMPLETED_UPLOAD_LEDGER_TABLE="$COMPLETED_UPLOAD_LEDGER_TABLE" \
COMPLETED_UPLOAD_PROCESSING_TTL_SECONDS="$COMPLETED_UPLOAD_PROCESSING_TTL_SECONDS" \
NLB_NAME="${NLB_NAME:-}" \
NLB_DNS_NAME="${NLB_DNS_NAME:-}" \
NLB_ARN="${NLB_ARN:-}" \
NLB_TARGET_GROUP_NAME="${NLB_TARGET_GROUP_NAME:-}" \
NLB_TARGET_GROUP_ARN="${NLB_TARGET_GROUP_ARN:-}" \
NLB_LISTENER_ARN="${NLB_LISTENER_ARN:-}" \
READ_ALB_NAME="${READ_ALB_NAME:-}" \
READ_ALB_DNS_NAME="${READ_ALB_DNS_NAME:-}" \
READ_ALB_ARN="${READ_ALB_ARN:-}" \
READ_ALB_TARGET_GROUP_ARN="${READ_ALB_TARGET_GROUP_ARN:-}" \
READ_ALB_LISTENER_ARN="${READ_ALB_LISTENER_ARN:-}" \
READ_SERVER_ASG_NAME="${READ_SERVER_ASG_NAME:-}" \
READ_SERVER_DESIRED_CAPACITY="${READ_SERVER_DESIRED_CAPACITY:-0}" \
READ_SERVER_MIN_SIZE="${READ_SERVER_MIN_SIZE:-0}" \
READ_SERVER_MAX_SIZE="${READ_SERVER_MAX_SIZE:-0}" \
RESULT_TABLE_S3_BUCKET="${RESULT_TABLE_S3_BUCKET:-}" \
RESULT_TABLE_S3_PREFIX="${RESULT_TABLE_S3_PREFIX:-}" \
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
        "read_server": os.environ["READ_SERVER_INSTANCE_TYPE"],
    },
    "ports": {
        "coordinator": int(os.environ["COORDINATOR_PORT"]),
        "read_server": int(os.environ["READ_SERVER_PORT"]),
        "read_alb": int(os.environ["READ_ALB_PORT"]),
        "coordinator_standby": int(os.environ["COORDINATOR_STANDBY_PORT"]),
        "shard0_leader": int(os.environ["SHARD0_LEADER_PORT"]),
        "shard0_follower": int(os.environ["SHARD0_FOLLOWER_PORT"]),
        "shard1_leader": int(os.environ["SHARD1_LEADER_PORT"]),
        "shard1_follower": int(os.environ["SHARD1_FOLLOWER_PORT"]),
        "shard0_standby_leader": int(os.environ["SHARD0_STANDBY_LEADER_PORT"]),
        "shard0_standby_follower": int(os.environ["SHARD0_STANDBY_FOLLOWER_PORT"]),
        "shard1_standby_leader": int(os.environ["SHARD1_STANDBY_LEADER_PORT"]),
        "shard1_standby_follower": int(os.environ["SHARD1_STANDBY_FOLLOWER_PORT"]),
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
        "hot_standby_ingestion": os.environ["HOT_STANDBY_INGESTION"],
        "ingestion_s3_bucket": os.environ["INGESTION_S3_BUCKET"],
        "ingestion_receive_batch_size": int(os.environ["INGESTION_RECEIVE_BATCH_SIZE"]),
        "ingestion_sqs_wait_seconds": int(os.environ["INGESTION_SQS_WAIT_SECONDS"]),
        "ingestion_sqs_visibility_timeout_seconds": int(os.environ["INGESTION_SQS_VISIBILITY_TIMEOUT_SECONDS"]),
        "ingestion_worker_error_backoff_ms": int(os.environ["INGESTION_WORKER_ERROR_BACKOFF_MS"]),
        "ingestion_sqs_shard0_queue_url": os.environ["INGESTION_SQS_SHARD0_QUEUE_URL"],
        "ingestion_sqs_shard1_queue_url": os.environ["INGESTION_SQS_SHARD1_QUEUE_URL"],
        "ingestion_sqs_shard0_standby_queue_url": os.environ["INGESTION_SQS_SHARD0_STANDBY_QUEUE_URL"],
        "ingestion_sqs_shard1_standby_queue_url": os.environ["INGESTION_SQS_SHARD1_STANDBY_QUEUE_URL"],
        "completed_upload_ledger_backend": os.environ["COMPLETED_UPLOAD_LEDGER_BACKEND"],
        "completed_upload_ledger_table": os.environ["COMPLETED_UPLOAD_LEDGER_TABLE"],
        "completed_upload_processing_ttl_seconds": int(os.environ["COMPLETED_UPLOAD_PROCESSING_TTL_SECONDS"]),
        "nlb": {
            "name": os.environ["NLB_NAME"],
            "dns_name": os.environ["NLB_DNS_NAME"],
            "arn": os.environ["NLB_ARN"],
            "target_group_name": os.environ["NLB_TARGET_GROUP_NAME"],
            "target_group_arn": os.environ["NLB_TARGET_GROUP_ARN"],
            "listener_arn": os.environ["NLB_LISTENER_ARN"],
        },
        "read_path": {
            "alb_name": os.environ["READ_ALB_NAME"],
            "alb_dns_name": os.environ["READ_ALB_DNS_NAME"],
            "alb_arn": os.environ["READ_ALB_ARN"],
            "target_group_arn": os.environ["READ_ALB_TARGET_GROUP_ARN"],
            "listener_arn": os.environ["READ_ALB_LISTENER_ARN"],
            "asg_name": os.environ["READ_SERVER_ASG_NAME"],
            "desired_capacity": int(os.environ["READ_SERVER_DESIRED_CAPACITY"]),
            "min_size": int(os.environ["READ_SERVER_MIN_SIZE"]),
            "max_size": int(os.environ["READ_SERVER_MAX_SIZE"]),
            "result_table_s3_bucket": os.environ["RESULT_TABLE_S3_BUCKET"],
            "result_table_s3_prefix": os.environ["RESULT_TABLE_S3_PREFIX"],
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
