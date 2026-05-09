#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"
load_state

require_cmd aws
require_cmd ssh
require_cmd scp
require_cmd python3

cloudwatch_observability_enabled || die "15-demo-sqs-idempotence-cloudwatch.sh requires CLOUDWATCH_OBSERVABILITY=1"
dynamodb_control_enabled || die "15-demo-sqs-idempotence-cloudwatch.sh requires CONTROL_STORE_BACKEND=dynamodb"
dynamodb_session_enabled || die "15-demo-sqs-idempotence-cloudwatch.sh requires SESSION_STORE_BACKEND=dynamodb"
sqs_ingestion_enabled || die "15-demo-sqs-idempotence-cloudwatch.sh requires INGESTION_QUEUE_BACKEND=sqs"
[[ "$COMPLETED_UPLOAD_LEDGER_BACKEND" == "dynamodb" ]] || die "15-demo-sqs-idempotence-cloudwatch.sh requires COMPLETED_UPLOAD_LEDGER_BACKEND=dynamodb"

IDEMP_LOCAL_DIR="$STATE_DIR/sqs-idempotence"
IDEMP_REMOTE_ROOT="$REMOTE_SMOKE_DIR/sqs-idempotence/run"
IDEMP_LOGS_REMOTE="$IDEMP_REMOTE_ROOT/logs"
IDEMP_RESULTS_REMOTE="$IDEMP_REMOTE_ROOT/results"
IDEMP_EPOCH_SECONDS="${IDEMP_EPOCH_SECONDS:-30}"

cleanup() {
  stop_cloudwatch_status_pollers "$IDEMP_LOGS_REMOTE"
  kill_all_remote_processes
}
trap cleanup EXIT

assert_demo_ack_failure_status() {
  local path="$1"
  python3 - "$path" <<'PY'
import json
import sys

status = json.load(open(sys.argv[1]))
if int(status.get("ingestion_ack_error_count", 0)) < 1:
    raise SystemExit(f"expected ingestion_ack_error_count >= 1, got {status}")
if int(status.get("completed_upload_duplicate_skip_count", 0)) < 1:
    raise SystemExit(f"expected completed_upload_duplicate_skip_count >= 1, got {status}")
if int(status.get("ingestion_ack_count", 0)) < 1:
    raise SystemExit(f"expected eventual ack after redelivery, got {status}")
if int(status.get("ingestion_queue_depth", 0)) != 0 or int(status.get("ingestion_inflight_count", 0)) != 0:
    raise SystemExit(f"expected drained ingestion queue, got {status}")
PY
}

info "resetting DynamoDB/SQS state for SQS idempotence demo"
FORCE_CONTROL_STATE=1 FORCE_SHARD_CONFIG=1 "$SCRIPT_DIR/07-create-control-table.sh"
load_state

for queue_url in \
  "${INGESTION_SQS_SHARD0_QUEUE_URL:-}" \
  "${INGESTION_SQS_SHARD1_QUEUE_URL:-}" \
  "${INGESTION_SQS_SHARD0_STANDBY_QUEUE_URL:-}" \
  "${INGESTION_SQS_SHARD1_STANDBY_QUEUE_URL:-}"; do
  [[ -n "$queue_url" ]] || continue
  aws_region sqs purge-queue --queue-url "$queue_url" >/dev/null 2>&1 || true
done
sleep 5

reset_all_remote_workspaces
kill_all_remote_processes
mkdir -p "$IDEMP_LOCAL_DIR"
start_cloudwatch_status_pollers "sqs-idempotence" "$IDEMP_LOGS_REMOTE"
write_cloudwatch_demo_event "sqs-idempotence" "$IDEMP_LOGS_REMOTE" "scenario_start" "SQS ack failure and idempotent redelivery demo started"

old_server_extra_args="$SERVER_EXTRA_ARGS"
old_wait_seconds="$INGESTION_SQS_WAIT_SECONDS"
old_visibility_seconds="$INGESTION_SQS_VISIBILITY_TIMEOUT_SECONDS"
SERVER_EXTRA_ARGS="$SERVER_EXTRA_ARGS -demo-fail-ingestion-ack-once"
INGESTION_SQS_WAIT_SECONDS=1
INGESTION_SQS_VISIBILITY_TIMEOUT_SECONDS=5

info "starting SQS/S3 ingestion servers with demo ack failure enabled"
start_remote_server "$SHARD0_FOLLOWER_PUBLIC_IP" 1 0 "$(server_pair_csv "$SHARD0_LEADER_PRIVATE_IP" "$SHARD0_LEADER_PORT" "$SHARD0_FOLLOWER_PRIVATE_IP" "$SHARD0_FOLLOWER_PORT")" "$IDEMP_RESULTS_REMOTE/shard0" "$IDEMP_LOGS_REMOTE/shard0-follower.log"
start_remote_server "$SHARD0_LEADER_PUBLIC_IP" 0 0 "$(server_pair_csv "$SHARD0_LEADER_PRIVATE_IP" "$SHARD0_LEADER_PORT" "$SHARD0_FOLLOWER_PRIVATE_IP" "$SHARD0_FOLLOWER_PORT")" "$IDEMP_RESULTS_REMOTE/shard0" "$IDEMP_LOGS_REMOTE/shard0-leader.log"
start_remote_server "$SHARD1_FOLLOWER_PUBLIC_IP" 1 1 "$(server_pair_csv "$SHARD1_LEADER_PRIVATE_IP" "$SHARD1_LEADER_PORT" "$SHARD1_FOLLOWER_PRIVATE_IP" "$SHARD1_FOLLOWER_PORT")" "$IDEMP_RESULTS_REMOTE/shard1" "$IDEMP_LOGS_REMOTE/shard1-follower.log"
start_remote_server "$SHARD1_LEADER_PUBLIC_IP" 0 1 "$(server_pair_csv "$SHARD1_LEADER_PRIVATE_IP" "$SHARD1_LEADER_PORT" "$SHARD1_FOLLOWER_PRIVATE_IP" "$SHARD1_FOLLOWER_PORT")" "$IDEMP_RESULTS_REMOTE/shard1" "$IDEMP_LOGS_REMOTE/shard1-leader.log"
if hot_standby_ingestion_enabled; then
  start_remote_server "$SHARD0_FOLLOWER_PUBLIC_IP" 1 0 "$(server_pair_csv "$SHARD0_LEADER_PRIVATE_IP" "$SHARD0_STANDBY_LEADER_PORT" "$SHARD0_FOLLOWER_PRIVATE_IP" "$SHARD0_STANDBY_FOLLOWER_PORT")" "$IDEMP_RESULTS_REMOTE/shard0-standby" "$IDEMP_LOGS_REMOTE/shard0-standby-follower.log" standby
  start_remote_server "$SHARD0_LEADER_PUBLIC_IP" 0 0 "$(server_pair_csv "$SHARD0_LEADER_PRIVATE_IP" "$SHARD0_STANDBY_LEADER_PORT" "$SHARD0_FOLLOWER_PRIVATE_IP" "$SHARD0_STANDBY_FOLLOWER_PORT")" "$IDEMP_RESULTS_REMOTE/shard0-standby" "$IDEMP_LOGS_REMOTE/shard0-standby-leader.log" standby
  start_remote_server "$SHARD1_FOLLOWER_PUBLIC_IP" 1 1 "$(server_pair_csv "$SHARD1_LEADER_PRIVATE_IP" "$SHARD1_STANDBY_LEADER_PORT" "$SHARD1_FOLLOWER_PRIVATE_IP" "$SHARD1_STANDBY_FOLLOWER_PORT")" "$IDEMP_RESULTS_REMOTE/shard1-standby" "$IDEMP_LOGS_REMOTE/shard1-standby-follower.log" standby
  start_remote_server "$SHARD1_LEADER_PUBLIC_IP" 0 1 "$(server_pair_csv "$SHARD1_LEADER_PRIVATE_IP" "$SHARD1_STANDBY_LEADER_PORT" "$SHARD1_FOLLOWER_PRIVATE_IP" "$SHARD1_STANDBY_FOLLOWER_PORT")" "$IDEMP_RESULTS_REMOTE/shard1-standby" "$IDEMP_LOGS_REMOTE/shard1-standby-leader.log" standby
fi

SERVER_EXTRA_ARGS="$old_server_extra_args"
INGESTION_SQS_WAIT_SECONDS="$old_wait_seconds"
INGESTION_SQS_VISIBILITY_TIMEOUT_SECONDS="$old_visibility_seconds"

remote_wait_for_port "$SHARD0_LEADER_PUBLIC_IP" "127.0.0.1" "$SHARD0_LEADER_PORT"
remote_wait_for_port "$SHARD1_LEADER_PUBLIC_IP" "127.0.0.1" "$SHARD1_LEADER_PORT"
wait_for_server_peer_ready "$COORDINATOR_PUBLIC_IP" "$(shard0_leader_addr)" 60
wait_for_server_peer_ready "$COORDINATOR_PUBLIC_IP" "$(shard1_leader_addr)" 60
if hot_standby_ingestion_enabled; then
  remote_wait_for_port "$SHARD0_LEADER_PUBLIC_IP" "127.0.0.1" "$SHARD0_STANDBY_LEADER_PORT"
  remote_wait_for_port "$SHARD1_LEADER_PUBLIC_IP" "127.0.0.1" "$SHARD1_STANDBY_LEADER_PORT"
  wait_for_server_peer_ready "$COORDINATOR_PUBLIC_IP" "$(shard0_standby_leader_addr)" 60
  wait_for_server_peer_ready "$COORDINATOR_PUBLIC_IP" "$(shard1_standby_leader_addr)" 60
fi

info "starting coordinator"
start_remote_coordinator "$COORDINATOR_PUBLIC_IP" "$(coordinator_addr)" "$IDEMP_LOGS_REMOTE/coordinator.log"
remote_wait_for_port "$COORDINATOR_PUBLIC_IP" "127.0.0.1" "$COORDINATOR_PORT"

info "starting epoch and sending one write that will fail first SQS ack after ledger commit"
start_line="$(retry_start_epoch coordinator "$COORDINATOR_PUBLIC_IP" "$(coordinator_addr)" "$IDEMP_EPOCH_SECONDS")"
epoch_id="$(extract_field "$start_line" "epoch")"
[[ -n "$epoch_id" ]] || die "could not parse epoch id from: $start_line"
write_cloudwatch_demo_event "sqs-idempotence" "$IDEMP_LOGS_REMOTE" "upload_start" "client write will trigger demo ack failure"
remote_cmd "$CLIENT_PUBLIC_IP" "mkdir -p '$IDEMP_LOGS_REMOTE'; ~/client -coordinator '$(coordinator_addr)' -x 1 -y 0 -payload sqs-idempotence -threads '$CLIENT_THREADS' -log '$IDEMP_LOGS_REMOTE/client.log'"

info "waiting for redelivery, duplicate skip, and queue drain"
wait_for_epoch_complete coordinator "$COORDINATOR_PUBLIC_IP" "$(coordinator_addr)" 180
wait_for_sqs_ingestion_drain 180
write_cloudwatch_demo_event "sqs-idempotence" "$IDEMP_LOGS_REMOTE" "queue_drained" "SQS redelivery was acked after ledger duplicate skip"

capture_remote_status_json server "$SHARD0_LEADER_PUBLIC_IP" "$(shard0_leader_addr)" "$IDEMP_LOGS_REMOTE/status-shard0-leader.json"
copy_from_remote "$SHARD0_LEADER_PUBLIC_IP" "$IDEMP_LOGS_REMOTE/status-shard0-leader.json" "$IDEMP_LOCAL_DIR/status-shard0-leader.json"
assert_demo_ack_failure_status "$IDEMP_LOCAL_DIR/status-shard0-leader.json"

capture_ingestion_artifacts "$IDEMP_LOCAL_DIR/ingestion"
copy_to_remote "$IDEMP_LOCAL_DIR/ingestion/shard0-queue-attributes.json" "$COORDINATOR_PUBLIC_IP" "$IDEMP_LOGS_REMOTE/shard0-queue-attributes.json" || true
copy_to_remote "$IDEMP_LOCAL_DIR/ingestion/completed-upload-ledger.json" "$COORDINATOR_PUBLIC_IP" "$IDEMP_LOGS_REMOTE/completed-upload-ledger.json" || true
write_cloudwatch_demo_event "sqs-idempotence" "$IDEMP_LOGS_REMOTE" "scenario_complete" "ack failure handled by ledger duplicate skip and eventual ack"

cat <<EOF
AWS SQS idempotence CloudWatch demo passed.
  epoch: $epoch_id
  status: $IDEMP_LOCAL_DIR/status-shard0-leader.json
  artifacts: $IDEMP_LOCAL_DIR/ingestion
EOF

if [[ -n "${CLOUDWATCH_DASHBOARD_NAME:-}" ]]; then
  echo "CloudWatch dashboard: https://${AWS_REGION}.console.aws.amazon.com/cloudwatch/home?region=${AWS_REGION}#dashboards:name=${CLOUDWATCH_DASHBOARD_NAME}"
fi
