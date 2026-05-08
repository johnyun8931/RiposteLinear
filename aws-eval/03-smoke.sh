#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"
load_state

require_cmd ssh
require_cmd scp
require_cmd python3
if dynamodb_control_enabled || public_entry_enabled || sqs_ingestion_enabled; then
  require_cmd aws
fi

SMOKE_LOCAL_DIR="$STATE_DIR/smoke"
SMOKE_EPOCH_SECONDS="${SMOKE_EPOCH_SECONDS:-30}"
SMOKE_RESULTS_REMOTE="$(smoke_results_dir)"
SMOKE_LOGS_REMOTE="$(smoke_log_dir)"

capture_dynamodb_smoke_state() {
  local stage="$1"
  local local_dir="$SMOKE_LOCAL_DIR/dynamodb-$stage"
  local remote_dir="$SMOKE_LOGS_REMOTE/dynamodb-$stage"
  local pk

  if ! dynamodb_control_enabled; then
    return 0
  fi

  mkdir -p "$local_dir"
  remote_cmd "$COORDINATOR_PUBLIC_IP" "mkdir -p '$remote_dir'"
  for pk in lease epoch shard-config; do
    capture_dynamodb_control_item "$pk" "$local_dir/${pk}.json"
    copy_to_remote "$local_dir/${pk}.json" "$COORDINATOR_PUBLIC_IP" "$remote_dir/${pk}.json"
  done
  if capture_dynamodb_epoch_shard_config "$local_dir/epoch.json" "$local_dir/epoch-shard-config.json"; then
    copy_to_remote "$local_dir/epoch-shard-config.json" "$COORDINATOR_PUBLIC_IP" "$remote_dir/epoch-shard-config.json"
  fi
}

assert_dynamodb_smoke_state() {
  local stage="$1"
  local epoch_id="$2"
  local accepting="$3"
  local allow_completed="${4:-false}"
  local holder
  local epoch_file="$SMOKE_LOCAL_DIR/dynamodb-$stage/epoch.json"
  local lease_file="$SMOKE_LOCAL_DIR/dynamodb-$stage/lease.json"
  local shard_config_file="$SMOKE_LOCAL_DIR/dynamodb-$stage/shard-config.json"
  local epoch_shard_config_file="$SMOKE_LOCAL_DIR/dynamodb-$stage/epoch-shard-config.json"

  if ! dynamodb_control_enabled; then
    return 0
  fi

  holder="$(coordinator_holder_id)"
  python3 - "$epoch_file" "$lease_file" "$shard_config_file" "$epoch_shard_config_file" "$epoch_id" "$accepting" "$allow_completed" "$holder" <<'PY'
import json
import os
import sys

epoch_path, lease_path, shard_config_path, epoch_shard_config_path, epoch_id, accepting, allow_completed, holder = sys.argv[1:]
def load_item(path):
    try:
        return json.load(open(path)).get("Item", {})
    except (OSError, json.JSONDecodeError) as err:
        raise SystemExit(f"{path}: invalid JSON: {err}")

epoch = load_item(epoch_path)
lease = load_item(lease_path)
shard_config = load_item(shard_config_path)
epoch_shard_config = load_item(epoch_shard_config_path) if os.path.exists(epoch_shard_config_path) else {}

actual_epoch = epoch.get("epoch_id", {}).get("N")
actual_accepting = epoch.get("accepting", {}).get("BOOL")
actual_state = epoch.get("state", {}).get("S")
actual_shard_config_key = epoch.get("shard_config_key", {}).get("S")
actual_holder = lease.get("holder", {}).get("S")
actual_shard_version = shard_config.get("version", {}).get("N")
actual_shard_count = shard_config.get("shard_count", {}).get("N")
actual_rows_per_shard = shard_config.get("rows_per_shard", {}).get("N")
actual_global_height = shard_config.get("global_table_height", {}).get("N")
actual_shards = shard_config.get("shards", {}).get("L", [])
snapshot_pk = epoch_shard_config.get("pk", {}).get("S")
snapshot_shard_version = epoch_shard_config.get("version", {}).get("N")
snapshot_shard_count = epoch_shard_config.get("shard_count", {}).get("N")
snapshot_rows_per_shard = epoch_shard_config.get("rows_per_shard", {}).get("N")
snapshot_global_height = epoch_shard_config.get("global_table_height", {}).get("N")
snapshot_shards = epoch_shard_config.get("shards", {}).get("L", [])

if actual_epoch != epoch_id:
    raise SystemExit(f"DynamoDB epoch id mismatch: expected {epoch_id}, got {actual_epoch}")
expected_snapshot_key = f"shard-config#epoch#{epoch_id}"
if actual_shard_config_key != expected_snapshot_key:
    raise SystemExit(f"DynamoDB epoch shard_config_key mismatch: expected {expected_snapshot_key}, got {actual_shard_config_key}")
expected_accepting = accepting == "true"
completed_is_allowed = allow_completed == "true" and actual_state == "completed" and actual_accepting is False
if actual_accepting != expected_accepting and not completed_is_allowed:
    raise SystemExit(f"DynamoDB accepting mismatch: expected {accepting}, got {actual_accepting}")
if actual_holder != holder:
    raise SystemExit(f"DynamoDB lease holder mismatch: expected {holder}, got {actual_holder}")
if actual_shard_version != "1":
    raise SystemExit(f"DynamoDB shard config version mismatch: expected 1, got {actual_shard_version}")
if actual_shard_count != "2":
    raise SystemExit(f"DynamoDB shard count mismatch: expected 2, got {actual_shard_count}")
if actual_rows_per_shard != "256":
    raise SystemExit(f"DynamoDB rows_per_shard mismatch: expected 256, got {actual_rows_per_shard}")
if actual_global_height != "512":
    raise SystemExit(f"DynamoDB global_table_height mismatch: expected 512, got {actual_global_height}")
if len(actual_shards) != 2:
    raise SystemExit(f"DynamoDB shard entries mismatch: expected 2, got {len(actual_shards)}")
if snapshot_pk != expected_snapshot_key:
    raise SystemExit(f"DynamoDB epoch shard config key mismatch: expected {expected_snapshot_key}, got {snapshot_pk}")
if snapshot_shard_version != "1":
    raise SystemExit(f"DynamoDB epoch shard config version mismatch: expected 1, got {snapshot_shard_version}")
if snapshot_shard_count != "2":
    raise SystemExit(f"DynamoDB epoch shard count mismatch: expected 2, got {snapshot_shard_count}")
if snapshot_rows_per_shard != "256":
    raise SystemExit(f"DynamoDB epoch rows_per_shard mismatch: expected 256, got {snapshot_rows_per_shard}")
if snapshot_global_height != "512":
    raise SystemExit(f"DynamoDB epoch global_table_height mismatch: expected 512, got {snapshot_global_height}")
if len(snapshot_shards) != 2:
    raise SystemExit(f"DynamoDB epoch shard entries mismatch: expected 2, got {len(snapshot_shards)}")
PY
}

assert_remote_scaling_status() {
  local path="$1"
  local epoch_id="$2"
  local accepted_requests="$3"
  local expected_global_height="$4"

  remote_cmd "$COORDINATOR_PUBLIC_IP" "python3 - '$path' '$epoch_id' '$accepted_requests' '$expected_global_height' <<'PY'
import json
import sys

path, epoch_id, accepted_requests, expected_global_height = sys.argv[1:]
try:
    status = json.load(open(path))
except (OSError, json.JSONDecodeError) as err:
    raise SystemExit(f\"{path}: invalid JSON: {err}\")

if int(status.get('scaling_epoch_id', 0)) != int(epoch_id):
    raise SystemExit(f\"scaling_epoch_id mismatch: expected {epoch_id}, got {status.get('scaling_epoch_id')}\")
if int(status.get('scaling_accepted_requests', 0)) != int(accepted_requests):
    raise SystemExit(f\"scaling_accepted_requests mismatch: expected {accepted_requests}, got {status.get('scaling_accepted_requests')}\")
if int(status.get('scaling_duration_secs', 0)) <= 0:
    raise SystemExit('scaling_duration_secs must be positive')
if 'request_density' not in status:
    raise SystemExit('missing request_density')
if not status.get('scaling_action'):
    raise SystemExit('missing scaling_action')
if int(status.get('global_table_height', 0)) != int(expected_global_height):
    raise SystemExit(f\"global_table_height mismatch: expected {expected_global_height}, got {status.get('global_table_height')}\")
PY"
}

assert_ingestion_smoke_artifacts() {
  local dir="$1"
  if ! sqs_ingestion_enabled; then
    return 0
  fi
  python3 - "$dir/shard0-queue-attributes.json" "$dir/shard1-queue-attributes.json" "$dir/s3-payloads.json" "$dir/completed-upload-ledger.json" "$COMPLETED_UPLOAD_LEDGER_BACKEND" <<'PY'
import json
import sys

shard0_path, shard1_path, payloads_path, ledger_path, ledger_backend = sys.argv[1:]

def load(path):
    try:
        return json.load(open(path))
    except (OSError, json.JSONDecodeError) as err:
        raise SystemExit(f"{path}: invalid JSON: {err}")

for path in (shard0_path, shard1_path):
    attrs = load(path).get("Attributes", {})
    visible = int(attrs.get("ApproximateNumberOfMessages", "0"))
    inflight = int(attrs.get("ApproximateNumberOfMessagesNotVisible", "0"))
    if visible != 0 or inflight != 0:
        raise SystemExit(f"{path}: expected drained queue, got visible={visible} inflight={inflight}")

payloads = load(payloads_path).get("Contents", [])
keys = [item.get("Key", "") for item in payloads]
if len([key for key in keys if key.startswith("completed-uploads/")]) < 2:
    raise SystemExit(f"expected at least two completed-upload S3 payloads, got {keys}")

if ledger_backend == "dynamodb":
    items = load(ledger_path).get("Items", [])
    committed = [item for item in items if item.get("state", {}).get("S") == "committed"]
    if len(committed) < 2:
        raise SystemExit(f"expected at least two committed completed-upload ledger records, got {items}")
PY
}

assert_remote_ingestion_status() {
  local host="$1"
  local path="$2"
  if ! sqs_ingestion_enabled; then
    return 0
  fi
  remote_cmd "$host" "python3 - '$path' <<'PY'
import json
import sys

path = sys.argv[1]
try:
    status = json.load(open(path))
except (OSError, json.JSONDecodeError) as err:
    raise SystemExit(f\"{path}: invalid JSON: {err}\")

if status.get('ingestion_queue_backend') != 'sqs':
    raise SystemExit(f\"{path}: expected sqs ingestion backend, got {status.get('ingestion_queue_backend')}\")
if int(status.get('ingestion_processed_count', 0)) < 1:
    raise SystemExit(f\"{path}: expected at least one processed upload, got {status.get('ingestion_processed_count')}\")
if int(status.get('ingestion_ack_count', 0)) < 1:
    raise SystemExit(f\"{path}: expected at least one acked upload, got {status.get('ingestion_ack_count')}\")
for key in ('ingestion_receive_error_count', 'ingestion_process_error_count', 'ingestion_ack_error_count'):
    if int(status.get(key, 0)) != 0:
        raise SystemExit(f\"{path}: expected {key}=0, got {status.get(key)} last_error={status.get('ingestion_last_error')}\")
if status.get('completed_upload_ledger_backend') == 'dynamodb':
    if int(status.get('completed_upload_committed_count', 0)) < 1:
        raise SystemExit(f\"{path}: expected at least one committed ledger upload, got {status.get('completed_upload_committed_count')}\")
    for key in ('completed_upload_ledger_begin_error_count', 'completed_upload_ledger_complete_error_count'):
        if int(status.get(key, 0)) != 0:
            raise SystemExit(f\"{path}: expected {key}=0, got {status.get(key)} last_error={status.get('completed_upload_ledger_last_error')}\")
PY"
}

cleanup() {
  kill_all_remote_processes
}
trap cleanup EXIT

reset_all_remote_workspaces
kill_all_remote_processes
mkdir -p "$SMOKE_LOCAL_DIR"

info "starting full sharded topology for smoke test"
start_remote_server "$SHARD0_FOLLOWER_PUBLIC_IP" 1 0 "$(server_pair_csv "$SHARD0_LEADER_PRIVATE_IP" "$SHARD0_LEADER_PORT" "$SHARD0_FOLLOWER_PRIVATE_IP" "$SHARD0_FOLLOWER_PORT")" "$SMOKE_RESULTS_REMOTE/shard0" "$SMOKE_LOGS_REMOTE/shard0-follower.log"
start_remote_server "$SHARD0_LEADER_PUBLIC_IP" 0 0 "$(server_pair_csv "$SHARD0_LEADER_PRIVATE_IP" "$SHARD0_LEADER_PORT" "$SHARD0_FOLLOWER_PRIVATE_IP" "$SHARD0_FOLLOWER_PORT")" "$SMOKE_RESULTS_REMOTE/shard0" "$SMOKE_LOGS_REMOTE/shard0-leader.log"
start_remote_server "$SHARD1_FOLLOWER_PUBLIC_IP" 1 1 "$(server_pair_csv "$SHARD1_LEADER_PRIVATE_IP" "$SHARD1_LEADER_PORT" "$SHARD1_FOLLOWER_PRIVATE_IP" "$SHARD1_FOLLOWER_PORT")" "$SMOKE_RESULTS_REMOTE/shard1" "$SMOKE_LOGS_REMOTE/shard1-follower.log"
start_remote_server "$SHARD1_LEADER_PUBLIC_IP" 0 1 "$(server_pair_csv "$SHARD1_LEADER_PRIVATE_IP" "$SHARD1_LEADER_PORT" "$SHARD1_FOLLOWER_PRIVATE_IP" "$SHARD1_FOLLOWER_PORT")" "$SMOKE_RESULTS_REMOTE/shard1" "$SMOKE_LOGS_REMOTE/shard1-leader.log"

remote_wait_for_port "$SHARD0_LEADER_PUBLIC_IP" "127.0.0.1" "$SHARD0_LEADER_PORT"
remote_wait_for_port "$SHARD1_LEADER_PUBLIC_IP" "127.0.0.1" "$SHARD1_LEADER_PORT"

start_remote_coordinator "$COORDINATOR_PUBLIC_IP" "$(coordinator_addr)" "$SMOKE_LOGS_REMOTE/coordinator.log"
remote_wait_for_port "$COORDINATOR_PUBLIC_IP" "127.0.0.1" "$COORDINATOR_PORT"
wait_for_nlb_target_healthy 180
if public_entry_enabled; then
  capture_nlb_artifacts "$SMOKE_LOCAL_DIR/nlb-active"
  remote_cmd "$COORDINATOR_PUBLIC_IP" "mkdir -p '$SMOKE_LOGS_REMOTE/nlb-active'"
  copy_to_remote "$SMOKE_LOCAL_DIR/nlb-active/target-health.json" "$COORDINATOR_PUBLIC_IP" "$SMOKE_LOGS_REMOTE/nlb-active/target-health.json"
fi

info "retrying coordinator StartEpoch until shard leaders are ready"
start_line="$(retry_start_epoch coordinator "$COORDINATOR_PUBLIC_IP" "$(coordinator_addr)" "$SMOKE_EPOCH_SECONDS")"
epoch_id="$(extract_field "$start_line" "epoch")"
[[ -n "$epoch_id" ]] || die "could not parse smoke epoch id from: $start_line"

wait_for_status_state coordinator "$COORDINATOR_PUBLIC_IP" "$(coordinator_addr)" active 20 >/dev/null || die "coordinator did not reach active state"
wait_for_status_state server "$SHARD0_LEADER_PUBLIC_IP" "$(shard0_leader_addr)" active 20 >/dev/null || die "shard0 leader did not reach active state"
wait_for_status_state server "$SHARD1_LEADER_PUBLIC_IP" "$(shard1_leader_addr)" active 20 >/dev/null || die "shard1 leader did not reach active state"
capture_remote_status_json coordinator "$COORDINATOR_PUBLIC_IP" "$(coordinator_addr)" "$SMOKE_LOGS_REMOTE/status-active-coordinator.json"
capture_remote_status_json server "$SHARD0_LEADER_PUBLIC_IP" "$(shard0_leader_addr)" "$SMOKE_LOGS_REMOTE/status-active-shard0-leader.json"
capture_remote_status_json server "$SHARD1_LEADER_PUBLIC_IP" "$(shard1_leader_addr)" "$SMOKE_LOGS_REMOTE/status-active-shard1-leader.json"
capture_dynamodb_smoke_state active
assert_dynamodb_smoke_state active "$epoch_id" true true

info "sending deterministic smoke writes through the coordinator"
remote_cmd "$CLIENT_PUBLIC_IP" "mkdir -p '$SMOKE_LOGS_REMOTE'; ~/client -coordinator '$(public_coordinator_addr)' -x 1 -y 0 -payload shard0-boundary -threads '$CLIENT_THREADS' -log '$SMOKE_LOGS_REMOTE/client-row0.log'"
remote_cmd "$CLIENT_PUBLIC_IP" "mkdir -p '$SMOKE_LOGS_REMOTE'; ~/client -coordinator '$(public_coordinator_addr)' -x 2 -y 256 -payload shard1-boundary -threads '$CLIENT_THREADS' -log '$SMOKE_LOGS_REMOTE/client-row256.log'"

wait_for_epoch_complete coordinator "$COORDINATOR_PUBLIC_IP" "$(coordinator_addr)" 120
wait_for_epoch_complete server "$SHARD0_LEADER_PUBLIC_IP" "$(shard0_leader_addr)" 120
wait_for_epoch_complete server "$SHARD1_LEADER_PUBLIC_IP" "$(shard1_leader_addr)" 120
wait_for_sqs_ingestion_drain 120
capture_remote_status_json coordinator "$COORDINATOR_PUBLIC_IP" "$(coordinator_addr)" "$SMOKE_LOGS_REMOTE/status-completed-coordinator.json"
capture_remote_status_json server "$SHARD0_LEADER_PUBLIC_IP" "$(shard0_leader_addr)" "$SMOKE_LOGS_REMOTE/status-completed-shard0-leader.json"
capture_remote_status_json server "$SHARD1_LEADER_PUBLIC_IP" "$(shard1_leader_addr)" "$SMOKE_LOGS_REMOTE/status-completed-shard1-leader.json"
assert_remote_ingestion_status "$SHARD0_LEADER_PUBLIC_IP" "$SMOKE_LOGS_REMOTE/status-completed-shard0-leader.json"
assert_remote_ingestion_status "$SHARD1_LEADER_PUBLIC_IP" "$SMOKE_LOGS_REMOTE/status-completed-shard1-leader.json"
assert_remote_scaling_status "$SMOKE_LOGS_REMOTE/status-completed-coordinator.json" "$epoch_id" 2 512
if public_entry_enabled; then
  capture_nlb_artifacts "$SMOKE_LOCAL_DIR/nlb-completed"
  remote_cmd "$COORDINATOR_PUBLIC_IP" "mkdir -p '$SMOKE_LOGS_REMOTE/nlb-completed'"
  copy_to_remote "$SMOKE_LOCAL_DIR/nlb-completed/target-health.json" "$COORDINATOR_PUBLIC_IP" "$SMOKE_LOGS_REMOTE/nlb-completed/target-health.json"
fi
if sqs_ingestion_enabled; then
  capture_ingestion_artifacts "$SMOKE_LOCAL_DIR/ingestion-completed"
  assert_ingestion_smoke_artifacts "$SMOKE_LOCAL_DIR/ingestion-completed"
  remote_cmd "$COORDINATOR_PUBLIC_IP" "mkdir -p '$SMOKE_LOGS_REMOTE/ingestion-completed'"
  for artifact in shard0-queue-attributes.json shard1-queue-attributes.json s3-payloads.json completed-upload-ledger.json; do
    if [[ -f "$SMOKE_LOCAL_DIR/ingestion-completed/$artifact" ]]; then
      copy_to_remote "$SMOKE_LOCAL_DIR/ingestion-completed/$artifact" "$COORDINATOR_PUBLIC_IP" "$SMOKE_LOGS_REMOTE/ingestion-completed/$artifact"
    fi
  done
fi
capture_dynamodb_smoke_state completed
assert_dynamodb_smoke_state completed "$epoch_id" false

local_shard0="$SMOKE_LOCAL_DIR/$(result_file_name "$epoch_id" 0)"
local_shard1="$SMOKE_LOCAL_DIR/$(result_file_name "$epoch_id" 1)"
copy_from_remote "$SHARD0_LEADER_PUBLIC_IP" "$SMOKE_RESULTS_REMOTE/shard0/$(result_file_name "$epoch_id" 0)" "$local_shard0"
copy_from_remote "$SHARD1_LEADER_PUBLIC_IP" "$SMOKE_RESULTS_REMOTE/shard1/$(result_file_name "$epoch_id" 1)" "$local_shard1"

[[ -f "$local_shard0" ]] || die "missing shard0 result file: $local_shard0"
[[ -f "$local_shard1" ]] || die "missing shard1 result file: $local_shard1"

assert_result_contains_slot "$local_shard0" 0 0 256 0 1 "shard0-boundary"
assert_result_contains_slot "$local_shard1" 1 256 512 256 2 "shard1-boundary"

cat <<EOF
AWS smoke test passed.
  epoch: $epoch_id
  shard0 result: $local_shard0
  shard1 result: $local_shard1
EOF
