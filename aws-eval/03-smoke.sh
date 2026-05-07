#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"
load_state

require_cmd ssh
require_cmd scp
require_cmd python3
if dynamodb_control_enabled; then
  require_cmd aws
fi

SMOKE_LOCAL_DIR="$STATE_DIR/smoke"
SMOKE_EPOCH_SECONDS="${SMOKE_EPOCH_SECONDS:-8}"
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
}

assert_dynamodb_smoke_state() {
  local stage="$1"
  local epoch_id="$2"
  local accepting="$3"
  local holder
  local epoch_file="$SMOKE_LOCAL_DIR/dynamodb-$stage/epoch.json"
  local lease_file="$SMOKE_LOCAL_DIR/dynamodb-$stage/lease.json"
  local shard_config_file="$SMOKE_LOCAL_DIR/dynamodb-$stage/shard-config.json"

  if ! dynamodb_control_enabled; then
    return 0
  fi

  holder="$(coordinator_holder_id)"
  python3 - "$epoch_file" "$lease_file" "$shard_config_file" "$epoch_id" "$accepting" "$holder" <<'PY'
import json
import sys

epoch_path, lease_path, shard_config_path, epoch_id, accepting, holder = sys.argv[1:]
epoch = json.load(open(epoch_path)).get("Item", {})
lease = json.load(open(lease_path)).get("Item", {})
shard_config = json.load(open(shard_config_path)).get("Item", {})

actual_epoch = epoch.get("epoch_id", {}).get("N")
actual_accepting = epoch.get("accepting", {}).get("BOOL")
actual_holder = lease.get("holder", {}).get("S")
actual_shard_version = shard_config.get("version", {}).get("N")

if actual_epoch != epoch_id:
    raise SystemExit(f"DynamoDB epoch id mismatch: expected {epoch_id}, got {actual_epoch}")
if actual_accepting != (accepting == "true"):
    raise SystemExit(f"DynamoDB accepting mismatch: expected {accepting}, got {actual_accepting}")
if actual_holder != holder:
    raise SystemExit(f"DynamoDB lease holder mismatch: expected {holder}, got {actual_holder}")
if actual_shard_version != "1":
    raise SystemExit(f"DynamoDB shard config version mismatch: expected 1, got {actual_shard_version}")
PY
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
assert_dynamodb_smoke_state active "$epoch_id" true

info "sending deterministic smoke writes through the coordinator"
remote_cmd "$CLIENT_PUBLIC_IP" "mkdir -p '$SMOKE_LOGS_REMOTE'; ~/client -coordinator '$(coordinator_addr)' -x 1 -y 0 -payload shard0-boundary -threads '$CLIENT_THREADS' -log '$SMOKE_LOGS_REMOTE/client-row0.log'"
remote_cmd "$CLIENT_PUBLIC_IP" "mkdir -p '$SMOKE_LOGS_REMOTE'; ~/client -coordinator '$(coordinator_addr)' -x 2 -y 128 -payload shard1-boundary -threads '$CLIENT_THREADS' -log '$SMOKE_LOGS_REMOTE/client-row128.log'"

wait_for_epoch_complete coordinator "$COORDINATOR_PUBLIC_IP" "$(coordinator_addr)" 120
capture_remote_status_json coordinator "$COORDINATOR_PUBLIC_IP" "$(coordinator_addr)" "$SMOKE_LOGS_REMOTE/status-completed-coordinator.json"
capture_remote_status_json server "$SHARD0_LEADER_PUBLIC_IP" "$(shard0_leader_addr)" "$SMOKE_LOGS_REMOTE/status-completed-shard0-leader.json"
capture_remote_status_json server "$SHARD1_LEADER_PUBLIC_IP" "$(shard1_leader_addr)" "$SMOKE_LOGS_REMOTE/status-completed-shard1-leader.json"
capture_dynamodb_smoke_state completed
assert_dynamodb_smoke_state completed "$epoch_id" false

local_shard0="$SMOKE_LOCAL_DIR/$(result_file_name "$epoch_id" 0)"
local_shard1="$SMOKE_LOCAL_DIR/$(result_file_name "$epoch_id" 1)"
copy_from_remote "$SHARD0_LEADER_PUBLIC_IP" "$SMOKE_RESULTS_REMOTE/shard0/$(result_file_name "$epoch_id" 0)" "$local_shard0"
copy_from_remote "$SHARD1_LEADER_PUBLIC_IP" "$SMOKE_RESULTS_REMOTE/shard1/$(result_file_name "$epoch_id" 1)" "$local_shard1"

[[ -f "$local_shard0" ]] || die "missing shard0 result file: $local_shard0"
[[ -f "$local_shard1" ]] || die "missing shard1 result file: $local_shard1"

assert_result_contains_slot "$local_shard0" 0 0 128 0 1 "shard0-boundary"
assert_result_contains_slot "$local_shard1" 1 128 256 128 2 "shard1-boundary"

cat <<EOF
AWS smoke test passed.
  epoch: $epoch_id
  shard0 result: $local_shard0
  shard1 result: $local_shard1
EOF
