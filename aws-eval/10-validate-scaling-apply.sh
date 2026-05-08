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

dynamodb_control_enabled || die "10-validate-scaling-apply.sh requires CONTROL_STORE_BACKEND=dynamodb"
dynamodb_session_enabled || die "10-validate-scaling-apply.sh requires SESSION_STORE_BACKEND=dynamodb"

APPLY_LOCAL_DIR="$STATE_DIR/scaling-apply"
APPLY_REMOTE_ROOT="$REMOTE_SMOKE_DIR/scaling-apply/run"
APPLY_LOGS_REMOTE="$APPLY_REMOTE_ROOT/logs"
APPLY_RESULTS_REMOTE="$APPLY_REMOTE_ROOT/results"
APPLY_EPOCH1_SECONDS="${APPLY_EPOCH1_SECONDS:-30}"
APPLY_EPOCH2_SECONDS="${APPLY_EPOCH2_SECONDS:-45}"
APPLY_GROW_THRESHOLD="${APPLY_GROW_THRESHOLD:-0.001}"
APPLY_WITH_AUTOSCALER="${APPLY_WITH_AUTOSCALER:-0}"

cleanup() {
  kill_all_remote_processes
}
trap cleanup EXIT

capture_status() {
  local label="$1"
  local remote_path="$APPLY_LOGS_REMOTE/status-${label}.json"
  local local_path="$APPLY_LOCAL_DIR/status-${label}.json"
  capture_remote_status_json coordinator "$COORDINATOR_PUBLIC_IP" "$(coordinator_addr)" "$remote_path"
  copy_from_remote "$COORDINATOR_PUBLIC_IP" "$remote_path" "$local_path"
}

assert_json_field() {
  local path="$1"
  local key="$2"
  local want="$3"
  python3 - "$path" "$key" "$want" <<'PY'
import json
import sys

path, key, want = sys.argv[1:]
data = json.load(open(path))
got = data.get(key)
if str(got) != want:
    raise SystemExit(f"{path}: {key} mismatch: got {got!r}, want {want!r}")
PY
}

capture_dynamodb_snapshot() {
  local stage="$1"
  local local_dir="$APPLY_LOCAL_DIR/dynamodb-$stage"
  local remote_dir="$APPLY_LOGS_REMOTE/dynamodb-$stage"
  mkdir -p "$local_dir"
  remote_cmd "$COORDINATOR_PUBLIC_IP" "mkdir -p '$remote_dir'"
  capture_dynamodb_control_item lease "$local_dir/lease.json"
  capture_dynamodb_control_item epoch "$local_dir/epoch.json"
  capture_dynamodb_control_item epoch-cycle "$local_dir/epoch-cycle.json"
  capture_dynamodb_control_item shard-config "$local_dir/shard-config.json"
  capture_dynamodb_control_item scaling#latest "$local_dir/scaling-latest.json"
  if capture_dynamodb_epoch_shard_config "$local_dir/epoch.json" "$local_dir/epoch-shard-config.json"; then
    copy_to_remote "$local_dir/epoch-shard-config.json" "$COORDINATOR_PUBLIC_IP" "$remote_dir/epoch-shard-config.json"
  fi
  copy_to_remote "$local_dir/lease.json" "$COORDINATOR_PUBLIC_IP" "$remote_dir/lease.json"
  copy_to_remote "$local_dir/epoch.json" "$COORDINATOR_PUBLIC_IP" "$remote_dir/epoch.json"
  copy_to_remote "$local_dir/epoch-cycle.json" "$COORDINATOR_PUBLIC_IP" "$remote_dir/epoch-cycle.json"
  copy_to_remote "$local_dir/shard-config.json" "$COORDINATOR_PUBLIC_IP" "$remote_dir/shard-config.json"
  copy_to_remote "$local_dir/scaling-latest.json" "$COORDINATOR_PUBLIC_IP" "$remote_dir/scaling-latest.json"
}

delete_control_pk() {
  local pk="$1"
  aws_control_region dynamodb delete-item \
    --table-name "$(dynamodb_control_table)" \
    --key "{\"pk\":{\"S\":\"$pk\"}}" >/dev/null
}

reset_dynamodb_validation_state() {
  local pk session_pks

  info "resetting DynamoDB scaling-apply validation state"
  for pk in lease epoch epoch-cycle shard-config shard-config#epoch#1 shard-config#epoch#2 scaling#latest scaling#epoch#1 scaling#epoch#2; do
    delete_control_pk "$pk"
  done

  session_pks="$(aws_base --region "$(dynamodb_session_region)" dynamodb scan \
    --table-name "$(dynamodb_session_table)" \
    --consistent-read \
    --projection-expression pk \
    --filter-expression "begins_with(pk, :prefix)" \
    --expression-attribute-values '{":prefix":{"S":"session#"}}' \
    --query 'Items[].pk.S' \
    --output text)"
  for pk in $session_pks; do
    [[ -n "$pk" ]] || continue
    aws_base --region "$(dynamodb_session_region)" dynamodb delete-item \
      --table-name "$(dynamodb_session_table)" \
      --key "{\"pk\":{\"S\":\"$pk\"}}" >/dev/null
  done
}

assert_shard_config_count() {
  local path="$1"
  local version="$2"
  local shard_count="$3"
  local global_height="$4"
  python3 - "$path" "$version" "$shard_count" "$global_height" <<'PY'
import json
import sys

path, version, shard_count, global_height = sys.argv[1:]
item = json.load(open(path)).get("Item", {})
def number(name):
    return int(item[name]["N"])
checks = {
    "version": int(version),
    "shard_count": int(shard_count),
    "global_table_height": int(global_height),
}
for key, want in checks.items():
    got = number(key)
    if got != want:
        raise SystemExit(f"{path}: {key} mismatch: got {got}, want {want}")
PY
}

reset_all_remote_workspaces
kill_all_remote_processes
mkdir -p "$APPLY_LOCAL_DIR"
reset_dynamodb_validation_state

info "seeding one active shard in DynamoDB while keeping two configured endpoints"
ACTIVE_SHARD_COUNT=1 FORCE_SHARD_CONFIG=1 "$SCRIPT_DIR/07-create-control-table.sh"
load_state

info "starting sharded servers for scaling-apply validation"
start_remote_server "$SHARD0_FOLLOWER_PUBLIC_IP" 1 0 "$(server_pair_csv "$SHARD0_LEADER_PRIVATE_IP" "$SHARD0_LEADER_PORT" "$SHARD0_FOLLOWER_PRIVATE_IP" "$SHARD0_FOLLOWER_PORT")" "$APPLY_RESULTS_REMOTE/shard0" "$APPLY_LOGS_REMOTE/shard0-follower.log"
start_remote_server "$SHARD0_LEADER_PUBLIC_IP" 0 0 "$(server_pair_csv "$SHARD0_LEADER_PRIVATE_IP" "$SHARD0_LEADER_PORT" "$SHARD0_FOLLOWER_PRIVATE_IP" "$SHARD0_FOLLOWER_PORT")" "$APPLY_RESULTS_REMOTE/shard0" "$APPLY_LOGS_REMOTE/shard0-leader.log"
start_remote_server "$SHARD1_FOLLOWER_PUBLIC_IP" 1 1 "$(server_pair_csv "$SHARD1_LEADER_PRIVATE_IP" "$SHARD1_LEADER_PORT" "$SHARD1_FOLLOWER_PRIVATE_IP" "$SHARD1_FOLLOWER_PORT")" "$APPLY_RESULTS_REMOTE/shard1" "$APPLY_LOGS_REMOTE/shard1-follower.log"
start_remote_server "$SHARD1_LEADER_PUBLIC_IP" 0 1 "$(server_pair_csv "$SHARD1_LEADER_PRIVATE_IP" "$SHARD1_LEADER_PORT" "$SHARD1_FOLLOWER_PRIVATE_IP" "$SHARD1_FOLLOWER_PORT")" "$APPLY_RESULTS_REMOTE/shard1" "$APPLY_LOGS_REMOTE/shard1-leader.log"
remote_wait_for_port "$SHARD0_LEADER_PUBLIC_IP" "127.0.0.1" "$SHARD0_LEADER_PORT"
remote_wait_for_port "$SHARD1_LEADER_PUBLIC_IP" "127.0.0.1" "$SHARD1_LEADER_PORT"

info "starting coordinator with grow recommendation thresholds"
COORDINATOR_EXTRA_ARGS="-scaling-min-shards 1 -scaling-max-shards 2 -scaling-up-density '$APPLY_GROW_THRESHOLD' -scaling-down-density 0" \
  start_remote_coordinator "$COORDINATOR_PUBLIC_IP" "$(coordinator_addr)" "$APPLY_LOGS_REMOTE/coordinator.log"
remote_wait_for_port "$COORDINATOR_PUBLIC_IP" "127.0.0.1" "$COORDINATOR_PORT"
capture_status initial
assert_json_field "$APPLY_LOCAL_DIR/status-initial.json" current_shard_count 1
assert_json_field "$APPLY_LOCAL_DIR/status-initial.json" global_table_height 256
capture_dynamodb_snapshot initial
assert_shard_config_count "$APPLY_LOCAL_DIR/dynamodb-initial/shard-config.json" 1 1 256

info "starting epoch 1 before apply"
start_line="$(retry_start_epoch coordinator "$COORDINATOR_PUBLIC_IP" "$(coordinator_addr)" "$APPLY_EPOCH1_SECONDS")"
epoch1="$(extract_field "$start_line" "epoch")"
[[ -n "$epoch1" ]] || die "could not parse epoch 1 id from: $start_line"
wait_for_status_state coordinator "$COORDINATOR_PUBLIC_IP" "$(coordinator_addr)" active 20 >/dev/null || die "epoch 1 did not become active"
capture_status epoch1-active
assert_json_field "$APPLY_LOCAL_DIR/status-epoch1-active.json" epoch_cycle_state active

info "verifying row 256 is rejected before apply"
if remote_cmd "$CLIENT_PUBLIC_IP" "mkdir -p '$APPLY_LOGS_REMOTE'; ~/client -coordinator '$(coordinator_addr)' -x 2 -y 256 -payload pre-apply-row256 -threads '$CLIENT_THREADS' -log '$APPLY_LOGS_REMOTE/client-pre-apply-row256.log'"; then
  die "row 256 unexpectedly succeeded before scaling apply"
fi

remote_cmd "$CLIENT_PUBLIC_IP" "mkdir -p '$APPLY_LOGS_REMOTE'; ~/client -coordinator '$(coordinator_addr)' -x 1 -y 0 -payload epoch1-row0 -threads '$CLIENT_THREADS' -log '$APPLY_LOGS_REMOTE/client-epoch1-row0.log'"
wait_for_epoch_complete coordinator "$COORDINATOR_PUBLIC_IP" "$(coordinator_addr)" 120
wait_for_epoch_complete server "$SHARD0_LEADER_PUBLIC_IP" "$(shard0_leader_addr)" 120
capture_status before-apply
assert_json_field "$APPLY_LOCAL_DIR/status-before-apply.json" current_shard_count 1
assert_json_field "$APPLY_LOCAL_DIR/status-before-apply.json" latest_scaling_action grow
assert_json_field "$APPLY_LOCAL_DIR/status-before-apply.json" latest_scaling_recommended_shards 2
assert_json_field "$APPLY_LOCAL_DIR/status-before-apply.json" scaling_apply_status applicable
assert_json_field "$APPLY_LOCAL_DIR/status-before-apply.json" epoch_cycle_state recommendation_ready
capture_dynamodb_snapshot before-apply
assert_shard_config_count "$APPLY_LOCAL_DIR/dynamodb-before-apply/epoch-shard-config.json" 1 1 256

info "dry-running latest scaling recommendation"
remote_cmd "$COORDINATOR_PUBLIC_IP" "~/coordinator -admin-target '$(coordinator_addr)' -dry-run-scaling-recommendation > '$APPLY_LOGS_REMOTE/dry-run-output.log' 2>&1"
copy_from_remote "$COORDINATOR_PUBLIC_IP" "$APPLY_LOGS_REMOTE/dry-run-output.log" "$APPLY_LOCAL_DIR/dry-run-output.log"
grep -q "applied=false" "$APPLY_LOCAL_DIR/dry-run-output.log" || die "dry-run output did not report applied=false"
grep -q "dry_run=true" "$APPLY_LOCAL_DIR/dry-run-output.log" || die "dry-run output did not report dry_run=true"
grep -q "version=1->2" "$APPLY_LOCAL_DIR/dry-run-output.log" || die "dry-run output did not report version 1->2"
grep -q "shards=1->2" "$APPLY_LOCAL_DIR/dry-run-output.log" || die "dry-run output did not report shards 1->2"
grep -q "global_table_height=256->512" "$APPLY_LOCAL_DIR/dry-run-output.log" || die "dry-run output did not report global table height 256->512"
capture_status after-dry-run
assert_json_field "$APPLY_LOCAL_DIR/status-after-dry-run.json" current_shard_count 1
assert_json_field "$APPLY_LOCAL_DIR/status-after-dry-run.json" global_table_height 256
assert_json_field "$APPLY_LOCAL_DIR/status-after-dry-run.json" epoch_cycle_state recommendation_ready
capture_dynamodb_snapshot after-dry-run
assert_shard_config_count "$APPLY_LOCAL_DIR/dynamodb-after-dry-run/shard-config.json" 1 1 256

if [[ "$APPLY_WITH_AUTOSCALER" == "1" || "$APPLY_WITH_AUTOSCALER" == "true" ]]; then
  info "applying latest scaling recommendation through autoscaler"
  run_remote_autoscaler_once "$COORDINATOR_PUBLIC_IP" "$(coordinator_addr)" "$APPLY_LOGS_REMOTE/autoscaler.log" 1
  copy_from_remote "$COORDINATOR_PUBLIC_IP" "$APPLY_LOGS_REMOTE/autoscaler.log" "$APPLY_LOCAL_DIR/autoscaler.log"
  grep -q "decision=dry_run_applicable" "$APPLY_LOCAL_DIR/autoscaler.log" || die "autoscaler log did not report dry_run_applicable"
  grep -q "decision=applied" "$APPLY_LOCAL_DIR/autoscaler.log" || die "autoscaler log did not report applied"
else
  info "applying latest scaling recommendation"
  remote_cmd "$COORDINATOR_PUBLIC_IP" "~/coordinator -admin-target '$(coordinator_addr)' -apply-scaling-recommendation > '$APPLY_LOGS_REMOTE/apply-output.log' 2>&1"
  copy_from_remote "$COORDINATOR_PUBLIC_IP" "$APPLY_LOGS_REMOTE/apply-output.log" "$APPLY_LOCAL_DIR/apply-output.log"
  grep -q "version=1->2" "$APPLY_LOCAL_DIR/apply-output.log" || die "apply output did not report version 1->2"
  grep -q "shards=1->2" "$APPLY_LOCAL_DIR/apply-output.log" || die "apply output did not report shards 1->2"
  grep -q "global_table_height=256->512" "$APPLY_LOCAL_DIR/apply-output.log" || die "apply output did not report global table height 256->512"
fi
capture_status after-apply
assert_json_field "$APPLY_LOCAL_DIR/status-after-apply.json" current_shard_count 2
assert_json_field "$APPLY_LOCAL_DIR/status-after-apply.json" global_table_height 512
assert_json_field "$APPLY_LOCAL_DIR/status-after-apply.json" epoch_cycle_state scaling_applied
capture_dynamodb_snapshot after-apply
assert_shard_config_count "$APPLY_LOCAL_DIR/dynamodb-after-apply/shard-config.json" 2 2 512

info "starting epoch 2 after apply"
start_line="$(retry_start_epoch coordinator "$COORDINATOR_PUBLIC_IP" "$(coordinator_addr)" "$APPLY_EPOCH2_SECONDS")"
epoch2="$(extract_field "$start_line" "epoch")"
[[ -n "$epoch2" ]] || die "could not parse epoch 2 id from: $start_line"
wait_for_status_state coordinator "$COORDINATOR_PUBLIC_IP" "$(coordinator_addr)" active 20 >/dev/null || die "epoch 2 did not become active"
capture_status epoch2-active
assert_json_field "$APPLY_LOCAL_DIR/status-epoch2-active.json" current_shard_count 2
assert_json_field "$APPLY_LOCAL_DIR/status-epoch2-active.json" epoch_cycle_state active

remote_cmd "$CLIENT_PUBLIC_IP" "mkdir -p '$APPLY_LOGS_REMOTE'; ~/client -coordinator '$(coordinator_addr)' -x 7 -y 0 -payload epoch2-row0 -threads '$CLIENT_THREADS' -log '$APPLY_LOGS_REMOTE/client-epoch2-row0.log'"
remote_cmd "$CLIENT_PUBLIC_IP" "mkdir -p '$APPLY_LOGS_REMOTE'; ~/client -coordinator '$(coordinator_addr)' -x 8 -y 256 -payload epoch2-row256 -threads '$CLIENT_THREADS' -log '$APPLY_LOGS_REMOTE/client-epoch2-row256.log'"
wait_for_epoch_complete coordinator "$COORDINATOR_PUBLIC_IP" "$(coordinator_addr)" 120
wait_for_epoch_complete server "$SHARD0_LEADER_PUBLIC_IP" "$(shard0_leader_addr)" 120
wait_for_epoch_complete server "$SHARD1_LEADER_PUBLIC_IP" "$(shard1_leader_addr)" 120
capture_status epoch2-completed
capture_dynamodb_snapshot epoch2-completed
assert_shard_config_count "$APPLY_LOCAL_DIR/dynamodb-epoch2-completed/epoch-shard-config.json" 2 2 512

local_epoch2_shard0="$APPLY_LOCAL_DIR/epoch2-$(result_file_name "$epoch2" 0)"
local_epoch2_shard1="$APPLY_LOCAL_DIR/epoch2-$(result_file_name "$epoch2" 1)"
copy_from_remote "$SHARD0_LEADER_PUBLIC_IP" "$APPLY_RESULTS_REMOTE/shard0/$(result_file_name "$epoch2" 0)" "$local_epoch2_shard0"
copy_from_remote "$SHARD1_LEADER_PUBLIC_IP" "$APPLY_RESULTS_REMOTE/shard1/$(result_file_name "$epoch2" 1)" "$local_epoch2_shard1"
assert_result_contains_slot "$local_epoch2_shard0" 0 0 256 0 7 "epoch2-row0"
assert_result_contains_slot "$local_epoch2_shard1" 1 256 512 256 8 "epoch2-row256"

cat <<EOF
AWS scaling apply validation passed.
  epoch before apply: $epoch1
  epoch after apply:  $epoch2
  autoscaler apply:   $APPLY_WITH_AUTOSCALER
  artifacts:          $APPLY_LOCAL_DIR
EOF
