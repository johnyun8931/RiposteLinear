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

public_entry_multi_coordinator_enabled || die "09-validate-multi-coordinator-ingress.sh requires PUBLIC_ENTRY_BACKEND=nlb and PUBLIC_ENTRY_MULTI_COORDINATOR=1"
dynamodb_control_enabled || die "09-validate-multi-coordinator-ingress.sh requires CONTROL_STORE_BACKEND=dynamodb"
dynamodb_session_enabled || die "09-validate-multi-coordinator-ingress.sh requires SESSION_STORE_BACKEND=dynamodb"

MULTI_LOCAL_DIR="$STATE_DIR/multi-coordinator-ingress"
MULTI_REMOTE_ROOT="$REMOTE_SMOKE_DIR/multi-coordinator-ingress/run"
MULTI_LOGS_REMOTE="$MULTI_REMOTE_ROOT/logs"
MULTI_RESULTS_REMOTE="$MULTI_REMOTE_ROOT/results"
MULTI_EPOCH_SECONDS="${MULTI_EPOCH_SECONDS:-90}"
MULTI_LEASE_TTL_SECONDS="${MULTI_LEASE_TTL_SECONDS:-8}"
MULTI_LEASE_RENEW_SECONDS="${MULTI_LEASE_RENEW_SECONDS:-2}"
COORDINATOR_A_HOLDER="$(coordinator_holder_id)-a"
COORDINATOR_B_HOLDER="$(coordinator_holder_id)-b"

cleanup() {
  stop_cloudwatch_status_pollers "$MULTI_LOGS_REMOTE"
  kill_all_remote_processes
}
trap cleanup EXIT

capture_status() {
  local label="$1"
  local target_addr="$2"
  local remote_path="$MULTI_LOGS_REMOTE/status-${label}.json"
  local local_path="$MULTI_LOCAL_DIR/status-${label}.json"
  capture_remote_status_json coordinator "$COORDINATOR_PUBLIC_IP" "$target_addr" "$remote_path"
  copy_from_remote "$COORDINATOR_PUBLIC_IP" "$remote_path" "$local_path"
}

capture_dynamodb_snapshot() {
  local stage="$1"
  local local_dir="$MULTI_LOCAL_DIR/dynamodb-$stage"
  local remote_dir="$MULTI_LOGS_REMOTE/dynamodb-$stage"
  mkdir -p "$local_dir"
  remote_cmd "$COORDINATOR_PUBLIC_IP" "mkdir -p '$remote_dir'"
  capture_dynamodb_control_item lease "$local_dir/lease.json"
  capture_dynamodb_control_item epoch "$local_dir/epoch.json"
  capture_dynamodb_control_item shard-config "$local_dir/shard-config.json"
  if capture_dynamodb_epoch_shard_config "$local_dir/epoch.json" "$local_dir/epoch-shard-config.json"; then
    copy_to_remote "$local_dir/epoch-shard-config.json" "$COORDINATOR_PUBLIC_IP" "$remote_dir/epoch-shard-config.json"
  fi
  aws_base --region "$(dynamodb_session_region)" dynamodb scan \
    --table-name "$(dynamodb_session_table)" \
    --consistent-read \
    --filter-expression "begins_with(pk, :prefix)" \
    --expression-attribute-values '{":prefix":{"S":"session#"}}' \
    --output json >"$local_dir/sessions.json"
  copy_to_remote "$local_dir/lease.json" "$COORDINATOR_PUBLIC_IP" "$remote_dir/lease.json"
  copy_to_remote "$local_dir/epoch.json" "$COORDINATOR_PUBLIC_IP" "$remote_dir/epoch.json"
  copy_to_remote "$local_dir/shard-config.json" "$COORDINATOR_PUBLIC_IP" "$remote_dir/shard-config.json"
  copy_to_remote "$local_dir/sessions.json" "$COORDINATOR_PUBLIC_IP" "$remote_dir/sessions.json"
}

reset_dynamodb_validation_state() {
  local pk session_pks

  info "resetting transient DynamoDB validation state"
  for pk in lease epoch epoch-cycle scaling#latest scaling#epoch#1 scaling#epoch#2 shard-config#epoch#1 shard-config#epoch#2; do
    aws_control_region dynamodb delete-item \
      --table-name "$(dynamodb_control_table)" \
      --key "{\"pk\":{\"S\":\"$pk\"}}" >/dev/null
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

assert_role() {
  local path="$1"
  local want="$2"
  python3 - "$path" "$want" <<'PY'
import json
import sys

path, want = sys.argv[1:]
try:
    status = json.load(open(path))
except (OSError, json.JSONDecodeError) as err:
    raise SystemExit(f"{path}: invalid JSON: {err}")
actual = status.get("role")
if actual != want:
    raise SystemExit(f"expected role {want}, got {actual}")
PY
}

wait_for_coordinator_role() {
  local target_addr="$1"
  local want="$2"
  local timeout="$3"
  local deadline role
  deadline=$((SECONDS + timeout))
  while true; do
    role="$(remote_cmd "$COORDINATOR_PUBLIC_IP" "~/coordinator -admin-target '$target_addr' -status 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin).get(\"role\", \"\"))'" 2>/dev/null || true)"
    if [[ "$role" == "$want" ]]; then
      return 0
    fi
    if (( SECONDS >= deadline )); then
      return 1
    fi
    sleep 2
  done
}

stop_remote_pid() {
  local pid_path="$1"
  remote_cmd "$COORDINATOR_PUBLIC_IP" "if [[ -s '$pid_path' ]]; then pid=\$(cat '$pid_path'); kill -TERM \"\$pid\" >/dev/null 2>&1 || true; for _ in \$(seq 1 20); do kill -0 \"\$pid\" >/dev/null 2>&1 || exit 0; sleep 1; done; kill -KILL \"\$pid\" >/dev/null 2>&1 || true; fi"
}

reset_all_remote_workspaces
kill_all_remote_processes
mkdir -p "$MULTI_LOCAL_DIR"
reset_dynamodb_validation_state
start_cloudwatch_status_pollers "coordinator-failover" "$MULTI_LOGS_REMOTE"
write_cloudwatch_demo_event "coordinator-failover" "$MULTI_LOGS_REMOTE" "scenario_start" "multi-coordinator failover validation started"

info "starting sharded servers for multi-coordinator ingress validation"
start_remote_server "$SHARD0_FOLLOWER_PUBLIC_IP" 1 0 "$(server_pair_csv "$SHARD0_LEADER_PRIVATE_IP" "$SHARD0_LEADER_PORT" "$SHARD0_FOLLOWER_PRIVATE_IP" "$SHARD0_FOLLOWER_PORT")" "$MULTI_RESULTS_REMOTE/shard0" "$MULTI_LOGS_REMOTE/shard0-follower.log"
start_remote_server "$SHARD0_LEADER_PUBLIC_IP" 0 0 "$(server_pair_csv "$SHARD0_LEADER_PRIVATE_IP" "$SHARD0_LEADER_PORT" "$SHARD0_FOLLOWER_PRIVATE_IP" "$SHARD0_FOLLOWER_PORT")" "$MULTI_RESULTS_REMOTE/shard0" "$MULTI_LOGS_REMOTE/shard0-leader.log"
start_remote_server "$SHARD1_FOLLOWER_PUBLIC_IP" 1 1 "$(server_pair_csv "$SHARD1_LEADER_PRIVATE_IP" "$SHARD1_LEADER_PORT" "$SHARD1_FOLLOWER_PRIVATE_IP" "$SHARD1_FOLLOWER_PORT")" "$MULTI_RESULTS_REMOTE/shard1" "$MULTI_LOGS_REMOTE/shard1-follower.log"
start_remote_server "$SHARD1_LEADER_PUBLIC_IP" 0 1 "$(server_pair_csv "$SHARD1_LEADER_PRIVATE_IP" "$SHARD1_LEADER_PORT" "$SHARD1_FOLLOWER_PRIVATE_IP" "$SHARD1_FOLLOWER_PORT")" "$MULTI_RESULTS_REMOTE/shard1" "$MULTI_LOGS_REMOTE/shard1-leader.log"
remote_wait_for_port "$SHARD0_LEADER_PUBLIC_IP" "127.0.0.1" "$SHARD0_LEADER_PORT"
remote_wait_for_port "$SHARD1_LEADER_PUBLIC_IP" "127.0.0.1" "$SHARD1_LEADER_PORT"
wait_for_server_peer_ready "$COORDINATOR_PUBLIC_IP" "$(shard0_leader_addr)" 60
wait_for_server_peer_ready "$COORDINATOR_PUBLIC_IP" "$(shard1_leader_addr)" 60

info "starting coordinator A active candidate and coordinator B standby"
start_remote_coordinator "$COORDINATOR_PUBLIC_IP" "$(coordinator_addr)" "$MULTI_LOGS_REMOTE/coordinator-a.log" "$COORDINATOR_A_HOLDER" "$MULTI_LEASE_TTL_SECONDS" "$MULTI_LEASE_RENEW_SECONDS" "$MULTI_LOGS_REMOTE/coordinator-a.pid"
remote_wait_for_port "$COORDINATOR_PUBLIC_IP" "127.0.0.1" "$COORDINATOR_PORT"
start_remote_coordinator "$COORDINATOR_PUBLIC_IP" "$(coordinator_standby_addr)" "$MULTI_LOGS_REMOTE/coordinator-b.log" "$COORDINATOR_B_HOLDER" "$MULTI_LEASE_TTL_SECONDS" "$MULTI_LEASE_RENEW_SECONDS" "$MULTI_LOGS_REMOTE/coordinator-b.pid" 1
remote_wait_for_port "$COORDINATOR_PUBLIC_IP" "127.0.0.1" "$COORDINATOR_STANDBY_PORT"

info "ensuring both coordinator ports are registered with the NLB"
aws_region elbv2 register-targets \
  --target-group-arn "$NLB_TARGET_GROUP_ARN" \
  --targets "Id=$COORDINATOR_ID,Port=$COORDINATOR_PORT" "Id=$COORDINATOR_ID,Port=$COORDINATOR_STANDBY_PORT"

wait_for_nlb_target_healthy 180 "$COORDINATOR_PORT"
wait_for_nlb_target_healthy 180 "$COORDINATOR_STANDBY_PORT"
capture_nlb_artifacts "$MULTI_LOCAL_DIR/nlb-both-targets"
remote_cmd "$COORDINATOR_PUBLIC_IP" "mkdir -p '$MULTI_LOGS_REMOTE/nlb-both-targets'"
copy_to_remote "$MULTI_LOCAL_DIR/nlb-both-targets/target-health.json" "$COORDINATOR_PUBLIC_IP" "$MULTI_LOGS_REMOTE/nlb-both-targets/target-health.json"

capture_status a-active "$(coordinator_addr)"
capture_status b-passive "$(coordinator_standby_addr)"
assert_role "$MULTI_LOCAL_DIR/status-a-active.json" active
assert_role "$MULTI_LOCAL_DIR/status-b-passive.json" passive

info "starting epoch 1 through active coordinator A"
start_line="$(retry_start_epoch coordinator "$COORDINATOR_PUBLIC_IP" "$(coordinator_addr)" "$MULTI_EPOCH_SECONDS")"
epoch1="$(extract_field "$start_line" "epoch")"
[[ -n "$epoch1" ]] || die "could not parse epoch 1 id from: $start_line"
wait_for_status_state coordinator "$COORDINATOR_PUBLIC_IP" "$(coordinator_addr)" active 20 >/dev/null || die "coordinator A did not report active epoch"
capture_dynamodb_snapshot epoch1-active

info "forcing public NLB traffic to passive coordinator B"
aws_region elbv2 deregister-targets \
  --target-group-arn "$NLB_TARGET_GROUP_ARN" \
  --targets "Id=$COORDINATOR_ID,Port=$COORDINATOR_PORT"
sleep 10
capture_nlb_artifacts "$MULTI_LOCAL_DIR/nlb-passive-only"
remote_cmd "$COORDINATOR_PUBLIC_IP" "mkdir -p '$MULTI_LOGS_REMOTE/nlb-passive-only'"
copy_to_remote "$MULTI_LOCAL_DIR/nlb-passive-only/target-health.json" "$COORDINATOR_PUBLIC_IP" "$MULTI_LOGS_REMOTE/nlb-passive-only/target-health.json"

info "sending row 0 through NLB to passive coordinator B"
remote_cmd "$CLIENT_PUBLIC_IP" "mkdir -p '$MULTI_LOGS_REMOTE'; ~/client -coordinator '$(public_coordinator_addr)' -x 1 -y 0 -payload passive-route -threads '$CLIENT_THREADS' -log '$MULTI_LOGS_REMOTE/client-passive-route.log'"
capture_dynamodb_snapshot epoch1-after-passive-upload

wait_for_epoch_complete coordinator "$COORDINATOR_PUBLIC_IP" "$(coordinator_addr)" 120
local_epoch1_shard0="$MULTI_LOCAL_DIR/epoch1-$(result_file_name "$epoch1" 0)"
copy_from_remote "$SHARD0_LEADER_PUBLIC_IP" "$MULTI_RESULTS_REMOTE/shard0/$(result_file_name "$epoch1" 0)" "$local_epoch1_shard0"
assert_result_contains_slot "$local_epoch1_shard0" 0 0 256 0 1 "passive-route"

info "stopping coordinator A and waiting for coordinator B promotion"
write_cloudwatch_demo_event "coordinator-failover" "$MULTI_LOGS_REMOTE" "kill_coordinator_a" "stopping active coordinator A"
stop_remote_pid "$MULTI_LOGS_REMOTE/coordinator-a.pid"
wait_for_coordinator_role "$(coordinator_standby_addr)" active "$((MULTI_LEASE_TTL_SECONDS + 60))" || die "coordinator B did not become active after coordinator A stopped"
write_cloudwatch_demo_event "coordinator-failover" "$MULTI_LOGS_REMOTE" "coordinator_b_promoted" "standby coordinator acquired lease"
capture_status b-promoted "$(coordinator_standby_addr)"
assert_role "$MULTI_LOCAL_DIR/status-b-promoted.json" active
capture_dynamodb_snapshot b-promoted

info "starting epoch 2 through promoted coordinator B"
start_line="$(retry_start_epoch coordinator "$COORDINATOR_PUBLIC_IP" "$(coordinator_standby_addr)" "$MULTI_EPOCH_SECONDS")"
epoch2="$(extract_field "$start_line" "epoch")"
[[ -n "$epoch2" ]] || die "could not parse epoch 2 id from: $start_line"
remote_cmd "$CLIENT_PUBLIC_IP" "mkdir -p '$MULTI_LOGS_REMOTE'; ~/client -coordinator '$(public_coordinator_addr)' -x 2 -y 256 -payload promoted-route -threads '$CLIENT_THREADS' -log '$MULTI_LOGS_REMOTE/client-promoted-route.log'"
wait_for_epoch_complete coordinator "$COORDINATOR_PUBLIC_IP" "$(coordinator_standby_addr)" 120
local_epoch2_shard1="$MULTI_LOCAL_DIR/epoch2-$(result_file_name "$epoch2" 1)"
copy_from_remote "$SHARD1_LEADER_PUBLIC_IP" "$MULTI_RESULTS_REMOTE/shard1/$(result_file_name "$epoch2" 1)" "$local_epoch2_shard1"
assert_result_contains_slot "$local_epoch2_shard1" 1 256 512 256 2 "promoted-route"
capture_dynamodb_snapshot epoch2-completed
write_cloudwatch_demo_event "coordinator-failover" "$MULTI_LOGS_REMOTE" "scenario_complete" "writes completed after coordinator promotion"

cat <<EOF
AWS multi-coordinator ingress validation passed.
  epoch 1 through passive NLB target: $epoch1
  epoch 2 after standby promotion: $epoch2
  coordinator A holder: $COORDINATOR_A_HOLDER
  coordinator B holder: $COORDINATOR_B_HOLDER
EOF
