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

cloudwatch_observability_enabled || die "13-demo-coordinator-mid-epoch-failover-cloudwatch.sh requires CLOUDWATCH_OBSERVABILITY=1"
public_entry_multi_coordinator_enabled || die "13-demo-coordinator-mid-epoch-failover-cloudwatch.sh requires PUBLIC_ENTRY_BACKEND=nlb and PUBLIC_ENTRY_MULTI_COORDINATOR=1"
dynamodb_control_enabled || die "13-demo-coordinator-mid-epoch-failover-cloudwatch.sh requires CONTROL_STORE_BACKEND=dynamodb"
dynamodb_session_enabled || die "13-demo-coordinator-mid-epoch-failover-cloudwatch.sh requires SESSION_STORE_BACKEND=dynamodb"

STRICT_LOCAL_DIR="$STATE_DIR/coordinator-mid-epoch-failover"
STRICT_REMOTE_ROOT="$REMOTE_SMOKE_DIR/coordinator-mid-epoch-failover/run"
STRICT_LOGS_REMOTE="$STRICT_REMOTE_ROOT/logs"
STRICT_RESULTS_REMOTE="$STRICT_REMOTE_ROOT/results"
STRICT_EPOCH_SECONDS="${STRICT_EPOCH_SECONDS:-140}"
STRICT_LEASE_TTL_SECONDS="${STRICT_LEASE_TTL_SECONDS:-8}"
STRICT_LEASE_RENEW_SECONDS="${STRICT_LEASE_RENEW_SECONDS:-2}"
COORDINATOR_A_HOLDER="$(coordinator_holder_id)-strict-a"
COORDINATOR_B_HOLDER="$(coordinator_holder_id)-strict-b"

cleanup() {
  stop_cloudwatch_status_pollers "$STRICT_LOGS_REMOTE"
  kill_all_remote_processes
}
trap cleanup EXIT

stop_remote_pid() {
  local pid_path="$1"
  remote_cmd "$COORDINATOR_PUBLIC_IP" "if [[ -s '$pid_path' ]]; then pid=\$(cat '$pid_path'); kill -TERM \"\$pid\" >/dev/null 2>&1 || true; for _ in \$(seq 1 20); do kill -0 \"\$pid\" >/dev/null 2>&1 || exit 0; sleep 1; done; kill -KILL \"\$pid\" >/dev/null 2>&1 || true; fi"
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

capture_status() {
  local label="$1"
  local target_addr="$2"
  local remote_path="$STRICT_LOGS_REMOTE/status-${label}.json"
  local local_path="$STRICT_LOCAL_DIR/status-${label}.json"
  capture_remote_status_json coordinator "$COORDINATOR_PUBLIC_IP" "$target_addr" "$remote_path"
  copy_from_remote "$COORDINATOR_PUBLIC_IP" "$remote_path" "$local_path"
}

capture_dynamodb_snapshot() {
  local stage="$1"
  local local_dir="$STRICT_LOCAL_DIR/dynamodb-$stage"
  local remote_dir="$STRICT_LOGS_REMOTE/dynamodb-$stage"
  mkdir -p "$local_dir"
  remote_cmd "$COORDINATOR_PUBLIC_IP" "mkdir -p '$remote_dir'"
  capture_dynamodb_control_item lease "$local_dir/lease.json"
  capture_dynamodb_control_item epoch "$local_dir/epoch.json"
  capture_dynamodb_control_item shard-config "$local_dir/shard-config.json"
  copy_to_remote "$local_dir/lease.json" "$COORDINATOR_PUBLIC_IP" "$remote_dir/lease.json"
  copy_to_remote "$local_dir/epoch.json" "$COORDINATOR_PUBLIC_IP" "$remote_dir/epoch.json"
  copy_to_remote "$local_dir/shard-config.json" "$COORDINATOR_PUBLIC_IP" "$remote_dir/shard-config.json"
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

reset_all_remote_workspaces
kill_all_remote_processes
mkdir -p "$STRICT_LOCAL_DIR"
reset_dynamodb_validation_state
start_cloudwatch_status_pollers "coordinator-mid-epoch-failover" "$STRICT_LOGS_REMOTE"
write_cloudwatch_demo_event "coordinator-mid-epoch-failover" "$STRICT_LOGS_REMOTE" "scenario_start" "mid-epoch coordinator failover validation started"

info "starting active shard processes"
start_remote_server "$SHARD0_FOLLOWER_PUBLIC_IP" 1 0 "$(server_pair_csv "$SHARD0_LEADER_PRIVATE_IP" "$SHARD0_LEADER_PORT" "$SHARD0_FOLLOWER_PRIVATE_IP" "$SHARD0_FOLLOWER_PORT")" "$STRICT_RESULTS_REMOTE/shard0" "$STRICT_LOGS_REMOTE/shard0-follower.log"
start_remote_server "$SHARD0_LEADER_PUBLIC_IP" 0 0 "$(server_pair_csv "$SHARD0_LEADER_PRIVATE_IP" "$SHARD0_LEADER_PORT" "$SHARD0_FOLLOWER_PRIVATE_IP" "$SHARD0_FOLLOWER_PORT")" "$STRICT_RESULTS_REMOTE/shard0" "$STRICT_LOGS_REMOTE/shard0-leader.log"
start_remote_server "$SHARD1_FOLLOWER_PUBLIC_IP" 1 1 "$(server_pair_csv "$SHARD1_LEADER_PRIVATE_IP" "$SHARD1_LEADER_PORT" "$SHARD1_FOLLOWER_PRIVATE_IP" "$SHARD1_FOLLOWER_PORT")" "$STRICT_RESULTS_REMOTE/shard1" "$STRICT_LOGS_REMOTE/shard1-follower.log"
start_remote_server "$SHARD1_LEADER_PUBLIC_IP" 0 1 "$(server_pair_csv "$SHARD1_LEADER_PRIVATE_IP" "$SHARD1_LEADER_PORT" "$SHARD1_FOLLOWER_PRIVATE_IP" "$SHARD1_FOLLOWER_PORT")" "$STRICT_RESULTS_REMOTE/shard1" "$STRICT_LOGS_REMOTE/shard1-leader.log"
if hot_standby_ingestion_enabled; then
  start_remote_server "$SHARD0_FOLLOWER_PUBLIC_IP" 1 0 "$(server_pair_csv "$SHARD0_LEADER_PRIVATE_IP" "$SHARD0_STANDBY_LEADER_PORT" "$SHARD0_FOLLOWER_PRIVATE_IP" "$SHARD0_STANDBY_FOLLOWER_PORT")" "$STRICT_RESULTS_REMOTE/shard0-standby" "$STRICT_LOGS_REMOTE/shard0-standby-follower.log" standby
  start_remote_server "$SHARD0_LEADER_PUBLIC_IP" 0 0 "$(server_pair_csv "$SHARD0_LEADER_PRIVATE_IP" "$SHARD0_STANDBY_LEADER_PORT" "$SHARD0_FOLLOWER_PRIVATE_IP" "$SHARD0_STANDBY_FOLLOWER_PORT")" "$STRICT_RESULTS_REMOTE/shard0-standby" "$STRICT_LOGS_REMOTE/shard0-standby-leader.log" standby
  start_remote_server "$SHARD1_FOLLOWER_PUBLIC_IP" 1 1 "$(server_pair_csv "$SHARD1_LEADER_PRIVATE_IP" "$SHARD1_STANDBY_LEADER_PORT" "$SHARD1_FOLLOWER_PRIVATE_IP" "$SHARD1_STANDBY_FOLLOWER_PORT")" "$STRICT_RESULTS_REMOTE/shard1-standby" "$STRICT_LOGS_REMOTE/shard1-standby-follower.log" standby
  start_remote_server "$SHARD1_LEADER_PUBLIC_IP" 0 1 "$(server_pair_csv "$SHARD1_LEADER_PRIVATE_IP" "$SHARD1_STANDBY_LEADER_PORT" "$SHARD1_FOLLOWER_PRIVATE_IP" "$SHARD1_STANDBY_FOLLOWER_PORT")" "$STRICT_RESULTS_REMOTE/shard1-standby" "$STRICT_LOGS_REMOTE/shard1-standby-leader.log" standby
fi

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

info "starting coordinator A active candidate and coordinator B standby"
start_remote_coordinator "$COORDINATOR_PUBLIC_IP" "$(coordinator_addr)" "$STRICT_LOGS_REMOTE/coordinator-a.log" "$COORDINATOR_A_HOLDER" "$STRICT_LEASE_TTL_SECONDS" "$STRICT_LEASE_RENEW_SECONDS" "$STRICT_LOGS_REMOTE/coordinator-a.pid"
remote_wait_for_port "$COORDINATOR_PUBLIC_IP" "127.0.0.1" "$COORDINATOR_PORT"
start_remote_coordinator "$COORDINATOR_PUBLIC_IP" "$(coordinator_standby_addr)" "$STRICT_LOGS_REMOTE/coordinator-b.log" "$COORDINATOR_B_HOLDER" "$STRICT_LEASE_TTL_SECONDS" "$STRICT_LEASE_RENEW_SECONDS" "$STRICT_LOGS_REMOTE/coordinator-b.pid" 1
remote_wait_for_port "$COORDINATOR_PUBLIC_IP" "127.0.0.1" "$COORDINATOR_STANDBY_PORT"

info "registering both coordinator ports with the NLB"
aws_region elbv2 register-targets \
  --target-group-arn "$NLB_TARGET_GROUP_ARN" \
  --targets "Id=$COORDINATOR_ID,Port=$COORDINATOR_PORT" "Id=$COORDINATOR_ID,Port=$COORDINATOR_STANDBY_PORT"
wait_for_nlb_target_healthy 180 "$COORDINATOR_PORT"
wait_for_nlb_target_healthy 180 "$COORDINATOR_STANDBY_PORT"

info "starting epoch 1 through coordinator A"
start_line="$(retry_start_epoch coordinator "$COORDINATOR_PUBLIC_IP" "$(coordinator_addr)" "$STRICT_EPOCH_SECONDS")"
epoch1="$(extract_field "$start_line" "epoch")"
[[ -n "$epoch1" ]] || die "could not parse epoch 1 id from: $start_line"
capture_status a-epoch1-active "$(coordinator_addr)"
capture_status b-epoch1-passive "$(coordinator_standby_addr)"
capture_dynamodb_snapshot epoch1-started

info "forcing NLB traffic to coordinator B while B is passive"
aws_region elbv2 deregister-targets \
  --target-group-arn "$NLB_TARGET_GROUP_ARN" \
  --targets "Id=$COORDINATOR_ID,Port=$COORDINATOR_PORT"
sleep 10

remote_cmd "$CLIENT_PUBLIC_IP" "mkdir -p '$STRICT_LOGS_REMOTE'; ~/client -coordinator '$(public_coordinator_addr)' -x 1 -y 0 -payload strict-passive-route -threads '$CLIENT_THREADS' -log '$STRICT_LOGS_REMOTE/client-passive-route.log'"
write_cloudwatch_demo_event "coordinator-mid-epoch-failover" "$STRICT_LOGS_REMOTE" "passive_route_succeeded" "coordinator B routed upload during epoch 1 before lease acquisition"

info "stopping coordinator A during epoch 1"
write_cloudwatch_demo_event "coordinator-mid-epoch-failover" "$STRICT_LOGS_REMOTE" "kill_coordinator_a_mid_epoch" "stopping active coordinator A before epoch 1 ends"
stop_remote_pid "$STRICT_LOGS_REMOTE/coordinator-a.pid"
wait_for_coordinator_role "$(coordinator_standby_addr)" active "$((STRICT_LEASE_TTL_SECONDS + 60))" || die "coordinator B did not become active during epoch 1"
capture_status b-mid-epoch-active "$(coordinator_standby_addr)"
capture_dynamodb_snapshot b-mid-epoch-active
write_cloudwatch_demo_event "coordinator-mid-epoch-failover" "$STRICT_LOGS_REMOTE" "coordinator_b_active_mid_epoch" "coordinator B acquired lease before epoch 1 ended"

remote_cmd "$CLIENT_PUBLIC_IP" "mkdir -p '$STRICT_LOGS_REMOTE'; ~/client -coordinator '$(public_coordinator_addr)' -x 2 -y 256 -payload strict-active-route -threads '$CLIENT_THREADS' -log '$STRICT_LOGS_REMOTE/client-active-route.log'"
write_cloudwatch_demo_event "coordinator-mid-epoch-failover" "$STRICT_LOGS_REMOTE" "active_route_succeeded" "coordinator B routed upload after acquiring lease during epoch 1"

wait_for_epoch_complete server "$SHARD0_LEADER_PUBLIC_IP" "$(shard0_leader_addr)" 180
wait_for_epoch_complete server "$SHARD1_LEADER_PUBLIC_IP" "$(shard1_leader_addr)" 180
wait_for_sqs_ingestion_drain 180

local_shard0="$STRICT_LOCAL_DIR/$(result_file_name "$epoch1" 0)"
local_shard1="$STRICT_LOCAL_DIR/$(result_file_name "$epoch1" 1)"
copy_from_remote "$SHARD0_LEADER_PUBLIC_IP" "$STRICT_RESULTS_REMOTE/shard0/$(result_file_name "$epoch1" 0)" "$local_shard0"
copy_from_remote "$SHARD1_LEADER_PUBLIC_IP" "$STRICT_RESULTS_REMOTE/shard1/$(result_file_name "$epoch1" 1)" "$local_shard1"
assert_result_contains_slot "$local_shard0" 0 0 256 0 1 "strict-passive-route"
assert_result_contains_slot "$local_shard1" 1 256 512 256 2 "strict-active-route"
capture_dynamodb_snapshot epoch1-shards-completed
write_cloudwatch_demo_event "coordinator-mid-epoch-failover" "$STRICT_LOGS_REMOTE" "scenario_complete" "mid-epoch coordinator failover writes reached shard results"

cat <<EOF
AWS mid-epoch coordinator failover validation passed.
  epoch: $epoch1
  passive-route payload: strict-passive-route
  post-lease-acquisition payload: strict-active-route
  coordinator A holder: $COORDINATOR_A_HOLDER
  coordinator B holder: $COORDINATOR_B_HOLDER
EOF
