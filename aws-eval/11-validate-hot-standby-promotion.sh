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

dynamodb_control_enabled || die "11-validate-hot-standby-promotion.sh requires CONTROL_STORE_BACKEND=dynamodb"
dynamodb_session_enabled || die "11-validate-hot-standby-promotion.sh requires SESSION_STORE_BACKEND=dynamodb"
sqs_ingestion_enabled || die "11-validate-hot-standby-promotion.sh requires INGESTION_QUEUE_BACKEND=sqs"
hot_standby_ingestion_enabled || die "11-validate-hot-standby-promotion.sh requires HOT_STANDBY_INGESTION=1"

PROMOTE_LOCAL_DIR="$STATE_DIR/hot-standby-promotion"
PROMOTE_REMOTE_ROOT="$REMOTE_SMOKE_DIR/hot-standby-promotion/run"
PROMOTE_LOGS_REMOTE="$PROMOTE_REMOTE_ROOT/logs"
PROMOTE_RESULTS_REMOTE="$PROMOTE_REMOTE_ROOT/results"
PROMOTE_EPOCH_SECONDS="${PROMOTE_EPOCH_SECONDS:-30}"

cleanup() {
  kill_all_remote_processes
}
trap cleanup EXIT

info "resetting DynamoDB control/session state for hot-standby promotion validation"
FORCE_CONTROL_STATE=1 FORCE_SHARD_CONFIG=1 "$SCRIPT_DIR/07-create-control-table.sh"
load_state

capture_promotion_status() {
  local label="$1"
  local remote_path="$PROMOTE_LOGS_REMOTE/status-${label}.json"
  local local_path="$PROMOTE_LOCAL_DIR/status-${label}.json"
  capture_remote_status_json coordinator "$COORDINATOR_PUBLIC_IP" "$(coordinator_addr)" "$remote_path"
  copy_from_remote "$COORDINATOR_PUBLIC_IP" "$remote_path" "$local_path"
}

stop_active_shard0_leader() {
  remote_cmd "$SHARD0_LEADER_PUBLIC_IP" "python3 - '$PROMOTE_LOGS_REMOTE/shard0-leader.log' <<'PY'
import os
import signal
import shlex
import subprocess
import sys
import time

needle = sys.argv[1]
self_pid = os.getpid()
ps = subprocess.run(['ps', '-eo', 'pid=,args='], text=True, stdout=subprocess.PIPE, check=True)
pids = []
for line in ps.stdout.splitlines():
    stripped = line.strip()
    if not stripped:
        continue
    pid_text, _, args = stripped.partition(' ')
    try:
        pid = int(pid_text)
    except ValueError:
        continue
    if pid == self_pid:
        continue
    try:
        argv = shlex.split(args)
    except ValueError:
        argv = args.split()
    if argv and os.path.basename(argv[0]) == 'server' and needle in args:
        pids.append(pid)
for pid in pids:
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
time.sleep(2)
for pid in pids:
    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
print('stopped active shard0 leader pids=' + ','.join(map(str, pids)))
PY"
}

reset_all_remote_workspaces
kill_all_remote_processes
mkdir -p "$PROMOTE_LOCAL_DIR"

info "starting active and hot-standby shard processes for promotion validation"
start_remote_server "$SHARD0_FOLLOWER_PUBLIC_IP" 1 0 "$(server_pair_csv "$SHARD0_LEADER_PRIVATE_IP" "$SHARD0_LEADER_PORT" "$SHARD0_FOLLOWER_PRIVATE_IP" "$SHARD0_FOLLOWER_PORT")" "$PROMOTE_RESULTS_REMOTE/shard0" "$PROMOTE_LOGS_REMOTE/shard0-follower.log"
start_remote_server "$SHARD0_LEADER_PUBLIC_IP" 0 0 "$(server_pair_csv "$SHARD0_LEADER_PRIVATE_IP" "$SHARD0_LEADER_PORT" "$SHARD0_FOLLOWER_PRIVATE_IP" "$SHARD0_FOLLOWER_PORT")" "$PROMOTE_RESULTS_REMOTE/shard0" "$PROMOTE_LOGS_REMOTE/shard0-leader.log"
start_remote_server "$SHARD1_FOLLOWER_PUBLIC_IP" 1 1 "$(server_pair_csv "$SHARD1_LEADER_PRIVATE_IP" "$SHARD1_LEADER_PORT" "$SHARD1_FOLLOWER_PRIVATE_IP" "$SHARD1_FOLLOWER_PORT")" "$PROMOTE_RESULTS_REMOTE/shard1" "$PROMOTE_LOGS_REMOTE/shard1-follower.log"
start_remote_server "$SHARD1_LEADER_PUBLIC_IP" 0 1 "$(server_pair_csv "$SHARD1_LEADER_PRIVATE_IP" "$SHARD1_LEADER_PORT" "$SHARD1_FOLLOWER_PRIVATE_IP" "$SHARD1_FOLLOWER_PORT")" "$PROMOTE_RESULTS_REMOTE/shard1" "$PROMOTE_LOGS_REMOTE/shard1-leader.log"
start_remote_server "$SHARD0_FOLLOWER_PUBLIC_IP" 1 0 "$(server_pair_csv "$SHARD0_LEADER_PRIVATE_IP" "$SHARD0_STANDBY_LEADER_PORT" "$SHARD0_FOLLOWER_PRIVATE_IP" "$SHARD0_STANDBY_FOLLOWER_PORT")" "$PROMOTE_RESULTS_REMOTE/shard0-standby" "$PROMOTE_LOGS_REMOTE/shard0-standby-follower.log" standby
start_remote_server "$SHARD0_LEADER_PUBLIC_IP" 0 0 "$(server_pair_csv "$SHARD0_LEADER_PRIVATE_IP" "$SHARD0_STANDBY_LEADER_PORT" "$SHARD0_FOLLOWER_PRIVATE_IP" "$SHARD0_STANDBY_FOLLOWER_PORT")" "$PROMOTE_RESULTS_REMOTE/shard0-standby" "$PROMOTE_LOGS_REMOTE/shard0-standby-leader.log" standby
start_remote_server "$SHARD1_FOLLOWER_PUBLIC_IP" 1 1 "$(server_pair_csv "$SHARD1_LEADER_PRIVATE_IP" "$SHARD1_STANDBY_LEADER_PORT" "$SHARD1_FOLLOWER_PRIVATE_IP" "$SHARD1_STANDBY_FOLLOWER_PORT")" "$PROMOTE_RESULTS_REMOTE/shard1-standby" "$PROMOTE_LOGS_REMOTE/shard1-standby-follower.log" standby
start_remote_server "$SHARD1_LEADER_PUBLIC_IP" 0 1 "$(server_pair_csv "$SHARD1_LEADER_PRIVATE_IP" "$SHARD1_STANDBY_LEADER_PORT" "$SHARD1_FOLLOWER_PRIVATE_IP" "$SHARD1_STANDBY_FOLLOWER_PORT")" "$PROMOTE_RESULTS_REMOTE/shard1-standby" "$PROMOTE_LOGS_REMOTE/shard1-standby-leader.log" standby

remote_wait_for_port "$SHARD0_LEADER_PUBLIC_IP" "127.0.0.1" "$SHARD0_LEADER_PORT"
remote_wait_for_port "$SHARD1_LEADER_PUBLIC_IP" "127.0.0.1" "$SHARD1_LEADER_PORT"
remote_wait_for_port "$SHARD0_LEADER_PUBLIC_IP" "127.0.0.1" "$SHARD0_STANDBY_LEADER_PORT"
remote_wait_for_port "$SHARD1_LEADER_PUBLIC_IP" "127.0.0.1" "$SHARD1_STANDBY_LEADER_PORT"

info "starting coordinator"
start_remote_coordinator "$COORDINATOR_PUBLIC_IP" "$(coordinator_addr)" "$PROMOTE_LOGS_REMOTE/coordinator.log"
remote_wait_for_port "$COORDINATOR_PUBLIC_IP" "127.0.0.1" "$COORDINATOR_PORT"

info "starting epoch 1 and warming standby completed-upload state"
start_line="$(retry_start_epoch coordinator "$COORDINATOR_PUBLIC_IP" "$(coordinator_addr)" "$PROMOTE_EPOCH_SECONDS")"
epoch1="$(extract_field "$start_line" "epoch")"
[[ -n "$epoch1" ]] || die "could not parse epoch 1 id from: $start_line"
remote_cmd "$CLIENT_PUBLIC_IP" "mkdir -p '$PROMOTE_LOGS_REMOTE'; ~/client -coordinator '$(public_coordinator_addr)' -x 1 -y 0 -payload pre-promotion -threads '$CLIENT_THREADS' -log '$PROMOTE_LOGS_REMOTE/client-pre-promotion.log'"
wait_for_epoch_complete coordinator "$COORDINATOR_PUBLIC_IP" "$(coordinator_addr)" 120
wait_for_sqs_ingestion_drain 120
capture_promotion_status "before-promotion"

info "stopping active shard 0 leader and force-promoting its standby"
stop_active_shard0_leader
remote_cmd "$COORDINATOR_PUBLIC_IP" "~/coordinator -admin-target '$(coordinator_addr)' -promote-shard-standby -promote-shard-id 0 -force-promote-shard > '$PROMOTE_LOGS_REMOTE/promote-shard0.log' 2>&1"
capture_promotion_status "after-promotion"
capture_dynamodb_control_item shard-config "$PROMOTE_LOCAL_DIR/shard-config-after-promotion.json"
copy_to_remote "$PROMOTE_LOCAL_DIR/shard-config-after-promotion.json" "$COORDINATOR_PUBLIC_IP" "$PROMOTE_LOGS_REMOTE/shard-config-after-promotion.json"

info "starting epoch 2 and verifying row 0 routes to promoted standby"
start_line="$(retry_start_epoch coordinator "$COORDINATOR_PUBLIC_IP" "$(coordinator_addr)" "$PROMOTE_EPOCH_SECONDS")"
epoch2="$(extract_field "$start_line" "epoch")"
[[ -n "$epoch2" ]] || die "could not parse epoch 2 id from: $start_line"
remote_cmd "$CLIENT_PUBLIC_IP" "mkdir -p '$PROMOTE_LOGS_REMOTE'; ~/client -coordinator '$(public_coordinator_addr)' -x 2 -y 0 -payload promoted-standby -threads '$CLIENT_THREADS' -log '$PROMOTE_LOGS_REMOTE/client-promoted-standby.log'"
wait_for_epoch_complete coordinator "$COORDINATOR_PUBLIC_IP" "$(coordinator_addr)" 120
wait_for_epoch_complete server "$SHARD0_LEADER_PUBLIC_IP" "$(shard0_standby_leader_addr)" 120

local_promoted_result="$PROMOTE_LOCAL_DIR/epoch2-promoted-$(result_file_name "$epoch2" 0)"
copy_from_remote "$SHARD0_LEADER_PUBLIC_IP" "$PROMOTE_RESULTS_REMOTE/shard0-standby/$(result_file_name "$epoch2" 0)" "$local_promoted_result"
assert_result_contains_slot "$local_promoted_result" 0 0 256 0 2 "promoted-standby"

cat <<EOF
AWS hot standby promotion validation passed.
  pre-promotion epoch: $epoch1
  promoted epoch: $epoch2
  promoted shard result: $local_promoted_result
EOF
