#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"
load_state

require_cmd ssh
require_cmd scp
require_cmd python3

SMOKE_LOCAL_DIR="$STATE_DIR/smoke"
SMOKE_EPOCH_SECONDS="${SMOKE_EPOCH_SECONDS:-8}"
SMOKE_RESULTS_REMOTE="$(smoke_results_dir)"
SMOKE_LOGS_REMOTE="$(smoke_log_dir)"

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

info "sending deterministic smoke writes through the coordinator"
remote_cmd "$CLIENT_PUBLIC_IP" "mkdir -p '$SMOKE_LOGS_REMOTE'; ~/client -coordinator '$(coordinator_addr)' -x 1 -y 0 -payload shard0-boundary -threads '$CLIENT_THREADS' -log '$SMOKE_LOGS_REMOTE/client-row0.log'"
remote_cmd "$CLIENT_PUBLIC_IP" "mkdir -p '$SMOKE_LOGS_REMOTE'; ~/client -coordinator '$(coordinator_addr)' -x 2 -y 128 -payload shard1-boundary -threads '$CLIENT_THREADS' -log '$SMOKE_LOGS_REMOTE/client-row128.log'"

wait_for_epoch_complete coordinator "$COORDINATOR_PUBLIC_IP" "$(coordinator_addr)" 120

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
