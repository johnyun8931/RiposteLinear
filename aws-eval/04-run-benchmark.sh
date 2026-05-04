#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
USER_SERVER_THREADS="${SERVER_THREADS-}"
USER_CLIENT_THREADS="${CLIENT_THREADS-}"
USER_CLIENT_CONCURRENCY="${CLIENT_CONCURRENCY-}"
USER_WARMUP_EPOCH_SECONDS="${WARMUP_EPOCH_SECONDS-}"
USER_MEASURED_EPOCH_SECONDS="${MEASURED_EPOCH_SECONDS-}"
USER_POST_EPOCH_FLUSH_SECONDS="${POST_EPOCH_FLUSH_SECONDS-}"
USER_CLIENT_EXIT_GRACE_SECONDS="${CLIENT_EXIT_GRACE_SECONDS-}"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"
load_state

[[ -n "$USER_SERVER_THREADS" ]] && SERVER_THREADS="$USER_SERVER_THREADS"
[[ -n "$USER_CLIENT_THREADS" ]] && CLIENT_THREADS="$USER_CLIENT_THREADS"
[[ -n "$USER_CLIENT_CONCURRENCY" ]] && CLIENT_CONCURRENCY="$USER_CLIENT_CONCURRENCY"
[[ -n "$USER_WARMUP_EPOCH_SECONDS" ]] && WARMUP_EPOCH_SECONDS="$USER_WARMUP_EPOCH_SECONDS"
[[ -n "$USER_MEASURED_EPOCH_SECONDS" ]] && MEASURED_EPOCH_SECONDS="$USER_MEASURED_EPOCH_SECONDS"
[[ -n "$USER_POST_EPOCH_FLUSH_SECONDS" ]] && POST_EPOCH_FLUSH_SECONDS="$USER_POST_EPOCH_FLUSH_SECONDS"
[[ -n "$USER_CLIENT_EXIT_GRACE_SECONDS" ]] && CLIENT_EXIT_GRACE_SECONDS="$USER_CLIENT_EXIT_GRACE_SECONDS"

require_cmd ssh

cleanup() {
  kill_all_remote_processes
}
trap cleanup EXIT

write_benchmark_state() {
  cat >"$BENCHMARK_STATE_FILE" <<EOF_STATE
SERVER_THREADS=$(quote "$SERVER_THREADS")
CLIENT_THREADS=$(quote "$CLIENT_THREADS")
CLIENT_CONCURRENCY=$(quote "$CLIENT_CONCURRENCY")
WARMUP_EPOCH_SECONDS=$(quote "$WARMUP_EPOCH_SECONDS")
MEASURED_EPOCH_SECONDS=$(quote "$MEASURED_EPOCH_SECONDS")
POST_EPOCH_FLUSH_SECONDS=$(quote "$POST_EPOCH_FLUSH_SECONDS")
CLIENT_EXIT_GRACE_SECONDS=$(quote "$CLIENT_EXIT_GRACE_SECONDS")
EOF_STATE
}

classify_remote_client_exit() {
  local log_path="$1"
  remote_cmd "$CLIENT_PUBLIC_IP" "if grep -q 'unexpected EOF' '$log_path' 2>/dev/null; then echo unexpected_eof; elif grep -q 'server overloaded: ready queue full' '$log_path' 2>/dev/null; then echo overload; elif grep -q 'No active epoch' '$log_path' 2>/dev/null; then echo no_active_epoch; elif grep -q 'Client died.' '$log_path' 2>/dev/null; then echo exited_without_no_active_epoch; else echo log_inconclusive; fi"
}

wait_for_phase_client() {
  local phase="$1"
  local duration="$2"
  local phase_logs="$3"
  local client_pid_path="$4"
  local client_log_path="$phase_logs/client.log"
  local wait_timeout=$((duration + CLIENT_EXIT_GRACE_SECONDS))
  local wait_status client_reason valid invalid_reason

  info "waiting up to ${wait_timeout}s for $phase client to exit"
  set +e
  wait_remote_pid_exit "$CLIENT_PUBLIC_IP" "$client_pid_path" "$wait_timeout"
  wait_status=$?
  set -e

  valid="false"
  invalid_reason=""

  if [[ "$wait_status" -eq 0 ]]; then
    client_reason="$(classify_remote_client_exit "$client_log_path")"
    if [[ "$client_reason" == "no_active_epoch" ]]; then
      valid="true"
    elif [[ "$client_reason" == "overload" ]]; then
      invalid_reason="client exited after server overload"
      capture_remote_process_snapshot "$CLIENT_PUBLIC_IP" "$phase_logs/client-process-snapshot.txt"
    else
      invalid_reason="client exited without clean No active epoch signal"
      capture_remote_process_snapshot "$CLIENT_PUBLIC_IP" "$phase_logs/client-process-snapshot.txt"
    fi
  elif [[ "$wait_status" -eq 1 ]]; then
    client_reason="timeout"
    invalid_reason="client did not exit before timeout"
    capture_remote_process_snapshot "$CLIENT_PUBLIC_IP" "$phase_logs/client-process-snapshot.txt"
  else
    client_reason="missing_pid"
    invalid_reason="client pid file was not created before timeout"
    capture_remote_process_snapshot "$CLIENT_PUBLIC_IP" "$phase_logs/client-process-snapshot.txt"
  fi

  write_remote_phase_status "$CLIENT_PUBLIC_IP" "$phase" "$duration" "$wait_timeout" "$valid" "$wait_status" "$client_reason" "$invalid_reason"

  if [[ "$valid" != "true" ]]; then
    info "$phase marked invalid: $invalid_reason ($client_reason)"
    return 1
  fi

  sleep "$POST_EPOCH_FLUSH_SECONDS"
  return 0
}

run_baseline_phase() {
  local phase="$1"
  local duration="$2"
  local phase_logs
  local phase_results
  local client_pid_path
  phase_logs="$(phase_log_dir "$phase")"
  phase_results="$(phase_results_dir "$phase")"
  client_pid_path="$(phase_dir "$phase")/client.pid"

  info "starting baseline topology for $phase (${duration}s)"
  kill_all_remote_processes
  start_remote_server "$SHARD0_FOLLOWER_PUBLIC_IP" 1 0 "$(server_pair_csv "$SHARD0_LEADER_PRIVATE_IP" "$SHARD0_LEADER_PORT" "$SHARD0_FOLLOWER_PRIVATE_IP" "$SHARD0_FOLLOWER_PORT")" "$phase_results/shard0" "$phase_logs/shard0-follower.log"
  start_remote_server "$SHARD0_LEADER_PUBLIC_IP" 0 0 "$(server_pair_csv "$SHARD0_LEADER_PRIVATE_IP" "$SHARD0_LEADER_PORT" "$SHARD0_FOLLOWER_PRIVATE_IP" "$SHARD0_FOLLOWER_PORT")" "$phase_results/shard0" "$phase_logs/shard0-leader.log"
  remote_wait_for_port "$SHARD0_LEADER_PUBLIC_IP" "127.0.0.1" "$SHARD0_LEADER_PORT"

  retry_start_epoch server "$SHARD0_LEADER_PUBLIC_IP" "$(shard0_leader_addr)" "$duration" >/dev/null
  start_remote_hammer_client "$CLIENT_PUBLIC_IP" -leader "$(shard0_leader_addr)" "$phase_logs/client.log" "$client_pid_path"
  if ! wait_for_phase_client "$phase" "$duration" "$phase_logs" "$client_pid_path"; then
    kill_all_remote_processes
    return 1
  fi
  kill_all_remote_processes
}

# run_sharded_phase "sharded-measured" "$MEASURED_EPOCH_SECONDS"

run_sharded_phase() {
  local phase="$1"
  local duration="$2"
  local phase_logs
  local phase_results
  local client_pid_path
  phase_logs="$(phase_log_dir "$phase")"
  phase_results="$(phase_results_dir "$phase")"
  client_pid_path="$(phase_dir "$phase")/client.pid"

  info "starting sharded topology for $phase (${duration}s)"
  kill_all_remote_processes
  start_remote_server "$SHARD0_FOLLOWER_PUBLIC_IP" 1 0 "$(server_pair_csv "$SHARD0_LEADER_PRIVATE_IP" "$SHARD0_LEADER_PORT" "$SHARD0_FOLLOWER_PRIVATE_IP" "$SHARD0_FOLLOWER_PORT")" "$phase_results/shard0" "$phase_logs/shard0-follower.log"
  start_remote_server "$SHARD0_LEADER_PUBLIC_IP" 0 0 "$(server_pair_csv "$SHARD0_LEADER_PRIVATE_IP" "$SHARD0_LEADER_PORT" "$SHARD0_FOLLOWER_PRIVATE_IP" "$SHARD0_FOLLOWER_PORT")" "$phase_results/shard0" "$phase_logs/shard0-leader.log"
  start_remote_server "$SHARD1_FOLLOWER_PUBLIC_IP" 1 1 "$(server_pair_csv "$SHARD1_LEADER_PRIVATE_IP" "$SHARD1_LEADER_PORT" "$SHARD1_FOLLOWER_PRIVATE_IP" "$SHARD1_FOLLOWER_PORT")" "$phase_results/shard1" "$phase_logs/shard1-follower.log"
  start_remote_server "$SHARD1_LEADER_PUBLIC_IP" 0 1 "$(server_pair_csv "$SHARD1_LEADER_PRIVATE_IP" "$SHARD1_LEADER_PORT" "$SHARD1_FOLLOWER_PRIVATE_IP" "$SHARD1_FOLLOWER_PORT")" "$phase_results/shard1" "$phase_logs/shard1-leader.log"
  remote_wait_for_port "$SHARD0_LEADER_PUBLIC_IP" "127.0.0.1" "$SHARD0_LEADER_PORT"
  remote_wait_for_port "$SHARD1_LEADER_PUBLIC_IP" "127.0.0.1" "$SHARD1_LEADER_PORT"

  start_remote_coordinator "$COORDINATOR_PUBLIC_IP" "$(coordinator_addr)" "$phase_logs/coordinator.log"
  remote_wait_for_port "$COORDINATOR_PUBLIC_IP" "127.0.0.1" "$COORDINATOR_PORT"

  retry_start_epoch coordinator "$COORDINATOR_PUBLIC_IP" "$(coordinator_addr)" "$duration" >/dev/null
  start_remote_hammer_client "$CLIENT_PUBLIC_IP" -coordinator "$(coordinator_addr)" "$phase_logs/client.log" "$client_pid_path"
  if ! wait_for_phase_client "$phase" "$duration" "$phase_logs" "$client_pid_path"; then
    kill_all_remote_processes
    return 1
  fi
  kill_all_remote_processes
}

info "resetting remote workspaces"
write_benchmark_state
reset_all_remote_workspaces
kill_all_remote_processes

run_baseline_phase "baseline-warmup" "$WARMUP_EPOCH_SECONDS" || die "baseline warm-up failed validation; stopping benchmark before measured phases"
run_baseline_phase "baseline-measured" "$MEASURED_EPOCH_SECONDS" || die "baseline measured phase failed validation"
run_sharded_phase "sharded-warmup" "$WARMUP_EPOCH_SECONDS" || die "sharded warm-up failed validation; stopping benchmark before sharded measured phase"
run_sharded_phase "sharded-measured" "$MEASURED_EPOCH_SECONDS" || die "sharded measured phase failed validation"

cat <<EOF
AWS benchmark run complete.
  baseline warm-up:  ${WARMUP_EPOCH_SECONDS}s
  baseline measured: ${MEASURED_EPOCH_SECONDS}s
  sharded warm-up:   ${WARMUP_EPOCH_SECONDS}s
  sharded measured:  ${MEASURED_EPOCH_SECONDS}s
  client exit grace: ${CLIENT_EXIT_GRACE_SECONDS}s
  client concurrency: ${CLIENT_CONCURRENCY}
EOF
