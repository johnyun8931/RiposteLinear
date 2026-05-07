#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"
load_state

require_cmd ssh
require_cmd scp
require_cmd python3
require_cmd aws

dynamodb_control_enabled || die "08-validate-coordinator-lease.sh requires CONTROL_STORE_BACKEND=dynamodb"

LEASE_VALIDATE_TTL_SECONDS="${LEASE_VALIDATE_TTL_SECONDS:-8}"
LEASE_VALIDATE_RENEW_SECONDS="${LEASE_VALIDATE_RENEW_SECONDS:-2}"
LEASE_VALIDATE_EPOCH_SECONDS="${LEASE_VALIDATE_EPOCH_SECONDS:-20}"
COORDINATOR_STANDBY_PORT="${COORDINATOR_STANDBY_PORT:-$((COORDINATOR_PORT + 1))}"

LEASE_LOCAL_DIR="$STATE_DIR/coordinator-lease"
LEASE_REMOTE_ROOT="$REMOTE_SMOKE_DIR/coordinator-lease/run"
LEASE_LOGS_REMOTE="$LEASE_REMOTE_ROOT/logs"
LEASE_RESULTS_REMOTE="$LEASE_REMOTE_ROOT/results"

COORDINATOR_A_ADDR="$COORDINATOR_PRIVATE_IP:$COORDINATOR_PORT"
COORDINATOR_B_ADDR="$COORDINATOR_PRIVATE_IP:$COORDINATOR_STANDBY_PORT"
COORDINATOR_A_HOLDER="$(coordinator_holder_id)-a"
COORDINATOR_B_HOLDER="$(coordinator_holder_id)-b"

cleanup() {
  kill_all_remote_processes
}
trap cleanup EXIT

assert_status_lease() {
  local file="$1"
  local holder="$2"
  local min_token="${3:-1}"
  python3 - "$file" "$holder" "$min_token" <<'PY'
import json
import sys

path, holder, min_token = sys.argv[1], sys.argv[2], int(sys.argv[3])
status = json.load(open(path))
if status.get("lease_holder") != holder:
    raise SystemExit(f"expected lease holder {holder}, got {status.get('lease_holder')}")
if int(status.get("lease_fencing_token", 0)) < min_token:
    raise SystemExit(f"expected lease fencing token >= {min_token}, got {status.get('lease_fencing_token')}")
if not status.get("lease_active"):
    raise SystemExit("expected active lease in coordinator status")
if int(status.get("lease_expires_unix_ms", 0)) <= 0:
    raise SystemExit("expected lease expiry timestamp")
PY
}

assert_dynamodb_lease() {
  local file="$1"
  local holder="$2"
  local min_token="${3:-1}"
  python3 - "$file" "$holder" "$min_token" <<'PY'
import json
import sys

path, holder, min_token = sys.argv[1], sys.argv[2], int(sys.argv[3])
item = json.load(open(path)).get("Item", {})
actual_holder = item.get("holder", {}).get("S")
actual_token = int(item.get("fencing_token", {}).get("N", "0"))
if actual_holder != holder:
    raise SystemExit(f"expected DynamoDB lease holder {holder}, got {actual_holder}")
if actual_token < min_token:
    raise SystemExit(f"expected DynamoDB fencing token >= {min_token}, got {actual_token}")
print(actual_token)
PY
}

capture_status_local_and_remote() {
  local target_addr="$1"
  local local_path="$2"
  local remote_path="$3"
  remote_cmd "$COORDINATOR_PUBLIC_IP" "~/coordinator -admin-target '$target_addr' -status" >"$local_path"
  copy_to_remote "$local_path" "$COORDINATOR_PUBLIC_IP" "$remote_path"
}

capture_dynamodb_lease_local_and_remote() {
  local local_path="$1"
  local remote_path="$2"
  capture_dynamodb_control_item lease "$local_path"
  copy_to_remote "$local_path" "$COORDINATOR_PUBLIC_IP" "$remote_path"
}

stop_remote_pid() {
  local pid_path="$1"
  remote_cmd "$COORDINATOR_PUBLIC_IP" "if [[ -s '$pid_path' ]]; then pid=\$(cat '$pid_path'); kill -TERM \"\$pid\" >/dev/null 2>&1 || true; for _ in \$(seq 1 20); do kill -0 \"\$pid\" >/dev/null 2>&1 || exit 0; sleep 1; done; kill -KILL \"\$pid\" >/dev/null 2>&1 || true; fi"
}

reset_all_remote_workspaces
kill_all_remote_processes
mkdir -p "$LEASE_LOCAL_DIR"

info "starting sharded servers for coordinator lease validation"
start_remote_server "$SHARD0_FOLLOWER_PUBLIC_IP" 1 0 "$(server_pair_csv "$SHARD0_LEADER_PRIVATE_IP" "$SHARD0_LEADER_PORT" "$SHARD0_FOLLOWER_PRIVATE_IP" "$SHARD0_FOLLOWER_PORT")" "$LEASE_RESULTS_REMOTE/shard0" "$LEASE_LOGS_REMOTE/shard0-follower.log"
start_remote_server "$SHARD0_LEADER_PUBLIC_IP" 0 0 "$(server_pair_csv "$SHARD0_LEADER_PRIVATE_IP" "$SHARD0_LEADER_PORT" "$SHARD0_FOLLOWER_PRIVATE_IP" "$SHARD0_FOLLOWER_PORT")" "$LEASE_RESULTS_REMOTE/shard0" "$LEASE_LOGS_REMOTE/shard0-leader.log"
start_remote_server "$SHARD1_FOLLOWER_PUBLIC_IP" 1 1 "$(server_pair_csv "$SHARD1_LEADER_PRIVATE_IP" "$SHARD1_LEADER_PORT" "$SHARD1_FOLLOWER_PRIVATE_IP" "$SHARD1_FOLLOWER_PORT")" "$LEASE_RESULTS_REMOTE/shard1" "$LEASE_LOGS_REMOTE/shard1-follower.log"
start_remote_server "$SHARD1_LEADER_PUBLIC_IP" 0 1 "$(server_pair_csv "$SHARD1_LEADER_PRIVATE_IP" "$SHARD1_LEADER_PORT" "$SHARD1_FOLLOWER_PRIVATE_IP" "$SHARD1_FOLLOWER_PORT")" "$LEASE_RESULTS_REMOTE/shard1" "$LEASE_LOGS_REMOTE/shard1-leader.log"
remote_wait_for_port "$SHARD0_LEADER_PUBLIC_IP" "127.0.0.1" "$SHARD0_LEADER_PORT"
remote_wait_for_port "$SHARD1_LEADER_PUBLIC_IP" "127.0.0.1" "$SHARD1_LEADER_PORT"

info "starting coordinator A with lease holder $COORDINATOR_A_HOLDER"
start_remote_coordinator "$COORDINATOR_PUBLIC_IP" "$COORDINATOR_A_ADDR" "$LEASE_LOGS_REMOTE/coordinator-a.log" "$COORDINATOR_A_HOLDER" "$LEASE_VALIDATE_TTL_SECONDS" "$LEASE_VALIDATE_RENEW_SECONDS" "$LEASE_LOGS_REMOTE/coordinator-a.pid"
remote_wait_for_port "$COORDINATOR_PUBLIC_IP" "127.0.0.1" "$COORDINATOR_PORT"
capture_status_local_and_remote "$COORDINATOR_A_ADDR" "$LEASE_LOCAL_DIR/status-a-active.json" "$LEASE_LOGS_REMOTE/status-a-active.json"
assert_status_lease "$LEASE_LOCAL_DIR/status-a-active.json" "$COORDINATOR_A_HOLDER"
capture_dynamodb_lease_local_and_remote "$LEASE_LOCAL_DIR/dynamodb-lease-a.json" "$LEASE_LOGS_REMOTE/dynamodb-lease-a.json"
token_a="$(assert_dynamodb_lease "$LEASE_LOCAL_DIR/dynamodb-lease-a.json" "$COORDINATOR_A_HOLDER")"

info "verifying coordinator B exits while coordinator A holds the lease"
start_remote_coordinator "$COORDINATOR_PUBLIC_IP" "$COORDINATOR_B_ADDR" "$LEASE_LOGS_REMOTE/coordinator-b-held.log" "$COORDINATOR_B_HOLDER" "$LEASE_VALIDATE_TTL_SECONDS" "$LEASE_VALIDATE_RENEW_SECONDS" "$LEASE_LOGS_REMOTE/coordinator-b-held.pid"
if ! wait_remote_pid_exit "$COORDINATOR_PUBLIC_IP" "$LEASE_LOGS_REMOTE/coordinator-b-held.pid" 20; then
  die "coordinator B did not exit while coordinator A held the lease"
fi
remote_cmd "$COORDINATOR_PUBLIC_IP" "grep -qi 'coordinator lease is held' '$LEASE_LOGS_REMOTE/coordinator-b-held.log'"

info "stopping coordinator A and waiting for lease expiry"
stop_remote_pid "$LEASE_LOGS_REMOTE/coordinator-a.pid"
sleep "$((LEASE_VALIDATE_TTL_SECONDS + 2))"

info "starting coordinator B after lease expiry"
start_remote_coordinator "$COORDINATOR_PUBLIC_IP" "$COORDINATOR_B_ADDR" "$LEASE_LOGS_REMOTE/coordinator-b-active.log" "$COORDINATOR_B_HOLDER" "$LEASE_VALIDATE_TTL_SECONDS" "$LEASE_VALIDATE_RENEW_SECONDS" "$LEASE_LOGS_REMOTE/coordinator-b-active.pid"
remote_wait_for_port "$COORDINATOR_PUBLIC_IP" "127.0.0.1" "$COORDINATOR_STANDBY_PORT"
capture_status_local_and_remote "$COORDINATOR_B_ADDR" "$LEASE_LOCAL_DIR/status-b-active.json" "$LEASE_LOGS_REMOTE/status-b-active.json"
assert_status_lease "$LEASE_LOCAL_DIR/status-b-active.json" "$COORDINATOR_B_HOLDER" "$((token_a + 1))"
capture_dynamodb_lease_local_and_remote "$LEASE_LOCAL_DIR/dynamodb-lease-b.json" "$LEASE_LOGS_REMOTE/dynamodb-lease-b.json"
token_b="$(assert_dynamodb_lease "$LEASE_LOCAL_DIR/dynamodb-lease-b.json" "$COORDINATOR_B_HOLDER" "$((token_a + 1))")"

info "starting an epoch through coordinator B"
start_line="$(retry_start_epoch coordinator "$COORDINATOR_PUBLIC_IP" "$COORDINATOR_B_ADDR" "$LEASE_VALIDATE_EPOCH_SECONDS")"
epoch_id="$(extract_field "$start_line" "epoch")"
[[ -n "$epoch_id" ]] || die "could not parse epoch id from: $start_line"
wait_for_status_state coordinator "$COORDINATOR_PUBLIC_IP" "$COORDINATOR_B_ADDR" active 20 >/dev/null || die "coordinator B did not reach active state"
capture_status_local_and_remote "$COORDINATOR_B_ADDR" "$LEASE_LOCAL_DIR/status-b-epoch-active.json" "$LEASE_LOGS_REMOTE/status-b-epoch-active.json"
assert_status_lease "$LEASE_LOCAL_DIR/status-b-epoch-active.json" "$COORDINATOR_B_HOLDER" "$token_b"
capture_dynamodb_lease_local_and_remote "$LEASE_LOCAL_DIR/dynamodb-lease-b-epoch.json" "$LEASE_LOGS_REMOTE/dynamodb-lease-b-epoch.json"
assert_dynamodb_lease "$LEASE_LOCAL_DIR/dynamodb-lease-b-epoch.json" "$COORDINATOR_B_HOLDER" "$token_b" >/dev/null

cat <<EOF
AWS coordinator lease validation passed.
  coordinator A holder: $COORDINATOR_A_HOLDER
  coordinator B holder: $COORDINATOR_B_HOLDER
  token A: $token_a
  token B: $token_b
  epoch started through B: $epoch_id
EOF
