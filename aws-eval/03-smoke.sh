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

SMOKE_LOCAL_DIR="$STATE_DIR/smoke"
SMOKE_EPOCH_SECONDS="${SMOKE_EPOCH_SECONDS:-8}"
SMOKE_RESULTS_REMOTE="$(smoke_results_dir)"
SMOKE_LOGS_REMOTE="$(smoke_log_dir)"
SMOKE_S3_PREFIX="$(results_s3_phase_prefix smoke)"

cleanup() {
  kill_all_remote_processes
}
trap cleanup EXIT

reset_all_remote_workspaces
kill_all_remote_processes
mkdir -p "$SMOKE_LOCAL_DIR"

info "starting full sharded topology for smoke test"
start_remote_server "$SHARD0_FOLLOWER_PUBLIC_IP" 1 0 "$(server_pair_csv "$SHARD0_LEADER_PRIVATE_IP" "$SHARD0_LEADER_PORT" "$SHARD0_FOLLOWER_PRIVATE_IP" "$SHARD0_FOLLOWER_PORT")" "$SMOKE_RESULTS_REMOTE/shard0" "$SMOKE_LOGS_REMOTE/shard0-follower.log" "$SMOKE_S3_PREFIX"
start_remote_server "$SHARD0_LEADER_PUBLIC_IP" 0 0 "$(server_pair_csv "$SHARD0_LEADER_PRIVATE_IP" "$SHARD0_LEADER_PORT" "$SHARD0_FOLLOWER_PRIVATE_IP" "$SHARD0_FOLLOWER_PORT")" "$SMOKE_RESULTS_REMOTE/shard0" "$SMOKE_LOGS_REMOTE/shard0-leader.log" "$SMOKE_S3_PREFIX"
start_remote_server "$SHARD1_FOLLOWER_PUBLIC_IP" 1 1 "$(server_pair_csv "$SHARD1_LEADER_PRIVATE_IP" "$SHARD1_LEADER_PORT" "$SHARD1_FOLLOWER_PRIVATE_IP" "$SHARD1_FOLLOWER_PORT")" "$SMOKE_RESULTS_REMOTE/shard1" "$SMOKE_LOGS_REMOTE/shard1-follower.log" "$SMOKE_S3_PREFIX"
start_remote_server "$SHARD1_LEADER_PUBLIC_IP" 0 1 "$(server_pair_csv "$SHARD1_LEADER_PRIVATE_IP" "$SHARD1_LEADER_PORT" "$SHARD1_FOLLOWER_PRIVATE_IP" "$SHARD1_FOLLOWER_PORT")" "$SMOKE_RESULTS_REMOTE/shard1" "$SMOKE_LOGS_REMOTE/shard1-leader.log" "$SMOKE_S3_PREFIX"

remote_wait_for_port "$SHARD0_LEADER_PUBLIC_IP" "127.0.0.1" "$SHARD0_LEADER_PORT"
remote_wait_for_port "$SHARD1_LEADER_PUBLIC_IP" "127.0.0.1" "$SHARD1_LEADER_PORT"

start_remote_coordinator "$COORDINATOR_PUBLIC_IP" "$(coordinator_addr)" "$SMOKE_LOGS_REMOTE/coordinator.log" "$SMOKE_S3_PREFIX"
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

if [[ -n "${RESULTS_S3_BUCKET:-}" ]]; then
  padded_epoch="$(printf '%06d' "$epoch_id")"
  for key in \
    "$SMOKE_S3_PREFIX/shards/0/latest.json" \
    "$SMOKE_S3_PREFIX/shards/1/latest.json" \
    "$SMOKE_S3_PREFIX/shards/0/epochs/$padded_epoch/result.json" \
    "$SMOKE_S3_PREFIX/shards/0/epochs/$padded_epoch/result.bin" \
    "$SMOKE_S3_PREFIX/shards/1/epochs/$padded_epoch/result.json" \
    "$SMOKE_S3_PREFIX/shards/1/epochs/$padded_epoch/result.bin"; do
    aws_region s3api head-object --bucket "$RESULTS_S3_BUCKET" --key "$key" >/dev/null
  done
fi

info "reading deterministic smoke slots back through the coordinator"
remote_cmd "$CLIENT_PUBLIC_IP" "mkdir -p '$SMOKE_RESULTS_REMOTE/reads' '$SMOKE_LOGS_REMOTE'; ~/client -coordinator '$(coordinator_addr)' -read-latest -x 1 -y 0 -threads '$CLIENT_THREADS' -log '$SMOKE_LOGS_REMOTE/read-row0.log' > '$SMOKE_RESULTS_REMOTE/reads/row0-col1.bin'"
remote_cmd "$CLIENT_PUBLIC_IP" "mkdir -p '$SMOKE_RESULTS_REMOTE/reads' '$SMOKE_LOGS_REMOTE'; ~/client -coordinator '$(coordinator_addr)' -read-latest -x 2 -y 128 -threads '$CLIENT_THREADS' -log '$SMOKE_LOGS_REMOTE/read-row128.log' > '$SMOKE_RESULTS_REMOTE/reads/row128-col2.bin'"
remote_cmd "$CLIENT_PUBLIC_IP" "python3 - '$SMOKE_RESULTS_REMOTE/reads/row0-col1.bin' shard0-boundary '$SMOKE_RESULTS_REMOTE/reads/row128-col2.bin' shard1-boundary <<'PY'
from pathlib import Path
import sys

checks = [
    (Path(sys.argv[1]), sys.argv[2].encode()),
    (Path(sys.argv[3]), sys.argv[4].encode()),
]

for path, want in checks:
    data = path.read_bytes()
    if len(data) != 160:
        raise SystemExit(f'{path}: expected 160 bytes, got {len(data)}')
    if not data.startswith(want):
        raise SystemExit(f'{path}: payload mismatch, first bytes={data[:len(want)]!r}, want={want!r}')
print('read smoke slots ok')
PY"

cat <<EOF
AWS smoke test passed.
  epoch: $epoch_id
  shard0 result: $local_shard0
  shard1 result: $local_shard1
  read checks: row0/col1 and row128/col2
  s3 prefix: s3://$RESULTS_S3_BUCKET/$SMOKE_S3_PREFIX
EOF
