#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/phase3-local-common.sh"

APPLY_DIR="$TMP_DIR/apply-scaling"
EPOCH1_DURATION="${APPLY_EPOCH1_DURATION:-5}"
EPOCH2_DURATION="${APPLY_EPOCH2_DURATION:-8}"
GROW_THRESHOLD="${APPLY_GROW_THRESHOLD:-0.001}"
PRE_APPLY_ROW256_LOG="$APPLY_DIR/pre-apply-row256.log"
EPOCH1_ROW0_LOG="$APPLY_DIR/epoch1-row0.log"
EPOCH2_ROW0_LOG="$APPLY_DIR/epoch2-row0.log"
EPOCH2_ROW256_LOG="$APPLY_DIR/epoch2-row256.log"

assert_status_field() {
	local file="$1"
	local key="$2"
	local want="$3"
	python3 - "$file" "$key" "$want" <<'PY'
import json
import sys

path, key, want = sys.argv[1:]
try:
    data = json.load(open(path))
except (OSError, json.JSONDecodeError) as err:
    raise SystemExit(f"{path}: invalid JSON: {err}")
got = data.get(key)
if str(got) != want:
    raise SystemExit(f"{path}: {key} mismatch: got {got!r}, want {want!r}")
PY
}

cleanup() {
	stop_all_phase3_processes
}
trap cleanup EXIT

reset_state_dirs
mkdir -p "$APPLY_DIR"
build_binaries

info "Starting shard inventory with one active shard seeded in the coordinator"
threads="$RIPOSTE_BENCH_SERVER_THREADS_DEFAULT"
thread_args=()
if [[ -n "$threads" && "$threads" -gt 0 ]]; then
	thread_args=(-threads "$threads")
fi

start_process "shard0-follower" "$SERVER_BIN" -idx 1 -servers "$SHARD0_LEADER_ADDR,$SHARD0_FOLLOWER_ADDR" -shard-id 0 -global-row-start 0 -results-dir "$RESULTS_S0_DIR" -log "$(log_file shard0-follower)" "${thread_args[@]}"
start_process "shard0-leader" "$SERVER_BIN" -idx 0 -servers "$SHARD0_LEADER_ADDR,$SHARD0_FOLLOWER_ADDR" -shard-id 0 -global-row-start 0 -results-dir "$RESULTS_S0_DIR" -log "$(log_file shard0-leader)" "${thread_args[@]}"
start_process "shard1-follower" "$SERVER_BIN" -idx 1 -servers "$SHARD1_LEADER_ADDR,$SHARD1_FOLLOWER_ADDR" -shard-id 1 -global-row-start "$ROWS_PER_SHARD" -results-dir "$RESULTS_S1_DIR" -log "$(log_file shard1-follower)" "${thread_args[@]}"
start_process "shard1-leader" "$SERVER_BIN" -idx 0 -servers "$SHARD1_LEADER_ADDR,$SHARD1_FOLLOWER_ADDR" -shard-id 1 -global-row-start "$ROWS_PER_SHARD" -results-dir "$RESULTS_S1_DIR" -log "$(log_file shard1-leader)" "${thread_args[@]}"
wait_for_port "$SHARD0_LEADER_ADDR" 50 || die "shard 0 leader did not start listening"
wait_for_port "$SHARD1_LEADER_ADDR" 50 || die "shard 1 leader did not start listening"
sleep "$RIPOSTE_TOPOLOGY_SETTLE_DELAY"

start_process "coordinator" "$COORDINATOR_BIN" -listen "$COORDINATOR_ADDR" -log "$(log_file coordinator)" \
	-initial-active-shards 1 \
	-scaling-min-shards 1 \
	-scaling-max-shards 2 \
	-scaling-up-density "$GROW_THRESHOLD" \
	-scaling-down-density 0 \
	-shard "0,0,256,$SHARD0_LEADER_ADDR,$SHARD0_FOLLOWER_ADDR" \
	-shard "1,256,512,$SHARD1_LEADER_ADDR,$SHARD1_FOLLOWER_ADDR"
wait_for_port "$COORDINATOR_ADDR" 50 || die "coordinator did not start listening"

status_json coordinator "$COORDINATOR_ADDR" >"$APPLY_DIR/status-initial.json"
assert_status_field "$APPLY_DIR/status-initial.json" current_shard_count 1
assert_status_field "$APPLY_DIR/status-initial.json" global_table_height 256
echo "PASS: initial active shard config uses one shard while two endpoints are configured"

info "Starting epoch 1 with one active shard"
start_line="$(start_epoch coordinator "$COORDINATOR_ADDR" "$EPOCH1_DURATION")"
epoch1="$(extract_field "$start_line" "epoch")"
[[ -n "$epoch1" ]] || die "could not parse epoch 1 id from: $start_line"
wait_for_status_state coordinator "$COORDINATOR_ADDR" active 10 >/dev/null || die "epoch 1 did not become active"
status_json coordinator "$COORDINATOR_ADDR" >"$APPLY_DIR/status-epoch1-active.json"
assert_status_field "$APPLY_DIR/status-epoch1-active.json" epoch_cycle_state active

if run_client -coordinator "$COORDINATOR_ADDR" -x 2 -y 256 -payload pre-apply-row256 -log "$PRE_APPLY_ROW256_LOG"; then
	die "row 256 unexpectedly succeeded before scaling apply"
fi
echo "PASS: row 256 is rejected before scaling apply"

run_client -coordinator "$COORDINATOR_ADDR" -x 1 -y 0 -payload epoch1-row0 -log "$EPOCH1_ROW0_LOG"
wait_for_status_state coordinator "$COORDINATOR_ADDR" completed 20 >/dev/null || die "epoch 1 did not complete"
wait_for_status_state server "$SHARD0_LEADER_ADDR" completed 20 >/dev/null || die "shard 0 did not complete epoch 1"
status_json coordinator "$COORDINATOR_ADDR" >"$APPLY_DIR/status-before-apply.json"
assert_status_field "$APPLY_DIR/status-before-apply.json" current_shard_count 1
assert_status_field "$APPLY_DIR/status-before-apply.json" latest_scaling_action grow
assert_status_field "$APPLY_DIR/status-before-apply.json" latest_scaling_recommended_shards 2
assert_status_field "$APPLY_DIR/status-before-apply.json" scaling_apply_status applicable
assert_status_field "$APPLY_DIR/status-before-apply.json" epoch_cycle_state recommendation_ready
echo "PASS: epoch 1 persisted an applicable grow recommendation"

info "Dry-running latest scaling recommendation"
dry_run_line="$(dry_run_scaling_recommendation "$COORDINATOR_ADDR")"
printf '%s\n' "$dry_run_line" >"$APPLY_DIR/dry-run-output.log"
grep -q "applied=false" "$APPLY_DIR/dry-run-output.log" || die "dry-run output did not report applied=false: $dry_run_line"
grep -q "dry_run=true" "$APPLY_DIR/dry-run-output.log" || die "dry-run output did not report dry_run=true: $dry_run_line"
grep -q "version=1->2" "$APPLY_DIR/dry-run-output.log" || die "dry-run output did not report version 1->2: $dry_run_line"
grep -q "shards=1->2" "$APPLY_DIR/dry-run-output.log" || die "dry-run output did not report shards 1->2: $dry_run_line"
grep -q "global_table_height=256->512" "$APPLY_DIR/dry-run-output.log" || die "dry-run output did not report global table height 256->512: $dry_run_line"
status_json coordinator "$COORDINATOR_ADDR" >"$APPLY_DIR/status-after-dry-run.json"
assert_status_field "$APPLY_DIR/status-after-dry-run.json" current_shard_count 1
assert_status_field "$APPLY_DIR/status-after-dry-run.json" global_table_height 256
assert_status_field "$APPLY_DIR/status-after-dry-run.json" epoch_cycle_state recommendation_ready
echo "PASS: dry-run validates the proposal without changing the active shard config"

info "Applying latest scaling recommendation"
apply_line="$(apply_scaling_recommendation "$COORDINATOR_ADDR")"
printf '%s\n' "$apply_line" >"$APPLY_DIR/apply-output.log"
grep -q "version=1->2" "$APPLY_DIR/apply-output.log" || die "apply output did not report version 1->2: $apply_line"
grep -q "shards=1->2" "$APPLY_DIR/apply-output.log" || die "apply output did not report shards 1->2: $apply_line"
grep -q "global_table_height=256->512" "$APPLY_DIR/apply-output.log" || die "apply output did not report global table height 256->512: $apply_line"
status_json coordinator "$COORDINATOR_ADDR" >"$APPLY_DIR/status-after-apply.json"
assert_status_field "$APPLY_DIR/status-after-apply.json" current_shard_count 2
assert_status_field "$APPLY_DIR/status-after-apply.json" global_table_height 512
assert_status_field "$APPLY_DIR/status-after-apply.json" epoch_cycle_state scaling_applied
echo "PASS: manual apply updated the active shard config to two shards"

info "Starting epoch 2 after scaling apply"
start_line="$(start_epoch coordinator "$COORDINATOR_ADDR" "$EPOCH2_DURATION")"
epoch2="$(extract_field "$start_line" "epoch")"
[[ -n "$epoch2" ]] || die "could not parse epoch 2 id from: $start_line"
if [[ "$epoch2" -le "$epoch1" ]]; then
	die "epoch 2 id $epoch2 did not advance beyond epoch 1 id $epoch1"
fi
wait_for_status_state coordinator "$COORDINATOR_ADDR" active 10 >/dev/null || die "epoch 2 did not become active"
status_json coordinator "$COORDINATOR_ADDR" >"$APPLY_DIR/status-epoch2-active.json"
assert_status_field "$APPLY_DIR/status-epoch2-active.json" current_shard_count 2
assert_status_field "$APPLY_DIR/status-epoch2-active.json" epoch_cycle_state active

run_client -coordinator "$COORDINATOR_ADDR" -x 7 -y 0 -payload epoch2-row0 -log "$EPOCH2_ROW0_LOG"
run_client -coordinator "$COORDINATOR_ADDR" -x 8 -y 256 -payload epoch2-row256 -log "$EPOCH2_ROW256_LOG"
wait_for_status_state coordinator "$COORDINATOR_ADDR" completed 20 >/dev/null || die "epoch 2 did not complete"
wait_for_status_state server "$SHARD0_LEADER_ADDR" completed 20 >/dev/null || die "shard 0 did not complete epoch 2"
wait_for_status_state server "$SHARD1_LEADER_ADDR" completed 20 >/dev/null || die "shard 1 did not complete epoch 2"
status_json coordinator "$COORDINATOR_ADDR" >"$APPLY_DIR/status-epoch2-completed.json"

result_s0="$(latest_result_file "$RESULTS_S0_DIR" "$epoch2" 0)"
result_s1="$(latest_result_file "$RESULTS_S1_DIR" "$epoch2" 1)"
wait_for_file "$result_s0" 10 || die "expected file $result_s0"
wait_for_file "$result_s1" 10 || die "expected file $result_s1"
assert_result_contains_slot "$result_s0" 0 0 256 0 7 "epoch2-row0"
assert_result_contains_slot "$result_s1" 1 256 512 256 8 "epoch2-row256"
echo "PASS: post-apply epoch routes row 0 to shard 0 and row 256 to shard 1"

cat <<EOF
Manual scaling apply validation complete.
  epoch before apply: $epoch1
  epoch after apply:  $epoch2
  apply output:       $APPLY_DIR/apply-output.log
  status artifacts:   $APPLY_DIR/status-*.json
EOF
