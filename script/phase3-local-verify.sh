#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/phase3-local-common.sh"

for name in coordinator shard0-leader shard0-follower shard1-leader shard1-follower; do
	is_process_running "$name" || die "$name is not running; start the topology with script/phase3-local-up.sh"
done

PRESTART_LOG="$TMP_DIR/prestart-client.log"
VERIFY0_LOG="$TMP_DIR/verify-shard0-client.log"
VERIFY1_LOG="$TMP_DIR/verify-shard1-client.log"
SECOND_EPOCH_DURATION=4
VERIFY_EPOCH_DURATION=8

info "Checking that uploads are rejected before epoch start"
if run_client -coordinator "$COORDINATOR_ADDR" -x 1 -y 0 -payload prestart -log "$PRESTART_LOG"; then
	die "pre-start upload unexpectedly succeeded"
fi
grep -q "No active epoch" "$PRESTART_LOG" || die "pre-start client log did not report No active epoch"
echo "PASS: writes are rejected before epoch start"

info "Starting coordinated verification epoch"
start_line="$(start_epoch coordinator "$COORDINATOR_ADDR" "$VERIFY_EPOCH_DURATION")"
epoch_id="$(extract_field "$start_line" "epoch")"
[[ -n "$epoch_id" ]] || die "could not parse epoch id from: $start_line"

coord_status="$(wait_for_status_state coordinator "$COORDINATOR_ADDR" active 10)" || die "coordinator did not become active"
shard0_status="$(wait_for_status_state server "$SHARD0_LEADER_ADDR" active 10)" || die "shard 0 leader did not become active"
shard1_status="$(wait_for_status_state server "$SHARD1_LEADER_ADDR" active 10)" || die "shard 1 leader did not become active"

for field in epoch state accepting start end duration; do
	assert_equals "$(extract_field "$coord_status" "$field")" "$(extract_field "$shard0_status" "$field")" "coordinator vs shard0 $field"
	assert_equals "$(extract_field "$coord_status" "$field")" "$(extract_field "$shard1_status" "$field")" "coordinator vs shard1 $field"
done
echo "PASS: coordinator and shard leaders report matching active epoch metadata"

info "Sending deterministic boundary writes"
run_client -coordinator "$COORDINATOR_ADDR" -x 1 -y 0 -payload shard0-boundary -log "$VERIFY0_LOG"
run_client -coordinator "$COORDINATOR_ADDR" -x 2 -y 128 -payload shard1-boundary -log "$VERIFY1_LOG"
echo "PASS: deterministic writes to rows 0 and 128 succeeded"

coord_complete="$(wait_for_status_state coordinator "$COORDINATOR_ADDR" completed 20)" || die "coordinator did not reach completed state"
assert_equals "$(extract_field "$coord_complete" "epoch")" "$epoch_id" "completed epoch id"
echo "PASS: verification epoch completed"

result_s0="$(latest_result_file "$RESULTS_S0_DIR" "$epoch_id" 0)"
result_s1="$(latest_result_file "$RESULTS_S1_DIR" "$epoch_id" 1)"
assert_file_exists "$result_s0"
assert_file_exists "$result_s1"
assert_result_contains_slot "$result_s0" 0 0 128 0 1 "shard0-boundary"
assert_result_contains_slot "$result_s1" 1 128 256 128 2 "shard1-boundary"
echo "PASS: shard-local result files are present, unambiguous, and preserve deterministic payload bytes"

info "Starting a second coordinated epoch"
second_start="$(start_epoch coordinator "$COORDINATOR_ADDR" "$SECOND_EPOCH_DURATION")"
second_epoch_id="$(extract_field "$second_start" "epoch")"
[[ -n "$second_epoch_id" ]] || die "could not parse second epoch id"
if [[ "$second_epoch_id" -le "$epoch_id" ]]; then
	die "second epoch id $second_epoch_id did not advance beyond first epoch $epoch_id"
fi
wait_for_status_state coordinator "$COORDINATOR_ADDR" active 10 >/dev/null || die "second epoch did not become active"
wait_for_status_state coordinator "$COORDINATOR_ADDR" completed 20 >/dev/null || die "second epoch did not complete"
echo "PASS: a second coordinated epoch starts cleanly after the first completes"

cat <<EOF
Verification complete.
  verified epoch: $epoch_id
  shard 0 result: $result_s0
  shard 1 result: $result_s1
EOF
