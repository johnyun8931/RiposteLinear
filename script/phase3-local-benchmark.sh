#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/phase3-local-common.sh"

THREAD_SWEEP="${RIPOSTE_BENCH_SERVER_THREADS:-1 2 4 8}"
BENCH_DURATION="${RIPOSTE_BENCH_DURATION}"
WARMUP_DURATION="${RIPOSTE_BENCH_WARMUP_DURATION}"
POST_RUN_WAIT="${RIPOSTE_BENCH_POST_RUN_WAIT}"
START_DELAY="${RIPOSTE_BENCH_START_DELAY}"
CLIENT_THREADS="${RIPOSTE_BENCH_CLIENT_THREADS}"
CLIENT_CONCURRENCY="${RIPOSTE_BENCH_CLIENT_CONCURRENCY}"
CLIENT_RETRY_OVERLOAD="${RIPOSTE_BENCH_CLIENT_RETRY_OVERLOAD}"
CLIENT_OVERLOAD_BACKOFF_INITIAL_MS="${RIPOSTE_BENCH_CLIENT_OVERLOAD_BACKOFF_INITIAL_MS}"
CLIENT_OVERLOAD_BACKOFF_MAX_MS="${RIPOSTE_BENCH_CLIENT_OVERLOAD_BACKOFF_MAX_MS}"
CLIENT_RETRY_ARGS=()
if [[ "$CLIENT_RETRY_OVERLOAD" == "1" || "$CLIENT_RETRY_OVERLOAD" == "true" ]]; then
	CLIENT_RETRY_ARGS=(-retry-overload -overload-backoff-initial-ms "$CLIENT_OVERLOAD_BACKOFF_INITIAL_MS" -overload-backoff-max-ms "$CLIENT_OVERLOAD_BACKOFF_MAX_MS")
fi

SUMMARY_TSV="$STATE_DIR/benchmark-summary.tsv"
SUMMARY_MD="$STATE_DIR/benchmark-summary.md"
HOST_INFO_FILE="$STATE_DIR/benchmark-host.txt"

baseline_run() {
	local threads="$1"
	local phase="$2"
	local client_log="$LOG_DIR/baseline-${phase}-t${threads}.log"
	local before after delta max_sent

	before="$(last_since_start "$(log_file baseline-leader)")"
	start_epoch server "$BASELINE_LEADER_ADDR" "$BENCH_DURATION" >/dev/null
	sleep "$START_DELAY"
	if [[ "${#CLIENT_RETRY_ARGS[@]}" -gt 0 ]]; then
		run_client -leader "$BASELINE_LEADER_ADDR" -hammer -threads "$CLIENT_THREADS" -concurrency "$CLIENT_CONCURRENCY" "${CLIENT_RETRY_ARGS[@]}" -log "$client_log"
	else
		run_client -leader "$BASELINE_LEADER_ADDR" -hammer -threads "$CLIENT_THREADS" -concurrency "$CLIENT_CONCURRENCY" -log "$client_log"
	fi
	wait_for_epoch_complete server "$BASELINE_LEADER_ADDR"
	sleep "$POST_RUN_WAIT"
	after="$(last_since_start "$(log_file baseline-leader)")"
	[[ "$after" -gt "$before" ]] || die "baseline leader stats did not advance for phase $phase threads=$threads"
	delta="$((after - before))"
	max_sent="$(max_sent_from_client_log "$client_log")"
	printf '%s\t%s\n' "$delta" "$max_sent"
}

sharded_run() {
	local threads="$1"
	local phase="$2"
	local client_log="$LOG_DIR/sharded-${phase}-t${threads}.log"
	local status_file="$TMP_DIR/status-sharded-${phase}-t${threads}.json"
	local shard0_before shard1_before shard0_after shard1_after shard0_delta shard1_delta total max_sent

	shard0_before="$(last_since_start "$(log_file shard0-leader)")"
	shard1_before="$(last_since_start "$(log_file shard1-leader)")"
	start_epoch coordinator "$COORDINATOR_ADDR" "$BENCH_DURATION" >/dev/null
	sleep "$START_DELAY"
	if [[ "${#CLIENT_RETRY_ARGS[@]}" -gt 0 ]]; then
		run_client -coordinator "$COORDINATOR_ADDR" -hammer -threads "$CLIENT_THREADS" -concurrency "$CLIENT_CONCURRENCY" "${CLIENT_RETRY_ARGS[@]}" -log "$client_log"
	else
		run_client -coordinator "$COORDINATOR_ADDR" -hammer -threads "$CLIENT_THREADS" -concurrency "$CLIENT_CONCURRENCY" -log "$client_log"
	fi
	wait_for_epoch_complete coordinator "$COORDINATOR_ADDR"
	status_json coordinator "$COORDINATOR_ADDR" >"$status_file"
	sleep "$POST_RUN_WAIT"
	shard0_after="$(last_since_start "$(log_file shard0-leader)")"
	shard1_after="$(last_since_start "$(log_file shard1-leader)")"
	shard0_delta="$((shard0_after - shard0_before))"
	shard1_delta="$((shard1_after - shard1_before))"
	total="$((shard0_delta + shard1_delta))"
	[[ "$total" -gt 0 ]] || die "sharded leaders did not advance for phase $phase threads=$threads"
	max_sent="$(max_sent_from_client_log "$client_log")"
	printf '%s\t%s\t%s\t%s\t%s\n' "$shard0_delta" "$shard1_delta" "$total" "$max_sent" "$(scaling_status_tsv "$status_file")"
}

run_baseline_pair_for_threads() {
	local threads="$1"
	stop_baseline_pair
	rm -rf "$RESULTS_SINGLE_DIR"
	mkdir -p "$RESULTS_SINGLE_DIR"
	start_baseline_pair "$threads"
	if [[ "$WARMUP_DURATION" -gt 0 ]]; then
		info "Baseline warm-up (threads=$threads, duration=${WARMUP_DURATION}s)"
		start_epoch server "$BASELINE_LEADER_ADDR" "$WARMUP_DURATION" >/dev/null
		wait_for_epoch_complete server "$BASELINE_LEADER_ADDR"
		sleep "$POST_RUN_WAIT"
	fi
	baseline_run "$threads" "measured"
	stop_baseline_pair
}

run_sharded_topology_for_threads() {
	local threads="$1"
	stop_sharded_topology
	rm -rf "$RESULTS_S0_DIR" "$RESULTS_S1_DIR"
	mkdir -p "$RESULTS_S0_DIR" "$RESULTS_S1_DIR"
	start_sharded_topology "$threads"
	if [[ "$WARMUP_DURATION" -gt 0 ]]; then
		info "Sharded warm-up (threads=$threads, duration=${WARMUP_DURATION}s)"
		start_epoch coordinator "$COORDINATOR_ADDR" "$WARMUP_DURATION" >/dev/null
		wait_for_epoch_complete coordinator "$COORDINATOR_ADDR"
		sleep "$POST_RUN_WAIT"
	fi
	sharded_run "$threads" "measured"
	stop_sharded_topology
}

winner_for_row() {
	local baseline_total="$1"
	local sharded_total="$2"
	if [[ "$sharded_total" -gt "$baseline_total" ]]; then
		echo "sharded"
	elif [[ "$sharded_total" -lt "$baseline_total" ]]; then
		echo "baseline"
	else
		echo "tie"
	fi
}

cleanup() {
	stop_baseline_pair
	stop_sharded_topology
}
trap cleanup EXIT

require_tools
reset_state_dirs
build_binaries

host_context_line | tee "$HOST_INFO_FILE"
{
	echo -e "server_threads\tclient_concurrency\tclient_retry_overload\tbaseline_total\tbaseline_req_per_sec\tbaseline_client_max_sent\tshard0_total\tshard1_total\tsharded_total\tsharded_req_per_sec\tsharded_client_max_sent\tscaling_epoch_id\tscaling_accepted_requests\tscaling_duration_secs\trequest_density\tscaling_action\tscaling_reason\tdelta\twinner"
} >"$SUMMARY_TSV"

for threads in $THREAD_SWEEP; do
	info "Benchmark sweep for server threads=$threads"
	baseline_output="$TMP_DIR/baseline-t${threads}.tsv"
	sharded_output="$TMP_DIR/sharded-t${threads}.tsv"
	run_baseline_pair_for_threads "$threads" >"$baseline_output"
	IFS=$'\t' read -r baseline_total baseline_client_max_sent <"$baseline_output"
	run_sharded_topology_for_threads "$threads" >"$sharded_output"
	IFS=$'\t' read -r shard0_total shard1_total sharded_total sharded_client_max_sent scaling_epoch_id scaling_accepted_requests scaling_duration_secs request_density scaling_action scaling_reason <"$sharded_output"

	baseline_req_per_sec="$(python3 - "$baseline_total" "$BENCH_DURATION" <<'PY'
import sys
total = int(sys.argv[1]); duration = int(sys.argv[2]); print(f"{total / duration:.2f}")
PY
)"
	sharded_req_per_sec="$(python3 - "$sharded_total" "$BENCH_DURATION" <<'PY'
import sys
total = int(sys.argv[1]); duration = int(sys.argv[2]); print(f"{total / duration:.2f}")
PY
)"
	delta="$((sharded_total - baseline_total))"
	winner="$(winner_for_row "$baseline_total" "$sharded_total")"

	printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
		"$threads" "$CLIENT_CONCURRENCY" "$CLIENT_RETRY_OVERLOAD" "$baseline_total" "$baseline_req_per_sec" "$baseline_client_max_sent" \
		"$shard0_total" "$shard1_total" "$sharded_total" "$sharded_req_per_sec" "$sharded_client_max_sent" \
		"$scaling_epoch_id" "$scaling_accepted_requests" "$scaling_duration_secs" "$request_density" "$scaling_action" "$scaling_reason" \
		"$delta" "$winner" >>"$SUMMARY_TSV"
done

python3 - "$HOST_INFO_FILE" "$SUMMARY_TSV" >"$SUMMARY_MD" <<'PY'
import csv
import sys

host_info_path = sys.argv[1]
summary_tsv_path = sys.argv[2]

with open(host_info_path) as fh:
    host_info = fh.read().strip()

rows = list(csv.DictReader(open(summary_tsv_path), delimiter="\t"))

print("# Phase 3 Local Throughput Sweep")
print()
print(f"- {host_info}")
print()
print("| server_threads | client_concurrency | client_retry_overload | baseline_total | baseline_req_per_sec | shard0_total | shard1_total | sharded_total | sharded_req_per_sec | scaling_accepted_requests | request_density | scaling_action | delta | winner |")
print("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
for row in rows:
    print(
        f"| {row['server_threads']} | {row['client_concurrency']} | {row['client_retry_overload']} | {row['baseline_total']} | {row['baseline_req_per_sec']} | "
        f"{row['shard0_total']} | {row['shard1_total']} | {row['sharded_total']} | "
        f"{row['sharded_req_per_sec']} | {row['scaling_accepted_requests']} | {row['request_density']} | {row['scaling_action']} | "
        f"{row['delta']} | {row['winner']} |"
    )
PY

cat <<EOF
Phase 3 local throughput sweep complete.

Host context:
  $(cat "$HOST_INFO_FILE")

Summary table:
$(cat "$SUMMARY_MD")

Artifacts:
  TSV: $SUMMARY_TSV
  MD:  $SUMMARY_MD
EOF
