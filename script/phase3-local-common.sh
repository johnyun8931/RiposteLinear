#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_DIR="${RIPOSTE_PHASE3_STATE_DIR:-/tmp/riposte-phase3-local}"
BIN_DIR="$STATE_DIR/bin"
LOG_DIR="$STATE_DIR/logs"
PID_DIR="$STATE_DIR/pids"
TMP_DIR="$STATE_DIR/tmp"

RESULTS_SINGLE_DIR="$STATE_DIR/results-single"
RESULTS_S0_DIR="$STATE_DIR/results-s0"
RESULTS_S1_DIR="$STATE_DIR/results-s1"

# utils.ListenAndServe currently slices the last 5 chars of the address, so keep
# local verification ports at 4 digits until that helper is cleaned up.
COORDINATOR_ADDR="127.0.0.1:8630"
SHARD0_LEADER_ADDR="127.0.0.1:8610"
SHARD0_FOLLOWER_ADDR="127.0.0.1:8611"
SHARD1_LEADER_ADDR="127.0.0.1:8620"
SHARD1_FOLLOWER_ADDR="127.0.0.1:8621"
BASELINE_LEADER_ADDR="127.0.0.1:8640"
BASELINE_FOLLOWER_ADDR="127.0.0.1:8641"

SERVER_BIN="$BIN_DIR/riposte-server"
COORDINATOR_BIN="$BIN_DIR/riposte-coordinator"
CLIENT_BIN="$BIN_DIR/riposte-client"

export GOCACHE="${GOCACHE:-/tmp/riposte-go-cache}"
export RIPOSTE_BENCH_SERVER_THREADS_DEFAULT="${RIPOSTE_BENCH_SERVER_THREADS_DEFAULT:-4}"
export RIPOSTE_BENCH_CLIENT_THREADS="${RIPOSTE_BENCH_CLIENT_THREADS:-1}"
export RIPOSTE_BENCH_CLIENT_CONCURRENCY="${CLIENT_CONCURRENCY:-${RIPOSTE_BENCH_CLIENT_CONCURRENCY:-16}}"
export RIPOSTE_BENCH_CLIENT_RETRY_OVERLOAD="${CLIENT_RETRY_OVERLOAD:-${RIPOSTE_BENCH_CLIENT_RETRY_OVERLOAD:-0}}"
export RIPOSTE_BENCH_CLIENT_OVERLOAD_BACKOFF_INITIAL_MS="${CLIENT_OVERLOAD_BACKOFF_INITIAL_MS:-${RIPOSTE_BENCH_CLIENT_OVERLOAD_BACKOFF_INITIAL_MS:-10}}"
export RIPOSTE_BENCH_CLIENT_OVERLOAD_BACKOFF_MAX_MS="${CLIENT_OVERLOAD_BACKOFF_MAX_MS:-${RIPOSTE_BENCH_CLIENT_OVERLOAD_BACKOFF_MAX_MS:-250}}"
export RIPOSTE_BENCH_DURATION="${RIPOSTE_BENCH_DURATION:-8}"
export RIPOSTE_BENCH_WARMUP_DURATION="${RIPOSTE_BENCH_WARMUP_DURATION:-4}"
export RIPOSTE_BENCH_POST_RUN_WAIT="${RIPOSTE_BENCH_POST_RUN_WAIT:-12}"
export RIPOSTE_BENCH_START_DELAY="${RIPOSTE_BENCH_START_DELAY:-1}"
# Server-side startup readiness is now enforced; this delay is only a small
# convenience so the local scripts avoid immediate retry loops right after ports
# open.
export RIPOSTE_TOPOLOGY_SETTLE_DELAY="${RIPOSTE_TOPOLOGY_SETTLE_DELAY:-2}"

mkdir -p "$STATE_DIR" "$BIN_DIR" "$LOG_DIR" "$PID_DIR" "$TMP_DIR"

die() {
	echo "ERROR: $*" >&2
	exit 1
}

info() {
	echo "==> $*" >&2
}

require_tools() {
	command -v go >/dev/null 2>&1 || die "go is required"
	command -v python3 >/dev/null 2>&1 || die "python3 is required"
}

prepare_state_dirs() {
	mkdir -p "$BIN_DIR" "$LOG_DIR" "$PID_DIR" "$TMP_DIR" \
		"$RESULTS_SINGLE_DIR" "$RESULTS_S0_DIR" "$RESULTS_S1_DIR"
}

reset_state_dirs() {
	rm -rf "$LOG_DIR" "$PID_DIR" "$TMP_DIR" "$RESULTS_SINGLE_DIR" "$RESULTS_S0_DIR" "$RESULTS_S1_DIR"
	prepare_state_dirs
}

build_binaries() {
	require_tools
	prepare_state_dirs
	info "Building local binaries into $BIN_DIR"
	(
		cd "$ROOT_DIR"
		go build -o "$SERVER_BIN" ./server
		go build -o "$COORDINATOR_BIN" ./coordinator
		go build -o "$CLIENT_BIN" ./client
	)
}

server_threads_args() {
	local threads="$1"
	if [[ -n "$threads" && "$threads" -gt 0 ]]; then
		printf -- "-threads %s" "$threads"
	fi
}

pid_file() {
	echo "$PID_DIR/$1.pid"
}

log_file() {
	echo "$LOG_DIR/$1.log"
}

is_pid_running() {
	local pid="$1"
	kill -0 "$pid" 2>/dev/null
}

is_process_running() {
	local file
	file="$(pid_file "$1")"
	if [[ ! -f "$file" ]]; then
		return 1
	fi
	is_pid_running "$(cat "$file")"
}

start_process() {
	local name="$1"
	shift
	local pidf logf
	pidf="$(pid_file "$name")"
	logf="$(log_file "$name")"
	if is_process_running "$name"; then
		die "$name is already running (pid $(cat "$pidf"))"
	fi
	: >"$logf"
	nohup "$@" >>"$logf" 2>&1 </dev/null &
	echo $! >"$pidf"
}

stop_process() {
	local name="$1"
	local pidf
	pidf="$(pid_file "$name")"
	if [[ ! -f "$pidf" ]]; then
		return 0
	fi
	local pid
	pid="$(cat "$pidf")"
	if is_pid_running "$pid"; then
		kill "$pid" 2>/dev/null || true
		wait "$pid" 2>/dev/null || true
	fi
	rm -f "$pidf"
}

stop_all_phase3_processes() {
	stop_process "coordinator"
	stop_process "shard0-leader"
	stop_process "shard0-follower"
	stop_process "shard1-leader"
	stop_process "shard1-follower"
	stop_process "baseline-leader"
	stop_process "baseline-follower"
}

start_sharded_topology() {
	local threads="${1:-$RIPOSTE_BENCH_SERVER_THREADS_DEFAULT}"
	local thread_args
	thread_args="$(server_threads_args "$threads")"

	info "Starting shard 0 (server threads=$threads)"
	# shellcheck disable=SC2086
	start_process "shard0-follower" "$SERVER_BIN" -idx 1 -servers "$SHARD0_LEADER_ADDR,$SHARD0_FOLLOWER_ADDR" -shard-id 0 -results-dir "$RESULTS_S0_DIR" -log "$(log_file shard0-follower)" $thread_args
	# shellcheck disable=SC2086
	start_process "shard0-leader" "$SERVER_BIN" -idx 0 -servers "$SHARD0_LEADER_ADDR,$SHARD0_FOLLOWER_ADDR" -shard-id 0 -results-dir "$RESULTS_S0_DIR" -log "$(log_file shard0-leader)" $thread_args

	info "Starting shard 1 (server threads=$threads)"
	# shellcheck disable=SC2086
	start_process "shard1-follower" "$SERVER_BIN" -idx 1 -servers "$SHARD1_LEADER_ADDR,$SHARD1_FOLLOWER_ADDR" -shard-id 1 -results-dir "$RESULTS_S1_DIR" -log "$(log_file shard1-follower)" $thread_args
	# shellcheck disable=SC2086
	start_process "shard1-leader" "$SERVER_BIN" -idx 0 -servers "$SHARD1_LEADER_ADDR,$SHARD1_FOLLOWER_ADDR" -shard-id 1 -results-dir "$RESULTS_S1_DIR" -log "$(log_file shard1-leader)" $thread_args

	wait_for_port "$SHARD0_LEADER_ADDR" 50 || die "shard 0 leader did not start listening"
	wait_for_port "$SHARD1_LEADER_ADDR" 50 || die "shard 1 leader did not start listening"
	sleep "$RIPOSTE_TOPOLOGY_SETTLE_DELAY"

	info "Starting coordinator"
	start_process "coordinator" "$COORDINATOR_BIN" -listen "$COORDINATOR_ADDR" -log "$(log_file coordinator)" \
		-shard "0,0,128,$SHARD0_LEADER_ADDR,$SHARD0_FOLLOWER_ADDR" \
		-shard "1,128,256,$SHARD1_LEADER_ADDR,$SHARD1_FOLLOWER_ADDR"
	wait_for_port "$COORDINATOR_ADDR" 50 || die "coordinator did not start listening"
}

stop_sharded_topology() {
	stop_process "coordinator"
	stop_process "shard0-leader"
	stop_process "shard0-follower"
	stop_process "shard1-leader"
	stop_process "shard1-follower"
}

start_baseline_pair() {
	local threads="${1:-$RIPOSTE_BENCH_SERVER_THREADS_DEFAULT}"
	local thread_args
	thread_args="$(server_threads_args "$threads")"
	info "Starting single-shard baseline pair (server threads=$threads)"
	# shellcheck disable=SC2086
	start_process "baseline-follower" "$SERVER_BIN" -idx 1 -servers "$BASELINE_LEADER_ADDR,$BASELINE_FOLLOWER_ADDR" -shard-id 0 -results-dir "$RESULTS_SINGLE_DIR" -log "$(log_file baseline-follower)" $thread_args
	# shellcheck disable=SC2086
	start_process "baseline-leader" "$SERVER_BIN" -idx 0 -servers "$BASELINE_LEADER_ADDR,$BASELINE_FOLLOWER_ADDR" -shard-id 0 -results-dir "$RESULTS_SINGLE_DIR" -log "$(log_file baseline-leader)" $thread_args
	wait_for_port "$BASELINE_LEADER_ADDR" 50 || die "baseline leader did not start listening"
	sleep "$RIPOSTE_TOPOLOGY_SETTLE_DELAY"
}

stop_baseline_pair() {
	stop_process "baseline-leader"
	stop_process "baseline-follower"
}

wait_for_port() {
	local addr="$1"
	local host="${addr%:*}"
	local port="${addr##*:}"
	local attempts="${2:-50}"
	for _ in $(seq 1 "$attempts"); do
		if python3 - "$host" "$port" <<'PY'
import socket
import sys

host = sys.argv[1]
port = int(sys.argv[2])
sock = socket.socket()
sock.settimeout(0.2)
try:
    sock.connect((host, port))
except OSError:
    sys.exit(1)
finally:
    sock.close()
PY
		then
			return 0
		fi
		sleep 0.2
	done
	return 1
}

wait_for_status_state() {
	local kind="$1"
	local addr="$2"
	local want_state="$3"
	local timeout="${4:-20}"
	local line state
	for _ in $(seq 1 "$timeout"); do
		line="$(epoch_status "$kind" "$addr")"
		state="$(extract_field "$line" "state")"
		if [[ "$state" == "$want_state" ]]; then
			echo "$line"
			return 0
		fi
		sleep 1
	done
	return 1
}

extract_field() {
	local line="$1"
	local key="$2"
	printf '%s\n' "$line" | sed -n "s/.*${key}=\([^ ]*\).*/\1/p"
}

epoch_status() {
	local kind="$1"
	local addr="$2"
	local out
	if [[ "$kind" == "coordinator" ]]; then
		out="$("$COORDINATOR_BIN" -admin-target "$addr" -epoch-status 2>&1)"
	else
		out="$("$SERVER_BIN" -admin-target "$addr" -epoch-status 2>&1)"
	fi
	printf '%s\n' "$out" | tail -n1
}

status_json() {
	local kind="$1"
	local addr="$2"
	if [[ "$kind" == "coordinator" ]]; then
		"$COORDINATOR_BIN" -admin-target "$addr" -status
	else
		"$SERVER_BIN" -admin-target "$addr" -status
	fi
}

start_epoch() {
	local kind="$1"
	local addr="$2"
	local duration="$3"
	local out
	if [[ "$kind" == "coordinator" ]]; then
		out="$("$COORDINATOR_BIN" -admin-target "$addr" -start-epoch-seconds "$duration" 2>&1)"
	else
		out="$("$SERVER_BIN" -admin-target "$addr" -start-epoch-seconds "$duration" 2>&1)"
	fi
	printf '%s\n' "$out" | tail -n1
}

run_client() {
	"$CLIENT_BIN" "$@"
}

latest_result_file() {
	local dir="$1"
	local epoch_id="$2"
	local shard_id="$3"
	printf '%s/epoch-%06d-shard-%d-server-0.json\n' "$dir" "$epoch_id" "$shard_id"
}

assert_equals() {
	local got="$1"
	local want="$2"
	local label="$3"
	if [[ "$got" != "$want" ]]; then
		die "$label mismatch: got '$got', want '$want'"
	fi
}

assert_file_exists() {
	local file="$1"
	[[ -f "$file" ]] || die "expected file $file"
}

wait_for_file() {
	local file="$1"
	local timeout="${2:-10}"
	for _ in $(seq 1 "$timeout"); do
		if [[ -f "$file" ]]; then
			return 0
		fi
		sleep 1
	done
	return 1
}

assert_result_contains_slot() {
	local file="$1"
	local shard_id="$2"
	local row_min="$3"
	local row_max="$4"
	local want_row="$5"
	local want_col="$6"
	local want_payload="$7"

	python3 - "$file" "$shard_id" "$row_min" "$row_max" "$want_row" "$want_col" "$want_payload" <<'PY'
import json
import sys

path = sys.argv[1]
shard_id = int(sys.argv[2])
row_min = int(sys.argv[3])
row_max = int(sys.argv[4])
want_row = int(sys.argv[5])
want_col = int(sys.argv[6])
want_payload = sys.argv[7].encode()

with open(path) as fh:
    data = json.load(fh)

if data["shard_id"] != shard_id:
    raise SystemExit(f"shard_id mismatch in {path}: got {data['shard_id']} want {shard_id}")

for slot in data["slots"]:
    row = slot["row"]
    if row < row_min or row >= row_max:
        raise SystemExit(f"slot row {row} outside [{row_min},{row_max}) in {path}")

want_hex = want_payload.hex() + ("00" * (160 - len(want_payload)))
for slot in data["slots"]:
    if slot["row"] == want_row and slot["column"] == want_col:
        if slot["message_hex"] != want_hex:
            raise SystemExit(f"payload mismatch for ({want_row},{want_col}) in {path}")
        print("ok")
        break
else:
    raise SystemExit(f"missing slot ({want_row},{want_col}) in {path}")
PY
}

last_since_start() {
	local file="$1"
	if [[ ! -f "$file" ]]; then
		echo 0
		return
	fi
	local value
	value="$(sed -n 's/.*\[since start: \([0-9][0-9]*\)\].*/\1/p' "$file" | tail -n1)"
	if [[ -z "$value" ]]; then
		echo 0
	else
		echo "$value"
	fi
}

wait_for_count_advance() {
	local file="$1"
	local before="$2"
	local timeout="${3:-15}"
	for _ in $(seq 1 "$timeout"); do
		local now
		now="$(last_since_start "$file")"
		if [[ "$now" -gt "$before" ]]; then
			echo "$now"
			return 0
		fi
		sleep 1
	done
	return 1
}

max_sent_from_client_log() {
	local file="$1"
	if [[ ! -f "$file" ]]; then
		echo 0
		return
	fi
	local value
	value="$(sed -n 's/.*Sent \([0-9][0-9]*\) requests.*/\1/p' "$file" | tail -n1)"
	if [[ -z "$value" ]]; then
		echo 0
	else
		echo "$value"
	fi
}

host_context_line() {
	local model="unknown"
	local physical="unknown"
	local logical="unknown"
	if command -v sysctl >/dev/null 2>&1; then
		model="$(sysctl -n hw.model 2>/dev/null || echo unknown)"
		physical="$(sysctl -n hw.physicalcpu 2>/dev/null || echo unknown)"
		logical="$(sysctl -n hw.logicalcpu 2>/dev/null || echo unknown)"
	elif command -v nproc >/dev/null 2>&1; then
		logical="$(nproc)"
		physical="$logical"
	fi
	printf 'host_model=%s physical_cpu=%s logical_cpu=%s\n' "$model" "$physical" "$logical"
}

wait_for_epoch_complete() {
	local kind="$1"
	local addr="$2"
	wait_for_status_state "$kind" "$addr" completed 30 >/dev/null || die "$kind at $addr did not complete epoch"
}
