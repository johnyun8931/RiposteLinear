#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"
load_state

require_cmd ssh

RUN_MINUTES="${1:-${RUN_MINUTES:-}}"
PHASE_NAME="${PHASE_NAME:-}"
SERVERS="$(server_list)"

usage() {
  cat <<EOF_USAGE
usage:
  $0 <minutes>       # run one hammer phase for <minutes>

environment overrides:
  RUN_MINUTES=<n>    # same as passing <minutes>
  PHASE_NAME=<name>  # name for the one-phase log directory; default: measured-<minutes>m
EOF_USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ -z "$RUN_MINUTES" ]]; then
  usage
  die "missing required minutes argument"
fi

if [[ -n "$RUN_MINUTES" && ! "$RUN_MINUTES" =~ ^[0-9]+$ ]]; then
  die "minutes must be a positive integer; got: $RUN_MINUTES"
fi

if [[ "$RUN_MINUTES" == "0" ]]; then
  die "minutes must be greater than zero"
fi

reset_all_processes() {
  for host in "$SERVER0_PUBLIC_IP" "$SERVER1_PUBLIC_IP" "$CLIENT0_PUBLIC_IP" "$CLIENT1_PUBLIC_IP"; do
    kill_remote_processes "$host"
  done
}

ensure_openssl() {
  local host="$1"
  remote_cmd "$host" "if ! command -v openssl >/dev/null 2>&1; then sudo apt-get update && sudo DEBIAN_FRONTEND=noninteractive apt-get install -y openssl; fi"
}

run_openssl_speed() {
  echo "running idle openssl AES speed checks"
  ensure_openssl "$SERVER0_PUBLIC_IP"
  ensure_openssl "$SERVER1_PUBLIC_IP"
  remote_cmd "$SERVER0_PUBLIC_IP" "mkdir -p /tmp/riposte-eval/crypto; openssl speed -evp aes-128-ctr > /tmp/riposte-eval/crypto/server-0-openssl-speed.txt 2>&1"
  remote_cmd "$SERVER1_PUBLIC_IP" "mkdir -p /tmp/riposte-eval/crypto; openssl speed -evp aes-128-ctr > /tmp/riposte-eval/crypto/server-1-openssl-speed.txt 2>&1"
}

start_servers_for_phase() {
  local phase="$1"
  local remote_dir="/tmp/riposte-eval/$phase"

  remote_cmd "$SERVER1_PUBLIC_IP" "mkdir -p '$remote_dir'; nohup ~/server -idx 1 -threads '$THREADS' -log '$remote_dir/server-1.log' -servers '$SERVERS' > '$remote_dir/server-1.nohup' 2>&1 &"
  sleep 2
  remote_cmd "$SERVER0_PUBLIC_IP" "mkdir -p '$remote_dir'; nohup ~/server -idx 0 -threads '$THREADS' -log '$remote_dir/server-0.log' -servers '$SERVERS' > '$remote_dir/server-0.nohup' 2>&1 &"
  sleep 5
}

start_clients_for_phase() {
  local phase="$1"
  local remote_dir="/tmp/riposte-eval/$phase"

  remote_cmd "$CLIENT0_PUBLIC_IP" "mkdir -p '$remote_dir'; nohup ~/client -threads '$THREADS' -hammer -log '$remote_dir/client-0.log' -leader '${SERVER0_PRIVATE_IP}:${SERVER0_PORT}' > '$remote_dir/client-0.nohup' 2>&1 &"
  remote_cmd "$CLIENT1_PUBLIC_IP" "mkdir -p '$remote_dir'; nohup ~/client -threads '$THREADS' -hammer -log '$remote_dir/client-1.log' -leader '${SERVER0_PRIVATE_IP}:${SERVER0_PORT}' > '$remote_dir/client-1.nohup' 2>&1 &"
}

run_phase() {
  local phase="$1"
  local duration="$2"

  echo "starting phase ${phase} for ${duration}s"
  reset_all_processes
  sleep 3
  start_servers_for_phase "$phase"
  start_clients_for_phase "$phase"
  sleep "$duration"
  echo "stopping phase ${phase}"
  reset_all_processes
  sleep 3
}

reset_all_processes
sleep 3
run_openssl_speed
phase="${PHASE_NAME:-measured-${RUN_MINUTES}m}"
run_phase "$phase" "$((RUN_MINUTES * 60))"

echo "benchmark phases complete"
