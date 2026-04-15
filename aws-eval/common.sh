#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

AWS_REGION="${AWS_REGION:-us-east-1}"
PROJECT_TAG="${PROJECT_TAG:-riposte-eval}"
INSTANCE_TYPE="${INSTANCE_TYPE:-c5n.4xlarge}"
VPC_ID="${VPC_ID:-vpc-0eec0c122a1df2a67}"
SUBNET_ID="${SUBNET_ID:-subnet-0d6798aac2c2708ec}"
AMI_SSM_PARAM="${AMI_SSM_PARAM:-/aws/service/canonical/ubuntu/server/22.04/stable/current/amd64/hvm/ebs-gp2/ami-id}"

SERVER0_PORT="${SERVER0_PORT:-9090}"
SERVER1_PORT="${SERVER1_PORT:-9091}"
THREADS="${THREADS:-16}"
SSH_USER="${SSH_USER:-ubuntu}"

STATE_DIR="$SCRIPT_DIR/.state"
STATE_FILE="$STATE_DIR/env.sh"
KEY_DIR="$SCRIPT_DIR/keys"
BIN_DIR="$SCRIPT_DIR/bin"
RESULTS_DIR="$SCRIPT_DIR/results"

die() {
  echo "error: $*" >&2
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

aws_base() {
  if [[ -n "${AWS_PROFILE:-}" ]]; then
    aws --profile "$AWS_PROFILE" "$@"
  else
    aws "$@"
  fi
}

aws_region() {
  aws_base --region "$AWS_REGION" "$@"
}

load_state() {
  [[ -f "$STATE_FILE" ]] || die "state file not found: $STATE_FILE. Run 01-launch.sh first."
  # shellcheck disable=SC1090
  source "$STATE_FILE"
}

quote() {
  printf "%q" "$1"
}

ssh_opts() {
  echo -i "$KEY_FILE" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10
}

remote_cmd() {
  local host="$1"
  local cmd="$2"
  # shellcheck disable=SC2046
  ssh $(ssh_opts) "${SSH_USER}@${host}" "$cmd"
}

copy_to_remote() {
  local src="$1"
  local host="$2"
  local dst="$3"
  # shellcheck disable=SC2046
  scp $(ssh_opts) "$src" "${SSH_USER}@${host}:${dst}"
}

copy_from_remote() {
  local host="$1"
  local src="$2"
  local dst="$3"
  # shellcheck disable=SC2046
  scp -r $(ssh_opts) "${SSH_USER}@${host}:${src}" "$dst"
}

wait_for_ssh() {
  local host="$1"
  local label="$2"
  local tries="${3:-60}"

  echo "waiting for SSH on ${label} (${host})"
  for _ in $(seq 1 "$tries"); do
    if remote_cmd "$host" "true" >/dev/null 2>&1; then
      echo "SSH ready on ${label}"
      return 0
    fi
    sleep 5
  done

  die "SSH did not become ready on ${label} (${host})"
}

server_list() {
  echo "${SERVER0_PRIVATE_IP}:${SERVER0_PORT},${SERVER1_PRIVATE_IP}:${SERVER1_PORT}"
}

kill_remote_processes() {
  local host="$1"
  remote_cmd "$host" "pkill -TERM -x server >/dev/null 2>&1 || true; pkill -TERM -x client >/dev/null 2>&1 || true; sleep 2; pkill -KILL -x server >/dev/null 2>&1 || true; pkill -KILL -x client >/dev/null 2>&1 || true"
}
