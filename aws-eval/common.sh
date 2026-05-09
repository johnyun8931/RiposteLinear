#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

AWS_REGION="${AWS_REGION:-us-east-1}"
PROJECT_TAG="${PROJECT_TAG:-riposte-aws-eval}"
COORDINATOR_INSTANCE_TYPE="${COORDINATOR_INSTANCE_TYPE:-c7i.large}"
SERVER_INSTANCE_TYPE="${SERVER_INSTANCE_TYPE:-c7i.large}"
CLIENT_INSTANCE_TYPE="${CLIENT_INSTANCE_TYPE:-c7i.xlarge}"
AMI_SSM_PARAM="${AMI_SSM_PARAM:-/aws/service/canonical/ubuntu/server/22.04/stable/current/amd64/hvm/ebs-gp2/ami-id}"
SSH_USER="${SSH_USER:-ubuntu}"

SERVER_THREADS="${SERVER_THREADS:-2}"
CLIENT_THREADS="${CLIENT_THREADS:-1}"
CLIENT_CONCURRENCY="${CLIENT_CONCURRENCY:-16}"
CLIENT_RETRY_OVERLOAD="${CLIENT_RETRY_OVERLOAD:-0}"
CLIENT_OVERLOAD_BACKOFF_INITIAL_MS="${CLIENT_OVERLOAD_BACKOFF_INITIAL_MS:-10}"
CLIENT_OVERLOAD_BACKOFF_MAX_MS="${CLIENT_OVERLOAD_BACKOFF_MAX_MS:-250}"
WARMUP_EPOCH_SECONDS="${WARMUP_EPOCH_SECONDS:-60}"
MEASURED_EPOCH_SECONDS="${MEASURED_EPOCH_SECONDS:-600}"
START_EPOCH_RETRY_TIMEOUT="${START_EPOCH_RETRY_TIMEOUT:-90}"
START_EPOCH_RETRY_INTERVAL="${START_EPOCH_RETRY_INTERVAL:-2}"
POST_EPOCH_FLUSH_SECONDS="${POST_EPOCH_FLUSH_SECONDS:-12}"
CLIENT_EXIT_GRACE_SECONDS="${CLIENT_EXIT_GRACE_SECONDS:-30}"
CONTROL_STORE_BACKEND="${CONTROL_STORE_BACKEND:-memory}"
DYNAMODB_CONTROL_TABLE="${DYNAMODB_CONTROL_TABLE:-${PROJECT_TAG}-control}"
DYNAMODB_CONTROL_REGION="${DYNAMODB_CONTROL_REGION:-}"
SESSION_STORE_BACKEND="${SESSION_STORE_BACKEND:-memory}"
DYNAMODB_SESSION_TABLE="${DYNAMODB_SESSION_TABLE:-}"
DYNAMODB_SESSION_REGION="${DYNAMODB_SESSION_REGION:-}"
COORDINATOR_IAM_POLICY_NAME="${COORDINATOR_IAM_POLICY_NAME:-RiposteDynamoDBControlStore}"
COORDINATOR_LEASE_TTL_SECONDS="${COORDINATOR_LEASE_TTL_SECONDS:-30}"
COORDINATOR_LEASE_RENEW_SECONDS="${COORDINATOR_LEASE_RENEW_SECONDS:-10}"
COORDINATOR_INITIAL_ACTIVE_SHARDS="${COORDINATOR_INITIAL_ACTIVE_SHARDS:-0}"
COORDINATOR_EXTRA_ARGS="${COORDINATOR_EXTRA_ARGS:-}"
PUBLIC_ENTRY_BACKEND="${PUBLIC_ENTRY_BACKEND:-none}"
PUBLIC_ENTRY_MULTI_COORDINATOR="${PUBLIC_ENTRY_MULTI_COORDINATOR:-0}"
INGESTION_QUEUE_BACKEND="${INGESTION_QUEUE_BACKEND:-memory}"
HOT_STANDBY_INGESTION="${HOT_STANDBY_INGESTION:-0}"
INGESTION_S3_BUCKET="${INGESTION_S3_BUCKET:-}"
INGESTION_RECEIVE_BATCH_SIZE="${INGESTION_RECEIVE_BATCH_SIZE:-1}"
INGESTION_SQS_WAIT_SECONDS="${INGESTION_SQS_WAIT_SECONDS:-10}"
INGESTION_SQS_VISIBILITY_TIMEOUT_SECONDS="${INGESTION_SQS_VISIBILITY_TIMEOUT_SECONDS:-300}"
INGESTION_WORKER_ERROR_BACKOFF_MS="${INGESTION_WORKER_ERROR_BACKOFF_MS:-250}"
COMPLETED_UPLOAD_LEDGER_BACKEND="${COMPLETED_UPLOAD_LEDGER_BACKEND:-memory}"
COMPLETED_UPLOAD_LEDGER_TABLE="${COMPLETED_UPLOAD_LEDGER_TABLE:-$DYNAMODB_CONTROL_TABLE}"
COMPLETED_UPLOAD_PROCESSING_TTL_SECONDS="${COMPLETED_UPLOAD_PROCESSING_TTL_SECONDS:-900}"
SERVER_INGESTION_IAM_POLICY_NAME="${SERVER_INGESTION_IAM_POLICY_NAME:-RiposteCompletedUploadIngestion}"
READ_SERVER_INSTANCE_TYPE="${READ_SERVER_INSTANCE_TYPE:-c7i.large}"
READ_SERVER_PORT="${READ_SERVER_PORT:-8080}"
READ_ALB_PORT="${READ_ALB_PORT:-80}"
READ_SERVER_DESIRED_CAPACITY="${READ_SERVER_DESIRED_CAPACITY:-2}"
READ_SERVER_MIN_SIZE="${READ_SERVER_MIN_SIZE:-1}"
READ_SERVER_MAX_SIZE="${READ_SERVER_MAX_SIZE:-4}"
RESULT_TABLE_S3_BUCKET="${RESULT_TABLE_S3_BUCKET:-}"
RESULT_TABLE_S3_PREFIX="${RESULT_TABLE_S3_PREFIX:-}"
READ_SERVER_IAM_ROLE_NAME="${READ_SERVER_IAM_ROLE_NAME:-}"
READ_SERVER_IAM_INSTANCE_PROFILE_NAME="${READ_SERVER_IAM_INSTANCE_PROFILE_NAME:-}"
READ_SERVER_IAM_POLICY_NAME="${READ_SERVER_IAM_POLICY_NAME:-RiposteReadServerS3}"

COORDINATOR_PORT="${COORDINATOR_PORT:-8630}"
COORDINATOR_STANDBY_PORT="${COORDINATOR_STANDBY_PORT:-8631}"
SHARD0_LEADER_PORT="${SHARD0_LEADER_PORT:-8610}"
SHARD0_FOLLOWER_PORT="${SHARD0_FOLLOWER_PORT:-8611}"
SHARD1_LEADER_PORT="${SHARD1_LEADER_PORT:-8620}"
SHARD1_FOLLOWER_PORT="${SHARD1_FOLLOWER_PORT:-8621}"
SHARD0_STANDBY_LEADER_PORT="${SHARD0_STANDBY_LEADER_PORT:-8640}"
SHARD0_STANDBY_FOLLOWER_PORT="${SHARD0_STANDBY_FOLLOWER_PORT:-8641}"
SHARD1_STANDBY_LEADER_PORT="${SHARD1_STANDBY_LEADER_PORT:-8650}"
SHARD1_STANDBY_FOLLOWER_PORT="${SHARD1_STANDBY_FOLLOWER_PORT:-8651}"

REMOTE_ROOT="${REMOTE_ROOT:-/tmp/riposte-eval}"
REMOTE_BIN_DIR="$REMOTE_ROOT/bin"
REMOTE_PHASES_DIR="$REMOTE_ROOT/phases"
REMOTE_SMOKE_DIR="$REMOTE_ROOT/smoke"
ROWS_PER_SHARD=256

STATE_DIR="$SCRIPT_DIR/.state"
STATE_FILE="$STATE_DIR/env.sh"
BENCHMARK_STATE_FILE="$STATE_DIR/benchmark-env.sh"
KEY_DIR="${KEY_DIR:-$HOME/.riposte-aws-eval-keys}"
BIN_DIR="$SCRIPT_DIR/bin"
RESULTS_DIR="$SCRIPT_DIR/results"

die() {
  echo "error: $*" >&2
  exit 1
}

info() {
  echo "==> $*" >&2
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

aws_base() {
  if [[ -n "${AWS_PROFILE:-}" ]]; then
    aws --no-cli-pager --profile "$AWS_PROFILE" "$@"
  else
    aws --no-cli-pager "$@"
  fi
}

aws_region() {
  aws_base --region "$AWS_REGION" "$@"
}

aws_control_region() {
  aws_base --region "$(dynamodb_control_region)" "$@"
}

quote() {
  printf "%q" "$1"
}

load_state() {
  [[ -f "$STATE_FILE" ]] || die "state file not found: $STATE_FILE. Run 01-launch.sh first."
  # shellcheck disable=SC1090
  source "$STATE_FILE"
  local name
  while IFS= read -r name; do
    [[ -n "${name:-}" ]] || continue
    [[ -v "$name" ]] || continue
    printf -v "$name" '%s' "${!name//$'\r'/}"
  done < <(sed -n 's/^\([A-Za-z_][A-Za-z0-9_]*\)=.*/\1/p' "$STATE_FILE")
  if [[ -n "${RUN_ID:-}" ]]; then
    local wsl_key="$HOME/.riposte-aws-eval-keys/riposte-aws-eval-$RUN_ID.pem"
    if [[ -f "$wsl_key" ]]; then
      KEY_FILE="$wsl_key"
    fi
  fi
}

ssh_opts() {
  echo -i "$KEY_FILE" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10
}

remote_cmd() {
  local host="$1"
  local cmd="$2"
  local attempt status
  for attempt in $(seq 1 "${SSH_COMMAND_ATTEMPTS:-12}"); do
    set +e
    # shellcheck disable=SC2046
    ssh $(ssh_opts) "${SSH_USER}@${host}" "$cmd"
    status=$?
    set -e
    if [[ "$status" -ne 255 ]]; then
      return "$status"
    fi
    sleep "$((attempt < 5 ? attempt : 5))"
  done
  return "$status"
}

copy_to_remote() {
  local src="$1"
  local host="$2"
  local dst="$3"
  local attempt status
  for attempt in $(seq 1 "${SSH_COMMAND_ATTEMPTS:-12}"); do
    set +e
    # shellcheck disable=SC2046
    scp $(ssh_opts) "$src" "${SSH_USER}@${host}:${dst}"
    status=$?
    set -e
    if [[ "$status" -ne 255 ]]; then
      return "$status"
    fi
    sleep "$((attempt < 5 ? attempt : 5))"
  done
  return "$status"
}

copy_from_remote() {
  local host="$1"
  local src="$2"
  local dst="$3"
  local attempt status
  for attempt in $(seq 1 "${SSH_COMMAND_ATTEMPTS:-12}"); do
    set +e
    # shellcheck disable=SC2046
    scp -r $(ssh_opts) "${SSH_USER}@${host}:${src}" "$dst"
    status=$?
    set -e
    if [[ "$status" -ne 255 ]]; then
      return "$status"
    fi
    sleep "$((attempt < 5 ? attempt : 5))"
  done
  return "$status"
}

wait_for_ssh() {
  local host="$1"
  local label="$2"
  local tries="${3:-60}"

  info "waiting for SSH on ${label} (${host})"
  for _ in $(seq 1 "$tries"); do
    if remote_cmd "$host" "true" >/dev/null 2>&1; then
      info "SSH ready on ${label}"
      return 0
    fi
    sleep 5
  done

  die "SSH did not become ready on ${label} (${host})"
}

contains_line() {
  local needle="$1"
  shift
  local item
  for item in "$@"; do
    if [[ "$item" == "$needle" ]]; then
      return 0
    fi
  done
  return 1
}

list_instance_offering_azs() {
  local instance_type="$1"
  aws_region ec2 describe-instance-type-offerings \
    --location-type availability-zone \
    --filters Name=instance-type,Values="$instance_type" \
    --query 'InstanceTypeOfferings[].Location' \
    --output text | tr '\t' '\n' | sed '/^$/d'
}

resolve_network_selection() {
  local requested_subnet="${SUBNET_ID:-}"
  local requested_vpc="${VPC_ID:-}"
  local requested_az="${AVAILABILITY_ZONE:-}"
  local vpc_id subnet_id subnet_az subnet_public

  if [[ -n "$requested_subnet" ]]; then
    vpc_id="$(aws_region ec2 describe-subnets \
      --subnet-ids "$requested_subnet" \
      --query 'Subnets[0].VpcId' \
      --output text)"
    subnet_az="$(aws_region ec2 describe-subnets \
      --subnet-ids "$requested_subnet" \
      --query 'Subnets[0].AvailabilityZone' \
      --output text)"
    subnet_public="$(aws_region ec2 describe-subnets \
      --subnet-ids "$requested_subnet" \
      --query 'Subnets[0].MapPublicIpOnLaunch' \
      --output text)"
    [[ "$vpc_id" != "None" ]] || die "subnet not found: $requested_subnet"
    [[ "$subnet_public" == "True" ]] || die "subnet $requested_subnet does not auto-assign public IPs"
    if [[ -n "$requested_vpc" && "$requested_vpc" != "$vpc_id" ]]; then
      die "SUBNET_ID $requested_subnet belongs to VPC $vpc_id, not requested VPC_ID $requested_vpc"
    fi
    if [[ -n "$requested_az" && "$requested_az" != "$subnet_az" ]]; then
      die "SUBNET_ID $requested_subnet is in AZ $subnet_az, not requested AVAILABILITY_ZONE $requested_az"
    fi
    printf '%s\t%s\t%s\n' "$vpc_id" "$requested_subnet" "$subnet_az"
    return 0
  fi

  if [[ -z "$requested_vpc" ]]; then
    requested_vpc="$(aws_region ec2 describe-vpcs \
      --filters Name=is-default,Values=true \
      --query 'Vpcs[0].VpcId' \
      --output text)"
  fi
  [[ -n "$requested_vpc" && "$requested_vpc" != "None" ]] || die "could not determine a VPC; set VPC_ID or SUBNET_ID"

  candidate_subnets=()
  while IFS=$'\t' read -r candidate_id candidate_az public_flag; do
    [[ -n "${candidate_id:-}" ]] || continue
    candidate_subnets+=("${candidate_id},${candidate_az},${public_flag}")
  done < <(aws_region ec2 describe-subnets \
    --filters \
      Name=vpc-id,Values="$requested_vpc" \
      Name=default-for-az,Values=true \
      Name=state,Values=available \
    --query 'Subnets[].[SubnetId,AvailabilityZone,MapPublicIpOnLaunch]' \
    --output text)
  [[ "${#candidate_subnets[@]}" -gt 0 ]] || die "no default subnets found in VPC $requested_vpc"

  server_azs=()
  while IFS= read -r az; do
    [[ -n "$az" ]] && server_azs+=("$az")
  done < <(list_instance_offering_azs "$SERVER_INSTANCE_TYPE")
  client_azs=()
  while IFS= read -r az; do
    [[ -n "$az" ]] && client_azs+=("$az")
  done < <(list_instance_offering_azs "$CLIENT_INSTANCE_TYPE")
  coordinator_azs=()
  while IFS= read -r az; do
    [[ -n "$az" ]] && coordinator_azs+=("$az")
  done < <(list_instance_offering_azs "$COORDINATOR_INSTANCE_TYPE")

  local row candidate_id candidate_az public_flag
  for row in "${candidate_subnets[@]}"; do
    IFS=, read -r candidate_id candidate_az public_flag <<<"$row"
    [[ "$public_flag" == "True" ]] || continue
    if [[ -n "$requested_az" && "$requested_az" != "$candidate_az" ]]; then
      continue
    fi
    if ! contains_line "$candidate_az" "${server_azs[@]}"; then
      continue
    fi
    if ! contains_line "$candidate_az" "${client_azs[@]}"; then
      continue
    fi
    if ! contains_line "$candidate_az" "${coordinator_azs[@]}"; then
      continue
    fi
    printf '%s\t%s\t%s\n' "$requested_vpc" "$candidate_id" "$candidate_az"
    return 0
  done

  die "could not find a default subnet with public IP mapping in a single AZ that offers $COORDINATOR_INSTANCE_TYPE, $SERVER_INSTANCE_TYPE, and $CLIENT_INSTANCE_TYPE"
}

resolve_read_alb_subnet_ids() {
  local vpc_id="$1"
  local primary_subnet_id="$2"
  local primary_az="$3"
  local requested="${READ_ALB_SUBNET_IDS:-}"
  local subnets=()
  local seen_azs=()
  local row candidate_id candidate_az public_flag

  if [[ -n "$requested" ]]; then
    printf '%s' "$requested"
    return 0
  fi

  subnets+=("$primary_subnet_id")
  seen_azs+=("$primary_az")
  while IFS=$'\t' read -r candidate_id candidate_az public_flag; do
    [[ -n "${candidate_id:-}" ]] || continue
    [[ "$candidate_id" != "$primary_subnet_id" ]] || continue
    [[ "$public_flag" == "True" ]] || continue
    if contains_line "$candidate_az" "${seen_azs[@]}"; then
      continue
    fi
    subnets+=("$candidate_id")
    seen_azs+=("$candidate_az")
    if [[ "${#subnets[@]}" -ge 2 ]]; then
      break
    fi
  done < <(aws_region ec2 describe-subnets \
    --filters \
      Name=vpc-id,Values="$vpc_id" \
      Name=default-for-az,Values=true \
      Name=state,Values=available \
    --query 'Subnets[].[SubnetId,AvailabilityZone,MapPublicIpOnLaunch]' \
    --output text)

  if [[ "${#subnets[@]}" -lt 2 ]]; then
    printf '%s' "$primary_subnet_id"
    return 0
  fi
  local joined
  joined="$(IFS=,; printf '%s' "${subnets[*]}")"
  printf '%s' "$joined"
}

server_pair_csv() {
  local leader_ip="$1"
  local leader_port="$2"
  local follower_ip="$3"
  local follower_port="$4"
  printf '%s:%s,%s:%s' "$leader_ip" "$leader_port" "$follower_ip" "$follower_port"
}

coordinator_addr() {
  printf '%s:%s' "$COORDINATOR_PRIVATE_IP" "$COORDINATOR_PORT"
}

coordinator_standby_addr() {
  printf '%s:%s' "$COORDINATOR_PRIVATE_IP" "$COORDINATOR_STANDBY_PORT"
}

public_entry_enabled() {
  [[ "$PUBLIC_ENTRY_BACKEND" == "nlb" ]]
}

public_entry_multi_coordinator_enabled() {
  public_entry_enabled && [[ "$PUBLIC_ENTRY_MULTI_COORDINATOR" == "1" || "$PUBLIC_ENTRY_MULTI_COORDINATOR" == "true" ]]
}

public_coordinator_addr() {
  if public_entry_enabled; then
    [[ -n "${NLB_DNS_NAME:-}" ]] || die "PUBLIC_ENTRY_BACKEND=nlb but NLB_DNS_NAME is missing from state"
    printf '%s:%s' "$NLB_DNS_NAME" "$COORDINATOR_PORT"
  else
    coordinator_addr
  fi
}

coordinator_holder_id() {
  printf '%s' "${COORDINATOR_HOLDER_ID:-${PROJECT_TAG}-${RUN_ID:-local}-coordinator}"
}

coordinator_iam_role_name() {
  printf '%s' "${COORDINATOR_IAM_ROLE_NAME:-${PROJECT_TAG}-${RUN_ID:-local}-coordinator}"
}

coordinator_iam_instance_profile_name() {
  printf '%s' "${COORDINATOR_IAM_INSTANCE_PROFILE_NAME:-$(coordinator_iam_role_name)}"
}

dynamodb_control_enabled() {
  [[ "$CONTROL_STORE_BACKEND" == "dynamodb" ]]
}

dynamodb_session_enabled() {
  [[ "$SESSION_STORE_BACKEND" == "dynamodb" ]]
}

dynamodb_runtime_enabled() {
  dynamodb_control_enabled || dynamodb_session_enabled || [[ "$COMPLETED_UPLOAD_LEDGER_BACKEND" == "dynamodb" ]]
}

sqs_ingestion_enabled() {
  [[ "$INGESTION_QUEUE_BACKEND" == "sqs" ]]
}

hot_standby_ingestion_enabled() {
  sqs_ingestion_enabled && [[ "$HOT_STANDBY_INGESTION" == "1" || "$HOT_STANDBY_INGESTION" == "true" ]]
}

server_ingestion_iam_role_name() {
  printf '%s' "${SERVER_INGESTION_IAM_ROLE_NAME:-${PROJECT_TAG}-${RUN_ID:-local}-server-ingestion}"
}

server_ingestion_iam_instance_profile_name() {
  printf '%s' "${SERVER_INGESTION_IAM_INSTANCE_PROFILE_NAME:-$(server_ingestion_iam_role_name)}"
}

read_server_iam_role_name() {
  printf '%s' "${READ_SERVER_IAM_ROLE_NAME:-${PROJECT_TAG}-${RUN_ID:-local}-readserver}"
}

read_server_iam_instance_profile_name() {
  printf '%s' "${READ_SERVER_IAM_INSTANCE_PROFILE_NAME:-$(read_server_iam_role_name)}"
}

dynamodb_control_table() {
  [[ -n "$DYNAMODB_CONTROL_TABLE" ]] || die "DYNAMODB_CONTROL_TABLE is required when CONTROL_STORE_BACKEND=dynamodb"
  printf '%s' "$DYNAMODB_CONTROL_TABLE"
}

dynamodb_control_region() {
  printf '%s' "${DYNAMODB_CONTROL_REGION:-$AWS_REGION}"
}

dynamodb_session_table() {
  printf '%s' "${DYNAMODB_SESSION_TABLE:-$DYNAMODB_CONTROL_TABLE}"
}

dynamodb_session_region() {
  printf '%s' "${DYNAMODB_SESSION_REGION:-$(dynamodb_control_region)}"
}

coordinator_control_store_args() {
  local holder="${1:-$(coordinator_holder_id)}"
  local lease_ttl="${2:-$COORDINATOR_LEASE_TTL_SECONDS}"
  local lease_renew="${3:-$COORDINATOR_LEASE_RENEW_SECONDS}"
  local lease_args
  lease_args="$(printf -- " -coordinator-id %s -lease-ttl-seconds %s -lease-renew-seconds %s" \
    "$(quote "$holder")" \
    "$(quote "$lease_ttl")" \
    "$(quote "$lease_renew")")"
  case "$CONTROL_STORE_BACKEND" in
    memory)
      printf '%s' "$lease_args"
      return 0
      ;;
    dynamodb)
      printf -- " -control-store dynamodb -control-table %s -aws-region %s%s" \
        "$(quote "$(dynamodb_control_table)")" \
        "$(quote "$(dynamodb_control_region)")" \
        "$lease_args"
      ;;
    *)
      die "unknown CONTROL_STORE_BACKEND: $CONTROL_STORE_BACKEND"
      ;;
  esac
}

coordinator_session_store_args() {
  case "$SESSION_STORE_BACKEND" in
    memory)
      return 0
      ;;
    dynamodb)
      if dynamodb_control_enabled; then
        printf -- " -session-store dynamodb -session-table %s" \
          "$(quote "$(dynamodb_session_table)")"
      else
        printf -- " -session-store dynamodb -session-table %s -aws-region %s" \
          "$(quote "$(dynamodb_session_table)")" \
          "$(quote "$(dynamodb_session_region)")"
      fi
      ;;
    *)
      die "unknown SESSION_STORE_BACKEND: $SESSION_STORE_BACKEND"
      ;;
  esac
}

capture_dynamodb_control_item() {
  local pk="$1"
  local output_path="$2"
  aws_control_region dynamodb get-item \
    --table-name "$(dynamodb_control_table)" \
    --consistent-read \
    --key "{\"pk\":{\"S\":\"$pk\"}}" \
    --output json >"$output_path"
}

capture_dynamodb_epoch_shard_config() {
  local epoch_file="$1"
  local output_path="$2"
  local epoch_id

  epoch_id="$(python3 - "$epoch_file" <<'PY'
import json
import sys

try:
    item = json.load(open(sys.argv[1])).get("Item", {})
except (FileNotFoundError, json.JSONDecodeError):
    item = {}
print(item.get("epoch_id", {}).get("N", ""))
PY
)"
  [[ -n "$epoch_id" ]] || return 1
  capture_dynamodb_control_item "shard-config#epoch#$epoch_id" "$output_path"
}

validate_public_entry_backend() {
  case "$PUBLIC_ENTRY_BACKEND" in
    none|nlb)
      return 0
      ;;
    *)
      die "unknown PUBLIC_ENTRY_BACKEND: $PUBLIC_ENTRY_BACKEND"
      ;;
  esac
}

validate_ingestion_queue_backend() {
  case "$INGESTION_QUEUE_BACKEND" in
    memory|sqs)
      ;;
    *)
      die "unknown INGESTION_QUEUE_BACKEND: $INGESTION_QUEUE_BACKEND"
      ;;
  esac
  case "$COMPLETED_UPLOAD_LEDGER_BACKEND" in
    memory|dynamodb)
      return 0
      ;;
    *)
      die "unknown COMPLETED_UPLOAD_LEDGER_BACKEND: $COMPLETED_UPLOAD_LEDGER_BACKEND"
      ;;
  esac
}

ingestion_queue_url_for_shard() {
  local shard_id="$1"
  case "$shard_id" in
    0)
      [[ -n "${INGESTION_SQS_SHARD0_QUEUE_URL:-}" ]] || die "missing INGESTION_SQS_SHARD0_QUEUE_URL"
      printf '%s' "$INGESTION_SQS_SHARD0_QUEUE_URL"
      ;;
    1)
      [[ -n "${INGESTION_SQS_SHARD1_QUEUE_URL:-}" ]] || die "missing INGESTION_SQS_SHARD1_QUEUE_URL"
      printf '%s' "$INGESTION_SQS_SHARD1_QUEUE_URL"
      ;;
    *)
      die "no ingestion queue configured for shard $shard_id"
      ;;
  esac
}

standby_ingestion_queue_url_for_shard() {
  local shard_id="$1"
  case "$shard_id" in
    0)
      [[ -n "${INGESTION_SQS_SHARD0_STANDBY_QUEUE_URL:-}" ]] || die "missing INGESTION_SQS_SHARD0_STANDBY_QUEUE_URL"
      printf '%s' "$INGESTION_SQS_SHARD0_STANDBY_QUEUE_URL"
      ;;
    1)
      [[ -n "${INGESTION_SQS_SHARD1_STANDBY_QUEUE_URL:-}" ]] || die "missing INGESTION_SQS_SHARD1_STANDBY_QUEUE_URL"
      printf '%s' "$INGESTION_SQS_SHARD1_STANDBY_QUEUE_URL"
      ;;
    *)
      die "no standby ingestion queue configured for shard $shard_id"
      ;;
  esac
}

server_ingestion_queue_args() {
	local shard_id="$1"
	local replica_id="${2:-active}"
	local worker_args ledger_args
	worker_args="$(printf -- " -ingestion-receive-batch-size %s -ingestion-worker-error-backoff-ms %s" \
		"$(quote "$INGESTION_RECEIVE_BATCH_SIZE")" \
		"$(quote "$INGESTION_WORKER_ERROR_BACKOFF_MS")")"
	ledger_args="$(printf -- " -completed-upload-processing-ttl-seconds %s" \
		"$(quote "$COMPLETED_UPLOAD_PROCESSING_TTL_SECONDS")")"
	case "$COMPLETED_UPLOAD_LEDGER_BACKEND" in
	memory)
		;;
	dynamodb)
		[[ -n "${COMPLETED_UPLOAD_LEDGER_TABLE:-}" ]] || die "COMPLETED_UPLOAD_LEDGER_TABLE is required when COMPLETED_UPLOAD_LEDGER_BACKEND=dynamodb"
		ledger_args="$(printf -- "%s -completed-upload-ledger dynamodb -completed-upload-ledger-table %s -aws-region %s" \
			"$ledger_args" \
			"$(quote "$COMPLETED_UPLOAD_LEDGER_TABLE")" \
			"$(quote "$AWS_REGION")")"
		;;
	*)
		die "unknown COMPLETED_UPLOAD_LEDGER_BACKEND: $COMPLETED_UPLOAD_LEDGER_BACKEND"
		;;
	esac
	case "$INGESTION_QUEUE_BACKEND" in
	memory)
		printf '%s%s -replica-id %s' "$worker_args" "$ledger_args" "$(quote "$replica_id")"
		;;
	sqs)
		[[ -n "${INGESTION_S3_BUCKET:-}" ]] || die "INGESTION_S3_BUCKET is required when INGESTION_QUEUE_BACKEND=sqs"
		local queue_url standby_fanout_arg
		if [[ "$replica_id" == "standby" ]]; then
			queue_url="$(standby_ingestion_queue_url_for_shard "$shard_id")"
			standby_fanout_arg=""
		else
			queue_url="$(ingestion_queue_url_for_shard "$shard_id")"
			standby_fanout_arg=""
			if hot_standby_ingestion_enabled; then
				standby_fanout_arg="$(printf -- " -standby-ingestion-sqs-queue-url %s" "$(quote "$(standby_ingestion_queue_url_for_shard "$shard_id")")")"
			fi
		fi
		printf -- "%s%s -replica-id %s -ingestion-queue sqs -ingestion-sqs-queue-url %s%s -ingestion-s3-bucket %s -aws-region %s -ingestion-sqs-wait-seconds %s -ingestion-sqs-visibility-timeout-seconds %s" \
			"$worker_args" \
			"$ledger_args" \
			"$(quote "$replica_id")" \
			"$(quote "$queue_url")" \
			"$standby_fanout_arg" \
			"$(quote "$INGESTION_S3_BUCKET")" \
			"$(quote "$AWS_REGION")" \
			"$(quote "$INGESTION_SQS_WAIT_SECONDS")" \
			"$(quote "$INGESTION_SQS_VISIBILITY_TIMEOUT_SECONDS")"
		;;
    *)
      die "unknown INGESTION_QUEUE_BACKEND: $INGESTION_QUEUE_BACKEND"
      ;;
  esac
}

readserver_binary_s3_key() {
  local prefix="${RESULT_TABLE_S3_PREFIX:-}"
  prefix="${prefix#/}"
  prefix="${prefix%/}"
  if [[ -n "$prefix" ]]; then
    printf '%s/bin/readserver' "$prefix"
  else
    printf 'bin/readserver'
  fi
}

server_result_table_args() {
  if [[ -z "${RESULT_TABLE_S3_BUCKET:-}" ]]; then
    return
  fi
  printf -- " -result-s3-bucket %s -result-s3-prefix %s -aws-region %s" \
    "$(quote "$RESULT_TABLE_S3_BUCKET")" \
    "$(quote "$RESULT_TABLE_S3_PREFIX")" \
    "$(quote "$AWS_REGION")"
}

capture_read_alb_artifacts() {
  local output_dir="$1"
  mkdir -p "$output_dir"
  [[ -n "${READ_ALB_ARN:-}" && -n "${READ_ALB_TARGET_GROUP_ARN:-}" && -n "${READ_ALB_LISTENER_ARN:-}" ]] || return 0
  aws_region elbv2 describe-load-balancers \
    --load-balancer-arns "$READ_ALB_ARN" \
    --output json >"$output_dir/load-balancer.json" || true
  aws_region elbv2 describe-target-groups \
    --target-group-arns "$READ_ALB_TARGET_GROUP_ARN" \
    --output json >"$output_dir/target-group.json" || true
  aws_region elbv2 describe-listeners \
    --listener-arns "$READ_ALB_LISTENER_ARN" \
    --output json >"$output_dir/listener.json" || true
  aws_region elbv2 describe-target-health \
    --target-group-arn "$READ_ALB_TARGET_GROUP_ARN" \
    --output json >"$output_dir/target-health.json" || true
}

readserver_instance_ids() {
  aws_region ec2 describe-instances \
    --filters Name=tag:Project,Values="$PROJECT_TAG" Name=tag:Role,Values=readserver Name=instance-state-name,Values=pending,running,stopping,stopped \
    --query 'Reservations[].Instances[].InstanceId' \
    --output text | tr '\t' '\n' | sed '/^$/d'
}

capture_ingestion_artifacts() {
  local output_dir="$1"
  mkdir -p "$output_dir"
  if ! sqs_ingestion_enabled; then
    return 0
  fi
  aws_region sqs get-queue-attributes \
    --queue-url "$INGESTION_SQS_SHARD0_QUEUE_URL" \
    --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible \
    --output json >"$output_dir/shard0-queue-attributes.json" || true
  aws_region sqs get-queue-attributes \
    --queue-url "$INGESTION_SQS_SHARD1_QUEUE_URL" \
    --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible \
    --output json >"$output_dir/shard1-queue-attributes.json" || true
  if hot_standby_ingestion_enabled; then
    aws_region sqs get-queue-attributes \
      --queue-url "$INGESTION_SQS_SHARD0_STANDBY_QUEUE_URL" \
      --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible \
      --output json >"$output_dir/shard0-standby-queue-attributes.json" || true
    aws_region sqs get-queue-attributes \
      --queue-url "$INGESTION_SQS_SHARD1_STANDBY_QUEUE_URL" \
      --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible \
      --output json >"$output_dir/shard1-standby-queue-attributes.json" || true
  fi
  aws_region s3api list-objects-v2 \
    --bucket "$INGESTION_S3_BUCKET" \
    --prefix "completed-uploads/" \
    --output json >"$output_dir/s3-payloads.json" || true
  if [[ "$COMPLETED_UPLOAD_LEDGER_BACKEND" == "dynamodb" ]]; then
    aws_region dynamodb scan \
      --table-name "$COMPLETED_UPLOAD_LEDGER_TABLE" \
      --filter-expression 'begins_with(pk, :prefix)' \
      --expression-attribute-values '{":prefix":{"S":"completed-upload#"}}' \
      --output json >"$output_dir/completed-upload-ledger.json" || true
  fi
}

wait_for_sqs_ingestion_drain() {
  local timeout="${1:-120}"
  local deadline=$((SECONDS + timeout))
  local shard0_counts shard1_counts shard0_standby_counts shard1_standby_counts
  if ! sqs_ingestion_enabled; then
    return 0
  fi
  info "waiting for SQS ingestion queues to drain"
  while true; do
    shard0_counts="$(aws_region sqs get-queue-attributes \
      --queue-url "$INGESTION_SQS_SHARD0_QUEUE_URL" \
      --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible \
      --query 'Attributes.[ApproximateNumberOfMessages,ApproximateNumberOfMessagesNotVisible]' \
      --output text 2>/dev/null || true)"
    shard1_counts="$(aws_region sqs get-queue-attributes \
      --queue-url "$INGESTION_SQS_SHARD1_QUEUE_URL" \
      --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible \
      --query 'Attributes.[ApproximateNumberOfMessages,ApproximateNumberOfMessagesNotVisible]' \
      --output text 2>/dev/null || true)"
    shard0_standby_counts=$'0\t0'
    shard1_standby_counts=$'0\t0'
    if hot_standby_ingestion_enabled; then
      shard0_standby_counts="$(aws_region sqs get-queue-attributes \
        --queue-url "$INGESTION_SQS_SHARD0_STANDBY_QUEUE_URL" \
        --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible \
        --query 'Attributes.[ApproximateNumberOfMessages,ApproximateNumberOfMessagesNotVisible]' \
        --output text 2>/dev/null || true)"
      shard1_standby_counts="$(aws_region sqs get-queue-attributes \
        --queue-url "$INGESTION_SQS_SHARD1_STANDBY_QUEUE_URL" \
        --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible \
        --query 'Attributes.[ApproximateNumberOfMessages,ApproximateNumberOfMessagesNotVisible]' \
        --output text 2>/dev/null || true)"
    fi
    if [[ "$shard0_counts" == $'0\t0' && "$shard1_counts" == $'0\t0' && "$shard0_standby_counts" == $'0\t0' && "$shard1_standby_counts" == $'0\t0' ]]; then
      return 0
    fi
    if (( SECONDS >= deadline )); then
      capture_ingestion_artifacts "$STATE_DIR/ingestion-drain-timeout"
      die "SQS ingestion queues did not drain before timeout; shard0=${shard0_counts:-unknown} shard1=${shard1_counts:-unknown} shard0-standby=${shard0_standby_counts:-unknown} shard1-standby=${shard1_standby_counts:-unknown}"
    fi
    sleep 2
  done
}

capture_nlb_artifacts() {
  local output_dir="$1"
  mkdir -p "$output_dir"
  if ! public_entry_enabled; then
    return 0
  fi
  [[ -n "${NLB_ARN:-}" && -n "${NLB_TARGET_GROUP_ARN:-}" && -n "${NLB_LISTENER_ARN:-}" ]] || return 0
  aws_region elbv2 describe-load-balancers \
    --load-balancer-arns "$NLB_ARN" \
    --output json >"$output_dir/load-balancer.json" || true
  aws_region elbv2 describe-target-groups \
    --target-group-arns "$NLB_TARGET_GROUP_ARN" \
    --output json >"$output_dir/target-group.json" || true
  aws_region elbv2 describe-listeners \
    --listener-arns "$NLB_LISTENER_ARN" \
    --output json >"$output_dir/listener.json" || true
  aws_region elbv2 describe-target-health \
    --target-group-arn "$NLB_TARGET_GROUP_ARN" \
    --output json >"$output_dir/target-health.json" || true
}

nlb_target_health_state() {
  local port="${1:-$COORDINATOR_PORT}"
  [[ -n "${NLB_TARGET_GROUP_ARN:-}" ]] || return 1
  aws_region elbv2 describe-target-health \
    --target-group-arn "$NLB_TARGET_GROUP_ARN" \
    --targets "Id=$COORDINATOR_ID,Port=$port" \
    --query 'TargetHealthDescriptions[0].TargetHealth.State' \
    --output text
}

wait_for_nlb_target_healthy() {
  local timeout="${1:-180}"
  local port="${2:-$COORDINATOR_PORT}"
  local deadline=$((SECONDS + timeout))
  local state
  if ! public_entry_enabled; then
    return 0
  fi
  info "waiting for NLB target health on coordinator port $port"
  while true; do
    state="$(nlb_target_health_state "$port" 2>/dev/null || true)"
    if [[ "$state" == "healthy" ]]; then
      info "NLB target is healthy on port $port"
      return 0
    fi
    if (( SECONDS >= deadline )); then
      capture_nlb_artifacts "$STATE_DIR/nlb-timeout"
      die "NLB target did not become healthy before timeout; last state=${state:-unknown}"
    fi
    sleep 5
  done
}

shard0_leader_addr() {
  printf '%s:%s' "$SHARD0_LEADER_PRIVATE_IP" "$SHARD0_LEADER_PORT"
}

shard0_follower_addr() {
  printf '%s:%s' "$SHARD0_FOLLOWER_PRIVATE_IP" "$SHARD0_FOLLOWER_PORT"
}

shard1_leader_addr() {
  printf '%s:%s' "$SHARD1_LEADER_PRIVATE_IP" "$SHARD1_LEADER_PORT"
}

shard1_follower_addr() {
  printf '%s:%s' "$SHARD1_FOLLOWER_PRIVATE_IP" "$SHARD1_FOLLOWER_PORT"
}

shard0_standby_leader_addr() {
  printf '%s:%s' "$SHARD0_LEADER_PRIVATE_IP" "$SHARD0_STANDBY_LEADER_PORT"
}

shard0_standby_follower_addr() {
  printf '%s:%s' "$SHARD0_FOLLOWER_PRIVATE_IP" "$SHARD0_STANDBY_FOLLOWER_PORT"
}

shard1_standby_leader_addr() {
  printf '%s:%s' "$SHARD1_LEADER_PRIVATE_IP" "$SHARD1_STANDBY_LEADER_PORT"
}

shard1_standby_follower_addr() {
  printf '%s:%s' "$SHARD1_FOLLOWER_PRIVATE_IP" "$SHARD1_STANDBY_FOLLOWER_PORT"
}

kill_remote_processes() {
  local host="$1"
  remote_cmd "$host" "pkill -TERM -x server >/dev/null 2>&1 || true; pkill -TERM -x coordinator >/dev/null 2>&1 || true; pkill -TERM -x client >/dev/null 2>&1 || true; pkill -TERM -x autoscaler >/dev/null 2>&1 || true; sleep 2; pkill -KILL -x server >/dev/null 2>&1 || true; pkill -KILL -x coordinator >/dev/null 2>&1 || true; pkill -KILL -x client >/dev/null 2>&1 || true; pkill -KILL -x autoscaler >/dev/null 2>&1 || true"
}

kill_all_remote_processes() {
  local host
  for host in \
    "$COORDINATOR_PUBLIC_IP" \
    "$SHARD0_LEADER_PUBLIC_IP" \
    "$SHARD0_FOLLOWER_PUBLIC_IP" \
    "$SHARD1_LEADER_PUBLIC_IP" \
    "$SHARD1_FOLLOWER_PUBLIC_IP" \
    "$CLIENT_PUBLIC_IP"; do
    kill_remote_processes "$host"
  done
}

prepare_remote_workspace() {
  local host="$1"
  remote_cmd "$host" "mkdir -p '$REMOTE_ROOT' '$REMOTE_PHASES_DIR' '$REMOTE_SMOKE_DIR'"
}

reset_remote_workspace() {
  local host="$1"
  remote_cmd "$host" "rm -rf '$REMOTE_ROOT'; mkdir -p '$REMOTE_ROOT' '$REMOTE_PHASES_DIR' '$REMOTE_SMOKE_DIR'"
}

reset_all_remote_workspaces() {
  local host
  for host in \
    "$COORDINATOR_PUBLIC_IP" \
    "$SHARD0_LEADER_PUBLIC_IP" \
    "$SHARD0_FOLLOWER_PUBLIC_IP" \
    "$SHARD1_LEADER_PUBLIC_IP" \
    "$SHARD1_FOLLOWER_PUBLIC_IP" \
    "$CLIENT_PUBLIC_IP"; do
    reset_remote_workspace "$host"
  done
}

remote_wait_for_port() {
  local ssh_host="$1"
  local target_host="$2"
  local target_port="$3"
  local attempts="${4:-60}"
  local quoted_host quoted_port quoted_attempts
  quoted_host="$(quote "$target_host")"
  quoted_port="$(quote "$target_port")"
  quoted_attempts="$(quote "$attempts")"
  remote_cmd "$ssh_host" "python3 - $quoted_host $quoted_port $quoted_attempts <<'PY'
import socket
import sys
import time

host = sys.argv[1]
port = int(sys.argv[2])
attempts = int(sys.argv[3])

for _ in range(attempts):
    sock = socket.socket()
    sock.settimeout(0.5)
    try:
        sock.connect((host, port))
    except OSError:
        time.sleep(1)
    else:
        sock.close()
        raise SystemExit(0)
    finally:
        try:
            sock.close()
        except OSError:
            pass
raise SystemExit(1)
PY"
}

phase_dir() {
  printf '%s/%s' "$REMOTE_PHASES_DIR" "$1"
}

phase_log_dir() {
  printf '%s/logs' "$(phase_dir "$1")"
}

phase_results_dir() {
  printf '%s/results' "$(phase_dir "$1")"
}

smoke_dir() {
  printf '%s/run' "$REMOTE_SMOKE_DIR"
}

smoke_log_dir() {
  printf '%s/logs' "$(smoke_dir)"
}

smoke_results_dir() {
  printf '%s/results' "$(smoke_dir)"
}

start_remote_server() {
  local host="$1"
  local idx="$2"
  local shard_id="$3"
  local servers_csv="$4"
  local results_dir="$5"
  local log_path="$6"
  local replica_id="${7:-active}"
  local global_row_start=$((shard_id * ROWS_PER_SHARD))
  local ingestion_args result_args
  ingestion_args="$(server_ingestion_queue_args "$shard_id" "$replica_id")"
  result_args="$(server_result_table_args)"
  remote_cmd "$host" "mkdir -p '$(dirname "$log_path")' '$results_dir'; nohup ~/server -idx '$idx' -threads '$SERVER_THREADS' -shard-id '$shard_id' -global-row-start '$global_row_start' -results-dir '$results_dir' -log '$log_path' -servers '$servers_csv'$ingestion_args$result_args > '${log_path}.nohup' 2>&1 &"
}

start_remote_coordinator() {
  local host="$1"
  local listen_addr="$2"
  local log_path="$3"
  local holder="${4:-$(coordinator_holder_id)}"
  local lease_ttl="${5:-$COORDINATOR_LEASE_TTL_SECONDS}"
  local lease_renew="${6:-$COORDINATOR_LEASE_RENEW_SECONDS}"
  local pid_path="${7:-}"
  local standby="${8:-0}"
  local control_args session_args seed_args mkdir_cmd shard0_arg shard1_arg
  control_args="$(coordinator_control_store_args "$holder" "$lease_ttl" "$lease_renew")"
  session_args="$(coordinator_session_store_args)"
  seed_args=""
  if [[ "$COORDINATOR_INITIAL_ACTIVE_SHARDS" != "0" ]]; then
    seed_args=" -initial-active-shards '$COORDINATOR_INITIAL_ACTIVE_SHARDS'"
  fi
  if [[ "$standby" == "1" || "$standby" == "true" ]]; then
    control_args="$control_args -standby"
  fi
  shard0_arg="0,0,256,$(shard0_leader_addr),$(shard0_follower_addr)"
  shard1_arg="1,256,512,$(shard1_leader_addr),$(shard1_follower_addr)"
  if hot_standby_ingestion_enabled; then
    shard0_arg="$shard0_arg,$(shard0_standby_leader_addr)|$(shard0_standby_follower_addr)"
    shard1_arg="$shard1_arg,$(shard1_standby_leader_addr)|$(shard1_standby_follower_addr)"
  fi
  mkdir_cmd="mkdir -p '$(dirname "$log_path")'"
  if [[ -n "$pid_path" ]]; then
    mkdir_cmd="$mkdir_cmd '$(dirname "$pid_path")'"
    remote_cmd "$host" "$mkdir_cmd; nohup ~/coordinator -listen '$listen_addr' -log '$log_path' -shard '$shard0_arg' -shard '$shard1_arg'$control_args$session_args$seed_args $COORDINATOR_EXTRA_ARGS > '${log_path}.nohup' 2>&1 & echo \$! > '$pid_path'"
  else
    remote_cmd "$host" "$mkdir_cmd; nohup ~/coordinator -listen '$listen_addr' -log '$log_path' -shard '$shard0_arg' -shard '$shard1_arg'$control_args$session_args$seed_args $COORDINATOR_EXTRA_ARGS > '${log_path}.nohup' 2>&1 &"
  fi
}

run_remote_autoscaler_once() {
  local host="$1"
  local coordinator_target="$2"
  local log_path="$3"
  local apply="${4:-0}"
  local apply_arg=""
  if [[ "$apply" == "1" || "$apply" == "true" ]]; then
    apply_arg=" -apply"
  fi
  remote_cmd "$host" "mkdir -p '$(dirname "$log_path")'; ~/autoscaler -coordinator '$coordinator_target' -control-table '$(dynamodb_control_table)' -aws-region '$(dynamodb_control_region)' -once$apply_arg -log '$log_path'"
}

start_remote_hammer_client() {
  local host="$1"
  local target_flag="$2"
  local target_addr="$3"
  local log_path="$4"
  local pid_path="${5:-}"
  local mkdir_cmd retry_args
  mkdir_cmd="mkdir -p '$(dirname "$log_path")'"
  retry_args=""
  if [[ "$CLIENT_RETRY_OVERLOAD" == "1" || "$CLIENT_RETRY_OVERLOAD" == "true" ]]; then
    retry_args="-retry-overload -overload-backoff-initial-ms '$CLIENT_OVERLOAD_BACKOFF_INITIAL_MS' -overload-backoff-max-ms '$CLIENT_OVERLOAD_BACKOFF_MAX_MS'"
  fi
  if [[ -n "$pid_path" ]]; then
    mkdir_cmd="$mkdir_cmd '$(dirname "$pid_path")'"
    remote_cmd "$host" "$mkdir_cmd; nohup ~/client '$target_flag' '$target_addr' -hammer -threads '$CLIENT_THREADS' -concurrency '$CLIENT_CONCURRENCY' $retry_args -log '$log_path' > '${log_path}.nohup' 2>&1 & echo \$! > '$pid_path'"
  else
    remote_cmd "$host" "$mkdir_cmd; nohup ~/client '$target_flag' '$target_addr' -hammer -threads '$CLIENT_THREADS' -concurrency '$CLIENT_CONCURRENCY' $retry_args -log '$log_path' > '${log_path}.nohup' 2>&1 &"
  fi
}

wait_remote_pid_exit() {
  local host="$1"
  local pid_path="$2"
  local timeout="$3"
  local quoted_pid_path deadline pid stat status
  quoted_pid_path="$(quote "$pid_path")"
  deadline=$((SECONDS + timeout))

  while ! remote_cmd "$host" "test -s $quoted_pid_path" >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
      return 2
    fi
    sleep 1
  done

  pid="$(remote_cmd "$host" "cat $quoted_pid_path")"

  while true; do
    set +e
    stat="$(remote_cmd "$host" "ps -p '$pid' -o stat= 2>/dev/null")"
    status=$?
    set -e
    if [[ "$status" -ne 0 || -z "${stat//[[:space:]]/}" || "$stat" == Z* ]]; then
      return 0
    fi
    if (( SECONDS >= deadline )); then
      return 1
    fi
    sleep 1
  done
}

capture_remote_process_snapshot() {
  local host="$1"
  local output_path="$2"
  remote_cmd "$host" "mkdir -p '$(dirname "$output_path")'; { date -u; pgrep -a -x server || true; pgrep -a -x coordinator || true; pgrep -a -x client || true; } > '$output_path'"
}

write_remote_phase_status() {
  local host="$1"
  local phase="$2"
  local duration="$3"
  local wait_timeout="$4"
  local valid="$5"
  local client_exit_status="$6"
  local client_exit_reason="$7"
  local invalid_reason="$8"
  local status_path
  local quoted_status_path quoted_phase quoted_duration quoted_wait_timeout
  local quoted_valid quoted_client_exit_status quoted_client_exit_reason quoted_invalid_reason
  local quoted_retry_overload quoted_retry_initial quoted_retry_max
  status_path="$(phase_dir "$phase")/phase-status.json"
  quoted_status_path="$(quote "$status_path")"
  quoted_phase="$(quote "$phase")"
  quoted_duration="$(quote "$duration")"
  quoted_wait_timeout="$(quote "$wait_timeout")"
  quoted_valid="$(quote "$valid")"
  quoted_client_exit_status="$(quote "$client_exit_status")"
  quoted_client_exit_reason="$(quote "$client_exit_reason")"
  quoted_invalid_reason="$(quote "$invalid_reason")"
  quoted_retry_overload="$(quote "$CLIENT_RETRY_OVERLOAD")"
  quoted_retry_initial="$(quote "$CLIENT_OVERLOAD_BACKOFF_INITIAL_MS")"
  quoted_retry_max="$(quote "$CLIENT_OVERLOAD_BACKOFF_MAX_MS")"
  remote_cmd "$host" "mkdir -p '$(dirname "$status_path")'; python3 - $quoted_status_path $quoted_phase $quoted_duration $quoted_wait_timeout $quoted_valid $quoted_client_exit_status $quoted_client_exit_reason $quoted_invalid_reason $quoted_retry_overload $quoted_retry_initial $quoted_retry_max <<'PY'
import json
import sys

status_path = sys.argv[1]
payload = {
    'phase': sys.argv[2],
    'duration_seconds': int(sys.argv[3]),
    'client_wait_timeout_seconds': int(sys.argv[4]),
    'valid': sys.argv[5] == 'true',
    'client_exit_status': sys.argv[6],
    'client_exit_reason': sys.argv[7],
    'invalid_reason': sys.argv[8],
    'client_retry_overload': sys.argv[9],
    'client_overload_backoff_initial_ms': int(sys.argv[10]),
    'client_overload_backoff_max_ms': int(sys.argv[11]),
}

with open(status_path, 'w') as fh:
    json.dump(payload, fh, indent=2, sort_keys=True)
    fh.write('\n')
PY"
}

run_remote_client_once() {
  local host="$1"
  shift
  remote_cmd "$host" "mkdir -p '$REMOTE_SMOKE_DIR'; ~/client $*"
}

admin_binary_for_kind() {
  if [[ "$1" == "coordinator" ]]; then
    echo "~/coordinator"
  else
    echo "~/server"
  fi
}

remote_epoch_status() {
  local kind="$1"
  local host="$2"
  local target_addr="$3"
  local bin
  bin="$(admin_binary_for_kind "$kind")"
  remote_cmd "$host" "$bin -admin-target '$target_addr' -epoch-status 2>&1 | tail -n1"
}

capture_remote_status_json() {
  local kind="$1"
  local host="$2"
  local target_addr="$3"
  local output_path="$4"
  local bin
  bin="$(admin_binary_for_kind "$kind")"
  remote_cmd "$host" "mkdir -p '$(dirname "$output_path")'; $bin -admin-target '$target_addr' -status > '$output_path'"
}

extract_field() {
  local line="$1"
  local key="$2"
  printf '%s\n' "$line" | sed -n "s/.*${key}=\([^ ]*\).*/\1/p"
}

retry_start_epoch() {
  local kind="$1"
  local host="$2"
  local target_addr="$3"
  local duration="$4"
  local timeout="${5:-$START_EPOCH_RETRY_TIMEOUT}"
  local interval="${6:-$START_EPOCH_RETRY_INTERVAL}"
  local bin out status last_line
  local deadline=$((SECONDS + timeout))

  bin="$(admin_binary_for_kind "$kind")"
  while true; do
    set +e
    out="$(remote_cmd "$host" "$bin -admin-target '$target_addr' -start-epoch-seconds '$duration' 2>&1")"
    status=$?
    set -e
    last_line="$(printf '%s\n' "$out" | tail -n1)"
    if [[ $status -eq 0 ]]; then
      printf '%s\n' "$last_line"
      return 0
    fi

    if printf '%s\n' "$out" | grep -qi "not ready"; then
      if (( SECONDS >= deadline )); then
        printf '%s\n' "$out" >&2
        die "timed out waiting for $kind at $target_addr to become ready for StartEpoch"
      fi
      sleep "$interval"
      continue
    fi

    printf '%s\n' "$out" >&2
    return "$status"
  done
}

wait_for_status_state() {
  local kind="$1"
  local host="$2"
  local target_addr="$3"
  local want_state="$4"
  local timeout="${5:-30}"
  local line state
  for _ in $(seq 1 "$timeout"); do
    line="$(remote_epoch_status "$kind" "$host" "$target_addr")"
    state="$(extract_field "$line" "state")"
    if [[ "$state" == "$want_state" ]]; then
      printf '%s\n' "$line"
      return 0
    fi
    sleep 1
  done
  return 1
}

wait_for_epoch_complete() {
  local kind="$1"
  local host="$2"
  local target_addr="$3"
  local timeout="${4:-120}"
  wait_for_status_state "$kind" "$host" "$target_addr" completed "$timeout" >/dev/null || die "$kind at $target_addr did not reach completed state"
}

result_file_name() {
  local epoch_id="$1"
  local shard_id="$2"
  printf 'epoch-%06d-shard-%d-server-0.json' "$epoch_id" "$shard_id"
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
        raise SystemExit(0)

raise SystemExit(f"missing slot ({want_row},{want_col}) in {path}")
PY
}

copy_remote_tree_if_present() {
  local label="$1"
  local host="$2"
  local remote_path="$3"
  local local_dir="$4"
  mkdir -p "$local_dir"
  if ! copy_from_remote "$host" "$remote_path" "$local_dir"; then
    echo "warning: failed to copy ${remote_path} from ${label}" >&2
  fi
}
