#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

require_cmd aws
require_cmd curl
require_cmd chmod
require_cmd terraform
require_cmd python3
require_cmd ssh-keygen
validate_public_entry_backend
validate_ingestion_queue_backend

if [[ -f "$STATE_FILE" && "${FORCE:-0}" != "1" ]]; then
  die "state already exists at $STATE_FILE. Run 06-teardown.sh first, or set FORCE=1 to overwrite local state."
fi

mkdir -p "$STATE_DIR" "$KEY_DIR"

RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
KEY_NAME="${KEY_NAME:-${PROJECT_TAG}-${RUN_ID}}"
KEY_FILE="${KEY_FILE:-$KEY_DIR/${KEY_NAME}.pem}"
SG_NAME="${SG_NAME:-${PROJECT_TAG}-${RUN_ID}}"
SSH_CIDR="${SSH_CIDR:-${TF_VAR_ssh_cidr:-$(curl -fsS https://checkip.amazonaws.com | tr -d '[:space:]')/32}}"
VPC_ID="${VPC_ID:-${TF_VAR_vpc_id:-}}"
SUBNET_ID="${SUBNET_ID:-${TF_VAR_subnet_id:-}}"

IFS=$'\t' read -r SELECTED_VPC_ID SELECTED_SUBNET_ID SELECTED_AZ <<<"$(resolve_network_selection)"
AMI_ID="${AMI_ID:-$(aws_region ssm get-parameter --name "$AMI_SSM_PARAM" --query 'Parameter.Value' --output text)}"

TF_DIR="$SCRIPT_DIR/terraform"
TFVARS_FILE="$STATE_DIR/terraform.tfvars.json"

if [[ ! -f "$KEY_FILE" ]]; then
  info "generating SSH key: $KEY_FILE"
  ssh-keygen -q -t rsa -b 4096 -m PEM -N "" -f "$KEY_FILE"
fi
chmod 400 "$KEY_FILE"

info "launch run id: $RUN_ID"
info "selected network: vpc=$SELECTED_VPC_ID subnet=$SELECTED_SUBNET_ID az=$SELECTED_AZ"
info "writing Terraform variables: $TFVARS_FILE"

export DYNAMODB_CONTROL_REGION_RESOLVED="$(dynamodb_control_region)"
export DYNAMODB_SESSION_TABLE_RESOLVED="$(dynamodb_session_table)"
export DYNAMODB_SESSION_REGION_RESOLVED="$(dynamodb_session_region)"
export COORDINATOR_HOLDER_ID_RESOLVED="$(coordinator_holder_id)"
export COORDINATOR_IAM_ROLE_NAME_RESOLVED="$(coordinator_iam_role_name)"
export COORDINATOR_IAM_INSTANCE_PROFILE_NAME_RESOLVED="$(coordinator_iam_instance_profile_name)"
export SERVER_INGESTION_IAM_ROLE_NAME_RESOLVED="$(server_ingestion_iam_role_name)"
export SERVER_INGESTION_IAM_INSTANCE_PROFILE_NAME_RESOLVED="$(server_ingestion_iam_instance_profile_name)"
CREATE_DYNAMODB_CONTROL_TABLE=false
CREATE_DYNAMODB_SESSION_TABLE=false
if dynamodb_runtime_enabled; then
  if [[ "$CONTROL_STORE_BACKEND" == "dynamodb" || ( "$COMPLETED_UPLOAD_LEDGER_BACKEND" == "dynamodb" && "$COMPLETED_UPLOAD_LEDGER_TABLE" == "$DYNAMODB_CONTROL_TABLE" ) ]]; then
    if aws_base --region "$DYNAMODB_CONTROL_REGION_RESOLVED" dynamodb describe-table --table-name "$DYNAMODB_CONTROL_TABLE" >/dev/null 2>&1; then
      info "DynamoDB control table already exists; Terraform will not create it: $DYNAMODB_CONTROL_TABLE"
    else
      CREATE_DYNAMODB_CONTROL_TABLE=true
    fi
  fi
  if [[ "$SESSION_STORE_BACKEND" == "dynamodb" ]]; then
    if [[ "$DYNAMODB_SESSION_TABLE_RESOLVED" == "$DYNAMODB_CONTROL_TABLE" && "$DYNAMODB_SESSION_REGION_RESOLVED" == "$DYNAMODB_CONTROL_REGION_RESOLVED" && "$CREATE_DYNAMODB_CONTROL_TABLE" == "true" ]]; then
      :
    elif aws_base --region "$DYNAMODB_SESSION_REGION_RESOLVED" dynamodb describe-table --table-name "$DYNAMODB_SESSION_TABLE_RESOLVED" >/dev/null 2>&1; then
      info "DynamoDB session table already exists; Terraform will not create it: $DYNAMODB_SESSION_TABLE_RESOLVED"
    else
      CREATE_DYNAMODB_SESSION_TABLE=true
    fi
  fi
fi
export AWS_REGION PROJECT_TAG RUN_ID AMI_ID AMI_SSM_PARAM SELECTED_VPC_ID SELECTED_SUBNET_ID SELECTED_AZ
export SSH_CIDR SSH_USER KEY_NAME KEY_FILE SG_NAME COORDINATOR_INSTANCE_TYPE SERVER_INSTANCE_TYPE CLIENT_INSTANCE_TYPE
export SERVER_THREADS CLIENT_THREADS CLIENT_CONCURRENCY CLIENT_RETRY_OVERLOAD CLIENT_OVERLOAD_BACKOFF_INITIAL_MS CLIENT_OVERLOAD_BACKOFF_MAX_MS
export WARMUP_EPOCH_SECONDS MEASURED_EPOCH_SECONDS START_EPOCH_RETRY_TIMEOUT START_EPOCH_RETRY_INTERVAL POST_EPOCH_FLUSH_SECONDS CLIENT_EXIT_GRACE_SECONDS
export COORDINATOR_PORT COORDINATOR_STANDBY_PORT SHARD0_LEADER_PORT SHARD0_FOLLOWER_PORT SHARD1_LEADER_PORT SHARD1_FOLLOWER_PORT
export REMOTE_ROOT REMOTE_BIN_DIR REMOTE_PHASES_DIR REMOTE_SMOKE_DIR CONTROL_STORE_BACKEND DYNAMODB_CONTROL_TABLE SESSION_STORE_BACKEND
export COORDINATOR_LEASE_TTL_SECONDS COORDINATOR_LEASE_RENEW_SECONDS COORDINATOR_IAM_POLICY_NAME PUBLIC_ENTRY_BACKEND PUBLIC_ENTRY_MULTI_COORDINATOR
export INGESTION_QUEUE_BACKEND INGESTION_S3_BUCKET INGESTION_RECEIVE_BATCH_SIZE INGESTION_SQS_WAIT_SECONDS INGESTION_SQS_VISIBILITY_TIMEOUT_SECONDS INGESTION_WORKER_ERROR_BACKOFF_MS
export COMPLETED_UPLOAD_LEDGER_BACKEND COMPLETED_UPLOAD_LEDGER_TABLE COMPLETED_UPLOAD_PROCESSING_TTL_SECONDS SERVER_INGESTION_IAM_POLICY_NAME
export SERVER_INGESTION_IAM_ROLE_NAME_RESOLVED SERVER_INGESTION_IAM_INSTANCE_PROFILE_NAME_RESOLVED
export CREATE_DYNAMODB_CONTROL_TABLE CREATE_DYNAMODB_SESSION_TABLE

python3 - "$TFVARS_FILE" <<'PY'
import json
import os
import sys

payload = {
    "aws_region": os.environ["AWS_REGION"],
    "project_tag": os.environ["PROJECT_TAG"],
    "run_id": os.environ["RUN_ID"],
    "ami_id": os.environ["AMI_ID"],
    "ami_ssm_param": os.environ["AMI_SSM_PARAM"],
    "vpc_id": os.environ["SELECTED_VPC_ID"],
    "subnet_id": os.environ["SELECTED_SUBNET_ID"],
    "availability_zone": os.environ["SELECTED_AZ"],
    "ssh_cidr": os.environ["SSH_CIDR"],
    "ssh_user": os.environ["SSH_USER"],
    "key_name": os.environ["KEY_NAME"],
    "key_file": os.environ["KEY_FILE"],
    "ssh_public_key_path": os.environ["KEY_FILE"] + ".pub",
    "sg_name": os.environ["SG_NAME"],
    "coordinator_instance_type": os.environ["COORDINATOR_INSTANCE_TYPE"],
    "server_instance_type": os.environ["SERVER_INSTANCE_TYPE"],
    "client_instance_type": os.environ["CLIENT_INSTANCE_TYPE"],
    "server_threads": os.environ["SERVER_THREADS"],
    "client_threads": os.environ["CLIENT_THREADS"],
    "client_concurrency": os.environ["CLIENT_CONCURRENCY"],
    "client_retry_overload": os.environ["CLIENT_RETRY_OVERLOAD"],
    "client_overload_backoff_initial_ms": os.environ["CLIENT_OVERLOAD_BACKOFF_INITIAL_MS"],
    "client_overload_backoff_max_ms": os.environ["CLIENT_OVERLOAD_BACKOFF_MAX_MS"],
    "warmup_epoch_seconds": os.environ["WARMUP_EPOCH_SECONDS"],
    "measured_epoch_seconds": os.environ["MEASURED_EPOCH_SECONDS"],
    "start_epoch_retry_timeout": os.environ["START_EPOCH_RETRY_TIMEOUT"],
    "start_epoch_retry_interval": os.environ["START_EPOCH_RETRY_INTERVAL"],
    "post_epoch_flush_seconds": os.environ["POST_EPOCH_FLUSH_SECONDS"],
    "client_exit_grace_seconds": os.environ["CLIENT_EXIT_GRACE_SECONDS"],
    "coordinator_port": os.environ["COORDINATOR_PORT"],
    "coordinator_standby_port": os.environ["COORDINATOR_STANDBY_PORT"],
    "shard0_leader_port": os.environ["SHARD0_LEADER_PORT"],
    "shard0_follower_port": os.environ["SHARD0_FOLLOWER_PORT"],
    "shard1_leader_port": os.environ["SHARD1_LEADER_PORT"],
    "shard1_follower_port": os.environ["SHARD1_FOLLOWER_PORT"],
    "remote_root": os.environ["REMOTE_ROOT"],
    "remote_bin_dir": os.environ["REMOTE_BIN_DIR"],
    "remote_phases_dir": os.environ["REMOTE_PHASES_DIR"],
    "remote_smoke_dir": os.environ["REMOTE_SMOKE_DIR"],
    "control_store_backend": os.environ["CONTROL_STORE_BACKEND"],
    "dynamodb_control_table": os.environ["DYNAMODB_CONTROL_TABLE"],
    "dynamodb_control_region": os.environ["DYNAMODB_CONTROL_REGION_RESOLVED"],
    "session_store_backend": os.environ["SESSION_STORE_BACKEND"],
    "dynamodb_session_table": os.environ["DYNAMODB_SESSION_TABLE_RESOLVED"],
    "dynamodb_session_region": os.environ["DYNAMODB_SESSION_REGION_RESOLVED"],
    "create_dynamodb_control_table": os.environ["CREATE_DYNAMODB_CONTROL_TABLE"] == "true",
    "create_dynamodb_session_table": os.environ["CREATE_DYNAMODB_SESSION_TABLE"] == "true",
    "coordinator_holder_id": os.environ["COORDINATOR_HOLDER_ID_RESOLVED"],
    "coordinator_lease_ttl_seconds": os.environ["COORDINATOR_LEASE_TTL_SECONDS"],
    "coordinator_lease_renew_seconds": os.environ["COORDINATOR_LEASE_RENEW_SECONDS"],
    "coordinator_iam_role_name": os.environ["COORDINATOR_IAM_ROLE_NAME_RESOLVED"],
    "coordinator_iam_instance_profile_name": os.environ["COORDINATOR_IAM_INSTANCE_PROFILE_NAME_RESOLVED"],
    "coordinator_iam_policy_name": os.environ["COORDINATOR_IAM_POLICY_NAME"],
    "public_entry_backend": os.environ["PUBLIC_ENTRY_BACKEND"],
    "public_entry_multi_coordinator": os.environ["PUBLIC_ENTRY_MULTI_COORDINATOR"],
    "ingestion_queue_backend": os.environ["INGESTION_QUEUE_BACKEND"],
    "ingestion_s3_bucket": os.environ["INGESTION_S3_BUCKET"],
    "ingestion_receive_batch_size": os.environ["INGESTION_RECEIVE_BATCH_SIZE"],
    "ingestion_sqs_wait_seconds": os.environ["INGESTION_SQS_WAIT_SECONDS"],
    "ingestion_sqs_visibility_timeout_seconds": os.environ["INGESTION_SQS_VISIBILITY_TIMEOUT_SECONDS"],
    "ingestion_worker_error_backoff_ms": os.environ["INGESTION_WORKER_ERROR_BACKOFF_MS"],
    "completed_upload_ledger_backend": os.environ["COMPLETED_UPLOAD_LEDGER_BACKEND"],
    "completed_upload_ledger_table": os.environ["COMPLETED_UPLOAD_LEDGER_TABLE"],
    "completed_upload_processing_ttl_seconds": os.environ["COMPLETED_UPLOAD_PROCESSING_TTL_SECONDS"],
    "server_ingestion_iam_role_name": os.environ["SERVER_INGESTION_IAM_ROLE_NAME_RESOLVED"],
    "server_ingestion_iam_instance_profile_name": os.environ["SERVER_INGESTION_IAM_INSTANCE_PROFILE_NAME_RESOLVED"],
    "server_ingestion_iam_policy_name": os.environ["SERVER_INGESTION_IAM_POLICY_NAME"],
}

with open(sys.argv[1], "w") as fh:
    json.dump(payload, fh, indent=2, sort_keys=True)
    fh.write("\n")
PY

terraform -chdir="$TF_DIR" init

apply_args=(-var-file="$TFVARS_FILE")
if [[ "${TERRAFORM_AUTO_APPROVE:-1}" != "0" ]]; then
  apply_args+=(-auto-approve)
fi

info "applying Terraform infrastructure"
terraform -chdir="$TF_DIR" apply "${apply_args[@]}"

info "writing launch state: $STATE_FILE"
terraform -chdir="$TF_DIR" output -json >"$STATE_DIR/terraform-output.json"
python3 - "$STATE_FILE" "$STATE_DIR/terraform-output.json" <<'PY'
import json
import shlex
import sys

with open(sys.argv[2]) as fh:
    payload = json.load(fh)["state_env"]["value"]
with open(sys.argv[1], "w") as fh:
    for key in sorted(payload):
        value = "" if payload[key] is None else str(payload[key])
        fh.write(f"{key}={shlex.quote(value)}\n")
PY

echo
cat "$STATE_FILE"
echo

# shellcheck disable=SC1090
source "$STATE_FILE"

wait_for_ssh "$COORDINATOR_PUBLIC_IP" "coordinator"
wait_for_ssh "$SHARD0_LEADER_PUBLIC_IP" "shard0-leader"
wait_for_ssh "$SHARD0_FOLLOWER_PUBLIC_IP" "shard0-follower"
wait_for_ssh "$SHARD1_LEADER_PUBLIC_IP" "shard1-leader"
wait_for_ssh "$SHARD1_FOLLOWER_PUBLIC_IP" "shard1-follower"
wait_for_ssh "$CLIENT_PUBLIC_IP" "client"

info "launch complete"
