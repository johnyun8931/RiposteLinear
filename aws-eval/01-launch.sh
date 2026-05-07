#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

require_cmd aws
require_cmd curl
require_cmd chmod
require_cmd mktemp

if [[ -f "$STATE_FILE" && "${FORCE:-0}" != "1" ]]; then
  die "state already exists at $STATE_FILE. Run 06-teardown.sh first, or set FORCE=1 to overwrite local state."
fi

mkdir -p "$STATE_DIR" "$KEY_DIR"

RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
KEY_NAME="${KEY_NAME:-${PROJECT_TAG}-${RUN_ID}}"
KEY_FILE="${KEY_FILE:-$KEY_DIR/${KEY_NAME}.pem}"
SG_NAME="${SG_NAME:-${PROJECT_TAG}-${RUN_ID}}"
SSH_CIDR="${SSH_CIDR:-$(curl -fsS https://checkip.amazonaws.com | tr -d '[:space:]')/32}"

IFS=$'\t' read -r SELECTED_VPC_ID SELECTED_SUBNET_ID SELECTED_AZ <<<"$(resolve_network_selection)"
AMI_ID="${AMI_ID:-$(aws_region ssm get-parameter --name "$AMI_SSM_PARAM" --query 'Parameter.Value' --output text)}"

SG_ID=""
COORDINATOR_ID=""
SHARD0_LEADER_ID=""
SHARD0_FOLLOWER_ID=""
SHARD1_LEADER_ID=""
SHARD1_FOLLOWER_ID=""
CLIENT_ID=""

COORDINATOR_PRIVATE_IP=""
SHARD0_LEADER_PRIVATE_IP=""
SHARD0_FOLLOWER_PRIVATE_IP=""
SHARD1_LEADER_PRIVATE_IP=""
SHARD1_FOLLOWER_PRIVATE_IP=""
CLIENT_PRIVATE_IP=""

COORDINATOR_PUBLIC_IP=""
SHARD0_LEADER_PUBLIC_IP=""
SHARD0_FOLLOWER_PUBLIC_IP=""
SHARD1_LEADER_PUBLIC_IP=""
SHARD1_FOLLOWER_PUBLIC_IP=""
CLIENT_PUBLIC_IP=""

write_launch_state() {
  cat >"$STATE_FILE" <<EOF_STATE
RUN_ID=$(quote "$RUN_ID")
PROJECT_TAG=$(quote "$PROJECT_TAG")
AWS_REGION=$(quote "$AWS_REGION")
AMI_ID=$(quote "$AMI_ID")
AMI_SSM_PARAM=$(quote "$AMI_SSM_PARAM")
SELECTED_VPC_ID=$(quote "$SELECTED_VPC_ID")
SELECTED_SUBNET_ID=$(quote "$SELECTED_SUBNET_ID")
SELECTED_AZ=$(quote "$SELECTED_AZ")
KEY_NAME=$(quote "$KEY_NAME")
KEY_FILE=$(quote "$KEY_FILE")
SG_ID=$(quote "$SG_ID")
SG_NAME=$(quote "$SG_NAME")
SSH_CIDR=$(quote "$SSH_CIDR")
COORDINATOR_INSTANCE_TYPE=$(quote "$COORDINATOR_INSTANCE_TYPE")
SERVER_INSTANCE_TYPE=$(quote "$SERVER_INSTANCE_TYPE")
CLIENT_INSTANCE_TYPE=$(quote "$CLIENT_INSTANCE_TYPE")
COORDINATOR_ID=$(quote "$COORDINATOR_ID")
SHARD0_LEADER_ID=$(quote "$SHARD0_LEADER_ID")
SHARD0_FOLLOWER_ID=$(quote "$SHARD0_FOLLOWER_ID")
SHARD1_LEADER_ID=$(quote "$SHARD1_LEADER_ID")
SHARD1_FOLLOWER_ID=$(quote "$SHARD1_FOLLOWER_ID")
CLIENT_ID=$(quote "$CLIENT_ID")
COORDINATOR_PRIVATE_IP=$(quote "$COORDINATOR_PRIVATE_IP")
SHARD0_LEADER_PRIVATE_IP=$(quote "$SHARD0_LEADER_PRIVATE_IP")
SHARD0_FOLLOWER_PRIVATE_IP=$(quote "$SHARD0_FOLLOWER_PRIVATE_IP")
SHARD1_LEADER_PRIVATE_IP=$(quote "$SHARD1_LEADER_PRIVATE_IP")
SHARD1_FOLLOWER_PRIVATE_IP=$(quote "$SHARD1_FOLLOWER_PRIVATE_IP")
CLIENT_PRIVATE_IP=$(quote "$CLIENT_PRIVATE_IP")
COORDINATOR_PUBLIC_IP=$(quote "$COORDINATOR_PUBLIC_IP")
SHARD0_LEADER_PUBLIC_IP=$(quote "$SHARD0_LEADER_PUBLIC_IP")
SHARD0_FOLLOWER_PUBLIC_IP=$(quote "$SHARD0_FOLLOWER_PUBLIC_IP")
SHARD1_LEADER_PUBLIC_IP=$(quote "$SHARD1_LEADER_PUBLIC_IP")
SHARD1_FOLLOWER_PUBLIC_IP=$(quote "$SHARD1_FOLLOWER_PUBLIC_IP")
CLIENT_PUBLIC_IP=$(quote "$CLIENT_PUBLIC_IP")
SSH_USER=$(quote "$SSH_USER")
SERVER_THREADS=$(quote "$SERVER_THREADS")
CLIENT_THREADS=$(quote "$CLIENT_THREADS")
CLIENT_CONCURRENCY=$(quote "$CLIENT_CONCURRENCY")
CLIENT_RETRY_OVERLOAD=$(quote "$CLIENT_RETRY_OVERLOAD")
CLIENT_OVERLOAD_BACKOFF_INITIAL_MS=$(quote "$CLIENT_OVERLOAD_BACKOFF_INITIAL_MS")
CLIENT_OVERLOAD_BACKOFF_MAX_MS=$(quote "$CLIENT_OVERLOAD_BACKOFF_MAX_MS")
WARMUP_EPOCH_SECONDS=$(quote "$WARMUP_EPOCH_SECONDS")
MEASURED_EPOCH_SECONDS=$(quote "$MEASURED_EPOCH_SECONDS")
START_EPOCH_RETRY_TIMEOUT=$(quote "$START_EPOCH_RETRY_TIMEOUT")
START_EPOCH_RETRY_INTERVAL=$(quote "$START_EPOCH_RETRY_INTERVAL")
POST_EPOCH_FLUSH_SECONDS=$(quote "$POST_EPOCH_FLUSH_SECONDS")
CLIENT_EXIT_GRACE_SECONDS=$(quote "$CLIENT_EXIT_GRACE_SECONDS")
COORDINATOR_PORT=$(quote "$COORDINATOR_PORT")
SHARD0_LEADER_PORT=$(quote "$SHARD0_LEADER_PORT")
SHARD0_FOLLOWER_PORT=$(quote "$SHARD0_FOLLOWER_PORT")
SHARD1_LEADER_PORT=$(quote "$SHARD1_LEADER_PORT")
SHARD1_FOLLOWER_PORT=$(quote "$SHARD1_FOLLOWER_PORT")
REMOTE_ROOT=$(quote "$REMOTE_ROOT")
REMOTE_BIN_DIR=$(quote "$REMOTE_BIN_DIR")
REMOTE_PHASES_DIR=$(quote "$REMOTE_PHASES_DIR")
REMOTE_SMOKE_DIR=$(quote "$REMOTE_SMOKE_DIR")
CONTROL_STORE_BACKEND=$(quote "$CONTROL_STORE_BACKEND")
DYNAMODB_CONTROL_TABLE=$(quote "$DYNAMODB_CONTROL_TABLE")
DYNAMODB_CONTROL_REGION=$(quote "$(dynamodb_control_region)")
COORDINATOR_HOLDER_ID=$(quote "$(coordinator_holder_id)")
COORDINATOR_LEASE_TTL_SECONDS=$(quote "$COORDINATOR_LEASE_TTL_SECONDS")
COORDINATOR_LEASE_RENEW_SECONDS=$(quote "$COORDINATOR_LEASE_RENEW_SECONDS")
COORDINATOR_IAM_ROLE_NAME=$(quote "$(coordinator_iam_role_name)")
COORDINATOR_IAM_INSTANCE_PROFILE_NAME=$(quote "$(coordinator_iam_instance_profile_name)")
COORDINATOR_IAM_POLICY_NAME=$(quote "$COORDINATOR_IAM_POLICY_NAME")
EOF_STATE
}

create_coordinator_instance_profile() {
  local account_id role_name profile_name policy_arn trust_doc policy_doc
  account_id="$(aws_base sts get-caller-identity --query Account --output text)"
  role_name="$(coordinator_iam_role_name)"
  profile_name="$(coordinator_iam_instance_profile_name)"
  policy_arn="arn:aws:dynamodb:$(dynamodb_control_region):${account_id}:table/$(dynamodb_control_table)"
  trust_doc="$(mktemp)"
  policy_doc="$(mktemp)"

  cat >"$trust_doc" <<'JSON'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "ec2.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
JSON

  cat >"$policy_doc" <<JSON
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "dynamodb:DescribeTable",
        "dynamodb:GetItem",
        "dynamodb:UpdateItem"
      ],
      "Resource": "$policy_arn"
    }
  ]
}
JSON

  info "creating coordinator IAM role/profile for DynamoDB control store: role=$role_name profile=$profile_name"
  if ! aws_base iam get-role --role-name "$role_name" >/dev/null 2>&1; then
    aws_base iam create-role \
      --role-name "$role_name" \
      --assume-role-policy-document "file://$trust_doc" \
      --tags Key=Project,Value="$PROJECT_TAG" Key=RunId,Value="$RUN_ID" >/dev/null
  fi

  aws_base iam put-role-policy \
    --role-name "$role_name" \
    --policy-name "$COORDINATOR_IAM_POLICY_NAME" \
    --policy-document "file://$policy_doc" >/dev/null

  if ! aws_base iam get-instance-profile --instance-profile-name "$profile_name" >/dev/null 2>&1; then
    aws_base iam create-instance-profile \
      --instance-profile-name "$profile_name" \
      --tags Key=Project,Value="$PROJECT_TAG" Key=RunId,Value="$RUN_ID" >/dev/null
  fi

  aws_base iam add-role-to-instance-profile \
    --instance-profile-name "$profile_name" \
    --role-name "$role_name" >/dev/null 2>&1 || true

  for _ in $(seq 1 30); do
    if aws_base iam get-instance-profile \
      --instance-profile-name "$profile_name" \
      --query "InstanceProfile.Roles[?RoleName=='${role_name}'].RoleName | [0]" \
      --output text | grep -qx "$role_name"; then
      rm -f "$trust_doc" "$policy_doc"
      return 0
    fi
    sleep 2
  done

  rm -f "$trust_doc" "$policy_doc"
  die "coordinator IAM instance profile did not become ready: $profile_name"
}

info "launch run id: $RUN_ID"
info "selected network: vpc=$SELECTED_VPC_ID subnet=$SELECTED_SUBNET_ID az=$SELECTED_AZ"
if dynamodb_control_enabled; then
  create_coordinator_instance_profile
  write_launch_state
fi
info "creating key pair: $KEY_NAME"
aws_region ec2 create-key-pair \
  --key-name "$KEY_NAME" \
  --key-type rsa \
  --key-format pem \
  --tag-specifications "ResourceType=key-pair,Tags=[{Key=Project,Value=${PROJECT_TAG}},{Key=RunId,Value=${RUN_ID}}]" \
  --query 'KeyMaterial' \
  --output text >"$KEY_FILE"
chmod 400 "$KEY_FILE"
write_launch_state

info "creating security group: $SG_NAME"
SG_ID="$(aws_region ec2 create-security-group \
  --group-name "$SG_NAME" \
  --description "Temporary Riposte AWS evaluation security group ${RUN_ID}" \
  --vpc-id "$SELECTED_VPC_ID" \
  --query 'GroupId' \
  --output text)"

aws_region ec2 create-tags \
  --resources "$SG_ID" \
  --tags Key=Project,Value="$PROJECT_TAG" Key=RunId,Value="$RUN_ID" Key=Name,Value="$SG_NAME"
write_launch_state

info "authorizing SSH from $SSH_CIDR"
aws_region ec2 authorize-security-group-ingress \
  --group-id "$SG_ID" \
  --ip-permissions "IpProtocol=tcp,FromPort=22,ToPort=22,IpRanges=[{CidrIp=${SSH_CIDR}}]"

info "authorizing all TCP within $SG_ID"
aws_region ec2 authorize-security-group-ingress \
  --group-id "$SG_ID" \
  --ip-permissions "IpProtocol=tcp,FromPort=0,ToPort=65535,UserIdGroupPairs=[{GroupId=${SG_ID}}]"

launch_instance() {
  local name="$1"
  local role="$2"
  local instance_type="$3"
  local instance_profile_name=""

  if [[ "$role" == "coordinator" ]] && dynamodb_control_enabled; then
    instance_profile_name="$(coordinator_iam_instance_profile_name)"
  fi

  local args=(
    --image-id "$AMI_ID" \
    --instance-type "$instance_type" \
    --key-name "$KEY_NAME" \
    --security-group-ids "$SG_ID" \
    --subnet-id "$SELECTED_SUBNET_ID" \
    --instance-initiated-shutdown-behavior terminate \
    --tag-specifications \
      "ResourceType=instance,Tags=[{Key=Project,Value=${PROJECT_TAG}},{Key=RunId,Value=${RUN_ID}},{Key=Name,Value=${name}},{Key=Role,Value=${role}}]" \
      "ResourceType=volume,Tags=[{Key=Project,Value=${PROJECT_TAG}},{Key=RunId,Value=${RUN_ID}},{Key=Name,Value=${name}-root},{Key=Role,Value=${role}}]" \
    --query 'Instances[0].InstanceId' \
    --output text
  )

  if [[ -n "$instance_profile_name" ]]; then
    args+=(--iam-instance-profile "Name=$instance_profile_name")
  fi

  aws_region ec2 run-instances "${args[@]}"
}

info "launching instances"
COORDINATOR_ID="$(launch_instance "${PROJECT_TAG}-coordinator" coordinator "$COORDINATOR_INSTANCE_TYPE")"
write_launch_state
SHARD0_LEADER_ID="$(launch_instance "${PROJECT_TAG}-shard0-leader" shard0-leader "$SERVER_INSTANCE_TYPE")"
write_launch_state
SHARD0_FOLLOWER_ID="$(launch_instance "${PROJECT_TAG}-shard0-follower" shard0-follower "$SERVER_INSTANCE_TYPE")"
write_launch_state
SHARD1_LEADER_ID="$(launch_instance "${PROJECT_TAG}-shard1-leader" shard1-leader "$SERVER_INSTANCE_TYPE")"
write_launch_state
SHARD1_FOLLOWER_ID="$(launch_instance "${PROJECT_TAG}-shard1-follower" shard1-follower "$SERVER_INSTANCE_TYPE")"
write_launch_state
CLIENT_ID="$(launch_instance "${PROJECT_TAG}-client" client "$CLIENT_INSTANCE_TYPE")"
write_launch_state

INSTANCE_IDS=(
  "$COORDINATOR_ID"
  "$SHARD0_LEADER_ID"
  "$SHARD0_FOLLOWER_ID"
  "$SHARD1_LEADER_ID"
  "$SHARD1_FOLLOWER_ID"
  "$CLIENT_ID"
)

info "waiting for instances to enter running state"
aws_region ec2 wait instance-running --instance-ids "${INSTANCE_IDS[@]}"

describe_field() {
  local instance_id="$1"
  local field="$2"
  aws_region ec2 describe-instances \
    --instance-ids "$instance_id" \
    --query "Reservations[0].Instances[0].${field}" \
    --output text
}

COORDINATOR_PRIVATE_IP="$(describe_field "$COORDINATOR_ID" PrivateIpAddress)"
SHARD0_LEADER_PRIVATE_IP="$(describe_field "$SHARD0_LEADER_ID" PrivateIpAddress)"
SHARD0_FOLLOWER_PRIVATE_IP="$(describe_field "$SHARD0_FOLLOWER_ID" PrivateIpAddress)"
SHARD1_LEADER_PRIVATE_IP="$(describe_field "$SHARD1_LEADER_ID" PrivateIpAddress)"
SHARD1_FOLLOWER_PRIVATE_IP="$(describe_field "$SHARD1_FOLLOWER_ID" PrivateIpAddress)"
CLIENT_PRIVATE_IP="$(describe_field "$CLIENT_ID" PrivateIpAddress)"

COORDINATOR_PUBLIC_IP="$(describe_field "$COORDINATOR_ID" PublicIpAddress)"
SHARD0_LEADER_PUBLIC_IP="$(describe_field "$SHARD0_LEADER_ID" PublicIpAddress)"
SHARD0_FOLLOWER_PUBLIC_IP="$(describe_field "$SHARD0_FOLLOWER_ID" PublicIpAddress)"
SHARD1_LEADER_PUBLIC_IP="$(describe_field "$SHARD1_LEADER_ID" PublicIpAddress)"
SHARD1_FOLLOWER_PUBLIC_IP="$(describe_field "$SHARD1_FOLLOWER_ID" PublicIpAddress)"
CLIENT_PUBLIC_IP="$(describe_field "$CLIENT_ID" PublicIpAddress)"

write_launch_state

echo
cat "$STATE_FILE"
echo

wait_for_ssh "$COORDINATOR_PUBLIC_IP" "coordinator"
wait_for_ssh "$SHARD0_LEADER_PUBLIC_IP" "shard0-leader"
wait_for_ssh "$SHARD0_FOLLOWER_PUBLIC_IP" "shard0-follower"
wait_for_ssh "$SHARD1_LEADER_PUBLIC_IP" "shard1-leader"
wait_for_ssh "$SHARD1_FOLLOWER_PUBLIC_IP" "shard1-follower"
wait_for_ssh "$CLIENT_PUBLIC_IP" "client"

info "launch complete"
