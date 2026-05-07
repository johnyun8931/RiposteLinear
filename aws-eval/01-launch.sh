#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

require_cmd aws
require_cmd curl
require_cmd chmod
require_cmd mktemp
validate_public_entry_backend

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

NLB_NAME=""
NLB_ARN=""
NLB_DNS_NAME=""
NLB_TARGET_GROUP_NAME=""
NLB_TARGET_GROUP_ARN=""
NLB_LISTENER_ARN=""

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
SESSION_STORE_BACKEND=$(quote "$SESSION_STORE_BACKEND")
DYNAMODB_SESSION_TABLE=$(quote "$(dynamodb_session_table)")
DYNAMODB_SESSION_REGION=$(quote "$(dynamodb_session_region)")
COORDINATOR_HOLDER_ID=$(quote "$(coordinator_holder_id)")
COORDINATOR_LEASE_TTL_SECONDS=$(quote "$COORDINATOR_LEASE_TTL_SECONDS")
COORDINATOR_LEASE_RENEW_SECONDS=$(quote "$COORDINATOR_LEASE_RENEW_SECONDS")
COORDINATOR_IAM_ROLE_NAME=$(quote "$(coordinator_iam_role_name)")
COORDINATOR_IAM_INSTANCE_PROFILE_NAME=$(quote "$(coordinator_iam_instance_profile_name)")
COORDINATOR_IAM_POLICY_NAME=$(quote "$COORDINATOR_IAM_POLICY_NAME")
PUBLIC_ENTRY_BACKEND=$(quote "$PUBLIC_ENTRY_BACKEND")
NLB_NAME=$(quote "$NLB_NAME")
NLB_ARN=$(quote "$NLB_ARN")
NLB_DNS_NAME=$(quote "$NLB_DNS_NAME")
NLB_TARGET_GROUP_NAME=$(quote "$NLB_TARGET_GROUP_NAME")
NLB_TARGET_GROUP_ARN=$(quote "$NLB_TARGET_GROUP_ARN")
NLB_LISTENER_ARN=$(quote "$NLB_LISTENER_ARN")
EOF_STATE
}

create_coordinator_instance_profile() {
  local account_id role_name profile_name control_policy_arn session_policy_arn trust_doc policy_doc
  account_id="$(aws_base sts get-caller-identity --query Account --output text)"
  role_name="$(coordinator_iam_role_name)"
  profile_name="$(coordinator_iam_instance_profile_name)"
  control_policy_arn="arn:aws:dynamodb:$(dynamodb_control_region):${account_id}:table/$(dynamodb_control_table)"
  session_policy_arn="arn:aws:dynamodb:$(dynamodb_session_region):${account_id}:table/$(dynamodb_session_table)"
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
        "dynamodb:UpdateItem",
        "dynamodb:DeleteItem"
      ],
      "Resource": [
        "$control_policy_arn",
        "$session_policy_arn"
      ]
    }
  ]
}
JSON

  info "creating coordinator IAM role/profile for DynamoDB stores: role=$role_name profile=$profile_name"
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
if dynamodb_runtime_enabled; then
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

if public_entry_enabled; then
  info "authorizing public coordinator RPC ingress for NLB entry on port $COORDINATOR_PORT"
  aws_region ec2 authorize-security-group-ingress \
    --group-id "$SG_ID" \
    --ip-permissions "IpProtocol=tcp,FromPort=${COORDINATOR_PORT},ToPort=${COORDINATOR_PORT},IpRanges=[{CidrIp=0.0.0.0/0,Description='Riposte NLB public coordinator entry'}]"
fi

launch_instance() {
  local name="$1"
  local role="$2"
  local instance_type="$3"
  local instance_profile_name=""
  local attempt out status

  if [[ "$role" == "coordinator" ]] && dynamodb_runtime_enabled; then
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

  for attempt in $(seq 1 12); do
    set +e
    out="$(aws_region ec2 run-instances "${args[@]}" 2>&1)"
    status=$?
    set -e
    if [[ $status -eq 0 ]]; then
      printf '%s\n' "$out"
      return 0
    fi
    if [[ -n "$instance_profile_name" ]] && printf '%s\n' "$out" | grep -qi "Invalid IAM Instance Profile name"; then
      info "waiting for IAM instance profile propagation before launching $name (attempt $attempt/12)"
      sleep 10
      continue
    fi
    printf '%s\n' "$out" >&2
    return "$status"
  done

  printf '%s\n' "$out" >&2
  return "$status"
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

create_public_entry_nlb() {
  local nlb_suffix
  nlb_suffix="$(printf '%s' "$RUN_ID" | tr -cd '[:alnum:]' | tail -c 16)"
  NLB_NAME="${NLB_NAME:-riposte-${nlb_suffix}}"
  NLB_TARGET_GROUP_NAME="${NLB_TARGET_GROUP_NAME:-riposte-tg-${nlb_suffix}}"

  info "creating public Network Load Balancer: $NLB_NAME"
  NLB_ARN="$(aws_region elbv2 create-load-balancer \
    --name "$NLB_NAME" \
    --type network \
    --scheme internet-facing \
    --subnets "$SELECTED_SUBNET_ID" \
    --tags Key=Project,Value="$PROJECT_TAG" Key=RunId,Value="$RUN_ID" \
    --query 'LoadBalancers[0].LoadBalancerArn' \
    --output text)"
  NLB_DNS_NAME="$(aws_region elbv2 describe-load-balancers \
    --load-balancer-arns "$NLB_ARN" \
    --query 'LoadBalancers[0].DNSName' \
    --output text)"
  write_launch_state

  aws_region elbv2 wait load-balancer-available --load-balancer-arns "$NLB_ARN"

  info "creating NLB target group: $NLB_TARGET_GROUP_NAME"
  NLB_TARGET_GROUP_ARN="$(aws_region elbv2 create-target-group \
    --name "$NLB_TARGET_GROUP_NAME" \
    --protocol TCP \
    --port "$COORDINATOR_PORT" \
    --vpc-id "$SELECTED_VPC_ID" \
    --target-type instance \
    --health-check-protocol TCP \
    --tags Key=Project,Value="$PROJECT_TAG" Key=RunId,Value="$RUN_ID" \
    --query 'TargetGroups[0].TargetGroupArn' \
    --output text)"
  write_launch_state

  info "registering coordinator instance with NLB target group"
  aws_region elbv2 register-targets \
    --target-group-arn "$NLB_TARGET_GROUP_ARN" \
    --targets "Id=$COORDINATOR_ID,Port=$COORDINATOR_PORT"

  info "creating NLB listener on TCP port $COORDINATOR_PORT"
  NLB_LISTENER_ARN="$(aws_region elbv2 create-listener \
    --load-balancer-arn "$NLB_ARN" \
    --protocol TCP \
    --port "$COORDINATOR_PORT" \
    --default-actions "Type=forward,TargetGroupArn=$NLB_TARGET_GROUP_ARN" \
    --query 'Listeners[0].ListenerArn' \
    --output text)"
  write_launch_state
}

if public_entry_enabled; then
  create_public_entry_nlb
fi

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
