#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

require_cmd aws
require_cmd curl
require_cmd chmod

if [[ -f "$STATE_FILE" && "${FORCE:-0}" != "1" ]]; then
  die "state already exists at $STATE_FILE. Run 06-teardown.sh first, or set FORCE=1 to overwrite local state."
fi

mkdir -p "$STATE_DIR" "$KEY_DIR"

RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
RESULTS_S3_BUCKET="${RESULTS_S3_BUCKET:-$(default_results_s3_bucket)}"
RESULTS_S3_PREFIX="${RESULTS_S3_PREFIX:-runs/$RUN_ID}"
KEY_NAME="${KEY_NAME:-${PROJECT_TAG}-${RUN_ID}}"
KEY_FILE="${KEY_FILE:-$KEY_DIR/${KEY_NAME}.pem}"
SG_NAME="${SG_NAME:-${PROJECT_TAG}-${RUN_ID}}"
SSH_CIDR="${SSH_CIDR:-$(curl -fsS https://checkip.amazonaws.com | tr -d '[:space:]')/32}"
IAM_ROLE_NAME="${IAM_ROLE_NAME:-${PROJECT_TAG}-${RUN_ID}-role}"
IAM_POLICY_NAME="${IAM_POLICY_NAME:-${PROJECT_TAG}-${RUN_ID}-s3-results}"
INSTANCE_PROFILE_NAME="${INSTANCE_PROFILE_NAME:-${PROJECT_TAG}-${RUN_ID}-profile}"

IFS=$'\t' read -r SELECTED_VPC_ID SELECTED_SUBNET_ID SELECTED_AZ <<<"$(resolve_network_selection)"
AMI_ID="${AMI_ID:-$(aws_region ssm get-parameter --name "$AMI_SSM_PARAM" --query 'Parameter.Value' --output text)}"

SG_ID=""
COORDINATOR_ID=""
SHARD0_LEADER_ID=""
SHARD0_FOLLOWER_ID=""
SHARD1_LEADER_ID=""
SHARD1_FOLLOWER_ID=""
CLIENT_ID=""
IAM_POLICY_ARN=""
RESULTS_S3_BUCKET_CREATED=""

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
RESULTS_S3_BUCKET=$(quote "$RESULTS_S3_BUCKET")
RESULTS_S3_PREFIX=$(quote "$RESULTS_S3_PREFIX")
RESULTS_S3_REGION=$(quote "$RESULTS_S3_REGION")
IAM_ROLE_NAME=$(quote "$IAM_ROLE_NAME")
IAM_POLICY_NAME=$(quote "$IAM_POLICY_NAME")
IAM_POLICY_ARN=$(quote "$IAM_POLICY_ARN")
INSTANCE_PROFILE_NAME=$(quote "$INSTANCE_PROFILE_NAME")
RESULTS_S3_BUCKET_CREATED=$(quote "$RESULTS_S3_BUCKET_CREATED")
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
EOF_STATE
}

create_results_bucket() {
  info "creating S3 results bucket: s3://$RESULTS_S3_BUCKET"
  if [[ "$AWS_REGION" == "us-east-1" ]]; then
    aws_region s3api create-bucket --bucket "$RESULTS_S3_BUCKET" >/dev/null
  else
    aws_region s3api create-bucket \
      --bucket "$RESULTS_S3_BUCKET" \
      --create-bucket-configuration "LocationConstraint=${AWS_REGION}" >/dev/null
  fi

  aws_region s3api put-public-access-block \
    --bucket "$RESULTS_S3_BUCKET" \
    --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
  aws_region s3api put-bucket-tagging \
    --bucket "$RESULTS_S3_BUCKET" \
    --tagging "TagSet=[{Key=Project,Value=${PROJECT_TAG}},{Key=RunId,Value=${RUN_ID}}]"
  RESULTS_S3_BUCKET_CREATED=1
}

create_results_instance_profile() {
  local assume_doc policy_doc
  assume_doc="$(mktemp)"
  policy_doc="$(mktemp)"

  cat >"$assume_doc" <<'JSON'
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
        "s3:GetObject",
        "s3:PutObject"
      ],
      "Resource": "arn:aws:s3:::${RESULTS_S3_BUCKET}/*"
    },
    {
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::${RESULTS_S3_BUCKET}"
    }
  ]
}
JSON

  info "creating IAM role/profile for S3 result access"
  aws_base iam create-role \
    --role-name "$IAM_ROLE_NAME" \
    --assume-role-policy-document "file://${assume_doc}" \
    --tags Key=Project,Value="$PROJECT_TAG" Key=RunId,Value="$RUN_ID" >/dev/null

  IAM_POLICY_ARN="$(aws_base iam create-policy \
    --policy-name "$IAM_POLICY_NAME" \
    --policy-document "file://${policy_doc}" \
    --tags Key=Project,Value="$PROJECT_TAG" Key=RunId,Value="$RUN_ID" \
    --query 'Policy.Arn' \
    --output text)"
  write_launch_state

  aws_base iam attach-role-policy \
    --role-name "$IAM_ROLE_NAME" \
    --policy-arn "$IAM_POLICY_ARN"

  aws_base iam create-instance-profile \
    --instance-profile-name "$INSTANCE_PROFILE_NAME" \
    --tags Key=Project,Value="$PROJECT_TAG" Key=RunId,Value="$RUN_ID" >/dev/null
  write_launch_state

  aws_base iam add-role-to-instance-profile \
    --instance-profile-name "$INSTANCE_PROFILE_NAME" \
    --role-name "$IAM_ROLE_NAME"

  rm -f "$assume_doc" "$policy_doc"

  # IAM instance profiles are eventually consistent; give EC2 a moment to see it.
  sleep 10
}

info "launch run id: $RUN_ID"
info "selected network: vpc=$SELECTED_VPC_ID subnet=$SELECTED_SUBNET_ID az=$SELECTED_AZ"
create_results_bucket
write_launch_state
create_results_instance_profile
write_launch_state
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

  aws_region ec2 run-instances \
    --image-id "$AMI_ID" \
    --instance-type "$instance_type" \
    --key-name "$KEY_NAME" \
    --security-group-ids "$SG_ID" \
    --subnet-id "$SELECTED_SUBNET_ID" \
    --iam-instance-profile "Name=${INSTANCE_PROFILE_NAME}" \
    --instance-initiated-shutdown-behavior terminate \
    --tag-specifications \
      "ResourceType=instance,Tags=[{Key=Project,Value=${PROJECT_TAG}},{Key=RunId,Value=${RUN_ID}},{Key=Name,Value=${name}},{Key=Role,Value=${role}}]" \
      "ResourceType=volume,Tags=[{Key=Project,Value=${PROJECT_TAG}},{Key=RunId,Value=${RUN_ID}},{Key=Name,Value=${name}-root},{Key=Role,Value=${role}}]" \
    --query 'Instances[0].InstanceId' \
    --output text
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
