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
KEY_NAME="${KEY_NAME:-${PROJECT_TAG}-${RUN_ID}}"
KEY_FILE="${KEY_FILE:-$KEY_DIR/${KEY_NAME}.pem}"
SG_NAME="${SG_NAME:-${PROJECT_TAG}-${RUN_ID}}"
SSH_CIDR="${SSH_CIDR:-$(curl -fsS https://checkip.amazonaws.com | tr -d '[:space:]')/32}"

AMI_ID="${AMI_ID:-}"
SG_ID="${SG_ID:-}"
SERVER0_ID="${SERVER0_ID:-}"
SERVER1_ID="${SERVER1_ID:-}"
CLIENT0_ID="${CLIENT0_ID:-}"
CLIENT1_ID="${CLIENT1_ID:-}"
SERVER0_PRIVATE_IP="${SERVER0_PRIVATE_IP:-}"
SERVER1_PRIVATE_IP="${SERVER1_PRIVATE_IP:-}"
CLIENT0_PRIVATE_IP="${CLIENT0_PRIVATE_IP:-}"
CLIENT1_PRIVATE_IP="${CLIENT1_PRIVATE_IP:-}"
SERVER0_PUBLIC_IP="${SERVER0_PUBLIC_IP:-}"
SERVER1_PUBLIC_IP="${SERVER1_PUBLIC_IP:-}"
CLIENT0_PUBLIC_IP="${CLIENT0_PUBLIC_IP:-}"
CLIENT1_PUBLIC_IP="${CLIENT1_PUBLIC_IP:-}"

write_launch_state() {
  cat > "$STATE_FILE" <<EOF_STATE
RUN_ID=$(quote "$RUN_ID")
PROJECT_TAG=$(quote "$PROJECT_TAG")
AWS_REGION=$(quote "$AWS_REGION")
VPC_ID=$(quote "$VPC_ID")
SUBNET_ID=$(quote "$SUBNET_ID")
AMI_ID=$(quote "$AMI_ID")
INSTANCE_TYPE=$(quote "$INSTANCE_TYPE")
KEY_NAME=$(quote "$KEY_NAME")
KEY_FILE=$(quote "$KEY_FILE")
SG_ID=$(quote "$SG_ID")
SG_NAME=$(quote "$SG_NAME")
SSH_CIDR=$(quote "$SSH_CIDR")
SERVER0_ID=$(quote "$SERVER0_ID")
SERVER1_ID=$(quote "$SERVER1_ID")
CLIENT0_ID=$(quote "$CLIENT0_ID")
CLIENT1_ID=$(quote "$CLIENT1_ID")
SERVER0_PRIVATE_IP=$(quote "$SERVER0_PRIVATE_IP")
SERVER1_PRIVATE_IP=$(quote "$SERVER1_PRIVATE_IP")
CLIENT0_PRIVATE_IP=$(quote "$CLIENT0_PRIVATE_IP")
CLIENT1_PRIVATE_IP=$(quote "$CLIENT1_PRIVATE_IP")
SERVER0_PUBLIC_IP=$(quote "$SERVER0_PUBLIC_IP")
SERVER1_PUBLIC_IP=$(quote "$SERVER1_PUBLIC_IP")
CLIENT0_PUBLIC_IP=$(quote "$CLIENT0_PUBLIC_IP")
CLIENT1_PUBLIC_IP=$(quote "$CLIENT1_PUBLIC_IP")
SERVER0_PORT=$(quote "$SERVER0_PORT")
SERVER1_PORT=$(quote "$SERVER1_PORT")
THREADS=$(quote "$THREADS")
SSH_USER=$(quote "$SSH_USER")
EOF_STATE
}

echo "launch run id: $RUN_ID"
echo "creating key pair: $KEY_NAME"
aws_region ec2 create-key-pair \
  --key-name "$KEY_NAME" \
  --key-type rsa \
  --key-format pem \
  --tag-specifications "ResourceType=key-pair,Tags=[{Key=Project,Value=${PROJECT_TAG}},{Key=RunId,Value=${RUN_ID}}]" \
  --query 'KeyMaterial' \
  --output text > "$KEY_FILE"
chmod 400 "$KEY_FILE"
write_launch_state

echo "creating security group: $SG_NAME"
SG_ID="$(aws_region ec2 create-security-group \
  --group-name "$SG_NAME" \
  --description "Temporary Riposte evaluation security group ${RUN_ID}" \
  --vpc-id "$VPC_ID" \
  --query 'GroupId' \
  --output text)"

aws_region ec2 create-tags \
  --resources "$SG_ID" \
  --tags Key=Project,Value="$PROJECT_TAG" Key=RunId,Value="$RUN_ID" Key=Name,Value="$SG_NAME"
write_launch_state

echo "authorizing SSH from $SSH_CIDR"
aws_region ec2 authorize-security-group-ingress \
  --group-id "$SG_ID" \
  --ip-permissions "IpProtocol=tcp,FromPort=22,ToPort=22,IpRanges=[{CidrIp=${SSH_CIDR}}]"

echo "authorizing all TCP within $SG_ID"
aws_region ec2 authorize-security-group-ingress \
  --group-id "$SG_ID" \
  --ip-permissions "IpProtocol=tcp,FromPort=0,ToPort=65535,UserIdGroupPairs=[{GroupId=${SG_ID}}]"

AMI_ID="${AMI_ID:-$(aws_region ssm get-parameter --name "$AMI_SSM_PARAM" --query 'Parameter.Value' --output text)}"
echo "using AMI: $AMI_ID"
write_launch_state

launch_instance() {
  local name="$1"
  local role="$2"

  aws_region ec2 run-instances \
    --image-id "$AMI_ID" \
    --instance-type "$INSTANCE_TYPE" \
    --key-name "$KEY_NAME" \
    --security-group-ids "$SG_ID" \
    --subnet-id "$SUBNET_ID" \
    --instance-initiated-shutdown-behavior terminate \
    --tag-specifications \
      "ResourceType=instance,Tags=[{Key=Project,Value=${PROJECT_TAG}},{Key=RunId,Value=${RUN_ID}},{Key=Name,Value=${name}},{Key=Role,Value=${role}}]" \
      "ResourceType=volume,Tags=[{Key=Project,Value=${PROJECT_TAG}},{Key=RunId,Value=${RUN_ID}},{Key=Name,Value=${name}-root},{Key=Role,Value=${role}}]" \
    --query 'Instances[0].InstanceId' \
    --output text
}

echo "launching instances"
SERVER0_ID="$(launch_instance "${PROJECT_TAG}-server-0" server)"
write_launch_state
SERVER1_ID="$(launch_instance "${PROJECT_TAG}-server-1" server)"
write_launch_state
CLIENT0_ID="$(launch_instance "${PROJECT_TAG}-client-0" client)"
write_launch_state
CLIENT1_ID="$(launch_instance "${PROJECT_TAG}-client-1" client)"
write_launch_state

INSTANCE_IDS=("$SERVER0_ID" "$SERVER1_ID" "$CLIENT0_ID" "$CLIENT1_ID")
echo "waiting for instances: ${INSTANCE_IDS[*]}"
aws_region ec2 wait instance-running --instance-ids "${INSTANCE_IDS[@]}"

describe_field() {
  local instance_id="$1"
  local field="$2"
  aws_region ec2 describe-instances \
    --instance-ids "$instance_id" \
    --query "Reservations[0].Instances[0].${field}" \
    --output text
}

SERVER0_PRIVATE_IP="$(describe_field "$SERVER0_ID" PrivateIpAddress)"
SERVER1_PRIVATE_IP="$(describe_field "$SERVER1_ID" PrivateIpAddress)"
CLIENT0_PRIVATE_IP="$(describe_field "$CLIENT0_ID" PrivateIpAddress)"
CLIENT1_PRIVATE_IP="$(describe_field "$CLIENT1_ID" PrivateIpAddress)"

SERVER0_PUBLIC_IP="$(describe_field "$SERVER0_ID" PublicIpAddress)"
SERVER1_PUBLIC_IP="$(describe_field "$SERVER1_ID" PublicIpAddress)"
CLIENT0_PUBLIC_IP="$(describe_field "$CLIENT0_ID" PublicIpAddress)"
CLIENT1_PUBLIC_IP="$(describe_field "$CLIENT1_ID" PublicIpAddress)"

write_launch_state

echo "state written to $STATE_FILE"
echo
cat "$STATE_FILE"

wait_for_ssh "$SERVER0_PUBLIC_IP" "server-0"
wait_for_ssh "$SERVER1_PUBLIC_IP" "server-1"
wait_for_ssh "$CLIENT0_PUBLIC_IP" "client-0"
wait_for_ssh "$CLIENT1_PUBLIC_IP" "client-1"

echo "launch complete"
