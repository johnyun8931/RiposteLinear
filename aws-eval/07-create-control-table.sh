#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"
if [[ -f "$STATE_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$STATE_FILE"
fi

require_cmd aws
require_cmd grep
require_cmd mktemp

DYNAMODB_CONTROL_TABLE="${DYNAMODB_CONTROL_TABLE:-${PROJECT_TAG}-control}"

if [[ -f "$STATE_FILE" && -z "${COORDINATOR_IAM_INSTANCE_PROFILE_NAME:-}" ]]; then
  die "DynamoDB smoke requires launching with CONTROL_STORE_BACKEND=dynamodb so the coordinator gets an IAM instance profile. Re-run 01-launch.sh with CONTROL_STORE_BACKEND=dynamodb."
fi

mkdir -p "$STATE_DIR"

if aws_region dynamodb describe-table --table-name "$DYNAMODB_CONTROL_TABLE" >/dev/null 2>&1; then
  info "DynamoDB control table already exists: $DYNAMODB_CONTROL_TABLE"
else
  info "creating DynamoDB control table: $DYNAMODB_CONTROL_TABLE"
  aws_region dynamodb create-table \
    --table-name "$DYNAMODB_CONTROL_TABLE" \
    --attribute-definitions AttributeName=pk,AttributeType=S \
    --key-schema AttributeName=pk,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST \
    --tags Key=Project,Value="$PROJECT_TAG" >/dev/null
fi

info "waiting for table to become active"
aws_region dynamodb wait table-exists --table-name "$DYNAMODB_CONTROL_TABLE"

info "seeding shard config version"
aws_region dynamodb update-item \
  --table-name "$DYNAMODB_CONTROL_TABLE" \
  --key '{"pk":{"S":"shard-config"}}' \
  --update-expression 'SET #version = if_not_exists(#version, :version)' \
  --expression-attribute-names '{"#version":"version"}' \
  --expression-attribute-values '{":version":{"N":"1"}}' >/dev/null

state_tmp="$(mktemp)"
if [[ -f "$STATE_FILE" ]]; then
  grep -v -E '^(CONTROL_STORE_BACKEND|DYNAMODB_CONTROL_TABLE|DYNAMODB_CONTROL_REGION)=' "$STATE_FILE" >"$state_tmp" || true
else
  : >"$state_tmp"
fi
{
  cat "$state_tmp"
  echo "CONTROL_STORE_BACKEND=$(quote dynamodb)"
  echo "DYNAMODB_CONTROL_TABLE=$(quote "$DYNAMODB_CONTROL_TABLE")"
  echo "DYNAMODB_CONTROL_REGION=$(quote "$AWS_REGION")"
} >"$STATE_FILE"
rm -f "$state_tmp"

info "recorded DynamoDB control table in $STATE_FILE"
