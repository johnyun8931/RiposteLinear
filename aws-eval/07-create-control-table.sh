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
DYNAMODB_SESSION_TABLE="${DYNAMODB_SESSION_TABLE:-$DYNAMODB_CONTROL_TABLE}"

if [[ -f "$STATE_FILE" && -z "${COORDINATOR_IAM_INSTANCE_PROFILE_NAME:-}" ]]; then
  die "DynamoDB smoke requires launching with a DynamoDB-backed coordinator store so the coordinator gets an IAM instance profile. Re-run 01-launch.sh with CONTROL_STORE_BACKEND=dynamodb or SESSION_STORE_BACKEND=dynamodb."
fi

mkdir -p "$STATE_DIR"

ensure_table() {
  local table="$1"
  local label="$2"
  local region="$3"
  if aws_base --region "$region" dynamodb describe-table --table-name "$table" >/dev/null 2>&1; then
    info "DynamoDB $label table already exists: $table"
  else
    info "creating DynamoDB $label table: $table"
    aws_base --region "$region" dynamodb create-table \
      --table-name "$table" \
      --attribute-definitions AttributeName=pk,AttributeType=S \
      --key-schema AttributeName=pk,KeyType=HASH \
      --billing-mode PAY_PER_REQUEST \
      --tags Key=Project,Value="$PROJECT_TAG" >/dev/null
  fi

  info "waiting for $label table to become active"
  aws_base --region "$region" dynamodb wait table-exists --table-name "$table"
}

ensure_table "$DYNAMODB_CONTROL_TABLE" "control" "$(dynamodb_control_region)"
if [[ "$DYNAMODB_SESSION_TABLE" != "$DYNAMODB_CONTROL_TABLE" || "$(dynamodb_session_region)" != "$(dynamodb_control_region)" ]]; then
  ensure_table "$DYNAMODB_SESSION_TABLE" "session" "$(dynamodb_session_region)"
fi

info "seeding shard config version"
aws_base --region "$(dynamodb_control_region)" dynamodb update-item \
  --table-name "$DYNAMODB_CONTROL_TABLE" \
  --key '{"pk":{"S":"shard-config"}}' \
  --update-expression 'SET #version = if_not_exists(#version, :version)' \
  --expression-attribute-names '{"#version":"version"}' \
  --expression-attribute-values '{":version":{"N":"1"}}' >/dev/null

state_tmp="$(mktemp)"
if [[ -f "$STATE_FILE" ]]; then
  grep -v -E '^(CONTROL_STORE_BACKEND|DYNAMODB_CONTROL_TABLE|DYNAMODB_CONTROL_REGION|SESSION_STORE_BACKEND|DYNAMODB_SESSION_TABLE|DYNAMODB_SESSION_REGION)=' "$STATE_FILE" >"$state_tmp" || true
else
  : >"$state_tmp"
fi
{
  cat "$state_tmp"
  echo "CONTROL_STORE_BACKEND=$(quote dynamodb)"
  echo "DYNAMODB_CONTROL_TABLE=$(quote "$DYNAMODB_CONTROL_TABLE")"
  echo "DYNAMODB_CONTROL_REGION=$(quote "$(dynamodb_control_region)")"
  echo "SESSION_STORE_BACKEND=$(quote dynamodb)"
  echo "DYNAMODB_SESSION_TABLE=$(quote "$DYNAMODB_SESSION_TABLE")"
  echo "DYNAMODB_SESSION_REGION=$(quote "$(dynamodb_session_region)")"
} >"$STATE_FILE"
rm -f "$state_tmp"

info "recorded DynamoDB control/session tables in $STATE_FILE"
