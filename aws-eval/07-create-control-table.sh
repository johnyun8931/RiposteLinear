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
require_cmd python3

DYNAMODB_CONTROL_TABLE="${DYNAMODB_CONTROL_TABLE:-${PROJECT_TAG}-control}"
DYNAMODB_SESSION_TABLE="${DYNAMODB_SESSION_TABLE:-$DYNAMODB_CONTROL_TABLE}"
ACTIVE_SHARD_COUNT="${ACTIVE_SHARD_COUNT:-2}"
FORCE_SHARD_CONFIG="${FORCE_SHARD_CONFIG:-0}"
FORCE_CONTROL_STATE="${FORCE_CONTROL_STATE:-0}"

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

purge_table_items() {
  local table="$1"
  local region="$2"
  local label="$3"
  local scan_file
  scan_file="$(mktemp)"
  info "purging DynamoDB $label table items: $table"
  aws_base --region "$region" dynamodb scan \
    --table-name "$table" \
    --projection-expression pk \
    --output json >"$scan_file"
  python3 - "$scan_file" <<'PY' | while IFS= read -r pk; do
import json
import sys

with open(sys.argv[1]) as fh:
    data = json.load(fh)
for item in data.get("Items", []):
    value = item.get("pk", {}).get("S")
    if value:
        print(value)
PY
    aws_base --region "$region" dynamodb delete-item \
      --table-name "$table" \
      --key "$(python3 - "$pk" <<'PY'
import json
import sys

print(json.dumps({"pk": {"S": sys.argv[1]}}))
PY
)" >/dev/null
  done
  rm -f "$scan_file"
}

if [[ "$FORCE_CONTROL_STATE" == "1" || "$FORCE_CONTROL_STATE" == "true" ]]; then
  purge_table_items "$DYNAMODB_CONTROL_TABLE" "$(dynamodb_control_region)" "control"
  if [[ "$DYNAMODB_SESSION_TABLE" != "$DYNAMODB_CONTROL_TABLE" || "$(dynamodb_session_region)" != "$(dynamodb_control_region)" ]]; then
    purge_table_items "$DYNAMODB_SESSION_TABLE" "$(dynamodb_session_region)" "session"
  fi
fi

info "seeding shard config"
shard_values="$(mktemp)"
ROWS_PER_SHARD="$ROWS_PER_SHARD" \
ACTIVE_SHARD_COUNT="$ACTIVE_SHARD_COUNT" \
SHARD0_LEADER_ADDR="$(shard0_leader_addr)" \
SHARD0_FOLLOWER_ADDR="$(shard0_follower_addr)" \
SHARD1_LEADER_ADDR="$(shard1_leader_addr)" \
SHARD1_FOLLOWER_ADDR="$(shard1_follower_addr)" \
SHARD0_STANDBY_LEADER_ADDR="$(shard0_standby_leader_addr)" \
SHARD0_STANDBY_FOLLOWER_ADDR="$(shard0_standby_follower_addr)" \
SHARD1_STANDBY_LEADER_ADDR="$(shard1_standby_leader_addr)" \
SHARD1_STANDBY_FOLLOWER_ADDR="$(shard1_standby_follower_addr)" \
HOT_STANDBY_INGESTION="$HOT_STANDBY_INGESTION" \
python3 - "$shard_values" <<'PY'
import json
import os
import sys

rows = int(os.environ["ROWS_PER_SHARD"])
active_count = int(os.environ["ACTIVE_SHARD_COUNT"])
hot_standby = os.environ["HOT_STANDBY_INGESTION"] in ("1", "true")
all_shards = [
    {
        "M": {
            "id": {"N": "0"},
            "start_row": {"N": "0"},
            "end_row": {"N": str(rows)},
            "active_leader_addr": {"S": os.environ["SHARD0_LEADER_ADDR"]},
            "active_follower_addr": {"S": os.environ["SHARD0_FOLLOWER_ADDR"]},
            "has_standby": {"BOOL": hot_standby},
            "standby_leader_addr": {"S": os.environ["SHARD0_STANDBY_LEADER_ADDR"] if hot_standby else ""},
            "standby_follower_addr": {"S": os.environ["SHARD0_STANDBY_FOLLOWER_ADDR"] if hot_standby else ""},
        }
    },
    {
        "M": {
            "id": {"N": "1"},
            "start_row": {"N": str(rows)},
            "end_row": {"N": str(2 * rows)},
            "active_leader_addr": {"S": os.environ["SHARD1_LEADER_ADDR"]},
            "active_follower_addr": {"S": os.environ["SHARD1_FOLLOWER_ADDR"]},
            "has_standby": {"BOOL": hot_standby},
            "standby_leader_addr": {"S": os.environ["SHARD1_STANDBY_LEADER_ADDR"] if hot_standby else ""},
            "standby_follower_addr": {"S": os.environ["SHARD1_STANDBY_FOLLOWER_ADDR"] if hot_standby else ""},
        }
    },
]
if active_count < 1 or active_count > len(all_shards):
    raise SystemExit(f"ACTIVE_SHARD_COUNT must be in [1,{len(all_shards)}], got {active_count}")
shards = all_shards[:active_count]
payload = {
    ":version": {"N": "1"},
    ":shard_count": {"N": str(active_count)},
    ":rows_per_shard": {"N": str(rows)},
    ":global_table_height": {"N": str(active_count * rows)},
    ":shards": {"L": shards},
}
with open(sys.argv[1], "w") as fh:
    json.dump(payload, fh)
PY
if [[ "$FORCE_SHARD_CONFIG" == "1" || "$FORCE_SHARD_CONFIG" == "true" ]]; then
  shard_update_expression='SET #version = :version, shard_count = :shard_count, rows_per_shard = :rows_per_shard, global_table_height = :global_table_height, shards = :shards'
else
  shard_update_expression='SET #version = if_not_exists(#version, :version), shard_count = if_not_exists(shard_count, :shard_count), rows_per_shard = if_not_exists(rows_per_shard, :rows_per_shard), global_table_height = if_not_exists(global_table_height, :global_table_height), shards = if_not_exists(shards, :shards)'
fi
aws_base --region "$(dynamodb_control_region)" dynamodb update-item \
  --table-name "$DYNAMODB_CONTROL_TABLE" \
  --key '{"pk":{"S":"shard-config"}}' \
  --update-expression "$shard_update_expression" \
  --expression-attribute-names '{"#version":"version"}' \
  --expression-attribute-values "file://$shard_values" >/dev/null
rm -f "$shard_values"

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
