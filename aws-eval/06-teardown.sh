#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"
load_state

require_cmd aws

INSTANCE_IDS=()
for id in "${SERVER0_ID:-}" "${SERVER1_ID:-}" "${CLIENT0_ID:-}" "${CLIENT1_ID:-}"; do
  if [[ -n "$id" ]]; then
    INSTANCE_IDS+=("$id")
  fi
done

if [[ "${#INSTANCE_IDS[@]}" -gt 0 ]]; then
  echo "terminating instances: ${INSTANCE_IDS[*]}"
  aws_region ec2 terminate-instances --instance-ids "${INSTANCE_IDS[@]}" >/dev/null || true
  aws_region ec2 wait instance-terminated --instance-ids "${INSTANCE_IDS[@]}" || true
else
  echo "no instance IDs found in state"
fi

echo "deleting AWS key pair: $KEY_NAME"
if [[ -n "${KEY_NAME:-}" ]]; then
  aws_region ec2 delete-key-pair --key-name "$KEY_NAME" >/dev/null 2>&1 || true
fi

echo "deleting security group: $SG_ID"
if [[ -n "${SG_ID:-}" ]]; then
  for _ in $(seq 1 20); do
    if aws_region ec2 delete-security-group --group-id "$SG_ID" >/dev/null 2>&1; then
      break
    fi
    sleep 10
  done
fi

echo "checking for remaining non-terminated project instances"
aws_region ec2 describe-instances \
  --filters Name=tag:Project,Values="$PROJECT_TAG" Name=instance-state-name,Values=pending,running,stopping,stopped \
  --query 'Reservations[].Instances[].{InstanceId:InstanceId,State:State.Name,Name:Tags[?Key==`Name`]|[0].Value}' \
  --output table

echo
echo "teardown complete"
echo "local state/key files were left in ignored paths for audit/debug:"
echo "  $STATE_DIR"
echo "  $KEY_FILE"
