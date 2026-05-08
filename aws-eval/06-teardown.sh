#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"
load_state

require_cmd aws

if [[ -n "${NLB_LISTENER_ARN:-}" ]]; then
  info "deleting NLB listener: $NLB_LISTENER_ARN"
  aws_region elbv2 delete-listener --listener-arn "$NLB_LISTENER_ARN" >/dev/null 2>&1 || true
fi

if [[ -n "${NLB_TARGET_GROUP_ARN:-}" && -n "${COORDINATOR_ID:-}" ]]; then
  info "deregistering coordinator from NLB target group"
  aws_region elbv2 deregister-targets \
    --target-group-arn "$NLB_TARGET_GROUP_ARN" \
    --targets "Id=$COORDINATOR_ID,Port=$COORDINATOR_PORT" >/dev/null 2>&1 || true
  if [[ -n "${COORDINATOR_STANDBY_PORT:-}" ]]; then
    aws_region elbv2 deregister-targets \
      --target-group-arn "$NLB_TARGET_GROUP_ARN" \
      --targets "Id=$COORDINATOR_ID,Port=$COORDINATOR_STANDBY_PORT" >/dev/null 2>&1 || true
  fi
fi

if [[ -n "${NLB_ARN:-}" ]]; then
  info "deleting NLB: $NLB_ARN"
  aws_region elbv2 delete-load-balancer --load-balancer-arn "$NLB_ARN" >/dev/null 2>&1 || true
  aws_region elbv2 wait load-balancers-deleted --load-balancer-arns "$NLB_ARN" >/dev/null 2>&1 || true
fi

if [[ -n "${NLB_TARGET_GROUP_ARN:-}" ]]; then
  info "deleting NLB target group: $NLB_TARGET_GROUP_ARN"
  for _ in $(seq 1 20); do
    if aws_region elbv2 delete-target-group --target-group-arn "$NLB_TARGET_GROUP_ARN" >/dev/null 2>&1; then
      break
    fi
    sleep 5
  done
fi

INSTANCE_IDS=()
for id in \
  "${COORDINATOR_ID:-}" \
  "${SHARD0_LEADER_ID:-}" \
  "${SHARD0_FOLLOWER_ID:-}" \
  "${SHARD1_LEADER_ID:-}" \
  "${SHARD1_FOLLOWER_ID:-}" \
  "${CLIENT_ID:-}"; do
  if [[ -n "$id" ]]; then
    INSTANCE_IDS+=("$id")
  fi
done

if [[ "${#INSTANCE_IDS[@]}" -gt 0 ]]; then
  info "terminating instances: ${INSTANCE_IDS[*]}"
  aws_region ec2 terminate-instances --instance-ids "${INSTANCE_IDS[@]}" >/dev/null || true
  aws_region ec2 wait instance-terminated --instance-ids "${INSTANCE_IDS[@]}" || true
else
  info "no instance IDs found in state"
fi

if [[ -n "${KEY_NAME:-}" ]]; then
  info "deleting AWS key pair: $KEY_NAME"
  aws_region ec2 delete-key-pair --key-name "$KEY_NAME" >/dev/null 2>&1 || true
fi

if [[ -n "${SG_ID:-}" ]]; then
  info "deleting security group: $SG_ID"
  for _ in $(seq 1 20); do
    if aws_region ec2 delete-security-group --group-id "$SG_ID" >/dev/null 2>&1; then
      break
    fi
    sleep 10
  done
fi

if [[ -n "${COORDINATOR_IAM_INSTANCE_PROFILE_NAME:-}" && -n "${COORDINATOR_IAM_ROLE_NAME:-}" ]]; then
  info "deleting coordinator IAM instance profile: $COORDINATOR_IAM_INSTANCE_PROFILE_NAME"
  aws_base iam remove-role-from-instance-profile \
    --instance-profile-name "$COORDINATOR_IAM_INSTANCE_PROFILE_NAME" \
    --role-name "$COORDINATOR_IAM_ROLE_NAME" >/dev/null 2>&1 || true

  for _ in $(seq 1 30); do
    if aws_base iam delete-instance-profile \
      --instance-profile-name "$COORDINATOR_IAM_INSTANCE_PROFILE_NAME" >/dev/null 2>&1; then
      break
    fi
    sleep 2
  done
fi

if [[ -n "${COORDINATOR_IAM_ROLE_NAME:-}" ]]; then
  info "deleting coordinator IAM role: $COORDINATOR_IAM_ROLE_NAME"
  if [[ -n "${COORDINATOR_IAM_POLICY_NAME:-}" ]]; then
    aws_base iam delete-role-policy \
      --role-name "$COORDINATOR_IAM_ROLE_NAME" \
      --policy-name "$COORDINATOR_IAM_POLICY_NAME" >/dev/null 2>&1 || true
  fi

  for _ in $(seq 1 30); do
    if aws_base iam delete-role --role-name "$COORDINATOR_IAM_ROLE_NAME" >/dev/null 2>&1; then
      break
    fi
    sleep 2
  done
fi

remaining_instances="$(aws_region ec2 describe-instances \
  --filters Name=tag:Project,Values="$PROJECT_TAG" Name=instance-state-name,Values=pending,running,stopping,stopped \
  --query 'Reservations[].Instances[].{InstanceId:InstanceId,State:State.Name,Name:Tags[?Key==`Name`]|[0].Value}' \
  --output text)"

if [[ -n "$remaining_instances" ]]; then
  echo "warning: remaining non-terminated instances still have Project=$PROJECT_TAG" >&2
  echo "$remaining_instances" >&2
  echo "run 06-teardown.sh again after AWS finishes detaching resources, or inspect EC2 manually." >&2
  exit 1
fi

echo
echo "teardown complete"
echo "local state, keys, binaries, and copied results remain in ignored paths for audit/debug:"
echo "  $STATE_DIR"
echo "  $KEY_DIR"
echo "  $BIN_DIR"
echo "  $RESULTS_DIR"
