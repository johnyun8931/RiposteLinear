#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"
load_state

cloudwatch_observability_enabled || die "13-demo-coordinator-failover-cloudwatch.sh requires CLOUDWATCH_OBSERVABILITY=1"
public_entry_multi_coordinator_enabled || die "13-demo-coordinator-failover-cloudwatch.sh requires PUBLIC_ENTRY_BACKEND=nlb and PUBLIC_ENTRY_MULTI_COORDINATOR=1"
dynamodb_control_enabled || die "13-demo-coordinator-failover-cloudwatch.sh requires CONTROL_STORE_BACKEND=dynamodb"
dynamodb_session_enabled || die "13-demo-coordinator-failover-cloudwatch.sh requires SESSION_STORE_BACKEND=dynamodb"

"$SCRIPT_DIR/09-validate-multi-coordinator-ingress.sh"

if [[ -n "${CLOUDWATCH_DASHBOARD_NAME:-}" ]]; then
  echo "CloudWatch dashboard: https://${AWS_REGION}.console.aws.amazon.com/cloudwatch/home?region=${AWS_REGION}#dashboards:name=${CLOUDWATCH_DASHBOARD_NAME}"
fi
