#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"
load_state

cloudwatch_observability_enabled || die "14-demo-shard-auto-promotion-cloudwatch.sh requires CLOUDWATCH_OBSERVABILITY=1"
dynamodb_control_enabled || die "14-demo-shard-auto-promotion-cloudwatch.sh requires CONTROL_STORE_BACKEND=dynamodb"
dynamodb_session_enabled || die "14-demo-shard-auto-promotion-cloudwatch.sh requires SESSION_STORE_BACKEND=dynamodb"
sqs_ingestion_enabled || die "14-demo-shard-auto-promotion-cloudwatch.sh requires INGESTION_QUEUE_BACKEND=sqs"
hot_standby_ingestion_enabled || die "14-demo-shard-auto-promotion-cloudwatch.sh requires HOT_STANDBY_INGESTION=1"

"$SCRIPT_DIR/12-validate-auto-hot-standby-promotion.sh"

if [[ -n "${CLOUDWATCH_DASHBOARD_NAME:-}" ]]; then
  echo "CloudWatch dashboard: https://${AWS_REGION}.console.aws.amazon.com/cloudwatch/home?region=${AWS_REGION}#dashboards:name=${CLOUDWATCH_DASHBOARD_NAME}"
fi
