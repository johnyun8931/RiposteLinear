#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"
load_state

require_cmd aws
require_cmd python3
require_cmd ssh
require_cmd scp

[[ -n "${READ_ALB_DNS_NAME:-}" ]] || die "READ_ALB_DNS_NAME is missing from state"

READ_LOAD_ID="${READ_LOAD_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
READ_LOAD_X="${READ_LOAD_X:-7}"
READ_LOAD_Y="${READ_LOAD_Y:-10}"
READ_LOAD_DURATION_SECONDS="${READ_LOAD_DURATION_SECONDS:-120}"
READ_LOAD_CONCURRENCY="${READ_LOAD_CONCURRENCY:-128}"
READ_LOAD_TIMEOUT_MS="${READ_LOAD_TIMEOUT_MS:-2000}"
READ_LOAD_REMOTE_DIR="$REMOTE_ROOT/read-load/$READ_LOAD_ID"
READ_LOAD_LOCAL_DIR="$RESULTS_DIR/read-load-$READ_LOAD_ID"
READ_LOAD_URL="http://$READ_ALB_DNS_NAME:$READ_ALB_PORT"

mkdir -p "$READ_LOAD_LOCAL_DIR/aws"

alb_dimension() {
  printf '%s' "$READ_ALB_ARN" | sed 's#.*loadbalancer/##'
}

target_group_dimension() {
  printf '%s' "$READ_ALB_TARGET_GROUP_ARN" | sed 's#.*targetgroup/#targetgroup/#'
}

metric_json() {
  local metric="$1"
  local stat="$2"
  local output="$3"
  local start_iso="$4"
  local end_iso="$5"
  aws_region cloudwatch get-metric-statistics \
    --namespace AWS/ApplicationELB \
    --metric-name "$metric" \
    --statistics "$stat" \
    --period 60 \
    --start-time "$start_iso" \
    --end-time "$end_iso" \
    --dimensions \
      "Name=LoadBalancer,Value=$(alb_dimension)" \
      "Name=TargetGroup,Value=$(target_group_dimension)" \
    --output json >"$output" || true
}

metric_extended_json() {
  local metric="$1"
  local stat="$2"
  local output="$3"
  local start_iso="$4"
  local end_iso="$5"
  aws_region cloudwatch get-metric-statistics \
    --namespace AWS/ApplicationELB \
    --metric-name "$metric" \
    --extended-statistics "$stat" \
    --period 60 \
    --start-time "$start_iso" \
    --end-time "$end_iso" \
    --dimensions \
      "Name=LoadBalancer,Value=$(alb_dimension)" \
      "Name=TargetGroup,Value=$(target_group_dimension)" \
    --output json >"$output" || true
}

info "running read load: url=$READ_LOAD_URL x=$READ_LOAD_X y=$READ_LOAD_Y duration=${READ_LOAD_DURATION_SECONDS}s concurrency=$READ_LOAD_CONCURRENCY"
start_epoch="$(date -u +%s)"
start_iso="$(date -u -d "@$start_epoch" +%Y-%m-%dT%H:%M:%SZ)"

remote_cmd "$CLIENT_PUBLIC_IP" "mkdir -p '$READ_LOAD_REMOTE_DIR'; ~/readload -url '$READ_LOAD_URL' -x '$READ_LOAD_X' -y '$READ_LOAD_Y' -duration-seconds '$READ_LOAD_DURATION_SECONDS' -concurrency '$READ_LOAD_CONCURRENCY' -timeout-ms '$READ_LOAD_TIMEOUT_MS' -output '$READ_LOAD_REMOTE_DIR/summary.json' > '$READ_LOAD_REMOTE_DIR/readload.stdout' 2> '$READ_LOAD_REMOTE_DIR/readload.stderr'"

end_epoch="$(date -u +%s)"
end_iso="$(date -u -d "@$((end_epoch + 180))" +%Y-%m-%dT%H:%M:%SZ)"

copy_from_remote "$CLIENT_PUBLIC_IP" "$READ_LOAD_REMOTE_DIR" "$READ_LOAD_LOCAL_DIR"
capture_read_alb_artifacts "$READ_LOAD_LOCAL_DIR/aws/read-alb"
metric_json RequestCount Sum "$READ_LOAD_LOCAL_DIR/aws/alb-request-count.json" "$start_iso" "$end_iso"
metric_json TargetResponseTime Average "$READ_LOAD_LOCAL_DIR/aws/alb-target-response-time-avg.json" "$start_iso" "$end_iso"
metric_extended_json TargetResponseTime p95 "$READ_LOAD_LOCAL_DIR/aws/alb-target-response-time-p95.json" "$start_iso" "$end_iso"
metric_json HTTPCode_Target_2XX_Count Sum "$READ_LOAD_LOCAL_DIR/aws/alb-target-2xx.json" "$start_iso" "$end_iso"
metric_json HTTPCode_Target_5XX_Count Sum "$READ_LOAD_LOCAL_DIR/aws/alb-target-5xx.json" "$start_iso" "$end_iso"
python3 "$SCRIPT_DIR/render-cloudwatch-graphs.py" \
  --state-file "$STATE_FILE" \
  --result-dir "$READ_LOAD_LOCAL_DIR" \
  --start "$start_iso" \
  --end "$end_iso" \
  --title-prefix "Read load $READ_LOAD_ID" >/dev/null

cat <<EOF
read load complete
  summary:   $READ_LOAD_LOCAL_DIR/$READ_LOAD_ID/summary.json
  evidence:  $READ_LOAD_LOCAL_DIR
  graphs:    $READ_LOAD_LOCAL_DIR/cloudwatch-graphs
EOF
