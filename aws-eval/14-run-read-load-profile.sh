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

READ_PROFILE_ID="${READ_PROFILE_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
READ_PROFILE_X="${READ_PROFILE_X:-7}"
READ_PROFILE_Y="${READ_PROFILE_Y:-10}"
READ_PROFILE_BASELINE_CONCURRENCY="${READ_PROFILE_BASELINE_CONCURRENCY:-32}"
READ_PROFILE_SPIKE_CONCURRENCY="${READ_PROFILE_SPIKE_CONCURRENCY:-256}"
READ_PROFILE_BASELINE_SECONDS="${READ_PROFILE_BASELINE_SECONDS:-240}"
READ_PROFILE_SPIKE_SECONDS="${READ_PROFILE_SPIKE_SECONDS:-120}"
READ_PROFILE_COOLDOWN_SECONDS="${READ_PROFILE_COOLDOWN_SECONDS:-240}"
READ_PROFILE_TIMEOUT_MS="${READ_PROFILE_TIMEOUT_MS:-5000}"
READ_PROFILE_URL="http://$READ_ALB_DNS_NAME:$READ_ALB_PORT"
READ_PROFILE_REMOTE_DIR="$REMOTE_ROOT/read-load-profile/$READ_PROFILE_ID"
READ_PROFILE_LOCAL_DIR="$RESULTS_DIR/read-load-profile-$READ_PROFILE_ID"

mkdir -p "$READ_PROFILE_LOCAL_DIR/aws"

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

run_phase() {
  local name="$1"
  local duration="$2"
  local concurrency="$3"
  local remote_phase_dir="$READ_PROFILE_REMOTE_DIR/$name"
  info "read load phase=$name duration=${duration}s concurrency=$concurrency"
  remote_cmd "$CLIENT_PUBLIC_IP" "mkdir -p '$remote_phase_dir'; ~/readload -url '$READ_PROFILE_URL' -x '$READ_PROFILE_X' -y '$READ_PROFILE_Y' -duration-seconds '$duration' -concurrency '$concurrency' -timeout-ms '$READ_PROFILE_TIMEOUT_MS' -output '$remote_phase_dir/summary.json' > '$remote_phase_dir/stdout.log' 2> '$remote_phase_dir/stderr.log'"
}

info "running 10-minute read load profile against $READ_PROFILE_URL"
start_epoch="$(date -u +%s)"
start_iso="$(date -u -d "@$start_epoch" +%Y-%m-%dT%H:%M:%SZ)"
capture_read_alb_artifacts "$READ_PROFILE_LOCAL_DIR/aws/read-alb-before"

run_phase baseline "$READ_PROFILE_BASELINE_SECONDS" "$READ_PROFILE_BASELINE_CONCURRENCY"
run_phase spike "$READ_PROFILE_SPIKE_SECONDS" "$READ_PROFILE_SPIKE_CONCURRENCY"
run_phase cooldown "$READ_PROFILE_COOLDOWN_SECONDS" "$READ_PROFILE_BASELINE_CONCURRENCY"

end_epoch="$(date -u +%s)"
end_iso="$(date -u -d "@$((end_epoch + 300))" +%Y-%m-%dT%H:%M:%SZ)"

copy_from_remote "$CLIENT_PUBLIC_IP" "$READ_PROFILE_REMOTE_DIR" "$READ_PROFILE_LOCAL_DIR"
capture_read_alb_artifacts "$READ_PROFILE_LOCAL_DIR/aws/read-alb-after"

metric_json RequestCount Sum "$READ_PROFILE_LOCAL_DIR/aws/alb-request-count.json" "$start_iso" "$end_iso"
metric_json HTTPCode_Target_2XX_Count Sum "$READ_PROFILE_LOCAL_DIR/aws/alb-target-2xx.json" "$start_iso" "$end_iso"
metric_json HTTPCode_Target_4XX_Count Sum "$READ_PROFILE_LOCAL_DIR/aws/alb-target-4xx.json" "$start_iso" "$end_iso"
metric_json HTTPCode_Target_5XX_Count Sum "$READ_PROFILE_LOCAL_DIR/aws/alb-target-5xx.json" "$start_iso" "$end_iso"
metric_json HTTPCode_ELB_5XX_Count Sum "$READ_PROFILE_LOCAL_DIR/aws/alb-elb-5xx.json" "$start_iso" "$end_iso"
metric_json TargetConnectionErrorCount Sum "$READ_PROFILE_LOCAL_DIR/aws/alb-target-connection-errors.json" "$start_iso" "$end_iso"
metric_json TargetResponseTime Average "$READ_PROFILE_LOCAL_DIR/aws/alb-target-response-time-average.json" "$start_iso" "$end_iso"
metric_extended_json TargetResponseTime p95 "$READ_PROFILE_LOCAL_DIR/aws/alb-target-response-time-p95.json" "$start_iso" "$end_iso"
metric_extended_json TargetResponseTime p99 "$READ_PROFILE_LOCAL_DIR/aws/alb-target-response-time-p99.json" "$start_iso" "$end_iso"
metric_json HealthyHostCount Average "$READ_PROFILE_LOCAL_DIR/aws/alb-healthy-host-count.json" "$start_iso" "$end_iso"
metric_json UnHealthyHostCount Average "$READ_PROFILE_LOCAL_DIR/aws/alb-unhealthy-host-count.json" "$start_iso" "$end_iso"
python3 "$SCRIPT_DIR/render-cloudwatch-graphs.py" \
  --state-file "$STATE_FILE" \
  --result-dir "$READ_PROFILE_LOCAL_DIR" \
  --start "$start_iso" \
  --end "$end_iso" \
  --title-prefix "Read load profile $READ_PROFILE_ID" >/dev/null

python3 - "$READ_PROFILE_LOCAL_DIR/summary.json" "$READ_PROFILE_LOCAL_DIR" "$READ_PROFILE_ID" "$start_iso" "$end_iso" "$(alb_dimension)" "$(target_group_dimension)" <<'PY'
import json
import sys
from pathlib import Path

out_path = Path(sys.argv[1])
root = Path(sys.argv[2])
profile_id = sys.argv[3]
payload = {
    "window_start": sys.argv[4],
    "window_end": sys.argv[5],
    "load_balancer_dimension": sys.argv[6],
    "target_group_dimension": sys.argv[7],
    "phases": {},
    "cloudwatch_files": sorted(p.name for p in (root / "aws").glob("*.json")),
}
for name in ["baseline", "spike", "cooldown"]:
    path = root / profile_id / name / "summary.json"
    if path.exists():
        payload["phases"][name] = json.loads(path.read_text())
out_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
PY

cat <<EOF
read load profile complete
  summary:   $READ_PROFILE_LOCAL_DIR/summary.json
  evidence:  $READ_PROFILE_LOCAL_DIR
  graphs:    $READ_PROFILE_LOCAL_DIR/cloudwatch-graphs
EOF
