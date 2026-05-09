#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"
load_state
READ_SERVER_DESIRED_CAPACITY="${READ_SERVER_DESIRED_CAPACITY//$'\r'/}"

require_cmd aws
require_cmd curl
require_cmd python3
require_cmd ssh
require_cmd scp

[[ -n "${READ_ALB_DNS_NAME:-}" ]] || die "READ_ALB_DNS_NAME is missing; run 01-launch.sh with the read ALB infrastructure"
[[ -n "${READ_ALB_TARGET_GROUP_ARN:-}" ]] || die "READ_ALB_TARGET_GROUP_ARN is missing from state"
[[ -n "${RESULT_TABLE_S3_BUCKET:-}" ]] || die "RESULT_TABLE_S3_BUCKET is missing from state"
[[ "${READ_SERVER_DESIRED_CAPACITY:-2}" -ge 2 ]] || die "read failover validation requires READ_SERVER_DESIRED_CAPACITY >= 2"

RESULT_ID="${RESULT_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
OUT_DIR="$RESULTS_DIR/read-failover-$RESULT_ID"
REMOTE_FAILOVER_DIR="$REMOTE_ROOT/read-failover"
REMOTE_LOG_DIR="$REMOTE_FAILOVER_DIR/logs"
REMOTE_RESULT_DIR="$REMOTE_FAILOVER_DIR/results"
READ_X="${READ_X:-7}"
READ_Y="${READ_Y:-10}"
READ_PAYLOAD="${READ_PAYLOAD:-read-failover-payload}"
EPOCH_SECONDS="${READ_FAILOVER_EPOCH_SECONDS:-5}"
READ_FAILOVER_LOAD_DURATION_SECONDS="${READ_FAILOVER_LOAD_DURATION_SECONDS:-600}"
READ_FAILOVER_LOAD_CONCURRENCY="${READ_FAILOVER_LOAD_CONCURRENCY:-64}"
READ_FAILOVER_LOAD_TIMEOUT_MS="${READ_FAILOVER_LOAD_TIMEOUT_MS:-5000}"
READ_FAILOVER_LOAD_WARMUP_SECONDS="${READ_FAILOVER_LOAD_WARMUP_SECONDS:-60}"
READ_FAILOVER_LOAD_RECOVERY_SECONDS="${READ_FAILOVER_LOAD_RECOVERY_SECONDS:-60}"
REMOTE_LOAD_DIR="$REMOTE_FAILOVER_DIR/readload-$RESULT_ID"

mkdir -p "$OUT_DIR"
run_start_epoch="$(date -u +%s)"
run_start_iso="$(date -u -d "@$run_start_epoch" +%Y-%m-%dT%H:%M:%SZ)"

read_url() {
  printf 'http://%s:%s/read?x=%s&y=%s' "$READ_ALB_DNS_NAME" "$READ_ALB_PORT" "$READ_X" "$READ_Y"
}

healthy_read_target_count() {
  aws_region elbv2 describe-target-health \
    --target-group-arn "$READ_ALB_TARGET_GROUP_ARN" \
    --query "length(TargetHealthDescriptions[?TargetHealth.State=='healthy'])" \
    --output text
}

first_healthy_read_target() {
  aws_region elbv2 describe-target-health \
    --target-group-arn "$READ_ALB_TARGET_GROUP_ARN" \
    --query "TargetHealthDescriptions[?TargetHealth.State=='healthy'].Target.Id | [0]" \
    --output text
}

wait_for_read_alb_healthy_count() {
  local want="$1"
  local timeout="${2:-240}"
  local deadline=$((SECONDS + timeout))
  local count
  info "waiting for at least $want healthy read ALB targets"
  while true; do
    count="$(healthy_read_target_count 2>/dev/null || echo 0)"
    if [[ "$count" =~ ^[0-9]+$ && "$count" -ge "$want" ]]; then
      info "read ALB has $count healthy targets"
      return 0
    fi
    if (( SECONDS >= deadline )); then
      capture_read_alb_artifacts "$OUT_DIR/read-alb-timeout"
      die "read ALB did not reach $want healthy targets before timeout; last count=${count:-unknown}"
    fi
    sleep 5
  done
}

capture_read_snapshot() {
  local label="$1"
  local dir="$OUT_DIR/$label"
  mkdir -p "$dir"
  capture_read_alb_artifacts "$dir/read-alb"
  aws_region autoscaling describe-auto-scaling-groups \
    --auto-scaling-group-names "$READ_SERVER_ASG_NAME" \
    --output json >"$dir/readserver-asg.json" || true
  aws_region ec2 describe-instances \
    --filters Name=tag:Project,Values="$PROJECT_TAG" Name=tag:Role,Values=readserver \
    --output json >"$dir/readserver-instances.json" || true
  curl -fsS "http://$READ_ALB_DNS_NAME:$READ_ALB_PORT/status" >"$dir/status.json" || true
}

validate_read_response() {
  local response_path="$1"
  python3 - "$response_path" "$READ_X" "$READ_Y" "$READ_PAYLOAD" <<'PY'
import binascii
import json
import sys

path, want_x, want_y, payload = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), sys.argv[4]
data = json.load(open(path))
if data.get("x") != want_x or data.get("y") != want_y:
    raise SystemExit(f"unexpected coordinates: {data}")
message = binascii.unhexlify(data["message_hex"])
if not message.startswith(payload.encode()):
    raise SystemExit(f"message does not start with expected payload; response={data}")
print(json.dumps({
    "server_id": data.get("server_id"),
    "epoch_id": data.get("epoch_id"),
    "shard_id": data.get("shard_id"),
}, sort_keys=True))
PY
}

start_background_read_load() {
  local url load_start_epoch load_start_iso
  url="http://$READ_ALB_DNS_NAME:$READ_ALB_PORT"
  load_start_epoch="$(date -u +%s)"
  load_start_iso="$(date -u -d "@$load_start_epoch" +%Y-%m-%dT%H:%M:%SZ)"
  printf '%s\n' "$load_start_iso" >"$OUT_DIR/readload-start.txt"
  info "starting background read load: duration=${READ_FAILOVER_LOAD_DURATION_SECONDS}s concurrency=$READ_FAILOVER_LOAD_CONCURRENCY"
  remote_cmd "$CLIENT_PUBLIC_IP" "rm -rf '$REMOTE_LOAD_DIR'; mkdir -p '$REMOTE_LOAD_DIR'; nohup ~/readload -url '$url' -x '$READ_X' -y '$READ_Y' -duration-seconds '$READ_FAILOVER_LOAD_DURATION_SECONDS' -concurrency '$READ_FAILOVER_LOAD_CONCURRENCY' -timeout-ms '$READ_FAILOVER_LOAD_TIMEOUT_MS' -output '$REMOTE_LOAD_DIR/summary.json' > '$REMOTE_LOAD_DIR/readload.stdout' 2> '$REMOTE_LOAD_DIR/readload.stderr' & echo \$! > '$REMOTE_LOAD_DIR/readload.pid'"
}

wait_for_background_read_load() {
  local deadline pid status load_end_epoch load_end_iso
  deadline=$((SECONDS + READ_FAILOVER_LOAD_DURATION_SECONDS + 180))
  info "waiting for background read load to finish"
  while true; do
    pid="$(remote_cmd "$CLIENT_PUBLIC_IP" "cat '$REMOTE_LOAD_DIR/readload.pid' 2>/dev/null || true")"
    if [[ -n "${pid//[[:space:]]/}" ]]; then
      status="$(remote_cmd "$CLIENT_PUBLIC_IP" "if ps -p '$pid' >/dev/null 2>&1; then echo running; else echo done; fi")"
      if [[ "$status" == "done" ]]; then
        break
      fi
    fi
    if (( SECONDS >= deadline )); then
      remote_cmd "$CLIENT_PUBLIC_IP" "pid=\$(cat '$REMOTE_LOAD_DIR/readload.pid' 2>/dev/null || true); if [[ -n \"\$pid\" ]]; then kill -TERM \"\$pid\" >/dev/null 2>&1 || true; fi"
      die "background read load did not finish before timeout"
    fi
    sleep 5
  done
  load_end_epoch="$(date -u +%s)"
  load_end_iso="$(date -u -d "@$load_end_epoch" +%Y-%m-%dT%H:%M:%SZ)"
  printf '%s\n' "$load_end_iso" >"$OUT_DIR/readload-end.txt"
  copy_from_remote "$CLIENT_PUBLIC_IP" "$REMOTE_LOAD_DIR" "$OUT_DIR/load"
}

info "starting active write path and publishing a deterministic read table"
kill_all_remote_processes
start_remote_server "$SHARD0_FOLLOWER_PUBLIC_IP" 1 0 "$(server_pair_csv "$SHARD0_LEADER_PRIVATE_IP" "$SHARD0_LEADER_PORT" "$SHARD0_FOLLOWER_PRIVATE_IP" "$SHARD0_FOLLOWER_PORT")" "$REMOTE_RESULT_DIR/shard0" "$REMOTE_LOG_DIR/shard0-follower.log"
start_remote_server "$SHARD0_LEADER_PUBLIC_IP" 0 0 "$(server_pair_csv "$SHARD0_LEADER_PRIVATE_IP" "$SHARD0_LEADER_PORT" "$SHARD0_FOLLOWER_PRIVATE_IP" "$SHARD0_FOLLOWER_PORT")" "$REMOTE_RESULT_DIR/shard0" "$REMOTE_LOG_DIR/shard0-leader.log"
start_remote_server "$SHARD1_FOLLOWER_PUBLIC_IP" 1 1 "$(server_pair_csv "$SHARD1_LEADER_PRIVATE_IP" "$SHARD1_LEADER_PORT" "$SHARD1_FOLLOWER_PRIVATE_IP" "$SHARD1_FOLLOWER_PORT")" "$REMOTE_RESULT_DIR/shard1" "$REMOTE_LOG_DIR/shard1-follower.log"
start_remote_server "$SHARD1_LEADER_PUBLIC_IP" 0 1 "$(server_pair_csv "$SHARD1_LEADER_PRIVATE_IP" "$SHARD1_LEADER_PORT" "$SHARD1_FOLLOWER_PRIVATE_IP" "$SHARD1_FOLLOWER_PORT")" "$REMOTE_RESULT_DIR/shard1" "$REMOTE_LOG_DIR/shard1-leader.log"
start_remote_coordinator "$COORDINATOR_PUBLIC_IP" "$(coordinator_addr)" "$REMOTE_LOG_DIR/coordinator.log"

retry_start_epoch coordinator "$COORDINATOR_PUBLIC_IP" "$(coordinator_addr)" "$EPOCH_SECONDS" >/dev/null
remote_cmd "$CLIENT_PUBLIC_IP" "mkdir -p '$REMOTE_LOG_DIR'; ~/client -coordinator '$(public_coordinator_addr)' -x '$READ_X' -y '$READ_Y' -payload '$READ_PAYLOAD' -log '$REMOTE_LOG_DIR/client-write.log'"
wait_for_epoch_complete coordinator "$COORDINATOR_PUBLIC_IP" "$(coordinator_addr)" 120

wait_for_read_alb_healthy_count "$READ_SERVER_DESIRED_CAPACITY" 300
capture_read_snapshot "before"
curl -fsS "$(read_url)" >"$OUT_DIR/before/read.json"
validate_read_response "$OUT_DIR/before/read.json" >"$OUT_DIR/before/read-summary.json"

start_background_read_load
sleep "$READ_FAILOVER_LOAD_WARMUP_SECONDS"

victim="$(first_healthy_read_target)"
[[ -n "$victim" && "$victim" != "None" ]] || die "could not find a healthy read target to terminate"
printf '%s\n' "$victim" >"$OUT_DIR/terminated-read-instance.txt"

info "terminating read instance $victim to demonstrate ALB failover and ASG replacement"
aws_region ec2 terminate-instances --instance-ids "$victim" >/dev/null
sleep 15
capture_read_snapshot "during"

info "verifying reads continue through remaining read targets"
for attempt in $(seq 1 30); do
  if curl -fsS "$(read_url)" >"$OUT_DIR/during/read-attempt-$attempt.json"; then
    validate_read_response "$OUT_DIR/during/read-attempt-$attempt.json" >"$OUT_DIR/during/read-summary.json"
    break
  fi
  sleep 2
  if [[ "$attempt" -eq 30 ]]; then
    die "read ALB did not serve the known coordinate after terminating $victim"
  fi
done

wait_for_read_alb_healthy_count "$READ_SERVER_DESIRED_CAPACITY" 420
capture_read_snapshot "after"
curl -fsS "$(read_url)" >"$OUT_DIR/after/read.json"
validate_read_response "$OUT_DIR/after/read.json" >"$OUT_DIR/after/read-summary.json"
sleep "$READ_FAILOVER_LOAD_RECOVERY_SECONDS"
wait_for_background_read_load

run_end_epoch="$(date -u +%s)"
run_end_iso="$(date -u -d "@$((run_end_epoch + 300))" +%Y-%m-%dT%H:%M:%SZ)"
python3 - "$OUT_DIR" "$RESULT_ID" "$victim" "$READ_FAILOVER_LOAD_DURATION_SECONDS" "$READ_FAILOVER_LOAD_CONCURRENCY" "$READ_FAILOVER_LOAD_WARMUP_SECONDS" "$READ_FAILOVER_LOAD_RECOVERY_SECONDS" <<'PY'
import json
import pathlib
import sys

out_dir = pathlib.Path(sys.argv[1])
summary = {
    "result_id": sys.argv[2],
    "terminated_instance": sys.argv[3],
    "readload_duration_seconds": int(sys.argv[4]),
    "readload_concurrency": int(sys.argv[5]),
    "readload_warmup_seconds": int(sys.argv[6]),
    "readload_recovery_observation_seconds": int(sys.argv[7]),
}
for name in ("readload-start", "readload-end"):
    path = out_dir / f"{name}.txt"
    if path.exists():
        summary[name.replace("-", "_")] = path.read_text().strip()
load_summary = out_dir / "load" / f"readload-{summary['result_id']}" / "summary.json"
if not load_summary.exists():
    direct = out_dir / "load" / "summary.json"
    if direct.exists():
        load_summary = direct
if not load_summary.exists():
    matches = list((out_dir / "load").glob("*/summary.json"))
    if matches:
        load_summary = matches[0]
if load_summary.exists():
    summary["readload_summary"] = json.loads(load_summary.read_text())
(out_dir / "failover-load-summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
PY
python3 "$SCRIPT_DIR/render-cloudwatch-graphs.py" \
  --state-file "$STATE_FILE" \
  --result-dir "$OUT_DIR" \
  --start "$run_start_iso" \
  --end "$run_end_iso" \
  --title-prefix "Read failover $RESULT_ID" >/dev/null

cat <<EOF
read failover validation complete
  evidence:       $OUT_DIR
  graphs:         $OUT_DIR/cloudwatch-graphs
  read url:       $(read_url)
  killed target:  $victim
EOF
