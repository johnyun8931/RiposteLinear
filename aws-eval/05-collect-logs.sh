#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"
load_state

require_cmd scp
require_cmd git
require_cmd python3
require_cmd aws

RESULT_ID="${RESULT_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
OUT_DIR="$RESULTS_DIR/$RESULT_ID"
mkdir -p "$OUT_DIR/remotes"

copy_remote_tree() {
  local label="$1"
  local host="$2"
  local dst="$OUT_DIR/remotes/$label"
  mkdir -p "$dst"
  echo "copying logs from $label"
  if ! copy_from_remote "$host" "/tmp/riposte-eval" "$dst"; then
    echo "warning: failed to copy /tmp/riposte-eval from $label" >&2
  fi
}

copy_remote_tree "server-0" "$SERVER0_PUBLIC_IP"
copy_remote_tree "server-1" "$SERVER1_PUBLIC_IP"
copy_remote_tree "client-0" "$CLIENT0_PUBLIC_IP"
copy_remote_tree "client-1" "$CLIENT1_PUBLIC_IP"

cp "$STATE_FILE" "$OUT_DIR/state-env.sh"

GIT_COMMIT="$(cd "$REPO_ROOT" && git rev-parse HEAD)"
GIT_BRANCH="$(cd "$REPO_ROOT" && git rev-parse --abbrev-ref HEAD)"
AWS_IDENTITY="$(aws_base sts get-caller-identity --output json 2>/dev/null || echo '{}')"

cat > "$OUT_DIR/metadata.json" <<EOF_METADATA
{
  "result_id": "$RESULT_ID",
  "git_commit": "$GIT_COMMIT",
  "git_branch": "$GIT_BRANCH",
  "aws_region": "$AWS_REGION",
  "aws_identity": $AWS_IDENTITY,
  "project_tag": "$PROJECT_TAG",
  "run_id": "$RUN_ID",
  "vpc_id": "$VPC_ID",
  "subnet_id": "$SUBNET_ID",
  "ami_id": "$AMI_ID",
  "instance_type": "$INSTANCE_TYPE",
  "table_rows": 65536,
  "row_bytes": 160,
  "threads": $THREADS,
  "server_list": "$(server_list)",
  "leader": "${SERVER0_PRIVATE_IP}:${SERVER0_PORT}",
  "instances": {
    "server_0": {"id": "$SERVER0_ID", "public_ip": "$SERVER0_PUBLIC_IP", "private_ip": "$SERVER0_PRIVATE_IP"},
    "server_1": {"id": "$SERVER1_ID", "public_ip": "$SERVER1_PUBLIC_IP", "private_ip": "$SERVER1_PRIVATE_IP"},
    "client_0": {"id": "$CLIENT0_ID", "public_ip": "$CLIENT0_PUBLIC_IP", "private_ip": "$CLIENT0_PRIVATE_IP"},
    "client_1": {"id": "$CLIENT1_ID", "public_ip": "$CLIENT1_PUBLIC_IP", "private_ip": "$CLIENT1_PRIVATE_IP"}
  }
}
EOF_METADATA

"$SCRIPT_DIR/parse-throughput.sh" "$OUT_DIR"

echo "results collected in $OUT_DIR"
