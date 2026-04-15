#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"
load_state

require_cmd ssh

PHASE="smoke"
REMOTE_DIR="/tmp/riposte-eval/$PHASE"
SERVERS="$(server_list)"

echo "resetting remote processes"
for host in "$SERVER0_PUBLIC_IP" "$SERVER1_PUBLIC_IP" "$CLIENT0_PUBLIC_IP" "$CLIENT1_PUBLIC_IP"; do
  kill_remote_processes "$host"
done

echo "starting smoke servers"
remote_cmd "$SERVER1_PUBLIC_IP" "rm -rf /tmp/riposte-eval; mkdir -p '$REMOTE_DIR'; nohup ~/server -idx 1 -threads '$THREADS' -log '$REMOTE_DIR/server-1.log' -servers '$SERVERS' > '$REMOTE_DIR/server-1.nohup' 2>&1 &"
sleep 2
remote_cmd "$SERVER0_PUBLIC_IP" "rm -rf /tmp/riposte-eval; mkdir -p '$REMOTE_DIR'; nohup ~/server -idx 0 -threads '$THREADS' -log '$REMOTE_DIR/server-0.log' -servers '$SERVERS' > '$REMOTE_DIR/server-0.nohup' 2>&1 &"
sleep 5

echo "running one non-hammer client request"
remote_cmd "$CLIENT0_PUBLIC_IP" "mkdir -p '$REMOTE_DIR'; ~/client -threads '$THREADS' -log '$REMOTE_DIR/client-0.log' -leader '${SERVER0_PRIVATE_IP}:${SERVER0_PORT}'"

echo "checking remote server processes"
remote_cmd "$SERVER0_PUBLIC_IP" "pgrep server >/dev/null"
remote_cmd "$SERVER1_PUBLIC_IP" "pgrep server >/dev/null"

echo "stopping smoke processes"
for host in "$SERVER0_PUBLIC_IP" "$SERVER1_PUBLIC_IP" "$CLIENT0_PUBLIC_IP" "$CLIENT1_PUBLIC_IP"; do
  kill_remote_processes "$host"
done

echo "smoke passed"
