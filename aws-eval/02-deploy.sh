#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"
load_state

require_cmd go
require_cmd ssh
require_cmd scp

mkdir -p "$BIN_DIR"

echo "building Linux amd64 binaries"
(cd "$REPO_ROOT" && env GOOS=linux GOARCH=amd64 GOCACHE="${GOCACHE:-/tmp/riposte-go-cache-aws-eval}" go build -o "$BIN_DIR/server" ./server)
(cd "$REPO_ROOT" && env GOOS=linux GOARCH=amd64 GOCACHE="${GOCACHE:-/tmp/riposte-go-cache-aws-eval}" go build -o "$BIN_DIR/client" ./client)

echo "copying server binary"
copy_to_remote "$BIN_DIR/server" "$SERVER0_PUBLIC_IP" "~/server"
copy_to_remote "$BIN_DIR/server" "$SERVER1_PUBLIC_IP" "~/server"

echo "copying client binary"
copy_to_remote "$BIN_DIR/client" "$CLIENT0_PUBLIC_IP" "~/client"
copy_to_remote "$BIN_DIR/client" "$CLIENT1_PUBLIC_IP" "~/client"

echo "marking binaries executable"
remote_cmd "$SERVER0_PUBLIC_IP" "chmod +x ~/server"
remote_cmd "$SERVER1_PUBLIC_IP" "chmod +x ~/server"
remote_cmd "$CLIENT0_PUBLIC_IP" "chmod +x ~/client"
remote_cmd "$CLIENT1_PUBLIC_IP" "chmod +x ~/client"

echo "deploy complete"

