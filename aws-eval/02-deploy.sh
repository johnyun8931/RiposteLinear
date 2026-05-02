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

info "building Linux amd64 binaries"
(cd "$REPO_ROOT" && env GOOS=linux GOARCH=amd64 GOCACHE="${GOCACHE:-/tmp/riposte-go-cache-aws-eval}" go build -o "$BIN_DIR/server" ./server)
(cd "$REPO_ROOT" && env GOOS=linux GOARCH=amd64 GOCACHE="${GOCACHE:-/tmp/riposte-go-cache-aws-eval}" go build -o "$BIN_DIR/client" ./client)
(cd "$REPO_ROOT" && env GOOS=linux GOARCH=amd64 GOCACHE="${GOCACHE:-/tmp/riposte-go-cache-aws-eval}" go build -o "$BIN_DIR/coordinator" ./coordinator)

for host in \
  "$COORDINATOR_PUBLIC_IP" \
  "$SHARD0_LEADER_PUBLIC_IP" \
  "$SHARD0_FOLLOWER_PUBLIC_IP" \
  "$SHARD1_LEADER_PUBLIC_IP" \
  "$SHARD1_FOLLOWER_PUBLIC_IP" \
  "$CLIENT_PUBLIC_IP"; do
  prepare_remote_workspace "$host"
done

info "copying server binary to shard nodes"
copy_to_remote "$BIN_DIR/server" "$SHARD0_LEADER_PUBLIC_IP" "~/server"
copy_to_remote "$BIN_DIR/server" "$SHARD0_FOLLOWER_PUBLIC_IP" "~/server"
copy_to_remote "$BIN_DIR/server" "$SHARD1_LEADER_PUBLIC_IP" "~/server"
copy_to_remote "$BIN_DIR/server" "$SHARD1_FOLLOWER_PUBLIC_IP" "~/server"

info "copying coordinator binary"
copy_to_remote "$BIN_DIR/coordinator" "$COORDINATOR_PUBLIC_IP" "~/coordinator"

info "copying client binary"
copy_to_remote "$BIN_DIR/client" "$CLIENT_PUBLIC_IP" "~/client"

remote_cmd "$COORDINATOR_PUBLIC_IP" "chmod +x ~/coordinator"
remote_cmd "$SHARD0_LEADER_PUBLIC_IP" "chmod +x ~/server"
remote_cmd "$SHARD0_FOLLOWER_PUBLIC_IP" "chmod +x ~/server"
remote_cmd "$SHARD1_LEADER_PUBLIC_IP" "chmod +x ~/server"
remote_cmd "$SHARD1_FOLLOWER_PUBLIC_IP" "chmod +x ~/server"
remote_cmd "$CLIENT_PUBLIC_IP" "chmod +x ~/client"

info "deploy complete"
