#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/phase3-local-common.sh"

reset_state_dirs
build_binaries

start_sharded_topology "${RIPOSTE_PHASE3_SERVER_THREADS:-$RIPOSTE_BENCH_SERVER_THREADS_DEFAULT}"

cat <<EOF
Phase 3 local topology is up.

State dir:      $STATE_DIR
Coordinator:    $COORDINATOR_ADDR
Shard 0 leader: $SHARD0_LEADER_ADDR
Shard 0 peer:   $SHARD0_FOLLOWER_ADDR
Shard 1 leader: $SHARD1_LEADER_ADDR
Shard 1 peer:   $SHARD1_FOLLOWER_ADDR
Server threads: ${RIPOSTE_PHASE3_SERVER_THREADS:-$RIPOSTE_BENCH_SERVER_THREADS_DEFAULT}

Next steps:
  bash script/phase3-local-verify.sh
  bash script/phase3-local-benchmark.sh
  bash script/phase3-local-down.sh
EOF
