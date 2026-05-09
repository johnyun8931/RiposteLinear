#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/phase3-local-common.sh"

stop_all_phase3_processes

echo "Stopped local Phase 3 processes and removed pid files from $PID_DIR"
