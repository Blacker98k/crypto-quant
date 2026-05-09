#!/usr/bin/env bash
# crypto-quant · 停止脚本骨架
#
# Phase 1 起步阶段无后台进程，本脚本仅打印提示。
# P1.3 / P1.9 上线后将按 PID 文件优雅停止对应进程。

set -euo pipefail

export no_proxy="localhost,127.0.0.1,0.0.0.0,::1"
export NO_PROXY="${no_proxy}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${PROJECT_ROOT}"

cat <<'EOF'
[stop.sh] crypto-quant · Phase 1 起步阶段
─────────────────────────────────────────────────
当前无后台进程可停止。

P1.3 / P1.9 上线后，本脚本将按 data/*.pid 优雅停 SIGTERM，
30s 未退则 SIGKILL。
EOF
