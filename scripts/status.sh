#!/usr/bin/env bash
# crypto-quant · 状态检查脚本骨架
#
# Phase 1 起步阶段无后台进程；本脚本仅打印项目环境概览。
# P1.3 / P1.9 上线后将检查 PID 存活、健康自检接口等。

set -euo pipefail

export no_proxy="localhost,127.0.0.1,0.0.0.0,::1"
export NO_PROXY="${no_proxy}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${PROJECT_ROOT}"

echo "[status.sh] crypto-quant 环境概览"
echo "─────────────────────────────────────────────────"

# Python / uv
if command -v uv >/dev/null 2>&1; then
  echo "uv:           $(uv --version)"
else
  echo "uv:           (not installed)"
fi
if [[ -f .python-version ]]; then
  echo "py-version:   $(cat .python-version)"
fi

# 仓库
echo "git branch:   $(/usr/bin/git rev-parse --abbrev-ref HEAD 2>/dev/null || echo '(not a git repo)')"
echo "git head:     $(/usr/bin/git rev-parse --short HEAD 2>/dev/null || echo '-')"

# data 目录
if [[ -d data ]]; then
  echo "data dir:     $(du -sh data 2>/dev/null | cut -f1)"
else
  echo "data dir:     (not created yet)"
fi

# 进程
running=$(ls data/*.pid 2>/dev/null | wc -l)
echo "live procs:   ${running}（按 PID 文件计；P1.3 之后才有）"

cat <<'EOF'

P1.3 / P1.9 上线后追加：
  - data-feed-daemon  PID + WS 心跳
  - dashboard         http://127.0.0.1:8089/api/data_health
EOF
