#!/usr/bin/env bash
# crypto-quant · 启动脚本骨架
#
# Phase 1 起步阶段无后台进程。本脚本现仅做：
#   1. 显式重置 no_proxy（避免本机 Dashboard 被全局 http_proxy 截走，见
#      docs/09-踩坑记录.md "全局代理变量污染本地访问"）
#   2. 输出当前阶段提示
#
# 等 P1.3 (WS 数据采集) / P1.9 (Dashboard) 上线后，本脚本将启动对应进程
# 并落 PID 到 data/。

set -euo pipefail

# ─── 防代理污染 ─────────────────────────────────────────────────────────
export no_proxy="localhost,127.0.0.1,0.0.0.0,::1"
export NO_PROXY="${no_proxy}"

# ─── 切到项目根 ─────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${PROJECT_ROOT}"

cat <<'EOF'
[start.sh] crypto-quant · Phase 1 起步阶段
─────────────────────────────────────────────────
当前无后台进程可启动。

P1.3 上线 WS 数据采集器后，本脚本将启动：
  - data-feed-daemon   （WS 订阅 + parquet 异步落库）

P1.9 上线 Dashboard 后追加：
  - dashboard          （监听 127.0.0.1:8088）

如需开发期跑测试：
  uv sync
  uv run pytest

如需手动回填历史数据（PR-4 之后）：
  uv run python scripts/backfill.py --symbols BTCUSDT --tf 1m --since 2024-01-01
EOF
