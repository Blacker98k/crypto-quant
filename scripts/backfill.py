#!/usr/bin/env python3
"""历史数据回填 CLI。

用法
====

.. code-block:: bash

    # 回填 BTCUSDT 和 ETHUSDT 最近 1 年的 1h K 线
    uv run python scripts/backfill.py --symbols BTCUSDT,ETHUSDT --tf 1h --days-back 365

    # 回填指定时间区间
    uv run python scripts/backfill.py --symbols BTCUSDT --tf 1h,4h --since 2024-01-01 --until 2024-06-01

    # 仅 spot（默认），或指定 perp
    uv run python scripts/backfill.py --symbols BTCUSDT --tf 1h --market spot
    uv run python scripts/backfill.py --symbols BTCUSDT --tf 1h --market perp

    # 清除进度后全量重拉
    uv run python scripts/backfill.py --symbols BTCUSDT --tf 1h --no-resume
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sqlite3
import sys
from pathlib import Path

# 确保项目根在 sys.path
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

os.environ.setdefault("PYTHONUNBUFFERED", "1")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="crypto-quant 历史 K 线回填",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--symbols",
        required=True,
        help="回填标的，逗号分隔（如 BTCUSDT,ETHUSDT）",
    )
    p.add_argument(
        "--tf",
        required=True,
        help="时间框架，逗号分隔（如 1h,4h,1d）",
    )
    p.add_argument(
        "--market",
        default="spot",
        choices=["spot", "perp"],
        help="市场类型（默认 spot）",
    )
    p.add_argument(
        "--days-back",
        type=int,
        default=365,
        help="回填天数（默认 365）",
    )
    p.add_argument(
        "--since",
        help="起始日期（YYYY-MM-DD），与 --until 配合使用",
    )
    p.add_argument(
        "--until",
        help="结束日期（YYYY-MM-DD），默认今天",
    )
    p.add_argument(
        "--concurrency",
        type=int,
        default=5,
        help="并发数（默认 5）",
    )
    p.add_argument(
        "--no-resume",
        action="store_true",
        help="不从断点继续，全量重拉",
    )
    p.add_argument(
        "--data-root",
        default="data",
        help="数据根目录（默认 data）",
    )
    p.add_argument(
        "--sqlite-path",
        default="data/crypto.sqlite",
        help="SQLite 数据库路径",
    )
    p.add_argument(
        "--proxy",
        default="",
        help="代理 URL（如 http://127.0.0.1:57777）",
    )
    return p.parse_args(argv)


def resolve_time_range(args: argparse.Namespace) -> tuple[int, int]:
    """将 --since/--until/--days-back 转为 UTC 毫秒区间 (start_ms, end_ms)。"""
    if args.since:
        from core.common.time_utils import iso_to_ms

        start_ms = iso_to_ms(f"{args.since}T00:00:00Z")
        if args.until:
            end_ms = iso_to_ms(f"{args.until}T00:00:00Z")
        else:
            from core.common.clock import SystemClock
            end_ms = SystemClock().now_ms()
        return start_ms, end_ms

    from core.common.clock import SystemClock
    now_ms = SystemClock().now_ms()
    start_ms = now_ms - args.days_back * 24 * 60 * 60 * 1000
    return start_ms, now_ms


async def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    timeframes = [t.strip() for t in args.tf.split(",") if t.strip()]

    if not symbols:
        print("错误: --symbols 不能为空", file=sys.stderr)
        return 1
    if not timeframes:
        print("错误: --tf 不能为空", file=sys.stderr)
        return 1

    start_ms, end_ms = resolve_time_range(args)

    # 初始化依赖
    from core.common.logging import setup_logging
    setup_logging("INFO")

    import logging
    log = logging.getLogger("backfill_cli")

    log.info(
        "backfill_cli_start",
        extra={
            "symbols": symbols,
            "timeframes": timeframes,
            "market": args.market,
            "start_ms": start_ms,
            "end_ms": end_ms,
        },
    )

    # SQLite 连接
    conn = sqlite3.connect(args.sqlite_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row

    # 运行迁移建表（V1 + V2）
    from core.db.migration_runner import MigrationRunner
    runner = MigrationRunner(migrations_dir=Path("migrations"))
    runner.apply_all(conn)

    from core.data.backfill import BackfillJob
    from core.data.parquet_io import ParquetIO
    from core.data.sqlite_repo import SqliteRepo

    parquet_io = ParquetIO(data_root=args.data_root)
    repo = SqliteRepo(conn)

    # Exchange adapter
    if args.market == "perp":
        from core.data.exchange.binance_usdm import BinanceUsdmAdapter
        exchange = BinanceUsdmAdapter(proxy=args.proxy)
    else:
        from core.data.exchange.binance_spot import BinanceSpotAdapter
        exchange = BinanceSpotAdapter(proxy=args.proxy)

    job = BackfillJob(exchange, parquet_io, repo)

    if args.no_resume:
        for sym in symbols:
            for tf in timeframes:
                job.clear_progress(symbol=sym, timeframe=tf)

    results = await job.run(
        symbols=symbols,
        timeframes=timeframes,
        start_ms=start_ms,
        end_ms=end_ms,
        concurrency=args.concurrency,
        resume=not args.no_resume,
    )

    # 打印结果
    ok = sum(1 for r in results if r.complete)
    fail = len(results) - ok
    total_bars = sum(r.bars_written for r in results)

    print(f"\n{'='*60}")
    print(f"  回填完成: {ok} 成功, {fail} 未完成, 共写入 {total_bars} 根 K 线")
    print(f"{'='*60}\n")

    if fail:
        for r in results:
            if not r.complete:
                print(f"  [未完成] {r.symbol} {r.timeframe}: 已写 {r.bars_written} 根")

    await exchange.close()
    conn.close()
    return 0 if fail == 0 else 2


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
