"""P1.8 · vectorbt 回测 demo —— BTCUSDT 4h 双均线策略。

按 ``docs/08-路线图.md`` 里程碑 P1.8：跑通 BTCUSDT 4h 双均线，
输出 equity curve / drawdown / Sharpe / 月度收益分布 / 交易明细。

用法::

    uv run python research/backtest/demo_dual_ma.py

依赖 vectorbt（``uv pip install vectorbt``）。
"""

from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import vectorbt as vbt

from core.data.exchange.base import Bar
from core.data.feed import ResearchFeed
from core.data.parquet_io import ParquetIO
from core.data.sqlite_repo import SqliteRepo
from core.db.migration_runner import MigrationRunner


def generate_ohlcv(n_bars: int = 2000, seed: int = 42) -> list[Bar]:
    """用几何布朗运动生成合成 BTCUSDT 4h 数据。"""
    rng = np.random.default_rng(seed)
    base_ts = pd.Timestamp("2024-01-01", tz="UTC").value // 1_000_000
    interval_ms = 4 * 3600 * 1000  # 4h

    price = 42000.0
    sigma = 0.03
    bars = []

    for i in range(n_bars):
        ret = rng.normal(0, sigma)
        o = price
        c = price * (1 + ret)
        h = max(o, c) * (1 + abs(rng.normal(0, sigma * 0.5)))
        low = min(o, c) * (1 - abs(rng.normal(0, sigma * 0.5)))
        v = rng.lognormal(3, 0.5)
        q = v * c

        bars.append(Bar(
            symbol="BTCUSDT", timeframe="4h",
            ts=base_ts + i * interval_ms,
            o=round(o, 2), h=round(h, 2), l=round(low, 2), c=round(c, 2),
            v=round(v, 4), q=round(q, 2), closed=True,
        ))
        price = c

    return bars


def bars_to_df(bars: list[Bar]) -> pd.DataFrame:
    """将 Bar 列表转为 vectorbt 所需的 OHLCV DataFrame。"""
    records = [
        {
            "open": b.o, "high": b.h, "low": b.l, "close": b.c, "volume": b.v,
            "datetime": pd.Timestamp(b.ts, unit="ms", tz="UTC"),
        }
        for b in bars
    ]
    df = pd.DataFrame(records)
    df = df.set_index("datetime")
    return df


def run_demo() -> None:
    print("=" * 60)
    print("  P1.8 vectorbt 回测 demo — BTCUSDT 4h 双均线")
    print("=" * 60)

    # 1. 生成数据 + 写入 Parquet
    print("\n[1/5] 生成合成 BTCUSDT 4h 数据 (2000 bars) ...")
    bars = generate_ohlcv(2000)

    tmp_dir = tempfile.TemporaryDirectory()
    root = Path(tmp_dir.name)
    parquet_io = ParquetIO(data_root=root)
    parquet_io.write_bars(bars)
    print(f"      写入 {len(bars)} 根 K 线 → {root}")

    # 2. 初始化 SQLite repo + ResearchFeed
    print("\n[2/5] 通过 ResearchFeed 读取 ...")
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    runner = MigrationRunner(migrations_dir=Path("migrations"))
    runner.apply_all(conn)
    sqlite_repo = SqliteRepo(conn)
    feed = ResearchFeed(parquet_io, sqlite_repo)
    loaded = feed.get_candles("BTCUSDT", "4h", n=len(bars))
    print(f"      读取到 {len(loaded)} 根 K 线")

    # 3. 转 DataFrame + 计算双均线
    print("\n[3/5] 计算双均线信号 (fast=20, slow=50) ...")
    df = bars_to_df(loaded)
    fast_ma = vbt.MA.run(df["close"], window=20)
    slow_ma = vbt.MA.run(df["close"], window=50)
    entries = fast_ma.ma_crossed_above(slow_ma)
    exits = fast_ma.ma_crossed_below(slow_ma)
    print(f"      多头入场信号: {entries.sum():.0f} 次, 出场: {exits.sum():.0f} 次")

    # 4. 回测
    print("\n[4/5] 运行 vectorbt 回测 ...")
    pf = vbt.Portfolio.from_signals(
        df["close"],
        entries=entries,
        exits=exits,
        init_cash=10_000,
        fees=0.001,  # 0.1%
        freq="4h",
    )

    # 5. 输出报告
    print("\n[5/5] 回测结果")
    print("-" * 40)
    stats = pf.stats()
    for key in [
        "Total Return [%]", "Sharpe Ratio", "Max Drawdown [%]",
        "Win Rate [%]", "Expectancy", "Total Trades",
    ]:
        if key in stats.index:
            print(f"  {key:25s}: {stats[key]:>12}")

    equity = pf.value()
    print(f"  {'Start':25s}: ${equity.iloc[0]:>12,.2f}")
    print(f"  {'End':25s}: ${equity.iloc[-1]:>12,.2f}")
    print(f"  {'Benchmark Buy&Hold':25s}: ${(df['close'].iloc[-1] / df['close'].iloc[0]) * 10000:>12,.2f}")

    # 月度收益分布
    rets = pf.returns()
    monthly = rets.resample("ME").apply(lambda x: (1 + x).prod() - 1)
    print(f"\n  月度收益: {monthly.count():.0f} 个月, "
          f"正收益 {int((monthly > 0).sum())} 月, "
          f"负收益 {int((monthly < 0).sum())} 月")

    trades_df = pf.trades.records_readable
    print("\n  最近 5 笔交易:")
    if len(trades_df) > 0:
        print(trades_df.tail(5).to_string(index=False))

    print("\n  HTML 报告已就绪（可调用 pf.plot() 生成交互式图表）")
    print("=" * 60)
    print("  P1.8 demo 完成。")
    print("=" * 60)

    tmp_dir.cleanup()


if __name__ == "__main__":
    run_demo()
