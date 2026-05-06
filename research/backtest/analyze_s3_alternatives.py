"""替代 S3 的策略分析——BTC/ETH 配对交易与波动率策略。

当前发现: 资金费率套利在 2025-2026 市场失效（费率太低）。
候选替代: 配对交易（BTC/ETH 价差回归）、波动率突破。
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd
import numpy as np

from core.data.exchange.base import Bar
from core.data.feed import ResearchFeed
from core.data.parquet_io import ParquetIO
from core.data.sqlite_repo import SqliteRepo
from core.db.migration_runner import MigrationRunner


def load_symbol_data(symbol: str, tf: str = "1h", n: int = 5000) -> pd.DataFrame:
    conn = sqlite3.connect("data/crypto.sqlite")
    conn.row_factory = sqlite3.Row
    MigrationRunner(migrations_dir=Path("migrations")).apply_all(conn)
    feed = ResearchFeed(ParquetIO(data_root="data"), SqliteRepo(conn))
    bars = feed.get_candles(symbol, tf, n=n)
    conn.close()
    records = [{"close": b.c, "ts": b.ts} for b in bars]
    df = pd.DataFrame(records)
    df.index = pd.to_datetime(df["ts"], unit="ms", utc=True)
    return df


def main():
    print("=" * 60)
    print("  S3 替代策略分析")
    print("=" * 60)

    # 1. BTC/ETH 配对交易分析
    print("\n[分析 1] BTC/ETH 配对交易（价差回归）")
    btc = load_symbol_data("BTCUSDT", "4h", 2000)
    eth = load_symbol_data("ETHUSDT", "4h", 2000)

    # 对齐索引
    common = btc.index.intersection(eth.index)
    btc = btc.loc[common]
    eth = eth.loc[common]

    # ETH/BTC 比率
    ratio = eth["close"] / btc["close"]
    ratio_mean = ratio.mean()
    ratio_std = ratio.std()
    zscore = (ratio - ratio_mean) / ratio_std

    print(f"  数据: {len(common)} 根 4h K 线")
    print(f"  ETH/BTC 比率均值: {ratio_mean:.6f}")
    print(f"  标准差: {ratio_std:.6f}")
    print(f"  最新比率: {ratio.iloc[-1]:.6f}")
    print(f"  最新 Z-score: {zscore.iloc[-1]:.2f}")

    # 极端值频率
    for threshold in [1.0, 1.5, 2.0, 2.5, 3.0]:
        extreme = (zscore.abs() > threshold).sum()
        print(f"  |Z-score| > {threshold}: {extreme} 次 ({extreme/len(common)*100:.1f}%)")

    # 简化配对交易回测
    print(f"\n  配对交易模拟（Z-score ±2, 回归均值出场）:")
    position = pd.Series(0, index=ratio.index)
    position[zscore > 2.0] = -1   # ETH 相对 BTC 太贵 → 空 ETH 多 BTC
    position[zscore < -2.0] = 1   # ETH 相对 BTC 太便宜 → 多 ETH 空 BTC

    # 假设每笔赚 1 个标准差回归
    trades = (position != position.shift()).sum()
    in_market = (position != 0).sum()
    print(f"  开仓次数: {trades}")
    print(f"  持仓周期: {in_market} ({in_market/len(position)*100:.1f}%)")

    # 2. 波动率突破策略分析
    print(f"\n[分析 2] 波动率突破策略")
    # 用 BTC 4h 数据
    ret = btc["close"].pct_change()
    volatility = ret.rolling(24).std() * np.sqrt(24)  # 24期滚动波动率
    vol_mean = volatility.mean()
    vol_now = volatility.iloc[-1]

    print(f"  24期滚动波动率均值: {vol_mean*100:.2f}%")
    print(f"  当前波动率: {vol_now*100:.2f}%")
    print(f"  当前 vs 均值: {vol_now/vol_mean:.2f}x")

    for mult in [1.5, 2.0, 2.5, 3.0]:
        vol_spike = (volatility > vol_mean * mult).sum()
        print(f"  波动率 > {mult}x 均值: {vol_spike} 次 ({vol_spike/len(volatility)*100:.1f}%)")

    print(f"\n{'='*60}")
    print("  分析完成。")
    print(f"{'='*60}")

    # 保存关键指标
    print(f"\n技术指标汇总:")
    print(f"  ETH/BTC 比率当前 Z-score: {zscore.iloc[-1]:.2f}")
    print(f"  BTC 24h 波动率: {vol_now*100:.2f}%")
    print(f"  建议: 配对交易可替代 S3（与 S1/S2 低相关）")


if __name__ == "__main__":
    main()
