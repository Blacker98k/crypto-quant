"""S3 资金费率套利——数据分析和策略逻辑验证。

分析 BTC/ETH 180 天资金费率数据，找出套利机会。
"""

from __future__ import annotations

import asyncio
import time
from pathlib import Path

import numpy as np
import pandas as pd

from core.data.exchange.binance_usdm import BinanceUsdmAdapter
from core.strategy.indicators import compute_sma


def analyze_funding_rates():
    """分析已有资金费率数据。"""
    import sqlite3
    conn = sqlite3.connect("data/crypto.sqlite")
    conn.row_factory = sqlite3.Row

    # 从 run_log 和 orders 不会有资金费率，需要单独分析 API 返回
    # 这里直接用 adapter 重新拉取
    pass


async def fetch_and_analyze():
    print("=" * 60)
    print("  S3 资金费率分析")
    print("=" * 60)

    ex = BinanceUsdmAdapter(proxy="http://127.0.0.1:57777", timeout_ms=30000)

    symbols = ["BTCUSDT", "ETHUSDT"]
    now = int(time.time() * 1000)
    start = now - 180 * 24 * 60 * 60 * 1000  # 180天

    all_data = []

    for sym in symbols:
        rates = await ex.fetch_funding_rates(sym, start, now)
        print(f"\n[{sym}] 共 {len(rates)} 条资金费率数据")

        if not rates:
            continue

        df = pd.DataFrame([
            {"ts": r.ts, "rate": r.rate}
            for r in rates
        ])
        df["datetime"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
        df = df.sort_values("ts")

        # 基础统计
        print(f"  区间: {df['datetime'].min()} ~ {df['datetime'].max()}")
        print(f"  数据点: {len(df)}")
        print(f"  均值: {df['rate'].mean()*100:.4f}%")
        print(f"  中位数: {df['rate'].median()*100:.4f}%")
        print(f"  标准差: {df['rate'].std()*100:.4f}%")
        print(f"  最大值: {df['rate'].max()*100:.4f}%")
        print(f"  最小值: {df['rate'].min()*100:.4f}%")
        print(f"  最新: {df['rate'].iloc[-1]*100:.4f}%")

        # 极端值分析
        for threshold_pct, label in [(0.01, "0.01%"), (0.025, "0.025%"), (0.05, "0.05%"), (0.1, "0.1%")]:
            extreme_positive = (df["rate"] > threshold_pct / 100).sum()
            extreme_negative = (df["rate"] < -threshold_pct / 100).sum()
            print(f"  |费率| > {label}: 正={extreme_positive} 负={extreme_negative}")

        # 连续极端值分析
        df["pos_extreme"] = df["rate"] > 0.01 / 100  # > 0.01%
        df["neg_extreme"] = df["rate"] < -0.01 / 100  # < -0.01%

        # 标记连续序列
        df["pos_streak"] = (df["pos_extreme"] != df["pos_extreme"].shift()).cumsum()
        pos_streaks = df[df["pos_extreme"]].groupby("pos_streak").size()
        if len(pos_streaks) > 0:
            print(f"\n  正极端持续时长:")
            print(f"    最长: {pos_streaks.max()} 个周期 ({pos_streaks.max()*8}h)")
            print(f"    平均: {pos_streaks.mean():.1f} 个周期")
            print(f"    次数: {len(pos_streaks)}")

        df["neg_streak"] = (df["neg_extreme"] != df["neg_extreme"].shift()).cumsum()
        neg_streaks = df[df["neg_extreme"]].groupby("neg_streak").size()
        if len(neg_streaks) > 0:
            print(f"  负极端持续时长:")
            print(f"    最长: {neg_streaks.max()} 个周期 ({neg_streaks.max()*8}h)")
            print(f"    平均: {neg_streaks.mean():.1f} 个周期")
            print(f"    次数: {len(neg_streaks)}")

        # 套利收益模拟（简化）
        print(f"\n  套利收益模拟（当 |费率| > 0.01% 时开仓）:")
        df["position"] = 0
        df.loc[df["rate"] > 0.01/100, "position"] = -1  # 费率正→做空永续
        df.loc[df["rate"] < -0.01/100, "position"] = 1  # 费率负→做多永续
        df["funding_pnl"] = df["position"].shift(1) * df["rate"]  # 收取资金费
        total_funding = df["funding_pnl"].sum()
        trades = (df["position"] != df["position"].shift()).sum()
        print(f"    开仓次数: {trades}")
        print(f"    总资金费收入: {total_funding*100:.2f}%")
        print(f"    年化(180天): {total_funding*100*365/180:.2f}%")

        # 只开在更极端时
        print(f"\n  套利收益模拟（当 |费率| > 0.05% 时开仓）:")
        df["position2"] = 0
        df.loc[df["rate"] > 0.05/100, "position2"] = -1
        df.loc[df["rate"] < -0.05/100, "position2"] = 1
        df["funding_pnl2"] = df["position2"].shift(1) * df["rate"]
        total_funding2 = df["funding_pnl2"].sum()
        trades2 = (df["position2"] != df["position2"].shift()).sum()
        open_count2 = (df["position2"] != 0).sum()
        print(f"    开仓次数: {trades2}, 持仓周期数: {open_count2}")
        print(f"    总资金费收入: {total_funding2*100:.2f}%")
        print(f"    年化(180天): {total_funding2*100*365/180:.2f}%")

        all_data.append(df)

    await ex.close()
    print("\n" + "=" * 60)
    print("  分析完成。")
    print("=" * 60)
    return all_data


if __name__ == "__main__":
    asyncio.run(fetch_and_analyze())
