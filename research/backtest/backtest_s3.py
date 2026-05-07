#!/usr/bin/env python3
"""S3 · BTC/ETH 配对交易回测。

基于 ETH/BTC 比率 Z-score 均值回归的双腿策略。

用法: uv run python research/backtest/backtest_s3.py
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd

from core.data.feed import ResearchFeed
from core.data.parquet_io import ParquetIO
from core.data.sqlite_repo import SqliteRepo
from core.db.migration_runner import MigrationRunner
from core.strategy.indicators import bars_to_df

# 参数
ZSCORE_ENTRY = 2.0
ZSCORE_EXIT = 0.5
ZSCORE_STOP = 3.0
LOOKBACK = 200
MAX_HOLD_BARS = 84
INITIAL_CASH = 10_000
FEE = 0.0004
LEG_SIZE = 0.1  # 每腿名义价值的比例


def load_data():
    conn = sqlite3.connect("data/crypto.sqlite")
    conn.row_factory = sqlite3.Row
    MigrationRunner(migrations_dir=Path("migrations")).apply_all(conn)
    feed = ResearchFeed(ParquetIO(data_root="data"), SqliteRepo(conn))

    btc = feed.get_candles("BTCUSDT", "4h", n=2000)
    eth = feed.get_candles("ETHUSDT", "4h", n=2000)
    conn.close()

    btc_df = bars_to_df(btc)
    eth_df = bars_to_df(eth)
    return btc_df, eth_df


def run_backtest():
    print("=" * 60)
    print("  S3 BTC/ETH 配对交易回测")
    print(f"  参数: Z_entry={ZSCORE_ENTRY}, Z_exit={ZSCORE_EXIT}, 窗口={LOOKBACK}根")
    print("=" * 60)

    btc_df, eth_df = load_data()

    # 对齐时间轴
    common = btc_df.index.intersection(eth_df.index)
    btc_close = btc_df.loc[common, "close"]
    eth_close = eth_df.loc[common, "close"]
    print(f"  数据: {len(common)} 根 4h K 线")

    # 滚动 Z-score
    ratio = eth_close / btc_close
    ratio_mean = ratio.rolling(LOOKBACK, min_periods=50).mean()
    ratio_std = ratio.rolling(LOOKBACK, min_periods=50).std()
    zscore = (ratio - ratio_mean) / ratio_std

    # 持仓状态机
    n = len(ratio)
    equity = pd.Series(INITIAL_CASH, index=ratio.index)
    entry_z = pd.Series(0.0, index=ratio.index)
    trade_log = []
    in_pos = 0
    e_idx = -1
    prev_equity = INITIAL_CASH

    btc_pos = 0.0  # BTC 持仓数量（正=多, 负=空）
    eth_pos = 0.0  # ETH 持仓数量

    for i in range(LOOKBACK, n):
        z = zscore.iloc[i]
        idx = ratio.index[i]

        # 初始开仓价
        if in_pos == 0:
            if z > ZSCORE_ENTRY:
                # BTC 强 → 空 ETH 多 BTC（卖 ETH 买 BTC）
                # 简化：每腿用 LEG_SIZE * equity
                in_pos = -1
                e_idx = i
                entry_z.iloc[i] = z
                btc_pos_val = INITIAL_CASH * 0.5
                eth_pos_val = -INITIAL_CASH * 0.5
                btc_pos = btc_pos_val / btc_close.iloc[i]
                eth_pos = eth_pos_val / eth_close.iloc[i]
                trade_log.append({
                    "entry_time": idx, "type": "short_eth_long_btc",
                    "entry_z": z, "entry_ratio": ratio.iloc[i],
                    "btc_entry": btc_close.iloc[i], "eth_entry": eth_close.iloc[i],
                })

            elif z < -ZSCORE_ENTRY:
                # ETH 强 → 多 ETH 空 BTC
                in_pos = 1
                e_idx = i
                entry_z.iloc[i] = z
                btc_pos_val = -INITIAL_CASH * 0.5
                eth_pos_val = INITIAL_CASH * 0.5
                btc_pos = btc_pos_val / btc_close.iloc[i]
                eth_pos = eth_pos_val / eth_close.iloc[i]
                trade_log.append({
                    "entry_time": idx, "type": "long_eth_short_btc",
                    "entry_z": z, "entry_ratio": ratio.iloc[i],
                    "btc_entry": btc_close.iloc[i], "eth_entry": eth_close.iloc[i],
                })

        elif in_pos != 0:
            bars_held = i - e_idx
            exit_now = False
            exit_reason = ""

            if abs(z) <= ZSCORE_EXIT:
                exit_now = True
                exit_reason = "mean_return"
            elif abs(z) >= ZSCORE_STOP:
                exit_now = True
                exit_reason = "zscore_stop"
            elif bars_held >= MAX_HOLD_BARS:
                exit_now = True
                exit_reason = "time_stop"

            if exit_now:
                # 计算 PnL
                btc_pnl = btc_pos * (btc_close.iloc[i] - btc_close.iloc[e_idx])
                eth_pnl = eth_pos * (eth_close.iloc[i] - eth_close.iloc[e_idx])
                total_pnl = btc_pnl + eth_pnl
                fees = (abs(btc_pos) * btc_close.iloc[e_idx] +
                        abs(eth_pos) * eth_close.iloc[e_idx]) * FEE * 2  # 开+平

                trade_log[-1].update({
                    "exit_time": idx, "exit_reason": exit_reason,
                    "exit_z": z, "exit_ratio": ratio.iloc[i],
                    "btc_exit": btc_close.iloc[i], "eth_exit": eth_close.iloc[i],
                    "btc_pnl": round(btc_pnl, 2),
                    "eth_pnl": round(eth_pnl, 2),
                    "total_pnl": round(total_pnl - fees, 2),
                    "bars_held": bars_held,
                    "fees": round(fees, 2),
                })

                prev_equity += total_pnl - fees
                in_pos = 0
                btc_pos = 0.0
                eth_pos = 0.0

        # 更新权益
        if in_pos != 0:
            current_btc_pnl = btc_pos * (btc_close.iloc[i] - btc_close.iloc[e_idx])
            current_eth_pnl = eth_pos * (eth_close.iloc[i] - eth_close.iloc[e_idx])
            equity.iloc[i] = prev_equity + current_btc_pnl + current_eth_pnl
        else:
            equity.iloc[i] = prev_equity

    # 结果统计
    completed = [t for t in trade_log if "total_pnl" in t]
    wins = [t for t in completed if t["total_pnl"] > 0]
    losses = [t for t in completed if t["total_pnl"] <= 0]

    print("\n  交易统计:")
    print(f"    总交易: {len(completed)} 笔")
    print(f"    盈利: {len(wins)} 笔 ({len(wins)/len(completed)*100:.0f}% 胜率)" if completed else "    盈利: 0 笔")
    print(f"    亏损: {len(losses)} 笔" if losses else "    亏损: 0 笔")

    if wins:
        avg_win = np.mean([t["total_pnl"] for t in wins])
        print(f"    平均盈利: ${avg_win:.2f}")
    if losses:
        avg_loss = np.mean([t["total_pnl"] for t in losses])
        print(f"    平均亏损: ${avg_loss:.2f}")

    total_pnl = sum(t["total_pnl"] for t in completed)
    total_fees = sum(t["fees"] for t in completed)
    print(f"    净利润: ${total_pnl:.2f}")
    print(f"    总手续费: ${total_fees:.2f}")

    final_equity = equity.iloc[-1]
    total_return = (final_equity / INITIAL_CASH - 1) * 100
    print("\n  资金曲线:")
    print(f"    起始: ${INITIAL_CASH:.0f}")
    print(f"    结束: ${final_equity:.2f}")
    print(f"    收益: {total_return:.2f}%")

    # 最大回撤
    peak = equity.expanding().max()
    dd = (equity - peak) / peak * 100
    max_dd = dd.min()
    print(f"    最大回撤: {max_dd:.2f}%")

    # 最近交易明细
    if completed:
        print("\n  最近 5 笔交易:")
        for t in completed[-5:]:
            print(f"    {t['type'][:20]:20s} | "
                  f"Z {t['entry_z']:+.2f}→{t['exit_z']:+.2f} | "
                  f"{t['bars_held']:2d}根 | "
                  f"PnL ${t['total_pnl']:>8.2f} | {t['exit_reason']}")

    # 对比 Buy&Hold
    btc_bh = (btc_close.iloc[-1] / btc_close.iloc[LOOKBACK] - 1) * 100
    eth_bh = (eth_close.iloc[-1] / eth_close.iloc[LOOKBACK] - 1) * 100
    print("\n  基准 (Buy&Hold):")
    print(f"    BTC: {btc_bh:.2f}%")
    print(f"    ETH: {eth_bh:.2f}%")
    print(f"    等权重: {(btc_bh + eth_bh) / 2:.2f}%")

    # 储存结果
    report_dir = Path("data/reports")
    report_dir.mkdir(parents=True, exist_ok=True)
    (btc_close * 0).to_csv(report_dir / "s3_signals.csv")
    if completed:
        pd.DataFrame(completed).to_csv(report_dir / "s3_trades.csv", index=False)

    print(f"\n{'='*60}")
    print("  回测完成。")
    print(f"{'='*60}")


if __name__ == "__main__":
    run_backtest()
