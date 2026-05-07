#!/usr/bin/env python3
"""S2 · 中市值均值回归回测。

先用 BTCUSDT/ETHUSDT 1h 数据验证逻辑，
后续扩展到 top 50 币池。

用法: uv run python research/backtest/backtest_s2.py
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd
import vectorbt as vbt

from core.data.feed import ResearchFeed
from core.data.parquet_io import ParquetIO
from core.data.sqlite_repo import SqliteRepo
from core.db.migration_runner import MigrationRunner
from core.strategy.indicators import (
    compute_bollinger,
    compute_rsi,
    compute_sma,
)

SYMBOLS = ["BTCUSDT", "ETHUSDT"]
ENTRY_TF = "1h"
TREND_TF = "1d"
INITIAL_CASH = 10_000
FEE = 0.0004

RSI_PERIOD = 14
RSI_OVERSOLD = 25
BB_PERIOD = 20
BB_STD = 2.0
MAX_DROP_PCT = 0.05
STOP_LOSS_PCT = 0.05
TAKE_PROFIT_PCT = 0.05
MAX_CONCURRENT = 5
RISK_PER_TRADE = 0.005
MAX_HOLD_BARS = 72  # 72 小时


def load_data(symbol: str):
    conn = sqlite3.connect("data/crypto.sqlite")
    conn.row_factory = sqlite3.Row
    MigrationRunner(migrations_dir=Path("migrations")).apply_all(conn)
    feed = ResearchFeed(ParquetIO(data_root="data"), SqliteRepo(conn))

    bars = feed.get_candles(symbol, ENTRY_TF, n=2000)
    trend_bars = feed.get_candles(symbol, TREND_TF, n=200)
    conn.close()

    records = [{"open": b.o, "high": b.h, "low": b.l, "close": b.c, "volume": b.v, "quote_volume": b.q} for b in bars]
    df = pd.DataFrame(records)
    df.index = pd.to_datetime([b.ts for b in bars], unit="ms", utc=True)

    trend_records = [{"close": b.c} for b in trend_bars]
    tdf = pd.DataFrame(trend_records)
    tdf.index = pd.to_datetime([b.ts for b in trend_bars], unit="ms", utc=True)
    return df, tdf


def compute_signals(entry_df, trend_df):
    close = entry_df["close"]

    rsi = compute_rsi(close, RSI_PERIOD)
    bb = compute_bollinger(close, BB_PERIOD, BB_STD)

    # 1d 趋势
    tc = trend_df["close"]
    tma50 = compute_sma(tc, 50)
    tma20 = compute_sma(tc, 20)
    bull = (tc > tma50) & (tma20 > tma50)
    tm = pd.DataFrame({"b": bull}, index=trend_df.index)
    t1h = tm.reindex(entry_df.index, method="ffill")

    # 跌幅
    prev_close = close.shift(1)
    drop = (prev_close - close) / prev_close

    # 入场
    entry_long = (
        t1h["b"]
        & (rsi < RSI_OVERSOLD)
        & (close <= bb["lower"])
        & (drop <= MAX_DROP_PCT)
    )

    # 出场
    entry_price_arr = np.full(len(close), np.nan)
    stop_price_arr = np.full(len(close), np.nan)
    entry_bar_arr = np.full(len(close), -1)
    active_count = 0

    entries = pd.Series(False, index=close.index)
    exits = pd.Series(False, index=close.index)
    in_position = False

    for i in range(1, len(close)):
        if not in_position:
            if entry_long.iloc[i] and active_count < MAX_CONCURRENT:
                entries.iloc[i] = True
                entry_price_arr[i] = close.iloc[i]
                stop_price_arr[i] = close.iloc[i] * (1 - STOP_LOSS_PCT)
                entry_bar_arr[i] = i
                in_position = True
                active_count += 1
        else:
            ep = entry_price_arr[i-1] if not np.isnan(entry_price_arr[i-1]) else close.iloc[i]
            sp = stop_price_arr[i-1] if not np.isnan(stop_price_arr[i-1]) else ep * (1 - STOP_LOSS_PCT)
            eb = entry_bar_arr[i-1]
            gain = (close.iloc[i] - ep) / ep
            bars_held = i - eb

            # 出场条件
            exit_now = False
            if close.iloc[i] <= sp:
                exit_now = True  # 止损
            elif close.iloc[i] >= bb["middle"].iloc[i]:
                exit_now = True  # 回归中轨
            elif gain >= TAKE_PROFIT_PCT:
                exit_now = True  # 止盈
            elif bars_held >= MAX_HOLD_BARS:
                exit_now = True  # 时间止损

            if exit_now:
                exits.iloc[i] = True
                in_position = False
                active_count = max(0, active_count - 1)

    return entries, exits


def run_backtest():
    print("=" * 60)
    print("  S2 中市值均值回归回测")
    print("=" * 60)

    all_trades = []

    for symbol in SYMBOLS:
        print(f"\n[{symbol}]")
        entry_df, trend_df = load_data(symbol)
        print(f"  数据: {len(entry_df)} 根 1h K 线")

        entries, exits = compute_signals(entry_df, trend_df)
        n_entries = int(entries.sum())
        print(f"  入场信号: {n_entries} 次")

        if n_entries == 0:
            print("  无信号，跳过")
            continue

        pf = vbt.Portfolio.from_signals(
            entry_df["close"],
            entries=entries, exits=exits,
            init_cash=INITIAL_CASH / len(SYMBOLS),
            fees=FEE, freq=ENTRY_TF,
        )

        stats = pf.stats()
        print(f"  收益: {stats.get('Total Return [%]', 0):.2f}%")
        print(f"  Sharpe: {stats.get('Sharpe Ratio', 0):.2f}")
        print(f"  最大回撤: {stats.get('Max Drawdown [%]', 0):.2f}%")
        print(f"  交易次数: {int(stats.get('Total Trades', 0))}")
        print(f"  胜率: {stats.get('Win Rate [%]', 0):.1f}%")

        if pf.trades.count() > 0:
            td = pf.trades.records_readable
            wins = td[td["PnL"] > 0]
            losses = td[td["PnL"] <= 0]
            print(f"  盈利 {len(wins)} / 亏损 {len(losses)}")
            if len(wins) > 0:
                print(f"  平均盈利: ${wins['PnL'].mean():.2f}")
            if len(losses) > 0:
                print(f"  平均亏损: ${losses['PnL'].mean():.2f}")
            all_trades.append(td)

    # 合并组合
    if len(all_trades) >= 2:
        combined = pd.concat(all_trades, ignore_index=True)
        print(f"\n{'='*60}")
        print(f"  组合总计: {len(combined)} 笔交易")
        wins = combined[combined["PnL"] > 0]
        losses = combined[combined["PnL"] <= 0]
        print(f"  盈利 {len(wins)} / 亏损 {len(losses)}")
        if len(wins) > 0:
            print(f"  平均盈利: ${wins['PnL'].mean():.2f}, "
                  f"总盈利: ${wins['PnL'].sum():.2f}")
        if len(losses) > 0:
            print(f"  平均亏损: ${losses['PnL'].mean():.2f}, "
                  f"总亏损: ${losses['PnL'].sum():.2f}")
        print(f"  净利: ${combined['PnL'].sum():.2f}")

    print(f"\n{'='*60}")
    print("  回测完成。")
    print(f"{'='*60}")


if __name__ == "__main__":
    run_backtest()
