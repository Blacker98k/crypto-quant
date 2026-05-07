#!/usr/bin/env python3
"""S1 · BTC/ETH 趋势跟随回测 v2——完整的入场/出场/方向跟踪。

改进：
  - 用全量 Parquet 数据（不限日期）
  - 修复出场逻辑：分别跟踪多头和空头持仓
  - 更准确的信号对齐

用法：
    uv run python research/backtest/backtest_s1.py
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd
import vectorbt as vbt

from core.data.exchange.base import Bar
from core.data.feed import ResearchFeed
from core.data.parquet_io import ParquetIO
from core.data.sqlite_repo import SqliteRepo
from core.db.migration_runner import MigrationRunner
from core.strategy.indicators import (
    compute_atr,
    compute_donchian,
    compute_sma,
)

# ─── 配置 ──────────────────────────────────────────────────────────────────

SYMBOL = "BTCUSDT"
ENTRY_TF = "4h"
TREND_TF = "1d"
DONCHIAN_PERIOD = 15            # 参数扫描最优
ATR_PERIOD = 10
ATR_MIN_MULT = 1.0
VOL_SPIKE_MULT = 1.2
TRAIL_STOP_ATR_MULT = 1.5
TREND_MA_PERIOD = 20
TREND_LONG_MA_PERIOD = 50
INITIAL_CASH = 10_000
RISK_PER_TRADE = 0.01
LOOKBACK_BARS = 500


def load_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    """从 Parquet 加载全量数据。"""
    conn = sqlite3.connect("data/crypto.sqlite")
    conn.row_factory = sqlite3.Row
    MigrationRunner(migrations_dir=Path("migrations")).apply_all(conn)
    repo = SqliteRepo(conn)
    parquet_io = ParquetIO(data_root="data")
    feed = ResearchFeed(parquet_io, repo)

    # 加载 4h 和 1d（全量）
    entry_bars: list[Bar] = feed.get_candles(SYMBOL, ENTRY_TF, n=2000)
    trend_bars: list[Bar] = feed.get_candles(SYMBOL, TREND_TF, n=200)

    def _to_df(bars: list[Bar]) -> pd.DataFrame:
        records = [{"open": b.o, "high": b.h, "low": b.l, "close": b.c,
                     "volume": b.v, "quote_volume": b.q} for b in bars]
        df = pd.DataFrame(records)
        df.index = pd.to_datetime([b.ts for b in bars], unit="ms", utc=True)
        return df

    conn.close()
    return _to_df(entry_bars), _to_df(trend_bars)


def compute_signals_v2(
    entry_df: pd.DataFrame, trend_df: pd.DataFrame,
) -> tuple[pd.Series, pd.Series]:
    """计算入场/出场信号，带多空方向跟踪。"""
    close = entry_df["close"]
    high = entry_df["high"]
    low = entry_df["low"]
    vol = entry_df["quote_volume"]

    # ── 指标 ──────────────────────────────────────────────────────────────
    donchian = compute_donchian(high, low, DONCHIAN_PERIOD)
    donchian_upper = donchian["upper"].shift(1)  # 滞后 1 根
    donchian_lower = donchian["lower"].shift(1)
    atr = compute_atr(high, low, close, ATR_PERIOD)

    # ── 1d 趋势过滤 ───────────────────────────────────────────────────────
    trend_close = trend_df["close"]
    trend_ma20 = compute_sma(trend_close, TREND_MA_PERIOD)
    trend_ma50 = compute_sma(trend_close, TREND_LONG_MA_PERIOD)
    trend_bull = (trend_close > trend_ma50) & (trend_ma20 > trend_ma50)
    trend_bear = (trend_close < trend_ma50) & (trend_ma20 < trend_ma50)

    trend_map = pd.DataFrame({"bull": trend_bull, "bear": trend_bear}, index=trend_df.index)
    trend_4h = trend_map.reindex(entry_df.index, method="ffill")

    # ── 过滤条件 ──────────────────────────────────────────────────────────
    atr_median_30d = atr.rolling(window=120, min_periods=30).median()
    atr_ok = (atr >= atr_median_30d * ATR_MIN_MULT).fillna(True)

    vol_7d_mean = vol.rolling(window=42, min_periods=10).mean()
    vol_ok = (vol >= vol_7d_mean * VOL_SPIKE_MULT).fillna(True)

    # ── 入场信号 ──────────────────────────────────────────────────────────
    entry_long_raw = (
        trend_4h["bull"] &
        (close > donchian_upper) &
        atr_ok & vol_ok
    )

    entry_short_raw = (
        trend_4h["bear"] &
        (close < donchian_lower) &
        atr_ok & vol_ok
    )

    # ── 出场信号（跟踪止损 + MA20 反转）───────────────────────────────────
    ma20 = compute_sma(close, TREND_MA_PERIOD)
    trail_long_stop = close - TRAIL_STOP_ATR_MULT * atr
    trail_short_stop = close + TRAIL_STOP_ATR_MULT * atr

    exit_long_cond = (close < ma20) | (close <= trail_long_stop)
    exit_short_cond = (close > ma20) | (close >= trail_short_stop)

    # ── 状态机：跟踪多头/空头持仓 ─────────────────────────────────────────
    n = len(close)
    entries = pd.Series(False, index=close.index)
    exits = pd.Series(False, index=close.index)
    in_long = False
    in_short = False

    for i in range(1, n):
        if not in_long and not in_short:
            # 无持仓 → 检查入场
            if entry_long_raw.iloc[i]:
                entries.iloc[i] = True
                in_long = True
            elif entry_short_raw.iloc[i]:
                entries.iloc[i] = True
                in_short = True
        elif in_long:
            # 多头持仓 → 检查出场
            if exit_long_cond.iloc[i]:
                exits.iloc[i] = True
                in_long = False
                # 出场后可以立即反向入场
                if entry_short_raw.iloc[i]:
                    entries.iloc[i] = True
                    in_short = True
        elif in_short:
            if exit_short_cond.iloc[i]:
                exits.iloc[i] = True
                in_short = False
                if entry_long_raw.iloc[i]:
                    entries.iloc[i] = True
                    in_long = True

    return entries, exits


def run_backtest_v2() -> None:
    print("=" * 60)
    print("  S1 BTC/ETH 趋势跟随回测 v2")
    print(f"  标的: {SYMBOL} {ENTRY_TF}")
    print(f"  参数: Donchian={DONCHIAN_PERIOD}, ATR={ATR_PERIOD}, Trail={TRAIL_STOP_ATR_MULT}x")
    print("=" * 60)

    # 1. 加载数据
    print("\n[1/4] 加载数据...")
    entry_df, trend_df = load_data()
    print(f"      4h K 线: {len(entry_df)} 根 ({entry_df.index[0]} ~ {entry_df.index[-1]})")
    print(f"      1d K 线: {len(trend_df)} 根")

    if len(entry_df) < 200:
        print("      数据不足，跳过")
        return

    # 2. 计算信号
    print("\n[2/4] 计算信号...")
    entries, exits = compute_signals_v2(entry_df, trend_df)
    print(f"      入场信号: {int(entries.sum())} 次")
    print(f"      出场信号: {int(exits.sum())} 次")

    if entries.sum() == 0:
        print("      无入场信号，跳过")
        return

    # 3. 回测
    print("\n[3/4] 运行 vectorbt 回测...")
    pf = vbt.Portfolio.from_signals(
        entry_df["close"],
        entries=entries,
        exits=exits,
        init_cash=INITIAL_CASH,
        fees=0.0004,  # taker 0.04%
        freq=ENTRY_TF,
    )

    # 4. 结果
    print("\n[4/4] 回测结果")
    print("-" * 40)
    stats = pf.stats()

    for key in [
        "Start Value", "End Value", "Total Return [%]",
        "Sharpe Ratio", "Max Drawdown [%]", "Win Rate [%]",
        "Expectancy", "Total Trades", "Total Fees Paid",
    ]:
        if key in stats.index:
            print(f"  {key:25s}: {stats[key]}")

    equity = pf.value()
    print(f"\n  Start: ${equity.iloc[0]:>10,.2f}")
    print(f"  End:   ${equity.iloc[-1]:>10,.2f}")
    total_return = (equity.iloc[-1] / equity.iloc[0] - 1) * 100
    print(f"  Return: {total_return:.2f}%")

    bnh_return = (entry_df["close"].iloc[-1] / entry_df["close"].iloc[0] - 1) * 100
    print(f"  Buy&Hold: {bnh_return:.2f}%")

    # 月度收益
    rets = pf.returns()
    monthly = rets.resample("ME").apply(lambda x: (1 + x).prod() - 1)
    print(f"\n  月度收益: {monthly.count():.0f} 个月, "
          f"正 {int((monthly > 0).sum())} 月, 负 {int((monthly < 0).sum())} 月")

    # 交易明细
    if pf.trades.count() > 0:
        trades_df = pf.trades.records_readable
        print(f"\n  全部交易 ({len(trades_df)} 笔):")
        # 关键指标
        wins = trades_df[trades_df["PnL"] > 0]
        losses = trades_df[trades_df["PnL"] <= 0]
        print(f"  盈利: {len(wins)} 笔, 亏损: {len(losses)} 笔")
        if len(wins) > 0:
            print(f"  平均盈利: ${wins['PnL'].mean():.2f}")
        if len(losses) > 0:
            print(f"  平均亏损: ${losses['PnL'].mean():.2f}")

        print("\n  最近 10 笔:")
        print(trades_df.tail(10).to_string(index=False))

    # HTML 报告
    report_dir = Path("data/reports")
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / "s1_backtest.html"
    try:
        fig = pf.plot()
        fig.write_html(str(report_path))
        print(f"\n  HTML 报告: {report_path}")
    except Exception as e:
        print(f"  HTML 报告失败: {e}")

    print("=" * 60)
    print("  回测完成。")
    print("=" * 60)


if __name__ == "__main__":
    run_backtest_v2()
