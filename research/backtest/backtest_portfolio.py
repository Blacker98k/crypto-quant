#!/usr/bin/env python3
"""S1 + S2 组合回测——等权重多币种组合。

从 Parquet 中扫描所有有 1h 数据的币，分别运行 S1 和 S2 回测，
汇总组合表现。

用法: uv run python research/backtest/backtest_portfolio.py
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd

from core.data.exchange.base import Bar
from core.data.feed import ResearchFeed
from core.data.parquet_io import ParquetIO
from core.data.sqlite_repo import SqliteRepo
from core.db.migration_runner import MigrationRunner
from core.strategy.indicators import (
    compute_atr, compute_bollinger, compute_donchian, compute_rsi,
    compute_sma,
)

INITIAL_CASH = 10_000
FEE = 0.0004

# S1 params
S1_SYMBOLS = ["BTCUSDT", "ETHUSDT"]
S1_TF = "4h"
S1_DONCHIAN = 15
S1_ATR = 10
S1_TRAIL = 1.5
S1_TREND_MA = 20
S1_TREND_LONG = 50

# S2 params
S2_TF = "1h"
S2_RSI = 14
S2_RSI_OVERSOLD = 25
S2_BB = 20
S2_STOP = 0.05
S2_MAX_CONCURRENT = 5


def find_available_symbols() -> list[str]:
    """扫描 candles 目录，找出有 1h 数据的币（排除 BTC/ETH 已被 S1 覆盖）。"""
    base = Path("data/candles")
    if not base.exists():
        return []
    symbols = []
    for d in base.iterdir():
        if d.is_dir() and (d / "1h").exists() and list((d / "1h").glob("*.parquet")):
            sym = d.name.upper()
            if sym not in ("BTCUSDT", "ETHUSDT"):
                symbols.append(sym)
    return sorted(symbols)


def load_candles(symbol: str, tf: str, n: int = 1000) -> pd.DataFrame:
    """从 Parquet 加载 K 线数据。"""
    conn = sqlite3.connect("data/crypto.sqlite")
    conn.row_factory = sqlite3.Row
    MigrationRunner(migrations_dir=Path("migrations")).apply_all(conn)
    feed = ResearchFeed(ParquetIO(data_root="data"), SqliteRepo(conn))
    bars = feed.get_candles(symbol, tf, n=n)
    conn.close()
    if not bars:
        return pd.DataFrame()
    records = [{"open": b.o, "high": b.h, "low": b.l, "close": b.c, "volume": b.v, "quote_volume": b.q} for b in bars]
    df = pd.DataFrame(records)
    df.index = pd.to_datetime([b.ts for b in bars], unit="ms", utc=True)
    return df


def backtest_s1_symbol(df: pd.DataFrame, trend_df: pd.DataFrame, cash: float) -> dict:
    """对单个标的运行 S1 回测。"""
    close = df["close"]; high = df["high"]; low = df["low"]; vol = df["quote_volume"]

    donch = compute_donchian(high, low, S1_DONCHIAN)
    donch_upper = donch["upper"].shift(1)
    donch_lower = donch["lower"].shift(1)
    atr = compute_atr(high, low, close, S1_ATR)

    tc = trend_df["close"]
    tma = compute_sma(tc, S1_TREND_MA)
    tml = compute_sma(tc, S1_TREND_LONG)
    bull = (tc > tml) & (tma > tml)
    bear = (tc < tml) & (tma < tml)
    tm = pd.DataFrame({"b": bull, "be": bear}, index=trend_df.index)
    t4h = tm.reindex(df.index, method="ffill")

    atr_med = atr.rolling(120).median()
    atr_ok = (atr >= atr_med).fillna(True)
    vol7 = vol.rolling(42).mean()
    vol_ok = (vol >= vol7 * 1.2).fillna(True)

    el = t4h["b"] & (close > donch_upper) & atr_ok & vol_ok
    es_ = t4h["be"] & (close < donch_lower) & atr_ok & vol_ok
    ma20 = compute_sma(close, S1_TREND_MA)
    tls = close - S1_TRAIL * atr
    tss = close + S1_TRAIL * atr
    xl = (close < ma20) | (close <= tls)
    xs = (close > ma20) | (close >= tss)

    n, entries, exits = len(close), pd.Series(False, index=close.index), pd.Series(False, index=close.index)
    il, iss = False, False
    for i in range(1, n):
        if not il and not iss:
            if el.iloc[i]: entries.iloc[i] = True; il = True
            elif es_.iloc[i]: entries.iloc[i] = True; iss = True
        elif il:
            if xl.iloc[i]: exits.iloc[i] = True; il = False
        elif iss:
            if xs.iloc[i]: exits.iloc[i] = True; iss = False

    import vectorbt as vbt
    if entries.sum() == 0:
        return {"trades": 0, "return_pct": 0, "sharpe": 0, "mdd": 0, "win_rate": 0}

    pf = vbt.Portfolio.from_signals(close, entries=entries, exits=exits, init_cash=cash, fees=FEE, freq=S1_TF)
    stats = pf.stats()
    return {
        "trades": int(stats.get("Total Trades", 0)),
        "return_pct": float(stats.get("Total Return [%]", 0)),
        "sharpe": float(stats.get("Sharpe Ratio", 0)),
        "mdd": float(stats.get("Max Drawdown [%]", 0)),
        "win_rate": float(stats.get("Win Rate [%]", 0)),
    }


def backtest_s2_symbol(df: pd.DataFrame, trend_df: pd.DataFrame, cash: float) -> dict:
    """对单个标的运行 S2 回测（均值回归）。"""
    close = df["close"]
    rsi = compute_rsi(close, S2_RSI)
    bb = compute_bollinger(close, S2_BB, 2.0)

    tc = trend_df["close"]
    tma50 = compute_sma(tc, 50)
    bull = (tc > tma50)
    tm = pd.DataFrame({"b": bull}, index=trend_df.index)
    t1h = tm.reindex(df.index, method="ffill")

    prev_close = close.shift(1)
    drop = (prev_close - close) / prev_close

    entry_long = t1h["b"] & (rsi < S2_RSI_OVERSOLD) & (close <= bb["lower"]) & (drop <= 0.05)

    ep_arr = np.full(len(close), np.nan)
    sp_arr = np.full(len(close), np.nan)
    eb_arr = np.full(len(close), -1)
    entries = pd.Series(False, index=close.index)
    exits = pd.Series(False, index=close.index)
    in_pos = False
    for i in range(1, len(close)):
        if not in_pos:
            if entry_long.iloc[i]:
                entries.iloc[i] = True
                ep_arr[i] = close.iloc[i]
                sp_arr[i] = close.iloc[i] * (1 - S2_STOP)
                eb_arr[i] = i
                in_pos = True
        else:
            ep = ep_arr[i-1] if not np.isnan(ep_arr[i-1]) else close.iloc[i]
            sp = sp_arr[i-1] if not np.isnan(sp_arr[i-1]) else ep * (1 - S2_STOP)
            eb = eb_arr[i-1]
            gain = (close.iloc[i] - ep) / ep
            bars_held = i - eb
            if close.iloc[i] <= sp or close.iloc[i] >= bb["middle"].iloc[i] or gain >= 0.05 or bars_held >= 72:
                exits.iloc[i] = True
                in_pos = False

    import vectorbt as vbt
    if entries.sum() == 0:
        return {"trades": 0, "return_pct": 0, "sharpe": 0, "mdd": 0, "win_rate": 0}

    pf = vbt.Portfolio.from_signals(close, entries=entries, exits=exits, init_cash=cash, fees=FEE, freq=S2_TF)
    stats = pf.stats()
    return {
        "trades": int(stats.get("Total Trades", 0)),
        "return_pct": float(stats.get("Total Return [%]", 0)),
        "sharpe": float(stats.get("Sharpe Ratio", 0)),
        "mdd": float(stats.get("Max Drawdown [%]", 0)),
        "win_rate": float(stats.get("Win Rate [%]", 0)),
    }


def main():
    print("=" * 60)
    print("  S1 + S2 组合回测")
    print("=" * 60)

    # S1: BTC/ETH
    print("\n[S1] BTC/ETH 趋势跟随")
    s1_total_return = 0
    for sym in S1_SYMBOLS:
        df = load_candles(sym, S1_TF, 2000)
        trend_df = load_candles(sym, "1d", 200)
        if df.empty or trend_df.empty:
            print(f"  [{sym}] 无数据")
            continue
        cash = INITIAL_CASH / len(S1_SYMBOLS)
        result = backtest_s1_symbol(df, trend_df, cash)
        ret = result["return_pct"]
        s1_total_return += ret
        print(f"  [{sym}] Trades={result['trades']} Ret={ret:.2f}% Sharpe={result['sharpe']:.2f} MDD={result['mdd']:.2f}% WR={result['win_rate']:.1f}%")

    print(f"  S1 总收益: {s1_total_return:.2f}%")

    # S2: 多币均值回归
    print("\n[S2] 中市值均值回归")
    symbols = find_available_symbols()
    print(f"  可用币数: {len(symbols)}: {', '.join(symbols[:10])}...")

    s2_results = []
    for sym in symbols:
        df = load_candles(sym, S2_TF, 2000)
        trend_df = load_candles(sym, "1d", 200)
        if df.empty or trend_df.empty or len(df) < 500:
            continue
        cash = INITIAL_CASH / len(symbols) * 0.5  # S2 用小仓位
        result = backtest_s2_symbol(df, trend_df, cash)
        if result["trades"] > 0:
            s2_results.append({**result, "symbol": sym})
            print(f"  [{sym}] Trades={result['trades']} Ret={result['return_pct']:.2f}% Sharpe={result['sharpe']:.2f} WR={result['win_rate']:.1f}%")

    if s2_results:
        s2_df = pd.DataFrame(s2_results)
        s2_avg_return = s2_df["return_pct"].mean()
        s2_total_trades = s2_df["trades"].sum()
        s2_winners = (s2_df["return_pct"] > 0).sum()
        print(f"\n  S2 汇总 ({len(s2_results)} 个币有交易):")
        print(f"    总交易: {s2_total_trades} 笔")
        print(f"    盈利币: {s2_winners}/{len(s2_results)}")
        print(f"    平均收益: {s2_avg_return:.2f}%")
        print(f"    总收益: {s2_df['return_pct'].sum():.2f}%")
    else:
        print("  S2: 无交易信号（数据不足）")

    print(f"\n{'=' * 60}")
    print(f"  组合回测完成。")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
