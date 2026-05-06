"""技术指标计算——ATR / Donchian 通道 / 移动平均线。

所有函数接受 ``pd.Series`` 或 ``list[Bar]``，返回 ``pd.Series``，
方便同时用于回测（vectorbt）和实盘（on_bar 回调）。

按 ``docs/03-详细设计/strategies/S1-BTC-ETH趋势跟随.md`` §3–§4 实现。
"""

from __future__ import annotations

from typing import Sequence

import numpy as np
import pandas as pd

from core.data.exchange.base import Bar


# ─── 辅助：Bar 列表 → OHLCV DataFrame ───────────────────────────────────────


def bars_to_df(bars: Sequence[Bar]) -> pd.DataFrame:
    """将 Bar 列表转为 OHLCV DataFrame（index = ts）。"""
    records = [
        {
            "open": b.o, "high": b.h, "low": b.l, "close": b.c,
            "volume": b.v, "quote_volume": b.q,
        }
        for b in bars
    ]
    df = pd.DataFrame(records)
    df.index = pd.to_datetime([b.ts for b in bars], unit="ms", utc=True)
    return df


# ─── ATR（Average True Range）───────────────────────────────────────────────


def compute_atr(
    high: pd.Series, low: pd.Series, close: pd.Series,
    period: int = 14,
) -> pd.Series:
    """计算平均真实波幅（ATR），Wilder 平滑法，与 TradingView 一致。

    Args:
        high: 最高价 Series
        low: 最低价 Series
        close: 收盘价 Series
        period: 平滑窗口（默认 14）

    Returns:
        ATR 值 Series，前 period 根为 NaN
    """
    prev_close = close.shift(1)
    tr = pd.concat([
        (high - low).abs(),
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)

    # Wilder 平滑：ATR = (prev_ATR * (period-1) + TR) / period
    atr = tr.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()
    atr.name = f"ATR_{period}"
    return atr


def compute_atr_from_bars(bars: Sequence[Bar], period: int = 14) -> pd.Series:
    """从 Bar 列表直接计算 ATR 的便捷函数。"""
    df = bars_to_df(bars)
    return compute_atr(df["high"], df["low"], df["close"], period)


# ─── Donchian 通道 ──────────────────────────────────────────────────────────


def compute_donchian(
    high: pd.Series, low: pd.Series, period: int = 20,
) -> pd.DataFrame:
    """计算 Donchian 通道。

    Returns:
        DataFrame 含三列：``upper`` / ``lower`` / ``middle``
    """
    upper = high.rolling(window=period, min_periods=period).max()
    lower = low.rolling(window=period, min_periods=period).min()
    middle = (upper + lower) / 2.0
    return pd.DataFrame({
        "upper": upper,
        "lower": lower,
        "middle": middle,
    })


def compute_donchian_from_bars(
    bars: Sequence[Bar], period: int = 20,
) -> pd.DataFrame:
    """从 Bar 列表直接计算 Donchian 通道。"""
    df = bars_to_df(bars)
    return compute_donchian(df["high"], df["low"], period)


# ─── 移动平均线 ──────────────────────────────────────────────────────────────


def compute_sma(close: pd.Series, period: int) -> pd.Series:
    """简单移动平均线。"""
    return close.rolling(window=period, min_periods=period).mean()


def compute_ema(close: pd.Series, period: int) -> pd.Series:
    """指数移动平均线（与 TradingView 一致的 span）。"""
    return close.ewm(span=period, min_periods=period, adjust=False).mean()


# ─── 布林带（用于 S2）────────────────────────────────────────────────────────


def compute_bollinger(
    close: pd.Series, period: int = 20, std_mult: float = 2.0,
) -> pd.DataFrame:
    """计算布林带。

    Returns:
        DataFrame 含三列：``upper`` / ``middle`` / ``lower``
    """
    middle = compute_sma(close, period)
    std = close.rolling(window=period, min_periods=period).std(ddof=0)
    return pd.DataFrame({
        "upper": middle + std_mult * std,
        "middle": middle,
        "lower": middle - std_mult * std,
    })


# ─── RSI（用于 S2）───────────────────────────────────────────────────────────


def compute_rsi(close: pd.Series, period: int = 14) -> pd.Series:
    """计算相对强弱指标（RSI），与 TradingView 一致。

    Uses Wilder 平滑法（与 ATR 一致）。
    """
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()

    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    rsi.name = f"RSI_{period}"
    return rsi


__all__ = [
    "bars_to_df",
    "compute_atr", "compute_atr_from_bars",
    "compute_donchian", "compute_donchian_from_bars",
    "compute_sma", "compute_ema",
    "compute_bollinger",
    "compute_rsi",
]
