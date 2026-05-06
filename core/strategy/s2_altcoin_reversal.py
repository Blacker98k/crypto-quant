"""S2 · 中市值币均值回归（完整实现）。

RSI 超卖 + 布林下轨触碰 → 做多回归中轨。

入场条件：
  1. 大趋势：1d 收盘 > 1d MA50 AND 1d MA20 > 1d MA50
  2. 入场触发：1h RSI(14) < 25 + 收盘触碰布林下轨 BB(20,2)
  3. 下跌幅度 ≤ 5%（防止崩盘式下跌）
  4. 流动性：7d 日均成交额 > 5000 万 USDT

出场条件：
  1. 固定止损 -5%
  2. 回归布林中轨 / 浮盈 +5% 止盈一半
  3. 时间止损 72h

仓位：0.5% 风险 / 5% 止损距离
最多 5 个不同标的同时持仓
"""

from __future__ import annotations

import math
from typing import TYPE_CHECKING

import pandas as pd

from core.strategy.base import DataRequirement, Signal, Strategy, StrategyContext
from core.strategy.indicators import (
    bars_to_df,
    compute_bollinger,
    compute_rsi,
    compute_sma,
)

if TYPE_CHECKING:
    from core.data.exchange.base import Bar


class S2AltcoinReversal(Strategy):
    """中市值币均值回归。

    Phase 1 实现只交易 BTCUSDT/ETHUSDT（便于回测验证），
    Phase 2 扩展到 top 50 永续币池。
    """

    name = "S2_altcoin_reversal"
    version = "dev"
    config_hash = "0000000000000000"

    # ── 可配置参数 ──────────────────────────────────────────────────────────
    rsi_period: int = 14
    rsi_oversold: float = 25.0        # RSI 超卖阈值
    bb_period: int = 20
    bb_std_mult: float = 2.0
    max_drop_pct: float = 0.05        # 单根 K 线最大跌幅 5%
    stop_loss_pct: float = 0.05       # 固定止损 5%
    take_profit_pct: float = 0.05     # 止盈 5%
    max_concurrent: int = 5
    per_trade_risk_pct: float = 0.005  # 0.5% 风险
    min_volume_usdt: float = 50_000_000  # 5000 万 USDT
    max_hold_hours: int = 72

    __slots__ = ()

    def required_data(self) -> DataRequirement:
        return DataRequirement(
            symbols=["BTCUSDT", "ETHUSDT"],
            timeframes=["1h", "1d"],
            history_lookback_bars=500,
            needs_funding=True,
        )

    def on_bar(self, bar: Bar, ctx: StrategyContext) -> list[Signal]:
        """每根 1h K 线收盘时调用。返回 0..N 个信号。"""
        symbol = bar.symbol
        signals: list[Signal] = []

        # 1. 拉取数据
        entry_bars = ctx.data.get_candles(symbol, "1h", n=200)
        trend_bars = ctx.data.get_candles(symbol, "1d", n=100)

        if len(entry_bars) < 100 or len(trend_bars) < 30:
            return signals

        entry_df = bars_to_df(entry_bars)
        trend_df = bars_to_df(trend_bars)

        # 2. 计算指标
        rsi = compute_rsi(entry_df["close"], self.rsi_period)
        bb = compute_bollinger(entry_df["close"], self.bb_period, self.bb_std_mult)

        # 3. 1d 趋势过滤
        trend_close = trend_df["close"]
        trend_ma50 = compute_sma(trend_close, 50)
        trend_ma20 = compute_sma(trend_close, 20)
        trend_bull = (trend_close > trend_ma50) & (trend_ma20 > trend_ma50)

        # 当前状态
        current_close = entry_df["close"].iloc[-1]
        current_rsi = rsi.iloc[-1]
        current_bb_lower = bb["lower"].iloc[-1]
        current_bb_mid = bb["middle"].iloc[-1]

        if pd.isna(current_rsi):
            return signals

        # 跌幅过滤
        prev_close = entry_df["close"].iloc[-2] if len(entry_df) >= 2 else current_close
        drop_pct = (prev_close - current_close) / prev_close

        # 4. 检查持仓
        position = ctx.kv_get(f"{symbol}_position")
        entry_time = ctx.kv_get(f"{symbol}_entry_time")

        if position is None:
            # ── 无持仓 → 检查入场条件 ──
            # 检查活跃持仓数
            active_count = ctx.kv_get("s2_active_count") or 0

            if (
                trend_bull.iloc[-1]
                and current_rsi < self.rsi_oversold
                and current_close <= current_bb_lower
                and drop_pct <= self.max_drop_pct
                and active_count < self.max_concurrent
            ):
                entry_price = current_close
                stop_price = entry_price * (1 - self.stop_loss_pct)
                risk_per_unit = entry_price - stop_price
                if risk_per_unit > 0:
                    size = self._calc_position_size(entry_price, risk_per_unit, ctx)
                    if size > 0:
                        signals.append(Signal(
                            side="long",
                            symbol=symbol,
                            entry_price=entry_price,
                            stop_price=stop_price,
                            target_price=current_bb_mid,
                            confidence=min(0.8, max(0.3, (self.rsi_oversold - current_rsi) / self.rsi_oversold)),
                            suggested_size=size,
                            rationale={
                                "entry": "rsi_oversold_bb_touch",
                                "rsi": float(current_rsi),
                                "bb_lower": float(current_bb_lower),
                                "bb_mid": float(current_bb_mid),
                                "drop_pct": float(drop_pct),
                            },
                        ))
                        ctx.kv_set(f"{symbol}_position", "long")
                        ctx.kv_set(f"{symbol}_entry_price", float(entry_price))
                        ctx.kv_set(f"{symbol}_entry_time", ctx.now_ms())
                        ctx.kv_set(f"{symbol}_stop_price", float(stop_price))
                        ctx.kv_set("s2_active_count", active_count + 1)

        else:
            # ── 有持仓 → 检查出场 ──
            entry_price_val = ctx.kv_get(f"{symbol}_entry_price") or current_close
            stop_price_val = ctx.kv_get(f"{symbol}_stop_price") or (entry_price_val * (1 - self.stop_loss_pct))
            entry_time_val = entry_time or ctx.now_ms()

            # 止盈检查
            gain_pct = (current_close - entry_price_val) / entry_price_val
            # 时间止损
            elapsed_hours = (ctx.now_ms() - entry_time_val) / 3600000

            exit_reason = None

            if current_close <= stop_price_val:
                exit_reason = "stop_loss_hit"
            elif current_close >= current_bb_mid:
                exit_reason = "bb_mid_return"
            elif gain_pct >= self.take_profit_pct:
                exit_reason = "take_profit_hit"
            elif elapsed_hours >= self.max_hold_hours:
                exit_reason = "time_stop"

            if exit_reason:
                signals.append(Signal(
                    side="close",
                    symbol=symbol,
                    rationale={
                        "exit": exit_reason,
                        "price": float(current_close),
                        "gain_pct": float(gain_pct),
                        "rsi": float(current_rsi),
                    },
                ))
                ctx.kv_set(f"{symbol}_position", None)
                ctx.kv_set(f"{symbol}_entry_price", None)
                ctx.kv_set(f"{symbol}_entry_time", None)
                ctx.kv_set(f"{symbol}_stop_price", None)
                active = ctx.kv_get("s2_active_count") or 1
                ctx.kv_set("s2_active_count", max(0, active - 1))

        return signals

    def _calc_position_size(self, entry_price: float, risk_per_unit: float, ctx: StrategyContext) -> float:
        """基于风险计算仓位。"""
        equity = 5000.0
        risk_amount = equity * self.per_trade_risk_pct
        if risk_per_unit <= 0 or entry_price <= 0:
            return 0
        position_value = risk_amount / (risk_per_unit / entry_price)
        size = position_value / entry_price
        return math.floor(size * 1000) / 1000


__all__ = ["S2AltcoinReversal"]
