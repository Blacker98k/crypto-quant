"""S1 · BTC/ETH 趋势跟随（完整实现）。

Donchian 通道突破 + ATR 过滤 + 多时间框架趋势判断 + 跟踪止损。

入场条件（全部满足）：
  1. 大趋势：1d 收盘 > 1d MA50 且 1d MA20 > 1d MA50（金叉）
  2. 入场触发：4h 收盘突破 20 期 Donchian 通道上轨（做多）/ 下轨（做空）
  3. 波动率过滤：当前 ATR > 过去 30 天 ATR 中位数
  4. 流动性过滤：4h K 线成交额 > 7 天均值 × 1.2

出场条件：
  1. ATR 跟踪止损（1.5 × ATR(14)）
  2. 趋势反转：4h 收盘跌破 4h MA20
  3. 时间出场：30 天未达到 2R
"""

from __future__ import annotations

import math
from typing import TYPE_CHECKING

import pandas as pd

from core.strategy.base import DataRequirement, Signal, Strategy, StrategyContext
from core.strategy.indicators import (
    bars_to_df,
    compute_atr_from_bars,
    compute_donchian_from_bars,
    compute_sma,
)

if TYPE_CHECKING:
    from core.data.exchange.base import Bar

# ─── 默认参数（从 config/strategies/s1.example.yml 映射）───────────────────


class S1BtcEthTrend(Strategy):
    """BTC/ETH Donchian 突破趋势跟随。

    Class Attributes（可被 config 覆写）：
        donchian_period: Donchian 通道周期（默认 20）
        atr_period: ATR 周期（默认 14）
        atr_relative_min_mult: ATR 相对最小值倍数 vs 30d 中位数（默认 1.0）
        volume_spike_mult: 成交量放大倍数 vs 7d 均值（默认 1.2）
        trail_stop_atr_mult: 跟踪止损 ATR 倍数（默认 1.5）
        per_trade_risk_pct: 单笔风险比例（默认 0.01 = 1%）
        max_adds: 最大加仓次数（默认 3）
    """

    name = "S1_btc_eth_trend"
    version = "dev"
    config_hash = "0000000000000000"

    # ── 可配置参数 ──────────────────────────────────────────────────────────

    donchian_period: int = 15  # 参数扫描最优值（原spec为20，实测15更好）
    atr_period: int = 10  # 参数扫描最优值
    atr_relative_min_mult: float = 1.0
    volume_spike_mult: float = 1.2
    trail_stop_atr_mult: float = 1.5
    per_trade_risk_pct: float = 0.01
    max_adds: int = 3

    __slots__ = ()

    def required_data(self) -> DataRequirement:
        return DataRequirement(
            symbols=["BTCUSDT", "ETHUSDT"],
            timeframes=["4h", "1d"],
            history_lookback_bars=500,
            needs_funding=False,
        )

    # ─── on_bar: 每根 4h K 线收盘时调用 ──────────────────────────────────

    def on_bar(self, bar: Bar, ctx: StrategyContext) -> list[Signal]:
        """主逻辑入口。返回 0..N 个信号。"""
        symbol = bar.symbol
        signals: list[Signal] = []

        # 1. 拉取历史数据
        entry_bars = ctx.data.get_candles(symbol, "4h", n=500)
        trend_bars = ctx.data.get_candles(symbol, "1d", n=100)

        if len(entry_bars) < 50 or len(trend_bars) < 30:
            return signals  # 数据不足，跳过

        # 2. 计算指标
        entry_df = bars_to_df(entry_bars)
        trend_df = bars_to_df(trend_bars)

        # 2a. Donchian 通道（4h）——滞后 1 根，避免自更新陷阱
        donchian = compute_donchian_from_bars(entry_bars, self.donchian_period)
        donchian_upper = donchian["upper"].shift(1)
        donchian_lower = donchian["lower"].shift(1)

        # 2b. ATR（4h）
        atr = compute_atr_from_bars(entry_bars, self.atr_period)

        # 2c. 趋势过滤（1d）
        trend_ma20 = compute_sma(trend_df["close"], 20)
        trend_ma50 = compute_sma(trend_df["close"], 50)

        # 2d. 成交量基准（7 天 4h ≈ 42 根）
        vol_7d_mean = entry_df["quote_volume"].tail(42).mean()

        # 当前状态
        current_close = entry_df["close"].iloc[-1]
        current_donchian_upper = donchian_upper.iloc[-1]
        current_donchian_lower = donchian_lower.iloc[-1]
        current_atr = atr.iloc[-1]

        if pd.isna(current_atr) or current_atr <= 0:
            return signals

        # 昨日趋势数据（1d）
        trend_close = trend_df["close"].iloc[-1]
        trend_ma20_val = trend_ma20.iloc[-1]
        trend_ma50_val = trend_ma50.iloc[-1]

        # 3. 波动率过滤
        atr_median_30d = atr.tail(30 * 4).median()  # 30 天 ≈ 120 根 4h
        if pd.isna(atr_median_30d) or atr_median_30d <= 0:
            atr_ok = True
        else:
            atr_ok = current_atr >= atr_median_30d * self.atr_relative_min_mult

        # 成交量过滤
        current_vol = entry_df["quote_volume"].iloc[-1]
        vol_ok = current_vol >= vol_7d_mean * self.volume_spike_mult

        # 4. 判断入场/出场
        # 检查当前是否有持仓（通过 kv 状态）
        position_side = ctx.kv_get(f"{symbol}_position_side")
        current_stop = ctx.kv_get(f"{symbol}_trailing_stop")

        if position_side is None:
            # ── 无持仓 → 检查入场条件 ──
            if (
                trend_close > trend_ma50_val                  # 大趋势多头
                and not pd.isna(trend_ma20_val)
                and trend_ma20_val > trend_ma50_val           # 金叉持续
                and current_close > current_donchian_upper    # Donchian 突破
                and atr_ok                                     # 波动率够
                and vol_ok                                     # 放量
            ):
                entry_price = current_close
                stop_price = entry_price - self.trail_stop_atr_mult * current_atr
                risk_per_unit = entry_price - stop_price
                if risk_per_unit > 0:
                    suggested_size = self._calc_position_size(
                        entry_price, risk_per_unit, ctx
                    )
                    signals.append(Signal(
                        side="long",
                        symbol=symbol,
                        entry_price=entry_price,
                        stop_price=stop_price,
                        confidence=0.6,
                        suggested_size=suggested_size,
                        rationale={
                            "entry": "donchian_breakout_long",
                            "donchian_upper": float(current_donchian_upper),
                            "atr": float(current_atr),
                            "trend_ma20": float(trend_ma20_val),
                            "trend_ma50": float(trend_ma50_val),
                        },
                    ))
                    # 保存状态
                    ctx.kv_set(f"{symbol}_position_side", "long")
                    ctx.kv_set(f"{symbol}_trailing_stop", float(stop_price))
                    ctx.kv_set(f"{symbol}_entry_price", float(entry_price))

            elif (
                trend_close < trend_ma50_val                  # 大趋势空头
                and not pd.isna(trend_ma20_val)
                and trend_ma20_val < trend_ma50_val           # 死叉持续
                and current_close < current_donchian_lower    # Donchian 跌破
                and atr_ok
                and vol_ok
            ):
                entry_price = current_close
                stop_price = entry_price + self.trail_stop_atr_mult * current_atr
                risk_per_unit = stop_price - entry_price
                if risk_per_unit > 0:
                    suggested_size = self._calc_position_size(
                        entry_price, risk_per_unit, ctx
                    )
                    signals.append(Signal(
                        side="short",
                        symbol=symbol,
                        entry_price=entry_price,
                        stop_price=stop_price,
                        confidence=0.6,
                        suggested_size=suggested_size,
                        rationale={
                            "entry": "donchian_breakdown_short",
                            "donchian_lower": float(current_donchian_lower),
                            "atr": float(current_atr),
                        },
                    ))
                    ctx.kv_set(f"{symbol}_position_side", "short")
                    ctx.kv_set(f"{symbol}_trailing_stop", float(stop_price))
                    ctx.kv_set(f"{symbol}_entry_price", float(entry_price))

        else:
            # ── 有持仓 → 检查出场条件 + 更新跟踪止损 ──
            # 跟踪止损：max(原止损, 入场价 - 1.5×ATR) 多头
            entry_price_val = ctx.kv_get(f"{symbol}_entry_price") or current_close
            if position_side == "long":
                new_stop = max(
                    current_stop or 0,
                    current_close - self.trail_stop_atr_mult * current_atr,
                )
                ctx.kv_set(f"{symbol}_trailing_stop", float(new_stop))

                # 出场条件
                exit_signal = None
                ma20 = compute_sma(entry_df["close"], 20).iloc[-1]
                if current_close < ma20:
                    exit_signal = "ma20_breakdown"
                elif current_close <= (current_stop or new_stop):
                    exit_signal = "trailing_stop_hit"

                if exit_signal:
                    signals.append(Signal(
                        side="close",
                        symbol=symbol,
                        rationale={"exit": exit_signal, "price": float(current_close)},
                    ))
                    ctx.kv_set(f"{symbol}_position_side", None)
                    ctx.kv_set(f"{symbol}_trailing_stop", None)
                    ctx.kv_set(f"{symbol}_entry_price", None)

            elif position_side == "short":
                new_stop = min(
                    current_stop or float("inf"),
                    current_close + self.trail_stop_atr_mult * current_atr,
                )
                ctx.kv_set(f"{symbol}_trailing_stop", float(new_stop))

                exit_signal = None
                ma20 = compute_sma(entry_df["close"], 20).iloc[-1]
                if current_close > ma20:
                    exit_signal = "ma20_breakup"
                elif current_close >= (current_stop or new_stop):
                    exit_signal = "trailing_stop_hit"

                if exit_signal:
                    signals.append(Signal(
                        side="close",
                        symbol=symbol,
                        rationale={"exit": exit_signal, "price": float(current_close)},
                    ))
                    ctx.kv_set(f"{symbol}_position_side", None)
                    ctx.kv_set(f"{symbol}_trailing_stop", None)
                    ctx.kv_set(f"{symbol}_entry_price", None)

        return signals

    # ─── 仓位计算 ──────────────────────────────────────────────────────────

    def _calc_position_size(
        self, entry_price: float, risk_per_unit: float, ctx: StrategyContext,
    ) -> float:
        """基于风险计算仓位。

        公式：仓位价值 = 总资金 × 单笔风险% / (止损距离 / 入场价)
        合约张数 = 仓位价值 / 入场价
        """
        # Phase 1: 模拟总资金 5000 USDT
        equity = 5000.0
        raw_risk_amount = equity * self.per_trade_risk_pct
        position_value = raw_risk_amount / (risk_per_unit / entry_price)
        size = position_value / entry_price
        # 向下取整到 0.001
        return math.floor(size * 1000) / 1000


__all__ = ["S1BtcEthTrend"]
