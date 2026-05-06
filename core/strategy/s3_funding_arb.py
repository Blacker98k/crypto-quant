"""S3 · 资金费率套利。

正资金费率 → 做空永续 + 买入现货，赚取费率差。

按 ``docs/03-详细设计/strategies/S3-资金费率套利.md`` 与
``config/strategies/s3.example.yml``。
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from core.strategy.base import DataRequirement, Signal, Strategy, StrategyContext

if TYPE_CHECKING:
    from core.data.exchange.base import Bar


class S3FundingArbitrage(Strategy):
    """资金费率套利（delta-neutral）。

    - 池：core universe（top 30），按流动性排
    - 入场：永续 funding rate ≥ 0.10%/8h + 同向连续 ≥ 3 次 + 基差偏离 ≤ 0.5%
    - 双腿：做空永续 + 买入现货（spot + perp）
    - 退出：费率回归 < 0.03% / 基差扩大 / 超时
    - 原子性：30s 内双腿必须成交，否则 unwind
    """

    name = "S3_funding_arbitrage"
    version = "dev"
    config_hash = "0000000000000000"

    __slots__ = ()

    def required_data(self) -> DataRequirement:
        return DataRequirement(
            symbols=[],  # 动态从 core universe 获取
            timeframes=["1h", "4h"],
            history_lookback_bars=500,
            needs_funding=True,
        )

    def on_bar(self, bar: Bar, ctx: StrategyContext) -> list[Signal]:
        """Phase 1 stub: 返回空信号。完整逻辑待实现。"""
        return []


__all__ = ["S3FundingArbitrage"]
