"""策略层：Strategy ABC / Signal 数据模型 / S1/S2/S3 策略。

详见 ``docs/04-接口文档/01-策略接口.md``。
"""

from core.strategy.base import (
    DataRequirement,
    Signal,
    Strategy,
    StrategyContext,
)
from core.strategy.s1_btc_trend import S1BtcEthTrend
from core.strategy.s2_altcoin_reversal import S2AltcoinReversal
from core.strategy.s3_funding_arb import S3FundingArbitrage

__all__ = [
    "DataRequirement",
    "S1BtcEthTrend",
    "S2AltcoinReversal",
    "S3FundingArbitrage",
    "Signal",
    "Strategy",
    "StrategyContext",
]
