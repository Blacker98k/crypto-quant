"""Small-live safety helpers.

This package must not submit real exchange orders. It only contains preflight
checks used before any future live adapter is allowed to start.
"""

from core.live.executor import LiveTradingAdapter, SmallLiveExecutor, SmallLiveOrderResult
from core.live.small_live import (
    ACK_ENV_VALUE,
    PaperStatus,
    ReadinessReport,
    SmallLiveConfig,
    evaluate_small_live_readiness,
)
from core.live.trading_adapter import (
    API_KEY_ENV_VAR,
    API_SECRET_ENV_VAR,
    BinanceSpotCredentials,
    BinanceSpotTradingAdapter,
)

__all__ = [
    "ACK_ENV_VALUE",
    "API_KEY_ENV_VAR",
    "API_SECRET_ENV_VAR",
    "BinanceSpotCredentials",
    "BinanceSpotTradingAdapter",
    "LiveTradingAdapter",
    "PaperStatus",
    "ReadinessReport",
    "SmallLiveConfig",
    "SmallLiveExecutor",
    "SmallLiveOrderResult",
    "evaluate_small_live_readiness",
]
