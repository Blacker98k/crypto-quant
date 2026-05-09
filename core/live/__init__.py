"""Small-live safety helpers.

This package must not submit real exchange orders. It only contains preflight
checks used before any future live adapter is allowed to start.
"""

from core.live.small_live import (
    ACK_ENV_VALUE,
    PaperStatus,
    ReadinessReport,
    SmallLiveConfig,
    evaluate_small_live_readiness,
)

__all__ = [
    "ACK_ENV_VALUE",
    "PaperStatus",
    "ReadinessReport",
    "SmallLiveConfig",
    "evaluate_small_live_readiness",
]
