"""执行层：Paper 撮合引擎 / OMS 数据模型 / 错误码映射。

详见 ``docs/03-详细设计/04-执行层.md``。Phase 1 落 paper 撮合器最小子集；
实盘 OMS（OrderRouter / OrderStateMachine / StopLossManager）延至 Phase 3a。
"""

from core.execution.error_map import (
    BINANCE_ERROR_TABLE,
    build_error_message,
    is_ip_banned,
    is_rate_limited,
    is_server_error,
    map_binance_error,
)
from core.execution.order_types import (
    CancelResult,
    Fill,
    Order,
    OrderHandle,
    OrderIntent,
)
from core.execution.paper_engine import PaperMatchingEngine
from core.execution.simulation import SimulatedPaperSession, SimulationResult

__all__ = [
    "BINANCE_ERROR_TABLE",
    "CancelResult",
    "Fill",
    "Order",
    "OrderHandle",
    "OrderIntent",
    "PaperMatchingEngine",
    "SimulatedPaperSession",
    "SimulationResult",
    "build_error_message",
    "is_ip_banned",
    "is_rate_limited",
    "is_server_error",
    "map_binance_error",
]
