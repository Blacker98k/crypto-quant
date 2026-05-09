"""OMS 数据模型：OrderIntent / Order / Fill / OrderHandle / CancelResult。

按 ``docs/04-接口文档/03-订单接口.md`` §3–§4 定义。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

# ─── OrderIntent（下单参数）─────────────────────────────────────────────────────


@dataclass(slots=True)
class OrderIntent:
    """OMS 接受的已校准下单意图（经 L1 风控通过后）。"""

    # 来源溯源
    signal_id: int
    strategy: str
    strategy_version: str
    trade_group_id: str | None = None

    # 订单本体
    symbol: str = ""  # 'BTC/USDT'
    side: Literal["buy", "sell"] = "buy"
    order_type: Literal["limit", "market", "stop", "stop_limit", "take_profit"] = "market"
    quantity: float = 0.0
    price: float | None = None
    stop_price: float | None = None
    time_in_force: Literal["GTC", "IOC", "FOK", "DAY"] = "GTC"
    reduce_only: bool = False
    purpose: Literal["entry", "stop_loss", "take_profit", "exit", "hedge_leg"] = "entry"

    # 强制止损保护
    stop_loss_price: float | None = None

    # 幂等
    client_order_id: str = ""


# ─── Order（数据库订单行）───────────────────────────────────────────────────────


@dataclass(slots=True)
class Order:
    """orders 表行映射。"""

    id: int = 0
    signal_id: int | None = None
    client_order_id: str = ""
    exchange_order_id: str | None = None
    symbol_id: int = 0
    side: str = ""
    type: str = ""
    price: float | None = None
    stop_price: float | None = None
    quantity: float = 0.0
    filled_qty: float = 0.0
    avg_fill_price: float | None = None
    status: str = "new"
    parent_order_id: int | None = None
    purpose: str = "entry"
    time_in_force: str = "GTC"
    reduce_only: bool = False
    trade_group_id: str | None = None
    strategy_version: str = ""
    placed_at: int = 0
    updated_at: int = 0


# ─── Fill（成交回报）───────────────────────────────────────────────────────────


@dataclass(slots=True)
class Fill:
    """fills 表行映射。"""

    id: int = 0
    order_id: int = 0
    exchange_fill_id: str = ""
    price: float = 0.0
    quantity: float = 0.0
    fee: float = 0.0
    fee_currency: str = "USDT"
    is_maker: bool = False
    ts: int = 0
    raw_payload: str | None = None


# ─── OrderHandle（返回值）───────────────────────────────────────────────────────


@dataclass(slots=True)
class OrderHandle:
    """place_order 返回值——调用方手持的"订单句柄"。"""

    client_order_id: str
    exchange_order_id: str | None
    status: Literal["new", "accepted", "partial", "filled", "rejected", "canceled"]
    submitted_at: int

    def to_order(self) -> Order:
        """当前快照转为 Order 对象。"""
        return Order(
            client_order_id=self.client_order_id,
            exchange_order_id=self.exchange_order_id,
            status=self.status,
            placed_at=self.submitted_at,
            updated_at=self.submitted_at,
        )


# ─── CancelResult ──────────────────────────────────────────────────────────────


@dataclass(slots=True)
class CancelResult:
    """cancel_order 返回值。"""

    client_order_id: str
    success: bool
    reason: str | None = None


__all__ = [
    "CancelResult",
    "Fill",
    "Order",
    "OrderHandle",
    "OrderIntent",
]
