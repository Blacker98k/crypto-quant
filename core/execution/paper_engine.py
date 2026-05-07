"""Paper 撮合引擎——模拟成交，含费率 + 滑点模型。

按 ``docs/03-详细设计/04-执行层.md`` §7 paper 模式 与
``docs/04-接口文档/03-订单接口.md`` §3–§5。

Phase 1 覆盖 market / limit / stop 三种订单类型。
强制止损状态机与多腿原子化延后至 Phase 3a/3b。
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from typing import Any, Protocol

from core.common.exceptions import (
    IdempotencyConflict,
    InvalidOrderIntent,
    InvalidStopLoss,
)
from core.execution.order_types import CancelResult, Fill, OrderHandle, OrderIntent

# Binance 默认费率
_DEFAULT_TAKER_FEE = 0.0004  # 0.04%
_DEFAULT_MAKER_FEE = 0.0002  # 0.02%
_DEFAULT_SLIPPAGE = 0.0001   # 0.01%


class _PaperRepoLike(Protocol):
    def insert_order(self, row: dict[str, Any]) -> int: ...

    def update_order(self, order_id: int, changes: dict[str, Any]) -> None: ...

    def get_order(self, client_order_id: str) -> dict[str, Any] | None: ...

    def get_open_orders(self) -> list[dict[str, Any]]: ...

    def insert_fill(self, row: dict[str, Any]) -> int: ...

    def get_symbol(self, symbol: str, exchange: str = "binance", stype: str = "perp") -> dict[str, Any] | None: ...

    def get_symbol_by_id(self, symbol_id: int) -> dict[str, Any] | None: ...

    def list_symbols(
        self,
        exchange: str | None = None,
        stype: str | None = None,
        universe: str | None = None,
    ) -> list[dict[str, Any]]: ...


def _make_exchange_order_id() -> str:
    return "paper_" + uuid.uuid4().hex[:12]


class PaperMatchingEngine:
    """Paper 撮合引擎——模拟订单撮合，不回连真实交易所。

    通过 ``repo`` 读写 orders/fills 表；通过 ``get_price`` 回调获取当前市价。
    """

    __slots__ = (
        "_get_price",
        "_maker_fee",
        "_repo",
        "_slippage",
        "_taker_fee",
    )

    def __init__(
        self,
        repo: _PaperRepoLike,
        get_price: Callable[[str], float | None],
        *,
        taker_fee: float = _DEFAULT_TAKER_FEE,
        maker_fee: float = _DEFAULT_MAKER_FEE,
        slippage: float = _DEFAULT_SLIPPAGE,
    ) -> None:
        self._repo = repo
        self._get_price = get_price
        self._taker_fee = taker_fee
        self._maker_fee = maker_fee
        self._slippage = slippage

    # ─── 公开 API ─────────────────────────────────────────────────────────

    def place_order(self, intent: OrderIntent, now_ms: int) -> OrderHandle:
        """验证 OrderIntent → 持久化 → 模拟撮合（市价单立即成交）。"""
        self._validate(intent)
        self._check_idempotent(intent.client_order_id)

        order_row = self._intent_to_row(intent, now_ms)
        order_id = self._repo.insert_order(order_row)

        if intent.order_type == "market":
            return self._simulate_market_fill(order_id, intent, now_ms)
        elif intent.order_type == "limit":
            return self._try_limit_fill(order_id, intent, now_ms)
        elif intent.order_type in ("stop", "stop_limit"):
            return self._park_stop_order(order_id, intent, now_ms)
        else:
            # take_profit 等 Phase 3a 实现
            return OrderHandle(
                client_order_id=intent.client_order_id,
                exchange_order_id=None,
                status="new",
                submitted_at=now_ms,
            )

    def cancel_order(self, client_order_id: str, now_ms: int) -> CancelResult:
        """取消指定订单。"""
        row = self._repo.get_order(client_order_id)
        if row is None:
            return CancelResult(client_order_id, False, "order not found")
        if row["status"] in ("filled", "canceled", "rejected", "expired"):
            return CancelResult(client_order_id, False, f"order already {row['status']}")
        self._repo.update_order(row["id"], {"status": "canceled", "updated_at": now_ms})
        return CancelResult(client_order_id, True)

    def get_order(self, client_order_id: str) -> dict | None:
        """查询订单。"""
        return self._repo.get_order(client_order_id)

    def check_pending_orders(self, now_ms: int) -> list[Fill]:
        """检查限价/止损单是否满足成交条件，返回新产生的 Fill 列表。"""
        fills: list[Fill] = []
        open_orders = self._repo.get_open_orders()
        for row in open_orders:
            price = self._get_price(self._symbol_id_to_name(row["symbol_id"], self._repo))
            if price is None:
                continue
            if row["type"] == "limit":
                fill = self._check_limit_trigger(row, price, now_ms)
            elif row["type"] in ("stop", "stop_limit"):
                fill = self._check_stop_trigger(row, price, now_ms)
            else:
                continue
            if fill is not None:
                fills.append(fill)
        return fills

    # ─── 验证 ────────────────────────────────────────────────────────────

    def _validate(self, intent: OrderIntent) -> None:
        """字段级验证，不合法则 raise InvalidOrderIntent。"""
        if not intent.client_order_id:
            raise InvalidOrderIntent("client_order_id 必填")

        if not intent.symbol:
            raise InvalidOrderIntent("symbol 必填")

        if intent.quantity <= 0:
            raise InvalidOrderIntent("quantity 必须 > 0")

        if intent.order_type in ("limit", "stop_limit"):
            if intent.price is None or intent.price <= 0:
                raise InvalidOrderIntent(f"{intent.order_type} 必须指定 price")

        if intent.order_type == "market":
            if intent.price is not None:
                raise InvalidOrderIntent("market 订单不得指定 price")

        if intent.order_type in ("stop", "stop_limit"):
            if intent.stop_price is None or intent.stop_price <= 0:
                raise InvalidOrderIntent(f"{intent.order_type} 必须指定 stop_price")

        # 止损价位方向校验
        if intent.stop_loss_price is not None:
            entry_price = intent.price
            if entry_price is None:
                return
            if intent.side == "buy" and intent.stop_loss_price >= entry_price:
                raise InvalidStopLoss("多头止损价必须 < entry 价")
            if intent.side == "sell" and intent.stop_loss_price <= entry_price:
                raise InvalidStopLoss("空头止损价必须 > entry 价")

    def _check_idempotent(self, client_order_id: str) -> None:
        existing = self._repo.get_order(client_order_id)
        if existing is None:
            return
        if existing["status"] in ("filled", "rejected", "canceled", "expired"):
            raise IdempotencyConflict(
                f"client_order_id={client_order_id} 已处于终态 {existing['status']}"
            )
        # 相同状态 → 幂等返回，由 place_order 调用方处理
        raise IdempotencyConflict(
            f"client_order_id={client_order_id} 已存在且状态为 {existing['status']}"
        )

    # ─── 撮合逻辑 ────────────────────────────────────────────────────────

    def _simulate_market_fill(self, order_id: int, intent: OrderIntent, now_ms: int) -> OrderHandle:
        """市价单：立即以当前价 + 滑点 + taker 费率成交。"""
        price = self._get_price(intent.symbol)
        if price is None:
            # 无市价 → 留在 accepted 状态
            self._repo.update_order(order_id, {"status": "accepted", "updated_at": now_ms})
            return OrderHandle(
                client_order_id=intent.client_order_id,
                exchange_order_id=None,
                status="accepted",
                submitted_at=now_ms,
            )

        fill_price = self._apply_slippage(price, intent.side)
        fill = self._build_fill(order_id, fill_price, intent.quantity, is_maker=False, now_ms=now_ms)
        fill_id = self._repo.insert_fill(self._fill_to_row(fill))
        fill.id = fill_id

        self._repo.update_order(order_id, {
            "status": "filled",
            "filled_qty": intent.quantity,
            "avg_fill_price": fill_price,
            "exchange_order_id": _make_exchange_order_id(),
            "updated_at": now_ms,
        })

        return OrderHandle(
            client_order_id=intent.client_order_id,
            exchange_order_id=_make_exchange_order_id(),
            status="filled",
            submitted_at=now_ms,
        )

    def _try_limit_fill(self, order_id: int, intent: OrderIntent, now_ms: int) -> OrderHandle:
        """限价单：若当前价已满足限价则立即成交，否则保持 accepted。"""
        price = self._get_price(intent.symbol)
        exchange_id = _make_exchange_order_id()
        self._repo.update_order(order_id, {
            "status": "accepted",
            "exchange_order_id": exchange_id,
            "updated_at": now_ms,
        })

        limit_price = intent.price
        if limit_price is None:
            raise InvalidOrderIntent("limit 订单必须指定 price")

        if price is not None and self._limit_crossed(intent.side, limit_price, price):
            fill_price = limit_price  # 限价单以限价成交（maker）
            fill = self._build_fill(order_id, fill_price, intent.quantity, is_maker=True, now_ms=now_ms)
            self._repo.insert_fill(self._fill_to_row(fill))
            self._repo.update_order(order_id, {
                "status": "filled",
                "filled_qty": intent.quantity,
                "avg_fill_price": fill_price,
                "updated_at": now_ms,
            })
            return OrderHandle(
                client_order_id=intent.client_order_id,
                exchange_order_id=exchange_id,
                status="filled",
                submitted_at=now_ms,
            )

        return OrderHandle(
            client_order_id=intent.client_order_id,
            exchange_order_id=exchange_id,
            status="accepted",
            submitted_at=now_ms,
        )

    def _park_stop_order(self, order_id: int, intent: OrderIntent, now_ms: int) -> OrderHandle:
        """止损单：挂起等待触发。"""
        exchange_id = _make_exchange_order_id()
        self._repo.update_order(order_id, {
            "status": "accepted",
            "exchange_order_id": exchange_id,
            "updated_at": now_ms,
        })
        return OrderHandle(
            client_order_id=intent.client_order_id,
            exchange_order_id=exchange_id,
            status="accepted",
            submitted_at=now_ms,
        )

    def _check_limit_trigger(self, row: dict, price: float, now_ms: int) -> Fill | None:
        """检查限价单是否满足成交条件。"""
        side = row["side"]
        limit_price = row["price"]
        if limit_price is None:
            return None
        if not self._limit_crossed(side, limit_price, price):
            return None
        fill = self._build_fill(row["id"], limit_price, row["quantity"], is_maker=True, now_ms=now_ms)
        self._repo.insert_fill(self._fill_to_row(fill))
        self._repo.update_order(row["id"], {
            "status": "filled",
            "filled_qty": row["quantity"],
            "avg_fill_price": limit_price,
            "updated_at": now_ms,
        })
        return fill

    def _check_stop_trigger(self, row: dict, price: float, now_ms: int) -> Fill | None:
        """检查止损单是否触发。"""
        side = row["side"]
        trigger = row["stop_price"]
        if trigger is None:
            return None
        triggered = (side == "buy" and price >= trigger) or (side == "sell" and price <= trigger)
        if not triggered:
            return None
        # 止损单触发后以市价成交（taker）
        fill_price = self._apply_slippage(price, side)
        fill = self._build_fill(row["id"], fill_price, row["quantity"], is_maker=False, now_ms=now_ms)
        self._repo.insert_fill(self._fill_to_row(fill))
        self._repo.update_order(row["id"], {
            "status": "filled",
            "filled_qty": row["quantity"],
            "avg_fill_price": fill_price,
            "updated_at": now_ms,
        })
        return fill

    # ─── 辅助 ────────────────────────────────────────────────────────────

    @staticmethod
    def _limit_crossed(side: str, limit_price: float, current_price: float) -> bool:
        """限价单是否已达到可成交价位。"""
        return (side == "buy" and current_price <= limit_price) or (
            side == "sell" and current_price >= limit_price
        )

    def _apply_slippage(self, price: float, side: str) -> float:
        """对市价成交施加不利方向滑点。"""
        if side == "buy":
            return price * (1.0 + self._slippage)
        else:
            return price * (1.0 - self._slippage)

    def _build_fill(self, order_id: int, price: float, qty: float, *, is_maker: bool, now_ms: int) -> Fill:
        fee_rate = self._maker_fee if is_maker else self._taker_fee
        return Fill(
            order_id=order_id,
            exchange_fill_id="fill_" + uuid.uuid4().hex[:12],
            price=price,
            quantity=qty,
            fee=qty * price * fee_rate,
            fee_currency="USDT",
            is_maker=is_maker,
            ts=now_ms,
        )

    def _resolve_symbol_id(self, symbol: str) -> int:
        """从 symbols 表解析 symbol 文本 → symbol_id。"""
        # 内部格式：BTC/USDT → BTCUSDT
        internal = symbol.replace("/", "")
        row = self._repo.get_symbol(internal)
        if row is not None:
            return int(row["id"])
        # 回退：尝试模糊搜索
        rows = self._repo.list_symbols()
        for r in rows:
            if r["symbol"] == internal:
                return int(r["id"])
        raise InvalidOrderIntent(f"symbol 不在 symbols 表中: {symbol}")

    def _intent_to_row(self, intent: OrderIntent, now_ms: int) -> dict:
        return {
            "signal_id": intent.signal_id if intent.signal_id > 0 else None,
            "client_order_id": intent.client_order_id,
            "exchange_order_id": None,
            "symbol_id": self._resolve_symbol_id(intent.symbol),
            "side": intent.side,
            "type": intent.order_type,
            "price": intent.price,
            "stop_price": intent.stop_price,
            "quantity": intent.quantity,
            "filled_qty": 0,
            "avg_fill_price": None,
            "status": "new",
            "parent_order_id": None,
            "purpose": intent.purpose,
            "time_in_force": intent.time_in_force,
            "reduce_only": int(intent.reduce_only),
            "trade_group_id": intent.trade_group_id,
            "strategy_version": intent.strategy_version,
            "placed_at": now_ms,
            "updated_at": now_ms,
        }

    @staticmethod
    def _fill_to_row(fill: Fill) -> dict:
        return {
            "order_id": fill.order_id,
            "exchange_fill_id": fill.exchange_fill_id,
            "price": fill.price,
            "quantity": fill.quantity,
            "fee": fill.fee,
            "fee_currency": fill.fee_currency,
            "is_maker": int(fill.is_maker),
            "ts": fill.ts,
            "raw_payload": fill.raw_payload,
        }

    @staticmethod
    def _symbol_id_to_name(symbol_id: int, repo: _PaperRepoLike) -> str:
        row = repo.get_symbol_by_id(symbol_id)
        if row is not None:
            return str(row["symbol"])
        return "BTCUSDT"


__all__ = ["PaperMatchingEngine"]
