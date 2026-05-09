"""Paper 撮合引擎——模拟成交，含费率 + 滑点模型。

按 ``docs/03-详细设计/04-执行层.md`` §7 paper 模式 与
``docs/04-接口文档/03-订单接口.md`` §3–§5。

Phase 1 覆盖 market / limit / stop 三种订单类型。
强制止损状态机与多腿原子化延后至 Phase 3a/3b。
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from typing import Any, Literal, Protocol

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

    def get_fills(self, order_id: int) -> list[dict[str, Any]]: ...

    def get_symbol(self, symbol: str, exchange: str = "binance", stype: str = "perp") -> dict[str, Any] | None: ...

    def get_symbol_by_id(self, symbol_id: int) -> dict[str, Any] | None: ...

    def get_open_position(self, symbol_id: int, strategy_version: str) -> dict[str, Any] | None: ...

    def insert_position(self, row: dict[str, Any]) -> int: ...

    def update_position(self, position_id: int, changes: dict[str, Any]) -> None: ...

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

    def close_position(
        self,
        *,
        symbol: str,
        strategy: str,
        strategy_version: str,
        client_order_id: str,
        now_ms: int,
    ) -> OrderHandle | None:
        """Submit a market order that closes the open simulated position, if any."""
        symbol_id = self._resolve_symbol_id(symbol)
        position = self._repo.get_open_position(symbol_id, strategy_version)
        if position is None:
            return None
        side: Literal["buy", "sell"] = "sell" if position["side"] == "long" else "buy"
        return self.place_order(
            OrderIntent(
                signal_id=0,
                strategy=strategy,
                strategy_version=strategy_version,
                trade_group_id=position["trade_group_id"],
                symbol=symbol,
                side=side,
                order_type="market",
                quantity=float(position["qty"]),
                purpose="exit",
                reduce_only=True,
                client_order_id=client_order_id,
            ),
            now_ms,
        )

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
        self._record_fill(fill, intent, now_ms)
        exchange_id = _make_exchange_order_id()

        self._repo.update_order(order_id, {
            "status": "filled",
            "filled_qty": intent.quantity,
            "avg_fill_price": fill_price,
            "exchange_order_id": exchange_id,
            "updated_at": now_ms,
        })

        return OrderHandle(
            client_order_id=intent.client_order_id,
            exchange_order_id=exchange_id,
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
            self._record_fill(fill, intent, now_ms)
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
        intent = self._row_to_intent(row)
        self._record_fill(fill, intent, now_ms)
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
        intent = self._row_to_intent(row)
        self._record_fill(fill, intent, now_ms)
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

    def _record_fill(self, fill: Fill, intent: OrderIntent, now_ms: int) -> None:
        fill.id = self._repo.insert_fill(self._fill_to_row(fill))
        if intent.purpose not in ("entry", "exit") or fill.quantity <= 0:
            return
        symbol_id = self._resolve_symbol_id(intent.symbol)
        self._apply_position_fill(
            symbol_id=symbol_id,
            strategy=intent.strategy,
            strategy_version=intent.strategy_version,
            trade_group_id=intent.trade_group_id,
            side="long" if intent.side == "buy" else "short",
            qty=fill.quantity,
            price=fill.price,
            signal_id=intent.signal_id if intent.signal_id > 0 else None,
            now_ms=now_ms,
        )

    def _apply_position_fill(
        self,
        *,
        symbol_id: int,
        strategy: str,
        strategy_version: str,
        trade_group_id: str | None,
        side: str,
        qty: float,
        price: float,
        signal_id: int | None,
        now_ms: int,
    ) -> None:
        current = self._repo.get_open_position(symbol_id, strategy_version)
        if current is None:
            self._open_position(
                symbol_id=symbol_id,
                strategy=strategy,
                strategy_version=strategy_version,
                signal_id=signal_id,
                side=side,
                qty=qty,
                price=price,
                trade_group_id=trade_group_id,
                now_ms=now_ms,
            )
            return

        if current["side"] == side:
            old_qty = float(current["qty"])
            new_qty = old_qty + qty
            avg_price = ((old_qty * float(current["avg_entry_price"])) + (qty * price)) / new_qty
            self._repo.update_position(
                int(current["id"]),
                {
                    "qty": new_qty,
                    "avg_entry_price": avg_price,
                    "current_price": price,
                    "unrealized_pnl": 0.0,
                },
            )
            return

        old_qty = float(current["qty"])
        if qty < old_qty:
            self._repo.update_position(
                int(current["id"]),
                {
                    "qty": old_qty - qty,
                    "current_price": price,
                    "realized_pnl": self._realized_pnl(current, qty, price),
                },
            )
            return

        self._repo.update_position(
            int(current["id"]),
            {
                "qty": 0.0,
                "current_price": price,
                "realized_pnl": self._realized_pnl(current, old_qty, price),
                "closed_at": now_ms,
            },
        )
        excess = qty - old_qty
        if excess > 0:
            self._open_position(
                symbol_id=symbol_id,
                strategy=strategy,
                strategy_version=strategy_version,
                signal_id=signal_id,
                side=side,
                qty=excess,
                price=price,
                trade_group_id=trade_group_id,
                now_ms=now_ms,
            )

    def _open_position(
        self,
        *,
        symbol_id: int,
        strategy: str,
        strategy_version: str,
        signal_id: int | None,
        side: str,
        qty: float,
        price: float,
        trade_group_id: str | None,
        now_ms: int,
    ) -> None:
        self._repo.insert_position(
            {
                "symbol_id": symbol_id,
                "strategy": strategy,
                "strategy_version": strategy_version,
                "opening_signal_id": signal_id,
                "side": side,
                "qty": qty,
                "avg_entry_price": price,
                "current_price": price,
                "unrealized_pnl": 0.0,
                "realized_pnl": 0.0,
                "leverage": 1.0,
                "margin": None,
                "liq_price": None,
                "stop_order_id": None,
                "trade_group_id": trade_group_id,
                "opened_at": now_ms,
                "closed_at": None,
            }
        )

    @staticmethod
    def _realized_pnl(position: dict[str, Any], qty: float, exit_price: float) -> float:
        entry = float(position["avg_entry_price"])
        if position["side"] == "long":
            pnl = (exit_price - entry) * qty
        else:
            pnl = (entry - exit_price) * qty
        return float(position.get("realized_pnl") or 0.0) + pnl

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

    def _row_to_intent(self, row: dict[str, Any]) -> OrderIntent:
        symbol = self._symbol_id_to_name(int(row["symbol_id"]), self._repo)
        return OrderIntent(
            signal_id=int(row["signal_id"] or 0),
            strategy="paper",
            strategy_version=str(row["strategy_version"]),
            trade_group_id=row["trade_group_id"],
            symbol=symbol,
            side=row["side"],
            order_type=row["type"],
            quantity=float(row["quantity"]),
            price=row["price"],
            stop_price=row["stop_price"],
            time_in_force=row["time_in_force"],
            reduce_only=bool(row["reduce_only"]),
            purpose=row["purpose"],
            client_order_id=row["client_order_id"],
        )

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
