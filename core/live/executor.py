"""Guarded small-live order executor."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from core.common.exceptions import OMSDegraded, RiskBlockedError
from core.data.symbol import normalize_symbol
from core.execution.order_types import OrderHandle, OrderIntent
from core.live.small_live import ReadinessReport, SmallLiveConfig


class LiveTradingAdapter(Protocol):
    async def place_order(self, intent: OrderIntent, *, now_ms: int) -> OrderHandle: ...


@dataclass(frozen=True, slots=True)
class SmallLiveOrderResult:
    entry: OrderHandle
    stop_loss: OrderHandle | None = None


class SmallLiveExecutor:
    """Submit small-live spot orders only after local safety gates pass."""

    def __init__(
        self,
        *,
        adapter: LiveTradingAdapter,
        config: SmallLiveConfig,
        readiness: ReadinessReport,
    ) -> None:
        self._adapter = adapter
        self._config = config
        self._readiness = readiness
        self._allowed_symbols = {normalize_symbol(symbol) for symbol in config.allowed_symbols}

    async def submit_order(self, intent: OrderIntent, *, now_ms: int) -> SmallLiveOrderResult:
        self._validate_runtime()
        self._validate_intent(intent)

        entry = await self._adapter.place_order(intent, now_ms=now_ms)
        stop_loss = None
        if (
            intent.purpose == "entry"
            and intent.stop_loss_price is not None
            and entry.status in {"accepted", "filled", "partial"}
        ):
            stop_loss = await self._adapter.place_order(
                _protective_stop_intent(intent),
                now_ms=now_ms,
            )
        return SmallLiveOrderResult(entry=entry, stop_loss=stop_loss)

    def _validate_runtime(self) -> None:
        if not self._readiness.ready:
            raise OMSDegraded(f"small_live_not_ready: {','.join(self._readiness.blockers)}")
        if self._config.exchange != "binance_spot":
            raise OMSDegraded("small_live_spot_only")
        if self._config.allow_futures or self._config.allow_margin or self._config.allow_withdrawals:
            raise OMSDegraded("small_live_permissions_forbidden")

    def _validate_intent(self, intent: OrderIntent) -> None:
        symbol = normalize_symbol(intent.symbol)
        if symbol not in self._allowed_symbols:
            raise RiskBlockedError(f"symbol_not_allowed: {symbol}")
        if intent.order_type not in {"market", "limit"}:
            raise RiskBlockedError(f"unsupported_small_live_order_type: {intent.order_type}")
        if intent.purpose == "entry":
            if intent.side != "buy":
                raise RiskBlockedError("spot_entry_must_buy")
            if intent.stop_loss_price is None or intent.stop_loss_price <= 0:
                raise RiskBlockedError("entry_stop_loss_required")


def _protective_stop_intent(intent: OrderIntent) -> OrderIntent:
    return OrderIntent(
        signal_id=intent.signal_id,
        strategy=intent.strategy,
        strategy_version=intent.strategy_version,
        trade_group_id=intent.trade_group_id,
        symbol=intent.symbol,
        side="sell",
        order_type="stop",
        quantity=intent.quantity,
        stop_price=intent.stop_loss_price,
        reduce_only=False,
        purpose="stop_loss",
        client_order_id=f"{intent.client_order_id}-sl",
    )


__all__ = [
    "LiveTradingAdapter",
    "SmallLiveExecutor",
    "SmallLiveOrderResult",
]
