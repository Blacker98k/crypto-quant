"""L2 position-level risk sizing."""

from __future__ import annotations

from dataclasses import dataclass

from core.execution.order_types import OrderIntent


@dataclass(frozen=True, slots=True)
class PositionRiskLimits:
    equity: float = 10_000.0
    max_single_trade_risk_pct: float = 0.01
    max_notional_pct: float = 0.25


@dataclass(frozen=True, slots=True)
class PositionRiskDecision:
    accepted: bool
    quantity: float
    reason: str | None = None


class L2PositionRiskSizer:
    """Clamp entry order quantity to position-risk limits."""

    def __init__(self, limits: PositionRiskLimits | None = None) -> None:
        self._limits = limits or PositionRiskLimits()

    def size(
        self,
        intent: OrderIntent,
        *,
        reference_price: float | None,
    ) -> PositionRiskDecision:
        if intent.purpose != "entry" or intent.reduce_only:
            return PositionRiskDecision(True, intent.quantity)
        if intent.quantity <= 0:
            return PositionRiskDecision(False, 0.0, "quantity")
        price = intent.price if intent.price is not None else reference_price
        if price is None or price <= 0:
            return PositionRiskDecision(False, 0.0, "reference_price")
        if intent.stop_loss_price is None or intent.stop_loss_price <= 0:
            return PositionRiskDecision(False, 0.0, "stop_loss_price")

        quantity = min(
            intent.quantity,
            self._max_quantity_by_risk(intent, price),
            self._max_quantity_by_notional(price),
        )
        if quantity <= 0:
            return PositionRiskDecision(False, 0.0, "position_size")
        reason = "resized" if quantity < intent.quantity else None
        return PositionRiskDecision(True, quantity, reason)

    def _max_quantity_by_risk(self, intent: OrderIntent, price: float) -> float:
        stop = float(intent.stop_loss_price or 0)
        risk_per_unit = abs(price - stop)
        if risk_per_unit <= 0:
            return 0.0
        risk_cap = self._limits.equity * self._limits.max_single_trade_risk_pct
        return risk_cap / risk_per_unit

    def _max_quantity_by_notional(self, price: float) -> float:
        notional_cap = self._limits.equity * self._limits.max_notional_pct
        return notional_cap / price


__all__ = ["L2PositionRiskSizer", "PositionRiskDecision", "PositionRiskLimits"]
