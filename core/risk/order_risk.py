"""L1 order-level risk validation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from core.execution.order_types import OrderIntent


@dataclass(frozen=True, slots=True)
class RiskDecision:
    """Result of a risk check."""

    accepted: bool
    reason: str | None = None


class L1OrderRiskValidator:
    """Validate a concrete order intent immediately before execution."""

    def validate(
        self,
        intent: OrderIntent,
        *,
        symbol_info: dict[str, Any],
        reference_price: float | None,
    ) -> RiskDecision:
        if intent.quantity <= 0:
            return RiskDecision(False, "quantity")

        price = intent.price if intent.price is not None else reference_price
        if price is None or price <= 0:
            return RiskDecision(False, "reference_price")

        if intent.purpose == "entry" and not intent.reduce_only and intent.stop_loss_price is None:
            return RiskDecision(False, "entry_stop_loss_required")

        if intent.stop_loss_price is not None:
            if intent.side == "buy" and intent.stop_loss_price >= price:
                return RiskDecision(False, "invalid_stop_loss_direction")
            if intent.side == "sell" and intent.stop_loss_price <= price:
                return RiskDecision(False, "invalid_stop_loss_direction")

        if intent.quantity * price < float(symbol_info.get("min_notional") or 0):
            return RiskDecision(False, "min_notional")

        if intent.price is not None and not self._aligned(intent.price, float(symbol_info.get("tick_size") or 0)):
            return RiskDecision(False, "price_tick_alignment")

        if not self._aligned(intent.quantity, float(symbol_info.get("lot_size") or 0)):
            return RiskDecision(False, "quantity_lot_alignment")

        return RiskDecision(True)

    @staticmethod
    def _aligned(value: float, step: float) -> bool:
        if step <= 0:
            return True
        quotient = value / step
        return abs(quotient - round(quotient)) < 1e-9


__all__ = ["L1OrderRiskValidator", "RiskDecision"]
