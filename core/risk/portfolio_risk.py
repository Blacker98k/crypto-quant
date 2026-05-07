"""L3 portfolio-level risk validation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from core.execution.order_types import OrderIntent


@dataclass(frozen=True, slots=True)
class PortfolioRiskLimits:
    equity: float = 10_000.0
    max_gross_leverage: float = 3.0
    max_symbol_notional_pct: float = 0.25


@dataclass(frozen=True, slots=True)
class PortfolioRiskDecision:
    accepted: bool
    reason: str | None = None


class L3PortfolioRiskValidator:
    """Reject new entries that would breach portfolio exposure caps."""

    def __init__(self, limits: PortfolioRiskLimits | None = None) -> None:
        self._limits = limits or PortfolioRiskLimits()

    def validate(
        self,
        intent: OrderIntent,
        *,
        reference_price: float | None,
        open_positions: list[dict[str, Any]],
        symbol_id: int | None = None,
    ) -> PortfolioRiskDecision:
        if intent.purpose != "entry" or intent.reduce_only:
            return PortfolioRiskDecision(True)
        if reference_price is None or reference_price <= 0:
            return PortfolioRiskDecision(False, "reference_price")
        if intent.quantity <= 0:
            return PortfolioRiskDecision(False, "quantity")

        order_notional = intent.quantity * reference_price
        gross_notional = self._gross_notional(open_positions) + order_notional
        if gross_notional > self._limits.equity * self._limits.max_gross_leverage:
            return PortfolioRiskDecision(False, "gross_leverage")

        if symbol_id is not None:
            symbol_notional = self._symbol_notional(open_positions, symbol_id) + order_notional
            if symbol_notional > self._limits.equity * self._limits.max_symbol_notional_pct:
                return PortfolioRiskDecision(False, "symbol_exposure")

        return PortfolioRiskDecision(True)

    @staticmethod
    def _gross_notional(open_positions: list[dict[str, Any]]) -> float:
        return sum(
            abs(float(row["qty"]) * float(row.get("current_price") or row["avg_entry_price"]))
            for row in open_positions
        )

    @staticmethod
    def _symbol_notional(open_positions: list[dict[str, Any]], symbol_id: int) -> float:
        return sum(
            abs(float(row["qty"]) * float(row.get("current_price") or row["avg_entry_price"]))
            for row in open_positions
            if int(row["symbol_id"]) == symbol_id
        )


__all__ = ["L3PortfolioRiskValidator", "PortfolioRiskDecision", "PortfolioRiskLimits"]
