"""Strategy signal validation before order-risk conversion."""

from __future__ import annotations

from dataclasses import dataclass

from core.strategy.base import DataRequirement, Signal


@dataclass(frozen=True, slots=True)
class SignalDecision:
    accepted: bool
    reason: str | None = None


class StrategySignalValidator:
    """Validate raw strategy signals before converting them to orders."""

    def validate(
        self,
        signal: Signal,
        *,
        requirement: DataRequirement,
        reference_symbol: str,
        reference_price: float,
    ) -> SignalDecision:
        if not signal.symbol:
            return SignalDecision(False, "symbol_required")
        if signal.symbol != reference_symbol:
            return SignalDecision(False, "symbol_mismatch")
        if requirement.symbols and signal.symbol not in requirement.symbols:
            return SignalDecision(False, "symbol_not_required")
        if not 0 <= signal.confidence <= 1:
            return SignalDecision(False, "confidence_range")
        if signal.expires_in_ms <= 0:
            return SignalDecision(False, "expiry")
        if signal.side == "close":
            return SignalDecision(True)
        if signal.suggested_size <= 0:
            return SignalDecision(False, "suggested_size")
        stop = signal.stop_price
        if stop is None or stop <= 0:
            return SignalDecision(False, "stop_price_required")
        price = signal.entry_price if signal.entry_price is not None else reference_price
        if price <= 0:
            return SignalDecision(False, "reference_price")
        if signal.side == "long" and stop >= price:
            return SignalDecision(False, "invalid_stop_direction")
        if signal.side == "short" and stop <= price:
            return SignalDecision(False, "invalid_stop_direction")
        return SignalDecision(True)


__all__ = ["SignalDecision", "StrategySignalValidator"]
