"""Strategy signal validation tests."""

from __future__ import annotations

from core.risk import StrategySignalValidator
from core.strategy.base import DataRequirement, Signal


def _requirement() -> DataRequirement:
    return DataRequirement(symbols=["BTCUSDT"], timeframes=["1m"])


def test_signal_validator_accepts_valid_long_signal() -> None:
    signal = Signal(
        side="long",
        symbol="BTCUSDT",
        stop_price=49_000,
        suggested_size=0.01,
    )

    decision = StrategySignalValidator().validate(
        signal,
        requirement=_requirement(),
        reference_symbol="BTCUSDT",
        reference_price=50_000,
    )

    assert decision.accepted is True


def test_signal_validator_accepts_close_without_size_or_stop() -> None:
    signal = Signal(side="close", symbol="BTCUSDT", stop_price=None, suggested_size=0)

    decision = StrategySignalValidator().validate(
        signal,
        requirement=_requirement(),
        reference_symbol="BTCUSDT",
        reference_price=50_000,
    )

    assert decision.accepted is True


def test_signal_validator_rejects_missing_entry_stop() -> None:
    signal = Signal(side="long", symbol="BTCUSDT", stop_price=None, suggested_size=0.01)

    decision = StrategySignalValidator().validate(
        signal,
        requirement=_requirement(),
        reference_symbol="BTCUSDT",
        reference_price=50_000,
    )

    assert decision.accepted is False
    assert decision.reason == "stop_price_required"


def test_signal_validator_rejects_symbol_mismatch() -> None:
    signal = Signal(side="long", symbol="ETHUSDT", stop_price=49_000, suggested_size=0.01)

    decision = StrategySignalValidator().validate(
        signal,
        requirement=_requirement(),
        reference_symbol="BTCUSDT",
        reference_price=50_000,
    )

    assert decision.accepted is False
    assert decision.reason == "symbol_mismatch"


def test_signal_validator_rejects_bad_confidence_and_stop_direction() -> None:
    validator = StrategySignalValidator()
    bad_confidence = Signal(
        side="long",
        symbol="BTCUSDT",
        stop_price=49_000,
        suggested_size=0.01,
        confidence=1.5,
    )
    bad_stop = Signal(side="short", symbol="BTCUSDT", stop_price=49_000, suggested_size=0.01)

    assert validator.validate(
        bad_confidence,
        requirement=_requirement(),
        reference_symbol="BTCUSDT",
        reference_price=50_000,
    ).reason == "confidence_range"
    assert validator.validate(
        bad_stop,
        requirement=_requirement(),
        reference_symbol="BTCUSDT",
        reference_price=50_000,
    ).reason == "invalid_stop_direction"
