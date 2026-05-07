"""L1 order risk validation tests."""

from __future__ import annotations

from core.execution.order_types import OrderIntent
from core.risk.order_risk import L1OrderRiskValidator


def _intent(**kwargs) -> OrderIntent:
    defaults = {
        "signal_id": 1,
        "strategy": "s1",
        "strategy_version": "dev",
        "symbol": "BTCUSDT",
        "side": "buy",
        "order_type": "market",
        "quantity": 0.01,
        "stop_loss_price": 49000.0,
        "client_order_id": "risk-test",
    }
    defaults.update(kwargs)
    return OrderIntent(**defaults)


def _symbol(**kwargs) -> dict:
    defaults = {
        "symbol": "BTCUSDT",
        "tick_size": 0.1,
        "lot_size": 0.001,
        "min_notional": 10.0,
    }
    defaults.update(kwargs)
    return defaults


def test_accepts_valid_market_entry_order() -> None:
    decision = L1OrderRiskValidator().validate(
        _intent(),
        symbol_info=_symbol(),
        reference_price=50000.0,
    )

    assert decision.accepted is True
    assert decision.reason is None


def test_rejects_entry_without_stop_loss() -> None:
    decision = L1OrderRiskValidator().validate(
        _intent(stop_loss_price=None),
        symbol_info=_symbol(),
        reference_price=50000.0,
    )

    assert decision.accepted is False
    assert decision.reason == "entry_stop_loss_required"


def test_reduce_only_exit_does_not_require_stop_loss() -> None:
    decision = L1OrderRiskValidator().validate(
        _intent(side="sell", purpose="exit", reduce_only=True, stop_loss_price=None),
        symbol_info=_symbol(),
        reference_price=50000.0,
    )

    assert decision.accepted is True


def test_rejects_min_notional_violation() -> None:
    decision = L1OrderRiskValidator().validate(
        _intent(quantity=0.0001),
        symbol_info=_symbol(min_notional=10.0),
        reference_price=50000.0,
    )

    assert decision.accepted is False
    assert decision.reason == "min_notional"


def test_rejects_bad_stop_direction_for_market_buy() -> None:
    decision = L1OrderRiskValidator().validate(
        _intent(side="buy", stop_loss_price=51000.0),
        symbol_info=_symbol(),
        reference_price=50000.0,
    )

    assert decision.accepted is False
    assert decision.reason == "invalid_stop_loss_direction"


def test_rejects_unaligned_limit_price() -> None:
    decision = L1OrderRiskValidator().validate(
        _intent(order_type="limit", price=50000.05),
        symbol_info=_symbol(tick_size=0.1),
        reference_price=50000.0,
    )

    assert decision.accepted is False
    assert decision.reason == "price_tick_alignment"


def test_rejects_unaligned_quantity() -> None:
    decision = L1OrderRiskValidator().validate(
        _intent(quantity=0.0105),
        symbol_info=_symbol(lot_size=0.001),
        reference_price=50000.0,
    )

    assert decision.accepted is False
    assert decision.reason == "quantity_lot_alignment"

