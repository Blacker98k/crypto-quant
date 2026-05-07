"""L2 position-risk sizing tests."""

from __future__ import annotations

from core.execution.order_types import OrderIntent
from core.risk.position_risk import L2PositionRiskSizer, PositionRiskLimits


def _intent(**kwargs) -> OrderIntent:
    defaults = {
        "signal_id": 0,
        "strategy": "s1",
        "strategy_version": "dev",
        "symbol": "BTCUSDT",
        "side": "buy",
        "order_type": "market",
        "quantity": 0.01,
        "stop_loss_price": 49_000.0,
        "client_order_id": "l2-test",
        "purpose": "entry",
    }
    defaults.update(kwargs)
    return OrderIntent(**defaults)


def test_l2_accepts_quantity_inside_limits() -> None:
    decision = L2PositionRiskSizer().size(_intent(), reference_price=50_000.0)

    assert decision.accepted is True
    assert decision.quantity == 0.01
    assert decision.reason is None


def test_l2_resizes_by_single_trade_risk_cap() -> None:
    decision = L2PositionRiskSizer(
        PositionRiskLimits(equity=10_000, max_single_trade_risk_pct=0.01, max_notional_pct=1)
    ).size(_intent(quantity=1.0), reference_price=50_000.0)

    assert decision.accepted is True
    assert decision.quantity == 0.1
    assert decision.reason == "resized"


def test_l2_resizes_by_notional_cap() -> None:
    decision = L2PositionRiskSizer(
        PositionRiskLimits(equity=10_000, max_single_trade_risk_pct=1, max_notional_pct=0.25)
    ).size(_intent(quantity=1.0), reference_price=50_000.0)

    assert decision.accepted is True
    assert decision.quantity == 0.05
    assert decision.reason == "resized"


def test_l2_rejects_missing_reference_price() -> None:
    decision = L2PositionRiskSizer().size(_intent(), reference_price=None)

    assert decision.accepted is False
    assert decision.reason == "reference_price"


def test_l2_does_not_resize_reduce_only_exit() -> None:
    decision = L2PositionRiskSizer().size(
        _intent(purpose="exit", reduce_only=True, quantity=5.0, stop_loss_price=None),
        reference_price=50_000.0,
    )

    assert decision.accepted is True
    assert decision.quantity == 5.0
