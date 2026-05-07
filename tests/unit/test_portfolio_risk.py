"""L3 portfolio-risk validation tests."""

from __future__ import annotations

from core.execution.order_types import OrderIntent
from core.risk.portfolio_risk import L3PortfolioRiskValidator, PortfolioRiskLimits


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
        "client_order_id": "l3-test",
        "purpose": "entry",
    }
    defaults.update(kwargs)
    return OrderIntent(**defaults)


def _position(**kwargs) -> dict:
    defaults = {
        "symbol_id": 1,
        "qty": 0.1,
        "avg_entry_price": 50_000.0,
        "current_price": 50_000.0,
    }
    defaults.update(kwargs)
    return defaults


def test_l3_accepts_entry_inside_portfolio_limits() -> None:
    decision = L3PortfolioRiskValidator().validate(
        _intent(quantity=0.01),
        reference_price=50_000.0,
        open_positions=[],
        symbol_id=1,
    )

    assert decision.accepted is True


def test_l3_rejects_gross_leverage_breach() -> None:
    decision = L3PortfolioRiskValidator(
        PortfolioRiskLimits(equity=10_000, max_gross_leverage=3.0, max_symbol_notional_pct=1)
    ).validate(
        _intent(quantity=0.11),
        reference_price=50_000.0,
        open_positions=[_position(qty=0.5, current_price=50_000.0)],
        symbol_id=2,
    )

    assert decision.accepted is False
    assert decision.reason == "gross_leverage"


def test_l3_rejects_symbol_exposure_breach() -> None:
    decision = L3PortfolioRiskValidator(
        PortfolioRiskLimits(equity=10_000, max_gross_leverage=3.0, max_symbol_notional_pct=0.25)
    ).validate(
        _intent(quantity=0.01),
        reference_price=50_000.0,
        open_positions=[_position(qty=0.05, current_price=50_000.0)],
        symbol_id=1,
    )

    assert decision.accepted is False
    assert decision.reason == "symbol_exposure"


def test_l3_ignores_reduce_only_exits() -> None:
    decision = L3PortfolioRiskValidator().validate(
        _intent(purpose="exit", reduce_only=True, quantity=100),
        reference_price=50_000.0,
        open_positions=[_position(qty=1.0, current_price=50_000.0)],
        symbol_id=1,
    )

    assert decision.accepted is True
