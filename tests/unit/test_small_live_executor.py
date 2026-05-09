from __future__ import annotations

import pytest

from core.common.exceptions import OMSDegraded, RiskBlockedError
from core.execution.order_types import OrderHandle, OrderIntent
from core.live.executor import SmallLiveExecutor
from core.live.small_live import ReadinessReport, SmallLiveConfig


class FakeLiveAdapter:
    def __init__(self) -> None:
        self.intents: list[OrderIntent] = []

    async def place_order(self, intent: OrderIntent, *, now_ms: int) -> OrderHandle:
        self.intents.append(intent)
        return OrderHandle(
            client_order_id=intent.client_order_id,
            exchange_order_id=f"ex-{len(self.intents)}",
            status="accepted",
            submitted_at=now_ms,
        )


def _config() -> SmallLiveConfig:
    return SmallLiveConfig(
        enabled=True,
        mode="small_live",
        environment="production",
        exchange="binance_spot",
        allow_futures=False,
        allow_margin=False,
        allow_withdrawals=False,
        max_total_quote_usdt=float(10 * 4),
        max_order_quote_usdt=float(len("order")),
        max_daily_loss_usdt=float(len("cap")),
        max_open_positions=1,
        allowed_symbols=("BTCUSDT",),
        kill_switch_enabled=True,
        reconciliation_required=True,
    )


def _ready_report() -> ReadinessReport:
    return ReadinessReport(
        ready=True,
        blockers=[],
        warnings=[],
        budget_limits_configured=True,
    )


def _intent(**overrides: object) -> OrderIntent:
    payload = {
        "signal_id": 1,
        "strategy": "manual_small_live",
        "strategy_version": "v1",
        "symbol": "BTCUSDT",
        "side": "buy",
        "order_type": "market",
        "quantity": 0.001,
        "purpose": "entry",
        "stop_loss_price": 49_000.0,
        "client_order_id": "live-entry-1",
    }
    payload.update(overrides)
    return OrderIntent(**payload)


@pytest.mark.asyncio
async def test_executor_blocks_when_readiness_report_is_not_ready() -> None:
    adapter = FakeLiveAdapter()
    executor = SmallLiveExecutor(
        adapter=adapter,
        config=_config(),
        readiness=ReadinessReport(
            ready=False,
            blockers=["paper_not_running"],
            warnings=[],
            budget_limits_configured=True,
        ),
    )

    with pytest.raises(OMSDegraded, match="small_live_not_ready"):
        await executor.submit_order(_intent(), now_ms=1_700_000_000_000)

    assert adapter.intents == []


@pytest.mark.asyncio
async def test_executor_blocks_symbols_outside_allowlist() -> None:
    adapter = FakeLiveAdapter()
    executor = SmallLiveExecutor(adapter=adapter, config=_config(), readiness=_ready_report())

    with pytest.raises(RiskBlockedError, match="symbol_not_allowed"):
        await executor.submit_order(_intent(symbol="ETHUSDT"), now_ms=1_700_000_000_000)

    assert adapter.intents == []


@pytest.mark.asyncio
async def test_executor_requires_stop_loss_for_entry_orders() -> None:
    adapter = FakeLiveAdapter()
    executor = SmallLiveExecutor(adapter=adapter, config=_config(), readiness=_ready_report())

    with pytest.raises(RiskBlockedError, match="entry_stop_loss_required"):
        await executor.submit_order(_intent(stop_loss_price=None), now_ms=1_700_000_000_000)

    assert adapter.intents == []


@pytest.mark.asyncio
async def test_executor_places_protective_stop_after_entry_order() -> None:
    adapter = FakeLiveAdapter()
    executor = SmallLiveExecutor(adapter=adapter, config=_config(), readiness=_ready_report())

    result = await executor.submit_order(_intent(), now_ms=1_700_000_000_000)

    assert result.entry.exchange_order_id == "ex-1"
    assert result.stop_loss is not None
    assert result.stop_loss.exchange_order_id == "ex-2"
    assert [intent.purpose for intent in adapter.intents] == ["entry", "stop_loss"]
    stop = adapter.intents[1]
    assert stop.side == "sell"
    assert stop.order_type == "stop"
    assert stop.stop_price == 49_000.0
    assert stop.client_order_id == "live-entry-1-sl"
