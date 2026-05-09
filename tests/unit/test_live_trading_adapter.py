from __future__ import annotations

import pytest

from core.execution.order_types import OrderIntent
from core.live.trading_adapter import BinanceSpotCredentials, BinanceSpotTradingAdapter


class FakePrivateCcxtClient:
    def __init__(self) -> None:
        self.markets = {
            "BTC/USDT": {
                "id": "BTCUSDT",
                "symbol": "BTC/USDT",
                "base": "BTC",
                "quote": "USDT",
                "type": "spot",
                "active": True,
                "precision": {"price": 0.01, "amount": 0.000001},
                "limits": {"cost": {"min": 10}},
            }
        }
        self.loaded = False
        self.created_orders: list[tuple] = []
        self.cancel_calls: list[tuple] = []
        self.fetch_calls: list[tuple] = []

    def load_markets(self) -> dict:
        self.loaded = True
        return self.markets

    def create_order(
        self,
        symbol: str,
        type_: str,
        side: str,
        amount: float,
        price: float | None = None,
        params: dict | None = None,
    ) -> dict:
        self.created_orders.append((symbol, type_, side, amount, price, params or {}))
        return {
            "id": "exchange-order-id",
            "clientOrderId": (params or {}).get("newClientOrderId"),
            "status": "open",
            "timestamp": 1_700_000_000_000,
        }

    def cancel_order(self, id_: str | None, symbol: str, params: dict | None = None) -> dict:
        self.cancel_calls.append((id_, symbol, params or {}))
        return {"clientOrderId": (params or {}).get("origClientOrderId"), "status": "canceled"}

    def fetch_order(self, id_: str | None, symbol: str, params: dict | None = None) -> dict:
        self.fetch_calls.append((id_, symbol, params or {}))
        return {"id": id_ or "by-client-id", "clientOrderId": (params or {}).get("origClientOrderId"), "status": "closed"}

    def fetch_balance(self) -> dict:
        return {"free": {"USDT": 1.0}, "used": {"USDT": 0.0}, "total": {"USDT": 1.0}}


def test_credentials_load_from_environment_without_defaults() -> None:
    credentials = BinanceSpotCredentials.from_env(
        {
            "BINANCE_SPOT_API_KEY": "key",
            "BINANCE_SPOT_API_SECRET": "secret",
        }
    )

    assert credentials.api_key == "key"
    assert credentials.api_secret == "secret"


def test_credentials_reject_missing_environment_values() -> None:
    with pytest.raises(ValueError, match="missing Binance spot credentials"):
        BinanceSpotCredentials.from_env({})


@pytest.mark.asyncio
async def test_spot_adapter_places_market_order_with_client_id() -> None:
    client = FakePrivateCcxtClient()
    adapter = BinanceSpotTradingAdapter(client=client)

    handle = await adapter.place_order(
        OrderIntent(
            signal_id=1,
            strategy="manual_small_live",
            strategy_version="v1",
            symbol="BTCUSDT",
            side="buy",
            order_type="market",
            quantity=0.001,
            client_order_id="client-1",
        ),
        now_ms=1_700_000_000_000,
    )

    assert handle.status == "accepted"
    assert handle.exchange_order_id == "exchange-order-id"
    assert client.created_orders == [
        (
            "BTC/USDT",
            "market",
            "buy",
            0.001,
            None,
            {"newClientOrderId": "client-1"},
        )
    ]


@pytest.mark.asyncio
async def test_spot_adapter_cancels_and_queries_by_client_id() -> None:
    client = FakePrivateCcxtClient()
    adapter = BinanceSpotTradingAdapter(client=client)

    cancel = await adapter.cancel_order("BTCUSDT", client_order_id="client-1")
    handle = await adapter.fetch_order("BTCUSDT", client_order_id="client-1")

    assert cancel.success is True
    assert handle.status == "filled"
    assert client.cancel_calls == [(None, "BTC/USDT", {"origClientOrderId": "client-1"})]
    assert client.fetch_calls == [(None, "BTC/USDT", {"origClientOrderId": "client-1"})]
