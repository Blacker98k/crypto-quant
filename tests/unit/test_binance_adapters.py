"""Binance exchange adapters using fake clients, without network access."""

from __future__ import annotations

import pytest

from core.data.exchange.binance_spot import BinanceSpotAdapter
from core.data.exchange.binance_usdm import BinanceUsdmAdapter


class FakeCcxtClient:
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
                "info": {"onboardDate": "1600000000000"},
            },
            "ETH/USDT:USDT": {
                "id": "ETHUSDT",
                "symbol": "ETH/USDT:USDT",
                "base": "ETH",
                "quote": "USDT",
                "type": "swap",
                "active": True,
                "precision": {"price": 0.01, "amount": 0.001},
                "limits": {"cost": {"min": 5}},
                "info": {"onboardDate": "1610000000000"},
            },
        }
        self.loaded = False
        self.closed = False
        self.ohlcv_calls: list[tuple[str, str, int, int | None]] = []

    def load_markets(self) -> dict:
        self.loaded = True
        return self.markets

    def fetch_ohlcv(self, symbol: str, timeframe: str, since: int, limit: int | None = None, params: dict | None = None) -> list[list[float]]:
        self.ohlcv_calls.append((symbol, timeframe, since, limit))
        if since >= 1700007200000:
            return []
        return [
            [since, 100, 110, 90, 105, 2],
            [since + 3_600_000, 105, 115, 95, 111, 3],
        ]

    def fetch_tickers(self) -> dict:
        return {
            "BTC/USDT": {"symbol": "BTC/USDT", "last": 50_000, "quoteVolume": 123_000, "timestamp": 1700000000000}
        }

    def fetch_funding_rate_history(self, symbol: str, since: int, limit: int | None = None, params: dict | None = None) -> list[dict]:
        return [
            {"symbol": symbol, "timestamp": since, "fundingRate": "0.0001"},
            {"symbol": symbol, "timestamp": since + 28_800_000, "fundingRate": "-0.0002"},
        ]

    def fetch_open_interest_history(self, symbol: str, timeframe: str = "5m", since: int | None = None, limit: int | None = None, params: dict | None = None) -> list[dict]:
        start = since or 0
        return [
            {"symbol": symbol, "timestamp": start, "sumOpenInterest": "1000"},
            {"symbol": symbol, "timestamp": start + 300_000, "sumOpenInterest": "1100"},
        ]

    def close(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_spot_fetch_exchange_info_normalizes_symbols() -> None:
    client = FakeCcxtClient()
    adapter = BinanceSpotAdapter(client=client)

    symbols = await adapter.fetch_exchange_info()

    assert client.loaded is True
    assert [s.symbol for s in symbols] == ["BTCUSDT"]
    assert symbols[0].stype == "spot"
    assert symbols[0].tick_size == 0.01


@pytest.mark.asyncio
async def test_fetch_klines_maps_internal_symbol_and_end_time() -> None:
    client = FakeCcxtClient()
    adapter = BinanceSpotAdapter(client=client)

    bars = await adapter.fetch_klines("BTCUSDT", "1h", 1700000000000, 1700007200000, limit=1000)

    assert len(bars) == 2
    assert bars[0].symbol == "BTCUSDT"
    assert bars[0].q == 210
    assert client.ohlcv_calls[0][0] == "BTC/USDT"


@pytest.mark.asyncio
async def test_fetch_24h_tickers() -> None:
    adapter = BinanceSpotAdapter(client=FakeCcxtClient())

    tickers = await adapter.fetch_24h_tickers()

    assert len(tickers) == 1
    assert tickers[0].symbol == "BTCUSDT"
    assert tickers[0].quote_volume == 123_000


@pytest.mark.asyncio
async def test_usdm_funding_and_open_interest() -> None:
    adapter = BinanceUsdmAdapter(client=FakeCcxtClient())

    funding = await adapter.fetch_funding_rates("ETHUSDT", 1700000000000, 1700100000000)
    oi = await adapter.fetch_open_interest("ETHUSDT", 1700000000000, 1700100000000)

    assert [row.rate for row in funding] == [0.0001, -0.0002]
    assert [row.oi for row in oi] == [1000.0, 1100.0]

