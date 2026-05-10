"""WebSocket subscriber parsing and lifecycle tests."""

from __future__ import annotations

import asyncio
import json

import aiohttp

from core.data.exchange.base import Bar
from core.data.memory_cache import MemoryCache
from core.data.ws_subscriber import WsSubscriber, _default_client_session


class FakeParquetIO:
    def __init__(self) -> None:
        self.writes = []

    def write_bars(self, bars) -> None:
        self.writes.extend(bars)


class FakeMessage:
    def __init__(self, data: str) -> None:
        self.data = data


class DroppingWebSocket:
    def __init__(self, payloads: list[dict]) -> None:
        self.payloads = payloads
        self.closed = False

    async def __aiter__(self):
        for payload in self.payloads:
            yield FakeMessage(json.dumps(payload))
        raise RuntimeError("socket dropped")

    async def close(self) -> None:
        self.closed = True


class BlockingWebSocket:
    def __init__(self) -> None:
        self.closed = False

    async def __aiter__(self):
        while not self.closed:
            await asyncio.sleep(0.01)
        return
        yield

    async def close(self) -> None:
        self.closed = True


class FakeSession:
    def __init__(self, ws) -> None:
        self.ws = ws
        self.closed = False
        self.urls: list[str] = []

    async def ws_connect(self, url: str, heartbeat: int, proxy: str | None = None):
        self.urls.append(url)
        return self.ws

    async def close(self) -> None:
        self.closed = True


class FakeExchange:
    market_type = "perp"

    def __init__(self, bars: list[Bar]) -> None:
        self.bars = bars
        self.calls: list[tuple[str, str, int, int, int]] = []

    async def fetch_klines(
        self,
        symbol: str,
        timeframe: str,
        start_ms: int,
        end_ms: int,
        limit: int = 1000,
    ) -> list[Bar]:
        self.calls.append((symbol, timeframe, start_ms, end_ms, limit))
        return [
            bar
            for bar in self.bars
            if bar.symbol == symbol and bar.timeframe == timeframe and start_ms <= bar.ts < end_ms
        ]


def _kline_payload(*, closed: bool = True) -> dict:
    return {
        "stream": "btcusdt@kline_1m",
        "data": {
            "e": "kline",
            "s": "BTCUSDT",
            "k": {
                "t": 1700000000000,
                "i": "1m",
                "o": "100",
                "h": "110",
                "l": "90",
                "c": "105",
                "v": "2",
                "q": "210",
                "x": closed,
            },
        },
    }


def _mini_ticker_payload() -> dict:
    return {
        "stream": "btcusdt@miniTicker",
        "data": {
            "e": "24hrMiniTicker",
            "E": 1700000001234,
            "s": "BTCUSDT",
            "c": "106.25",
            "o": "100.00",
            "h": "109.00",
            "l": "98.50",
            "v": "250.5",
            "q": "26612.5",
        },
    }


async def _wait_for(predicate, timeout: float = 1.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("timed out waiting for predicate")


async def test_handle_closed_kline_updates_cache_writes_parquet_and_calls_back() -> None:
    cache = MemoryCache()
    parquet = FakeParquetIO()
    seen = []
    ws = WsSubscriber(cache, parquet)
    ws.subscribe_candles("BTCUSDT", "1m", seen.append)

    await ws._handle_payload(_kline_payload(closed=True))

    assert cache.latest_price("BTCUSDT") == 105
    assert cache.bar_count("BTCUSDT", "1m") == 1
    assert len(parquet.writes) == 1
    assert seen[0].closed is True


async def test_partial_kline_updates_cache_without_parquet_write() -> None:
    cache = MemoryCache()
    parquet = FakeParquetIO()
    ws = WsSubscriber(cache, parquet)

    await ws._handle_payload(_kline_payload(closed=False))

    assert cache.latest_price("BTCUSDT") == 105
    assert parquet.writes == []


async def test_mini_ticker_updates_latest_price_without_mutating_bars() -> None:
    cache = MemoryCache()
    parquet = FakeParquetIO()
    ws = WsSubscriber(cache, parquet)
    ws.subscribe_tickers(["BTCUSDT"])

    await ws._handle_payload(_mini_ticker_payload())

    assert cache.latest_price("BTCUSDT") == 106.25
    assert cache.latest_price_meta("BTCUSDT")["source_ts"] == 1700000001234
    assert cache.bar_count("BTCUSDT", "1m") == 0
    assert parquet.writes == []


async def test_default_session_uses_threaded_dns_resolver() -> None:
    session = _default_client_session()
    try:
        assert isinstance(session.connector._resolver, aiohttp.ThreadedResolver)
    finally:
        await session.close()


def test_stream_url_uses_spot_or_futures_endpoint() -> None:
    spot = WsSubscriber(MemoryCache(), FakeParquetIO())
    spot.subscribe_candles("BTCUSDT", "1m", lambda bar: None)

    assert spot._stream_url() == "wss://stream.binance.com:9443/stream?streams=btcusdt@kline_1m"

    class Exchange:
        market_type = "perp"

    perp = WsSubscriber(MemoryCache(), FakeParquetIO(), exchange=Exchange())
    perp.subscribe_candles("ETHUSDT", "4h", lambda bar: None)

    assert perp._stream_url() == "wss://fstream.binance.com/stream?streams=ethusdt@kline_4h"


def test_stream_url_can_include_ticker_streams() -> None:
    ws = WsSubscriber(MemoryCache(), FakeParquetIO())
    ws.subscribe_candles("BTCUSDT", "1m", lambda bar: None)
    ws.subscribe_tickers(["BTCUSDT", "ETHUSDT"])

    assert ws._stream_url() == (
        "wss://stream.binance.com:9443/stream?"
        "streams=btcusdt@kline_1m/btcusdt@miniTicker/ethusdt@miniTicker"
    )


async def test_reconnect_uses_rest_to_replay_missed_closed_bars() -> None:
    cache = MemoryCache()
    parquet = FakeParquetIO()
    seen = []
    missed = Bar(
        symbol="BTCUSDT",
        timeframe="1m",
        ts=1700000060000,
        o=105,
        h=112,
        l=101,
        c=110,
        v=3,
        q=330,
    )
    exchange = FakeExchange([missed])
    sessions = [
        FakeSession(DroppingWebSocket([_kline_payload(closed=True)])),
        FakeSession(BlockingWebSocket()),
    ]

    ws = WsSubscriber(
        cache,
        parquet,
        exchange=exchange,
        session_factory=lambda: sessions.pop(0),
        reconnect_delay_sec=0,
        clock_ms=lambda: 1700000120001,
    )
    ws.subscribe_candles("BTCUSDT", "1m", seen.append)

    await ws.connect()
    await _wait_for(lambda: len(exchange.calls) == 1 and len(seen) == 2)
    await ws.close()

    assert exchange.calls[0][2] == 1700000060000
    assert [bar.ts for bar in parquet.writes] == [1700000000000, 1700000060000]
    assert [bar.ts for bar in seen] == [1700000000000, 1700000060000]
    assert cache.latest_price("BTCUSDT") == 110
