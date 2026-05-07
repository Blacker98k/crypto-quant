"""WebSocket subscriber parsing and lifecycle tests."""

from __future__ import annotations

from core.data.memory_cache import MemoryCache
from core.data.ws_subscriber import WsSubscriber


class FakeParquetIO:
    def __init__(self) -> None:
        self.writes = []

    def write_bars(self, bars) -> None:
        self.writes.extend(bars)


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


def test_stream_url_uses_spot_or_futures_endpoint() -> None:
    spot = WsSubscriber(MemoryCache(), FakeParquetIO())
    spot.subscribe_candles("BTCUSDT", "1m", lambda bar: None)

    assert spot._stream_url() == "wss://stream.binance.com:9443/stream?streams=btcusdt@kline_1m"

    class Exchange:
        market_type = "perp"

    perp = WsSubscriber(MemoryCache(), FakeParquetIO(), exchange=Exchange())
    perp.subscribe_candles("ETHUSDT", "4h", lambda bar: None)

    assert perp._stream_url() == "wss://fstream.binance.com/stream?streams=ethusdt@kline_4h"

