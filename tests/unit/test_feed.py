"""DataFeed 测试——ResearchFeed / LiveFeed 行为。"""

from __future__ import annotations

import pytest

from core.common.exceptions import InvalidQueryError
from core.data.exchange.base import Bar
from core.data.feed import LiveFeed, ResearchFeed, SubscriptionHandle, Tick


class TestResearchFeed:
    """ResearchFeed 测试。"""

    def test_get_candles_range(self, parquet_io, sqlite_repo):
        bars = [
            Bar(symbol="BTCUSDT", timeframe="1h", ts=1700000000000, o=100, h=110, l=90, c=105, v=1, closed=True),
            Bar(symbol="BTCUSDT", timeframe="1h", ts=1700003600000, o=105, h=115, l=95, c=110, v=2, closed=True),
        ]
        parquet_io.write_bars(bars)
        feed = ResearchFeed(parquet_io, sqlite_repo)
        result = feed.get_candles("BTCUSDT", "1h", n=2)
        assert len(result) == 2

    def test_get_candles_n_and_start_mutex(self, parquet_io, sqlite_repo):
        feed = ResearchFeed(parquet_io, sqlite_repo)
        with pytest.raises(InvalidQueryError):
            feed.get_candles("BTCUSDT", "1h", n=10, start_ms=1000)

    def test_get_candles_no_args_raises(self, parquet_io, sqlite_repo):
        feed = ResearchFeed(parquet_io, sqlite_repo)
        with pytest.raises(InvalidQueryError):
            feed.get_candles("BTCUSDT", "1h")

    def test_get_last_price(self, parquet_io, sqlite_repo):
        from core.data.exchange.base import Bar
        parquet_io.write_bars([Bar(symbol="BTCUSDT", timeframe="1m", ts=1700000000000, o=50000, h=51000, l=49000, c=50555, v=1, closed=True)])
        feed = ResearchFeed(parquet_io, sqlite_repo)
        assert feed.get_last_price("BTCUSDT") == 50555

    def test_get_last_price_empty(self, parquet_io, sqlite_repo):
        feed = ResearchFeed(parquet_io, sqlite_repo)
        assert feed.get_last_price("NOEXIST") == 0.0

    def test_list_universe_empty(self, parquet_io, sqlite_repo):
        feed = ResearchFeed(parquet_io, sqlite_repo)
        result = feed.list_universe("core")
        assert result == []

    def test_subscribe_not_supported(self, parquet_io, sqlite_repo):
        feed = ResearchFeed(parquet_io, sqlite_repo)
        with pytest.raises(NotImplementedError):
            feed.subscribe_candles("BTCUSDT", "1h", lambda b: None)


class TestLiveFeed:
    """LiveFeed 测试。"""

    def test_get_candles_fallback_parquet(self, parquet_io, sqlite_repo):
        bars = [
            Bar(symbol="BTCUSDT", timeframe="1h", ts=1700000000000, o=100, h=110, l=90, c=105, v=1, closed=True),
        ]
        parquet_io.write_bars(bars)
        feed = LiveFeed(parquet_io, sqlite_repo, memory_cache=None)
        result = feed.get_candles("BTCUSDT", "1h", n=1)
        assert len(result) == 1
        assert result[0].c == 105

    def test_get_candles_with_cache(self, parquet_io, sqlite_repo):
        from core.data.memory_cache import MemoryCache
        cache = MemoryCache()
        cache.push_bar(Bar(symbol="BTCUSDT", timeframe="1h", ts=1700000000000, o=100, h=110, l=90, c=105, v=1, closed=True))
        cache.push_bar(Bar(symbol="BTCUSDT", timeframe="1h", ts=1700003600000, o=105, h=115, l=95, c=110, v=2, closed=True))

        feed = LiveFeed(parquet_io, sqlite_repo, memory_cache=cache)
        result = feed.get_candles("BTCUSDT", "1h", n=2)
        assert len(result) == 2
        assert result[-1].c == 110

    def test_get_last_price_from_cache(self, parquet_io, sqlite_repo):
        from core.data.memory_cache import MemoryCache
        cache = MemoryCache()
        cache.update_latest_price("BTCUSDT", 67890)
        feed = LiveFeed(parquet_io, sqlite_repo, memory_cache=cache)
        assert feed.get_last_price("BTCUSDT") == 67890

    def test_get_last_price_fallback_parquet(self, parquet_io, sqlite_repo):
        from core.data.exchange.base import Bar
        parquet_io.write_bars([Bar(symbol="ETHUSDT", timeframe="1m", ts=1700000000000, o=3000, h=3100, l=2900, c=3050, v=1, closed=True)])
        feed = LiveFeed(parquet_io, sqlite_repo, memory_cache=None)
        assert feed.get_last_price("ETHUSDT") == 3050

    def test_unsubscribe_removes_handle(self, parquet_io, sqlite_repo):
        feed = LiveFeed(parquet_io, sqlite_repo, memory_cache=None)
        handle = SubscriptionHandle(id="test", symbol="BTCUSDT", stream="candles", timeframe="1h")
        feed._subscriptions["BTCUSDT:candles:1h"] = handle
        feed.unsubscribe(handle)
        assert "BTCUSDT:candles:1h" not in feed._subscriptions

    def test_subscribe_candles_registers_handle_and_delegates_to_ws(self, parquet_io, sqlite_repo):
        class FakeWsSubscriber:
            def __init__(self):
                self.calls = []

            def subscribe_candles(self, symbol, timeframe, callback):
                self.calls.append((symbol, timeframe, callback))

        ws = FakeWsSubscriber()

        def callback(bar):
            return None

        feed = LiveFeed(parquet_io, sqlite_repo, memory_cache=None, ws_subscriber=ws)

        handle = feed.subscribe_candles("btcusdt", "1m", callback)

        assert handle.symbol == "BTCUSDT"
        assert handle.stream == "candles"
        assert handle.timeframe == "1m"
        assert handle.state == "active"
        assert ws.calls == [("BTCUSDT", "1m", callback)]
        assert feed._subscriptions[handle.id] is handle


class TestSubscriptionHandle:
    """SubscriptionHandle 测试。"""

    def test_handle_attributes(self):
        h = SubscriptionHandle(id="abc", symbol="BTCUSDT", stream="candles", timeframe="1h")
        assert h.id == "abc"
        assert h.symbol == "BTCUSDT"
        assert h.stream == "candles"
        assert h.timeframe == "1h"


class TestTick:
    """Tick 数据类测试。"""

    def test_tick_creation(self):
        t = Tick(symbol="BTCUSDT", price=50000.0, volume=100.5, ts=1700000000000)
        assert t.symbol == "BTCUSDT"
        assert t.price == 50000.0
        assert t.volume == 100.5
