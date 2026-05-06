"""MemoryCache 测试——线程安全、K 线缓存、最新价。"""

from __future__ import annotations

import threading

from core.data.exchange.base import Bar
from core.data.memory_cache import MemoryCache


def _bar(ts=1000, closed=True):
    return Bar(symbol="BTCUSDT", timeframe="1h", ts=ts, o=100, h=110, l=90, c=105, v=1, q=105, closed=closed)


class TestPushBar:
    """push_bar 行为测试。"""

    def test_push_adds_bar(self):
        cache = MemoryCache()
        cache.push_bar(_bar(1000))
        assert cache.bar_count("BTCUSDT", "1h") == 1

    def test_push_closed_dedup_same_ts(self):
        cache = MemoryCache()
        cache.push_bar(_bar(1000, closed=True))
        cache.push_bar(_bar(1000, closed=True))
        assert cache.bar_count("BTCUSDT", "1h") == 1

    def test_push_unclosed_overwrites_same_ts(self):
        cache = MemoryCache()
        cache.push_bar(_bar(1000, closed=True))
        b2 = _bar(1000, closed=False)
        b2.c = 999
        cache.push_bar(b2)
        bars = cache.get_bars("BTCUSDT", "1h")
        assert bars[-1].c == 999
        assert not bars[-1].closed

    def test_push_respects_max_bars(self):
        cache = MemoryCache(max_bars=3)
        for i in range(5):
            cache.push_bar(Bar(symbol="BTCUSDT", timeframe="1h", ts=1000 + i * 3600, o=100, h=110, l=90, c=105, v=1, closed=True))
        assert cache.bar_count("BTCUSDT", "1h") == 3
        bars = cache.get_bars("BTCUSDT", "1h")
        assert bars[0].ts == 1000 + 2 * 3600  # 最旧的被挤掉


class TestGetBars:
    """get_bars 行为测试。"""

    def test_get_all(self):
        cache = MemoryCache()
        for i in range(5):
            cache.push_bar(Bar(symbol="BTCUSDT", timeframe="1h", ts=1000 + i * 3600, o=100, h=110, l=90, c=105, v=1, closed=True))
        bars = cache.get_bars("BTCUSDT", "1h")
        assert len(bars) == 5
        assert bars[0].ts == 1000

    def test_get_n(self):
        cache = MemoryCache()
        for i in range(10):
            cache.push_bar(Bar(symbol="BTCUSDT", timeframe="1h", ts=1000 + i * 3600, o=100, h=110, l=90, c=105, v=1, closed=True))
        bars = cache.get_bars("BTCUSDT", "1h", n=3)
        assert len(bars) == 3
        assert bars[-1].ts == 1000 + 9 * 3600

    def test_get_nonexistent(self):
        cache = MemoryCache()
        bars = cache.get_bars("ETHUSDT", "1h")
        assert bars == []


class TestLatestPrice:
    """最新价测试。"""

    def test_latest_price_initially_none(self):
        cache = MemoryCache()
        assert cache.latest_price("BTCUSDT") is None

    def test_update_and_get_latest_price(self):
        cache = MemoryCache()
        cache.update_latest_price("BTCUSDT", 55555.0)
        assert cache.latest_price("BTCUSDT") == 55555.0

    def test_push_closed_bar_updates_price(self):
        cache = MemoryCache()
        b = _bar(1000, closed=True)
        b.c = 50001.0
        cache.push_bar(b)
        assert cache.latest_price("BTCUSDT") == 50001.0

    def test_latest_prices_all(self):
        cache = MemoryCache()
        cache.update_latest_price("BTCUSDT", 50000)
        cache.update_latest_price("ETHUSDT", 3000)
        all_prices = cache.latest_prices_all()
        assert all_prices["BTCUSDT"] == 50000
        assert all_prices["ETHUSDT"] == 3000


class TestThreadSafety:
    """线程安全测试。"""

    def test_concurrent_push(self):
        cache = MemoryCache(max_bars=500)
        errors = []

        def push_n(thread_id, n):
            try:
                for i in range(n):
                    # 每个线程用不同的 ts 范围，避免去重导致计数不准
                    ts = 1000 + thread_id * 1000000 + i * 3600
                    cache.push_bar(Bar(symbol="BTCUSDT", timeframe="1h", ts=ts, o=100, h=110, l=90, c=105, v=1, closed=True))
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=push_n, args=(tid, 100)) for tid in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors, f"线程错误: {errors}"
        # 每个 bar ts 唯一（i * 3600），5 线程各 100 条 = 500，但 deque max 500
        assert cache.bar_count("BTCUSDT", "1h") == 500

    def test_concurrent_read_write(self):
        cache = MemoryCache()
        cache.update_latest_price("BTCUSDT", 50000)
        errors = []

        def worker():
            try:
                for _ in range(100):
                    cache.latest_price("BTCUSDT")
                    cache.get_bars("BTCUSDT", "1h")
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert not errors


class TestManagement:
    """缓存管理测试。"""

    def test_clear_symbol(self):
        cache = MemoryCache()
        cache.push_bar(_bar(1000))
        cache.push_bar(Bar(symbol="ETHUSDT", timeframe="1h", ts=1000, o=3000, h=3100, l=2900, c=3050, v=1, closed=True))
        cache.clear_symbol("BTCUSDT")
        assert cache.bar_count("BTCUSDT", "1h") == 0
        assert cache.bar_count("ETHUSDT", "1h") == 1

    def test_clear_symbol_all_timeframes(self):
        cache = MemoryCache()
        cache.push_bar(_bar(1000))
        cache.push_bar(Bar(symbol="BTCUSDT", timeframe="4h", ts=1000, o=100, h=110, l=90, c=105, v=1, closed=True))
        cache.clear_symbol("BTCUSDT")
        assert cache.symbol_count() == 0

    def test_clear_all(self):
        cache = MemoryCache()
        cache.push_bar(_bar(1000))
        cache.update_latest_price("BTCUSDT", 50000)
        cache.clear_all()
        assert cache.symbol_count() == 0
        assert cache.latest_price("BTCUSDT") is None

    def test_symbol_count(self):
        cache = MemoryCache()
        cache.push_bar(_bar(1000))
        cache.push_bar(Bar(symbol="ETHUSDT", timeframe="1h", ts=1000, o=3000, h=3100, l=2900, c=3050, v=1, closed=True))
        cache.push_bar(Bar(symbol="BTCUSDT", timeframe="4h", ts=1000, o=100, h=110, l=90, c=105, v=1, closed=True))
        assert cache.symbol_count() == 3
