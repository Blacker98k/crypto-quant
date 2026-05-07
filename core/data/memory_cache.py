"""线程安全内存行情缓存。"""

from __future__ import annotations

from collections import deque
from threading import RLock

from core.data.exchange.base import Bar


class MemoryCache:
    """保存最近 N 根 K 线和最新价。"""

    def __init__(self, max_bars: int = 1000) -> None:
        self._max_bars = max_bars
        self._bars: dict[tuple[str, str], deque[Bar]] = {}
        self._latest_prices: dict[str, float] = {}
        self._lock = RLock()

    def push_bar(self, bar: Bar) -> None:
        """插入或覆盖一根 K 线。"""
        key = (bar.symbol, bar.timeframe)
        with self._lock:
            dq = self._bars.setdefault(key, deque(maxlen=self._max_bars))
            if dq and dq[-1].ts == bar.ts:
                dq[-1] = bar
            else:
                dq.append(bar)
            self._latest_prices[bar.symbol] = bar.c

    def get_bars(self, symbol: str, timeframe: str, n: int | None = None) -> list[Bar]:
        """读取缓存 K 线。"""
        with self._lock:
            bars = list(self._bars.get((symbol, timeframe), ()))
        if n is None:
            return bars
        return bars[-n:]

    def bar_count(self, symbol: str, timeframe: str) -> int:
        """缓存中某 symbol/timeframe 的 bar 数量。"""
        with self._lock:
            return len(self._bars.get((symbol, timeframe), ()))

    def update_latest_price(self, symbol: str, price: float) -> None:
        """更新最新价。"""
        with self._lock:
            self._latest_prices[symbol] = price

    def latest_price(self, symbol: str) -> float | None:
        """读取最新价。"""
        with self._lock:
            return self._latest_prices.get(symbol)

    def latest_prices_all(self) -> dict[str, float]:
        """读取所有最新价快照。"""
        with self._lock:
            return dict(self._latest_prices)

    def clear_symbol(self, symbol: str) -> None:
        """清除某个 symbol 的所有 timeframe 缓存。"""
        with self._lock:
            for key in list(self._bars):
                if key[0] == symbol:
                    del self._bars[key]
            self._latest_prices.pop(symbol, None)

    def clear_all(self) -> None:
        """清空缓存。"""
        with self._lock:
            self._bars.clear()
            self._latest_prices.clear()

    def symbol_count(self) -> int:
        """返回缓存中的 symbol/timeframe 组合数量。"""
        with self._lock:
            return len(self._bars)


__all__ = ["MemoryCache"]
