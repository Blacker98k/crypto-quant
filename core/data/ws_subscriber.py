"""WebSocket 订阅器占位实现。

真实 Binance WS 将在 P1.3 完整实现；当前类提供 Dashboard/PaperRunner 所需的
生命周期接口，避免上层 import 失败。
"""

from __future__ import annotations

from collections.abc import Callable

from core.data.exchange.base import Bar


class WsSubscriber:
    """轻量 WS 订阅器壳。"""

    def __init__(self, cache: object, parquet_io: object, exchange: object | None = None) -> None:
        self._cache = cache
        self._parquet_io = parquet_io
        self._exchange = exchange
        self._running = False
        self._subscriptions: list[tuple[str, str, Callable[[Bar], None]]] = []

    def subscribe_candles(self, symbol: str, timeframe: str, callback: Callable[[Bar], None]) -> None:
        """登记 K 线订阅。"""
        self._subscriptions.append((symbol, timeframe, callback))

    async def connect(self, proxy: str = "") -> None:
        """启动连接。当前占位实现只标记 running。"""
        self._running = True

    async def close(self) -> None:
        """关闭连接。"""
        self._running = False


__all__ = ["WsSubscriber"]
