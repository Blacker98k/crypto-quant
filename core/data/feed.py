"""ResearchFeed / LiveFeed 统一数据访问接口。"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol

from core.common.exceptions import InvalidQueryError
from core.data.exchange.base import Bar, FundingRate
from core.data.memory_cache import MemoryCache
from core.data.parquet_io import ParquetIO
from core.data.symbol import normalize_symbol


@dataclass(slots=True)
class Tick:
    """实时 tick。"""

    symbol: str
    price: float
    volume: float
    ts: int


@dataclass(slots=True)
class SubscriptionHandle:
    """订阅句柄。"""

    id: str
    symbol: str
    stream: str
    timeframe: str | None = None
    state: str = "active"
    callback: Callable | None = None


class DataFeed(Protocol):
    """策略层依赖的数据访问协议。"""

    def get_candles(
        self,
        symbol: str,
        timeframe: str,
        start_ms: int | None = None,
        end_ms: int | None = None,
        n: int | None = None,
    ) -> list[Bar]: ...

    def get_last_price(self, symbol: str) -> float: ...


class _WsSubscriberLike(Protocol):
    def subscribe_candles(
        self, symbol: str, timeframe: str, callback: Callable[[Bar], None]
    ) -> None: ...


class ResearchFeed:
    """研究模式：从 Parquet/SQLite 读取数据。"""

    def __init__(self, parquet_io: ParquetIO, repo: object) -> None:
        self._parquet_io = parquet_io
        self._repo = repo

    def get_candles(
        self,
        symbol: str,
        timeframe: str,
        start_ms: int | None = None,
        end_ms: int | None = None,
        n: int | None = None,
    ) -> list[Bar]:
        self._validate_query(start_ms, end_ms, n)
        return self._parquet_io.read_bars(symbol, timeframe, start_ms=start_ms, end_ms=end_ms, n=n)

    def get_funding_rates(
        self, symbol: str, start_ms: int | None = None, end_ms: int | None = None
    ) -> list[FundingRate]:
        return []

    def get_last_price(self, symbol: str) -> float:
        bars = self._parquet_io.read_bars(symbol, "1m", n=1)
        return bars[-1].c if bars else 0.0

    def list_universe(self, layer: str) -> list[str]:
        rows = self._repo.list_symbols(universe=layer) if hasattr(self._repo, "list_symbols") else []
        return [row["symbol"] for row in rows]

    def subscribe_candles(
        self, symbol: str, timeframe: str, callback: Callable[[Bar], None]
    ) -> SubscriptionHandle:
        raise NotImplementedError("ResearchFeed 不支持实时订阅")

    @staticmethod
    def _validate_query(start_ms: int | None, end_ms: int | None, n: int | None) -> None:
        if n is not None and (start_ms is not None or end_ms is not None):
            raise InvalidQueryError("n 与 start/end 不能混用")
        if n is None and start_ms is None and end_ms is None:
            raise InvalidQueryError("必须指定 n 或 start/end")


class LiveFeed(ResearchFeed):
    """实时/paper 模式：优先读内存缓存，缺失时回退 Parquet。"""

    def __init__(
        self,
        parquet_io: ParquetIO,
        repo: object,
        memory_cache: MemoryCache | None,
        ws_subscriber: _WsSubscriberLike | None = None,
    ) -> None:
        super().__init__(parquet_io, repo)
        self._cache = memory_cache
        self._ws_subscriber = ws_subscriber
        self._subscriptions: dict[str, SubscriptionHandle] = {}

    def get_candles(
        self,
        symbol: str,
        timeframe: str,
        start_ms: int | None = None,
        end_ms: int | None = None,
        n: int | None = None,
    ) -> list[Bar]:
        self._validate_query(start_ms, end_ms, n)
        if self._cache is not None and n is not None:
            cached = self._cache.get_bars(symbol, timeframe, n=n)
            if len(cached) >= n:
                return cached
        return self._parquet_io.read_bars(symbol, timeframe, start_ms=start_ms, end_ms=end_ms, n=n)

    def get_last_price(self, symbol: str) -> float:
        if self._cache is not None:
            price = self._cache.latest_price(symbol)
            if price is not None:
                return price
        return super().get_last_price(symbol)

    def subscribe_candles(
        self, symbol: str, timeframe: str, callback: Callable[[Bar], None]
    ) -> SubscriptionHandle:
        normalized = normalize_symbol(symbol)
        handle = SubscriptionHandle(
            id=self._subscription_id(normalized, "candles", timeframe),
            symbol=normalized,
            stream="candles",
            timeframe=timeframe,
            state="active",
            callback=callback,
        )
        self._subscriptions[handle.id] = handle
        if self._ws_subscriber is not None:
            self._ws_subscriber.subscribe_candles(normalized, timeframe, callback)
        return handle

    def unsubscribe(self, handle: SubscriptionHandle) -> None:
        """移除订阅句柄。"""
        for key, existing in list(self._subscriptions.items()):
            if existing.id == handle.id:
                del self._subscriptions[key]

    def _subscription_id(self, symbol: str, stream: str, timeframe: str | None = None) -> str:
        suffix = f":{timeframe}" if timeframe else ""
        return f"{symbol}:{stream}{suffix}:{len(self._subscriptions) + 1}"


__all__ = ["DataFeed", "LiveFeed", "ResearchFeed", "SubscriptionHandle", "Tick"]
