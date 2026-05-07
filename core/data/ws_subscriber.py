"""Binance WebSocket candle subscriber."""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import Callable
from typing import Any, Protocol

from core.data.exchange.base import Bar
from core.data.symbol import normalize_symbol


class _CacheLike(Protocol):
    def push_bar(self, bar: Bar) -> None: ...


class _ParquetLike(Protocol):
    def write_bars(self, bars: list[Bar]) -> None: ...


class WsSubscriber:
    """Subscribe to Binance kline streams and fan out normalized bars."""

    def __init__(
        self,
        cache: _CacheLike,
        parquet_io: _ParquetLike,
        exchange: object | None = None,
        session_factory: Callable[[], Any] | None = None,
    ) -> None:
        self._cache = cache
        self._parquet_io = parquet_io
        self._exchange = exchange
        self._session_factory = session_factory
        self._running = False
        self._subscriptions: dict[tuple[str, str], list[Callable[[Bar], None]]] = {}
        self._session: Any | None = None
        self._ws: Any | None = None
        self._reader_task: asyncio.Task[None] | None = None

    def subscribe_candles(self, symbol: str, timeframe: str, callback: Callable[[Bar], None]) -> None:
        """Register a candle callback."""
        key = (normalize_symbol(symbol), timeframe)
        self._subscriptions.setdefault(key, []).append(callback)

    async def connect(self, proxy: str = "") -> None:
        """Open the combined-stream connection and start the reader task."""
        self._running = True
        if not self._subscriptions:
            return

        import aiohttp

        self._session = self._session_factory() if self._session_factory else aiohttp.ClientSession()
        self._ws = await self._session.ws_connect(
            self._stream_url(),
            heartbeat=30,
            proxy=proxy or None,
        )
        self._reader_task = asyncio.create_task(self._read_loop())

    async def close(self) -> None:
        """Close the WebSocket session and stop the reader task."""
        self._running = False
        if self._reader_task is not None:
            self._reader_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._reader_task
            self._reader_task = None
        if self._ws is not None:
            await self._maybe_await(self._ws.close())
            self._ws = None
        if self._session is not None:
            await self._maybe_await(self._session.close())
            self._session = None

    async def _read_loop(self) -> None:
        if self._ws is None:
            return
        async for msg in self._ws:
            payload = msg.data
            if isinstance(payload, str):
                await self._handle_payload(json.loads(payload))

    async def _handle_payload(self, payload: dict[str, Any]) -> None:
        """Parse a Binance combined-stream kline payload."""
        data = payload.get("data", payload)
        if data.get("e") != "kline":
            return
        kline = data.get("k") or {}
        symbol = normalize_symbol(str(data.get("s") or kline.get("s") or ""))
        timeframe = str(kline.get("i") or "")
        if not symbol or not timeframe:
            return

        bar = Bar(
            symbol=symbol,
            timeframe=timeframe,
            ts=int(kline["t"]),
            o=float(kline["o"]),
            h=float(kline["h"]),
            l=float(kline["l"]),
            c=float(kline["c"]),
            v=float(kline["v"]),
            q=float(kline.get("q") or 0),
            closed=bool(kline.get("x")),
        )
        self._cache.push_bar(bar)
        if bar.closed:
            self._parquet_io.write_bars([bar])
        for callback in self._subscriptions.get((bar.symbol, bar.timeframe), []):
            callback(bar)

    def _stream_url(self) -> str:
        streams = "/".join(
            f"{symbol.lower()}@kline_{timeframe}"
            for symbol, timeframe in sorted(self._subscriptions)
        )
        base = (
            "wss://fstream.binance.com/stream"
            if getattr(self._exchange, "market_type", "spot") == "perp"
            else "wss://stream.binance.com:9443/stream"
        )
        return f"{base}?streams={streams}"

    @staticmethod
    async def _maybe_await(value: Any) -> None:
        if hasattr(value, "__await__"):
            await value


__all__ = ["WsSubscriber"]
