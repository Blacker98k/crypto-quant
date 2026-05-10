"""Binance WebSocket candle subscriber."""

from __future__ import annotations

import asyncio
import contextlib
import json
import time
from collections.abc import Callable
from typing import Any, Protocol

from core.common.time_utils import tf_interval_ms
from core.data.exchange.base import Bar
from core.data.symbol import normalize_symbol


def _default_client_session() -> Any:
    import aiohttp

    connector = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver())
    return aiohttp.ClientSession(connector=connector)


class _CacheLike(Protocol):
    def push_bar(self, bar: Bar, *, update_latest: bool = True) -> None: ...
    def update_latest_price(self, symbol: str, price: float, source_ts: int | None = None) -> None: ...


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
        reconnect_delay_sec: float = 1.0,
        clock_ms: Callable[[], int] | None = None,
    ) -> None:
        self._cache = cache
        self._parquet_io = parquet_io
        self._exchange = exchange
        self._session_factory = session_factory
        self._reconnect_delay_sec = max(reconnect_delay_sec, 0)
        self._clock_ms = clock_ms or self._system_clock_ms
        self._running = False
        self._subscriptions: dict[tuple[str, str], list[Callable[[Bar], None]]] = {}
        self._ticker_symbols: set[str] = set()
        self._last_closed_ts: dict[tuple[str, str], int] = {}
        self._session: Any | None = None
        self._ws: Any | None = None
        self._reader_task: asyncio.Task[None] | None = None

    def subscribe_candles(self, symbol: str, timeframe: str, callback: Callable[[Bar], None]) -> None:
        """Register a candle callback."""
        key = (normalize_symbol(symbol), timeframe)
        self._subscriptions.setdefault(key, []).append(callback)

    def subscribe_tickers(self, symbols: list[str]) -> None:
        """Register mini ticker streams for faster latest-price updates."""
        self._ticker_symbols.update(normalize_symbol(symbol) for symbol in symbols)

    async def connect(self, proxy: str = "") -> None:
        """Open the combined-stream connection and start the reader task."""
        self._running = True
        if not self._subscriptions:
            return

        await self._open_connection(proxy)
        self._reader_task = asyncio.create_task(self._read_loop(proxy))

    async def _open_connection(self, proxy: str = "") -> None:
        """Open one combined-stream connection."""
        self._session = self._session_factory() if self._session_factory else _default_client_session()
        self._ws = await self._session.ws_connect(
            self._stream_url(),
            heartbeat=30,
            proxy=proxy or None,
        )

    async def close(self) -> None:
        """Close the WebSocket session and stop the reader task."""
        self._running = False
        if self._reader_task is not None:
            self._reader_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._reader_task
            self._reader_task = None
        await self._close_connection()

    async def _close_connection(self) -> None:
        if self._ws is not None:
            await self._maybe_await(self._ws.close())
            self._ws = None
        if self._session is not None:
            await self._maybe_await(self._session.close())
            self._session = None

    async def _read_loop(self, proxy: str = "") -> None:
        while self._running:
            try:
                if self._ws is None:
                    return
                async for msg in self._ws:
                    payload = msg.data
                    if isinstance(payload, str):
                        await self._handle_payload(json.loads(payload))
            except asyncio.CancelledError:
                raise
            except Exception:
                pass

            if not self._running:
                return
            await self._close_connection()
            if self._reconnect_delay_sec:
                await asyncio.sleep(self._reconnect_delay_sec)
            if not self._running:
                return
            await self._open_connection(proxy)
            await self._catch_up_closed_bars()

    async def _handle_payload(self, payload: dict[str, Any]) -> None:
        """Parse a Binance combined-stream kline payload."""
        data = payload.get("data", payload)
        if data.get("e") == "24hrMiniTicker":
            symbol = normalize_symbol(str(data.get("s") or ""))
            price = float(data.get("c") or 0)
            if symbol and price > 0:
                self._cache.update_latest_price(symbol, price, source_ts=int(data.get("E") or 0))
            return
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
        await self._publish_bar(bar)

    async def _catch_up_closed_bars(self) -> None:
        fetch_klines = getattr(self._exchange, "fetch_klines", None)
        if fetch_klines is None:
            return

        now_ms = self._clock_ms()
        for symbol, timeframe in sorted(self._subscriptions):
            last_ts = self._last_closed_ts.get((symbol, timeframe))
            if last_ts is None:
                continue
            start_ms = last_ts + tf_interval_ms(timeframe)
            if start_ms >= now_ms:
                continue
            bars = await fetch_klines(symbol, timeframe, start_ms, now_ms, limit=1000)
            for bar in self._normalize_recovered_bars(bars, symbol, timeframe, start_ms, now_ms):
                await self._publish_bar(bar)

    async def _publish_bar(self, bar: Bar) -> None:
        self._cache.push_bar(bar, update_latest=bar.timeframe == "1m")
        if bar.closed:
            key = (bar.symbol, bar.timeframe)
            self._last_closed_ts[key] = max(self._last_closed_ts.get(key, bar.ts), bar.ts)
            self._parquet_io.write_bars([bar])
        for callback in self._subscriptions.get((bar.symbol, bar.timeframe), []):
            callback(bar)

    @staticmethod
    def _normalize_recovered_bars(
        bars: list[Bar],
        symbol: str,
        timeframe: str,
        start_ms: int,
        end_ms: int,
    ) -> list[Bar]:
        deduped: dict[int, Bar] = {}
        for bar in bars:
            if bar.closed and start_ms <= bar.ts < end_ms:
                deduped[bar.ts] = Bar(
                    symbol=symbol,
                    timeframe=timeframe,
                    ts=bar.ts,
                    o=bar.o,
                    h=bar.h,
                    l=bar.l,
                    c=bar.c,
                    v=bar.v,
                    q=bar.q,
                    closed=True,
                )
        return [deduped[ts] for ts in sorted(deduped)]

    def _stream_url(self) -> str:
        streams = [
            f"{symbol.lower()}@kline_{timeframe}"
            for symbol, timeframe in sorted(self._subscriptions)
        ]
        streams.extend(f"{symbol.lower()}@miniTicker" for symbol in sorted(self._ticker_symbols))
        base = (
            "wss://fstream.binance.com/stream"
            if getattr(self._exchange, "market_type", "spot") == "perp"
            else "wss://stream.binance.com:9443/stream"
        )
        return f"{base}?streams={'/'.join(streams)}"

    @staticmethod
    async def _maybe_await(value: Any) -> None:
        if hasattr(value, "__await__"):
            await value

    @staticmethod
    def _system_clock_ms() -> int:
        return int(time.time() * 1000)


__all__ = ["WsSubscriber"]
