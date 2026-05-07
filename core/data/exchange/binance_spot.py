"""Binance Spot REST adapter.

The adapter wraps the synchronous ccxt client behind async methods so callers can
use a single async data-layer contract. Tests inject a fake client and never
touch the network.
"""

from __future__ import annotations

import asyncio
from typing import Any

from core.data.exchange.base import Bar, SymbolInfo, Ticker24h
from core.data.symbol import normalize_symbol


class BinanceSpotAdapter:
    """Binance spot adapter backed by ccxt."""

    name = "binance"
    market_type = "spot"
    _ccxt_exchange_name = "binance"

    def __init__(
        self,
        proxy: str = "",
        timeout_ms: int = 30_000,
        client: Any | None = None,
    ) -> None:
        self.proxy = proxy
        self.timeout_ms = timeout_ms
        self._ex = client
        self._markets_loaded = False
        self._symbol_to_ccxt: dict[str, str] = {}

    async def fetch_exchange_info(self) -> list[SymbolInfo]:
        """Return normalized exchange symbols for this adapter's market type."""
        await self._ensure_markets_loaded()
        client = self._client()
        symbols: list[SymbolInfo] = []
        for market in client.markets.values():
            if not self._is_target_market(market):
                continue
            internal = self._market_to_internal_symbol(market)
            symbols.append(
                SymbolInfo(
                    exchange=self.name,
                    symbol=internal,
                    base=str(market.get("base") or ""),
                    quote=str(market.get("quote") or ""),
                    stype=self.market_type,
                    tick_size=self._market_tick_size(market),
                    lot_size=self._market_lot_size(market),
                    min_notional=self._market_min_notional(market),
                    listed_at=self._market_listed_at(market),
                    delisted_at=None,
                )
            )
        return symbols

    async def fetch_24h_tickers(self) -> list[Ticker24h]:
        """Fetch 24h tickers and normalize their symbols."""
        await self._ensure_markets_loaded()
        client = self._client()
        raw = await self._call(client.fetch_tickers)
        tickers: list[Ticker24h] = []
        for ccxt_symbol, row in raw.items():
            if self._ccxt_symbol_market_type(ccxt_symbol) != self.market_type:
                continue
            internal = self._to_internal_symbol(str(row.get("symbol") or ccxt_symbol))
            tickers.append(
                Ticker24h(
                    symbol=internal,
                    last_price=float(row.get("last") or 0),
                    quote_volume=float(row.get("quoteVolume") or row.get("baseVolume") or 0),
                    ts=int(row.get("timestamp") or 0),
                )
            )
        return tickers

    async def fetch_klines(
        self,
        symbol: str,
        timeframe: str,
        start_ms: int,
        end_ms: int,
        limit: int = 1000,
    ) -> list[Bar]:
        """Fetch OHLCV bars in ccxt pages and stop before ``end_ms``."""
        await self._ensure_markets_loaded()
        client = self._client()
        ccxt_symbol = self._to_ccxt_symbol(symbol)
        bars: list[Bar] = []
        cursor = start_ms
        page_limit = min(max(limit, 1), 1000)

        while cursor < end_ms:
            rows = await self._call(client.fetch_ohlcv, ccxt_symbol, timeframe, cursor, page_limit)
            if not rows:
                break
            advanced = False
            for ts, open_, high, low, close, volume, *rest in rows:
                bar_ts = int(ts)
                if bar_ts >= end_ms:
                    return bars
                quote_volume = float(rest[0]) if rest else float(volume) * float(close)
                bars.append(
                    Bar(
                        symbol=normalize_symbol(symbol),
                        timeframe=timeframe,
                        ts=bar_ts,
                        o=float(open_),
                        h=float(high),
                        l=float(low),
                        c=float(close),
                        v=float(volume),
                        q=quote_volume,
                        closed=True,
                    )
                )
                next_cursor = bar_ts + 1
                if next_cursor > cursor:
                    cursor = next_cursor
                    advanced = True
            if not advanced or len(rows) < page_limit:
                break
        return bars

    async def _ensure_markets_loaded(self) -> None:
        """Load ccxt markets once."""
        if self._ex is None:
            self._ex = self._build_client()
        client = self._client()
        if self._markets_loaded:
            return
        await self._call(client.load_markets)
        self._symbol_to_ccxt.clear()
        for ccxt_symbol, market in client.markets.items():
            internal = self._market_to_internal_symbol(market)
            self._symbol_to_ccxt[internal] = ccxt_symbol
        self._markets_loaded = True

    async def close(self) -> None:
        """Close client resources when the underlying client supports it."""
        if self._ex is not None and hasattr(self._ex, "close"):
            await self._call(self._ex.close)

    def _build_client(self) -> Any:
        import ccxt  # type: ignore[import-untyped]

        cls = getattr(ccxt, self._ccxt_exchange_name)
        options: dict[str, Any] = {
            "enableRateLimit": True,
            "timeout": self.timeout_ms,
        }
        if self.proxy:
            options["proxies"] = {"http": self.proxy, "https": self.proxy}
        return cls(options)

    async def _call(self, fn: Any, *args: Any, **kwargs: Any) -> Any:
        return await asyncio.to_thread(fn, *args, **kwargs)

    def _client(self) -> Any:
        if self._ex is None:
            raise RuntimeError("exchange client is not initialized")
        return self._ex

    def _is_target_market(self, market: dict[str, Any]) -> bool:
        if not bool(market.get("active", True)):
            return False
        return str(market.get("type") or "").lower() == "spot"

    def _ccxt_symbol_market_type(self, ccxt_symbol: str) -> str | None:
        market = self._client().markets.get(ccxt_symbol)
        if market is None:
            return None
        if str(market.get("type") or "").lower() == "spot":
            return "spot"
        return "perp" if bool(market.get("swap")) or str(market.get("type")) == "swap" else None

    def _to_ccxt_symbol(self, symbol: str) -> str:
        internal = normalize_symbol(symbol)
        if internal in self._symbol_to_ccxt:
            return self._symbol_to_ccxt[internal]
        return self._display_symbol(internal)

    @staticmethod
    def _to_internal_symbol(ccxt_symbol: str) -> str:
        return normalize_symbol(ccxt_symbol.split(":")[0])

    def _market_to_internal_symbol(self, market: dict[str, Any]) -> str:
        symbol = str(market.get("symbol") or market.get("id") or "")
        return self._to_internal_symbol(symbol)

    @staticmethod
    def _display_symbol(symbol: str) -> str:
        normalized = normalize_symbol(symbol)
        for quote in ("USDT", "USDC", "BUSD", "FDUSD", "BTC", "ETH", "BNB"):
            if normalized.endswith(quote) and normalized != quote:
                return f"{normalized[:-len(quote)]}/{quote}"
        return normalized

    @staticmethod
    def _market_tick_size(market: dict[str, Any]) -> float:
        return float((market.get("precision") or {}).get("price") or 0)

    @staticmethod
    def _market_lot_size(market: dict[str, Any]) -> float:
        return float((market.get("precision") or {}).get("amount") or 0)

    @staticmethod
    def _market_min_notional(market: dict[str, Any]) -> float:
        return float(((market.get("limits") or {}).get("cost") or {}).get("min") or 0)

    @staticmethod
    def _market_listed_at(market: dict[str, Any]) -> int | None:
        value = (market.get("info") or {}).get("onboardDate")
        if value in (None, ""):
            return None
        return int(str(value))


__all__ = ["BinanceSpotAdapter"]
