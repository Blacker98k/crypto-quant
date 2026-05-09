"""Binance USDM perpetual REST adapter."""

from __future__ import annotations

from typing import Any

from core.data.exchange.base import FundingRate, OpenInterest
from core.data.exchange.binance_spot import BinanceSpotAdapter
from core.data.symbol import normalize_symbol


class BinanceUsdmAdapter(BinanceSpotAdapter):
    """Binance USD-M futures adapter backed by ccxt."""

    market_type = "perp"
    _ccxt_exchange_name = "binanceusdm"

    def _is_target_market(self, market: dict[str, Any]) -> bool:
        if not bool(market.get("active", True)):
            return False
        return bool(market.get("swap")) or str(market.get("type") or "").lower() == "swap"

    async def fetch_funding_rates(
        self,
        symbol: str,
        start_ms: int,
        end_ms: int,
        limit: int = 1000,
    ) -> list[FundingRate]:
        """Fetch funding-rate history for a perpetual contract."""
        await self._ensure_markets_loaded()
        client = self._client()
        ccxt_symbol = self._to_ccxt_symbol(symbol)
        rows = await self._call(
            client.fetch_funding_rate_history,
            ccxt_symbol,
            start_ms,
            limit,
        )
        result: list[FundingRate] = []
        for row in rows:
            ts = int(row.get("timestamp") or 0)
            if start_ms <= ts < end_ms:
                result.append(
                    FundingRate(
                        symbol=normalize_symbol(symbol),
                        ts=ts,
                        rate=float(row.get("fundingRate") or 0),
                    )
                )
        return result

    async def fetch_open_interest(
        self,
        symbol: str,
        start_ms: int,
        end_ms: int,
        limit: int = 500,
    ) -> list[OpenInterest]:
        """Fetch open-interest history for a perpetual contract."""
        await self._ensure_markets_loaded()
        client = self._client()
        ccxt_symbol = self._to_ccxt_symbol(symbol)
        rows = await self._call(
            client.fetch_open_interest_history,
            ccxt_symbol,
            "5m",
            start_ms,
            limit,
        )
        result: list[OpenInterest] = []
        for row in rows:
            ts = int(row.get("timestamp") or 0)
            if start_ms <= ts < end_ms:
                result.append(
                    OpenInterest(
                        symbol=normalize_symbol(symbol),
                        ts=ts,
                        oi=float(row.get("sumOpenInterest") or row.get("openInterestAmount") or 0),
                    )
                )
        return result


__all__ = ["BinanceUsdmAdapter"]
