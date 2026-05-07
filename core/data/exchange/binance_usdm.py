"""Binance USDM 永续适配器占位。"""

from __future__ import annotations

from core.data.exchange.binance_spot import BinanceSpotAdapter


class BinanceUsdmAdapter(BinanceSpotAdapter):
    """P1.2 前的轻量占位适配器。"""

    market_type = "perp"


__all__ = ["BinanceUsdmAdapter"]
