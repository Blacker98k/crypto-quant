"""Binance Spot 适配器占位。"""

from __future__ import annotations


class BinanceSpotAdapter:
    """P1.2/P1.3 前的轻量占位适配器。"""

    name = "binance"
    market_type = "spot"

    def __init__(self, proxy: str = "", timeout_ms: int = 30_000) -> None:
        self.proxy = proxy
        self.timeout_ms = timeout_ms

    async def _ensure_markets_loaded(self) -> None:
        """预加载市场信息。当前占位 no-op。"""

    async def close(self) -> None:
        """释放资源。当前占位 no-op。"""


__all__ = ["BinanceSpotAdapter"]
