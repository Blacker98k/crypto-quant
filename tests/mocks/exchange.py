"""Mock ExchangeAdapter——返回合成历史数据，不访问真实交易所。

所有方法都是同步的（返回已知数据），供集成测试使用。
"""

from __future__ import annotations

import random

from core.common.time_utils import tf_interval_ms
from core.data.exchange.base import Bar, FundingRate, OpenInterest, SymbolInfo, Ticker24h


def _generate_bars(
    symbol: str,
    timeframe: str,
    start_ms: int,
    end_ms: int,
    *,
    base_price: float = 50000.0,
) -> list[Bar]:
    """生成合成 K 线数据。"""
    interval = tf_interval_ms(timeframe)
    if interval <= 0:
        return []

    bars: list[Bar] = []
    ts = start_ms
    price = base_price

    while ts < end_ms:
        o = price
        c = o * (1 + random.uniform(-0.02, 0.02))
        h = max(o, c) * (1 + random.uniform(0, 0.01))
        low = min(o, c) * (1 - random.uniform(0, 0.01))
        v = random.uniform(0.1, 100.0)
        q = v * c

        bars.append(
            Bar(
                symbol=symbol,
                timeframe=timeframe,
                ts=ts,
                o=round(o, 2),
                h=round(h, 2),
                l=round(low, 2),
                c=round(c, 2),
                v=round(v, 6),
                q=round(q, 2),
                closed=True,
            )
        )
        price = c
        ts += interval

    return bars


class MockExchangeAdapter:
    """Mock 交易所适配器——返回合成数据。

    Args:
        symbols: 模拟支持的 symbol 列表（默认 BTCUSDT, ETHUSDT）
        market_type: ``'spot'`` 或 ``'perp'``
    """

    name = "mock"
    market_type: str = "spot"

    def __init__(
        self,
        symbols: list[str] | None = None,
        market_type: str = "spot",
    ) -> None:
        self.market_type = market_type
        self._symbols = symbols or ["BTCUSDT", "ETHUSDT"]

    # ─── 元数据 ──────────────────────────────────────────────────────────

    async def fetch_exchange_info(self) -> list[SymbolInfo]:
        """返回 mock 标的元数据。"""
        result: list[SymbolInfo] = []
        for sym in self._symbols:
            quote = "USDT" if "USDT" in sym else "BTC"
            result.append(
                SymbolInfo(
                    exchange="mock",
                    symbol=sym,
                    base=sym.replace(quote, ""),
                    quote=quote,
                    stype=self.market_type,
                    tick_size=0.01,
                    lot_size=0.001,
                    min_notional=10.0,
                )
            )
        return result

    async def fetch_24h_tickers(self) -> list[Ticker24h]:
        """返回 mock 24h 行情。"""
        import time

        ts = int(time.time() * 1000)
        result: list[Ticker24h] = []
        for sym in self._symbols:
            result.append(
                Ticker24h(
                    symbol=sym,
                    last_price=50000.0 if sym.startswith("BTC") else 3000.0,
                    quote_volume=1_000_000_000.0,
                    ts=ts,
                )
            )
        return result

    # ─── 历史 K 线 ───────────────────────────────────────────────────────

    async def fetch_klines(
        self,
        symbol: str,
        timeframe: str,
        start_ms: int,
        end_ms: int,
        limit: int = 1000,
    ) -> list[Bar]:
        """返回合成 K 线数据。"""
        base_price = 50000.0 if symbol.startswith("BTC") else 3000.0
        all_bars = _generate_bars(symbol, timeframe, start_ms, end_ms, base_price=base_price)
        return all_bars[:limit]

    # ─── 资金费率 / 持仓量（仅 perp）─────────────────────────────────────

    async def fetch_funding_rates(
        self,
        symbol: str,
        start_ms: int,
        end_ms: int,
    ) -> list[FundingRate]:
        """返回合成资金费率。每 8h 一条。"""
        if self.market_type != "perp":
            raise NotImplementedError("spot 无资金费率")
        interval = 8 * 60 * 60_000
        ts = start_ms
        result: list[FundingRate] = []
        while ts < end_ms:
            result.append(
                FundingRate(
                    symbol=symbol,
                    ts=ts,
                    rate=round(random.uniform(-0.001, 0.003), 6),
                )
            )
            ts += interval
        return result

    async def fetch_open_interest(
        self,
        symbol: str,
        start_ms: int,
        end_ms: int,
    ) -> list[OpenInterest]:
        """返回合成持仓量。"""
        if self.market_type != "perp":
            raise NotImplementedError("spot 无持仓量")
        ts = start_ms
        result: list[OpenInterest] = []
        while ts < end_ms:
            result.append(
                OpenInterest(
                    symbol=symbol,
                    ts=ts,
                    oi=round(random.uniform(10000, 50000), 4),
                )
            )
            ts += 5 * 60_000
        return result

    # ─── 资源管理 ────────────────────────────────────────────────────────

    async def close(self) -> None:
        pass


__all__ = ["MockExchangeAdapter"]
