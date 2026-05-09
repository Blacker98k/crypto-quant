"""交易所数据类型。

这些 dataclass 是数据层和策略层之间的稳定契约，字段名与文档中的
K 线、资金费率、标的元数据保持一致。
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class Bar:
    """标准 K 线。``ts`` 是 UTC 毫秒开盘时间。"""

    symbol: str
    timeframe: str
    ts: int
    o: float
    h: float
    l: float
    c: float
    v: float
    q: float = 0.0
    closed: bool = True


@dataclass(slots=True)
class FundingRate:
    """永续合约资金费率。"""

    symbol: str
    ts: int
    rate: float


@dataclass(slots=True)
class OpenInterest:
    """永续合约持仓量。"""

    symbol: str
    ts: int
    oi: float


@dataclass(slots=True)
class SymbolInfo:
    """交易所标的元数据。"""

    exchange: str
    symbol: str
    base: str
    quote: str
    stype: str
    tick_size: float
    lot_size: float
    min_notional: float
    listed_at: int | None = None
    delisted_at: int | None = None


@dataclass(slots=True)
class Ticker24h:
    """24 小时 ticker，用于币池筛选。"""

    symbol: str
    last_price: float
    quote_volume: float
    ts: int


__all__ = ["Bar", "FundingRate", "OpenInterest", "SymbolInfo", "Ticker24h"]
