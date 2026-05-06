"""Strategy 抽象基类 / Signal / StrategyContext / DataRequirement。

按 ``docs/04-接口文档/01-策略接口.md §2–§4`` 定义。
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from core.data.exchange.base import Bar
    from core.data.feed import DataFeed, Tick

# ─── 数据类 ──────────────────────────────────────────────────────────────────


@dataclass(slots=True)
class DataRequirement:
    """策略声明所需的数据依赖。"""

    symbols: list[str] = field(default_factory=list)
    timeframes: list[str] = field(default_factory=list)
    history_lookback_bars: int = 500
    needs_funding: bool = False
    needs_orderbook_l1: bool = False
    needs_orderbook_l5: bool = False
    subscribe_partial_bars: bool = False


@dataclass(slots=True)
class Signal:
    """策略产出的交易信号。

    风控层 L1/L2 可能修改 ``suggested_size``、拒绝信号，或叠加止盈/止损。
    """

    side: Literal["long", "short", "close"]
    symbol: str
    entry_price: float | None = None
    stop_price: float = 0.0
    target_price: float | None = None
    confidence: float = 0.5
    suggested_size: float = 0.0
    time_in_force: Literal["GTC", "IOC", "FOK", "DAY"] = "GTC"
    rationale: dict[str, Any] = field(default_factory=dict)
    expires_in_ms: int = 60_000
    trade_group_id: str | None = None


# ─── StrategyContext ─────────────────────────────────────────────────────────


class StrategyContext:
    """策略运行时上下文——回测和实盘无感的数据/状态访问。

    策略通过 ``ctx`` 取数据、查状态、持久化 KV，不直接访问外部系统。
    """

    __slots__ = ("_clock", "_data", "_repo", "_strategy_name")

    def __init__(
        self,
        data: DataFeed,
        clock: object,  # core.common.clock.Clock
        repo: object,  # core.data.sqlite_repo.SqliteRepo
        strategy_name: str,
    ) -> None:
        self._data = data
        self._clock = clock
        self._repo = repo
        self._strategy_name = strategy_name

    # ─── 数据访问 ────────────────────────────────────────────────────────

    @property
    def data(self) -> DataFeed:
        """DataFeed 统一数据接口。"""
        return self._data

    # ─── 时间 ────────────────────────────────────────────────────────────

    def now_ms(self) -> int:
        """当前 UTC 毫秒时间戳。回测时返回注入的虚拟时间。"""
        return self._clock.now_ms()

    # ─── 持久化 KV ───────────────────────────────────────────────────────

    def kv_get(self, key: str) -> Any | None:
        """读取策略持久化状态。"""
        import json

        raw = self._repo.kv_get(self._strategy_name, key)
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return raw

    def kv_set(self, key: str, value: Any) -> None:
        """写入策略持久化状态。"""
        import json

        raw = json.dumps(value, ensure_ascii=False, default=str)
        self._repo.kv_set(self._strategy_name, key, raw)

    # ─── 日志 ────────────────────────────────────────────────────────────

    def log(self, level: str, msg: str, **kwargs: Any) -> None:
        """策略级结构化日志。"""
        import logging

        log = logging.getLogger(f"strategy.{self._strategy_name}")
        extra = kwargs.copy()
        extra["strategy"] = self._strategy_name
        getattr(log, level)(msg, extra=extra)


# ─── Strategy ABC ────────────────────────────────────────────────────────────


class Strategy(ABC):
    """策略抽象基类。

    所有策略必须实现 ``required_data`` 和 ``on_bar``。
    可选覆写 ``on_start`` / ``on_stop`` 等生命周期钩子。

    Class Attributes:
        name: 策略唯一标识（如 ``'S1_btc_trend'``）
        version: 策略代码版本（git short hash，Phase 1 用 'dev'）
        config_hash: 配置归一化后 SHA-256 前 16 位
    """

    name: str = ""
    version: str = "dev"
    config_hash: str = "0000000000000000"

    # ─── 必须实现 ────────────────────────────────────────────────────────

    @abstractmethod
    def required_data(self) -> DataRequirement:
        """声明策略依赖的数据。框架据此预热历史数据 + 订阅实时流。"""
        ...

    @abstractmethod
    def on_bar(self, bar: Bar, ctx: StrategyContext) -> list[Signal]:
        """K 线收盘时调用。返回 0..N 个信号。**必须是纯函数**（不写 IO）。"""
        ...

    # ─── 可选回调 ────────────────────────────────────────────────────────

    def on_tick(self, tick: Tick, ctx: StrategyContext) -> list[Signal]:
        """tick 级响应。默认 no-op。"""
        return []

    def on_start(self, ctx: StrategyContext) -> None:  # noqa: B027
        """启动钩子。"""

    def on_stop(self, ctx: StrategyContext) -> None:  # noqa: B027
        """停机钩子。"""


__all__ = [
    "DataRequirement",
    "Signal",
    "Strategy",
    "StrategyContext",
]
