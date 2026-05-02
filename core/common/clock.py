"""时钟抽象——支持 live 模式和回测时间注入。

为什么要抽象时钟
=================

策略代码统一通过 ``ctx.now_ms()`` 取当前时间。回测引擎需要把"当前时间"
设为某根历史 bar 的时刻，否则 ``time.time()`` 会拿到真实当前时刻 → 策略
看到"未来数据"。

按 ``docs/04-接口文档/01-策略接口.md §4`` 与决策 B5：

* live / paper 模式：``SystemClock`` → ``int(time.time() * 1000)``
* 回测：``FixedClock`` 由 ``BacktestContext`` 在每根 bar 推送前更新
"""

from __future__ import annotations

import time
from typing import Protocol


class Clock(Protocol):
    """时钟协议。任何需要"当前时间"的代码都应通过此接口取，不要直接调
    ``time.time()`` / ``datetime.now()``——否则回测会被污染。
    """

    def now_ms(self) -> int:
        """返回当前 UTC 毫秒时间戳（int）。"""


class SystemClock:
    """生产环境用。基于系统时钟。"""

    def now_ms(self) -> int:
        return int(time.time() * 1000)


class FixedClock:
    """回测 / 测试用。当前时间由外部显式 set。"""

    __slots__ = ("_now_ms",)

    def __init__(self, now_ms: int = 0) -> None:
        self._now_ms = int(now_ms)

    def now_ms(self) -> int:
        return self._now_ms

    def set(self, now_ms: int) -> None:
        """更新当前时间。回测引擎在每根 bar 推送前调用。"""
        self._now_ms = int(now_ms)

    def advance(self, delta_ms: int) -> None:
        """前进 ``delta_ms`` 毫秒。"""
        self._now_ms += int(delta_ms)


__all__ = ["Clock", "FixedClock", "SystemClock"]
