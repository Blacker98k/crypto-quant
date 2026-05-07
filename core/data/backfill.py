"""历史回填任务占位。"""

from __future__ import annotations

from typing import Any


class BackfillJob:
    """P1.2 实现真实 REST 回填前的占位类。"""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.args = args
        self.kwargs = kwargs


__all__ = ["BackfillJob"]
