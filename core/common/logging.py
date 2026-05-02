"""结构化 JSON 日志。

按 ``记忆/03-编码约定.md §5``：

* 用 ``DEBUG`` / ``INFO`` / ``WARNING`` / ``ERROR`` / ``CRITICAL`` 五级
* JSON 格式（方便后续接 Loki / Grafana）
* **不打印敏感信息**（API key / token 等只能脱敏）

用法
====

.. code-block:: python

    from core.common.logging import setup_logging, get_logger

    setup_logging(level="INFO")
    log = get_logger(__name__)
    log.info("signal_placed", extra={"strategy": "s1", "symbol": "BTCUSDT"})

注意
====

本模块只提供基础设施。具体的脱敏 / 字段过滤 / sink（文件 / stdout / 远程）
等由后续 PR 按需扩展。Phase 1 起步时只做 stdout JSON 输出 + 行号 / 时间戳
固定字段。
"""

from __future__ import annotations

import json
import logging
import sys
import time
from typing import Any

# Python 标准 logging 在 LogRecord 上预定义的字段，``extra`` 里再传同名会冲突
_RESERVED_KEYS: frozenset[str] = frozenset(
    {
        "name",
        "msg",
        "args",
        "levelname",
        "levelno",
        "pathname",
        "filename",
        "module",
        "exc_info",
        "exc_text",
        "stack_info",
        "lineno",
        "funcName",
        "created",
        "msecs",
        "relativeCreated",
        "thread",
        "threadName",
        "processName",
        "process",
        "asctime",
        "message",
        "taskName",
    }
)


class JsonFormatter(logging.Formatter):
    """把 LogRecord 渲染成单行 JSON。"""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": int(record.created * 1000),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        # 透传 extra 里的额外字段（结构化字段，方便 grep / 后续接 Loki）
        for key, value in record.__dict__.items():
            if key in _RESERVED_KEYS or key.startswith("_"):
                continue
            payload[key] = value
        return json.dumps(payload, ensure_ascii=False, default=_json_default)


def _json_default(obj: Any) -> Any:
    """对 datetime / set / Path 等做兜底 JSON 序列化。"""
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    if isinstance(obj, (set, frozenset)):
        return sorted(obj)
    return repr(obj)


def setup_logging(level: str = "INFO", *, stream: Any = None) -> None:
    """配置全局 root logger 输出 JSON 行到 stdout。

    Args:
        level: 日志级别字符串（``'DEBUG'`` / ``'INFO'`` / ...）。
        stream: 默认 ``sys.stdout``；测试时可以传 ``StringIO``。

    多次调用是幂等的——重复 setup 不会累加 handler。
    """
    root = logging.getLogger()
    root.setLevel(level.upper())

    # 移除已有的 handler 防重复
    for h in list(root.handlers):
        root.removeHandler(h)

    handler = logging.StreamHandler(stream or sys.stdout)
    handler.setFormatter(JsonFormatter())
    root.addHandler(handler)

    logging.Formatter.converter = time.gmtime  # 输出 UTC 时间


def get_logger(name: str) -> logging.Logger:
    """返回带统一配置的 logger。模块内用 ``log = get_logger(__name__)``。"""
    return logging.getLogger(name)


def mask_secret(value: str, *, keep_head: int = 4, keep_tail: int = 4) -> str:
    """脱敏 API key / token。

    ``'abcdefghijklmnop'`` → ``'abcd********mnop'``
    长度过短直接返回 ``'****'`` 避免泄露片段。
    """
    if not value or len(value) <= keep_head + keep_tail:
        return "****"
    return f"{value[:keep_head]}{'*' * 8}{value[-keep_tail:]}"


__all__ = ["JsonFormatter", "get_logger", "mask_secret", "setup_logging"]
