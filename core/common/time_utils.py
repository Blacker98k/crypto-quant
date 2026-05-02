"""UTC 毫秒时间工具。

按 ``记忆/03-编码约定.md §10``：

* 内部统一 **UTC 毫秒时间戳**（``int``）
* 显示给人时再转本地时区（北京 UTC+8）
* 不存 ISO 字符串到 DB
* K 线 ``ts`` 字段是 **开盘时间**

本模块只提供纯函数，不持有状态。当前时间请通过
:class:`core.common.clock.Clock` 获取（便于回测注入）。
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

# ─── timeframe 常量 ──────────────────────────────────────────────────────

# 每个 timeframe 对应的毫秒间隔。供缺洞检测、对齐 K 线开盘时间等用。
TF_INTERVAL_MS: dict[str, int] = {
    "1m": 60_000,
    "3m": 3 * 60_000,
    "5m": 5 * 60_000,
    "15m": 15 * 60_000,
    "30m": 30 * 60_000,
    "1h": 60 * 60_000,
    "2h": 2 * 60 * 60_000,
    "4h": 4 * 60 * 60_000,
    "6h": 6 * 60 * 60_000,
    "8h": 8 * 60 * 60_000,
    "12h": 12 * 60 * 60_000,
    "1d": 24 * 60 * 60_000,
    "3d": 3 * 24 * 60 * 60_000,
    "1w": 7 * 24 * 60 * 60_000,
}


# ─── 转换工具 ────────────────────────────────────────────────────────────


def tf_interval_ms(timeframe: str) -> int:
    """``timeframe`` 字符串 → 毫秒间隔。

    Args:
        timeframe: ``'1m'`` / ``'4h'`` / ``'1d'`` 等。

    Returns:
        对应的毫秒数。

    Raises:
        ValueError: 不认识的 timeframe。
    """
    try:
        return TF_INTERVAL_MS[timeframe]
    except KeyError as e:
        raise ValueError(f"unknown timeframe: {timeframe!r}") from e


def iso_to_ms(iso: str) -> int:
    """ISO 8601 字符串 → UTC 毫秒。

    支持的格式：

    * ``'2026-05-01T00:00:00Z'``
    * ``'2026-05-01T00:00:00+00:00'``
    * ``'2026-05-01T08:00:00+08:00'``

    Args:
        iso: ISO 8601 时间字符串。

    Returns:
        UTC 毫秒时间戳。

    Raises:
        ValueError: 字符串格式不合法。
    """
    # 兼容 'Z' 结尾（Python < 3.11 不认）
    s = iso.replace("Z", "+00:00") if iso.endswith("Z") else iso
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        # 朴素 datetime 一律按 UTC 解释（不要静默猜本地时区）
        dt = dt.replace(tzinfo=UTC)
    return int(dt.astimezone(UTC).timestamp() * 1000)


def ms_to_iso(ms: int, *, with_tz: bool = True) -> str:
    """UTC 毫秒 → ISO 8601 字符串。

    Args:
        ms: UTC 毫秒。
        with_tz: True 输出 ``'2026-05-01T00:00:00+00:00'``；
                 False 输出 ``'2026-05-01T00:00:00'``。

    Returns:
        ISO 8601 字符串。
    """
    dt = datetime.fromtimestamp(ms / 1000, tz=UTC)
    if not with_tz:
        return dt.replace(tzinfo=None).isoformat(timespec="milliseconds")
    return dt.isoformat(timespec="milliseconds")


def ms_to_local_iso(ms: int, *, tz_offset_hours: int = 8) -> str:
    """UTC 毫秒 → 本地时区 ISO 字符串（默认北京 UTC+8）。**仅用于显示**。"""
    from datetime import timezone

    tz = timezone(timedelta(hours=tz_offset_hours))
    dt = datetime.fromtimestamp(ms / 1000, tz=tz)
    return dt.isoformat(timespec="milliseconds")


# ─── 对齐 ────────────────────────────────────────────────────────────────


def align_to_timeframe(ms: int, timeframe: str) -> int:
    """把任意 ms 对齐到 timeframe 的开盘时刻（向下取整）。

    例如 1h 对齐：``2026-05-01T03:27:34Z`` → ``2026-05-01T03:00:00Z``。

    注意：周线和月线的对齐严格说不能简单 ``ms // interval``，因为周首日
    与月首日和 epoch 不对齐。本项目暂用整数除法近似——若将来要做精确周月
    K 线，需重写本函数。
    """
    interval = tf_interval_ms(timeframe)
    return (ms // interval) * interval


def next_bar_open(ms: int, timeframe: str) -> int:
    """给定时刻在 ``timeframe`` 周期里的下一根 bar 的开盘时间。"""
    aligned = align_to_timeframe(ms, timeframe)
    return aligned + tf_interval_ms(timeframe)


__all__ = [
    "TF_INTERVAL_MS",
    "align_to_timeframe",
    "iso_to_ms",
    "ms_to_iso",
    "ms_to_local_iso",
    "next_bar_open",
    "tf_interval_ms",
]
