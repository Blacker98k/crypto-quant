"""交易所错误码 → 项目自定义异常映射。

按 ``docs/04-接口文档/04-外部接口.md §5.2`` 维护。
由 ``ExchangeAdapter`` 实现层在收到交易所错误时调用
:func:`map_binance_error` 做归一化抛异常。

约定
====

* 调用方只 catch 本项目的异常类（:class:`CryptoQuantError` 子类），
  不直接 catch 原始 HTTP/库异常
* 本文件仅维护映射字典，不包含网络 I/O 逻辑
* Phase 1 仅含 Binance；Phase 5+ 追加 OKX 时在此文件新增映射表
"""

from __future__ import annotations

from core.common.exceptions import (
    AuthError,
    ExchangeServerError,
    InsufficientBalance,
    InvalidOrderIntent,
    IPBanned,
    OrderRejectedError,
    PositionModeMismatch,
    RateLimited,
    ReduceOnlyRejected,
)

# ─── Binance 错误码 → (异常类, 原因描述) ──────────────────────────────────────
#
# 格式：{code: (exception_class, reason_message)}
# code 支持 int（Binance JSON 错误码）和 str（HTTP 状态码或描述关键词）。

BINANCE_ERROR_TABLE: dict[int | str, tuple[type[Exception], str]] = {
    # ── 限流 / 封禁 ──
    -1003: (RateLimited, "请求过于频繁，触发限流"),
    429: (RateLimited, "HTTP 429 请求限流"),
    "HTTP_429": (RateLimited, "HTTP 429 请求限流"),
    418: (IPBanned, "IP 已被封禁，需人工解封"),
    "HTTP_418": (IPBanned, "HTTP 418 IP 被封禁"),
    # ── 鉴权 ──
    -1021: (AuthError, "时间戳偏移过大，请同步系统时间"),
    -2014: (AuthError, "API key 格式无效"),
    -2015: (AuthError, "签名无效（secret 配置错误或时钟偏移）"),
    # ── 订单相关 ──
    -2010: (OrderRejectedError, "订单被交易所拒绝"),
    -2011: (OrderRejectedError, "撤单被拒（可能已成交）"),
    -2013: (OrderRejectedError, "订单不存在"),
    -1013: (InvalidOrderIntent, "数量不合法（检查 lot_size）"),
    -1111: (InvalidOrderIntent, "精度不合法（检查 tick_size）"),
    -1102: (InvalidOrderIntent, "必填参数缺失"),
    -2018: (InsufficientBalance, "余额不足"),
    -4061: (PositionModeMismatch, "持仓模式不匹配（hedge vs one-way）"),
    -2022: (ReduceOnlyRejected, "reduce-only 单被拒（方向反了或持仓不足）"),
    # ── 服务器 ──
    500: (ExchangeServerError, "Binance HTTP 500 内部错误"),
    502: (ExchangeServerError, "Binance HTTP 502 网关错误"),
    503: (ExchangeServerError, "Binance HTTP 503 服务不可用"),
    504: (ExchangeServerError, "Binance HTTP 504 网关超时"),
    "HTTP_5XX": (ExchangeServerError, "Binance 服务器错误（5xx）"),
}

# ─── flat dict ── 快速查表用的扁平字典，key 统一为 int ───────────────────────
_BINANCE_FLAT: dict[int, type[Exception]] = {}
_REASON_FLAT: dict[int, str] = {}
for _code, (_exc_cls, _reason) in BINANCE_ERROR_TABLE.items():
    if isinstance(_code, int):
        _BINANCE_FLAT[_code] = _exc_cls
        _REASON_FLAT[_code] = _reason

# ─── 公共 API ──────────────────────────────────────────────────────────────


def map_binance_error(
    error_code: int | None = None,
    http_status: int | None = None,
    message: str = "",
) -> Exception | None:
    """根据 Binance 错误信息返回对应的项目异常实例。

    查表优先级：
    1. Binance JSON 错误码（``error_code``，如 -1003）
    2. HTTP 状态码（``http_status``，如 429 / 418 / 5xx）
    3. 消息关键词匹配（兜底）

    Args:
        error_code: Binance JSON body 中的 ``code`` 字段，如 ``-1003``
        http_status: HTTP 响应状态码，如 ``429``
        message: 错误消息原文（用于兜底关键词匹配）

    Returns:
        对应的异常实例，或 None（无法映射时由调用方自行决定策略）。

    Example:
        >>> exc = map_binance_error(error_code=-1003)
        >>> isinstance(exc, RateLimited)
        True
        >>> exc = map_binance_error(http_status=503)
        >>> isinstance(exc, ExchangeServerError)
        True
    """
    # 1) 精确匹配 Binance 错误码
    if error_code is not None and error_code in _BINANCE_FLAT:
        exc_cls = _BINANCE_FLAT[error_code]
        reason = _REASON_FLAT.get(error_code, "")
        return exc_cls(build_error_message(error_code, reason, message))

    # 2) HTTP 状态码映射
    if http_status is not None:
        if http_status in (429,):
            return RateLimited(f"HTTP {http_status} 请求限流: {message}")
        if http_status in (418,):
            return IPBanned(f"HTTP {http_status} IP 已被封禁: {message}")
        if http_status in (500, 502, 503, 504):
            return ExchangeServerError(f"HTTP {http_status} 服务器错误: {message}")

    # 3) 消息关键词兜底
    msg_lower = message.lower()
    if "429" in message or "rate limit" in msg_lower:
        return RateLimited(f"限流: {message}")
    if "418" in message or "ip banned" in msg_lower or "ban" in msg_lower:
        return IPBanned(f"IP 封禁: {message}")
    if "signature" in msg_lower:
        return AuthError(f"签名错误: {message}")
    if "timestamp" in msg_lower:
        return AuthError(f"时间戳错误: {message}")

    return None


def build_error_message(code: int | None, reason: str, raw_message: str = "") -> str:
    """统一构造错误消息，含原始码 + 原因。

    不在消息中包含完整 API key / secret 等敏感信息。
    """
    parts: list[str] = []
    if code is not None:
        parts.append(f"[Binance {code}]")
    if reason:
        parts.append(reason)
    if raw_message:
        # 截断过长消息，避免日志膨胀
        truncated = raw_message[:300] + ("..." if len(raw_message) > 300 else "")
        parts.append(f"({truncated})")
    return " ".join(parts)


# ─── HTTP 状态码判断 ─────────────────────────────────────────────────────


def is_rate_limited(http_status: int) -> bool:
    """HTTP 状态码是否表示限流。"""
    return http_status == 429


def is_ip_banned(http_status: int) -> bool:
    """HTTP 状态码是否表示 IP 被封禁。"""
    return http_status == 418


def is_server_error(http_status: int) -> bool:
    """HTTP 状态码是否表示服务器错误（≥ 500）。"""
    return http_status >= 500


__all__ = [
    "BINANCE_ERROR_TABLE",
    "build_error_message",
    "is_ip_banned",
    "is_rate_limited",
    "is_server_error",
    "map_binance_error",
]
