"""项目自定义异常树。

按 ``docs/04-接口文档/04-外部接口.md §5.1`` 的层级落地。所有项目内
代码 raise 的异常都应该是 :class:`CryptoQuantError` 的子类——上层
catch ``CryptoQuantError`` 即可统一处理。

设计原则
========

* 业务错误（信号被风控拒绝等）→ 返回带原因的对象，**不抛异常**
* 可恢复错误（API 限流等）→ 抛 :class:`RateLimited` 类，由调用方决定退避
* 不可恢复错误（DB / 配置问题）→ 立即抛，由 engine 层捕获并停服
* 编程错误（类型 / 字段不合法）→ 抛 :class:`InvalidOrderIntent` 等子类

约定
====

* 异常 message **不得**包含完整 API key / token / 签名等敏感信息
  （脱敏到前 4 / 后 4 字符即可）
* 异常的 ``__cause__`` 链必须保留（``raise ... from e``），不要吞底层异常
"""

from __future__ import annotations

# ─── 顶层 ────────────────────────────────────────────────────────────────


class CryptoQuantError(Exception):
    """项目所有自定义异常的根。"""


# ─── 数据层 ──────────────────────────────────────────────────────────────


class DataFeedError(CryptoQuantError):
    """数据层通用错误的基类。"""


class DataNotAvailable(DataFeedError):
    """请求的 (symbol, timeframe) 完全没有数据。

    与 "区间内有少量缺洞" 不同——后者通过 ``meta.missing_ranges`` 暗示，
    本异常用于 "压根没回填过 / symbol 不存在" 场景。
    """


class DataTimeout(DataFeedError):
    """``get_*`` 拉数据超时（默认 5s）。"""


class ParquetCorrupt(DataFeedError):
    """Parquet 文件读取失败 / 校验不通过。

    触发后必须 **拒绝启动数据层**——真相源不可信，不允许带病运行。
    """


class InvalidQueryError(DataFeedError, ValueError):
    """``DataFeed`` 调用参数不合法（如 ``n`` 与 ``start_ms/end_ms`` 混用）。"""


class NotInLiveMode(DataFeedError):
    """research 模式调用了仅 live / paper 才支持的方法（如订单簿快照）。"""


class DuplicateSubscription(DataFeedError):
    """对同一 ``(symbol, stream)`` 重复 ``subscribe_*``。"""


# ─── 外部接口（交易所 API） ──────────────────────────────────────────────


class ExchangeAPIError(CryptoQuantError):
    """与交易所交互层面的错误基类。

    由 ``ExchangeAdapter`` 实现按 ``error_map`` 把交易所原始码归一化抛出。
    """


class RateLimited(ExchangeAPIError):
    """HTTP 429 / Binance ``-1003`` 等限流。

    OMS / 数据层内部的限速器应自动退避（按响应头 ``Retry-After``），
    上层一般不需要处理这个异常。
    """


class IPBanned(ExchangeAPIError):
    """HTTP 418 / 已被 IP ban。

    必须 critical 告警 + **全局停所有 REST**，等维护者人工解封。
    """


class AuthError(ExchangeAPIError):
    """签名失败 / api_key 无效 / 时钟偏移过大等鉴权类问题。"""


class NetworkError(ExchangeAPIError):
    """网络层问题：超时 / DNS / 连接拒绝。"""


class ExchangeServerError(ExchangeAPIError):
    """交易所端 5xx。"""


# ─── 订单 / OMS ──────────────────────────────────────────────────────────


class OrderRejectedError(CryptoQuantError):
    """交易所拒单。具体原因放在 ``message`` 里 + ``reason`` 属性。"""

    def __init__(self, message: str, reason: str | None = None) -> None:
        super().__init__(message)
        self.reason = reason


class InvalidOrderIntent(OrderRejectedError, ValueError):
    """``OrderIntent`` 字段不合法（精度、必填项缺失、stop 价位方向反等）。

    属于编程错误——OMS 不应吞掉，上层调用方修代码。
    """


class InvalidStopLoss(InvalidOrderIntent):
    """止损价位与方向不匹配（多头 stop ≥ entry 等）。"""


class InsufficientBalance(OrderRejectedError):
    """账户余额不足。L2 sizer 应早判断，到这一步说明 sizer 与交易所不一致。"""


class PositionModeMismatch(OrderRejectedError):
    """配置 hedge mode 但交易所在 one-way mode（或反之）。

    OMS 启动时探测到则拒绝启动，等维护者修配置。
    """


class ReduceOnlyRejected(OrderRejectedError):
    """合约 reduce-only 单被交易所拒（如方向反了 / 持仓不足）。

    见 ``docs/03-详细设计/04-执行层.md §4.4`` 失败 #8。
    """


class IdempotencyConflict(OrderRejectedError):
    """同 ``client_order_id`` 已存在但状态冲突。

    例如重试时本地认为还是 ``new``，交易所那边其实已 ``canceled``。
    """


class SubmitTimeout(CryptoQuantError):
    """``place_order`` 5s 内未收到 ACK。

    **不要直接重发**——必须先用同 ``client_order_id`` 调 ``query_order``
    确认真实状态。
    """


class OMSDegraded(CryptoQuantError):
    """OMS 当前在 degraded / frozen 模式，拒绝新单。

    风控层应该早判断不让信号走到这一步。
    """


# ─── 风控 ────────────────────────────────────────────────────────────────


class RiskBlockedError(CryptoQuantError):
    """L1/L2/L3 拒绝信号的统一异常类型。

    实际开发中风控更倾向 "返回带原因的对象 + 落 ``risk_events``" 而非抛异常，
    本异常仅在确实需要中断流程时使用。
    """


# ─── 执行层 / 对账 ───────────────────────────────────────────────────────


class ReconcileAnomaly(CryptoQuantError):
    """5 分钟对账发现本地 vs 交易所不一致。

    OMS 自动 freeze + critical 告警；维护者必须确认后才能解冻。
    """


# ─── 策略 ────────────────────────────────────────────────────────────────


class StrategyError(CryptoQuantError):
    """策略主动 raise 表示 "本次信号丢弃 + 落 ``risk_events``"。

    与 "策略代码崩溃" 区分：后者是普通 ``Exception``，框架捕获后会自动
    停用该策略到下次重启。
    """


class DataNotReadyError(StrategyError):
    """策略需要的历史数据还不够（如要 200 根但只有 50 根）。

    策略 ``on_bar`` 应该跳过本次。
    """


class KVStoreError(StrategyError):
    """``ctx.kv_get/set`` 后端 IO 错误。"""


# ─── 配置 / 启动 ─────────────────────────────────────────────────────────


class ConfigError(CryptoQuantError):
    """配置文件不合法 / 必填字段缺失 / 类型不对。"""


__all__ = [
    "CryptoQuantError",
    # 数据层
    "DataFeedError",
    "DataNotAvailable",
    "DataTimeout",
    "ParquetCorrupt",
    "InvalidQueryError",
    "NotInLiveMode",
    "DuplicateSubscription",
    # 外部接口
    "ExchangeAPIError",
    "RateLimited",
    "IPBanned",
    "AuthError",
    "NetworkError",
    "ExchangeServerError",
    # 订单 / OMS
    "OrderRejectedError",
    "InvalidOrderIntent",
    "InvalidStopLoss",
    "InsufficientBalance",
    "PositionModeMismatch",
    "ReduceOnlyRejected",
    "IdempotencyConflict",
    "SubmitTimeout",
    "OMSDegraded",
    # 风控
    "RiskBlockedError",
    # 执行 / 对账
    "ReconcileAnomaly",
    # 策略
    "StrategyError",
    "DataNotReadyError",
    "KVStoreError",
    # 配置
    "ConfigError",
]
