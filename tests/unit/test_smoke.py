"""项目骨架 smoke 测试：验证所有公开模块可 import + 异常树结构正确。

新模块加进来时，**必须**在这里加一行 ``import``——这是最便宜的"项目还能起来"
检测；ruff 也会标 unused import 提醒维护者。
"""

from __future__ import annotations

import importlib

import pytest

# ─── 公开包必须可 import ─────────────────────────────────────────────────


@pytest.mark.parametrize(
    "module_name",
    [
        # core 主包
        "core",
        "core.common",
        "core.common.exceptions",
        "core.common.clock",
        "core.common.time_utils",
        "core.common.logging",
        # core 业务子包（Phase 1 起步时为空）
        "core.data",
        "core.strategy",
        "core.risk",
        "core.execution",
        "core.monitor",
        "core.universe",
        "core.db",
        # 研究区
        "research",
        "research.factors",
        "research.backtest",
        # 策略实现
        "strategies",
    ],
)
def test_public_module_importable(module_name: str) -> None:
    """所有公开模块 import 不抛异常。"""
    importlib.import_module(module_name)


# ─── 异常树结构 ──────────────────────────────────────────────────────────


def test_exception_tree_root() -> None:
    """所有自定义异常都是 ``CryptoQuantError`` 的子类。"""
    from core.common import exceptions as ex

    cqe = ex.CryptoQuantError
    assert issubclass(cqe, Exception)

    for name in ex.__all__:
        if name == "CryptoQuantError":
            continue
        cls = getattr(ex, name)
        assert isinstance(cls, type), f"{name} 不是 class"
        assert issubclass(cls, cqe), f"{name} 不是 CryptoQuantError 子类"


def test_invalid_order_intent_dual_inheritance() -> None:
    """``InvalidOrderIntent`` 同时是 ``OrderRejectedError`` 和 ``ValueError``——
    便于既能按"订单异常"统一捕获，又能按"参数异常"识别为编程错误。
    """
    from core.common.exceptions import InvalidOrderIntent, OrderRejectedError

    assert issubclass(InvalidOrderIntent, OrderRejectedError)
    assert issubclass(InvalidOrderIntent, ValueError)


# ─── 时钟 ────────────────────────────────────────────────────────────────


def test_system_clock_returns_int_ms() -> None:
    from core.common.clock import SystemClock

    ts = SystemClock().now_ms()
    assert isinstance(ts, int)
    # 简单合理性检查：时间戳应该是 13 位（毫秒级 2001-2280 之间）
    assert 10**12 < ts < 10**13


def test_fixed_clock_set_and_advance() -> None:
    from core.common.clock import FixedClock

    c = FixedClock(1_700_000_000_000)
    assert c.now_ms() == 1_700_000_000_000
    c.advance(60_000)
    assert c.now_ms() == 1_700_000_060_000
    c.set(1_800_000_000_000)
    assert c.now_ms() == 1_800_000_000_000


# ─── 时间工具 ────────────────────────────────────────────────────────────


def test_tf_interval_ms_known_timeframes() -> None:
    from core.common.time_utils import tf_interval_ms

    assert tf_interval_ms("1m") == 60_000
    assert tf_interval_ms("4h") == 14_400_000
    assert tf_interval_ms("1d") == 86_400_000


def test_tf_interval_ms_unknown_raises() -> None:
    from core.common.time_utils import tf_interval_ms

    with pytest.raises(ValueError, match="unknown timeframe"):
        tf_interval_ms("7m")


def test_iso_to_ms_and_back() -> None:
    from core.common.time_utils import iso_to_ms, ms_to_iso

    iso = "2026-05-01T00:00:00+00:00"
    ms = iso_to_ms(iso)
    assert ms == 1_777_593_600_000
    # round-trip
    assert ms_to_iso(ms).startswith("2026-05-01T00:00:00")


def test_iso_to_ms_z_suffix() -> None:
    from core.common.time_utils import iso_to_ms

    a = iso_to_ms("2026-05-01T00:00:00Z")
    b = iso_to_ms("2026-05-01T00:00:00+00:00")
    assert a == b


def test_iso_to_ms_naive_treated_as_utc() -> None:
    from core.common.time_utils import iso_to_ms

    naive = iso_to_ms("2026-05-01T00:00:00")
    explicit = iso_to_ms("2026-05-01T00:00:00+00:00")
    assert naive == explicit


def test_align_to_timeframe() -> None:
    from core.common.time_utils import align_to_timeframe, iso_to_ms

    ts = iso_to_ms("2026-05-01T03:27:34+00:00")
    aligned_1h = align_to_timeframe(ts, "1h")
    expected_1h = iso_to_ms("2026-05-01T03:00:00+00:00")
    assert aligned_1h == expected_1h


def test_next_bar_open() -> None:
    from core.common.time_utils import iso_to_ms, next_bar_open

    ts = iso_to_ms("2026-05-01T03:27:34+00:00")
    nbo = next_bar_open(ts, "1h")
    assert nbo == iso_to_ms("2026-05-01T04:00:00+00:00")


# ─── 日志 ────────────────────────────────────────────────────────────────


def test_mask_secret_basic() -> None:
    from core.common.logging import mask_secret

    masked = mask_secret("abcdefghijklmnopqrstuvwx")
    assert masked.startswith("abcd")
    assert masked.endswith("uvwx")
    assert "ghijkl" not in masked


def test_mask_secret_short_value_fully_masked() -> None:
    from core.common.logging import mask_secret

    assert mask_secret("abc") == "****"
    assert mask_secret("") == "****"


def test_setup_logging_emits_json() -> None:
    """``setup_logging`` 后 ``log.info`` 输出能被 ``json.loads`` 解析。"""
    import io
    import json

    from core.common.logging import get_logger, setup_logging

    buf = io.StringIO()
    setup_logging(level="INFO", stream=buf)
    log = get_logger("test_smoke")
    log.info("hello", extra={"strategy": "s1"})

    out = buf.getvalue().strip()
    assert out, "no log output captured"
    parsed = json.loads(out)
    assert parsed["msg"] == "hello"
    assert parsed["level"] == "INFO"
    assert parsed["strategy"] == "s1"
    assert isinstance(parsed["ts"], int)


# ─── 项目版本号 ──────────────────────────────────────────────────────────


def test_core_version_is_set() -> None:
    import core

    assert hasattr(core, "__version__")
    assert isinstance(core.__version__, str)
    assert "." in core.__version__
