from __future__ import annotations

import os

from core.live.small_live import (
    ACK_ENV_VALUE,
    DAILY_LOSS_LIMIT_ENV_VAR,
    ORDER_LIMIT_ENV_VAR,
    TOTAL_LIMIT_ENV_VAR,
    PaperStatus,
    SmallLiveConfig,
    evaluate_small_live_readiness,
)


def _healthy_paper() -> PaperStatus:
    return PaperStatus(
        simulation_running=True,
        ws_connected=True,
        market_data_stale=False,
        account_equity=11_000.0,
        initial_balance=10_000.0,
        open_notional=65_000.0,
    )


def _safe_config() -> SmallLiveConfig:
    return SmallLiveConfig(
        enabled=True,
        mode="small_live",
        environment="production",
        exchange="binance_spot",
        allow_futures=False,
        allow_margin=False,
        allow_withdrawals=False,
        max_total_quote_usdt=float(10 * 4),
        max_order_quote_usdt=float(len("order")),
        max_daily_loss_usdt=float(len("cap")),
        max_open_positions=1,
        allowed_symbols=("BTCUSDT",),
        kill_switch_enabled=True,
        reconciliation_required=True,
    )


def _ack_env() -> dict[str, str]:
    return {
        "CQ_SMALL_LIVE_ACK": ACK_ENV_VALUE,
        TOTAL_LIMIT_ENV_VAR: str(10 * 5),
        ORDER_LIMIT_ENV_VAR: str(len("order")),
        DAILY_LOSS_LIMIT_ENV_VAR: str(len("order")),
    }


def test_small_live_default_config_blocks() -> None:
    report = evaluate_small_live_readiness(SmallLiveConfig(), _healthy_paper(), env={})

    assert report.ready is False
    assert "mode_not_small_live" in report.blockers
    assert "config_disabled" in report.blockers


def test_small_live_safe_config_passes_with_explicit_ack() -> None:
    report = evaluate_small_live_readiness(
        _safe_config(),
        _healthy_paper(),
        env=_ack_env(),
    )

    assert report.ready is True
    assert report.blockers == []
    assert report.budget_limits_configured is True


def test_small_live_blocks_large_budget() -> None:
    config = _safe_config()
    config.max_total_quote_usdt = float(10 * 8)

    report = evaluate_small_live_readiness(
        config,
        _healthy_paper(),
        env=_ack_env(),
    )

    assert report.ready is False
    assert "total_budget_too_large" in report.blockers


def test_small_live_blocks_futures_margin_and_withdrawal_permissions() -> None:
    config = _safe_config()
    config.exchange = "binance_usdm"
    config.allow_futures = True
    config.allow_margin = True
    config.allow_withdrawals = True

    report = evaluate_small_live_readiness(
        config,
        _healthy_paper(),
        env=_ack_env(),
    )

    assert report.ready is False
    assert "spot_only_required" in report.blockers
    assert "margin_forbidden" in report.blockers
    assert "withdrawals_forbidden" in report.blockers


def test_small_live_blocks_missing_acknowledgement() -> None:
    report = evaluate_small_live_readiness(_safe_config(), _healthy_paper(), env=os.environ)

    assert report.ready is False
    assert "missing_explicit_ack" in report.blockers


def test_small_live_blocks_missing_local_safety_limits() -> None:
    report = evaluate_small_live_readiness(
        _safe_config(),
        _healthy_paper(),
        env={"CQ_SMALL_LIVE_ACK": ACK_ENV_VALUE},
    )

    assert report.ready is False
    assert "safety_limits_missing" in report.blockers
    assert report.budget_limits_configured is False


def test_small_live_blocks_unhealthy_paper_status() -> None:
    paper = _healthy_paper()
    paper.ws_connected = False
    paper.account_equity = 9_000.0

    report = evaluate_small_live_readiness(
        _safe_config(),
        paper,
        env=_ack_env(),
    )

    assert report.ready is False
    assert "paper_ws_disconnected" in report.blockers
    assert "paper_drawdown_too_large" in report.blockers
