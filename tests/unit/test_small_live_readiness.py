from __future__ import annotations

import os

from core.live.small_live import (
    ACK_ENV_VALUE,
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
        max_total_quote_usdt=40.0,
        max_order_quote_usdt=5.0,
        max_daily_loss_usdt=3.0,
        max_open_positions=1,
        allowed_symbols=("BTCUSDT",),
        kill_switch_enabled=True,
        reconciliation_required=True,
    )


def test_small_live_default_config_blocks() -> None:
    report = evaluate_small_live_readiness(SmallLiveConfig(), _healthy_paper(), env={})

    assert report.ready is False
    assert "mode_not_small_live" in report.blockers
    assert "config_disabled" in report.blockers


def test_small_live_safe_config_passes_with_explicit_ack() -> None:
    report = evaluate_small_live_readiness(
        _safe_config(),
        _healthy_paper(),
        env={"CQ_SMALL_LIVE_ACK": ACK_ENV_VALUE},
    )

    assert report.ready is True
    assert report.blockers == []
    assert report.max_total_quote_usdt == 40.0


def test_small_live_blocks_large_budget() -> None:
    config = _safe_config()
    config.max_total_quote_usdt = 80.0

    report = evaluate_small_live_readiness(
        config,
        _healthy_paper(),
        env={"CQ_SMALL_LIVE_ACK": ACK_ENV_VALUE},
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
        env={"CQ_SMALL_LIVE_ACK": ACK_ENV_VALUE},
    )

    assert report.ready is False
    assert "spot_only_required" in report.blockers
    assert "margin_forbidden" in report.blockers
    assert "withdrawals_forbidden" in report.blockers


def test_small_live_blocks_missing_acknowledgement() -> None:
    report = evaluate_small_live_readiness(_safe_config(), _healthy_paper(), env=os.environ)

    assert report.ready is False
    assert "missing_explicit_ack" in report.blockers


def test_small_live_blocks_unhealthy_paper_status() -> None:
    paper = _healthy_paper()
    paper.ws_connected = False
    paper.account_equity = 9_000.0

    report = evaluate_small_live_readiness(
        _safe_config(),
        paper,
        env={"CQ_SMALL_LIVE_ACK": ACK_ENV_VALUE},
    )

    assert report.ready is False
    assert "paper_ws_disconnected" in report.blockers
    assert "paper_drawdown_too_large" in report.blockers
