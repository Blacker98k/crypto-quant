"""Readiness checks for a future small-live mode.

The functions in this module are intentionally side-effect free. They do not
read secrets, create exchange clients, or place orders.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

ACK_ENV_VAR = "CQ_SMALL_LIVE_ACK"
ACK_ENV_VALUE = "I_UNDERSTAND_REAL_MONEY_RISK"
TOTAL_LIMIT_ENV_VAR = "CQ_SMALL_LIVE_MAX_TOTAL_QUOTE_USDT"
ORDER_LIMIT_ENV_VAR = "CQ_SMALL_LIVE_MAX_ORDER_QUOTE_USDT"
DAILY_LOSS_LIMIT_ENV_VAR = "CQ_SMALL_LIVE_MAX_DAILY_LOSS_USDT"
MAX_OPEN_POSITIONS = 2
MAX_PAPER_DRAWDOWN_PCT = 5.0


@dataclass(slots=True)
class SmallLiveConfig:
    enabled: bool = False
    mode: str = "paper"
    environment: str = "development"
    exchange: str = "binance_spot"
    allow_futures: bool = False
    allow_margin: bool = False
    allow_withdrawals: bool = False
    max_total_quote_usdt: float = 0.0
    max_order_quote_usdt: float = 0.0
    max_daily_loss_usdt: float = 0.0
    max_open_positions: int = 0
    allowed_symbols: tuple[str, ...] = field(default_factory=tuple)
    kill_switch_enabled: bool = False
    reconciliation_required: bool = False


@dataclass(slots=True)
class PaperStatus:
    simulation_running: bool
    ws_connected: bool
    market_data_stale: bool
    account_equity: float
    initial_balance: float
    open_notional: float


@dataclass(slots=True)
class ReadinessReport:
    ready: bool
    blockers: list[str]
    warnings: list[str]
    budget_limits_configured: bool

    def as_dict(self) -> dict[str, object]:
        return {
            "ready": self.ready,
            "blockers": self.blockers,
            "warnings": self.warnings,
            "budget_limits_configured": self.budget_limits_configured,
        }


def evaluate_small_live_readiness(
    config: SmallLiveConfig,
    paper_status: PaperStatus,
    *,
    env: Mapping[str, str],
) -> ReadinessReport:
    blockers: list[str] = []
    warnings: list[str] = []
    safety_limits = _load_safety_limits(env)

    if not config.enabled:
        blockers.append("config_disabled")
    if config.mode != "small_live":
        blockers.append("mode_not_small_live")
    if config.environment != "production":
        blockers.append("production_environment_required")
    if env.get(ACK_ENV_VAR) != ACK_ENV_VALUE:
        blockers.append("missing_explicit_ack")

    if config.exchange != "binance_spot" or config.allow_futures:
        blockers.append("spot_only_required")
    if config.allow_margin:
        blockers.append("margin_forbidden")
    if config.allow_withdrawals:
        blockers.append("withdrawals_forbidden")

    if config.max_total_quote_usdt <= 0:
        blockers.append("total_budget_missing")
    elif safety_limits is None or config.max_total_quote_usdt > safety_limits["total"]:
        blockers.append("total_budget_too_large")
    if config.max_order_quote_usdt <= 0:
        blockers.append("order_budget_missing")
    elif safety_limits is None or config.max_order_quote_usdt > safety_limits["order"]:
        blockers.append("order_budget_too_large")
    if config.max_daily_loss_usdt <= 0:
        blockers.append("daily_loss_cap_missing")
    elif safety_limits is None or config.max_daily_loss_usdt > safety_limits["daily_loss"]:
        blockers.append("daily_loss_cap_too_large")
    if safety_limits is None:
        blockers.append("safety_limits_missing")
    if config.max_open_positions <= 0:
        blockers.append("position_limit_missing")
    elif config.max_open_positions > MAX_OPEN_POSITIONS:
        blockers.append("too_many_open_positions_allowed")
    if not config.allowed_symbols:
        blockers.append("allowed_symbols_missing")

    if not config.kill_switch_enabled:
        blockers.append("kill_switch_required")
    if not config.reconciliation_required:
        blockers.append("reconciliation_required")

    if not paper_status.simulation_running:
        blockers.append("paper_not_running")
    if not paper_status.ws_connected:
        blockers.append("paper_ws_disconnected")
    if paper_status.market_data_stale:
        blockers.append("paper_market_data_stale")
    if _paper_drawdown_pct(paper_status) > MAX_PAPER_DRAWDOWN_PCT:
        blockers.append("paper_drawdown_too_large")
    if paper_status.open_notional > paper_status.account_equity * 8:
        warnings.append("paper_open_notional_high")

    return ReadinessReport(
        ready=not blockers,
        blockers=blockers,
        warnings=warnings,
        budget_limits_configured=safety_limits is not None,
    )


def _load_safety_limits(env: Mapping[str, str]) -> dict[str, float] | None:
    total = _positive_float_env(env, TOTAL_LIMIT_ENV_VAR)
    order = _positive_float_env(env, ORDER_LIMIT_ENV_VAR)
    daily_loss = _positive_float_env(env, DAILY_LOSS_LIMIT_ENV_VAR)
    if total is None or order is None or daily_loss is None:
        return None
    return {"daily_loss": daily_loss, "order": order, "total": total}


def _positive_float_env(env: Mapping[str, str], key: str) -> float | None:
    raw = env.get(key)
    if raw is None:
        return None
    try:
        value = float(raw)
    except ValueError:
        return None
    if value <= 0:
        return None
    return value


def _paper_drawdown_pct(status: PaperStatus) -> float:
    if status.initial_balance <= 0:
        return 100.0
    if status.account_equity >= status.initial_balance:
        return 0.0
    return (status.initial_balance - status.account_equity) / status.initial_balance * 100


__all__ = [
    "ACK_ENV_VALUE",
    "DAILY_LOSS_LIMIT_ENV_VAR",
    "ORDER_LIMIT_ENV_VAR",
    "TOTAL_LIMIT_ENV_VAR",
    "PaperStatus",
    "ReadinessReport",
    "SmallLiveConfig",
    "evaluate_small_live_readiness",
]
