from __future__ import annotations

import asyncio
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, ClassVar

import pytest
from fastapi import FastAPI

import dashboard.server as dashboard_server
from core.data.exchange.base import Bar
from core.data.memory_cache import MemoryCache
from core.data.parquet_io import ParquetIO
from core.data.sqlite_repo import SqliteRepo
from core.execution.paper_engine import PaperMatchingEngine
from dashboard.server import (
    LiveDataFeeder,
    _apply_rest_price_rows,
    _apply_ticker_24h_rows,
    _compute_positions,
    _start_of_day_ts,
    _start_of_week_ts,
    create_app,
)


class _DummyWs:
    _running = True


class _DummyFeeder:
    _bar_counter = 7
    _ws = _DummyWs()
    _last_bar_ms = 9_999_999_999_999
    _bar_stale_after_ms = 120_000
    _running = True
    started = 0
    stopped = 0

    async def start(self) -> None:
        self.started += 1
        self._running = True
        self._ws._running = True

    async def stop(self) -> None:
        self.stopped += 1
        self._running = False
        self._ws._running = False


def _build_app(tmp_path: Path, conn: sqlite3.Connection) -> FastAPI:
    repo = SqliteRepo(conn)
    cache = MemoryCache(max_bars=10)
    parquet_io = ParquetIO(data_root=tmp_path / "parquet")
    engine = PaperMatchingEngine(repo, get_price=lambda symbol: 50_000.0)
    static_dir = tmp_path / "static"
    static_dir.mkdir()
    (static_dir / "index.html").write_text("<html></html>", encoding="utf-8")
    return create_app(cache, repo, parquet_io, engine, _DummyFeeder(), static_dir)


def _call_route(app: FastAPI, path: str, **kwargs: Any) -> Any:
    for route in app.routes:
        if getattr(route, "path", None) == path:
            return route.endpoint(**kwargs)
    raise AssertionError(f"route not found: {path}")


def _safe_small_live_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CQ_SMALL_LIVE_ACK", "I_UNDERSTAND_REAL_MONEY_RISK")
    monkeypatch.setenv("CQ_SMALL_LIVE_MAX_TOTAL_QUOTE_USDT", str(10 * 5))
    monkeypatch.setenv("CQ_SMALL_LIVE_MAX_ORDER_QUOTE_USDT", str(len("order")))
    monkeypatch.setenv("CQ_SMALL_LIVE_MAX_DAILY_LOSS_USDT", str(len("order")))


def _write_small_live_config(tmp_path: Path) -> Path:
    config_path = tmp_path / "small_live.yml"
    config_path.write_text(
        f"""
enabled: true
mode: small_live
environment: production
exchange: binance_spot
allow_futures: false
allow_margin: false
allow_withdrawals: false
max_total_quote_usdt: {10 * 4}
max_order_quote_usdt: {len("order")}
max_daily_loss_usdt: {len("cap")}
max_open_positions: 1
allowed_symbols: [BTCUSDT]
kill_switch_enabled: true
reconciliation_required: true
""".strip(),
        encoding="utf-8",
    )
    return config_path


def test_paper_strategy_notional_multipliers_keep_profitable_strategy_only() -> None:
    multipliers = dashboard_server._paper_strategy_notional_multipliers()

    assert multipliers == {
        "paper_mean_reversion": 1.0,
        "paper_trend_momentum": 1.0,
        "paper_swing_breakout": 1.0,
    }


def test_dashboard_small_live_preflight_exposes_safe_readiness_without_secrets(
    tmp_path: Path, tmp_db: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    _safe_small_live_env(monkeypatch)
    app = _build_app(tmp_path, tmp_db)
    app.state.small_live_config_path = _write_small_live_config(tmp_path)

    payload = _call_route(app, "/api/small_live/preflight")

    assert payload["ready"] is True
    assert payload["mode"] == "small_live"
    assert payload["spot_only"] is True
    assert payload["credentials_present"] is False
    assert payload["allowed_symbols"] == ["BTCUSDT"]
    assert "api_secret" not in str(payload).lower()


@pytest.mark.asyncio
async def test_dashboard_small_live_order_dry_run_never_submits(
    tmp_path: Path, tmp_db: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    _safe_small_live_env(monkeypatch)
    app = _build_app(tmp_path, tmp_db)
    app.state.small_live_config_path = _write_small_live_config(tmp_path)

    payload = await _call_route(
        app,
        "/api/small_live/order",
        payload={
            "symbol": "BTCUSDT",
            "side": "buy",
            "order_type": "market",
            "quantity": 0.001,
            "stop_loss_price": 49_000.0,
            "client_order_id": "ui-dry-run-1",
            "dry_run": True,
        },
    )

    assert payload["ready"] is True
    assert payload["dry_run"] is True
    assert payload["would_submit"] is True
    assert payload["submitted"] is False
    assert payload["order"]["symbol"] == "BTCUSDT"
    assert payload["order"]["has_stop_loss"] is True


@pytest.mark.asyncio
async def test_dashboard_small_live_order_blocks_real_submit_without_confirmation(
    tmp_path: Path, tmp_db: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    _safe_small_live_env(monkeypatch)
    app = _build_app(tmp_path, tmp_db)
    app.state.small_live_config_path = _write_small_live_config(tmp_path)

    payload = await _call_route(
        app,
        "/api/small_live/order",
        payload={
            "symbol": "BTCUSDT",
            "side": "buy",
            "quantity": 0.001,
            "stop_loss_price": 49_000.0,
            "client_order_id": "ui-live-1",
            "dry_run": False,
        },
    )

    assert payload["ready"] is True
    assert payload["submitted"] is False
    assert "missing_live_order_confirmation" in payload["blockers"]


def test_dashboard_risk_events_endpoint_parses_payload(
    tmp_path: Path, tmp_db: sqlite3.Connection
) -> None:
    repo = SqliteRepo(tmp_db)
    first_id = repo.insert_risk_event(
        {
            "type": "order_rejected",
            "severity": "warn",
            "source": "L1",
            "related_id": 10,
            "payload": '{"reason":"min_notional"}',
            "captured_at": 1_700_000_000_000,
        }
    )
    second_id = repo.insert_risk_event(
        {
            "type": "order_rejected",
            "severity": "critical",
            "source": "L3",
            "related_id": 11,
            "payload": '{"reason":"gross_leverage"}',
            "captured_at": 1_700_000_001_000,
        }
    )
    repo.insert_risk_event(
        {
            "type": "paper_signal_skipped",
            "severity": "info",
            "source": "dashboard_trader",
            "related_id": None,
            "payload": '{"reason": "cooldown", "symbol": "BTCUSDT"}',
            "captured_at": 1_700_000_002_000,
        }
    )
    repo.insert_risk_event(
        {
            "type": "paper_signal_skipped",
            "severity": "warn",
            "source": "dashboard_trader",
            "related_id": None,
            "payload": '{"reason":"symbol_order_cap","symbol":"BTCUSDT"}',
            "captured_at": 1_700_000_003_000,
        }
    )
    app = _build_app(tmp_path, tmp_db)

    rows: list[dict[str, Any]] = _call_route(app, "/api/risk_events", limit=10, since_ms=None)

    assert [row["id"] for row in rows] == [second_id, first_id]
    assert rows[0]["payload"] == {"reason": "gross_leverage"}
    assert rows[0]["reason"] == "gross_leverage"
    assert rows[0]["type_label"] == "订单拒绝"
    assert rows[0]["reason_label"] == "组合杠杆过高"
    assert rows[0]["action"] == "order_rejected"
    assert rows[0]["source"] == "L3"
    assert rows[0]["source_label"] == "组合风控"
    assert rows[0]["severity_label"] == "严重"
    assert rows[0]["ts"] == 1_700_000_001_000
    assert rows[0]["detail"] == "组合杠杆过高"
    assert rows[1]["payload"] == {"reason": "min_notional"}
    assert rows[1]["reason"] == "min_notional"
    assert rows[1]["reason_label"] == "低于最小下单金额"


def test_dashboard_risk_events_endpoint_flattens_signal_context(
    tmp_path: Path, tmp_db: sqlite3.Connection
) -> None:
    repo = SqliteRepo(tmp_db)
    event_id = repo.insert_risk_event(
        {
            "type": "paper_signal_skipped",
            "severity": "warn",
            "source": "dashboard_trader",
            "related_id": None,
            "payload": '{"reason":"min_notional","symbol":"BTCUSDT","strategy":"paper_momentum"}',
            "captured_at": 1_700_000_000_000,
        }
    )
    app = _build_app(tmp_path, tmp_db)

    rows: list[dict[str, Any]] = _call_route(app, "/api/risk_events", limit=10, since_ms=None)

    assert rows == [
        {
            "id": event_id,
            "type": "paper_signal_skipped",
            "action": "paper_signal_skipped",
            "severity": "warn",
            "source": "dashboard_trader",
            "related_id": None,
            "payload": {"reason": "min_notional", "symbol": "BTCUSDT", "strategy": "paper_momentum"},
            "reason": "min_notional",
            "reason_label": "低于最小下单金额",
            "type_label": "纸面信号跳过",
            "severity_label": "警告",
            "source_label": "模拟交易调度",
            "symbol": "BTCUSDT",
            "strategy": "paper_momentum",
            "detail": "低于最小下单金额 / BTCUSDT / paper_momentum",
            "captured_at": 1_700_000_000_000,
            "ts": 1_700_000_000_000,
        }
    ]


def test_dashboard_status_includes_risk_event_counts(
    tmp_path: Path, tmp_db: sqlite3.Connection
) -> None:
    repo = SqliteRepo(tmp_db)
    repo.insert_risk_event(
        {
            "type": "order_rejected",
            "severity": "warn",
            "source": "L1",
            "related_id": None,
            "payload": '{"reason":"min_notional"}',
            "captured_at": 1_700_000_000_000,
        }
    )
    repo.insert_risk_event(
        {
            "type": "circuit_breaker",
            "severity": "critical",
            "source": "L3",
            "related_id": None,
            "payload": '{"reason":"drawdown"}',
            "captured_at": 1_700_000_001_000,
        }
    )
    repo.insert_risk_event(
        {
            "type": "paper_signal_skipped",
            "severity": "info",
            "source": "dashboard_trader",
            "related_id": None,
            "payload": '{"reason":"cooldown","symbol":"BTCUSDT"}',
            "captured_at": 1_700_000_002_000,
        }
    )
    repo.insert_risk_event(
        {
            "type": "paper_signal_skipped",
            "severity": "warn",
            "source": "dashboard_trader",
            "related_id": None,
            "payload": '{"reason":"symbol_order_cap","symbol":"BTCUSDT"}',
            "captured_at": 1_700_000_003_000,
        }
    )
    app = _build_app(tmp_path, tmp_db)

    payload = _call_route(app, "/api/status")

    assert payload["risk_events_n"] == 2
    assert payload["raw_risk_events_n"] == 4
    assert payload["critical_risk_events_n"] == 1


def test_dashboard_status_exposes_live_feeder_running_state(
    tmp_path: Path, tmp_db: sqlite3.Connection
) -> None:
    app = _build_app(tmp_path, tmp_db)

    payload = _call_route(app, "/api/status")

    assert payload["mode"] == "live_paper"
    assert payload["ws_connected"] is True
    assert payload["simulation_running"] is True
    assert payload["bars_received"] == 7
    assert payload["market_data_stale"] is False
    assert payload["last_bar_age_ms"] >= 0


def test_dashboard_status_exposes_data_source_identity(
    tmp_path: Path, tmp_db: sqlite3.Connection
) -> None:
    repo = SqliteRepo(tmp_db)
    repo.upsert_symbols(
        [
            {
                "exchange": "binance",
                "symbol": "BTCUSDT",
                "type": "perp",
                "base": "BTC",
                "quote": "USDT",
                "tick_size": 0.1,
                "lot_size": 0.001,
                "min_notional": 10.0,
                "listed_at": 1,
            }
        ]
    )
    symbol = repo.get_symbol("BTCUSDT")
    assert symbol is not None
    order_id = repo.insert_order(
        {
            "client_order_id": "source-check-1",
            "symbol_id": symbol["id"],
            "side": "buy",
            "type": "market",
            "price": 10.0,
            "stop_price": None,
            "quantity": 1.0,
            "filled_qty": 1.0,
            "avg_fill_price": 10.0,
            "status": "filled",
            "purpose": "entry",
            "strategy_version": "explore_momentum",
            "placed_at": 1_700_000_000_000,
            "updated_at": 1_700_000_000_100,
        }
    )
    repo.insert_fill(
        {
            "order_id": order_id,
            "exchange_fill_id": "source-check-fill",
            "price": 10.0,
            "quantity": 1.0,
            "fee": 0.01,
            "fee_currency": "USDT",
            "ts": 1_700_000_000_050,
        }
    )
    app = _build_app(tmp_path, tmp_db)

    payload = _call_route(app, "/api/status")

    assert payload["workspace_path"].endswith("crypto-quant")
    assert payload["db_path"] == ":memory:"
    assert payload["data_started_at"] == 1_700_000_000_000
    assert payload["fills_count"] == 1


def test_dashboard_status_filters_legacy_strategy_counts_for_active_runtime(
    tmp_path: Path, tmp_db: sqlite3.Connection
) -> None:
    repo = SqliteRepo(tmp_db)
    repo.upsert_symbols(
        [
            {
                "exchange": "binance",
                "symbol": "BTCUSDT",
                "type": "perp",
                "base": "BTC",
                "quote": "USDT",
                "tick_size": 0.1,
                "lot_size": 0.001,
                "min_notional": 10.0,
                "listed_at": 1,
            }
        ]
    )
    symbol = repo.get_symbol("BTCUSDT")
    assert symbol is not None
    for strategy in ["explore_momentum", "S1_btc_eth_trend"]:
        order_id = repo.insert_order(
            {
                "client_order_id": f"source-{strategy}",
                "symbol_id": symbol["id"],
                "side": "buy",
                "type": "market",
                "price": 10.0,
                "stop_price": None,
                "quantity": 1.0,
                "filled_qty": 1.0,
                "avg_fill_price": 10.0,
                "status": "filled",
                "purpose": "entry",
                "strategy_version": strategy,
                "placed_at": 1_700_000_000_000,
                "updated_at": 1_700_000_000_100,
            }
        )
        repo.insert_fill(
            {
                "order_id": order_id,
                "exchange_fill_id": f"fill-{strategy}",
                "price": 10.0,
                "quantity": 1.0,
                "fee": 0.01,
                "fee_currency": "USDT",
                "ts": 1_700_000_000_050,
            }
        )
    app = _build_app(tmp_path, tmp_db)
    app.state.active_strategy_ids = ["S1_btc_eth_trend"]

    payload = _call_route(app, "/api/status")

    assert payload["orders_count"] == 1
    assert payload["fills_count"] == 1


def test_dashboard_fills_endpoint_reports_per_fill_pnl(
    tmp_path: Path, tmp_db: sqlite3.Connection
) -> None:
    repo = SqliteRepo(tmp_db)
    repo.upsert_symbols(
        [
            {
                "exchange": "binance",
                "symbol": "BTCUSDT",
                "type": "perp",
                "base": "BTC",
                "quote": "USDT",
                "tick_size": 0.1,
                "lot_size": 0.001,
                "min_notional": 10.0,
                "listed_at": 1,
            }
        ]
    )
    symbol = repo.get_symbol("BTCUSDT")
    assert symbol is not None
    buy_order_id = repo.insert_order(
        {
            "client_order_id": "fill-pnl-buy",
            "symbol_id": symbol["id"],
            "side": "buy",
            "type": "market",
            "price": 100.0,
            "stop_price": None,
            "quantity": 1.0,
            "filled_qty": 1.0,
            "avg_fill_price": 100.0,
            "status": "filled",
            "purpose": "entry",
            "strategy_version": "paper_momentum",
            "placed_at": 1_700_000_000_000,
            "updated_at": 1_700_000_000_000,
        }
    )
    repo.insert_fill(
        {
            "order_id": buy_order_id,
            "exchange_fill_id": "fill-pnl-buy",
            "price": 100.0,
            "quantity": 1.0,
            "fee": 0.1,
            "fee_currency": "USDT",
            "ts": 1_700_000_000_000,
        }
    )
    sell_order_id = repo.insert_order(
        {
            "client_order_id": "fill-pnl-sell",
            "symbol_id": symbol["id"],
            "side": "sell",
            "type": "market",
            "price": 110.0,
            "stop_price": None,
            "quantity": 1.0,
            "filled_qty": 1.0,
            "avg_fill_price": 110.0,
            "status": "filled",
            "purpose": "exit",
            "strategy_version": "paper_momentum",
            "placed_at": 1_700_000_001_000,
            "updated_at": 1_700_000_001_000,
        }
    )
    repo.insert_fill(
        {
            "order_id": sell_order_id,
            "exchange_fill_id": "fill-pnl-sell",
            "price": 110.0,
            "quantity": 1.0,
            "fee": 0.1,
            "fee_currency": "USDT",
            "ts": 1_700_000_001_000,
        }
    )
    app = _build_app(tmp_path, tmp_db)

    rows: list[dict[str, Any]] = _call_route(app, "/api/fills", limit=10)

    assert rows[0]["id"] == 2
    assert rows[0]["gross_pnl"] == 10.0
    assert rows[0]["net_pnl"] == 9.9
    assert rows[1]["id"] == 1
    assert rows[1]["gross_pnl"] == 0.0
    assert rows[1]["net_pnl"] == -0.1


def test_dashboard_pnl_windows_use_shanghai_calendar_boundaries() -> None:
    shanghai = timezone(timedelta(hours=8))
    now_s = datetime(2026, 5, 9, 19, 30, tzinfo=shanghai).timestamp()

    assert _start_of_day_ts(now_s=now_s) == int(
        datetime(2026, 5, 9, 0, 0, tzinfo=shanghai).timestamp() * 1000
    )
    assert _start_of_week_ts(now_s=now_s) == int(
        datetime(2026, 5, 4, 0, 0, tzinfo=shanghai).timestamp() * 1000
    )


def test_dashboard_status_reports_futures_equity_and_available_balance(
    tmp_path: Path, tmp_db: sqlite3.Connection
) -> None:
    repo = SqliteRepo(tmp_db)
    repo.upsert_symbols(
        [
            {
                "exchange": "binance",
                "symbol": "BTCUSDT",
                "type": "perp",
                "base": "BTC",
                "quote": "USDT",
                "tick_size": 0.1,
                "lot_size": 0.001,
                "min_notional": 10.0,
                "listed_at": 1,
            }
        ]
    )
    symbol = repo.get_symbol("BTCUSDT")
    assert symbol is not None
    repo.insert_position(
        {
            "symbol_id": symbol["id"],
            "strategy": "explore_momentum",
            "strategy_version": "explore_momentum",
            "opening_signal_id": None,
            "side": "long",
            "qty": 0.1,
            "avg_entry_price": 50_000.0,
            "current_price": 50_000.0,
            "unrealized_pnl": 0.0,
            "realized_pnl": 0.0,
            "leverage": 1.0,
            "margin": None,
            "liq_price": None,
            "stop_order_id": None,
            "trade_group_id": None,
            "opened_at": 1_700_000_000_000,
            "closed_at": None,
        }
    )
    app = _build_app(tmp_path, tmp_db)
    app.state.cache.update_latest_price("BTCUSDT", 50_500.0)

    payload = _call_route(app, "/api/status")

    assert payload["initial_balance"] == 1_000.0
    assert payload["portfolio_value"] == 1_050.0
    assert payload["account_equity"] == 1_050.0
    assert payload["used_margin"] == 202.0
    assert payload["usdt_balance"] == 848.0
    assert payload["available_balance"] == 848.0
    assert payload["day_pnl"]["pnl"] == 50.0
    assert payload["day_pnl"]["unrealized_pnl"] == 50.0


def test_dashboard_status_reconciles_positions_fees_and_margin(
    tmp_path: Path, tmp_db: sqlite3.Connection
) -> None:
    repo = SqliteRepo(tmp_db)
    repo.upsert_symbols(
        [
            {
                "exchange": "binance",
                "symbol": "BTCUSDT",
                "type": "perp",
                "base": "BTC",
                "quote": "USDT",
                "tick_size": 0.1,
                "lot_size": 0.001,
                "min_notional": 10.0,
                "listed_at": 1,
            }
        ]
    )
    symbol = repo.get_symbol("BTCUSDT")
    assert symbol is not None
    order_id = repo.insert_order(
        {
            "client_order_id": "fee-check",
            "symbol_id": symbol["id"],
            "side": "sell",
            "type": "market",
            "price": 51_000.0,
            "quantity": 0.1,
            "filled_qty": 0.1,
            "avg_fill_price": 51_000.0,
            "status": "filled",
            "purpose": "entry",
            "strategy_version": "explore_momentum",
            "placed_at": 1_700_000_000_000,
            "updated_at": 1_700_000_000_000,
        }
    )
    repo.insert_fill(
        {
            "order_id": order_id,
            "exchange_fill_id": "fee-check-fill",
            "price": 51_000.0,
            "quantity": 0.1,
            "fee": 2.0,
            "fee_currency": "USDT",
            "ts": 1_700_000_000_100,
        }
    )
    for side, qty, realized, closed_at in [
        ("long", 0.1, 7.0, None),
        ("short", 0.0, 3.0, 1_700_000_010_000),
    ]:
        repo.insert_position(
            {
                "symbol_id": symbol["id"],
                "strategy": "explore_momentum",
                "strategy_version": "explore_momentum",
                "opening_signal_id": None,
                "side": side,
                "qty": qty,
                "avg_entry_price": 50_000.0,
                "current_price": 50_500.0,
                "unrealized_pnl": 0.0,
                "realized_pnl": realized,
                "leverage": 1.0,
                "margin": None,
                "liq_price": None,
                "stop_order_id": None,
                "trade_group_id": None,
                "opened_at": 1_700_000_000_000,
                "closed_at": closed_at,
            }
        )
    app = _build_app(tmp_path, tmp_db)

    payload = _call_route(app, "/api/status")

    assert payload["gross_realized_pnl"] == 10.0
    assert payload["fees_paid"] == 2.0
    assert payload["realized_pnl"] == 8.0
    assert payload["unrealized_pnl"] == 50.0
    assert payload["portfolio_value"] == 1_058.0
    assert payload["open_notional"] == 5_050.0
    assert payload["used_margin"] == 202.0
    assert payload["available_balance"] == 856.0


def test_dashboard_positions_preserve_low_price_precision(
    tmp_db: sqlite3.Connection,
) -> None:
    repo = SqliteRepo(tmp_db)
    repo.upsert_symbols(
        [
            {
                "exchange": "binance",
                "symbol": "GALAUSDT",
                "type": "perp",
                "base": "GALA",
                "quote": "USDT",
                "tick_size": 0.000001,
                "lot_size": 1.0,
                "min_notional": 5.0,
                "listed_at": 1,
            }
        ]
    )
    symbol = repo.get_symbol("GALAUSDT")
    assert symbol is not None
    repo.insert_position(
        {
            "symbol_id": symbol["id"],
            "strategy": "S2_altcoin_reversal",
            "strategy_version": "S2_altcoin_reversal",
            "opening_signal_id": None,
            "side": "long",
            "qty": 1000.0,
            "avg_entry_price": 0.004251,
            "current_price": 0.004251,
            "unrealized_pnl": 0.0,
            "realized_pnl": 0.0,
            "leverage": 1.0,
            "margin": None,
            "liq_price": None,
            "stop_order_id": None,
            "trade_group_id": None,
            "opened_at": 1_700_000_000_000,
            "closed_at": None,
        }
    )
    cache = MemoryCache(max_bars=10)
    cache.update_latest_price("GALAUSDT", 0.004267)

    positions = _compute_positions(repo, cache)

    assert positions[0]["entry_price"] == 0.004251
    assert positions[0]["current_price"] == 0.004267


def test_dashboard_price_refresh_includes_open_position_symbols(tmp_db: sqlite3.Connection) -> None:
    repo = SqliteRepo(tmp_db)
    repo.upsert_symbols(
        [
            {
                "exchange": "binance",
                "symbol": "DOGEUSDT",
                "type": "perp",
                "base": "DOGE",
                "quote": "USDT",
                "tick_size": 0.00001,
                "lot_size": 1.0,
                "min_notional": 5.0,
                "listed_at": 1,
            }
        ]
    )
    symbol = repo.get_symbol("DOGEUSDT")
    assert symbol is not None
    repo.insert_position(
        {
            "symbol_id": symbol["id"],
            "strategy": "paper_momentum",
            "strategy_version": "paper_momentum",
            "opening_signal_id": None,
            "side": "long",
            "qty": 100.0,
            "avg_entry_price": 0.1,
            "current_price": 0.1,
            "unrealized_pnl": 0.0,
            "realized_pnl": 0.0,
            "leverage": 1.0,
            "margin": None,
            "liq_price": None,
            "stop_order_id": None,
            "trade_group_id": None,
            "opened_at": 1_700_000_000_000,
            "closed_at": None,
        }
    )

    symbols = dashboard_server._symbols_with_open_positions(repo, ["BTCUSDT"])

    assert symbols == ["BTCUSDT", "DOGEUSDT"]


def test_dashboard_price_snapshot_matches_prices_endpoint_shape() -> None:
    cache = MemoryCache(max_bars=10)
    cache.update_latest_price("BTCUSDT", 50_000.0, source_ts=1_700_000_000_000, updated_at_ms=1_700_000_001_000)

    payload = dashboard_server._price_snapshot(
        cache,
        {"BTCUSDT": {"change_24h": 1.25, "high_24h": 51_000.0, "low_24h": 49_000.0, "quote_volume": 123.0}},
        now_ms=1_700_000_002_000,
    )

    assert payload == {
        "BTCUSDT": {
            "price": 50_000.0,
            "change_24h": 1.25,
            "high_24h": 51_000.0,
            "low_24h": 49_000.0,
            "source_ts": 1_700_000_000_000,
            "updated_at": 1_700_000_001_000,
            "age_ms": 1_000,
            "quote_volume": 123.0,
        }
    }


def test_dashboard_status_marks_stale_feeder_unhealthy(
    tmp_path: Path, tmp_db: sqlite3.Connection
) -> None:
    app = _build_app(tmp_path, tmp_db)
    app.state.feeder._last_bar_ms = 1
    app.state.feeder._bar_stale_after_ms = 1

    payload = _call_route(app, "/api/status")

    assert payload["ws_connected"] is True
    assert payload["simulation_running"] is False
    assert payload["market_data_stale"] is True


def test_dashboard_status_keeps_running_when_rest_fallback_is_fresh(
    tmp_path: Path, tmp_db: sqlite3.Connection
) -> None:
    app = _build_app(tmp_path, tmp_db)
    app.state.feeder._running = True
    app.state.feeder._ws._running = False
    app.state.feeder._last_bar_ms = int(time.time() * 1000)

    payload = _call_route(app, "/api/status")

    assert payload["simulation_running"] is True
    assert payload["ws_connected"] is False
    assert payload["market_data_stale"] is False


async def test_dashboard_control_endpoint_starts_and_stops_feeder(
    tmp_path: Path, tmp_db: sqlite3.Connection
) -> None:
    app = _build_app(tmp_path, tmp_db)

    stopped = await _call_route(app, "/api/control", payload={"action": "stop"})
    started = await _call_route(app, "/api/control", payload={"action": "start"})

    assert stopped["simulation_running"] is False
    assert started["simulation_running"] is True


async def test_dashboard_control_reports_strategy_evaluation_order_count(
    tmp_path: Path, tmp_db: sqlite3.Connection
) -> None:
    repo = SqliteRepo(tmp_db)
    cache = MemoryCache(max_bars=10)
    parquet_io = ParquetIO(data_root=tmp_path / "parquet")
    engine = PaperMatchingEngine(repo, get_price=lambda symbol: 50_000.0)
    static_dir = tmp_path / "static"
    static_dir.mkdir()
    (static_dir / "index.html").write_text("<html></html>", encoding="utf-8")

    class _Trader:
        symbols: ClassVar[list[str]] = ["BTCUSDT"]
        strategies: ClassVar[list[str]] = ["S1_btc_eth_trend"]

        def __init__(self) -> None:
            self.seen_timeframes: list[str] = []

        def on_bar(self, bar: Bar, now_ms: int | None = None) -> list[Any]:
            self.seen_timeframes.append(bar.timeframe)
            return []

    trader = _Trader()
    app = create_app(cache, repo, parquet_io, engine, _DummyFeeder(), static_dir, trader=trader)  # type: ignore[arg-type]

    payload = await _call_route(app, "/api/control", payload={"action": "random_order"})

    assert payload["ok"] is True
    assert payload["orders_generated"] == 0
    assert payload["message"] == "no_core_strategy_signal"
    assert trader.seen_timeframes


async def test_live_feeder_falls_back_to_direct_ws_when_proxy_fails(
    tmp_path: Path, tmp_db: sqlite3.Connection, monkeypatch: Any
) -> None:
    class _ProxyAwareWs:
        instances: ClassVar[list[_ProxyAwareWs]] = []

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.connect_proxies: list[str] = []
            self._running = False
            _ProxyAwareWs.instances.append(self)

        def subscribe_candles(self, *args: Any, **kwargs: Any) -> None:
            pass

        def subscribe_tickers(self, *args: Any, **kwargs: Any) -> None:
            pass

        async def connect(self, proxy: str = "") -> None:
            self.connect_proxies.append(proxy)
            if proxy:
                raise RuntimeError("502 Bad Gateway")
            self._running = True

        async def close(self) -> None:
            self._running = False

    monkeypatch.setattr(dashboard_server, "WsSubscriber", _ProxyAwareWs)
    repo = SqliteRepo(tmp_db)
    cache = MemoryCache(max_bars=10)
    parquet_io = ParquetIO(data_root=tmp_path / "parquet")
    engine = PaperMatchingEngine(repo, get_price=lambda symbol: 50_000.0)
    feeder = LiveDataFeeder(
        cache,
        parquet_io,
        repo,
        engine,
        proxy="http://127.0.0.1:57777",
        symbols=["BTCUSDT"],
    )

    await feeder._connect_ws()

    assert len(_ProxyAwareWs.instances) == 2
    assert _ProxyAwareWs.instances[0].connect_proxies == ["http://127.0.0.1:57777"]
    assert _ProxyAwareWs.instances[1].connect_proxies == [""]
    assert feeder._proxy == ""
    assert feeder._ws is _ProxyAwareWs.instances[1]


async def test_live_feeder_start_keeps_rest_fallback_running_when_ws_unavailable(
    tmp_path: Path, tmp_db: sqlite3.Connection, monkeypatch: Any
) -> None:
    repo = SqliteRepo(tmp_db)
    cache = MemoryCache(max_bars=10)
    parquet_io = ParquetIO(data_root=tmp_path / "parquet")
    engine = PaperMatchingEngine(repo, get_price=lambda symbol: 50_000.0)
    feeder = LiveDataFeeder(cache, parquet_io, repo, engine, symbols=["BTCUSDT"])

    async def _refresh_universe(*, timeframes: tuple[str, ...] | None = None) -> None:
        feeder._symbols = ["BTCUSDT"]

    async def _connect_ws() -> None:
        raise RuntimeError("ws unavailable")

    async def _idle_loop() -> None:
        await asyncio.sleep(3600)

    monkeypatch.setattr(feeder, "_refresh_universe", _refresh_universe)
    monkeypatch.setattr(feeder, "_connect_ws", _connect_ws)
    monkeypatch.setattr(feeder, "_lazy_load_exchange", _idle_loop)
    monkeypatch.setattr(feeder, "_engine_loop", _idle_loop)
    monkeypatch.setattr(feeder, "_rest_price_loop", _idle_loop)
    monkeypatch.setattr(feeder, "_bar_watchdog_loop", _idle_loop)
    monkeypatch.setattr(feeder, "_rest_bar_fallback_loop", _idle_loop)

    await feeder.start()

    try:
        assert feeder._running is True
        assert feeder._task is not None
        assert feeder._price_task is not None
        assert feeder._watchdog_task is not None
        assert feeder._rest_bar_task is not None
        row = repo._conn.execute(
            "SELECT status, note FROM run_log WHERE endpoint=?",
            ("dashboard_ws_connect",),
        ).fetchone()
        assert row["status"] == "fail"
        assert "ws unavailable" in row["note"]
    finally:
        await feeder.stop()


async def test_live_feeder_start_connects_ws_after_fast_1m_warmup(
    tmp_path: Path, tmp_db: sqlite3.Connection, monkeypatch: Any
) -> None:
    repo = SqliteRepo(tmp_db)
    cache = MemoryCache(max_bars=10)
    parquet_io = ParquetIO(data_root=tmp_path / "parquet")
    engine = PaperMatchingEngine(repo, get_price=lambda symbol: 50_000.0)
    feeder = LiveDataFeeder(cache, parquet_io, repo, engine, symbols=["BTCUSDT"])
    events: list[tuple[str, tuple[str, ...] | None]] = []

    async def _refresh_universe(*, timeframes: tuple[str, ...] | None = None) -> None:
        events.append(("refresh_universe", timeframes))
        feeder._symbols = ["BTCUSDT"]

    async def _backfill_recent_bars(symbols: list[str], *, publish: bool = False, publish_latest: bool = False) -> None:
        events.append(("background_warmup", ("publish_latest",) if publish_latest else None))

    async def _connect_ws() -> None:
        events.append(("connect_ws", None))

    async def _idle_loop() -> None:
        await asyncio.sleep(3600)

    monkeypatch.setattr(feeder, "_refresh_universe", _refresh_universe)
    monkeypatch.setattr(feeder, "_backfill_recent_bars", _backfill_recent_bars)
    monkeypatch.setattr(feeder, "_connect_ws", _connect_ws)
    monkeypatch.setattr(feeder, "_lazy_load_exchange", _idle_loop)
    monkeypatch.setattr(feeder, "_engine_loop", _idle_loop)
    monkeypatch.setattr(feeder, "_rest_price_loop", _idle_loop)
    monkeypatch.setattr(feeder, "_bar_watchdog_loop", _idle_loop)
    monkeypatch.setattr(feeder, "_rest_bar_fallback_loop", _idle_loop)
    monkeypatch.setattr(feeder, "_ws_reconnect_loop", _idle_loop)

    await feeder.start()
    try:
        await asyncio.sleep(0)
        assert events[:2] == [
            ("refresh_universe", ("1m",)),
            ("connect_ws", None),
        ]
        assert ("background_warmup", ("publish_latest",)) in events
    finally:
        await feeder.stop()


async def test_live_feeder_reconnect_loop_recovers_ws_after_start_failure(
    tmp_path: Path, tmp_db: sqlite3.Connection
) -> None:
    repo = SqliteRepo(tmp_db)
    cache = MemoryCache(max_bars=10)
    parquet_io = ParquetIO(data_root=tmp_path / "parquet")
    engine = PaperMatchingEngine(repo, get_price=lambda symbol: 50_000.0)
    feeder = LiveDataFeeder(cache, parquet_io, repo, engine, symbols=["BTCUSDT"])
    attempts = 0

    class _ConnectedWs:
        _running = True

    async def _connect_ws() -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("temporary ws outage")
        feeder._ws = _ConnectedWs()  # type: ignore[assignment]
        feeder._running = False

    feeder._running = True
    feeder._connect_ws = _connect_ws  # type: ignore[method-assign]

    await feeder._ws_reconnect_loop(interval_sec=0)

    assert attempts == 2
    assert feeder._ws is not None
    row = repo._conn.execute(
        "SELECT status FROM run_log WHERE endpoint=? ORDER BY id DESC LIMIT 1",
        ("dashboard_ws_reconnect",),
    ).fetchone()
    assert row["status"] == "ok"


async def test_live_feeder_ws_connect_times_out_slow_candidates(
    tmp_path: Path, tmp_db: sqlite3.Connection, monkeypatch: Any
) -> None:
    class _SlowWs:
        def subscribe_candles(self, *args: Any, **kwargs: Any) -> None:
            pass

        def subscribe_tickers(self, *args: Any, **kwargs: Any) -> None:
            pass

        async def connect(self, proxy: str = "") -> None:
            await asyncio.sleep(3600)

        async def close(self) -> None:
            pass

    monkeypatch.setattr(dashboard_server, "WsSubscriber", lambda *args, **kwargs: _SlowWs())
    repo = SqliteRepo(tmp_db)
    cache = MemoryCache(max_bars=10)
    parquet_io = ParquetIO(data_root=tmp_path / "parquet")
    engine = PaperMatchingEngine(repo, get_price=lambda symbol: 50_000.0)
    feeder = LiveDataFeeder(cache, parquet_io, repo, engine, symbols=["BTCUSDT"])
    feeder._ws_connect_timeout_sec = 0.01

    with pytest.raises(TimeoutError):
        await feeder._connect_ws()


async def test_live_feeder_falls_back_to_direct_rest_when_proxy_fails(
    tmp_path: Path, tmp_db: sqlite3.Connection, monkeypatch: Any
) -> None:
    top_symbol_proxies: list[str] = []
    kline_proxies: list[str] = []

    async def _fetch_top_symbols(*, proxy: str = "", limit: int = 30) -> list[str]:
        top_symbol_proxies.append(proxy)
        if proxy:
            raise RuntimeError("502 Bad Gateway")
        return ["BTCUSDT"]

    async def _fetch_klines(
        symbol: str, timeframe: str, *, proxy: str = "", limit: int = 20
    ) -> list[Bar]:
        kline_proxies.append(proxy)
        return []

    monkeypatch.setattr(dashboard_server, "fetch_binance_top_usdt_symbols", _fetch_top_symbols)
    monkeypatch.setattr(dashboard_server, "fetch_binance_recent_klines", _fetch_klines)
    repo = SqliteRepo(tmp_db)
    cache = MemoryCache(max_bars=10)
    parquet_io = ParquetIO(data_root=tmp_path / "parquet")
    engine = PaperMatchingEngine(repo, get_price=lambda symbol: 50_000.0)
    feeder = LiveDataFeeder(
        cache,
        parquet_io,
        repo,
        engine,
        proxy="http://127.0.0.1:57777",
        symbols=["ETHUSDT"],
    )

    await feeder._refresh_universe()

    assert top_symbol_proxies == ["http://127.0.0.1:57777", ""]
    assert kline_proxies == ["", "", "", "", "", ""]
    assert feeder._proxy == ""
    assert feeder._symbols == ["BTCUSDT"]


async def test_live_feeder_recovers_preferred_proxy_after_direct_rest_fails(
    tmp_path: Path, tmp_db: sqlite3.Connection, monkeypatch: Any
) -> None:
    kline_proxies: list[str] = []
    bars = [
        Bar(
            symbol="BTCUSDT",
            timeframe="1m",
            ts=1_700_000_000_000,
            o=50_000.0,
            h=50_010.0,
            l=49_990.0,
            c=50_005.0,
            v=1.0,
            q=50_005.0,
            closed=True,
        )
    ]

    async def _fetch_klines(
        symbol: str, timeframe: str, *, proxy: str = "", limit: int = 20
    ) -> list[Bar]:
        kline_proxies.append(proxy)
        if not proxy:
            raise RuntimeError("Could not contact DNS servers")
        return bars

    monkeypatch.setattr(dashboard_server, "fetch_binance_recent_klines", _fetch_klines)
    repo = SqliteRepo(tmp_db)
    cache = MemoryCache(max_bars=10)
    parquet_io = ParquetIO(data_root=tmp_path / "parquet")
    engine = PaperMatchingEngine(repo, get_price=lambda symbol: 50_000.0)
    feeder = LiveDataFeeder(
        cache,
        parquet_io,
        repo,
        engine,
        proxy="http://127.0.0.1:57777",
        symbols=["BTCUSDT"],
    )
    feeder._proxy = ""

    await feeder._backfill_recent_1m(["BTCUSDT"], publish=True)

    assert kline_proxies == ["", "http://127.0.0.1:57777"]
    assert feeder._proxy == "http://127.0.0.1:57777"
    assert cache.latest_price("BTCUSDT") == 50_005.0


async def test_live_feeder_rest_bar_fallback_publishes_new_closed_bars_once(
    tmp_path: Path, tmp_db: sqlite3.Connection, monkeypatch: Any
) -> None:
    bars = [
        Bar(
            symbol="BTCUSDT",
            timeframe="1m",
            ts=1_700_000_000_000,
            o=50_000.0,
            h=50_010.0,
            l=49_990.0,
            c=50_005.0,
            v=1.0,
            q=50_005.0,
            closed=True,
        ),
        Bar(
            symbol="BTCUSDT",
            timeframe="1m",
            ts=1_700_000_060_000,
            o=50_005.0,
            h=50_020.0,
            l=49_995.0,
            c=50_015.0,
            v=1.0,
            q=50_015.0,
            closed=True,
        ),
    ]

    async def _fetch_klines(
        symbol: str, timeframe: str, *, proxy: str = "", limit: int = 20
    ) -> list[Bar]:
        return bars

    monkeypatch.setattr(dashboard_server, "fetch_binance_recent_klines", _fetch_klines)
    repo = SqliteRepo(tmp_db)
    cache = MemoryCache(max_bars=10)
    parquet_io = ParquetIO(data_root=tmp_path / "parquet")
    engine = PaperMatchingEngine(repo, get_price=lambda symbol: 50_000.0)
    feeder = LiveDataFeeder(cache, parquet_io, repo, engine, symbols=["BTCUSDT"])
    published: list[Bar] = []
    feeder._on_bar = published.append  # type: ignore[method-assign]

    await feeder._backfill_recent_1m(["BTCUSDT"], publish=True)
    await feeder._backfill_recent_1m(["BTCUSDT"], publish=True)

    assert [bar.ts for bar in published] == [bar.ts for bar in bars]
    assert cache.latest_price("BTCUSDT") == 50_015.0


async def test_live_feeder_initial_backfill_publishes_only_latest_closed_bar(
    tmp_path: Path, tmp_db: sqlite3.Connection, monkeypatch: Any
) -> None:
    bars = [
        Bar(
            symbol="BTCUSDT",
            timeframe="1m",
            ts=1_700_000_000_000,
            o=50_000.0,
            h=50_010.0,
            l=49_990.0,
            c=50_005.0,
            v=1.0,
            q=50_005.0,
            closed=True,
        ),
        Bar(
            symbol="BTCUSDT",
            timeframe="1m",
            ts=1_700_000_060_000,
            o=50_005.0,
            h=50_020.0,
            l=49_995.0,
            c=50_015.0,
            v=1.0,
            q=50_015.0,
            closed=True,
        ),
    ]

    async def _fetch_klines(
        symbol: str, timeframe: str, *, proxy: str = "", limit: int = 20
    ) -> list[Bar]:
        return bars

    monkeypatch.setattr(dashboard_server, "fetch_binance_recent_klines", _fetch_klines)
    repo = SqliteRepo(tmp_db)
    cache = MemoryCache(max_bars=10)
    parquet_io = ParquetIO(data_root=tmp_path / "parquet")
    engine = PaperMatchingEngine(repo, get_price=lambda symbol: 50_000.0)
    feeder = LiveDataFeeder(cache, parquet_io, repo, engine, symbols=["BTCUSDT"])
    published: list[Bar] = []
    feeder._on_bar = published.append  # type: ignore[method-assign]

    await feeder._backfill_recent_1m(["BTCUSDT"], publish_latest=True)
    await feeder._backfill_recent_1m(["BTCUSDT"], publish_latest=True)

    assert [bar.ts for bar in published] == [bars[-1].ts]
    assert cache.latest_price("BTCUSDT") == 50_015.0


def test_dashboard_paper_metrics_endpoint(tmp_path: Path, tmp_db: sqlite3.Connection) -> None:
    repo = SqliteRepo(tmp_db)
    repo.upsert_symbols(
        [
            {
                "exchange": "binance",
                "symbol": "BTCUSDT",
                "type": "perp",
                "base": "BTC",
                "quote": "USDT",
                "tick_size": 0.1,
                "lot_size": 0.001,
                "min_notional": 10.0,
                "listed_at": 1,
            }
        ]
    )
    symbol = repo.get_symbol("BTCUSDT")
    assert symbol is not None
    order_id = repo.insert_order(
        {
            "client_order_id": "metrics-buy",
            "symbol_id": symbol["id"],
            "side": "buy",
            "type": "market",
            "price": 50_000.0,
            "quantity": 0.1,
            "filled_qty": 0.1,
            "avg_fill_price": 50_000.0,
            "status": "filled",
            "purpose": "entry",
            "strategy_version": "dev",
            "placed_at": 1_700_000_000_000,
            "updated_at": 1_700_000_000_000,
        }
    )
    repo.insert_fill(
        {
            "order_id": order_id,
            "exchange_fill_id": "metrics-fill",
            "price": 50_000.0,
            "quantity": 0.1,
            "fee": 2.5,
            "fee_currency": "USDT",
            "ts": 1_700_000_000_100,
        }
    )
    repo.insert_risk_event(
        {
            "type": "order_rejected",
            "severity": "warn",
            "source": "L1",
            "related_id": order_id,
            "payload": "{}",
            "captured_at": 1_700_000_000_200,
        }
    )
    app = _build_app(tmp_path, tmp_db)

    payload = _call_route(
        app,
        "/api/paper_metrics",
        since_ms=1_700_000_000_000,
        until_ms=1_700_000_001_000,
    )

    assert payload["orders"] == {"total": 1, "by_status": {"filled": 1}}
    assert payload["fills"]["net_cash_flow"] == -5_002.5
    assert payload["fills"]["cash_pnl"] == -2.5
    assert payload["risk_events"] == {"total": 1, "by_severity": {"warn": 1}}
    assert payload["symbols"] == ["BTCUSDT"]


def test_dashboard_data_health_endpoint_summarizes_run_log(
    tmp_path: Path, tmp_db: sqlite3.Connection
) -> None:
    repo = SqliteRepo(tmp_db)
    repo.log_run("binance_usdm_public_ticker", "ok", http_code=200, latency_ms=100)
    repo.log_run("binance_usdm_public_ticker", "fail", latency_ms=1300, note="timeout")
    app = _build_app(tmp_path, tmp_db)

    payload = _call_route(app, "/api/data_health", limit=10, since_ms=None)

    assert payload["status"] == "degraded"
    assert payload["by_status"] == {"fail": 1, "ok": 1}
    assert payload["endpoints"] == ["binance_usdm_public_ticker"]
    assert payload["recent_failures"][0]["note"] == "timeout"


def test_dashboard_data_health_endpoint_ignores_stale_failures_by_default(
    tmp_path: Path, tmp_db: sqlite3.Connection
) -> None:
    repo = SqliteRepo(tmp_db)
    stale_id = repo.log_run("dashboard_ws_watchdog", "fail", note="old stale stream")
    tmp_db.execute(
        "UPDATE run_log SET captured_at=? WHERE id=?",
        (int(time.time() * 1000) - 20 * 60_000, stale_id),
    )
    tmp_db.commit()
    repo.log_run("binance_usdt_top30_universe", "ok", latency_ms=50)
    app = _build_app(tmp_path, tmp_db)

    payload = _call_route(app, "/api/data_health", limit=10, since_ms=None)

    assert payload["status"] == "ok"
    assert payload["by_status"] == {"ok": 1}
    assert payload["recent_failures"] == []


def test_dashboard_data_health_endpoint_reports_live_feed_ok_without_recent_logs(
    tmp_path: Path, tmp_db: sqlite3.Connection
) -> None:
    app = _build_app(tmp_path, tmp_db)

    payload = _call_route(app, "/api/data_health", limit=10, since_ms=None)

    assert payload["status"] == "ok"
    assert payload["live_feed"] == {
        "status": "ok",
        "simulation_running": True,
        "ws_connected": True,
        "market_data_stale": False,
        "bars_received": 7,
        "last_bar_age_ms": 0,
    }


def test_dashboard_positions_use_open_positions_table(
    tmp_path: Path, tmp_db: sqlite3.Connection
) -> None:
    repo = SqliteRepo(tmp_db)
    repo.upsert_symbols(
        [
            {
                "exchange": "binance",
                "symbol": "BTCUSDT",
                "type": "perp",
                "base": "BTC",
                "quote": "USDT",
                "tick_size": 0.1,
                "lot_size": 0.001,
                "min_notional": 10.0,
                "listed_at": 1,
            }
        ]
    )
    symbol = repo.get_symbol("BTCUSDT")
    assert symbol is not None
    repo.insert_position(
        {
            "symbol_id": symbol["id"],
            "strategy": "explore_momentum",
            "strategy_version": "explore_momentum",
            "opening_signal_id": None,
            "side": "long",
            "qty": 0.1,
            "avg_entry_price": 50_000.0,
            "current_price": 50_000.0,
            "unrealized_pnl": 0.0,
            "realized_pnl": 0.0,
            "leverage": 1.0,
            "margin": None,
            "liq_price": None,
            "stop_order_id": None,
            "trade_group_id": None,
            "opened_at": 1_700_000_000_000,
            "closed_at": None,
        }
    )
    repo.insert_position(
        {
            "symbol_id": symbol["id"],
            "strategy": "explore_momentum",
            "strategy_version": "explore_momentum",
            "opening_signal_id": None,
            "side": "short",
            "qty": 0.2,
            "avg_entry_price": 51_000.0,
            "current_price": 51_000.0,
            "unrealized_pnl": 0.0,
            "realized_pnl": 12.0,
            "leverage": 1.0,
            "margin": None,
            "liq_price": None,
            "stop_order_id": None,
            "trade_group_id": None,
            "opened_at": 1_700_000_000_000,
            "closed_at": 1_700_000_060_000,
        }
    )
    cache = MemoryCache(max_bars=10)
    cache.update_latest_price("BTCUSDT", 50_500.0)
    app = _build_app(tmp_path, tmp_db)
    app.state.cache = cache

    payload = _compute_positions(repo, cache)

    assert payload == [
        {
            "symbol": "BTCUSDT",
            "side": "buy",
            "position_side": "long",
            "strategy": "explore_momentum",
            "qty": 0.1,
            "entry_price": 50_000.0,
            "current_price": 50_500.0,
            "unrealized_pnl": 50.0,
            "realized_pnl": 0.0,
        }
    ]



def test_dashboard_strategies_endpoint_reports_real_pnl(tmp_path: Path, tmp_db: sqlite3.Connection) -> None:
    repo = SqliteRepo(tmp_db)
    repo.upsert_symbols(
        [
            {
                "exchange": "binance",
                "symbol": "BTCUSDT",
                "type": "perp",
                "base": "BTC",
                "quote": "USDT",
                "tick_size": 0.1,
                "lot_size": 0.001,
                "min_notional": 10.0,
                "listed_at": 1,
            }
        ]
    )
    symbol = repo.get_symbol("BTCUSDT")
    assert symbol is not None
    order_id = repo.insert_order(
        {
            "client_order_id": "strategy-buy",
            "symbol_id": symbol["id"],
            "side": "sell",
            "type": "market",
            "price": 51_000.0,
            "quantity": 0.1,
            "filled_qty": 0.1,
            "avg_fill_price": 51_000.0,
            "status": "filled",
            "purpose": "entry",
            "strategy_version": "explore_momentum",
            "placed_at": 1_700_000_000_000,
            "updated_at": 1_700_000_000_000,
        }
    )
    repo.insert_fill(
        {
            "order_id": order_id,
            "exchange_fill_id": "strategy-fill",
            "price": 51_000.0,
            "quantity": 0.1,
            "fee": 1.0,
            "fee_currency": "USDT",
            "ts": 1_700_000_000_100,
        }
    )
    repo.insert_position(
        {
            "symbol_id": symbol["id"],
            "strategy": "explore_momentum",
            "strategy_version": "explore_momentum",
            "opening_signal_id": None,
            "side": "long",
            "qty": 0.1,
            "avg_entry_price": 50_000.0,
            "current_price": 50_500.0,
            "unrealized_pnl": 0.0,
            "realized_pnl": 7.0,
            "leverage": 1.0,
            "margin": None,
            "liq_price": None,
            "stop_order_id": None,
            "trade_group_id": None,
            "opened_at": 1_700_000_000_000,
            "closed_at": None,
        }
    )
    repo.insert_position(
        {
            "symbol_id": symbol["id"],
            "strategy": "explore_momentum",
            "strategy_version": "explore_momentum",
            "opening_signal_id": None,
            "side": "short",
            "qty": 0.0,
            "avg_entry_price": 51_000.0,
            "current_price": 50_900.0,
            "unrealized_pnl": 0.0,
            "realized_pnl": 3.0,
            "leverage": 1.0,
            "margin": None,
            "liq_price": None,
            "stop_order_id": None,
            "trade_group_id": None,
            "opened_at": 1_700_000_000_000,
            "closed_at": 1_700_000_010_000,
        }
    )
    app = _build_app(tmp_path, tmp_db)

    payload = _call_route(app, "/api/strategies")

    row = next(item for item in payload if item["name"] == "explore_momentum")
    assert row["orders_count"] == 1
    assert row["fills_count"] == 1
    assert row["gross_realized_pnl"] == 10.0
    assert row["fees_paid"] == 1.0
    assert row["realized_pnl"] == 9.0
    assert row["unrealized_pnl"] == 50.0
    assert row["total_pnl"] == 59.0
    assert row["used_margin"] == 202.0
    assert row["win_rate"] == 100.0
    assert row["roi"] > 0

def test_dashboard_prices_include_freshness_metadata(
    tmp_path: Path, tmp_db: sqlite3.Connection
) -> None:
    app = _build_app(tmp_path, tmp_db)
    app.state.cache.update_latest_price("BTCUSDT", 50_123.45, source_ts=1_700_000_000_000)

    payload = _call_route(app, "/api/prices")

    assert payload["BTCUSDT"]["price"] == 50_123.45
    assert payload["BTCUSDT"]["source_ts"] == 1_700_000_000_000
    assert isinstance(payload["BTCUSDT"]["updated_at"], int)
    assert isinstance(payload["BTCUSDT"]["age_ms"], int)


def test_dashboard_prices_prefer_24h_ticker_stats(
    tmp_path: Path, tmp_db: sqlite3.Connection
) -> None:
    app = _build_app(tmp_path, tmp_db)
    app.state.cache.update_latest_price("BTCUSDT", 50_123.45, source_ts=1_700_000_000_000)
    app.state.feeder._ticker_24h = {
        "BTCUSDT": {
            "change_24h": 2.34,
            "high_24h": 51_000.0,
            "low_24h": 49_000.0,
            "quote_volume": 123_456_789.0,
        }
    }

    payload = _call_route(app, "/api/prices")

    assert payload["BTCUSDT"]["change_24h"] == 2.34
    assert payload["BTCUSDT"]["high_24h"] == 51_000.0
    assert payload["BTCUSDT"]["low_24h"] == 49_000.0
    assert payload["BTCUSDT"]["quote_volume"] == 123_456_789.0


def test_dashboard_price_history_backfills_parquet_when_cache_is_short(
    tmp_path: Path, tmp_db: sqlite3.Connection
) -> None:
    app = _build_app(tmp_path, tmp_db)
    base_ts = 1_700_000_000_000
    historical = [
        Bar(
            symbol="BTCUSDT",
            timeframe="1m",
            ts=base_ts + index * 60_000,
            o=100 + index,
            h=101 + index,
            l=99 + index,
            c=100.5 + index,
            v=10 + index,
        )
        for index in range(10)
    ]
    app.state.parquet_io.write_bars(historical)
    for index in range(8, 12):
        app.state.cache.push_bar(
            Bar(
                symbol="BTCUSDT",
                timeframe="1m",
                ts=base_ts + index * 60_000,
                o=200 + index,
                h=201 + index,
                l=199 + index,
                c=200.5 + index,
                v=20 + index,
                closed=index < 11,
            )
        )

    payload = _call_route(app, "/api/price_history", symbol="BTCUSDT", tf="1m", n=10)

    assert len(payload) == 10
    assert [row["ts"] for row in payload] == [base_ts + index * 60_000 for index in range(2, 12)]
    assert payload[-1]["c"] == 211.5
    assert payload[-4]["c"] == 208.5


def test_apply_rest_price_rows_updates_latest_price_metadata() -> None:
    cache = MemoryCache(max_bars=10)

    _apply_rest_price_rows(
        cache,
        [{"symbol": "BTCUSDT", "price": "50100.25"}, {"symbol": "ETHUSDT", "price": "2500.5"}],
        captured_at_ms=1_700_000_123_000,
    )

    assert cache.latest_price("BTCUSDT") == 50_100.25
    assert cache.latest_price("ETHUSDT") == 2_500.5
    assert cache.latest_price_meta("BTCUSDT") == {
        "source_ts": 1_700_000_123_000,
        "updated_at": 1_700_000_123_000,
    }


def test_apply_rest_price_rows_can_filter_to_dashboard_universe() -> None:
    cache = MemoryCache(max_bars=10)

    _apply_rest_price_rows(
        cache,
        [{"symbol": "BTCUSDT", "price": "50100.25"}, {"symbol": "NOTUSDT", "price": "1.0"}],
        captured_at_ms=1_700_000_123_000,
        allowed_symbols={"BTCUSDT"},
    )

    assert cache.latest_prices_all() == {"BTCUSDT": 50_100.25}


def test_apply_ticker_24h_rows_normalizes_binance_payload() -> None:
    rows = [
        {
            "symbol": "BTCUSDT",
            "priceChangePercent": "2.345",
            "highPrice": "51000.5",
            "lowPrice": "49000.25",
            "quoteVolume": "123456789.12",
        }
    ]

    payload = _apply_ticker_24h_rows(rows)

    assert payload == {
        "BTCUSDT": {
            "change_24h": 2.35,
            "high_24h": 51_000.5,
            "low_24h": 49_000.25,
            "quote_volume": 123_456_789.12,
        }
    }
