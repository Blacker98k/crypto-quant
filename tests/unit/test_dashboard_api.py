from __future__ import annotations

import sqlite3
import time
from pathlib import Path
from typing import Any, ClassVar

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
    assert rows[1]["payload"] == {"reason": "min_notional"}


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

    assert payload["initial_balance"] == 10_000.0
    assert payload["portfolio_value"] == 10_050.0
    assert payload["account_equity"] == 10_050.0
    assert payload["used_margin"] == 202.0
    assert payload["usdt_balance"] == 9_848.0
    assert payload["available_balance"] == 9_848.0
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
    assert payload["portfolio_value"] == 10_058.0
    assert payload["open_notional"] == 5_050.0
    assert payload["used_margin"] == 202.0
    assert payload["available_balance"] == 9_856.0


def test_dashboard_status_marks_stale_feeder_unhealthy(
    tmp_path: Path, tmp_db: sqlite3.Connection
) -> None:
    app = _build_app(tmp_path, tmp_db)
    app.state.feeder._last_bar_ms = 1
    app.state.feeder._bar_stale_after_ms = 1

    payload = _call_route(app, "/api/status")

    assert payload["ws_connected"] is False
    assert payload["simulation_running"] is False
    assert payload["market_data_stale"] is True


async def test_dashboard_control_endpoint_starts_and_stops_feeder(
    tmp_path: Path, tmp_db: sqlite3.Connection
) -> None:
    app = _build_app(tmp_path, tmp_db)

    stopped = await _call_route(app, "/api/control", payload={"action": "stop"})
    started = await _call_route(app, "/api/control", payload={"action": "start"})

    assert stopped["simulation_running"] is False
    assert started["simulation_running"] is True


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
    assert kline_proxies == [""]
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
