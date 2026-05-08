from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from fastapi import FastAPI

from core.data.exchange.base import Bar
from core.data.memory_cache import MemoryCache
from core.data.parquet_io import ParquetIO
from core.data.sqlite_repo import SqliteRepo
from core.execution.paper_engine import PaperMatchingEngine
from dashboard.server import _apply_rest_price_rows, create_app


class _DummyWs:
    _running = True


class _DummyFeeder:
    _bar_counter = 7
    _ws = _DummyWs()
    started = 0
    stopped = 0

    async def start(self) -> None:
        self.started += 1
        self._ws._running = True

    async def stop(self) -> None:
        self.stopped += 1
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


async def test_dashboard_control_endpoint_starts_and_stops_feeder(
    tmp_path: Path, tmp_db: sqlite3.Connection
) -> None:
    app = _build_app(tmp_path, tmp_db)

    stopped = await _call_route(app, "/api/control", payload={"action": "stop"})
    started = await _call_route(app, "/api/control", payload={"action": "start"})

    assert stopped["simulation_running"] is False
    assert started["simulation_running"] is True


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
    assert payload["fills"]["cash_pnl"] == -5_002.5
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
