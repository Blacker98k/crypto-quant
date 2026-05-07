from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from fastapi import FastAPI

from core.data.memory_cache import MemoryCache
from core.data.parquet_io import ParquetIO
from core.data.sqlite_repo import SqliteRepo
from core.execution.paper_engine import PaperMatchingEngine
from dashboard.server import create_app


class _DummyWs:
    _running = True


class _DummyFeeder:
    _bar_counter = 7
    _ws = _DummyWs()


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
