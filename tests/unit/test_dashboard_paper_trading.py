from __future__ import annotations

import sqlite3
from pathlib import Path

from fastapi import FastAPI

from core.data.exchange.base import Bar
from core.data.memory_cache import MemoryCache
from core.data.parquet_io import ParquetIO
from core.data.sqlite_repo import SqliteRepo
from core.execution.paper_engine import PaperMatchingEngine
from dashboard.paper_trading import (
    DashboardPaperTrader,
    ExplorationStrategy,
    bars_from_binance_klines,
    select_top_usdt_symbols,
    upsert_dashboard_universe,
)
from dashboard.server import create_app


class _StoppedFeeder:
    _bar_counter = 0
    _running = False
    _ws = None

    async def start(self) -> None:
        self._running = True

    async def stop(self) -> None:
        self._running = False


def _seed_symbol(repo: SqliteRepo, symbol: str) -> None:
    repo.upsert_symbols(
        [
            {
                "exchange": "binance",
                "symbol": symbol,
                "type": "perp",
                "base": symbol.replace("USDT", ""),
                "quote": "USDT",
                "universe": "top30",
                "tick_size": 0.01,
                "lot_size": 0.0001,
                "min_notional": 5.0,
                "listed_at": 1,
            }
        ]
    )


def _build_dashboard_app(
    tmp_path: Path,
    conn: sqlite3.Connection,
    cache: MemoryCache,
    trader: DashboardPaperTrader,
) -> FastAPI:
    repo = SqliteRepo(conn)
    parquet_io = ParquetIO(data_root=tmp_path / "parquet")
    engine = PaperMatchingEngine(repo, get_price=lambda symbol: cache.latest_price(symbol))
    static_dir = tmp_path / "static"
    static_dir.mkdir()
    (static_dir / "index.html").write_text("<html></html>", encoding="utf-8")
    return create_app(cache, repo, parquet_io, engine, _StoppedFeeder(), static_dir, trader=trader)


def _call_route(app: FastAPI, path: str, **kwargs):
    for route in app.routes:
        if getattr(route, "path", None) == path:
            return route.endpoint(**kwargs)
    raise AssertionError(f"route not found: {path}")


def test_select_top_usdt_symbols_filters_to_mainstream_quote_volume() -> None:
    rows = [
        {"symbol": "BTCUSDT", "quoteVolume": "9000000000"},
        {"symbol": "ETHUSDT", "quoteVolume": "8000000000"},
        {"symbol": "XAUUSDT", "quoteVolume": "7500000000"},
        {"symbol": "USDCUSDT", "quoteVolume": "7000000000"},
        {"symbol": "BNBUSDT", "quoteVolume": "6000000000"},
        {"symbol": "DOGEUPUSDT", "quoteVolume": "5000000000"},
        {"symbol": "SOLBTC", "quoteVolume": "4000000000"},
        {"symbol": "XRPUSDT", "quoteVolume": "3000000000"},
    ]

    symbols = select_top_usdt_symbols(rows, limit=3)

    assert symbols == ["BTCUSDT", "ETHUSDT", "BNBUSDT"]


def test_upsert_dashboard_universe_marks_top30_symbols(sqlite_repo: SqliteRepo) -> None:
    count = upsert_dashboard_universe(sqlite_repo, ["BTCUSDT", "ETHUSDT", "SOLUSDT"])

    rows = sqlite_repo.list_symbols(exchange="binance", stype="perp", universe="top30")

    assert count == 3
    assert [row["symbol"] for row in rows] == ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    assert {row["quote"] for row in rows} == {"USDT"}


def test_upsert_dashboard_universe_replaces_stale_top30_marks(sqlite_repo: SqliteRepo) -> None:
    upsert_dashboard_universe(sqlite_repo, ["BTCUSDT", "ETHUSDT", "SOLUSDT"])
    count = upsert_dashboard_universe(sqlite_repo, ["BTCUSDT", "ETHUSDT"])

    rows = sqlite_repo.list_symbols(exchange="binance", stype="perp", universe="top30")

    assert count == 2
    assert [row["symbol"] for row in rows] == ["BTCUSDT", "ETHUSDT"]


def test_bars_from_binance_klines_normalizes_closed_1m_rows() -> None:
    rows = [
        [
            1_700_000_000_000,
            "100.0",
            "102.0",
            "99.0",
            "101.0",
            "12.5",
            1_700_000_059_999,
            "1262.5",
        ]
    ]

    bars = bars_from_binance_klines("BTCUSDT", "1m", rows)

    assert bars == [
        Bar(
            symbol="BTCUSDT",
            timeframe="1m",
            ts=1_700_000_000_000,
            o=100.0,
            h=102.0,
            l=99.0,
            c=101.0,
            v=12.5,
            q=1262.5,
            closed=True,
        )
    ]


def test_dashboard_paper_trader_places_parallel_strategy_fills(
    sqlite_repo: SqliteRepo,
) -> None:
    _seed_symbol(sqlite_repo, "BTCUSDT")
    cache = MemoryCache(max_bars=20)
    engine = PaperMatchingEngine(sqlite_repo, get_price=lambda symbol: cache.latest_price(symbol))
    trader = DashboardPaperTrader(
        repo=sqlite_repo,
        cache=cache,
        engine=engine,
        symbols=["BTCUSDT"],
        strategies=[
            ExplorationStrategy("explore_momentum", min_bars=2),
            ExplorationStrategy("explore_mean_reversion", min_bars=2),
            ExplorationStrategy("explore_volatility", min_bars=2),
        ],
        notional_usdt=25.0,
        cooldown_ms=0,
    )
    first = Bar("BTCUSDT", "1m", 1_700_000_000_000, 100, 101, 99, 100, 10, closed=True)
    second = Bar("BTCUSDT", "1m", 1_700_000_060_000, 100, 103, 99, 102, 12, closed=True)

    trader.on_bar(first, now_ms=first.ts)
    handles = trader.on_bar(second, now_ms=second.ts)

    signal_rows = sqlite_repo._conn.execute("SELECT * FROM signals").fetchall()
    order_rows = sqlite_repo._conn.execute("SELECT * FROM orders").fetchall()
    fill_rows = sqlite_repo._conn.execute("SELECT * FROM fills").fetchall()
    position_rows = sqlite_repo._conn.execute("SELECT * FROM positions WHERE closed_at IS NULL").fetchall()

    assert [handle.status for handle in handles] == ["filled", "filled", "filled"]
    assert len(signal_rows) == 3
    assert len(order_rows) == 3
    assert len(fill_rows) == 3
    assert len(position_rows) == 3
    assert {row["strategy_version"] for row in order_rows} == {
        "explore_momentum",
        "explore_mean_reversion",
        "explore_volatility",
    }


def test_dashboard_trading_endpoints_expose_universe_matrix_and_recent_trades(
    tmp_path: Path,
    tmp_db: sqlite3.Connection,
) -> None:
    repo = SqliteRepo(tmp_db)
    _seed_symbol(repo, "BTCUSDT")
    cache = MemoryCache(max_bars=20)
    engine = PaperMatchingEngine(repo, get_price=lambda symbol: cache.latest_price(symbol))
    trader = DashboardPaperTrader(
        repo=repo,
        cache=cache,
        engine=engine,
        symbols=["BTCUSDT"],
        strategies=[ExplorationStrategy("explore_momentum", min_bars=1)],
        notional_usdt=25.0,
        cooldown_ms=0,
    )
    bar = Bar("BTCUSDT", "1m", 1_700_000_000_000, 100, 101, 99, 100, 10, closed=True)
    trader.on_bar(bar, now_ms=bar.ts)
    app = _build_dashboard_app(tmp_path, tmp_db, cache, trader)

    universe = _call_route(app, "/api/universe")
    matrix = _call_route(app, "/api/strategy_matrix")
    recent_trades = _call_route(app, "/api/recent_trades", limit=10, symbol=None, strategy_id=None)

    assert universe["count"] == 1
    assert universe["symbols"][0]["symbol"] == "BTCUSDT"
    assert matrix["symbols"] == ["BTCUSDT"]
    assert matrix["strategies"] == ["explore_momentum"]
    assert matrix["cells"][0]["orders"] == 1
    assert recent_trades[0]["symbol"] == "BTCUSDT"
    assert recent_trades[0]["strategy"] == "explore_momentum"
