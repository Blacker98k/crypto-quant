from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi import FastAPI

from core.data.exchange.base import Bar
from core.data.feed import LiveFeed
from core.data.memory_cache import MemoryCache
from core.data.parquet_io import ParquetIO
from core.data.sqlite_repo import SqliteRepo
from core.execution.paper_engine import PaperMatchingEngine
from core.strategy.base import DataRequirement, Signal, Strategy
from dashboard.paper_trading import (
    CoreStrategyAdapter,
    DashboardPaperTrader,
    ExplorationStrategy,
    bars_from_binance_klines,
    default_dashboard_strategies,
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


class _SignalOnEveryBarStrategy(Strategy):
    name = "S_core_test"
    version = "dev"
    config_hash = "testhash"
    __slots__ = ()

    def required_data(self) -> DataRequirement:
        return DataRequirement(symbols=["BTCUSDT"], timeframes=["1m"], history_lookback_bars=2)

    def on_bar(self, bar: Bar, ctx) -> list[Signal]:
        return [
            Signal(
                side="long",
                symbol=bar.symbol,
                entry_price=None,
                stop_price=bar.c * 0.99,
                confidence=0.7,
                suggested_size=0.25,
            )
        ]


class _SignalOnFourHourStrategy(_SignalOnEveryBarStrategy):
    name = "S_core_4h_test"
    __slots__ = ()

    def required_data(self) -> DataRequirement:
        return DataRequirement(symbols=["BTCUSDT"], timeframes=["4h"], history_lookback_bars=2)


def _seed_symbol(repo: SqliteRepo, symbol: str) -> None:
    _seed_symbol_with_limits(repo, symbol=symbol)


def _seed_symbol_with_limits(
    repo: SqliteRepo,
    *,
    symbol: str,
    tick_size: float = 0.01,
    lot_size: float = 0.0001,
    min_notional: float = 5.0,
) -> None:
    repo.upsert_symbols(
        [
            {
                "exchange": "binance",
                "symbol": symbol,
                "type": "perp",
                "base": symbol.replace("USDT", ""),
                "quote": "USDT",
                "universe": "top30",
                "tick_size": tick_size,
                "lot_size": lot_size,
                "min_notional": min_notional,
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


def test_select_top_usdt_symbols_uses_pol_instead_of_delisted_matic() -> None:
    rows = [
        {"symbol": "MATICUSDT", "quoteVolume": "9000000000"},
        {"symbol": "POLUSDT", "quoteVolume": "8000000000"},
        {"symbol": "BTCUSDT", "quoteVolume": "7000000000"},
    ]

    symbols = select_top_usdt_symbols(rows, limit=3)

    assert symbols == ["POLUSDT", "BTCUSDT"]


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


def test_dashboard_paper_trader_scales_notional_by_strategy(
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
        ],
        notional_usdt=25.0,
        strategy_notional_multipliers={"explore_mean_reversion": 0.5},
        cooldown_ms=0,
    )
    first = Bar("BTCUSDT", "1m", 1_700_000_000_000, 100, 101, 99, 100, 10, closed=True)
    second = Bar("BTCUSDT", "1m", 1_700_000_060_000, 100, 103, 99, 102, 12, closed=True)

    trader.on_bar(first, now_ms=first.ts)
    trader.on_bar(second, now_ms=second.ts)

    rows = sqlite_repo._conn.execute(
        "SELECT o.strategy_version, f.price * f.quantity AS notional "
        "FROM fills f JOIN orders o ON f.order_id = o.id"
    ).fetchall()
    notional_by_strategy = {row["strategy_version"]: row["notional"] for row in rows}

    assert notional_by_strategy["explore_mean_reversion"] < notional_by_strategy["explore_momentum"]
    assert notional_by_strategy["explore_mean_reversion"] == pytest.approx(
        notional_by_strategy["explore_momentum"] * 0.5,
        rel=0.02,
    )


def test_dashboard_paper_trader_rejects_orders_through_l1_risk(
    sqlite_repo: SqliteRepo,
) -> None:
    _seed_symbol_with_limits(sqlite_repo, symbol="BTCUSDT", min_notional=50.0)
    cache = MemoryCache(max_bars=20)
    engine = PaperMatchingEngine(sqlite_repo, get_price=lambda symbol: cache.latest_price(symbol))
    trader = DashboardPaperTrader(
        repo=sqlite_repo,
        cache=cache,
        engine=engine,
        symbols=["BTCUSDT"],
        strategies=[ExplorationStrategy("explore_momentum", min_bars=1)],
        notional_usdt=25.0,
        cooldown_ms=0,
    )
    bar = Bar("BTCUSDT", "1m", 1_700_000_000_000, 100, 101, 99, 100, 10, closed=True)

    handles = trader.on_bar(bar, now_ms=bar.ts)

    order_count = sqlite_repo._conn.execute("SELECT COUNT(*) AS n FROM orders").fetchone()
    risk_row = sqlite_repo._conn.execute("SELECT * FROM risk_events").fetchone()
    assert handles == []
    assert order_count["n"] == 0
    assert risk_row["source"] == "L1"
    assert "min_notional" in risk_row["payload"]


def test_dashboard_paper_trader_runs_core_strategy_adapter(
    tmp_path: Path,
    sqlite_repo: SqliteRepo,
) -> None:
    _seed_symbol(sqlite_repo, "BTCUSDT")
    cache = MemoryCache(max_bars=20)
    parquet_io = ParquetIO(data_root=tmp_path / "parquet")
    feed = LiveFeed(parquet_io, sqlite_repo, cache)
    engine = PaperMatchingEngine(sqlite_repo, get_price=lambda symbol: cache.latest_price(symbol))
    trader = DashboardPaperTrader(
        repo=sqlite_repo,
        cache=cache,
        engine=engine,
        symbols=["BTCUSDT"],
        strategies=[CoreStrategyAdapter(_SignalOnEveryBarStrategy(), feed=feed, repo=sqlite_repo)],
        notional_usdt=25.0,
        cooldown_ms=0,
    )
    bar = Bar("BTCUSDT", "1m", 1_700_000_000_000, 100, 101, 99, 100, 10, closed=True)

    handles = trader.on_bar(bar, now_ms=bar.ts)

    order_rows = sqlite_repo._conn.execute("SELECT * FROM orders").fetchall()
    assert [handle.status for handle in handles] == ["filled"]
    assert [row["strategy_version"] for row in order_rows] == ["S_core_test"]


def test_default_dashboard_strategies_use_core_strategies_by_default(
    tmp_path: Path,
    sqlite_repo: SqliteRepo,
) -> None:
    cache = MemoryCache(max_bars=20)
    feed = LiveFeed(ParquetIO(data_root=tmp_path / "parquet"), sqlite_repo, cache)

    names = [strategy.name for strategy in default_dashboard_strategies(feed=feed, repo=sqlite_repo)]

    assert "S1_btc_eth_trend" in names
    assert "S2_altcoin_reversal" in names
    assert "explore_momentum" not in names
    assert "S3_pair_trading" not in names


def test_default_dashboard_s2_follows_top30_symbols(
    tmp_path: Path,
    sqlite_repo: SqliteRepo,
) -> None:
    cache = MemoryCache(max_bars=20)
    feed = LiveFeed(ParquetIO(data_root=tmp_path / "parquet"), sqlite_repo, cache)
    strategies = default_dashboard_strategies(feed=feed, repo=sqlite_repo)
    s2 = next(strategy for strategy in strategies if strategy.name == "S2_altcoin_reversal")

    assert "SOLUSDT" in s2.requirement.symbols
    assert s2.supports("SOLUSDT", "1h")

    s2.replace_symbols(["BTCUSDT", "ETHUSDT", "SUIUSDT"])

    assert s2.requirement.symbols == ["BTCUSDT", "ETHUSDT", "SUIUSDT"]
    assert s2.supports("SUIUSDT", "1h")
    assert not s2.supports("SOLUSDT", "1h")


def test_dashboard_paper_trader_routes_required_timeframe_to_core_strategy(
    tmp_path: Path,
    sqlite_repo: SqliteRepo,
) -> None:
    _seed_symbol(sqlite_repo, "BTCUSDT")
    cache = MemoryCache(max_bars=20)
    feed = LiveFeed(ParquetIO(data_root=tmp_path / "parquet"), sqlite_repo, cache)
    engine = PaperMatchingEngine(sqlite_repo, get_price=lambda symbol: cache.latest_price(symbol))
    trader = DashboardPaperTrader(
        repo=sqlite_repo,
        cache=cache,
        engine=engine,
        symbols=["BTCUSDT"],
        strategies=[CoreStrategyAdapter(_SignalOnFourHourStrategy(), feed=feed, repo=sqlite_repo)],
        notional_usdt=25.0,
        cooldown_ms=0,
    )
    bar = Bar("BTCUSDT", "4h", 1_700_000_000_000, 100, 101, 99, 100, 10, closed=True)

    handles = trader.on_bar(bar, now_ms=bar.ts)

    assert [handle.status for handle in handles] == ["filled"]


def test_dashboard_paper_trader_ignores_unsupported_core_timeframes(
    tmp_path: Path,
    sqlite_repo: SqliteRepo,
) -> None:
    _seed_symbol(sqlite_repo, "BTCUSDT")
    cache = MemoryCache(max_bars=20)
    feed = LiveFeed(ParquetIO(data_root=tmp_path / "parquet"), sqlite_repo, cache)
    engine = PaperMatchingEngine(sqlite_repo, get_price=lambda symbol: cache.latest_price(symbol))
    trader = DashboardPaperTrader(
        repo=sqlite_repo,
        cache=cache,
        engine=engine,
        symbols=["BTCUSDT"],
        strategies=[CoreStrategyAdapter(_SignalOnFourHourStrategy(), feed=feed, repo=sqlite_repo)],
        notional_usdt=25.0,
        cooldown_ms=0,
    )
    one_minute = Bar("BTCUSDT", "1m", 1_700_000_000_000, 100, 101, 99, 100, 10, closed=True)

    handles = trader.on_bar(one_minute, now_ms=one_minute.ts)
    matrix = trader.strategy_matrix()

    assert handles == []
    assert matrix["cells"][0]["last_eval_at"] is None
    assert matrix["cells"][0]["bars"] == 0


def test_dashboard_paper_trader_does_not_record_cooldown_as_risk_event(
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
        strategies=[ExplorationStrategy("explore_momentum", min_bars=1)],
        notional_usdt=25.0,
        cooldown_ms=120_000,
    )
    first = Bar("BTCUSDT", "1m", 1_700_000_000_000, 100, 101, 99, 100, 10, closed=True)
    second = Bar("BTCUSDT", "1m", 1_700_000_030_000, 100, 103, 99, 102, 12, closed=True)

    trader.on_bar(first, now_ms=first.ts)
    handles = trader.on_bar(second, now_ms=second.ts)

    risk_count = sqlite_repo._conn.execute("SELECT COUNT(*) AS n FROM risk_events").fetchone()
    assert handles == []
    assert risk_count["n"] == 0


def test_dashboard_paper_trader_order_cap_uses_rolling_window(
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
        strategies=[ExplorationStrategy("explore_momentum", min_bars=1)],
        notional_usdt=25.0,
        cooldown_ms=0,
        max_orders_per_symbol=1,
        order_cap_window_ms=60_000,
    )
    first = Bar("BTCUSDT", "1m", 1_700_000_000_000, 100, 101, 99, 100, 10, closed=True)
    second = Bar("BTCUSDT", "1m", 1_700_000_120_000, 100, 103, 99, 102, 12, closed=True)

    first_handles = trader.on_bar(first, now_ms=first.ts)
    second_handles = trader.on_bar(second, now_ms=second.ts)

    order_count = sqlite_repo._conn.execute("SELECT COUNT(*) AS n FROM orders").fetchone()
    risk_count = sqlite_repo._conn.execute("SELECT COUNT(*) AS n FROM risk_events").fetchone()
    assert [handle.status for handle in first_handles] == ["filled"]
    assert [handle.status for handle in second_handles] == ["filled"]
    assert order_count["n"] == 2
    assert risk_count["n"] == 0


def test_dashboard_paper_trader_order_cap_throttles_without_risk_event(
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
        strategies=[ExplorationStrategy("explore_momentum", min_bars=1)],
        notional_usdt=25.0,
        cooldown_ms=0,
        max_orders_per_symbol=1,
        order_cap_window_ms=60_000,
    )
    first = Bar("BTCUSDT", "1m", 1_700_000_000_000, 100, 101, 99, 100, 10, closed=True)
    second = Bar("BTCUSDT", "1m", 1_700_000_030_000, 100, 103, 99, 102, 12, closed=True)

    first_handles = trader.on_bar(first, now_ms=first.ts)
    second_handles = trader.on_bar(second, now_ms=second.ts)
    matrix = trader.strategy_matrix()

    order_count = sqlite_repo._conn.execute("SELECT COUNT(*) AS n FROM orders").fetchone()
    risk_count = sqlite_repo._conn.execute("SELECT COUNT(*) AS n FROM risk_events").fetchone()
    assert [handle.status for handle in first_handles] == ["filled"]
    assert second_handles == []
    assert order_count["n"] == 1
    assert risk_count["n"] == 0
    assert matrix["cells"][0]["throttled"] is True
    assert matrix["cells"][0]["throttle_reason"] == "symbol_order_cap"


def test_dashboard_paper_trader_notional_cap_blocks_adds_but_allows_reductions(
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
        strategies=[ExplorationStrategy("explore_momentum", min_bars=1)],
        notional_usdt=25.0,
        cooldown_ms=0,
        max_open_notional_usdt=25.0,
    )
    first = Bar("BTCUSDT", "1m", 1_700_000_000_000, 100, 101, 99, 100, 10, closed=True)
    second = Bar("BTCUSDT", "1m", 1_700_000_060_000, 100, 103, 99, 102, 12, closed=True)
    third = Bar("BTCUSDT", "1m", 1_700_000_120_000, 102, 103, 98, 99, 12, closed=True)

    first_handles = trader.on_bar(first, now_ms=first.ts)
    second_handles = trader.on_bar(second, now_ms=second.ts)
    capped_matrix = trader.strategy_matrix()
    third_handles = trader.on_bar(third, now_ms=third.ts)

    order_count = sqlite_repo._conn.execute("SELECT COUNT(*) AS n FROM orders").fetchone()
    risk_count = sqlite_repo._conn.execute("SELECT COUNT(*) AS n FROM risk_events").fetchone()
    assert [handle.status for handle in first_handles] == ["filled"]
    assert second_handles == []
    assert [handle.status for handle in third_handles] == ["filled"]
    assert order_count["n"] == 2
    assert risk_count["n"] == 0
    assert capped_matrix["cells"][0]["throttled"] is True
    assert capped_matrix["cells"][0]["throttle_reason"] == "portfolio_notional_cap"


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
