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
    SwingBreakoutStrategy,
    TrendMomentumStrategy,
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


class _SignalWithDailyDependencyStrategy(_SignalOnEveryBarStrategy):
    name = "S_core_trigger_test"
    __slots__ = ()

    def required_data(self) -> DataRequirement:
        return DataRequirement(symbols=["BTCUSDT"], timeframes=["4h", "1d"], history_lookback_bars=2)


class _ScriptedDashboardStrategy:
    def __init__(self, name: str, signals: list[Signal | None]) -> None:
        self.name = name
        self.min_bars = 1
        self._signals = list(signals)

    def supports(self, symbol: str, timeframe: str) -> bool:
        return timeframe == "1m"

    def evaluate(self, symbol: str, bars: list[Bar]) -> Signal | None:
        if not self._signals:
            return None
        return self._signals.pop(0)


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


def test_exploration_momentum_requires_move_above_cost_buffer() -> None:
    strategy = ExplorationStrategy("paper_momentum", min_bars=2)
    tiny_move = [
        Bar("BTCUSDT", "1m", 1_700_000_000_000, 100, 100.05, 99.95, 100, 10, closed=True),
        Bar("BTCUSDT", "1m", 1_700_000_060_000, 100, 100.06, 99.96, 100.05, 10, closed=True),
    ]
    tradable_move = [
        Bar("BTCUSDT", "1m", 1_700_000_000_000, 100, 100.05, 99.95, 100, 10, closed=True),
        Bar("BTCUSDT", "1m", 1_700_000_060_000, 100, 100.65, 99.96, 100.5, 10, closed=True),
    ]

    assert strategy.evaluate("BTCUSDT", tiny_move) is None
    assert strategy.evaluate("BTCUSDT", tradable_move) is not None


def test_exploration_momentum_samples_quiet_but_tradeable_move() -> None:
    strategy = ExplorationStrategy("paper_momentum", min_bars=2)
    quiet_tradeable_move = [
        Bar("BTCUSDT", "1m", 1_700_000_000_000, 100, 100.05, 99.95, 100, 10, closed=True),
        Bar("BTCUSDT", "1m", 1_700_000_060_000, 100, 100.26, 99.96, 100.22, 10, closed=True),
    ]

    assert strategy.evaluate("BTCUSDT", quiet_tradeable_move) is not None


def test_exploration_mean_reversion_requires_sma_deviation() -> None:
    strategy = ExplorationStrategy("paper_mean_reversion", min_bars=3)
    near_sma = [
        Bar("BTCUSDT", "1m", 1_700_000_000_000, 100, 100.1, 99.9, 100, 10, closed=True),
        Bar("BTCUSDT", "1m", 1_700_000_060_000, 100, 100.1, 99.9, 100, 10, closed=True),
        Bar("BTCUSDT", "1m", 1_700_000_120_000, 100, 100.1, 99.9, 99.98, 10, closed=True),
    ]
    far_from_sma = [
        Bar("BTCUSDT", "1m", 1_700_000_000_000, 100, 100.1, 99.9, 100, 10, closed=True),
        Bar("BTCUSDT", "1m", 1_700_000_060_000, 100, 100.1, 99.9, 100, 10, closed=True),
        Bar("BTCUSDT", "1m", 1_700_000_120_000, 100, 100.1, 99.3, 99.4, 10, closed=True),
    ]

    assert strategy.evaluate("BTCUSDT", near_sma) is None
    assert strategy.evaluate("BTCUSDT", far_from_sma) is not None


def test_exploration_mean_reversion_adapts_to_quiet_tradeable_market() -> None:
    strategy = ExplorationStrategy("paper_mean_reversion", min_bars=3)
    bars = [
        Bar(
            "BTCUSDT",
            "1m",
            1_700_000_000_000 + index * 60_000,
            100.0,
            100.03,
            99.97,
            100.0,
            10,
            closed=True,
        )
        for index in range(29)
    ]
    bars.extend(
        [
            Bar("BTCUSDT", "1m", 1_700_001_740_000, 100, 100.03, 99.97, 100.0, 10, closed=True),
            Bar("BTCUSDT", "1m", 1_700_001_800_000, 100, 100.02, 99.84, 99.88, 10, closed=True),
        ]
    )

    signal = strategy.evaluate("BTCUSDT", bars)

    assert signal is not None
    assert signal.side == "long"


def test_exploration_mean_reversion_blocks_countertrend_entries() -> None:
    strategy = ExplorationStrategy("paper_mean_reversion", min_bars=3)
    bars = [
        Bar(
            "BTCUSDT",
            "1m",
            1_700_000_000_000 + index * 60_000,
            100.0 - index * 0.08,
            100.08 - index * 0.08,
            99.92 - index * 0.08,
            100.0 - index * 0.08,
            10,
            closed=True,
        )
        for index in range(31)
    ]
    bars[-1] = Bar("BTCUSDT", "1m", bars[-1].ts, 97.4, 97.45, 97.2, 97.3, 10, closed=True)

    assert strategy.evaluate("BTCUSDT", bars) is None


def test_exploration_volatility_requires_decisive_range() -> None:
    strategy = ExplorationStrategy("paper_volatility", min_bars=2)
    narrow_bar = [
        Bar("BTCUSDT", "1m", 1_700_000_000_000, 100, 100.1, 99.9, 100, 10, closed=True),
        Bar("BTCUSDT", "1m", 1_700_000_060_000, 100, 100.08, 99.95, 100.02, 10, closed=True),
    ]
    decisive_bar = [
        Bar("BTCUSDT", "1m", 1_700_000_000_000, 100, 100.1, 99.9, 100, 10, closed=True),
        Bar("BTCUSDT", "1m", 1_700_000_060_000, 100, 100.95, 99.9, 100.9, 10, closed=True),
    ]

    assert strategy.evaluate("BTCUSDT", narrow_bar) is None
    assert strategy.evaluate("BTCUSDT", decisive_bar) is not None


def test_exploration_strategy_uses_wider_profit_target_and_longer_ttl() -> None:
    strategy = ExplorationStrategy("paper_mean_reversion", min_bars=3)
    bars = [
        Bar("BTCUSDT", "1m", 1_700_000_000_000, 100, 100.1, 99.9, 100, 10, closed=True),
        Bar("BTCUSDT", "1m", 1_700_000_060_000, 100, 100.1, 99.9, 100, 10, closed=True),
        Bar("BTCUSDT", "1m", 1_700_000_120_000, 100, 100.1, 99.3, 99.4, 10, closed=True),
    ]

    signal = strategy.evaluate("BTCUSDT", bars)

    assert signal is not None
    assert signal.expires_in_ms >= 5 * 60_000
    assert signal.target_price is not None
    reward = abs(signal.target_price - bars[-1].c)
    risk = abs(bars[-1].c - float(signal.stop_price))
    assert reward / risk >= 2.0


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
    second = Bar("BTCUSDT", "1m", 1_700_000_060_000, 100, 103, 99, 102.5, 12, closed=True)

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


def test_exploration_strategy_does_not_trade_on_higher_timeframe_warmup_bars(
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
        strategies=[ExplorationStrategy("paper_mean_reversion", min_bars=2)],
        notional_usdt=25.0,
        cooldown_ms=0,
    )
    first = Bar("BTCUSDT", "1m", 1_700_000_000_000, 100, 101, 99, 100, 10, closed=True)
    warmup_first = Bar("BTCUSDT", "1h", 1_700_000_060_000, 100, 101, 99, 100, 12, closed=True)
    warmup_second = Bar("BTCUSDT", "1h", 1_700_003_660_000, 100, 150, 99, 150, 12, closed=True)

    trader.on_bar(first, now_ms=first.ts)
    trader.on_bar(warmup_first, now_ms=warmup_first.ts)
    handles = trader.on_bar(warmup_second, now_ms=warmup_second.ts)

    assert handles == []
    assert sqlite_repo._conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0] == 0


def test_dashboard_paper_trader_closes_position_when_target_is_hit(
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
        strategies=[ExplorationStrategy("paper_momentum", min_bars=2)],
        notional_usdt=25.0,
        cooldown_ms=0,
    )
    first = Bar("BTCUSDT", "1m", 1_700_000_000_000, 100, 101, 99, 100, 10, closed=True)
    entry = Bar("BTCUSDT", "1m", 1_700_000_060_000, 100, 103, 99, 102, 12, closed=True)
    target_hit = Bar("BTCUSDT", "1m", 1_700_000_121_000, 102, 109, 101.5, 106, 12, closed=True)

    trader.on_bar(first, now_ms=first.ts)
    trader.on_bar(entry, now_ms=entry.ts)
    handles = trader.on_bar(target_hit, now_ms=target_hit.ts)

    order_rows = sqlite_repo._conn.execute("SELECT * FROM orders ORDER BY id").fetchall()
    fill_rows = sqlite_repo._conn.execute("SELECT * FROM fills ORDER BY id").fetchall()
    open_positions = sqlite_repo._conn.execute("SELECT * FROM positions WHERE closed_at IS NULL").fetchall()
    closed_positions = sqlite_repo._conn.execute("SELECT * FROM positions WHERE closed_at IS NOT NULL").fetchall()

    assert [handle.status for handle in handles] == ["filled"]
    assert [row["purpose"] for row in order_rows] == ["entry", "exit"]
    assert len(fill_rows) == 2
    assert open_positions == []
    assert len(closed_positions) == 1


def test_dashboard_paper_trader_closes_position_when_signal_expires(
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
        strategies=[ExplorationStrategy("paper_momentum", min_bars=2)],
        notional_usdt=25.0,
        cooldown_ms=0,
    )
    first = Bar("BTCUSDT", "1m", 1_700_000_000_000, 100, 101, 99.8, 100, 10, closed=True)
    entry = Bar("BTCUSDT", "1m", 1_700_000_060_000, 100, 102.2, 101.8, 102, 12, closed=True)
    expired = Bar("BTCUSDT", "1m", 1_700_000_541_000, 102, 102.2, 101.9, 102.1, 12, closed=True)

    trader.on_bar(first, now_ms=first.ts)
    trader.on_bar(entry, now_ms=entry.ts)
    handles = trader.on_bar(expired, now_ms=expired.ts)

    order_rows = sqlite_repo._conn.execute("SELECT purpose FROM orders ORDER BY id").fetchall()
    open_positions = sqlite_repo._conn.execute("SELECT * FROM positions WHERE closed_at IS NULL").fetchall()

    assert [handle.status for handle in handles] == ["filled"]
    assert [row["purpose"] for row in order_rows] == ["entry", "exit"]
    assert open_positions == []


def test_dashboard_paper_trader_skips_fee_dominated_mean_reversion_reversal(
    sqlite_repo: SqliteRepo,
) -> None:
    _seed_symbol(sqlite_repo, "BTCUSDT")
    cache = MemoryCache(max_bars=20)
    engine = PaperMatchingEngine(sqlite_repo, get_price=lambda symbol: cache.latest_price(symbol))
    strategy = _ScriptedDashboardStrategy(
        "paper_mean_reversion",
        [
            Signal(
                side="long",
                symbol="BTCUSDT",
                stop_price=99.0,
                target_price=None,
                confidence=0.7,
                expires_in_ms=8 * 60_000,
            ),
            Signal(
                side="short",
                symbol="BTCUSDT",
                stop_price=101.0,
                target_price=None,
                confidence=0.7,
                expires_in_ms=8 * 60_000,
            ),
        ],
    )
    trader = DashboardPaperTrader(
        repo=sqlite_repo,
        cache=cache,
        engine=engine,
        symbols=["BTCUSDT"],
        strategies=[strategy],
        notional_usdt=25.0,
        cooldown_ms=0,
    )
    entry = Bar("BTCUSDT", "1m", 1_700_000_000_000, 100, 100.1, 99.9, 100, 10, closed=True)
    tiny_reversal = Bar("BTCUSDT", "1m", 1_700_000_061_000, 100, 100.2, 99.9, 100.01, 10, closed=True)

    first_handles = trader.on_bar(entry, now_ms=entry.ts)
    second_handles = trader.on_bar(tiny_reversal, now_ms=tiny_reversal.ts)
    matrix = trader.strategy_matrix()

    order_count = sqlite_repo._conn.execute("SELECT COUNT(*) AS n FROM orders").fetchone()
    open_positions = sqlite_repo._conn.execute("SELECT * FROM positions WHERE closed_at IS NULL").fetchall()
    assert [handle.status for handle in first_handles] == ["filled"]
    assert second_handles == []
    assert order_count["n"] == 1
    assert len(open_positions) == 1
    assert matrix["cells"][0]["throttle_reason"] == "insufficient_edge"


def test_dashboard_paper_trader_allows_mean_reversion_reversal_with_fee_edge(
    sqlite_repo: SqliteRepo,
) -> None:
    _seed_symbol(sqlite_repo, "BTCUSDT")
    cache = MemoryCache(max_bars=20)
    engine = PaperMatchingEngine(sqlite_repo, get_price=lambda symbol: cache.latest_price(symbol))
    strategy = _ScriptedDashboardStrategy(
        "paper_mean_reversion",
        [
            Signal(
                side="long",
                symbol="BTCUSDT",
                stop_price=99.0,
                target_price=None,
                confidence=0.7,
                expires_in_ms=8 * 60_000,
            ),
            Signal(
                side="short",
                symbol="BTCUSDT",
                stop_price=102.0,
                target_price=None,
                confidence=0.7,
                expires_in_ms=8 * 60_000,
            ),
        ],
    )
    trader = DashboardPaperTrader(
        repo=sqlite_repo,
        cache=cache,
        engine=engine,
        symbols=["BTCUSDT"],
        strategies=[strategy],
        notional_usdt=25.0,
        cooldown_ms=0,
    )
    entry = Bar("BTCUSDT", "1m", 1_700_000_000_000, 100, 100.1, 99.9, 100, 10, closed=True)
    strong_reversal = Bar("BTCUSDT", "1m", 1_700_000_061_000, 100, 101.2, 99.9, 101.0, 10, closed=True)

    trader.on_bar(entry, now_ms=entry.ts)
    handles = trader.on_bar(strong_reversal, now_ms=strong_reversal.ts)

    order_count = sqlite_repo._conn.execute("SELECT COUNT(*) AS n FROM orders").fetchone()
    assert [handle.status for handle in handles] == ["filled"]
    assert order_count["n"] == 2


def test_dashboard_paper_trader_skips_fee_dominated_mean_reversion_entry(
    sqlite_repo: SqliteRepo,
) -> None:
    _seed_symbol(sqlite_repo, "BTCUSDT")
    cache = MemoryCache(max_bars=20)
    engine = PaperMatchingEngine(sqlite_repo, get_price=lambda symbol: cache.latest_price(symbol))
    strategy = _ScriptedDashboardStrategy(
        "paper_mean_reversion",
        [
            Signal(
                side="long",
                symbol="BTCUSDT",
                stop_price=99.0,
                target_price=100.4,
                confidence=0.7,
                expires_in_ms=8 * 60_000,
            ),
        ],
    )
    trader = DashboardPaperTrader(
        repo=sqlite_repo,
        cache=cache,
        engine=engine,
        symbols=["BTCUSDT"],
        strategies=[strategy],
        notional_usdt=25.0,
        cooldown_ms=0,
    )
    tiny_edge = Bar("BTCUSDT", "1m", 1_700_000_000_000, 100, 100.1, 99.9, 100, 10, closed=True)

    handles = trader.on_bar(tiny_edge, now_ms=tiny_edge.ts)
    matrix = trader.strategy_matrix()

    order_count = sqlite_repo._conn.execute("SELECT COUNT(*) AS n FROM orders").fetchone()
    assert handles == []
    assert order_count["n"] == 0
    assert matrix["cells"][0]["throttle_reason"] == "insufficient_expected_edge"


def test_dashboard_paper_trader_skips_low_value_mean_reversion_entry(
    sqlite_repo: SqliteRepo,
) -> None:
    _seed_symbol(sqlite_repo, "BTCUSDT")
    cache = MemoryCache(max_bars=20)
    engine = PaperMatchingEngine(sqlite_repo, get_price=lambda symbol: cache.latest_price(symbol))
    strategy = _ScriptedDashboardStrategy(
        "paper_mean_reversion",
        [
            Signal(
                side="long",
                symbol="BTCUSDT",
                stop_price=98.0,
                target_price=100.5,
                confidence=0.7,
                expires_in_ms=8 * 60_000,
            ),
        ],
    )
    trader = DashboardPaperTrader(
        repo=sqlite_repo,
        cache=cache,
        engine=engine,
        symbols=["BTCUSDT"],
        strategies=[strategy],
        notional_usdt=150.0,
        cooldown_ms=0,
    )
    small_edge = Bar("BTCUSDT", "1m", 1_700_000_000_000, 100, 100.1, 99.9, 100, 10, closed=True)

    handles = trader.on_bar(small_edge, now_ms=small_edge.ts)
    matrix = trader.strategy_matrix()

    order_count = sqlite_repo._conn.execute("SELECT COUNT(*) AS n FROM orders").fetchone()
    assert handles == []
    assert order_count["n"] == 0
    assert matrix["cells"][0]["throttle_reason"] == "insufficient_expected_edge"


def test_dashboard_paper_trader_skips_medium_value_mean_reversion_entry(
    sqlite_repo: SqliteRepo,
) -> None:
    _seed_symbol(sqlite_repo, "BTCUSDT")
    cache = MemoryCache(max_bars=20)
    engine = PaperMatchingEngine(sqlite_repo, get_price=lambda symbol: cache.latest_price(symbol))
    strategy = _ScriptedDashboardStrategy(
        "paper_mean_reversion",
        [
            Signal(
                side="long",
                symbol="BTCUSDT",
                stop_price=98.0,
                target_price=101.0,
                confidence=0.7,
                expires_in_ms=8 * 60_000,
            ),
        ],
    )
    trader = DashboardPaperTrader(
        repo=sqlite_repo,
        cache=cache,
        engine=engine,
        symbols=["BTCUSDT"],
        strategies=[strategy],
        notional_usdt=150.0,
        cooldown_ms=0,
    )
    medium_edge = Bar("BTCUSDT", "1m", 1_700_000_000_000, 100, 100.1, 99.9, 100, 10, closed=True)

    handles = trader.on_bar(medium_edge, now_ms=medium_edge.ts)
    matrix = trader.strategy_matrix()

    order_count = sqlite_repo._conn.execute("SELECT COUNT(*) AS n FROM orders").fetchone()
    assert handles == []
    assert order_count["n"] == 0
    assert matrix["cells"][0]["throttle_reason"] == "insufficient_expected_edge"


def test_dashboard_paper_trader_keeps_mean_reversion_open_on_tiny_ttl_edge(
    sqlite_repo: SqliteRepo,
) -> None:
    _seed_symbol(sqlite_repo, "BTCUSDT")
    cache = MemoryCache(max_bars=20)
    engine = PaperMatchingEngine(sqlite_repo, get_price=lambda symbol: cache.latest_price(symbol))
    strategy = _ScriptedDashboardStrategy(
        "paper_mean_reversion",
        [
            Signal(
                side="long",
                symbol="BTCUSDT",
                stop_price=98.0,
                target_price=103.0,
                confidence=0.7,
                expires_in_ms=8 * 60_000,
            ),
            None,
        ],
    )
    trader = DashboardPaperTrader(
        repo=sqlite_repo,
        cache=cache,
        engine=engine,
        symbols=["BTCUSDT"],
        strategies=[strategy],
        notional_usdt=150.0,
        cooldown_ms=0,
    )
    entry = Bar("BTCUSDT", "1m", 1_700_000_000_000, 100, 100.1, 99.9, 100, 10, closed=True)
    tiny_ttl_edge = Bar("BTCUSDT", "1m", 1_700_000_541_000, 100.0, 100.2, 99.95, 100.2, 10, closed=True)

    trader.on_bar(entry, now_ms=entry.ts)
    handles = trader.on_bar(tiny_ttl_edge, now_ms=tiny_ttl_edge.ts)

    order_rows = sqlite_repo._conn.execute("SELECT purpose FROM orders ORDER BY id").fetchall()
    open_positions = sqlite_repo._conn.execute("SELECT * FROM positions WHERE closed_at IS NULL").fetchall()
    assert handles == []
    assert [row["purpose"] for row in order_rows] == ["entry"]
    assert len(open_positions) == 1


def test_dashboard_paper_trader_closes_legacy_tiny_mean_reversion_position(
    sqlite_repo: SqliteRepo,
) -> None:
    _seed_symbol(sqlite_repo, "BTCUSDT")
    symbol = sqlite_repo.get_symbol("BTCUSDT")
    assert symbol is not None
    sqlite_repo.insert_position(
        {
            "symbol_id": symbol["id"],
            "strategy": "paper_mean_reversion",
            "strategy_version": "paper_mean_reversion",
            "opening_signal_id": None,
            "side": "long",
            "qty": 0.2,
            "avg_entry_price": 100.0,
            "current_price": 100.0,
            "unrealized_pnl": 0.0,
            "realized_pnl": 0.0,
            "leverage": 1.0,
            "margin": None,
            "liq_price": None,
            "stop_order_id": None,
            "trade_group_id": "legacy-tiny",
            "opened_at": 1_700_000_000_000,
            "closed_at": None,
        }
    )
    cache = MemoryCache(max_bars=20)
    engine = PaperMatchingEngine(sqlite_repo, get_price=lambda symbol: cache.latest_price(symbol))
    strategy = _ScriptedDashboardStrategy("paper_mean_reversion", [None])
    trader = DashboardPaperTrader(
        repo=sqlite_repo,
        cache=cache,
        engine=engine,
        symbols=["BTCUSDT"],
        strategies=[strategy],
        notional_usdt=150.0,
        cooldown_ms=0,
    )
    bar = Bar("BTCUSDT", "1m", 1_700_000_061_000, 100, 100.1, 99.9, 100, 10, closed=True)

    handles = trader.on_bar(bar, now_ms=bar.ts)

    order_rows = sqlite_repo._conn.execute("SELECT purpose FROM orders ORDER BY id").fetchall()
    open_positions = sqlite_repo._conn.execute("SELECT * FROM positions WHERE closed_at IS NULL").fetchall()
    assert [handle.status for handle in handles] == ["filled"]
    assert [row["purpose"] for row in order_rows] == ["exit"]
    assert open_positions == []


def test_dashboard_paper_trader_closes_legacy_tiny_position_outside_active_symbols(
    sqlite_repo: SqliteRepo,
) -> None:
    _seed_symbol(sqlite_repo, "GALAUSDT")
    symbol = sqlite_repo.get_symbol("GALAUSDT")
    assert symbol is not None
    sqlite_repo.insert_position(
        {
            "symbol_id": symbol["id"],
            "strategy": "paper_mean_reversion",
            "strategy_version": "paper_mean_reversion",
            "opening_signal_id": None,
            "side": "short",
            "qty": 4901.96,
            "avg_entry_price": 0.0041,
            "current_price": 0.0034,
            "unrealized_pnl": 0.0,
            "realized_pnl": 0.0,
            "leverage": 1.0,
            "margin": None,
            "liq_price": None,
            "stop_order_id": None,
            "trade_group_id": "legacy-outside-universe",
            "opened_at": 1_700_000_000_000,
            "closed_at": None,
        }
    )
    cache = MemoryCache(max_bars=20)
    cache.push_bar(Bar("GALAUSDT", "1m", 1_700_000_060_000, 0.0034, 0.0035, 0.0033, 0.0034, 10, closed=True))
    engine = PaperMatchingEngine(sqlite_repo, get_price=lambda symbol: cache.latest_price(symbol))
    strategy = _ScriptedDashboardStrategy("paper_mean_reversion", [None])
    trader = DashboardPaperTrader(
        repo=sqlite_repo,
        cache=cache,
        engine=engine,
        symbols=["BTCUSDT"],
        strategies=[strategy],
        notional_usdt=150.0,
        cooldown_ms=0,
    )

    handles = trader.close_legacy_tiny_positions(now_ms=1_700_000_061_000)

    order_rows = sqlite_repo._conn.execute("SELECT purpose FROM orders ORDER BY id").fetchall()
    open_positions = sqlite_repo._conn.execute("SELECT * FROM positions WHERE closed_at IS NULL").fetchall()
    assert [handle.status for handle in handles] == ["filled"]
    assert [row["purpose"] for row in order_rows] == ["exit"]
    assert open_positions == []


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


def test_dashboard_paper_trader_rounds_quantity_up_to_min_notional(
    sqlite_repo: SqliteRepo,
) -> None:
    _seed_symbol_with_limits(sqlite_repo, symbol="UNIUSDT", tick_size=0.001, lot_size=0.01, min_notional=5.0)
    cache = MemoryCache(max_bars=20)
    engine = PaperMatchingEngine(sqlite_repo, get_price=lambda symbol: cache.latest_price(symbol))
    trader = DashboardPaperTrader(
        repo=sqlite_repo,
        cache=cache,
        engine=engine,
        symbols=["UNIUSDT"],
        strategies=[ExplorationStrategy("paper_momentum", min_bars=2)],
        notional_usdt=20.0,
        strategy_notional_multipliers={"paper_momentum": 0.25},
        cooldown_ms=0,
    )
    warmup = Bar("UNIUSDT", "1m", 1_700_000_000_000, 3.8, 3.82, 3.79, 3.8, 10, closed=True)
    signal_bar = Bar("UNIUSDT", "1m", 1_700_000_060_000, 3.8, 3.86, 3.79, 3.84, 10, closed=True)

    trader.on_bar(warmup, now_ms=warmup.ts)
    handles = trader.on_bar(signal_bar, now_ms=signal_bar.ts)

    fill = sqlite_repo._conn.execute("SELECT price, quantity FROM fills").fetchone()
    risk = sqlite_repo._conn.execute("SELECT * FROM risk_events").fetchone()
    assert [handle.status for handle in handles] == ["filled"]
    assert fill["price"] * fill["quantity"] >= 5.0
    assert risk is None


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
        strategies=[ExplorationStrategy("explore_momentum", min_bars=2)],
        notional_usdt=25.0,
        cooldown_ms=0,
    )
    warmup = Bar("BTCUSDT", "1m", 1_700_000_000_000, 100, 100.2, 99.9, 100, 10, closed=True)
    bar = Bar("BTCUSDT", "1m", 1_700_000_060_000, 100, 102.2, 101.7, 102, 10, closed=True)

    trader.on_bar(warmup, now_ms=warmup.ts)
    handles = trader.on_bar(bar, now_ms=bar.ts)

    order_count = sqlite_repo._conn.execute("SELECT COUNT(*) AS n FROM orders").fetchone()
    risk_row = sqlite_repo._conn.execute("SELECT * FROM risk_events").fetchone()
    assert handles == []
    assert order_count["n"] == 0
    assert risk_row["source"] == "L1"
    assert "min_notional" in risk_row["payload"]


def test_dashboard_paper_trader_ignores_legacy_positions_for_active_risk_scope(
    tmp_path: Path,
    sqlite_repo: SqliteRepo,
) -> None:
    _seed_symbol(sqlite_repo, "BTCUSDT")
    symbol = sqlite_repo.get_symbol("BTCUSDT")
    assert symbol is not None
    sqlite_repo.insert_position(
        {
            "symbol_id": symbol["id"],
            "strategy": "explore_momentum",
            "strategy_version": "explore_momentum",
            "opening_signal_id": None,
            "side": "long",
            "qty": 10.0,
            "avg_entry_price": 100.0,
            "current_price": 100.0,
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
    cache = MemoryCache(max_bars=20)
    feed = LiveFeed(ParquetIO(data_root=tmp_path / "parquet"), sqlite_repo, cache)
    engine = PaperMatchingEngine(sqlite_repo, get_price=lambda symbol: cache.latest_price(symbol))
    trader = DashboardPaperTrader(
        repo=sqlite_repo,
        cache=cache,
        engine=engine,
        symbols=["BTCUSDT"],
        strategies=[CoreStrategyAdapter(_SignalOnEveryBarStrategy(), feed=feed, repo=sqlite_repo)],
        notional_usdt=25.0,
        cooldown_ms=0,
        max_open_notional_usdt=25.0,
    )
    bar = Bar("BTCUSDT", "1m", 1_700_000_060_000, 100, 101, 99, 100, 10, closed=True)

    handles = trader.on_bar(bar, now_ms=bar.ts)

    assert [handle.status for handle in handles] == ["filled"]


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


def test_default_dashboard_strategies_use_paper_strategies_only(
    tmp_path: Path,
    sqlite_repo: SqliteRepo,
) -> None:
    cache = MemoryCache(max_bars=20)
    feed = LiveFeed(ParquetIO(data_root=tmp_path / "parquet"), sqlite_repo, cache)

    names = [strategy.name for strategy in default_dashboard_strategies(feed=feed, repo=sqlite_repo)]

    assert "paper_mean_reversion" in names
    assert "paper_trend_momentum" in names
    assert "paper_swing_breakout" in names
    assert "S1_btc_eth_trend" not in names
    assert "S2_altcoin_reversal" not in names
    assert "paper_momentum" not in names
    assert "paper_volatility" not in names
    assert "explore_momentum" not in names
    assert "explore_mean_reversion" not in names
    assert "explore_volatility" not in names
    assert "S3_pair_trading" not in names


def test_trend_momentum_strategy_trades_on_confirmed_5m_breakout() -> None:
    strategy = TrendMomentumStrategy(min_bars=12)
    base_ts = 1_700_000_000_000
    warmup = [
        Bar("BTCUSDT", "5m", base_ts + i * 300_000, 100, 101 + i * 0.03, 99.5, 100.2 + i * 0.02, 100 + i, closed=True)
        for i in range(12)
    ]
    weak_breakout = [
        *warmup,
        Bar("BTCUSDT", "5m", base_ts + 12 * 300_000, 100.4, 101.25, 100.1, 101.22, 120, closed=True),
    ]
    confirmed_breakout = [
        *warmup,
        Bar("BTCUSDT", "5m", base_ts + 12 * 300_000, 100.4, 102.2, 100.1, 102.1, 150, closed=True),
    ]

    assert not strategy.supports("BTCUSDT", "1m")
    assert strategy.supports("BTCUSDT", "5m")
    assert strategy.evaluate("BTCUSDT", weak_breakout) is None

    signal = strategy.evaluate("BTCUSDT", confirmed_breakout)

    assert signal is not None
    assert signal.side == "long"
    assert signal.target_price is not None
    assert signal.stop_price is not None
    assert (signal.target_price - 102.1) / (102.1 - signal.stop_price) >= 2.4
    assert signal.expires_in_ms >= 2 * 60 * 60 * 1000


def test_swing_breakout_strategy_trades_only_on_confirmed_15m_breakout() -> None:
    strategy = SwingBreakoutStrategy(min_bars=24)
    base_ts = 1_700_000_000_000
    warmup = [
        Bar("BTCUSDT", "15m", base_ts + i * 900_000, 100, 101 + i * 0.02, 99, 100.5 + i * 0.01, 100 + i, closed=True)
        for i in range(24)
    ]
    weak_breakout = [
        *warmup,
        Bar("BTCUSDT", "15m", base_ts + 24 * 900_000, 100.5, 101.02, 100.2, 101.05, 130, closed=True),
    ]
    confirmed_breakout = [
        *warmup,
        Bar("BTCUSDT", "15m", base_ts + 24 * 900_000, 100.5, 103.2, 100.2, 103.0, 180, closed=True),
    ]

    assert not strategy.supports("BTCUSDT", "1m")
    assert strategy.supports("BTCUSDT", "15m")
    assert strategy.evaluate("BTCUSDT", weak_breakout) is None

    signal = strategy.evaluate("BTCUSDT", confirmed_breakout)

    assert signal is not None
    assert signal.side == "long"
    assert signal.target_price is not None
    assert signal.stop_price is not None
    assert (signal.target_price - 103.0) / (103.0 - signal.stop_price) >= 2.9
    assert signal.expires_in_ms >= 4 * 60 * 60 * 1000


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


def test_core_strategy_adapter_separates_data_dependencies_from_trigger_timeframes(
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
        strategies=[
            CoreStrategyAdapter(
                _SignalWithDailyDependencyStrategy(),
                feed=feed,
                repo=sqlite_repo,
                trigger_timeframes=["4h"],
            )
        ],
        notional_usdt=25.0,
        cooldown_ms=0,
    )
    daily = Bar("BTCUSDT", "1d", 1_700_000_000_000, 100, 101, 99, 100, 10, closed=True)
    four_hour = Bar("BTCUSDT", "4h", 1_700_014_400_000, 100, 101, 99, 100, 10, closed=True)

    daily_handles = trader.on_bar(daily, now_ms=daily.ts)
    four_hour_handles = trader.on_bar(four_hour, now_ms=four_hour.ts)

    assert daily_handles == []
    assert [handle.status for handle in four_hour_handles] == ["filled"]


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
        strategies=[ExplorationStrategy("explore_momentum", min_bars=2)],
        notional_usdt=25.0,
        cooldown_ms=120_000,
    )
    warmup = Bar("BTCUSDT", "1m", 1_700_000_000_000, 100, 100.2, 99.9, 100, 10, closed=True)
    first = Bar("BTCUSDT", "1m", 1_700_000_060_000, 100, 102.2, 101.7, 102, 10, closed=True)
    second = Bar("BTCUSDT", "1m", 1_700_000_090_000, 102, 102.45, 102.1, 102.4, 12, closed=True)

    trader.on_bar(warmup, now_ms=warmup.ts)
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
        strategies=[ExplorationStrategy("explore_momentum", min_bars=2)],
        notional_usdt=25.0,
        cooldown_ms=0,
        max_orders_per_symbol=1,
        order_cap_window_ms=60_000,
    )
    warmup = Bar("BTCUSDT", "1m", 1_700_000_000_000, 100, 100.2, 99.9, 100, 10, closed=True)
    first = Bar("BTCUSDT", "1m", 1_700_000_060_000, 100, 102.2, 101.7, 102, 10, closed=True)
    second = Bar("BTCUSDT", "1m", 1_700_000_180_000, 102, 102.6, 102.1, 102.5, 12, closed=True)

    trader.on_bar(warmup, now_ms=warmup.ts)
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
        strategies=[ExplorationStrategy("explore_momentum", min_bars=2)],
        notional_usdt=25.0,
        cooldown_ms=0,
        max_orders_per_symbol=1,
        order_cap_window_ms=60_000,
    )
    warmup = Bar("BTCUSDT", "1m", 1_700_000_000_000, 100, 100.2, 99.9, 100, 10, closed=True)
    first = Bar("BTCUSDT", "1m", 1_700_000_060_000, 100, 102.2, 101.7, 102, 10, closed=True)
    second = Bar("BTCUSDT", "1m", 1_700_000_090_000, 102, 102.6, 102.1, 102.5, 12, closed=True)

    trader.on_bar(warmup, now_ms=warmup.ts)
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
        strategies=[ExplorationStrategy("explore_momentum", min_bars=2)],
        notional_usdt=25.0,
        cooldown_ms=0,
        max_open_notional_usdt=25.0,
    )
    warmup = Bar("BTCUSDT", "1m", 1_700_000_000_000, 100, 100.2, 99.9, 100, 10, closed=True)
    first = Bar("BTCUSDT", "1m", 1_700_000_060_000, 100, 102.2, 101.7, 102, 10, closed=True)
    second = Bar("BTCUSDT", "1m", 1_700_000_090_000, 102, 102.6, 102.1, 102.5, 12, closed=True)
    third = Bar("BTCUSDT", "1m", 1_700_000_120_000, 102.4, 102.45, 101.3, 101.5, 12, closed=True)

    trader.on_bar(warmup, now_ms=warmup.ts)
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
        strategies=[ExplorationStrategy("explore_momentum", min_bars=2)],
        notional_usdt=25.0,
        cooldown_ms=0,
    )
    warmup = Bar("BTCUSDT", "1m", 1_700_000_000_000, 100, 100.2, 99.9, 100, 10, closed=True)
    bar = Bar("BTCUSDT", "1m", 1_700_000_060_000, 100, 102.2, 101.7, 102, 10, closed=True)
    trader.on_bar(warmup, now_ms=warmup.ts)
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
