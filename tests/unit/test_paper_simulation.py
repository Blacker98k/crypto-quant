"""End-to-end simulated paper session tests."""

from __future__ import annotations

from core.data.exchange.base import Bar
from core.execution.simulation import SimulatedPaperSession
from core.strategy.base import DataRequirement, Signal, Strategy


class ToggleStrategy(Strategy):
    name = "toggle"
    version = "dev"
    __slots__ = ()

    def required_data(self):
        return DataRequirement(symbols=["BTCUSDT"], timeframes=["1m"], history_lookback_bars=2)

    def on_bar(self, bar, ctx):
        if bar.ts == 1_700_000_000_000:
            return [
                Signal(
                    side="long",
                    symbol="BTCUSDT",
                    stop_price=bar.c - 100,
                    suggested_size=0.01,
                )
            ]
        if bar.ts == 1_700_000_060_000:
            return [Signal(side="close", symbol="BTCUSDT")]
        return []


def _seed_symbol(sqlite_repo) -> None:
    sqlite_repo.upsert_symbols([
        {
            "exchange": "binance",
            "symbol": "BTCUSDT",
            "type": "perp",
            "base": "BTC",
            "quote": "USDT",
            "tick_size": 0.1,
            "lot_size": 0.001,
            "min_notional": 10.0,
            "listed_at": 1_500_000_000_000,
        }
    ])


def test_simulated_session_runs_signal_to_position_round_trip(sqlite_repo) -> None:
    _seed_symbol(sqlite_repo)
    bars = [
        Bar("BTCUSDT", "1m", 1_700_000_000_000, 50000, 50100, 49900, 50000, 1, 50000),
        Bar("BTCUSDT", "1m", 1_700_000_060_000, 50100, 50200, 50000, 50100, 1, 50100),
    ]

    result = SimulatedPaperSession(sqlite_repo, [ToggleStrategy()]).run(bars)

    assert result.bars == 2
    assert result.signals == 2
    assert result.orders == 2
    assert result.fills == 2
    assert result.rejected == 0
    assert result.open_positions == 0


class NoStopStrategy(ToggleStrategy):
    name = "no_stop"

    def on_bar(self, bar, ctx):
        return [
            Signal(
                side="long",
                symbol="BTCUSDT",
                stop_price=None,
                suggested_size=0.01,
            )
        ]


def test_simulated_session_counts_l1_rejections(sqlite_repo) -> None:
    _seed_symbol(sqlite_repo)
    bars = [Bar("BTCUSDT", "1m", 1_700_000_000_000, 50000, 50100, 49900, 50000, 1, 50000)]

    result = SimulatedPaperSession(sqlite_repo, [NoStopStrategy()]).run(bars)

    assert result.signals == 1
    assert result.rejected == 1
    assert result.orders == 0
    assert result.fills == 0

