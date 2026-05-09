"""End-to-end simulated paper session tests."""

from __future__ import annotations

import json

import pytest

from core.data.exchange.base import Bar
from core.execution.simulation import (
    SimulatedPaperSession,
    SyntheticBarSpec,
    generate_synthetic_bars,
)
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


def test_simulated_session_counts_and_records_signal_rejections(sqlite_repo) -> None:
    _seed_symbol(sqlite_repo)
    bars = [Bar("BTCUSDT", "1m", 1_700_000_000_000, 50000, 50100, 49900, 50000, 1, 50000)]

    result = SimulatedPaperSession(sqlite_repo, [NoStopStrategy()]).run(bars)

    assert result.signals == 1
    assert result.rejected == 1
    assert result.orders == 0
    assert result.fills == 0
    assert result.risk_events == 1

    events = sqlite_repo.get_recent_risk_events(limit=1)
    assert events[0]["type"] == "signal_rejected"
    assert events[0]["source"] == "strategy"
    assert json.loads(events[0]["payload"])["reason"] == "stop_price_required"


def test_generate_synthetic_bars_is_deterministic() -> None:
    bars = generate_synthetic_bars(
        SyntheticBarSpec(
            symbol="BTCUSDT",
            timeframe="1m",
            start_ms=1_700_000_000_000,
            count=3,
            start_price=50_000,
            step_bps=10,
            volume=2,
        )
    )

    assert [bar.ts for bar in bars] == [
        1_700_000_000_000,
        1_700_000_060_000,
        1_700_000_120_000,
    ]
    assert bars[0].o == 50_000
    assert bars[0].c == 50_050
    assert bars[1].o == 50_050
    assert bars[1].q == pytest.approx(bars[1].v * bars[1].c)


def test_generate_synthetic_bars_rejects_bad_specs() -> None:
    with pytest.raises(ValueError, match="start_price"):
        generate_synthetic_bars(SyntheticBarSpec("BTCUSDT", "1m", 0, 1, 0))
    with pytest.raises(ValueError, match="unsupported timeframe"):
        generate_synthetic_bars(SyntheticBarSpec("BTCUSDT", "tick", 0, 1, 50_000))
