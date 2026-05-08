"""Historical parquet-backed paper backtests."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from core.data.exchange.base import Bar
from core.data.parquet_io import ParquetIO
from core.data.sqlite_repo import SqliteRepo
from core.execution.simulation import SimulatedPaperSession, SimulationResult
from core.monitor.simulation_report import SimulationReportWriter, summarize_simulation_cycles
from core.strategy import DataRequirement, Signal, Strategy


@dataclass(frozen=True, slots=True)
class HistoricalPaperBacktestConfig:
    symbol: str = "BTCUSDT"
    timeframe: str = "1h"
    start_ms: int | None = None
    end_ms: int | None = None
    n: int | None = None
    report_path: Path | None = None
    summary_path: Path | None = None


@dataclass(frozen=True, slots=True)
class HistoricalPaperBatchBacktestConfig:
    symbols: tuple[str, ...] = ("BTCUSDT",)
    timeframes: tuple[str, ...] = ("1h",)
    start_ms: int | None = None
    end_ms: int | None = None
    n: int | None = None
    report_path: Path | None = None
    summary_path: Path | None = None


class HistoricalPulseStrategy(Strategy):
    """Open on the first historical bar and close on the last bar."""

    version = "dev"
    __slots__ = ("_close_at_ms", "_opened", "_symbol", "_timeframe", "name")

    def __init__(self, symbol: str, timeframe: str, close_at_ms: int) -> None:
        self._symbol = symbol
        self._timeframe = timeframe
        self._close_at_ms = close_at_ms
        self._opened = False
        self.name = f"historical_pulse_{symbol}_{timeframe}"

    def required_data(self) -> DataRequirement:
        return DataRequirement(
            symbols=[self._symbol],
            timeframes=[self._timeframe],
            history_lookback_bars=1,
        )

    def on_bar(self, bar: Bar, ctx: Any) -> list[Signal]:
        if bar.ts == self._close_at_ms:
            return [Signal(side="close", symbol=bar.symbol)]
        if self._opened:
            return []
        self._opened = True
        return [
            Signal(
                side="long",
                symbol=bar.symbol,
                stop_price=bar.c * 0.99,
                suggested_size=0.01,
                rationale={"source": "historical_pulse"},
            )
        ]


def run_historical_paper_backtest(
    repo: SqliteRepo,
    parquet_io: ParquetIO,
    config: HistoricalPaperBacktestConfig,
) -> dict[str, Any]:
    bars = parquet_io.read_bars(
        config.symbol,
        config.timeframe,
        start_ms=config.start_ms,
        end_ms=config.end_ms,
        n=config.n,
    )
    if not bars:
        payload = _payload(config, SimulationResult(), passed=False, reason="no_bars")
        _write_outputs(payload, config)
        return payload

    _seed_symbol(repo, config.symbol)
    strategy = HistoricalPulseStrategy(config.symbol, config.timeframe, close_at_ms=bars[-1].ts)
    result = SimulatedPaperSession(repo, [strategy]).run(bars)
    passed = result.orders == 2 and result.fills == 2 and result.open_positions == 0
    payload = _payload(config, result, passed=passed)
    _write_outputs(payload, config)
    return payload


def run_historical_paper_backtest_batch(
    repo: SqliteRepo,
    parquet_io: ParquetIO,
    config: HistoricalPaperBatchBacktestConfig,
) -> dict[str, Any]:
    results: list[dict[str, Any]] = []
    for symbol in config.symbols:
        for timeframe in config.timeframes:
            payload = run_historical_paper_backtest(
                repo,
                parquet_io,
                HistoricalPaperBacktestConfig(
                    symbol=symbol,
                    timeframe=timeframe,
                    start_ms=config.start_ms,
                    end_ms=config.end_ms,
                    n=config.n,
                ),
            )
            payload["cycle"] = len(results) + 1
            results.append(payload)

    _write_batch_outputs(results, config)
    failed = sum(1 for row in results if not row["passed"])
    return {
        "cycles": len(results),
        "passed": bool(results) and failed == 0,
        "failed": failed,
        "results": results,
        "summary": summarize_simulation_cycles(results),
    }


def _payload(
    config: HistoricalPaperBacktestConfig,
    result: SimulationResult,
    *,
    passed: bool,
    reason: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "cycle": 1,
        "symbol": config.symbol,
        "timeframe": config.timeframe,
        "bar_source": "parquet",
        "price_source": "historical_parquet",
        "result": asdict(result),
        "passed": passed,
    }
    if reason is not None:
        payload["reason"] = reason
    return payload


def _write_outputs(payload: dict[str, Any], config: HistoricalPaperBacktestConfig) -> None:
    if config.report_path is not None:
        with SimulationReportWriter(config.report_path) as writer:
            writer.write_cycle(payload)
    if config.summary_path is not None:
        config.summary_path.parent.mkdir(parents=True, exist_ok=True)
        config.summary_path.write_text(
            json.dumps(
                summarize_simulation_cycles([payload]),
                ensure_ascii=False,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )


def _write_batch_outputs(
    results: list[dict[str, Any]],
    config: HistoricalPaperBatchBacktestConfig,
) -> None:
    if config.report_path is not None:
        with SimulationReportWriter(config.report_path) as writer:
            for payload in results:
                writer.write_cycle(payload)
    if config.summary_path is not None:
        config.summary_path.parent.mkdir(parents=True, exist_ok=True)
        config.summary_path.write_text(
            json.dumps(
                summarize_simulation_cycles(results),
                ensure_ascii=False,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )


def _seed_symbol(repo: SqliteRepo, symbol: str) -> None:
    base = symbol.removesuffix("USDT") or symbol
    repo.upsert_symbols(
        [
            {
                "exchange": "binance",
                "symbol": symbol,
                "type": "perp",
                "base": base,
                "quote": "USDT",
                "tick_size": 0.1,
                "lot_size": 0.001,
                "min_notional": 10.0,
                "listed_at": 1_500_000_000_000,
            }
        ]
    )


__all__ = [
    "HistoricalPaperBacktestConfig",
    "HistoricalPaperBatchBacktestConfig",
    "HistoricalPulseStrategy",
    "run_historical_paper_backtest",
    "run_historical_paper_backtest_batch",
]
