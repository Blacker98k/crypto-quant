#!/usr/bin/env python3
"""Run a deterministic paper-session simulation.

The runner can seed the synthetic price path from Binance USDM public tickers,
then drives the normal strategy -> L1 risk -> paper matching -> positions path
with generated bars. It never sends live orders or requires API keys.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sqlite3
import sys
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from core.data.exchange.base import Bar
from core.data.exchange.binance_usdm import BinanceUsdmAdapter
from core.data.sqlite_repo import SqliteRepo
from core.db.migration_runner import MigrationRunner
from core.execution.simulation import (
    SimulatedPaperSession,
    SyntheticBarSpec,
    generate_synthetic_bars,
)
from core.monitor.simulation_report import SimulationReportWriter
from core.strategy import DataRequirement, Signal, Strategy

_DEFAULT_DB_PATH = Path("data/simulated_paper.sqlite")
_DEFAULT_STATIC_PRICE = 50_000.0


class PulseStrategy(Strategy):
    """Minimal strategy that opens on the first bar and closes on the last bar."""

    name = "sim_pulse"
    version = "dev"
    __slots__ = ("_close_at_ms", "_symbol")

    def __init__(self, symbol: str, close_at_ms: int) -> None:
        self._symbol = symbol
        self._close_at_ms = close_at_ms

    def required_data(self) -> DataRequirement:
        return DataRequirement(symbols=[self._symbol], timeframes=["1m"], history_lookback_bars=1)

    def on_bar(self, bar: Bar, ctx: Any) -> list[Signal]:
        if bar.ts == self._close_at_ms:
            return [Signal(side="close", symbol=bar.symbol)]
        if ctx.kv_get(f"{bar.symbol}_opened"):
            return []
        ctx.kv_set(f"{bar.symbol}_opened", True)
        return [
            Signal(
                side="long",
                symbol=bar.symbol,
                stop_price=bar.c * 0.99,
                suggested_size=0.01,
                rationale={"source": "synthetic_pulse"},
            )
        ]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--bars", type=int, default=120)
    parser.add_argument("--step-bps", type=float, default=3.0)
    parser.add_argument("--volume", type=float, default=1.0)
    parser.add_argument("--db", type=Path, default=_DEFAULT_DB_PATH)
    parser.add_argument("--proxy", default="")
    parser.add_argument(
        "--price-source",
        choices=("live", "static"),
        default="live",
        help="Use Binance USDM public ticker for the synthetic start price, or a static fallback.",
    )
    parser.add_argument("--static-price", type=float, default=_DEFAULT_STATIC_PRICE)
    parser.add_argument("--keep-db", action="store_true")
    parser.add_argument("--cycles", type=int, default=1)
    parser.add_argument("--interval-sec", type=float, default=0.0)
    parser.add_argument(
        "--report",
        type=Path,
        default=None,
        help="Optional JSONL path for durable per-cycle simulation reports.",
    )
    return parser.parse_args()


async def _fetch_live_price(symbol: str, proxy: str) -> float | None:
    exchange = BinanceUsdmAdapter(proxy=proxy, timeout_ms=15_000)
    try:
        tickers = await exchange.fetch_24h_tickers()
        for ticker in tickers:
            if ticker.symbol == symbol and ticker.last_price > 0:
                return ticker.last_price
    finally:
        await exchange.close()
    return None


def _open_repo(db_path: Path, keep_db: bool) -> SqliteRepo:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists() and not keep_db:
        db_path.unlink()
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    MigrationRunner(migrations_dir=Path("migrations")).apply_all(conn)
    return SqliteRepo(conn)


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


async def _resolve_start_price(args: argparse.Namespace) -> tuple[float, str]:
    if args.price_source == "static":
        return float(args.static_price), "static"
    try:
        live = await _fetch_live_price(str(args.symbol), str(args.proxy))
    except Exception as exc:
        return float(args.static_price), f"static_fallback:{type(exc).__name__}"
    if live is None:
        return float(args.static_price), "static_fallback:not_found"
    return live, "binance_usdm_public_ticker"


async def _run_cycle(args: argparse.Namespace, cycle: int) -> dict[str, Any]:
    start_price, price_source = await _resolve_start_price(args)
    now_ms = int(time.time() * 1000)
    start_ms = now_ms - (args.bars * 60_000)
    bars = generate_synthetic_bars(
        SyntheticBarSpec(
            symbol=args.symbol,
            timeframe="1m",
            start_ms=start_ms,
            count=args.bars,
            start_price=start_price,
            step_bps=args.step_bps,
            volume=args.volume,
        )
    )

    repo = _open_repo(args.db, bool(args.keep_db))
    try:
        _seed_symbol(repo, args.symbol)
        strategy = PulseStrategy(args.symbol, close_at_ms=bars[-1].ts)
        result = SimulatedPaperSession(repo, [strategy]).run(bars)
    finally:
        repo.close()

    passed = result.orders == 2 and result.fills == 2 and result.open_positions == 0
    return {
        "cycle": cycle,
        "symbol": args.symbol,
        "db": str(args.db),
        "price_source": price_source,
        "start_price": start_price,
        "result": asdict(result),
        "passed": passed,
    }


async def main() -> None:
    args = _parse_args()
    if args.bars < 2:
        raise SystemExit("--bars must be >= 2")
    if args.cycles < 1:
        raise SystemExit("--cycles must be >= 1")
    if args.interval_sec < 0:
        raise SystemExit("--interval-sec must be >= 0")

    all_passed = True
    writer = SimulationReportWriter(args.report) if args.report is not None else None
    try:
        for cycle in range(1, args.cycles + 1):
            payload = await _run_cycle(args, cycle)
            all_passed = all_passed and bool(payload["passed"])
            print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
            if writer is not None:
                writer.write_cycle(payload)
            if cycle < args.cycles and args.interval_sec:
                await asyncio.sleep(args.interval_sec)
    finally:
        if writer is not None:
            writer.close()

    if not all_passed:
        raise SystemExit(1)


if __name__ == "__main__":
    asyncio.run(main())
