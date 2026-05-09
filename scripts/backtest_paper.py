#!/usr/bin/env python3
"""Run a paper backtest over historical parquet candles."""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--symbols", default=None, help="Comma-separated symbols for batch runs.")
    parser.add_argument("--timeframe", default="1h")
    parser.add_argument("--timeframes", default=None, help="Comma-separated timeframes for batch runs.")
    parser.add_argument("--data-root", type=Path, default=Path("data"))
    parser.add_argument("--db", type=Path, default=Path("data/historical-paper.sqlite"))
    parser.add_argument("--start-ms", type=int, default=None)
    parser.add_argument("--end-ms", type=int, default=None)
    parser.add_argument("--n", type=int, default=None)
    parser.add_argument("--min-bars", type=int, default=2)
    parser.add_argument("--report", type=Path, default=None)
    parser.add_argument("--summary", type=Path, default=None)
    return parser.parse_args()


def _open_repo(db_path: Path):
    from core.data.sqlite_repo import SqliteRepo
    from core.db.migration_runner import MigrationRunner

    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    MigrationRunner(migrations_dir=Path("migrations")).apply_all(conn)
    return conn, SqliteRepo(conn)


def _split_csv(value: str, *, upper: bool = False) -> tuple[str, ...]:
    items = (item.strip() for item in value.split(","))
    if upper:
        return tuple(item.upper() for item in items if item)
    return tuple(item for item in items if item)


def main() -> None:
    from core.data.parquet_io import ParquetIO
    from core.research.historical_paper_backtest import (
        HistoricalPaperBacktestConfig,
        HistoricalPaperBatchBacktestConfig,
        run_historical_paper_backtest,
        run_historical_paper_backtest_batch,
    )

    args = _parse_args()
    symbols = _split_csv(args.symbols, upper=True) if args.symbols else (args.symbol.upper(),)
    timeframes = _split_csv(args.timeframes) if args.timeframes else (args.timeframe,)
    is_batch = args.symbols is not None or args.timeframes is not None or len(symbols) * len(timeframes) > 1
    conn, repo = _open_repo(args.db)
    try:
        parquet_io = ParquetIO(args.data_root)
        if is_batch:
            payload = run_historical_paper_backtest_batch(
                repo,
                parquet_io,
                HistoricalPaperBatchBacktestConfig(
                    symbols=symbols,
                    timeframes=timeframes,
                    start_ms=args.start_ms,
                    end_ms=args.end_ms,
                    n=args.n,
                    min_bars=args.min_bars,
                    report_path=args.report,
                    summary_path=args.summary,
                ),
            )
        else:
            payload = run_historical_paper_backtest(
                repo,
                parquet_io,
                HistoricalPaperBacktestConfig(
                    symbol=symbols[0],
                    timeframe=timeframes[0],
                    start_ms=args.start_ms,
                    end_ms=args.end_ms,
                    n=args.n,
                    min_bars=args.min_bars,
                    report_path=args.report,
                    summary_path=args.summary,
                ),
            )
    finally:
        conn.close()

    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    if not payload["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
