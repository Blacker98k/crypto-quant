#!/usr/bin/env python3
"""Validate a historical parquet paper-backtest JSONL report."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path)
    parser.add_argument("--symbols", required=True, help="Comma-separated required symbols.")
    parser.add_argument("--timeframes", required=True, help="Comma-separated required timeframes.")
    parser.add_argument("--min-bars-per-cycle", type=int, required=True)
    parser.add_argument("--max-open-positions-per-cycle", type=int, default=0)
    parser.add_argument("--max-rejected-per-cycle", type=int, default=0)
    parser.add_argument("--max-risk-events-per-cycle", type=int, default=0)
    parser.add_argument("--min-pass-rate", type=float, default=1.0)
    parser.add_argument("--min-cycles", type=int, default=None)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    command = [
        sys.executable,
        str(Path(__file__).with_name("summarize_simulation_report.py")),
        str(args.report),
        "--min-pass-rate",
        str(args.min_pass_rate),
        "--max-failed-cycles",
        "0",
        "--min-bars-per-cycle",
        str(args.min_bars_per_cycle),
        "--max-open-positions-per-cycle",
        str(args.max_open_positions_per_cycle),
        "--max-rejected-per-cycle",
        str(args.max_rejected_per_cycle),
        "--max-risk-events-per-cycle",
        str(args.max_risk_events_per_cycle),
        "--require-all-price-source",
        "historical_parquet",
        "--forbid-reason",
        "no_bars",
        "--forbid-reason",
        "insufficient_bars",
    ]
    if args.min_cycles is not None:
        command.extend(["--min-cycles", str(args.min_cycles)])
    for symbol in _split_csv(args.symbols, upper=True):
        command.extend(["--require-symbol", symbol])
    for timeframe in _split_csv(args.timeframes):
        command.extend(["--require-timeframe", timeframe])

    result = subprocess.run(command, check=False, capture_output=True, encoding="utf-8")
    sys.stdout.write(result.stdout)
    sys.stderr.write(result.stderr)
    if result.returncode != 0:
        raise SystemExit(result.returncode)


def _split_csv(value: str, *, upper: bool = False) -> tuple[str, ...]:
    items = (item.strip() for item in value.split(","))
    if upper:
        return tuple(item.upper() for item in items if item)
    return tuple(item for item in items if item)


if __name__ == "__main__":
    main()
