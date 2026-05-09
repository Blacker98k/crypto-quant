from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from core.data.exchange.base import Bar
from core.research.historical_paper_backtest import (
    HistoricalPaperBacktestConfig,
    HistoricalPaperBatchBacktestConfig,
    run_historical_paper_backtest,
    run_historical_paper_backtest_batch,
)


def _bars(symbol: str = "BTCUSDT", timeframe: str = "1h") -> list[Bar]:
    base_ts = 1_700_000_000_000
    return [
        Bar(symbol, timeframe, base_ts, 50000, 50100, 49900, 50000, 1, 50000),
        Bar(symbol, timeframe, base_ts + 3_600_000, 50050, 50200, 50000, 50100, 1, 50100),
        Bar(symbol, timeframe, base_ts + 7_200_000, 50100, 50300, 50050, 50200, 1, 50200),
    ]


def test_historical_paper_backtest_runs_parquet_bars(sqlite_repo, parquet_io) -> None:
    parquet_io.write_bars(_bars())

    payload = run_historical_paper_backtest(
        sqlite_repo,
        parquet_io,
        HistoricalPaperBacktestConfig(symbol="BTCUSDT", timeframe="1h"),
    )

    assert payload["passed"] is True
    assert payload["price_source"] == "historical_parquet"
    assert payload["bar_source"] == "parquet"
    assert payload["result"] == {
        "bars": 3,
        "signals": 2,
        "rejected": 0,
        "orders": 2,
        "fills": 2,
        "open_positions": 0,
        "risk_events": 0,
    }


def test_historical_paper_backtest_fails_without_bars(sqlite_repo, parquet_io) -> None:
    payload = run_historical_paper_backtest(
        sqlite_repo,
        parquet_io,
        HistoricalPaperBacktestConfig(symbol="BTCUSDT", timeframe="1h"),
    )

    assert payload["passed"] is False
    assert payload["reason"] == "no_bars"
    assert payload["result"]["bars"] == 0


def test_historical_paper_backtest_fails_when_window_is_too_short(
    sqlite_repo,
    parquet_io,
) -> None:
    parquet_io.write_bars(_bars())

    payload = run_historical_paper_backtest(
        sqlite_repo,
        parquet_io,
        HistoricalPaperBacktestConfig(symbol="BTCUSDT", timeframe="1h", min_bars=4),
    )

    assert payload["passed"] is False
    assert payload["reason"] == "insufficient_bars"
    assert payload["result"]["bars"] == 3
    assert payload["result"]["orders"] == 0


def test_historical_paper_backtest_writes_report_and_summary(
    sqlite_repo,
    parquet_io,
    tmp_path: Path,
) -> None:
    parquet_io.write_bars(_bars(symbol="ETHUSDT"))
    report_path = tmp_path / "reports" / "historical.jsonl"
    summary_path = tmp_path / "reports" / "historical-summary.json"

    payload = run_historical_paper_backtest(
        sqlite_repo,
        parquet_io,
        HistoricalPaperBacktestConfig(
            symbol="ETHUSDT",
            timeframe="1h",
            report_path=report_path,
            summary_path=summary_path,
        ),
    )

    assert json.loads(report_path.read_text(encoding="utf-8")) == payload
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["cycles"] == 1
    assert summary["passed"] == 1
    assert summary["price_sources"] == ["historical_parquet"]
    assert summary["totals"]["orders"] == 2


def test_backtest_paper_cli_reads_parquet_and_writes_report(
    parquet_io,
    tmp_path: Path,
) -> None:
    parquet_io.write_bars(_bars())
    db_path = tmp_path / "historical.sqlite"
    report_path = tmp_path / "historical.jsonl"
    summary_path = tmp_path / "historical-summary.json"

    result = subprocess.run(
        [
            sys.executable,
            "scripts/backtest_paper.py",
            "--symbol",
            "BTCUSDT",
            "--timeframe",
            "1h",
            "--data-root",
            str(parquet_io._root),
            "--db",
            str(db_path),
            "--report",
            str(report_path),
            "--summary",
            str(summary_path),
        ],
        check=True,
        capture_output=True,
        encoding="utf-8",
    )

    payload = json.loads(result.stdout)
    assert payload["passed"] is True
    assert payload["price_source"] == "historical_parquet"
    assert json.loads(report_path.read_text(encoding="utf-8")) == payload
    assert json.loads(summary_path.read_text(encoding="utf-8"))["passed"] == 1


def test_historical_paper_backtest_batch_isolates_timeframe_state(
    sqlite_repo,
    parquet_io,
    tmp_path: Path,
) -> None:
    parquet_io.write_bars(_bars(symbol="BTCUSDT", timeframe="1h"))
    parquet_io.write_bars(_bars(symbol="BTCUSDT", timeframe="4h"))
    report_path = tmp_path / "historical-batch.jsonl"
    summary_path = tmp_path / "historical-batch-summary.json"

    payload = run_historical_paper_backtest_batch(
        sqlite_repo,
        parquet_io,
        HistoricalPaperBatchBacktestConfig(
            symbols=("BTCUSDT",),
            timeframes=("1h", "4h"),
            report_path=report_path,
            summary_path=summary_path,
        ),
    )

    assert payload["passed"] is True
    assert payload["cycles"] == 2
    assert [cycle["cycle"] for cycle in payload["results"]] == [1, 2]
    assert [cycle["timeframe"] for cycle in payload["results"]] == ["1h", "4h"]
    assert [cycle["result"]["orders"] for cycle in payload["results"]] == [2, 2]
    assert [json.loads(line) for line in report_path.read_text(encoding="utf-8").splitlines()] == payload[
        "results"
    ]
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["cycles"] == 2
    assert summary["passed"] == 2
    assert summary["totals"]["orders"] == 4


def test_backtest_paper_cli_runs_batch_symbols_and_timeframes(
    parquet_io,
    tmp_path: Path,
) -> None:
    parquet_io.write_bars(_bars(symbol="BTCUSDT", timeframe="1h"))
    parquet_io.write_bars(_bars(symbol="ETHUSDT", timeframe="1h"))
    db_path = tmp_path / "historical-batch.sqlite"
    summary_path = tmp_path / "historical-batch-summary.json"

    result = subprocess.run(
        [
            sys.executable,
            "scripts/backtest_paper.py",
            "--symbols",
            "BTCUSDT,ETHUSDT",
            "--timeframes",
            "1h",
            "--data-root",
            str(parquet_io._root),
            "--db",
            str(db_path),
            "--summary",
            str(summary_path),
        ],
        check=True,
        capture_output=True,
        encoding="utf-8",
    )

    payload = json.loads(result.stdout)
    assert payload["passed"] is True
    assert payload["cycles"] == 2
    assert [cycle["symbol"] for cycle in payload["results"]] == ["BTCUSDT", "ETHUSDT"]
    assert json.loads(summary_path.read_text(encoding="utf-8"))["totals"]["fills"] == 4


def test_historical_readiness_cli_runs_backtest_and_validator(
    parquet_io,
    tmp_path: Path,
) -> None:
    parquet_io.write_bars(_bars(symbol="BTCUSDT", timeframe="1h"))
    parquet_io.write_bars(_bars(symbol="ETHUSDT", timeframe="1h"))
    db_path = tmp_path / "historical-readiness.sqlite"
    report_path = tmp_path / "historical-readiness.jsonl"
    summary_path = tmp_path / "historical-readiness-summary.json"

    result = subprocess.run(
        [
            sys.executable,
            "scripts/historical_readiness.py",
            "--symbols",
            "BTCUSDT,ETHUSDT",
            "--timeframes",
            "1h",
            "--data-root",
            str(parquet_io._root),
            "--db",
            str(db_path),
            "--report",
            str(report_path),
            "--summary",
            str(summary_path),
            "--min-bars-per-cycle",
            "3",
        ],
        check=True,
        capture_output=True,
        encoding="utf-8",
    )

    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["backtest"]["cycles"] == 2
    assert payload["validation"]["symbols"] == ["BTCUSDT", "ETHUSDT"]
    assert payload["validation"]["timeframes"] == ["1h"]
    assert len(report_path.read_text(encoding="utf-8").splitlines()) == 2


def test_historical_readiness_cli_resets_owned_artifacts_before_run(
    parquet_io,
    tmp_path: Path,
) -> None:
    parquet_io.write_bars(_bars(symbol="BTCUSDT", timeframe="1h"))
    db_path = tmp_path / "historical-readiness.sqlite"
    report_path = tmp_path / "historical-readiness.jsonl"
    summary_path = tmp_path / "historical-readiness-summary.json"
    report_path.write_text('{"stale": true}\n', encoding="utf-8")
    summary_path.write_text('{"stale": true}\n', encoding="utf-8")

    result = subprocess.run(
        [
            sys.executable,
            "scripts/historical_readiness.py",
            "--symbols",
            "BTCUSDT",
            "--timeframes",
            "1h",
            "--data-root",
            str(parquet_io._root),
            "--db",
            str(db_path),
            "--report",
            str(report_path),
            "--summary",
            str(summary_path),
            "--min-bars-per-cycle",
            "3",
        ],
        check=True,
        capture_output=True,
        encoding="utf-8",
    )

    payload = json.loads(result.stdout)
    report_rows = [json.loads(line) for line in report_path.read_text(encoding="utf-8").splitlines()]
    assert payload["status"] == "ok"
    assert report_rows == payload["backtest"]["results"]
    assert json.loads(summary_path.read_text(encoding="utf-8"))["cycles"] == 1
