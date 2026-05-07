from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def _write_report(path: Path, rows: list[dict[str, object]]) -> None:
    path.write_text(
        "\n".join(json.dumps(row, sort_keys=True) for row in rows) + "\n",
        encoding="utf-8",
    )


def test_summarize_simulation_report_cli_enforces_strict_source_gates(
    tmp_path: Path,
) -> None:
    report_path = tmp_path / "sim.jsonl"
    _write_report(
        report_path,
        [
            {
                "cycle": 1,
                "symbol": "BTCUSDT",
                "price_source": "binance_usdm_public_ticker",
                "passed": True,
                "result": {"bars": 2, "orders": 2},
            },
            {
                "cycle": 2,
                "symbol": "BTCUSDT",
                "price_source": "static_fallback:TimeoutError",
                "passed": True,
                "result": {"bars": 2, "orders": 2},
            },
            {
                "cycle": 3,
                "symbol": "BTCUSDT",
                "price_source": "static",
                "passed": False,
                "result": {"bars": 2, "orders": 0},
            },
        ],
    )

    result = subprocess.run(
        [
            sys.executable,
            "scripts/summarize_simulation_report.py",
            str(report_path),
            "--min-cycles",
            "4",
            "--max-failed-cycles",
            "0",
            "--forbid-price-source-prefix",
            "static_fallback",
            "--require-all-price-source",
            "binance_usdm_public_ticker",
        ],
        check=False,
        capture_output=True,
        encoding="utf-8",
    )

    assert result.returncode == 1
    assert json.loads(result.stdout)["price_sources"] == [
        "binance_usdm_public_ticker",
        "static",
        "static_fallback:TimeoutError",
    ]
    assert "cycles 3 below required 4" in result.stderr
    assert "failed cycles 1 above allowed 0" in result.stderr
    assert (
        "forbidden price source prefix static_fallback matched: "
        "static_fallback:TimeoutError"
    ) in result.stderr
    assert (
        "unexpected price sources: static, static_fallback:TimeoutError; "
        "required only: binance_usdm_public_ticker"
    ) in result.stderr


def test_summarize_simulation_report_cli_accepts_strict_live_source(
    tmp_path: Path,
) -> None:
    report_path = tmp_path / "sim.jsonl"
    _write_report(
        report_path,
        [
            {
                "cycle": 1,
                "symbol": "BTCUSDT",
                "price_source": "binance_usdm_public_ticker",
                "passed": True,
                "result": {"bars": 2, "orders": 2},
            },
            {
                "cycle": 2,
                "symbol": "BTCUSDT",
                "price_source": "binance_usdm_public_ticker",
                "passed": True,
                "result": {"bars": 2, "orders": 2},
            },
        ],
    )

    result = subprocess.run(
        [
            sys.executable,
            "scripts/summarize_simulation_report.py",
            str(report_path),
            "--min-cycles",
            "2",
            "--min-pass-rate",
            "1.0",
            "--max-failed-cycles",
            "0",
            "--forbid-price-source-prefix",
            "static_fallback",
            "--require-all-price-source",
            "binance_usdm_public_ticker",
        ],
        check=True,
        capture_output=True,
        encoding="utf-8",
    )

    assert json.loads(result.stdout)["price_sources"] == ["binance_usdm_public_ticker"]
