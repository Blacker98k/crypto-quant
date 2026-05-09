from __future__ import annotations

import json
import sys
from pathlib import Path

from core.monitor.paper_readiness import (
    PaperReadinessConfig,
    build_readiness_steps,
    run_paper_readiness,
)


def test_build_readiness_steps_include_real_market_and_strict_source_gates() -> None:
    config = PaperReadinessConfig(
        symbol="ETHUSDT",
        db_path=Path("data/readiness.sqlite"),
        report_path=Path("reports/readiness.jsonl"),
        summary_path=Path("reports/readiness-summary.json"),
        proxy="http://127.0.0.1:57777",
        bars=8,
        cycles=3,
        interval_sec=1.0,
        require_kline=True,
    )

    steps = build_readiness_steps(config)

    assert [step.name for step in steps] == [
        "market_health",
        "simulate_paper",
        "strict_report_gate",
    ]
    assert steps[0].command == [
        sys.executable,
        "scripts/market_health.py",
        "--db",
        "data/readiness.sqlite",
        "--market",
        "perp",
        "--symbol",
        "ETHUSDT",
        "--timeframe",
        "1m",
        "--proxy",
        "http://127.0.0.1:57777",
        "--require-kline",
    ]
    assert "--price-source" in steps[1].command
    assert "live" in steps[1].command
    assert steps[2].command[-4:] == [
        "--require-all-price-source",
        "binance_usdm_public_ticker",
        "--forbid-price-source-prefix",
        "static_fallback",
    ]


def test_run_paper_readiness_stops_after_failed_step() -> None:
    calls: list[list[str]] = []

    def runner(command: list[str]):
        calls.append(command)
        return 2, '{"status":"fail","endpoint":"binance_usdm_public_ticker"}\n', "timeout\n"

    result = run_paper_readiness(PaperReadinessConfig(), runner=runner)

    assert result["status"] == "fail"
    assert result["failed_step"] == "market_health"
    assert len(result["steps"]) == 1
    assert len(calls) == 1
    assert result["steps"][0]["parsed_stdout"] == {
        "status": "fail",
        "endpoint": "binance_usdm_public_ticker",
    }


def test_run_paper_readiness_resets_stale_report_artifacts(tmp_path: Path) -> None:
    report_path = tmp_path / "stale.jsonl"
    summary_path = tmp_path / "stale-summary.json"
    report_path.write_text('{"price_source":"static_fallback:TimeoutError"}\n', encoding="utf-8")
    summary_path.write_text('{"cycles":99}\n', encoding="utf-8")

    def runner(command: list[str]):
        assert not report_path.exists()
        assert not summary_path.exists()
        return 2, '{"status":"fail"}\n', ""

    result = run_paper_readiness(
        PaperReadinessConfig(report_path=report_path, summary_path=summary_path),
        runner=runner,
    )

    assert result["status"] == "fail"
    assert result["failed_step"] == "market_health"


def test_run_paper_readiness_returns_ok_with_step_json() -> None:
    outputs = [
        '{"status":"ok","endpoint":"binance_usdm_public_ticker"}\n',
        '{"cycle":1,"passed":true,"price_source":"binance_usdm_public_ticker"}\n'
        '{"cycle":2,"passed":true,"price_source":"binance_usdm_public_ticker"}\n',
        json.dumps(
            {
                "cycles": 2,
                "passed": 2,
                "failed": 0,
                "pass_rate": 1.0,
                "price_sources": ["binance_usdm_public_ticker"],
            }
        )
        + "\n",
    ]

    def runner(command: list[str]):
        return 0, outputs.pop(0), ""

    result = run_paper_readiness(
        PaperReadinessConfig(cycles=2, bars=4),
        runner=runner,
    )

    assert result["status"] == "ok"
    assert result["failed_step"] is None
    assert [step["returncode"] for step in result["steps"]] == [0, 0, 0]
    assert result["steps"][1]["parsed_stdout"] == {
        "cycle": 2,
        "passed": True,
        "price_source": "binance_usdm_public_ticker",
    }
    assert result["steps"][2]["parsed_stdout"]["pass_rate"] == 1.0
