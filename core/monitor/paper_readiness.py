"""Paper-readiness orchestration for public data and simulated execution."""

from __future__ import annotations

import json
import subprocess
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import TypeAlias

Runner: TypeAlias = Callable[[list[str]], tuple[int, str, str]]


@dataclass(frozen=True, slots=True)
class PaperReadinessConfig:
    symbol: str = "BTCUSDT"
    market: str = "perp"
    timeframe: str = "1m"
    db_path: Path = Path("data/paper-readiness.sqlite")
    report_path: Path = Path("reports/simulations/paper-readiness.jsonl")
    summary_path: Path = Path("reports/simulations/paper-readiness-summary.json")
    proxy: str = ""
    bars: int = 16
    cycles: int = 3
    interval_sec: float = 1.0
    require_kline: bool = True


@dataclass(frozen=True, slots=True)
class ReadinessStep:
    name: str
    command: list[str]


def build_readiness_steps(config: PaperReadinessConfig) -> list[ReadinessStep]:
    market_health = [
        sys.executable,
        "scripts/market_health.py",
        "--db",
        _path_arg(config.db_path),
        "--market",
        config.market,
        "--symbol",
        config.symbol,
        "--timeframe",
        config.timeframe,
        "--proxy",
        config.proxy,
    ]
    if config.require_kline:
        market_health.append("--require-kline")

    simulate_paper = [
        sys.executable,
        "scripts/simulate_paper.py",
        "--price-source",
        "live",
        "--symbol",
        config.symbol,
        "--bars",
        str(config.bars),
        "--cycles",
        str(config.cycles),
        "--interval-sec",
        _format_float(config.interval_sec),
        "--db",
        _path_arg(config.db_path),
        "--report",
        _path_arg(config.report_path),
        "--summary",
        _path_arg(config.summary_path),
        "--proxy",
        config.proxy,
    ]

    strict_report_gate = [
        sys.executable,
        "scripts/summarize_simulation_report.py",
        _path_arg(config.report_path),
        "--min-cycles",
        str(config.cycles),
        "--min-pass-rate",
        "1.0",
        "--max-failed-cycles",
        "0",
        "--require-all-price-source",
        "binance_usdm_public_ticker",
        "--forbid-price-source-prefix",
        "static_fallback",
    ]

    return [
        ReadinessStep("market_health", market_health),
        ReadinessStep("simulate_paper", simulate_paper),
        ReadinessStep("strict_report_gate", strict_report_gate),
    ]


def run_paper_readiness(
    config: PaperReadinessConfig,
    *,
    runner: Runner | None = None,
) -> dict[str, object]:
    run_command = _run_command if runner is None else runner
    step_results: list[dict[str, object]] = []
    failed_step: str | None = None

    for step in build_readiness_steps(config):
        returncode, stdout, stderr = run_command(step.command)
        step_result = {
            "name": step.name,
            "command": step.command,
            "returncode": returncode,
            "stdout": stdout,
            "stderr": stderr,
            "parsed_stdout": _parse_last_json_line(stdout),
        }
        step_results.append(step_result)
        if returncode != 0:
            failed_step = step.name
            break

    return {
        "status": "fail" if failed_step else "ok",
        "failed_step": failed_step,
        "symbol": config.symbol,
        "cycles": config.cycles,
        "report": _path_arg(config.report_path),
        "summary": _path_arg(config.summary_path),
        "steps": step_results,
    }


def _run_command(command: list[str]) -> tuple[int, str, str]:
    completed = subprocess.run(
        command,
        check=False,
        capture_output=True,
        encoding="utf-8",
    )
    return completed.returncode, completed.stdout, completed.stderr


def _parse_last_json_line(stdout: str) -> object | None:
    for line in reversed(stdout.splitlines()):
        if not line.strip():
            continue
        try:
            payload: object = json.loads(line)
            return payload
        except json.JSONDecodeError:
            return None
    return None


def _format_float(value: float) -> str:
    return str(int(value)) if value.is_integer() else str(value)


def _path_arg(path: Path) -> str:
    return path.as_posix()


__all__ = [
    "PaperReadinessConfig",
    "ReadinessStep",
    "build_readiness_steps",
    "run_paper_readiness",
]
