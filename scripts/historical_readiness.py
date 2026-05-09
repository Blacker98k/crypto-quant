from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run historical paper backtests and strict readiness validation."
    )
    parser.add_argument("--symbols", required=True, help="Comma-separated symbols, e.g. BTCUSDT,ETHUSDT")
    parser.add_argument("--timeframes", required=True, help="Comma-separated timeframes, e.g. 1h,4h")
    parser.add_argument("--data-root", type=Path, default=Path("data/parquet"))
    parser.add_argument("--db", type=Path, default=Path("data/historical-readiness.sqlite"))
    parser.add_argument(
        "--report",
        type=Path,
        default=Path("reports/simulations/historical-readiness.jsonl"),
    )
    parser.add_argument(
        "--summary",
        type=Path,
        default=Path("reports/simulations/historical-readiness-summary.json"),
    )
    parser.add_argument("--start-ms", type=int)
    parser.add_argument("--end-ms", type=int)
    parser.add_argument("--n", type=int)
    parser.add_argument("--min-bars-per-cycle", type=int, required=True)
    parser.add_argument("--min-cycles", type=int)
    parser.add_argument("--max-open-positions-per-cycle", type=int, default=0)
    parser.add_argument("--max-rejected-per-cycle", type=int, default=0)
    parser.add_argument("--max-risk-events-per-cycle", type=int, default=0)
    return parser


def _reset_artifact(path: Path) -> None:
    if not path.exists():
        return
    if path.is_dir():
        raise SystemExit(f"refusing to reset directory artifact: {path}")
    path.unlink()


def _run_json_command(command: list[str]) -> dict[str, Any]:
    result = subprocess.run(command, check=False, capture_output=True, encoding="utf-8")
    if result.returncode != 0:
        sys.stdout.write(result.stdout)
        sys.stderr.write(result.stderr)
        raise SystemExit(result.returncode)

    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        sys.stdout.write(result.stdout)
        sys.stderr.write(result.stderr)
        raise SystemExit(f"command did not emit JSON: {' '.join(command)}") from exc

    if not isinstance(payload, dict):
        raise SystemExit(f"command emitted non-object JSON: {' '.join(command)}")
    return payload


def _append_optional_int(command: list[str], flag: str, value: int | None) -> None:
    if value is not None:
        command.extend([flag, str(value)])


def main() -> int:
    args = _parser().parse_args()
    script_dir = Path(__file__).resolve().parent

    for artifact in (args.db, args.report, args.summary):
        _reset_artifact(artifact)

    backtest_command = [
        sys.executable,
        str(script_dir / "backtest_paper.py"),
        "--symbols",
        args.symbols,
        "--timeframes",
        args.timeframes,
        "--data-root",
        str(args.data_root),
        "--db",
        str(args.db),
        "--report",
        str(args.report),
        "--summary",
        str(args.summary),
        "--min-bars",
        str(args.min_bars_per_cycle),
    ]
    _append_optional_int(backtest_command, "--start-ms", args.start_ms)
    _append_optional_int(backtest_command, "--end-ms", args.end_ms)
    _append_optional_int(backtest_command, "--n", args.n)

    validation_command = [
        sys.executable,
        str(script_dir / "validate_historical_report.py"),
        str(args.report),
        "--symbols",
        args.symbols,
        "--timeframes",
        args.timeframes,
        "--min-bars-per-cycle",
        str(args.min_bars_per_cycle),
        "--max-open-positions-per-cycle",
        str(args.max_open_positions_per_cycle),
        "--max-rejected-per-cycle",
        str(args.max_rejected_per_cycle),
        "--max-risk-events-per-cycle",
        str(args.max_risk_events_per_cycle),
    ]
    _append_optional_int(validation_command, "--min-cycles", args.min_cycles)

    backtest_payload = _run_json_command(backtest_command)
    validation_payload = _run_json_command(validation_command)
    print(
        json.dumps(
            {
                "status": "ok",
                "report": str(args.report),
                "summary": str(args.summary),
                "backtest": backtest_payload,
                "validation": validation_payload,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
