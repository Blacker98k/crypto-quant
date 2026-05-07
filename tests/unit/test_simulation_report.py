from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from core.monitor.simulation_report import SimulationReportWriter


def test_report_writer_creates_parent_and_appends_jsonl(tmp_path: Path) -> None:
    report_path = tmp_path / "reports" / "simulations" / "smoke.jsonl"
    writer = SimulationReportWriter(report_path)

    writer.write_cycle({"cycle": 1, "passed": True})
    writer.write_cycle({"cycle": 2, "passed": False, "reason": "fills"})
    writer.close()

    rows = [json.loads(line) for line in report_path.read_text(encoding="utf-8").splitlines()]
    assert rows == [
        {"cycle": 1, "passed": True},
        {"cycle": 2, "passed": False, "reason": "fills"},
    ]


def test_report_writer_can_be_used_as_context_manager(tmp_path: Path) -> None:
    report_path = tmp_path / "smoke.jsonl"

    with SimulationReportWriter(report_path) as writer:
        writer.write_cycle({"cycle": 1, "symbol": "BTCUSDT"})

    assert json.loads(report_path.read_text(encoding="utf-8")) == {
        "cycle": 1,
        "symbol": "BTCUSDT",
    }


def test_report_writer_appends_existing_report(tmp_path: Path) -> None:
    report_path = tmp_path / "smoke.jsonl"
    report_path.write_text('{"cycle":1}\n', encoding="utf-8")

    with SimulationReportWriter(report_path) as writer:
        writer.write_cycle({"cycle": 2})

    assert report_path.read_text(encoding="utf-8").splitlines() == [
        '{"cycle":1}',
        '{"cycle":2}',
    ]


def test_simulate_paper_cli_writes_report(tmp_path: Path) -> None:
    db_path = tmp_path / "sim.sqlite"
    report_path = tmp_path / "reports" / "sim.jsonl"

    result = subprocess.run(
        [
            sys.executable,
            "scripts/simulate_paper.py",
            "--price-source",
            "static",
            "--static-price",
            "50000",
            "--bars",
            "2",
            "--db",
            str(db_path),
            "--report",
            str(report_path),
        ],
        check=True,
        capture_output=True,
        encoding="utf-8",
    )

    stdout_payload = json.loads(result.stdout)
    report_payload = json.loads(report_path.read_text(encoding="utf-8"))

    assert report_payload["cycle"] == stdout_payload["cycle"] == 1
    assert report_payload["passed"] is True
    assert stdout_payload["passed"] is True
    assert report_payload["price_source"] == stdout_payload["price_source"] == "static"
    assert report_payload["result"] == stdout_payload["result"]
