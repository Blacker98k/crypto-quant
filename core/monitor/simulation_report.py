"""Helpers for durable simulation run reports."""

from __future__ import annotations

import json
from pathlib import Path
from types import TracebackType
from typing import Any, Self, TextIO


class SimulationReportWriter:
    """Append per-cycle simulation payloads as JSON Lines."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._file: TextIO | None = None

    def __enter__(self) -> Self:
        self._ensure_open()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    def write_cycle(self, payload: dict[str, Any]) -> None:
        handle = self._ensure_open()
        handle.write(json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
        handle.write("\n")
        handle.flush()

    def close(self) -> None:
        if self._file is not None:
            self._file.close()
            self._file = None

    def _ensure_open(self) -> TextIO:
        if self._file is None:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            self._file = self._path.open("a", encoding="utf-8")
        return self._file


def summarize_simulation_cycles(rows: list[dict[str, Any]]) -> dict[str, Any]:
    passed = sum(1 for row in rows if row.get("passed") is True)
    totals: dict[str, int | float] = {}
    for row in rows:
        result = row.get("result")
        if not isinstance(result, dict):
            continue
        for key, value in result.items():
            if isinstance(value, int | float):
                totals[key] = totals.get(key, 0) + value
    cycles = len(rows)
    summary = {
        "cycles": cycles,
        "passed": passed,
        "failed": cycles - passed,
        "pass_rate": round(passed / cycles, 4) if cycles else 0.0,
        "symbols": sorted({str(row["symbol"]) for row in rows if "symbol" in row}),
        "price_sources": sorted(
            {str(row["price_source"]) for row in rows if "price_source" in row}
        ),
        "totals": totals,
    }
    reasons = sorted({str(row["reason"]) for row in rows if "reason" in row})
    if reasons:
        summary["reasons"] = reasons
    return summary


def summarize_simulation_report(path: Path) -> dict[str, Any]:
    rows = [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    return summarize_simulation_cycles(rows)


__all__ = [
    "SimulationReportWriter",
    "summarize_simulation_cycles",
    "summarize_simulation_report",
]
