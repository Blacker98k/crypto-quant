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


__all__ = ["SimulationReportWriter"]
