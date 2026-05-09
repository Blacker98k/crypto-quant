"""Run the small-live readiness gate without placing real orders."""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import fields
from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.live import PaperStatus, SmallLiveConfig, evaluate_small_live_readiness  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, help="Path to non-secret small-live YAML config")
    parser.add_argument(
        "--paper-status-json",
        required=True,
        help="Path to paper dashboard status JSON captured from /api/status",
    )
    args = parser.parse_args(argv)

    config = _load_dataclass(SmallLiveConfig, Path(args.config), loader=_load_yaml)
    paper_status = _load_dataclass(PaperStatus, Path(args.paper_status_json), loader=_load_json)
    report = evaluate_small_live_readiness(config, paper_status, env=os.environ)
    print(json.dumps(report.as_dict(), ensure_ascii=False, indent=2))
    return 0 if report.ready else 2


def _load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise SystemExit(f"config must be a mapping: {path}")
    return data


def _load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8-sig") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise SystemExit(f"paper status must be an object: {path}")
    return data


def _load_dataclass(cls: type, path: Path, *, loader) -> Any:
    data = loader(path)
    allowed = {field.name for field in fields(cls)}
    kwargs = {key: value for key, value in data.items() if key in allowed}
    if "allowed_symbols" in kwargs and isinstance(kwargs["allowed_symbols"], list):
        kwargs["allowed_symbols"] = tuple(str(item) for item in kwargs["allowed_symbols"])
    return cls(**kwargs)


if __name__ == "__main__":
    raise SystemExit(main())
