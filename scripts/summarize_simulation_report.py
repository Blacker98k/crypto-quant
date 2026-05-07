#!/usr/bin/env python3
"""Summarize a JSONL paper-simulation report."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path)
    return parser.parse_args()


def main() -> None:
    from core.monitor.simulation_report import summarize_simulation_report

    args = _parse_args()
    print(json.dumps(summarize_simulation_report(args.report), ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
