#!/usr/bin/env python3
"""Print paper-trading metrics for a SQLite database window."""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--since-ms", type=int, required=True)
    parser.add_argument("--until-ms", type=int, default=None)
    return parser.parse_args()


def main() -> None:
    from core.monitor.paper_metrics import paper_metrics

    args = _parse_args()
    until_ms = int(args.until_ms) if args.until_ms is not None else int(time.time() * 1000) + 1
    if args.since_ms >= until_ms:
        raise SystemExit("--since-ms must be before --until-ms")
    conn = sqlite3.connect(str(args.db))
    conn.row_factory = sqlite3.Row
    try:
        payload = paper_metrics(conn, since_ms=int(args.since_ms), until_ms=until_ms)
    finally:
        conn.close()
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
