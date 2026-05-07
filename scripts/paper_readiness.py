#!/usr/bin/env python3
"""Run the public-data and simulated-paper readiness gate."""

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
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--market", choices=["spot", "perp"], default="perp")
    parser.add_argument("--timeframe", default="1m")
    parser.add_argument("--db", type=Path, default=Path("data/paper-readiness.sqlite"))
    parser.add_argument(
        "--report",
        type=Path,
        default=Path("reports/simulations/paper-readiness.jsonl"),
    )
    parser.add_argument(
        "--summary",
        type=Path,
        default=Path("reports/simulations/paper-readiness-summary.json"),
    )
    parser.add_argument("--proxy", default="")
    parser.add_argument("--bars", type=int, default=16)
    parser.add_argument("--cycles", type=int, default=3)
    parser.add_argument("--interval-sec", type=float, default=1.0)
    parser.add_argument("--no-require-kline", action="store_true")
    return parser.parse_args()


def main() -> None:
    from core.monitor.paper_readiness import PaperReadinessConfig, run_paper_readiness

    args = _parse_args()
    result = run_paper_readiness(
        PaperReadinessConfig(
            symbol=args.symbol,
            market=args.market,
            timeframe=args.timeframe,
            db_path=args.db,
            report_path=args.report,
            summary_path=args.summary,
            proxy=args.proxy,
            bars=args.bars,
            cycles=args.cycles,
            interval_sec=args.interval_sec,
            require_kline=not args.no_require_kline,
        )
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    if result["status"] != "ok":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
