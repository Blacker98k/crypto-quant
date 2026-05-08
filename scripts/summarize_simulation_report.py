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
    parser.add_argument(
        "--min-pass-rate",
        type=float,
        default=None,
        help="Exit non-zero when the report pass rate is below this threshold.",
    )
    parser.add_argument(
        "--min-cycles",
        type=int,
        default=None,
        help="Exit non-zero when the report has fewer cycles than this threshold.",
    )
    parser.add_argument(
        "--max-failed-cycles",
        type=int,
        default=None,
        help="Exit non-zero when failed cycle count exceeds this threshold.",
    )
    parser.add_argument(
        "--require-price-source",
        action="append",
        default=[],
        help="Exit non-zero unless the summary includes this price source. Repeatable.",
    )
    parser.add_argument(
        "--forbid-price-source-prefix",
        action="append",
        default=[],
        help="Exit non-zero if any price source starts with this prefix. Repeatable.",
    )
    parser.add_argument(
        "--require-all-price-source",
        default=None,
        help="Exit non-zero unless every cycle used this exact price source.",
    )
    parser.add_argument(
        "--require-symbol",
        action="append",
        default=[],
        help="Exit non-zero unless the summary includes this symbol. Repeatable.",
    )
    parser.add_argument(
        "--forbid-reason",
        action="append",
        default=[],
        help="Exit non-zero if any cycle has this reason. Repeatable.",
    )
    return parser.parse_args()


def main() -> None:
    from core.monitor.simulation_report import summarize_simulation_report

    args = _parse_args()
    summary = summarize_simulation_report(args.report)
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))

    failed = False
    cycles = int(summary.get("cycles") or 0)
    failed_cycles = int(summary.get("failed") or 0)
    pass_rate = float(summary.get("pass_rate") or 0)

    if args.min_cycles is not None and cycles < args.min_cycles:
        print(f"cycles {cycles} below required {args.min_cycles}", file=sys.stderr)
        failed = True

    if args.min_pass_rate is not None and pass_rate < args.min_pass_rate:
        print(
            f"pass_rate {pass_rate} below required {args.min_pass_rate}",
            file=sys.stderr,
        )
        failed = True

    if args.max_failed_cycles is not None and failed_cycles > args.max_failed_cycles:
        print(
            f"failed cycles {failed_cycles} above allowed {args.max_failed_cycles}",
            file=sys.stderr,
        )
        failed = True

    price_sources = {str(source) for source in summary.get("price_sources", [])}
    symbols = {str(symbol) for symbol in summary.get("symbols", [])}
    reasons = {str(reason) for reason in summary.get("reasons", [])}
    for required in args.require_price_source:
        if required not in price_sources:
            print(f"missing required price source: {required}", file=sys.stderr)
            failed = True

    for required in args.require_symbol:
        if required not in symbols:
            print(f"missing required symbol: {required}", file=sys.stderr)
            failed = True

    for reason in args.forbid_reason:
        if reason in reasons:
            print(f"forbidden reason matched: {reason}", file=sys.stderr)
            failed = True

    for prefix in args.forbid_price_source_prefix:
        matches = sorted(source for source in price_sources if source.startswith(prefix))
        if matches:
            print(
                f"forbidden price source prefix {prefix} matched: {', '.join(matches)}",
                file=sys.stderr,
            )
            failed = True

    if args.require_all_price_source is not None:
        required_source = str(args.require_all_price_source)
        unexpected_sources = sorted(price_sources - {required_source})
        if unexpected_sources or price_sources != {required_source}:
            if unexpected_sources:
                print(
                    "unexpected price sources: "
                    f"{', '.join(unexpected_sources)}; required only: {required_source}",
                    file=sys.stderr,
                )
            else:
                print(
                    f"missing required price source: {required_source}",
                    file=sys.stderr,
                )
            failed = True

    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
