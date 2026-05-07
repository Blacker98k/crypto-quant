#!/usr/bin/env python3
"""Probe public exchange market data and record run_log health."""

from __future__ import annotations

import argparse
import asyncio
import json
import sqlite3
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=Path("data/crypto.sqlite"))
    parser.add_argument("--market", choices=["spot", "perp"], default="perp")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--timeframe", default="1m")
    parser.add_argument("--require-kline", action="store_true")
    parser.add_argument("--proxy", default="")
    parser.add_argument("--timeout-ms", type=int, default=30_000)
    return parser.parse_args(argv)


def _open_repo(db_path: Path):
    from core.data.sqlite_repo import SqliteRepo
    from core.db.migration_runner import MigrationRunner

    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    MigrationRunner(migrations_dir=Path("migrations")).apply_all(conn)
    return conn, SqliteRepo(conn)


async def _run(argv: list[str] | None = None) -> int:
    from core.monitor.market_health import probe_market_data

    args = _parse_args(argv)
    conn, repo = _open_repo(args.db)
    if args.market == "spot":
        from core.data.exchange.binance_spot import BinanceSpotAdapter

        exchange = BinanceSpotAdapter(proxy=args.proxy, timeout_ms=args.timeout_ms)
        endpoint = "binance_spot_public_ticker"
    else:
        from core.data.exchange.binance_usdm import BinanceUsdmAdapter

        exchange = BinanceUsdmAdapter(proxy=args.proxy, timeout_ms=args.timeout_ms)
        endpoint = "binance_usdm_public_ticker"

    try:
        result = await probe_market_data(
            exchange,
            repo,
            symbol=args.symbol,
            endpoint=endpoint,
            timeframe=args.timeframe,
            require_kline=args.require_kline,
        )
    finally:
        await exchange.close()
        conn.close()

    print(json.dumps(result.as_dict(), ensure_ascii=False, sort_keys=True))
    return 0 if result.status == "ok" else 2


def main() -> None:
    raise SystemExit(asyncio.run(_run()))


if __name__ == "__main__":
    main()
