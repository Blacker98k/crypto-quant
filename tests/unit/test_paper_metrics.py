from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
from pathlib import Path

from core.db.migration_runner import MigrationRunner
from core.monitor.paper_metrics import paper_metrics

_SINCE_MS = 1_700_000_000_000
_UNTIL_MS = 1_700_000_100_000


def _seed_metrics_fixture(conn: sqlite3.Connection) -> None:
    btc_id = _insert_symbol(conn, "BTCUSDT", "BTC")
    eth_id = _insert_symbol(conn, "ETHUSDT", "ETH")
    old_id = _insert_order(conn, btc_id, "old", "buy", "filled", _SINCE_MS - 1, 0.01, 50_000.0)
    buy_id = _insert_order(conn, btc_id, "buy", "buy", "filled", _SINCE_MS + 1, 0.1, 50_000.0)
    sell_id = _insert_order(conn, eth_id, "sell", "sell", "filled", _SINCE_MS + 2, 1.0, 3_000.0)
    _insert_order(conn, btc_id, "rejected", "buy", "rejected", _SINCE_MS + 3, 0.2, 49_000.0)
    _insert_fill(conn, old_id, "old-fill", 50_000.0, 0.01, 0.25, _SINCE_MS - 1)
    _insert_fill(conn, buy_id, "buy-fill", 50_000.0, 0.1, 2.5, _SINCE_MS + 4)
    _insert_fill(conn, sell_id, "sell-fill", 3_000.0, 1.0, 1.5, _SINCE_MS + 5)
    _insert_position(conn, btc_id, qty=0.1, current_price=51_000.0, closed_at=None)
    _insert_position(conn, eth_id, qty=1.0, current_price=3_000.0, closed_at=_SINCE_MS + 10)
    _insert_risk_event(conn, "warn", _SINCE_MS + 6)
    _insert_risk_event(conn, "critical", _SINCE_MS + 7)
    _insert_risk_event(conn, "info", _UNTIL_MS + 1)
    conn.commit()


def test_paper_metrics_aggregates_window(tmp_db: sqlite3.Connection) -> None:
    _seed_metrics_fixture(tmp_db)

    payload = paper_metrics(tmp_db, since_ms=_SINCE_MS, until_ms=_UNTIL_MS)

    assert payload == {
        "since_ms": _SINCE_MS,
        "until_ms": _UNTIL_MS,
        "orders": {"total": 3, "by_status": {"filled": 2, "rejected": 1}},
        "fills": {
            "total": 2,
            "filled_notional": 8_000.0,
            "buy_notional": 5_000.0,
            "sell_notional": 3_000.0,
            "fees": 4.0,
            "cash_pnl": -2_004.0,
        },
        "risk_events": {"total": 2, "by_severity": {"critical": 1, "warn": 1}},
        "positions": {"open": 1, "open_notional": 5_100.0},
        "symbols": ["BTCUSDT", "ETHUSDT"],
    }


def test_paper_metrics_cli_outputs_json(tmp_path: Path) -> None:
    db_path = tmp_path / "paper.sqlite"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    MigrationRunner(migrations_dir=Path("migrations")).apply_all(conn)
    _seed_metrics_fixture(conn)
    conn.close()

    result = subprocess.run(
        [
            sys.executable,
            "scripts/paper_metrics.py",
            "--db",
            str(db_path),
            "--since-ms",
            str(_SINCE_MS),
            "--until-ms",
            str(_UNTIL_MS),
        ],
        check=True,
        capture_output=True,
        encoding="utf-8",
    )

    payload = json.loads(result.stdout)

    assert payload["orders"] == {"total": 3, "by_status": {"filled": 2, "rejected": 1}}
    assert payload["fills"]["cash_pnl"] == -2_004.0
    assert payload["symbols"] == ["BTCUSDT", "ETHUSDT"]


def _insert_symbol(conn: sqlite3.Connection, symbol: str, base: str) -> int:
    cur = conn.execute(
        "INSERT INTO symbols (exchange, symbol, type, base, quote, tick_size, lot_size, "
        "min_notional, listed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("binance", symbol, "perp", base, "USDT", 0.1, 0.001, 10.0, 1),
    )
    return _lastrowid(cur)


def _insert_order(
    conn: sqlite3.Connection,
    symbol_id: int,
    client_order_id: str,
    side: str,
    status: str,
    placed_at: int,
    quantity: float,
    price: float,
) -> int:
    cur = conn.execute(
        "INSERT INTO orders (client_order_id, symbol_id, side, type, price, quantity, "
        "filled_qty, avg_fill_price, status, purpose, strategy_version, placed_at, updated_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            client_order_id,
            symbol_id,
            side,
            "market",
            price,
            quantity,
            quantity if status == "filled" else 0.0,
            price if status == "filled" else None,
            status,
            "entry",
            "dev",
            placed_at,
            placed_at,
        ),
    )
    return _lastrowid(cur)


def _insert_fill(
    conn: sqlite3.Connection,
    order_id: int,
    exchange_fill_id: str,
    price: float,
    quantity: float,
    fee: float,
    ts: int,
) -> None:
    conn.execute(
        "INSERT INTO fills (order_id, exchange_fill_id, price, quantity, fee, fee_currency, ts) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (order_id, exchange_fill_id, price, quantity, fee, "USDT", ts),
    )


def _insert_position(
    conn: sqlite3.Connection,
    symbol_id: int,
    qty: float,
    current_price: float,
    closed_at: int | None,
) -> None:
    conn.execute(
        "INSERT INTO positions (symbol_id, strategy, strategy_version, side, qty, "
        "avg_entry_price, current_price, realized_pnl, leverage, opened_at, closed_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (symbol_id, "sim", "dev", "long", qty, current_price, current_price, 0.0, 1.0, 1, closed_at),
    )


def _insert_risk_event(conn: sqlite3.Connection, severity: str, captured_at: int) -> None:
    conn.execute(
        "INSERT INTO risk_events (type, severity, source, payload, captured_at) "
        "VALUES (?, ?, ?, ?, ?)",
        ("order_rejected", severity, "L1", "{}", captured_at),
    )


def _lastrowid(cur: sqlite3.Cursor) -> int:
    if cur.lastrowid is None:
        raise AssertionError("expected lastrowid")
    return cur.lastrowid
