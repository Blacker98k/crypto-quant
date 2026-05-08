"""Read-only paper-trading metrics from the SQLite production tables."""

from __future__ import annotations

import sqlite3
from typing import Any


def paper_metrics(
    conn: sqlite3.Connection,
    *,
    since_ms: int,
    until_ms: int,
) -> dict[str, Any]:
    """Aggregate paper-trading activity in a half-open time window."""
    return {
        "since_ms": since_ms,
        "until_ms": until_ms,
        "orders": _order_metrics(conn, since_ms, until_ms),
        "fills": _fill_metrics(conn, since_ms, until_ms),
        "risk_events": _risk_event_metrics(conn, since_ms, until_ms),
        "positions": _position_metrics(conn),
        "symbols": _symbols(conn, since_ms, until_ms),
    }


def _order_metrics(conn: sqlite3.Connection, since_ms: int, until_ms: int) -> dict[str, Any]:
    rows = conn.execute(
        "SELECT status, COUNT(*) AS n FROM orders "
        "WHERE placed_at >= ? AND placed_at < ? GROUP BY status ORDER BY status",
        (since_ms, until_ms),
    ).fetchall()
    by_status = {str(row["status"]): int(row["n"]) for row in rows}
    return {"total": sum(by_status.values()), "by_status": by_status}


def _fill_metrics(conn: sqlite3.Connection, since_ms: int, until_ms: int) -> dict[str, Any]:
    rows = conn.execute(
        "SELECT f.price, f.quantity, f.fee, o.side FROM fills f "
        "JOIN orders o ON f.order_id = o.id WHERE f.ts >= ? AND f.ts < ?",
        (since_ms, until_ms),
    ).fetchall()
    buy_notional = 0.0
    sell_notional = 0.0
    fees = 0.0
    for row in rows:
        notional = float(row["price"]) * float(row["quantity"])
        fees += float(row["fee"] or 0.0)
        if row["side"] == "buy":
            buy_notional += notional
        elif row["side"] == "sell":
            sell_notional += notional
    filled_notional = buy_notional + sell_notional
    net_cash_flow = sell_notional - buy_notional - fees
    realized_pnl = _realized_pnl_from_fills(conn, since_ms, until_ms)
    return {
        "total": len(rows),
        "filled_notional": _round_money(filled_notional),
        "buy_notional": _round_money(buy_notional),
        "sell_notional": _round_money(sell_notional),
        "fees": _round_money(fees),
        "net_cash_flow": _round_money(net_cash_flow),
        "cash_pnl": _round_money(realized_pnl - fees),
        "realized_pnl": _round_money(realized_pnl - fees),
    }


def _realized_pnl_from_fills(conn: sqlite3.Connection, since_ms: int, until_ms: int) -> float:
    rows = conn.execute(
        "SELECT f.id, f.price, f.quantity, f.ts, o.side, o.strategy_version, s.symbol "
        "FROM fills f JOIN orders o ON f.order_id = o.id "
        "LEFT JOIN symbols s ON o.symbol_id = s.id "
        "WHERE f.ts < ? ORDER BY f.ts, f.id",
        (until_ms,),
    ).fetchall()
    positions: dict[tuple[str, str], dict[str, float | str | None]] = {}
    realized = 0.0
    for row in rows:
        qty = float(row["quantity"])
        price = float(row["price"])
        trade_side = "long" if row["side"] == "buy" else "short"
        key = (str(row["symbol"] or "?"), str(row["strategy_version"] or ""))
        current = positions.setdefault(key, {"side": None, "qty": 0.0, "avg": 0.0})
        current_side = current["side"]
        current_qty = float(current["qty"] or 0.0)
        current_avg = float(current["avg"] or 0.0)

        if current_side is None or current_qty <= 0:
            current.update({"side": trade_side, "qty": qty, "avg": price})
            continue
        if current_side == trade_side:
            new_qty = current_qty + qty
            current.update(
                {
                    "qty": new_qty,
                    "avg": ((current_qty * current_avg) + (qty * price)) / new_qty,
                }
            )
            continue

        closing_qty = min(current_qty, qty)
        if since_ms <= int(row["ts"]) < until_ms:
            if current_side == "long":
                realized += (price - current_avg) * closing_qty
            else:
                realized += (current_avg - price) * closing_qty

        remaining_position_qty = current_qty - closing_qty
        excess_trade_qty = qty - closing_qty
        if remaining_position_qty > 1e-12:
            current.update({"qty": remaining_position_qty})
        elif excess_trade_qty > 1e-12:
            current.update({"side": trade_side, "qty": excess_trade_qty, "avg": price})
        else:
            current.update({"side": None, "qty": 0.0, "avg": 0.0})
    return realized


def _risk_event_metrics(conn: sqlite3.Connection, since_ms: int, until_ms: int) -> dict[str, Any]:
    rows = conn.execute(
        "SELECT severity, COUNT(*) AS n FROM risk_events "
        "WHERE captured_at >= ? AND captured_at < ? "
        "AND NOT (type = 'paper_signal_skipped' "
        "AND (payload LIKE '%\"reason\": \"cooldown\"%' OR payload LIKE '%\"reason\":\"cooldown\"%' "
        "OR payload LIKE '%\"reason\": \"symbol_order_cap\"%' "
        "OR payload LIKE '%\"reason\":\"symbol_order_cap\"%')) "
        "GROUP BY severity ORDER BY severity",
        (since_ms, until_ms),
    ).fetchall()
    by_severity = {str(row["severity"]): int(row["n"]) for row in rows}
    return {"total": sum(by_severity.values()), "by_severity": by_severity}


def _position_metrics(conn: sqlite3.Connection) -> dict[str, Any]:
    rows = conn.execute(
        "SELECT qty, current_price, avg_entry_price FROM positions WHERE closed_at IS NULL"
    ).fetchall()
    open_notional = 0.0
    for row in rows:
        price = row["current_price"] if row["current_price"] is not None else row["avg_entry_price"]
        open_notional += abs(float(row["qty"]) * float(price))
    return {"open": len(rows), "open_notional": _round_money(open_notional)}


def _symbols(conn: sqlite3.Connection, since_ms: int, until_ms: int) -> list[str]:
    rows = conn.execute(
        "SELECT DISTINCT s.symbol FROM orders o "
        "LEFT JOIN symbols s ON o.symbol_id = s.id "
        "WHERE o.placed_at >= ? AND o.placed_at < ? AND s.symbol IS NOT NULL "
        "UNION "
        "SELECT DISTINCT s.symbol FROM fills f "
        "JOIN orders o ON f.order_id = o.id "
        "LEFT JOIN symbols s ON o.symbol_id = s.id "
        "WHERE f.ts >= ? AND f.ts < ? AND s.symbol IS NOT NULL "
        "ORDER BY symbol",
        (since_ms, until_ms, since_ms, until_ms),
    ).fetchall()
    return [str(row["symbol"]) for row in rows]


def _round_money(value: float) -> float:
    return round(value, 8)


__all__ = ["paper_metrics"]
