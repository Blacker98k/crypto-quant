"""SQLite 数据访问封装。"""

from __future__ import annotations

import sqlite3
import time
from typing import Any


class SqliteRepo:
    """封装项目内 SQLite 表的常用 CRUD。"""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def upsert_symbols(self, rows: list[dict[str, Any]]) -> int:
        """批量 upsert symbols。"""
        if not rows:
            return 0
        sql = """
        INSERT INTO symbols (
            exchange, symbol, type, base, quote, universe, tick_size, lot_size,
            min_notional, listed_at, delisted_at
        ) VALUES (
            :exchange, :symbol, :type, :base, :quote, :universe, :tick_size, :lot_size,
            :min_notional, :listed_at, :delisted_at
        )
        ON CONFLICT(exchange, symbol, type) DO UPDATE SET
            base=excluded.base,
            quote=excluded.quote,
            universe=excluded.universe,
            tick_size=excluded.tick_size,
            lot_size=excluded.lot_size,
            min_notional=excluded.min_notional,
            listed_at=excluded.listed_at,
            delisted_at=excluded.delisted_at
        """
        normalized = [self._symbol_row_defaults(row) for row in rows]
        self._conn.executemany(sql, normalized)
        self._conn.commit()
        return len(rows)

    def get_symbol(
        self, symbol: str, exchange: str = "binance", stype: str = "perp"
    ) -> dict[str, Any] | None:
        """读取单个 symbol。"""
        row = self._conn.execute(
            "SELECT * FROM symbols WHERE exchange=? AND symbol=? AND type=?",
            (exchange, symbol, stype),
        ).fetchone()
        if row is None and stype == "perp":
            row = self._conn.execute(
                "SELECT * FROM symbols WHERE symbol=? ORDER BY id LIMIT 1", (symbol,)
            ).fetchone()
        return self._to_dict(row)

    def get_symbol_by_id(self, symbol_id: int) -> dict[str, Any] | None:
        """按 id 读取 symbol。"""
        row = self._conn.execute("SELECT * FROM symbols WHERE id=?", (symbol_id,)).fetchone()
        return self._to_dict(row)

    def list_symbols(
        self,
        exchange: str | None = None,
        stype: str | None = None,
        universe: str | None = None,
    ) -> list[dict[str, Any]]:
        """按条件列出 symbols。"""
        clauses: list[str] = []
        params: list[Any] = []
        if exchange is not None:
            clauses.append("exchange=?")
            params.append(exchange)
        if stype is not None:
            clauses.append("type=?")
            params.append(stype)
        if universe is not None:
            clauses.append("universe=?")
            params.append(universe)
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self._conn.execute(f"SELECT * FROM symbols{where} ORDER BY symbol", params).fetchall()
        return [dict(row) for row in rows]

    def update_universe(
        self, symbol: str, exchange: str, stype: str, universe: str | None
    ) -> None:
        """更新 symbol 的 universe 字段。"""
        self._conn.execute(
            "UPDATE symbols SET universe=? WHERE exchange=? AND symbol=? AND type=?",
            (universe, exchange, symbol, stype),
        )
        self._conn.commit()

    def clear_universe_column(self) -> None:
        """清空 symbols.universe。"""
        self._conn.execute("UPDATE symbols SET universe=NULL")
        self._conn.commit()

    def log_run(
        self,
        endpoint: str,
        status: str,
        http_code: int | None = None,
        latency_ms: int | None = None,
        note: str | None = None,
    ) -> int:
        """写 run_log。"""
        now = int(time.time() * 1000)
        cur = self._conn.execute(
            "INSERT INTO run_log (endpoint, status, http_code, latency_ms, note, captured_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (endpoint, status, http_code, latency_ms, note, now),
        )
        self._conn.commit()
        return self._lastrowid(cur)

    def get_recent_run_log(
        self, limit: int = 100, since_ms: int | None = None
    ) -> list[dict[str, Any]]:
        """读取最近 run_log。"""
        if since_ms is None:
            rows = self._conn.execute(
                "SELECT * FROM run_log ORDER BY captured_at DESC, id DESC LIMIT ?", (limit,)
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT * FROM run_log WHERE captured_at >= ? ORDER BY captured_at DESC, id DESC LIMIT ?",
                (since_ms, limit),
            ).fetchall()
        return [dict(row) for row in rows]

    def kv_set(self, strategy: str, key: str, value_json: str) -> None:
        """写 strategy_kv。"""
        now = int(time.time() * 1000)
        self._conn.execute(
            "INSERT INTO strategy_kv (strategy, key, value_json, updated_at) VALUES (?, ?, ?, ?) "
            "ON CONFLICT(strategy, key) DO UPDATE SET value_json=excluded.value_json, updated_at=excluded.updated_at",
            (strategy, key, value_json, now),
        )
        self._conn.commit()

    def kv_get(self, strategy: str, key: str) -> str | None:
        """读 strategy_kv。"""
        row = self._conn.execute(
            "SELECT value_json FROM strategy_kv WHERE strategy=? AND key=?", (strategy, key)
        ).fetchone()
        return None if row is None else str(row["value_json"])

    def kv_delete(self, strategy: str, key: str) -> bool:
        """删除 strategy_kv。"""
        cur = self._conn.execute(
            "DELETE FROM strategy_kv WHERE strategy=? AND key=?", (strategy, key)
        )
        self._conn.commit()
        return cur.rowcount > 0

    def insert_order(self, row: dict[str, Any]) -> int:
        """插入 orders 行。"""
        keys = list(row)
        placeholders = ", ".join("?" for _ in keys)
        cur = self._conn.execute(
            f"INSERT INTO orders ({', '.join(keys)}) VALUES ({placeholders})",
            [row[k] for k in keys],
        )
        self._conn.commit()
        return self._lastrowid(cur)

    def update_order(self, order_id: int, changes: dict[str, Any]) -> None:
        """更新 orders 行。"""
        sets = ", ".join(f"{key}=?" for key in changes)
        self._conn.execute(
            f"UPDATE orders SET {sets} WHERE id=?", [*changes.values(), order_id]
        )
        self._conn.commit()

    def get_order(self, client_order_id: str) -> dict[str, Any] | None:
        """按 client_order_id 读取订单。"""
        row = self._conn.execute(
            "SELECT * FROM orders WHERE client_order_id=?", (client_order_id,)
        ).fetchone()
        return self._to_dict(row)

    def get_order_by_id(self, order_id: int) -> dict[str, Any] | None:
        """按 id 读取订单。"""
        row = self._conn.execute("SELECT * FROM orders WHERE id=?", (order_id,)).fetchone()
        return self._to_dict(row)

    def get_open_orders(self) -> list[dict[str, Any]]:
        """读取未终态订单。"""
        rows = self._conn.execute(
            "SELECT * FROM orders WHERE status IN ('new', 'accepted', 'partial') ORDER BY placed_at"
        ).fetchall()
        return [dict(row) for row in rows]

    def insert_fill(self, row: dict[str, Any]) -> int:
        """插入 fills 行。"""
        keys = list(row)
        placeholders = ", ".join("?" for _ in keys)
        cur = self._conn.execute(
            f"INSERT INTO fills ({', '.join(keys)}) VALUES ({placeholders})",
            [row[k] for k in keys],
        )
        self._conn.commit()
        return self._lastrowid(cur)

    def get_fills(self, order_id: int) -> list[dict[str, Any]]:
        """读取某订单成交。"""
        rows = self._conn.execute(
            "SELECT * FROM fills WHERE order_id=? ORDER BY ts, id", (order_id,)
        ).fetchall()
        return [dict(row) for row in rows]

    @staticmethod
    def _to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
        return None if row is None else dict(row)

    @staticmethod
    def _lastrowid(cur: sqlite3.Cursor) -> int:
        if cur.lastrowid is None:
            raise RuntimeError("SQLite insert did not return lastrowid")
        return cur.lastrowid

    @staticmethod
    def _symbol_row_defaults(row: dict[str, Any]) -> dict[str, Any]:
        return {
            "exchange": row["exchange"],
            "symbol": row["symbol"],
            "type": row["type"],
            "base": row["base"],
            "quote": row["quote"],
            "universe": row.get("universe"),
            "tick_size": row["tick_size"],
            "lot_size": row["lot_size"],
            "min_notional": row["min_notional"],
            "listed_at": row["listed_at"],
            "delisted_at": row.get("delisted_at"),
        }


__all__ = ["SqliteRepo"]
