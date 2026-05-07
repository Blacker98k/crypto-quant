"""测试 SQLite 数据访问：symbols 增删改查 / run_log / strategy_kv。

覆盖 ``core/data/sqlite_repo.py`` 中 ``SqliteRepo`` 所有公开方法。
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

import pytest

from core.data.sqlite_repo import SqliteRepo

# ─── 辅助常量 ──────────────────────────────────────────────────────────────

_MIGRATIONS_DIR = Path(__file__).resolve().parents[2] / "migrations"

# 一份完整的上游 symbol 数据模板，供测试复用
_BTC_ROW: dict[str, Any] = dict(
    exchange="binance",
    symbol="BTCUSDT",
    type="perp",
    base="BTC",
    quote="USDT",
    tick_size=0.1,
    lot_size=0.001,
    min_notional=5.0,
    listed_at=1_500_000_000_000,
    delisted_at=None,
)

_ETH_ROW: dict[str, Any] = dict(
    exchange="binance",
    symbol="ETHUSDT",
    type="perp",
    base="ETH",
    quote="USDT",
    tick_size=0.01,
    lot_size=0.01,
    min_notional=5.0,
    listed_at=1_500_000_000_000,
    delisted_at=None,
)

_SOL_SPOT_ROW: dict[str, Any] = dict(
    exchange="binance",
    symbol="SOLUSDT",
    type="spot",
    base="SOL",
    quote="USDT",
    tick_size=0.001,
    lot_size=0.1,
    min_notional=10.0,
    listed_at=1_500_000_000_000,
    delisted_at=None,
)

_OKX_BTC_ROW: dict[str, Any] = dict(
    exchange="okx",
    symbol="BTCUSDT",
    type="perp",
    base="BTC",
    quote="USDT",
    tick_size=0.1,
    lot_size=0.001,
    min_notional=5.0,
    listed_at=1_500_000_000_000,
    delisted_at=None,
)


# ─── fixtures ──────────────────────────────────────────────────────────────


def _read_migration_sql(filename: str) -> str:
    """读取迁移 SQL 文件内容。"""
    path = _MIGRATIONS_DIR / filename
    if not path.exists():
        raise FileNotFoundError(f"迁移文件不存在: {path}")
    return path.read_text(encoding="utf-8")


def _execute_migration(conn: sqlite3.Connection, sql: str) -> None:
    """执行一段可能含多语句的迁移 SQL。"""
    # 使用 executescript 支持多语句
    conn.executescript(sql)


@pytest.fixture
def db_conn() -> sqlite3.Connection:
    """创建 :memory: SQLite 连接，启用 WAL + foreign_keys，并执行 V1 / V2 迁移。

    ``row_factory = sqlite3.Row`` 是必需的，否则 ``SqliteRepo`` 内部 ``dict(row)`` 会失败。
    """
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    # 执行迁移脚本（幂等：IF NOT EXISTS 保证可重复跑）
    _execute_migration(conn, _read_migration_sql("V1__init.sql"))
    _execute_migration(conn, _read_migration_sql("V2__core_tables.sql"))
    return conn


@pytest.fixture
def empty_repo(db_conn: sqlite3.Connection) -> SqliteRepo:
    """空库的 SqliteRepo 实例。"""
    return SqliteRepo(db_conn)


@pytest.fixture
def repo_with_btc(empty_repo: SqliteRepo) -> SqliteRepo:
    """预写入一条 BTCUSDT symbol 的 SqliteRepo。"""
    empty_repo.upsert_symbols([_BTC_ROW])
    return empty_repo


# ─── upsert_symbols ───────────────────────────────────────────────────────


def test_upsert_symbols_insert_new(db_conn: sqlite3.Connection, empty_repo: SqliteRepo) -> None:
    """插入新 symbol → 表中能查到对应记录。"""
    count = empty_repo.upsert_symbols([_BTC_ROW])
    assert count == 1

    row = db_conn.execute(
        "SELECT * FROM symbols WHERE exchange=? AND symbol=? AND type=?",
        ("binance", "BTCUSDT", "perp"),
    ).fetchone()
    assert row is not None
    assert row["symbol"] == "BTCUSDT"
    assert row["type"] == "perp"


def test_upsert_symbols_update_no_duplicate(
    db_conn: sqlite3.Connection, repo_with_btc: SqliteRepo
) -> None:
    """更新同一 (exchange, symbol, type) 不创建重复行。"""
    updated_row = {**_BTC_ROW, "tick_size": 0.5, "lot_size": 0.002}
    count = repo_with_btc.upsert_symbols([updated_row])
    assert count == 1

    # 确认只有一行
    rows = db_conn.execute(
        "SELECT * FROM symbols WHERE exchange=? AND symbol=? AND type=?",
        ("binance", "BTCUSDT", "perp"),
    ).fetchall()
    assert len(rows) == 1
    # 确认字段被更新
    assert rows[0]["tick_size"] == 0.5
    assert rows[0]["lot_size"] == 0.002


def test_upsert_symbols_batch_insert(db_conn: sqlite3.Connection, empty_repo: SqliteRepo) -> None:
    """批量插入多条 symbol。"""
    count = empty_repo.upsert_symbols([_BTC_ROW, _ETH_ROW, _SOL_SPOT_ROW])
    assert count == 3
    total = db_conn.execute("SELECT COUNT(*) FROM symbols").fetchone()[0]
    assert total == 3


def test_upsert_symbols_empty_list_returns_zero(empty_repo: SqliteRepo) -> None:
    """空列表 → 返回 0，不报错。"""
    assert empty_repo.upsert_symbols([]) == 0


# ─── get_symbol ───────────────────────────────────────────────────────────


def test_get_symbol_exists(repo_with_btc: SqliteRepo) -> None:
    """存在时返回 dict。"""
    row = repo_with_btc.get_symbol("BTCUSDT", exchange="binance", stype="perp")
    assert row is not None
    assert row["symbol"] == "BTCUSDT"
    assert row["exchange"] == "binance"


def test_get_symbol_not_found(repo_with_btc: SqliteRepo) -> None:
    """不存在时返回 None。"""
    assert repo_with_btc.get_symbol("ETHUSDT", exchange="binance", stype="perp") is None


# ─── list_symbols ─────────────────────────────────────────────────────────


def test_list_symbols_all(empty_repo: SqliteRepo) -> None:
    """无过滤条件时返回全部 symbol。"""
    empty_repo.upsert_symbols([_BTC_ROW, _ETH_ROW])
    rows = empty_repo.list_symbols()
    assert len(rows) == 2


def test_list_symbols_filter_by_exchange(empty_repo: SqliteRepo) -> None:
    """按 exchange 过滤。"""
    empty_repo.upsert_symbols([_BTC_ROW, _OKX_BTC_ROW])
    binance_rows = empty_repo.list_symbols(exchange="binance")
    assert len(binance_rows) == 1
    assert binance_rows[0]["exchange"] == "binance"

    okx_rows = empty_repo.list_symbols(exchange="okx")
    assert len(okx_rows) == 1
    assert okx_rows[0]["exchange"] == "okx"


def test_list_symbols_filter_by_type(empty_repo: SqliteRepo) -> None:
    """按 type（spot / perp）过滤。"""
    empty_repo.upsert_symbols([_BTC_ROW, _SOL_SPOT_ROW])
    perp_rows = empty_repo.list_symbols(stype="perp")
    assert len(perp_rows) == 1
    assert perp_rows[0]["type"] == "perp"

    spot_rows = empty_repo.list_symbols(stype="spot")
    assert len(spot_rows) == 1
    assert spot_rows[0]["type"] == "spot"


def test_list_symbols_filter_by_universe(empty_repo: SqliteRepo) -> None:
    """按 universe 字段过滤。"""
    empty_repo.upsert_symbols([_BTC_ROW, _ETH_ROW])
    empty_repo.update_universe("BTCUSDT", "binance", "perp", "core")
    empty_repo.update_universe("ETHUSDT", "binance", "perp", "observation")

    core_rows = empty_repo.list_symbols(universe="core")
    assert len(core_rows) == 1
    assert core_rows[0]["symbol"] == "BTCUSDT"

    obs_rows = empty_repo.list_symbols(universe="observation")
    assert len(obs_rows) == 1
    assert obs_rows[0]["symbol"] == "ETHUSDT"


def test_list_symbols_empty_db(empty_repo: SqliteRepo) -> None:
    """空库返回空列表。"""
    assert empty_repo.list_symbols() == []


# ─── update_universe / clear_universe_column ──────────────────────────────


def test_update_universe_set_value(db_conn: sqlite3.Connection, repo_with_btc: SqliteRepo) -> None:
    """设置 universe → 表中对应列被更新。"""
    repo_with_btc.update_universe("BTCUSDT", "binance", "perp", "core")
    row = db_conn.execute(
        "SELECT universe FROM symbols WHERE exchange=? AND symbol=? AND type=?",
        ("binance", "BTCUSDT", "perp"),
    ).fetchone()
    assert row is not None
    assert row["universe"] == "core"


def test_update_universe_set_null(
    db_conn: sqlite3.Connection, repo_with_btc: SqliteRepo
) -> None:
    """universe 设为 None → 列值为 NULL。"""
    repo_with_btc.update_universe("BTCUSDT", "binance", "perp", None)
    row = db_conn.execute(
        "SELECT universe FROM symbols WHERE exchange=? AND symbol=? AND type=?",
        ("binance", "BTCUSDT", "perp"),
    ).fetchone()
    assert row is not None
    assert row["universe"] is None


def test_clear_universe_column(db_conn: sqlite3.Connection, empty_repo: SqliteRepo) -> None:
    """清空所有 symbol 的 universe 列。"""
    empty_repo.upsert_symbols([_BTC_ROW, _ETH_ROW])
    empty_repo.update_universe("BTCUSDT", "binance", "perp", "core")
    empty_repo.update_universe("ETHUSDT", "binance", "perp", "observation")

    empty_repo.clear_universe_column()

    rows = db_conn.execute("SELECT universe FROM symbols WHERE universe IS NOT NULL").fetchall()
    assert len(rows) == 0


# ─── log_run / get_recent_run_log ─────────────────────────────────────────


def test_log_run_and_get_recent(db_conn: sqlite3.Connection, empty_repo: SqliteRepo) -> None:
    """写入 run_log 后 get_recent_run_log 能查到。"""
    empty_repo.log_run(endpoint="GET /api/v4/wallet", status="ok", http_code=200, latency_ms=120)
    empty_repo.log_run(
        endpoint="GET /api/v4/positions", status="fail", http_code=500, latency_ms=5000
    )

    logs = empty_repo.get_recent_run_log(limit=10)
    # 倒序：最新在前
    assert len(logs) == 2
    assert logs[0]["endpoint"] == "GET /api/v4/positions"
    assert logs[0]["status"] == "fail"
    assert logs[1]["endpoint"] == "GET /api/v4/wallet"
    assert logs[1]["status"] == "ok"


def test_get_recent_run_log_since(db_conn: sqlite3.Connection, empty_repo: SqliteRepo) -> None:
    """since_ms 参数能过滤旧记录。"""
    empty_repo.log_run(endpoint="GET /api/v1/ping", status="ok", http_code=200, latency_ms=5)

    # 用远大于 captured_at 的时间戳过滤 → 应该查不到
    logs = empty_repo.get_recent_run_log(limit=10, since_ms=9_999_999_999_999)
    assert len(logs) == 0

    # since_ms=0 不应当过滤掉记录
    logs_all = empty_repo.get_recent_run_log(limit=10, since_ms=0)
    assert len(logs_all) == 1


def test_get_recent_run_log_limit(db_conn: sqlite3.Connection, empty_repo: SqliteRepo) -> None:
    """limit 参数控制返回行数。"""
    for i in range(5):
        empty_repo.log_run(endpoint=f"GET /api/v{i}", status="ok")
    logs = empty_repo.get_recent_run_log(limit=3)
    assert len(logs) == 3


def test_log_run_with_note(db_conn: sqlite3.Connection, empty_repo: SqliteRepo) -> None:
    """带 note 的 run_log 能正确存取。"""
    empty_repo.log_run(
        endpoint="POST /api/v4/order", status="ok", http_code=200, note="limit order placed"
    )
    logs = empty_repo.get_recent_run_log(limit=1)
    assert logs[0]["note"] == "limit order placed"


# ─── kv_get / kv_set / kv_delete ──────────────────────────────────────────


def test_kv_set_and_get(empty_repo: SqliteRepo) -> None:
    """set 后 get 得到相同值。"""
    empty_repo.kv_set("s1_btc_trend", "ma_window", "60")
    val = empty_repo.kv_get("s1_btc_trend", "ma_window")
    assert val == "60"


def test_kv_set_overwrite(empty_repo: SqliteRepo) -> None:
    """对同一 key 再次 set 覆盖旧值。"""
    empty_repo.kv_set("s1", "risk_level", "low")
    empty_repo.kv_set("s1", "risk_level", "high")
    val = empty_repo.kv_get("s1", "risk_level")
    assert val == "high"


def test_kv_get_nonexistent(empty_repo: SqliteRepo) -> None:
    """不存在的 key → None。"""
    assert empty_repo.kv_get("nonexistent", "key") is None


def test_kv_delete_existing(empty_repo: SqliteRepo) -> None:
    """删除存在的记录 → 返回 True，再 get 返回 None。"""
    empty_repo.kv_set("s1", "to_delete", "{}")
    deleted = empty_repo.kv_delete("s1", "to_delete")
    assert deleted is True
    assert empty_repo.kv_get("s1", "to_delete") is None


def test_kv_delete_nonexistent(empty_repo: SqliteRepo) -> None:
    """删除不存在的记录 → 返回 False。"""
    deleted = empty_repo.kv_delete("s1", "never_set")
    assert deleted is False


def test_kv_multiple_strategies(db_conn: sqlite3.Connection, empty_repo: SqliteRepo) -> None:
    """同 key 不同 strategy 互不影响。"""
    empty_repo.kv_set("s1", "window", "10")
    empty_repo.kv_set("s2", "window", "20")

    assert empty_repo.kv_get("s1", "window") == "10"
    assert empty_repo.kv_get("s2", "window") == "20"

    # DB 中有两条记录
    rows = db_conn.execute("SELECT * FROM strategy_kv WHERE key='window'").fetchall()
    assert len(rows) == 2


# ─── 集成：完整 CRUD 流程 ──────────────────────────────────────────────────


def test_full_crud_flow(empty_repo: SqliteRepo) -> None:
    """模拟一次完整数据操作：写入 symbol → 查 → 更新 → 确认唯一。"""
    # 写入
    empty_repo.upsert_symbols([_BTC_ROW, _ETH_ROW])

    # 查单个
    btc = empty_repo.get_symbol("BTCUSDT")
    assert btc is not None
    assert btc["base"] == "BTC"

    # 查列表
    all_rows = empty_repo.list_symbols()
    assert len(all_rows) == 2

    # 更新 universe
    empty_repo.update_universe("BTCUSDT", "binance", "perp", "core")
    btc2 = empty_repo.get_symbol("BTCUSDT")
    assert btc2 is not None
    assert btc2["universe"] == "core"

    # 按 universe 过滤
    core_rows = empty_repo.list_symbols(universe="core")
    assert len(core_rows) == 1

    # 写 run_log
    empty_repo.log_run(endpoint="test", status="ok")
    logs = empty_repo.get_recent_run_log(limit=1)
    assert len(logs) == 1
    assert logs[0]["status"] == "ok"

    # 写/读 KV
    empty_repo.kv_set("s1", "state", '{"ready": true}')
    assert empty_repo.kv_get("s1", "state") == '{"ready": true}'


def test_close_releases_connection(empty_repo: SqliteRepo) -> None:
    empty_repo.close()

    with pytest.raises(sqlite3.ProgrammingError):
        empty_repo.list_symbols()
