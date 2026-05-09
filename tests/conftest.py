"""pytest 全局 fixtures。"""

from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path

import pytest

from core.data.sqlite_repo import SqliteRepo
from core.db.migration_runner import MigrationRunner


@pytest.fixture(scope="function")
def tmp_db() -> sqlite3.Connection:
    """创建临时 SQLite 数据库并运行全部迁移。"""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    runner = MigrationRunner(migrations_dir=Path("migrations"))
    runner.apply_all(conn)
    return conn


@pytest.fixture(scope="function")
def sqlite_repo(tmp_db: sqlite3.Connection) -> SqliteRepo:
    """基于临时数据库的 SqliteRepo。"""
    return SqliteRepo(tmp_db)


@pytest.fixture(scope="function")
def tmp_data_root() -> Path:
    """创建临时 parquet 数据目录。"""
    with tempfile.TemporaryDirectory() as td:
        yield Path(td)


@pytest.fixture(scope="function")
def parquet_io(tmp_data_root: Path):
    """基于临时目录的 ParquetIO。"""
    from core.data.parquet_io import ParquetIO

    return ParquetIO(data_root=tmp_data_root)


@pytest.fixture(scope="function")
def mock_exchange():
    """Mock 现货交易所适配器。"""
    from tests.mocks.exchange import MockExchangeAdapter

    return MockExchangeAdapter()


@pytest.fixture(scope="function")
def sample_bars():
    """生成一组测试用 Bar。"""
    from core.data.exchange.base import Bar

    return [
        Bar(symbol="BTCUSDT", timeframe="1h", ts=1000000, o=50000, h=51000, l=49000, c=50500, v=10, q=505000, closed=True),
        Bar(symbol="BTCUSDT", timeframe="1h", ts=1003600, o=50500, h=52000, l=50400, c=51800, v=12, q=621600, closed=True),
        Bar(symbol="BTCUSDT", timeframe="1h", ts=1007200, o=51800, h=53000, l=51700, c=52500, v=15, q=787500, closed=True),
    ]
