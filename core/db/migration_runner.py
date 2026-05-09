"""SQLite 迁移 runner——扫描 ``migrations/`` 目录，按版本号排序，应用未执行的迁移。

按 ``docs/05-数据模型.md §6`` 的规范：
- 迁移脚本命名：``V{n}__{desc}.sql``
- 脚本必须幂等（用 IF NOT EXISTS / INSERT OR IGNORE 等）
- 启动时自动应用；严禁手动改库

用法
====

.. code-block:: python

    from core.db.migration_runner import MigrationRunner

    runner = MigrationRunner(migrations_dir="migrations")
    runner.apply_all(conn)
"""

from __future__ import annotations

import logging
import re
import sqlite3
from pathlib import Path

log = logging.getLogger(__name__)

# V1__init.sql, V2__core_tables.sql, V123__some_desc.sql
_FILENAME_RE = re.compile(r"^V(\d+)__.*\.sql$")


class MigrationError(Exception):
    """迁移执行失败（版本不连续 / SQL 错误等）。"""


class MigrationRunner:
    """迁移 runner。每个 SQLite 连接启动时调用 ``apply_all`` 一次。"""

    def __init__(self, migrations_dir: str | Path) -> None:
        self._dir = Path(migrations_dir)
        if not self._dir.is_dir():
            raise MigrationError(f"migrations dir not found: {self._dir}")

    # ─── public ──────────────────────────────────────────────────────────

    def apply_all(self, conn: sqlite3.Connection) -> list[int]:
        """应用所有未执行的迁移。返回本次新应用的版本号列表。

        Raises:
            MigrationError: 版本号不连续（有缺失的迁移文件）或 SQL 执行失败。
        """
        pending = self._pending_versions(conn)
        if not pending:
            return []

        applied: list[int] = []
        for version, path in pending:
            sql = path.read_text(encoding="utf-8")
            try:
                conn.executescript(sql)
            except sqlite3.Error as e:
                conn.rollback()
                raise MigrationError(
                    f"migration V{version} ({path.name}) failed: {e}"
                ) from e
            conn.execute(
                "INSERT OR REPLACE INTO schema_version (version, applied_at, description) "
                "VALUES (?, CAST(strftime('%s','now') AS INTEGER) * 1000, ?)",
                (version, path.stem),
            )
            conn.commit()
            applied.append(version)
            log.info("migration_applied", extra={"version": version, "file": path.name})

        return applied

    # ─── internal ────────────────────────────────────────────────────────

    def _pending_versions(
        self, conn: sqlite3.Connection
    ) -> list[tuple[int, Path]]:
        """返回 (version, path) 列表，按 version 升序。"""
        files = self._scan_files()
        if not files:
            return []

        # 验证版本连续性（不允许 1 2 4 缺失 3）
        versions = [v for v, _ in files]
        expected = list(range(min(versions), max(versions) + 1))
        missing = set(expected) - set(versions)
        if missing:
            raise MigrationError(
                f"migration version gap: missing V{min(missing)}. "
                f"found versions: {versions}"
            )

        last = self._last_applied(conn)
        return [(v, p) for v, p in files if v > last]

    def _scan_files(self) -> list[tuple[int, Path]]:
        """扫描 migrations/ 下所有 V*.sql 文件，按版本号排序。"""
        result: list[tuple[int, Path]] = []
        for p in sorted(self._dir.glob("V*.sql")):
            m = _FILENAME_RE.match(p.name)
            if m is None:
                log.warning("skip_non_matching_file", extra={"file": p.name})
                continue
            result.append((int(m.group(1)), p))
        result.sort(key=lambda x: x[0])
        return result

    @staticmethod
    def _last_applied(conn: sqlite3.Connection) -> int:
        """已应用的最后一个版本号；0 表示尚未应用任何迁移。"""
        try:
            row = conn.execute(
                "SELECT MAX(version) FROM schema_version"
            ).fetchone()
        except sqlite3.OperationalError:
            # schema_version 表还不存在 → 从未跑过迁移
            return 0
        return row[0] if row and row[0] is not None else 0


__all__ = ["MigrationError", "MigrationRunner"]
