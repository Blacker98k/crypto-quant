"""数据库层：SQLite 连接 / 迁移 runner / 通用 repo helpers。

详见 ``docs/05-数据模型.md`` 与 ``migrations/``。

用法
====

.. code-block:: python

    import sqlite3
    from core.db.migration_runner import MigrationRunner

    conn = sqlite3.connect("data/crypto.sqlite")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    MigrationRunner("migrations").apply_all(conn)
"""

from core.db.migration_runner import MigrationError, MigrationRunner

__all__ = ["MigrationError", "MigrationRunner"]
