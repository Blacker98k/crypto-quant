"""pytest 全局 fixtures。

Phase 1 起步时为空，PR-3+ 会按需加：

* ``tmp_sqlite_db`` —— 临时 SQLite + 应用 migrations
* ``mock_exchange`` —— 模拟 ExchangeAdapter
* ``fixed_clock`` —— FixedClock 注入
* ``capture_logs`` —— 抓 JSON 日志
"""

from __future__ import annotations
