-- ============================================================================
-- V1__init.sql · 初始化 schema 版本表
-- ============================================================================
--
-- 唯一职责：建立 schema_version 元表，让后续迁移脚本（V2+）可以判断"哪些
-- 已经跑过、还有哪些没跑"。其它 9 张业务表（symbols / signals / orders /
-- fills / positions / trade_groups / risk_events / run_log / strategy_kv）由
-- PR-3 的 V2__core_tables.sql 落地。
--
-- 文档参考：docs/05-数据模型.md §6.1
-- 迁移规范：docs/05-数据模型.md §6.2
-- ============================================================================

-- 启动 SQLite 时本项目会显式 PRAGMA：
--   PRAGMA foreign_keys = ON;
--   PRAGMA journal_mode = WAL;
--   PRAGMA synchronous = NORMAL;
-- 这些 PRAGMA 是 connection-level，无法在 .sql 文件里持久化，由 core/db
-- 在打开连接时统一设置。

CREATE TABLE IF NOT EXISTS schema_version (
    version     INTEGER PRIMARY KEY,
    applied_at  INTEGER NOT NULL,                -- UTC ms
    description TEXT
);

-- 标记 V1 已执行。INSERT OR IGNORE 保证脚本幂等（重复执行不报错）。
INSERT OR IGNORE INTO schema_version (version, applied_at, description)
VALUES (1, CAST(strftime('%s','now') AS INTEGER) * 1000, 'init schema_version table');
