-- ============================================================================
-- V2__core_tables.sql · 核心业务表（9 张）
-- ============================================================================
--
-- 按 docs/05-数据模型.md §3 创建共享实体与生产实体。
-- 所有表均为幂等（CREATE TABLE IF NOT EXISTS），可安全重复执行。
--
-- 表清单：
--   共享：symbols（标的字典）
--   生产：signals / orders / fills / positions /
--         trade_groups / risk_events / run_log / strategy_kv
-- ============================================================================

-- ============================================================================
-- §3.1.1  symbols · 标的字典
-- ============================================================================
CREATE TABLE IF NOT EXISTS symbols (
    id           INTEGER PRIMARY KEY,
    exchange     TEXT    NOT NULL,            -- binance / okx
    symbol       TEXT    NOT NULL,            -- BTCUSDT（对内无分隔符大写）
    type         TEXT    NOT NULL,            -- spot / perp / futures
    base         TEXT    NOT NULL,            -- BTC
    quote        TEXT    NOT NULL,            -- USDT
    universe     TEXT,                        -- core / observation / scan / NULL
    tick_size    REAL    NOT NULL,            -- 最小价格变动
    lot_size     REAL    NOT NULL,            -- 最小数量
    min_notional REAL    NOT NULL,            -- 最小订单价值
    listed_at    INTEGER NOT NULL,            -- 上线时间 ms
    delisted_at  INTEGER,                     -- 下线时间 ms（NULL=活跃）
    UNIQUE(exchange, symbol, type)
);

-- ============================================================================
-- §3.2.1  signals · 策略信号
-- ============================================================================
CREATE TABLE IF NOT EXISTS signals (
    id                  INTEGER PRIMARY KEY,
    strategy            TEXT    NOT NULL,      -- e.g. 's1_btc_trend'
    strategy_version    TEXT    NOT NULL,      -- 策略代码 git short hash
    config_hash         TEXT    NOT NULL,      -- 配置 SHA-256 前 16 位
    universe_version    TEXT    NOT NULL,      -- 币池版本；Phase 1 用 'p1-stub'
    run_id              TEXT    NOT NULL,      -- 进程启动 UUID
    symbol_id           INTEGER NOT NULL REFERENCES symbols(id),
    side                TEXT    NOT NULL,      -- long / short / close
    entry_price         REAL,                  -- 建议入场价（NULL=市价）
    stop_price          REAL    NOT NULL,      -- 止损价
    target_price        REAL,                  -- 目标价
    confidence          REAL    NOT NULL,      -- 0-1
    suggested_size      REAL    NOT NULL,      -- 建议仓位（base asset）
    time_in_force       TEXT    NOT NULL DEFAULT 'GTC',  -- GTC / IOC / FOK / DAY
    rationale           TEXT    NOT NULL,      -- JSON 决策依据
    status              TEXT    NOT NULL DEFAULT 'pending',  -- pending/placed/rejected/skipped/expired
    reject_reason       TEXT,                  -- 拒绝原因
    trade_group_id      TEXT,                  -- NULL=单腿；非空关联 trade_groups.id
    captured_at         INTEGER NOT NULL,      -- 信号产生时间 ms
    expires_at          INTEGER NOT NULL       -- 过期时间 ms
);

CREATE INDEX IF NOT EXISTS idx_signals_status_captured ON signals(status, captured_at);
CREATE INDEX IF NOT EXISTS idx_signals_strategy_captured ON signals(strategy, captured_at);

-- ============================================================================
-- §3.2.2  orders · 订单
-- ============================================================================
CREATE TABLE IF NOT EXISTS orders (
    id                  INTEGER PRIMARY KEY,
    signal_id           INTEGER REFERENCES signals(id),
    client_order_id     TEXT    NOT NULL UNIQUE,  -- 幂等 key（UUID v4）
    exchange_order_id   TEXT,                      -- 交易所返回的订单号
    symbol_id           INTEGER NOT NULL REFERENCES symbols(id),
    side                TEXT    NOT NULL,          -- buy / sell
    type                TEXT    NOT NULL,          -- limit / market / stop / stop_limit / take_profit
    price               REAL,                      -- 限价（市价单为 NULL）
    stop_price          REAL,                      -- 触发价（仅止损/止盈）
    quantity            REAL    NOT NULL,
    filled_qty          REAL    NOT NULL DEFAULT 0,
    avg_fill_price      REAL,
    status              TEXT    NOT NULL DEFAULT 'new',
    parent_order_id     INTEGER,                   -- 关联的母单（止损单的母仓）
    purpose             TEXT    NOT NULL DEFAULT 'entry',  -- entry/stop_loss/take_profit/exit/hedge_leg
    time_in_force       TEXT    NOT NULL DEFAULT 'GTC',
    reduce_only         INTEGER NOT NULL DEFAULT 0,  -- 0/1
    trade_group_id      TEXT,                        -- NULL=单腿
    strategy_version    TEXT    NOT NULL,            -- 复制自 signal
    placed_at           INTEGER NOT NULL,
    updated_at          INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_orders_status_placed ON orders(status, placed_at);
CREATE INDEX IF NOT EXISTS idx_orders_symbol_placed ON orders(symbol_id, placed_at);

-- ============================================================================
-- §3.2.3  fills · 成交回报
-- ============================================================================
CREATE TABLE IF NOT EXISTS fills (
    id                INTEGER PRIMARY KEY,
    order_id          INTEGER NOT NULL REFERENCES orders(id),
    exchange_fill_id  TEXT    NOT NULL,
    price             REAL    NOT NULL,
    quantity          REAL    NOT NULL,
    fee               REAL    NOT NULL DEFAULT 0,
    fee_currency      TEXT    NOT NULL,
    is_maker          INTEGER NOT NULL DEFAULT 0,  -- 0/1
    ts                INTEGER NOT NULL,            -- 成交时间 ms
    raw_payload       TEXT,                         -- 原始 API 返回（备份）
    UNIQUE(order_id, exchange_fill_id)
);

-- ============================================================================
-- §3.2.4  positions · 持仓
-- ============================================================================
CREATE TABLE IF NOT EXISTS positions (
    id                  INTEGER PRIMARY KEY,
    symbol_id           INTEGER NOT NULL REFERENCES symbols(id),
    strategy            TEXT    NOT NULL,
    strategy_version    TEXT    NOT NULL,            -- 复制自开仓 signal
    opening_signal_id   INTEGER REFERENCES signals(id),
    side                TEXT    NOT NULL,            -- long / short
    qty                 REAL    NOT NULL,
    avg_entry_price     REAL    NOT NULL,
    current_price       REAL,
    unrealized_pnl      REAL,
    realized_pnl        REAL    NOT NULL DEFAULT 0,
    leverage            REAL    NOT NULL DEFAULT 1,
    margin              REAL,
    liq_price           REAL,
    stop_order_id       INTEGER REFERENCES orders(id),  -- 关联交易所侧止损单
    trade_group_id      TEXT,
    opened_at           INTEGER NOT NULL,
    closed_at           INTEGER                         -- NULL 表示持仓中
);

CREATE INDEX IF NOT EXISTS idx_positions_open ON positions(closed_at) WHERE closed_at IS NULL;

-- ============================================================================
-- §3.2.5  trade_groups · 多腿事务组
-- ============================================================================
CREATE TABLE IF NOT EXISTS trade_groups (
    id              TEXT    PRIMARY KEY,          -- UUID
    strategy        TEXT    NOT NULL,             -- S3
    group_type      TEXT    NOT NULL,             -- spread_arb / pair_trade / hedge
    status          TEXT    NOT NULL DEFAULT 'forming',  -- forming/hedged/unwinding/closed/broken/partial_alert
    leg_plan        TEXT    NOT NULL,             -- JSON：计划的腿
    leg_count       INTEGER NOT NULL,
    filled_legs     INTEGER NOT NULL DEFAULT 0,
    target_basis    REAL,                         -- 入场时基差
    stop_basis      REAL,                         -- 基差扩大止损线
    created_at      INTEGER NOT NULL,
    hedged_at       INTEGER,                      -- 双腿全成交时间
    closing_at      INTEGER,
    closed_at       INTEGER
);

-- ============================================================================
-- §3.2.6  risk_events · 风控事件
-- ============================================================================
CREATE TABLE IF NOT EXISTS risk_events (
    id          INTEGER PRIMARY KEY,
    type        TEXT    NOT NULL,  -- circuit_breaker / drawdown_limit / exposure_cap / liq_warning / api_error / fill_anomaly
    severity    TEXT    NOT NULL,  -- info / warn / critical
    source      TEXT    NOT NULL,  -- L1 / L2 / L3 / engine / oms
    related_id  INTEGER,           -- signal_id / order_id
    payload     TEXT,              -- JSON 详情
    captured_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_risk_events_time_sev ON risk_events(captured_at, severity);

-- ============================================================================
-- §3.2.8  run_log · 接口健康
-- ============================================================================
CREATE TABLE IF NOT EXISTS run_log (
    id          INTEGER PRIMARY KEY,
    endpoint    TEXT    NOT NULL,
    status      TEXT    NOT NULL,  -- ok / fail / unauthorized / rate_limited
    http_code   INTEGER,
    latency_ms  INTEGER,
    note        TEXT,
    captured_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_run_log_time ON run_log(captured_at);

-- ============================================================================
-- §3.2.9  strategy_kv · 策略持久化 KV
-- ============================================================================
CREATE TABLE IF NOT EXISTS strategy_kv (
    strategy    TEXT    NOT NULL,  -- 策略 name
    key         TEXT    NOT NULL,  -- 策略自定义 key
    value_json  TEXT    NOT NULL,  -- 任意 JSON
    updated_at  INTEGER NOT NULL,
    PRIMARY KEY (strategy, key)
);
