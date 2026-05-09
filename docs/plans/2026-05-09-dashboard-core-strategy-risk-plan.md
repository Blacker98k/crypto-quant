# Dashboard Core Strategy Risk Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make the realtime dashboard paper pipeline run the project's core strategies through the same layered risk path used by research simulation, then clean up stale operational drift that can mislead readiness decisions.

**Architecture:** Keep the dashboard paper runner as the live orchestration surface, but replace its strategy-only shortcut with a reusable signal-to-paper execution path. Strategy execution should produce core strategy identifiers in the same `dashboard.sqlite` database, and every entry order must pass signal validation plus L3, L2, and L1 checks before reaching `PaperMatchingEngine`.

**Tech Stack:** Python, FastAPI, SQLite, existing `core.strategy`, `core.risk`, `core.execution`, `dashboard.paper_trading`, `pytest`, `ruff`, `mypy`.

---

### Task 1: Connect Formal Risk Gates To Dashboard Paper Orders

**Files:**
- Modify: `dashboard/paper_trading.py`
- Test: `tests/unit/test_dashboard_paper_trading.py`

**Steps:**
1. Write a failing test proving dashboard orders are rejected and recorded when an L1/L2/L3 risk gate rejects.
2. Run the focused test and verify it fails because dashboard currently places directly through the paper engine.
3. Add a small `DashboardRiskPipeline` wrapper around `StrategySignalValidator`, `L3PortfolioRiskValidator`, `L2PositionRiskSizer`, and `L1OrderRiskValidator`.
4. Call that wrapper from `_place_signal()` before `PaperMatchingEngine.place_order()`.
5. Record rejected signals in `risk_events` with source `signal`, `L3`, `L2`, or `L1`.
6. Run focused tests.

### Task 2: Attach S1/S2/S3 To The Dashboard Strategy Loop

**Files:**
- Modify: `dashboard/paper_trading.py`
- Modify: `dashboard/server.py`
- Test: `tests/unit/test_dashboard_paper_trading.py`

**Steps:**
1. Write a failing test proving `DashboardPaperTrader` can run a core `Strategy` object and persist its strategy name, instead of only exploration strategy names.
2. Write a failing test proving default dashboard strategies include S1 and S2 as active paper strategies, while S3 is shadow-only until paired execution is supported.
3. Add a `CoreStrategyAdapter` that builds `StrategyContext` from `LiveFeed`, maps required timeframes, and converts core strategy output to dashboard orders.
4. Keep exploration strategies disabled by default for readiness evidence; allow them only as explicit exploratory mode.
5. Update `LiveDataFeeder` to warm and feed the required higher timeframes.
6. Run focused tests.

### Task 3: Unify Equity And Runtime Configuration

**Files:**
- Modify: `core/strategy/base.py`
- Modify: `core/strategy/s1_btc_trend.py`
- Modify: `core/strategy/s2_altcoin_reversal.py`
- Modify: `dashboard/server.py`
- Test: `tests/unit/test_strategy.py`

**Steps:**
1. Write failing tests showing S1/S2 position sizing reads equity from `StrategyContext`.
2. Add a minimal account/equity accessor on `StrategyContext`, defaulting to a configured value.
3. Pass dashboard account equity into core strategy contexts.
4. Remove hard-coded strategy equity constants.
5. Run focused tests.

### Task 4: Fix Operational Drift

**Files:**
- Modify: `dashboard/paper_trading.py`
- Modify: `dashboard/server.py`
- Modify: `scripts/run_paper.py`
- Modify: `scripts/backfill_top50.py`
- Modify: `research/backtest/analyze_funding.py`
- Modify: `scripts/status.sh`
- Modify: `scripts/start.sh`
- Test: `tests/unit/test_dashboard_api.py`

**Steps:**
1. Write failing tests for small-price position display precision.
2. Change position display precision to preserve low-priced symbols.
3. Replace stale fallback symbols with active symbols.
4. Centralize proxy lookup through env/config helper.
5. Update script status text to the actual dashboard port and health endpoint.
6. Run focused tests.

### Task 5: Governance And Documentation Cleanup

**Files:**
- Create: `.github/workflows/doc-sync-check.yml`
- Modify: `记忆/01-项目概览.md`
- Modify: `记忆/02-当前状态.md`
- Modify: `docs/09-踩坑记录.md`
- Move/Delete: `research/debug_donchian_analysis.py`, `research/debug_s1_conditions.py`

**Steps:**
1. Add a doc-sync workflow that enforces AGENTS §7 path rules.
2. Refresh metadata and current status without writing any concrete personal balance or PnL numbers.
3. Move or remove temporary debug scripts.
4. Run tests plus static checks.
5. Commit with a sanitized technical message.
