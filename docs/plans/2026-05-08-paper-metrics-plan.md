# Paper Metrics Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add reusable paper-trading metrics for a time window, with dashboard and CLI access.

**Architecture:** Create `core.monitor.paper_metrics` for all aggregation. Keep SQL read-only and scoped to the existing SQLite schema. Import the helper in `dashboard.server` and a new `scripts/paper_metrics.py`.

**Tech Stack:** Python 3.11, SQLite, FastAPI route functions, pytest subprocess checks.

---

### Task 1: Core Metrics Helper

**Files:**
- Create: `tests/unit/test_paper_metrics.py`
- Create: `core/monitor/paper_metrics.py`

**Step 1: Write the failing test**

Seed a migrated in-memory SQLite database with two symbols, orders, fills, positions, and risk events. Call `paper_metrics(conn, since_ms=..., until_ms=...)` and assert exact counts, notional values, fee totals, risk severities, open position notional, and symbols.

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_paper_metrics.py::test_paper_metrics_aggregates_window -q`

Expected: import failure because `core.monitor.paper_metrics` does not exist yet.

**Step 3: Write minimal implementation**

Implement SQL queries against orders/fills/positions/risk_events. Return plain dictionaries containing only JSON-serializable values.

**Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_paper_metrics.py::test_paper_metrics_aggregates_window -q`

Expected: pass.

### Task 2: Dashboard Endpoint

**Files:**
- Modify: `tests/unit/test_dashboard_api.py`
- Modify: `dashboard/server.py`

**Step 1: Write the failing test**

Add a test that seeds one order/fill/risk event, builds the app, calls `/api/paper_metrics`, and asserts the route returns the helper payload.

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_dashboard_api.py::test_dashboard_paper_metrics_endpoint -q`

Expected: route not found.

**Step 3: Write minimal implementation**

Import `paper_metrics` and add `/api/paper_metrics` with optional `since_ms` and `until_ms`. Default `since_ms` to `_start_of_day_ts()` and `until_ms` to current time plus one millisecond.

**Step 4: Run focused tests**

Run: `uv run pytest tests/unit/test_dashboard_api.py tests/unit/test_paper_metrics.py -q`

Expected: pass.

### Task 3: CLI Access

**Files:**
- Modify: `tests/unit/test_paper_metrics.py`
- Create: `scripts/paper_metrics.py`
- Modify: `pyproject.toml`

**Step 1: Write the failing test**

Create a temporary file database, seed it, run `scripts/paper_metrics.py --db <path> --since-ms ... --until-ms ...`, and assert JSON output.

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_paper_metrics.py::test_paper_metrics_cli_outputs_json -q`

Expected: script file does not exist.

**Step 3: Write minimal implementation**

Create the script, connect read-only enough for local use, set row factory, call the helper, and print sorted JSON.

**Step 4: Run checks and publish**

Run `uv run pytest`, `uv run ruff check .`, `uv run mypy core`, and `git diff --check`, then commit and update PR #5.
