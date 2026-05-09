# Dashboard Paper Metrics Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Show daily paper-trading metrics in the dashboard using the existing `/api/paper_metrics` endpoint.

**Architecture:** Keep all aggregation in `core.monitor.paper_metrics`; the dashboard only fetches and renders the API payload. Use the existing Vue single-file page and polling loop.

**Tech Stack:** Static Vue 3, Tailwind CDN utilities, FastAPI route already covered by pytest.

---

### Task 1: Static Contract Test

**Files:**
- Create: `tests/unit/test_dashboard_static.py`

**Step 1: Write the failing test**

Assert `dashboard/static/index.html` contains `paperMetrics`, `/api/paper_metrics`, `filled_notional`, `cash_pnl`, and `risk_events.total`.

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_dashboard_static.py -q`

Expected: fail because the page does not reference paper metrics yet.

### Task 2: Render Metrics Panel

**Files:**
- Modify: `dashboard/static/index.html`

**Step 1: Add compact panel markup**

Place a panel below the top status card showing filled notional, fees, cash PnL, risk events, open positions, open notional, and symbols.

**Step 2: Add Vue state and fetch**

Create a `paperMetrics` reactive object with zero defaults, fetch `/api/paper_metrics` inside `loadAll()`, and return the object to the template.

**Step 3: Run focused test**

Run: `uv run pytest tests/unit/test_dashboard_static.py -q`

Expected: pass.

### Task 3: Verify

Run `uv run pytest tests/unit/test_dashboard_static.py tests/unit/test_dashboard_api.py tests/unit/test_paper_metrics.py -q`, then full lint/type/test checks before publishing.
