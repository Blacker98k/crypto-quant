# Small Live Execution Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a default-off Binance Spot small-live execution path guarded by readiness and local safety checks.

**Architecture:** Keep public market data and paper trading unchanged. Add private order submission behind `core.live`, with ccxt injected for tests and environment-only credentials for real use. The executor accepts existing `OrderIntent` objects so strategy output can be wired later without changing signal semantics.

**Tech Stack:** Python 3.11, ccxt, pytest, ruff, mypy.

---

### Task 1: Private Spot Adapter

**Files:**
- Create: `tests/unit/test_live_trading_adapter.py`
- Create: `core/live/trading_adapter.py`
- Modify: `core/live/__init__.py`

**Steps:**
1. Write failing tests for loading credentials from environment, submitting a market order with a client order ID, cancelling an order, and querying an order.
2. Run `uv run pytest tests/unit/test_live_trading_adapter.py -q` and verify failure because the module does not exist.
3. Implement the adapter around an injected ccxt-like client.
4. Run the targeted test and verify pass.

### Task 2: Small Live Executor

**Files:**
- Create: `tests/unit/test_small_live_executor.py`
- Create: `core/live/executor.py`
- Modify: `core/live/__init__.py`

**Steps:**
1. Write failing tests for readiness gating, symbol allowlist, entry stop-loss requirement, and successful accepted order flow.
2. Run `uv run pytest tests/unit/test_small_live_executor.py -q` and verify failure because the executor does not exist.
3. Implement minimal executor behavior using the adapter protocol and existing `OrderIntent`.
4. Run the targeted test and verify pass.

### Task 3: Docs And Quality Gate

**Files:**
- Modify: `docs/10-small-live-safety.md`
- Modify: `README.md`

**Steps:**
1. Document the new execution path as default-off and operator-controlled.
2. Run `uv run pytest -q`.
3. Run `uv run ruff check .`.
4. Run `uv run mypy core`.
5. Commit and push.
