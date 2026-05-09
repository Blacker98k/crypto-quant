# Small Live Safety Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a non-ordering `small_live` readiness gate so real-money testing cannot start without explicit small-budget safety checks.

**Architecture:** Implement a pure Python readiness module under `core/live`, expose a CLI preflight in `scripts/`, and add a non-secret example config. No private exchange client or real order submission is added in this plan.

**Tech Stack:** Python 3.11, dataclasses, PyYAML, pytest, ruff, mypy.

---

### Task 1: Core Readiness Tests

**Files:**
- Create: `tests/unit/test_small_live_readiness.py`
- Create: `core/live/small_live.py`

**Step 1:** Write failing tests for blocked defaults, passing safe config, budget violation, futures rejection, missing acknowledgement, and unhealthy paper status.

**Step 2:** Run `uv run pytest tests/unit/test_small_live_readiness.py -q` and verify failure due to missing module.

**Step 3:** Implement dataclasses and `evaluate_small_live_readiness()`.

**Step 4:** Run focused tests until green.

### Task 2: CLI And Config

**Files:**
- Create: `scripts/small_live_readiness.py`
- Create: `config/small_live.example.yml`
- Modify: `pyproject.toml` if script lint ignores are needed.

**Step 1:** Add a CLI test or focused subprocess smoke if existing patterns fit.

**Step 2:** Implement config loading and JSON output.

**Step 3:** Verify blocked example config exits non-zero and prints blocking reasons.

### Task 3: Documentation And Gates

**Files:**
- Modify: `README.md` or add docs note if needed.
- Run: `uv run pytest -q`, `uv run ruff check .`, `uv run mypy core`.

**Step 1:** Document that this is not live trading and does not place orders.

**Step 2:** Run full gates.

**Step 3:** Commit and push.
