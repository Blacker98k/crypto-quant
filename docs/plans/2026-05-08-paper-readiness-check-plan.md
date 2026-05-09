# Paper Readiness Check Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a single paper-readiness command that verifies public market data, live-seeded simulated paper execution, and strict report source gates.

**Architecture:** Put orchestration logic in `core.monitor.paper_readiness` so it can be unit-tested without real network calls. Keep `scripts/paper_readiness.py` as a thin CLI that parses arguments, runs the orchestrator, prints JSON, and exits non-zero on failure.

**Tech Stack:** Python stdlib subprocess/dataclasses/pathlib/json, existing market health and simulation scripts, pytest.

---

### Task 1: Failing Tests

**Files:**
- Create: `tests/unit/test_paper_readiness.py`

**Steps:**
1. Write tests for command construction, fail-fast behavior, and successful JSON aggregation.
2. Run `uv run pytest tests/unit/test_paper_readiness.py` and confirm it fails because `core.monitor.paper_readiness` does not exist.

### Task 2: Orchestrator

**Files:**
- Create: `core/monitor/paper_readiness.py`

**Steps:**
1. Implement dataclasses for config, step result, and readiness result.
2. Build the three commands with `sys.executable` and existing script paths.
3. Execute steps through an injectable runner, parse JSON from stdout, and stop on first non-zero return code.
4. Run focused tests until green.

### Task 3: CLI

**Files:**
- Create: `scripts/paper_readiness.py`

**Steps:**
1. Parse symbol, bars, cycles, interval, proxy, db/report/summary paths, and kline requirements.
2. Call the orchestrator, print summary JSON, and exit 1 on failure.
3. Add focused CLI coverage only if the thin wrapper grows beyond argument mapping.

### Task 4: Verification

**Steps:**
1. Run focused tests and static checks.
2. Run the real Binance public API readiness smoke through the proxy.
3. Run the full quality gate.
4. Commit and update PR #5.
