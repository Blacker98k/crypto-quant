# Simulation Summary Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add `--summary` output to `scripts/simulate_paper.py` for compact run-level simulation results.

**Architecture:** Reuse `core.monitor.simulation_report` for aggregation. Add an in-memory helper, make the file summarizer delegate to it, then have the CLI collect current-run payloads and write one JSON summary file.

**Tech Stack:** Python 3.11, pytest, existing stdlib JSON/pathlib CLI code.

---

### Task 1: In-Memory Summary Helper

**Files:**
- Modify: `tests/unit/test_simulation_report.py`
- Modify: `core/monitor/simulation_report.py`

**Step 1: Write the failing test**

Add a test that calls `summarize_simulation_cycles(rows)` with two payload dictionaries and asserts the same aggregate shape as the file summarizer.

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_simulation_report.py::test_summarize_simulation_cycles_aggregates_payloads -q`

Expected: import failure because the helper does not exist yet.

**Step 3: Write minimal implementation**

Move the current aggregation logic into `summarize_simulation_cycles(rows)` and make `summarize_simulation_report(path)` parse JSONL rows then delegate to it.

**Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_simulation_report.py::test_summarize_simulation_cycles_aggregates_payloads -q`

Expected: pass.

### Task 2: CLI Summary Output

**Files:**
- Modify: `tests/unit/test_simulation_report.py`
- Modify: `scripts/simulate_paper.py`

**Step 1: Write the failing test**

Add a subprocess test that runs `scripts/simulate_paper.py` with `--price-source static`, `--cycles 2`, `--bars 2`, `--summary <tmp>/summary.json`, then asserts summary totals and pass rate.

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_simulation_report.py::test_simulate_paper_cli_writes_summary -q`

Expected: argparse rejects unknown `--summary`.

**Step 3: Write minimal implementation**

Add the argparse option, collect cycle payloads in a list, and write the final summary JSON with sorted keys after the loop.

**Step 4: Run focused tests**

Run: `uv run pytest tests/unit/test_simulation_report.py -q`

Expected: all simulation report tests pass.

### Task 3: Quality Gate

**Files:**
- No source changes expected.

**Step 1: Run full tests**

Run: `uv run pytest`

**Step 2: Run static checks**

Run: `uv run ruff check .`

Run: `uv run mypy core`

Run: `git diff --check`

**Step 3: Commit and publish**

Commit the focused change, update PR #5, and keep the branch clean.
