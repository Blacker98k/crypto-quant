# Simulation Report Implementation Plan

Goal: persist per-cycle simulation payloads as JSONL reports for long-running paper smoke tests.

Architecture: add a small `core.monitor.simulation_report` helper and wire it into `scripts/simulate_paper.py` behind an optional `--report` argument. Keep stdout JSON unchanged.

Tech stack: Python stdlib `json`, `pathlib`, existing pytest/ruff/mypy gates.

---

### Task 1: Report Writer

Files:
- Create: `core/monitor/simulation_report.py`
- Test: `tests/unit/test_simulation_report.py`

Steps:
1. Write failing tests for directory creation, JSONL append, and context-manager close.
2. Run `uv run pytest tests/unit/test_simulation_report.py -q` and confirm failure.
3. Implement `SimulationReportWriter`.
4. Re-run the focused test.

### Task 2: CLI Wiring

Files:
- Modify: `scripts/simulate_paper.py`
- Test: `tests/unit/test_simulation_report.py`

Steps:
1. Add a failing test that writes two payloads through the same helper shape used by the CLI.
2. Add `--report` to the parser and call the writer per cycle.
3. Run focused tests.

### Task 3: Verification

Run:
- `uv run pytest`
- `uv run ruff check .`
- `uv run mypy core`
- `git diff --check`
- static simulation with `--report reports/simulations/smoke.jsonl`
- live public Binance simulation with `--report reports/simulations/live-smoke.jsonl`
