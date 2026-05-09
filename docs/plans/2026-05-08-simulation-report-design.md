# Simulation Report Design

Goal: make paper simulations leave durable, machine-readable run evidence without changing strategy, risk, or matching behavior.

Recommended approach: add an optional JSONL report sink to `scripts/simulate_paper.py`. Each simulation cycle already emits a complete JSON payload to stdout, so the least risky design is to persist the same payload one line at a time when `--report` is provided. This keeps the CLI useful for long-running smoke runs, makes failures auditable, and gives the dashboard/reporter a stable file format to read later.

Alternatives considered:
- SQLite report tables: stronger querying, but requires a migration and schema decisions before the report shape has settled.
- HTML reports: better for humans, but slower to build and premature before daily/reporting metrics are finalized.
- JSONL file sink: smallest surface area, append-friendly, and directly testable.

Data flow: `simulate_paper.py` resolves the start price, runs one cycle, builds the existing payload, prints it, and writes it to the JSONL report path when configured. The writer creates parent directories, appends UTF-8 JSON, flushes each cycle, and preserves stdout behavior.

Testing: add unit tests around a small `SimulationReportWriter` helper before wiring the CLI. Then verify the CLI with static and live public Binance ticker smoke runs.
