# Paper Readiness Check Design

## Goal

Provide one repeatable local/CI entrypoint that proves the paper system can read real public market data, run deterministic paper execution seeded by that data, and reject degraded report sources.

## Shape

- Add a small readiness orchestrator in `core.monitor` that builds and runs existing scripts in order:
  1. `scripts/market_health.py` with public ticker and optional kline checks.
  2. `scripts/simulate_paper.py` with `--price-source live`.
  3. `scripts/summarize_simulation_report.py` with strict cycle, pass-rate, failed-cycle, and source gates.
- Add `scripts/paper_readiness.py` as the CLI wrapper.
- Return a single JSON status payload with per-step command, return code, stdout/stderr, and parsed JSON when available.
- Stop after the first failed step because later steps depend on earlier artifacts.

## Safety

The check uses only public market-data REST APIs and the simulated paper engine. It never accepts API keys, signs requests, places orders, or switches execution mode.
