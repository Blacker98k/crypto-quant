# Simulation Health Thresholds Design

## Goal

Let continuous paper simulation checks fail automatically when a report is not
healthy enough, instead of requiring a human to inspect the JSON summary.

## CLI Additions

`scripts/summarize_simulation_report.py` now accepts:

- `--min-pass-rate FLOAT`
- `--require-price-source SOURCE`

The CLI still prints the JSON summary to stdout. Health check failures are
written to stderr and return exit code 1.

## Use Cases

- Require `pass_rate=1.0` for short smoke loops.
- Require `binance_usdm_public_ticker` to prove the run used live public market
  prices rather than a static fallback.
- Keep the summary JSON machine-readable for dashboards and automation.
