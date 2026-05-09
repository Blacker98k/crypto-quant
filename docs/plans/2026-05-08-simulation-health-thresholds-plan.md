# Simulation Health Thresholds Plan

## Scope

Add automation-friendly failure checks to the existing simulation report summary
CLI.

## Steps

1. Add a failing CLI test for pass-rate and price-source requirements.
2. Parse `--min-pass-rate` and repeatable `--require-price-source` options.
3. Always print the JSON summary to stdout.
4. Print health failures to stderr and exit 1 when requirements are not met.
5. Verify focused report tests, full quality gates, and the extended live smoke
   report with strict thresholds.

## Acceptance

- Existing summary output remains stable.
- Threshold failures are machine-detectable through exit status.
- Live-market smoke automation can assert that Binance public ticker prices were
  actually used.
