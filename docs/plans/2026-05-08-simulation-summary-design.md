# Simulation Summary Design

## Goal

Add a durable JSON summary artifact for each paper simulation run, so continuous smoke tests can emit both per-cycle JSONL details and one compact run-level result.

## Approach

Use the existing simulation report module as the aggregation boundary. Add an in-memory summary helper that accepts cycle payload dictionaries, then keep `summarize_simulation_report(path)` as a file-reading wrapper around that helper.

`scripts/simulate_paper.py` will gain an optional `--summary PATH`. During a run it already receives every cycle payload, so it can keep those payloads in memory and write the final summary after all cycles finish. The summary represents the current invocation only, even if `--report` appends to a longer JSONL file.

## Data Shape

The summary keeps the current fields:

- `cycles`, `passed`, `failed`, `pass_rate`
- sorted `symbols`
- sorted `price_sources`
- numeric `totals` aggregated from each payload's `result`

## Error Handling

The script validates cycle counts and bar counts before running, as it already does. Summary parent directories are created automatically. A failed simulation still writes a summary before exiting non-zero, as long as the failure is represented by a completed cycle payload.

## Testing

Add unit tests for the in-memory helper and a CLI test for `--summary` with multiple static cycles. Keep the tests offline and deterministic.
