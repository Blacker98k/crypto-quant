# Historical Paper Backtest Design

## Goal

Run the paper execution path against real historical OHLCV that was backfilled into parquet, while keeping the safety boundary at simulated execution only.

## Shape

- Add a small historical paper backtest module that:
  - reads bars from `ParquetIO`,
  - seeds the symbol metadata into SQLite,
  - runs the existing `SimulatedPaperSession`,
  - writes JSONL/summary reports compatible with simulation gates.
- Start with a deterministic pulse strategy so this validates the historical data -> strategy -> risk -> paper matching path without adding live trading or complex strategy-specific assumptions.
- Add a CLI wrapper for repeated local runs over backfilled data.
- Add a batch wrapper that expands symbol/timeframe combinations, runs each pair through the same single-backtest path, and writes one JSONL stream plus one aggregated summary.
- Reject windows shorter than a configurable minimum bar count before order simulation, so smoke reports clearly distinguish missing/insufficient data from strategy or execution failures.
- Add a readiness CLI that chains batch historical paper replay and strict report validation in one repeatable command.

## Safety

The backtest reads local parquet files and writes local SQLite/report artifacts. It does not call private APIs, place orders, or enable live execution.

The readiness CLI only deletes the exact output files passed through `--db`, `--report`, and `--summary`. This keeps repeated runs deterministic without removing data directories or historical parquet inputs.

## Batch State

Each historical replay must keep strategy state inside the replay instance. Persisted KV is useful for live strategy continuity, but it can leak across repeated historical runs in the same SQLite database and hide orders in later symbol/timeframe cycles. The pulse strategy therefore uses in-memory state for this validation backtest.
