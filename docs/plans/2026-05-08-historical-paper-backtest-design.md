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

## Safety

The backtest reads local parquet files and writes local SQLite/report artifacts. It does not call private APIs, place orders, or enable live execution.

## Batch State

Each historical replay must keep strategy state inside the replay instance. Persisted KV is useful for live strategy continuity, but it can leak across repeated historical runs in the same SQLite database and hide orders in later symbol/timeframe cycles. The pulse strategy therefore uses in-memory state for this validation backtest.
