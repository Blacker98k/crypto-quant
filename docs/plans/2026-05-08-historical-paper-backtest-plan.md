# Historical Paper Backtest Implementation Plan

1. Add failing tests for parquet-backed paper backtest success, empty-data failure, and report/summary output.
2. Implement `core.research.historical_paper_backtest` with config, pulse strategy, symbol seeding, and report writing.
3. Add `scripts/backtest_paper.py` as a thin CLI around the module.
4. Run focused tests and static checks.
5. Run real backfill plus historical backtest smoke with Binance public data.
6. Run full quality gates, commit, publish, and update PR #5.
7. Extend the historical backtest with a batch symbol/timeframe runner and aggregated report output.
8. Add regression coverage for per-run state isolation when the same symbol is replayed across multiple timeframes.
