# Historical Backfill Job Plan

## Scope

Implement the P1.2 historical backfill core behind the existing CLI without
changing migration shape or exchange adapter contracts.

## Steps

1. Add failing unit coverage for write, resume, clear-progress, and exchange
   error paths.
2. Implement `BackfillJob` with typed result objects and timeframe parsing.
3. Store resume cursors in `strategy_kv`.
4. Verify with focused tests, full quality gates, and a real Binance USDM
   backfill smoke using public market data.

## Acceptance

- `scripts/backfill.py --no-resume` no longer fails on the missing
  `clear_progress` method.
- Backfilled candles are written into parquet partitions.
- A later run can resume from the saved `next_ms` cursor.
- Exchange/API failures do not crash the whole job; each pair reports its own
  incomplete result.
