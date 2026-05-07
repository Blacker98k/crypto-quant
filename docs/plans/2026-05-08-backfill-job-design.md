# Historical Backfill Job Design

## Goal

Replace the placeholder backfill component with a resumable OHLCV job that can pull
real Binance klines through the existing exchange adapters and persist them as
project parquet candles.

## Interfaces

- `BackfillJob(exchange, parquet_io, repo, page_limit=1000)`
- `await run(symbols, timeframes, start_ms, end_ms, concurrency=4, resume=True)`
- `clear_progress(symbol, timeframe)`

## Persistence

Bar data is written through `ParquetIO.write_bars`, which already partitions by
symbol, timeframe, and calendar period, and de-duplicates by `ts`.

Resume state uses `strategy_kv`:

- `strategy`: `backfill`
- `key`: `{SYMBOL}:{TIMEFRAME}`
- `value_json`: `next_ms`, `end_ms`, `complete`, and `updated_at`

This avoids a migration while keeping the resume cursor inspectable.

## Behavior

Each symbol/timeframe pair runs as an independent task behind an asyncio
semaphore. A result is returned for every pair so the CLI can report partial
success instead of hiding failures.

The job normalizes each returned page before writing:

- keeps bars in `[cursor, end_ms)`
- rewrites symbol/timeframe to the requested normalized pair
- de-duplicates by timestamp within the page
- advances by one full timeframe interval

If an exchange call fails, the current pair returns `complete=False` with the
error string. Already written bars and the last saved cursor remain available for
the next `resume=True` run.
