# LiveFeed WebSocket Bridge Design

## Goal

Expose realtime candle subscriptions through the unified `LiveFeed` interface
instead of requiring callers to know about `WsSubscriber` directly.

## Behavior

`LiveFeed.subscribe_candles(symbol, timeframe, callback)` now:

- normalizes the symbol
- creates an active `SubscriptionHandle`
- stores the handle by its id for later `unsubscribe`
- delegates the actual stream registration to an optional `WsSubscriber`

The WebSocket connection lifecycle still belongs to `WsSubscriber`; this slice is
only the strategy-facing bridge.

## Compatibility

Existing `LiveFeed(parquet_io, repo, memory_cache)` construction remains valid.
When no `ws_subscriber` is provided, the handle is still tracked locally, which
keeps tests and dry-run wiring simple.
