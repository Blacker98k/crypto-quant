# WebSocket Reconnect Plan

## Scope

Add reconnect and REST catch-up behavior to the existing `WsSubscriber` without
changing the public subscription API.

## Steps

1. Add a failing unit test for a dropped stream followed by reconnect and REST
   replay of the missed closed candle.
2. Split connection open/close from the reader loop.
3. Track latest closed timestamps per subscribed pair.
4. On reconnect, fetch and publish closed bars missed during the outage.
5. Verify with focused WS tests, full quality gates, and a live Binance USDM WS
   smoke using public market data.

## Acceptance

- Existing kline parsing behavior remains unchanged.
- Dropped streams reconnect automatically while the subscriber is running.
- Recovered bars are written to cache, parquet, and callbacks in timestamp order.
- Closing the subscriber still cancels the reader and closes active resources.
