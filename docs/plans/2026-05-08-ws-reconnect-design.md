# WebSocket Reconnect Design

## Goal

Make `WsSubscriber` recover from a dropped Binance kline stream and replay missed
closed candles through the existing REST adapter contract.

## State

The subscriber tracks the latest closed candle timestamp per `(symbol,
timeframe)` in memory. Partial bars still update cache/callbacks but are not used
as recovery anchors because they are not durable.

## Reconnect Flow

1. The reader consumes the current combined stream.
2. If the stream raises or ends while the subscriber is still running, the
   current websocket/session is closed.
3. The subscriber waits `reconnect_delay_sec`.
4. A fresh combined-stream connection is opened with the same subscription set.
5. For every pair with a known closed candle, REST klines are fetched from
   `last_closed_ts + interval` up to the injected clock time.
6. Recovered closed bars are normalized, written to cache/parquet, and replayed
   to the same callbacks.

## Boundaries

This slice keeps the current single-connection combined stream model. It does
not yet add stream sharding, alerting, or data-degraded dashboard state.
