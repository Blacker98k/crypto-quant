# LiveFeed WebSocket Bridge Plan

## Scope

Wire the realtime subscription entrypoint from `LiveFeed` to `WsSubscriber`
without starting or stopping network resources from the feed itself.

## Steps

1. Add a failing feed test for candle subscription delegation.
2. Add an optional `ws_subscriber` dependency to `LiveFeed`.
3. Normalize symbols and create tracked `SubscriptionHandle` objects.
4. Delegate candle registrations to the subscriber when one is present.
5. Verify focused feed tests and full quality gates.

## Acceptance

- Research mode still rejects realtime subscriptions.
- Live mode returns an active handle for candle subscriptions.
- Live mode delegates the normalized symbol/timeframe/callback to the WS layer.
- Existing `unsubscribe` behavior remains compatible with stored handles.
