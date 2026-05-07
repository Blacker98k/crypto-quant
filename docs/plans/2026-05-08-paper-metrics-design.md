# Paper Metrics Design

## Goal

Expose compact paper-trading metrics from the existing SQLite tables so simulations, dashboard endpoints, and later reports can consume the same daily rollup.

## Approach

Add a read-only metrics helper in `core.monitor.paper_metrics`. It will accept a SQLite connection plus a `[since_ms, until_ms)` window and aggregate existing `orders`, `fills`, `positions`, and `risk_events` rows. No schema change is needed.

The dashboard should use this helper through a new `/api/paper_metrics` route. A small CLI can also print the same JSON from a database file for smoke and scheduled runs.

## Metrics

The first slice focuses on durable operational numbers:

- order counts total and by status
- fill count, filled notional, fees, buy notional, sell notional, and cash PnL
- risk event count and counts by severity
- open position count and total open notional from current positions
- symbols touched in the window

Cash PnL is intentionally simple: sells minus buys minus fees. It is not a full realized-PnL ledger yet, but it is stable and useful for paper smoke reporting.

## Error Handling

Invalid windows are rejected by the CLI. Empty windows return zero counts and empty maps/lists. The dashboard clamps bad limits through existing FastAPI validation semantics and uses the current UTC day start by default.

## Testing

Add offline unit tests with an in-memory migrated SQLite database. Seed deterministic symbols, orders, fills, positions, and risk events, then assert the exact aggregate payload. Add a dashboard route test and a CLI subprocess test.
