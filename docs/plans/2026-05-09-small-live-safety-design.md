# Small Live Safety Design

## Goal

Build a guarded `small_live` preparation layer for testing with a very small real-money budget later, while keeping the current system paper-only by default.

## Recommendation

Use a readiness gate before any private exchange integration. The first version does not place real orders and does not read secrets. It only answers: "Would this runtime configuration be allowed to start a small-live session?"

## Architecture

- Add a pure `core.live.small_live` module with dataclasses for config, observed paper status, and readiness results.
- Add a CLI preflight script that reads a non-secret YAML config and optional paper status JSON, runs the readiness gate, prints JSON, and exits non-zero when blocked.
- Keep private order submission out of scope. Future live adapters must call the same readiness gate before any exchange client is created.

## Hard Gates

The readiness gate blocks unless all of these are true:

- Runtime mode is exactly `small_live`.
- Explicit acknowledgement token is present in the environment.
- Exchange is spot-only; futures, margin, leverage, and withdrawals are forbidden.
- Total budget, per-order budget, daily loss cap, and position count are below small-live limits.
- Kill switch and reconciliation are enabled.
- Paper dashboard is running, connected to real public market data, not stale, and not in material drawdown.

## Data Flow

`config/small_live.example.yml` -> `scripts/small_live_readiness.py` -> `core.live.small_live.evaluate_small_live_readiness()` -> JSON report.

The report lists blocking reasons and warnings. A passing report is not an order signal; it only says the local safety posture is acceptable for the next development step.

## Error Handling

Invalid or missing config blocks. Missing paper status blocks. Any stale market data or disconnected WebSocket blocks. The default example config intentionally blocks until copied and edited by the operator.

## Testing

Unit tests cover default blocking behavior, a fully safe passing configuration, budget violations, futures/margin rejection, missing acknowledgement, and paper-status health failures.
