# Small Live Execution Design

## Goal

Build a spot-only small-live execution path so the system can submit real exchange orders only after the operator explicitly enables it. The implementation must stay default-off, keep secrets out of the repository, and leave the final launch decision outside the code.

## Scope

- Add a private Binance Spot trading adapter that wraps ccxt private endpoints.
- Add a small-live executor that gates every order through readiness, symbol allowlist, spot-only rules, idempotent client IDs, and local safety checks.
- Support market and limit entry/exit orders first. Entry orders may carry a protective stop price; the executor prepares the protective order only after the entry is accepted or filled.
- Do not connect the dashboard auto-strategy loop to live orders in this step. Paper trading remains independent.

## Safety

- No API key is stored in config files or docs; keys are read from local environment variables only when a live adapter is explicitly built.
- Futures, margin, withdrawals, and leverage remain blocked.
- The executor refuses to start unless `evaluate_small_live_readiness()` returns ready.
- The repository records only generic placeholders for budgets and caps.

## Data Flow

`small_live.yml + local env + paper status` -> `evaluate_small_live_readiness()` -> `SmallLiveExecutor` -> `BinanceSpotTradingAdapter` -> Binance private Spot API.

## Testing

- Unit-test the adapter with a fake ccxt client, verifying key injection, symbol mapping, client order IDs, and cancel/query behavior.
- Unit-test the executor with a fake adapter, verifying readiness blocks, allowlist blocks, stop-loss requirement, and accepted order flow.
- Keep live network tests manual and marked out of normal CI.
