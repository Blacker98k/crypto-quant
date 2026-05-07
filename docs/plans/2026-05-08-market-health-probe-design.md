# Market Health Probe Design

## Goal

Expose whether the system can currently read real public exchange market data without enabling live trading, private credentials, or order placement.

## Shape

- Add a small monitoring module that probes public ticker data and optionally a recent kline.
- Record each probe in the existing `run_log` table with endpoint, status, latency, and note.
- Summarize recent `run_log` rows for dashboard use:
  - `ok` when recent rows are all healthy.
  - `degraded` when any recent row failed.
  - `idle` when no probes have run yet.
- Add `scripts/market_health.py` for local and CI smoke checks against Binance spot or USD-M public market APIs.
- Add dashboard `/api/data_health` and a compact panel in the static UI.

## Safety

The probe uses only public REST market data and never accepts API keys, signs requests, places orders, or switches execution mode.
