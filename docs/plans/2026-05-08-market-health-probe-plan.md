# Market Health Probe Plan

1. Add unit tests for successful and failed public market-data probes.
2. Implement the probe and recent run-log health summary.
3. Add the `scripts/market_health.py` CLI with spot/perp selection, proxy support, and optional kline verification.
4. Wire `/api/data_health` into the FastAPI dashboard.
5. Render the new data-health panel in the static dashboard.
6. Run focused tests, the full quality gate, and a real Binance public API smoke probe.
