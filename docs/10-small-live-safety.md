# Small Live Safety

`small_live` is the guarded real-money preparation mode. It now has a default-off manual Spot order path, but it still does not connect automated strategies to real orders. API secrets are read only from local environment variables when the operator runs the live-order command.

## Preflight

Capture the paper dashboard status:

```powershell
Invoke-RestMethod -Uri http://127.0.0.1:8089/api/status | ConvertTo-Json -Depth 8 > reports/small-live-status.json
```

Run the gate:

```powershell
$env:CQ_SMALL_LIVE_ACK="I_UNDERSTAND_REAL_MONEY_RISK"
$env:CQ_SMALL_LIVE_MAX_TOTAL_QUOTE_USDT="<local-total-cap>"
$env:CQ_SMALL_LIVE_MAX_ORDER_QUOTE_USDT="<local-order-cap>"
$env:CQ_SMALL_LIVE_MAX_DAILY_LOSS_USDT="<local-daily-loss-cap>"
uv run python scripts/small_live_readiness.py --config config/small_live.yml --paper-status-json reports/small-live-status.json
```

Exit code `0` means the local safety posture is acceptable for the next development step. Exit code `2` means the gate is blocked. A passing report is not permission to place orders by itself; the operator must still run the live-order command with an explicit confirmation flag.

## Manual Order Path

Dry-run first:

```powershell
uv run python scripts/small_live_order.py `
  --config config/small_live.yml `
  --paper-status-json reports/small-live-status.json `
  --symbol BTCUSDT `
  --side buy `
  --quantity "<local-quantity>" `
  --stop-loss-price "<local-stop-price>" `
  --client-order-id "<unique-local-id>" `
  --dry-run
```

Real submission requires all preflight environment variables, local Binance Spot credentials, and an explicit confirmation:

```powershell
$env:BINANCE_SPOT_API_KEY="<local-api-key>"
$env:BINANCE_SPOT_API_SECRET="<local-api-secret>"
uv run python scripts/small_live_order.py `
  --config config/small_live.yml `
  --paper-status-json reports/small-live-status.json `
  --symbol BTCUSDT `
  --side buy `
  --quantity "<local-quantity>" `
  --stop-loss-price "<local-stop-price>" `
  --client-order-id "<unique-local-id>" `
  --confirm-live-order "I_UNDERSTAND_LIVE_ORDER_RISK"
```

The CLI submits only the requested manual order. The dashboard strategy loop remains paper-only.

## Non-Negotiable Rules

- Spot only.
- No leverage, margin, futures, or withdrawals.
- Total quote budget must stay under the configured safety cap.
- Per-order quote budget must stay under the configured safety cap.
- Daily loss cap must stay under the configured safety cap.
- Kill switch and reconciliation must be enabled.
- Paper dashboard must be running, connected, fresh, and above the drawdown threshold.
