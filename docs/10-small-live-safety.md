# Small Live Safety

`small_live` is a future real-money preparation mode. The current implementation is a readiness gate only. It does not read API secrets, create private exchange clients, or place orders.

## Preflight

Capture the paper dashboard status:

```powershell
Invoke-RestMethod -Uri http://127.0.0.1:8089/api/status | ConvertTo-Json -Depth 8 > reports/small-live-status.json
```

Run the gate:

```powershell
$env:CQ_SMALL_LIVE_ACK="I_UNDERSTAND_REAL_MONEY_RISK"
uv run python scripts/small_live_readiness.py --config config/small_live.yml --paper-status-json reports/small-live-status.json
```

Exit code `0` means the local safety posture is acceptable for the next development step. Exit code `2` means the gate is blocked. A passing report is not permission to place orders; real order submission still needs a separate adapter, reconciliation, and emergency-stop implementation.

## Non-Negotiable Rules

- Spot only.
- No leverage, margin, futures, or withdrawals.
- Total quote budget must stay under the configured safety cap.
- Per-order quote budget must stay under the configured safety cap.
- Daily loss cap must stay under the configured safety cap.
- Kill switch and reconciliation must be enabled.
- Paper dashboard must be running, connected, fresh, and above the drawdown threshold.
