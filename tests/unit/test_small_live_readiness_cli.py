from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

from core.live.small_live import ACK_ENV_VALUE


def test_small_live_readiness_cli_passes_safe_inputs(tmp_path: Path) -> None:
    config_path = tmp_path / "small_live.yml"
    status_path = tmp_path / "paper_status.json"
    config_path.write_text(
        """
enabled: true
mode: small_live
environment: production
exchange: binance_spot
allow_futures: false
allow_margin: false
allow_withdrawals: false
max_total_quote_usdt: 40
max_order_quote_usdt: 5
max_daily_loss_usdt: 3
max_open_positions: 1
allowed_symbols: [BTCUSDT]
kill_switch_enabled: true
reconciliation_required: true
""".strip(),
        encoding="utf-8",
    )
    status_path.write_text(
        json.dumps(
            {
                "simulation_running": True,
                "ws_connected": True,
                "market_data_stale": False,
                "account_equity": 11_000.0,
                "initial_balance": 10_000.0,
                "open_notional": 65_000.0,
            }
        ),
        encoding="utf-8-sig",
    )
    env = {**os.environ, "CQ_SMALL_LIVE_ACK": ACK_ENV_VALUE}

    result = subprocess.run(
        [
            sys.executable,
            "scripts/small_live_readiness.py",
            "--config",
            str(config_path),
            "--paper-status-json",
            str(status_path),
        ],
        check=False,
        cwd=Path.cwd(),
        env=env,
        text=True,
        capture_output=True,
    )

    assert result.returncode == 0
    payload = json.loads(result.stdout)
    assert payload["ready"] is True
    assert payload["blockers"] == []


def test_small_live_readiness_cli_blocks_unsafe_inputs(tmp_path: Path) -> None:
    config_path = tmp_path / "small_live.yml"
    status_path = tmp_path / "paper_status.json"
    config_path.write_text("mode: paper\n", encoding="utf-8")
    status_path.write_text(
        json.dumps(
            {
                "simulation_running": False,
                "ws_connected": False,
                "market_data_stale": True,
                "account_equity": 9_000.0,
                "initial_balance": 10_000.0,
                "open_notional": 80_000.0,
            }
        ),
        encoding="utf-8",
    )

    result = subprocess.run(
        [
            sys.executable,
            "scripts/small_live_readiness.py",
            "--config",
            str(config_path),
            "--paper-status-json",
            str(status_path),
        ],
        check=False,
        cwd=Path.cwd(),
        text=True,
        capture_output=True,
    )

    assert result.returncode == 2
    payload = json.loads(result.stdout)
    assert payload["ready"] is False
    assert "mode_not_small_live" in payload["blockers"]
    assert "paper_not_running" in payload["blockers"]
