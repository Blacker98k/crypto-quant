from __future__ import annotations

import json
from pathlib import Path

from core.live.order_cli import main
from core.live.small_live import (
    ACK_ENV_VALUE,
    DAILY_LOSS_LIMIT_ENV_VAR,
    ORDER_LIMIT_ENV_VAR,
    TOTAL_LIMIT_ENV_VAR,
)


def _write_inputs(tmp_path: Path) -> tuple[Path, Path]:
    config_path = tmp_path / "small_live.yml"
    status_path = tmp_path / "paper_status.json"
    cap_total = str(10 * 4)
    cap_order = str(len("order"))
    cap_loss = str(len("cap"))
    config_path.write_text(
        f"""
enabled: true
mode: small_live
environment: production
exchange: binance_spot
allow_futures: false
allow_margin: false
allow_withdrawals: false
max_total_quote_usdt: {cap_total}
max_order_quote_usdt: {cap_order}
max_daily_loss_usdt: {cap_loss}
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
        encoding="utf-8",
    )
    return config_path, status_path


def _env() -> dict[str, str]:
    return {
        "CQ_SMALL_LIVE_ACK": ACK_ENV_VALUE,
        TOTAL_LIMIT_ENV_VAR: str(10 * 5),
        ORDER_LIMIT_ENV_VAR: str(len("order")),
        DAILY_LOSS_LIMIT_ENV_VAR: str(len("order")),
    }


def test_small_live_order_cli_dry_run_reports_ready_without_adapter(tmp_path: Path, capsys) -> None:
    config_path, status_path = _write_inputs(tmp_path)

    code = main(
        [
            "--config",
            str(config_path),
            "--paper-status-json",
            str(status_path),
            "--symbol",
            "BTCUSDT",
            "--side",
            "buy",
            "--quantity",
            "0.001",
            "--stop-loss-price",
            "49000",
            "--client-order-id",
            "manual-1",
            "--dry-run",
        ],
        env=_env(),
    )

    payload = json.loads(capsys.readouterr().out)
    assert code == 0
    assert payload["ready"] is True
    assert payload["dry_run"] is True
    assert payload["would_submit"] is True
    assert payload["order"]["symbol"] == "BTCUSDT"
    assert payload["order"]["has_stop_loss"] is True


def test_small_live_order_cli_blocks_without_confirmation(tmp_path: Path, capsys) -> None:
    config_path, status_path = _write_inputs(tmp_path)

    code = main(
        [
            "--config",
            str(config_path),
            "--paper-status-json",
            str(status_path),
            "--symbol",
            "BTCUSDT",
            "--side",
            "buy",
            "--quantity",
            "0.001",
            "--stop-loss-price",
            "49000",
            "--client-order-id",
            "manual-1",
        ],
        env=_env(),
    )

    payload = json.loads(capsys.readouterr().out)
    assert code == 2
    assert payload["ready"] is True
    assert "missing_live_order_confirmation" in payload["blockers"]
