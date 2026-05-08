from __future__ import annotations

from pathlib import Path


def test_dashboard_static_page_renders_paper_metrics_panel() -> None:
    html = Path("dashboard/static/index.html").read_text(encoding="utf-8")

    assert "paperMetrics" in html
    assert "/api/paper_metrics" in html
    assert "filled_notional" in html
    assert "cash_pnl" in html
    assert "risk_events.total" in html


def test_dashboard_static_page_renders_data_health_panel() -> None:
    html = Path("dashboard/static/index.html").read_text(encoding="utf-8")

    assert "dataHealth" in html
    assert "/api/data_health" in html
    assert "Data health" in html
    assert "recent_failures" in html


def test_dashboard_static_page_uses_live_running_state_for_status_badge() -> None:
    html = Path("dashboard/static/index.html").read_text(encoding="utf-8")

    assert "isRunning" in html
    assert "status.ws_connected" in html


def test_dashboard_static_page_uses_full_width_trading_terminal_layout() -> None:
    html = Path("dashboard/static/index.html").read_text(encoding="utf-8")

    assert "terminal-shell" in html
    assert "operations-grid" in html
    assert "max-w-[1500px]" not in html
    assert "mx-auto" not in html


def test_dashboard_static_page_avoids_emoji_action_icons() -> None:
    html = Path("dashboard/static/index.html").read_text(encoding="utf-8")

    assert "🎲" not in html
    assert "▶" not in html
    assert "⏸" not in html
