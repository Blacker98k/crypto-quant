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
    assert "数据健康" in html
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


def test_dashboard_static_page_uses_light_professional_theme() -> None:
    html = Path("dashboard/static/index.html").read_text(encoding="utf-8")

    assert "--bg:#eef3f8" in html
    assert "--panel:#ffffff" in html
    assert "--text:#0f172a" in html
    assert "--bg:#081018" not in html


def test_dashboard_static_page_uses_chinese_operational_metrics() -> None:
    html = Path("dashboard/static/index.html").read_text(encoding="utf-8")

    for label in [
        "初始基准",
        "账户权益",
        "可用余额",
        "已用保证金",
        "名义仓位",
        "浮动盈亏",
        "今日成交额",
        "行情覆盖",
        "今日订单",
        "今日成交",
        "已实现结果",
        "数据健康",
        "运行日志",
        "平均延迟",
        "最大延迟",
    ]:
        assert label in html

    for label in ["Data health", "Run logs", "Avg latency", "Max latency", "Cash PnL"]:
        assert label not in html
    assert "合约名义额，不是现金余额" in html


def test_dashboard_static_page_uses_professional_candlestick_chart() -> None:
    html = Path("dashboard/static/index.html").read_text(encoding="utf-8")

    assert "lightweight-charts" in html
    assert "addCandlestickSeries" in html
    assert "addHistogramSeries" in html
    assert "当前价" in html
    assert "更新延迟" in html
    assert "<canvas id=\"priceChart\"" not in html


def test_dashboard_static_page_refreshes_prices_quickly() -> None:
    html = Path("dashboard/static/index.html").read_text(encoding="utf-8")

    assert "pricePollTimer" in html
    assert "setInterval(loadPrices, 1000)" in html
    assert "await asyncio.sleep(1)" in Path("dashboard/server.py").read_text(encoding="utf-8")


def test_dashboard_static_page_uses_modern_visual_system() -> None:
    html = Path("dashboard/static/index.html").read_text(encoding="utf-8")

    for token in [
        "command-center",
        "brand-lockup",
        "metric-card",
        "market-workbench",
        "chart-card",
        "side-stack",
        "mini-stat",
    ]:
        assert token in html

    assert "box-shadow:0 18px 48px rgba(15,23,42,.10)" in html
    assert "border-radius:10px" in html
    assert "background:linear-gradient(135deg,#ffffff 0%,#f8fbff 100%)" in html
    assert "width:max-content" in html


def test_dashboard_static_page_aligns_market_panel_with_chart() -> None:
    html = Path("dashboard/static/index.html").read_text(encoding="utf-8")

    assert ".side-stack > .panel:first-child" in html
    assert "align-items:start" in html
    assert "height:645px" in html
    assert ".side-stack > .panel:first-child .panel-body" in html
    assert "overflow:auto" in html
    assert "scrollbar-width:thin" in html
    assert "market-list" in html
    assert ".market-tile .value" in html
    assert "font-size:14px" in html
    assert ".market-change" in html


def test_dashboard_static_page_shows_strategy_pnl_breakdown() -> None:
    html = Path("dashboard/static/index.html").read_text(encoding="utf-8")

    assert "sc.realized_pnl" in html
    assert "sc.unrealized_pnl" in html
    assert "sc.open_notional" in html
    assert "sc.used_margin" in html
    assert "sc.fees_paid" in html
    assert "sc.margin_roi" in html
    assert "sc.win_rate" in html
    assert "净实现" in html
    assert "浮动" in html
    assert "合计" in html
    assert "保证金收益率" in html
