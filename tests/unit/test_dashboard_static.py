from __future__ import annotations

from pathlib import Path


def test_dashboard_static_page_renders_paper_metrics_panel() -> None:
    html = Path("dashboard/static/index.html").read_text(encoding="utf-8")

    assert "paperMetrics" in html
    assert "/api/paper_metrics" in html
    assert "filled_notional" in html
    assert "cash_pnl" in html
    assert "paperMetrics.risk_events.total" in html


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
    assert "exchange-grid" in html
    assert "bottom-tabs" in html
    assert "max-w-[1500px]" not in html
    assert "mx-auto" not in html


def test_dashboard_static_page_avoids_emoji_action_icons() -> None:
    html = Path("dashboard/static/index.html").read_text(encoding="utf-8")

    assert "🎉" not in html
    assert "▶" not in html
    assert "⏸" not in html


def test_dashboard_static_page_uses_binance_terminal_theme() -> None:
    html = Path("dashboard/static/index.html").read_text(encoding="utf-8")

    assert "--bg:#181a20" in html
    assert "--panel:#1e2329" in html
    assert "--accent:#f0b90b" in html
    assert "--bg:#eef3f8" not in html


def test_dashboard_static_page_uses_chinese_operational_metrics() -> None:
    html = Path("dashboard/static/index.html").read_text(encoding="utf-8")

    for label in [
        "行情覆盖",
        "订单总数",
        "成交笔数",
        "现金盈亏",
        "数据健康",
        "运行日志",
        "平均延迟",
        "最大延迟",
        "持仓名义额",
    ]:
        assert label in html

    for label in ["Data health", "Run logs", "Avg latency", "Max latency", "Cash PnL"]:
        assert label not in html


def test_dashboard_static_page_uses_professional_candlestick_chart() -> None:
    html = Path("dashboard/static/index.html").read_text(encoding="utf-8")

    assert "lightweight-charts" in html
    assert "addCandlestickSeries" in html
    assert "addHistogramSeries" in html
    assert "当前价" in html
    assert "延迟" in html
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
        "exchange-grid",
        "chart-card",
        "market-list",
        "order-flow",
    ]:
        assert token in html

    assert "border-radius:6px" in html
    assert "background:var(--panel)" in html
    assert "grid-template-columns:310px minmax(0,1fr) 360px" in html


def test_dashboard_static_page_uses_binance_inspired_terminal_layout() -> None:
    html = Path("dashboard/static/index.html").read_text(encoding="utf-8")

    for token in [
        "ticker-strip",
        "exchange-grid",
        "market-list",
        "order-flow",
        "strategy-matrix",
        "bottom-tabs",
        "交易矩阵",
        "Top30 主流币池",
        "/api/universe",
        "/api/strategy_matrix",
        "/api/recent_trades",
    ]:
        assert token in html

    assert "#f0b90b" in html.lower()
    assert "Binance logo" not in html


def test_dashboard_static_page_does_not_hardcode_only_btc_eth_symbols() -> None:
    html = Path("dashboard/static/index.html").read_text(encoding="utf-8")

    assert "universe.symbols" in html
    assert "<option>BTCUSDT</option>" not in html
    assert "<option>ETHUSDT</option>" not in html
