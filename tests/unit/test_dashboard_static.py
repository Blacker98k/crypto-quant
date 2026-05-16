"""Dashboard static-page contract tests.

只测试 ``index.html`` 与后端 API、数据契约、关键操作链路的"功能性断言"。
视觉细节（具体阴影值、CSS class 名称、边框圆角等）由 UI 设计自由迭代，
不做硬断言以免每次重设计都要改测试。
"""

from __future__ import annotations

from pathlib import Path

INDEX_PATH = Path("dashboard/static/index.html")


def _html() -> str:
    return INDEX_PATH.read_text(encoding="utf-8")


# ─── 1. 与后端 API 的契约 ──────────────────────────────────────────────


def test_dashboard_static_page_calls_all_required_apis() -> None:
    html = _html()
    for endpoint in [
        "/api/status",
        "/api/prices",
        "/api/orders",
        "/api/fills",
        "/api/positions",
        "/api/strategies",
        "/api/paper_metrics",
        "/api/data_health",
        "/api/price_history",
        "/api/balance_history",
        "/api/control",
        "/api/small_live/preflight",
        "/api/small_live/order",
    ]:
        assert endpoint in html, f"missing API hookup: {endpoint}"


def test_dashboard_static_page_renders_paper_metrics_panel() -> None:
    html = _html()
    assert "paperMetrics" in html
    assert "/api/paper_metrics" in html
    assert "filled_notional" in html
    # cash_pnl 现在通过 paperMetrics.fills.* 在脚本里读取，模板可省略
    # 但 reactive 默认值仍然要暴露这个字段，作为契约
    assert "cash_pnl" in html


def test_dashboard_static_page_renders_data_health_panel() -> None:
    html = _html()
    assert "dataHealth" in html
    assert "/api/data_health" in html
    assert "数据健康" in html
    assert "recent_failures" in html


def test_dashboard_static_page_shows_risk_event_details() -> None:
    html = _html()
    for token in [
        "riskEventTitle",
        "riskEventReason",
        "riskEventPayload",
        "riskEventTypeLabel",
        "riskEventReasonLabel",
        "RISK_PAYLOAD_KEY_LABELS",
        "strategyName(ev.strategy)",
        "ev.source",
        "ev.strategy",
        "ev.symbol",
        "ev.related_id",
    ]:
        assert token in html


def test_dashboard_static_page_shows_fill_pnl_column() -> None:
    html = _html()
    for token in [
        "当笔盈亏",
        "f.net_pnl",
        "filteredFills.length===0",
        "colspan=\"7\"",
    ]:
        assert token in html


def test_dashboard_static_page_syncs_prices_from_websocket() -> None:
    html = _html()
    for token in [
        "syncLivePrices",
        "msg.prices",
        "msg.latest_prices",
        "Object.assign(prices[sym]",
    ]:
        assert token in html


# ─── 2. 关键状态与操作 ────────────────────────────────────────────────


def test_dashboard_static_page_uses_live_running_state_for_status_badge() -> None:
    html = _html()
    assert "isRunning" in html
    assert "status.ws_connected" in html


def test_dashboard_static_page_shows_data_source_identity() -> None:
    html = _html()
    assert "status.db_path" in html
    assert "status.data_started_at" in html
    assert "数据源" in html


def test_dashboard_static_page_avoids_emoji_action_icons() -> None:
    html = _html()
    # 这些 emoji 在过去的 PR 里被明确判定与本系统的工业风不一致
    assert "🎲" not in html
    assert "▶" not in html
    assert "⏸" not in html


def test_dashboard_static_page_labels_manual_control_as_strategy_evaluation() -> None:
    html = _html()
    assert "触发评估" in html
    assert "本次没有核心策略信号" in html
    assert "随机下单" not in html


def test_dashboard_static_page_refreshes_prices_quickly() -> None:
    html = _html()
    assert "pricePollTimer" in html
    assert "setInterval(loadPrices, 1000)" in html
    assert "await asyncio.sleep(1)" in Path("dashboard/server.py").read_text(encoding="utf-8")


def test_dashboard_static_page_uses_professional_candlestick_chart() -> None:
    html = _html()
    assert "lightweight-charts" in html
    assert "addCandlestickSeries" in html
    assert "addHistogramSeries" in html
    # 价格图表挂载点不再是 <canvas>（lightweight-charts 自己渲染 SVG）
    assert '<canvas id="priceChart"' not in html


def test_dashboard_static_page_keeps_balance_chart_canvas() -> None:
    """资金曲线仍然用 Chart.js（canvas 渲染）。"""
    html = _html()
    assert 'id="balanceChart"' in html


# ─── 3. 关键中文运营术语 ──────────────────────────────────────────────


def test_dashboard_static_page_uses_chinese_operational_metrics() -> None:
    """保留必须可见的中文运营术语；细节标签可以随设计迭代。"""
    html = _html()
    # 必须出现的领域核心术语
    for label in [
        "账户权益",
        "可用余额",
        "已用保证金",
        "名义仓位",
        "浮动",
        "数据健康",
    ]:
        assert label in html, f"missing operational label: {label!r}"

    # 必须避免的英文标签（保持中文一致性）
    for forbidden in ["Data health", "Cash PnL"]:
        assert forbidden not in html


# ─── 4. 浅色主题（视觉约束的最小集，允许自由调色） ─────────────────────


def test_dashboard_static_page_uses_light_theme() -> None:
    """看板必须是浅色风格。这个断言是个最小约束："""

    html = _html()
    # 不能是深色看板
    assert "--bg:#081018" not in html
    assert "background:#0f172a" not in html
    # 必须有明亮背景（任意浅色变量名都可，只要值是浅色）
    assert "--bg:#fbfbfa" in html or "--bg:#ffffff" in html or "--bg:#fafafa" in html
    # 表面色必须是白
    assert "--surface:#ffffff" in html or "--panel:#ffffff" in html


# ─── 5. 策略详情含完整 PnL 拆解（业务约束） ───────────────────────────


def test_dashboard_static_page_shows_strategy_pnl_breakdown() -> None:
    html = _html()
    for token in [
        "sc.realized_pnl",
        "sc.unrealized_pnl",
        "sc.open_notional",
        "sc.used_margin",
        "sc.fees_paid",
        "sc.margin_roi",
        "sc.win_rate",
        "净实现",
        "浮动",
        "保证金收益率",
    ]:
        assert token in html, f"missing strategy breakdown token: {token!r}"


def test_dashboard_static_page_labels_swing_breakout_strategy() -> None:
    html = _html()
    assert "paper_trend_momentum" in html
    assert "paper_swing_breakout" in html
    assert "5m" in html
    assert "波段突破" in html
    assert "15m" in html
    assert ':title="strategyTitle(sc.name)"' in html
    assert '{{ strategySubtitle(sc.name) }}' in html
    assert '{{ strategySubtitle(ev.name) }}' in html


# ─── 6. 实盘试运行通道（安全约束） ──────────────────────────────────


def test_dashboard_static_page_renders_small_live_control_panel() -> None:
    html = _html()
    assert "smallLive" in html
    assert "/api/small_live/preflight" in html
    assert "/api/small_live/order" in html
    assert "实盘" in html  # tab 或 banner 任一处出现即可
    assert "模拟检查" in html
    assert "提交实盘订单" in html
    assert "I_UNDERSTAND_LIVE_ORDER_RISK" in html
    assert "dryRunSmallLiveOrder" in html
    assert "submitSmallLiveOrder" in html


# ─── 7. 现代字体系统（业务上要求 monospace 数字对齐） ─────────────────


def test_dashboard_static_page_uses_monospace_for_numbers() -> None:
    """数字列必须用 monospace 字体（避免位数不齐）。"""
    html = _html()
    # 至少声明了一种 monospace 字体
    assert "JetBrains Mono" in html or "SF Mono" in html or "Menlo" in html
    # 必须仍然启用 tabular 数字
    assert "tabular-nums" in html
