"""实时交易看板——FastAPI + WebSocket 后端（真实行情版）。

用 WsSubscriber 替代 DataSimulator，从 Binance 推送实时数据到看板。
PaperMatchingEngine 基于真实行情做模拟成交。

启动方式: uv run python -m dashboard.server
浏览器打开: http://localhost:8089
"""

from __future__ import annotations

import asyncio
import json
import math
import os
import sqlite3
import sys
import time
from collections.abc import Mapping
from dataclasses import fields
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import yaml
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from core.common.proxy import binance_proxy_url
from core.data.exchange.base import Bar
from core.data.exchange.binance_spot import BinanceSpotAdapter
from core.data.feed import LiveFeed
from core.data.memory_cache import MemoryCache
from core.data.parquet_io import ParquetIO
from core.data.sqlite_repo import SqliteRepo
from core.data.ws_subscriber import WsSubscriber
from core.db.migration_runner import MigrationRunner
from core.execution.order_types import OrderHandle, OrderIntent
from core.execution.paper_engine import PaperMatchingEngine
from core.live.executor import SmallLiveExecutor
from core.live.order_cli import LIVE_ORDER_CONFIRM_VALUE
from core.live.small_live import PaperStatus, SmallLiveConfig, evaluate_small_live_readiness
from core.live.trading_adapter import (
    API_KEY_ENV_VAR,
    API_SECRET_ENV_VAR,
    BinanceSpotCredentials,
    BinanceSpotTradingAdapter,
)
from core.monitor.market_health import summarize_market_health
from core.monitor.paper_metrics import paper_metrics
from core.risk import (
    L2PositionRiskSizer,
    L3PortfolioRiskValidator,
    PortfolioRiskLimits,
    PositionRiskLimits,
)
from dashboard.paper_trading import (
    DEFAULT_TOP30_USDT,
    UNIVERSE_NAME,
    DashboardPaperTrader,
    DashboardRiskPipeline,
    default_dashboard_strategies,
    fetch_binance_recent_klines,
    fetch_binance_top_usdt_symbols,
    upsert_dashboard_universe,
)

# 代理配置（根据环境变量或写死）
_PROXY = binance_proxy_url()
_SYMBOLS = DEFAULT_TOP30_USDT
_FUTURES_PRICE_URL = "https://fapi.binance.com/fapi/v1/ticker/price"
_FUTURES_24H_URL = "https://fapi.binance.com/fapi/v1/ticker/24hr"
_DATA_HEALTH_DEFAULT_WINDOW_MS = 10 * 60_000
_INITIAL_USDT_BALANCE = 1_000.0
_PAPER_MARGIN_LEVERAGE = 25.0
_PAPER_BASE_ORDER_NOTIONAL = 150.0
_PAPER_MAX_OPEN_NOTIONAL = _INITIAL_USDT_BALANCE * 3.0
_LOCAL_TZ = timezone(timedelta(hours=8))


def _paper_strategy_notional_multipliers() -> dict[str, float]:
    return {
        "paper_mean_reversion": 1.0,
    }


# ─── 工兛函数 ──────────────────────────────────────────────────────────────────


def _start_of_day_ts(now_s: float | None = None) -> int:
    now = datetime.fromtimestamp(time.time() if now_s is None else now_s, tz=_LOCAL_TZ)
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    return int(start.timestamp() * 1000)


def _start_of_week_ts(now_s: float | None = None) -> int:
    now = datetime.fromtimestamp(time.time() if now_s is None else now_s, tz=_LOCAL_TZ)
    start = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    return int(start.timestamp() * 1000)


def _pnl_in_window(repo: SqliteRepo, since_ms: int, positions: list[dict] | None = None) -> dict:
    return _pnl_in_window_for_strategies(repo, since_ms, positions=positions)


def _pnl_in_window_for_strategies(
    repo: SqliteRepo,
    since_ms: int,
    positions: list[dict] | None = None,
    *,
    strategies: list[str] | None = None,
) -> dict:
    if strategies:
        until_ms = int(time.time() * 1000)
        realized_pnl = _sum_position_realized_pnl(
            repo,
            strategies=strategies,
            since_ms=since_ms,
            until_ms=until_ms,
        ) - _sum_fees_paid(repo, since_ms=since_ms, until_ms=until_ms, strategies=strategies)
        unrealized_pnl = _positions_unrealized_pnl(positions or [])
        return {
            "n": _count_fills(repo, since_ms=since_ms, until_ms=until_ms, strategies=strategies),
            "pnl": round(realized_pnl + unrealized_pnl, 2),
            "realized_pnl": round(realized_pnl, 2),
            "unrealized_pnl": round(unrealized_pnl, 2),
            "roi": 0.0,
        }
    metrics = paper_metrics(repo._conn, since_ms=since_ms, until_ms=int(time.time() * 1000))
    realized_pnl = float(metrics["fills"]["cash_pnl"])
    unrealized_pnl = _positions_unrealized_pnl(positions or [])
    return {
        "n": metrics["fills"]["total"],
        "pnl": round(realized_pnl + unrealized_pnl, 2),
        "realized_pnl": round(realized_pnl, 2),
        "unrealized_pnl": round(unrealized_pnl, 2),
        "roi": 0.0,
    }


# ─── 实时行情理货器 ────────────────────────────────────────────────────────────


def _apply_rest_price_rows(
    cache: MemoryCache,
    rows: list[dict],
    *,
    captured_at_ms: int,
    allowed_symbols: set[str] | None = None,
) -> None:
    for row in rows:
        symbol = str(row.get("symbol") or "")
        if allowed_symbols is not None and symbol not in allowed_symbols:
            continue
        price = float(row.get("price") or 0)
        if symbol and price > 0:
            cache.update_latest_price(
                symbol,
                price,
                source_ts=captured_at_ms,
                updated_at_ms=captured_at_ms,
            )


def _apply_ticker_24h_rows(rows: list[dict]) -> dict[str, dict[str, float]]:
    stats = {}
    for row in rows:
        symbol = str(row.get("symbol") or "")
        if not symbol:
            continue
        try:
            stats[symbol] = {
                "change_24h": round(float(row.get("priceChangePercent") or 0.0), 2),
                "high_24h": float(row.get("highPrice") or 0.0),
                "low_24h": float(row.get("lowPrice") or 0.0),
                "quote_volume": float(row.get("quoteVolume") or 0.0),
            }
        except (TypeError, ValueError):
            continue
    return stats


class LiveDataFeeder:
    """WS 实时行情 → MemoryCache + Paper 引擎。"""

    def __init__(
        self,
        cache: MemoryCache,
        parquet_io: ParquetIO,
        repo: SqliteRepo,
        engine: PaperMatchingEngine,
        trader: DashboardPaperTrader | None = None,
        proxy: str = "",
        symbols: list[str] | None = None,
    ):
        self._cache = cache
        self._parquet_io = parquet_io
        self._repo = repo
        self._engine = engine
        self._trader = trader
        self._proxy = proxy
        self._preferred_proxy = proxy
        self._symbols = list(symbols or _SYMBOLS)
        self._ws: WsSubscriber | None = None
        self._exchange: BinanceSpotAdapter | None = None
        self._exchange_task: asyncio.Task | None = None
        self._bar_counter = 0
        self._last_bar_ms = 0
        self._bar_stale_after_ms = 90_000
        self._ws_connect_timeout_sec = 10.0
        self._rest_bar_seen_ts: dict[tuple[str, str], int] = {}
        self._strategy_timeframes = ("1m", "1h", "4h", "1d")
        self._running = False
        self._task: asyncio.Task | None = None
        self._price_task: asyncio.Task | None = None
        self._watchdog_task: asyncio.Task | None = None
        self._rest_bar_task: asyncio.Task | None = None
        self._ws_reconnect_task: asyncio.Task | None = None
        self._warmup_task: asyncio.Task | None = None
        self._ticker_24h: dict[str, dict[str, float]] = {}

    async def _lazy_load_exchange(self) -> None:
        """后台加载交易所 adapter（不阻塞启动）。"""
        try:
            ex = BinanceSpotAdapter(proxy=self._proxy, timeout_ms=30000)
            await ex._ensure_markets_loaded()
            self._exchange = ex
            # 通知 ws_subscriber exchange 已就绪（用于断线补漏）
            if self._ws:
                self._ws._exchange = ex
            print("  [LiveFeed] 交易所信息加载完成")
        except Exception as e:
            print(f"  [LiveFeed] 交易所加载失败: {e}（WS 仍可工作）")

    async def start(self) -> None:
        """建立 WS 连接并启动数据消费循环。"""
        if self._running:
            return
        self._running = True

        # WS 订阅（不依赖 exchange adapter 预加载）
        await self._refresh_universe(timeframes=("1m",))

        try:
            await self._connect_ws()
            print(f"  [LiveFeed] WS 已连接，订阅: {', '.join(self._symbols)} 1m")
        except Exception as exc:
            self._repo.log_run("dashboard_ws_connect", "fail", note=str(exc)[:200])
            print(f"  [LiveFeed] WS 连接失败，已降级为 REST 轮询兜底: {exc}")

        # 后台加载 exchange（不录响启动）
        self._exchange_task = asyncio.create_task(self._lazy_load_exchange())

        # 启动定时检查引擎
        self._task = asyncio.create_task(self._engine_loop())
        self._price_task = asyncio.create_task(self._rest_price_loop())
        self._watchdog_task = asyncio.create_task(self._bar_watchdog_loop())
        self._rest_bar_task = asyncio.create_task(self._rest_bar_fallback_loop())
        self._ws_reconnect_task = asyncio.create_task(self._ws_reconnect_loop())
        self._warmup_task = asyncio.create_task(
            self._backfill_recent_bars(self._symbols, publish_latest=True)
        )

    def _build_ws(self) -> WsSubscriber:
        self._ws = WsSubscriber(self._cache, self._parquet_io, self._exchange)
        for sym in self._symbols:
            for timeframe in self._strategy_timeframes:
                self._ws.subscribe_candles(sym, timeframe, self._on_bar)
        self._ws.subscribe_tickers(self._symbols)
        return self._ws

    async def _connect_ws(self) -> None:
        last_error: Exception | None = None
        for proxy in self._proxy_candidates():
            ws = self._build_ws()
            try:
                await asyncio.wait_for(
                    ws.connect(proxy=proxy),
                    timeout=self._ws_connect_timeout_sec,
                )
            except Exception as exc:
                last_error = exc
                await ws.close()
                continue
            if proxy != self._proxy:
                self._repo.log_run(
                    "dashboard_ws_proxy_fallback",
                    "ok",
                    note=f"connected with fallback proxy={proxy or 'direct'}"[:200],
                )
            self._proxy = proxy
            print(f"  [LiveFeed] WS connected, subscribed {len(self._symbols)} symbols")
            return
        if last_error:
            raise last_error
        raise RuntimeError("no WS proxy candidate available")

    async def _call_binance_with_proxy_fallback(self, endpoint: str, call):
        last_error: Exception | None = None
        for proxy in self._proxy_candidates():
            try:
                result = await call(proxy)
            except Exception as exc:
                last_error = exc
                continue
            if proxy != self._proxy:
                self._proxy = proxy
                self._repo.log_run(
                    "binance_proxy_fallback",
                    "ok",
                    note=f"{endpoint}: recovered with proxy={proxy or 'direct'}"[:200],
                )
            return result
        if last_error:
            raise last_error
        raise RuntimeError(f"{endpoint}: no proxy candidate available")

    def _proxy_candidates(self) -> list[str]:
        candidates: list[str] = []
        for proxy in (self._proxy, self._preferred_proxy, ""):
            if proxy not in candidates:
                candidates.append(proxy)
        return candidates

    async def _restart_ws(self, reason: str) -> None:
        self._repo.log_run("dashboard_ws_watchdog", "fail", note=reason[:200])
        print(f"  [LiveFeed] watchdog restarting WS: {reason}")
        if self._ws:
            await self._ws.close()
        self._last_bar_ms = 0
        await self._backfill_recent_bars(self._symbols)
        await self._connect_ws()

    async def _ws_reconnect_loop(self, interval_sec: float = 30.0) -> None:
        while self._running:
            await asyncio.sleep(interval_sec)
            if not self._running:
                return
            if _feeder_ws_connected(self):
                continue
            try:
                await self._connect_ws()
            except Exception as exc:
                self._repo.log_run("dashboard_ws_reconnect", "fail", note=str(exc)[:200])
                continue
            self._repo.log_run("dashboard_ws_reconnect", "ok")

    def _on_bar(self, bar) -> None:
        """收到收盘 K 线时触发引擎检查。"""
        self._bar_counter += 1
        self._last_bar_ms = int(time.time() * 1000)
        if self._trader and getattr(bar, "closed", False):
            handles = self._trader.on_bar(bar, now_ms=int(time.time() * 1000))
            if handles:
                print(f"  [PaperTrader] {bar.symbol} generated {len(handles)} paper orders")
        if self._bar_counter % 5 == 0:
            print(f"  [LiveFeed] 已收 {self._bar_counter} 根 K 线, "
                  f"{bar.symbol} {bar.timeframe} c={bar.c:.2f}")

    async def _engine_loop(self) -> None:
        """定时检查挂单 + 记录资金曲线。"""
        while self._running:
            try:
                now_ms = int(time.time() * 1000)
                fills = self._engine.check_pending_orders(now_ms)
                if fills:
                    print(f"  [引擎] {len(fills)} 笔新成交!")
            except Exception:
                pass
            await asyncio.sleep(2)

    async def _bar_watchdog_loop(self) -> None:
        while self._running:
            await asyncio.sleep(30)
            if not self._running or self._bar_counter == 0:
                continue
            age_ms = _feeder_last_bar_age_ms(self)
            if age_ms is not None and age_ms > self._bar_stale_after_ms:
                try:
                    await self._restart_ws(f"bar stream stale for {age_ms}ms")
                except Exception as exc:
                    self._repo.log_run("dashboard_ws_watchdog", "fail", note=str(exc)[:200])

    async def _rest_price_loop(self) -> None:
        """Use REST as a latest-price freshness fallback when WS frames stall."""
        while self._running:
            try:
                await self._refresh_rest_prices()
            except Exception:
                pass
            await asyncio.sleep(2)

    async def _rest_bar_fallback_loop(self) -> None:
        """Backfill recent 1m bars and run strategies if WS bars stop arriving."""
        while self._running:
            await asyncio.sleep(30)
            if not self._running:
                return
            age_ms = _feeder_last_bar_age_ms(self)
            if age_ms is not None and age_ms <= 45_000:
                continue
            try:
                await self._backfill_recent_bars(self._symbols, publish=True)
            except Exception as exc:
                self._repo.log_run("dashboard_rest_bar_fallback", "fail", note=str(exc)[:200])

    async def _refresh_rest_prices(self) -> None:
        import aiohttp

        captured_at_ms = int(time.time() * 1000)
        symbols = _symbols_with_open_positions(self._repo, self._symbols)
        params = {"symbols": json.dumps(symbols)}
        timeout = aiohttp.ClientTimeout(total=5)

        async def fetch_prices(proxy: str):
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(
                    _FUTURES_PRICE_URL,
                    params=params,
                    proxy=proxy or None,
                ) as resp:
                    resp.raise_for_status()
                    return await resp.json()

        rows = await self._call_binance_with_proxy_fallback("binance_futures_prices", fetch_prices)
        _apply_rest_price_rows(
            self._cache,
            rows,
            captured_at_ms=captured_at_ms,
            allowed_symbols=set(symbols),
        )
        if self._trader is not None:
            self._trader.close_legacy_tiny_positions(now_ms=captured_at_ms)
        await self._refresh_24h_tickers()

    async def _refresh_24h_tickers(self) -> None:
        import aiohttp

        symbols = _symbols_with_open_positions(self._repo, self._symbols)
        params = {"symbols": json.dumps(symbols)}
        timeout = aiohttp.ClientTimeout(total=5)

        async def fetch_tickers(proxy: str):
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(
                    _FUTURES_24H_URL,
                    params=params,
                    proxy=proxy or None,
                ) as resp:
                    resp.raise_for_status()
                    return await resp.json()

        rows = await self._call_binance_with_proxy_fallback("binance_futures_24h", fetch_tickers)
        self._ticker_24h = _apply_ticker_24h_rows(rows)

    async def _refresh_universe(self, *, timeframes: tuple[str, ...] | None = None) -> None:
        try:
            symbols = await self._call_binance_with_proxy_fallback(
                "binance_usdt_top30_universe",
                lambda proxy: fetch_binance_top_usdt_symbols(proxy=proxy, limit=30),
            )
        except Exception as exc:
            symbols = DEFAULT_TOP30_USDT
            self._repo.log_run("binance_usdt_top30_universe", "fail", note=str(exc)[:200])
        else:
            self._repo.log_run("binance_usdt_top30_universe", "ok")
        upsert_dashboard_universe(self._repo, symbols)
        self._symbols = symbols
        if self._trader:
            self._trader.replace_symbols(symbols)
        await self._backfill_timeframes(
            symbols,
            timeframes=timeframes or self._strategy_timeframes,
            publish_latest=True,
        )

    async def _backfill_recent_bars(
        self,
        symbols: list[str],
        *,
        publish: bool = False,
        publish_latest: bool = False,
    ) -> None:
        await self._backfill_timeframes(
            symbols,
            timeframes=self._strategy_timeframes,
            publish=publish,
            publish_latest=publish_latest,
        )

    async def _backfill_recent_1m(
        self,
        symbols: list[str],
        *,
        publish: bool = False,
        publish_latest: bool = False,
    ) -> None:
        await self._backfill_timeframes(
            symbols,
            timeframes=("1m",),
            publish=publish,
            publish_latest=publish_latest,
        )

    async def _backfill_timeframes(
        self,
        symbols: list[str],
        *,
        timeframes: tuple[str, ...],
        publish: bool = False,
        publish_latest: bool = False,
    ) -> None:
        for symbol in symbols:
            for timeframe in timeframes:
                limit = 120 if timeframe in {"1h", "4h", "1d"} else 20
                try:
                    bars = await self._call_binance_with_proxy_fallback(
                        f"binance_{timeframe}_warmup_{symbol}",
                        lambda proxy, symbol=symbol, timeframe=timeframe, limit=limit: fetch_binance_recent_klines(
                            symbol, timeframe, proxy=proxy, limit=limit
                        ),
                    )
                except Exception as exc:
                    self._repo.log_run(f"binance_{timeframe}_warmup_{symbol}", "fail", note=str(exc)[:200])
                    continue
                for bar in bars:
                    self._cache.push_bar(bar, update_latest=timeframe == "1m")
                if bars:
                    self._parquet_io.write_bars(bars)
                    closed_bars = [bar for bar in bars if bar.closed]
                    if not closed_bars:
                        continue
                    latest_ts = max(bar.ts for bar in closed_bars)
                    key = (symbol, timeframe)
                    if publish_latest:
                        latest_bar = max(closed_bars, key=lambda item: item.ts)
                        last_seen = self._rest_bar_seen_ts.get(key, 0)
                        if latest_bar.ts > last_seen:
                            self._on_bar(latest_bar)
                        self._rest_bar_seen_ts[key] = max(latest_ts, last_seen)
                    elif publish:
                        last_seen = self._rest_bar_seen_ts.get(key, 0)
                        for bar in sorted(closed_bars, key=lambda item: item.ts):
                            if bar.ts > last_seen:
                                self._on_bar(bar)
                        self._rest_bar_seen_ts[key] = max(latest_ts, last_seen)
                    else:
                        self._rest_bar_seen_ts[key] = max(latest_ts, self._rest_bar_seen_ts.get(key, 0))
                    self._repo.log_run(f"binance_{timeframe}_warmup_{symbol}", "ok")

    async def stop(self) -> None:
        """关闭连接。"""
        self._running = False
        if self._task:
            self._task.cancel()
        if self._price_task:
            self._price_task.cancel()
        if self._watchdog_task:
            self._watchdog_task.cancel()
        if self._rest_bar_task:
            self._rest_bar_task.cancel()
        if self._ws_reconnect_task:
            self._ws_reconnect_task.cancel()
        if self._warmup_task:
            self._warmup_task.cancel()
        if self._ws:
            await self._ws.close()
        if self._exchange:
            await self._exchange.close()
        print("  [LiveFeed] 已停止")


# ─── create_app ──────────────────────────────────────────────────────────────


def create_app(
    cache: MemoryCache,
    repo: SqliteRepo,
    parquet_io: ParquetIO,
    engine: PaperMatchingEngine,
    feeder: LiveDataFeeder,
    static_dir: Path,
    trader: DashboardPaperTrader | None = None,
) -> FastAPI:
    app = FastAPI(title="crypto-quant Dashboard (Live)", version="0.2.0")
    app.state.cache = cache
    app.state.repo = repo
    app.state.parquet_io = parquet_io
    app.state.engine = engine
    app.state.feeder = feeder
    app.state.initial_balance = _INITIAL_USDT_BALANCE
    app.state.paper_leverage = _PAPER_MARGIN_LEVERAGE
    app.state.balance_history: list[dict] = []
    app.state.trader = trader
    app.state.active_strategy_ids = trader.strategies if trader else None
    app.state.small_live_config_path = Path(os.getenv("CQ_SMALL_LIVE_CONFIG", "config/small_live.yml"))

    # ─── REST API ──────────────────────────────────────────────────────────

    @app.get("/api/status")
    def api_status():
        prices = cache.latest_prices_all()
        orders = repo.get_open_orders()
        active_strategies = app.state.active_strategy_ids
        positions = _compute_positions(repo, cache, strategies=active_strategies)
        account = _account_snapshot(
            repo,
            positions,
            initial_balance=app.state.initial_balance,
            leverage=app.state.paper_leverage,
            strategies=active_strategies,
        )
        risk_counts = _risk_event_counts(repo)
        simulation_running = _feeder_simulation_running(feeder)
        ws_connected = _feeder_ws_connected(feeder)
        last_bar_age_ms = _feeder_last_bar_age_ms(feeder)
        market_data_stale = _feeder_market_data_stale(feeder)
        data_source = _data_source_identity(repo, strategies=active_strategies)
        return {
            "mode": "live_paper",
            **data_source,
            "simulation_running": simulation_running,
            "ws_connected": ws_connected,
            "bars_received": feeder._bar_counter,
            "last_bar_age_ms": last_bar_age_ms,
            "market_data_stale": market_data_stale,
            "initial_balance": account["initial_balance"],
            "portfolio_value": account["equity"],
            "account_equity": account["equity"],
            "usdt_balance": account["available_balance"],
            "available_balance": account["available_balance"],
            "used_margin": account["used_margin"],
            "open_notional": account["open_notional"],
            "gross_realized_pnl": account["gross_realized_pnl"],
            "fees_paid": account["fees_paid"],
            "net_realized_pnl": account["realized_pnl"],
            "realized_pnl": account["realized_pnl"],
            "unrealized_pnl": account["unrealized_pnl"],
            "paper_leverage": account["leverage"],
            "open_positions_n": len(positions),
            "open_orders_n": len(orders),
            "risk_events_n": risk_counts["total"],
            "raw_risk_events_n": risk_counts["raw_total"],
            "critical_risk_events_n": risk_counts["critical"],
            "day_pnl": _pnl_in_window_for_strategies(
                repo,
                _start_of_day_ts(),
                positions,
                strategies=active_strategies,
            ),
            "week_pnl": _pnl_in_window_for_strategies(
                repo,
                _start_of_week_ts(),
                positions,
                strategies=active_strategies,
            ),
            "latest_prices": prices,
        }

    @app.get("/api/prices")
    def api_prices():
        return _price_snapshot(cache, getattr(feeder, "_ticker_24h", {}))

    @app.get("/api/universe")
    def api_universe():
        rows = repo.list_symbols(exchange="binance", stype="perp", universe=UNIVERSE_NAME)
        prices = cache.latest_prices_all()
        price_meta = cache.latest_price_meta_all()
        now_ms = int(time.time() * 1000)
        symbols = []
        for row in rows:
            symbol = row["symbol"]
            updated_at = price_meta.get(symbol, {}).get("updated_at")
            symbols.append(
                {
                    "symbol": symbol,
                    "base": row["base"],
                    "quote": row["quote"],
                    "universe": row["universe"],
                    "price": prices.get(symbol),
                    "age_ms": max(0, now_ms - int(updated_at)) if updated_at else None,
                }
            )
        return {"name": UNIVERSE_NAME, "count": len(symbols), "symbols": symbols}

    @app.get("/api/price_history")
    def api_price_history(symbol: str = "BTCUSDT", tf: str = "1m", n: int = 200):
        cached = cache.get_bars(symbol, tf, n=n)
        historical = parquet_io.read_bars(symbol, tf, n=n) if len(cached) < n else []
        bars_by_ts = {bar.ts: bar for bar in historical}
        bars_by_ts.update({bar.ts: bar for bar in cached})
        bars = [bars_by_ts[ts] for ts in sorted(bars_by_ts)][-n:]
        return [{"ts": b.ts, "o": b.o, "h": b.h, "l": b.l, "c": b.c, "v": b.v}
                for b in bars]

    @app.get("/api/orders")
    def api_orders(limit: int = 50, status: str = ""):
        clauses = []
        params: list[object] = []
        if status:
            clauses.append("o.status = ?")
            params.append(status)
        if app.state.active_strategy_ids:
            placeholders = ",".join("?" for _ in app.state.active_strategy_ids)
            clauses.append(f"o.strategy_version IN ({placeholders})")
            params.extend(app.state.active_strategy_ids)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = repo._conn.execute(
            "SELECT o.*, s.symbol as sym FROM orders o "
            "LEFT JOIN symbols s ON o.symbol_id = s.id "
            f"{where} ORDER BY o.placed_at DESC LIMIT ?",
            (*params, limit),
        ).fetchall()
        return [_order_row(r) for r in rows]

    @app.get("/api/fills")
    def api_fills(limit: int = 50):
        where = ""
        params: list[object] = []
        if app.state.active_strategy_ids:
            placeholders = ",".join("?" for _ in app.state.active_strategy_ids)
            where = f"WHERE o.strategy_version IN ({placeholders})"
            params.extend(app.state.active_strategy_ids)
        rows = repo._conn.execute(
            "SELECT f.*, s.symbol as sym, o.side, o.strategy_version as strategy "
            "FROM fills f JOIN orders o ON f.order_id = o.id "
            "LEFT JOIN symbols s ON o.symbol_id = s.id "
            f"{where} ORDER BY f.ts DESC LIMIT ?", (*params, limit),
        ).fetchall()
        pnl_by_fill_id = _fill_pnl_by_id(repo, strategies=app.state.active_strategy_ids)
        return [{"id": r["id"], "order_id": r["order_id"],
                 "symbol": r["sym"] or "?", "side": r["side"],
                 "strategy": r["strategy"], "price": r["price"],
                 "quantity": r["quantity"], "fee": r["fee"],
                 "gross_pnl": pnl_by_fill_id.get(int(r["id"]), {}).get("gross_pnl", 0.0),
                 "net_pnl": pnl_by_fill_id.get(int(r["id"]), {}).get("net_pnl", 0.0),
                 "fee_currency": r["fee_currency"],
                 "is_maker": r["is_maker"], "ts": r["ts"]}
                for r in rows]

    @app.get("/api/positions")
    def api_positions():
        return _compute_positions(repo, cache, strategies=app.state.active_strategy_ids)

    @app.get("/api/balance_history")
    def api_balance_history(limit: int = 500):
        hist = app.state.balance_history
        return hist[-limit:] if len(hist) > limit else hist

    @app.get("/api/risk_events")
    def api_risk_events(limit: int = 50, since_ms: int | None = None):
        rows = _recent_actionable_risk_events(repo, limit=max(1, min(limit, 500)), since_ms=since_ms)
        return [_risk_event_row(row) for row in rows]

    @app.get("/api/paper_metrics")
    def api_paper_metrics(since_ms: int | None = None, until_ms: int | None = None):
        start_ms = _start_of_day_ts() if since_ms is None else since_ms
        end_ms = int(time.time() * 1000) + 1 if until_ms is None else until_ms
        return paper_metrics(
            repo._conn,
            since_ms=start_ms,
            until_ms=end_ms,
            strategies=app.state.active_strategy_ids,
        )

    @app.get("/api/data_health")
    def api_data_health(limit: int = 100, since_ms: int | None = None):
        start_ms = (
            int(time.time() * 1000) - _DATA_HEALTH_DEFAULT_WINDOW_MS
            if since_ms is None
            else since_ms
        )
        payload = summarize_market_health(repo, limit=limit, since_ms=start_ms)
        live_feed = _live_feed_health(feeder)
        payload["live_feed"] = live_feed
        if live_feed["status"] == "degraded":
            payload["status"] = "degraded"
        elif payload["status"] == "idle" and live_feed["status"] == "ok":
            payload["status"] = "ok"
        return payload

    @app.get("/api/small_live/preflight")
    def api_small_live_preflight():
        return _small_live_preflight_payload(app, repo, cache, feeder)

    @app.post("/api/small_live/order")
    async def api_small_live_order(payload: dict[str, object]):
        config, config_blockers = _load_small_live_config_for_app(app)
        readiness = _small_live_readiness(app, repo, cache, feeder, config)
        blockers = [*config_blockers, *readiness.blockers]
        intent = _small_live_intent_from_payload(payload)
        dry_run = bool(payload.get("dry_run", True))
        confirmation = str(payload.get("confirmation") or "")

        if not dry_run and confirmation != LIVE_ORDER_CONFIRM_VALUE:
            blockers.append("missing_live_order_confirmation")
        if not dry_run and not _small_live_credentials_present(os.environ):
            blockers.append("missing_binance_spot_credentials")

        if blockers:
            return {
                "ready": readiness.ready,
                "dry_run": dry_run,
                "submitted": False,
                "would_submit": False,
                "blockers": blockers,
                "warnings": readiness.warnings,
                "order": _small_live_order_summary(intent),
            }

        adapter = _DryRunLiveAdapter() if dry_run else BinanceSpotTradingAdapter(
            credentials=BinanceSpotCredentials.from_env(os.environ),
            proxy=_PROXY,
        )
        try:
            result = await SmallLiveExecutor(
                adapter=adapter,
                config=config,
                readiness=readiness,
            ).submit_order(intent, now_ms=int(time.time() * 1000))
        except Exception as exc:
            return {
                "ready": readiness.ready,
                "dry_run": dry_run,
                "submitted": False,
                "would_submit": False,
                "blockers": [str(exc)],
                "warnings": readiness.warnings,
                "order": _small_live_order_summary(intent),
            }

        return {
            "ready": True,
            "dry_run": dry_run,
            "submitted": not dry_run,
            "would_submit": dry_run,
            "blockers": [],
            "warnings": readiness.warnings,
            "order": _small_live_order_summary(intent),
            "entry": _small_live_handle_summary(result.entry),
            "stop_loss": (
                _small_live_handle_summary(result.stop_loss)
                if result.stop_loss is not None
                else None
            ),
        }

    @app.get("/api/strategies")
    def api_strategies():
        active_strategies = app.state.active_strategy_ids
        strategy_where = ""
        strategy_params: list[object] = []
        if active_strategies:
            placeholders = ",".join("?" for _ in active_strategies)
            strategy_where = f"WHERE strategy_version IN ({placeholders})"
            strategy_params.extend(active_strategies)
        rows = repo._conn.execute(
            "SELECT strategy_version, COUNT(*) AS orders_count "
            f"FROM orders {strategy_where} GROUP BY strategy_version ORDER BY strategy_version",
            strategy_params,
        ).fetchall()
        fill_where = ""
        fill_params: list[object] = []
        if active_strategies:
            placeholders = ",".join("?" for _ in active_strategies)
            fill_where = f"WHERE o.strategy_version IN ({placeholders})"
            fill_params.extend(active_strategies)
        fills = repo._conn.execute(
            "SELECT o.strategy_version, COUNT(f.id) AS fills_count "
            "FROM orders o LEFT JOIN fills f ON f.order_id = o.id "
            f"{fill_where} GROUP BY o.strategy_version",
            fill_params,
        ).fetchall()
        fills_by_strategy = {row["strategy_version"]: row["fills_count"] for row in fills}
        pnl_by_strategy = _strategy_pnl_summary(repo, cache)
        win_rate_by_strategy = _strategy_win_rates(repo)
        payload = [
            {
                "name": row["strategy_version"] or "unknown",
                "orders_count": row["orders_count"],
                "fills_count": fills_by_strategy.get(row["strategy_version"], 0),
                "win_rate": win_rate_by_strategy.get(row["strategy_version"] or "unknown", 0.0),
                **pnl_by_strategy.get(
                    row["strategy_version"],
                    {
                        "gross_realized_pnl": 0.0,
                        "fees_paid": 0.0,
                        "realized_pnl": 0.0,
                        "unrealized_pnl": 0.0,
                        "total_pnl": 0.0,
                        "open_notional": 0.0,
                        "used_margin": 0.0,
                        "notional_roi": 0.0,
                        "margin_roi": 0.0,
                        "roi": 0.0,
                    },
                ),
            }
            for row in rows
        ]
        seen = {row["name"] for row in payload}
        if trader:
            for strategy in trader.strategies:
                if strategy not in seen:
                    payload.append(
                        {
                            "name": strategy,
                            "orders_count": 0,
                            "fills_count": 0,
                            "win_rate": win_rate_by_strategy.get(strategy, 0.0),
                            **pnl_by_strategy.get(
                                strategy,
                                {
                                    "gross_realized_pnl": 0.0,
                                    "fees_paid": 0.0,
                                    "realized_pnl": 0.0,
                                    "unrealized_pnl": 0.0,
                                    "total_pnl": 0.0,
                                    "open_notional": 0.0,
                                    "used_margin": 0.0,
                                    "notional_roi": 0.0,
                                    "margin_roi": 0.0,
                                    "roi": 0.0,
                                },
                            ),
                        }
                    )
        return payload

    @app.get("/api/strategy_matrix")
    def api_strategy_matrix():
        if trader:
            return trader.strategy_matrix()
        return {"symbols": [], "strategies": [], "cells": []}

    @app.get("/api/recent_trades")
    def api_recent_trades(
        limit: int = 80,
        symbol: str | None = None,
        strategy_id: str | None = None,
    ):
        clauses = []
        params: list[object] = []
        if symbol:
            clauses.append("s.symbol = ?")
            params.append(symbol)
        if strategy_id:
            clauses.append("o.strategy_version = ?")
            params.append(strategy_id)
        elif app.state.active_strategy_ids:
            placeholders = ",".join("?" for _ in app.state.active_strategy_ids)
            clauses.append(f"o.strategy_version IN ({placeholders})")
            params.extend(app.state.active_strategy_ids)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = repo._conn.execute(
            "SELECT f.*, s.symbol as sym, o.side, o.strategy_version as strategy "
            "FROM fills f JOIN orders o ON f.order_id = o.id "
            "LEFT JOIN symbols s ON o.symbol_id = s.id "
            f"{where} ORDER BY f.ts DESC LIMIT ?",
            (*params, max(1, min(limit, 500))),
        ).fetchall()
        return [
            {
                "id": row["id"],
                "order_id": row["order_id"],
                "symbol": row["sym"] or "?",
                "side": row["side"],
                "strategy": row["strategy"],
                "price": row["price"],
                "quantity": row["quantity"],
                "fee": row["fee"],
                "notional": round(row["price"] * row["quantity"], 4),
                "ts": row["ts"],
            }
            for row in rows
        ]

    @app.post("/api/control")
    async def api_control(payload: dict[str, str]):
        action = payload.get("action", "")
        if action == "start":
            await feeder.start()
        elif action == "stop":
            await feeder.stop()
        elif action == "random_order":
            handles: list = []
            message = "no_core_strategy_signal"
            if trader:
                symbol = trader.symbols[0] if trader.symbols else "BTCUSDT"
                price = cache.latest_price(symbol) or 100.0
                now_ms = int(time.time() * 1000)
                # 1) 用每个核心策略支持的真实 timeframe 各触发一次评估
                for tf in ("4h", "1h", "1m"):
                    extra = trader.on_bar(
                        Bar(
                            symbol=symbol,
                            timeframe=tf,
                            ts=now_ms,
                            o=price,
                            h=price * 1.002,
                            l=price * 0.998,
                            c=price,
                            v=1.0,
                            closed=True,
                        ),
                        now_ms=now_ms,
                    )
                    handles.extend(extra)
                if handles:
                    message = "strategy_order_generated"
            orders_generated = len(handles)
        else:
            orders_generated = 0
            message = "unknown_action"
        if action in {"start", "stop"}:
            orders_generated = 0
            message = f"{action}_ok"
        return {
            "ok": action in {"start", "stop", "random_order"},
            "action": action,
            "orders_generated": orders_generated,
            "message": message,
            "simulation_running": _feeder_simulation_running(feeder),
        }

    # ─── WebSocket ────────────────────────────────────────────────────────

    @app.websocket("/ws")
    async def ws_endpoint(ws: WebSocket):
        await ws.accept()
        try:
            while True:
                latest_prices = cache.latest_prices_all()
                price_rows = _price_snapshot(cache, getattr(feeder, "_ticker_24h", {}))
                positions = _compute_positions(repo, cache, strategies=app.state.active_strategy_ids)
                account = _account_snapshot(
                    repo,
                    positions,
                    initial_balance=app.state.initial_balance,
                    leverage=app.state.paper_leverage,
                    strategies=app.state.active_strategy_ids,
                )
                _append_balance_history(app.state.balance_history, account)
                recent_orders = repo._conn.execute(
                    "SELECT o.*, s.symbol as sym FROM orders o "
                    "LEFT JOIN symbols s ON o.symbol_id = s.id "
                    "ORDER BY o.placed_at DESC LIMIT 5"
                ).fetchall()
                recent_fills = repo._conn.execute(
                    "SELECT f.*, s.symbol as sym, o.side FROM fills f "
                    "JOIN orders o ON f.order_id = o.id "
                    "LEFT JOIN symbols s ON o.symbol_id = s.id "
                    "ORDER BY f.ts DESC LIMIT 5"
                ).fetchall()
                await ws.send_json({
                    "initial_balance": account["initial_balance"],
                    "portfolio_value": account["equity"],
                    "account_equity": account["equity"],
                    "usdt_balance": account["available_balance"],
                    "available_balance": account["available_balance"],
                    "used_margin": account["used_margin"],
                    "open_notional": account["open_notional"],
                    "gross_realized_pnl": account["gross_realized_pnl"],
                    "fees_paid": account["fees_paid"],
                    "net_realized_pnl": account["realized_pnl"],
                    "realized_pnl": account["realized_pnl"],
                    "unrealized_pnl": account["unrealized_pnl"],
                    "paper_leverage": account["leverage"],
                    "simulation_running": _feeder_simulation_running(feeder),
                    "ws_connected": _feeder_ws_connected(feeder),
                    "bars_received": feeder._bar_counter,
                    "last_bar_age_ms": _feeder_last_bar_age_ms(feeder),
                    "market_data_stale": _feeder_market_data_stale(feeder),
                    "latest_prices": latest_prices,
                    "prices": price_rows,
                    "open_positions_n": len(positions),
                    "positions": positions,
                    "recent_orders": [_order_row(r) for r in recent_orders],
                    "recent_fills": [
                        {"symbol": r["sym"] or "?", "side": r["side"],
                         "price": r["price"], "quantity": r["quantity"],
                         "fee": r["fee"], "ts": r["ts"]}
                        for r in recent_fills
                    ],
                })
                await asyncio.sleep(1)
        except WebSocketDisconnect:
            pass

    # ─── 静态文件 ────────────────────────────────────────────────────────

    @app.get("/")
    def index():
        return FileResponse(static_dir / "index.html")

    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    return app


# ─── 辅助 ──────────────────────────────────────────────────────────────────────


_RISK_TYPE_LABELS = {
    "order_rejected": "订单拒绝",
    "paper_signal_skipped": "纸面信号跳过",
    "risk_rejected": "风控拒单",
}
_RISK_REASON_LABELS = {
    "cooldown": "冷却期内跳过",
    "gross_leverage": "组合杠杆过高",
    "min_notional": "低于最小下单金额",
    "symbol_order_cap": "单币种订单数限制",
    "portfolio_notional_cap": "组合名义仓位限制",
    "daily_trade_cap": "日内交易次数限制",
    "no_stop_loss": "缺少止损",
    "position_cap": "仓位上限",
    "unknown_symbol": "未知交易对",
}
_RISK_SOURCE_LABELS = {
    "L1": "订单风控",
    "L2": "仓位风控",
    "L3": "组合风控",
    "dashboard_trader": "模拟交易调度",
}
_RISK_SEVERITY_LABELS = {
    "info": "提示",
    "warn": "警告",
    "warning": "警告",
    "medium": "警告",
    "critical": "严重",
    "high": "严重",
    "danger": "严重",
    "error": "严重",
}


def _order_row(r) -> dict:
    return {
        "id": r["id"], "client_order_id": r["client_order_id"],
        "symbol": r["sym"] or "?", "side": r["side"], "type": r["type"],
        "price": r["price"], "stop_price": r["stop_price"],
        "quantity": r["quantity"], "filled_qty": r["filled_qty"],
        "avg_fill_price": r["avg_fill_price"], "status": r["status"],
        "purpose": r["purpose"], "strategy": r["strategy_version"],
        "placed_at": r["placed_at"],
    }


def _risk_event_row(r) -> dict:
    payload = r["payload"]
    if isinstance(payload, str) and payload:
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError:
            payload = {"raw": payload}
    if not isinstance(payload, dict):
        payload = {"raw": payload}
    reason = _risk_payload_text(payload, "reason")
    symbol = _risk_payload_text(payload, "symbol")
    strategy = _risk_payload_text(payload, "strategy")
    type_label = _risk_label(_RISK_TYPE_LABELS, r["type"])
    reason_label = _risk_label(_RISK_REASON_LABELS, reason)
    source_label = _risk_label(_RISK_SOURCE_LABELS, r["source"])
    severity_label = _risk_label(_RISK_SEVERITY_LABELS, r["severity"])
    detail = " / ".join(part for part in (reason_label, symbol, strategy) if part)
    return {
        "id": r["id"],
        "type": r["type"],
        "action": r["type"],
        "type_label": type_label,
        "severity": r["severity"],
        "severity_label": severity_label,
        "source": r["source"],
        "source_label": source_label,
        "related_id": r["related_id"],
        "payload": payload,
        "reason": reason,
        "reason_label": reason_label,
        "symbol": symbol,
        "strategy": strategy,
        "detail": detail,
        "captured_at": r["captured_at"],
        "ts": r["captured_at"],
    }


def _risk_payload_text(payload: dict, key: str) -> str | None:
    value = payload.get(key)
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _risk_label(labels: dict[str, str], value: object) -> str:
    text = str(value or "").strip()
    return labels.get(text, text)


def _risk_event_counts(repo: SqliteRepo) -> dict[str, int]:
    row = repo._conn.execute(
        "SELECT COUNT(*) AS total, "
        "SUM(CASE WHEN severity='critical' THEN 1 ELSE 0 END) AS critical "
        "FROM risk_events "
        f"WHERE {_actionable_risk_sql()}"
    ).fetchone()
    raw_total = repo._conn.execute("SELECT COUNT(*) AS n FROM risk_events").fetchone()
    return {
        "total": int(row["total"] or 0),
        "raw_total": int(raw_total["n"] or 0),
        "critical": int(row["critical"] or 0),
    }


def _price_snapshot(
    cache: MemoryCache,
    ticker_24h: dict[str, dict[str, float]] | None = None,
    *,
    now_ms: int | None = None,
) -> dict[str, dict[str, float | int | None]]:
    prices = cache.latest_prices_all()
    price_meta = cache.latest_price_meta_all()
    now = int(time.time() * 1000) if now_ms is None else now_ms
    ticker = ticker_24h or {}
    result: dict[str, dict[str, float | int | None]] = {}
    for sym, price in prices.items():
        bars = cache.get_bars(sym, "1h", n=24)
        change_24h, high_24h, low_24h = 0.0, price, price
        if bars:
            prev = bars[0].c
            if prev > 0:
                change_24h = round((price - prev) / prev * 100, 2)
            high_24h = max(b.h for b in bars)
            low_24h = min(b.l for b in bars)
        if sym in ticker:
            stats = ticker[sym]
            change_24h = stats.get("change_24h", change_24h)
            high_24h = stats.get("high_24h", high_24h)
            low_24h = stats.get("low_24h", low_24h)
        meta = price_meta.get(sym, {})
        updated_at = meta.get("updated_at")
        age_ms = max(0, now - int(updated_at)) if updated_at is not None else None
        result[sym] = {
            "price": price,
            "change_24h": change_24h,
            "high_24h": high_24h,
            "low_24h": low_24h,
            "source_ts": meta.get("source_ts"),
            "updated_at": updated_at,
            "age_ms": age_ms,
            "quote_volume": ticker.get(sym, {}).get("quote_volume", 0.0),
        }
    return result


def _data_source_identity(repo: SqliteRepo, *, strategies: list[str] | None = None) -> dict[str, object]:
    db_rows = repo._conn.execute("PRAGMA database_list").fetchall()
    db_path = ":memory:"
    for row in db_rows:
        if row["name"] == "main" and row["file"]:
            db_path = str(Path(row["file"]).resolve())
            break

    strategy_clause = ""
    params: list[object] = []
    if strategies:
        placeholders = ",".join("?" for _ in strategies)
        strategy_clause = f" WHERE strategy_version IN ({placeholders})"
        params.extend(strategies)
    counts = repo._conn.execute(
        "SELECT "
        "(SELECT COUNT(*) FROM fills f JOIN orders o ON o.id = f.order_id"
        f"{strategy_clause.replace('strategy_version', 'o.strategy_version')}) AS fills_count, "
        f"(SELECT COUNT(*) FROM orders{strategy_clause}) AS orders_count",
        (*params, *params),
    ).fetchone()
    started_where = ""
    started_params: list[object] = []
    if strategies:
        placeholders = ",".join("?" for _ in strategies)
        started_where = f" AND strategy_version IN ({placeholders})"
        started_params.extend(strategies)
    started = repo._conn.execute(
        "SELECT MIN(ts) AS started_at FROM ("
        f"SELECT MIN(placed_at) AS ts FROM orders WHERE placed_at IS NOT NULL{started_where} "
        "UNION ALL "
        "SELECT MIN(f.ts) AS ts FROM fills f JOIN orders o ON o.id = f.order_id "
        f"WHERE f.ts IS NOT NULL{started_where.replace('strategy_version', 'o.strategy_version')}"
        ") WHERE ts IS NOT NULL"
        ,
        (*started_params, *started_params),
    ).fetchone()
    return {
        "workspace_path": str(_PROJECT_ROOT),
        "db_path": db_path,
        "data_started_at": int(started["started_at"]) if started and started["started_at"] is not None else None,
        "orders_count": int(counts["orders_count"] or 0),
        "fills_count": int(counts["fills_count"] or 0),
    }


def _recent_actionable_risk_events(
    repo: SqliteRepo,
    *,
    limit: int,
    since_ms: int | None,
) -> list[dict]:
    if since_ms is None:
        rows = repo._conn.execute(
            "SELECT * FROM risk_events "
            f"WHERE {_actionable_risk_sql()} "
            "ORDER BY captured_at DESC, id DESC LIMIT ?",
            (limit,),
        ).fetchall()
    else:
        rows = repo._conn.execute(
            "SELECT * FROM risk_events "
            f"WHERE captured_at >= ? AND {_actionable_risk_sql()} "
            "ORDER BY captured_at DESC, id DESC LIMIT ?",
            (since_ms, limit),
        ).fetchall()
    return [dict(row) for row in rows]


def _actionable_risk_sql() -> str:
    return (
        "NOT (type = 'paper_signal_skipped' "
        "AND (payload LIKE '%\"reason\": \"cooldown\"%' OR payload LIKE '%\"reason\":\"cooldown\"%' "
        "OR payload LIKE '%\"reason\": \"symbol_order_cap\"%' "
        "OR payload LIKE '%\"reason\":\"symbol_order_cap\"%'))"
    )


def _feeder_running(feeder: LiveDataFeeder) -> bool:
    if not _feeder_simulation_running(feeder):
        return False
    return _feeder_ws_connected(feeder)


def _feeder_simulation_running(feeder: LiveDataFeeder) -> bool:
    if not bool(getattr(feeder, "_running", False)):
        return False
    return not _feeder_market_data_stale(feeder)


def _feeder_ws_connected(feeder: LiveDataFeeder) -> bool:
    ws = getattr(feeder, "_ws", None)
    return bool(getattr(ws, "_running", False))


def _feeder_last_bar_age_ms(feeder: LiveDataFeeder) -> int | None:
    last_bar_ms = int(getattr(feeder, "_last_bar_ms", 0) or 0)
    if last_bar_ms <= 0:
        return None
    return max(0, int(time.time() * 1000) - last_bar_ms)


def _feeder_market_data_stale(feeder: LiveDataFeeder) -> bool:
    if int(getattr(feeder, "_bar_counter", 0) or 0) <= 0:
        return False
    age_ms = _feeder_last_bar_age_ms(feeder)
    if age_ms is None:
        return False
    stale_after_ms = int(getattr(feeder, "_bar_stale_after_ms", 120_000) or 120_000)
    return age_ms > stale_after_ms


def _live_feed_health(feeder: LiveDataFeeder) -> dict[str, object]:
    ws = getattr(feeder, "_ws", None)
    simulation_running = bool(getattr(feeder, "_running", False))
    ws_connected = bool(getattr(ws, "_running", False))
    market_data_stale = _feeder_market_data_stale(feeder)
    status = "ok" if simulation_running and ws_connected and not market_data_stale else "degraded"
    return {
        "status": status,
        "simulation_running": simulation_running,
        "ws_connected": ws_connected,
        "market_data_stale": market_data_stale,
        "bars_received": int(getattr(feeder, "_bar_counter", 0) or 0),
        "last_bar_age_ms": _feeder_last_bar_age_ms(feeder),
    }


class _DryRunLiveAdapter:
    def __init__(self) -> None:
        self._n = 0

    async def place_order(self, intent: OrderIntent, *, now_ms: int) -> OrderHandle:
        self._n += 1
        return OrderHandle(
            client_order_id=intent.client_order_id,
            exchange_order_id=f"dry-run-{self._n}",
            status="accepted",
            submitted_at=now_ms,
        )


def _small_live_preflight_payload(
    app: FastAPI,
    repo: SqliteRepo,
    cache: MemoryCache,
    feeder: LiveDataFeeder,
) -> dict[str, object]:
    config, config_blockers = _load_small_live_config_for_app(app)
    readiness = _small_live_readiness(app, repo, cache, feeder, config)
    blockers = [*config_blockers, *readiness.blockers]
    return {
        "ready": not blockers,
        "mode": config.mode,
        "enabled": config.enabled,
        "spot_only": config.exchange == "binance_spot"
        and not config.allow_futures
        and not config.allow_margin,
        "credentials_present": _small_live_credentials_present(os.environ),
        "allowed_symbols": list(config.allowed_symbols),
        "blockers": blockers,
        "warnings": readiness.warnings,
        "budget_limits_configured": readiness.budget_limits_configured,
        "config_path": str(getattr(app.state, "small_live_config_path", "")),
    }


def _small_live_readiness(
    app: FastAPI,
    repo: SqliteRepo,
    cache: MemoryCache,
    feeder: LiveDataFeeder,
    config: SmallLiveConfig,
) -> Any:
    positions = _compute_positions(repo, cache, strategies=app.state.active_strategy_ids)
    account = _account_snapshot(
        repo,
        positions,
        initial_balance=app.state.initial_balance,
        leverage=app.state.paper_leverage,
        strategies=app.state.active_strategy_ids,
    )
    paper_status = PaperStatus(
        simulation_running=_feeder_running(feeder),
        ws_connected=_feeder_running(feeder),
        market_data_stale=_feeder_market_data_stale(feeder),
        account_equity=float(account["equity"]),
        initial_balance=float(account["initial_balance"]),
        open_notional=float(account["open_notional"]),
    )
    return evaluate_small_live_readiness(config, paper_status, env=os.environ)


def _load_small_live_config_for_app(app: FastAPI) -> tuple[SmallLiveConfig, list[str]]:
    config_path = Path(getattr(app.state, "small_live_config_path", "config/small_live.yml"))
    if not config_path.exists():
        return SmallLiveConfig(), ["config_file_missing"]
    return _load_dataclass(SmallLiveConfig, _load_yaml_mapping(config_path)), []


def _load_yaml_mapping(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    return data if isinstance(data, dict) else {}


def _load_dataclass(cls: type, data: Mapping[str, Any]) -> Any:
    allowed = {field.name for field in fields(cls)}
    kwargs = {key: value for key, value in data.items() if key in allowed}
    if "allowed_symbols" in kwargs and isinstance(kwargs["allowed_symbols"], list):
        kwargs["allowed_symbols"] = tuple(str(item) for item in kwargs["allowed_symbols"])
    return cls(**kwargs)


def _small_live_credentials_present(env: Mapping[str, str]) -> bool:
    return bool(env.get(API_KEY_ENV_VAR, "").strip() and env.get(API_SECRET_ENV_VAR, "").strip())


def _small_live_intent_from_payload(payload: Mapping[str, object]) -> OrderIntent:
    purpose = "exit" if payload.get("purpose") == "exit" else "entry"
    return OrderIntent(
        signal_id=0,
        strategy="dashboard_small_live",
        strategy_version="manual",
        symbol=str(payload.get("symbol") or ""),
        side="sell" if payload.get("side") == "sell" else "buy",
        order_type="limit" if payload.get("order_type") == "limit" else "market",
        quantity=float(payload.get("quantity") or 0.0),
        price=_optional_float(payload.get("price")),
        stop_loss_price=_optional_float(payload.get("stop_loss_price")),
        purpose=purpose,
        reduce_only=purpose == "exit",
        client_order_id=str(payload.get("client_order_id") or ""),
    )


def _optional_float(value: object) -> float | None:
    if value in (None, ""):
        return None
    return float(value)


def _small_live_order_summary(intent: OrderIntent) -> dict[str, object]:
    return {
        "symbol": intent.symbol,
        "side": intent.side,
        "order_type": intent.order_type,
        "purpose": intent.purpose,
        "client_order_id": intent.client_order_id,
        "has_stop_loss": intent.stop_loss_price is not None,
    }


def _small_live_handle_summary(handle: OrderHandle) -> dict[str, object]:
    return {
        "client_order_id": handle.client_order_id,
        "exchange_order_id": handle.exchange_order_id,
        "status": handle.status,
        "submitted_at": handle.submitted_at,
    }


def _compute_positions(repo: SqliteRepo, cache: MemoryCache, *, strategies: list[str] | None = None) -> list[dict]:
    positions = []
    for row in repo.list_open_positions():
        if strategies and str(row["strategy_version"] or "") not in strategies:
            continue
        symbol = repo.get_symbol_by_id(int(row["symbol_id"]))
        sym = str(symbol["symbol"]) if symbol is not None else "BTCUSDT"
        qty = float(row["qty"])
        entry = float(row["avg_entry_price"])
        cur = cache.latest_price(sym) or float(row["current_price"] or entry)
        position_side = str(row["side"])
        if position_side == "long":
            side = "buy"
            unrealized = qty * (cur - entry)
        else:
            side = "sell"
            unrealized = qty * (entry - cur)
        positions.append(
            {
                "symbol": sym,
                "side": side,
                "position_side": position_side,
                "strategy": row["strategy_version"],
                "qty": round(qty, 6),
                "entry_price": _round_display_price(entry, symbol),
                "current_price": _round_display_price(cur, symbol),
                "unrealized_pnl": round(unrealized, 2),
                "realized_pnl": round(float(row["realized_pnl"] or 0.0), 2),
            }
        )
    return positions


def _round_display_price(value: float, symbol: sqlite3.Row | None) -> float:
    tick_size = float(symbol["tick_size"] or 0.0) if symbol is not None else 0.0
    if tick_size > 0 and tick_size < 1:
        decimals = min(8, max(2, math.ceil(-math.log10(tick_size))))
        return round(value, decimals)
    if abs(value) < 1:
        return round(value, 8)
    return round(value, 2)


def _symbols_with_open_positions(repo: SqliteRepo, symbols: list[str]) -> list[str]:
    seen = set()
    result = []
    for symbol in symbols:
        if symbol not in seen:
            seen.add(symbol)
            result.append(symbol)
    for row in repo.list_open_positions():
        symbol = repo.get_symbol_by_id(int(row["symbol_id"]))
        if symbol is None:
            continue
        name = str(symbol["symbol"])
        if name not in seen:
            seen.add(name)
            result.append(name)
    return result


def _fill_pnl_by_id(repo: SqliteRepo, *, strategies: list[str] | None = None) -> dict[int, dict[str, float]]:
    clauses = []
    params: list[object] = []
    if strategies:
        placeholders = ",".join("?" for _ in strategies)
        clauses.append(f"o.strategy_version IN ({placeholders})")
        params.extend(strategies)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    rows = repo._conn.execute(
        "SELECT f.id, f.price, f.quantity, f.fee, o.symbol_id, o.side, o.strategy_version "
        "FROM fills f JOIN orders o ON o.id = f.order_id "
        f"{where} ORDER BY f.ts ASC, f.id ASC",
        params,
    ).fetchall()
    state: dict[tuple[int, str], dict[str, float | str]] = {}
    pnl_by_id: dict[int, dict[str, float]] = {}
    for row in rows:
        key = (int(row["symbol_id"]), str(row["strategy_version"] or ""))
        side = "long" if row["side"] == "buy" else "short"
        qty = float(row["quantity"] or 0.0)
        price = float(row["price"] or 0.0)
        fee = float(row["fee"] or 0.0)
        current = state.get(key)
        gross_pnl = 0.0
        if current is None or current["side"] == side or float(current["qty"] or 0.0) <= 0:
            old_qty = float(current["qty"] or 0.0) if current else 0.0
            old_avg = float(current["avg"] or 0.0) if current else 0.0
            new_qty = old_qty + qty
            avg = ((old_qty * old_avg) + (qty * price)) / new_qty if new_qty > 0 else price
            state[key] = {"side": side, "qty": new_qty, "avg": avg}
        else:
            old_qty = float(current["qty"] or 0.0)
            avg = float(current["avg"] or 0.0)
            close_qty = min(qty, old_qty)
            if current["side"] == "long":
                gross_pnl = (price - avg) * close_qty
            else:
                gross_pnl = (avg - price) * close_qty
            remaining = old_qty - close_qty
            excess = qty - close_qty
            if remaining > 1e-12:
                state[key] = {"side": current["side"], "qty": remaining, "avg": avg}
            elif excess > 1e-12:
                state[key] = {"side": side, "qty": excess, "avg": price}
            else:
                state.pop(key, None)
        pnl_by_id[int(row["id"])] = {
            "gross_pnl": round(gross_pnl, 8),
            "net_pnl": round(gross_pnl - fee, 8),
        }
    return pnl_by_id


def _strategy_win_rates(repo: SqliteRepo) -> dict[str, float]:
    rows = repo._conn.execute(
        "SELECT strategy_version, COUNT(*) AS closed_count, "
        "SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) AS wins "
        "FROM positions WHERE closed_at IS NOT NULL GROUP BY strategy_version"
    ).fetchall()
    return {
        str(row["strategy_version"] or "unknown"): round(
            float(row["wins"] or 0) / float(row["closed_count"] or 1) * 100,
            2,
        )
        for row in rows
    }


def _strategy_pnl_summary(repo: SqliteRepo, cache: MemoryCache) -> dict[str, dict[str, float]]:
    summary: dict[str, dict[str, float]] = {}
    realized_rows = repo._conn.execute(
        "SELECT strategy_version, SUM(realized_pnl) AS realized_pnl "
        "FROM positions GROUP BY strategy_version"
    ).fetchall()
    fee_rows = repo._conn.execute(
        "SELECT o.strategy_version, SUM(f.fee) AS fees_paid "
        "FROM fills f JOIN orders o ON o.id = f.order_id "
        "GROUP BY o.strategy_version"
    ).fetchall()
    for realized_row in realized_rows:
        strategy = str(realized_row["strategy_version"] or "unknown")
        row = summary.setdefault(
            strategy,
            {
                "gross_realized_pnl": 0.0,
                "fees_paid": 0.0,
                "realized_pnl": 0.0,
                "unrealized_pnl": 0.0,
                "total_pnl": 0.0,
                "open_notional": 0.0,
                "used_margin": 0.0,
                "notional_roi": 0.0,
                "margin_roi": 0.0,
                "roi": 0.0,
            },
        )
        row["gross_realized_pnl"] = float(realized_row["realized_pnl"] or 0.0)

    for fee_row in fee_rows:
        strategy = str(fee_row["strategy_version"] or "unknown")
        row = summary.setdefault(
            strategy,
            {
                "gross_realized_pnl": 0.0,
                "fees_paid": 0.0,
                "realized_pnl": 0.0,
                "unrealized_pnl": 0.0,
                "total_pnl": 0.0,
                "open_notional": 0.0,
                "used_margin": 0.0,
                "notional_roi": 0.0,
                "margin_roi": 0.0,
                "roi": 0.0,
            },
        )
        row["fees_paid"] = float(fee_row["fees_paid"] or 0.0)

    for position in _compute_positions(repo, cache):
        strategy = str(position.get("strategy") or "unknown")
        row = summary.setdefault(
            strategy,
            {
                "gross_realized_pnl": 0.0,
                "fees_paid": 0.0,
                "realized_pnl": 0.0,
                "unrealized_pnl": 0.0,
                "total_pnl": 0.0,
                "open_notional": 0.0,
                "used_margin": 0.0,
                "notional_roi": 0.0,
                "margin_roi": 0.0,
                "roi": 0.0,
            },
        )
        row["unrealized_pnl"] += float(position.get("unrealized_pnl") or 0.0)
        row["open_notional"] += abs(
            float(position.get("qty") or 0.0) * float(position.get("current_price") or 0.0)
        )

    for row in summary.values():
        net_realized_pnl = row["gross_realized_pnl"] - row["fees_paid"]
        total_pnl = net_realized_pnl + row["unrealized_pnl"]
        used_margin = row["open_notional"] / _PAPER_MARGIN_LEVERAGE if _PAPER_MARGIN_LEVERAGE else 0.0
        row["gross_realized_pnl"] = round(row["gross_realized_pnl"], 2)
        row["fees_paid"] = round(row["fees_paid"], 2)
        row["realized_pnl"] = round(net_realized_pnl, 2)
        row["unrealized_pnl"] = round(row["unrealized_pnl"], 2)
        row["total_pnl"] = round(total_pnl, 2)
        row["open_notional"] = round(row["open_notional"], 2)
        row["used_margin"] = round(used_margin, 2)
        row["notional_roi"] = (
            round(total_pnl / row["open_notional"] * 100, 2) if row["open_notional"] else 0.0
        )
        row["margin_roi"] = round(total_pnl / used_margin * 100, 2) if used_margin else 0.0
        row["roi"] = row["margin_roi"]
    return summary


def _sum_position_realized_pnl(
    repo: SqliteRepo,
    *,
    strategies: list[str] | None = None,
    since_ms: int | None = None,
    until_ms: int | None = None,
) -> float:
    clauses: list[str] = []
    params: list[object] = []
    if strategies:
        placeholders = ",".join("?" for _ in strategies)
        clauses.append(f"strategy_version IN ({placeholders})")
        params.extend(strategies)
    if since_ms is not None:
        clauses.append("COALESCE(closed_at, opened_at) >= ?")
        params.append(since_ms)
    if until_ms is not None:
        clauses.append("COALESCE(closed_at, opened_at) < ?")
        params.append(until_ms)
    where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
    value = repo._conn.execute(f"SELECT SUM(realized_pnl) FROM positions{where}", params).fetchone()[0]
    return float(value or 0.0)


def _sum_fees_paid(
    repo: SqliteRepo,
    since_ms: int = 0,
    until_ms: int | None = None,
    *,
    strategies: list[str] | None = None,
) -> float:
    if until_ms is None:
        until_ms = int(time.time() * 1000)
    strategy_clause = ""
    params: list[object] = [since_ms, until_ms]
    if strategies:
        placeholders = ",".join("?" for _ in strategies)
        strategy_clause = f" AND o.strategy_version IN ({placeholders})"
        params.extend(strategies)
    value = repo._conn.execute(
        "SELECT SUM(f.fee) FROM fills f "
        "JOIN orders o ON o.id = f.order_id "
        f"WHERE f.ts >= ? AND f.ts < ?{strategy_clause}",
        params,
    ).fetchone()[0]
    return float(value or 0.0)


def _count_fills(
    repo: SqliteRepo,
    *,
    since_ms: int,
    until_ms: int,
    strategies: list[str] | None = None,
) -> int:
    strategy_clause = ""
    params: list[object] = [since_ms, until_ms]
    if strategies:
        placeholders = ",".join("?" for _ in strategies)
        strategy_clause = f" AND o.strategy_version IN ({placeholders})"
        params.extend(strategies)
    value = repo._conn.execute(
        "SELECT COUNT(f.id) FROM fills f "
        "JOIN orders o ON o.id = f.order_id "
        f"WHERE f.ts >= ? AND f.ts < ?{strategy_clause}",
        params,
    ).fetchone()[0]
    return int(value or 0)


def _account_snapshot(
    repo: SqliteRepo,
    positions: list[dict],
    *,
    initial_balance: float = _INITIAL_USDT_BALANCE,
    leverage: float = _PAPER_MARGIN_LEVERAGE,
    strategies: list[str] | None = None,
) -> dict[str, float]:
    gross_realized_pnl = _sum_position_realized_pnl(repo, strategies=strategies)
    fees_paid = _sum_fees_paid(repo, strategies=strategies)
    realized_pnl = gross_realized_pnl - fees_paid
    unrealized_pnl = _positions_unrealized_pnl(positions)
    open_notional = _positions_open_notional(positions)
    effective_leverage = leverage if leverage > 0 else 1.0
    used_margin = open_notional / effective_leverage
    equity = initial_balance + realized_pnl + unrealized_pnl
    return {
        "initial_balance": round(initial_balance, 2),
        "equity": round(equity, 2),
        "available_balance": round(equity - used_margin, 2),
        "used_margin": round(used_margin, 2),
        "open_notional": round(open_notional, 2),
        "gross_realized_pnl": round(gross_realized_pnl, 2),
        "fees_paid": round(fees_paid, 2),
        "realized_pnl": round(realized_pnl, 2),
        "unrealized_pnl": round(unrealized_pnl, 2),
        "leverage": round(effective_leverage, 2),
    }


def _positions_unrealized_pnl(positions: list[dict]) -> float:
    return sum(float(row.get("unrealized_pnl") or 0.0) for row in positions)


def _positions_open_notional(positions: list[dict]) -> float:
    return sum(abs(float(row.get("qty") or 0.0) * float(row.get("current_price") or 0.0)) for row in positions)


def _append_balance_history(history: list[dict], account: dict[str, float]) -> None:
    history.append(
        {
            "ts": int(time.time() * 1000),
            "balance": account["equity"],
            "portfolio_value": account["equity"],
            "available_balance": account["available_balance"],
            "used_margin": account["used_margin"],
        }
    )
    if len(history) > 2_000:
        del history[:-2_000]


# ─── main ─────────────────────────────────────────────────────────────────


def main() -> None:
    print("=" * 60)
    print("  crypto-quant 实时交易看板 (真实行情)")
    print("  端口: 8089")
    print("=" * 60)

    # 1. 初始化数据存储
    data_root = Path("data")
    data_root.mkdir(exist_ok=True)
    db_path = data_root / "dashboard.sqlite"

    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")

    runner = MigrationRunner(migrations_dir=Path("migrations"))
    runner.apply_all(conn)

    repo = SqliteRepo(conn)
    parquet_io = ParquetIO(data_root=data_root)
    cache = MemoryCache(max_bars=1000)

    # 2. 插入 symbol 数据（幂笙）
    for sym, base, quote, stype in [
        ("BTCUSDT", "BTC", "USDT", "perp"),
        ("ETHUSDT", "ETH", "USDT", "perp"),
    ]:
        if repo.get_symbol(sym) is None:
            conn.execute(
                "INSERT INTO symbols (exchange, symbol, type, base, quote, tick_size, lot_size, min_notional, listed_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                ("binance", sym, stype, base, quote, 0.01, 0.001, 10.0, 1500000000000),
            )
            conn.commit()

    # 3. 创建 PaperMatchingEngine
    upsert_dashboard_universe(repo, _SYMBOLS)
    engine = PaperMatchingEngine(repo, get_price=lambda s: cache.latest_price(s))
    live_feed = LiveFeed(parquet_io, repo, cache)
    active_strategy_names: list[str] = []
    def current_equity() -> float:
        positions = _compute_positions(repo, cache, strategies=active_strategy_names or None)
        return _account_snapshot(
            repo,
            positions,
            initial_balance=_INITIAL_USDT_BALANCE,
            leverage=_PAPER_MARGIN_LEVERAGE,
            strategies=active_strategy_names or None,
        )["equity"]

    dashboard_strategies = default_dashboard_strategies(feed=live_feed, repo=repo, account_equity=current_equity)
    active_strategy_names = [strategy.name for strategy in dashboard_strategies]
    trader = DashboardPaperTrader(
        repo=repo,
        cache=cache,
        engine=engine,
        symbols=_SYMBOLS,
        strategies=dashboard_strategies,
        notional_usdt=_PAPER_BASE_ORDER_NOTIONAL,
        strategy_notional_multipliers=_paper_strategy_notional_multipliers(),
        max_open_notional_usdt=_PAPER_MAX_OPEN_NOTIONAL,
        risk_pipeline=DashboardRiskPipeline(
            portfolio_risk=L3PortfolioRiskValidator(
                PortfolioRiskLimits(equity=_INITIAL_USDT_BALANCE)
            ),
            position_risk=L2PositionRiskSizer(
                PositionRiskLimits(equity=_INITIAL_USDT_BALANCE)
            ),
        ),
    )

    # 4. 创建 LiveFeed 并启动（后台任务在 uvicorn 事件循环中运行）
    feeder = LiveDataFeeder(cache, parquet_io, repo, engine, trader=trader, proxy=_PROXY)

    # 5. 构建 FastAPI + 生命周期管理
    from contextlib import asynccontextmanager
    static_dir = Path(__file__).resolve().parent / "static"

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        task = asyncio.create_task(feeder.start())
        yield
        task.cancel()
        await feeder.stop()
        conn.close()

    app = create_app(cache, repo, parquet_io, engine, feeder, static_dir, trader=trader)
    app.router.lifespan_context = lifespan

    print("  浏览器打开: http://localhost:8089")
    print("  退出: Ctrl+C")
    print("=" * 60)

    import uvicorn
    host = os.getenv("CQ_DASHBOARD_HOST", "127.0.0.1")
    port = int(os.getenv("CQ_DASHBOARD_PORT", "8089"))
    uvicorn.run(app, host=host, port=port, log_level="warning")


if __name__ == "__main__":
    main()
