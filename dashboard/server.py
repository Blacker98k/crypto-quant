"""实时交易看板——FastAPI + WebSocket 后端（真实行情版）。

用 WsSubscriber 替代 DataSimulator，从 Binance 推送实时数据到看板。
PaperMatchingEngine 基于真实行情做模拟成交。

启动方式: uv run python -m dashboard.server
浏览器打开: http://localhost:8089
"""

from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import sys
import time
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from core.data.exchange.base import Bar
from core.data.exchange.binance_spot import BinanceSpotAdapter
from core.data.memory_cache import MemoryCache
from core.data.parquet_io import ParquetIO
from core.data.sqlite_repo import SqliteRepo
from core.data.ws_subscriber import WsSubscriber
from core.db.migration_runner import MigrationRunner
from core.execution.paper_engine import PaperMatchingEngine
from core.monitor.market_health import summarize_market_health
from core.monitor.paper_metrics import paper_metrics
from dashboard.paper_trading import (
    DEFAULT_TOP30_USDT,
    UNIVERSE_NAME,
    DashboardPaperTrader,
    fetch_binance_recent_klines,
    fetch_binance_top_usdt_symbols,
    upsert_dashboard_universe,
)

# 代理配置（根据环境变量或写死）
_PROXY = os.getenv("CQ_BINANCE_PROXY", "http://127.0.0.1:57777").strip()
_SYMBOLS = DEFAULT_TOP30_USDT
_FUTURES_PRICE_URL = "https://fapi.binance.com/fapi/v1/ticker/price"
_FUTURES_24H_URL = "https://fapi.binance.com/fapi/v1/ticker/24hr"
_DATA_HEALTH_DEFAULT_WINDOW_MS = 10 * 60_000
_INITIAL_USDT_BALANCE = 10_000.0
_PAPER_MARGIN_LEVERAGE = 25.0


# ─── 工兛函数 ──────────────────────────────────────────────────────────────────


def _start_of_day_ts() -> int:
    now_s = time.time()
    return int((now_s - now_s % 86400) * 1000)


def _pnl_in_window(repo: SqliteRepo, since_ms: int, positions: list[dict] | None = None) -> dict:
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
        self._rest_bar_seen_ts: dict[tuple[str, str], int] = {}
        self._running = False
        self._task: asyncio.Task | None = None
        self._price_task: asyncio.Task | None = None
        self._watchdog_task: asyncio.Task | None = None
        self._rest_bar_task: asyncio.Task | None = None
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
        await self._refresh_universe()

        await self._connect_ws()
        print(f"  [LiveFeed] WS 已连接，订阅: {', '.join(self._symbols)} 1m")

        # 后台加载 exchange（不录响启动）
        self._exchange_task = asyncio.create_task(self._lazy_load_exchange())

        # 启动定时检查引擎
        self._task = asyncio.create_task(self._engine_loop())
        self._price_task = asyncio.create_task(self._rest_price_loop())
        self._watchdog_task = asyncio.create_task(self._bar_watchdog_loop())
        self._rest_bar_task = asyncio.create_task(self._rest_bar_fallback_loop())

    def _build_ws(self) -> WsSubscriber:
        self._ws = WsSubscriber(self._cache, self._parquet_io, self._exchange)
        for sym in self._symbols:
            self._ws.subscribe_candles(sym, "1m", self._on_bar)
        self._ws.subscribe_tickers(self._symbols)
        return self._ws

    async def _connect_ws(self) -> None:
        last_error: Exception | None = None
        for proxy in self._proxy_candidates():
            ws = self._build_ws()
            try:
                await ws.connect(proxy=proxy)
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
        await self._backfill_recent_1m(self._symbols)
        await self._connect_ws()

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
                await self._backfill_recent_1m(self._symbols, publish=True)
            except Exception as exc:
                self._repo.log_run("dashboard_rest_bar_fallback", "fail", note=str(exc)[:200])

    async def _refresh_rest_prices(self) -> None:
        import aiohttp

        captured_at_ms = int(time.time() * 1000)
        params = {"symbols": json.dumps(self._symbols)}
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
            allowed_symbols=set(self._symbols),
        )
        await self._refresh_24h_tickers()

    async def _refresh_24h_tickers(self) -> None:
        import aiohttp

        params = {"symbols": json.dumps(self._symbols)}
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

    async def _refresh_universe(self) -> None:
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
        await self._backfill_recent_1m(symbols)

    async def _backfill_recent_1m(self, symbols: list[str], *, publish: bool = False) -> None:
        for symbol in symbols:
            try:
                bars = await self._call_binance_with_proxy_fallback(
                    f"binance_1m_warmup_{symbol}",
                    lambda proxy, symbol=symbol: fetch_binance_recent_klines(
                        symbol, "1m", proxy=proxy, limit=20
                    ),
                )
            except Exception as exc:
                self._repo.log_run(f"binance_1m_warmup_{symbol}", "fail", note=str(exc)[:200])
                continue
            for bar in bars:
                self._cache.push_bar(bar)
            if bars:
                self._parquet_io.write_bars(bars)
                closed_bars = [bar for bar in bars if bar.closed]
                if not closed_bars:
                    continue
                latest_ts = max(bar.ts for bar in closed_bars)
                key = (symbol, "1m")
                if publish:
                    last_seen = self._rest_bar_seen_ts.get(key, 0)
                    for bar in sorted(closed_bars, key=lambda item: item.ts):
                        if bar.ts > last_seen:
                            self._on_bar(bar)
                    self._rest_bar_seen_ts[key] = max(latest_ts, last_seen)
                else:
                    self._rest_bar_seen_ts[key] = max(latest_ts, self._rest_bar_seen_ts.get(key, 0))
                self._repo.log_run(f"binance_1m_warmup_{symbol}", "ok")

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

    # ─── REST API ──────────────────────────────────────────────────────────

    @app.get("/api/status")
    def api_status():
        prices = cache.latest_prices_all()
        orders = repo.get_open_orders()
        positions = _compute_positions(repo, cache)
        account = _account_snapshot(
            repo,
            positions,
            initial_balance=app.state.initial_balance,
            leverage=app.state.paper_leverage,
        )
        risk_counts = _risk_event_counts(repo)
        running = _feeder_running(feeder)
        last_bar_age_ms = _feeder_last_bar_age_ms(feeder)
        market_data_stale = _feeder_market_data_stale(feeder)
        return {
            "mode": "live_paper",
            "simulation_running": running,
            "ws_connected": running,
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
            "critical_risk_events_n": risk_counts["critical"],
            "day_pnl": _pnl_in_window(repo, _start_of_day_ts(), positions),
            "week_pnl": _pnl_in_window(repo, _start_of_day_ts() - 7 * 86400_000, positions),
            "latest_prices": prices,
        }

    @app.get("/api/prices")
    def api_prices():
        prices = cache.latest_prices_all()
        price_meta = cache.latest_price_meta_all()
        now_ms = int(time.time() * 1000)
        result = {}
        ticker_24h = getattr(feeder, "_ticker_24h", {})
        for sym, price in prices.items():
            bars = cache.get_bars(sym, "1h", n=24)
            change_24h, high_24h, low_24h = 0.0, price, price
            if bars:
                prev = bars[0].c
                if prev > 0:
                    change_24h = round((price - prev) / prev * 100, 2)
                high_24h = max(b.h for b in bars)
                low_24h = min(b.l for b in bars)
            if sym in ticker_24h:
                stats = ticker_24h[sym]
                change_24h = stats.get("change_24h", change_24h)
                high_24h = stats.get("high_24h", high_24h)
                low_24h = stats.get("low_24h", low_24h)
            meta = price_meta.get(sym, {})
            updated_at = meta.get("updated_at")
            age_ms = max(0, now_ms - int(updated_at)) if updated_at is not None else None
            result[sym] = {
                "price": price,
                "change_24h": change_24h,
                "high_24h": high_24h,
                "low_24h": low_24h,
                "source_ts": meta.get("source_ts"),
                "updated_at": updated_at,
                "age_ms": age_ms,
                "quote_volume": ticker_24h.get(sym, {}).get("quote_volume", 0.0),
            }
        return result

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
        if status:
            rows = repo._conn.execute(
                "SELECT o.*, s.symbol as sym FROM orders o "
                "LEFT JOIN symbols s ON o.symbol_id = s.id "
                "WHERE o.status = ? ORDER BY o.placed_at DESC LIMIT ?",
                (status, limit),
            ).fetchall()
        else:
            rows = repo._conn.execute(
                "SELECT o.*, s.symbol as sym FROM orders o "
                "LEFT JOIN symbols s ON o.symbol_id = s.id "
                "ORDER BY o.placed_at DESC LIMIT ?", (limit,),
            ).fetchall()
        return [_order_row(r) for r in rows]

    @app.get("/api/fills")
    def api_fills(limit: int = 50):
        rows = repo._conn.execute(
            "SELECT f.*, s.symbol as sym, o.side, o.strategy_version as strategy "
            "FROM fills f JOIN orders o ON f.order_id = o.id "
            "LEFT JOIN symbols s ON o.symbol_id = s.id "
            "ORDER BY f.ts DESC LIMIT ?", (limit,),
        ).fetchall()
        return [{"id": r["id"], "order_id": r["order_id"],
                 "symbol": r["sym"] or "?", "side": r["side"],
                 "strategy": r["strategy"], "price": r["price"],
                 "quantity": r["quantity"], "fee": r["fee"],
                 "fee_currency": r["fee_currency"],
                 "is_maker": r["is_maker"], "ts": r["ts"]}
                for r in rows]

    @app.get("/api/positions")
    def api_positions():
        return _compute_positions(repo, cache)

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
        return paper_metrics(repo._conn, since_ms=start_ms, until_ms=end_ms)

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

    @app.get("/api/strategies")
    def api_strategies():
        rows = repo._conn.execute(
            "SELECT strategy_version, COUNT(*) AS orders_count "
            "FROM orders GROUP BY strategy_version ORDER BY strategy_version"
        ).fetchall()
        fills = repo._conn.execute(
            "SELECT o.strategy_version, COUNT(f.id) AS fills_count "
            "FROM orders o LEFT JOIN fills f ON f.order_id = o.id "
            "GROUP BY o.strategy_version"
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
            if trader:
                symbol = trader.symbols[0] if trader.symbols else "BTCUSDT"
                price = cache.latest_price(symbol) or 100.0
                now_ms = int(time.time() * 1000)
                trader.on_bar(
                    Bar(
                        symbol=symbol,
                        timeframe="1m",
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
        return {
            "ok": action in {"start", "stop", "random_order"},
            "action": action,
            "simulation_running": _feeder_running(feeder),
        }

    # ─── WebSocket ────────────────────────────────────────────────────────

    @app.websocket("/ws")
    async def ws_endpoint(ws: WebSocket):
        await ws.accept()
        try:
            while True:
                prices = cache.latest_prices_all()
                positions = _compute_positions(repo, cache)
                account = _account_snapshot(
                    repo,
                    positions,
                    initial_balance=app.state.initial_balance,
                    leverage=app.state.paper_leverage,
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
                    "simulation_running": _feeder_running(feeder),
                    "ws_connected": _feeder_running(feeder),
                    "bars_received": feeder._bar_counter,
                    "last_bar_age_ms": _feeder_last_bar_age_ms(feeder),
                    "market_data_stale": _feeder_market_data_stale(feeder),
                    "latest_prices": prices,
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
    return {
        "id": r["id"],
        "type": r["type"],
        "severity": r["severity"],
        "source": r["source"],
        "related_id": r["related_id"],
        "payload": payload,
        "captured_at": r["captured_at"],
    }


def _risk_event_counts(repo: SqliteRepo) -> dict[str, int]:
    row = repo._conn.execute(
        "SELECT COUNT(*) AS total, "
        "SUM(CASE WHEN severity='critical' THEN 1 ELSE 0 END) AS critical "
        "FROM risk_events "
        f"WHERE {_actionable_risk_sql()}"
    ).fetchone()
    return {"total": int(row["total"] or 0), "critical": int(row["critical"] or 0)}


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
    ws = getattr(feeder, "_ws", None)
    if not bool(getattr(feeder, "_running", False) and getattr(ws, "_running", False)):
        return False
    return not _feeder_market_data_stale(feeder)


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


def _compute_positions(repo: SqliteRepo, cache: MemoryCache) -> list[dict]:
    positions = []
    for row in repo.list_open_positions():
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
                "entry_price": round(entry, 2),
                "current_price": round(cur, 2),
                "unrealized_pnl": round(unrealized, 2),
                "realized_pnl": round(float(row["realized_pnl"] or 0.0), 2),
            }
        )
    return positions


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


def _sum_position_realized_pnl(repo: SqliteRepo) -> float:
    value = repo._conn.execute("SELECT SUM(realized_pnl) FROM positions").fetchone()[0]
    return float(value or 0.0)


def _sum_fees_paid(repo: SqliteRepo, since_ms: int = 0, until_ms: int | None = None) -> float:
    if until_ms is None:
        until_ms = int(time.time() * 1000)
    value = repo._conn.execute(
        "SELECT SUM(fee) FROM fills WHERE ts >= ? AND ts < ?",
        (since_ms, until_ms),
    ).fetchone()[0]
    return float(value or 0.0)


def _account_snapshot(
    repo: SqliteRepo,
    positions: list[dict],
    *,
    initial_balance: float = _INITIAL_USDT_BALANCE,
    leverage: float = _PAPER_MARGIN_LEVERAGE,
) -> dict[str, float]:
    gross_realized_pnl = _sum_position_realized_pnl(repo)
    fees_paid = _sum_fees_paid(repo)
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
    trader = DashboardPaperTrader(
        repo=repo,
        cache=cache,
        engine=engine,
        symbols=_SYMBOLS,
        strategy_notional_multipliers={"explore_mean_reversion": 0.5},
        max_open_notional_usdt=90_000.0,
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
    uvicorn.run(app, host="127.0.0.1", port=8089, log_level="warning")


if __name__ == "__main__":
    main()
