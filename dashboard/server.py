"""实时交易看板——FastAPI + WebSocket 后端（真实行情版）。

用 WsSubscriber 替代 DataSimulator，从 Binance 推送实时数据到看板。
PaperMatchingEngine 基于真实行情做模拟成交。

启动方式: uv run python -m dashboard.server
浏览器打开: http://localhost:8089
"""

from __future__ import annotations

import asyncio
import json
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

from core.data.exchange.binance_spot import BinanceSpotAdapter
from core.data.memory_cache import MemoryCache
from core.data.parquet_io import ParquetIO
from core.data.sqlite_repo import SqliteRepo
from core.data.ws_subscriber import WsSubscriber
from core.db.migration_runner import MigrationRunner
from core.execution.paper_engine import PaperMatchingEngine
from core.monitor.market_health import summarize_market_health
from core.monitor.paper_metrics import paper_metrics

# 代理配置（根据环境变量或写死）
_PROXY = "http://127.0.0.1:57777"
_SYMBOLS = ["BTCUSDT", "ETHUSDT"]


# ─── 工兛函数 ──────────────────────────────────────────────────────────────────


def _start_of_day_ts() -> int:
    now_s = time.time()
    return int((now_s - now_s % 86400) * 1000)


def _pnl_in_window(repo: SqliteRepo, since_ms: int) -> dict:
    fills = repo._conn.execute(
        "SELECT f.fee, f.quantity, f.price, o.side FROM fills f "
        "JOIN orders o ON f.order_id = o.id WHERE f.ts >= ?", (since_ms,)
    ).fetchall()
    pnl = 0.0
    for f in fills:
        val = f["price"] * f["quantity"]
        if f["side"] == "buy":
            pnl -= val + f["fee"]
        else:
            pnl += val - f["fee"]
    return {"n": len(fills), "pnl": round(pnl, 2), "roi": 0.0}


# ─── 实时行情理货器 ────────────────────────────────────────────────────────────


class LiveDataFeeder:
    """WS 实时行情 → MemoryCache + Paper 引擎。"""

    def __init__(
        self,
        cache: MemoryCache,
        parquet_io: ParquetIO,
        repo: SqliteRepo,
        engine: PaperMatchingEngine,
        proxy: str = "",
    ):
        self._cache = cache
        self._parquet_io = parquet_io
        self._repo = repo
        self._engine = engine
        self._proxy = proxy
        self._ws: WsSubscriber | None = None
        self._exchange: BinanceSpotAdapter | None = None
        self._exchange_task: asyncio.Task | None = None
        self._bar_counter = 0
        self._running = False
        self._task: asyncio.Task | None = None

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
        self._ws = WsSubscriber(self._cache, self._parquet_io, self._exchange)

        for sym in _SYMBOLS:
            self._ws.subscribe_candles(sym, "1m", self._on_bar)

        await self._ws.connect(proxy=self._proxy)
        print(f"  [LiveFeed] WS 已连接，订阅: {', '.join(_SYMBOLS)} 1m")

        # 后台加载 exchange（不录响启动）
        self._exchange_task = asyncio.create_task(self._lazy_load_exchange())

        # 启动定时检查引擎
        self._task = asyncio.create_task(self._engine_loop())

    def _on_bar(self, bar) -> None:
        """收到收盘 K 线时触发引擎检查。"""
        self._bar_counter += 1
        if self._bar_counter % 5 == 0:
            print(f"  [LiveFeed] 已收 {self._bar_counter} 根 K 线, "
                  f"{bar.symbol} {bar.timeframe} c={bar.c:.2f}")

    async def _engine_loop(self) -> None:
        """定时检查挂单 + 记录资金曲线。"""
        while self._running and self._ws and self._ws._running:
            try:
                now_ms = int(time.time() * 1000)
                fills = self._engine.check_pending_orders(now_ms)
                if fills:
                    print(f"  [引擎] {len(fills)} 笔新成交!")
            except Exception:
                pass
            await asyncio.sleep(2)

    async def stop(self) -> None:
        """关闭连接。"""
        self._running = False
        if self._task:
            self._task.cancel()
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
) -> FastAPI:
    app = FastAPI(title="crypto-quant Dashboard (Live)", version="0.2.0")
    app.state.cache = cache
    app.state.repo = repo
    app.state.parquet_io = parquet_io
    app.state.engine = engine
    app.state.feeder = feeder
    app.state.usdt_balance = 10000.0
    app.state.balance_history: list[dict] = []

    # ─── REST API ──────────────────────────────────────────────────────────

    @app.get("/api/status")
    def api_status():
        prices = cache.latest_prices_all()
        orders = repo.get_open_orders()
        positions = _compute_positions(repo, cache)
        port_val = _portfolio_value(app.state.usdt_balance, positions, cache)
        risk_counts = _risk_event_counts(repo)
        return {
            "mode": "live_paper",
            "ws_connected": feeder._ws._running if feeder._ws else False,
            "bars_received": feeder._bar_counter,
            "portfolio_value": round(port_val, 2),
            "usdt_balance": round(app.state.usdt_balance, 2),
            "open_positions_n": len(positions),
            "open_orders_n": len(orders),
            "risk_events_n": risk_counts["total"],
            "critical_risk_events_n": risk_counts["critical"],
            "day_pnl": _pnl_in_window(repo, _start_of_day_ts()),
            "week_pnl": _pnl_in_window(repo, _start_of_day_ts() - 7 * 86400_000),
            "latest_prices": prices,
        }

    @app.get("/api/prices")
    def api_prices():
        prices = cache.latest_prices_all()
        result = {}
        for sym, price in prices.items():
            bars = cache.get_bars(sym, "1h", n=24)
            change_24h, high_24h, low_24h = 0.0, price, price
            if bars:
                prev = bars[0].c
                if prev > 0:
                    change_24h = round((price - prev) / prev * 100, 2)
                high_24h = max(b.h for b in bars)
                low_24h = min(b.l for b in bars)
            result[sym] = {"price": price, "change_24h": change_24h,
                           "high_24h": high_24h, "low_24h": low_24h}
        return result

    @app.get("/api/price_history")
    def api_price_history(symbol: str = "BTCUSDT", tf: str = "1m", n: int = 200):
        bars = cache.get_bars(symbol, tf, n=n)
        if not bars:
            bars = parquet_io.read_bars(symbol, tf, n=n)
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
        rows = repo.get_recent_risk_events(limit=max(1, min(limit, 500)), since_ms=since_ms)
        return [_risk_event_row(row) for row in rows]

    @app.get("/api/paper_metrics")
    def api_paper_metrics(since_ms: int | None = None, until_ms: int | None = None):
        start_ms = _start_of_day_ts() if since_ms is None else since_ms
        end_ms = int(time.time() * 1000) + 1 if until_ms is None else until_ms
        return paper_metrics(repo._conn, since_ms=start_ms, until_ms=end_ms)

    @app.get("/api/data_health")
    def api_data_health(limit: int = 100, since_ms: int | None = None):
        return summarize_market_health(repo, limit=limit, since_ms=since_ms)

    # ─── WebSocket ────────────────────────────────────────────────────────

    @app.websocket("/ws")
    async def ws_endpoint(ws: WebSocket):
        await ws.accept()
        try:
            while True:
                prices = cache.latest_prices_all()
                positions = _compute_positions(repo, cache)
                port_val = _portfolio_value(app.state.usdt_balance, positions, cache)
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
                    "portfolio_value": round(port_val, 2),
                    "usdt_balance": round(app.state.usdt_balance, 2),
                    "ws_connected": feeder._ws._running if feeder._ws else False,
                    "bars_received": feeder._bar_counter,
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
                await asyncio.sleep(2)
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
        "FROM risk_events"
    ).fetchone()
    return {"total": int(row["total"] or 0), "critical": int(row["critical"] or 0)}


def _compute_positions(repo: SqliteRepo, cache: MemoryCache) -> list[dict]:
    fills_rows = repo._conn.execute(
        "SELECT o.symbol_id, o.side, o.strategy_version, s.symbol as sym, "
        "SUM(f.quantity) as total_qty, "
        "SUM(f.price * f.quantity) / NULLIF(SUM(f.quantity), 0) as avg_price "
        "FROM fills f JOIN orders o ON f.order_id = o.id "
        "LEFT JOIN symbols s ON o.symbol_id = s.id "
        "GROUP BY o.symbol_id, o.side, o.strategy_version"
    ).fetchall()
    positions = []
    for r in fills_rows:
        qty = r["total_qty"]
        entry = r["avg_price"]
        sym = r["sym"] or "BTCUSDT"
        cur = cache.latest_price(sym) or entry
        unrealized = round(qty * (cur - entry), 2)
        positions.append({
            "symbol": sym, "side": r["side"], "strategy": r["strategy_version"],
            "qty": round(qty, 6), "entry_price": round(entry, 2),
            "current_price": round(cur, 2), "unrealized_pnl": unrealized,
        })
    return positions


def _portfolio_value(usdt_balance: float, positions: list[dict], cache: MemoryCache) -> float:
    total = usdt_balance
    for p in positions:
        cur = p["current_price"]
        total += p["qty"] * cur
    return round(total, 2)


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
    engine = PaperMatchingEngine(repo, get_price=lambda s: cache.latest_price(s))

    # 4. 创建 LiveFeed 并启动（后台任务在 uvicorn 事件循环中运行）
    feeder = LiveDataFeeder(cache, parquet_io, repo, engine, proxy=_PROXY)

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

    app = create_app(cache, repo, parquet_io, engine, feeder, static_dir)
    app.router.lifespan_context = lifespan

    print("  浏览器打开: http://localhost:8089")
    print("  退出: Ctrl+C")
    print("=" * 60)

    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8089, log_level="warning")


if __name__ == "__main__":
    main()
