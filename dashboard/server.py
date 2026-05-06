"""实时交易看板——FastAPI + WebSocket 后端。

对标 asia231-scraper/dashboard/server.py 模式。
启动方式：``uv run python -m dashboard.server``，浏览器打开 http://localhost:8089 。
"""

from __future__ import annotations

import asyncio
import sqlite3
import sys
import time
from pathlib import Path

# 确保项目根在 sys.path（必须在 core 导入之前）
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from fastapi import FastAPI, WebSocket, WebSocketDisconnect  # noqa: E402
from fastapi.responses import FileResponse  # noqa: E402
from fastapi.staticfiles import StaticFiles  # noqa: E402

from core.data.memory_cache import MemoryCache  # noqa: E402
from core.data.parquet_io import ParquetIO  # noqa: E402
from core.data.sqlite_repo import SqliteRepo  # noqa: E402
from core.db.migration_runner import MigrationRunner  # noqa: E402
from core.execution.paper_engine import PaperMatchingEngine  # noqa: E402
from dashboard.data_simulator import DataSimulator  # noqa: E402

# ─── 工具函数 ──────────────────────────────────────────────────────────────────


def _start_of_day_ts() -> int:
    """今天 00:00 UTC 毫秒时间戳。"""
    now_s = time.time()
    return int((now_s - now_s % 86400) * 1000)


def _start_of_week_ts() -> int:
    """本周一 00:00 UTC 毫秒时间戳。"""
    now_s = time.time()
    # 周一是 weekday 0
    days_since_monday = (time.gmtime(now_s).tm_wday + 6) % 7
    return int((now_s - now_s % 86400 - days_since_monday * 86400) * 1000)


def _pnl_in_window(repo: SqliteRepo, since_ms: int) -> dict:
    """统计时间窗口内已实现 PnL + ROI。"""
    fills = repo._conn.execute(
        "SELECT f.fee, f.quantity, f.price, o.side FROM fills f "
        "JOIN orders o ON f.order_id = o.id WHERE f.ts >= ?", (since_ms,)
    ).fetchall()
    pnl = 0.0
    for f in fills:
        # 简化 PnL 计算：卖出 -> 收入，买入 -> 支出
        val = f["price"] * f["quantity"]
        if f["side"] == "buy":
            pnl -= val + f["fee"]
        else:
            pnl += val - f["fee"]
    n = len(fills)
    return {"n": n, "pnl": round(pnl, 2), "roi": 0.0}


# ─── create_app ────────────────────────────────────────────────────────────────


def create_app(
    cache: MemoryCache,
    repo: SqliteRepo,
    parquet_io: ParquetIO,
    engine: PaperMatchingEngine,
    simulator: DataSimulator,
    static_dir: Path,
) -> FastAPI:
    app = FastAPI(title="crypto-quant Dashboard", version="0.1.0")

    # 存储共享状态
    app.state.cache = cache
    app.state.repo = repo
    app.state.parquet_io = parquet_io
    app.state.engine = engine
    app.state.simulator = simulator
    app.state.usdt_balance = 10000.0  # 起始 USDT
    app.state.balance_history: list[dict] = []

    # ─── REST API ──────────────────────────────────────────────────────────

    @app.get("/api/status")
    def api_status():
        prices = cache.latest_prices_all()
        orders = repo.get_open_orders()
        positions = _compute_positions(repo, cache)
        port_val = _portfolio_value(app.state.usdt_balance, positions, cache)

        return {
            "mode": "paper",
            "simulation_running": simulator.is_running(),
            "portfolio_value": round(port_val, 2),
            "usdt_balance": round(app.state.usdt_balance, 2),
            "open_positions_n": len(positions),
            "open_orders_n": len(orders),
            "day_pnl": _pnl_in_window(repo, _start_of_day_ts()),
            "week_pnl": _pnl_in_window(repo, _start_of_week_ts()),
            "latest_prices": prices,
        }

    @app.get("/api/prices")
    def api_prices():
        prices = cache.latest_prices_all()
        result = {}
        for sym, price in prices.items():
            bars = cache.get_bars(sym, "1h", n=24)
            change_24h = 0.0
            high_24h = price
            low_24h = price
            if bars:
                prev = bars[0].c
                if prev > 0:
                    change_24h = round((price - prev) / prev * 100, 2)
                high_24h = max(b.h for b in bars)
                low_24h = min(b.l for b in bars)
            result[sym] = {
                "price": price,
                "change_24h": change_24h,
                "high_24h": high_24h,
                "low_24h": low_24h,
            }
        return result

    @app.get("/api/price_history")
    def api_price_history(symbol: str = "BTCUSDT", tf: str = "1m", n: int = 200):
        bars = cache.get_bars(symbol, tf, n=n)
        if not bars:
            bars_parquet = parquet_io.read_bars(symbol, tf, n=n)
            bars = bars_parquet
        return [
            {"ts": b.ts, "o": b.o, "h": b.h, "l": b.l, "c": b.c, "v": b.v}
            for b in bars
        ]

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
                "ORDER BY o.placed_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [_order_row(r) for r in rows]

    @app.get("/api/fills")
    def api_fills(limit: int = 50):
        rows = repo._conn.execute(
            "SELECT f.*, s.symbol as sym, o.side, o.strategy_version as strategy "
            "FROM fills f "
            "JOIN orders o ON f.order_id = o.id "
            "LEFT JOIN symbols s ON o.symbol_id = s.id "
            "ORDER BY f.ts DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [
            {
                "id": r["id"],
                "order_id": r["order_id"],
                "symbol": r["sym"] or "?",
                "side": r["side"],
                "strategy": r["strategy"],
                "price": r["price"],
                "quantity": r["quantity"],
                "fee": r["fee"],
                "fee_currency": r["fee_currency"],
                "is_maker": r["is_maker"],
                "ts": r["ts"],
            }
            for r in rows
        ]

    @app.get("/api/positions")
    def api_positions():
        return _compute_positions(repo, cache)

    @app.get("/api/strategies")
    def api_strategies():
        rows = repo._conn.execute(
            "SELECT o.strategy_version as name, COUNT(DISTINCT o.id) as orders_n, "
            "COUNT(DISTINCT f.id) as fills_n "
            "FROM orders o LEFT JOIN fills f ON f.order_id = o.id "
            "GROUP BY o.strategy_version"
        ).fetchall()
        return [
            {
                "name": r["name"],
                "orders_count": r["orders_n"],
                "fills_count": r["fills_n"],
                "win_rate": 0,
                "total_pnl": 0.0,
                "roi": 0.0,
            }
            for r in rows
        ]

    @app.get("/api/balance_history")
    def api_balance_history(limit: int = 500):
        hist = app.state.balance_history
        return hist[-limit:] if len(hist) > limit else hist

    @app.post("/api/control")
    async def api_control(req: dict):
        action = req.get("action", "")
        if action == "start":
            simulator.start()
            return {"ok": True, "action": "start"}
        elif action == "stop":
            simulator.stop()
            return {"ok": True, "action": "stop"}
        elif action == "random_order":
            simulator._place_random_order(int(time.time() * 1000))
            return {"ok": True, "action": "random_order"}
        return {"ok": False, "error": f"unknown action: {action}"}

    # ─── WebSocket ─────────────────────────────────────────────────────────

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
                    "simulation_running": simulator.is_running(),
                    "latest_prices": prices,
                    "open_positions_n": len(positions),
                    "positions": positions,
                    "recent_orders": [_order_row(r) for r in recent_orders],
                    "recent_fills": [
                        {"symbol": r["sym"] or "?", "side": r["side"], "price": r["price"],
                         "quantity": r["quantity"], "fee": r["fee"], "ts": r["ts"]}
                        for r in recent_fills
                    ],
                })
                await asyncio.sleep(2)
        except WebSocketDisconnect:
            pass

    # ─── 静态文件 ──────────────────────────────────────────────────────────

    @app.get("/")
    def index():
        return FileResponse(static_dir / "index.html")

    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    return app


# ─── 辅助 ──────────────────────────────────────────────────────────────────────


def _order_row(r) -> dict:
    return {
        "id": r["id"],
        "client_order_id": r["client_order_id"],
        "symbol": r["sym"] or "?",
        "side": r["side"],
        "type": r["type"],
        "price": r["price"],
        "stop_price": r["stop_price"],
        "quantity": r["quantity"],
        "filled_qty": r["filled_qty"],
        "avg_fill_price": r["avg_fill_price"],
        "status": r["status"],
        "purpose": r["purpose"],
        "strategy": r["strategy_version"],
        "placed_at": r["placed_at"],
    }


def _compute_positions(repo: SqliteRepo, cache: MemoryCache) -> list[dict]:
    """从已成交订单聚合持仓。"""
    fills_rows = repo._conn.execute(
        "SELECT o.symbol_id, o.side, o.strategy_version, s.symbol as sym, "
        "SUM(f.quantity) as total_qty, "
        "SUM(f.price * f.quantity) / SUM(f.quantity) as avg_price "
        "FROM fills f "
        "JOIN orders o ON f.order_id = o.id "
        "LEFT JOIN symbols s ON o.symbol_id = s.id "
        "GROUP BY o.symbol_id, o.side, o.strategy_version"
    ).fetchall()

    positions = []
    for r in fills_rows:
        qty = r["total_qty"]
        entry = r["avg_price"]
        sym = r["sym"] or "BTCUSDT"
        cur_price = cache.latest_price(sym) or entry
        if r["side"] == "buy":
            unrealized = round(qty * (cur_price - entry), 2)
        else:
            unrealized = round(qty * (entry - cur_price), 2)
        positions.append({
            "symbol": sym,
            "side": r["side"],
            "strategy": r["strategy_version"],
            "qty": round(qty, 6),
            "entry_price": round(entry, 2),
            "current_price": round(cur_price, 2),
            "unrealized_pnl": unrealized,
        })
    return positions


def _portfolio_value(usdt_balance: float, positions: list[dict], cache: MemoryCache) -> float:
    total = usdt_balance
    for p in positions:
        cur = p["current_price"]
        if p["side"] == "buy":
            total += p["qty"] * cur
        else:
            total -= p["qty"] * cur  # 空头暂简化
    return round(total, 2)


# ─── main ──────────────────────────────────────────────────────────────────────


def main() -> None:
    print("=" * 60)
    print("  crypto-quant 实时交易看板")
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

    # 2. 插入桩 symbol 数据（幂等）
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

    # 4. 创建并启动数据模拟器
    simulator = DataSimulator(cache, repo, parquet_io, engine)
    simulator.start()
    print("  [模拟器] 已启动（生成 BTCUSDT/ETHUSDT 合成行情 + 随机订单）")

    # 5. 启动 FastAPI
    import uvicorn

    static_dir = Path(__file__).resolve().parent / "static"
    app = create_app(cache, repo, parquet_io, engine, simulator, static_dir)

    print("  浏览器打开: http://localhost:8089")
    print("  退出: Ctrl+C")
    print("=" * 60)

    uvicorn.run(app, host="127.0.0.1", port=8089, log_level="warning")


if __name__ == "__main__":
    main()
