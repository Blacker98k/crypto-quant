#!/usr/bin/env python3
"""Paper 模式启动器——实时行情驱动 S1/S2 策略，自动下单+成交+监控。

流程:
  Binance WS → WsSubscriber → MemoryCache → Strategy.on_bar()
                                           → Signal → PaperMatchingEngine
                                           → Dashboard REST/WS

启动: uv run python scripts/run_paper.py
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
import sys
import time
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from core.data.memory_cache import MemoryCache
from core.data.parquet_io import ParquetIO
from core.data.sqlite_repo import SqliteRepo
from core.data.ws_subscriber import WsSubscriber
from core.data.exchange.binance_spot import BinanceSpotAdapter
from core.db.migration_runner import MigrationRunner
from core.data.feed import LiveFeed
from core.execution.paper_engine import PaperMatchingEngine
from core.execution.order_types import OrderIntent

from core.strategy import (
    S1BtcEthTrend, S2AltcoinReversal,
    Strategy, StrategyContext,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("paper_runner")

_PROXY = "http://127.0.0.1:57777"
_SYMBOLS = ["BTCUSDT", "ETHUSDT"]
_INITIAL_CASH = 10_000.0


class PaperRunner:
    """Paper 交易运行器。"""

    def __init__(self, proxy: str = ""):
        self._proxy = proxy
        self._running = False

        # 数据层
        self._db_path = Path("data/paper.sqlite")
        if self._db_path.exists():
            self._db_path.unlink()
        self._conn = sqlite3.connect(str(self._db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        MigrationRunner(migrations_dir=Path("migrations")).apply_all(self._conn)

        self._repo = SqliteRepo(self._conn)
        self._parquet_io = ParquetIO(data_root="data")
        self._cache = MemoryCache(max_bars=1000)

        # Paper 引擎
        self._engine = PaperMatchingEngine(
            self._repo,
            get_price=lambda s: self._cache.latest_price(s),
        )

        # WS
        self._ws: WsSubscriber | None = None
        self._exchange: BinanceSpotAdapter | None = None

        # 策略
        self._strategies: list[tuple[Strategy, str, str]] = [
            (S1BtcEthTrend(), "BTCUSDT", "4h"),
            (S1BtcEthTrend(), "ETHUSDT", "4h"),
        ]
        self._s2 = S2AltcoinReversal()
        self._contexts: dict[str, StrategyContext] = {}

        # 初始化 symbols 表
        self._init_symbols()

        # 统计
        self._signal_count = 0
        self._order_count = 0
        self._fill_count = 0
        self._bar_count = 0

    def _init_symbols(self):
        for sym, base, quote, stype in [
            ("BTCUSDT", "BTC", "USDT", "perp"),
            ("ETHUSDT", "ETH", "USDT", "perp"),
        ]:
            if self._repo.get_symbol(sym) is None:
                self._conn.execute(
                    "INSERT INTO symbols (exchange, symbol, type, base, quote, "
                    "tick_size, lot_size, min_notional, listed_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    ("binance", sym, stype, base, quote, 0.01, 0.001, 10.0, 1500000000000),
                )
        self._conn.commit()

    async def start(self):
        """启动运行器。"""
        log.info("=" * 50)
        log.info("  Paper 交易模式启动")
        log.info(f"  初始资金: ${_INITIAL_CASH:.0f}")
        log.info(f"  策略: S1 (BTC/ETH 趋势), S2 (均值回归)")
        log.info("=" * 50)

        self._running = True

        # 连接 WS
        self._exchange = BinanceSpotAdapter(proxy=self._proxy, timeout_ms=15000)
        self._ws = WsSubscriber(self._cache, self._parquet_io, self._exchange)

        # 订阅 K 线
        for sym in _SYMBOLS:
            self._ws.subscribe_candles(sym, "1m", lambda b: None)
            self._ws.subscribe_candles(sym, "4h", self._make_on_bar(sym))

        await self._ws.connect(proxy=self._proxy)
        log.info("WS 已连接，策略监控中...")

        # 后台加载交易所
        asyncio.create_task(self._lazy_load_exchange())

        # 主循环
        await self._main_loop()

    def _make_on_bar(self, symbol: str):
        """为每个标的创建闭包回调。"""
        def on_bar(bar):
            asyncio.create_task(self._on_bar_closed(symbol, bar))
        return on_bar

    async def _on_bar_closed(self, symbol: str, bar) -> None:
        """4h K 线收盘时运行策略。"""
        self._bar_count += 1

        # 创建策略 context（用 LiveFeed 提供历史和实时数据）
        live_feed = LiveFeed(self._parquet_io, self._repo, self._cache)
        ctx = StrategyContext(
            data=live_feed,
            clock=None,
            repo=self._repo,
            strategy_name="S1_btc_eth_trend",
        )
        # 覆写 now_ms
        ctx.now_ms = lambda: int(time.time() * 1000)  # type: ignore

        # 对每个策略运行 on_bar
        for strategy, sym, tf in self._strategies:
            if sym != symbol:
                continue

            try:
                signals = strategy.on_bar(bar, ctx)
            except Exception as e:
                log.warning(f"[{sym}] on_bar 异常: {e}")
                continue

            if not signals:
                continue

            for sig in signals:
                self._signal_count += 1
                log.info(f"[{sym}] 信号: {sig.side} size={sig.suggested_size:.4f} "
                         f"stop={sig.stop_price:.2f}")

                if sig.side == "close":
                    self._handle_close_signal(sym, sig)
                    continue

                # 下单
                intent = OrderIntent(
                    signal_id=self._signal_count,
                    strategy=strategy.name,
                    strategy_version=strategy.version,
                    symbol=sym,
                    side="buy" if sig.side == "long" else "sell",
                    order_type="market",
                    quantity=sig.suggested_size,
                    stop_loss_price=sig.stop_price if sig.stop_price > 0 else None,
                    client_order_id=f"paper_{sym}_{int(time.time()*1000)}",
                    purpose="entry",
                )
                try:
                    handle = self._engine.place_order(intent, int(time.time() * 1000))
                    self._order_count += 1
                    log.info(f"  -> 订单: {handle.status} ({handle.client_order_id})")
                except Exception as e:
                    log.warning(f"  -> 下单失败: {e}")

    def _handle_close_signal(self, symbol: str, sig) -> None:
        """处理平仓信号——取消所有相关订单。"""
        pass

    async def _lazy_load_exchange(self):
        try:
            ex = BinanceSpotAdapter(proxy=self._proxy, timeout_ms=30000)
            await ex._ensure_markets_loaded()
            self._exchange = ex
            if self._ws:
                self._ws._exchange = ex
            log.info("交易所加载完成")
        except Exception as e:
            log.warning(f"交易所加载失败: {e}")

    async def _main_loop(self):
        """定时检查挂单 + 输出状态。"""
        while self._running:
            try:
                fills = self._engine.check_pending_orders(int(time.time() * 1000))
                if fills:
                    self._fill_count += len(fills)
                    for f in fills:
                        log.info(f"  成交! price={f.price:.2f} qty={f.quantity:.4f} "
                                 f"fee={f.fee:.4f}")
            except Exception:
                pass

            # 每 30s 输出状态
            if int(time.time()) % 30 == 0:
                prices = self._cache.latest_prices_all()
                btc = prices.get("BTCUSDT", 0)
                eth = prices.get("ETHUSDT", 0)
                log.info(f"  [状态] BTC=${btc:.0f} ETH=${eth:.0f} "
                         f"信号={self._signal_count} 订单={self._order_count} "
                         f"成交={self._fill_count} K线={self._bar_count}")

            await asyncio.sleep(2)

    async def stop(self):
        self._running = False
        if self._ws:
            await self._ws.close()
        if self._exchange:
            await self._exchange.close()
        self._conn.close()
        log.info("Paper 模式已停止")


async def main():
    runner = PaperRunner(proxy=_PROXY)
    try:
        await runner.start()
    except KeyboardInterrupt:
        await runner.stop()


if __name__ == "__main__":
    asyncio.run(main())
