"""后台数据模拟线程——生成合成行情 + 随机 paper 订单。

按 MockExchangeAdapter 生成连续的 K 线数据，写入 MemoryCache + ParquetIO，
同时随机触发 PaperMatchingEngine 模拟订单，让看板有"活的"交易数据可展示。
"""

from __future__ import annotations

import random
import threading
import time
import uuid
from typing import ClassVar

from core.common.time_utils import tf_interval_ms
from core.execution.order_types import OrderIntent
from tests.mocks.exchange import _generate_bars


class DataSimulator:
    """后台线程：生成合成行情 + 模拟 paper 交易。"""

    __slots__ = (
        "_cache",
        "_engine",
        "_last_price",
        "_last_ts",
        "_paper_trading",
        "_parquet_io",
        "_repo",
        "_running",
        "_thread",
    )

    SYMBOLS: ClassVar[list[str]] = ["BTCUSDT", "ETHUSDT"]
    TIMEFRAMES: ClassVar[list[str]] = ["1m", "5m", "1h"]

    def __init__(self, cache, repo, parquet_io, engine) -> None:
        self._cache = cache       # MemoryCache
        self._repo = repo          # SqliteRepo
        self._parquet_io = parquet_io  # ParquetIO
        self._engine = engine      # PaperMatchingEngine
        self._last_ts: dict[tuple[str, str], int] = {}
        self._last_price: dict[str, float] = {"BTCUSDT": 50000.0, "ETHUSDT": 3000.0}
        self._running = False
        self._paper_trading = True
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._running = False

    def is_running(self) -> bool:
        return self._running

    def toggle_paper_trading(self) -> bool:
        self._paper_trading = not self._paper_trading
        return self._paper_trading

    # ─── 内部循环 ─────────────────────────────────────────────────────────

    def _run_loop(self) -> None:
        while self._running:
            now_ms = int(time.time() * 1000)
            try:
                self._tick(now_ms)
            except Exception:
                pass  # 单次 tick 失败不影响后续
            time.sleep(1.0)

    def _tick(self, now_ms: int) -> None:
        # 1. 生成新 K 线
        for symbol in self.SYMBOLS:
            for tf in self.TIMEFRAMES:
                self._gen_bars(symbol, tf, now_ms)

        # 2. 检查挂单是否触发成交
        if self._paper_trading and self._engine is not None:
            self._engine.check_pending_orders(now_ms)

        # 3. 随机下单（10% 概率）
        if self._paper_trading and self._engine is not None and random.random() < 0.1:
            self._place_random_order(now_ms)

    def _gen_bars(self, symbol: str, tf: str, now_ms: int) -> None:
        interval = tf_interval_ms(tf)
        key = (symbol, tf)
        last = self._last_ts.get(key, 0)

        # 首次生成往前推 500 根历史，之后只生成新 bar
        if last == 0:
            start_ms = now_ms - interval * 500
        else:
            start_ms = last + interval

        end_ms = now_ms
        if end_ms - start_ms < interval:
            return  # 还不够一根新 bar

        bars = _generate_bars(symbol, tf, start_ms, end_ms, base_price=self._last_price[symbol])
        for b in bars:
            if b.ts > last:
                self._cache.push_bar(b)
                self._last_ts[key] = b.ts
                if b.closed:
                    self._last_price[symbol] = b.c

        # 周期性地写 Parquet（每 100 根一批）
        if len(bars) > 0:
            self._parquet_io.write_bars(bars)

    def _place_random_order(self, now_ms: int) -> None:
        symbol = random.choice(self.SYMBOLS)
        price = self._last_price.get(symbol, 50000.0)
        side = random.choice(["buy", "sell"])
        order_type = random.choice(["market", "limit", "limit", "limit"])  # limit 3x more likely
        quantity = round(random.uniform(0.001, 0.05), 4)

        intent = OrderIntent(
            signal_id=0,
            strategy=random.choice(["paper_mean_reversion", "paper_swing_breakout"]),
            strategy_version="dev",
            client_order_id=f"paper_{uuid.uuid4().hex[:8]}",
            symbol=symbol,
            side=side,
            order_type=order_type,
            quantity=quantity,
            price=round(price * random.uniform(0.99, 1.01), 2) if order_type == "limit" else None,
            purpose="entry",
        )
        try:
            self._engine.place_order(intent, now_ms)
        except Exception:
            pass  # 幂等冲突等忽略


__all__ = ["DataSimulator"]
