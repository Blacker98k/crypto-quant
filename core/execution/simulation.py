"""Synthetic paper-session runner for regression testing."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

from core.common.clock import FixedClock
from core.data.exchange.base import Bar
from core.data.feed import LiveFeed
from core.data.memory_cache import MemoryCache
from core.execution.order_types import OrderIntent
from core.execution.paper_engine import PaperMatchingEngine
from core.risk import L1OrderRiskValidator
from core.strategy.base import Strategy, StrategyContext


class _NoopParquetIO:
    def read_bars(
        self,
        symbol: str,
        timeframe: str,
        start_ms: int | None = None,
        end_ms: int | None = None,
        n: int | None = None,
    ) -> list[Bar]:
        return []


@dataclass(frozen=True, slots=True)
class SimulationResult:
    bars: int = 0
    signals: int = 0
    rejected: int = 0
    orders: int = 0
    fills: int = 0
    open_positions: int = 0


class SimulatedPaperSession:
    """Run strategies over synthetic bars through risk and paper execution."""

    def __init__(self, repo: Any, strategies: list[Strategy]) -> None:
        self._repo = repo
        self._strategies = strategies
        self._cache = MemoryCache(max_bars=2000)
        self._feed = LiveFeed(cast(Any, _NoopParquetIO()), repo, self._cache)
        self._engine = PaperMatchingEngine(repo, get_price=lambda symbol: self._cache.latest_price(symbol))
        self._risk = L1OrderRiskValidator()

    def run(self, bars: list[Bar]) -> SimulationResult:
        signals = rejected = orders = fills = 0
        for bar in bars:
            self._cache.push_bar(bar)
            for strategy in self._strategies:
                requirement = strategy.required_data()
                if bar.symbol not in requirement.symbols or bar.timeframe not in requirement.timeframes:
                    continue
                ctx = StrategyContext(
                    data=self._feed,
                    clock=FixedClock(bar.ts),
                    repo=self._repo,
                    strategy_name=strategy.name,
                )
                produced = strategy.on_bar(bar, ctx)
                signals += len(produced)
                for signal_index, signal in enumerate(produced, start=1):
                    cid = f"sim_{strategy.name}_{bar.symbol}_{bar.ts}_{signal_index}"
                    if signal.side == "close":
                        handle = self._engine.close_position(
                            symbol=bar.symbol,
                            strategy=strategy.name,
                            strategy_version=strategy.version,
                            client_order_id=cid,
                            now_ms=bar.ts,
                        )
                        if handle is not None:
                            orders += 1
                            fills += len(self._repo.get_fills(self._repo.get_order(cid)["id"]))
                        continue

                    intent = OrderIntent(
                        signal_id=0,
                        strategy=strategy.name,
                        strategy_version=strategy.version,
                        trade_group_id=signal.trade_group_id,
                        symbol=bar.symbol,
                        side="buy" if signal.side == "long" else "sell",
                        order_type="market",
                        quantity=signal.suggested_size,
                        stop_loss_price=signal.stop_price,
                        client_order_id=cid,
                        purpose="entry",
                    )
                    decision = self._risk.validate(
                        intent,
                        symbol_info=self._repo.get_symbol(bar.symbol) or {},
                        reference_price=bar.c,
                    )
                    if not decision.accepted:
                        rejected += 1
                        continue
                    self._engine.place_order(intent, bar.ts)
                    orders += 1
                    fills += len(self._repo.get_fills(self._repo.get_order(cid)["id"]))

        open_positions = len(
            self._repo._conn.execute(
                "SELECT id FROM positions WHERE closed_at IS NULL"
            ).fetchall()
        )
        return SimulationResult(
            bars=len(bars),
            signals=signals,
            rejected=rejected,
            orders=orders,
            fills=fills,
            open_positions=open_positions,
        )


__all__ = ["SimulatedPaperSession", "SimulationResult"]
