"""Dashboard paper-trading orchestration for live public-market data.

This module deliberately stays in paper mode: it reads public Binance market data,
creates simulated signals/orders/fills, and never talks to private trading APIs.
"""

from __future__ import annotations

import json
import math
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from itertools import pairwise
from typing import Any, Protocol

from core.data.exchange.base import Bar
from core.data.memory_cache import MemoryCache
from core.data.sqlite_repo import SqliteRepo
from core.execution.order_types import OrderHandle, OrderIntent
from core.execution.paper_engine import PaperMatchingEngine
from core.risk import (
    L1OrderRiskValidator,
    L2PositionRiskSizer,
    L3PortfolioRiskValidator,
    StrategySignalValidator,
)
from core.strategy import S1BtcEthTrend, S2AltcoinReversal
from core.strategy.base import DataRequirement, Signal, Strategy, StrategyContext

UNIVERSE_NAME = "top30"
UNIVERSE_VERSION = "binance-usdt-top30"

DEFAULT_TOP30_USDT = [
    "BTCUSDT",
    "ETHUSDT",
    "BNBUSDT",
    "SOLUSDT",
    "XRPUSDT",
    "DOGEUSDT",
    "ADAUSDT",
    "AVAXUSDT",
    "LINKUSDT",
    "TRXUSDT",
    "DOTUSDT",
    "POLUSDT",
    "LTCUSDT",
    "BCHUSDT",
    "UNIUSDT",
    "ATOMUSDT",
    "ETCUSDT",
    "FILUSDT",
    "APTUSDT",
    "ARBUSDT",
    "OPUSDT",
    "NEARUSDT",
    "INJUSDT",
    "SUIUSDT",
    "SEIUSDT",
    "AAVEUSDT",
    "MKRUSDT",
    "RUNEUSDT",
    "TIAUSDT",
    "WLDUSDT",
]

_STABLE_BASES = {"USDC", "FDUSD", "TUSD", "BUSD", "DAI", "USDP", "USDE", "EUR", "TRY"}
_LEVERAGED_SUFFIXES = ("UP", "DOWN", "BULL", "BEAR")
_EXPLORATION_MOMENTUM_MIN_MOVE_PCT = 0.0018
_EXPLORATION_MEAN_REVERSION_MIN_DEVIATION_PCT = 0.0018
_EXPLORATION_MEAN_REVERSION_MIN_ADAPTIVE_DEVIATION_PCT = 0.00075
_EXPLORATION_MEAN_REVERSION_TREND_LOOKBACK = 30
_EXPLORATION_MEAN_REVERSION_TREND_BLOCK_PCT = 0.006
_EXPLORATION_VOLATILITY_MIN_RANGE_PCT = 0.0035
_EXPLORATION_VOLATILITY_CLOSE_EXTREME = 0.80
_EXPLORATION_TARGET_RISK_MULTIPLIER = 2.2
_EXPLORATION_SIGNAL_TTL_MS = 8 * 60_000
_PAPER_TAKER_FEE_RATE = 0.0004
_PAPER_MARKET_SLIPPAGE_PCT = 0.0001
_MEAN_REVERSION_MIN_REVERSAL_EDGE_PCT = 0.003
_MEAN_REVERSION_MIN_REVERSAL_EDGE_USDT = 0.05
_MEAN_REVERSION_MIN_REVERSAL_FEE_MULTIPLE = 3.0
_MEAN_REVERSION_MIN_EXPECTED_NET_EDGE_PCT = 0.0015
_MEAN_REVERSION_MIN_EXPECTED_NET_EDGE_USDT = 0.15
_MEAN_REVERSION_LEGACY_DUST_NOTIONAL_MULTIPLE = 0.75
_MAINSTREAM_BASES = {
    "BTC",
    "ETH",
    "BNB",
    "SOL",
    "XRP",
    "DOGE",
    "ADA",
    "AVAX",
    "LINK",
    "TRX",
    "DOT",
    "POL",
    "LTC",
    "BCH",
    "UNI",
    "ATOM",
    "ETC",
    "FIL",
    "APT",
    "ARB",
    "OP",
    "NEAR",
    "INJ",
    "SUI",
    "SEI",
    "AAVE",
    "MKR",
    "RUNE",
    "TIA",
    "WLD",
    "TON",
    "ZEC",
    "PEPE",
    "ENA",
    "TAO",
    "ORDI",
    "ICP",
    "FET",
    "GALA",
    "SAND",
    "MANA",
    "CRV",
    "WIF",
    "JUP",
    "PENDLE",
    "LDO",
    "DYDX",
    "STX",
    "IMX",
    "RENDER",
    "AR",
    "KAS",
    "ALGO",
    "VET",
    "HBAR",
}


class _StrategyLike(Protocol):
    name: str
    min_bars: int

    def evaluate(self, symbol: str, bars: list[Bar]) -> Signal | None: ...


class _ClockAtBar:
    def __init__(self, now_ms: int) -> None:
        self._now_ms = now_ms

    def now_ms(self) -> int:
        return self._now_ms


class CoreStrategyAdapter:
    """Adapt a core Strategy into the dashboard's per-bar strategy protocol."""

    def __init__(
        self,
        strategy: Strategy,
        *,
        feed: Any,
        repo: SqliteRepo,
        account_equity: Callable[[], float] | float = 10_000.0,
        symbols: list[str] | None = None,
        follow_symbols: bool = False,
        trigger_timeframes: list[str] | None = None,
    ) -> None:
        self._strategy = strategy
        self._feed = feed
        self._repo = repo
        self._account_equity = account_equity
        self._base_requirement = strategy.required_data()
        self._follow_symbols = follow_symbols
        self._requirement = self._requirement_with_symbols(symbols)
        self._trigger_timeframes = set(trigger_timeframes or self._requirement.timeframes)
        self.name = strategy.name
        self.min_bars = max(1, min(self._requirement.history_lookback_bars, 120))

    @property
    def requirement(self) -> DataRequirement:
        return self._requirement

    def replace_symbols(self, symbols: list[str]) -> None:
        if self._follow_symbols:
            self._requirement = self._requirement_with_symbols(symbols)

    def supports(self, symbol: str, timeframe: str) -> bool:
        if self._requirement.symbols and symbol not in self._requirement.symbols:
            return False
        return not self._trigger_timeframes or timeframe in self._trigger_timeframes

    def evaluate(self, symbol: str, bars: list[Bar]) -> Signal | None:
        if not bars:
            return None
        bar = bars[-1]
        if not self.supports(symbol, bar.timeframe):
            return None
        ctx = StrategyContext(
            data=self._feed,
            clock=_ClockAtBar(bar.ts),
            repo=self._repo,
            strategy_name=self._strategy.name,
            account_equity=self._current_equity(),
        )
        signals = self._strategy.on_bar(bar, ctx)
        for signal in signals:
            if signal.symbol == symbol and signal.side in {"long", "short"}:
                return signal
        return None

    def _current_equity(self) -> float:
        if callable(self._account_equity):
            return float(self._account_equity())
        return float(self._account_equity)

    def _requirement_with_symbols(self, symbols: list[str] | None) -> DataRequirement:
        active_symbols = list(dict.fromkeys(symbols or self._base_requirement.symbols))
        return DataRequirement(
            symbols=active_symbols,
            timeframes=list(self._base_requirement.timeframes),
            history_lookback_bars=self._base_requirement.history_lookback_bars,
            needs_funding=self._base_requirement.needs_funding,
            needs_orderbook_l1=self._base_requirement.needs_orderbook_l1,
            needs_orderbook_l5=self._base_requirement.needs_orderbook_l5,
            subscribe_partial_bars=self._base_requirement.subscribe_partial_bars,
        )


@dataclass(slots=True)
class DashboardRiskResult:
    accepted: bool
    intent: OrderIntent | None = None
    source: str = "risk"
    reason: str | None = None


class DashboardRiskPipeline:
    """Apply the formal signal, portfolio, position, and order risk gates."""

    def __init__(
        self,
        *,
        signal_validator: StrategySignalValidator | None = None,
        portfolio_risk: L3PortfolioRiskValidator | None = None,
        position_risk: L2PositionRiskSizer | None = None,
        order_risk: L1OrderRiskValidator | None = None,
    ) -> None:
        self._signal_validator = signal_validator or StrategySignalValidator()
        self._portfolio_risk = portfolio_risk or L3PortfolioRiskValidator()
        self._position_risk = position_risk or L2PositionRiskSizer()
        self._order_risk = order_risk or L1OrderRiskValidator()

    def validate(
        self,
        *,
        signal: Signal,
        requirement: DataRequirement,
        reference_symbol: str,
        reference_price: float,
        intent: OrderIntent,
        open_positions: list[dict[str, Any]] | None = None,
        repo: SqliteRepo,
    ) -> DashboardRiskResult:
        signal_decision = self._signal_validator.validate(
            signal,
            requirement=requirement,
            reference_symbol=reference_symbol,
            reference_price=reference_price,
        )
        if not signal_decision.accepted:
            return DashboardRiskResult(False, source="signal", reason=signal_decision.reason)

        symbol_info = repo.get_symbol(intent.symbol)
        symbol_id = int(symbol_info["id"]) if symbol_info is not None else None
        portfolio_decision = self._portfolio_risk.validate(
            intent,
            reference_price=reference_price,
            open_positions=open_positions if open_positions is not None else repo.list_open_positions(),
            symbol_id=symbol_id,
        )
        if not portfolio_decision.accepted:
            return DashboardRiskResult(False, source="L3", reason=portfolio_decision.reason)

        position_decision = self._position_risk.size(intent, reference_price=reference_price)
        if not position_decision.accepted:
            return DashboardRiskResult(False, source="L2", reason=position_decision.reason)
        intent.quantity = position_decision.quantity

        order_decision = self._order_risk.validate(
            intent,
            symbol_info=dict(symbol_info or {}),
            reference_price=reference_price,
        )
        if not order_decision.accepted:
            return DashboardRiskResult(False, source="L1", reason=order_decision.reason)

        return DashboardRiskResult(True, intent=intent)


def select_top_usdt_symbols(rows: list[dict[str, Any]], limit: int = 30) -> list[str]:
    """Select mainstream USDT pairs by quote volume from Binance ticker rows."""
    candidates: list[tuple[float, str]] = []
    for row in rows:
        symbol = str(row.get("symbol") or "").upper()
        if not _is_mainstream_usdt_symbol(symbol):
            continue
        try:
            quote_volume = float(row.get("quoteVolume") or row.get("quote_volume") or 0)
        except (TypeError, ValueError):
            quote_volume = 0.0
        if quote_volume <= 0:
            continue
        candidates.append((quote_volume, symbol))
    candidates.sort(key=lambda item: (-item[0], item[1]))
    return [symbol for _, symbol in candidates[:limit]]


def upsert_dashboard_universe(repo: SqliteRepo, symbols: list[str]) -> int:
    """Persist the dashboard Top30 universe into the shared symbols table."""
    rows = []
    repo.clear_universe_column()
    for symbol in symbols:
        base = symbol.removesuffix("USDT")
        rows.append(
            {
                "exchange": "binance",
                "symbol": symbol,
                "type": "perp",
                "base": base,
                "quote": "USDT",
                "universe": UNIVERSE_NAME,
                "tick_size": _default_tick_size(symbol),
                "lot_size": _default_lot_size(symbol),
                "min_notional": 5.0,
                "listed_at": 1,
                "delisted_at": None,
            }
        )
    return repo.upsert_symbols(rows)


async def fetch_binance_top_usdt_symbols(
    *,
    proxy: str = "",
    limit: int = 30,
    timeout_sec: float = 6.0,
) -> list[str]:
    """Fetch the Binance public 24h ticker and return a filtered Top-N USDT universe."""
    import aiohttp

    timeout = aiohttp.ClientTimeout(total=timeout_sec)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(
            "https://fapi.binance.com/fapi/v1/ticker/24hr",
            proxy=proxy or None,
        ) as resp:
            resp.raise_for_status()
            rows = await resp.json()
    selected = select_top_usdt_symbols(rows, limit=limit)
    return selected or DEFAULT_TOP30_USDT[:limit]


def bars_from_binance_klines(symbol: str, timeframe: str, rows: list[list[Any]]) -> list[Bar]:
    """Normalize Binance kline arrays into closed project Bar objects."""
    bars = []
    for row in rows:
        if len(row) < 7:
            continue
        bars.append(
            Bar(
                symbol=symbol,
                timeframe=timeframe,
                ts=int(row[0]),
                o=float(row[1]),
                h=float(row[2]),
                l=float(row[3]),
                c=float(row[4]),
                v=float(row[5]),
                q=float(row[7]) if len(row) > 7 else 0.0,
                closed=True,
            )
        )
    return bars


async def fetch_binance_recent_klines(
    symbol: str,
    timeframe: str,
    *,
    proxy: str = "",
    limit: int = 20,
    timeout_sec: float = 6.0,
) -> list[Bar]:
    """Fetch recent public USD-M klines for warmup without private credentials."""
    import aiohttp

    timeout = aiohttp.ClientTimeout(total=timeout_sec)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(
            "https://fapi.binance.com/fapi/v1/klines",
            params={"symbol": symbol, "interval": timeframe, "limit": limit},
            proxy=proxy or None,
        ) as resp:
            resp.raise_for_status()
            rows = await resp.json()
    return bars_from_binance_klines(symbol, timeframe, rows)


@dataclass(slots=True)
class ExplorationStrategy:
    """Signal-rich paper strategy for collecting simulated trade samples."""

    name: str
    min_bars: int = 3
    confidence: float = 0.58

    def supports(self, symbol: str, timeframe: str) -> bool:
        return timeframe == "1m"

    def evaluate(self, symbol: str, bars: list[Bar]) -> Signal | None:
        if len(bars) < self.min_bars:
            return None
        last = bars[-1]
        previous = bars[-2] if len(bars) >= 2 else last
        if self.name.endswith("mean_reversion"):
            sma = _sma(bars[-self.min_bars :])
            if sma <= 0:
                return None
            deviation_pct = (last.c - sma) / sma
            deviation_threshold = _mean_reversion_deviation_threshold(bars)
            if abs(deviation_pct) < deviation_threshold:
                return None
            side = "long" if deviation_pct <= 0 else "short"
            trend = _local_trend_direction(
                bars,
                lookback=_EXPLORATION_MEAN_REVERSION_TREND_LOOKBACK,
                threshold_pct=_EXPLORATION_MEAN_REVERSION_TREND_BLOCK_PCT,
            )
            if (side == "long" and trend == "down") or (side == "short" and trend == "up"):
                return None
            rationale = {
                "type": "mean_reversion",
                "window": self.min_bars,
                "deviation_pct": deviation_pct,
                "deviation_threshold": deviation_threshold,
                "trend": trend,
            }
        elif self.name.endswith("volatility"):
            range_pct = (last.h - last.l) / last.c if last.c > 0 else 0.0
            if range_pct < _EXPLORATION_VOLATILITY_MIN_RANGE_PCT or last.h <= last.l:
                return None
            close_location = (last.c - last.l) / (last.h - last.l)
            if close_location >= _EXPLORATION_VOLATILITY_CLOSE_EXTREME:
                side = "long"
            elif close_location <= 1.0 - _EXPLORATION_VOLATILITY_CLOSE_EXTREME:
                side = "short"
            else:
                return None
            rationale = {"type": "atr_proxy", "range_pct": range_pct, "close_location": close_location}
        else:
            if previous.c <= 0 or abs(last.c - previous.c) / previous.c < _EXPLORATION_MOMENTUM_MIN_MOVE_PCT:
                return None
            side = "long" if last.c >= previous.c else "short"
            rationale = {"type": "momentum", "prev_close": previous.c, "move_pct": (last.c - previous.c) / previous.c}

        stop_distance = max(abs(last.c - last.l), last.c * 0.003)
        stop_price = last.c - stop_distance if side == "long" else last.c + stop_distance
        target_price = (
            last.c + stop_distance * _EXPLORATION_TARGET_RISK_MULTIPLIER
            if side == "long"
            else last.c - stop_distance * _EXPLORATION_TARGET_RISK_MULTIPLIER
        )
        return Signal(
            side=side,
            symbol=symbol,
            entry_price=None,
            stop_price=round(stop_price, 8),
            target_price=round(target_price, 8),
            confidence=self.confidence,
            suggested_size=0.0,
            rationale=rationale,
            expires_in_ms=_EXPLORATION_SIGNAL_TTL_MS,
        )


class DashboardPaperTrader:
    """Run several exploration strategies per symbol and persist paper trades."""

    def __init__(
        self,
        *,
        repo: SqliteRepo,
        cache: MemoryCache,
        engine: PaperMatchingEngine,
        symbols: list[str],
        strategies: list[_StrategyLike] | None = None,
        notional_usdt: float = 20.0,
        strategy_notional_multipliers: dict[str, float] | None = None,
        cooldown_ms: int = 120_000,
        max_orders_per_symbol: int = 120,
        order_cap_window_ms: int = 60 * 60 * 1000,
        max_open_notional_usdt: float | None = None,
        min_exit_age_ms: int = 60_000,
        risk_pipeline: DashboardRiskPipeline | None = None,
    ) -> None:
        self._repo = repo
        self._cache = cache
        self._engine = engine
        self._symbols = list(dict.fromkeys(symbols))
        self._strategies = strategies or default_exploration_strategies()
        self._notional_usdt = max(notional_usdt, 5.0)
        self._strategy_notional_multipliers = {
            strategy: max(float(multiplier), 0.0)
            for strategy, multiplier in (strategy_notional_multipliers or {}).items()
        }
        self._cooldown_ms = max(cooldown_ms, 0)
        self._max_orders_per_symbol = max(max_orders_per_symbol, 1)
        self._order_cap_window_ms = max(order_cap_window_ms, 60_000)
        self._max_open_notional_usdt = (
            max(float(max_open_notional_usdt), 0.0) if max_open_notional_usdt is not None else None
        )
        self._min_exit_age_ms = max(int(min_exit_age_ms), 0)
        self._risk_pipeline = risk_pipeline or DashboardRiskPipeline()
        self._last_trade_ms: dict[tuple[str, str], int] = {}
        self._evaluations: dict[tuple[str, str], dict[str, Any]] = {}

    @property
    def symbols(self) -> list[str]:
        return list(self._symbols)

    @property
    def strategies(self) -> list[str]:
        return [strategy.name for strategy in self._strategies]

    @property
    def active_strategy_names(self) -> list[str]:
        return self.strategies

    def replace_symbols(self, symbols: list[str]) -> None:
        self._symbols = list(dict.fromkeys(symbols))
        for strategy in self._strategies:
            replace_symbols = getattr(strategy, "replace_symbols", None)
            if callable(replace_symbols):
                replace_symbols(self._symbols)

    def on_bar(self, bar: Bar, now_ms: int | None = None) -> list[OrderHandle]:
        if not bar.closed or bar.symbol not in self._symbols:
            return []
        now = now_ms or int(time.time() * 1000)
        self._cache.push_bar(bar)
        handles: list[OrderHandle] = []
        bars = self._cache.get_bars(bar.symbol, bar.timeframe, n=500)
        for strategy in self._strategies:
            supports = getattr(strategy, "supports", None)
            if callable(supports) and not supports(bar.symbol, bar.timeframe):
                continue
            exit_handle = self._close_position_if_exit_triggered(bar, strategy.name, now)
            if exit_handle is not None:
                self._evaluations[(bar.symbol, strategy.name)] = {
                    "symbol": bar.symbol,
                    "strategy": strategy.name,
                    "bars": len(bars),
                    "ready": len(bars) >= strategy.min_bars,
                    "last_eval_at": now,
                    "last_signal": "exit",
                }
                self._last_trade_ms[(bar.symbol, strategy.name)] = now
                handles.append(exit_handle)
                continue
            signal = strategy.evaluate(bar.symbol, bars)
            self._evaluations[(bar.symbol, strategy.name)] = {
                "symbol": bar.symbol,
                "strategy": strategy.name,
                "bars": len(bars),
                "ready": len(bars) >= strategy.min_bars,
                "last_eval_at": now,
                "last_signal": signal.side if signal else None,
            }
            if signal is None:
                continue
            handle = self._place_signal(strategy.name, signal, now)
            if handle is not None:
                handles.append(handle)
        return handles

    def strategy_matrix(self) -> dict[str, Any]:
        rows = self._repo._conn.execute(
            "SELECT s.symbol, o.strategy_version, COUNT(o.id) AS orders, "
            "COUNT(f.id) AS fills, COALESCE(SUM(f.price * f.quantity), 0) AS notional, "
            "COALESCE(SUM(f.fee), 0) AS fees "
            "FROM symbols s "
            "LEFT JOIN orders o ON o.symbol_id = s.id "
            "LEFT JOIN fills f ON f.order_id = o.id "
            "WHERE s.universe = ? "
            "GROUP BY s.symbol, o.strategy_version "
            "ORDER BY s.symbol, o.strategy_version",
            (UNIVERSE_NAME,),
        ).fetchall()
        metrics = {
            (row["symbol"], row["strategy_version"]): row
            for row in rows
            if row["strategy_version"] is not None
        }
        cells = []
        for symbol in self._symbols:
            for strategy in self.strategies:
                row = metrics.get((symbol, strategy))
                evaluation = self._evaluations.get((symbol, strategy), {})
                cells.append(
                    {
                        "symbol": symbol,
                        "strategy": strategy,
                        "ready": bool(evaluation.get("ready")),
                        "bars": int(evaluation.get("bars") or 0),
                        "last_signal": evaluation.get("last_signal"),
                        "last_eval_at": evaluation.get("last_eval_at"),
                        "throttled": bool(evaluation.get("throttled")),
                        "throttle_reason": evaluation.get("throttle_reason"),
                        "orders": int(row["orders"] if row is not None else 0),
                        "fills": int(row["fills"] if row is not None else 0),
                        "notional": round(float(row["notional"] if row is not None else 0), 2),
                        "fees": round(float(row["fees"] if row is not None else 0), 4),
                    }
                )
        return {"symbols": self._symbols, "strategies": self.strategies, "cells": cells}

    def close_legacy_tiny_positions(self, now_ms: int | None = None) -> list[OrderHandle]:
        now = now_ms or int(time.time() * 1000)
        handles: list[OrderHandle] = []
        for position in self._open_positions_for_active_strategies():
            strategy_name = str(position["strategy_version"] or "")
            if now - int(position["opened_at"] or 0) <= self._min_exit_age_ms:
                continue
            symbol_row = self._repo.get_symbol_by_id(int(position["symbol_id"]))
            if symbol_row is None:
                continue
            symbol = str(symbol_row["symbol"])
            price = self._cache.latest_price(symbol) or float(position["current_price"] or position["avg_entry_price"] or 0.0)
            if not self._is_legacy_tiny_mean_reversion_position(strategy_name, position, price):
                continue
            handles.append(
                self._engine.close_position(
                    symbol=symbol,
                    strategy=strategy_name,
                    strategy_version=strategy_name,
                    client_order_id=f"{strategy_name}-{symbol}-exit-legacy_dust-{now}-{uuid.uuid4().hex[:6]}",
                    now_ms=now,
                )
            )
        return handles

    def _place_signal(self, strategy_name: str, signal: Signal, now_ms: int) -> OrderHandle | None:
        key = (signal.symbol, strategy_name)
        if now_ms - self._last_trade_ms.get(key, 0) < self._cooldown_ms:
            evaluation = self._evaluations.setdefault(
                key,
                {
                    "symbol": signal.symbol,
                    "strategy_id": strategy_name,
                    "ready": True,
                    "bars": 0,
                },
            )
            evaluation["throttled"] = True
            evaluation["throttle_reason"] = "cooldown"
            evaluation["cooldown_until_ms"] = self._last_trade_ms.get(key, 0) + self._cooldown_ms
            return None
        price = self._cache.latest_price(signal.symbol)
        if price is None or price <= 0:
            self._record_risk("missing_price", "warn", strategy_name, signal, now_ms)
            return None
        if self._orders_for_symbol(signal.symbol, since_ms=now_ms - self._order_cap_window_ms) >= self._max_orders_per_symbol:
            evaluation = self._evaluations.setdefault(
                key,
                {
                    "symbol": signal.symbol,
                    "strategy_id": strategy_name,
                    "ready": True,
                    "bars": 0,
                },
            )
            evaluation["throttled"] = True
            evaluation["throttle_reason"] = "symbol_order_cap"
            evaluation["order_cap_window_ms"] = self._order_cap_window_ms
            return None
        if (
            self._max_open_notional_usdt is not None
            and self._portfolio_open_notional() >= self._max_open_notional_usdt
            and self._signal_adds_exposure(strategy_name, signal)
        ):
            evaluation = self._evaluations.setdefault(
                key,
                {
                    "symbol": signal.symbol,
                    "strategy_id": strategy_name,
                    "ready": True,
                    "bars": 0,
                },
            )
            evaluation["throttled"] = True
            evaluation["throttle_reason"] = "portfolio_notional_cap"
            evaluation["max_open_notional_usdt"] = self._max_open_notional_usdt
            return None
        target_notional = self._notional_for_strategy(strategy_name)
        raw_quantity = signal.suggested_size if signal.suggested_size > 0 else target_notional / price
        quantity = self._round_qty(signal.symbol, raw_quantity)
        min_notional = self._min_notional(signal.symbol)
        if quantity * price < min_notional and raw_quantity * price >= min_notional:
            quantity = self._round_qty(signal.symbol, min_notional / price, rounding="up")
        signal.suggested_size = quantity
        if self._mean_reversion_reversal_edge_too_small(strategy_name, signal, price, quantity):
            evaluation = self._evaluations.setdefault(
                key,
                {
                    "symbol": signal.symbol,
                    "strategy_id": strategy_name,
                    "ready": True,
                    "bars": 0,
                },
            )
            evaluation["throttled"] = True
            evaluation["throttle_reason"] = "insufficient_edge"
            return None
        if self._mean_reversion_expected_edge_too_small(strategy_name, signal, price, quantity):
            evaluation = self._evaluations.setdefault(
                key,
                {
                    "symbol": signal.symbol,
                    "strategy_id": strategy_name,
                    "ready": True,
                    "bars": 0,
                },
            )
            evaluation["throttled"] = True
            evaluation["throttle_reason"] = "insufficient_expected_edge"
            return None
        signal_id = self._insert_signal(strategy_name, signal, now_ms)
        side = "buy" if signal.side == "long" else "sell"
        intent = OrderIntent(
            signal_id=signal_id,
            strategy=strategy_name,
            strategy_version=strategy_name,
            trade_group_id=signal.trade_group_id or uuid.uuid4().hex[:12],
            symbol=signal.symbol,
            side=side,
            order_type="market",
            quantity=quantity,
            purpose="entry",
            stop_loss_price=signal.stop_price,
            client_order_id=f"{strategy_name}-{signal.symbol}-{now_ms}-{uuid.uuid4().hex[:6]}",
        )
        risk = self._risk_pipeline.validate(
            signal=signal,
            requirement=self._requirement_for_strategy(strategy_name, signal.symbol),
            reference_symbol=signal.symbol,
            reference_price=price,
            intent=intent,
            open_positions=self._open_positions_for_active_strategies(),
            repo=self._repo,
        )
        if not risk.accepted or risk.intent is None:
            self._record_risk(
                risk.reason or "risk_rejected",
                "warn",
                strategy_name,
                signal,
                now_ms,
                source=risk.source,
                event_type="order_rejected",
            )
            self._repo._conn.execute("UPDATE signals SET status=?, reject_reason=? WHERE id=?", ("rejected", risk.reason, signal_id))
            self._repo._conn.commit()
            return None
        handle = self._engine.place_order(
            risk.intent,
            now_ms,
        )
        self._repo._conn.execute(
            "UPDATE signals SET status=? WHERE id=?",
            ("placed" if handle.status in {"accepted", "filled"} else handle.status, signal_id),
        )
        self._repo._conn.commit()
        self._last_trade_ms[key] = now_ms
        return handle

    def _close_position_if_exit_triggered(
        self,
        bar: Bar,
        strategy_name: str,
        now_ms: int,
    ) -> OrderHandle | None:
        symbol_row = self._repo.get_symbol(bar.symbol)
        if symbol_row is None:
            return None
        position = self._repo.get_open_position(int(symbol_row["id"]), strategy_name)
        if position is None:
            return None
        if now_ms - int(position["opened_at"] or 0) <= self._min_exit_age_ms:
            return None
        if self._is_legacy_tiny_mean_reversion_position(strategy_name, position, bar.c):
            return self._engine.close_position(
                symbol=bar.symbol,
                strategy=strategy_name,
                strategy_version=strategy_name,
                client_order_id=f"{strategy_name}-{bar.symbol}-exit-legacy_dust-{now_ms}-{uuid.uuid4().hex[:6]}",
                now_ms=now_ms,
            )
        signal_row = self._opening_signal_for_position(position)
        if signal_row is None:
            return None

        reason = self._exit_reason(bar, position, signal_row, now_ms)
        if reason is None:
            return None

        return self._engine.close_position(
            symbol=bar.symbol,
            strategy=strategy_name,
            strategy_version=strategy_name,
            client_order_id=f"{strategy_name}-{bar.symbol}-exit-{reason}-{now_ms}-{uuid.uuid4().hex[:6]}",
            now_ms=now_ms,
        )

    def _opening_signal_for_position(self, position: dict[str, Any]) -> dict[str, Any] | None:
        signal_id = position.get("opening_signal_id")
        if signal_id is None:
            return None
        row = self._repo._conn.execute("SELECT * FROM signals WHERE id=?", (int(signal_id),)).fetchone()
        return dict(row) if row is not None else None

    @staticmethod
    def _exit_reason(
        bar: Bar,
        position: dict[str, Any],
        signal_row: dict[str, Any],
        now_ms: int,
    ) -> str | None:
        stop = float(signal_row["stop_price"] or 0.0)
        target = float(signal_row["target_price"] or 0.0)
        side = str(position["side"])
        if side == "long":
            if stop > 0 and bar.l <= stop:
                return "stop"
            if target > 0 and bar.h >= target:
                return "target"
        else:
            if stop > 0 and bar.h >= stop:
                return "stop"
            if target > 0 and bar.l <= target:
                return "target"
        expires_at = int(signal_row["expires_at"] or 0)
        if expires_at > 0 and now_ms >= expires_at:
            return "ttl"
        return None

    def _is_legacy_tiny_mean_reversion_position(
        self,
        strategy_name: str,
        position: dict[str, Any],
        reference_price: float,
    ) -> bool:
        if not strategy_name.endswith("mean_reversion") or reference_price <= 0:
            return False
        notional = abs(float(position["qty"] or 0.0) * reference_price)
        threshold = self._notional_for_strategy(strategy_name) * _MEAN_REVERSION_LEGACY_DUST_NOTIONAL_MULTIPLE
        return notional < threshold

    def _insert_signal(self, strategy_name: str, signal: Signal, now_ms: int) -> int:
        symbol_row = self._repo.get_symbol(signal.symbol)
        if symbol_row is None:
            raise RuntimeError(f"symbol not found: {signal.symbol}")
        cur = self._repo._conn.execute(
            "INSERT INTO signals (strategy, strategy_version, config_hash, universe_version, "
            "run_id, symbol_id, side, entry_price, stop_price, target_price, confidence, "
            "suggested_size, time_in_force, rationale, status, reject_reason, trade_group_id, "
            "captured_at, expires_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                strategy_name,
                strategy_name,
                "dashboard-explore",
                UNIVERSE_VERSION,
                "dashboard-live-paper",
                symbol_row["id"],
                signal.side,
                signal.entry_price,
                signal.stop_price or 0.0,
                signal.target_price,
                signal.confidence,
                signal.suggested_size,
                signal.time_in_force,
                json.dumps(signal.rationale, ensure_ascii=False),
                "pending",
                None,
                signal.trade_group_id,
                now_ms,
                now_ms + signal.expires_in_ms,
            ),
        )
        self._repo._conn.commit()
        if cur.lastrowid is None:
            raise RuntimeError("signal insert did not return lastrowid")
        return int(cur.lastrowid)

    def _record_risk(
        self,
        reason: str,
        severity: str,
        strategy_name: str,
        signal: Signal,
        now_ms: int,
        *,
        source: str = "dashboard_trader",
        event_type: str = "paper_signal_skipped",
    ) -> None:
        self._repo.insert_risk_event(
            {
                "type": event_type,
                "severity": severity,
                "source": source,
                "related_id": None,
                "payload": json.dumps(
                    {"reason": reason, "symbol": signal.symbol, "strategy": strategy_name},
                    ensure_ascii=False,
                ),
                "captured_at": now_ms,
            }
        )

    def _orders_for_symbol(self, symbol: str, *, since_ms: int) -> int:
        row = self._repo._conn.execute(
            "SELECT COUNT(o.id) AS n FROM orders o "
            "JOIN symbols s ON s.id = o.symbol_id WHERE s.symbol = ? AND o.placed_at >= ?",
            (symbol, since_ms),
        ).fetchone()
        return int(row["n"] or 0)

    def _portfolio_open_notional(self) -> float:
        active = self._open_positions_for_active_strategies()
        total = 0.0
        for row in active:
            symbol_row = self._repo.get_symbol_by_id(int(row["symbol_id"]))
            symbol = str(symbol_row["symbol"]) if symbol_row is not None else ""
            price = self._cache.latest_price(symbol) or row["current_price"] or row["avg_entry_price"]
            total += abs(float(row["qty"] or 0.0) * float(price or 0.0))
        return total

    def _open_positions_for_active_strategies(self) -> list[dict[str, Any]]:
        active = set(self.active_strategy_names)
        return [
            row
            for row in self._repo.list_open_positions()
            if str(row.get("strategy_version") or "") in active
        ]

    def _signal_adds_exposure(self, strategy_name: str, signal: Signal) -> bool:
        symbol_row = self._repo.get_symbol(signal.symbol)
        if symbol_row is None:
            return True
        current = self._repo.get_open_position(int(symbol_row["id"]), strategy_name)
        if current is None:
            return True
        desired_side = "long" if signal.side == "long" else "short"
        return str(current["side"]) == desired_side

    def _mean_reversion_reversal_edge_too_small(
        self,
        strategy_name: str,
        signal: Signal,
        reference_price: float,
        order_quantity: float,
    ) -> bool:
        if not strategy_name.endswith("mean_reversion"):
            return False
        symbol_row = self._repo.get_symbol(signal.symbol)
        if symbol_row is None:
            return False
        position = self._repo.get_open_position(int(symbol_row["id"]), strategy_name)
        if position is None:
            return False
        desired_side = "long" if signal.side == "long" else "short"
        current_side = str(position["side"])
        if current_side == desired_side:
            return False
        close_qty = min(abs(float(position["qty"] or 0.0)), abs(order_quantity))
        if close_qty <= 0 or reference_price <= 0:
            return True
        entry_price = float(position["avg_entry_price"] or 0.0)
        if entry_price <= 0:
            return True
        exit_price = (
            reference_price * (1.0 - _PAPER_MARKET_SLIPPAGE_PCT)
            if current_side == "long"
            else reference_price * (1.0 + _PAPER_MARKET_SLIPPAGE_PCT)
        )
        gross_pnl = (
            (exit_price - entry_price) * close_qty
            if current_side == "long"
            else (entry_price - exit_price) * close_qty
        )
        close_notional = close_qty * exit_price
        exit_fee = close_notional * _PAPER_TAKER_FEE_RATE
        required_gross = max(
            close_notional * _MEAN_REVERSION_MIN_REVERSAL_EDGE_PCT,
            exit_fee * _MEAN_REVERSION_MIN_REVERSAL_FEE_MULTIPLE,
            _MEAN_REVERSION_MIN_REVERSAL_EDGE_USDT,
        )
        return gross_pnl < required_gross

    def _mean_reversion_expected_edge_too_small(
        self,
        strategy_name: str,
        signal: Signal,
        reference_price: float,
        order_quantity: float,
    ) -> bool:
        if not strategy_name.endswith("mean_reversion"):
            return False
        if signal.target_price is None or signal.target_price <= 0:
            return False
        if reference_price <= 0 or order_quantity <= 0:
            return True
        if signal.side == "long":
            entry_price = reference_price * (1.0 + _PAPER_MARKET_SLIPPAGE_PCT)
            exit_price = float(signal.target_price) * (1.0 - _PAPER_MARKET_SLIPPAGE_PCT)
            gross_pnl = (exit_price - entry_price) * order_quantity
        else:
            entry_price = reference_price * (1.0 - _PAPER_MARKET_SLIPPAGE_PCT)
            exit_price = float(signal.target_price) * (1.0 + _PAPER_MARKET_SLIPPAGE_PCT)
            gross_pnl = (entry_price - exit_price) * order_quantity
        entry_notional = order_quantity * entry_price
        exit_notional = order_quantity * exit_price
        expected_net = gross_pnl - (entry_notional + exit_notional) * _PAPER_TAKER_FEE_RATE
        required_net = max(
            entry_notional * _MEAN_REVERSION_MIN_EXPECTED_NET_EDGE_PCT,
            _MEAN_REVERSION_MIN_EXPECTED_NET_EDGE_USDT,
        )
        return expected_net < required_net

    def _notional_for_strategy(self, strategy_name: str) -> float:
        multiplier = self._strategy_notional_multipliers.get(strategy_name, 1.0)
        return max(self._notional_usdt * multiplier, 5.0)

    def _requirement_for_strategy(self, strategy_name: str, symbol: str) -> DataRequirement:
        for strategy in self._strategies:
            if strategy.name == strategy_name and hasattr(strategy, "requirement"):
                return strategy.requirement
        return DataRequirement(symbols=[symbol], timeframes=["1m"])

    def _min_notional(self, symbol: str) -> float:
        row = self._repo.get_symbol(symbol)
        if row is None:
            return 5.0
        return max(float(row["min_notional"] or 0.0), 5.0)

    def _round_qty(self, symbol: str, quantity: float, *, rounding: str = "down") -> float:
        row = self._repo.get_symbol(symbol)
        lot_size = float(row["lot_size"] if row is not None else 0.0001)
        if lot_size <= 0:
            return round(quantity, 8)
        raw_steps = quantity / lot_size
        steps = math.ceil(raw_steps) if rounding == "up" else int(raw_steps)
        steps = max(steps, 1)
        return round(steps * lot_size, 8)


def default_exploration_strategies(*, prefix: str = "explore") -> list[ExplorationStrategy]:
    return [
        ExplorationStrategy(f"{prefix}_momentum", min_bars=2),
        ExplorationStrategy(f"{prefix}_mean_reversion", min_bars=3),
        ExplorationStrategy(f"{prefix}_volatility", min_bars=2),
    ]


def default_dashboard_strategies(
    *,
    feed: Any,
    repo: SqliteRepo,
    account_equity: Callable[[], float] | float = 10_000.0,
) -> list[_StrategyLike]:
    return [
        CoreStrategyAdapter(
            S1BtcEthTrend(),
            feed=feed,
            repo=repo,
            account_equity=account_equity,
            trigger_timeframes=["4h"],
        ),
        CoreStrategyAdapter(
            S2AltcoinReversal(),
            feed=feed,
            repo=repo,
            account_equity=account_equity,
            symbols=DEFAULT_TOP30_USDT,
            follow_symbols=True,
            trigger_timeframes=["1h"],
        ),
        ExplorationStrategy("paper_mean_reversion", min_bars=3),
    ]


def _is_mainstream_usdt_symbol(symbol: str) -> bool:
    if not symbol.endswith("USDT"):
        return False
    base = symbol.removesuffix("USDT")
    if not base or base in _STABLE_BASES:
        return False
    if base not in _MAINSTREAM_BASES:
        return False
    return not any(base.endswith(suffix) for suffix in _LEVERAGED_SUFFIXES)


def _sma(bars: list[Bar]) -> float:
    return sum(bar.c for bar in bars) / len(bars)


def _mean_reversion_deviation_threshold(bars: list[Bar]) -> float:
    if len(bars) < 3:
        return _EXPLORATION_MEAN_REVERSION_MIN_DEVIATION_PCT
    recent = bars[-min(len(bars), 20) :]
    returns = [
        abs(cur.c - prev.c) / prev.c
        for prev, cur in pairwise(recent)
        if prev.c > 0
    ]
    if not returns:
        return _EXPLORATION_MEAN_REVERSION_MIN_DEVIATION_PCT
    realized_noise = sum(returns) / len(returns)
    return max(
        _EXPLORATION_MEAN_REVERSION_MIN_ADAPTIVE_DEVIATION_PCT,
        min(_EXPLORATION_MEAN_REVERSION_MIN_DEVIATION_PCT, realized_noise * 1.5),
    )


def _local_trend_direction(
    bars: list[Bar],
    *,
    lookback: int,
    threshold_pct: float,
) -> str:
    if len(bars) <= lookback:
        return "flat"
    start = bars[-lookback - 1].c
    end = bars[-1].c
    if start <= 0:
        return "flat"
    move_pct = (end - start) / start
    if move_pct >= threshold_pct:
        return "up"
    if move_pct <= -threshold_pct:
        return "down"
    return "flat"


def _default_tick_size(symbol: str) -> float:
    if symbol.startswith(("BTC", "ETH", "BNB")):
        return 0.01
    return 0.0001


def _default_lot_size(symbol: str) -> float:
    if symbol.startswith("BTC"):
        return 0.0001
    if symbol.startswith("ETH"):
        return 0.001
    return 0.01


__all__ = [
    "DEFAULT_TOP30_USDT",
    "UNIVERSE_NAME",
    "CoreStrategyAdapter",
    "DashboardPaperTrader",
    "DashboardRiskPipeline",
    "ExplorationStrategy",
    "bars_from_binance_klines",
    "default_dashboard_strategies",
    "default_exploration_strategies",
    "fetch_binance_recent_klines",
    "fetch_binance_top_usdt_symbols",
    "select_top_usdt_symbols",
    "upsert_dashboard_universe",
]
