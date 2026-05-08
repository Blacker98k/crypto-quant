"""Dashboard paper-trading orchestration for live public-market data.

This module deliberately stays in paper mode: it reads public Binance market data,
creates simulated signals/orders/fills, and never talks to private trading APIs.
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass
from typing import Any, Protocol

from core.data.exchange.base import Bar
from core.data.memory_cache import MemoryCache
from core.data.sqlite_repo import SqliteRepo
from core.execution.order_types import OrderHandle, OrderIntent
from core.execution.paper_engine import PaperMatchingEngine
from core.strategy.base import Signal

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
    "MATICUSDT",
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
    "MATIC",
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

    def evaluate(self, symbol: str, bars: list[Bar]) -> Signal | None:
        if len(bars) < self.min_bars:
            return None
        last = bars[-1]
        previous = bars[-2] if len(bars) >= 2 else last
        if self.name.endswith("mean_reversion"):
            side = "long" if last.c <= _sma(bars[-self.min_bars :]) else "short"
            rationale = {"type": "mean_reversion", "window": self.min_bars}
        elif self.name.endswith("volatility"):
            if last.c <= 0 or (last.h - last.l) / last.c < 0.0001:
                return None
            side = "long" if last.c >= last.o else "short"
            rationale = {"type": "atr_proxy", "range_pct": (last.h - last.l) / last.c}
        else:
            side = "long" if last.c >= previous.c else "short"
            rationale = {"type": "momentum", "prev_close": previous.c}

        stop_distance = max(abs(last.c - last.l), last.c * 0.003)
        stop_price = last.c - stop_distance if side == "long" else last.c + stop_distance
        target_price = last.c + stop_distance * 1.6 if side == "long" else last.c - stop_distance * 1.6
        return Signal(
            side=side,
            symbol=symbol,
            entry_price=None,
            stop_price=round(stop_price, 8),
            target_price=round(target_price, 8),
            confidence=self.confidence,
            suggested_size=0.0,
            rationale=rationale,
            expires_in_ms=60_000,
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
        self._last_trade_ms: dict[tuple[str, str], int] = {}
        self._evaluations: dict[tuple[str, str], dict[str, Any]] = {}

    @property
    def symbols(self) -> list[str]:
        return list(self._symbols)

    @property
    def strategies(self) -> list[str]:
        return [strategy.name for strategy in self._strategies]

    def replace_symbols(self, symbols: list[str]) -> None:
        self._symbols = list(dict.fromkeys(symbols))

    def on_bar(self, bar: Bar, now_ms: int | None = None) -> list[OrderHandle]:
        if not bar.closed or bar.timeframe != "1m" or bar.symbol not in self._symbols:
            return []
        now = now_ms or int(time.time() * 1000)
        self._cache.push_bar(bar)
        handles: list[OrderHandle] = []
        bars = self._cache.get_bars(bar.symbol, "1m", n=80)
        for strategy in self._strategies:
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
        quantity = self._round_qty(signal.symbol, self._notional_for_strategy(strategy_name) / price)
        if quantity * price < 5.0:
            self._record_risk("min_notional", "warn", strategy_name, signal, now_ms)
            return None
        signal.suggested_size = quantity
        signal_id = self._insert_signal(strategy_name, signal, now_ms)
        side = "buy" if signal.side == "long" else "sell"
        handle = self._engine.place_order(
            OrderIntent(
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
            ),
            now_ms,
        )
        self._repo._conn.execute(
            "UPDATE signals SET status=? WHERE id=?",
            ("placed" if handle.status in {"accepted", "filled"} else handle.status, signal_id),
        )
        self._repo._conn.commit()
        self._last_trade_ms[key] = now_ms
        return handle

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
    ) -> None:
        self._repo.insert_risk_event(
            {
                "type": "paper_signal_skipped",
                "severity": severity,
                "source": "dashboard_trader",
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

    def _notional_for_strategy(self, strategy_name: str) -> float:
        multiplier = self._strategy_notional_multipliers.get(strategy_name, 1.0)
        return max(self._notional_usdt * multiplier, 5.0)

    def _round_qty(self, symbol: str, quantity: float) -> float:
        row = self._repo.get_symbol(symbol)
        lot_size = float(row["lot_size"] if row is not None else 0.0001)
        if lot_size <= 0:
            return round(quantity, 8)
        steps = max(int(quantity / lot_size), 1)
        return round(steps * lot_size, 8)


def default_exploration_strategies() -> list[ExplorationStrategy]:
    return [
        ExplorationStrategy("explore_momentum", min_bars=2),
        ExplorationStrategy("explore_mean_reversion", min_bars=3),
        ExplorationStrategy("explore_volatility", min_bars=2),
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
    "DashboardPaperTrader",
    "ExplorationStrategy",
    "bars_from_binance_klines",
    "fetch_binance_recent_klines",
    "fetch_binance_top_usdt_symbols",
    "select_top_usdt_symbols",
    "upsert_dashboard_universe",
]
