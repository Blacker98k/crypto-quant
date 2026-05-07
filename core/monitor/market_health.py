"""Market-data health probes and run-log summaries."""

from __future__ import annotations

import time
from collections import Counter
from dataclasses import dataclass
from typing import Protocol

from core.data.exchange.base import Bar, Ticker24h
from core.data.sqlite_repo import SqliteRepo
from core.data.symbol import normalize_symbol

OK_STATUSES = frozenset({"ok", "success"})


class MarketDataExchange(Protocol):
    """Minimal exchange contract used by the public market-data probe."""

    async def fetch_24h_tickers(self) -> list[Ticker24h]: ...

    async def fetch_klines(
        self,
        symbol: str,
        timeframe: str,
        start_ms: int,
        end_ms: int,
        limit: int = 1000,
    ) -> list[Bar]: ...


@dataclass(slots=True)
class MarketProbeResult:
    """Result returned by a public market-data health probe."""

    endpoint: str
    status: str
    symbol: str
    last_price: float | None
    ticker_ts: int | None
    kline_count: int
    latency_ms: int
    note: str | None = None

    def as_dict(self) -> dict[str, object]:
        return {
            "endpoint": self.endpoint,
            "status": self.status,
            "symbol": self.symbol,
            "last_price": self.last_price,
            "ticker_ts": self.ticker_ts,
            "kline_count": self.kline_count,
            "latency_ms": self.latency_ms,
            "note": self.note,
        }


async def probe_market_data(
    exchange: MarketDataExchange,
    repo: SqliteRepo,
    *,
    symbol: str = "BTCUSDT",
    endpoint: str = "binance_usdm_public_ticker",
    timeframe: str = "1m",
    require_kline: bool = False,
    now_ms: int | None = None,
) -> MarketProbeResult:
    """Probe public ticker data, optionally with a recent kline, and record run_log."""

    started = time.perf_counter()
    normalized_symbol = normalize_symbol(symbol)
    try:
        tickers = await exchange.fetch_24h_tickers()
        ticker = next((row for row in tickers if row.symbol == normalized_symbol), None)
        if ticker is None:
            raise RuntimeError(f"ticker missing for {normalized_symbol}")
        if ticker.last_price <= 0:
            raise RuntimeError(f"ticker last_price is not positive for {normalized_symbol}")

        kline_count = 0
        if require_kline:
            end_ms = int(time.time() * 1000) if now_ms is None else now_ms
            start_ms = end_ms - 5 * 60_000
            bars = await exchange.fetch_klines(
                normalized_symbol,
                timeframe,
                start_ms,
                end_ms,
                limit=5,
            )
            kline_count = len(bars)
            if kline_count == 0:
                raise RuntimeError(f"kline missing for {normalized_symbol} {timeframe}")

        latency_ms = _elapsed_ms(started)
        note = f"symbol={normalized_symbol};price={ticker.last_price};klines={kline_count}"
        repo.log_run(endpoint, "ok", http_code=200, latency_ms=latency_ms, note=note)
        return MarketProbeResult(
            endpoint=endpoint,
            status="ok",
            symbol=normalized_symbol,
            last_price=ticker.last_price,
            ticker_ts=ticker.ts,
            kline_count=kline_count,
            latency_ms=latency_ms,
            note=note,
        )
    except Exception as exc:
        latency_ms = _elapsed_ms(started)
        note = f"{type(exc).__name__}: {exc}"
        repo.log_run(endpoint, "fail", http_code=None, latency_ms=latency_ms, note=note)
        return MarketProbeResult(
            endpoint=endpoint,
            status="fail",
            symbol=normalized_symbol,
            last_price=None,
            ticker_ts=None,
            kline_count=0,
            latency_ms=latency_ms,
            note=note,
        )


def summarize_market_health(
    repo: SqliteRepo,
    *,
    limit: int = 100,
    since_ms: int | None = None,
) -> dict[str, object]:
    """Summarize recent run_log rows for dashboard and quality gates."""

    bounded_limit = max(1, min(limit, 500))
    rows = repo.get_recent_run_log(limit=bounded_limit, since_ms=since_ms)
    counts = Counter(str(row["status"]) for row in rows)
    failures = [row for row in rows if str(row["status"]) not in OK_STATUSES]
    latencies = [
        int(row["latency_ms"])
        for row in rows
        if row.get("latency_ms") is not None and int(row["latency_ms"]) >= 0
    ]

    if not rows:
        status = "idle"
    elif failures:
        status = "degraded"
    else:
        status = "ok"

    return {
        "status": status,
        "total": len(rows),
        "by_status": dict(sorted(counts.items())),
        "latest_captured_at": rows[0]["captured_at"] if rows else None,
        "latest_endpoint": rows[0]["endpoint"] if rows else None,
        "latency_ms": _latency_summary(latencies),
        "endpoints": sorted({str(row["endpoint"]) for row in rows}),
        "recent_failures": [_run_log_row(row) for row in failures[:10]],
        "recent": [_run_log_row(row) for row in rows[:10]],
    }


def _elapsed_ms(started: float) -> int:
    return max(0, int((time.perf_counter() - started) * 1000))


def _latency_summary(values: list[int]) -> dict[str, int | None]:
    if not values:
        return {"min": None, "max": None, "avg": None}
    return {
        "min": min(values),
        "max": max(values),
        "avg": round(sum(values) / len(values)),
    }


def _run_log_row(row: dict[str, object]) -> dict[str, object]:
    return {
        "id": row["id"],
        "endpoint": row["endpoint"],
        "status": row["status"],
        "http_code": row["http_code"],
        "latency_ms": row["latency_ms"],
        "note": row["note"],
        "captured_at": row["captured_at"],
    }


__all__ = ["MarketProbeResult", "probe_market_data", "summarize_market_health"]
