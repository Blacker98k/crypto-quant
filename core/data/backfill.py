"""Historical OHLCV backfill orchestration."""

from __future__ import annotations

import asyncio
import json
import re
import time
from dataclasses import dataclass
from typing import Protocol

from core.data.exchange.base import Bar
from core.data.parquet_io import ParquetIO
from core.data.sqlite_repo import SqliteRepo
from core.data.symbol import normalize_symbol


_TIMEFRAME_RE = re.compile(r"^(?P<count>[1-9][0-9]*)(?P<unit>[mhdw])$")
_TIMEFRAME_UNIT_MS = {
    "m": 60_000,
    "h": 3_600_000,
    "d": 86_400_000,
    "w": 604_800_000,
}


class KlineExchange(Protocol):
    async def fetch_klines(
        self,
        symbol: str,
        timeframe: str,
        start_ms: int,
        end_ms: int,
        limit: int = 1000,
    ) -> list[Bar]:
        """Fetch historical bars in ``[start_ms, end_ms)``."""
        ...


@dataclass(slots=True)
class BackfillResult:
    """Per symbol/timeframe backfill outcome."""

    symbol: str
    timeframe: str
    start_ms: int
    end_ms: int
    bars_written: int = 0
    complete: bool = False
    error: str | None = None


class BackfillJob:
    """Fetch exchange klines into parquet with resumable progress."""

    _KV_STRATEGY = "backfill"

    def __init__(
        self,
        exchange: KlineExchange,
        parquet_io: ParquetIO,
        repo: SqliteRepo,
        *,
        page_limit: int = 1000,
    ) -> None:
        self._exchange = exchange
        self._parquet_io = parquet_io
        self._repo = repo
        self._page_limit = min(max(page_limit, 1), 1000)

    async def run(
        self,
        symbols: list[str],
        timeframes: list[str],
        start_ms: int,
        end_ms: int,
        *,
        concurrency: int = 4,
        resume: bool = True,
    ) -> list[BackfillResult]:
        """Backfill all symbol/timeframe pairs and return one result per pair."""
        if end_ms <= start_ms:
            raise ValueError("end_ms must be greater than start_ms")

        semaphore = asyncio.Semaphore(max(concurrency, 1))
        tasks = [
            self._run_guarded(
                semaphore,
                normalize_symbol(symbol),
                timeframe,
                start_ms,
                end_ms,
                resume=resume,
            )
            for symbol in symbols
            for timeframe in timeframes
        ]
        return list(await asyncio.gather(*tasks))

    def clear_progress(self, symbol: str, timeframe: str) -> bool:
        """Delete the saved progress cursor for one symbol/timeframe pair."""
        return self._repo.kv_delete(self._KV_STRATEGY, self._progress_key(symbol, timeframe))

    async def _run_guarded(
        self,
        semaphore: asyncio.Semaphore,
        symbol: str,
        timeframe: str,
        start_ms: int,
        end_ms: int,
        *,
        resume: bool,
    ) -> BackfillResult:
        async with semaphore:
            return await self._run_one(symbol, timeframe, start_ms, end_ms, resume=resume)

    async def _run_one(
        self,
        symbol: str,
        timeframe: str,
        start_ms: int,
        end_ms: int,
        *,
        resume: bool,
    ) -> BackfillResult:
        result = BackfillResult(symbol=symbol, timeframe=timeframe, start_ms=start_ms, end_ms=end_ms)
        try:
            step_ms = timeframe_to_ms(timeframe)
            cursor = self._resume_cursor(symbol, timeframe, start_ms, end_ms) if resume else start_ms

            while cursor < end_ms:
                bars = await self._exchange.fetch_klines(
                    symbol,
                    timeframe,
                    cursor,
                    end_ms,
                    limit=self._page_limit,
                )
                page = self._normalize_page(bars, symbol, timeframe, cursor, end_ms)
                if not page:
                    break

                self._parquet_io.write_bars(page)
                result.bars_written += len(page)

                next_ms = max(bar.ts for bar in page) + step_ms
                if next_ms <= cursor:
                    raise RuntimeError(
                        f"backfill cursor did not advance for {symbol} {timeframe}: {cursor}"
                    )
                cursor = min(next_ms, end_ms)
                self._save_progress(symbol, timeframe, cursor, end_ms, complete=cursor >= end_ms)

            result.complete = True
            self._save_progress(symbol, timeframe, cursor, end_ms, complete=True)
        except Exception as exc:
            result.error = str(exc)
            result.complete = False
        return result

    def _resume_cursor(self, symbol: str, timeframe: str, start_ms: int, end_ms: int) -> int:
        raw = self._repo.kv_get(self._KV_STRATEGY, self._progress_key(symbol, timeframe))
        if raw is None:
            return start_ms
        try:
            value = json.loads(raw)
        except json.JSONDecodeError:
            return start_ms
        next_ms = int(value.get("next_ms") or start_ms)
        return min(max(next_ms, start_ms), end_ms)

    def _save_progress(
        self,
        symbol: str,
        timeframe: str,
        next_ms: int,
        end_ms: int,
        *,
        complete: bool,
    ) -> None:
        value = {
            "symbol": symbol,
            "timeframe": timeframe,
            "next_ms": next_ms,
            "end_ms": end_ms,
            "complete": complete,
            "updated_at": int(time.time() * 1000),
        }
        self._repo.kv_set(
            self._KV_STRATEGY,
            self._progress_key(symbol, timeframe),
            json.dumps(value, sort_keys=True),
        )

    @staticmethod
    def _normalize_page(
        bars: list[Bar],
        symbol: str,
        timeframe: str,
        start_ms: int,
        end_ms: int,
    ) -> list[Bar]:
        deduped: dict[int, Bar] = {}
        for bar in bars:
            if start_ms <= bar.ts < end_ms:
                deduped[bar.ts] = Bar(
                    symbol=symbol,
                    timeframe=timeframe,
                    ts=bar.ts,
                    o=bar.o,
                    h=bar.h,
                    l=bar.l,
                    c=bar.c,
                    v=bar.v,
                    q=bar.q,
                    closed=bar.closed,
                )
        return [deduped[ts] for ts in sorted(deduped)]

    @staticmethod
    def _progress_key(symbol: str, timeframe: str) -> str:
        return f"{normalize_symbol(symbol)}:{timeframe}"


def timeframe_to_ms(timeframe: str) -> int:
    """Convert Binance-style timeframe strings such as ``1h`` to milliseconds."""
    match = _TIMEFRAME_RE.match(timeframe)
    if match is None:
        raise ValueError(f"unsupported timeframe: {timeframe}")
    count = int(match.group("count"))
    unit = match.group("unit")
    return count * _TIMEFRAME_UNIT_MS[unit]


__all__ = ["BackfillJob", "BackfillResult", "timeframe_to_ms"]
