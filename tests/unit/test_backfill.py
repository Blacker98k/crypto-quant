"""Historical OHLCV backfill job behavior."""

from __future__ import annotations

import json

import pytest

from core.data.backfill import BackfillJob
from core.data.exchange.base import Bar

HOUR_MS = 3_600_000


class RecordingExchange:
    def __init__(self, bars: list[Bar], *, fail_after_calls: int | None = None) -> None:
        self.bars = bars
        self.fail_after_calls = fail_after_calls
        self.calls: list[tuple[str, str, int, int, int]] = []

    async def fetch_klines(
        self,
        symbol: str,
        timeframe: str,
        start_ms: int,
        end_ms: int,
        limit: int = 1000,
    ) -> list[Bar]:
        self.calls.append((symbol, timeframe, start_ms, end_ms, limit))
        if self.fail_after_calls is not None and len(self.calls) > self.fail_after_calls:
            raise RuntimeError("exchange unavailable")
        return [
            bar
            for bar in self.bars
            if bar.symbol == symbol and bar.timeframe == timeframe and start_ms <= bar.ts < end_ms
        ][:limit]


def make_bar(ts: int, close: float = 100.0) -> Bar:
    return Bar(
        symbol="BTCUSDT",
        timeframe="1h",
        ts=ts,
        o=close - 1,
        h=close + 2,
        l=close - 2,
        c=close,
        v=10,
        q=close * 10,
    )


@pytest.mark.asyncio
async def test_backfill_writes_bars_and_saves_progress(parquet_io, sqlite_repo) -> None:
    exchange = RecordingExchange([make_bar(0), make_bar(HOUR_MS), make_bar(2 * HOUR_MS)])
    job = BackfillJob(exchange, parquet_io, sqlite_repo, page_limit=2)

    results = await job.run(
        symbols=["BTCUSDT"],
        timeframes=["1h"],
        start_ms=0,
        end_ms=3 * HOUR_MS,
        concurrency=1,
    )

    assert len(results) == 1
    assert results[0].complete is True
    assert results[0].bars_written == 3
    assert [bar.ts for bar in parquet_io.read_bars("BTCUSDT", "1h")] == [0, HOUR_MS, 2 * HOUR_MS]

    progress = json.loads(sqlite_repo.kv_get("backfill", "BTCUSDT:1h") or "{}")
    assert progress["next_ms"] == 3 * HOUR_MS
    assert progress["complete"] is True


@pytest.mark.asyncio
async def test_backfill_resume_starts_from_saved_progress(parquet_io, sqlite_repo) -> None:
    sqlite_repo.kv_set("backfill", "BTCUSDT:1h", json.dumps({"next_ms": 2 * HOUR_MS}))
    exchange = RecordingExchange([make_bar(0), make_bar(HOUR_MS), make_bar(2 * HOUR_MS)])
    job = BackfillJob(exchange, parquet_io, sqlite_repo, page_limit=1000)

    results = await job.run(
        symbols=["BTCUSDT"],
        timeframes=["1h"],
        start_ms=0,
        end_ms=3 * HOUR_MS,
        concurrency=1,
        resume=True,
    )

    assert results[0].bars_written == 1
    assert exchange.calls[0][2] == 2 * HOUR_MS
    assert [bar.ts for bar in parquet_io.read_bars("BTCUSDT", "1h")] == [2 * HOUR_MS]


def test_clear_progress_deletes_resume_key(parquet_io, sqlite_repo) -> None:
    sqlite_repo.kv_set("backfill", "BTCUSDT:1h", json.dumps({"next_ms": HOUR_MS}))
    job = BackfillJob(RecordingExchange([]), parquet_io, sqlite_repo)

    assert job.clear_progress("BTCUSDT", "1h") is True
    assert sqlite_repo.kv_get("backfill", "BTCUSDT:1h") is None


@pytest.mark.asyncio
async def test_backfill_marks_result_incomplete_on_exchange_error(parquet_io, sqlite_repo) -> None:
    exchange = RecordingExchange(
        [make_bar(0), make_bar(HOUR_MS), make_bar(2 * HOUR_MS)],
        fail_after_calls=1,
    )
    job = BackfillJob(exchange, parquet_io, sqlite_repo, page_limit=1)

    results = await job.run(
        symbols=["BTCUSDT"],
        timeframes=["1h"],
        start_ms=0,
        end_ms=3 * HOUR_MS,
        concurrency=1,
    )

    assert results[0].complete is False
    assert results[0].bars_written == 1
    assert "exchange unavailable" in (results[0].error or "")
