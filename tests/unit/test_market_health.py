from __future__ import annotations

import pytest

from core.data.exchange.base import Bar, Ticker24h
from core.monitor.market_health import probe_market_data, summarize_market_health


class _FakeExchange:
    def __init__(
        self,
        tickers: list[Ticker24h] | None = None,
        bars: list[Bar] | None = None,
        exc: Exception | None = None,
    ) -> None:
        self.tickers = tickers or []
        self.bars = bars or []
        self.exc = exc

    async def fetch_24h_tickers(self) -> list[Ticker24h]:
        if self.exc is not None:
            raise self.exc
        return self.tickers

    async def fetch_klines(
        self,
        symbol: str,
        timeframe: str,
        start_ms: int,
        end_ms: int,
        limit: int = 1000,
    ) -> list[Bar]:
        return [
            bar
            for bar in self.bars
            if bar.symbol == symbol and bar.timeframe == timeframe and start_ms <= bar.ts < end_ms
        ][:limit]


@pytest.mark.asyncio
async def test_probe_market_data_logs_success_with_public_price(sqlite_repo) -> None:
    exchange = _FakeExchange(
        tickers=[Ticker24h(symbol="BTCUSDT", last_price=65_000.5, quote_volume=1_000_000, ts=100)],
        bars=[
            Bar(
                symbol="BTCUSDT",
                timeframe="1m",
                ts=1_700_000_000_000,
                o=65_000,
                h=65_010,
                l=64_990,
                c=65_000.5,
                v=2,
            )
        ],
    )

    result = await probe_market_data(
        exchange,
        sqlite_repo,
        symbol="btcusdt",
        require_kline=True,
        now_ms=1_700_000_060_000,
    )

    assert result.status == "ok"
    assert result.symbol == "BTCUSDT"
    assert result.last_price == 65_000.5
    assert result.kline_count == 1
    logs = sqlite_repo.get_recent_run_log(limit=1)
    assert logs[0]["endpoint"] == "binance_usdm_public_ticker"
    assert logs[0]["status"] == "ok"
    assert "price=65000.5" in logs[0]["note"]


@pytest.mark.asyncio
async def test_probe_market_data_logs_failure(sqlite_repo) -> None:
    exchange = _FakeExchange(exc=TimeoutError("public API timeout"))

    result = await probe_market_data(exchange, sqlite_repo, symbol="ETHUSDT")

    assert result.status == "fail"
    assert result.last_price is None
    logs = sqlite_repo.get_recent_run_log(limit=1)
    assert logs[0]["status"] == "fail"
    assert "TimeoutError: public API timeout" in logs[0]["note"]


def test_summarize_market_health_marks_degraded_when_recent_failure_exists(sqlite_repo) -> None:
    sqlite_repo.log_run("binance_usdm_public_ticker", "ok", http_code=200, latency_ms=120)
    sqlite_repo.log_run("binance_usdm_public_ticker", "fail", latency_ms=2000, note="timeout")

    payload = summarize_market_health(sqlite_repo)

    assert payload["status"] == "degraded"
    assert payload["total"] == 2
    assert payload["by_status"] == {"fail": 1, "ok": 1}
    assert payload["latency_ms"] == {"min": 120, "max": 2000, "avg": 1060}
    assert payload["recent_failures"][0]["note"] == "timeout"


def test_summarize_market_health_marks_idle_without_rows(sqlite_repo) -> None:
    payload = summarize_market_health(sqlite_repo)

    assert payload["status"] == "idle"
    assert payload["total"] == 0
    assert payload["recent"] == []
