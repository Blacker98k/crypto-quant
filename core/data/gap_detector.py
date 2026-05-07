"""K 线缺洞检测。"""

from __future__ import annotations

from dataclasses import dataclass

from core.common.time_utils import tf_interval_ms
from core.data.parquet_io import ParquetIO


@dataclass(slots=True)
class Gap:
    """缺失的左闭右开时间区间。"""

    start_ms: int
    end_ms: int
    symbol: str
    timeframe: str

    @property
    def bar_count(self) -> int:
        """区间内缺失的 bar 数。"""
        interval = tf_interval_ms(self.timeframe)
        return max(0, (self.end_ms - self.start_ms) // interval)


class GapDetector:
    """扫描 Parquet K 线连续性。"""

    def __init__(self, parquet_io: ParquetIO) -> None:
        self._parquet_io = parquet_io

    def scan(self, symbol: str, timeframe: str) -> list[Gap]:
        """扫描单个 symbol/timeframe。"""
        bars = self._parquet_io.read_bars(symbol, timeframe)
        if len(bars) < 2:
            return []
        interval = tf_interval_ms(timeframe)
        gaps: list[Gap] = []
        for prev, cur in zip(bars, bars[1:], strict=False):
            expected = prev.ts + interval
            if cur.ts > expected:
                gaps.append(Gap(expected, cur.ts, symbol, timeframe))
        return gaps

    def scan_all(self, symbols: list[str], timeframes: list[str]) -> dict[tuple[str, str], list[Gap]]:
        """批量扫描；仅返回有缺洞的组合。"""
        result: dict[tuple[str, str], list[Gap]] = {}
        for symbol in symbols:
            for timeframe in timeframes:
                gaps = self.scan(symbol, timeframe)
                if gaps:
                    result[(symbol, timeframe)] = gaps
        return result


__all__ = ["Gap", "GapDetector"]
