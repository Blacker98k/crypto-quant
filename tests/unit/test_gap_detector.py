"""GapDetector 测试——缺洞检测算法。"""

from __future__ import annotations

from core.data.gap_detector import Gap, GapDetector


class TestGap:
    """Gap 数据类测试。"""

    def test_bar_count(self):
        g = Gap(start_ms=100000, end_ms=100000 + 3 * 3600000, symbol="BTCUSDT", timeframe="1h")
        assert g.bar_count == 3

    def test_bar_count_zero(self):
        g = Gap(start_ms=100000, end_ms=100000, symbol="BTCUSDT", timeframe="1h")
        assert g.bar_count == 0


class TestGapDetectorScan:
    """缺洞扫描测试。"""

    def test_no_gaps_perfect_sequence(self, parquet_io, sample_bars):
        parquet_io.write_bars(sample_bars)
        gd = GapDetector(parquet_io)
        gaps = gd.scan("BTCUSDT", "1h")
        assert gaps == []

    def test_no_gaps_single_bar(self, parquet_io):
        from core.data.exchange.base import Bar
        parquet_io.write_bars([Bar(symbol="BTCUSDT", timeframe="1h", ts=1000, o=100, h=110, l=90, c=105, v=1, closed=True)])
        gd = GapDetector(parquet_io)
        gaps = gd.scan("BTCUSDT", "1h")
        assert gaps == []

    def test_detect_single_gap(self, parquet_io):
        from core.data.exchange.base import Bar
        interval = 3600000  # 1h
        bars = [
            Bar(symbol="BTCUSDT", timeframe="1h", ts=1000000, o=100, h=110, l=90, c=105, v=1, closed=True),
            # 跳过 1 根：1000000 + 3600000 = 4600000
            Bar(symbol="BTCUSDT", timeframe="1h", ts=1000000 + 2 * interval, o=100, h=110, l=90, c=105, v=1, closed=True),
        ]
        parquet_io.write_bars(bars)
        gd = GapDetector(parquet_io)
        gaps = gd.scan("BTCUSDT", "1h")
        assert len(gaps) == 1
        assert gaps[0].start_ms == 1000000 + interval  # prev.ts + interval
        assert gaps[0].end_ms == 1000000 + 2 * interval  # current.ts
        assert gaps[0].bar_count == 1

    def test_detect_multiple_gaps(self, parquet_io):
        from core.data.exchange.base import Bar
        interval = 3600000
        bars = [
            Bar(symbol="BTCUSDT", timeframe="1h", ts=1000000, o=100, h=110, l=90, c=105, v=1, closed=True),
            # gap 1: missing 1 bar
            Bar(symbol="BTCUSDT", timeframe="1h", ts=1000000 + 2 * interval, o=100, h=110, l=90, c=105, v=1, closed=True),
            # gap 2: missing 2 bars
            Bar(symbol="BTCUSDT", timeframe="1h", ts=1000000 + 5 * interval, o=100, h=110, l=90, c=105, v=1, closed=True),
        ]
        parquet_io.write_bars(bars)
        gd = GapDetector(parquet_io)
        gaps = gd.scan("BTCUSDT", "1h")
        assert len(gaps) == 2

    def test_empty_data_no_gaps(self, parquet_io):
        gd = GapDetector(parquet_io)
        gaps = gd.scan("BTCUSDT", "1h")
        assert gaps == []


class TestScanAll:
    """批量扫描测试。"""

    def test_scan_all_multiple_pairs(self, parquet_io):
        from core.data.exchange.base import Bar
        for sym, tf in [("BTCUSDT", "1h"), ("ETHUSDT", "4h")]:
            bars = []
            for i in range(5):
                interval = 3600000 if tf == "1h" else 14400000
                bars.append(Bar(symbol=sym, timeframe=tf, ts=1000000 + i * interval, o=100, h=110, l=90, c=105, v=1, closed=True))
            parquet_io.write_bars(bars)

        gd = GapDetector(parquet_io)
        results = gd.scan_all(["BTCUSDT", "ETHUSDT"], ["1h", "4h"])
        assert results == {}  # no gaps
