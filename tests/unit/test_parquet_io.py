"""ParquetIO 测试——写后读一致、分区路由、校验。"""

from __future__ import annotations

import pytest

from core.data.exchange.base import Bar


def _bar(**kw):
    defaults = dict(symbol="BTCUSDT", timeframe="1h", ts=1700000000000, o=50000, h=51000, l=49000, c=50500, v=1, q=50500, closed=True)
    defaults.update(kw)
    return Bar(**defaults)


class TestWriteRead:
    """写后读一致性测试。"""

    def test_write_then_read(self, parquet_io):
        bars = [
            _bar(ts=1700000000000),
            _bar(ts=1700003600000, o=50500, c=51000),
            _bar(ts=1700007200000, o=51000, c=51500),
        ]
        parquet_io.write_bars(bars)
        result = parquet_io.read_bars("BTCUSDT", "1h")
        assert len(result) == 3
        assert result[0].ts == 1700000000000
        assert result[-1].ts == 1700007200000

    def test_write_same_ts_deduplicates(self, parquet_io):
        b1 = _bar(ts=1700000000000, c=50000)
        b2 = _bar(ts=1700000000000, c=99999)
        parquet_io.write_bars([b1])
        parquet_io.write_bars([b2])
        result = parquet_io.read_bars("BTCUSDT", "1h")
        assert len(result) == 1
        assert result[0].c == 99999  # keep last

    def test_read_with_start_end(self, parquet_io):
        bars = [_bar(ts=ts) for ts in range(1700000000000, 1700010000000, 3600000)]
        parquet_io.write_bars(bars)
        result = parquet_io.read_bars(
            "BTCUSDT", "1h", start_ms=1700003600000, end_ms=1700007200000
        )
        assert all(1700003600000 <= b.ts < 1700007200000 for b in result)

    def test_read_n(self, parquet_io):
        base_ts = 1700000000000
        bars = [_bar(ts=base_ts + i * 3600000) for i in range(10)]
        parquet_io.write_bars(bars)
        result = parquet_io.read_bars("BTCUSDT", "1h", n=5)
        assert len(result) == 5
        latest_ts = max(b.ts for b in bars)
        assert result[-1].ts == latest_ts

    def test_read_nonexistent_directory(self, parquet_io):
        result = parquet_io.read_bars("NOEXIST", "1h")
        assert result == []


class TestPartitionRouting:
    """分区路由测试。"""

    def test_monthly_partition_for_1h(self, parquet_io):
        p = parquet_io.file_path("BTCUSDT", "1h", 1700000000000)
        assert "2023-11.parquet" in str(p)  # Nov 2023

    def test_yearly_partition_for_4h(self, parquet_io):
        p = parquet_io.file_path("BTCUSDT", "4h", 1700000000000)
        assert "2023.parquet" in str(p)


class TestVerify:
    """校验测试。"""

    def test_verify_valid_file(self, parquet_io):
        parquet_io.write_bars([_bar()])
        files = parquet_io.list_partitions("BTCUSDT", "1h")
        assert len(files) == 1
        assert parquet_io.verify_file(parquet_io._root / files[0])

    def test_verify_nonexistent_raises(self, parquet_io):
        from core.common.exceptions import ParquetCorrupt
        with pytest.raises(ParquetCorrupt):
            parquet_io.verify_file("/tmp/no-such-file-99999.parquet")

    def test_verify_all_empty_dir(self, parquet_io):
        corrupt = parquet_io.verify_all()
        assert corrupt == []

    def test_list_partitions(self, parquet_io):
        parquet_io.write_bars([_bar()])
        result = parquet_io.list_partitions("BTCUSDT", "1h")
        assert len(result) == 1
        assert result[0].endswith(".parquet")
