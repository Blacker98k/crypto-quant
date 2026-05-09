"""Parquet K 线读写。"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from core.common.exceptions import ParquetCorrupt
from core.data.exchange.base import Bar


class ParquetIO:
    """按 symbol/timeframe/年月分区读写 K 线。"""

    def __init__(self, data_root: str | Path) -> None:
        self._root = Path(data_root)
        self._root.mkdir(parents=True, exist_ok=True)

    def file_path(self, symbol: str, timeframe: str, ts: int) -> Path:
        """返回某根 K 线所在分区文件路径。"""
        dt = pd.to_datetime(ts, unit="ms", utc=True)
        if timeframe in {"1m", "3m", "5m", "15m", "30m", "1h", "2h"}:
            filename = f"{dt.year:04d}-{dt.month:02d}.parquet"
        else:
            filename = f"{dt.year:04d}.parquet"
        return self._root / "candles" / symbol / timeframe / filename

    def write_bars(self, bars: list[Bar]) -> None:
        """写入 K 线；同 ts 保留最后一条。"""
        if not bars:
            return
        groups: dict[Path, list[Bar]] = {}
        for bar in bars:
            groups.setdefault(self.file_path(bar.symbol, bar.timeframe, bar.ts), []).append(bar)

        for path, group in groups.items():
            path.parent.mkdir(parents=True, exist_ok=True)
            new_df = pd.DataFrame([self._bar_to_row(b) for b in group])
            if path.exists():
                old_df = pd.read_parquet(path)
                df = pd.concat([old_df, new_df], ignore_index=True)
            else:
                df = new_df
            df = df.drop_duplicates(subset=["ts"], keep="last").sort_values("ts")
            df.to_parquet(path, index=False)

    def read_bars(
        self,
        symbol: str,
        timeframe: str,
        start_ms: int | None = None,
        end_ms: int | None = None,
        n: int | None = None,
    ) -> list[Bar]:
        """读取 K 线，返回按 ts 升序排列。"""
        frames = []
        for rel in self.list_partitions(symbol, timeframe):
            frames.append(pd.read_parquet(self._root / rel))
        if not frames:
            return []
        df = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["ts"], keep="last")
        if start_ms is not None:
            df = df[df["ts"] >= start_ms]
        if end_ms is not None:
            df = df[df["ts"] < end_ms]
        df = df.sort_values("ts")
        if n is not None:
            df = df.tail(n)
        return [self._row_to_bar(row, symbol, timeframe) for row in df.to_dict("records")]

    def list_partitions(self, symbol: str, timeframe: str) -> list[str]:
        """列出某 symbol/timeframe 的 parquet 分区。"""
        base = self._root / "candles" / symbol / timeframe
        if not base.is_dir():
            return []
        return [
            str(path.relative_to(self._root)).replace("\\", "/")
            for path in sorted(base.glob("*.parquet"))
        ]

    def verify_file(self, path: str | Path) -> bool:
        """校验单个 parquet 文件可读。"""
        p = Path(path)
        if not p.exists():
            raise ParquetCorrupt(f"parquet file not found: {p}")
        try:
            pd.read_parquet(p)
        except Exception as e:
            raise ParquetCorrupt(f"parquet file corrupt: {p}") from e
        return True

    def verify_all(self) -> list[str]:
        """校验全部 parquet 文件，返回损坏文件列表。"""
        corrupt: list[str] = []
        for path in self._root.rglob("*.parquet"):
            try:
                self.verify_file(path)
            except ParquetCorrupt:
                corrupt.append(str(path))
        return corrupt

    @staticmethod
    def _bar_to_row(bar: Bar) -> dict[str, float | int]:
        return {"ts": bar.ts, "o": bar.o, "h": bar.h, "l": bar.l, "c": bar.c, "v": bar.v, "q": bar.q}

    @staticmethod
    def _row_to_bar(row: dict, symbol: str, timeframe: str) -> Bar:
        return Bar(
            symbol=symbol,
            timeframe=timeframe,
            ts=int(row["ts"]),
            o=float(row["o"]),
            h=float(row["h"]),
            l=float(row["l"]),
            c=float(row["c"]),
            v=float(row["v"]),
            q=float(row.get("q", 0.0)),
            closed=True,
        )


__all__ = ["ParquetIO"]
