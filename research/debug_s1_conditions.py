"""Debug: 检查 S1 入场条件各状态"""
import sqlite3
from pathlib import Path

import pandas as pd

from core.data.feed import ResearchFeed
from core.data.parquet_io import ParquetIO
from core.data.sqlite_repo import SqliteRepo
from core.db.migration_runner import MigrationRunner
from core.strategy.indicators import compute_atr, compute_donchian, compute_sma

conn = sqlite3.connect("data/crypto.sqlite")
conn.row_factory = sqlite3.Row
MigrationRunner(migrations_dir=Path("migrations")).apply_all(conn)
feed = ResearchFeed(ParquetIO(data_root="data"), SqliteRepo(conn))

entry_bars = feed.get_candles("BTCUSDT", "4h", n=500)
entry_df = pd.DataFrame([{"close": b.c, "high": b.h, "low": b.l, "quote_volume": b.q} for b in entry_bars])
entry_df.index = pd.to_datetime([b.ts for b in entry_bars], unit="ms", utc=True)

trend_bars = feed.get_candles("BTCUSDT", "1d", n=100)
trend_df = pd.DataFrame([{"close": b.c} for b in trend_bars])
trend_df.index = pd.to_datetime([b.ts for b in trend_bars], unit="ms", utc=True)

close = entry_df["close"]
high = entry_df["high"]
low = entry_df["low"]
vol = entry_df["quote_volume"]

# 1d 趋势
trend_close = trend_df["close"]
trend_ma20 = compute_sma(trend_close, 20)
trend_ma50 = compute_sma(trend_close, 50)
trend_bull = (trend_close > trend_ma50) & (trend_ma20 > trend_ma50)
trend_bear = (trend_close < trend_ma50) & (trend_ma20 < trend_ma50)

print("=== 1d 趋势状态（最近 10 根）===")
for i in range(-10, 0):
    print(f"  {trend_df.index[i].date()}: close={trend_close.iloc[i]:.0f} ma20={trend_ma20.iloc[i]:.0f} ma50={trend_ma50.iloc[i]:.0f} bull={trend_bull.iloc[i]}")

print()
print("=== 4h 入场条件（最近 30 根）===")

donchian = compute_donchian(high, low, 20)
atr = compute_atr(high, low, close, 14)
atr_median = atr.rolling(120).median()
atr_ok = (atr >= atr_median * 1.0).fillna(True)

vol_7d = vol.rolling(42).mean()
vol_ok = (vol >= vol_7d * 1.2).fillna(True)

trend_map = pd.DataFrame({"bull": trend_bull, "bear": trend_bear}, index=trend_df.index)
trend_4h = trend_map.reindex(entry_df.index, method="ffill")

signal_count = 0
for i in range(-30, 0):
    dc = close.iloc[i]
    du = donchian["upper"].iloc[i]
    dl = donchian["lower"].iloc[i]
    tr_bull = bool(trend_4h["bull"].iloc[i])
    tr_bear = bool(trend_4h["bear"].iloc[i])
    ao = bool(atr_ok.iloc[i])
    vo = bool(vol_ok.iloc[i])
    ba = dc > du
    bb = dc < dl

    sig_long = tr_bull and ba and ao and vo
    sig_short = tr_bear and bb and ao and vo

    if sig_long or sig_short:
        signal_count += 1
        mark = "[SIGNAL]"
    else:
        fails = []
        if not tr_bull and not tr_bear:
            fails.append("trend")
        if not ba and not bb:
            fails.append("donchian")
        if not ao:
            fails.append("atr")
        if not vo:
            fails.append("vol")
        mark = f"[NO: {'+'.join(fails)}]"

    print(f"  {str(entry_df.index[i])[:19]}: close={dc:.0f} donch_up={du:.0f} donch_lo={dl:.0f} {mark}")

print(f"\n总信号: {signal_count}")

conn.close()
