"""验证修正后的 Donchian 策略——滞后 1 根 K 线"""
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

bars = feed.get_candles("BTCUSDT", "4h", n=2000)
df = pd.DataFrame([{"close": b.c, "high": b.h, "low": b.l, "quote_volume": b.q} for b in bars])
df.index = pd.to_datetime([b.ts for b in bars], unit="ms", utc=True)

close = df["close"]
high = df["high"]
low = df["low"]
vol = df["quote_volume"]

# 修正方案: Donchian 用 high/low，但上轨滞后 1 根
donchian = compute_donchian(high, low, 20)
donchian_upper_lag1 = donchian["upper"].shift(1)  # 上一根的 Donchian 上轨
donchian_lower_lag1 = donchian["lower"].shift(1)  # 上一根的 Donchian 下轨

# 多头突破: 当前收盘 > 上一根的 Donchian 上轨
# 空头突破: 当前收盘 < 上一根的 Donchian 下轨
breakout_upper = close > donchian_upper_lag1
breakout_lower = close < donchian_lower_lag1

print("=== 修正方案: Donchian 滞后 1 根 ===")
print(f"  总 K 线: {len(df)}")
print(f"  多头突破（close > 前根上轨）: {breakout_upper.sum()} 次 ({breakout_upper.mean()*100:.1f}%)")
print(f"  空头突破（close < 前根下轨）: {breakout_lower.sum()} 次 ({breakout_lower.mean()*100:.1f}%)")

# 加趋势过滤: 1d MA
trend_bars = feed.get_candles("BTCUSDT", "1d", n=100)
trend_df = pd.DataFrame([{"close": b.c} for b in trend_bars])
trend_df.index = pd.to_datetime([b.ts for b in trend_bars], unit="ms", utc=True)
trend_close = trend_df["close"]
trend_ma20 = compute_sma(trend_close, 20)
trend_ma50 = compute_sma(trend_close, 50)
trend_bull = (trend_close > trend_ma50) & (trend_ma20 > trend_ma50)
trend_bear = (trend_close < trend_ma50) & (trend_ma20 < trend_ma50)

trend_map = pd.DataFrame({"bull": trend_bull, "bear": trend_bear}, index=trend_df.index)
trend_4h = trend_map.reindex(df.index, method="ffill")

# 加 ATR 过滤
atr = compute_atr(high, low, close, 14)
atr_median = atr.rolling(120).median()
atr_ok = (atr >= atr_median * 1.0).fillna(True)

# 加量过滤
vol_7d = vol.rolling(42).mean()
vol_ok = (vol >= vol_7d * 1.2).fillna(True)

# 完整组合
sig_long = (
    trend_4h["bull"] &
    breakout_upper &
    atr_ok &
    vol_ok
)

sig_short = (
    trend_4h["bear"] &
    breakout_lower &
    atr_ok &
    vol_ok
)

print("\n=== 完整组合: 趋势 + 突破(滞后1) + ATR + 量 ===")
print(f"  多头信号: {sig_long.sum()} 次 ({sig_long.mean()*100:.1f}%)")
print(f"  空头信号: {sig_short.sum()} 次 ({sig_short.mean()*100:.1f}%)")
print(f"  总计: {(sig_long | sig_short).sum()} 次")

# 看看信号出现在什么时候
if sig_long.any():
    long_dates = df.index[sig_long]
    print("\n  多头信号时间:")
    for d in long_dates[-10:]:
        print(f"    {d}")

# 去掉量过滤试试（BTC 大户市场，量信号不稳定）
sig_long_no_vol = trend_4h["bull"] & breakout_upper & atr_ok
sig_short_no_vol = trend_4h["bear"] & breakout_lower & atr_ok
print("\n=== 去掉量过滤 ===")
print(f"  多头: {sig_long_no_vol.sum()} 次, 空头: {sig_short_no_vol.sum()} 次")

# 也去掉 ATR 过滤
sig_long_bare = trend_4h["bull"] & breakout_upper
sig_short_bare = trend_4h["bear"] & breakout_lower
print("\n=== 仅趋势 + 突破 ===")
print(f"  多头: {sig_long_bare.sum()} 次, 空头: {sig_short_bare.sum()} 次")

conn.close()
