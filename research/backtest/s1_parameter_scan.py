#!/usr/bin/env python3
"""S1 参数扫描——找最优 Donchian/ATR/止损 组合。

用法: uv run python research/backtest/s1_parameter_scan.py
输出: data/reports/s1_scan_results.csv + 控制台 Top 10
"""

from __future__ import annotations

import itertools
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd
import vectorbt as vbt

from core.data.exchange.base import Bar
from core.data.feed import ResearchFeed
from core.data.parquet_io import ParquetIO
from core.data.sqlite_repo import SqliteRepo
from core.db.migration_runner import MigrationRunner
from core.strategy.indicators import compute_atr, compute_donchian, compute_sma

SYMBOL = "BTCUSDT"
ENTRY_TF = "4h"
TREND_TF = "1d"
INITIAL_CASH = 10_000
FEE = 0.0004


def load_data() -> pd.DataFrame:
    conn = sqlite3.connect("data/crypto.sqlite")
    conn.row_factory = sqlite3.Row
    MigrationRunner(migrations_dir=Path("migrations")).apply_all(conn)
    feed = ResearchFeed(ParquetIO(data_root="data"), SqliteRepo(conn))
    bars: list[Bar] = feed.get_candles(SYMBOL, ENTRY_TF, n=2000)
    trend_bars: list[Bar] = feed.get_candles(SYMBOL, TREND_TF, n=200)
    conn.close()

    records = [{"open": b.o, "high": b.h, "low": b.l, "close": b.c, "volume": b.v, "quote_volume": b.q} for b in bars]
    df = pd.DataFrame(records)
    df.index = pd.to_datetime([b.ts for b in bars], unit="ms", utc=True)

    trend_records = [{"close": b.c} for b in trend_bars]
    tdf = pd.DataFrame(trend_records)
    tdf.index = pd.to_datetime([b.ts for b in trend_bars], unit="ms", utc=True)

    return df, tdf


def compute_signals_for_params(df, tdf, donchian_p, atr_p, trail_mult, trend_p, trend_long_p):
    close = df["close"]
    high = df["high"]
    low = df["low"]
    vol = df["quote_volume"]

    donch = compute_donchian(high, low, donchian_p)
    donch_upper = donch["upper"].shift(1)
    donch_lower = donch["lower"].shift(1)
    atr = compute_atr(high, low, close, atr_p)

    tc = tdf["close"]
    tma = compute_sma(tc, trend_p)
    tml = compute_sma(tc, trend_long_p)
    bull = (tc > tml) & (tma > tml)
    bear = (tc < tml) & (tma < tml)
    tm = pd.DataFrame({"b": bull, "be": bear}, index=tdf.index)
    t4h = tm.reindex(df.index, method="ffill")

    atr_med = atr.rolling(120).median()
    atr_ok = (atr >= atr_med * 1.0).fillna(True)
    vol7 = vol.rolling(42).mean()
    vol_ok = (vol >= vol7 * 1.2).fillna(True)

    el = t4h["b"] & (close > donch_upper) & atr_ok & vol_ok
    es_ = t4h["be"] & (close < donch_lower) & atr_ok & vol_ok
    ma20 = compute_sma(close, trend_p)
    tls = close - trail_mult * atr
    tss = close + trail_mult * atr
    xl = (close < ma20) | (close <= tls)
    xs = (close > ma20) | (close >= tss)

    n, entries, exits = len(close), pd.Series(False, index=close.index), pd.Series(False, index=close.index)
    il, is_ = False, False
    for i in range(1, n):
        if not il and not is_:
            if el.iloc[i]:
                entries.iloc[i] = True
                il = True
            elif es_.iloc[i]:
                entries.iloc[i] = True
                is_ = True
        elif il:
            if xl.iloc[i]:
                exits.iloc[i] = True
                il = False
                if es_.iloc[i]:
                    entries.iloc[i] = True
                    is_ = True
        elif is_:
            if xs.iloc[i]:
                exits.iloc[i] = True
                is_ = False
                if el.iloc[i]:
                    entries.iloc[i] = True
                    il = True
    return entries, exits


def run_scan():
    print("S1 参数扫描...")

    df, tdf = load_data()
    print(f"  数据: {len(df)} 根 4h K 线")

    param_grid = {
        "donchian_period": [10, 15, 20, 30, 40],
        "atr_period": [10, 14, 20],
        "trail_atr_mult": [1.0, 1.5, 2.0, 2.5, 3.0],
        "trend_ma_period": [10, 20, 30],
        "trend_long_ma_period": [30, 50, 100],
    }

    keys = list(param_grid.keys())
    total = np.prod([len(param_grid[k]) for k in keys])
    print(f"  参数组合: {total} 组")

    results = []
    done = 0

    for combo in itertools.product(*[param_grid[k] for k in keys]):
        params = dict(zip(keys, combo, strict=False))
        # 确保趋势快线 < 慢线
        if params["trend_ma_period"] >= params["trend_long_ma_period"]:
            continue

        entries, exits = compute_signals_for_params(
            df, tdf,
            params["donchian_period"], params["atr_period"], params["trail_atr_mult"],
            params["trend_ma_period"], params["trend_long_ma_period"],
        )

        if entries.sum() == 0:
            results.append({**params, "trades": 0, "return_pct": 0, "sharpe": 0, "mdd_pct": 0, "win_rate": 0})
            done += 1
            continue

        pf = vbt.Portfolio.from_signals(
            df["close"], entries=entries, exits=exits,
            init_cash=INITIAL_CASH, fees=FEE, freq=ENTRY_TF,
        )
        stats = pf.stats()
        trades = int(stats.get("Total Trades", 0))
        ret = float(stats.get("Total Return [%]", 0))
        sharpe = float(stats.get("Sharpe Ratio", 0))
        mdd = float(stats.get("Max Drawdown [%]", 0))
        wr = float(stats.get("Win Rate [%]", 0))

        results.append({**params, "trades": trades, "return_pct": ret, "sharpe": sharpe, "mdd_pct": mdd, "win_rate": wr})
        done += 1

        if done % 50 == 0:
            print(f"    进度: {done}/{total}")

    rdf = pd.DataFrame(results)
    rdf = rdf[rdf["trades"] >= 3].sort_values("sharpe", ascending=False)

    report_dir = Path("data/reports")
    report_dir.mkdir(parents=True, exist_ok=True)
    rdf.to_csv(report_dir / "s1_scan_results.csv", index=False)

    print(f"\n完成 {done} 组, 有效 (>=3 trades): {len(rdf)} 组")
    print("\n=== Top 10 按 Sharpe ===")
    cols = ["donchian_period", "atr_period", "trail_atr_mult", "trend_ma_period", "trend_long_ma_period",
            "trades", "return_pct", "sharpe", "mdd_pct", "win_rate"]
    print(rdf[cols].head(10).to_string(index=False))

    print("\n=== Top 10 按 Return ===")
    rdf_ret = rdf.sort_values("return_pct", ascending=False)
    print(rdf_ret[cols].head(10).to_string(index=False))

    # 最佳参数
    best = rdf.iloc[0]
    print(f"\n最佳参数: Donchian={best['donchian_period']:.0f}, ATR={best['atr_period']:.0f}, "
          f"Trail={best['trail_atr_mult']:.1f}x, Trend MA={best['trend_ma_period']:.0f}/{best['trend_long_ma_period']:.0f}")
    print(f"  Trades={best['trades']:.0f}, Return={best['return_pct']:.2f}%, "
          f"Sharpe={best['sharpe']:.2f}, MDD={best['mdd_pct']:.2f}%, WR={best['win_rate']:.1f}%")


if __name__ == "__main__":
    run_scan()
