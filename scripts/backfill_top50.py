#!/usr/bin/env python3
"""拉取 top 50 永续币的 1h/1d K 线数据——供 S2 均值回归回测使用。

用法: uv run python scripts/backfill_top50.py
"""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

from core.data.exchange.binance_usdm import BinanceUsdmAdapter

STABLECOINS = {"USDT", "USDC", "DAI", "BUSD", "TUSD", "FDUSD", "PAX", "USTC", "USDD", "FRAX"}
EXCLUDE_SYMBOLS = {"BTCUSDT", "ETHUSDT", "BNBUSDT"}
TOP_N = 30  # 先拉前 30，跑通流程
TIMEFRAMES = ["1h", "1d"]
DAYS_BACK = 365
PROXY = "http://127.0.0.1:57777"


async def fetch_top_perp_symbols() -> list[str]:
    """从 Binance USDM 获取 top N 永续币列表（按 24h 成交额排序）。"""
    print("[1/4] 获取 Binance 永续币列表...")
    ex = BinanceUsdmAdapter(proxy=PROXY, timeout_ms=30000)
    try:
        await ex._ensure_markets_loaded()
    except Exception as e:
        print(f"  load_markets 失败: {e}")
        print("  回退到 ccxt 直连方式...")
        import ccxt
        cex = ccxt.binanceusdm({
            "enableRateLimit": True,
            "proxies": {"http": PROXY, "https": PROXY},
            "timeout": 30000,
        })
        cex.load_markets()
        markets = cex.markets
    else:
        markets = ex._ex.markets

    # 提取所有永续合约，按成交额排序
    perp_symbols = []
    for ccxt_sym, m in markets.items():
        if m.get("type") != "swap" or not m.get("active", False):
            continue
        internal = ccxt_sym.replace("/", "").split(":")[0].upper()
        # 过滤稳定币
        base = m.get("base", "")
        if base.upper() in STABLECOINS:
            continue
        if internal in EXCLUDE_SYMBOLS:
            continue
        quote_vol = float(m.get("info", {}).get("quoteVolume", 0) or 0)
        perp_symbols.append((internal, quote_vol, ccxt_sym))

    perp_symbols.sort(key=lambda x: x[1], reverse=True)
    top = [s[0] for s in perp_symbols[:TOP_N]]

    print(f"  Top {TOP_N}: {', '.join(top[:10])}...")
    await ex.close()
    return top


async def backfill_symbol(exchange, parquet_io, symbol: str, tf: str, days: int):
    """回填单个币的单个 timeframe。"""
    import time
    now = int(time.time() * 1000)
    start = now - days * 24 * 60 * 60 * 1000

    try:
        bars = await exchange.fetch_klines(symbol, tf, start, now)
        if bars:
            parquet_io.write_bars(bars)
            return len(bars)
    except Exception as e:
        print(f"    [{symbol}] {tf} 失败: {type(e).__name__}")
        return 0
    return 0


async def main():
    print("=" * 60)
    print("  Top 50 永续币数据回填")
    print(f"  Top {TOP_N}, Timeframes: {TIMEFRAMES}, Days: {DAYS_BACK}")
    print("=" * 60)

    # 1. 获取币列表
    symbols = await fetch_top_perp_symbols()
    if not symbols:
        print("  未获取到任何币，退出")
        return

    # 2. 初始化
    print("\n[2/4] 初始化数据层...")
    from core.data.parquet_io import ParquetIO
    from core.data.sqlite_repo import SqliteRepo
    from core.db.migration_runner import MigrationRunner

    conn = sqlite3.connect("data/crypto.sqlite")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    MigrationRunner(migrations_dir=Path("migrations")).apply_all(conn)
    SqliteRepo(conn)
    parquet_io = ParquetIO(data_root="data")

    # 3. 回填
    ex = BinanceUsdmAdapter(proxy=PROXY, timeout_ms=30000)

    print(f"\n[3/4] 开始回填 {len(symbols)} 个币 × {len(TIMEFRAMES)} 个 timeframe...")
    total_bars = 0
    completed = 0

    for symbol in symbols:
        for tf in TIMEFRAMES:
            bars = await backfill_symbol(ex, parquet_io, symbol, tf, DAYS_BACK)
            if bars:
                total_bars += bars
                completed += 1
        print(f"  [{symbol}] 完成")

    await ex.close()
    conn.close()

    print("\n[4/4] 完成!")
    print(f"  回填: {completed}/{len(symbols) * len(TIMEFRAMES)} 个 (symbol, tf) 对")
    print(f"  总 K 线: {total_bars} 根")


if __name__ == "__main__":
    asyncio.run(main())
