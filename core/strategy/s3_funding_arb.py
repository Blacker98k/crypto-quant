"""S3 · BTC/ETH 配对交易（替代原资金费率套利）。

基于 ETH/BTC 比率均值回归——当比率偏离均值超过 2σ 时开仓，
回归均值时平仓。

背景：原 S3 设计为资金费率套利，但 2025-2026 市场数据
显示费率极低（BTC 均值 0.0015%），套利空间不足以覆盖交易成本。
经数据分析，BTC/ETH 配对交易在相同时间段 |Z-score|>2 出现 126 次，
23 次交易机会，更适合当前市场环境。

入场：
  - Z-score > 2.0：ETH 相对 BTC 过贵 → 空 ETH 多 BTC
  - Z-score < -2.0：ETH 相对 BTC 过便宜 → 多 ETH 空 BTC

出场：
  - Z-score 回归 0（均值）
  - 或 Z-score 反向超过 3.0（极端止损）
  - 时间止损 14 天

注意：此策略需要双腿下单（多+空），当前 PaperMatchingEngine
只支持单腿，回测用 vectorbt 模拟。
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from core.strategy.base import DataRequirement, Signal, Strategy, StrategyContext
from core.strategy.indicators import bars_to_df

if TYPE_CHECKING:
    from core.data.exchange.base import Bar


class S3PairTrading(Strategy):
    """BTC/ETH 配对交易——使用 Z-score 均值回归。

    分析两个标的的价格比率，当比率极端偏离均值时开仓。
    需要 BTCUSDT 和 ETHUSDT 的同步数据。
    """

    name = "S3_pair_trading"
    version = "dev"
    config_hash = "0000000000000000"

    # ── 可配置参数 ──────────────────────────────────────────────────────────
    zscore_entry: float = 2.0       # 开仓阈值
    zscore_exit: float = 0.5        # 平仓阈值（接近均值）
    zscore_stop: float = 3.0        # 止损阈值
    lookback_bars: int = 200        # Z-score 计算窗口（4h bars）
    max_hold_bars: int = 84         # 时间止损（14 天 × 6 根 4h/天）
    per_trade_risk_pct: float = 0.01  # 1% 风险

    __slots__ = ()

    def required_data(self) -> DataRequirement:
        return DataRequirement(
            symbols=["BTCUSDT", "ETHUSDT"],
            timeframes=["4h", "1d"],
            history_lookback_bars=500,
            needs_funding=False,
        )

    def on_bar(self, bar: Bar, ctx: StrategyContext) -> list[Signal]:
        """每根 4h K 线收盘时检查价差。

        注意：只对 BTC 的 K 线触发（ETH 触发会重复）。
        """
        if bar.symbol != "BTCUSDT":
            return []

        signals: list[Signal] = []

        # 拉 BTC 和 ETH 的 4h 数据
        btc_bars = ctx.data.get_candles("BTCUSDT", "4h", n=self.lookback_bars)
        eth_bars = ctx.data.get_candles("ETHUSDT", "4h", n=self.lookback_bars)

        if len(btc_bars) < 100 or len(eth_bars) < 100:
            return signals

        btc_df = bars_to_df(btc_bars)
        eth_df = bars_to_df(eth_bars)

        # 对齐时间轴
        common_idx = btc_df.index.intersection(eth_df.index)
        if len(common_idx) < 50:
            return signals

        btc_close = btc_df.loc[common_idx, "close"]
        eth_close = eth_df.loc[common_idx, "close"]

        # 计算 ETH/BTC 比率和 Z-score
        ratio = eth_close / btc_close
        ratio_mean = ratio.mean()
        ratio_std = ratio.std()
        if ratio_std <= 0:
            return signals
        zscore = (ratio - ratio_mean) / ratio_std

        current_zscore = zscore.iloc[-1]
        # 检查当前持仓状态
        position = ctx.kv_get("s3_pair_position")

        if position is None:
            # 无持仓 → 检查入场
            if current_zscore > self.zscore_entry:
                signals.append(Signal(
                    side="close",  # 配对交易用 trade_group_id 标记
                    symbol="S3_PAIR",
                    entry_price=None,
                    stop_price=None,
                    confidence=min(0.8, max(0.3, (current_zscore - self.zscore_entry) / self.zscore_entry)),
                    suggested_size=0,
                    rationale={
                        "entry": "pair_short_eth_long_btc",
                        "zscore": float(current_zscore),
                        "ratio": float(ratio.iloc[-1]),
                        "ratio_mean": float(ratio_mean),
                    },
                    trade_group_id=f"s3_pair_{bar.ts}",
                ))
                ctx.kv_set("s3_pair_position", "short_eth_long_btc")
                ctx.kv_set("s3_pair_entry_zscore", float(current_zscore))
                ctx.kv_set("s3_pair_entry_ts", bar.ts)

            elif current_zscore < -self.zscore_entry:
                signals.append(Signal(
                    side="close",
                    symbol="S3_PAIR",
                    entry_price=None,
                    stop_price=None,
                    confidence=min(0.8, max(0.3, (-current_zscore - self.zscore_entry) / self.zscore_entry)),
                    suggested_size=0,
                    rationale={
                        "entry": "pair_long_eth_short_btc",
                        "zscore": float(current_zscore),
                        "ratio": float(ratio.iloc[-1]),
                        "ratio_mean": float(ratio_mean),
                    },
                    trade_group_id=f"s3_pair_{bar.ts}",
                ))
                ctx.kv_set("s3_pair_position", "long_eth_short_btc")
                ctx.kv_set("s3_pair_entry_zscore", float(current_zscore))
                ctx.kv_set("s3_pair_entry_ts", bar.ts)

        else:
            # 有持仓 → 检查出场
            entry_zscore = ctx.kv_get("s3_pair_entry_zscore") or 0
            entry_ts = ctx.kv_get("s3_pair_entry_ts") or 0
            bars_held = (bar.ts - entry_ts) / (4 * 3600_000) if entry_ts else 0

            exit_reason = None

            if abs(current_zscore) <= self.zscore_exit:
                exit_reason = "zscore_return_to_mean"
            elif abs(current_zscore) >= self.zscore_stop:
                exit_reason = "zscore_stop_loss"
            elif bars_held >= self.max_hold_bars:
                exit_reason = "time_stop"

            if exit_reason:
                signals.append(Signal(
                    side="close",
                    symbol="S3_PAIR",
                    rationale={
                        "exit": exit_reason,
                        "entry_zscore": float(entry_zscore),
                        "exit_zscore": float(current_zscore),
                        "bars_held": int(bars_held),
                    },
                ))
                ctx.kv_set("s3_pair_position", None)
                ctx.kv_set("s3_pair_entry_zscore", None)
                ctx.kv_set("s3_pair_entry_ts", None)

        return signals


__all__ = ["S3PairTrading"]
