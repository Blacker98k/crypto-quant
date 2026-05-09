"""分析 S1 参数扫描结果"""
import pandas as pd

df = pd.read_csv("data/reports/s1_scan_results.csv")
valid = df[df["trades"] >= 3]

non_m20_50 = valid[(valid["trend_ma_period"] != 20) | (valid["trend_long_ma_period"] != 50)]
top_other = non_m20_50.sort_values("sharpe", ascending=False).head(5)
print("=== Top 5 (非 MA20/50) ===")
for _, row in top_other.iterrows():
    print(f"  D={row.donchian_period:.0f} A={row.atr_period:.0f} T={row.trail_atr_mult:.1f} MA{row.trend_ma_period:.0f}/{row.trend_long_ma_period:.0f} | {row.trades:.0f}笔 Ret={row.return_pct:.2f}% Sharpe={row.sharpe:.2f}")

m20_50 = valid[(valid["trend_ma_period"]==20) & (valid["trend_long_ma_period"]==50)]
print("\n=== 按 Donchian 周期（MA20/50 子集）===")
for dc, grp in m20_50.groupby("donchian_period"):
    print(f"  D={dc:.0f}: Sharpe={grp.sharpe.mean():.3f} Ret={grp.return_pct.mean():.2f}% Trades={grp.trades.sum():.0f} MDD={grp.mdd_pct.mean():.2f}%")

best = valid[(valid["donchian_period"]==15) & (valid["trend_ma_period"]==20) & (valid["trend_long_ma_period"]==50)]
print("\n=== 最优参数 (D=15, MA20/50) ===")
print(f"  组数: {len(best)}")
print(f"  Avg Sharpe: {best.sharpe.mean():.3f}")
print(f"  Avg Return: {best.return_pct.mean():.2f}%")
print(f"  Avg MDD: {best.mdd_pct.mean():.2f}%")
