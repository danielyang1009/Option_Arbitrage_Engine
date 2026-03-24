#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
检查 ETF + 期权 Parquet 文件的数据质量。

用法：
    python scripts/analyze_etf_parquet.py               # 默认检查今日
    python scripts/analyze_etf_parquet.py 20260324      # 指定日期
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import argparse
from datetime import datetime

MARKET_DATA_DIR = Path(r"D:\MARKET_DATA")
UNDERLYINGS = ["510050", "510300", "510500"]


def check_file(path: Path, label: str) -> pd.DataFrame | None:
    if not path.exists():
        print(f"  [{label}] 文件不存在: {path}")
        return None
    df = pd.read_parquet(path)
    return df


def analyze_df(df: pd.DataFrame, label: str):
    print(f"  [{label}] rows={len(df):,}")

    # 时间跨度
    if "ts" in df.columns:
        times = pd.to_datetime(df["ts"], unit="ms", utc=True).dt.tz_convert("Asia/Shanghai")
        print(f"    时间跨度: {str(times.min())[11:19]} -> {str(times.max())[11:19]}")

        # 逐小时分布
        hour_dist = times.dt.hour.value_counts().sort_index()
        hour_str = "  ".join(f"{h}时:{c:,}" for h, c in hour_dist.items())
        print(f"    逐小时:  {hour_str}")

        # 午休缺口检查（12时应无数据）
        if 12 in hour_dist.index:
            print(f"    [警告] 12时有数据 ({hour_dist[12]:,} 条)，午休期间不应有 tick")

        # 开盘/收盘时刻检查
        first_h, first_m = times.min().hour, times.min().minute
        last_h, last_m = times.max().hour, times.max().minute
        if not (first_h == 9 and first_m <= 20):
            print(f"    [警告] 开盘时间偏晚: {str(times.min())[11:19]}（预期 ~09:15）")
        if last_h < 14 or (last_h == 14 and last_m < 55):
            print(f"    [警告] 收盘时间偏早: {str(times.max())[11:19]}（预期 ~14:59）")

    # NaN 检查
    total_cells = len(df) * len(df.columns)
    total_nan = df.isna().sum().sum()
    if total_nan == 0:
        print(f"    NaN:  无")
    else:
        nan_pct_overall = total_nan / total_cells * 100
        bad_cols = {c: f"{df[c].isna().mean()*100:.1f}%" for c in df.columns if df[c].isna().mean() > 0}
        print(f"    NaN:  总计 {total_nan:,} ({nan_pct_overall:.2f}%)  按列: {bad_cols}")

    # tick 频率（仅当行数适中时估算，避免期权文件耗时过长）
    if len(df) <= 20000 and "ts" in df.columns:
        df_sorted = df.sort_values("ts")
        diffs_ms = df_sorted["ts"].diff().dropna()
        valid = diffs_ms[diffs_ms > 0]
        if len(valid) > 0:
            median_ms = valid.median()
            mean_ms = valid.mean()
            tps = 1000 / mean_ms if mean_ms > 0 else 0
            print(f"    频率:  均值间隔 {mean_ms:.0f}ms / 中位 {median_ms:.0f}ms (~{tps:.1f} tick/s)")


def main():
    parser = argparse.ArgumentParser(description="检查 ETF + 期权 Parquet 数据质量")
    parser.add_argument("date", nargs="?", default=datetime.now().strftime("%Y%m%d"),
                        help="日期，格式 YYYYMMDD（默认今日）")
    args = parser.parse_args()
    date_str = args.date

    print(f"检查日期: {date_str}")
    print("=" * 60)

    for etf in UNDERLYINGS:
        print(f"\n[{etf}]")

        etf_path = MARKET_DATA_DIR / etf / f"etf_{date_str}.parquet"
        df_etf = check_file(etf_path, "ETF")
        if df_etf is not None:
            analyze_df(df_etf, "ETF")

        opt_path = MARKET_DATA_DIR / etf / f"options_{date_str}.parquet"
        df_opt = check_file(opt_path, "Options")
        if df_opt is not None:
            analyze_df(df_opt, "Options")

    print("\n" + "=" * 60)
    print("检查完成")


if __name__ == "__main__":
    main()
