#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""分析 ETF Parquet 文件的数据频率等信息。"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import numpy as np


def main():
    path = Path(r"d:\MARKET_DATA\etf_20260306.parquet")
    if not path.exists():
        print("File not found:", path)
        return

    df = pd.read_parquet(path)
    print("=== 基本信息 ===")
    print("总行数:", len(df))
    print("列:", list(df.columns))
    print()

    if "code" in df.columns:
        print("=== 品种分布 ===")
        vc = df["code"].value_counts()
        for code, cnt in vc.items():
            print(f"  {code}: {cnt} 条")
        print()

    if "ts" in df.columns:
        df["dt"] = pd.to_datetime(df["ts"], unit="ms")
        print("=== 时间范围 ===")
        print("最早:", df["dt"].min())
        print("最晚:", df["dt"].max())
        print("时长:", df["dt"].max() - df["dt"].min())
        print()

        # 按品种统计频率
        print("=== 各品种 tick 频率（估算）===")
        for code in df["code"].unique():
            sub = df[df["code"] == code]
            sub = sub.sort_values("ts")
            dts = sub["dt"].diff().dropna()
            dts_ms = dts.dt.total_seconds() * 1000
            valid = dts_ms[dts_ms > 0]  # 排除同毫秒
            if len(valid) > 0:
                median_ms = valid.median()
                mean_ms = valid.mean()
                p95_ms = valid.quantile(0.95)
                ticks_per_sec = 1000 / mean_ms if mean_ms > 0 else 0
                print(f"  {code}:")
                print(f"    条数: {len(sub)}")
                print(f"    间隔中位数: {median_ms:.1f} ms")
                print(f"    间隔均值: {mean_ms:.1f} ms")
                print(f"    间隔 P95: {p95_ms:.1f} ms")
                print(f"    约 {ticks_per_sec:.1f} tick/秒")
            print()

        # 按分钟统计 tick 数
        print("=== 按分钟 tick 数（前 10 分钟）===")
        df["minute"] = df["dt"].dt.floor("min")
        by_min = df.groupby("minute").size()
        for i, (m, cnt) in enumerate(by_min.head(10).items()):
            print(f"  {m}: {cnt} 条")
        print("  ...")
        print(f"  全天共 {len(by_min)} 个分钟有数据")
        print()

    print("=== 前 5 行 ===")
    print(df.head().to_string())
    print()
    print("=== 后 5 行 ===")
    print(df.tail().to_string())


if __name__ == "__main__":
    main()
