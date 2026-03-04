# -*- coding: utf-8 -*-
"""
开盘前从 Wind 抓取期权链（含合约乘数），保存到 metadata 目录。

用法:
    python -m data_engine.fetch_optionchain [--date YYYY-MM-DD]
    python fetch_optionchain.py [--date YYYY-MM-DD]

输出: metadata/YYYY-MM-DD_optionchain.csv
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, datetime
from pathlib import Path

logger = logging.getLogger(__name__)

# 监控的标的（与 monitors/common 一致）
UNDERLYINGS = ["510050.SH", "510300.SH", "510500.SH"]

PROJECT_ROOT = Path(__file__).resolve().parent.parent
METADATA_DIR = PROJECT_ROOT / "metadata"


def fetch_optionchain_from_wind(
    target_date: date,
    underlyings: list[str] | None = None,
):
    """
    从 Wind wset 抓取期权链，包含合约乘数等信息。

    Args:
        target_date: 查询日期
        underlyings: 标的代码列表，默认 50/300/500 ETF

    Returns:
        pandas DataFrame，多标的合并去重
    """
    try:
        from WindPy import w
        import pandas as pd
    except ImportError as e:
        raise RuntimeError(f"依赖缺失: {e}") from e

    if underlyings is None:
        underlyings = UNDERLYINGS

    date_str = target_date.strftime("%Y-%m-%d")
    dfs = []

    result = w.start()
    if result.ErrorCode != 0:
        raise RuntimeError(f"Wind 连接失败，ErrorCode={result.ErrorCode}")

    for us_code in underlyings:
        try:
            # 优先 usedf=True 获取 DataFrame；若不支持则用 WindData 转 DataFrame
            opts = f"date={date_str};us_code={us_code};option_var=all;call_put=all"
            raw = w.wset("optionchain", opts, usedf=True)
            if raw is None:
                logger.warning("wset optionchain 返回 None: us_code=%s", us_code)
                continue
            # 兼容 (ErrorCode, DataFrame) 或 直接 DataFrame
            if isinstance(raw, tuple):
                err, df = raw[0], raw[1]
                if err != 0 or df is None or (hasattr(df, "empty") and df.empty):
                    logger.warning("wset optionchain 失败: us_code=%s ErrorCode=%s", us_code, err)
                    continue
            else:
                df = raw
                if df is None or (hasattr(df, "empty") and df.empty):
                    logger.warning("wset optionchain 返回空: us_code=%s", us_code)
                    continue
            dfs.append(df)
        except TypeError:
            # usedf 可能不被支持，回退到 WindData
            try:
                result = w.wset("optionchain", opts)
                if result is None or result.ErrorCode != 0:
                    logger.warning("wset optionchain 失败: us_code=%s", us_code)
                    continue
                fields = getattr(result, "Fields", []) or []
                data = getattr(result, "Data", []) or []
                if not data or not fields:
                    continue
                df = pd.DataFrame(
                    {fields[j]: data[j] for j in range(min(len(fields), len(data)))}
                )
                if not df.empty:
                    dfs.append(df)
            except Exception as e:
                logger.warning("抓取 %s 期权链异常: %s", us_code, e)
        except Exception as e:
            logger.warning("抓取 %s 期权链异常: %s", us_code, e)

    if not dfs:
        return pd.DataFrame()

    merged = pd.concat(dfs, ignore_index=True)
    # 按期权合约代码去重（同一合约可能出现在多标的结果中）
    code_col = next(
        (c for c in ("option_code", "wind_code", "code") if c in merged.columns),
        None,
    )
    if code_col:
        merged = merged.drop_duplicates(subset=[code_col], keep="first")
    return merged


def main() -> int:
    parser = argparse.ArgumentParser(
        description="开盘前从 Wind 抓取期权链（含合约乘数），保存到 metadata",
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="查询日期 YYYY-MM-DD，默认今日",
    )
    args = parser.parse_args()

    if args.date:
        try:
            target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
        except ValueError:
            print(f"无效日期格式: {args.date}，应为 YYYY-MM-DD")
            return 1
    else:
        target_date = date.today()

    METADATA_DIR.mkdir(parents=True, exist_ok=True)
    out_path = METADATA_DIR / f"{target_date.strftime('%Y-%m-%d')}_optionchain.csv"

    print(f"正在从 Wind 抓取 {target_date} 期权链...")
    try:
        df = fetch_optionchain_from_wind(target_date)
    except RuntimeError as e:
        print(f"抓取失败: {e}")
        return 1

    if df is None or (hasattr(df, "empty") and df.empty):
        print("未获取到任何合约数据")
        return 1

    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    n = len(df)
    print(f"已保存 {n} 条至 {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
