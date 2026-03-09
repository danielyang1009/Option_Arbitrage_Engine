#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
债券期限结构抓取器（Shibor + 中债国债收益率曲线）。

数据来源：
    - Shibor：中国货币网 chinamoney.com.cn ShiborHis 接口
    - 中债国债：中债官网 yield.chinabond.com.cn historyQuery 接口（HTML 表格解析）
      查询近 7 日范围，自动取最新可用日期的「中债国债收益率曲线」行。

用法示例（命令行）：
    python -m data_engine.bond_termstructure_fetcher --kind all
    python -m data_engine.bond_termstructure_fetcher --kind shibor --date 2026-03-05

输出（默认，横表格式）：
    D:\\MARKET_DATA\\macro\\shibor\\shibor_yieldcurve_YYYYMMDD.csv
    D:\\MARKET_DATA\\macro\\cgb_yield\\cgb_yieldcurve_YYYYMMDD.csv
"""

from __future__ import annotations

import argparse
import csv
import logging
from datetime import date, datetime, timedelta
from io import StringIO
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

from config.settings import DEFAULT_MARKET_DATA_DIR
from utils.time_utils import bj_today

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 1. Shibor 抓取（中国货币网 ShiborHis 接口）
# ---------------------------------------------------------------------------

_SHIBOR_TENORS = ["O/N", "1W", "2W", "1M", "3M", "6M", "9M", "1Y"]
_SHIBOR_API_TO_OUT = {"ON": "O/N", "1W": "1W", "2W": "2W", "1M": "1M", "3M": "3M", "6M": "6M", "9M": "9M", "1Y": "1Y"}


def fetch_shibor(target_date: Optional[date] = None) -> Dict[str, Any]:
    """
    从中国货币网 ShiborHis 接口抓取 Shibor 期限结构，返回横表单行数据。
    注：ShiborTxt 已 404，改用 ShiborHis。
    """
    d = target_date or bj_today()
    date_str = d.strftime("%Y-%m-%d")
    out: Dict[str, Any] = {"date": date_str, **{t: None for t in _SHIBOR_TENORS}}

    url = "https://www.chinamoney.com.cn/ags/ms/cm-u-bk-shibor/ShiborHis"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Referer": "https://www.chinamoney.com.cn/chinese/bkshibor/",
    }
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        raise RuntimeError(f"Shibor 请求失败: {e}") from e

    records = data.get("records", [])
    if not records:
        logger.warning("ShiborHis 未返回数据")
        return out

    row = None
    for r in records:
        if r.get("showDateCN") == date_str:
            row = r
            break
    if row is None:
        row = records[0]

    for api_key, out_key in _SHIBOR_API_TO_OUT.items():
        val = row.get(api_key)
        if val is not None:
            try:
                out[out_key] = round(float(val), 4)
            except (TypeError, ValueError):
                out[out_key] = None
    return out


# ---------------------------------------------------------------------------
# 2. 中债国债收益率曲线抓取（中债 historyQuery 接口，HTML 表格解析）
# ---------------------------------------------------------------------------

# 8 个标准期限（来自 historyQuery 表格列）→ CSV 列名
_CGB_AK_COL_TO_TENOR: Dict[str, str] = {
    "3月":  "0.25y",
    "6月":  "0.5y",
    "1年":  "1.0y",
    "3年":  "3.0y",
    "5年":  "5.0y",
    "7年":  "7.0y",
    "10年": "10.0y",
    "30年": "30.0y",
}
_CGB_TENOR_COLS: List[str] = list(_CGB_AK_COL_TO_TENOR.values())

_CGB_HISTORY_URL = (
    "https://yield.chinabond.com.cn/cbweb-pbc-web/pbc/historyQuery"
)
_CGB_CURVE_NAME = "中债国债收益率曲线"


def fetch_cgb_yieldcurve(target_date: Optional[date] = None) -> Dict[str, Any]:
    """
    通过中债 historyQuery 接口抓取国债收益率曲线（8 个标准期限）。

    若 target_date 当日数据尚未发布，自动向前最多回溯 7 个自然日，
    取最新可用日期的「中债国债收益率曲线」行。

    Args:
        target_date: 目标日期；未指定则使用 bj_today()
    Returns:
        {"date": "YYYY-MM-DD", "0.25y": ..., "0.5y": ..., ..., "30.0y": ...}
        data_date 字段反映实际数据日期（可能早于 target_date）。
    """
    import math
    import pandas as pd

    d = target_date or bj_today()
    out: Dict[str, Any] = {"date": d.strftime("%Y-%m-%d"), **{t: None for t in _CGB_TENOR_COLS}}

    start_date = d - timedelta(days=7)
    params = {
        "startDate": start_date.strftime("%Y-%m-%d"),
        "endDate":   d.strftime("%Y-%m-%d"),
        "gjqx": "0",
        "qxId": "ycqx",
        "locale": "cn_ZH",
    }
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"
        ),
    }
    try:
        resp = requests.get(_CGB_HISTORY_URL, params=params, headers=headers, timeout=15)
        resp.raise_for_status()
        text = resp.text.replace("&nbsp", "")
        tables = pd.read_html(StringIO(text), header=0)
        # 数据在第二张表（index=1）
        if len(tables) < 2:
            logger.warning("中债 historyQuery 未找到数据表")
            return out
        df = tables[1]
        cgb_df = df[df["曲线名称"] == _CGB_CURVE_NAME].copy()
        if cgb_df.empty:
            logger.warning("中债 historyQuery 表中未找到「%s」行", _CGB_CURVE_NAME)
            return out
        # 取最新日期行
        cgb_df["日期"] = pd.to_datetime(cgb_df["日期"], errors="coerce").dt.date
        cgb_df = cgb_df.sort_values("日期", ascending=False)
        row = cgb_df.iloc[0]
        data_date = row["日期"]
        out["date"] = data_date.strftime("%Y-%m-%d") if data_date else d.strftime("%Y-%m-%d")
        for ak_col, tenor in _CGB_AK_COL_TO_TENOR.items():
            val = row.get(ak_col)
            if val is not None and not (isinstance(val, float) and math.isnan(val)):
                out[tenor] = round(float(val), 4)
        if data_date and data_date < d:
            logger.warning(
                "中债收益率曲线：%s 暂无数据，使用最近可用日期 %s", d, data_date
            )
    except Exception as e:
        logger.warning("中债国债收益率曲线抓取失败: %s", e)
    return out


# ---------------------------------------------------------------------------
# 落盘函数
# ---------------------------------------------------------------------------


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def save_shibor_daily(target_date: date, base_dir: Optional[Path] = None) -> Path:
    """保存 Shibor 期限结构为横表 CSV：date,O/N,1W,2W,1M,3M,6M,9M,1Y"""
    base = base_dir or Path(DEFAULT_MARKET_DATA_DIR)
    out_dir = base / "macro" / "shibor"
    _ensure_dir(out_dir)
    fname = f"shibor_yieldcurve_{target_date.strftime('%Y%m%d')}.csv"
    out_path = out_dir / fname

    row = fetch_shibor(target_date)
    cols = ["date"] + _SHIBOR_TENORS
    with out_path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(cols)
        writer.writerow([row.get(c) if c == "date" else row.get(c, "") for c in cols])
    return out_path


def save_cgb_yieldcurve_daily(target_date: date, base_dir: Optional[Path] = None) -> Path:
    """保存中债国债收益率曲线为横表 CSV：date,0.0y,0.08y,...,50y（共 17 个期限）"""
    base = base_dir or Path(DEFAULT_MARKET_DATA_DIR)
    out_dir = base / "macro" / "cgb_yield"
    _ensure_dir(out_dir)
    fname = f"cgb_yieldcurve_{target_date.strftime('%Y%m%d')}.csv"
    out_path = out_dir / fname

    row = fetch_cgb_yieldcurve(target_date)
    valid_count = sum(1 for k in _CGB_TENOR_COLS if row.get(k) is not None)
    if valid_count < len(_CGB_TENOR_COLS):
        raise RuntimeError(
            f"中债国债收益率曲线数据不完整（{target_date}，有效期限 {valid_count}/{len(_CGB_TENOR_COLS)} 个），跳过落盘以避免覆盖有效文件"
        )
    # date 列统一写文件名对应的 target_date，避免回退日期导致 from_cgb_daily 校验失败
    row["date"] = target_date.strftime("%Y-%m-%d")
    cols = ["date"] + _CGB_TENOR_COLS
    with out_path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(cols)
        writer.writerow([row.get(c) if c == "date" else row.get(c, "") for c in cols])
    return out_path


# ---------------------------------------------------------------------------
# CLI 入口
# ---------------------------------------------------------------------------


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="从 Shibor 与中债网站抓取当日无风险利率期限结构，并保存至 D:\\MARKET_DATA\\macro",
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="目标日期 YYYY-MM-DD，默认使用北京时间今日",
    )
    parser.add_argument(
        "--kind",
        type=str,
        choices=["shibor", "cgb", "all"],
        default="all",
        help="抓取品种：shibor / cgb / all（默认 all）",
    )
    parser.add_argument(
        "--base-dir",
        type=str,
        default=str(DEFAULT_MARKET_DATA_DIR),
        help="输出根目录，默认使用 config.settings.DEFAULT_MARKET_DATA_DIR",
    )
    args = parser.parse_args(argv)

    if args.date:
        try:
            target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
        except ValueError:
            print(f"无效日期格式: {args.date}，应为 YYYY-MM-DD")
            return 1
    else:
        target_date = bj_today()

    base_dir = Path(args.base_dir)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    try:
        if args.kind in ("shibor", "all"):
            path = save_shibor_daily(target_date, base_dir=base_dir)
            print(f"Shibor 数据已保存至: {path}")
        if args.kind in ("cgb", "all"):
            path = save_cgb_yieldcurve_daily(target_date, base_dir=base_dir)
            print(f"中债收益率曲线数据已保存至: {path}")
    except Exception as exc:
        print(f"抓取失败: {exc}")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
