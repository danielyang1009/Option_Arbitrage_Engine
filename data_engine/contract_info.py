"""
合约信息管理器

双通道设计：
1. 主通道：从 CSV 文件（metadata/上交所期权基本信息.csv）加载全量合约映射表
2. Fallback：根据上交所编码规则和证券简称解析合约属性

处理代码后缀标准化（.SH / .XSHG 互转）并提供 Call/Put 配对查询。
"""

from __future__ import annotations

import csv
import logging
import re
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from models import (
    ContractInfo,
    OptionType,
    UNDERLYING_MAP,
    normalize_code,
)

logger = logging.getLogger(__name__)

_SORTED_UNDERLYING_KEYS = sorted(UNDERLYING_MAP.keys(), key=len, reverse=True)


class ContractInfoManager:
    """
    期权合约基本信息管理器

    加载并维护合约代码到 ContractInfo 的映射表，
    提供高效的合约查询和 Call/Put 配对功能。

    Attributes:
        contracts: 合约代码 -> ContractInfo 的字典（代码统一 .SH 后缀）
    """

    def __init__(self) -> None:
        self.contracts: Dict[str, ContractInfo] = {}
        self._pairs_cache: Dict[str, List[Tuple[ContractInfo, ContractInfo]]] = {}

    def load_from_csv(self, filepath: str | Path) -> int:
        """
        从 CSV 文件加载合约基本信息

        CSV 格式要求（UTF-8/UTF-8-BOM 编码）：
        证券代码,证券简称,起始交易日期,最后交易日期,交割月份,行权价格,期权类型

        Args:
            filepath: CSV 文件路径

        Returns:
            成功加载的合约数量

        Raises:
            FileNotFoundError: CSV 文件不存在
        """
        filepath = Path(filepath)
        if not filepath.exists():
            raise FileNotFoundError(f"合约信息文件不存在: {filepath}")

        logger.info("加载合约信息: %s", filepath)
        count = 0

        with open(filepath, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    info = self._parse_csv_row(row)
                    if info is not None:
                        self.contracts[info.contract_code] = info
                        count += 1
                except Exception as e:
                    logger.warning("解析合约行失败（跳过）: %s | 行内容: %s", e, row)
                    continue

        self._pairs_cache.clear()
        logger.info("成功加载 %d 条合约信息", count)
        return count

    def get_info(self, contract_code: str) -> Optional[ContractInfo]:
        """
        查询合约信息

        Args:
            contract_code: 合约代码（支持 .SH 和 .XSHG 后缀，自动标准化）

        Returns:
            ContractInfo 或 None（未找到）
        """
        normalized = normalize_code(contract_code, ".SH")
        return self.contracts.get(normalized)

    def find_call_put_pairs(
        self,
        underlying: str,
        expiry: Optional[date] = None,
        delivery_month: Optional[str] = None,
    ) -> List[Tuple[ContractInfo, ContractInfo]]:
        """
        查找同行权价的 Call/Put 配对

        按标的代码和到期日/交割月份筛选，返回所有匹配的 (Call, Put) 元组。

        Args:
            underlying: 标的 ETF 代码（如 '510050.SH'）或简称（如 '50ETF'）
            expiry: 按到期日精确筛选（可选）
            delivery_month: 按交割月份筛选，如 '201503'（可选）

        Returns:
            (Call ContractInfo, Put ContractInfo) 元组列表，按行权价排序
        """
        underlying_code = self._resolve_underlying(underlying)

        cache_key = f"{underlying_code}|{expiry}|{delivery_month}"
        if cache_key in self._pairs_cache:
            return self._pairs_cache[cache_key]

        calls: Dict[float, ContractInfo] = {}
        puts: Dict[float, ContractInfo] = {}

        for info in self.contracts.values():
            if info.underlying_code != underlying_code:
                continue
            if expiry is not None and info.expiry_date != expiry:
                continue
            if delivery_month is not None and info.delivery_month != delivery_month:
                continue

            if info.option_type == OptionType.CALL:
                calls[info.strike_price] = info
            else:
                puts[info.strike_price] = info

        pairs: List[Tuple[ContractInfo, ContractInfo]] = []
        common_strikes = sorted(set(calls.keys()) & set(puts.keys()))
        for strike in common_strikes:
            pairs.append((calls[strike], puts[strike]))

        self._pairs_cache[cache_key] = pairs
        return pairs

    def get_available_expiries(self, underlying: str) -> List[date]:
        """
        获取指定标的的所有可用到期日

        Args:
            underlying: 标的代码或简称

        Returns:
            按日期排序的到期日列表
        """
        underlying_code = self._resolve_underlying(underlying)
        expiries = set()
        for info in self.contracts.values():
            if info.underlying_code == underlying_code:
                expiries.add(info.expiry_date)
        return sorted(expiries)

    def load_multipliers_from_wind(self, codes: Optional[List[str]] = None, batch_size: int = 200) -> int:
        """
        通过 Wind wss 批量查询真实合约乘数，更新 contract_unit 字段。

        Args:
            codes: 要查询的合约代码列表；None 表示查所有已加载的合约
            batch_size: 单批查询上限

        Returns:
            成功更新的合约数
        """
        try:
            from WindPy import w
        except ImportError:
            logger.warning("WindPy 不可用，跳过乘数查询，全部使用默认值 10000")
            return 0

        if codes is None:
            codes = list(self.contracts.keys())
        if not codes:
            return 0

        updated = 0
        for i in range(0, len(codes), batch_size):
            batch = codes[i : i + batch_size]
            result = w.wss(",".join(batch), "contractmultiplier")
            if result is None or result.ErrorCode != 0:
                logger.warning("Wind wss 查询乘数失败 (batch %d)", i // batch_size)
                continue
            for j, code in enumerate(result.Codes):
                norm = normalize_code(code, ".SH")
                info = self.contracts.get(norm)
                if info is None:
                    continue
                try:
                    mult = int(float(result.Data[0][j]))
                    if mult > 0:
                        info.contract_unit = mult
                        updated += 1
                except (TypeError, ValueError, IndexError):
                    pass

        logger.info("已从 Wind 更新 %d / %d 个合约的真实乘数", updated, len(codes))
        return updated

    def get_contracts_by_underlying(self, underlying: str) -> List[ContractInfo]:
        """
        获取指定标的的所有合约

        Args:
            underlying: 标的代码或简称

        Returns:
            ContractInfo 列表
        """
        underlying_code = self._resolve_underlying(underlying)
        return [
            info for info in self.contracts.values()
            if info.underlying_code == underlying_code
        ]

    def _resolve_underlying(self, underlying: str) -> str:
        """将简称或代码统一解析为标准代码"""
        if underlying in UNDERLYING_MAP:
            return UNDERLYING_MAP[underlying]
        return normalize_code(underlying, ".SH")

    _ADJUSTED_TAIL_RE = re.compile(r"[A-Z]$")

    def _parse_csv_row(self, row: Dict[str, str]) -> Optional[ContractInfo]:
        """
        解析 CSV 中的一行数据为 ContractInfo

        跳过空行（证券代码为空的行）。
        自动识别调整型合约（ETF 分红后产生，名称末尾带 A/B/C 标记）。
        """
        raw_code = row.get("证券代码", "").strip()
        if not raw_code:
            return None

        short_name = row.get("证券简称", "").strip()
        if not short_name:
            return None

        contract_code = normalize_code(raw_code, ".SH")
        option_type = self._parse_option_type(row.get("期权类型", ""))
        underlying_code = self._infer_underlying(short_name)
        strike_price = float(row["行权价格"])
        list_date = datetime.strptime(row["起始交易日期"], "%Y-%m-%d").date()
        expiry_date = datetime.strptime(row["最后交易日期"], "%Y-%m-%d").date()
        delivery_month = row.get("交割月份", "").strip()

        is_adjusted = bool(self._ADJUSTED_TAIL_RE.search(short_name))

        return ContractInfo(
            contract_code=contract_code,
            short_name=short_name,
            underlying_code=underlying_code,
            option_type=option_type,
            strike_price=strike_price,
            list_date=list_date,
            expiry_date=expiry_date,
            delivery_month=delivery_month,
            is_adjusted=is_adjusted,
        )

    @staticmethod
    def _parse_option_type(raw: str) -> OptionType:
        """将中文期权类型转换为枚举"""
        raw = raw.strip()
        if raw == "认购":
            return OptionType.CALL
        elif raw == "认沽":
            return OptionType.PUT
        else:
            raise ValueError(f"未知的期权类型: '{raw}'")

    @staticmethod
    def _infer_underlying(short_name: str) -> str:
        """
        从证券简称推断标的 ETF 代码

        匹配规则按长度从长到短，避免 '50ETF' 错误匹配 '科创板50'。

        Args:
            short_name: 如 "50ETF购2015年3月2200"

        Returns:
            标的 ETF 代码，如 "510050.SH"

        Raises:
            ValueError: 无法识别的标的类型
        """
        for prefix in _SORTED_UNDERLYING_KEYS:
            if prefix in short_name:
                return UNDERLYING_MAP[prefix]

        raise ValueError(f"无法从证券简称推断标的: '{short_name}'")
