"""
上交所期权保证金计算模块

实现卖出期权（非备兑）的开仓保证金和维持保证金计算。

保证金公式（上交所规则）：
卖出认购开仓保证金 = 权利金 + max(比例1 * 合约标的收盘价 * 合约单位 - 虚值额, 比例2 * 合约标的收盘价 * 合约单位)
卖出认沽开仓保证金 = 权利金 + max(比例1 * 合约标的收盘价 * 合约单位 - 虚值额, 比例2 * 行权价 * 合约单位)

其中虚值额：
  认购虚值额 = max(行权价 - 标的收盘价, 0) * 合约单位
  认沽虚值额 = max(标的收盘价 - 行权价, 0) * 合约单位
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

from config.settings import MarginConfig, TradingConfig
from models import ContractInfo, OptionType

logger = logging.getLogger(__name__)


@dataclass
class MarginResult:
    """保证金计算结果"""
    initial_margin: float       # 开仓保证金
    maintenance_margin: float   # 维持保证金
    premium_component: float    # 权利金部分
    risk_component: float       # 风险部分（max 项）
    out_of_money_amount: float  # 虚值额


class MarginCalculator:
    """
    期权卖方保证金计算器

    依据上交所规则计算非备兑卖出期权的保证金要求。

    Attributes:
        config: 保证金参数配置
        contract_unit: 合约单位
    """

    def __init__(self, config: Optional[TradingConfig] = None) -> None:
        """
        初始化保证金计算器

        Args:
            config: 交易配置（不传则使用默认配置）
        """
        if config is None:
            from config.settings import get_default_config
            config = get_default_config()

        self._margin_config = config.margin
        self._contract_unit = config.contract_unit

    def calc_initial_margin(
        self,
        contract_info: ContractInfo,
        option_premium: float,
        underlying_close: float,
    ) -> MarginResult:
        """
        计算卖出开仓初始保证金

        Args:
            contract_info: 合约基本信息
            option_premium: 期权权利金（每份价格，非总额）
            underlying_close: 标的 ETF 前收盘价

        Returns:
            MarginResult 保证金计算结果
        """
        K = contract_info.strike_price
        S = underlying_close
        unit = self._contract_unit
        mc = self._margin_config

        premium_total = option_premium * unit

        if contract_info.option_type == OptionType.CALL:
            out_of_money = max(K - S, 0.0) * unit
            risk_part = max(
                mc.call_margin_ratio_1 * S * unit - out_of_money,
                mc.call_margin_ratio_2 * S * unit,
            )
        else:
            out_of_money = max(S - K, 0.0) * unit
            risk_part = max(
                mc.put_margin_ratio_1 * S * unit - out_of_money,
                mc.put_margin_ratio_2 * K * unit,
            )

        initial_margin = premium_total + risk_part

        maintenance_margin = self._calc_maintenance_margin(
            contract_info, option_premium, underlying_close,
        )

        return MarginResult(
            initial_margin=initial_margin,
            maintenance_margin=maintenance_margin,
            premium_component=premium_total,
            risk_component=risk_part,
            out_of_money_amount=out_of_money,
        )

    def _calc_maintenance_margin(
        self,
        contract_info: ContractInfo,
        option_settle_price: float,
        underlying_close: float,
    ) -> float:
        """
        计算维持保证金

        维持保证金 = 结算价 * 合约单位 + max(比例1 * S * unit - 虚值额, 比例2 * S或K * unit)
        与初始保证金结构相似，但使用结算价替代权利金。

        Args:
            contract_info: 合约信息
            option_settle_price: 期权结算价
            underlying_close: 标的收盘价

        Returns:
            维持保证金金额
        """
        K = contract_info.strike_price
        S = underlying_close
        unit = self._contract_unit
        mc = self._margin_config

        settle_total = option_settle_price * unit

        if contract_info.option_type == OptionType.CALL:
            out_of_money = max(K - S, 0.0) * unit
            risk_part = max(
                mc.call_margin_ratio_1 * S * unit - out_of_money,
                mc.call_margin_ratio_2 * S * unit,
            )
        else:
            out_of_money = max(S - K, 0.0) * unit
            risk_part = max(
                mc.put_margin_ratio_1 * S * unit - out_of_money,
                mc.put_margin_ratio_2 * K * unit,
            )

        return settle_total + risk_part

    def calc_portfolio_margin(
        self,
        positions: list,
        underlying_close: float,
    ) -> float:
        """
        计算组合持仓的总保证金要求

        遍历所有卖方持仓（quantity < 0 的期权持仓），累加保证金。
        买方持仓不占用保证金。

        Args:
            positions: Position 对象列表
            underlying_close: 标的收盘价

        Returns:
            总保证金占用金额
        """
        total_margin = 0.0

        for pos in positions:
            if pos.quantity >= 0:
                continue

            from data_engine.contract_info import ContractInfoManager
            # 此处为骨架：实际使用中需传入 ContractInfoManager 实例
            logger.debug(
                "计算合约 %s 的保证金（持仓 %d 张）",
                pos.contract_code, pos.quantity,
            )
            total_margin += abs(pos.quantity) * pos.margin_occupied

        return total_margin
