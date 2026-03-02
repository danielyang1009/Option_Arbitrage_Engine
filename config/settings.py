"""
全局配置模块

集中管理交易费率、滑点参数、保证金比例、数据路径等系统级配置。
所有数值参数均可在实例化时覆盖默认值。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict


@dataclass
class FeeConfig:
    """交易费用配置"""
    option_commission_per_contract: float = 1.7   # 期权每张手续费（元）
    option_commission_rate: float = 0.0            # 期权按比例手续费（备用）
    etf_commission_rate: float = 0.00006           # ETF 佣金费率（万0.6）
    etf_min_commission: float = 0.1                # ETF 最低佣金（元）
    stamp_tax_rate: float = 0.0                    # 印花税（ETF 免征）
    exercise_fee_per_contract: float = 0.6         # 行权手续费（每张）


@dataclass
class SlippageConfig:
    """滑点配置"""
    option_slippage_ticks: int = 1                 # 期权滑点（最小变动单位数）
    etf_slippage_ticks: int = 1                    # ETF 滑点
    option_tick_size: float = 0.0001               # 期权最小变动价位
    etf_tick_size: float = 0.001                   # ETF 最小变动价位


@dataclass
class MarginConfig:
    """
    上交所期权保证金参数

    卖出开仓保证金 = 权利金 + max(比例1 * 标的价格 - 虚值额, 比例2 * 标的价格或行权价)
    具体比例根据品种和交易所规则设定。
    """
    call_margin_ratio_1: float = 0.12              # 认购保证金比例1
    call_margin_ratio_2: float = 0.07              # 认购保证金比例2
    put_margin_ratio_1: float = 0.12               # 认沽保证金比例1
    put_margin_ratio_2: float = 0.07               # 认沽保证金比例2


@dataclass
class TradingConfig:
    """
    系统主配置

    汇总所有子配置项，提供统一的配置入口。
    """
    # 子配置
    fee: FeeConfig = field(default_factory=FeeConfig)
    slippage: SlippageConfig = field(default_factory=SlippageConfig)
    margin: MarginConfig = field(default_factory=MarginConfig)

    # 市场参数
    risk_free_rate: float = 0.02                   # 无风险利率（年化）
    contract_unit: int = 10000                     # ETF 期权合约单位
    trading_days_per_year: int = 252               # 年交易日数

    # 回测参数
    initial_capital: float = 1_000_000.0           # 初始资金（元）
    max_position_per_signal: int = 10              # 单信号最大开仓组数

    # 数据路径
    data_paths: Dict[str, str] = field(default_factory=lambda: {
        "sample_data": "sample_data",
        "info_data": "info_data",
        "contract_info_csv": "info_data/上交所期权基本信息.csv",
    })

    # Wind 连接（实盘模式）
    wind_enabled: bool = False
    wind_timeout: int = 30                         # Wind API 超时（秒）

    # 信号过滤阈值
    min_profit_threshold: float = 50.0             # 最小净利润阈值（元/组）
    min_volume_threshold: int = 10                 # 最小成交量过滤

    # ETF 模拟器参数
    simulation_volatility: float = 0.20            # 模拟波动率
    simulation_drift: float = 0.03                 # 模拟漂移率


def get_default_config() -> TradingConfig:
    """获取默认配置实例"""
    return TradingConfig()
