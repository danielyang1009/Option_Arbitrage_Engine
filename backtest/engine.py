"""
Tick-by-Tick 回测引擎

按时间顺序撮合策略产生的信号，模拟账户资金管理。

核心流程：
1. 合并所有合约的 Tick 流，按时间排序
2. 逐 Tick 推送给策略（on_tick）
3. 接收策略信号，检查保证金后模拟成交
4. 更新账户状态，记录交易明细

约束：
- 期权 T+0：日内可开平仓
- 现货 T+1：买入后次日才能卖出
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Callable, Dict, List, Optional, Tuple

from config.settings import TradingConfig
from models import (
    AccountState,
    AssetType,
    ContractInfo,
    ETFTickData,
    OptionType,
    OrderSide,
    Position,
    SignalType,
    TickData,
    TradeRecord,
    TradeSignal,
)
from risk.margin import MarginCalculator

logger = logging.getLogger(__name__)


@dataclass
class MergedTick:
    """合并后的 Tick 事件，统一期权和 ETF Tick"""
    timestamp: datetime
    tick_type: str              # "option" | "etf"
    option_tick: Optional[TickData] = None
    etf_tick: Optional[ETFTickData] = None


class Account:
    """
    回测账户管理

    跟踪现金、持仓、保证金占用，处理开平仓和盈亏计算。

    Attributes:
        cash: 可用现金
        positions: 合约代码 -> Position 映射
        trade_history: 成交记录列表
    """

    def __init__(self, initial_capital: float, config: TradingConfig) -> None:
        """
        初始化账户

        Args:
            initial_capital: 初始资金
            config: 交易配置
        """
        self.cash = initial_capital
        self.initial_capital = initial_capital
        self.config = config
        self.positions: Dict[str, Position] = {}
        self.trade_history: List[TradeRecord] = []
        self.total_commission: float = 0.0
        self.total_margin: float = 0.0
        self._trade_counter: int = 0
        self._etf_buy_dates: Dict[str, date] = {}  # ETF T+1 约束

    def execute_signal(
        self,
        signal: TradeSignal,
        margin_calculator: MarginCalculator,
        contracts: Dict[str, ContractInfo],
        underlying_close: float,
        num_sets: int = 1,
        signal_id: Optional[int] = None,
    ) -> List[TradeRecord]:
        """
        执行套利信号

        一个正向套利信号包含3条腿：买入 ETF + 买入 Put + 卖出 Call。
        执行前检查资金和保证金是否充足。

        Args:
            signal: 交易信号
            margin_calculator: 保证金计算器
            contracts: 合约信息字典
            underlying_close: 标的前收盘价
            num_sets: 开仓组数

        Returns:
            成交记录列表（成功执行返回3条记录，失败返回空列表）
        """
        unit = self.config.contract_unit
        fee = self.config.fee
        slp = self.config.slippage

        if signal.signal_type == SignalType.FORWARD:
            # 买入 ETF
            etf_exec_price = signal.spot_price + slp.etf_slippage_ticks * slp.etf_tick_size
            etf_quantity = num_sets * unit
            etf_cost = etf_exec_price * etf_quantity
            etf_comm = max(etf_cost * fee.etf_commission_rate, fee.etf_min_commission)

            # 买入 Put
            put_exec_price = signal.put_ask + slp.option_slippage_ticks * slp.option_tick_size
            put_cost = put_exec_price * unit * num_sets
            put_comm = fee.option_commission_per_contract * num_sets

            # 卖出 Call
            call_exec_price = signal.call_bid - slp.option_slippage_ticks * slp.option_tick_size
            call_revenue = call_exec_price * unit * num_sets
            call_comm = fee.option_commission_per_contract * num_sets

            # 卖出 Call 的保证金
            call_info = contracts.get(signal.call_code)
            if call_info is None:
                logger.warning("未找到合约信息: %s", signal.call_code)
                return []

            margin_result = margin_calculator.calc_initial_margin(
                call_info, call_exec_price, underlying_close,
            )
            required_margin = margin_result.initial_margin * num_sets

            total_outflow = etf_cost + put_cost + etf_comm + put_comm + call_comm - call_revenue
            required_cash = total_outflow + required_margin

            if self.cash < required_cash:
                logger.info(
                    "资金不足，需要 %.2f，可用 %.2f（跳过信号）",
                    required_cash, self.cash,
                )
                return []

            records: List[TradeRecord] = []

            # 记录 ETF 买入
            records.append(self._record_trade(
                signal.timestamp, AssetType.ETF, signal.underlying_code,
                OrderSide.BUY, etf_exec_price, etf_quantity, etf_comm,
                slp.etf_slippage_ticks * slp.etf_tick_size * etf_quantity,
                signal_id=signal_id,
            ))
            self._update_position(
                signal.underlying_code, AssetType.ETF,
                OrderSide.BUY, etf_exec_price, etf_quantity,
            )
            self._etf_buy_dates[signal.underlying_code] = signal.timestamp.date()
            self.cash -= (etf_cost + etf_comm)

            # 记录 Put 买入
            records.append(self._record_trade(
                signal.timestamp, AssetType.OPTION, signal.put_code,
                OrderSide.BUY, put_exec_price, num_sets, put_comm,
                slp.option_slippage_ticks * slp.option_tick_size * unit * num_sets,
                signal_id=signal_id,
            ))
            self._update_position(
                signal.put_code, AssetType.OPTION,
                OrderSide.BUY, put_exec_price, num_sets,
            )
            self.cash -= (put_cost + put_comm)

            # 记录 Call 卖出
            records.append(self._record_trade(
                signal.timestamp, AssetType.OPTION, signal.call_code,
                OrderSide.SELL, call_exec_price, num_sets, call_comm,
                slp.option_slippage_ticks * slp.option_tick_size * unit * num_sets,
                signal_id=signal_id,
            ))
            self._update_position(
                signal.call_code, AssetType.OPTION,
                OrderSide.SELL, call_exec_price, num_sets,
            )
            self.cash += (call_revenue - call_comm)

            # 冻结保证金
            pos = self.positions.get(signal.call_code)
            if pos:
                pos.margin_occupied = required_margin
            self.total_margin += required_margin

            logger.info(
                "执行正向套利: Strike=%.4f, Expiry=%s, 组数=%d, 保证金=%.2f",
                signal.strike, signal.expiry, num_sets, required_margin,
            )
            return records

        elif signal.signal_type == SignalType.REVERSE:
            # 反向套利骨架（A股现货做空受限，仅记录不实际执行）
            logger.info(
                "检测到反向套利信号（A股做空受限，仅记录）: Strike=%.4f, 预估利润=%.2f",
                signal.strike, signal.net_profit_estimate,
            )
            return []

        return []

    def get_state(self, timestamp: datetime) -> AccountState:
        """
        获取当前账户状态快照

        Args:
            timestamp: 快照时间

        Returns:
            AccountState 实例
        """
        return AccountState(
            timestamp=timestamp,
            cash=self.cash,
            total_margin=self.total_margin,
            positions=dict(self.positions),
            realized_pnl=sum(p.realized_pnl for p in self.positions.values()),
            unrealized_pnl=0.0,  # 需在外部用市价更新
            total_commission=self.total_commission,
        )

    def update_unrealized_pnl(
        self,
        market_prices: Dict[str, float],
        contract_unit: int,
    ) -> float:
        """
        更新未实现盈亏

        Args:
            market_prices: 合约代码 -> 最新价格
            contract_unit: 合约单位

        Returns:
            总未实现盈亏
        """
        total_unrealized = 0.0
        for code, pos in self.positions.items():
            if pos.quantity == 0:
                continue
            current_price = market_prices.get(code)
            if current_price is None:
                continue

            if pos.asset_type == AssetType.OPTION:
                unrealized = (current_price - pos.avg_cost) * pos.quantity * contract_unit
            else:
                unrealized = (current_price - pos.avg_cost) * pos.quantity

            total_unrealized += unrealized

        return total_unrealized

    def _update_position(
        self,
        code: str,
        asset_type: AssetType,
        side: OrderSide,
        price: float,
        quantity: int,
    ) -> None:
        """更新持仓"""
        if code not in self.positions:
            self.positions[code] = Position(
                contract_code=code, asset_type=asset_type,
            )

        pos = self.positions[code]
        signed_qty = quantity if side == OrderSide.BUY else -quantity

        if (pos.quantity >= 0 and signed_qty > 0) or (pos.quantity <= 0 and signed_qty < 0):
            total_cost = pos.avg_cost * abs(pos.quantity) + price * abs(signed_qty)
            new_qty = pos.quantity + signed_qty
            pos.avg_cost = total_cost / abs(new_qty) if new_qty != 0 else 0.0
            pos.quantity = new_qty
        else:
            close_qty = min(abs(pos.quantity), abs(signed_qty))
            unit = self.config.contract_unit if asset_type == AssetType.OPTION else 1

            if pos.quantity > 0:
                realized = (price - pos.avg_cost) * close_qty * unit
            else:
                realized = (pos.avg_cost - price) * close_qty * unit

            pos.realized_pnl += realized
            remaining = abs(signed_qty) - close_qty
            pos.quantity += signed_qty

            if remaining > 0 and pos.quantity != 0:
                pos.avg_cost = price

    def _record_trade(
        self,
        timestamp: datetime,
        asset_type: AssetType,
        code: str,
        side: OrderSide,
        price: float,
        quantity: int,
        commission: float,
        slippage_cost: float,
        signal_id: Optional[int] = None,
    ) -> TradeRecord:
        """记录成交"""
        self._trade_counter += 1
        self.total_commission += commission

        record = TradeRecord(
            trade_id=self._trade_counter,
            timestamp=timestamp,
            asset_type=asset_type,
            contract_code=code,
            side=side,
            price=price,
            quantity=quantity,
            commission=commission,
            slippage_cost=slippage_cost,
            signal_id=signal_id,
        )
        self.trade_history.append(record)
        return record


class BacktestEngine:
    """
    Tick-by-Tick 回测引擎

    将所有合约的 Tick 流合并排序后逐一推送给策略，
    接收交易信号并通过 Account 执行。

    Attributes:
        config: 交易配置
        account: 回测账户
        signals_generated: 回测中产生的所有信号
        equity_curve: 权益曲线（时间 -> 权益值）
    """

    def __init__(self, config: TradingConfig) -> None:
        """
        初始化回测引擎

        Args:
            config: 交易配置
        """
        self.config = config
        self.account = Account(config.initial_capital, config)
        self.margin_calculator = MarginCalculator(config)
        self.signals_generated: List[TradeSignal] = []
        self.equity_curve: List[Tuple[datetime, float]] = []
        self._price_cache: Dict[str, float] = {}

    def run(
        self,
        option_ticks: Dict[str, List[TickData]],
        etf_ticks: List[ETFTickData],
        contracts: Dict[str, ContractInfo],
        strategy_callback: Callable[
            [MergedTick, "BacktestEngine"],
            List[TradeSignal],
        ],
        underlying_close: Optional[float] = None,
    ) -> Dict:
        """
        执行回测

        Args:
            option_ticks: 合约代码 -> TickData 列表
            etf_ticks: ETF Tick 列表
            contracts: 合约代码 -> ContractInfo
            strategy_callback: 策略回调函数，接收 MergedTick 返回信号列表
            underlying_close: 标的前收盘价（保证金计算用，不传则用首个 ETF 价格）

        Returns:
            回测结果字典，包含交易记录、信号、权益曲线等
        """
        merged = self._merge_tick_streams(option_ticks, etf_ticks)
        logger.info("回测开始：共 %d 个 Tick 事件", len(merged))

        if underlying_close is None and etf_ticks:
            underlying_close = etf_ticks[0].price

        total_signals = 0
        total_trades = 0

        for i, mtick in enumerate(merged):
            signals = strategy_callback(mtick, self)

            for signal in signals:
                sig_idx = len(self.signals_generated)
                self.signals_generated.append(signal)
                total_signals += 1

                num_sets = min(
                    self.config.max_position_per_signal,
                    self._calc_max_sets(signal, underlying_close or 0),
                )
                if num_sets <= 0:
                    continue

                trades = self.account.execute_signal(
                    signal, self.margin_calculator, contracts,
                    underlying_close or 0, num_sets,
                    signal_id=sig_idx,
                )
                total_trades += len(trades)

            if i % 100 == 0 or i == len(merged) - 1:
                market_prices = self._get_latest_prices(mtick)
                unrealized = self.account.update_unrealized_pnl(
                    market_prices, self.config.contract_unit,
                )
                equity = self.account.cash + unrealized
                self.equity_curve.append((mtick.timestamp, equity))

        logger.info(
            "回测完成：%d 个信号，%d 笔成交，最终权益 %.2f",
            total_signals, total_trades,
            self.equity_curve[-1][1] if self.equity_curve else self.account.cash,
        )

        return {
            "trade_history": self.account.trade_history,
            "signals": self.signals_generated,
            "equity_curve": self.equity_curve,
            "final_state": self.account.get_state(
                merged[-1].timestamp if merged else datetime.now(),
            ),
        }

    def _merge_tick_streams(
        self,
        option_ticks: Dict[str, List[TickData]],
        etf_ticks: List[ETFTickData],
    ) -> List[MergedTick]:
        """合并所有 Tick 流并按时间排序"""
        merged: List[MergedTick] = []

        for code, ticks in option_ticks.items():
            for tick in ticks:
                merged.append(MergedTick(
                    timestamp=tick.timestamp,
                    tick_type="option",
                    option_tick=tick,
                ))

        for tick in etf_ticks:
            merged.append(MergedTick(
                timestamp=tick.timestamp,
                tick_type="etf",
                etf_tick=tick,
            ))

        merged.sort(key=lambda m: m.timestamp)
        return merged

    def _calc_max_sets(self, signal: TradeSignal, underlying_close: float) -> int:
        """根据可用资金估算最大开仓组数"""
        unit = self.config.contract_unit
        etf_cost_per_set = signal.spot_price * unit
        margin_per_set = underlying_close * unit * self.config.margin.call_margin_ratio_1
        cost_per_set = etf_cost_per_set + margin_per_set

        if cost_per_set <= 0:
            return 0

        max_sets = int(self.account.cash * 0.8 / cost_per_set)
        return max(0, min(max_sets, self.config.max_position_per_signal))

    def _get_latest_prices(self, mtick: MergedTick) -> Dict[str, float]:
        """逐 Tick 更新价格缓存，返回全量快照供持仓估值使用"""
        if mtick.option_tick is not None:
            self._price_cache[mtick.option_tick.contract_code] = mtick.option_tick.current
        if mtick.etf_tick is not None:
            self._price_cache[mtick.etf_tick.etf_code] = mtick.etf_tick.price
        return self._price_cache
