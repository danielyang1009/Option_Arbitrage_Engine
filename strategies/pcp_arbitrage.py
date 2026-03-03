"""
Put-Call Parity 套利策略

基于认沽认购平价关系检测套利机会，严格区分买卖盘口（Bid/Ask）吃单。

正向套利（Forward / Conversion）：买现货 + 买Put + 卖Call
  理论单股利润 = K - (S_ask + P_ask - C_bid)
  真实单张净利 = 理论单股利润 × multiplier - ETF规费 - 期权双边手续费

反向套利（Reverse / Reversal）：融券卖现货 + 卖Put + 买Call
  理论单股利润 = (S_bid + P_bid - C_ask) - K
  真实单张净利 = 理论单股利润 × multiplier - ETF规费 - 期权双边手续费
  注意：反向套利未计融券利息，默认 enable_reverse=False 关闭。

乘数（multiplier）：标准合约 10000，ETF 分红后调整型合约可能为 10265 等。
现货对冲数量等于 multiplier（不一定是 10000 股）。
"""

from __future__ import annotations

import logging
import math
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from config.settings import TradingConfig
from models import (
    ContractInfo,
    ETFTickData,
    SignalType,
    TickData,
    TradeSignal,
)

logger = logging.getLogger(__name__)


class TickAligner:
    """
    多合约 Tick 流时间对齐器

    维护每个合约的最新报价快照（Last-Known-Value 机制）。
    支持多标的 ETF：按 etf_code 分别存储，避免多品种互相覆盖。

    Attributes:
        latest_option_quotes: 合约代码 -> 最新 TickData
        latest_etf_quotes: ETF代码 -> 最新 ETFTickData（多品种支持）
        latest_etf_quote: 最近更新的 ETF 行情（向后兼容）
    """

    def __init__(self) -> None:
        self.latest_option_quotes: Dict[str, TickData] = {}
        self.latest_etf_quotes: Dict[str, ETFTickData] = {}   # 按标的代码分别存储
        self.latest_etf_quote: Optional[ETFTickData] = None   # 向后兼容：最近更新的 ETF

    def update_option(self, tick: TickData) -> None:
        """更新期权报价快照"""
        self.latest_option_quotes[tick.contract_code] = tick

    def update_etf(self, tick: ETFTickData) -> None:
        """更新 ETF 报价快照（按 etf_code 分别存储）"""
        self.latest_etf_quotes[tick.etf_code] = tick
        self.latest_etf_quote = tick  # 向后兼容

    def get_option_quote(self, code: str) -> Optional[TickData]:
        """获取指定合约的最新报价"""
        return self.latest_option_quotes.get(code)

    def _get_etf_quote(self, underlying_code: Optional[str] = None) -> Optional[ETFTickData]:
        """获取指定（或最近更新的）ETF 行情快照"""
        if underlying_code:
            return self.latest_etf_quotes.get(underlying_code)
        return self.latest_etf_quote

    def get_etf_price(self, underlying_code: Optional[str] = None) -> Optional[float]:
        """获取 ETF 最新价格"""
        quote = self._get_etf_quote(underlying_code)
        return quote.price if quote is not None else None

    def get_etf_ask(self, underlying_code: Optional[str] = None) -> Optional[float]:
        """获取 ETF 卖一价（NaN 时回退到 last）"""
        quote = self._get_etf_quote(underlying_code)
        if quote is None:
            return None
        return quote.ask_price if not math.isnan(quote.ask_price) else quote.price

    def get_etf_bid(self, underlying_code: Optional[str] = None) -> Optional[float]:
        """获取 ETF 买一价（NaN 时回退到 last）"""
        quote = self._get_etf_quote(underlying_code)
        if quote is None:
            return None
        return quote.bid_price if not math.isnan(quote.bid_price) else quote.price

    def reset(self) -> None:
        """清空所有快照"""
        self.latest_option_quotes.clear()
        self.latest_etf_quotes.clear()
        self.latest_etf_quote = None


class PCPArbitrage:
    """
    Put-Call Parity 套利策略

    扫描同行权价的 Call/Put 对，结合实时标的价格检测 PCP 偏离，
    输出标准化的交易信号。

    Attributes:
        config: 交易配置
        aligner: Tick 对齐器
        signal_count: 累计产生的信号数量
    """

    def __init__(self, config: TradingConfig) -> None:
        """
        初始化策略

        Args:
            config: 交易配置（含费率、滑点、阈值等参数）
        """
        self.config = config
        self.aligner = TickAligner()
        self.signal_count: int = 0

    def on_option_tick(self, tick: TickData) -> None:
        """接收期权 Tick 更新"""
        self.aligner.update_option(tick)

    def on_etf_tick(self, tick: ETFTickData) -> None:
        """接收 ETF Tick 更新"""
        self.aligner.update_etf(tick)

    def scan_opportunities(
        self,
        call_put_pairs: List[Tuple[ContractInfo, ContractInfo]],
        current_time: Optional[datetime] = None,
    ) -> List[TradeSignal]:
        """
        扫描 PCP 套利机会

        遍历所有 Call/Put 配对，计算理论价差与实际价差的偏离，
        过滤出满足最低利润阈值的信号。

        Args:
            call_put_pairs: (Call ContractInfo, Put ContractInfo) 配对列表
            current_time: 当前时间（不传则从最新报价推断）

        Returns:
            满足条件的 TradeSignal 列表，按预估利润降序排列
        """
        signals: List[TradeSignal] = []

        for call_info, put_info in call_put_pairs:
            signal = self._evaluate_pair(call_info, put_info, current_time)
            if signal is not None:
                signals.append(signal)

        signals.sort(key=lambda s: s.net_profit_estimate, reverse=True)
        self.signal_count += len(signals)
        return signals

    def _evaluate_pair(
        self,
        call_info: ContractInfo,
        put_info: ContractInfo,
        current_time: Optional[datetime] = None,
    ) -> Optional[TradeSignal]:
        """
        评估单对 Call/Put 的套利机会。

        严格使用 Bid/Ask 吃单价格，动态读取合约真实乘数。
        """
        if (
            call_info.strike_price != put_info.strike_price
            or call_info.expiry_date != put_info.expiry_date
            or call_info.underlying_code != put_info.underlying_code
        ):
            logger.warning(
                "配对校验失败: Call=%s Put=%s (K=%.4f/%.4f, exp=%s/%s, und=%s/%s)",
                call_info.contract_code, put_info.contract_code,
                call_info.strike_price, put_info.strike_price,
                call_info.expiry_date, put_info.expiry_date,
                call_info.underlying_code, put_info.underlying_code,
            )
            return None

        call_tick = self.aligner.get_option_quote(call_info.contract_code)
        put_tick  = self.aligner.get_option_quote(put_info.contract_code)
        underlying = call_info.underlying_code
        etf_price  = self.aligner.get_etf_price(underlying)

        if call_tick is None or put_tick is None or etf_price is None:
            return None

        if current_time is None:
            current_time = max(call_tick.timestamp, put_tick.timestamp)

        C_bid = call_tick.bid_prices[0]
        C_ask = call_tick.ask_prices[0]
        P_bid = put_tick.bid_prices[0]
        P_ask = put_tick.ask_prices[0]

        if any(math.isnan(p) for p in [C_bid, C_ask, P_bid, P_ask]):
            return None
        if any(p <= 0 for p in [C_bid, C_ask, P_bid, P_ask]):
            return None

        K    = call_info.strike_price
        mult = call_info.contract_unit                 # 真实乘数（标准 10000 或调整后）
        T    = call_info.time_to_expiry(current_time.date())
        r    = self.config.risk_free_rate

        _s_ask = self.aligner.get_etf_ask(underlying)
        S_ask = _s_ask if _s_ask is not None else etf_price
        _s_bid = self.aligner.get_etf_bid(underlying)
        S_bid = _s_bid if _s_bid is not None else etf_price

        etf_fee_rate        = self.config.etf_fee_rate
        option_rt_fee       = self.config.option_round_trip_fee
        theoretical_spread  = etf_price - K * math.exp(-r * T)

        # ── 正向套利（Forward）：买现货 + 买Put + 卖Call ─────────
        fwd_per_share  = K - (S_ask + P_ask - C_bid)
        fwd_etf_fee    = S_ask * mult * etf_fee_rate
        fwd_profit     = fwd_per_share * mult - fwd_etf_fee - option_rt_fee
        fwd_detail     = (
            f"K({K:.3g})-S_a({S_ask:.4f})-P_a({P_ask:.4f})+C_b({C_bid:.4f})"
            f"={fwd_per_share:.4f}/股"
        )

        # ── 反向套利（Reverse）：融券卖现货 + 卖Put + 买Call ─────
        rev_per_share  = (S_bid + P_bid - C_ask) - K
        rev_etf_fee    = S_bid * mult * etf_fee_rate
        rev_profit     = rev_per_share * mult - rev_etf_fee - option_rt_fee
        rev_detail     = (
            f"S_b({S_bid:.4f})+P_b({P_bid:.4f})-C_a({C_ask:.4f})-K({K:.3g})"
            f"={rev_per_share:.4f}/股"
        )

        best: Optional[TradeSignal] = None

        if fwd_profit >= self.config.min_profit_threshold:
            best = TradeSignal(
                timestamp=current_time,
                signal_type=SignalType.FORWARD,
                call_code=call_info.contract_code,
                put_code=put_info.contract_code,
                underlying_code=underlying,
                strike=K,
                expiry=call_info.expiry_date,
                call_ask=C_ask, call_bid=C_bid,
                put_ask=P_ask,  put_bid=P_bid,
                spot_price=etf_price,
                theoretical_spread=theoretical_spread,
                actual_spread=C_bid - P_ask,
                net_profit_estimate=fwd_profit,
                confidence=self._calc_confidence(fwd_profit, call_tick, put_tick),
                multiplier=mult,
                is_adjusted=call_info.is_adjusted,
                calc_detail=fwd_detail,
            )

        if self.config.enable_reverse and rev_profit >= self.config.min_profit_threshold:
            if best is None or rev_profit > best.net_profit_estimate:
                best = TradeSignal(
                    timestamp=current_time,
                    signal_type=SignalType.REVERSE,
                    call_code=call_info.contract_code,
                    put_code=put_info.contract_code,
                    underlying_code=underlying,
                    strike=K,
                    expiry=call_info.expiry_date,
                    call_ask=C_ask, call_bid=C_bid,
                    put_ask=P_ask,  put_bid=P_bid,
                    spot_price=etf_price,
                    theoretical_spread=theoretical_spread,
                    actual_spread=P_bid - C_ask,
                    net_profit_estimate=rev_profit,
                    confidence=self._calc_confidence(rev_profit, call_tick, put_tick),
                    multiplier=mult,
                    is_adjusted=call_info.is_adjusted,
                    calc_detail=rev_detail,
                )

        return best

    @staticmethod
    def _calc_confidence(
        profit: float,
        call_tick: TickData,
        put_tick: TickData,
    ) -> float:
        """综合置信度：利润大小 + 盘口价差 + 挂单量"""
        profit_score = min(profit / 500.0, 1.0)

        call_spread = call_tick.spread
        put_spread  = put_tick.spread
        if math.isnan(call_spread) or math.isnan(put_spread):
            spread_score = 0.3
        else:
            avg_spread = (call_spread + put_spread) / 2.0
            spread_score = max(0.0, 1.0 - avg_spread / 0.01)

        min_vol = min(
            call_tick.bid_volumes[0], call_tick.ask_volumes[0],
            put_tick.bid_volumes[0], put_tick.ask_volumes[0],
        )
        volume_score = min(min_vol / 50.0, 1.0)

        return 0.4 * profit_score + 0.3 * spread_score + 0.3 * volume_score
