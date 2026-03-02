"""
期权定价引擎

实现 Black-Scholes 欧式期权定价模型和隐含波动率求解器。
包含完整的 Greeks（Delta, Gamma, Vega, Theta, Rho）计算。
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from typing import Optional

from scipy.stats import norm

logger = logging.getLogger(__name__)


@dataclass
class GreeksResult:
    """Greeks 计算结果"""
    delta: float
    gamma: float
    vega: float
    theta: float
    rho: float


class BlackScholes:
    """
    Black-Scholes 欧式期权定价模型

    假设标的资产服从几何布朗运动，无股息（ETF 期权可通过调整 S 处理分红）。

    Attributes:
        risk_free_rate: 无风险利率（年化）
    """

    def __init__(self, risk_free_rate: float = 0.02) -> None:
        """
        初始化定价模型

        Args:
            risk_free_rate: 无风险利率（年化连续复利）
        """
        self.risk_free_rate = risk_free_rate

    def call_price(
        self, S: float, K: float, T: float, sigma: float, r: Optional[float] = None,
    ) -> float:
        """
        计算欧式认购期权理论价格

        C = S*N(d1) - K*exp(-rT)*N(d2)

        Args:
            S: 标的价格
            K: 行权价
            T: 距到期时间（年化）
            sigma: 波动率
            r: 无风险利率（不传则使用实例默认值）

        Returns:
            认购期权理论价格
        """
        r = r if r is not None else self.risk_free_rate
        d1, d2 = self._d1_d2(S, K, T, sigma, r)
        return S * norm.cdf(d1) - K * math.exp(-r * T) * norm.cdf(d2)

    def put_price(
        self, S: float, K: float, T: float, sigma: float, r: Optional[float] = None,
    ) -> float:
        """
        计算欧式认沽期权理论价格

        P = K*exp(-rT)*N(-d2) - S*N(-d1)

        Args:
            S: 标的价格
            K: 行权价
            T: 距到期时间（年化）
            sigma: 波动率
            r: 无风险利率

        Returns:
            认沽期权理论价格
        """
        r = r if r is not None else self.risk_free_rate
        d1, d2 = self._d1_d2(S, K, T, sigma, r)
        return K * math.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)

    def greeks(
        self,
        S: float,
        K: float,
        T: float,
        sigma: float,
        is_call: bool = True,
        r: Optional[float] = None,
    ) -> GreeksResult:
        """
        计算全部 Greeks

        Args:
            S: 标的价格
            K: 行权价
            T: 距到期时间（年化）
            sigma: 波动率
            is_call: True 为认购，False 为认沽
            r: 无风险利率

        Returns:
            GreeksResult 包含 delta, gamma, vega, theta, rho
        """
        r = r if r is not None else self.risk_free_rate

        if T <= 0 or sigma <= 0:
            return GreeksResult(
                delta=1.0 if is_call and S > K else (-1.0 if not is_call and S < K else 0.0),
                gamma=0.0, vega=0.0, theta=0.0, rho=0.0,
            )

        d1, d2 = self._d1_d2(S, K, T, sigma, r)
        n_d1 = norm.pdf(d1)
        N_d1 = norm.cdf(d1)
        N_d2 = norm.cdf(d2)
        exp_rT = math.exp(-r * T)
        sqrt_T = math.sqrt(T)

        gamma = n_d1 / (S * sigma * sqrt_T)
        vega = S * n_d1 * sqrt_T / 100.0  # 除100使单位为 1% 波动率变动

        if is_call:
            delta = N_d1
            theta = (
                -S * n_d1 * sigma / (2.0 * sqrt_T)
                - r * K * exp_rT * N_d2
            ) / 365.0  # 每日 theta
            rho = K * T * exp_rT * N_d2 / 100.0
        else:
            delta = N_d1 - 1.0
            theta = (
                -S * n_d1 * sigma / (2.0 * sqrt_T)
                + r * K * exp_rT * norm.cdf(-d2)
            ) / 365.0
            rho = -K * T * exp_rT * norm.cdf(-d2) / 100.0

        return GreeksResult(
            delta=delta, gamma=gamma, vega=vega, theta=theta, rho=rho,
        )

    @staticmethod
    def _d1_d2(
        S: float, K: float, T: float, sigma: float, r: float,
    ) -> tuple[float, float]:
        """计算 d1 和 d2"""
        if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
            return 0.0, 0.0

        d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
        d2 = d1 - sigma * math.sqrt(T)
        return d1, d2


class IVCalculator:
    """
    隐含波动率求解器

    使用 Newton-Raphson 迭代法，从市场价格反推隐含波动率。
    收敛条件：价格误差 < tolerance 或迭代次数超过 max_iter。

    Attributes:
        bs: BlackScholes 定价模型实例
        tolerance: 收敛精度
        max_iter: 最大迭代次数
    """

    def __init__(
        self,
        bs: Optional[BlackScholes] = None,
        tolerance: float = 1e-8,
        max_iter: int = 100,
    ) -> None:
        """
        初始化 IV 求解器

        Args:
            bs: BlackScholes 实例（不传则新建默认实例）
            tolerance: 收敛判定阈值
            max_iter: 最大迭代次数
        """
        self.bs = bs or BlackScholes()
        self.tolerance = tolerance
        self.max_iter = max_iter

    def calc_iv(
        self,
        market_price: float,
        S: float,
        K: float,
        T: float,
        is_call: bool = True,
        r: Optional[float] = None,
        initial_guess: float = 0.25,
    ) -> Optional[float]:
        """
        计算隐含波动率

        Args:
            market_price: 期权市场价格
            S: 标的价格
            K: 行权价
            T: 距到期时间（年化）
            is_call: True 为认购，False 为认沽
            r: 无风险利率
            initial_guess: 初始猜测波动率

        Returns:
            隐含波动率，求解失败返回 None
        """
        r = r if r is not None else self.bs.risk_free_rate

        if T <= 0 or market_price <= 0 or S <= 0 or K <= 0:
            return None

        intrinsic = max(S - K, 0.0) if is_call else max(K * math.exp(-r * T) - S, 0.0)
        if market_price < intrinsic - self.tolerance:
            return None

        sigma = initial_guess

        for iteration in range(self.max_iter):
            if sigma <= 0:
                sigma = 0.001

            if is_call:
                theo_price = self.bs.call_price(S, K, T, sigma, r)
            else:
                theo_price = self.bs.put_price(S, K, T, sigma, r)

            diff = theo_price - market_price

            if abs(diff) < self.tolerance:
                return sigma

            # Vega（未除100的原始单位）
            d1, _ = BlackScholes._d1_d2(S, K, T, sigma, r)
            vega = S * norm.pdf(d1) * math.sqrt(T)

            if abs(vega) < 1e-12:
                logger.debug("Vega 过小，IV 求解终止 (iter=%d)", iteration)
                return None

            sigma = sigma - diff / vega

            if sigma < 0.001:
                sigma = 0.001
            elif sigma > 10.0:
                logger.debug("IV 发散 (sigma=%.4f)，求解终止", sigma)
                return None

        logger.debug("IV 未收敛（%d 次迭代），最终 sigma=%.6f", self.max_iter, sigma)
        return None

    def calc_bid_ask_iv(
        self,
        bid_price: float,
        ask_price: float,
        S: float,
        K: float,
        T: float,
        is_call: bool = True,
        r: Optional[float] = None,
    ) -> tuple[Optional[float], Optional[float], Optional[float]]:
        """
        分别计算 Bid-IV、Ask-IV 和 Mid-IV

        Args:
            bid_price: 买一价
            ask_price: 卖一价
            S, K, T, is_call, r: 同 calc_iv

        Returns:
            (bid_iv, ask_iv, mid_iv) 元组，求解失败的项为 None
        """
        mid_price = (bid_price + ask_price) / 2.0

        bid_iv = self.calc_iv(bid_price, S, K, T, is_call, r) if bid_price > 0 else None
        ask_iv = self.calc_iv(ask_price, S, K, T, is_call, r) if ask_price > 0 else None
        mid_iv = self.calc_iv(mid_price, S, K, T, is_call, r) if mid_price > 0 else None

        return bid_iv, ask_iv, mid_iv
