# -*- coding: utf-8 -*-
"""calculators/vectorized_pricer.py — 100% 向量化 Black-76 IV 求解器。

三条金工容错机制（见代码内注释）：
  [GUARD-1] 边界违规布尔掩码：Call C<disc*(F-K)、Put P<disc*(K-F) → 直接 nan，不进 NR
  [GUARD-2] Vega 坍缩双重保护：np.maximum(vega,1e-8) + np.clip(step,-0.5,0.5)
  [GUARD-3] T 毫秒级动态对齐：time.time() Unix 时间戳 + T<=0 拦截（np.maximum(T,1e-6)）
"""
from __future__ import annotations

import math
import time
import numpy as np
from scipy.special import erf as _erf

_SQRT2   = math.sqrt(2.0)
_SQRT2PI = math.sqrt(2.0 * math.pi)

# 年秒数（儒略年）
_SECS_PER_YEAR = 31_557_600.0


def _ncdf(x: np.ndarray) -> np.ndarray:
    return 0.5 * (1.0 + _erf(x / _SQRT2))


def _npdf(x: np.ndarray) -> np.ndarray:
    return np.exp(-0.5 * x * x) / _SQRT2PI


class VectorizedIVCalculator:
    """向量化 Black-76 IV 求解器（Newton-Raphson，防弹版）。"""

    def __init__(self, n_iter: int = 12, tol: float = 5e-5):
        self.n_iter = n_iter
        self.tol    = tol

    # ──────────────────────────────────────────────────────────────
    # [GUARD-3] T 的毫秒级计算
    # ──────────────────────────────────────────────────────────────
    @staticmethod
    def calc_T(expiry_timestamp: float) -> float:
        """
        [GUARD-3] 用 time.time() 计算毫秒精度的年化剩余时间。

        T = (ExpiryTimestamp - time.time()) / 31_557_600
        返回 max(T, 1e-6)：防止 T<=0 导致 sqrt(T) 产生无效值。
        """
        T_raw = (expiry_timestamp - time.time()) / _SECS_PER_YEAR
        return max(T_raw, 1e-6)

    def calc_iv(
        self,
        F: float,
        K_arr: np.ndarray,       # shape (N,) 行权价
        T: float,                # 年化剩余时间（由 calc_T 得到，已保证 > 0）
        r: float,
        price_arr: np.ndarray,   # shape (N,) 市场中间价
        flag_arr: np.ndarray,    # shape (N,) +1=call, -1=put
    ) -> np.ndarray:
        """
        批量求 Black-76 隐含波动率（无 Python for 循环）。

        Returns:
            iv_arr shape (N,)，无效/不收敛 → nan。
        """
        disc   = math.exp(-r * T)
        sqrt_T = math.sqrt(T)
        K_safe = np.where(K_arr > 0, K_arr, 1.0)

        # ── [GUARD-1] 边界违规过滤（布尔掩码） ──────────────────
        intrinsic_call = disc * np.maximum(F - K_safe, 0.0)
        intrinsic_put  = disc * np.maximum(K_safe - F, 0.0)
        intrinsic      = np.where(flag_arr > 0, intrinsic_call, intrinsic_put)

        valid = (
            (price_arr > 0)
            & np.isfinite(price_arr)
            & (K_arr > 0)
            & (price_arr >= intrinsic - 1e-4)   # 1e-4 容许 DDE 报价噪声
        )

        # ── 初始猜测（Brenner-Subrahmanyam ATM 近似） ─────────────
        moneyness = np.abs(np.log(F / K_safe))
        bs_guess  = price_arr / (F * sqrt_T) * math.sqrt(2 * math.pi) * math.exp(r * T)
        sigma = np.where(moneyness < 0.05, np.clip(bs_guess, 0.01, 3.0), 0.3)

        log_FK = np.log(F / K_safe)

        # ── Newton-Raphson 迭代（100% 向量化） ────────────────────
        for _ in range(self.n_iter):
            sig_sqT  = sigma * sqrt_T
            d1       = (log_FK + 0.5 * sigma ** 2 * T) / sig_sqT
            d2       = d1 - sig_sqT
            nd1, nd2 = _ncdf(d1), _ncdf(d2)
            call_th  = disc * (F * nd1 - K_safe * nd2)
            put_th   = disc * (K_safe * (1.0 - nd2) - F * (1.0 - nd1))
            price_th = np.where(flag_arr > 0, call_th, put_th)
            vega_raw = F * disc * _npdf(d1) * sqrt_T

            # ── [GUARD-2] Vega 坍缩双重保护 ──────────────────────
            safe_vega = np.maximum(vega_raw, 1e-8)
            step      = np.clip((price_th - price_arr) / safe_vega, -0.5, 0.5)

            sigma = np.clip(sigma - step, 1e-4, 5.0)

        # ── 收敛验证 ──────────────────────────────────────────────
        sig_sqT  = sigma * sqrt_T
        d1       = (log_FK + 0.5 * sigma ** 2 * T) / sig_sqT
        d2       = d1 - sig_sqT
        nd1, nd2 = _ncdf(d1), _ncdf(d2)
        call_th  = disc * (F * nd1 - K_safe * nd2)
        put_th   = disc * (K_safe * (1.0 - nd2) - F * (1.0 - nd1))
        price_th = np.where(flag_arr > 0, call_th, put_th)
        converged = np.abs(price_th - price_arr) < self.tol

        # [GUARD-1] valid=False 的废合约替换为 nan
        return np.where(valid & converged, sigma, np.nan)

    def calc_greeks(
        self,
        F: float,
        K_arr: np.ndarray,
        T: float,
        r: float,
        sigma_arr: np.ndarray,
        flag_arr: np.ndarray,
    ) -> dict:
        """向量化计算 Delta / Gamma / Vega / Theta（Black-76）。"""
        K_safe  = np.where(K_arr > 0, K_arr, 1.0)
        disc    = math.exp(-r * T)
        sqrt_T  = math.sqrt(T)
        log_FK  = np.log(F / K_safe)
        sig_sqT = sigma_arr * sqrt_T
        # [GUARD-2] Greeks 计算中同样保护分母
        safe_sig_sqT = np.maximum(sig_sqT, 1e-8)
        d1      = (log_FK + 0.5 * sigma_arr ** 2 * T) / safe_sig_sqT
        d2      = d1 - sig_sqT
        nd1, nd2 = _ncdf(d1), _ncdf(d2)
        npd1    = _npdf(d1)
        vega    = F * disc * npd1 * sqrt_T
        delta   = np.where(flag_arr > 0, disc * nd1, disc * (nd1 - 1.0))
        gamma   = disc * npd1 / np.maximum(F * safe_sig_sqT, 1e-8)
        theta_call = (-F * disc * npd1 * sigma_arr / (2 * sqrt_T)
                      + r * disc * (F * nd1 - K_safe * nd2))
        theta_put  = (-F * disc * npd1 * sigma_arr / (2 * sqrt_T)
                      + r * disc * (K_safe * (1.0 - nd2) - F * (1.0 - nd1)))
        theta   = np.where(flag_arr > 0, theta_call, theta_put)
        return {"delta": delta, "gamma": gamma, "vega": vega, "theta": theta}
