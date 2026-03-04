#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
输出 50ETF VIX 验算所需全部数据，便于手工验算。

用法:
    python scripts/dump_50etf_vix_data.py
    python scripts/dump_50etf_vix_data.py > vix_50etf_data.txt

依赖: 需先运行 Recorder 产生 snapshot_latest.parquet，或已有当日 optionchain。
"""

from __future__ import annotations

import json
import math
import sys
from datetime import datetime, time
from pathlib import Path

# 项目根目录
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from config.settings import DEFAULT_MARKET_DATA_DIR
from calculators.vix_engine import VIXEngine, VIXResult, MINUTES_PER_YEAR
from calculators.vix_engine import _StrikeQuote, _safe_mid, _to_expiry_dt
from monitors.common import (
    build_pairs_and_codes,
    init_strategy_and_contracts,
    load_active_contracts,
    restore_from_snapshot,
    estimate_etf_fallback_prices,
)
from strategies.pcp_arbitrage import PCPArbitrage


def main() -> int:
    target = "510050.SH"  # 50ETF
    snapshot_dir = DEFAULT_MARKET_DATA_DIR
    expiry_days = 90
    atm_range_pct = 10.0  # VIX 用宽范围
    min_profit = 30.0
    risk_free_rate = 0.02

    etf_prices: dict = {}
    strategy, contract_mgr, active, _, _, etf_codes = init_strategy_and_contracts(
        min_profit=min_profit,
        expiry_days=expiry_days,
        atm_range_pct=atm_range_pct,
        etf_prices=etf_prices,
    )
    n_snap = restore_from_snapshot(strategy, snapshot_dir, etf_prices)
    if n_snap == 0:
        print("警告: 未找到快照，期权报价可能为空。请确保 Recorder 曾运行并写入 snapshot_latest.parquet。")

    vix_pairs, _ = build_pairs_and_codes(
        contract_mgr, active, etf_prices, atm_range_pct=atm_range_pct
    )
    pairs_50 = [(c, p) for c, p in vix_pairs if c.underlying_code == target]
    if not pairs_50:
        print(f"错误: 无 {target} 的 Call/Put 配对")
        return 1

    now = datetime.now()
    engine = VIXEngine(risk_free_rate=risk_free_rate)

    # 收集每个到期日的 strike quotes
    by_expiry: dict = {}
    for call_info, put_info in pairs_50:
        if call_info.expiry_date != put_info.expiry_date or call_info.strike_price != put_info.strike_price:
            continue
        call_tick = strategy.aligner.get_option_quote(call_info.contract_code)
        put_tick = strategy.aligner.get_option_quote(put_info.contract_code)
        call_mid = _safe_mid(call_tick.bid_prices[0], call_tick.ask_prices[0]) if call_tick else None
        put_mid = _safe_mid(put_tick.bid_prices[0], put_tick.ask_prices[0]) if put_tick else None
        expiry_dt = _to_expiry_dt(call_info.expiry_date, time(15, 0))
        by_expiry.setdefault(expiry_dt, []).append(
            _StrikeQuote(strike=call_info.strike_price, call_mid=call_mid, put_mid=put_mid)
        )

    # 取最近到期
    sorted_expiries = sorted(by_expiry.keys())
    if not sorted_expiries:
        print("错误: 无有效到期日")
        return 1
    expiry_dt = sorted_expiries[0]
    quotes = sorted(by_expiry[expiry_dt], key=lambda x: x.strike)

    result = engine.compute_from_strike_quotes(
        expiry=expiry_dt,
        strike_quotes=quotes,
        now=now,
        last_result=None,
        enable_republication=False,
    )

    # 输出验算数据
    out: dict = {
        "meta": {
            "underlying": target,
            "now": now.strftime("%Y-%m-%d %H:%M:%S"),
            "expiry": expiry_dt.strftime("%Y-%m-%d %H:%M"),
            "risk_free_rate": risk_free_rate,
            "minutes_per_year": MINUTES_PER_YEAR,
        },
        "formula": {
            "sigma_sq": "(2/T) * sum(DeltaK/K^2 * exp(RT) * Q(K)) - (1/T) * (F/K0 - 1)^2",
            "vix": "100 * sqrt(sigma_sq)",
        },
    }

    if result is None:
        out["result"] = None
        out["error"] = "VIX 计算失败（可能 K0 无有效报价或数据不足）"
        print(json.dumps(out, ensure_ascii=False, indent=2))
        return 1

    t = engine._calc_t_minutes(now, expiry_dt)
    rt = risk_free_rate * t
    exp_rt = math.exp(rt)
    atm_ref = engine._pick_atm_reference(quotes)
    k_atm, c_atm, p_atm = atm_ref
    forward = k_atm + exp_rt * (c_atm - p_atm)
    k0_candidates = [q.strike for q in quotes if q.strike <= forward]
    k0 = max(k0_candidates) if k0_candidates else None

    # 构建 q_by_k（与 VIXEngine 一致）
    q_by_k: dict = {}
    k0_quote = next((q for q in quotes if q.strike == k0), None)
    if k0_quote and k0_quote.call_mid is not None and k0_quote.put_mid is not None:
        q_by_k[k0] = (k0_quote.call_mid + k0_quote.put_mid) / 2.0
    puts_below = [q for q in quotes if q.strike < k0]
    zero_streak = 0
    for q in sorted(puts_below, key=lambda x: x.strike, reverse=True):
        if q.put_mid is None or q.put_mid <= 0:
            zero_streak += 1
            if zero_streak >= 2:
                break
            continue
        zero_streak = 0
        q_by_k[q.strike] = q.put_mid
    calls_above = [q for q in quotes if q.strike > k0]
    zero_streak = 0
    for q in sorted(calls_above, key=lambda x: x.strike):
        if q.call_mid is None or q.call_mid <= 0:
            zero_streak += 1
            if zero_streak >= 2:
                break
            continue
        zero_streak = 0
        q_by_k[q.strike] = q.call_mid

    strikes = sorted(q_by_k.keys())
    sigma_terms = []
    sigma_sum = 0.0
    for i, k in enumerate(strikes):
        if i == 0:
            delta_k = strikes[1] - strikes[0]
        elif i == len(strikes) - 1:
            delta_k = strikes[-1] - strikes[-2]
        else:
            delta_k = (strikes[i + 1] - strikes[i - 1]) / 2.0
        qk = q_by_k[k]
        term = (delta_k / (k * k)) * exp_rt * qk
        sigma_sum += term
        sigma_terms.append({
            "K": k,
            "DeltaK": delta_k,
            "Q_K": qk,
            "term": term,
        })
    variance = (2.0 / t) * sigma_sum - (1.0 / t) * ((forward / k0 - 1.0) ** 2)
    variance = max(variance, 0.0)
    vix = 100.0 * math.sqrt(variance)

    out["intermediates"] = {
        "T_minutes": (expiry_dt - now).total_seconds() / 60.0,
        "t": t,
        "exp_RT": exp_rt,
        "K_ATM": k_atm,
        "C_ATM": c_atm,
        "P_ATM": p_atm,
        "forward_F": forward,
        "K0": k0,
        "sigma_sum": sigma_sum,
        "variance": variance,
        "vix": vix,
    }
    out["strike_quotes"] = [
        {"strike": q.strike, "call_mid": q.call_mid, "put_mid": q.put_mid}
        for q in quotes
    ]
    out["vix_components"] = sigma_terms
    out["result"] = {"vix": result.vix, "variance": result.variance}

    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
