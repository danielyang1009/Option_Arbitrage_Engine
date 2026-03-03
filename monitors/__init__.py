# -*- coding: utf-8 -*-
"""
monitors — DeltaZero 套利监控包

入口：
    python -m monitors.term_monitor   # 终端版
    python -m monitors.web_monitor    # 网页版
"""

from monitors.common import (
    ETF_NAME_MAP,
    ETF_ORDER,
    MONITOR_UNDERLYINGS,
    CONTRACT_INFO_CSV,
    fix_windows_encoding,
    load_active_contracts,
    build_pairs_and_codes,
    restore_from_snapshot,
    parse_zmq_message,
    signal_to_dict,
    init_strategy_and_contracts,
)

__all__ = [
    "ETF_NAME_MAP",
    "ETF_ORDER",
    "MONITOR_UNDERLYINGS",
    "CONTRACT_INFO_CSV",
    "fix_windows_encoding",
    "load_active_contracts",
    "build_pairs_and_codes",
    "restore_from_snapshot",
    "parse_zmq_message",
    "signal_to_dict",
    "init_strategy_and_contracts",
]
