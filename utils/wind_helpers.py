from __future__ import annotations

import math
import time
from datetime import datetime
from typing import Any, Optional

from models import ETFTickData, TickData


def fval(d: dict, key: str, default: float = math.nan) -> float:
    v = d.get(key)
    if v is None:
        return default
    try:
        f = float(v)
        return default if math.isnan(f) else f
    except (TypeError, ValueError):
        return default


def ival(d: dict, key: str, default: int = 0) -> int:
    v = d.get(key)
    if v is None:
        return default
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return default


def wind_row_to_option_tick(code: str, row: dict, ts: datetime) -> Optional[TickData]:
    last = fval(row, "RT_LAST", 0.0)
    ask1 = fval(row, "RT_ASK1")
    bid1 = fval(row, "RT_BID1")
    if last <= 0 or math.isnan(ask1) or math.isnan(bid1):
        return None
    if ask1 <= 0 or bid1 <= 0 or ask1 < bid1:
        return None
    return TickData(
        timestamp=ts,
        contract_code=code,
        current=float(last),
        volume=ival(row, "RT_VOL"),
        high=float(fval(row, "RT_HIGH", last)),
        low=float(fval(row, "RT_LOW", last)),
        money=0.0,
        position=ival(row, "RT_OI"),
        ask_prices=[float(ask1)] + [math.nan] * 4,
        ask_volumes=[100] + [0] * 4,
        bid_prices=[float(bid1)] + [math.nan] * 4,
        bid_volumes=[100] + [0] * 4,
    )


def wind_row_to_etf_tick(code: str, row: dict, ts: datetime) -> Optional[ETFTickData]:
    last = fval(row, "RT_LAST", 0.0)
    if last <= 0:
        return None
    return ETFTickData(
        timestamp=ts,
        etf_code=code,
        price=float(last),
        ask_price=float(fval(row, "RT_ASK1")),
        bid_price=float(fval(row, "RT_BID1")),
        is_simulated=False,
    )


def wind_row_to_option_tick_row(code: str, underlying: str, row: dict, ts: datetime) -> Optional[dict]:
    tick = wind_row_to_option_tick(code, row, ts)
    if tick is None:
        return None
    return {
        "ts": int(ts.timestamp() * 1000),
        "code": code,
        "underlying": underlying,
        "last": float(tick.current),
        "ask1": float(tick.ask_prices[0]),
        "bid1": float(tick.bid_prices[0]),
        "oi": int(tick.position),
        "vol": int(tick.volume),
        "high": float(tick.high),
        "low": float(tick.low),
    }


def wind_row_to_etf_tick_row(code: str, row: dict, ts: datetime) -> Optional[dict]:
    tick = wind_row_to_etf_tick(code, row, ts)
    if tick is None:
        return None
    return {
        "ts": int(ts.timestamp() * 1000),
        "code": code,
        "last": float(tick.price),
        "ask1": float(tick.ask_price),
        "bid1": float(tick.bid_price),
    }


def wind_connect(
    w: Any,
    *,
    timeout: int = 30,
    retries: int = 3,
    delay_secs: float = 2.0,
    logger: Any = None,
) -> bool:
    for attempt in range(1, retries + 1):
        try:
            result = w.start(waitTime=timeout)
            err = getattr(result, "ErrorCode", -1)
            if err == 0:
                return True
            if logger is not None:
                logger.warning("Wind 连接失败 ErrorCode=%s (%d/%d)", err, attempt, retries)
        except Exception as exc:
            if logger is not None:
                logger.warning("Wind 连接异常: %s (%d/%d)", exc, attempt, retries)
        if attempt < retries:
            time.sleep(delay_secs)
    return False

