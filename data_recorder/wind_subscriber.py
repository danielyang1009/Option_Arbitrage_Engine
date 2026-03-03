# -*- coding: utf-8 -*-
"""
Wind 实时行情订阅器

使用 Wind wsq 的 Push 回调模式（func=callback），而非轮询模式。
Wind 在行情有变动时主动调用 callback，callback 将 tick 放入线程安全队列，
由主线程统一处理（写 Parquet + ZMQ 广播）。

订阅策略：
  - 期权按每批 batch_size 个代码分组订阅（7字段×80代码=560点 < 600限制）
  - ETF 单独订阅（只用3个字段）
  - 启动时通过 ContractInfoManager 获取全量活跃合约

注意：wsq Push 模式下不需要 cancelRequest。
      cancelRequest(0) 只在需要停止所有订阅时调用（如程序退出）。
"""

from __future__ import annotations

import logging
import math
from datetime import date, datetime
from pathlib import Path
from queue import Queue
from typing import Callable, Dict, List, Optional, Tuple

from models import ETFTickData, TickData, normalize_code
from data_engine.contract_info import ContractInfoManager

logger = logging.getLogger(__name__)

# Wind 字段
OPTION_FIELDS = "rt_last,rt_ask1,rt_bid1,rt_oi,rt_vol,rt_high,rt_low"
ETF_FIELDS    = "rt_last,rt_ask1,rt_bid1"

CONTRACT_INFO_CSV = Path(__file__).parent.parent / "info_data" / "上交所期权基本信息.csv"


# ──────────────────────────────────────────────────────────────────────
# Tick 解析工具
# ──────────────────────────────────────────────────────────────────────

def _fval(row: dict, key: str, default: float = math.nan) -> float:
    v = row.get(key)
    if v is None:
        return default
    try:
        f = float(v)
        return default if math.isnan(f) else f
    except (TypeError, ValueError):
        return default


def _ival(row: dict, key: str, default: int = 0) -> int:
    v = row.get(key)
    if v is None:
        return default
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return default


def _parse_indata_row(indata, j: int) -> dict:
    """
    从 wsq 回调的 indata 对象中提取第 j 个代码的所有字段。

    Args:
        indata: Wind wsq 回调数据对象
        j: indata.Codes 中的代码下标

    Returns:
        {FIELD_NAME_UPPER: value} 字典
    """
    row: Dict[str, float] = {}
    for k, fn in enumerate(indata.Fields):
        try:
            row[fn.upper()] = indata.Data[k][j]
        except (IndexError, TypeError):
            row[fn.upper()] = None
    return row


def build_option_tick_row(code: str, underlying: str, row: dict, ts: datetime) -> Optional[dict]:
    """将 Wind 原始字段字典转为 parquet_writer 所需的 dict"""
    last = _fval(row, "RT_LAST", 0.0)
    ask1 = _fval(row, "RT_ASK1")
    bid1 = _fval(row, "RT_BID1")
    if last <= 0 or math.isnan(ask1) or math.isnan(bid1):
        return None
    if ask1 <= 0 or bid1 <= 0 or ask1 < bid1:
        return None
    return {
        "ts":         int(ts.timestamp() * 1000),
        "code":       code,
        "underlying": underlying,
        "last":       float(last),
        "ask1":       float(ask1),
        "bid1":       float(bid1),
        "oi":         _ival(row, "RT_OI"),
        "vol":        _ival(row, "RT_VOL"),
        "high":       float(_fval(row, "RT_HIGH", last)),
        "low":        float(_fval(row, "RT_LOW",  last)),
    }


def build_etf_tick_row(code: str, row: dict, ts: datetime) -> Optional[dict]:
    """将 Wind 原始字段字典转为 ETF parquet_writer 所需的 dict"""
    last = _fval(row, "RT_LAST", 0.0)
    if last <= 0:
        return None
    return {
        "ts":   int(ts.timestamp() * 1000),
        "code": code,
        "last": float(last),
        "ask1": float(_fval(row, "RT_ASK1")),
        "bid1": float(_fval(row, "RT_BID1")),
    }


def option_row_to_tick(code: str, row: dict, ts: datetime) -> Optional[TickData]:
    """将 Wind 原始字段字典转为 TickData（供 ZMQ 广播和策略使用）"""
    last = _fval(row, "RT_LAST", 0.0)
    ask1 = _fval(row, "RT_ASK1")
    bid1 = _fval(row, "RT_BID1")
    if last <= 0 or math.isnan(ask1) or math.isnan(bid1):
        return None
    if ask1 <= 0 or bid1 <= 0 or ask1 < bid1:
        return None
    return TickData(
        timestamp=ts,
        contract_code=code,
        current=float(last),
        volume=_ival(row, "RT_VOL"),
        high=float(_fval(row, "RT_HIGH", last)),
        low=float(_fval(row, "RT_LOW",  last)),
        money=0.0,
        position=_ival(row, "RT_OI"),
        ask_prices=[float(ask1)] + [math.nan] * 4,
        ask_volumes=[100] + [0] * 4,
        bid_prices=[float(bid1)] + [math.nan] * 4,
        bid_volumes=[100] + [0] * 4,
    )


def etf_row_to_tick(code: str, row: dict, ts: datetime) -> Optional[ETFTickData]:
    """将 Wind 原始字段字典转为 ETFTickData"""
    last = _fval(row, "RT_LAST", 0.0)
    if last <= 0:
        return None
    return ETFTickData(
        timestamp=ts,
        etf_code=code,
        price=float(last),
        ask_price=float(_fval(row, "RT_ASK1")),
        bid_price=float(_fval(row, "RT_BID1")),
        is_simulated=False,
    )


# ──────────────────────────────────────────────────────────────────────
# WindSubscriber
# ──────────────────────────────────────────────────────────────────────

class TickPacket:
    """从 Wind 回调线程传递到主线程的 tick 数据包"""
    __slots__ = ("is_etf", "tick_row", "tick_obj", "underlying_code")

    def __init__(self, is_etf: bool, tick_row: dict,
                 tick_obj, underlying_code: str) -> None:
        self.is_etf         = is_etf
        self.tick_row       = tick_row        # 供 ParquetWriter 使用
        self.tick_obj       = tick_obj        # TickData 或 ETFTickData，供 ZMQ 广播
        self.underlying_code = underlying_code


class WindSubscriber:
    """
    Wind 实时行情订阅器（Push 回调模式）

    工作流程：
      1. 从 ContractInfoManager 加载活跃合约，建立 code→underlying 映射
      2. 期权代码按 batch_size 分批，每批注册一个 wsq 回调
      3. ETF 代码单独注册一个 wsq 回调
      4. 每条 tick 被封装为 TickPacket 放入 tick_queue
      5. 主线程从 tick_queue 消费，写 Parquet + ZMQ 广播

    Args:
        products:    要订阅的 ETF 代码列表，如 ['510050.SH', '510300.SH']
        tick_queue:  线程安全队列，TickPacket 投入此队列
        batch_size:  单次 wsq 最大代码数（7字段时建议 ≤80）
        max_expiry_days: 合约到期天数上限（默认 365 = 全部上市合约）
    """

    def __init__(
        self,
        products: List[str],
        tick_queue: Queue,
        batch_size: int = 80,
        max_expiry_days: int = 365,
    ) -> None:
        self._products        = products
        self._queue           = tick_queue
        self._batch_size      = batch_size
        self._max_expiry_days = max_expiry_days

        self._w = None                            # WindPy w 实例
        self._code_to_underlying: Dict[str, str] = {}  # option_code → underlying_code
        self._code_is_adjusted:   set = set()           # 调整型合约代码集合
        self._code_multiplier:    Dict[str, int] = {}   # option_code → 真实合约乘数
        self._option_codes: List[str] = []
        self._etf_codes:    List[str] = list(products)
        self._is_running = False

    # ──────────────────────────────────────────────────────────
    # 公开接口
    # ──────────────────────────────────────────────────────────

    def start(self) -> bool:
        """
        连接 Wind，加载合约，注册 wsq 回调。

        Returns:
            是否成功启动
        """
        # 1. 导入 WindPy
        try:
            from WindPy import w
            self._w = w
        except ImportError:
            logger.error("WindPy 未安装，无法启动订阅器")
            return False

        # 2. 连接 Wind
        result = self._w.start()
        if result.ErrorCode != 0:
            logger.error("Wind 连接失败 ErrorCode=%d", result.ErrorCode)
            return False
        logger.info("Wind 连接成功")

        # 3. 加载合约
        self._load_contracts()

        if not self._option_codes:
            logger.warning("未找到任何活跃期权合约，请检查合约信息文件")

        # 4. 注册 wsq 订阅
        self._subscribe()
        self._is_running = True
        return True

    def stop(self) -> None:
        """取消所有 wsq 订阅并断开 Wind"""
        if self._w is None:
            return
        try:
            self._w.cancelRequest(0)
            self._w.stop()
        except Exception as e:
            logger.warning("Wind 停止异常: %s", e)
        self._is_running = False
        logger.info("Wind 订阅已停止")

    @property
    def option_count(self) -> int:
        return len(self._option_codes)

    @property
    def underlying_map(self) -> Dict[str, str]:
        return dict(self._code_to_underlying)

    # ──────────────────────────────────────────────────────────
    # 内部：加载合约
    # ──────────────────────────────────────────────────────────

    def _load_contracts(self) -> None:
        """从 CSV 加载合约信息，筛选出目标品种的活跃合约"""
        if not CONTRACT_INFO_CSV.exists():
            logger.error("合约信息文件不存在: %s", CONTRACT_INFO_CSV)
            return

        mgr = ContractInfoManager()
        mgr.load_from_csv(CONTRACT_INFO_CSV)

        today = date.today()
        product_set = set(self._products)

        n_adjusted = 0
        for code, info in mgr.contracts.items():
            if info.underlying_code not in product_set:
                continue
            if info.list_date > today or info.expiry_date < today:
                continue
            remaining = (info.expiry_date - today).days
            if remaining > self._max_expiry_days:
                continue
            self._option_codes.append(code)
            self._code_to_underlying[code] = info.underlying_code
            self._code_multiplier[code] = info.contract_unit
            if info.is_adjusted:
                self._code_is_adjusted.add(code)
                n_adjusted += 1

        # 通过 Wind 批量查询真实乘数
        n_mult = mgr.load_multipliers_from_wind(self._option_codes)
        if n_mult > 0:
            for code in self._option_codes:
                info = mgr.contracts.get(code)
                if info:
                    self._code_multiplier[code] = info.contract_unit
            logger.info("已从 Wind 更新 %d 个合约的真实乘数", n_mult)

        logger.info(
            "活跃期权合约: %d 个（品种: %s，到期天数: ≤%d，其中调整型: %d 个）",
            len(self._option_codes), self._products, self._max_expiry_days, n_adjusted,
        )

    # ──────────────────────────────────────────────────────────
    # 内部：注册 wsq 订阅
    # ──────────────────────────────────────────────────────────

    def _subscribe(self) -> None:
        """分批注册期权和 ETF 的 wsq Push 回调"""
        # -- 期权（分批）--
        batches = [
            self._option_codes[i : i + self._batch_size]
            for i in range(0, len(self._option_codes), self._batch_size)
        ]
        for idx, batch in enumerate(batches):
            cb = self._make_option_callback()
            result = self._w.wsq(",".join(batch), OPTION_FIELDS, func=cb)
            if result is None or result.ErrorCode not in (0, None):
                err = getattr(result, "ErrorCode", "unknown")
                logger.warning("期权批次 %d wsq 订阅失败 ErrorCode=%s", idx + 1, err)
            else:
                logger.info(
                    "期权批次 %d/%d 订阅成功 (%d 代码 × 7字段 = %d 数据点)",
                    idx + 1, len(batches), len(batch), len(batch) * 7,
                )

        # -- ETF --
        if self._etf_codes:
            cb_etf = self._make_etf_callback()
            result = self._w.wsq(",".join(self._etf_codes), ETF_FIELDS, func=cb_etf)
            if result is None or result.ErrorCode not in (0, None):
                err = getattr(result, "ErrorCode", "unknown")
                logger.warning("ETF wsq 订阅失败 ErrorCode=%s", err)
            else:
                logger.info("ETF 订阅成功: %s", self._etf_codes)

    # ──────────────────────────────────────────────────────────
    # 内部：回调闭包工厂
    # ──────────────────────────────────────────────────────────

    def _make_option_callback(self) -> Callable:
        """
        生成期权 wsq 回调函数（闭包）。
        Wind 在行情更新时调用此函数（来自 Wind 内部线程）。
        回调只做最轻量的工作：解析 → 入队，不做任何 IO。
        """
        queue        = self._queue
        c2u          = self._code_to_underlying
        adjusted_set = self._code_is_adjusted
        mult_map     = self._code_multiplier

        def callback(indata) -> None:
            if indata.ErrorCode != 0:
                logger.warning("期权 wsq 推送错误 ErrorCode=%d", indata.ErrorCode)
                return
            ts = datetime.now()
            for j, raw_code in enumerate(indata.Codes):
                code = normalize_code(raw_code, ".SH")
                underlying = c2u.get(code, "")
                if not underlying:
                    continue
                row = _parse_indata_row(indata, j)
                tick_row = build_option_tick_row(code, underlying, row, ts)
                tick_obj = option_row_to_tick(code, row, ts)
                if tick_row and tick_obj:
                    tick_row["is_adjusted"] = code in adjusted_set
                    tick_row["multiplier"]  = mult_map.get(code, 10000)
                    pkt = TickPacket(
                        is_etf=False,
                        tick_row=tick_row,
                        tick_obj=tick_obj,
                        underlying_code=underlying,
                    )
                    try:
                        queue.put_nowait(pkt)
                    except Exception:
                        pass

        return callback

    def _make_etf_callback(self) -> Callable:
        """生成 ETF wsq 回调函数（闭包）"""
        queue = self._queue

        def callback(indata) -> None:
            if indata.ErrorCode != 0:
                logger.warning("ETF wsq 推送错误 ErrorCode=%d", indata.ErrorCode)
                return
            ts = datetime.now()
            for j, raw_code in enumerate(indata.Codes):
                code = normalize_code(raw_code, ".SH")
                row = _parse_indata_row(indata, j)
                tick_row = build_etf_tick_row(code, row, ts)
                tick_obj = etf_row_to_tick(code, row, ts)
                if tick_row and tick_obj:
                    pkt = TickPacket(
                        is_etf=True,
                        tick_row=tick_row,
                        tick_obj=tick_obj,
                        underlying_code=code,
                    )
                    try:
                        queue.put_nowait(pkt)
                    except Exception:
                        pass

        return callback
