# -*- coding: utf-8 -*-
"""data_bus/dde_direct_client.py — 纯 Python DDE 直连通达信（pywin32，无需 Excel 运行）。

topic 地址来自调用方传入的 topic_map（从 wxy_*.xlsx 解析而来）；
未在 topic_map 中的合约退回 _code_to_topic() 推算（'510050.SH' → 'SH510050'）。
"""
from __future__ import annotations

import json
import logging
import threading
import time
from typing import Callable, Dict, Optional

logger = logging.getLogger(__name__)

# TdxW DDE 字段映射（通达信标准字段名）
_FIELD_MAP: Dict[str, str] = {
    "last":  "最新价",
    "bid1":  "买一价",
    "ask1":  "卖一价",
    "bidv1": "买一量",
    "askv1": "卖一量",
}

_HEARTBEAT_TOPIC = "SH510050"
_HEARTBEAT_ITEM  = "最新价"


def _code_to_topic(code: str) -> str:
    """
    内部合约代码 → TdxW DDE topic。
    '510050.SH'   → 'SH510050'
    '10004729.SH' → 'SH10004729'
    """
    if "." in code:
        sym, exch = code.rsplit(".", 1)
    else:
        sym, exch = code, "SH"
    return exch.upper() + sym


class TongdaxinDDEClient:
    """
    pywin32 DDE 直连通达信行情（ADVISE 模式）。

    用法：
        client = TongdaxinDDEClient(on_tick=my_callback)
        client.start(codes=["510050.SH", "10004729.SH", ...])
        # ... 运行中 ...
        client.stop()

    on_tick 签名：(code: str, fields: dict, ts_ms: int) -> None
    fields 包含 {"last": float, "bid1": float, ...}（已 float 转换，无效字段不出现）
    """

    def __init__(
        self,
        on_tick: Callable,
        service: str = "TdxW",
        heartbeat_interval: float = 30.0,
    ) -> None:
        self._on_tick       = on_tick
        self._service       = service
        self._hb_interval   = heartbeat_interval
        self._running       = False
        self._pump_thread: Optional[threading.Thread] = None
        self._topic_map: Dict[str, str] = {}   # 由 start() 注入，来自 wxy_*.xlsx
        # 每个合约的 tick 缓冲（code → field → value），攒满后回调
        self._tick_buf: Dict[str, Dict[str, float]] = {}
        self._buf_lock = threading.Lock()

    # ── 公开接口 ──────────────────────────────────────────

    def start(self, codes: list[str], topic_map: Optional[Dict[str, str]] = None) -> None:
        """
        启动消息泵线程。

        Args:
            codes: 合约代码列表（内部 .SH 格式）。
            topic_map: code → DDE topic 映射（从 wxy_*.xlsx 解析而来）。
                       未提供或 code 不在其中时，回退 _code_to_topic() 推算。
        """
        self._topic_map = topic_map or {}
        self._running = True
        self._pump_thread = threading.Thread(
            target=self._message_loop,
            args=(codes,),
            daemon=True,
            name="dde-direct-pump",
        )
        self._pump_thread.start()
        logger.info("TongdaxinDDEClient: 已启动，订阅 %d 个合约", len(codes))

    def stop(self) -> None:
        self._running = False
        if self._pump_thread:
            self._pump_thread.join(timeout=3.0)
        logger.info("TongdaxinDDEClient: 已停止")

    # ── 内部实现 ──────────────────────────────────────────

    def _message_loop(self, codes: list[str]) -> None:
        """Windows 消息泵线程（DDE advise 回调在此触发）。"""
        import pythoncom
        import dde

        convs: Dict[str, object] = {}
        pythoncom.CoInitialize()
        try:
            server = dde.CreateServer()
            server.Create("DeltaZeroDDE")

            self._subscribe_all(server, codes, convs)
            last_hb = time.time()

            while self._running:
                # 泵送 Windows 消息（DDE advise 回调在此触发）
                pythoncom.PumpWaitingMessages()
                time.sleep(0.005)   # 5ms 间隔，平衡 CPU 与延迟

                # ── 心跳 / 重连 ──────────────────────────
                if time.time() - last_hb > self._hb_interval:
                    if not self._heartbeat(convs):
                        logger.warning("DDE 心跳失败，重建连接")
                        self._disconnect_all(convs)
                        convs.clear()
                        self._subscribe_all(server, codes, convs)
                    last_hb = time.time()

        except Exception as e:
            logger.error("DDE message loop 异常: %s", e)
        finally:
            self._disconnect_all(convs)
            try:
                server.Shutdown()
            except Exception:
                pass
            pythoncom.CoUninitialize()

    def _subscribe_all(self, server, codes: list[str], convs: dict) -> None:
        import dde
        for code in codes:
            # 优先使用 wxy_*.xlsx 解析出的 topic，fallback 到推算值
            topic = self._topic_map.get(code) or _code_to_topic(code)
            if topic in convs:
                continue
            try:
                conv = dde.CreateConversation(server)
                conv.ConnectTo(self._service, topic)
                convs[topic] = conv
                # 订阅所有字段（ADVISE 模式：值变化时 TdxW 主动推送）
                for field_key, item_name in _FIELD_MAP.items():
                    conv.Advise(
                        item_name,
                        lambda val, c=code, f=field_key: self._on_advise(c, f, val),
                    )
            except Exception as e:
                logger.warning("DDE 订阅失败 %s (topic=%s): %s", code, topic, e)

    def _on_advise(self, code: str, field: str, raw_val: str) -> None:
        """ADVISE 回调（在消息泵线程中执行，不能阻塞）。"""
        try:
            value = float(str(raw_val).strip())
        except (ValueError, TypeError):
            return
        ts_ms = int(time.time() * 1000)
        with self._buf_lock:
            buf = self._tick_buf.setdefault(code, {})
            buf[field] = value
            # 攒齐三个核心字段后触发回调（last/bid1/ask1）
            if len(buf) >= 3:
                self._on_tick(code, dict(buf), ts_ms)
                buf.clear()

    def _heartbeat(self, convs: dict) -> bool:
        """心跳检测：同步 Request 一个已知字段，失败返回 False。"""
        conv = convs.get(_HEARTBEAT_TOPIC)
        if conv is None:
            return False
        try:
            conv.Request(_HEARTBEAT_ITEM)
            return True
        except Exception:
            return False

    @staticmethod
    def _disconnect_all(convs: dict) -> None:
        for conv in convs.values():
            try:
                conv.Disconnect()
            except Exception:
                pass


def make_zmq_on_tick(pub_socket, etf_codes: set):
    """生成 on_tick 回调，格式化为 OPT_/ETF_ ZMQ 消息，与现有 bus.py 格式兼容。"""
    def on_tick(code: str, fields: dict, ts_ms: int) -> None:
        prefix = "ETF_" if code in etf_codes else "OPT_"
        payload = json.dumps({
            "code": code,
            "type": "etf" if prefix == "ETF_" else "option",
            "last":  fields.get("last"),
            "bid1":  fields.get("bid1"),
            "ask1":  fields.get("ask1"),
            "bidv1": fields.get("bidv1"),
            "askv1": fields.get("askv1"),
            "ts": ts_ms,
        })
        pub_socket.send_string(f"{prefix}{code} {payload}")
    return on_tick
