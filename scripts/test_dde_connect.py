#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
DDE 连通性诊断脚本 — 在主线程、后台线程两种上下文分别测试 DdeConnect。

运行方式（确保通达信已登录并开启行情）：
    python scripts/test_dde_connect.py
"""
from __future__ import annotations
import ctypes
import ctypes.wintypes as wt
import threading
import time

# ── DDEML 常量 ───────────────────────────────────────────────────────────────
_APPCMD_CLIENTONLY = 0x00000010
_CP_WINUNICODE     = 1200

_DDECALLBACK = ctypes.WINFUNCTYPE(
    ctypes.c_void_p,
    wt.UINT, wt.UINT,
    ctypes.c_void_p, ctypes.c_void_p,
    ctypes.c_void_p, ctypes.c_void_p,
    ctypes.POINTER(wt.ULONG),
    ctypes.POINTER(wt.ULONG),
)

class _MSG(ctypes.Structure):
    _fields_ = [
        ("hwnd",    wt.HWND),
        ("message", wt.UINT),
        ("wParam",  wt.WPARAM),
        ("lParam",  wt.LPARAM),
        ("time",    wt.DWORD),
        ("pt",      wt.POINT),
    ]


def _try_connect(label: str, pre_peek: bool) -> None:
    """在当前线程尝试 DDE 连接，pre_peek=True 时先调用 PeekMessage 初始化消息队列。"""
    u32 = ctypes.windll.user32
    u32.DdeInitializeW.argtypes   = [ctypes.POINTER(wt.DWORD), _DDECALLBACK, wt.DWORD, wt.DWORD]
    u32.DdeInitializeW.restype    = wt.UINT
    u32.DdeCreateStringHandleW.argtypes = [wt.DWORD, wt.LPCWSTR, ctypes.c_int]
    u32.DdeCreateStringHandleW.restype  = ctypes.c_void_p
    u32.DdeFreeStringHandle.argtypes    = [wt.DWORD, ctypes.c_void_p]
    u32.DdeFreeStringHandle.restype     = wt.BOOL
    u32.DdeConnect.argtypes       = [wt.DWORD, ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p]
    u32.DdeConnect.restype        = ctypes.c_void_p
    u32.DdeDisconnect.argtypes    = [ctypes.c_void_p]
    u32.DdeDisconnect.restype     = wt.BOOL
    u32.DdeUninitialize.argtypes  = [wt.DWORD]
    u32.DdeUninitialize.restype   = wt.BOOL
    u32.DdeGetLastError.argtypes  = [wt.DWORD]
    u32.DdeGetLastError.restype   = wt.UINT
    u32.PeekMessageW.argtypes     = [ctypes.POINTER(_MSG), wt.HWND, wt.UINT, wt.UINT, wt.UINT]
    u32.PeekMessageW.restype      = wt.BOOL
    u32.TranslateMessage.argtypes = [ctypes.POINTER(_MSG)]
    u32.TranslateMessage.restype  = wt.BOOL
    u32.DispatchMessageW.argtypes = [ctypes.POINTER(_MSG)]
    u32.DispatchMessageW.restype  = ctypes.c_long

    def _noop_cb(*args):
        return 0

    cb = _DDECALLBACK(_noop_cb)

    if pre_peek:
        msg = _MSG()
        u32.PeekMessageW(ctypes.byref(msg), None, 0, 0, 1)
        print(f"[{label}] PeekMessage 已调用（初始化线程消息队列）")

    idInst = wt.DWORD(0)
    ret = u32.DdeInitializeW(ctypes.byref(idInst), cb, _APPCMD_CLIENTONLY, 0)
    print(f"[{label}] DdeInitializeW: ret={ret}, idInst={idInst.value}")
    if ret != 0:
        print(f"[{label}] DDEML 初始化失败，跳过连接测试")
        return

    def _make_hsz(s: str):
        return u32.DdeCreateStringHandleW(idInst, s, _CP_WINUNICODE)

    def _pump():
        msg = _MSG()
        while u32.PeekMessageW(ctypes.byref(msg), None, 0, 0, 1):
            u32.TranslateMessage(ctypes.byref(msg))
            u32.DispatchMessageW(ctypes.byref(msg))

    # 测试两个 topic
    for service, topic in [("TdxW", "SH510050"), ("TdxW", "SH10009633")]:
        hsz_srv = _make_hsz(service)
        hsz_top = _make_hsz(topic)
        _pump()
        hConv = u32.DdeConnect(idInst, hsz_srv, hsz_top, None)
        _pump()
        err   = u32.DdeGetLastError(idInst)
        if hConv:
            print(f"[{label}] OK DdeConnect({service!r}, {topic!r}) 成功")
            u32.DdeDisconnect(hConv)
        else:
            print(f"[{label}] FAIL DdeConnect({service!r}, {topic!r}) 失败 err=0x{err:04x}")
        u32.DdeFreeStringHandle(idInst, hsz_srv)
        u32.DdeFreeStringHandle(idInst, hsz_top)

    u32.DdeUninitialize(idInst)


def main() -> None:
    print("=" * 60)
    print("DDE 连通性诊断")
    print("=" * 60)

    # 1. 主线程 + 不预先 PeekMessage
    print("\n[场景 1] 主线程，不预 PeekMessage")
    _try_connect("主线程/无peek", pre_peek=False)

    print()

    # 2. 主线程 + 预 PeekMessage
    print("[场景 2] 主线程，预 PeekMessage")
    _try_connect("主线程/预peek", pre_peek=True)

    print()

    # 3. 后台线程 + 不预 PeekMessage
    print("[场景 3] 后台线程，不预 PeekMessage")
    t = threading.Thread(target=_try_connect, args=("后台/无peek", False), daemon=True)
    t.start()
    t.join(timeout=10)

    print()

    # 4. 后台线程 + 预 PeekMessage
    print("[场景 4] 后台线程，预 PeekMessage")
    t = threading.Thread(target=_try_connect, args=("后台/预peek", True), daemon=True)
    t.start()
    t.join(timeout=10)

    print("\n诊断完成。")


if __name__ == "__main__":
    main()
