from __future__ import annotations

import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import psutil

from config.settings import DEFAULT_MARKET_DATA_DIR

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def cmdline_str(proc: psutil.Process) -> str:
    try:
        return " ".join(proc.cmdline())
    except (psutil.AccessDenied, psutil.NoSuchProcess):
        return ""


def find_recorder_processes() -> List[psutil.Process]:
    result = []
    for proc in psutil.process_iter(["pid", "cmdline", "create_time"]):
        cmd = cmdline_str(proc).lower()
        if "recorder" in cmd and "monitor" not in cmd and "console" not in cmd and "dashboard" not in cmd:
            result.append(proc)
    return result


def find_monitor_processes() -> List[psutil.Process]:
    result = []
    for proc in psutil.process_iter(["pid", "cmdline", "create_time"]):
        cmd = cmdline_str(proc).lower()
        if "monitor" in cmd and "console" not in cmd and "dashboard" not in cmd:
            result.append(proc)
    return result


def safe_cmdline(proc: psutil.Process) -> List[str]:
    try:
        return proc.cmdline()
    except (psutil.AccessDenied, psutil.NoSuchProcess):
        return []


def arg_from_cmd(cmdline: List[str], flag: str, default: str = "") -> str:
    for idx, token in enumerate(cmdline):
        if token == flag and idx + 1 < len(cmdline):
            return cmdline[idx + 1]
    return default


def uptime_human(proc: psutil.Process) -> str:
    try:
        sec = int(max(datetime.now().timestamp() - proc.create_time(), 0))
    except Exception:
        return "-"
    h, rem = divmod(sec, 3600)
    m, s = divmod(rem, 60)
    if h > 0:
        return f"{h}h{m:02d}m"
    if m > 0:
        return f"{m}m{s:02d}s"
    return f"{s}s"


def count_wind_monitors() -> int:
    n = 0
    for proc in find_monitor_processes():
        cmd = safe_cmdline(proc)
        if arg_from_cmd(cmd, "--source", "wind").lower() == "wind":
            n += 1
    return n


def process_info(proc: psutil.Process, kind: str) -> Dict[str, Any]:
    cmd = safe_cmdline(proc)
    params = ""
    restart_args: List[str] = []
    if kind == "recorder":
        params = f"port={arg_from_cmd(cmd, '--port', '5555')} flush={arg_from_cmd(cmd, '--flush', '30')}"
    elif kind == "monitor":
        src = arg_from_cmd(cmd, "--source", "wind")
        mp = arg_from_cmd(cmd, "--min-profit", "30")
        ed = arg_from_cmd(cmd, "--expiry-days", "90")
        params = f"source={src} profit≥{mp} expiry≤{ed}d"
        restart_args = [
            "--source",
            src,
            "--min-profit",
            mp,
            "--expiry-days",
            ed,
            "--refresh",
            arg_from_cmd(cmd, "--refresh", "3"),
            "--atm-range",
            arg_from_cmd(cmd, "--atm-range", "0.20"),
        ]
        if src == "zmq":
            restart_args += [
                "--zmq-port",
                arg_from_cmd(cmd, "--zmq-port", "5555"),
                "--snapshot-dir",
                arg_from_cmd(cmd, "--snapshot-dir", DEFAULT_MARKET_DATA_DIR),
            ]
    return {
        "pid": proc.pid,
        "kind": kind,
        "uptime": uptime_human(proc),
        "params": params,
        "restart_args": restart_args,
    }


def spawn_module(module: str, args: List[str]) -> Dict[str, Any]:
    cmd = [sys.executable, "-m", module] + args
    flags = subprocess.CREATE_NEW_CONSOLE if sys.platform == "win32" else 0
    proc = subprocess.Popen(cmd, cwd=str(PROJECT_ROOT), creationflags=flags)
    return {"pid": proc.pid}

