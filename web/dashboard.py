#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""DeltaZero Web 控制台（统一监控 + 调度）。"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict

import psutil
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from config.settings import DEFAULT_MARKET_DATA_DIR
from web.data_stats import (
    chunks_readable,
    count_today_chunks,
    get_fetch_state,
    launch_fetch_task,
    read_snapshot_stats,
    run_merge,
    snapshot_readable,
)
from web.process_manager import (
    arg_from_cmd,
    count_wind_monitors,
    find_monitor_processes,
    find_recorder_processes,
    process_info,
    safe_cmdline,
    spawn_module,
)

TEMPLATE_PATH = Path(__file__).resolve().parent / "templates" / "index.html"

app = FastAPI(title="DeltaZero Web Console", version="0.3.0")


class MonitorStartRequest(BaseModel):
    source: str = Field(default="zmq", pattern="^(wind|zmq)$")
    min_profit: float = 30.0
    expiry_days: int = 90
    refresh: int = 3
    atm_range: float = 0.20
    zmq_port: int = 5555
    snapshot_dir: str = DEFAULT_MARKET_DATA_DIR


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    return TEMPLATE_PATH.read_text(encoding="utf-8")


@app.get("/api/state")
def get_state() -> Dict[str, Any]:
    rec_procs = find_recorder_processes()
    mon_procs = find_monitor_processes()
    all_procs = [process_info(p, "recorder") for p in rec_procs] + [process_info(p, "monitor") for p in mon_procs]
    snapshot_raw = read_snapshot_stats(DEFAULT_MARKET_DATA_DIR)
    chunks_raw = count_today_chunks(DEFAULT_MARKET_DATA_DIR)
    return {
        "server_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "processes": all_procs,
        "recorder_running": len(rec_procs) > 0,
        "recorder_count": len(rec_procs),
        "monitor_count": len(mon_procs),
        "snapshot": snapshot_readable(snapshot_raw),
        "chunks": chunks_readable(chunks_raw),
        "market_data_dir": DEFAULT_MARKET_DATA_DIR,
    }


@app.post("/api/processes/recorder/start")
def start_recorder() -> Dict[str, Any]:
    existing = find_recorder_processes()
    if existing:
        raise HTTPException(status_code=409, detail=f"Recorder 已在运行 (PID {existing[0].pid})，请先关闭")
    return {"ok": True, "started": spawn_module("data_recorder.recorder", [])}


@app.post("/api/processes/monitor/start")
def start_monitor(req: MonitorStartRequest) -> Dict[str, Any]:
    if req.source == "wind" and count_wind_monitors() >= 1:
        raise HTTPException(
            status_code=409,
            detail="Wind 模式仅支持单实例（Wind API 不支持多进程并发连接），请先关闭已有 Monitor 或改用 ZMQ 模式",
        )
    args = [
        "--source",
        req.source,
        "--min-profit",
        str(req.min_profit),
        "--expiry-days",
        str(req.expiry_days),
        "--refresh",
        str(req.refresh),
        "--atm-range",
        str(req.atm_range),
    ]
    if req.source == "zmq":
        args += ["--zmq-port", str(req.zmq_port), "--snapshot-dir", req.snapshot_dir]
    return {"ok": True, "started": spawn_module("monitors.monitor", args)}


@app.post("/api/processes/{pid}/kill")
def kill_process(pid: int) -> Dict[str, Any]:
    try:
        proc = psutil.Process(pid)
    except psutil.NoSuchProcess:
        raise HTTPException(status_code=404, detail="进程不存在")
    try:
        proc.terminate()
        proc.wait(timeout=5)
        return {"ok": True, "pid": pid, "forced": False}
    except psutil.TimeoutExpired:
        proc.kill()
        return {"ok": True, "pid": pid, "forced": True}
    except psutil.AccessDenied:
        raise HTTPException(status_code=403, detail="权限不足")


@app.post("/api/processes/{pid}/reopen")
def reopen_process(pid: int) -> Dict[str, Any]:
    try:
        proc = psutil.Process(pid)
    except psutil.NoSuchProcess:
        raise HTTPException(status_code=404, detail="进程不存在")

    cmd = safe_cmdline(proc)
    joined = " ".join(cmd).lower()

    if "recorder" in joined and "monitor" not in joined:
        module, args = "data_recorder.recorder", []
    elif "monitor" in joined:
        module = "monitors.monitor"
        args = [
            "--source",
            arg_from_cmd(cmd, "--source", "wind"),
            "--min-profit",
            arg_from_cmd(cmd, "--min-profit", "30"),
            "--expiry-days",
            arg_from_cmd(cmd, "--expiry-days", "90"),
            "--refresh",
            arg_from_cmd(cmd, "--refresh", "3"),
            "--atm-range",
            arg_from_cmd(cmd, "--atm-range", "0.20"),
        ]
        if arg_from_cmd(cmd, "--source", "wind") == "zmq":
            args += [
                "--zmq-port",
                arg_from_cmd(cmd, "--zmq-port", "5555"),
                "--snapshot-dir",
                arg_from_cmd(cmd, "--snapshot-dir", DEFAULT_MARKET_DATA_DIR),
            ]
    else:
        raise HTTPException(status_code=400, detail="无法识别进程类型")

    try:
        proc.terminate()
        proc.wait(timeout=5)
    except psutil.TimeoutExpired:
        proc.kill()
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        pass

    started = spawn_module(module, args)
    return {"ok": True, "old_pid": pid, "started": started}


@app.post("/api/processes/kill-all")
def kill_all() -> Dict[str, Any]:
    killed = []
    for proc in find_recorder_processes() + find_monitor_processes():
        try:
            proc.terminate()
            proc.wait(timeout=5)
            killed.append(proc.pid)
        except psutil.TimeoutExpired:
            proc.kill()
            killed.append(proc.pid)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    return {"ok": True, "killed": killed}


@app.post("/api/actions/fetch-optionchain")
def start_fetch_optionchain() -> Dict[str, Any]:
    today_str = date.today().strftime("%Y-%m-%d")
    if not launch_fetch_task(today_str):
        raise HTTPException(status_code=409, detail="期权链抓取正在进行中")
    return {"ok": True, "date": today_str}


@app.get("/api/actions/fetch-status")
def fetch_status() -> Dict[str, Any]:
    return get_fetch_state()


@app.post("/api/actions/merge")
def run_merge_api() -> Dict[str, Any]:
    return run_merge(date.today(), DEFAULT_MARKET_DATA_DIR)


def main() -> None:
    import argparse
    import uvicorn

    parser = argparse.ArgumentParser(description="DeltaZero Web 控制台")
    parser.add_argument("--host", default="127.0.0.1", help="监听地址")
    parser.add_argument("--port", type=int, default=8787, help="监听端口")
    parser.add_argument("--reload", action="store_true", help="开发模式热重载")
    args = parser.parse_args()
    uvicorn.run("web.dashboard:app", host=args.host, port=args.port, reload=args.reload)


if __name__ == "__main__":
    main()
