#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""启动入口（转发到 monitors.monitor）"""
import subprocess
import sys
from pathlib import Path

# --new-window：在新 cmd 窗口重启本脚本（仅 Windows）
if "--new-window" in sys.argv and sys.platform == "win32":
    cmd = [sys.executable, str(Path(__file__).resolve())] + [
        a for a in sys.argv[1:] if a != "--new-window"
    ]
    subprocess.Popen(cmd, creationflags=subprocess.CREATE_NEW_CONSOLE)
    sys.exit(0)

from monitors.monitor import parse_args, run_monitor, run_monitor_zmq

if __name__ == "__main__":
    args = parse_args()
    if args.source == "zmq":
        run_monitor_zmq(
            min_profit=args.min_profit,
            expiry_days=args.expiry_days,
            refresh_secs=args.refresh,
            atm_range_pct=args.atm_range,
            zmq_port=args.zmq_port,
            snapshot_dir=args.snapshot_dir,
        )
    else:
        run_monitor(
            min_profit=args.min_profit,
            expiry_days=args.expiry_days,
            refresh_secs=args.refresh,
            atm_range_pct=args.atm_range,
        )
