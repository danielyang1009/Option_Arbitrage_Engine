#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""启动入口（转发到 monitors.web_monitor）"""
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

sys.path.insert(0, str(Path(__file__).parent))

from monitors.web_monitor import app, _parse_args, _zmq_worker, log
import threading

if __name__ == "__main__":
    args = _parse_args()

    log.info("=" * 60)
    log.info("DeltaZero 套利监控（网页版）启动")
    log.info("  网页地址  : http://localhost:%d", args.port)
    log.info("  ZMQ 端口  : %d", args.zmq_port)
    log.info("=" * 60)

    app.config["REFRESH_SECS"] = args.refresh

    t = threading.Thread(
        target=_zmq_worker,
        args=(
            args.zmq_port, args.snapshot_dir, args.min_profit,
            args.expiry_days, args.atm_range, args.refresh,
        ),
        daemon=True,
        name="zmq-worker",
    )
    t.start()
    app.run(host="0.0.0.0", port=args.port, debug=False, use_reloader=False)
