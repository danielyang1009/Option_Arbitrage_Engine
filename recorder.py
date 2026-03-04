#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
启动入口（转发到 data_recorder/recorder.py）

用法:
  python recorder.py                    # 默认参数
  python recorder.py --new-window       # 在新 cmd 窗口启动（仅 Windows）
  python recorder.py --flush 60    # 默认写入 D:\MARKET_DATA
"""
import subprocess
import sys
from pathlib import Path

# --new-window：在新 cmd 窗口启动 data_recorder/recorder.py（仅 Windows）
if "--new-window" in sys.argv and sys.platform == "win32":
    recorder_script = Path(__file__).resolve().parent / "data_recorder" / "recorder.py"
    cmd = [sys.executable, str(recorder_script)] + [
        a for a in sys.argv[1:] if a != "--new-window"
    ]
    subprocess.Popen(cmd, creationflags=subprocess.CREATE_NEW_CONSOLE, cwd=str(Path(__file__).resolve().parent))
    sys.exit(0)

sys.path.insert(0, str(Path(__file__).resolve().parent))
from data_recorder.recorder import main

if __name__ == "__main__":
    main()
