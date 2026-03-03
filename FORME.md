# 交易运行程序
9:25 前
  └─ recorder --new-window        ← 全天不关，最重要
  └─ process_watcher --new-window ← 可选，看状态用

9:30 开盘
  └─ term_monitor --new-window --source zmq   ← 随时开关
  └─ web_monitor --new-window                 ← 随时开关

15:00 收盘
  └─ 等 recorder 在 15:10 自动合并
     或 process_watcher.py --merge 手动触发

15:10+
  └─ 依次 Ctrl+C 关闭所有窗口