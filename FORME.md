# 交易运行程序

9:25 前（开盘前）
  └─ fetch_optionchain               ← 抓取当日期权链（含合约乘数），必须
  └─ recorder --new-window           ← 全天不关，最重要
  └─ process_watcher --new-window    ← 可选，看状态用

9:30 开盘
  └─ monitor --new-window --source zmq   ← 随时开关（zmq 需 recorder 运行；wind 可独立）

15:00 收盘
  └─ recorder 约 15:10 自动合并分片
  └─ process_watcher --merge 可手动触发合并

15:10+
  └─ 依次 Ctrl+C 关闭所有窗口
