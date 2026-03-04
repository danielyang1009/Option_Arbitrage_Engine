# STATE

最后更新：2026-03-04

## 当前基线

- 根入口仅保留：`console.py`（启动 Web 控制台）
- Web 控制台已拆分为：
  - `web/dashboard.py`（API 装配）
  - `web/process_manager.py`（进程管理）
  - `web/data_stats.py`（快照/分片/抓链状态）
  - `web/templates/index.html`（前端）
- 回测入口在 `backtest/__main__.py`，命令：`python -m backtest`

## 关键运行约束

- 默认标的：`510050.SH` / `510300.SH` / `510500.SH`
- 默认数据目录：`D:\MARKET_DATA`
- Monitor 默认建议 `--source zmq`
- 控制台对 Wind 模式 Monitor 做单实例限制
- `fetch_optionchain` 已有超时重试与 `w.stop()` 清理

## 关键文件

- `models.py`：全局数据模型（被 recorder/monitor/strategy/backtest 共同依赖）
- `strategies/pcp_arbitrage.py`：套利核心逻辑
- `calculators/vix_engine.py`：VIX 计算
- `utils/wind_helpers.py`：Wind 连接与数据转换公共函数

## 当前任务偏好

- 优先通过 Web 控制台完成日常操作，不再依赖根目录旧脚本入口
- 除非用户明确要求，不主动更新 README/STATE

## 待关注点（简）

- 手工检查并删除 `debug-f46e3f.log`（若被本地进程占用）
- 持续观察 Wind 连接稳定性与 wsq 订阅失败重试效果

