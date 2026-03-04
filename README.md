# DeltaZero

ETF 期权 Put-Call Parity（PCP）套利工具集：  
**Web 控制台统一调度 + 实时数据记录 + 终端监控 + 回测分析**。

---

## 0. 超短上手（1分钟）

### 三条命令

```bash
pip install -r requirements.txt
python console.py
python -m backtest --help
```

### 日常流程（最小版）

1. 打开 Wind 并登录  
2. 运行 `python console.py`  
3. 网页中依次执行：抓取期权链 -> 启动 Recorder -> 启动 Monitor（默认 zmq） -> 收盘后合并并关闭全部进程

### 最小目录

```text
d:\DeltaZero
├─ console.py
├─ web/
├─ data_recorder/
├─ monitors/
├─ data_engine/
├─ backtest/
└─ models.py
```

---

## 1. 核心入口

- Web 控制台（推荐日常唯一入口）  
  `python console.py`
- 回测入口  
  `python -m backtest --help`

---

## 2. 你会用到的功能

- **盘前抓取期权链**：`data_engine.fetch_optionchain`
- **实时记录行情**：`data_recorder.recorder`（Wind -> Parquet + ZMQ）
- **正向套利监控**：`monitors.monitor`（`wind` 或 `zmq`）
- **网页统一调度**：`web/dashboard.py`（由 `console.py` 启动）

---

## 3. 项目结构（精简）

```text
d:\DeltaZero
├─ console.py                     # 启动 Web 控制台
├─ models.py                      # 全局数据模型
├─ README.md
├─ STATE.md
├─ requirements.txt
│
├─ web/
│  ├─ dashboard.py                # FastAPI 路由与控制台后端
│  ├─ process_manager.py          # 进程管理逻辑
│  ├─ data_stats.py               # 快照/分片/抓取状态
│  └─ templates/index.html        # 控制台前端
│
├─ data_recorder/
│  ├─ recorder.py                 # 记录主进程
│  ├─ wind_subscriber.py          # Wind 订阅与回调
│  ├─ parquet_writer.py           # 分片/快照/合并
│  └─ zmq_publisher.py            # ZMQ 广播
│
├─ monitors/
│  ├─ monitor.py                  # 终端监控（含 VIX 显示）
│  └─ common.py                   # 共享逻辑
│
├─ data_engine/
│  ├─ fetch_optionchain.py
│  ├─ contract_info.py
│  ├─ tick_loader.py
│  ├─ bar_loader.py
│  └─ etf_simulator.py
│
├─ strategies/pcp_arbitrage.py
├─ calculators/vix_engine.py
├─ backtest/
│  ├─ __main__.py
│  ├─ run.py
│  └─ engine.py
└─ utils/wind_helpers.py          # Wind 公共工具（连接重试/数据转换）
```

---

## 4. 快速开始

### 4.1 安装

```bash
pip install -r requirements.txt
```

### 4.2 启动控制台

```bash
python console.py
```

默认地址：`http://127.0.0.1:8787`

---

## 5. 日常流程（推荐）

1. 打开 Wind 并登录  
2. 启动 `python console.py`  
3. 在控制台中：
   - 先执行“抓取今日期权链”
   - 启动 Recorder
   - 启动 Monitor（默认 `zmq`）
4. 收盘后执行“合并今日分片”
5. 关闭所有进程

---

## 6. 关键命令（CLI 直跑）

### 抓取期权链

```bash
python -m data_engine.fetch_optionchain
python -m data_engine.fetch_optionchain --timeout 90 --retry 2
```

### 启动 Recorder

```bash
python -m data_recorder.recorder
python -m data_recorder.recorder --flush 30 --port 5555
```

### 启动 Monitor

```bash
# 推荐：读 recorder 的 ZMQ
python -m monitors.monitor --source zmq

# 直连 Wind
python -m monitors.monitor --source wind
```

### 启动回测

```bash
python -m backtest --data-dir "D:\TICK_DATA\..."
```

---

## 7. 当前约定与注意事项

- **默认监控标的**：`510050.SH` / `510300.SH` / `510500.SH`
- **默认市场数据目录**：`D:\MARKET_DATA`
- Monitor 默认优先 `zmq` 模式（减少 Wind 连接冲突）
- Wind 模式 Monitor 在控制台侧限制为单实例
- 期权链抓取已包含超时、重试与连接释放（`w.stop()`）

---

## 8. 数据产物

- `metadata/YYYY-MM-DD_optionchain.csv`：当日期权链（含 multiplier）
- `D:\MARKET_DATA\chunks\`：日内 Parquet 分片
- `D:\MARKET_DATA\snapshot_latest.parquet`：最新快照
- `D:\MARKET_DATA\options_YYYYMMDD.parquet` / `etf_YYYYMMDD.parquet`：日合并文件

---

## 9. 文档说明

- `README.md`：面向使用者，讲“怎么跑”
- `STATE.md`：面向开发/AI 接手，讲“当前状态与约束”

