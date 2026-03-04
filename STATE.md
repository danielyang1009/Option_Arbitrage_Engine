# 项目状态交接文档 (STATE.md)

> 最后更新：2026-03-03
> 用于新对话快速恢复上下文，请将本文件内容粘贴给新的 AI 会话。

---

## 一、项目概述

**项目名称**：DeltaZero
**项目路径**：`d:\DeltaZero`
**开发语言**：Python 3.10+（Anaconda 环境）
**当前版本**：v0.6（process_watcher + --new-window + merge 交易时间过滤）

**五大组件**：

| 组件 | 入口文件 | 实现位置 | 状态 |
|------|---------|---------|------|
| 实时数据记录 | `recorder.py` 或 `data_recorder/recorder.py` | — | ✅ 已上线 |
| 套利监控 | `monitor.py`（启动入口） | `monitors/monitor.py` | ✅ 已上线 |
| 历史回测 | `main.py` | — | ✅ 可用（支持真实 ETF K 线） |
| 进程看门狗 | `process_watcher.py` | — | ✅ 已上线 |

---

## 二、目录结构

```
d:\DeltaZero\
│
├── recorder.py              ★ 数据记录启动入口（转发到 data_recorder/recorder.py）
├── monitor.py               ★ 套利监控启动入口（转发到 monitors.monitor）
├── process_watcher.py       ★ 进程看门狗（recorder/monitor 状态 + --merge 合并分片）
├── main.py                    历史回测（--mode monitor 重定向到 monitors.*）
├── models.py                  全局数据模型
├── requirements.txt
├── README.md
├── STATE.md                   本文件
│
├── monitors/                ★ 监控包（v0.5 重构新增）
│   ├── __init__.py            导出公共符号
│   ├── common.py              共享逻辑（常量/合约加载/ZMQ解析/策略初始化）
│   └── monitor.py             套利监控完整实现（rich，Wind/ZMQ 双模式）
│
├── config/
│   └── settings.py            TradingConfig（含 etf_fee_rate / enable_reverse）+ RecorderConfig
│
├── data_recorder/
│   ├── recorder.py            主进程：队列消费 + Parquet + ZMQ
│   ├── wind_subscriber.py     Wind wsq Push 回调（含乘数查询 + is_adjusted 标记）
│   ├── parquet_writer.py      分片写入（merge 时仅保留交易时间 9:30-11:30、13:00-15:00）
│   └── zmq_publisher.py       ZMQ PUB 广播
│
├── data_engine/
│   ├── contract_info.py       合约管理（load_multipliers_from_optionchain 从当日 optionchain CSV 加载乘数）
│   ├── tick_loader.py         CSV Tick 加载器（向量化）
│   ├── bar_loader.py          K 线数据加载器（CSV/Parquet → ETFTickData，v0.5 新增）
│   ├── wind_adapter.py        Wind API 适配器
│   └── etf_simulator.py       ETF GBM 模拟器（回测兜底）
│
├── strategies/
│   └── pcp_arbitrage.py       PCP 策略（严格 Bid/Ask + 动态 multiplier + calc_detail）
│
├── core/pricing.py            Black-Scholes + IV
├── risk/margin.py             保证金
├── backtest/engine.py         回测引擎（支持真实 ETF K 线混合回测）
├── analysis/pnl.py            P&L 分析
│
├── metadata/
│   ├── YYYY-MM-DD_optionchain.csv  当日期权链（fetch_optionchain 产出）
│   ├── YYYY-MM-DD_optionchain.csv  当日期权链（含 multiplier，fetch_optionchain 产出）
│   └── etf_option_info.md      品种上市时间参考
│
└── sample_data/               小样本数据
```

**已删除**：
- `monitor_common.py`（根目录，已被 `monitors/common.py` 取代）
- `REFACTOR_PLAN.md`（重构蓝图已执行完毕）

---

## 三、v0.6 核心变更（2026-03-03）

### 3.0 新增 process_watcher.py

- 监控 recorder / monitor 运行状态（psutil + snapshot）
- `--merge` 模式：合并 chunks 分片，复用 ParquetWriter.merge_daily()
- `--new-window`：在新 cmd 窗口启动（仅 Windows）
- 依赖：psutil、requests

### 3.1 recorder / monitor 支持 --new-window

- 根目录入口（recorder.py / monitor.py）在解析参数前检测 `--new-window`，用 `subprocess.CREATE_NEW_CONSOLE` 弹窗启动
- `data_recorder/recorder.py` 自身也支持 `--new-window`

### 3.2 parquet_writer merge_daily 交易时间过滤

- 合并时仅保留 9:30-11:30、13:00-15:00 的 tick
- Wind 收盘后仍会推送，recorder 不过滤；merge 时过滤盘前/盘后数据

---

## 四、v0.5 核心变更（2026-03-03，monitors/ 包重构）

### 4.1 新建 `monitors/` 包，消除 10+ 处重复逻辑

**背景**：v0.4 的监控逻辑已统一到 `monitors/monitor.py`，共享逻辑在 `monitors/common.py`。

**做法**：提取共享逻辑到 `monitors/common.py`，两个 monitor 改为从包内导入。

提取内容：

| 函数/常量 | 说明 |
|----------|------|
| `fix_windows_encoding()` | Windows UTF-8 修复（原两处各自实现） |
| `ETF_NAME_MAP`, `ETF_ORDER`, `MONITOR_UNDERLYINGS` | 常量统一 |
| `load_active_contracts()` | 活跃合约筛选（原两版实现不同，已统一） |
| `build_pairs_and_codes()` | Call/Put 配对构建（原两版数据结构不同，已统一） |
| `restore_from_snapshot()` | 快照恢复（原两版 ask_volumes 填充方式不同，已统一为 `[100]+[0]*4`） |
| `parse_zmq_message()` | ZMQ 消息解析（原两版批次大小不同，已统一） |
| `signal_to_dict()` | 信号序列化（移入公共包） |
| `init_strategy_and_contracts()` | 策略初始化完整流程封装 |

**结果**：
- 根目录 `monitor.py` 为轻量启动入口（转发脚本）
- 业务逻辑在 `monitors/monitor.py`，共享逻辑在 `monitors/common.py`

### 4.2 新增 `data_engine/bar_loader.py`

- 将 ETF K 线（CSV/Parquet）转换为 `ETFTickData` 列表，供 `BacktestEngine` 混合频率回测
- 支持 `close` 模式（仅收盘价展开）和 `ohlc` 模式（四价路径模拟）
- `main.py` 新增 `--etf-data-dir` 和 `--bar-mode` 参数，优先使用真实 K 线数据

### 4.3 `main.py` 监控模式重定向

`--mode monitor` 不再包含空壳实现，改为打印提示并引导用户使用 `monitors.*` 包：

```
实盘监控已迁移，请使用独立入口：
  监控: python -m monitors.monitor --source wind
```

---

## 五、v0.4 核心特性（历史记录）

v0.5 在此基础上重构，核心算法不变。

### 套利公式

```
正向套利：
  理论单股利润 = K - (S_ask + P_ask - C_bid)
  真实单张净利 = 理论利润 × multiplier - S_ask×mult×0.0002 - 3.0

反向套利：
  理论单股利润 = (S_bid + P_bid - C_ask) - K
  真实单张净利 = 理论利润 × multiplier - S_bid×mult×0.0002 - 3.0
```

### 动态合约乘数

- `data_engine/contract_info.py` 的 `load_multipliers_from_optionchain(csv_path)` 从当日 `metadata/YYYY-MM-DD_optionchain.csv` 加载乘数（开盘前执行 `python fetch_optionchain.py`）
- 标准合约 = 10000，50ETF 调整型当前 = 10265
- `TradeSignal.multiplier` 传递到显示层

### 调整型合约分区展示

| 层级 | 行为 |
|------|------|
| 数据记录 | 全部记录，含 `is_adjusted` + `multiplier` 字段 |
| 监控过滤 | 不过滤调整型，分区展示 |
| 显示排序 | 正常合约在前 → 分隔线 → 调整型(A)在后 |

### 关键配置（`config/settings.py`）

```python
enable_reverse: bool = False          # 反向套利开关（默认关闭）
etf_fee_rate: float = 0.00020        # ETF 单边规费 万2
option_round_trip_fee: float = 3.0   # 期权双边手续费 3元/张
```

### `TradeSignal` 关键字段

```python
multiplier: int = 10000     # 真实乘数
is_adjusted: bool = False   # 是否调整型合约
calc_detail: str = ""       # 人可读盘口公式，如 K(3.1)-S_a(3.09)+C_b(0.12)-P_a(0.03)=...
```

---

## 六、数据资产

### 实时数据（data_recorder 产生）

```
D:\MARKET_DATA\
├── chunks\                    日内30秒分片
├── snapshot_latest.parquet    最新快照（含 is_adjusted + multiplier）
├── options_YYYYMMDD.parquet   期权日文件
└── etf_YYYYMMDD.parquet       ETF 日文件
```

### 历史 Tick 数据（不在仓库中）

```
D:\TICK_DATA\上交所\
├── 华夏上证50ETF期权/      129 个月度 CSV
├── 华泰柏瑞沪深300ETF期权/  73 个月度 CSV
├── 南方中证500ETF期权/      40 个月度 CSV
├── 科创50期权/              31 个月度 CSV
└── 科创板50期权/            31 个月度 CSV
```

### 合约信息

`metadata/YYYY-MM-DD_optionchain.csv`（fetch_optionchain 产出）
字段：证券代码、证券简称、起始交易日期、最后交易日期、交割月份、行权价格、期权类型

---

## 七、使用方法

```bash
# 安装依赖
pip install -r requirements.txt

# 数据记录（交易时间全程运行）
python recorder.py
python recorder.py --new-window

# 终端监控（直连 Wind，默认）
python monitor.py --min-profit 50

# 终端监控（ZMQ 模式，需先启动 recorder）
python monitor.py --source zmq --min-profit 100

# 网页监控（ZMQ 模式，浏览器访问 http://localhost:8080）

# 进程看门狗（监控 recorder/monitor 状态）
python process_watcher.py --new-window

# 合并今日分片（过滤非交易时间）
python process_watcher.py --merge

# 历史回测（GBM 模拟 ETF）
python main.py --data-dir "D:\TICK_DATA\上交所\华夏上证50ETF期权" --start-date 2024-01 --end-date 2024-01

# 历史回测（真实 ETF K 线，更准确）
python main.py --data-dir "D:\TICK_DATA\上交所\华夏上证50ETF期权" \
               --etf-data-dir "D:\MARKET_DATA\etf_kline" --bar-mode close \
               --start-date 2024-01 --end-date 2024-06
```

---

## 八、已验证功能

| 功能 | 状态 | 备注 |
|------|------|------|
| 合约信息加载 | ✅ | 11,102 条，含 is_adjusted 自动检测 |
| Wind 乘数查询 | ✅ | 164 个合约全部查询成功 |
| 严格 Bid/Ask 公式 | ✅ | 手算验证 net=-29.18(标准) / net=103.57(调整型) 完全一致 |
| enable_reverse 过滤 | ✅ | False 时反向信号归零 |
| monitors/ 包共享逻辑 | ✅ | load_active_contracts / restore_from_snapshot 等已统一 |
| 套利监控 monitor | ✅ | Wind 直连 / ZMQ 双模式，分区显示 |
| 数据记录 recorder | ✅ | Parquet 含 is_adjusted + multiplier |
| ZMQ PUB/SUB | ✅ | 毫秒级广播，SUB 重连不影响 PUB |
| 快照冷启动恢复 | ✅ | snapshot_latest.parquet → TickAligner |
| 终端 UTF-8 编码 | ✅ | ctypes SetConsoleOutputCP(65001) |
| Tick 数据加载 | ✅ | 向量化，104,511 条/1.3秒 |
| K 线数据加载（bar_loader） | ✅ | CSV/Parquet → ETFTickData，close/ohlc 两模式 |
| Black-Scholes + IV | ✅ | ATM 验证通过 |
| 回测引擎 | ✅ | Tick-by-Tick，支持真实 ETF K 线混合输入 |
| 进程看门狗 | ✅ | recorder/monitor 双面板，--merge 合并分片 |
| merge 交易时间过滤 | ✅ | 仅保留 9:30-11:30、13:00-15:00 |
| --new-window | ✅ | monitor / process_watcher 支持 |

---

## 九、已知问题与待办

### 🔴 高优先级

1. **回测引擎价格缓存缺失**（`backtest/engine.py`）
   - 现象：`_get_latest_prices` 只返回当前 Tick 的 1-2 个 code，unrealized P&L 严重低估
   - 方案：维护 `self._price_cache: Dict[str, float]`，逐 Tick 更新

2. **pnl.py 用预估值代替实际 P&L**（`analysis/pnl.py`）
   - 现象：`_calc_signal_pnls` 忽略 trade_history，用 `net_profit_estimate` 计算胜率
   - 方案：用 trade_history 匹配实际成交结果

3. **回测引擎重复信号**
   - 现象：同 Tick 时刻相同 Strike 重复
   - 方案：`scan_opportunities` 加去重

### 🟡 中优先级

4. `signal_count` 双重递增（`pcp_arbitrage.py`）— 移到 `scan_opportunities` 末尾统一计数
5. `S_ask`/`S_bid` 用 `or` 回退存在 0 值陷阱 — 改为显式 `None` 检查
6. 无 Tick 新鲜度检查 — 增加 `max_staleness_secs` 参数
7. 费率体系双轨（`FeeConfig` 3.4元 vs 顶层 `option_round_trip_fee` 3.0元）— 加注释标明各自用途
8. 月度批量回测模式
9. 回测结果持久化（CSV / JSON）

### 🟢 低优先级

10. 最大盘口价差过滤
11. 声音/弹窗警报
12. 多品种同时回测
13. monitor Wind 模式单线程阻塞 — 数据拉取移到独立线程（参考 REFACTOR_PLAN.md Step 5）

---

## 十、关键设计决策

| 决策 | 选择 | 原因 |
|------|------|------|
| 合约乘数获取 | optionchain CSV（fetch_optionchain 产出） | 开盘前 wset optionchain 抓取，含调整型真实乘数 |
| 套利公式 | 严格 Bid/Ask + 无折现 | 接近到期 PV(K)≈K，吃单价格反映真实执行 |
| 成本模型 | ETF规费万2 + 期权3元 | 简化且保守，避免过拟合 |
| 调整型合约 | 不过滤，分区展示 | 用户需要全面信息，调整型可能有套利机会 |
| 反向套利 | 默认关闭 | 未计融券利息，开启需用户自行评估 |
| 进程间通信 | ZeroMQ PUB/SUB | 轻量解耦，SUB 重连无影响 |
| 数据持久化 | Parquet 30s 分片 | 崩溃安全，列式高压缩 |
| 监控代码组织 | monitors/ 包 + 根目录启动入口 | 共享逻辑统一维护，命令行使用习惯不变 |
| ETF 回测数据 | 优先真实 K 线，兜底 GBM | bar_loader 支持 CSV/Parquet，不传则自动 GBM |
| merge 交易时间 | 仅保留 9:30-11:30、13:00-15:00 | Wind 收盘后仍推送，过滤盘前/盘后无效 tick |

---

## 十一、开发环境

```
OS: Windows 10/11
Python: 3.10+（Anaconda 环境）
WindPy: x64，C:\Wind\Wind.NET.Client\WindNET\x64\

核心依赖（实测可用）:
  pandas    >= 2.0
  numpy     >= 1.24
  rich      >= 13.0
  pyzmq     26.3.0
  pyarrow   23.0.1
  psutil    >= 5.9
```

---

## 十二、继续开发建议

```
项目在 d:\DeltaZero，DeltaZero（v0.6）。
请先读取 STATE.md 了解全貌，再读取相关源码后开始修改。

当前系统：
- data_recorder/recorder.py：数据记录永续进程（Wind Push → Parquet + ZMQ）
- monitor.py → monitors/monitor.py：套利监控（rich 表格，--source wind / --source zmq）
- main.py：历史回测（--etf-data-dir 支持真实 ETF K 线）
- monitors/common.py：monitor 的共享逻辑
- process_watcher.py：进程看门狗 + --merge + --new-window

v0.6 核心变更：
- process_watcher.py 新增
- monitor / process_watcher 支持 --new-window
- parquet_writer merge_daily 仅保留交易时间 tick

v0.5 核心变更：
- monitors/ 包重构（共享逻辑统一到 common.py）
- monitors/common.py 统一：load_active_contracts / build_pairs_and_codes /
  restore_from_snapshot / parse_zmq_message / init_strategy_and_contracts 等
- data_engine/bar_loader.py 新增（ETF K 线 → ETFTickData）
- main.py --etf-data-dir 参数支持真实 ETF K 线回测

v0.4 核心特性（不变）：
- 严格 Bid/Ask 吃单公式（不用最新价）
- 动态合约乘数（Wind wss 查询，标准10000/调整型10265等）
- 调整型合约分区展示（正常在前 + 分隔线 + 调整型(A)在后）
- enable_reverse=False 默认关闭反向套利
- TradeSignal 含 multiplier / is_adjusted / calc_detail 字段

下一步重点：
- Step 3：pcp_arbitrage.py 鲁棒性修复（signal_count / S_ask 回退 / Tick 新鲜度）
- Step 7：backtest/engine.py 价格缓存 + pnl.py 实际 P&L 计算
```
