# 项目状态交接文档 (STATE.md)

> 最后更新：2026-03-03
> 用于新对话快速恢复上下文，请将本文件内容粘贴给新的 AI 会话。

---

## 一、项目概述

**项目名称**：中国 ETF 期权 PCP 套利引擎
**项目路径**：`d:\Option_Arbitrage_Engine`
**开发语言**：Python 3.10+（环境：`D:\veighna_studio`）
**当前版本**：v0.4（严格 Bid/Ask 公式重构 + 动态乘数 + 调整型合约分区展示）

**四大组件**：

| 组件 | 入口文件 | 状态 |
|------|---------|------|
| 实时数据记录 | `data_recorder/recorder.py` | ✅ 已上线 |
| 终端套利监控 | `term_monitor.py` | ✅ 已上线 |
| 网页套利监控 | `web_monitor.py` | ✅ 已上线 |
| 历史回测 | `main.py` | ✅ 可用（ETF 模拟数据失真） |

---

## 二、目录结构

```
d:\Option_Arbitrage_Engine\
│
├── term_monitor.py          ★ 终端监控（rich，--source wind/zmq）
├── web_monitor.py           ★ 网页监控（Flask，localhost:8080）
├── main.py                    历史回测入口
├── models.py                  全局数据模型
├── requirements.txt           依赖列表
├── README.md                  完整架构文档
├── STATE.md                   本文件
│
├── config/
│   └── settings.py            TradingConfig（含 etf_fee_rate / enable_reverse）+ RecorderConfig
│
├── data_recorder/
│   ├── recorder.py            主进程：队列消费 + Parquet + ZMQ
│   ├── wind_subscriber.py     Wind wsq Push 回调（含乘数查询 + is_adjusted 标记）
│   ├── parquet_writer.py      分片写入（schema 含 is_adjusted + multiplier）
│   └── zmq_publisher.py       ZMQ PUB 广播
│
├── data_engine/
│   ├── contract_info.py       合约管理（load_multipliers_from_wind 动态乘数查询）
│   ├── tick_loader.py         CSV Tick 加载器
│   ├── wind_adapter.py        Wind API 适配器
│   └── etf_simulator.py       ETF 模拟器（回测用）
│
├── strategies/
│   └── pcp_arbitrage.py       PCP 策略（严格 Bid/Ask + 动态 multiplier + calc_detail）
│
├── core/pricing.py            Black-Scholes + IV
├── risk/margin.py             保证金
├── backtest/engine.py         回测引擎
├── analysis/pnl.py            P&L 分析
│
├── info_data/
│   └── 上交所期权基本信息.csv  11,102 条合约
│
└── sample_data/               小样本数据
```

---

## 三、v0.4 核心变更（2026-03-03）

### 3.1 套利公式彻底重构

**旧版**：使用 `etf_price`（最新价）+ 硬编码 `contract_unit=10000` + 估算成本函数
**新版**：严格 Bid/Ask 吃单 + 动态真实乘数 + 简化精确成本

```
正向套利：
  理论单股利润 = K - (S_ask + P_ask - C_bid)
  真实单张净利 = 理论利润 × multiplier - S_ask×mult×0.0002 - 3.0

反向套利：
  理论单股利润 = (S_bid + P_bid - C_ask) - K
  真实单张净利 = 理论利润 × multiplier - S_bid×mult×0.0002 - 3.0
```

涉及文件：`strategies/pcp_arbitrage.py`（删除 `_estimate_costs`，重写 `_evaluate_pair`）

### 3.2 动态合约乘数

- `data_engine/contract_info.py` 新增 `load_multipliers_from_wind(codes)` 方法
- 通过 Wind `wss("contractmultiplier")` 批量查询真实乘数
- 标准合约 = 10000，50ETF 调整型当前 = 10265
- `ContractInfo.contract_unit` 字段存储真实值
- `TradeSignal.multiplier` 传递到显示层

### 3.3 调整型合约处理策略

| 层级 | v0.3 行为 | v0.4 行为 |
|------|----------|----------|
| 数据记录 | 记录全部，标 `is_adjusted` | 记录全部，标 `is_adjusted` + `multiplier` |
| 监控过滤 | 完全过滤掉调整型 | **不再过滤**，分区展示 |
| 显示排序 | 按行权价升序 | 正常合约在前 → 分隔线 → 调整型(A)在后 |

### 3.4 新增配置项（settings.py）

```python
enable_reverse: bool = False          # 反向套利开关（默认关闭）
etf_fee_rate: float = 0.00020        # ETF 单边规费 万2
option_round_trip_fee: float = 3.0   # 期权双边手续费 3元/张
```

### 3.5 TradeSignal 新增字段

```python
multiplier: int = 10000     # 真实乘数
is_adjusted: bool = False   # 是否调整型合约
calc_detail: str = ""       # 人可读盘口公式
```

### 3.6 文件重命名

- `monitor_live.py` → `term_monitor.py`
- `monitor_live.ipynb` 已删除（被 web_monitor.py 替代）

### 3.7 Parquet Schema 扩展

期权和快照 schema 新增 `multiplier(int32)` 列（is_adjusted 此前已有）。

---

## 四、数据资产

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

`info_data/上交所期权基本信息.csv`（UTF-8-BOM，11,102 条）
字段：证券代码、证券简称、起始交易日期、最后交易日期、交割月份、行权价格、期权类型

---

## 五、使用方法

```bash
# 安装依赖
pip install -r requirements.txt

# 数据记录（交易时间全程运行）
python data_recorder/recorder.py

# 终端监控（直连 Wind）
python term_monitor.py --min-profit 50

# 终端监控（ZMQ 模式，需先启动 recorder）
python term_monitor.py --source zmq --min-profit 100

# 网页监控（ZMQ 模式）
python web_monitor.py --min-profit 100

# 历史回测
python main.py --data-dir "D:\TICK_DATA\上交所\华夏上证50ETF期权" --start-date 2024-01 --end-date 2024-01
```

---

## 六、已验证功能

| 功能 | 状态 | 备注 |
|------|------|------|
| 合约信息加载 | ✅ | 11,102 条，含 is_adjusted 自动检测 |
| Wind 乘数查询 | ✅ | 164 个合约全部查询成功 |
| 严格 Bid/Ask 公式 | ✅ | 手算验证 net=-29.18(标准) / net=103.57(调整型) 完全一致 |
| enable_reverse 过滤 | ✅ | False 时反向信号归零 |
| 终端监控 term_monitor | ✅ | Wind 直连 / ZMQ 双模式，分区显示 |
| 网页监控 web_monitor | ✅ | Flask + JS 轮询，暗色主题 |
| 数据记录 recorder | ✅ | Parquet 含 is_adjusted + multiplier |
| ZMQ PUB/SUB | ✅ | 毫秒级广播，SUB 重连不影响 PUB |
| 快照冷启动恢复 | ✅ | snapshot_latest.parquet → TickAligner |
| 终端 UTF-8 编码 | ✅ | ctypes SetConsoleOutputCP(65001) |
| Tick 数据加载 | ✅ | 向量化，104,511 条/1.3秒 |
| Black-Scholes + IV | ✅ | ATM 验证通过 |
| 回测引擎 | ✅ | Tick-by-Tick，T+0/T+1 约束 |

---

## 七、已知问题与待办

### 🔴 高优先级

1. **ETF 回测数据失真**
   - 现象：GBM 模拟价格偏差大，回测亏损
   - 方案：etf_simulator 增加真实 ETF Tick 加载通道

2. **回测引擎重复信号**
   - 现象：同 Tick 时刻相同 Strike 重复
   - 方案：scan_opportunities 加去重

### 🟡 中优先级

3. 月度批量回测模式
4. Greeks 归因完善
5. 回测结果持久化（CSV / JSON）

### 🟢 低优先级

6. 最大盘口价差过滤
7. 声音/弹窗警报
8. 多品种同时回测

---

## 八、关键设计决策

| 决策 | 选择 | 原因 |
|------|------|------|
| 合约乘数获取 | Wind wss 动态查询 | CSV 无乘数字段，Wind contractmultiplier 精确 |
| 套利公式 | 严格 Bid/Ask + 无折现 | 接近到期 PV(K)≈K，吃单价格反映真实执行 |
| 成本模型 | ETF规费万2 + 期权3元 | 简化且保守，避免过拟合 |
| 调整型合约 | 不过滤，分区展示 | 用户需要全面信息，调整型可能有套利机会 |
| 反向套利 | 默认关闭 | 未计融券利息，开启需用户自行评估 |
| 进程间通信 | ZeroMQ PUB/SUB | 轻量解耦，SUB 重连无影响 |
| 数据持久化 | Parquet 30s 分片 | 崩溃安全，列式高压缩 |
| 文件命名 | term_monitor / web_monitor | 明确区分终端版和网页版 |

---

## 九、开发环境

```
OS: Windows 10/11
Python: 3.10+（D:\veighna_studio 环境）
WindPy: x64，C:\Wind\Wind.NET.Client\WindNET\x64\

核心依赖（实测可用）:
  pandas    >= 2.0
  numpy     >= 1.24
  rich      >= 13.0
  pyzmq     26.3.0
  pyarrow   23.0.1
  flask     >= 3.0
```

---

## 十、继续开发建议

```
项目在 d:\Option_Arbitrage_Engine，是中国ETF期权PCP套利引擎（v0.4）。
请先读取 STATE.md 了解全貌，再读取相关源码后开始修改。

当前系统：
- data_recorder/recorder.py：数据记录永续进程（Wind Push → Parquet + ZMQ）
- term_monitor.py：终端监控（rich 表格，--source wind / --source zmq）
- web_monitor.py：网页监控（Flask，localhost:8080）
- main.py：历史回测

v0.4 核心特性：
- 严格 Bid/Ask 吃单公式（不用最新价）
- 动态合约乘数（Wind wss 查询，标准10000/调整型10265等）
- 调整型合约分区展示（正常在前 + 分隔线 + 调整型(A)在后）
- enable_reverse=False 默认关闭反向套利
- TradeSignal 含 multiplier / is_adjusted / calc_detail 字段
```
