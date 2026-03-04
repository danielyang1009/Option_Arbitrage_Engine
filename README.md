# DeltaZero

> **Put-Call Parity Arbitrage Engine for Chinese ETF Options**
> 实时数据采集 · 严格 Bid/Ask 套利计算 · 动态合约乘数 · 终端套利监控 · Tick 级回测
> Python 3.10+ | Wind API | ZeroMQ | Parquet

---

## 目录

- [项目概述](#一项目概述)
- [目录结构](#二目录结构)
- [核心模块说明](#三核心模块说明)
- [套利计算方法](#四套利计算方法)
- [调整型合约处理](#五调整型合约处理)
- [模块交互流程](#六模块交互流程)
- [快速开始](#七快速开始)
- [数据记录进程](#八数据记录进程)
- [套利监控](#九套利监控)
- [进程看门狗](#十进程看门狗)
- [回测](#十二回测)
- [配置说明](#十三配置说明)
- [数据说明](#十四数据说明)
- [已知限制](#十五已知限制)

---

## 一、项目概述

本引擎专为**上交所 ETF 期权**的 Put-Call Parity 套利设计，覆盖从数据采集到信号生成的完整链路。

| 功能 | 入口 | 说明 |
|------|------|------|
| **实时数据采集** | `recorder.py` 或 `data_recorder/recorder.py` | Wind Push 回调，全品种全到期月，Parquet 分片 + ZMQ 广播 |
| **套利监控** | `monitor.py` | rich 彩色表格，直连 Wind 或读 ZMQ，分区显示正常/调整型合约 |
| **历史回测** | `main.py` | 本地 Tick 级精确回测，P&L / Sharpe / 权益曲线 |
| **进程看门狗** | `process_watcher.py` | 监控 recorder / monitor 运行状态，支持 `--merge` 合并分片 |

**监控品种**：

| 品种 | ETF 代码 | 说明 |
|------|----------|------|
| 50ETF 期权 | 510050.SH | 华夏上证 50ETF |
| 300ETF 期权 | 510300.SH | 华泰柏瑞沪深 300ETF |
| 500ETF 期权 | 510500.SH | 南方中证 500ETF |

> 科创 50（588000.SH / 588050.SH）不纳入监控和记录。

---

## 二、目录结构

```
d:\DeltaZero\
│
├── recorder.py              ★ 数据记录启动入口（转发到 data_recorder/recorder.py）
├── monitor.py               ★ 套利监控启动入口（转发到 monitors.monitor）
├── process_watcher.py       ★ 进程看门狗（recorder/monitor 状态监控 + --merge 合并分片）
├── fetch_optionchain.py     ★ 开盘前抓取期权链（含 multiplier）→ metadata/YYYY-MM-DD_optionchain.csv
├── main.py                    历史回测入口（--mode monitor 重定向到 monitors.*）
├── models.py                  全局数据模型（TickData / ContractInfo / TradeSignal 等）
├── requirements.txt           依赖列表
├── README.md                  本文件
├── STATE.md                   项目状态文档（AI 上下文恢复用）
│
├── monitors/                ★ 监控包（重构后集中管理）
│   ├── __init__.py            公共符号导出
│   ├── common.py              共享逻辑（常量 / 合约加载 / ZMQ 解析 / 初始化流程）
│   └── monitor.py             套利监控完整实现（rich 表格，Wind / ZMQ 双模式）
│
├── config/
│   └── settings.py            全局配置（TradingConfig + RecorderConfig）
│
├── data_recorder/             ★ 实时数据记录进程
│   ├── recorder.py              主进程：队列消费 + 定时刷新 + 日终合并
│   ├── wind_subscriber.py       Wind wsq Push 回调（分批订阅，含乘数查询）
│   ├── parquet_writer.py        分片写入（30s，崩溃安全）+ 快照 + 日终合并
│   └── zmq_publisher.py         ZeroMQ PUB 广播
│
├── data_engine/
│   ├── tick_loader.py           CSV Tick 加载器（向量化）
│   ├── bar_loader.py            K 线数据加载器（CSV/Parquet → ETFTickData，回测用）
│   ├── contract_info.py         合约信息管理（CSV 加载 + optionchain 乘数）
│   ├── fetch_optionchain.py     开盘前抓取期权链（wset optionchain → metadata/YYYY-MM-DD_optionchain.csv）
│   ├── wind_adapter.py          Wind API 适配器
│   └── etf_simulator.py         ETF 价格模拟器（GBM，回测兜底）
│
├── core/
│   └── pricing.py               Black-Scholes 定价 + IV 求解
│
├── strategies/
│   └── pcp_arbitrage.py         PCP 套利策略（严格 Bid/Ask 公式 + 动态乘数）
│
├── risk/
│   └── margin.py                上交所卖方保证金计算
│
├── backtest/
│   └── engine.py                Tick-by-Tick 回测引擎（支持真实 ETF K 线混合回测）
│
├── analysis/
│   └── pnl.py                   P&L 分析 + 权益曲线
│
├── metadata/
│   ├── YYYY-MM-DD_optionchain.csv  当日期权链（fetch_optionchain 产出，含 multiplier）
│   └── etf_option_info.md        品种上市时间参考
│
└── sample_data/                  小样本数据（功能验证）
```

---

## 三、核心模块说明

### 3.1 `models.py` — 全局数据模型

| 类 | 关键字段 |
|----|---------|
| `TickData` | contract_code, current, bid_prices[5], ask_prices[5], volume, position |
| `ETFTickData` | etf_code, price, bid_price, ask_price |
| `ContractInfo` | contract_code, strike_price, expiry_date, option_type, **contract_unit**, **is_adjusted** |
| `TradeSignal` | signal_type, net_profit_estimate, **multiplier**, **is_adjusted**, **calc_detail** |

### 3.2 `monitors/` — 监控包

| 模块 | 职责 |
|------|------|
| `monitors/common.py` | 共享常量、`load_active_contracts`、`build_pairs_and_codes`、`restore_from_snapshot`、`parse_zmq_message`、`signal_to_dict`、`init_strategy_and_contracts` |
| `monitors/monitor.py` | 终端 UI（rich 表格渲染、Wind 行情拉取、主循环）|

根目录的 `monitor.py` 是**轻量启动入口**，仅做转发，不含业务逻辑。

### 3.3 `config/settings.py` — 配置体系

```
TradingConfig
├── FeeConfig              期权 1.7元/张、ETF 佣金万0.6（仅回测引擎使用）
├── SlippageConfig         期权 1跳(0.0001)、ETF 1跳(0.001)（仅回测引擎使用）
├── MarginConfig           认购/认沽保证金 12%/7%
├── etf_fee_rate           ETF 现货单边规费 万2（默认 0.0002，实时监控使用）
├── option_round_trip_fee  期权双边固定手续费 3.0 元/张（实时监控使用）
├── enable_reverse         反向套利开关（默认 False，未计融券利息）
├── min_profit_threshold   最小净利润阈值 50 元
└── contract_unit          默认合约单位 10000（实际由 Wind 动态查询覆盖）

RecorderConfig
├── products               ['510050.SH', '510300.SH', '510500.SH']
├── output_dir             D:\MARKET_DATA
├── zmq_port               5555
├── flush_interval_secs    30
└── batch_size             80
```

### 3.4 `strategies/pcp_arbitrage.py` — 核心策略引擎

**TickAligner**：多品种报价快照管理器，按 etf_code 分别存储，支持 Bid/Ask 价独立获取。

**PCPArbitrage**：
- 严格区分买卖盘口，**不使用最新价或中间价**
- 动态读取 `call_info.contract_unit` 作为真实乘数
- `enable_reverse=False` 时自动过滤反向信号
- 每个信号输出 `calc_detail` 人可读公式字符串

### 3.5 `data_engine/contract_info.py` — 合约信息管理

- 从 CSV 加载 11,102 条合约，自动检测调整型合约（名称以 A/B 等大写字母结尾）
- `load_multipliers_from_optionchain(csv_path)` 从当日 `metadata/YYYY-MM-DD_optionchain.csv` 加载真实乘数（开盘前需执行 `python fetch_optionchain.py`）
- 标准合约 = 10000，调整型如 50ETF 当前 = 10265

### 3.6 `data_engine/bar_loader.py` — K 线数据加载器

- 将 ETF K 线（1m/5m/日线等）CSV 或 Parquet 文件转换为 `ETFTickData` 列表
- 支持 `close` 模式（仅收盘价）和 `ohlc` 模式（四价路径模拟）
- 供 `main.py --etf-data-dir` 参数使用，替代 GBM 模拟以提升回测精度

### 3.7 `data_recorder/` — 数据记录系统

| 模块 | 职责 |
|------|------|
| `wind_subscriber.py` | wsq Push 回调，分批订阅，记录 is_adjusted + multiplier |
| `parquet_writer.py` | 30s 分片写入，崩溃安全，快照更新，日终合并（**仅保留交易时间 9:30-11:30、13:00-15:00 的 tick**） |
| `zmq_publisher.py` | ZMQ PUB 广播，主题格式 `OPT_510050` / `ETF_510050` |
| `recorder.py` | 主进程编排：队列消费 → 写 Parquet + ZMQ 广播 → 定时刷新 → 日终合并 |

---

## 四、套利计算方法

所有计算严格使用**吃单价格**（taker price），不使用最新价或中间价。

#### 正向套利（Forward / Conversion）

> 动作：以 S_ask 买入现货 + 以 P_ask 买入认沽 + 以 C_bid 卖出认购

```
理论单股利润 = K - (S_ask + P_ask - C_bid)
ETF 现货规费 = S_ask × multiplier × 0.0002
真实单张净利 = 理论单股利润 × multiplier - ETF规费 - 3.0元
```

计算明细输出示例：`K(3.1)-S_a(3.0920)-P_a(0.0300)+C_b(0.1200)=-0.0020/股`

#### 反向套利（Reverse / Reversal）

> 动作：以 S_bid 融券卖现货 + 以 P_bid 卖出认沽 + 以 C_ask 买入认购

```
理论单股利润 = (S_bid + P_bid - C_ask) - K
ETF 现货规费 = S_bid × multiplier × 0.0002
真实单张净利 = 理论单股利润 × multiplier - ETF规费 - 3.0元
```

> **默认 `enable_reverse=False`**，反向套利未计融券利息成本，信号默认不输出。

#### 成本参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `etf_fee_rate` | 0.0002 | ETF 单边规费（含佣金+过户费，约万2） |
| `option_round_trip_fee` | 3.0 元 | 期权双边手续费（卖 Call 约 1.7 + 买 Put 约 1.3） |

#### 操作建议

| 净利润（元/张） | 建议 |
|----------------|------|
| < 100 | 腿差风险高，谨慎 |
| 100 ~ 200 | 可考虑，确认盘口价差不过宽 |
| > 200 | 信号较强，优先操作 |

> 现货对冲数量 = 该合约的 **multiplier**（标准 10000 股，调整型可能是 10265 股等）。

---

## 五、调整型合约处理

ETF 分红后，交易所会对存续期权合约进行调整，产生**调整型合约**：
- 名称以 A、B 等大写字母结尾（如 `50ETF购3月2630A`）
- 行权价非标准（如 2.630 而非 2.600）
- 合约乘数 ≠ 10000（如 10265）

| 层级 | 处理 |
|------|------|
| **数据记录器** | 全部记录，Parquet 中包含 `is_adjusted` + `multiplier` 字段 |
| **合约加载** | `ContractInfo.is_adjusted=True`，`contract_unit` 从当日 optionchain CSV 加载 |
| **套利计算** | 使用真实 multiplier 计算利润，`TradeSignal.is_adjusted` 标记 |
| **监控显示** | 正常合约在前（按行权价升序）→ 分隔线 → 调整型在后（`(A)` 标记，乘数橙色高亮） |

---

## 六、模块交互流程

### 推荐架构：data_recorder + monitor

```
Wind Terminal
    │  wsq Push 回调（~3秒/次）
    ▼
data_recorder/recorder.py          ← 永续进程，交易时间全程运行
    ├── tick_queue（线程安全）
    ├── ParquetWriter
    │    ├── chunks/options_YYYYMMDD_HHMMSS.parquet（每30秒）
    │    ├── snapshot_latest.parquet（含 is_adjusted + multiplier）
    │    └── options_YYYYMMDD.parquet（15:10 日终合并）
    └── ZMQPublisher → tcp://127.0.0.1:5555

monitor.py --source zmq            ← 转发至 monitors/monitor.py，可随时重启
    ├── 启动：读 snapshot → 恢复 TickAligner + 从 optionchain CSV 加载乘数
    ├── 运行：ZMQ SUB → 更新 TickAligner
    └── 每N秒：scan_opportunities → rich 表格（正常合约 + 分隔线 + 调整型）
```

### 独立模式：直连 Wind

```
monitor.py --source wind           ← 不依赖 recorder
    ├── Wind 连接 + 从 optionchain CSV 加载乘数 + wsq 同步拉取
    ├── TickAligner.update_option/etf
    └── 每N秒：scan_opportunities → rich 表格
```

---

## 七、快速开始

### 安装依赖

```bash
pip install -r requirements.txt
```

核心依赖：`pandas` / `numpy` / `rich` / `pyzmq` / `pyarrow` / `psutil`

### WindPy 配置（x64 Python 环境）

```
1. 确认 Wind 终端已安装并登录
2. x64 版 WindPy 路径：C:\Wind\Wind.NET.Client\WindNET\x64\
3. site-packages 中创建 WindPy.pth 指向该路径
4. 测试：python -c "from WindPy import w; print('OK')"
```

---

## 八、数据记录进程

> 交易时间全程运行，**不要关闭**。监控进程从此获取实时数据。

```bash
python recorder.py                                        # 根目录快速调用（默认参数）
python recorder.py --new-window                            # 在新 cmd 窗口启动（仅 Windows）
python recorder.py --flush 60     # 默认写入 D:\MARKET_DATA，可加 --output 指定其他目录

# 或直接调用
python data_recorder/recorder.py --new-window
python data_recorder/recorder.py --port 5556
```

### Parquet Schema

**期权**：`ts(int64), code(str), underlying(str), last/ask1/bid1(float32), oi/vol(int32), high/low(float32), is_adjusted(bool), multiplier(int32)`

**ETF**：`ts(int64), code(str), last/ask1/bid1(float32)`

**快照**：期权 schema + `type(str)`，ETF 行补充 `is_adjusted=False, multiplier=0`

### 存储目录结构

```
D:\MARKET_DATA\
├── chunks\                            日内分片（15:10 合并后删除；Wind 收盘后仍会推送，可能产生 15:11+ 分片）
├── snapshot_latest.parquet            每合约最新一条（策略冷启动用）
├── options_YYYYMMDD.parquet           期权日文件（合并时过滤非交易时间 tick）
└── etf_YYYYMMDD.parquet               ETF 日文件（同上）
```

---

## 九、套利监控

```bash
# 直连 Wind 模式（独立运行，推荐日常使用）
python monitor.py --min-profit 50

# ZMQ 模式（需先启动 recorder）
python monitor.py --source zmq --min-profit 100

# 在新窗口启动（Windows，不占用当前终端）
python monitor.py --new-window --source zmq

# 完整参数
python monitor.py --source wind --min-profit 150 --expiry-days 45 --refresh 3 --atm-range 0.10

# 也可通过包直接调用
python -m monitors.monitor --source zmq
```

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--source` | `wind` | `wind` 直连 / `zmq` 读 recorder |
| `--min-profit` | `30` | 最小显示净利润（元/张） |
| `--expiry-days` | `90` | 最大到期天数 |
| `--refresh` | `5` | 刷新间隔（秒） |
| `--atm-range` | `0.20` | 行权价过滤（±20%） |
| `--zmq-port` | `5555` | ZMQ PUB 端口（ZMQ 模式） |
| `--snapshot-dir` | `D:\MARKET_DATA` | 快照文件目录（ZMQ 模式） |
| `--new-window` | — | 在新 cmd 窗口启动（仅 Windows） |

### 表格列说明

| 列 | 含义 |
|----|------|
| 到期 | 到期日 MM-DD |
| 行权价 | Strike，调整型前缀 `(A)` |
| 方向 | 正向 / 反向 |
| **乘数** | 真实合约乘数（非标准值橙色高亮） |
| C_b / C_a | Call 买一 / 卖一 |
| P_b / P_a | Put 买一 / 卖一 |
| S | ETF 现货价 |
| **净利润** | 扣费后每张净利润（元），**核心决策指标** |
| **明细** | 完整盘口公式，如 `K(3.1)-S_a(3.09)-P_a(0.03)+C_b(0.12)=...` |

### 显示分区

表格内按品种（50ETF / 300ETF / 500ETF）分块，每块内：
1. **正常合约**（行权价升序）
2. 分隔线
3. **调整型合约 (A)**（行权价升序，乘数橙色高亮）

---

## 十、进程看门狗

```bash
python process_watcher.py                              # 监控模式（5秒刷新）
python process_watcher.py --refresh 3                  # 3秒刷新
python process_watcher.py --market-data D:\MARKET_DATA # 指定数据目录
python process_watcher.py --new-window                 # 在新 cmd 窗口启动（仅 Windows）

# 合并今日分片并退出（过滤非交易时间 tick）
python process_watcher.py --merge
python process_watcher.py --merge --date 20260303     # 合并指定日期
```

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--market-data` | `D:\MARKET_DATA` | 数据目录 |
| `--refresh` | `5` | 监控模式刷新间隔（秒） |
| `--merge` | — | 合并分片并退出（不启动 Live 面板） |
| `--date` | 今日 | 合并指定日期，格式 YYYYMMDD（仅与 `--merge` 配合） |
| `--new-window` | — | 在新 cmd 窗口启动（仅 Windows） |

监控模式展示两块面板：Recorder 状态（品种覆盖、快照更新、今日分片）、Monitor 实例列表。

---

## 十一、回测

```bash
# 使用 GBM 模拟 ETF 价格（简单验证）
python main.py --data-dir "D:\TICK_DATA\上交所\华夏上证50ETF期权" \
               --start-date 2024-01 --end-date 2024-01

# 使用真实 ETF K 线数据（推荐，需提供 ETF K 线）
python main.py --data-dir "D:\TICK_DATA\上交所\华夏上证50ETF期权" \
               --etf-data-dir "D:\MARKET_DATA\etf_kline" \
               --bar-mode close \
               --start-date 2024-01 --end-date 2024-06 \
               --output-chart equity.png --min-profit 200
```

| 参数 | 说明 |
|------|------|
| `--data-dir` | 期权 Tick 数据目录 |
| `--etf-data-dir` | ETF K 线数据目录（CSV/Parquet），不传则 GBM 模拟 |
| `--bar-mode` | `close`（仅收盘价）或 `ohlc`（四价路径），默认 `close` |
| `--start-date` | 起始月份，格式 `YYYY-MM` |
| `--end-date` | 结束月份，格式 `YYYY-MM` |
| `--output-chart` | 权益曲线图保存路径 |
| `--min-profit` | 最小利润阈值（元/组） |

> ⚠️ 不传 `--etf-data-dir` 时，ETF 价格使用 GBM 模拟（非真实数据），回测结果仅供参考。

---

## 十二、配置说明

所有参数集中在 `config/settings.py`，可在实例化时覆盖：

```python
from config.settings import get_default_config

config = get_default_config()
config.etf_fee_rate = 0.00015                # 万1.5
config.option_round_trip_fee = 3.4           # 调整手续费
config.min_profit_threshold = 100.0          # 100元以上才输出
config.enable_reverse = True                 # 开启反向套利（需自行计算融券成本）
```

---

## 十三、数据说明

### 实时数据（data_recorder 产生）

每条期权 tick 包含 `is_adjusted`（是否调整型）和 `multiplier`（真实合约乘数），供下游研究分析使用。

### 历史 Tick 数据（不在仓库中）

```
D:\TICK_DATA\上交所\
├── 华夏上证50ETF期权/      129 个月度 CSV（2015-02 ~ 2025-10）
├── 华泰柏瑞沪深300ETF期权/  73 个月度 CSV
├── 南方中证500ETF期权/      40 个月度 CSV
├── 科创50期权/              31 个月度 CSV
└── 科创板50期权/            31 个月度 CSV
```

### 合约信息文件

`metadata/YYYY-MM-DD_optionchain.csv`：当日期权链，开盘前执行 `python fetch_optionchain.py` 生成。
字段：证券代码、证券简称、起始交易日期、最后交易日期、交割月份、行权价格、期权类型。

---

## 十四、已知限制

| 限制 | 说明 |
|------|------|
| ETF 回测数据 | 不传 `--etf-data-dir` 时用 GBM 模拟，结果失真 |
| 三腿非原子执行 | A 股无组合指令，存在腿差风险 |
| 反向套利未计融券利息 | `enable_reverse` 默认关闭 |
| Wind Level 2 权限 | 挂单量默认 100，置信度仅参考 |
| 回测重复信号 | 同 Tick 可能重复，统计偏高（待修复） |
| recorder 崩溃 | 最多丢 30 秒数据，可调小 `--flush` |
| 15:00 后分片 | Wind 收盘后仍推送，recorder 不过滤；merge 时自动过滤非交易时间 |
