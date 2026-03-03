# 中国 ETF 期权 PCP 套利引擎

> **Put-Call Parity Arbitrage Engine for Chinese ETF Options**
> 实时数据采集 · 严格 Bid/Ask 套利计算 · 动态合约乘数 · 终端/网页双监控 · Tick 级回测
> Python 3.10+ | Wind API | Flask | ZeroMQ | Parquet

---

## 目录

- [项目概述](#一项目概述)
- [目录结构](#二目录结构)
- [核心模块说明](#三核心模块说明)
- [套利计算方法（v2 严格公式）](#四套利计算方法)
- [调整型合约处理](#五调整型合约处理)
- [模块交互流程](#六模块交互流程)
- [快速开始](#七快速开始)
- [数据记录进程](#八数据记录进程)
- [终端监控 term_monitor](#九终端监控)
- [网页监控 web_monitor](#十网页监控)
- [回测](#十一回测)
- [配置说明](#十二配置说明)
- [数据说明](#十三数据说明)
- [已知限制](#十四已知限制)

---

## 一、项目概述

本引擎专为**上交所 ETF 期权**的 Put-Call Parity 套利设计，覆盖从数据采集到信号生成的完整链路。

| 功能 | 入口 | 说明 |
|------|------|------|
| **实时数据采集** | `data_recorder/recorder.py` | Wind Push 回调，全品种全到期月，Parquet 分片 + ZMQ 广播 |
| **终端套利监控** | `term_monitor.py` | rich 彩色表格，直连 Wind 或读 ZMQ，分区显示正常/调整型合约 |
| **网页套利监控** | `web_monitor.py` | Flask 暗色主题仪表盘，自动刷新，三品种分区 |
| **历史回测** | `main.py --mode backtest` | 本地 Tick 级精确回测，P&L / Sharpe / 权益曲线 |

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
d:\Option_Arbitrage_Engine\
│
├── term_monitor.py          ★ 终端套利监控（rich 表格，--source wind / --source zmq）
├── web_monitor.py           ★ 网页套利监控（Flask，localhost:8080）
├── main.py                    历史回测入口
├── models.py                  全局数据模型（TickData / ContractInfo / TradeSignal 等）
├── requirements.txt           依赖列表
├── README.md                  本文件
├── STATE.md                   项目状态文档（AI 上下文恢复用）
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
│   ├── contract_info.py         合约信息管理（CSV 加载 + Wind 乘数查询）
│   ├── wind_adapter.py          Wind API 适配器
│   └── etf_simulator.py         ETF 价格模拟器（回测用）
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
│   └── engine.py                Tick-by-Tick 回测引擎
│
├── analysis/
│   └── pnl.py                   P&L 分析 + 权益曲线
│
├── info_data/
│   ├── 上交所期权基本信息.csv    11,102 条合约记录
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

### 3.2 `config/settings.py` — 配置体系

```
TradingConfig
├── FeeConfig              期权 1.7元/张、ETF 佣金万0.6
├── SlippageConfig         期权 1跳(0.0001)、ETF 1跳(0.001)
├── MarginConfig           认购/认沽保证金 12%/7%
├── etf_fee_rate           ETF 现货单边规费 万2（默认 0.0002）
├── option_round_trip_fee  期权双边固定手续费 3.0 元/张
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

### 3.3 `strategies/pcp_arbitrage.py` — 核心策略引擎

**TickAligner**：多品种报价快照管理器，按 etf_code 分别存储，支持 Bid/Ask 价独立获取。

**PCPArbitrage**：
- 严格区分买卖盘口，**不使用最新价或中间价**
- 动态读取 `call_info.contract_unit` 作为真实乘数
- `enable_reverse=False` 时自动过滤反向信号
- 每个信号输出 `calc_detail` 人可读公式字符串

### 3.4 `data_engine/contract_info.py` — 合约信息管理

- 从 CSV 加载 11,102 条合约，自动检测调整型合约（名称以 A/B 等大写字母结尾）
- `load_multipliers_from_wind(codes)` 通过 Wind `wss("contractmultiplier")` 批量查询真实乘数
- 标准合约 = 10000，调整型如 50ETF 当前 = 10265

### 3.5 `data_recorder/` — 数据记录系统

| 模块 | 职责 |
|------|------|
| `wind_subscriber.py` | wsq Push 回调，分批订阅，记录 is_adjusted + multiplier |
| `parquet_writer.py` | 30s 分片写入，崩溃安全，快照更新，日终合并 |
| `zmq_publisher.py` | ZMQ PUB 广播，主题格式 `OPT_510050` / `ETF_510050` |
| `recorder.py` | 主进程编排：队列消费 → 写 Parquet + ZMQ 广播 → 定时刷新 → 日终合并 |

---

## 四、套利计算方法

### v2 严格 Bid/Ask 公式（当前版本）

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

### 引擎处理方式

| 层级 | 处理 |
|------|------|
| **数据记录器** | 全部记录，Parquet 中包含 `is_adjusted` + `multiplier` 字段 |
| **合约加载** | `ContractInfo.is_adjusted=True`，`contract_unit` 从 Wind 查询真实值 |
| **套利计算** | 使用真实 multiplier 计算利润，`TradeSignal.is_adjusted` 标记 |
| **监控显示** | 正常合约在前（按行权价升序）→ 分隔线 → 调整型在后（`(A)` 标记，乘数橙色高亮） |

---

## 六、模块交互流程

### 推荐架构：data_recorder + term_monitor / web_monitor

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

term_monitor.py --source zmq       ← 可随时重启
    ├── 启动：读 snapshot → 恢复 TickAligner + Wind 查乘数
    ├── 运行：ZMQ SUB → 更新 TickAligner
    └── 每N秒：scan_opportunities → rich 表格（正常合约 + 分隔线 + 调整型）

web_monitor.py                      ← 可随时重启
    ├── 后台线程：ZMQ SUB → TickAligner → scan → 共享 state
    └── Flask：/api/signals → JS 前端轮询渲染
```

### 独立模式：直连 Wind

```
term_monitor.py --source wind       ← 不依赖 recorder
    ├── Wind 连接 + 查乘数 + wsq 推送
    ├── TickAligner.update_option/etf
    └── 每N秒：scan_opportunities → rich 表格
```

---

## 七、快速开始

### 安装依赖

```bash
pip install -r requirements.txt
```

核心依赖：`pandas` / `numpy` / `rich` / `pyzmq` / `pyarrow` / `flask`

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
python data_recorder/recorder.py                          # 默认参数
python data_recorder/recorder.py --output D:\MARKET_DATA  # 自定义目录
python data_recorder/recorder.py --flush 60               # 60秒分片
python data_recorder/recorder.py --port 5556              # 自定义 ZMQ 端口
```

### Parquet Schema

**期权**：`ts(int64), code(str), underlying(str), last/ask1/bid1(float32), oi/vol(int32), high/low(float32), is_adjusted(bool), multiplier(int32)`

**ETF**：`ts(int64), code(str), last/ask1/bid1(float32)`

**快照**：期权 schema + `type(str)`，ETF 行补充 `is_adjusted=False, multiplier=0`

### 存储目录结构

```
D:\MARKET_DATA\
├── chunks\                            日内分片（15:10 合并后删除）
├── snapshot_latest.parquet            每合约最新一条（策略冷启动用）
├── options_YYYYMMDD.parquet           期权日文件（~60-90 MB/天）
└── etf_YYYYMMDD.parquet               ETF 日文件（< 1 MB/天）
```

---

## 九、终端监控

```bash
# 直连 Wind 模式（独立运行，推荐日常使用）
python term_monitor.py --min-profit 50

# ZMQ 模式（需先启动 recorder）
python term_monitor.py --source zmq --min-profit 100

# 完整参数
python term_monitor.py --source wind --min-profit 150 --expiry-days 45 --refresh 3 --atm-range 0.10
```

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--source` | `wind` | `wind` 直连 / `zmq` 读 recorder |
| `--min-profit` | `30` | 最小显示净利润（元/张） |
| `--expiry-days` | `90` | 最大到期天数 |
| `--refresh` | `5` | 刷新间隔（秒） |
| `--atm-range` | `0.20` | 行权价过滤（±20%） |

### 表格列说明

| 列 | 含义 |
|----|------|
| 到期 | 到期日 MM-DD |
| 行权价 | Strike，调整型前缀 `(A)` |
| 方向 | 正向 / 反向 |
| **乘数** | 真实合约乘数（非标准值高亮） |
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

## 十、网页监控

```bash
python web_monitor.py                              # 默认 8080 端口
python web_monitor.py --port 8081                   # 自定义端口
python web_monitor.py --min-profit 100 --refresh 5
```

浏览器打开 `http://localhost:8080`，暗色主题自动刷新仪表盘。

功能与终端监控一致：三品种分区、正常/调整型分区、乘数列 + 计算明细列。

---

## 十一、回测

```bash
python main.py --data-dir "D:\TICK_DATA\上交所\华夏上证50ETF期权" \
               --start-date 2024-01 --end-date 2024-01

python main.py --data-dir "D:\TICK_DATA\上交所\华夏上证50ETF期权" \
               --start-date 2024-01 --end-date 2024-06 \
               --output-chart equity.png --min-profit 200
```

> ⚠️ 回测中 ETF 价格使用 GBM 模拟（非真实数据），结果仅供参考。

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

每条期权 tick 包含 `is_adjusted`（是否调整型）和 `multiplier`（真实合约乘数），
供下游研究分析使用。

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

`info_data/上交所期权基本信息.csv`：11,102 条记录，UTF-8-BOM 编码。
字段：证券代码、证券简称、起始交易日期、最后交易日期、交割月份、行权价格、期权类型。

---

## 十四、已知限制

| 限制 | 说明 |
|------|------|
| ETF 回测数据为模拟 | GBM 模拟非真实价格，回测结果失真 |
| 三腿非原子执行 | A 股无组合指令，存在腿差风险 |
| 反向套利未计融券利息 | `enable_reverse` 默认关闭 |
| Wind Level 2 权限 | 挂单量默认 100，置信度仅参考 |
| 回测重复信号 | 同 Tick 可能重复，统计偏高（待修复） |
| recorder 崩溃 | 最多丢 30 秒数据，可调小 `--flush` |
