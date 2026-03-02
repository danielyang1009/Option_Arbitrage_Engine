# 项目状态交接文档 (STATE.md)

> 生成时间：2026-03-03  
> 用于新对话快速恢复上下文，请将本文件内容粘贴给新的 AI 会话。

---

## 一、项目概述

**项目名称**：中国 ETF 期权 PCP 套利回测与交易预警框架  
**项目路径**：`d:\Option_Arbitrage_Engine`  
**开发语言**：Python 3.10+  
**当前版本**：v0.1（骨架完整，核心逻辑已实现，等待真实 ETF 数据接入）

**核心功能**：
1. 实盘监控（Wind API）：检测 Put-Call Parity 套利机会，输出控制台警报，手动下单
2. 历史回测：Tick-by-Tick 精确回测，含盈亏分析和 Greeks 归因

---

## 二、目录结构

```
d:\Option_Arbitrage_Engine\
├── main.py                         # 统一入口（回测 + 实盘监控）
├── models.py                       # 全局数据模型（TickData/ContractInfo/TradeSignal 等）
├── requirements.txt                # 依赖：pandas/numpy/scipy/tabulate/matplotlib
├── STATE.md                        # 本文件
│
├── config/
│   └── settings.py                 # 全局配置（费率/滑点/保证金/数据路径）
│
├── data_engine/
│   ├── tick_loader.py              # CSV Tick 加载器（向量化，支持日期过滤）
│   ├── contract_info.py            # 合约信息管理（CSV加载 + .SH/.XSHG 标准化）
│   ├── wind_adapter.py             # Wind API 适配器（wsq/wsd，Mock降级）
│   └── etf_simulator.py            # 标的 ETF 价格模拟器（GBM + PCP隐含锚点）
│
├── core/
│   └── pricing.py                  # Black-Scholes 定价 + Newton-Raphson IV 求解
│
├── strategies/
│   └── pcp_arbitrage.py            # PCP 套利策略 + TickAligner 时间对齐器
│
├── risk/
│   └── margin.py                   # 上交所卖方保证金计算
│
├── backtest/
│   └── engine.py                   # Tick-by-Tick 回测引擎 + Account 账户管理
│
├── analysis/
│   └── pnl.py                      # P&L/回撤/Sharpe/Greeks 归因 + matplotlib 图表
│
├── info_data/
│   ├── 上交所期权基本信息.csv       # 11,102 条合约记录（行权价/类型/到期日）
│   └── etf_option_info.md          # 品种上市时间参考
│
└── sample_data/                    # 小样本数据（用于快速功能验证）
    ├── 华夏上证50ETF期权/
    ├── 华泰柏瑞沪深300ETF期权/
    └── 南方中证500ETF期权/
```

---

## 三、数据资产

### 3.1 完整 Tick 数据（不在仓库中，本地路径）

```
D:\TICK_DATA\上交所\
├── 华夏上证50ETF期权/     129 个月度CSV（2015-02 ~ 2025-10）
├── 华泰柏瑞沪深300ETF期权/ 73 个月度CSV（2019-12 ~ 2025-12）
├── 南方中证500ETF期权/     40 个月度CSV（2022-09 ~ 2025-12）
├── 科创50期权/             31 个月度CSV（2023-06 ~ 2025-12）
└── 科创板50期权/           31 个月度CSV（2023-06 ~ 2025-12）
```

**文件命名规律**：`{品种名}_option_ticks_{YYYY-MM}.csv`

### 3.2 Tick 数据 Schema

| 字段 | 类型 | 说明 |
|------|------|------|
| `time` | int64 | 时间戳，格式 `YYYYMMDDHHMMSSmmm`（17位，毫秒精度） |
| `current` | float | 最新价 |
| `volume` | int | 累计成交量 |
| `high` / `low` | float | 最高/最低价 |
| `money` | float | 成交额 |
| `position` | int | 持仓量 |
| `a1_p` ~ `a5_p` | float | 卖1~卖5价（50ETF有5档，300/500ETF仅1档） |
| `b1_p` ~ `b5_p` | float | 买1~买5价 |
| `contract_code` | str | 合约代码，后缀 `.XSHG`（加载时自动转为 `.SH`） |

### 3.3 合约信息文件

**路径**：`info_data/上交所期权基本信息.csv`  
**编码**：UTF-8-BOM  
**字段**：`证券代码,证券简称,起始交易日期,最后交易日期,交割月份,行权价格,期权类型`  
**记录数**：11,102 条（认购 5,551 + 认沽 5,551）  
**代码后缀**：`.SH`（与 Tick 数据的 `.XSHG` 不同，框架已自动处理）

---

## 四、使用方法

### 安装依赖
```bash
pip install -r requirements.txt
```

### 回测模式

```bash
# 单月回测（推荐从这里开始，耗时约 20 秒）
python main.py --data-dir "D:\TICK_DATA\上交所\华夏上证50ETF期权" --start-date 2024-01 --end-date 2024-01

# 季度回测
python main.py --data-dir "D:\TICK_DATA\上交所\华夏上证50ETF期权" --start-date 2024-01 --end-date 2024-03

# 带图表输出
python main.py --data-dir "D:\TICK_DATA\上交所\华夏上证50ETF期权" --start-date 2024-01 --end-date 2024-06 --output-chart equity.png

# 自定义资金和利润阈值
python main.py --data-dir "D:\TICK_DATA\上交所\华夏上证50ETF期权" --start-date 2024-01 --end-date 2024-01 --capital 2000000 --min-profit 200
```

### 实盘监控模式（需要 Wind 终端）
```bash
python main.py --mode monitor
```

### 命令行参数一览

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--mode` | `backtest` | `backtest` 或 `monitor` |
| `--data-dir` | `sample_data` | Tick 数据目录 |
| `--start-date` | 最早 | 回测起始月份，格式 `YYYY-MM` |
| `--end-date` | 最新 | 回测结束月份，格式 `YYYY-MM` |
| `--capital` | 1,000,000 | 初始资金（元） |
| `--min-profit` | 50 | 最小利润阈值（元/组） |
| `--output-chart` | 无 | 权益曲线图保存路径 |
| `--verbose` | False | 输出详细日志 |

---

## 五、已验证的功能

| 模块 | 状态 | 备注 |
|------|------|------|
| 合约信息加载 | ✅ | 11,102 条正常解析，.SH/.XSHG 互转正常 |
| Tick 数据加载 | ✅ | 向量化，104,511 条/1.3秒；自动识别1档/5档盘口 |
| 时间戳解析 | ✅ | 精确到毫秒，支持17位整型和科学计数法 |
| 日期范围过滤 | ✅ | `--start-date` / `--end-date` 按文件名过滤 |
| Black-Scholes 定价 | ✅ | ATM Call=0.1270，PCP等价关系精确验证 |
| IV 求解（Newton-Raphson） | ✅ | 从BS价格反推，收敛至 σ=0.200000 |
| PCP 套利信号扫描 | ✅ | 正向/反向套利，含费用和滑点估算 |
| 保证金计算 | ✅ | 上交所卖方公式，支持认购/认沽 |
| 回测引擎 | ✅ | Tick-by-Tick 撮合，T+0/T+1 约束，资金管理 |
| 盈亏分析 | ✅ | P&L/回撤/Sharpe/胜率，权益曲线图 |
| Wind 适配器 | ✅ | 无 Wind 时自动 Mock 降级 |

---

## 六、当前已知问题与待办

### 🔴 高优先级
1. **ETF 模拟数据失真**（最关键）  
   - **现象**：当前用 PCP 反推的隐含 ETF 价格作为模拟锚点，但期权上市初期存在大量错误定价，导致信号失真、回测亏损严重  
   - **解决方案**：接入真实 ETF Tick 数据。需在 `ETFSimulator` 或 `TickLoader` 中增加"真实 ETF 数据加载通道"，当同目录存在 ETF Tick 数据时自动使用，否则降级为模拟  
   - **ETF 代码与路径映射**：510050.SH → 50ETF，510300.SH → 300ETF，510500.SH → 500ETF

2. **回测引擎重复信号问题**  
   - **现象**：同一 Tick 时刻扫描了重复信号（见日志中相同时间戳、相同 Strike 的多条信号）  
   - **解决方案**：在 `PCPArbitrage.scan_opportunities()` 中加入信号去重逻辑，同一合约对在同一时间点只保留利润最高的一条

### 🟡 中优先级
3. **月度/批量回测脚本**  
   - 当前需手动指定日期范围，建议增加 `--batch-by-month` 模式，逐月回测并汇总结果
4. **Greeks 归因完善**  
   - 当前 `calc_greeks_attribution()` 是骨架实现（按固定比例拆分），需要逐 Tick 计算持仓 Greeks 变化量做真实归因
5. **回测结果持久化**  
   - 目前结果只打印到控制台，建议保存为 CSV 或 JSON 文件

### 🟢 低优先级
6. 实盘监控的弹窗/声音警报功能（当前只有控制台输出）
7. 多品种同时回测（当前各品种独立运行）

---

## 七、关键设计决策记录

| 决策 | 选择 | 原因 |
|------|------|------|
| 代码后缀标准 | 统一使用 `.SH` | Tick 数据用 `.XSHG`，CSV 用 `.SH`，框架内统一转换 |
| 时间戳解析 | 向量化整型运算 | 17位整型可直接用 int64 整除提取各时间分量，比逐行 Decimal 快20x |
| ETF 价格模拟 | PCP 隐含锚点 + GBM 插值 | 比纯随机 GBM 更与期权市场价格一致 |
| 现货做空 | 仅记录，不执行 | A股 ETF 现货受 T+1 限制，反向套利实际可操作性低 |
| 保证金比例 | 认购/认沽各 12%/7% | 上交所标准参数，可在 `config/settings.py` 覆盖 |

---

## 八、开发环境

```
OS: Windows 10/11
Python: 3.10+
关键依赖版本（实测可用）:
  pandas >= 2.0
  numpy >= 1.24
  scipy >= 1.10
  tabulate >= 0.9
  matplotlib >= 3.7
可选: WindPy（需要 Wind 金融终端授权）
```

---

## 九、继续开发建议（给新对话的提示词）

```
项目在 d:\Option_Arbitrage_Engine，是一个中国ETF期权PCP套利回测框架。
请先读取 STATE.md 了解全貌，然后读取相关源码文件再开始修改。

当前最重要的任务是：[在此填写具体需求，例如：]
- "在 data_engine/etf_simulator.py 中增加真实 ETF Tick 数据加载功能"
- "修复 strategies/pcp_arbitrage.py 中的重复信号问题"
- "增加逐月批量回测功能"
```
