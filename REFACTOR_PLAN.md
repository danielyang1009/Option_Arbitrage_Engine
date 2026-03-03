# ETF 期权 PCP 套利引擎 —— 重构蓝图

> **版本**: v0.4 审计基线
> **审计日期**: 2026-03-03
> **审计范围**: 全部 20+ 核心源文件（只读）
> **原则**: 每步只动一个模块，对外接口不变，主程序随时可跑通

---

## 目录

- [Part 1: 冗余与坏味道诊断](#part-1-冗余与坏味道诊断)
- [Part 2: 高频计算性能瓶颈](#part-2-高频计算性能瓶颈)
- [Part 3: 架构解耦建议](#part-3-架构解耦建议)
- [Part 4: 微创手术级重构路径](#part-4-微创手术级重构路径)

---

## Part 1: 冗余与坏味道诊断

### 1.1 最高风险：两个 Monitor 之间 10+ 处重复实现

这是当前代码库中**最大的维护隐患**。`term_monitor.py` 和 `web_monitor.py` 各自独立实现了几乎相同的业务逻辑，但细节存在微妙差异，任何 bug 修复都必须同时改两份代码，极易遗漏。

| # | 重复逻辑 | term_monitor.py | web_monitor.py | 差异风险 |
|---|---------|-----------------|----------------|---------|
| 1 | 活跃合约加载 | `load_active_contracts` (L231-246) | `_load_active_contracts` (L89-98) | **实现不同**：term 用 set 遍历 `contracts.values()`；web 用列表逐品种 `get_contracts_by_underlying()` |
| 2 | Call/Put 配对构建 | `build_pairs_and_codes` (L249-289) | `_build_pairs_and_codes` (L101-122) | **数据结构不同**：term 三级嵌套 dict；web 用元组 key 单层 dict |
| 3 | 快照恢复 | `_restore_from_snapshot` (L664-733) | `_restore_snapshot` (L125-164) | **细节差异**：`ask_volumes` 填充 `[100]+[0]*4` vs `[100]*5`；时间戳判断 `ts > 1e10` 仅 web 有 |
| 4 | ZMQ 消息解析 | L846-895 | L272-310 | **批次差异**：term 每批最多 200 条，web 最多 500 条 |
| 5 | Windows UTF-8 修复 | L36-48 | L31-40 | 完全相同，纯冗余 |
| 6 | `ETF_NAME_MAP` 常量 | L93-99 | L64-70 | 完全相同 |
| 7 | `CONTRACT_INFO_CSV` 路径 | L101 | L63 | 完全相同 |
| 8 | ETF 品种排序列表 | `_ETF_ORDER` L306 | `ETF_ORDER` L71 | 名字不同，值相同 |
| 9 | ETF 价格回退估算 | L797-801 | L225-229 | 模式相同 |
| 10 | Wind 乘数查询 + try/except | L776-782 | L233-240 | 模式相同 |

**关键风险**：第 3 项的 `ask_volumes` 差异（`[100]+[0]*4` vs `[100]*5`）目前不影响 PCP 计算（只用 ask1/bid1），但如果后续依赖 Level-2 盘口深度数据，两边的行为会分化且难以排查。

### 1.2 死代码

| 严重度 | 文件 | 位置 | 名称 | 说明 |
|--------|------|------|------|------|
| 中 | `term_monitor.py` | L296-302 | `_etf_panel()` | 已定义从未调用，ETF 价格展示已由 `build_display` 内部逻辑替代 |
| 中 | `term_monitor.py` | L445-479 | `build_operation_guide()` | 35 行完整实现，从未被 `run_monitor`/`run_monitor_zmq` 调用 |
| 中 | `main.py` | L254-294 | `run_monitor()` | 空壳函数：连接 Wind 后直接 `while True: time.sleep(1)`，实际监控已迁移到 `term_monitor.py` |
| 低 | `models.py` | L286-288 | `net_asset_value` 属性 | 全项目零引用；命名为"净资产"实际计算"可用资金" |
| 低 | `data_engine/etf_simulator.py` | L276-282 | `last_anchor_price` 变量 | 只被赋值、从未被读取，死变量 |
| 低 | `data_engine/etf_simulator.py` | L208-211 | `latest_quotes` 初始化循环 | 遍历全部 tick 只保留最后一条，但紧接着被 `tick_index` 循环覆盖 |

### 1.3 死导入

| 文件 | 位置 | 未使用的导入 |
|------|------|------------|
| `strategies/pcp_arbitrage.py` | L24-35 | `normalize_code`, `OptionType`, `date` |
| `web_monitor.py` | L48 | `ParquetWriter`（还可能导致启动崩溃） |
| `config/settings.py` | L11 | `Path` |
| `data_engine/etf_simulator.py` | L28 | `normalize_code` |
| `risk/margin.py` | L179 | `ContractInfoManager`（循环体内导入且未使用） |

### 1.4 参数冗余与矛盾

**费率体系双轨并存**（`config/settings.py`）：

```
FeeConfig:
  option_commission_per_contract = 1.7   # 每张 1.7 元
  → 双边 = 1.7 × 2 = 3.4 元

TradingConfig（顶层）:
  option_round_trip_fee = 3.0            # 双边 3.0 元
```

两者数值矛盾（3.4 vs 3.0）。`pcp_arbitrage.py` 只使用顶层 `option_round_trip_fee`，`FeeConfig` 的详细费率仅在已删除的 `_estimate_costs` 方法中使用过——现在是**完全的死配置**。同时 `SlippageConfig` 也不再被策略引用。

### 1.5 魔法数字

**`pcp_arbitrage.py` — `_calc_confidence` (L288-311)**：

6 个硬编码数值未抽取为配置：`500.0`（利润归一化基准）、`0.3`（NaN 默认得分）、`0.01`（价差归一化基准）、`50.0`（挂单量基准）、权重 `0.4 / 0.3 / 0.3`。调参必须改源码。

**`data_engine/etf_simulator.py`**：

`seconds_per_year = 252 * 6.5 * 3600` 在 L147 和 L273 重复定义，应提升为类常量。

### 1.6 其他坏味道

| 问题 | 文件 | 说明 |
|------|------|------|
| 工具函数放在模型层 | `models.py` L125-142 | `normalize_code` + 两个映射字典属于 utils，不属于数据模型 |
| `normalize_code` 用 `str.replace` | `models.py` L139 | 如果 src 子串出现在非后缀位置会错误替换，应用 `code[:-len(src)] + dst` |
| GBM 公式复制粘贴 | `etf_simulator.py` L156-164 / L287-295 | 两处 round 行为不一致：`round(max(...), 4)` vs 仅 `max(...)` |
| `TickAligner` 三个方法大量重复 | `pcp_arbitrage.py` L67-108 | `get_etf_price`/`get_etf_ask`/`get_etf_bid` 前半段完全相同 |
| `_ADJUSTED_TAIL_RE` 位置异常 | `contract_info.py` L231 | 类属性定义在两个实例方法之间，应提升到类顶部 |
| Mock 返回结构不一致 | `wind_adapter.py` L184-185 vs L201-206 | Mock 返回嵌套 `{"data": {...}}`，正常路径返回平铺字段 |
| `high`/`low` 用 `current` 伪造 | `wind_adapter.py` L268-269 | 实时模式下 `high`/`low` 永远等于 `current`，下游振幅计算会出错 |
| 日期 clip 到 28 | `tick_loader.py` L306 | 月末日期（29/30/31 号）被截断为 28 号，金融数据不可接受 |

---

## Part 2: 高频计算性能瓶颈

### 2.1 [高] term_monitor Wind 模式：单线程同步阻塞

**位置**: `term_monitor.py` L600-627

```
while True:
    poll_snapshot(Wind)     # ← 阻塞：网络 IO
    scan_opportunities()    # ← 计算
    live.update(render())   # ← 渲染
    time.sleep(N)           # ← 睡眠
```

整个数据拉取 → 策略扫描 → UI 渲染在单线程中串行执行。Wind API 一次网络抖动（100ms+）就会直接冻结终端界面。

**建议**: 将 Wind 数据拉取移到独立线程/协程，主线程只做渲染。或参考 `data_recorder` 的 Push 回调 + queue 架构。

### 2.2 [高] term_monitor ZMQ 模式：每批消息后都全量渲染

**位置**: `term_monitor.py` L905

```python
live.update(render())    # 每次 ZMQ 批处理后都执行
```

即使没有新信号，每次 ZMQ 消息批（~100ms 一次）都重新创建完整的 Rich Table/Panel 对象树。200+ 配对场景下造成不必要的 CPU 消耗和终端闪烁。

**建议**: 引入 dirty flag，仅在 scan 产出新信号后渲染；或与 scan 共享节流周期。

### 2.3 [中] tick_loader：向量化后仍有 O(n) Python 循环

**位置**: `data_engine/tick_loader.py` L132-148

```python
for i in range(n):          # n = 数十万~百万
    ticks.append(TickData(
        ask_prices=[float(ap_arrs[j][i]) for j in range(5)],   # 5 个 list
        ask_volumes=[int(av_arrs[j][i]) for j in range(5)],
        bid_prices=[float(bp_arrs[j][i]) for j in range(5)],
        bid_volumes=[int(bv_arrs[j][i]) for j in range(5)],
    ))
```

每个 tick 创建 1 个 TickData + 4 个 list = **约 500 万次小对象分配**（百万行），精心做的 numpy 向量化提取在此被抵消。

**建议**: 若下游只按列访问，传递 numpy 结构化数组或 DataFrame 替代逐行构建 dataclass。

### 2.4 [中] etf_simulator：O(T x P) 嵌套循环

**位置**: `data_engine/etf_simulator.py` L217-248

对所有时间戳 T（日内约 50 万 tick）× 所有 Call/Put 配对 P（数百个），总迭代可达数亿次。

**建议**: 仅在行情变化的时间点计算锚点，或按秒级窗口采样。

### 2.5 [低] `_infer_underlying` 每次调用重排序

**位置**: `data_engine/contract_info.py` L297-300

```python
sorted_keys = sorted(UNDERLYING_MAP.keys(), key=len, reverse=True)
```

`UNDERLYING_MAP` 是模块级常量，但 `sorted(...)` 在每次 `_parse_csv_row` 调用时都执行。11,102 条合约 = 11,102 次无意义排序。

**建议**: 缓存为模块级常量 `_SORTED_UNDERLYING_KEYS`。

### 2.6 [低] Parquet snapshot 全量重写

**位置**: `data_recorder/parquet_writer.py` L204-207

每 30 秒将全部合约的最新快照整体重写。对于 300+ 合约的小文件（~几十 KB）影响不大，但如果合约数增长到千级，写入开销会线性增加。

**建议**: 可暂不处理，未来考虑增量更新或内存映射。

---

## Part 2 补充：潜在内存问题

| 问题 | 文件 | 说明 |
|------|------|------|
| `tick_queue` 无背压 | `data_recorder/wind_subscriber.py` | `queue.put_nowait` 失败时静默丢弃，无告警统计 |
| `_snapshot` 字典无清理 | `data_recorder/parquet_writer.py` | 已退市合约的快照永远不会被清除，长期运行会缓慢膨胀 |
| `TickAligner` 无过期清理 | `strategies/pcp_arbitrage.py` | `latest_option_quotes` 只增不减，不清理已退市合约 |

---

## Part 3: 架构解耦建议

### 3.1 抽取 monitor_common.py（消除 10+ 处重复）

应从两个 monitor 中提取以下共享逻辑到独立模块：

```
monitor_common.py
├── fix_windows_encoding()           # UTF-8 修复
├── ETF_NAME_MAP / ETF_ORDER / CONTRACT_INFO_CSV / MONITOR_UNDERLYINGS
├── load_active_contracts()          # 统一实现（当前两份不同）
├── build_pairs_and_codes()          # 统一实现（当前两份不同）
├── restore_from_snapshot()          # 统一实现（消除 ask_volumes 差异）
├── parse_zmq_tick() -> Tick         # ZMQ 消息解析
├── init_strategy_and_contracts()    # 合约加载 + 乘数查询 + 策略初始化
└── signal_to_dict()                 # 信号序列化（web_monitor 专用但可共享）
```

提取后，`term_monitor.py` 和 `web_monitor.py` 各自仅保留 UI 渲染逻辑（约 50-80 行），业务行为完全统一。

### 3.2 三层分离架构

当前 `term_monitor.py` 的 `run_monitor`（L486-633）和 `run_monitor_zmq`（L736-912）是约 150 行的巨型函数，将数据获取、策略计算、UI 渲染揉在一起。建议：

```
                        ┌──────────────┐
                        │   DataFeed   │  Wind Push / ZMQ SUB / Replay
                        └──────┬───────┘
                               │ on_tick(TickData / ETFTickData)
                        ┌──────▼───────┐
                        │ StrategyHub  │  TickAligner + PCPArbitrage
                        └──────┬───────┘
                               │ on_signals(List[TradeSignal])
                  ┌────────────┼────────────┐
           ┌──────▼──────┐ ┌──▼─────┐ ┌────▼─────┐
           │ RichPresenter│ │FlaskAPI│ │LogPresenter│
           └─────────────┘ └────────┘ └──────────┘
```

好处：
- DataFeed 可替换（Wind / ZMQ / 文件回放），策略无需改动
- Presenter 可替换（终端 / Web / 纯日志），策略无需改动
- 策略可独立单元测试，无需 Wind 连接或 ZMQ 管道

### 3.3 策略层鲁棒性修复

| 问题 | 文件:行号 | 修复方案 |
|------|----------|---------|
| `signal_count` 双重递增 | `pcp_arbitrage.py:242,265` | 移到 `scan_opportunities` 末尾统一计数 `self.signal_count += len(signals)` |
| `or` 数值回退的 0 值陷阱 | `pcp_arbitrage.py:214-215` | 改为显式 `None` 检查：`s = self.aligner.get_etf_ask(u); S_ask = s if s is not None else etf_price` |
| 无 Call/Put 配对校验 | `pcp_arbitrage.py:177` | 入口加 `assert call_info.strike_price == put_info.strike_price` |
| 无 Tick 新鲜度检查 | `pcp_arbitrage.py:188-194` | 增加 `max_staleness` 参数，对比 `tick.timestamp` 与 `current_time` |

### 3.4 费率配置统一

当前 `FeeConfig` + `SlippageConfig` 已不被策略使用（`_estimate_costs` 已删除），但仍保留在 `TradingConfig` 中。建议：

- **方案 A（推荐）**: 保留 `FeeConfig`/`SlippageConfig` 供回测引擎使用，PCP 策略的简化参数 (`etf_fee_rate`, `option_round_trip_fee`) 加注释标明仅用于实时监控
- **方案 B**: 删除 `FeeConfig`/`SlippageConfig`，统一使用简化参数

无论哪种方案，都应在 `__post_init__` 中加基本校验：

```python
def __post_init__(self):
    assert self.contract_unit > 0, "contract_unit must be positive"
    assert 0 <= self.etf_fee_rate < 0.01, "etf_fee_rate out of range"
    assert self.option_round_trip_fee >= 0, "option_round_trip_fee must be non-negative"
```

### 3.5 ContractInfoManager 的 Wind 调用解耦

**当前**: `contract_info.py` 的 `load_multipliers_from_wind()` 直接 `from WindPy import w` 并调用 `w.wss()`。

**问题**: 合约信息管理器不应承担网络 IO 职责，已有 `WindAdapter` 专门封装 Wind 调用。

**建议**: 在 `WindAdapter` 中新增 `query_multipliers(codes) -> Dict[str, int]`，`ContractInfoManager` 通过方法参数接收结果（依赖注入）：

```python
# wind_adapter.py
def query_multipliers(self, codes: List[str], batch_size=200) -> Dict[str, int]: ...

# contract_info.py
def apply_multipliers(self, multiplier_map: Dict[str, int]) -> int:
    for code, mult in multiplier_map.items():
        if code in self.contracts and mult > 0:
            self.contracts[code].contract_unit = mult
```

### 3.6 回测子系统修复

**BacktestEngine 价格缓存** (`backtest/engine.py` L472-477)：

`_get_latest_prices` 只返回当前 Tick 的 1-2 个 code 价格，导致 `update_unrealized_pnl` 对其他持仓找不到价格、跳过不计。权益曲线中 unrealized P&L 严重低估。

**修复**: 维护 `self._price_cache: Dict[str, float]`，逐 Tick 更新，计算时使用全量缓存。

**pnl.py 用预估值代替实际 P&L** (`analysis/pnl.py` L321-333)：

`_calc_signal_pnls` 接收 `trade_history` 参数但完全忽略，直接用 `signal.net_profit_estimate`。胜率和盈亏比计算用的是预估值而非实际成交结果。

### 3.7 God Config 拆分

`TradingConfig` 混合了 7 类职责。建议拆为：

```
MarketConfig         # risk_free_rate, contract_unit, trading_days_per_year
StrategyConfig       # min_profit_threshold, enable_reverse, etf_fee_rate, option_round_trip_fee
BacktestConfig       # initial_capital, max_position_per_signal
DataConfig           # data_paths, contract_info_csv
WindConfig           # wind_enabled, wind_timeout
SimulationConfig     # simulation_volatility, simulation_drift
```

各模块只依赖自己需要的子配置，单元测试时构造 mock 更轻量。

> 注意：此项重构影响面大（所有 `TradingConfig` 消费者），建议放在最后执行。

---

## Part 4: 微创手术级重构路径

### Step 1 — 清理死代码与死导入

**范围**: 全部文件，纯删除操作
**风险**: 零（不改任何逻辑）
**验证**: `python -c "import term_monitor, web_monitor, main, models"` 通过

| 操作 | 文件 |
|------|------|
| 删除 `_etf_panel` 函数 (L296-302) | `term_monitor.py` |
| 删除 `build_operation_guide` 函数 (L445-479) | `term_monitor.py` |
| 删除 `run_monitor` 空壳函数 (L254-294)，`--mode monitor` 路由到 `term_monitor` | `main.py` |
| 删除 `net_asset_value` 属性 (L286-288) | `models.py` |
| 删除 `last_anchor_price` 死变量 (L276, L282) | `etf_simulator.py` |
| 删除 `latest_quotes` 无效初始化循环 (L208-211) | `etf_simulator.py` |
| 删除未使用导入：`normalize_code`/`OptionType`/`date` | `pcp_arbitrage.py` |
| 删除未使用导入：`ParquetWriter` | `web_monitor.py` |
| 删除未使用导入：`Path` | `settings.py` |
| 删除未使用导入：`normalize_code` | `etf_simulator.py` |
| 删除 `risk/margin.py` 循环内无用导入 (L179) | `margin.py` |

### Step 2 — 抽取 monitor_common.py

**范围**: 新建 `monitor_common.py`，修改 `term_monitor.py` 和 `web_monitor.py` 的导入
**风险**: 低（逻辑不变，只是代码搬迁 + 统一两份不同实现）
**验证**: 分别启动两个 monitor 确认行为一致

提取内容：
1. `fix_windows_encoding()` — 从两处提取合并
2. 常量：`ETF_NAME_MAP`, `ETF_ORDER`, `CONTRACT_INFO_CSV`, `MONITOR_UNDERLYINGS`
3. `load_active_contracts()` — 以 term_monitor 版为基础统一
4. `build_pairs_and_codes()` — 以 term_monitor 版为基础统一
5. `restore_from_snapshot()` — 统一 `ask_volumes` 为 `[100] + [0]*4`
6. `parse_zmq_option_tick()` / `parse_zmq_etf_tick()` — 统一批次大小参数化
7. `init_strategy(config, expiry_days) -> (strategy, pairs, option_codes, etf_codes)` — 封装初始化流程

### Step 3 — pcp_arbitrage.py 鲁棒性修复

**范围**: 仅 `strategies/pcp_arbitrage.py`
**风险**: 低（修复 bug，不改公共接口）
**验证**: 手算验证 + 启动 term_monitor 确认信号一致

1. `signal_count`：移到 `scan_opportunities` 末尾 `self.signal_count += len(signals)`
2. `S_ask`/`S_bid` 回退：改为显式 `None` 检查
3. 入口加 Call/Put 配对断言（strike/expiry/underlying 一致性）
4. 可选：增加 `max_staleness_secs` 参数，跳过过期 tick（默认 `None` = 不检查，保持向后兼容）
5. 提取 `TickAligner._get_etf_quote()` 消除三个方法的重复代码

### Step 4 — settings.py 费率统一 + 配置校验

**范围**: 仅 `config/settings.py`
**风险**: 低（加校验不改默认值）
**验证**: `get_default_config()` + `get_recorder_config()` 通过校验

1. 在 `FeeConfig` 和 `SlippageConfig` 上加注释"仅回测引擎使用"
2. `option_round_trip_fee` 注释更新为与 `FeeConfig` 一致的数值说明
3. `RecorderConfig.products` 类型注解改为 `list[str]`
4. 添加 `TradingConfig.__post_init__` 校验关键字段范围
5. `output_dir` 默认值改为 `"./market_data"`（相对路径，可移植）

### Step 5 — term_monitor 渲染节流

**范围**: 仅 `term_monitor.py`
**风险**: 中（改主循环逻辑，需仔细测试）
**验证**: 启动 ZMQ 和 Wind 两种模式各运行 5 分钟

**ZMQ 模式** (L905)：
```python
# 当前：每次 ZMQ 批处理后都渲染
live.update(render())

# 改为：与 scan 共享节流，仅在 scan 后渲染
if scanned_this_cycle:
    live.update(render())
```

**Wind 模式** (L600-627)：
- 将 `poll_snapshot` 移到后台线程，主线程只做渲染
- 或：将 Wind 模式也改为 Push 回调 + queue 架构（参考 `data_recorder`）

### Step 6 — tick_loader / etf_simulator 性能优化

**范围**: `data_engine/tick_loader.py` + `data_engine/etf_simulator.py`
**风险**: 中（改数据结构影响下游消费者）
**验证**: 回测结果数值一致

1. `tick_loader.py`：日期 clip 从 28 改为 31，依赖 `pd.to_datetime(errors='coerce')`
2. `etf_simulator.py`：提取 `_step_gbm(price, dt_seconds) -> float`，消除 GBM 公式重复
3. `etf_simulator.py`：`seconds_per_year` 提升为类常量 `_SECONDS_PER_YEAR`
4. `contract_info.py`：`_SORTED_UNDERLYING_KEYS` 缓存为模块级常量
5. 可选：`tick_loader` 的 Python 循环改为批量构建（需评估下游接口影响）

### Step 7 — backtest/engine + analysis/pnl 修复

**范围**: `backtest/engine.py` + `analysis/pnl.py`
**风险**: 中（改回测结果数值）
**验证**: 对比修复前后回测报告，确认 unrealized P&L 和胜率变化合理

1. `engine.py`：新增 `self._price_cache: Dict[str, float]`，逐 Tick 更新
2. `engine.py`：`_get_latest_prices` 改为更新并返回 `_price_cache`
3. `pnl.py`：`_calc_signal_pnls` 用 `trade_history` 匹配实际成交价格计算 P&L
4. `pnl.py`：`calc_greeks_attribution` 标注为"骨架实现"，输出时明确提示"仅供参考"

### Step 8 — 架构分层（远期目标）

**范围**: 全局接口重构
**风险**: 高（影响所有模块）
**验证**: 全量回归测试

1. 定义 `DataFeed` 抽象接口（`WindFeed`, `ZMQFeed`, `ReplayFeed`）
2. 定义 `Presenter` 抽象接口（`RichPresenter`, `FlaskPresenter`, `LogPresenter`）
3. `StrategyHub` 封装 `TickAligner` + `PCPArbitrage`，提供 `on_tick` / `get_signals` 接口
4. `TradingConfig` 拆分为多个子配置（`MarketConfig`, `StrategyConfig`, `BacktestConfig` 等）
5. `ContractInfoManager.load_multipliers_from_wind` 改为通过 `WindAdapter` 注入
6. `normalize_code` + 映射字典从 `models.py` 迁移到 `utils.py`

> 此步骤仅在 Step 1-7 全部完成、系统稳定运行一周后再启动。

---

## 附录：严重度速查表

| 严重度 | 问题 | 文件 | Step |
|--------|------|------|------|
| **严重** | 两个 Monitor 10+ 处重复（含细节差异） | term_monitor / web_monitor | 2 |
| **严重** | pnl.py 用预估值代替实际 P&L | analysis/pnl.py:321-333 | 7 |
| **高** | `signal_count` 双重递增 | pcp_arbitrage.py:242,265 | 3 |
| **高** | `or` 数值回退 0 值陷阱 | pcp_arbitrage.py:214-215 | 3 |
| **高** | 费率参数重复矛盾 | settings.py | 4 |
| **高** | BacktestEngine 价格缓存缺失 | backtest/engine.py:472-477 | 7 |
| **高** | 日期 clip 到 28 丢失月末 | tick_loader.py:306 | 6 |
| **中** | Wind 模式单线程阻塞 | term_monitor.py:600-627 | 5 |
| **中** | ZMQ 模式过度渲染 | term_monitor.py:905 | 5 |
| **中** | 无 Tick 新鲜度检查 | pcp_arbitrage.py | 3 |
| **中** | 无 Call/Put 配对校验 | pcp_arbitrage.py:177 | 3 |
| **中** | God Config 反模式 | settings.py:50-92 | 8 |
| **中** | ContractInfoManager 直接调 Wind | contract_info.py:164-207 | 8 |
| **低** | 死代码 / 死导入（多处） | 多个文件 | 1 |
| **低** | 魔法数字 x6 | pcp_arbitrage.py:288-311 | 3 |
| **低** | GBM 公式重复 | etf_simulator.py | 6 |
| **低** | `_infer_underlying` 重排序 | contract_info.py:297 | 6 |
