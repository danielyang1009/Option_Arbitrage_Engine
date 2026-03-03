# -*- coding: utf-8 -*-
"""
PCP 套利实时监控（终端版）

实时连接 Wind API，轮询 ETF 和期权行情，计算 Put-Call Parity 套利窗口，
在终端以彩色表格形式刷新输出。

运行方法:
    python monitor_live.py
    python monitor_live.py --min-profit 50    # 调高显示阈值（元/组）
    python monitor_live.py --expiry-days 60   # 只看60天内到期合约
    python monitor_live.py --refresh 3        # 3秒刷新一次

依赖:
    pip install rich
    WindPy（需要 Wind 金融终端已登录）
"""

from __future__ import annotations

import argparse
import ctypes
import io
import logging
import math
import os
import sys
import time
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# ── Windows 终端编码修复（PowerShell 默认 GBK，强制切换到 UTF-8）─────────
# 必须在所有其他 import 之前执行，否则 Rich/logging 已绑定旧的编码
if sys.platform == "win32":
    # 1. 修改当前进程的 Windows 控制台代码页为 UTF-8
    ctypes.windll.kernel32.SetConsoleOutputCP(65001)
    ctypes.windll.kernel32.SetConsoleCP(65001)
    # 2. 同步 Python 的 stdout / stderr 到 UTF-8
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except AttributeError:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")
    # 3. 告知子进程（WindPy 等）也使用 UTF-8
    os.environ["PYTHONIOENCODING"] = "utf-8"

# 将项目根目录加入 Python 路径
sys.path.insert(0, str(Path(__file__).parent))

from rich import box
from rich.columns import Columns
from rich.console import Console, Group as RenderGroup
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from config.settings import get_default_config, TradingConfig
from data_engine.contract_info import ContractInfoManager
from models import (
    ContractInfo,
    ETFTickData,
    OptionType,
    SignalType,
    TickData,
    TradeSignal,
    normalize_code,
)
from strategies.pcp_arbitrage import PCPArbitrage

# Rich Console：legacy_windows=False 禁用 WriteConsole API，改用 UTF-8 流输出
console = Console(legacy_windows=False, highlight=True)

# 配置 logging：监控时只显示 WARNING+，避免 INFO 日志干扰 Rich 界面
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stderr,
)

# ──────────────────────────────────────────────────────────────────────
# 常量
# ──────────────────────────────────────────────────────────────────────

WIND_OPTION_FIELDS = "rt_last,rt_ask1,rt_bid1"    # 3字段确保不超限（194x3=582点）；rt_oi不影响PCP计算
WIND_ETF_FIELDS    = "rt_last,rt_ask1,rt_bid1"
WIND_BATCH_SIZE    = 300   # Wind wsq 单批代码上限（实测194个无问题；超大批量时再分批+cancelRequest）

ETF_NAME_MAP: Dict[str, str] = {
    "510050": "50ETF",
    "510300": "300ETF",
    "510500": "500ETF",
    "588000": "科创50",
    "588050": "科创板50",
}

CONTRACT_INFO_CSV = Path(__file__).parent / "info_data" / "上交所期权基本信息.csv"


# ──────────────────────────────────────────────────────────────────────
# Wind 行情工具
# ──────────────────────────────────────────────────────────────────────

def _fval(d: dict, key: str, default: float = math.nan) -> float:
    """安全读取浮点字段"""
    v = d.get(key)
    if v is None:
        return default
    try:
        f = float(v)
        return default if math.isnan(f) else f
    except (TypeError, ValueError):
        return default


def _ival(d: dict, key: str, default: int = 0) -> int:
    """安全读取整型字段"""
    v = d.get(key)
    if v is None:
        return default
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return default


def poll_snapshot(
    w,
    codes: List[str],
    fields: str = WIND_OPTION_FIELDS,
    cancel_before: bool = False,
) -> Dict[str, Dict[str, float]]:
    """
    批量拉取 Wind 实时行情快照（同步 wsq，无回调）

    Wind wsq 每次调用都会累积订阅计数，超过账户订阅上限时报 -40522007。
    使用 cancel_before=True 可在首次调用前清空旧订阅（每轮轮询的第一次调用使用）。

    Args:
        w: WindPy w 实例
        codes: 合约代码列表（.SH 后缀）
        fields: Wind 字段字符串
        cancel_before: 是否先取消所有旧订阅（建议每轮轮询第一次调用时设为 True）

    Returns:
        {标准化代码: {FIELD_NAME: value}} 字典
    """
    if cancel_before:
        try:
            w.cancelRequest(0)  # 取消本轮第一个 wsq 前的所有旧订阅
        except Exception:
            pass

    out: Dict[str, Dict] = {}
    for i in range(0, len(codes), WIND_BATCH_SIZE):
        if i > 0:
            # 每批 wsq 前都取消旧订阅：多次 wsq 调用会累积订阅计数，超限报 -40522007
            try:
                w.cancelRequest(0)
            except Exception:
                pass
        batch = codes[i : i + WIND_BATCH_SIZE]
        result = w.wsq(",".join(batch), fields)
        if result is None or result.ErrorCode != 0:
            continue

        field_names = [f.upper() for f in result.Fields]
        for j, raw_code in enumerate(result.Codes):
            row: Dict[str, float] = {}
            for k, fn in enumerate(field_names):
                try:
                    row[fn] = result.Data[k][j]
                except (IndexError, TypeError):
                    row[fn] = None
            out[normalize_code(raw_code, ".SH")] = row

    return out


def make_option_tick(code: str, q: Dict, ts: datetime) -> Optional[TickData]:
    """将 Wind 行情字典转为 TickData，行情不完整时返回 None"""
    last = _fval(q, "RT_LAST", 0.0)
    ask1 = _fval(q, "RT_ASK1")
    bid1 = _fval(q, "RT_BID1")

    if last <= 0 or math.isnan(ask1) or math.isnan(bid1):
        return None
    if ask1 <= 0 or bid1 <= 0 or ask1 < bid1:
        return None

    # rt_ask_vol1/rt_bid_vol1 需 Level 2 权限，此处默认设为 100（表示有成交量，不影响套利计算）
    return TickData(
        timestamp=ts,
        contract_code=code,
        current=last,
        volume=0,
        high=last,
        low=last,
        money=0.0,
        position=_ival(q, "RT_OI"),
        ask_prices=[ask1] + [math.nan] * 4,
        ask_volumes=[100] + [0] * 4,
        bid_prices=[bid1] + [math.nan] * 4,
        bid_volumes=[100] + [0] * 4,
    )


def make_etf_tick(code: str, q: Dict, ts: datetime) -> Optional[ETFTickData]:
    """将 Wind 行情字典转为 ETFTickData"""
    last = _fval(q, "RT_LAST", 0.0)
    if last <= 0:
        return None
    return ETFTickData(
        timestamp=ts,
        etf_code=code,
        price=last,
        ask_price=_fval(q, "RT_ASK1"),
        bid_price=_fval(q, "RT_BID1"),
        is_simulated=False,
    )


# ──────────────────────────────────────────────────────────────────────
# 合约加载 & 配对构建
# ──────────────────────────────────────────────────────────────────────

def load_active_contracts(
    contract_mgr: ContractInfoManager,
    max_expiry_days: int,
) -> List[ContractInfo]:
    """
    筛选当日活跃且在 max_expiry_days 天内到期的合约。
    包含调整型合约（is_adjusted=True），由显示层分区展示。
    """
    today = date.today()
    return [
        info
        for info in contract_mgr.contracts.values()
        if info.underlying_code in _MONITOR_UNDERLYINGS
        and info.list_date <= today <= info.expiry_date
        and (info.expiry_date - today).days <= max_expiry_days
    ]


def build_pairs_and_codes(
    contract_mgr: ContractInfoManager,
    active: List[ContractInfo],
    etf_prices: Dict[str, float],
    atm_range_pct: float = 0.20,
) -> Tuple[List[Tuple[ContractInfo, ContractInfo]], List[str]]:
    """
    构建 Call/Put 配对并按 ATM 距离过滤，返回 (配对列表, 期权代码列表)

    ATM 距离超过 atm_range_pct * ETF价格 的合约将被过滤（提升效率）
    """
    by_underlying: Dict[str, Dict[date, List[ContractInfo]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for info in active:
        by_underlying[info.underlying_code][info.expiry_date].append(info)

    pairs: List[Tuple[ContractInfo, ContractInfo]] = []
    option_codes: set = set()

    for u_code, expiry_map in by_underlying.items():
        etf_px = etf_prices.get(u_code, 0.0)

        for expiry, contracts in expiry_map.items():
            calls = {c.strike_price: c for c in contracts if c.option_type == OptionType.CALL}
            puts  = {c.strike_price: c for c in contracts if c.option_type == OptionType.PUT}
            common_strikes = sorted(set(calls) & set(puts))

            for strike in common_strikes:
                if etf_px > 0:
                    dist_pct = abs(strike - etf_px) / etf_px
                    if dist_pct > atm_range_pct:
                        continue  # 深度虚值，流动性差，跳过

                call_info = calls[strike]
                put_info  = puts[strike]
                pairs.append((call_info, put_info))
                option_codes.add(call_info.contract_code)
                option_codes.add(put_info.contract_code)

    return pairs, list(option_codes)


# ──────────────────────────────────────────────────────────────────────
# Rich 显示构建
# ──────────────────────────────────────────────────────────────────────

def _etf_panel(etf_prices: Dict[str, float]) -> Panel:
    """构建 ETF 价格小面板"""
    parts = []
    for code, px in etf_prices.items():
        name = ETF_NAME_MAP.get(code.split(".")[0], code)
        parts.append(f"[cyan]{name}[/cyan] [bold yellow]{px:.4f}[/bold yellow]")
    return Panel("    ".join(parts) if parts else "[dim]等待行情...[/dim]", title="实时标的价格")


_MONITOR_UNDERLYINGS = {"510050.SH", "510300.SH", "510500.SH"}  # 不含科创品种
_ETF_ORDER = ["510050.SH", "510300.SH", "510500.SH"]
_ETF_BORDER = {
    "510050.SH": "bright_cyan",
    "510300.SH": "bright_blue",
    "510500.SH": "bright_magenta",
}


def _build_etf_table(
    underlying: str,
    sigs: List[TradeSignal],
    price: float,
) -> Table:
    """为单个品种构建信号表格"""
    u_name  = ETF_NAME_MAP.get(underlying.split(".")[0], underlying)
    n_fwd   = sum(1 for s in sigs if s.signal_type == SignalType.FORWARD)
    n_rev   = len(sigs) - n_fwd
    border  = "bright_green" if n_fwd > 0 else (_ETF_BORDER.get(underlying, "dim") if sigs else "dim")

    title_parts = [f"[bold]{u_name}[/bold]"]
    if price > 0:
        title_parts.append(f"[yellow]{price:.4f}[/yellow]")
    if n_fwd > 0:
        title_parts.append(f"[bold bright_green]正向 {n_fwd} 条[/bold bright_green]")
    if n_rev > 0:
        title_parts.append(f"[dim]反向 {n_rev} 条[/dim]")
    if not sigs:
        title_parts.append("[dim]暂无信号[/dim]")

    tbl = Table(
        title="  ".join(title_parts),
        box=box.SIMPLE_HEAVY,
        show_header=True,
        header_style="bold cyan",
        border_style=border,
        expand=True,
        padding=(0, 1),
    )
    tbl.add_column("到期",       style="dim",   width=5,  justify="center")
    tbl.add_column("行权价",                    width=7,  justify="right")
    tbl.add_column("方向",                      width=4,  justify="center")
    tbl.add_column("乘数",                      width=5,  justify="right")
    tbl.add_column("C_b",                       width=7,  justify="right")
    tbl.add_column("C_a",                       width=7,  justify="right")
    tbl.add_column("P_b",                       width=7,  justify="right")
    tbl.add_column("P_a",                       width=7,  justify="right")
    tbl.add_column("S",                         width=7,  justify="right")
    tbl.add_column("净利润",     style="bold",  width=8,  justify="right")
    tbl.add_column("明细",       style="dim",   min_width=30)

    if not sigs:
        tbl.add_row(*["—"] * 9, "[dim]暂无机会[/dim]", "—")
        return tbl

    # 分区排序：正常合约在前（按到期+行权价升序），调整型在后
    normal  = sorted([s for s in sigs if not s.is_adjusted], key=lambda s: (s.expiry, s.strike))
    adjusted = sorted([s for s in sigs if s.is_adjusted],    key=lambda s: (s.expiry, s.strike))

    def _add_sig_row(sig: TradeSignal, is_adj: bool) -> None:
        profit = sig.net_profit_estimate
        if profit >= 200:
            ps = "bold bright_green"
        elif profit >= 100:
            ps = "bold green"
        else:
            ps = "yellow"
        dir_str  = "[bold]正向[/bold]" if sig.signal_type == SignalType.FORWARD else "[italic dim]反向[/italic dim]"
        mult_str = f"[bold yellow]{sig.multiplier}[/bold yellow]" if sig.multiplier != 10000 else "[dim]10000[/dim]"
        adj_tag  = "[dim italic](A)[/dim italic] " if is_adj else ""
        tbl.add_row(
            sig.expiry.strftime("%m-%d"),
            f"{adj_tag}{sig.strike:.2f}",
            dir_str,
            mult_str,
            f"{sig.call_bid:.4f}",
            f"{sig.call_ask:.4f}",
            f"{sig.put_bid:.4f}",
            f"{sig.put_ask:.4f}",
            f"{sig.spot_price:.4f}",
            f"[{ps}]{profit:.0f}[/{ps}]",
            sig.calc_detail,
        )

    for sig in normal[:20]:
        _add_sig_row(sig, False)

    if adjusted and normal:
        tbl.add_section()

    for sig in adjusted[:10]:
        _add_sig_row(sig, True)

    return tbl


def build_display(
    signals: List[TradeSignal],
    ts: datetime,
    etf_prices: Dict[str, float],
    n_pairs: int,
    n_option_codes: int,
    iteration: int,
    min_profit: float,
) -> RenderGroup:
    """构建套利信号布局，按品种分块显示"""
    etf_line = "  ".join(
        f"[cyan]{ETF_NAME_MAP.get(c.split('.')[0], c)}[/cyan]=[bold yellow]{p:.4f}[/bold yellow]"
        for c, p in etf_prices.items()
        if p > 0
    )
    n_fwd = sum(1 for s in signals if s.signal_type == SignalType.FORWARD)
    header = Panel(
        f"[bold bright_green]⚡ PCP 套利实时监控[/bold bright_green]  "
        f"[dim]{ts.strftime('%H:%M:%S')}[/dim]  第 {iteration} 次刷新\n"
        f"{etf_line}\n"
        f"[dim]监控配对: {n_pairs} 组  订阅期权: {n_option_codes} 个  "
        f"套利信号 (≥{min_profit:.0f}元): {len(signals)} 条"
        f"  [bold bright_green]正向: {n_fwd}[/bold bright_green]"
        f"  反向: {len(signals) - n_fwd}[/dim]",
        box=box.MINIMAL,
        padding=(0, 2),
    )

    # 按品种分组
    groups: Dict[str, List[TradeSignal]] = defaultdict(list)
    for sig in signals:
        groups[sig.underlying_code].append(sig)

    # 确定要显示的品种（etf_prices 里有的都显示）
    underlying_list = [c for c in _ETF_ORDER if c in etf_prices]

    tables = [
        _build_etf_table(u, groups.get(u, []), etf_prices.get(u, 0.0))
        for u in underlying_list
    ]

    return RenderGroup(header, *tables)


def build_operation_guide(signals: List[TradeSignal]) -> Panel:
    """显示操作指引（正向/反向套利的手动下单参考）"""
    if not signals:
        return Panel("[dim]无套利机会，等待中...[/dim]", title="操作指引")

    top = signals[0]
    direction = "正向" if top.signal_type == SignalType.FORWARD else "反向"
    u_name = ETF_NAME_MAP.get(top.underlying_code.split(".")[0], top.underlying_code)

    if top.signal_type == SignalType.FORWARD:
        ops = (
            f"[bold green]【正向套利 Conversion】[/bold green]  "
            f"预估净利润: [bold yellow]{top.net_profit_estimate:.0f} 元/组[/bold yellow]\n\n"
            f"  1. 买入 {u_name}  {top.strike:.4f} [blue]认购[/blue] ({top.call_code})  "
            f"  卖出价参考: ≤[yellow]{top.call_bid:.4f}[/yellow]\n"
            f"  2. 买入 {u_name}  {top.strike:.4f} [red]认沽[/red] ({top.put_code})  "
            f"  买入价参考: ≤[yellow]{top.put_ask:.4f}[/yellow]\n"
            f"  3. 买入 {top.underlying_code.replace('.SH','')} ETF  "
            f"  买入价参考: ≤[yellow]{top.spot_price:.4f}[/yellow]\n"
            f"\n  [dim]到期日: {top.expiry}  置信度: {top.confidence:.2f}[/dim]"
        )
    else:
        ops = (
            f"[bold cyan]【反向套利 Reversal】[/bold cyan]  "
            f"预估净利润: [bold yellow]{top.net_profit_estimate:.0f} 元/组[/bold yellow]\n\n"
            f"  1. 卖出 {u_name}  {top.strike:.4f} [red]认沽[/red] ({top.put_code})  "
            f"  卖出价参考: ≥[yellow]{top.put_bid:.4f}[/yellow]\n"
            f"  2. 买入 {u_name}  {top.strike:.4f} [blue]认购[/blue] ({top.call_code})  "
            f"  买入价参考: ≤[yellow]{top.call_ask:.4f}[/yellow]\n"
            f"  3. 卖出 {top.underlying_code.replace('.SH','')} ETF  "
            f"  [dim](A股 T+1 限制，谨慎操作)[/dim]\n"
            f"\n  [dim]到期日: {top.expiry}  置信度: {top.confidence:.2f}[/dim]"
        )

    return Panel(ops, title=f"最优机会操作指引（{direction}）", border_style="yellow")


# ──────────────────────────────────────────────────────────────────────
# 主逻辑
# ──────────────────────────────────────────────────────────────────────

def run_monitor(
    min_profit: float = 30.0,
    expiry_days: int = 90,
    refresh_secs: int = 5,
    atm_range_pct: float = 0.20,
) -> None:
    """
    主监控循环

    Args:
        min_profit: 最小显示净利润（元/组）
        expiry_days: 最大到期天数
        refresh_secs: 刷新间隔（秒）
        atm_range_pct: ATM 距离过滤比例（20% = 仅看 ±20% 行权价）
    """
    # ── 1. 连接 Wind ──────────────────────────────────────────────────
    console.print("[bold]正在导入 WindPy...[/bold]", end=" ")
    try:
        from WindPy import w
    except ImportError:
        console.print("[red]失败：WindPy 未安装[/red]")
        console.print("请确认 Wind 终端已安装，并执行：")
        console.print("  python -c \"import sys; sys.path.insert(0, r'C:\\Wind\\Wind.NET.Client\\WindNET\\x64')\"")
        return
    console.print("[green]OK[/green]")

    console.print("[bold]正在连接 Wind 终端...[/bold]", end=" ")
    result = w.start()
    if result.ErrorCode != 0:
        console.print(f"[red]失败 (ErrorCode={result.ErrorCode})[/red]")
        console.print("[yellow]提示：请先打开并登录 Wind 金融终端[/yellow]")
        return
    console.print("[green]连接成功[/green]")

    # ── 2. 加载合约信息 ───────────────────────────────────────────────
    config = get_default_config()
    config.min_profit_threshold = min_profit

    strategy = PCPArbitrage(config)
    contract_mgr = ContractInfoManager()

    if not CONTRACT_INFO_CSV.exists():
        console.print(f"[red]合约信息文件不存在: {CONTRACT_INFO_CSV}[/red]")
        w.stop()
        return

    n = contract_mgr.load_from_csv(CONTRACT_INFO_CSV)
    console.print(f"已加载 {n} 条合约信息")

    active = load_active_contracts(contract_mgr, expiry_days)
    console.print(f"当前活跃合约（{expiry_days}天内到期）: {len(active)} 个")

    if not active:
        console.print("[red]无活跃合约，合约信息文件可能已过期[/red]")
        w.stop()
        return

    # 通过 Wind 查询真实合约乘数（标准 10000 / 调整型可能 10265 等）
    active_codes = [c.contract_code for c in active]
    n_mult = contract_mgr.load_multipliers_from_wind(active_codes)
    if n_mult > 0:
        console.print(f"[green]已从 Wind 更新 {n_mult} 个合约的真实乘数[/green]")

    # ── 3. 获取 ETF 代码 & 初始价格 ───────────────────────────────────
    etf_codes = sorted(set(c.underlying_code for c in active))
    console.print(f"标的 ETF: {etf_codes}")

    console.print("拉取 ETF 初始价格...")
    etf_snap = poll_snapshot(w, etf_codes, WIND_ETF_FIELDS)
    etf_prices: Dict[str, float] = {}

    for code in etf_codes:
        q = etf_snap.get(code, {})
        px = _fval(q, "RT_LAST", 0.0)
        name = ETF_NAME_MAP.get(code.split(".")[0], code)
        if px > 0:
            etf_prices[code] = px
            console.print(f"  {name} ({code}): [yellow]{px:.4f}[/yellow]")
        else:
            # 非交易时间：从行权价推算 ATM 中心
            strikes = [c.strike_price for c in active if c.underlying_code == code]
            if strikes:
                etf_prices[code] = (min(strikes) + max(strikes)) / 2
                console.print(
                    f"  {name} ({code}): [dim]未获得实时价格，使用估算 {etf_prices[code]:.4f}[/dim]"
                )

    # ── 4. 构建配对 ────────────────────────────────────────────────────
    pairs, option_codes = build_pairs_and_codes(
        contract_mgr, active, etf_prices, atm_range_pct
    )
    console.print(
        f"Call/Put 配对: [cyan]{len(pairs)}[/cyan] 组  "
        f"订阅期权: [cyan]{len(option_codes)}[/cyan] 个"
    )

    # ── 5. 主轮询循环 ──────────────────────────────────────────────────
    console.print(
        f"\n[bold green]开始实时监控[/bold green]  "
        f"刷新间隔 {refresh_secs}s  "
        f"最小利润显示阈值 {min_profit:.0f} 元  "
        f"按 Ctrl+C 退出\n"
    )

    iteration = 0
    last_signals: List[TradeSignal] = []
    etf_display: Dict[str, float] = dict(etf_prices)

    def render() -> RenderGroup:
        return build_display(
            last_signals, datetime.now(), etf_display,
            len(pairs), len(option_codes), iteration, min_profit
        )

    try:
        with Live(render(), console=console, refresh_per_second=0.5, screen=True) as live:
            while True:
                ts = datetime.now()

                # 拉取 ETF 实时行情（先取消旧订阅，避免 -40522007 累积超限）
                etf_snap = poll_snapshot(w, etf_codes, WIND_ETF_FIELDS, cancel_before=True)
                for code, q in etf_snap.items():
                    tick = make_etf_tick(code, q, ts)
                    if tick:
                        strategy.on_etf_tick(tick)
                        etf_display[code] = tick.price

                # 拉取期权实时行情（分批）
                opt_snap = poll_snapshot(w, option_codes, WIND_OPTION_FIELDS)
                for code, q in opt_snap.items():
                    tick = make_option_tick(code, q, ts)
                    if tick:
                        strategy.on_option_tick(tick)

                # 扫描 PCP 套利机会
                signals = strategy.scan_opportunities(pairs, current_time=ts)
                last_signals = [s for s in signals if s.net_profit_estimate >= min_profit]

                iteration += 1
                live.update(render())

                time.sleep(refresh_secs)

    except KeyboardInterrupt:
        pass
    finally:
        w.stop()
        console.print("\n[yellow]监控已停止，Wind 连接已断开[/yellow]")

    # 停止后显示最后的信号汇总
    if last_signals:
        console.print(f"\n[bold]最后一次扫描的套利信号（共 {len(last_signals)} 条）：[/bold]")
        for i, sig in enumerate(last_signals[:10]):
            direction = "正向" if sig.signal_type == SignalType.FORWARD else "反向"
            u_name = ETF_NAME_MAP.get(sig.underlying_code.split(".")[0], sig.underlying_code)
            console.print(
                f"  [{i+1}] {direction}  {u_name}  K={sig.strike:.4f}  "
                f"到期={sig.expiry}  净利润=[bold green]{sig.net_profit_estimate:.0f}元[/bold green]"
            )
            console.print(
                f"       Call: {sig.call_code}  买={sig.call_bid:.4f}  卖={sig.call_ask:.4f}"
            )
            console.print(
                f"       Put:  {sig.put_code}   买={sig.put_bid:.4f}  卖={sig.put_ask:.4f}"
            )
            console.print(
                f"       ETF:  {sig.spot_price:.4f}  PCP偏差={(sig.actual_spread - sig.theoretical_spread):+.4f}"
            )


# ──────────────────────────────────────────────────────────────────────
# CLI 入口
# ──────────────────────────────────────────────────────────────────────

# ──────────────────────────────────────────────────────────────────────
# ZMQ 模式监控（从 data_recorder 进程接收实时数据）
# ──────────────────────────────────────────────────────────────────────

def _restore_from_snapshot(
    strategy: "PCPArbitrage",
    snapshot_path: str,
    etf_prices: Dict[str, float],
) -> int:
    """
    从 snapshot_latest.parquet 恢复 TickAligner 状态。

    Args:
        strategy:      PCPArbitrage 实例（含 TickAligner）
        snapshot_path: snapshot_latest.parquet 路径
        etf_prices:    ETF 最新价字典（in-place 更新）

    Returns:
        恢复的 tick 条数
    """
    import math as _math
    path = Path(snapshot_path)
    if not path.exists():
        return 0
    try:
        import pandas as pd
        df = pd.read_parquet(str(path))
    except Exception as e:
        console.print(f"[yellow]快照读取失败（跳过）：{e}[/yellow]")
        return 0

    ts = datetime.now()
    count = 0
    for _, row in df.iterrows():
        try:
            last  = float(row.get("last")  or 0)
            ask1  = float(row.get("ask1")  or _math.nan)
            bid1  = float(row.get("bid1")  or _math.nan)
            code  = str(row["code"])

            if row.get("type") == "etf":
                tick = ETFTickData(
                    timestamp=ts,
                    etf_code=code,
                    price=last,
                    ask_price=ask1,
                    bid_price=bid1,
                    is_simulated=False,
                )
                strategy.on_etf_tick(tick)
                if last > 0:
                    etf_prices[code] = last
            else:
                if last <= 0 or _math.isnan(ask1) or _math.isnan(bid1):
                    continue
                tick = TickData(
                    timestamp=ts,
                    contract_code=code,
                    current=last,
                    volume=int(row.get("vol") or 0),
                    high=float(row.get("high") or last),
                    low=float(row.get("low")  or last),
                    money=0.0,
                    position=int(row.get("oi") or 0),
                    ask_prices=[ask1] + [_math.nan] * 4,
                    ask_volumes=[100]  + [0] * 4,
                    bid_prices=[bid1]  + [_math.nan] * 4,
                    bid_volumes=[100]  + [0] * 4,
                )
                strategy.on_option_tick(tick)
            count += 1
        except Exception:
            continue
    return count


def run_monitor_zmq(
    min_profit: float = 30.0,
    expiry_days: int = 90,
    refresh_secs: int = 5,
    atm_range_pct: float = 0.20,
    zmq_port: int = 5555,
    snapshot_dir: str = r"D:\MARKET_DATA",
) -> None:
    """
    ZMQ 模式监控：从 data_recorder 进程订阅实时行情，不直接连接 Wind。

    流程：
      1. 读取 snapshot_latest.parquet 恢复 TickAligner 状态（冷启动）
      2. 用 ZMQ SUB 订阅实时 tick 广播
      3. 每收到 ETF tick 或每 refresh_secs 秒 → 重新扫描 PCP 并刷新表格
    """
    try:
        import zmq
    except ImportError:
        console.print("[red]pyzmq 未安装，请执行：pip install pyzmq[/red]")
        return

    config = get_default_config()
    config.min_profit_threshold = min_profit

    # ── 1. 加载合约信息 ────────────────────────────────────────────────
    contract_mgr = ContractInfoManager()
    if not CONTRACT_INFO_CSV.exists():
        console.print(f"[red]合约信息文件不存在: {CONTRACT_INFO_CSV}[/red]")
        return
    n = contract_mgr.load_from_csv(CONTRACT_INFO_CSV)
    console.print(f"已加载 {n} 条合约信息")

    active = load_active_contracts(contract_mgr, expiry_days)
    if not active:
        console.print("[red]无活跃合约[/red]")
        return
    console.print(f"当前活跃合约（{expiry_days}天内到期）: {len(active)} 个")

    # 通过 Wind 查询真实合约乘数
    try:
        active_codes = [c.contract_code for c in active]
        n_mult = contract_mgr.load_multipliers_from_wind(active_codes)
        if n_mult > 0:
            console.print(f"[green]已从 Wind 更新 {n_mult} 个合约的真实乘数[/green]")
    except Exception:
        console.print("[yellow]Wind 乘数查询跳过（可能未连接）[/yellow]")

    etf_codes = sorted(set(c.underlying_code for c in active))

    # ── 2. 从快照恢复 TickAligner ─────────────────────────────────────
    strategy = PCPArbitrage(config)
    etf_prices: Dict[str, float] = {}
    snap_path  = str(Path(snapshot_dir) / "snapshot_latest.parquet")
    n_snap = _restore_from_snapshot(strategy, snap_path, etf_prices)
    if n_snap:
        console.print(f"[green]已从快照恢复 {n_snap} 条 tick[/green]")
    else:
        console.print("[yellow]未找到快照文件，等待第一批实时数据填充...[/yellow]")

    # 估算无实时价格时的 ETF 价格（回退）
    for code in etf_codes:
        if code not in etf_prices:
            strikes = [c.strike_price for c in active if c.underlying_code == code]
            if strikes:
                etf_prices[code] = (min(strikes) + max(strikes)) / 2

    # ── 3. 构建配对 ───────────────────────────────────────────────────
    pairs, option_codes = build_pairs_and_codes(
        contract_mgr, active, etf_prices, atm_range_pct
    )
    console.print(
        f"Call/Put 配对: [cyan]{len(pairs)}[/cyan] 组  "
        f"监控期权: [cyan]{len(option_codes)}[/cyan] 个"
    )

    # ── 4. 连接 ZMQ ──────────────────────────────────────────────────
    ctx  = zmq.Context.instance()
    sock = ctx.socket(zmq.SUB)
    sock.connect(f"tcp://127.0.0.1:{zmq_port}")
    sock.setsockopt_string(zmq.SUBSCRIBE, "")   # 订阅全部主题
    sock.setsockopt(zmq.RCVTIMEO, 100)          # 100ms 超时，保持循环可被中断

    console.print(
        f"\n[bold green]ZMQ 模式监控已启动[/bold green]  "
        f"连接 tcp://127.0.0.1:{zmq_port}  "
        f"刷新间隔 {refresh_secs}s  "
        f"最小利润 {min_profit:.0f} 元  "
        f"按 Ctrl+C 退出\n"
    )

    import json as _json
    import math as _math

    iteration   = 0
    last_scan   = datetime.now()
    last_signals: List[TradeSignal] = []
    etf_display  = dict(etf_prices)

    def render() -> RenderGroup:
        return build_display(
            last_signals, datetime.now(), etf_display,
            len(pairs), len(option_codes), iteration, min_profit
        )

    try:
        with Live(render(), console=console, refresh_per_second=1, screen=True) as live:
            while True:
                # 接收 ZMQ 消息（非阻塞循环，每次最多消费 200 条）
                msgs_recv = 0
                while msgs_recv < 200:
                    try:
                        raw = sock.recv_string()
                    except zmq.Again:
                        break  # 超时，没有新消息

                    try:
                        topic, _, body = raw.partition(" ")
                        d = _json.loads(body)
                        ts = datetime.fromtimestamp(d["ts"] / 1000)

                        if d["type"] == "etf":
                            last  = d.get("last") or 0
                            ask1  = d.get("ask1") or _math.nan
                            bid1  = d.get("bid1") or _math.nan
                            if last > 0:
                                etf_tick = ETFTickData(
                                    timestamp=ts,
                                    etf_code=d["code"],
                                    price=float(last),
                                    ask_price=float(ask1) if ask1 else _math.nan,
                                    bid_price=float(bid1) if bid1 else _math.nan,
                                    is_simulated=False,
                                )
                                strategy.on_etf_tick(etf_tick)
                                etf_display[d["code"]] = float(last)
                        else:
                            last = d.get("last") or 0
                            ask1 = d.get("ask1") or _math.nan
                            bid1 = d.get("bid1") or _math.nan
                            if last > 0 and not _math.isnan(ask1) and not _math.isnan(bid1):
                                opt_tick = TickData(
                                    timestamp=ts,
                                    contract_code=d["code"],
                                    current=float(last),
                                    volume=d.get("vol") or 0,
                                    high=float(d.get("high") or last),
                                    low=float(d.get("low")  or last),
                                    money=0.0,
                                    position=d.get("oi") or 0,
                                    ask_prices=[float(ask1)] + [_math.nan] * 4,
                                    ask_volumes=[100] + [0] * 4,
                                    bid_prices=[float(bid1)] + [_math.nan] * 4,
                                    bid_volumes=[100] + [0] * 4,
                                )
                                strategy.on_option_tick(opt_tick)
                    except Exception:
                        pass

                    msgs_recv += 1

                # 按 refresh_secs 节流扫描
                now = datetime.now()
                if (now - last_scan).total_seconds() >= refresh_secs:
                    signals      = strategy.scan_opportunities(pairs, current_time=now)
                    last_signals = [s for s in signals if s.net_profit_estimate >= min_profit]
                    iteration   += 1
                    last_scan    = now

                live.update(render())

    except KeyboardInterrupt:
        pass
    finally:
        sock.close()
        ctx.term()
        console.print("\n[yellow]ZMQ 监控已停止[/yellow]")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="PCP 套利实时监控（终端版）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python monitor_live.py                          # 直连 Wind（默认）
  python monitor_live.py --source zmq             # 从 recorder 进程读取
  python monitor_live.py --min-profit 150         # 净利润阈值
  python monitor_live.py --expiry-days 30         # 只看近月合约
  python monitor_live.py --refresh 3              # 每3秒刷新
  python monitor_live.py --atm-range 0.15        # ±15% 行权价过滤
  python monitor_live.py --source zmq --zmq-port 5556  # 自定义端口
""",
    )
    parser.add_argument(
        "--source", choices=["wind", "zmq"], default="wind",
        help="数据来源：wind=直连Wind（默认），zmq=从recorder进程读取",
    )
    parser.add_argument(
        "--min-profit", type=float, default=30.0,
        help="最小显示净利润（元/组，默认30）",
    )
    parser.add_argument(
        "--expiry-days", type=int, default=90,
        help="最大到期天数（默认90天）",
    )
    parser.add_argument(
        "--refresh", type=int, default=5,
        help="刷新间隔（秒，默认5）",
    )
    parser.add_argument(
        "--atm-range", type=float, default=0.20,
        help="ATM 距离过滤比例（默认0.20 = ±20%%）",
    )
    parser.add_argument(
        "--zmq-port", type=int, default=5555,
        help="ZMQ PUB 端口（zmq 模式专用，默认5555）",
    )
    parser.add_argument(
        "--snapshot-dir", type=str, default=r"D:\MARKET_DATA",
        help="snapshot_latest.parquet 所在目录（zmq 模式专用）",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.source == "zmq":
        run_monitor_zmq(
            min_profit    = args.min_profit,
            expiry_days   = args.expiry_days,
            refresh_secs  = args.refresh,
            atm_range_pct = args.atm_range,
            zmq_port      = args.zmq_port,
            snapshot_dir  = args.snapshot_dir,
        )
    else:
        run_monitor(
            min_profit    = args.min_profit,
            expiry_days   = args.expiry_days,
            refresh_secs  = args.refresh,
            atm_range_pct = args.atm_range,
        )
