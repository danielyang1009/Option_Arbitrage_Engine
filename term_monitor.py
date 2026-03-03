# -*- coding: utf-8 -*-
"""
PCP 套利实时监控（终端版）

运行方法:
    python term_monitor.py                          # 直连 Wind（默认）
    python term_monitor.py --source zmq             # 从 recorder 进程读取
    python term_monitor.py --min-profit 150         # 净利润阈值
    python term_monitor.py --expiry-days 30         # 只看近月合约
    python term_monitor.py --refresh 3              # 每3秒刷新
"""

from __future__ import annotations

import argparse
import math
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

# Windows UTF-8 修复必须在 rich 之前
from monitor_common import fix_windows_encoding
fix_windows_encoding()

sys.path.insert(0, str(Path(__file__).parent))

from rich import box
from rich.console import Console, Group as RenderGroup
from rich.live import Live
from rich.panel import Panel
from rich.table import Table

from monitor_common import (
    CONTRACT_INFO_CSV,
    ETF_NAME_MAP,
    ETF_ORDER,
    build_pairs_and_codes,
    estimate_etf_fallback_prices,
    init_strategy_and_contracts,
    load_active_contracts,
    parse_zmq_message,
    restore_from_snapshot,
)
from models import (
    ETFTickData,
    SignalType,
    TickData,
    TradeSignal,
    normalize_code,
)
from strategies.pcp_arbitrage import PCPArbitrage

console = Console(legacy_windows=False, highlight=True)

import logging
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stderr,
)

# ──────────────────────────────────────────────────────────────────────
# Wind 行情工具（仅 Wind 模式使用）
# ──────────────────────────────────────────────────────────────────────

WIND_OPTION_FIELDS = "rt_last,rt_ask1,rt_bid1"
WIND_ETF_FIELDS    = "rt_last,rt_ask1,rt_bid1"
WIND_BATCH_SIZE    = 300


def _fval(d: dict, key: str, default: float = math.nan) -> float:
    v = d.get(key)
    if v is None:
        return default
    try:
        f = float(v)
        return default if math.isnan(f) else f
    except (TypeError, ValueError):
        return default


def _ival(d: dict, key: str, default: int = 0) -> int:
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
    """批量拉取 Wind 实时行情快照（同步 wsq）"""
    if cancel_before:
        try:
            w.cancelRequest(0)
        except Exception:
            pass

    out: Dict[str, Dict] = {}
    for i in range(0, len(codes), WIND_BATCH_SIZE):
        if i > 0:
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
    """将 Wind 行情字典转为 TickData"""
    last = _fval(q, "RT_LAST", 0.0)
    ask1 = _fval(q, "RT_ASK1")
    bid1 = _fval(q, "RT_BID1")
    if last <= 0 or math.isnan(ask1) or math.isnan(bid1):
        return None
    if ask1 <= 0 or bid1 <= 0 or ask1 < bid1:
        return None
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
# Rich 显示构建
# ──────────────────────────────────────────────────────────────────────

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
    u_name = ETF_NAME_MAP.get(underlying.split(".")[0], underlying)
    n_fwd = sum(1 for s in sigs if s.signal_type == SignalType.FORWARD)
    n_rev = len(sigs) - n_fwd
    border = "bright_green" if n_fwd > 0 else (_ETF_BORDER.get(underlying, "dim") if sigs else "dim")

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
    tbl.add_column("到期",   style="dim", width=5,  justify="center")
    tbl.add_column("行权价",             width=7,  justify="right")
    tbl.add_column("方向",               width=4,  justify="center")
    tbl.add_column("乘数",               width=5,  justify="right")
    tbl.add_column("C_b",               width=7,  justify="right")
    tbl.add_column("C_a",               width=7,  justify="right")
    tbl.add_column("P_b",               width=7,  justify="right")
    tbl.add_column("P_a",               width=7,  justify="right")
    tbl.add_column("S",                 width=7,  justify="right")
    tbl.add_column("净利润", style="bold", width=8,  justify="right")
    tbl.add_column("明细",   style="dim", min_width=30)

    if not sigs:
        tbl.add_row(*["—"] * 9, "[dim]暂无机会[/dim]", "—")
        return tbl

    normal = sorted(
        [s for s in sigs if not s.is_adjusted], key=lambda s: (s.expiry, s.strike)
    )
    adjusted = sorted(
        [s for s in sigs if s.is_adjusted], key=lambda s: (s.expiry, s.strike)
    )

    def _add_sig_row(sig: TradeSignal, is_adj: bool) -> None:
        profit = sig.net_profit_estimate
        if profit >= 200:
            ps = "bold bright_green"
        elif profit >= 100:
            ps = "bold green"
        else:
            ps = "yellow"
        dir_str = (
            "[bold]正向[/bold]"
            if sig.signal_type == SignalType.FORWARD
            else "[italic dim]反向[/italic dim]"
        )
        mult_str = (
            f"[bold yellow]{sig.multiplier}[/bold yellow]"
            if sig.multiplier != 10000
            else "[dim]10000[/dim]"
        )
        adj_tag = "[dim italic](A)[/dim italic] " if is_adj else ""
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

    groups: Dict[str, List[TradeSignal]] = defaultdict(list)
    for sig in signals:
        groups[sig.underlying_code].append(sig)

    underlying_list = [c for c in ETF_ORDER if c in etf_prices]
    tables = [
        _build_etf_table(u, groups.get(u, []), etf_prices.get(u, 0.0))
        for u in underlying_list
    ]

    return RenderGroup(header, *tables)


# ──────────────────────────────────────────────────────────────────────
# 主逻辑：Wind 模式
# ──────────────────────────────────────────────────────────────────────

def run_monitor(
    min_profit: float = 30.0,
    expiry_days: int = 90,
    refresh_secs: int = 5,
    atm_range_pct: float = 0.20,
) -> None:
    """Wind 直连监控主循环"""
    console.print("[bold]正在导入 WindPy...[/bold]", end=" ")
    try:
        from WindPy import w
    except ImportError:
        console.print("[red]失败：WindPy 未安装[/red]")
        return
    console.print("[green]OK[/green]")

    console.print("[bold]正在连接 Wind 终端...[/bold]", end=" ")
    result = w.start()
    if result.ErrorCode != 0:
        console.print(f"[red]失败 (ErrorCode={result.ErrorCode})[/red]")
        return
    console.print("[green]连接成功[/green]")

    etf_prices: Dict[str, float] = {}
    try:
        strategy, contract_mgr, active, pairs, option_codes, etf_codes = (
            init_strategy_and_contracts(
                min_profit, expiry_days, atm_range_pct, etf_prices,
                log_fn=lambda msg: console.print(msg),
            )
        )
    except (FileNotFoundError, RuntimeError) as e:
        console.print(f"[red]{e}[/red]")
        w.stop()
        return

    # Wind 初始 ETF 价格
    etf_snap = poll_snapshot(w, etf_codes, WIND_ETF_FIELDS)
    for code in etf_codes:
        q = etf_snap.get(code, {})
        px = _fval(q, "RT_LAST", 0.0)
        name = ETF_NAME_MAP.get(code.split(".")[0], code)
        if px > 0:
            etf_prices[code] = px
            console.print(f"  {name} ({code}): [yellow]{px:.4f}[/yellow]")
        elif code not in etf_prices:
            console.print(f"  {name} ({code}): [dim]使用估算 {etf_prices.get(code, 0):.4f}[/dim]")

    console.print(
        f"\n[bold green]开始实时监控[/bold green]  "
        f"刷新间隔 {refresh_secs}s  最小利润 {min_profit:.0f} 元  按 Ctrl+C 退出\n"
    )

    iteration = 0
    last_signals: List[TradeSignal] = []
    etf_display: Dict[str, float] = dict(etf_prices)

    def render() -> RenderGroup:
        return build_display(
            last_signals, datetime.now(), etf_display,
            len(pairs), len(option_codes), iteration, min_profit,
        )

    try:
        with Live(render(), console=console, refresh_per_second=0.5, screen=True) as live:
            while True:
                ts = datetime.now()
                etf_snap = poll_snapshot(w, etf_codes, WIND_ETF_FIELDS, cancel_before=True)
                for code, q in etf_snap.items():
                    tick = make_etf_tick(code, q, ts)
                    if tick:
                        strategy.on_etf_tick(tick)
                        etf_display[code] = tick.price

                opt_snap = poll_snapshot(w, option_codes, WIND_OPTION_FIELDS)
                for code, q in opt_snap.items():
                    tick = make_option_tick(code, q, ts)
                    if tick:
                        strategy.on_option_tick(tick)

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


# ──────────────────────────────────────────────────────────────────────
# 主逻辑：ZMQ 模式
# ──────────────────────────────────────────────────────────────────────

def run_monitor_zmq(
    min_profit: float = 30.0,
    expiry_days: int = 90,
    refresh_secs: int = 5,
    atm_range_pct: float = 0.20,
    zmq_port: int = 5555,
    snapshot_dir: str = r"D:\MARKET_DATA",
) -> None:
    """ZMQ 订阅模式监控：从 data_recorder 进程接收实时行情。"""
    try:
        import zmq
    except ImportError:
        console.print("[red]pyzmq 未安装，请执行：pip install pyzmq[/red]")
        return

    etf_prices: Dict[str, float] = {}

    # 从快照恢复
    n_snap = 0
    # 先用临时 strategy 做快照恢复，后面 init 会创建正式的
    from config.settings import get_default_config
    tmp_config = get_default_config()
    tmp_config.min_profit_threshold = min_profit
    tmp_strategy = PCPArbitrage(tmp_config)
    n_snap = restore_from_snapshot(tmp_strategy, snapshot_dir, etf_prices)
    if n_snap:
        console.print(f"[green]已从快照恢复 {n_snap} 条 tick[/green]")
    else:
        console.print("[yellow]未找到快照文件，等待第一批实时数据填充...[/yellow]")

    try:
        strategy, contract_mgr, active, pairs, option_codes, etf_codes = (
            init_strategy_and_contracts(
                min_profit, expiry_days, atm_range_pct, etf_prices,
                log_fn=lambda msg: console.print(msg),
            )
        )
    except (FileNotFoundError, RuntimeError) as e:
        console.print(f"[red]{e}[/red]")
        return

    # 将快照恢复的状态注入正式 strategy
    if n_snap > 0:
        restore_from_snapshot(strategy, snapshot_dir, etf_prices)

    ctx = zmq.Context.instance()
    sock = ctx.socket(zmq.SUB)
    sock.connect(f"tcp://127.0.0.1:{zmq_port}")
    sock.setsockopt_string(zmq.SUBSCRIBE, "")
    sock.setsockopt(zmq.RCVTIMEO, 100)

    console.print(
        f"\n[bold green]ZMQ 模式监控已启动[/bold green]  "
        f"连接 tcp://127.0.0.1:{zmq_port}  "
        f"刷新间隔 {refresh_secs}s  最小利润 {min_profit:.0f} 元  按 Ctrl+C 退出\n"
    )

    iteration = 0
    last_scan = datetime.now()
    last_signals: List[TradeSignal] = []
    etf_display = dict(etf_prices)

    def render() -> RenderGroup:
        return build_display(
            last_signals, datetime.now(), etf_display,
            len(pairs), len(option_codes), iteration, min_profit,
        )

    try:
        with Live(render(), console=console, refresh_per_second=1, screen=True) as live:
            while True:
                msgs_recv = 0
                while msgs_recv < 200:
                    try:
                        raw = sock.recv_string()
                    except zmq.Again:
                        break

                    tick = parse_zmq_message(raw)
                    if tick is not None:
                        if isinstance(tick, ETFTickData):
                            strategy.on_etf_tick(tick)
                            etf_display[tick.etf_code] = tick.price
                        else:
                            strategy.on_option_tick(tick)
                    msgs_recv += 1

                now = datetime.now()
                if (now - last_scan).total_seconds() >= refresh_secs:
                    signals = strategy.scan_opportunities(pairs, current_time=now)
                    last_signals = [
                        s for s in signals if s.net_profit_estimate >= min_profit
                    ]
                    iteration += 1
                    last_scan = now
                    live.update(render())

    except KeyboardInterrupt:
        pass
    finally:
        sock.close()
        ctx.term()
        console.print("\n[yellow]ZMQ 监控已停止[/yellow]")


# ──────────────────────────────────────────────────────────────────────
# CLI 入口
# ──────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="PCP 套利实时监控（终端版）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python term_monitor.py                          # 直连 Wind（默认）
  python term_monitor.py --source zmq             # 从 recorder 进程读取
  python term_monitor.py --min-profit 150         # 净利润阈值
  python term_monitor.py --expiry-days 30         # 只看近月合约
  python term_monitor.py --refresh 3              # 每3秒刷新
  python term_monitor.py --source zmq --zmq-port 5556
""",
    )
    parser.add_argument(
        "--source", choices=["wind", "zmq"], default="wind",
        help="数据来源：wind=直连Wind（默认），zmq=从recorder进程读取",
    )
    parser.add_argument("--min-profit", type=float, default=30.0, help="最小显示净利润（元/组）")
    parser.add_argument("--expiry-days", type=int, default=90, help="最大到期天数")
    parser.add_argument("--refresh", type=int, default=5, help="刷新间隔（秒）")
    parser.add_argument("--atm-range", type=float, default=0.20, help="ATM 距离过滤比例")
    parser.add_argument("--zmq-port", type=int, default=5555, help="ZMQ PUB 端口")
    parser.add_argument("--snapshot-dir", type=str, default=r"D:\MARKET_DATA", help="快照文件目录")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.source == "zmq":
        run_monitor_zmq(
            min_profit=args.min_profit,
            expiry_days=args.expiry_days,
            refresh_secs=args.refresh,
            atm_range_pct=args.atm_range,
            zmq_port=args.zmq_port,
            snapshot_dir=args.snapshot_dir,
        )
    else:
        run_monitor(
            min_profit=args.min_profit,
            expiry_days=args.expiry_days,
            refresh_secs=args.refresh,
            atm_range_pct=args.atm_range,
        )
