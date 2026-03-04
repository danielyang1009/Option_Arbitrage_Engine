# -*- coding: utf-8 -*-
"""
DeltaZero — PCP 正向套利实时监控

运行方法:
    python monitor.py                          # 直连 Wind（默认）
    python monitor.py --source zmq             # 从 recorder 进程读取
    python monitor.py --min-profit 150         # 净利润阈值
    python monitor.py --expiry-days 30         # 只看近月合约
    python monitor.py --refresh 3              # 每3秒刷新
"""

from __future__ import annotations

import argparse
import sys
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

# Windows UTF-8 修复必须在 rich 之前
from monitors.common import fix_windows_encoding
fix_windows_encoding()

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from rich import box
from rich.console import Console, Group as RenderGroup
from rich.live import Live
from rich.panel import Panel
from rich.table import Table

from monitors.common import (
    ETF_NAME_MAP,
    ETF_ORDER,
    build_pairs_and_codes,
    estimate_etf_fallback_prices,
    init_strategy_and_contracts,
    parse_zmq_message,
    restore_from_snapshot,
    select_display_pairs,
)
from config.settings import DEFAULT_MARKET_DATA_DIR, UNDERLYINGS
from models import (
    ETFTickData,
    TradeSignal,
    normalize_code,
)
from calculators.vix_engine import VIXEngine, VIXResult
from strategies.pcp_arbitrage import PCPArbitrage
from utils.wind_helpers import (
    fval,
    wind_connect,
    wind_row_to_etf_tick,
    wind_row_to_option_tick,
)

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
WIND_ETF_FIELDS = "rt_last,rt_ask1,rt_bid1"
WIND_BATCH_SIZE = 300


def poll_snapshot(
    w,
    codes: List[str],
    fields: str = WIND_OPTION_FIELDS,
    cancel_before: bool = False,
    timeout_sec: int = 5,
) -> Dict[str, Dict[str, float]]:
    """批量拉取 Wind 实时行情快照（同步 wsq）"""
    if cancel_before:
        try:
            w.cancelRequest(0)
        except Exception:
            pass

    out: Dict[str, Dict[str, float]] = {}
    for i in range(0, len(codes), WIND_BATCH_SIZE):
        if i > 0:
            try:
                w.cancelRequest(0)
            except Exception:
                pass
        batch = codes[i : i + WIND_BATCH_SIZE]
        try:
            with ThreadPoolExecutor(max_workers=1) as ex:
                result = ex.submit(w.wsq, ",".join(batch), fields).result(timeout=timeout_sec)
        except (FuturesTimeoutError, Exception):
            continue
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


# ──────────────────────────────────────────────────────────────────────
# Rich 显示构建
# ──────────────────────────────────────────────────────────────────────

_ETF_BORDER = {
    "510050.SH": "bright_cyan",
    "510300.SH": "bright_blue",
    "510500.SH": "bright_magenta",
}

_VIX_TARGETS = list(UNDERLYINGS)


def _build_etf_table(
    underlying: str,
    sigs: List[TradeSignal],
    price: float,
    min_profit: float,
    vix_value: Optional[float] = None,
) -> Table:
    """为单个品种构建信号表格（固定显示 ATM 上下各 10 行）"""
    u_name = ETF_NAME_MAP.get(underlying.split(".")[0], underlying)
    n_profitable = sum(1 for s in sigs if s.net_profit_estimate >= min_profit)
    border = "bright_green" if n_profitable > 0 else (_ETF_BORDER.get(underlying, "dim") if sigs else "dim")

    title_parts = [f"[bold]{u_name}[/bold]"]
    if price > 0:
        title_parts.append(f"[yellow]{price:.4f}[/yellow]")
    if vix_value is not None:
        title_parts.append(f"[magenta]VIX {vix_value:.2f}[/magenta]")
    if n_profitable > 0:
        title_parts.append(f"[bold bright_green]正向 {n_profitable} 条[/bold bright_green]")
    if not sigs:
        title_parts.append("[dim]暂无数据[/dim]")

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
    tbl.add_column("行权价",             width=8,  justify="left")
    tbl.add_column("方向",               width=4,  justify="center")
    tbl.add_column("净利润",             width=8,  justify="right")
    tbl.add_column("乘数",               width=6,  justify="right")
    tbl.add_column("C_b",               width=7,  justify="right")
    tbl.add_column("C_a",               width=7,  justify="right")
    tbl.add_column("P_b",               width=7,  justify="right")
    tbl.add_column("P_a",               width=7,  justify="right")
    tbl.add_column("S",                 width=7,  justify="right")
    tbl.add_column("明细",   style="dim", min_width=30)

    if not sigs:
        tbl.add_row(*["—"] * 9, "[dim]暂无数据[/dim]", "—")
        return tbl

    def _add_sig_row(sig: TradeSignal) -> None:
        profit = sig.net_profit_estimate
        is_adj = sig.is_adjusted

        if profit >= min_profit:
            profit_str = f"[bold green]{profit:.0f}[/bold green]"
            dir_str = "[bold green]正向[/bold green]"
        elif profit >= 0:
            profit_str = f"{profit:.0f}"
            dir_str = "正向"
        else:
            profit_str = f"[dim]{profit:.0f}[/dim]"
            dir_str = ""

        mult_str = str(sig.multiplier)

        adj_tag = " [dim]A[/dim]" if is_adj else ""
        strike_str = f"{sig.strike:.2f}{adj_tag}"

        tbl.add_row(
            sig.expiry.strftime("%m-%d"),
            strike_str,
            dir_str,
            profit_str,
            mult_str,
            f"{sig.call_bid:.4f}",
            f"{sig.call_ask:.4f}",
            f"{sig.put_bid:.4f}",
            f"{sig.put_ask:.4f}",
            f"{sig.spot_price:.4f}",
            sig.calc_detail,
        )

    normal = sorted(
        [s for s in sigs if not s.is_adjusted], key=lambda s: (s.expiry, s.strike)
    )
    adjusted = sorted(
        [s for s in sigs if s.is_adjusted], key=lambda s: (s.expiry, s.strike)
    )

    for sig in normal:
        _add_sig_row(sig)
    if adjusted and normal:
        tbl.add_section()
    for sig in adjusted:
        _add_sig_row(sig)

    return tbl


def build_display(
    all_display_signals: List[TradeSignal],
    ts: datetime,
    etf_prices: Dict[str, float],
    vix_values: Dict[str, Optional[float]],
    n_pairs: int,
    n_option_codes: int,
    iteration: int,
    min_profit: float,
    n_each_side: int = 10,
) -> RenderGroup:
    """构建套利信号布局，按品种分块显示（每品种固定 ATM 上下各 n_each_side 行）"""
    etf_line = "  ".join(
        f"[cyan]{ETF_NAME_MAP.get(c.split('.')[0], c)}[/cyan]=[bold yellow]{p:.4f}[/bold yellow]"
        for c, p in etf_prices.items()
        if p > 0
    )
    vix_line = "  ".join(
        f"[magenta]{ETF_NAME_MAP.get(c.split('.')[0], c)}-VIX[/magenta]="
        f"{('[bold white]' + f'{vix_values[c]:.2f}' + '[/bold white]') if vix_values.get(c) is not None else '[dim]--[/dim]'}"
        for c in _VIX_TARGETS
    )
    n_profitable = sum(1 for s in all_display_signals if s.net_profit_estimate >= min_profit)
    header = Panel(
        f"[bold bright_green]⚡ DeltaZero 正向套利监控[/bold bright_green]  "
        f"[dim]{ts.strftime('%H:%M:%S')}[/dim]  第 {iteration} 次刷新\n"
        f"{etf_line}\n"
        f"{vix_line}\n"
        f"[dim]监控配对: {n_pairs} 组  订阅期权: {n_option_codes} 个  "
        f"有报价: {len(all_display_signals)} 条  "
        f"[bold bright_green]正向机会 (≥{min_profit:.0f}元): {n_profitable}[/bold bright_green][/dim]",
        box=box.MINIMAL,
        padding=(0, 2),
    )

    # 按品种分组，再各自按 ATM 上下各取 n_each_side
    groups: Dict[str, List[TradeSignal]] = defaultdict(list)
    for sig in all_display_signals:
        groups[sig.underlying_code].append(sig)

    underlying_list = [c for c in ETF_ORDER if c in etf_prices]
    tables = []
    for u in underlying_list:
        etf_px = etf_prices.get(u, 0.0)
        u_sigs = groups.get(u, [])
        display_sigs = select_display_pairs(u_sigs, etf_px, n_each_side) if u_sigs else []
        tables.append(_build_etf_table(u, display_sigs, etf_px, min_profit, vix_values.get(u)))

    return RenderGroup(header, *tables)


# ──────────────────────────────────────────────────────────────────────
# 主逻辑：Wind 模式
# ──────────────────────────────────────────────────────────────────────

def run_monitor(
    min_profit: float = 30.0,
    expiry_days: int = 90,
    refresh_secs: int = 3,
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
    if not wind_connect(w, timeout=30, retries=3, delay_secs=2.0, logger=logging.getLogger(__name__)):
        console.print("[red]失败（Wind 连接超时或重试耗尽）[/red]")
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
        px = fval(q, "RT_LAST", 0.0)
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
    vix_engine = VIXEngine(risk_free_rate=strategy.config.risk_free_rate)
    vix_pairs, vix_option_codes = build_pairs_and_codes(
        contract_mgr, active, etf_prices, atm_range_pct=10.0
    )
    option_codes = sorted(set(option_codes) | set(vix_option_codes))
    vix_pairs_by_underlying: Dict[str, list] = {
        u: [p for p in vix_pairs if p[0].underlying_code == u] for u in _VIX_TARGETS
    }
    vix_last: Dict[str, VIXResult] = {}
    vix_display: Dict[str, Optional[float]] = {u: None for u in _VIX_TARGETS}

    def render() -> RenderGroup:
        return build_display(
            last_signals, datetime.now(), etf_display, vix_display,
            len(pairs), len(option_codes), iteration, min_profit,
        )

    try:
        with Live(render(), console=console, refresh_per_second=0.5, screen=True) as live:
            while True:
                ts = datetime.now()
                etf_snap = poll_snapshot(w, etf_codes, WIND_ETF_FIELDS, cancel_before=True)
                for code, q in etf_snap.items():
                    tick = wind_row_to_etf_tick(code, q, ts)
                    if tick:
                        strategy.on_etf_tick(tick)
                        etf_display[code] = tick.price

                opt_snap = poll_snapshot(w, option_codes, WIND_OPTION_FIELDS)
                for code, q in opt_snap.items():
                    tick = wind_row_to_option_tick(code, q, ts)
                    if tick:
                        strategy.on_option_tick(tick)

                last_signals = strategy.scan_pairs_for_display(pairs, current_time=ts)
                for u in _VIX_TARGETS:
                    result = vix_engine.compute_for_underlying(
                        vix_pairs_by_underlying.get(u, []),
                        strategy.aligner,
                        ts,
                        last_result=vix_last.get(u),
                        enable_republication=True,
                    )
                    if result is not None:
                        vix_last[u] = result
                        vix_display[u] = result.vix
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
    refresh_secs: int = 3,
    atm_range_pct: float = 0.20,
    zmq_port: int = 5555,
    snapshot_dir: str = DEFAULT_MARKET_DATA_DIR,
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
        f"收到新数据即刷新（空闲时每 {refresh_secs}s）  最小利润 {min_profit:.0f} 元  按 Ctrl+C 退出\n"
    )

    iteration = 0
    last_scan = datetime.now()
    last_signals: List[TradeSignal] = []
    etf_display = dict(etf_prices)
    vix_engine = VIXEngine(risk_free_rate=strategy.config.risk_free_rate)
    vix_pairs, vix_option_codes = build_pairs_and_codes(
        contract_mgr, active, etf_prices, atm_range_pct=10.0
    )
    option_codes = sorted(set(option_codes) | set(vix_option_codes))
    vix_pairs_by_underlying: Dict[str, list] = {
        u: [p for p in vix_pairs if p[0].underlying_code == u] for u in _VIX_TARGETS
    }
    vix_last: Dict[str, VIXResult] = {}
    vix_display: Dict[str, Optional[float]] = {u: None for u in _VIX_TARGETS}

    def render() -> RenderGroup:
        return build_display(
            last_signals, datetime.now(), etf_display, vix_display,
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
                # 收到新数据即刷新；无数据时按 refresh_secs 周期刷新
                should_refresh = msgs_recv > 0 or (now - last_scan).total_seconds() >= refresh_secs
                if should_refresh:
                    last_signals = strategy.scan_pairs_for_display(pairs, current_time=now)
                    for u in _VIX_TARGETS:
                        result = vix_engine.compute_for_underlying(
                            vix_pairs_by_underlying.get(u, []),
                            strategy.aligner,
                            now,
                            last_result=vix_last.get(u),
                            enable_republication=True,
                        )
                        if result is not None:
                            vix_last[u] = result
                            vix_display[u] = result.vix
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
        description="DeltaZero — PCP 正向套利实时监控",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python monitor.py                          # 直连 Wind（默认）
  python monitor.py --source zmq             # 从 recorder 进程读取
  python monitor.py --min-profit 150         # 净利润阈值
  python monitor.py --expiry-days 30         # 只看近月合约
  python monitor.py --refresh 3              # 每3秒刷新
  python monitor.py --source zmq --zmq-port 5556
""",
    )
    parser.add_argument(
        "--source", choices=["wind", "zmq"], default="wind",
        help="数据来源：wind=直连Wind（默认），zmq=从recorder进程读取",
    )
    parser.add_argument("--min-profit", type=float, default=30.0, help="最小显示净利润（元/组）")
    parser.add_argument("--expiry-days", type=int, default=90, help="最大到期天数")
    parser.add_argument("--refresh", type=int, default=3, help="刷新间隔（秒），ZMQ 模式收到新数据会立即刷新")
    parser.add_argument("--atm-range", type=float, default=0.20, help="ATM 距离过滤比例")
    parser.add_argument("--zmq-port", type=int, default=5555, help="ZMQ PUB 端口")
    parser.add_argument("--snapshot-dir", type=str, default=DEFAULT_MARKET_DATA_DIR, help="快照文件目录")
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
