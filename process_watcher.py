#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
DeltaZero — 进程看门狗

使用方法:
    python process_watcher.py                              # 监控模式（5秒刷新）
    python process_watcher.py --refresh 3                  # 3秒刷新
    python process_watcher.py --market-data D:\\MARKET_DATA # 指定数据目录

    python process_watcher.py --merge                      # 合并今日分片并退出
    python process_watcher.py --merge --date 20260302      # 合并指定日期的分片
"""
from __future__ import annotations

import argparse
import ctypes
import io
import os
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# 项目根目录加入 sys.path，供 --merge 模式 import ParquetWriter
sys.path.insert(0, str(Path(__file__).resolve().parent))

# Windows UTF-8 修复（必须在 rich import 前）
if sys.platform == "win32":
    ctypes.windll.kernel32.SetConsoleOutputCP(65001)
    ctypes.windll.kernel32.SetConsoleCP(65001)
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except AttributeError:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")
    os.environ["PYTHONIOENCODING"] = "utf-8"

import psutil
from rich import box
from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

console = Console(legacy_windows=False)

# ETF 代码 → 显示名映射
_ETF_NAME: Dict[str, str] = {
    "510050.SH": "50ETF",
    "510300.SH": "300ETF",
    "510500.SH": "500ETF",
}


# ══════════════════════════════════════════════════════════════════════
# 进程检测工具
# ══════════════════════════════════════════════════════════════════════

def _cmdline(proc: psutil.Process) -> str:
    try:
        return " ".join(proc.cmdline())
    except (psutil.AccessDenied, psutil.NoSuchProcess):
        return ""


def _uptime(proc: psutil.Process) -> str:
    try:
        elapsed = datetime.now() - datetime.fromtimestamp(proc.create_time())
        h, rem = divmod(int(elapsed.total_seconds()), 3600)
        m, s = divmod(rem, 60)
        if h > 0:
            return f"{h}h{m:02d}m"
        if m > 0:
            return f"{m}m{s:02d}s"
        return f"{s}s"
    except Exception:
        return "—"


def _arg_value(proc: psutil.Process, flag: str, default: str = "") -> str:
    """从进程 cmdline 提取某个 flag 的值，如 --port 8080 → '8080'"""
    try:
        parts = proc.cmdline()
        for i, p in enumerate(parts):
            if p == flag and i + 1 < len(parts):
                return parts[i + 1]
    except Exception:
        pass
    return default


def find_recorder_processes() -> List[psutil.Process]:
    result = []
    for p in psutil.process_iter(["pid", "cmdline", "create_time"]):
        cmd = _cmdline(p)
        if "recorder.py" in cmd and "monitor" not in cmd:
            result.append(p)
    return result


def find_monitor_processes() -> List[psutil.Process]:
    """查找所有 monitor 进程。注意：若某进程 cmdline 因权限无法读取会返回空串，该进程会被漏检。"""
    result = []
    for p in psutil.process_iter(["pid", "cmdline", "create_time"]):
        cmd = _cmdline(p)
        cmd_lower = cmd.lower()
        if "monitor" in cmd_lower and "process_watcher" not in cmd_lower:
            result.append(p)
    return result


# ══════════════════════════════════════════════════════════════════════
# 数据目录统计
# ══════════════════════════════════════════════════════════════════════

def read_snapshot_stats(market_data_dir: str) -> Optional[Dict]:
    """读取 snapshot_latest.parquet，返回统计字典；失败返回 None 或 {'error': ...}"""
    snap_path = Path(market_data_dir) / "snapshot_latest.parquet"
    if not snap_path.exists():
        return None
    try:
        import pandas as pd
        df = pd.read_parquet(str(snap_path))

        if "type" in df.columns:
            opts = df[df["type"] == "option"]
            etfs = df[df["type"] == "etf"]
        else:
            opts = df
            etfs = pd.DataFrame()

        underlying_counts: Dict[str, int] = {}
        if "underlying" in df.columns:
            for code, name in _ETF_NAME.items():
                cnt = int((df["underlying"] == code).sum())
                if cnt > 0:
                    underlying_counts[name] = cnt

        adj_count = int(df["is_adjusted"].sum()) if "is_adjusted" in df.columns else 0

        mtime = datetime.fromtimestamp(snap_path.stat().st_mtime)
        return {
            "n_options": len(opts),
            "n_etf": len(etfs),
            "underlying_counts": underlying_counts,
            "adj_count": adj_count,
            "mtime": mtime,
        }
    except Exception as exc:
        return {"error": str(exc)}


def count_today_chunks(market_data_dir: str) -> Dict:
    """统计今日 chunks/ 目录下的分片文件"""
    chunks_dir = Path(market_data_dir) / "chunks"
    today_str = date.today().strftime("%Y%m%d")

    if not chunks_dir.exists():
        return {"n_opt": 0, "n_etf": 0, "total_mb": 0.0, "latest_time": None}

    opt_chunks = sorted(chunks_dir.glob(f"options_{today_str}_*.parquet"))
    etf_chunks = sorted(chunks_dir.glob(f"etf_{today_str}_*.parquet"))
    all_chunks = opt_chunks + etf_chunks

    total_bytes = sum(c.stat().st_size for c in all_chunks if c.exists())
    latest_time: Optional[datetime] = None
    if all_chunks:
        latest_mtime = max(c.stat().st_mtime for c in all_chunks if c.exists())
        latest_time = datetime.fromtimestamp(latest_mtime)

    return {
        "n_opt": len(opt_chunks),
        "n_etf": len(etf_chunks),
        "total_mb": total_bytes / (1024 * 1024),
        "latest_time": latest_time,
    }


# ══════════════════════════════════════════════════════════════════════
# 面板构建
# ══════════════════════════════════════════════════════════════════════

def _kv_table() -> Table:
    tbl = Table(box=box.SIMPLE, show_header=False, padding=(0, 1), expand=True)
    tbl.add_column("k", style="dim", width=14, no_wrap=True)
    tbl.add_column("v")
    return tbl


def build_recorder_panel(market_data_dir: str) -> Panel:
    procs = find_recorder_processes()
    snap = read_snapshot_stats(market_data_dir)
    chunks = count_today_chunks(market_data_dir)

    tbl = _kv_table()

    # 进程状态行
    if procs:
        proc = procs[0]
        st = Text()
        st.append("● 运行中", style="bold bright_green")
        st.append(f"   PID {proc.pid}", style="dim")
        st.append(f"   已运行 {_uptime(proc)}", style="dim")
        if len(procs) > 1:
            st.append(f"   ⚠ {len(procs)} 个实例！", style="bold yellow")
        tbl.add_row("进程状态", st)
    else:
        tbl.add_row("进程状态", Text("● 未运行", style="bold red"))

    # 快照统计
    if snap is None:
        tbl.add_row("快照文件", Text("不存在（recorder 尚未写入）", style="dim"))
    elif "error" in snap:
        tbl.add_row("快照读取", Text(snap["error"], style="red"))
    else:
        # 品种覆盖
        if snap["underlying_counts"]:
            parts = [
                f"[cyan]{name}[/cyan] [yellow]{cnt}[/yellow]合约"
                for name, cnt in snap["underlying_counts"].items()
            ]
            tbl.add_row("品种覆盖", Text.from_markup("   ".join(parts)))
        else:
            tbl.add_row("品种覆盖", Text("—", style="dim"))

        # 期权 / ETF 行数
        opt_str = f"[white]{snap['n_options']}[/white] 合约"
        if snap["adj_count"]:
            opt_str += f"   其中调整型 [yellow]{snap['adj_count']}[/yellow]"
        tbl.add_row("期权快照", Text.from_markup(opt_str))
        tbl.add_row("ETF 快照", Text.from_markup(f"[white]{snap['n_etf']}[/white] 条"))

        # 快照更新时间
        age_secs = (datetime.now() - snap["mtime"]).total_seconds()
        age_color = "bright_green" if age_secs < 60 else ("yellow" if age_secs < 300 else "red")
        age_str = f"{int(age_secs)}s 前" if age_secs < 3600 else f"{int(age_secs/3600)}h 前"
        tbl.add_row(
            "快照更新",
            Text.from_markup(
                f"[{age_color}]{snap['mtime'].strftime('%H:%M:%S')}  ({age_str})[/{age_color}]"
            ),
        )

    # 今日分片
    if chunks["n_opt"] + chunks["n_etf"] > 0:
        chunk_parts = (
            f"期权 [white]{chunks['n_opt']}[/white] 个   "
            f"ETF [white]{chunks['n_etf']}[/white] 个   "
            f"共 [white]{chunks['total_mb']:.1f}[/white] MB"
        )
        if chunks["latest_time"]:
            chunk_parts += f"   最新 [dim]{chunks['latest_time'].strftime('%H:%M:%S')}[/dim]"
        tbl.add_row("今日分片", Text.from_markup(chunk_parts))
    else:
        tbl.add_row("今日分片", Text("暂无", style="dim"))

    border = "bright_cyan" if procs else "red"
    return Panel(tbl, title="[bold]Recorder — 数据记录[/bold]", border_style=border)


# 数据来源 → 用途说明（wind 直连 Wind 获取行情，zmq 从 recorder 读 ZMQ）
_SOURCE_LABELS: Dict[str, str] = {
    "wind": "wind (直连 Wind 行情)",
    "zmq": "zmq (读 recorder ZMQ)",
}


def build_monitor_panel() -> Panel:
    procs = find_monitor_processes()

    if not procs:
        return Panel(
            Text("● 未运行", style="bold red"),
            title="[bold]Monitor — 进程监控[/bold]",
            border_style="red",
        )

    # 按数据来源排序：wind 在前，zmq 在后
    def _source_order(p: psutil.Process) -> int:
        s = _arg_value(p, "--source", "wind").lower()
        return 0 if s == "wind" else 1

    procs = sorted(procs, key=_source_order)

    border = "bright_green" if len(procs) == 1 else "yellow"

    tbl = Table(box=box.SIMPLE, show_header=True, padding=(0, 1), expand=True)
    tbl.add_column("PID", style="dim", width=7)
    tbl.add_column("数据来源", width=24)
    tbl.add_column("最小利润", width=8, justify="right")
    tbl.add_column("到期天数", width=8, justify="right")
    tbl.add_column("运行时长", width=8, justify="right")

    for proc in procs:
        source_raw = _arg_value(proc, "--source", "wind").lower()
        source_label = _SOURCE_LABELS.get(source_raw, f"{source_raw} (未知)")
        min_profit = _arg_value(proc, "--min-profit", "30")
        expiry_days = _arg_value(proc, "--expiry-days", "90")
        tbl.add_row(
            str(proc.pid),
            f"[cyan]{source_label}[/cyan]",
            f"{min_profit} 元",
            f"{expiry_days} 天",
            _uptime(proc),
        )

    return Panel(
        tbl,
        title=f"[bold]Monitor — 进程监控   ({len(procs)} 个实例)[/bold]",
        border_style=border,
    )


def build_display(market_data_dir: str, iteration: int, refresh_secs: int) -> Group:
    ts = datetime.now()
    header = Panel(
        Text.from_markup(
            f"[bold bright_green]DeltaZero 进程看门狗[/bold bright_green]   "
            f"[dim]{ts.strftime('%Y-%m-%d  %H:%M:%S')}   "
            f"第 {iteration} 次刷新   每 {refresh_secs}s 刷新   Ctrl+C 退出[/dim]\n"
            f"[dim]数据目录: {market_data_dir}[/dim]"
        ),
        box=box.MINIMAL,
        padding=(0, 2),
    )
    return Group(
        header,
        build_recorder_panel(market_data_dir),
        build_monitor_panel(),
    )


# ══════════════════════════════════════════════════════════════════════
# --merge 模式
# ══════════════════════════════════════════════════════════════════════

def run_merge(market_data_dir: str, date_str: Optional[str]) -> None:
    """合并指定日期（默认今日）的 chunks/ 分片文件，完成后删除分片。"""
    if date_str:
        try:
            target_date = datetime.strptime(date_str, "%Y%m%d").date()
        except ValueError:
            console.print(f"[red]日期格式错误：{date_str}，应为 YYYYMMDD（如 20260303）[/red]")
            return
    else:
        target_date = date.today()

    d_str = target_date.strftime("%Y%m%d")
    console.print(f"\n[bold]合并日期[/bold]  [cyan]{target_date.strftime('%Y-%m-%d')}[/cyan]")
    console.print(f"[bold]数据目录[/bold]  [cyan]{market_data_dir}[/cyan]\n")

    # 若 recorder 正在运行，给出警告
    rec_procs = find_recorder_processes()
    if rec_procs:
        console.print(
            f"[bold yellow]⚠ 警告[/bold yellow]: Recorder 进程仍在运行 (PID {rec_procs[0].pid})。\n"
            "  Recorder 会在 15:10 自动合并当日分片。\n"
            "  现在手动合并可能导致分片不完整。\n"
        )
        try:
            ans = input("是否仍要继续？[y/N] ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            ans = "n"
        if ans != "y":
            console.print("[yellow]已取消[/yellow]")
            return
        console.print()

    # 检查分片是否存在
    chunks_dir = Path(market_data_dir) / "chunks"
    opt_chunks = sorted(chunks_dir.glob(f"options_{d_str}_*.parquet")) if chunks_dir.exists() else []
    etf_chunks = sorted(chunks_dir.glob(f"etf_{d_str}_*.parquet")) if chunks_dir.exists() else []

    if not opt_chunks and not etf_chunks:
        console.print(f"[yellow]未找到 {target_date.strftime('%Y-%m-%d')} 的分片文件，无需合并[/yellow]")
        return

    console.print(
        f"发现分片：期权 [white]{len(opt_chunks)}[/white] 个   "
        f"ETF [white]{len(etf_chunks)}[/white] 个\n"
    )

    try:
        from data_recorder.parquet_writer import ParquetWriter
        writer = ParquetWriter(market_data_dir)
        writer.merge_daily(target_date)
    except Exception as exc:
        console.print(f"[bold red]合并失败：{exc}[/bold red]")
        return

    # 显示输出文件信息
    for prefix in ("options", "etf"):
        out = Path(market_data_dir) / f"{prefix}_{d_str}.parquet"
        if out.exists():
            size_mb = out.stat().st_size / (1024 * 1024)
            console.print(f"[green]✓[/green]  {out.name}   ({size_mb:.1f} MB)")

    console.print("\n[bold green]合并完成[/bold green]")


# ══════════════════════════════════════════════════════════════════════
# 入口
# ══════════════════════════════════════════════════════════════════════

def _relaunch_in_new_window() -> bool:
    """
    若 sys.argv 包含 --new-window，在新 cmd 窗口重启本脚本（去掉该标志）并退出。
    仅 Windows 有效；其他平台静默忽略该标志。
    返回 True 表示已重启（调用方应立即 return）。
    """
    if "--new-window" not in sys.argv:
        return False
    if sys.platform == "win32":
        import subprocess
        cmd = [sys.executable, str(Path(__file__).resolve())] + [
            a for a in sys.argv[1:] if a != "--new-window"
        ]
        subprocess.Popen(cmd, creationflags=subprocess.CREATE_NEW_CONSOLE)
    return True


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="DeltaZero 进程看门狗",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python process_watcher.py                                # 监控模式（5秒刷新）
  python process_watcher.py --refresh 3                    # 3秒刷新
  python process_watcher.py --market-data D:\\MARKET_DATA   # 指定数据目录
  python process_watcher.py --new-window                   # 在新窗口中启动（Windows）
  python process_watcher.py --merge                        # 合并今日分片并退出
  python process_watcher.py --merge --date 20260302        # 合并指定日期的分片
  python process_watcher.py --debug                         # 调试：列出 monitor 进程，排查漏检
""",
    )
    parser.add_argument(
        "--market-data",
        default=r"D:\MARKET_DATA",
        help="MARKET_DATA 目录路径（默认 D:\\MARKET_DATA）",
    )
    parser.add_argument(
        "--refresh",
        type=int,
        default=5,
        help="监控模式刷新间隔（秒，默认 5）",
    )
    parser.add_argument(
        "--merge",
        action="store_true",
        help="合并当日（或 --date 指定日期）的分片文件并退出",
    )
    parser.add_argument(
        "--date",
        default=None,
        metavar="YYYYMMDD",
        help="合并指定日期，格式 YYYYMMDD（默认今日，仅与 --merge 配合使用）",
    )
    parser.add_argument(
        "--new-window",
        action="store_true",
        help="在新终端窗口中启动本程序（仅 Windows）",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="调试模式：列出所有含 monitor 的 Python 进程及其 cmdline，用于排查漏检",
    )
    return parser.parse_args()


def _run_debug() -> None:
    """调试模式：列出所有含 monitor 的 Python 进程，便于排查漏检"""
    console.print("[bold]调试：扫描含 'monitor' 的进程[/bold]\n")
    found = 0
    for p in psutil.process_iter(["pid", "cmdline", "create_time"]):
        try:
            cmd = _cmdline(p)
            if not cmd:
                continue
            cmd_lower = cmd.lower()
            if "monitor" not in cmd_lower:
                continue
            if "process_watcher" in cmd_lower:
                console.print(f"[dim]排除 process_watcher: PID {p.pid}[/dim]")
                continue
            found += 1
            console.print(f"  PID {p.pid}  运行 {_uptime(p)}")
            console.print(f"    [dim]{cmd[:120]}{'...' if len(cmd) > 120 else ''}[/dim]\n")
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    console.print(f"[bold]共发现 {found} 个 monitor 实例[/bold]")


def main() -> None:
    if _relaunch_in_new_window():
        return

    args = parse_args()

    if args.debug:
        _run_debug()
        return

    if args.merge:
        run_merge(args.market_data, args.date)
        return

    # 监控模式
    iteration = 0
    try:
        with Live(
            build_display(args.market_data, iteration, args.refresh),
            console=console,
            refresh_per_second=0.5,
            screen=True,
        ) as live:
            while True:
                iteration += 1
                live.update(build_display(args.market_data, iteration, args.refresh))
                time.sleep(args.refresh)
    except KeyboardInterrupt:
        pass
    finally:
        console.print("\n[yellow]看门狗已退出[/yellow]")


if __name__ == "__main__":
    main()
