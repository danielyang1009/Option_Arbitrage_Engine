"""
DeltaZero — ETF 期权 PCP 套利回测与交易预警框架

支持两种运行模式：
1. 回测模式（默认）：加载历史 Tick 数据 -> 生成信号 -> 执行回测 -> 输出分析报告
2. 监控模式：连接 Wind 实时行情 -> 持续扫描套利机会 -> 控制台/弹窗警报

使用方法：
    python main.py                          # 回测模式（使用 sample_data）
    python main.py --mode monitor           # 实盘监控模式（需要 Wind）
    python main.py --data-dir tick_data     # 指定数据目录
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from config.settings import TradingConfig, get_default_config
from models import (
    ContractInfo,
    ETFTickData,
    SignalType,
    TickData,
    TradeSignal,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("main")


def run_backtest(
    config: TradingConfig,
    data_dir: str = "sample_data",
    start_month: Optional[str] = None,
    end_month: Optional[str] = None,
    output_chart: Optional[str] = None,
    etf_data_dir: Optional[str] = None,
    bar_mode: str = "close",
) -> None:
    """
    执行回测模式的完整流程

    流程：加载数据 -> 加载合约信息 -> ETF 价格（真实K线/模拟）-> 扫描信号 -> 执行回测 -> 输出报告

    Args:
        config: 交易配置
        data_dir: 期权 Tick 数据目录
        start_month: 起始月份，格式 'YYYY-MM'（含），如 '2024-01'
        end_month: 结束月份，格式 'YYYY-MM'（含），如 '2024-06'
        output_chart: 权益曲线图保存路径（不传则不绘图）
        etf_data_dir: ETF K 线数据目录（不传则使用 GBM 模拟）
        bar_mode: K 线展开模式 "close" 或 "ohlc"
    """
    from data_engine.tick_loader import TickLoader
    from data_engine.bar_loader import BarDataLoader
    from data_engine.contract_info import ContractInfoManager
    from data_engine.etf_simulator import ETFSimulator
    from strategies.pcp_arbitrage import PCPArbitrage
    from backtest.engine import BacktestEngine, MergedTick
    from analysis.pnl import PnLAnalyzer

    # ========== 1. 加载合约基本信息 ==========
    logger.info("=" * 60)
    logger.info("Step 1: 加载合约基本信息")
    logger.info("=" * 60)

    contract_mgr = ContractInfoManager()
    csv_path = Path(config.data_paths["contract_info_csv"])
    if csv_path.exists():
        count = contract_mgr.load_from_csv(csv_path)
        logger.info("已加载 %d 条合约信息", count)
    else:
        logger.warning("合约信息文件不存在: %s", csv_path)
        logger.warning("将无法进行 Call/Put 配对，回测功能受限")

    # ========== 2. 加载 Tick 数据 ==========
    logger.info("=" * 60)
    logger.info("Step 2: 加载 Tick 数据")
    logger.info("=" * 60)

    loader = TickLoader()
    data_path = Path(data_dir)
    if not data_path.exists():
        logger.error("数据目录不存在: %s", data_path)
        return

    option_ticks = loader.load_directory(data_path, start_month=start_month, end_month=end_month)
    if not option_ticks:
        logger.error("未加载到任何 Tick 数据")
        return

    for code, ticks in option_ticks.items():
        logger.info("  合约 %s: %d 条 Tick", code, len(ticks))

    # ========== 3. 匹配合约信息并构建配对 ==========
    logger.info("=" * 60)
    logger.info("Step 3: 匹配合约信息")
    logger.info("=" * 60)

    contracts: Dict[str, ContractInfo] = {}
    underlying_codes = set()

    for code in option_ticks:
        info = contract_mgr.get_info(code)
        if info is not None:
            contracts[code] = info
            underlying_codes.add(info.underlying_code)
            logger.info(
                "  %s -> %s | %s | K=%.4f | 到期=%s",
                code, info.short_name, info.option_type.value,
                info.strike_price, info.expiry_date,
            )
        else:
            logger.warning("  %s: 未找到合约信息", code)

    if not contracts:
        logger.error("没有任何合约能匹配到基本信息，无法继续")
        return

    # 查找 Call/Put 配对
    all_pairs: List[Tuple[ContractInfo, ContractInfo]] = []
    for underlying in underlying_codes:
        expiries = contract_mgr.get_available_expiries(underlying)
        for expiry in expiries:
            pairs = contract_mgr.find_call_put_pairs(underlying, expiry=expiry)
            # 只保留在 Tick 数据中有报价的配对
            active_pairs = [
                (c, p) for c, p in pairs
                if c.contract_code in option_ticks or p.contract_code in option_ticks
            ]
            all_pairs.extend(active_pairs)

    logger.info("找到 %d 组 Call/Put 配对（有 Tick 数据）", len(all_pairs))

    # ========== 4. ETF 数据（真实 K 线 或 GBM 模拟）==========
    logger.info("=" * 60)

    etf_ticks_map: Dict[str, List[ETFTickData]] = {}

    if etf_data_dir and Path(etf_data_dir).is_dir():
        logger.info("Step 4: 加载真实 ETF K 线数据 (%s)", etf_data_dir)
        logger.info("  展开模式: %s", bar_mode)

        bar_loader = BarDataLoader(mode=bar_mode)
        etf_ticks_map = bar_loader.load_directory(
            etf_data_dir,
            pattern="*.csv",
            start_date=start_month,
            end_date=end_month,
        )

        pq_ticks = bar_loader.load_directory(
            etf_data_dir,
            pattern="*.parquet",
            start_date=start_month,
            end_date=end_month,
        )
        for code, ticks in pq_ticks.items():
            etf_ticks_map.setdefault(code, []).extend(ticks)

        missing_etf = underlying_codes - set(etf_ticks_map.keys())
        if missing_etf:
            logger.warning("以下标的无 K 线数据，将使用 GBM 模拟: %s", missing_etf)
    else:
        if etf_data_dir:
            logger.warning("ETF 数据目录不存在: %s，将使用 GBM 模拟", etf_data_dir)
        logger.info("Step 4: 模拟标的 ETF 价格 (GBM)")

    if missing_etf := (underlying_codes - set(etf_ticks_map.keys())):
        simulator = ETFSimulator(
            volatility=config.simulation_volatility,
            drift=config.simulation_drift,
            risk_free_rate=config.risk_free_rate,
            seed=42,
        )
        for underlying in missing:
            related_contracts = {
                code: info for code, info in contracts.items()
                if info.underlying_code == underlying
            }
            related_ticks = {
                code: ticks for code, ticks in option_ticks.items()
                if code in related_contracts
            }
            strike_prices = [info.strike_price for info in related_contracts.values()]
            initial_price = sum(strike_prices) / len(strike_prices) if strike_prices else 3.0

            etf_data = simulator.simulate_from_option_ticks(
                related_ticks, related_contracts, underlying, initial_price,
            )
            etf_ticks_map[underlying] = etf_data
            logger.info("  %s: GBM 模拟 %d 条 ETF Tick（初始价 ≈ %.4f）",
                        underlying, len(etf_data), initial_price)

    for code, ticks in etf_ticks_map.items():
        logger.info("  %s: %d 条 ETF 事件", code, len(ticks))

    all_etf_ticks: List[ETFTickData] = []
    for ticks in etf_ticks_map.values():
        all_etf_ticks.extend(ticks)
    all_etf_ticks.sort(key=lambda t: t.timestamp)

    # ========== 5. 执行回测 ==========
    logger.info("=" * 60)
    logger.info("Step 5: 执行 Tick-by-Tick 回测")
    logger.info("=" * 60)

    strategy = PCPArbitrage(config)
    engine = BacktestEngine(config)

    underlying_close = all_etf_ticks[0].price if all_etf_ticks else 3.0

    def strategy_callback(
        mtick: MergedTick,
        bt_engine: BacktestEngine,
    ) -> List[TradeSignal]:
        """策略回调：更新报价快照 + 扫描套利机会"""
        if mtick.tick_type == "option" and mtick.option_tick is not None:
            strategy.on_option_tick(mtick.option_tick)
        elif mtick.tick_type == "etf" and mtick.etf_tick is not None:
            strategy.on_etf_tick(mtick.etf_tick)

        return strategy.scan_opportunities(all_pairs, mtick.timestamp)

    results = engine.run(
        option_ticks=option_ticks,
        etf_ticks=all_etf_ticks,
        contracts=contracts,
        strategy_callback=strategy_callback,
        underlying_close=underlying_close,
    )

    # ========== 6. 输出分析报告 ==========
    logger.info("=" * 60)
    logger.info("Step 6: 生成分析报告")
    logger.info("=" * 60)

    analyzer = PnLAnalyzer(
        risk_free_rate=config.risk_free_rate,
        trading_days_per_year=config.trading_days_per_year,
    )

    metrics = analyzer.analyze(
        trade_history=results["trade_history"],
        signals=results["signals"],
        equity_curve=results["equity_curve"],
        initial_capital=config.initial_capital,
    )

    attribution = analyzer.calc_greeks_attribution(
        trade_history=results["trade_history"],
        signals=results["signals"],
    )

    report = analyzer.print_report(metrics, attribution)
    print(report)

    # 权益曲线图
    if output_chart and results["equity_curve"]:
        analyzer.plot_equity_curve(
            results["equity_curve"],
            title="DeltaZero — PCP 套利回测 权益曲线",
            save_path=output_chart,
        )

    # 打印信号摘要
    if results["signals"]:
        print("\n--- 套利信号摘要（前10条） ---")
        for i, sig in enumerate(results["signals"][:10]):
            direction = "正向" if sig.signal_type == SignalType.FORWARD else "反向"
            print(
                f"  [{i+1}] {sig.timestamp} | {direction} | "
                f"K={sig.strike:.4f} | 预估利润={sig.net_profit_estimate:.2f}元 | "
                f"置信度={sig.confidence:.2f}"
            )


def parse_args() -> argparse.Namespace:
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description="DeltaZero — ETF 期权 PCP 套利回测与交易预警框架",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--mode",
        choices=["backtest", "monitor"],
        default="backtest",
        help="运行模式: backtest (回测) 或 monitor (实盘监控)",
    )
    parser.add_argument(
        "--data-dir",
        default="sample_data",
        help="Tick 数据目录路径（默认: sample_data）",
    )
    parser.add_argument(
        "--capital",
        type=float,
        default=1_000_000,
        help="初始资金（默认: 1,000,000 元）",
    )
    parser.add_argument(
        "--min-profit",
        type=float,
        default=50,
        help="最小利润阈值（元/组，默认: 50）",
    )
    parser.add_argument(
        "--start-date",
        default=None,
        metavar="YYYY-MM",
        help="回测起始月份（含），如 2024-01。不传则从最早数据开始",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        metavar="YYYY-MM",
        help="回测结束月份（含），如 2024-06。不传则到最新数据结束",
    )
    parser.add_argument(
        "--output-chart",
        default=None,
        help="权益曲线图保存路径（如 output/equity.png）",
    )
    parser.add_argument(
        "--etf-data-dir",
        default=None,
        help="ETF K 线数据目录（CSV/Parquet），不传则使用 GBM 模拟",
    )
    parser.add_argument(
        "--bar-mode",
        choices=["close", "ohlc"],
        default="close",
        help="K 线展开模式: close (仅收盘价，默认) 或 ohlc (四价路径模拟)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="输出详细日志",
    )
    return parser.parse_args()


def main() -> None:
    """主入口"""
    args = parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    config = get_default_config()
    config.initial_capital = args.capital
    config.min_profit_threshold = args.min_profit

    logger.info("DeltaZero v0.1")
    logger.info("运行模式: %s", args.mode)
    logger.info("初始资金: {:,.0f} 元".format(config.initial_capital))

    if args.start_date or args.end_date:
        logger.info(
            "回测日期范围: %s ~ %s",
            args.start_date or "最早", args.end_date or "最新",
        )

    if args.mode == "backtest":
        run_backtest(
            config,
            data_dir=args.data_dir,
            start_month=args.start_date,
            end_month=args.end_date,
            output_chart=args.output_chart,
            etf_data_dir=args.etf_data_dir,
            bar_mode=args.bar_mode,
        )
    elif args.mode == "monitor":
        print("实盘监控已迁移，请使用独立入口：")
        print("  终端版: python -m monitors.term_monitor --source wind")
        print("  网页版: python -m monitors.web_monitor")


if __name__ == "__main__":
    main()
