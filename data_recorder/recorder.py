# -*- coding: utf-8 -*-
"""
数据记录主进程

职责：
  1. 启动 Wind 订阅（Push 回调模式）
  2. 从 tick 队列消费数据：写 Parquet 缓冲区 + ZMQ 广播
  3. 每 flush_interval_secs 秒将缓冲区刷入磁盘（分片 Parquet）
  4. 更新 snapshot_latest.parquet（供策略进程冷启动恢复）
  5. 15:10 触发日终合并，将当日所有分片合并为日文件
  6. Ctrl+C 优雅退出：先刷新剩余数据，再合并

运行方式：
    python data_recorder/recorder.py                   # 默认配置
    python data_recorder/recorder.py --port 5556        # 自定义 ZMQ 端口
    python data_recorder/recorder.py --flush 60         # 每60秒刷新一次
    python data_recorder/recorder.py --output D:\\DATA  # 自定义存储目录
"""

from __future__ import annotations

import argparse
import ctypes
import io
import logging
import os
import sys
import time
from datetime import date, datetime
from pathlib import Path
from queue import Empty, Queue

# ── Windows UTF-8 编码修复 ────────────────────────────────────────────
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

# 将项目根目录加入 Python 路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import get_recorder_config, RecorderConfig
from data_recorder.parquet_writer import ParquetWriter
from data_recorder.wind_subscriber import WindSubscriber, TickPacket
from data_recorder.zmq_publisher import ZMQPublisher

# ──────────────────────────────────────────────────────────────────────
# 日志配置
# ──────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("recorder")


# ──────────────────────────────────────────────────────────────────────
# 主循环
# ──────────────────────────────────────────────────────────────────────

def run(config: RecorderConfig) -> None:
    """数据记录主循环"""

    logger.info("=" * 60)
    logger.info("Wind 数据记录进程启动")
    logger.info("  品种    : %s", config.products)
    logger.info("  存储目录: %s", config.output_dir)
    logger.info("  ZMQ 端口: %d", config.zmq_port)
    logger.info("  刷新间隔: %d 秒", config.flush_interval_secs)
    logger.info("  合并时间: %02d:%02d", config.merge_hour, config.merge_minute)
    logger.info("=" * 60)

    # ── 1. 初始化各组件 ──────────────────────────────────────────────
    tick_queue = Queue(maxsize=config.queue_maxsize)

    writer    = ParquetWriter(config.output_dir, config.flush_interval_secs)
    publisher = ZMQPublisher(config.zmq_port)
    subscriber = WindSubscriber(
        products        = config.products,
        tick_queue      = tick_queue,
        batch_size      = config.batch_size,
        max_expiry_days = config.max_expiry_days,
    )

    # ── 2. 启动 Wind 订阅 ──────────────────────────────────────────────
    logger.info("正在连接 Wind 并注册订阅...")
    if not subscriber.start():
        logger.error("Wind 订阅启动失败，退出")
        publisher.close()
        return

    logger.info(
        "订阅完成：%d 个期权合约 + %d 个 ETF",
        subscriber.option_count, len(config.products),
    )
    logger.info("开始接收行情，按 Ctrl+C 退出...")

    # ── 3. 主循环 ──────────────────────────────────────────────────────
    stats_received   = 0        # 本次刷新周期内收到的 tick 数
    stats_total      = 0        # 累计 tick 数
    merge_done_today = False    # 日终合并是否已在今日执行

    try:
        while True:
            now = datetime.now()

            # 3a. 从队列消费 tick（每次最多处理 1000 条，避免单次循环过长）
            processed = 0
            while processed < 1000:
                try:
                    pkt: TickPacket = tick_queue.get_nowait()
                except Empty:
                    break

                if pkt.is_etf:
                    writer.publish_etf if False else None  # 类型提示占位
                    writer.on_etf_tick(pkt.tick_row)
                    publisher.publish_etf(pkt.tick_obj)
                else:
                    writer.on_option_tick(pkt.tick_row)
                    publisher.publish_option(pkt.tick_obj, pkt.underlying_code)

                processed += 1

            stats_received += processed
            stats_total    += processed

            # 3b. 定时刷新 Parquet 分片
            if writer.should_flush():
                written = writer.flush(now)
                if written:
                    logger.info(
                        "分片写入: %d 条 | 本周期接收: %d | 累计: %d | 队列剩余: %d",
                        written, stats_received, stats_total, tick_queue.qsize(),
                    )
                stats_received = 0

            # 3c. 日终合并（触发一次后当天不再重复）
            if (not merge_done_today
                    and now.hour == config.merge_hour
                    and now.minute >= config.merge_minute):
                logger.info("触发日终合并...")
                writer.flush(now)          # 先把剩余数据写入最后一个分片
                writer.merge_daily(now.date())
                merge_done_today = True
                logger.info("日终合并完成")

            # 日期切换：重置合并标志
            if merge_done_today and now.hour == 9:
                merge_done_today = False

            # 3d. 状态心跳（每 60 秒一次）
            _maybe_heartbeat(stats_total, tick_queue.qsize(), writer, now)

            # 没有 tick 时短暂休眠，避免空转
            if processed == 0:
                time.sleep(0.05)

    except KeyboardInterrupt:
        logger.info("收到 Ctrl+C，开始优雅退出...")

    finally:
        # ── 4. 优雅退出 ───────────────────────────────────────────────
        logger.info("正在刷新剩余 %d 条 tick 到磁盘...", tick_queue.qsize())

        # 清空队列剩余 tick
        while not tick_queue.empty():
            try:
                pkt = tick_queue.get_nowait()
                if pkt.is_etf:
                    writer.on_etf_tick(pkt.tick_row)
                else:
                    writer.on_option_tick(pkt.tick_row)
            except Empty:
                break

        writer.flush(datetime.now())

        # 如果在交易时间后退出，自动执行日终合并
        now = datetime.now()
        if now.hour >= config.merge_hour and not merge_done_today:
            logger.info("退出时触发日终合并...")
            writer.merge_daily(now.date())

        subscriber.stop()
        publisher.close()
        logger.info("数据记录进程已退出。累计接收 %d 条 tick。", stats_total)


# ──────────────────────────────────────────────────────────────────────
# 心跳状态输出
# ──────────────────────────────────────────────────────────────────────

_last_heartbeat: datetime = datetime.min

def _maybe_heartbeat(total: int, queue_size: int, writer: ParquetWriter, now: datetime) -> None:
    global _last_heartbeat
    if (now - _last_heartbeat).total_seconds() < 60:
        return
    _last_heartbeat = now
    logger.info(
        "心跳 %s | 累计 %d tick | 队列 %d | 缓冲 opt=%d etf=%d",
        now.strftime("%H:%M:%S"),
        total,
        queue_size,
        writer.opt_buffer_len,
        writer.etf_buffer_len,
    )


# ──────────────────────────────────────────────────────────────────────
# 命令行入口
# ──────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Wind 实时数据记录进程（交易时间全程运行）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python data_recorder/recorder.py
  python data_recorder/recorder.py --flush 60 --output D:\\MARKET_DATA
  python data_recorder/recorder.py --port 5556
        """,
    )
    parser.add_argument("--output", type=str, default=None,
                        help="Parquet 存储目录（默认: D:\\MARKET_DATA）")
    parser.add_argument("--port",  type=int, default=None,
                        help="ZMQ PUB 端口（默认: 5555）")
    parser.add_argument("--flush", type=int, default=None,
                        help="分片刷新间隔秒数（默认: 30）")
    parser.add_argument("--batch", type=int, default=None,
                        help="wsq 每批代码数（默认: 80）")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    config = get_recorder_config()

    if args.output:
        config.output_dir = args.output
    if args.port:
        config.zmq_port = args.port
    if args.flush:
        config.flush_interval_secs = args.flush
    if args.batch:
        config.batch_size = args.batch

    run(config)


if __name__ == "__main__":
    main()
