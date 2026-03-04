#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
开盘前抓取期权链（含合约乘数）— 根目录快速调用

用法:
  python fetch_optionchain.py              # 抓取今日
  python fetch_optionchain.py --date 2026-03-04
  python -m data_engine.fetch_optionchain  # 同上
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from data_engine.fetch_optionchain import main

if __name__ == "__main__":
    sys.exit(main())
