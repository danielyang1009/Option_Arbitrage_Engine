# -*- coding: utf-8 -*-
"""
DeltaZero — PCP 套利实时监控（网页版）

启动方式:
    python web_monitor.py
    python web_monitor.py --port 8080 --min-profit 100 --expiry-days 90

浏览器访问: http://localhost:8080
数据来源:   data_recorder/recorder.py 广播的 ZMQ PUB (默认端口 5555)
"""

from __future__ import annotations

import argparse
import logging
import math
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List

from monitors.common import fix_windows_encoding
fix_windows_encoding()

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from flask import Flask, jsonify, render_template_string

from monitors.common import (
    ETF_NAME_MAP,
    ETF_ORDER,
    init_strategy_and_contracts,
    parse_zmq_message,
    restore_from_snapshot,
    signal_to_dict,
)
from models import ETFTickData, TickData

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stderr,
)
log = logging.getLogger("web_monitor")

# ──────────────────────────────────────────────────────────────────────────
# 全局共享状态（后台线程写，Flask 线程读）
# ──────────────────────────────────────────────────────────────────────────
_lock = threading.Lock()
_state: Dict = {
    "signals": [],
    "etf_prices": {},
    "n_pairs": 0,
    "n_options": 0,
    "tick_count": 0,
    "last_scan": None,
    "status": "初始化中",
    "errors": [],
}


# ──────────────────────────────────────────────────────────────────────────
# 后台 ZMQ 消费线程
# ──────────────────────────────────────────────────────────────────────────
def _zmq_worker(
    zmq_port: int,
    snapshot_dir: str,
    min_profit: float,
    expiry_days: int,
    atm_range: float,
    refresh_secs: int,
) -> None:
    try:
        import zmq
    except ImportError:
        with _lock:
            _state["status"] = "错误：pyzmq 未安装"
        return

    etf_prices: Dict[str, float] = {}
    n_snap = 0

    try:
        from config.settings import get_default_config
        from strategies.pcp_arbitrage import PCPArbitrage

        tmp_config = get_default_config()
        tmp_config.min_profit_threshold = min_profit
        tmp_strategy = PCPArbitrage(tmp_config)
        n_snap = restore_from_snapshot(tmp_strategy, snapshot_dir, etf_prices)
        log.info("快照恢复 %d 条", n_snap)

        strategy, mgr, active, pairs, option_codes, etf_codes = (
            init_strategy_and_contracts(
                min_profit, expiry_days, atm_range, etf_prices,
                log_fn=log.info,
            )
        )
    except (FileNotFoundError, RuntimeError) as e:
        with _lock:
            _state["status"] = f"错误：{e}"
        return

    if n_snap > 0:
        restore_from_snapshot(strategy, snapshot_dir, etf_prices)

    with _lock:
        _state["n_pairs"] = len(pairs)
        _state["n_options"] = len(option_codes)
        _state["etf_prices"] = dict(etf_prices)
        _state["status"] = "运行中"

    ctx = zmq.Context.instance()
    sock = ctx.socket(zmq.SUB)
    sock.connect(f"tcp://127.0.0.1:{zmq_port}")
    sock.setsockopt_string(zmq.SUBSCRIBE, "")
    sock.setsockopt(zmq.RCVTIMEO, 100)
    log.info("ZMQ 已连接 tcp://127.0.0.1:%d", zmq_port)

    last_scan = datetime.now()
    last_heartbeat = datetime.now()
    tick_counter = 0

    log.info("进入主循环，等待 ZMQ 消息...")

    while True:
        try:
            for _ in range(500):
                try:
                    raw = sock.recv_string()
                except zmq.Again:
                    break
                except zmq.ZMQError as e:
                    log.warning("ZMQ 接收错误: %s", e)
                    break

                tick = parse_zmq_message(raw)
                if tick is not None:
                    if isinstance(tick, ETFTickData):
                        strategy.on_etf_tick(tick)
                        etf_prices[tick.etf_code] = tick.price
                    elif isinstance(tick, TickData):
                        strategy.on_option_tick(tick)
                    tick_counter += 1

            now = datetime.now()

            if (now - last_heartbeat).total_seconds() >= 30:
                log.info(
                    "心跳 %s | tick=%d | etf_prices=%s",
                    now.strftime("%H:%M:%S"),
                    tick_counter,
                    {k.split(".")[0]: f"{v:.4f}" for k, v in etf_prices.items()},
                )
                last_heartbeat = now

            if (now - last_scan).total_seconds() >= refresh_secs:
                try:
                    sigs = strategy.scan_opportunities(pairs, current_time=now)
                    filtered = [
                        s for s in sigs if s.net_profit_estimate >= min_profit
                    ]
                    etf_rank = {c: i for i, c in enumerate(ETF_ORDER)}
                    filtered.sort(
                        key=lambda s: (
                            etf_rank.get(s.underlying_code, 99),
                            s.expiry,
                            s.strike,
                        )
                    )
                    serialized = [signal_to_dict(s) for s in filtered]
                    with _lock:
                        _state["signals"] = serialized
                        _state["etf_prices"] = dict(etf_prices)
                        _state["tick_count"] = tick_counter
                        _state["last_scan"] = now.strftime("%H:%M:%S")
                        _state["status"] = "运行中"
                    log.info(
                        "扫描完成 %s | 信号 %d 条 | tick累计 %d",
                        now.strftime("%H:%M:%S"),
                        len(serialized),
                        tick_counter,
                    )
                except Exception as e:
                    log.error("PCP 扫描异常: %s", e, exc_info=True)
                last_scan = now

        except Exception as e:
            log.error("主循环异常: %s", e, exc_info=True)
            time.sleep(1)

        time.sleep(0.05)


# ──────────────────────────────────────────────────────────────────────────
# Flask 应用
# ──────────────────────────────────────────────────────────────────────────
app = Flask(__name__)

_HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>DeltaZero 套利监控</title>
<style>
  :root {
    --bg:       #0d1117;
    --bg2:      #161b22;
    --bg3:      #21262d;
    --border:   #30363d;
    --text:     #e6edf3;
    --dim:      #8b949e;
    --green:    #3fb950;
    --bgreen:   #56d364;
    --cyan:     #79c0ff;
    --yellow:   #e3b341;
    --red:      #f85149;
    --blue:     #58a6ff;
    --purple:   #bc8cff;
    --orange:   #ffa657;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: var(--bg); color: var(--text); font-family: 'Consolas','Menlo','Monaco',monospace; font-size: 13px; }

  #header {
    background: var(--bg2);
    border-bottom: 1px solid var(--border);
    padding: 10px 20px;
    display: flex;
    align-items: center;
    gap: 24px;
    flex-wrap: wrap;
    position: sticky; top: 0; z-index: 100;
  }
  #header .title { font-size: 16px; font-weight: bold; color: var(--bgreen); }
  #header .etf-price { display: flex; gap: 16px; flex-wrap: wrap; }
  #header .etf-item { display: flex; gap: 6px; align-items: baseline; }
  #header .etf-name { color: var(--cyan); font-weight: bold; }
  #header .etf-val  { color: var(--yellow); font-weight: bold; font-size: 15px; }
  #header .meta     { color: var(--dim); font-size: 12px; margin-left: auto; }
  #header .status-dot { width: 8px; height: 8px; border-radius: 50%; background: var(--green); display: inline-block; margin-right: 4px; }
  #header .status-dot.stale { background: var(--yellow); }

  #main { padding: 16px 20px; display: flex; flex-direction: column; gap: 16px; }

  .card {
    background: var(--bg2);
    border: 1px solid var(--border);
    border-radius: 8px;
    overflow: hidden;
  }
  .card.has-forward { border-color: var(--bgreen); box-shadow: 0 0 8px rgba(86,211,100,0.2); }
  .card-header {
    padding: 8px 16px;
    background: var(--bg3);
    display: flex;
    align-items: center;
    gap: 14px;
    border-bottom: 1px solid var(--border);
  }
  .card-header .name   { font-size: 15px; font-weight: bold; }
  .card-header .price  { color: var(--yellow); font-size: 15px; font-weight: bold; }
  .card-header .badge  { padding: 2px 8px; border-radius: 12px; font-size: 12px; font-weight: bold; }
  .badge.fwd  { background: rgba(86,211,100,0.15); color: var(--bgreen); border: 1px solid var(--bgreen); }
  .badge.rev  { background: rgba(139,148,158,0.1);  color: var(--dim);   border: 1px solid var(--border); }
  .badge.none { background: rgba(139,148,158,0.08); color: var(--dim);   border: 1px solid var(--border); }

  .tbl-wrap { overflow-x: auto; }
  table { width: 100%; border-collapse: collapse; }
  thead th {
    padding: 6px 10px;
    text-align: right;
    color: var(--cyan);
    font-weight: bold;
    border-bottom: 1px solid var(--border);
    white-space: nowrap;
  }
  thead th:first-child { text-align: center; }
  thead th:nth-child(3) { text-align: center; }
  tbody tr { border-bottom: 1px solid #21262d; transition: background 0.15s; }
  tbody tr:hover { background: var(--bg3); }
  tbody tr:last-child { border-bottom: none; }
  td { padding: 5px 10px; text-align: right; white-space: nowrap; }
  td:first-child { text-align: center; color: var(--dim); }
  td:nth-child(3) { text-align: center; }

  .no-signal { padding: 14px 16px; color: var(--dim); font-style: italic; }

  .profit-hi  { color: var(--bgreen); font-weight: bold; }
  .profit-mid { color: var(--green);  font-weight: bold; }
  .profit-lo  { color: var(--yellow); }

  .dir-fwd { color: var(--bgreen); font-weight: bold; }
  .dir-rev { color: var(--dim); font-style: italic; }

  .row-adj td { opacity: 0.75; }
  .section-sep td { padding: 2px 0 !important; }

  @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.4} }
  .refreshing { animation: pulse 0.6s ease-in-out; }

  #footer { text-align: center; color: var(--dim); font-size: 11px; padding: 16px; }
</style>
</head>
<body>

<div id="header">
  <span class="title">⚡ DeltaZero 套利监控</span>
  <div class="etf-price" id="etf-prices"></div>
  <div class="meta">
    <span class="status-dot" id="status-dot"></span>
    <span id="status-text">连接中...</span>
    &nbsp;|&nbsp;
    最后扫描: <span id="last-scan">—</span>
    &nbsp;|&nbsp;
    累计 Tick: <span id="tick-count">0</span>
    &nbsp;|&nbsp;
    信号: <span id="total-signals">0</span>
    &nbsp;|&nbsp;
    <span id="countdown">5s</span> 后刷新
  </div>
</div>

<div id="main">
  <div id="card-50ETF"  class="card"><div class="card-header"><span class="name">50ETF</span>  <span class="price" id="p-510050">—</span><span class="badge none" id="b-510050">暂无信号</span></div><div class="no-signal" id="t-510050">等待数据...</div></div>
  <div id="card-300ETF" class="card"><div class="card-header"><span class="name">300ETF</span> <span class="price" id="p-510300">—</span><span class="badge none" id="b-510300">暂无信号</span></div><div class="no-signal" id="t-510300">等待数据...</div></div>
  <div id="card-500ETF" class="card"><div class="card-header"><span class="name">500ETF</span> <span class="price" id="p-510500">—</span><span class="badge none" id="b-510500">暂无信号</span></div><div class="no-signal" id="t-510500">等待数据...</div></div>
</div>

<div id="footer">
  DeltaZero &nbsp;|&nbsp; 仅正向套利在 A 股可执行（ETF T+1 约束）&nbsp;|&nbsp; 净利润已扣除手续费与滑点估算
</div>

<script>
const ETF_CODES = ['510050', '510300', '510500'];
const ETF_NAMES = {'510050': '50ETF', '510300': '300ETF', '510500': '500ETF'};
let countdown = {{ refresh_secs }};
let refreshSecs = {{ refresh_secs }};

function fmt(v, dec=4) { return v == null ? '—' : Number(v).toFixed(dec); }

function buildTable(signals) {
  if (!signals || signals.length === 0) return null;
  const cols = [
    ['到期','expiry','c'],['行权价','strike','r'],['方向','direction','c'],
    ['乘数','multiplier','r'],
    ['C_b','call_bid','r'],['C_a','call_ask','r'],
    ['P_b','put_bid','r'], ['P_a','put_ask','r'],
    ['S','spot','r'],
    ['净利润','profit','r'],['计算明细','calc_detail','l'],
  ];

  const normal   = signals.filter(s => !s.is_adjusted).sort((a,b) => a.strike - b.strike);
  const adjusted = signals.filter(s =>  s.is_adjusted).sort((a,b) => a.strike - b.strike);

  function renderRow(s) {
    const isAdj = s.is_adjusted;
    const rowCls = isAdj ? ' class="row-adj"' : '';
    let html = `<tr${rowCls}>`;
    cols.forEach(([, key]) => {
      let val = s[key];
      let cell = '';
      if (key === 'direction') {
        cell = s.is_forward
          ? '<span class="dir-fwd">正向</span>'
          : '<span class="dir-rev">反向</span>';
      } else if (key === 'profit') {
        const p = Number(val);
        const cls = p >= 200 ? 'profit-hi' : p >= 100 ? 'profit-mid' : 'profit-lo';
        cell = `<span class="${cls}">${p.toFixed(0)}</span>`;
      } else if (key === 'multiplier') {
        const m = Number(val);
        cell = m !== 10000 ? `<span style="color:var(--orange);font-weight:bold">${m}</span>` : `<span style="color:var(--dim)">${m}</span>`;
      } else if (key === 'calc_detail') {
        cell = `<span style="color:var(--dim);font-size:12px">${val || ''}</span>`;
      } else if (key === 'expiry') {
        cell = `<span style="color:var(--dim)">${val}</span>`;
      } else if (key === 'strike') {
        const tag = isAdj ? '<span style="color:var(--orange);font-size:11px">(A) </span>' : '';
        cell = tag + fmt(val, 2);
      } else if (key === 'spot') {
        cell = fmt(val, 4);
      } else if (['call_bid','call_ask','put_bid','put_ask'].includes(key)) {
        cell = fmt(val, 4);
      } else {
        cell = val;
      }
      html += `<td>${cell}</td>`;
    });
    html += '</tr>';
    return html;
  }

  let html = '<div class="tbl-wrap"><table><thead><tr>';
  cols.forEach(([label]) => { html += `<th>${label}</th>`; });
  html += '</tr></thead><tbody>';
  normal.forEach(s => { html += renderRow(s); });
  if (normal.length > 0 && adjusted.length > 0) {
    html += `<tr class="section-sep"><td colspan="${cols.length}" style="border-top:1px dashed var(--dim);padding:2px;text-align:center;font-size:11px;color:var(--dim)">── 分红调整型合约 (A) ──</td></tr>`;
  }
  adjusted.forEach(s => { html += renderRow(s); });
  html += '</tbody></table></div>';
  return html;
}

async function refresh() {
  try {
    const res = await fetch('/api/signals');
    const data = await res.json();

    document.getElementById('status-dot').className = 'status-dot';
    document.getElementById('status-text').textContent = data.status || '运行中';
    document.getElementById('last-scan').textContent  = data.last_scan || '—';
    document.getElementById('tick-count').textContent = (data.tick_count || 0).toLocaleString();

    const priceEl = document.getElementById('etf-prices');
    let pHtml = '';
    ETF_CODES.forEach(code => {
      const p = (data.etf_prices || {})[code + '.SH'];
      if (p) pHtml += `<div class="etf-item"><span class="etf-name">${ETF_NAMES[code]}</span><span class="etf-val">${Number(p).toFixed(4)}</span></div>`;
    });
    priceEl.innerHTML = pHtml;

    const groups = {};
    ETF_CODES.forEach(c => { groups[c + '.SH'] = []; });
    (data.signals || []).forEach(s => {
      if (groups[s.underlying] !== undefined) groups[s.underlying].push(s);
    });

    let totalSigs = 0;
    ETF_CODES.forEach(code => {
      const underlying = code + '.SH';
      const sigs  = groups[underlying] || [];
      const nFwd  = sigs.filter(s => s.is_forward).length;
      const nRev  = sigs.length - nFwd;
      totalSigs  += sigs.length;

      const prEl = document.getElementById('p-' + code);
      const px   = (data.etf_prices || {})[underlying];
      if (prEl && px) prEl.textContent = Number(px).toFixed(4);

      const badge = document.getElementById('b-' + code);
      if (badge) {
        if (nFwd > 0) {
          badge.className = 'badge fwd';
          badge.textContent = `正向 ${nFwd} 条`;
        } else if (nRev > 0) {
          badge.className = 'badge rev';
          badge.textContent = `反向 ${nRev} 条`;
        } else {
          badge.className = 'badge none';
          badge.textContent = '暂无信号';
        }
      }

      const card = document.getElementById('card-' + ETF_NAMES[code]);
      if (card) {
        card.className = nFwd > 0 ? 'card has-forward' : 'card';
      }

      const tblEl = document.getElementById('t-' + code);
      if (tblEl) {
        const tbl = buildTable(sigs);
        tblEl.outerHTML = tbl
          ? tbl.replace('<div class="tbl-wrap">', `<div class="tbl-wrap" id="t-${code}">`)
          : `<div class="no-signal" id="t-${code}">暂无套利机会</div>`;
      }
    });

    document.getElementById('total-signals').textContent = totalSigs;

    document.getElementById('main').classList.add('refreshing');
    setTimeout(() => document.getElementById('main').classList.remove('refreshing'), 600);

    countdown = refreshSecs;
  } catch (e) {
    document.getElementById('status-dot').className = 'status-dot stale';
    document.getElementById('status-text').textContent = '连接断开';
    countdown = refreshSecs;
  }
}

setInterval(() => {
  countdown -= 1;
  document.getElementById('countdown').textContent = countdown + 's';
  if (countdown <= 0) refresh();
}, 1000);

refresh();
</script>
</body>
</html>"""


@app.route("/")
def index():
    refresh_secs = app.config.get("REFRESH_SECS", 5)
    return render_template_string(_HTML_TEMPLATE, refresh_secs=refresh_secs)


@app.route("/api/signals")
def api_signals():
    with _lock:
        return jsonify({
            "signals": _state["signals"],
            "etf_prices": {k: v for k, v in _state["etf_prices"].items()},
            "n_pairs": _state["n_pairs"],
            "n_options": _state["n_options"],
            "tick_count": _state["tick_count"],
            "last_scan": _state["last_scan"],
            "status": _state["status"],
        })


@app.route("/api/status")
def api_status():
    with _lock:
        return jsonify({"status": _state["status"], "errors": _state["errors"]})


# ──────────────────────────────────────────────────────────────────────────
# 入口
# ──────────────────────────────────────────────────────────────────────────
def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="DeltaZero — PCP 套利网页版监控")
    p.add_argument("--port", type=int, default=8080, help="网页端口")
    p.add_argument("--zmq-port", type=int, default=5555, help="ZMQ PUB 端口")
    p.add_argument("--min-profit", type=float, default=100.0, help="最小净利润显示阈值（元）")
    p.add_argument("--expiry-days", type=int, default=90, help="最大到期天数")
    p.add_argument("--atm-range", type=float, default=0.20, help="ATM 距离过滤")
    p.add_argument("--refresh", type=int, default=5, help="前端刷新间隔（秒）")
    p.add_argument("--snapshot-dir", type=str, default=r"D:\MARKET_DATA", help="快照目录")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()

    log.info("=" * 60)
    log.info("DeltaZero 套利监控（网页版）启动")
    log.info("  网页地址  : http://localhost:%d", args.port)
    log.info("  ZMQ 端口  : %d", args.zmq_port)
    log.info("  最小利润  : %.0f 元", args.min_profit)
    log.info("  刷新间隔  : %d 秒", args.refresh)
    log.info("  快照目录  : %s", args.snapshot_dir)
    log.info("=" * 60)

    app.config["REFRESH_SECS"] = args.refresh

    t = threading.Thread(
        target=_zmq_worker,
        args=(
            args.zmq_port, args.snapshot_dir, args.min_profit,
            args.expiry_days, args.atm_range, args.refresh,
        ),
        daemon=True,
        name="zmq-worker",
    )
    t.start()

    app.run(host="0.0.0.0", port=args.port, debug=False, use_reloader=False)
