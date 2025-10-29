#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BTC FOOTPRINT ORDERFLOW - v9
Layout Orizzontale Stesso Lato: Bid e Ask affiancati da sinistra
"""

from flask import Flask, jsonify, request
import time
import requests
from collections import defaultdict
from datetime import datetime
import threading

app = Flask(__name__)

SYMBOL_BINANCE = "BTCUSDT"
CACHE = {'data': {}, 'orderbook': {}, 'lock': threading.Lock()}

# Configurazione filtro trade rilevanti
TRADE_FILTER_MODE = "percentile"
TRADE_MIN_QTY_PERCENT = 0.5
TRADE_PERCENTILE = 75
TRADE_TOP_N = 300


def get_interval_ms(interval):
    intervals = {"1m": 60000, "5m": 300000, "15m": 900000, "30m": 1800000, "1h": 3600000, "1d": 86400000}
    return intervals.get(interval, 60000)

def fetch_with_retry(url, params, max_retries=3, timeout=15):
    for attempt in range(max_retries):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.Timeout:
            if attempt < max_retries - 1:
                time.sleep(1)
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(1)
    return None

def fetch_klines(interval, limit=150):
    url = "https://api.binance.com/api/v3/klines"
    params = {"symbol": SYMBOL_BINANCE, "interval": interval, "limit": limit}
    result = fetch_with_retry(url, params, max_retries=3, timeout=15)
    return result if result else []

def fetch_trades(start_ms, end_ms):
    url = "https://api.binance.com/api/v3/aggTrades"
    params = {"symbol": SYMBOL_BINANCE, "startTime": start_ms, "endTime": end_ms, "limit": 1000}
    result = fetch_with_retry(url, params, max_retries=2, timeout=12)
    return result if result else []

def fetch_orderbook():
    url = "https://api.binance.com/api/v3/depth"
    params = {"symbol": SYMBOL_BINANCE, "limit": 1000}
    result = fetch_with_retry(url, params, max_retries=2, timeout=10)
    return result if result else {"bids": [], "asks": []}

def round_price(price, step):
    return round(price / step) * step

def process_data(interval, step, update_last_only=False, filter_mode='none', filter_percentile=75, filter_min_qty=0.5, filter_top_n=300):
    klines = fetch_klines(interval, limit=150)
    if not klines:
        return {"bars": [], "stats": {"error": "Timeout API"}}

    bars = []
    interval_ms = get_interval_ms(interval)
    total_volume = 0
    total_delta = 0

    for i, k in enumerate(klines):
        ts = int(k[0])
        o, h, l, c = float(k[1]), float(k[2]), float(k[3]), float(k[4])
        vol = float(k[5])

        open_rounded = round_price(o, step)
        close_rounded = round_price(c, step)
        high_rounded = round_price(h, step)
        low_rounded = round_price(l, step)

        bid_vol = defaultdict(float)
        ask_vol = defaultdict(float)
        
        should_calc_trades = False
        if update_last_only:
            should_calc_trades = (i == len(klines) - 1)  # CORRETTO: ultima candela!
        else:
            should_calc_trades = (i >= len(klines) - 20)
        
        if should_calc_trades:
            trades = fetch_trades(ts, ts + interval_ms - 1)
            if trades:
                # Applica filtro SOLO all'ultima candela
                filtered_trades = trades
                is_last_candle = (i == len(klines) - 1)

                if is_last_candle and filter_mode != "none":
                    if filter_mode == "min_qty":
                        min_threshold = vol * (filter_min_qty / 100)
                        filtered_trades = [t for t in trades if float(t.get('q', 0)) >= min_threshold]

                    elif filter_mode == "percentile":
                        quantities = sorted([float(t.get('q', 0)) for t in trades])
                        if quantities:
                            idx = int(len(quantities) * (filter_percentile / 100))
                            threshold = quantities[min(idx, len(quantities)-1)]
                            filtered_trades = [t for t in trades if float(t.get('q', 0)) >= threshold]

                    elif filter_mode == "top_n":
                        filtered_trades = sorted(trades, key=lambda t: float(t.get('q', 0)), reverse=True)[:filter_top_n]

                # Processa i trade (filtrati solo se ultima candela)
                for t in filtered_trades:
                    try:
                        price = round_price(float(t.get('p', 0)), step)
                        qty = float(t.get('q', 0))
                        if low_rounded <= price <= high_rounded:
                            if t.get('m'):
                                bid_vol[price] += qty
                            else:
                                ask_vol[price] += qty
                    except (ValueError, KeyError, TypeError):
                        continue

        active_prices = set()
        min_body = min(open_rounded, close_rounded)
        max_body = max(open_rounded, close_rounded)
        current_price = min_body
        while current_price <= max_body:
            active_prices.add(current_price)
            current_price += step
        
        for price in bid_vol.keys():
            active_prices.add(price)
        for price in ask_vol.keys():
            active_prices.add(price)
        
        sorted_prices = sorted(active_prices, reverse=True)

        levels_data = []
        bar_total_bid = sum(bid_vol.values())
        bar_total_ask = sum(ask_vol.values())
        
        for price_level in sorted_prices:
            bid = bid_vol.get(price_level, 0)
            ask = ask_vol.get(price_level, 0)
            
            is_in_body = close_rounded <= price_level <= open_rounded if open_rounded >= close_rounded else open_rounded <= price_level <= close_rounded
            
            levels_data.append({
                "price": price_level,
                "bid": round(bid, 2),
                "ask": round(ask, 2),
                "significant": (bid + ask) > max((bar_total_bid + bar_total_ask) * 0.12, 0.1),
                "in_body": is_in_body
            })

        bar_delta = bar_total_ask - bar_total_bid
        total_volume += vol
        total_delta += bar_delta

        bars.append({
            "timestamp": ts,
            "time": datetime.fromtimestamp(ts/1000).strftime("%H:%M"),
            "open": round(o, 2),
            "high": round(h, 2),
            "low": round(l, 2),
            "close": round(c, 2),
            "open_rounded": open_rounded,
            "close_rounded": close_rounded,
            "volume": round(vol, 2),
            "levels": levels_data,
            "bullish": c > o,
            "delta": round(bar_delta, 2)
        })

    stats = {
        "price": bars[-1]["close"] if bars else 0,
        "volume": round(total_volume, 2),
        "delta": round(total_delta, 2),
        "bars_count": len(bars)
    }

    return {"bars": bars, "stats": stats}

@app.route('/')
def index():
    html = r"""
<!DOCTYPE html>
<html>
<head>
    <title>BTC Footprint + Order Book Live v9</title>
    <meta charset="utf-8">
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: Arial; background: #0a0a0a; color: #e0e0e0; margin: 0; padding: 0; height: 100vh; overflow: hidden; }
        .header { background: #1a1a1a; padding: 8px 15px; border-bottom: 1px solid #333; display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 10px; }
        .header h1 { font-size: 14px; color: #00d4ff; }
        .controls { display: flex; gap: 8px; }
        .controls select, .controls button { background: #2a2a2a; color: #e0e0e0; border: 1px solid #444; padding: 4px 8px; border-radius: 3px; font-size: 11px; cursor: pointer; }
        .controls button { background: #00d4ff; color: #000; font-weight: 600; }
        .controls button.active { background: #26a69a; }
        
        .ob-legend { display: flex; gap: 15px; font-size: 10px; }
        .ob-legend-item { display: flex; align-items: center; gap: 5px; }
        .ob-color-box { width: 12px; height: 12px; border-radius: 2px; }
        .ob-bid-color { background: rgba(76, 175, 80, 0.6); }
        .ob-ask-color { background: rgba(239, 83, 80, 0.6); }
        .ob-summary { display: flex; gap: 10px; align-items: center; font-size: 10px; color: #888; padding: 0 10px; border-left: 1px solid #333; }
        .ob-count { color: #00d4ff; font-weight: 600; }
        
        .stats-bar { background: #1a1a1a; padding: 5px 15px; border-bottom: 1px solid #333; display: flex; gap: 15px; font-size: 10px; flex-wrap: wrap; }
        .stat-item { display: flex; gap: 5px; align-items: center; }
        .stat-label { color: #888; }
        .stat-value { color: #00d4ff; font-weight: 600; }
        .stat-value.positive { color: #26a69a; }
        .stat-value.negative { color: #ef5350; }
        
        .navigation { background: #1a1a1a; padding: 8px 15px; border-bottom: 1px solid #333; display: flex; gap: 10px; align-items: center; flex-wrap: wrap; }
        .navigation button { background: #2a2a2a; color: #e0e0e0; border: 1px solid #444; padding: 5px 15px; border-radius: 3px; cursor: pointer; font-size: 11px; }
        .navigation button:hover { background: #3a3a3a; }
        .navigation input[type="range"] { flex: 1; max-width: 400px; }
        .zoom-controls { display: flex; gap: 8px; align-items: center; margin-left: 20px; }
        .zoom-slider { width: 120px; }
        
        .chart-container { height: calc(100vh - 150px); overflow: auto; padding: 10px; background: #0a0a0a; }
        .footprint-table { border-collapse: collapse; width: auto; }
        .bar-column { 
            border: 1px solid #1a1a1a; 
            padding: 0; 
            vertical-align: top; 
            position: relative;
            width: 70px !important;
            min-width: 70px !important;
            max-width: 70px !important;
            display: table-cell;
        }
        .time-header { background: #1a1a1a; padding: 5px; text-align: center; border-bottom: 2px solid #333; position: sticky; top: 0; z-index: 10; height: auto; }
        .time-text { font-weight: 600; margin-bottom: 2px; color: #00d4ff; font-size: 10px; }
        .ohlc-text { color: #666; font-size: 7px; }
        
        .price-row { display: table-row; }
        .price-cell { 
            background: #0a0a0a; 
            padding: 0; 
            position: relative;
            height: 22px !important;
            min-height: 22px !important;
            max-height: 22px !important;
            display: table-cell;
            border: 1px solid #1a1a1a;
            width: 70px !important;
            overflow: hidden;
        }
        
        .orderbook-overlay { position: absolute; top: 0; left: 0; right: 0; bottom: 0; pointer-events: none; z-index: 0; }
        .ob-bid-bar { position: absolute; right: 0; height: 100%; background: rgba(76, 175, 80, 0.25); border-right: 2px solid rgba(76, 175, 80, 0.6); }
        .ob-ask-bar { position: absolute; right: 0; height: 100%; background: rgba(239, 83, 80, 0.25); border-right: 2px solid rgba(239, 83, 80, 0.6); }
        
        .price-cell.in-body { border-left: 2px solid rgba(76, 175, 80, 0.7) !important; border-right: 2px solid rgba(76, 175, 80, 0.7) !important; }
        .price-cell.in-body.bullish { background: rgba(76, 175, 80, 0.06) !important; }
        .price-cell.in-body.bearish { background: rgba(244, 67, 54, 0.06) !important; }
        .price-cell.open-level { border-top: 3px solid #4caf50 !important; }
        .price-cell.open-level.bearish { border-top: 3px solid #f44336 !important; }
        .price-cell.close-level { border-bottom: 3px solid #4caf50 !important; }
        .price-cell.close-level.bearish { border-bottom: 3px solid #f44336 !important; }
        
        .price-cell-content { display: flex; justify-content: flex-start; gap: 2px; height: 100%; position: relative; z-index: 1; padding-left: 2px; }
        .bid-value, .ask-value { text-align: center; display: flex; align-items: center; justify-content: center; font-size: 7px; padding: 0 3px; border-radius: 2px; }
        .bid-value { background: rgba(76, 175, 80, 0.15); color: #26a69a; }
        .ask-value { background: rgba(239, 83, 80, 0.15); color: #ef5350; margin-left: 2px; }
        .bid-value.significant { background: rgba(76, 175, 80, 0.4); font-weight: 700; }
        .ask-value.significant { background: rgba(239, 83, 80, 0.4); font-weight: 700; }
        .price-label { display: none; }
        
        .delta-footer { background: #1a1a1a; padding: 4px; text-align: center; border-top: 2px solid #333; height: auto; }
        .delta-value { font-weight: 700; font-size: 9px; }
        .delta-value.positive { color: #26a69a; }
        .delta-value.negative { color: #ef5350; }
        
        .loading { position: fixed; top: 20px; right: 20px; transform: none; background: rgba(0,212,255,0.08); backdrop-filter: blur(2px); padding: 8px 16px; border-radius: 4px; z-index: 10000; display: none; color: #00d4ff; box-shadow: 0 0 12px rgba(0,212,255,0.4); pointer-events: none; border: 1px solid rgba(0,212,255,0.3); font-size: 11px; }
        .loading.active { display: block; }
        ::-webkit-scrollbar { width: 5px; }
        ::-webkit-scrollbar-track { background: #0a0a0a; }
        ::-webkit-scrollbar-thumb { background: #333; }
    
        .trading-signal { 
            position: fixed; 
            top: 250px; 
            left: 20px;
            width: 140px; 
            z-index: 999; 
            animation: pulse 2s ease-in-out infinite;
        }

        @keyframes pulse {
            0%, 100% { transform: scale(1); }
            50% { transform: scale(1.05); }
        }
        
        #chart-container {
            width: 100%;
            height: calc(100vh - 200px);
            background: #1a1a1a;
            position: relative;
            overflow-y: auto;
            overflow-x: auto;
            z-index: 1;
            scroll-behavior: smooth;
            padding-bottom: 180px;
            padding-right: 100px;
        }
        

        /* Order Panel Styles */
        .order-panel {
            position: fixed;
            bottom: 0;
            left: 0;
            right: 0;
            background: #1a1a1a;
            border-top: 2px solid #00d4ff;
            max-height: 400px;
            overflow: hidden;
            transition: max-height 0.3s ease;
            z-index: 1000;
            box-shadow: 0 -4px 12px rgba(0, 212, 255, 0.2);
        }

        .order-panel.collapsed {
            max-height: 50px;
        }

        .order-panel-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 10px 20px;
            background: #0f0f0f;
            border-bottom: 1px solid #333;
            cursor: pointer;
        }

        .order-panel-header h3 {
            margin: 0;
            font-size: 14px;
            color: #00d4ff;
            font-weight: 600;
        }

        .panel-toggle {
            background: transparent;
            border: none;
            color: #00d4ff;
            font-size: 16px;
            cursor: pointer;
            transition: transform 0.3s ease;
        }

        .order-panel.collapsed .panel-toggle {
            transform: rotate(-180deg);
        }

        .order-panel-content {
            padding: 15px 20px;
            overflow-y: auto;
            max-height: 340px;
        }

        .order-summary {
            background: rgba(0, 212, 255, 0.05);
            padding: 10px 15px;
            border-radius: 5px;
            margin-bottom: 15px;
            display: flex;
            gap: 20px;
            font-size: 11px;
            flex-wrap: wrap;
        }

        .order-summary-item {
            display: flex;
            flex-direction: column;
            gap: 3px;
        }

        .order-summary-label {
            color: #888;
            font-size: 9px;
            text-transform: uppercase;
        }

        .order-summary-value {
            color: #00d4ff;
            font-weight: 600;
            font-size: 12px;
        }

        .order-summary-value.positive {
            color: #26a69a;
        }

        .order-summary-value.negative {
            color: #ef5350;
        }

        .order-tables {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 20px;
        }

        .order-table-container {
            background: rgba(0, 0, 0, 0.3);
            border-radius: 5px;
            padding: 10px;
        }

        .order-table-container h4 {
            margin: 0 0 10px 0;
            font-size: 12px;
            padding: 5px 10px;
            border-radius: 3px;
        }

        .bid-header {
            background: rgba(76, 175, 80, 0.2);
            color: #26a69a;
        }

        .ask-header {
            background: rgba(239, 83, 80, 0.2);
            color: #ef5350;
        }

        .order-table {
            width: 100%;
            border-collapse: collapse;
            font-size: 10px;
        }

        .order-table thead {
            background: rgba(255, 255, 255, 0.05);
        }

        .order-table th {
            padding: 8px 10px;
            text-align: right;
            color: #888;
            font-weight: 600;
            border-bottom: 1px solid #333;
        }

        .order-table th:first-child {
            text-align: left;
        }

        .order-table tbody tr {
            border-bottom: 1px solid rgba(255, 255, 255, 0.05);
        }

        .order-table tbody tr:hover {
            background: rgba(0, 212, 255, 0.05);
        }

        .order-table td {
            padding: 6px 10px;
            text-align: right;
            color: #e0e0e0;
        }

        .order-table td:first-child {
            text-align: left;
            color: #00d4ff;
            font-weight: 600;
        }

        .order-table .qty-bar {
            position: relative;
            height: 4px;
            background: rgba(255, 255, 255, 0.1);
            border-radius: 2px;
            margin-top: 2px;
        }

        .order-table .qty-bar-fill {
            height: 100%;
            border-radius: 2px;
        }

        .bid-bar-fill {
            background: linear-gradient(90deg, rgba(76, 175, 80, 0.6), rgba(76, 175, 80, 0.9));
        }

        .ask-bar-fill {
            background: linear-gradient(90deg, rgba(239, 83, 80, 0.6), rgba(239, 83, 80, 0.9));
        }

        /* Adjust chart container to account for panel */
        .chart-container {
            padding-bottom: 420px !important;
        }

    /* PROFESSIONAL MODAL - 20% RANGE */
    .chart-modal {
        display: none;
        position: fixed;
        top: 0;
        left: 0;
        width: 100vw;
        height: 100vh;
        z-index: 10000;
        animation: fadeIn 0.3s ease;
    }

    @keyframes fadeIn {
        from { opacity: 0; }
        to { opacity: 1; }
    }

    .chart-modal.active {
        display: block;
    }

    .chart-modal-backdrop {
        position: absolute;
        top: 0;
        left: 0;
        width: 100%;
        height: 100%;
        background: rgba(0, 0, 0, 0.9);
        backdrop-filter: blur(6px);
    }

    .chart-modal-content {
        position: absolute;
        top: 1.5vh;
        left: 1vw;
        width: 98vw;
        height: 97vh;
        background: linear-gradient(135deg, #0a0a0a 0%, #1a1a1a 100%);
        border: 2px solid #00d4ff;
        border-radius: 16px;
        box-shadow: 0 20px 60px rgba(0, 212, 255, 0.3);
        display: flex;
        flex-direction: column;
        overflow: hidden;
        animation: slideIn 0.4s ease;
    }

    @keyframes slideIn {
        from { transform: translateY(-30px); opacity: 0; }
        to { transform: translateY(0); opacity: 1; }
    }

    .chart-modal-header {
        background: linear-gradient(135deg, #1a1a1a 0%, #0f0f0f 100%);
        padding: 18px 30px;
        border-bottom: 2px solid rgba(0, 212, 255, 0.3);
        display: flex;
        justify-content: space-between;
        align-items: center;
        flex-shrink: 0;
    }

    .header-left {
        display: flex;
        flex-direction: column;
        gap: 5px;
    }

    .header-left h2 {
        margin: 0;
        font-size: 24px;
        font-weight: 700;
        color: #00d4ff;
        text-shadow: 0 0 20px rgba(0, 212, 255, 0.5);
    }

    .header-subtitle {
        font-size: 12px;
        color: #888;
        font-weight: 500;
    }

    .header-controls {
        display: flex;
        gap: 20px;
        align-items: center;
    }

    .control-group {
        display: flex;
        flex-direction: column;
        gap: 5px;
    }

    .control-group label {
        font-size: 10px;
        color: #888;
        text-transform: uppercase;
        font-weight: 600;
        letter-spacing: 1px;
    }

    .control-group select {
        padding: 9px 16px;
        background: linear-gradient(135deg, rgba(0, 212, 255, 0.1) 0%, rgba(0, 212, 255, 0.05) 100%);
        color: #00d4ff;
        border: 1.5px solid rgba(0, 212, 255, 0.3);
        border-radius: 8px;
        font-size: 13px;
        font-weight: 600;
        cursor: pointer;
        transition: all 0.3s;
    }

    .control-group select:hover {
        background: linear-gradient(135deg, rgba(0, 212, 255, 0.2) 0%, rgba(0, 212, 255, 0.1) 100%);
        border-color: #00d4ff;
        box-shadow: 0 0 15px rgba(0, 212, 255, 0.3);
    }

    .status-indicator {
        font-size: 16px;
        color: #26a69a;
        animation: pulse 2s infinite;
    }

    @keyframes pulse {
        0%, 100% { opacity: 1; }
        50% { opacity: 0.5; }
    }

    .status-indicator.loading {
        color: #ffa726;
    }

    .btn-close {
        padding: 9px 22px;
        background: linear-gradient(135deg, #ef5350 0%, #c62828 100%);
        color: #fff;
        border: none;
        border-radius: 8px;
        font-size: 18px;
        font-weight: bold;
        cursor: pointer;
        transition: all 0.3s;
        box-shadow: 0 4px 12px rgba(239, 83, 80, 0.3);
    }

    .btn-close:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 20px rgba(239, 83, 80, 0.5);
    }

    .chart-stats {
        background: linear-gradient(135deg, rgba(0, 0, 0, 0.5) 0%, rgba(0, 0, 0, 0.3) 100%);
        padding: 15px 30px;
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
        gap: 18px;
        border-bottom: 1px solid rgba(255, 255, 255, 0.1);
        flex-shrink: 0;
    }

    .stats-loading {
        grid-column: 1 / -1;
        text-align: center;
        color: #888;
    }

    .stat-card {
        display: flex;
        flex-direction: column;
        gap: 5px;
        padding: 10px 14px;
        background: rgba(255, 255, 255, 0.02);
        border-radius: 8px;
        border-left: 3px solid transparent;
        transition: all 0.3s;
    }

    .stat-card:hover {
        background: rgba(255, 255, 255, 0.05);
        transform: translateY(-2px);
    }

    .stat-card.primary { border-left-color: #00d4ff; }
    .stat-card.success { border-left-color: #26a69a; }
    .stat-card.danger { border-left-color: #ef5350; }

    .stat-label {
        font-size: 10px;
        color: #888;
        text-transform: uppercase;
        font-weight: 600;
    }

    .stat-value {
        font-size: 16px;
        font-weight: 700;
        color: #fff;
    }

    .stat-value.primary { color: #00d4ff; }
    .stat-value.success { color: #26a69a; }
    .stat-value.danger { color: #ef5350; }

    .chart-canvas-wrapper {
        flex: 1;
        display: flex;
        justify-content: center;
        align-items: center;
        padding: 25px;
        background: #000;
        position: relative;
        overflow: hidden;
    }

    .chart-canvas-wrapper::before {
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        bottom: 0;
        background: 
            radial-gradient(circle at 20% 50%, rgba(0, 212, 255, 0.05) 0%, transparent 50%),
            radial-gradient(circle at 80% 50%, rgba(38, 166, 154, 0.05) 0%, transparent 50%);
        pointer-events: none;
    }

    #orderChart {
        max-width: 100%;
        max-height: 100%;
        position: relative;
        z-index: 1;
    }

    
    /* COLLAPSIBLE ORDER PANEL TOGGLE */
    .order-panel-toggle {
        position: fixed;
        bottom: 0;
        left: 0;
        right: 0;
        z-index: 999;
        background: linear-gradient(135deg, #1a1a1a 0%, #0f0f0f 100%);
        border-top: 2px solid #00d4ff;
        box-shadow: 0 -4px 12px rgba(0, 212, 255, 0.2);
        cursor: pointer;
        transition: all 0.3s ease;
    }

    .order-panel-toggle:hover {
        background: linear-gradient(135deg, #2a2a2a 0%, #1a1a1a 100%);
        box-shadow: 0 -6px 16px rgba(0, 212, 255, 0.3);
    }

    .toggle-bar {
        display: flex;
        align-items: center;
        justify-content: center;
        gap: 15px;
        padding: 12px 20px;
    }

    .toggle-icon {
        font-size: 20px;
    }

    .toggle-text {
        font-size: 14px;
        font-weight: 600;
        color: #00d4ff;
        text-transform: uppercase;
        letter-spacing: 1px;
    }

    .toggle-arrow {
        font-size: 16px;
        color: #00d4ff;
        transition: transform 0.3s ease;
    }

    .toggle-arrow.rotated {
        transform: rotate(180deg);
    }


    .fab-chart:hover {
        transform: translateY(-4px) scale(1.05);
        box-shadow: 0 12px 32px rgba(0, 212, 255, 0.6);
    }

    </style>
</head>
<body>
    <div class="header">
        <div style="display: flex; align-items: center; gap: 15px;">
            <h1>BTC Footprint + OB Live v9</h1>
            <div class="ob-legend">
                <div class="ob-legend-item"><div class="ob-color-box ob-bid-color"></div><span>Bids</span></div>
                <div class="ob-legend-item"><div class="ob-color-box ob-ask-color"></div><span>Asks</span></div>
            </div>
            <div class="ob-summary"><span>Orders: <span class="ob-count" id="obTotal">-</span></span> <span style="margin-right: 15px; padding-right: 15px; border-right: 2px solid #333;">OB Δ: <span id="obDeltaHeader" class="ob-count" style="font-weight: bold;">-</span></span></div>
        </div>
        <div class="controls">
            <select id="interval" onchange="changeTimeframe()">
                <option value="1m" selected>1m</option>
                <option value="5m">5m</option>
                <option value="15m">15m</option>
                <option value="30m">30m</option>
                <option value="1h">1h</option>
                <option value="1d">1d</option>
            </select>
            <select id="step" onchange="loadData()">
                <option value="1">1$</option>
                <option value="5">5$</option>
                <option value="10" selected>10$</option>
                <option value="25">25$</option>
                <option value="50">50$</option>
                <option value="100">100$</option>
                <option value="250">250$</option>
            </select>
            <button onclick="loadData()">Aggiorna</button>
            <button id="autoRefreshBtn" onclick="toggleAutoRefresh()">Auto</button>

            <!-- Controllo Filtro Trade -->
            <div style="margin-left: 20px; display: inline-flex; align-items: center; padding: 5px 10px; background: rgba(255,255,255,0.03); border-radius: 6px; border: 1px solid #444;">
                <label style="margin-right: 8px; font-weight: 600; color: #00d4ff; font-size: 11px;">📊 Filtro:</label>
                <button id="filterToggle" onclick="toggleFilter()" style="padding: 5px 12px; border-radius: 4px; background: #28a745; color: white; border: none; cursor: pointer; font-weight: 600; font-size: 11px;">ON</button>
                <span style="margin: 0 8px; color: #555;">|</span>
                <label for="filterPercentileSlider" style="margin-right: 6px; font-size: 10px; color: #aaa;">Top</label>
                <input type="range" id="filterPercentileSlider" min="50" max="95" value="75" step="5" style="width: 80px; cursor: pointer;" oninput="updatePercentileLabel()">
                <span id="percentileValue" style="margin-left: 6px; font-weight: 600; color: #00d4ff; min-width: 30px; font-size: 11px;">25%</span>
            </div>
        </div>
    </div>
    <div class="stats-bar" id="stats-bar">Caricamento...</div>
    <div class="trading-signal" id="trading-signal">Caricamento...</div>

    <div class="phase-distribution-container" style="margin: 15px 0; padding: 15px; background: #1e1e1e; border-radius: 8px;">
        <div style="margin-bottom: 8px; font-size: 13px; font-weight: 600; color: #fff;">
            Distribuzione Strategia (Pesata per Intensità)
        </div>
        <div id="phaseBar" class="phase-bar" style="display: flex; height: 30px; border-radius: 6px; overflow: hidden; box-shadow: 0 2px 4px rgba(0,0,0,0.3);">
            <!-- Riempito da JavaScript -->
        </div>
        <div id="phaseStats" style="margin-top: 10px; font-size: 11px; color: #aaa; display: flex; justify-content: space-between; flex-wrap: wrap; gap: 5px;">
            <!-- Statistiche dettagliate -->
        </div>
    </div>

    <div class="navigation">
        <button onclick="scrollBars(-10)"><<<</button>
        <button onclick="scrollBars(-1)"><</button>
        <input type="range" id="rangeSlider" min="0" max="100" value="100" oninput="updateRange(this.value)">
        <span id="rangeLabel">-</span>
        <button onclick="scrollBars(1)">></button>
        <button onclick="scrollBars(10)">>>></button>
        <button onclick="resetView()">Reset</button>
        <div class="zoom-controls">
            <input type="range" id="zoomSlider" class="zoom-slider" min="50" max="150" value="100" oninput="applyZoom(this.value)">
        </div>
    </div>
    <div class="loading" id="loading">Caricamento...</div>
    <div class="chart-container" id="chart-container">In attesa...</div>
    <script>
        let currentData = null, orderBookData = null, viewStart = 0, viewCount = 22, isFirstLoad = true;
        let autoRefreshInterval = null, obRefreshInterval = null;
        let currentInterval = '1m', currentStep = '10';
        const refreshIntervals = {'1m': 15000, '5m': 30000, '15m': 60000, '30m': 60000, '1h': 60000, '1d': 300000};
        
        
        // Variabili globali per filtro
        let filterEnabled = true;
        let currentPercentile = 75;

        function toggleFilter() {
            filterEnabled = !filterEnabled;
            const btn = document.getElementById('filterToggle');
            const slider = document.getElementById('filterPercentileSlider');
            const label = document.getElementById('percentileValue');

            if (filterEnabled) {
                btn.textContent = 'ON';
                btn.style.background = '#28a745';
                slider.disabled = false;
                slider.style.opacity = '1';
                label.style.opacity = '1';
            } else {
                btn.textContent = 'OFF';
                btn.style.background = '#666';
                slider.disabled = true;
                slider.style.opacity = '0.4';
                label.style.opacity = '0.4';
            }

            loadData();
        }

        function updatePercentileLabel() {
            currentPercentile = parseInt(document.getElementById('filterPercentileSlider').value);
            const topPercent = 100 - currentPercentile;
            document.getElementById('percentileValue').textContent = topPercent + '%';
            loadData();
        }

        function changeTimeframe() {
            currentInterval = document.getElementById('interval').value;
            isFirstLoad = true;  // Reset per nuovo timeframe
            if (autoRefreshInterval) {
                clearInterval(autoRefreshInterval);
                autoRefreshInterval = setInterval(() => loadDataLastOnly(), refreshIntervals[currentInterval] || 30000);
            }
            loadData();
        }
        
        function toggleAutoRefresh() {
            const btn = document.getElementById('autoRefreshBtn');
            if (autoRefreshInterval) {
                clearInterval(autoRefreshInterval);
                autoRefreshInterval = null;
                btn.style.background = '#2a2a2a';
            } else {
                const interval = refreshIntervals[currentInterval] || 30000;
                autoRefreshInterval = setInterval(() => loadDataLastOnly(), interval);
                btn.style.background = '#26a69a';
            }
        }
        
        function loadData() {
            document.getElementById('loading').classList.add('active');
            const interval = document.getElementById('interval').value;
            const step = document.getElementById('step').value;
            currentInterval = interval;
            currentStep = step;
            
            fetch('/api/data?interval=' + interval + '&step=' + step + '&filter_mode=' + (filterEnabled ? 'percentile' : 'none') + '&filter_percentile=' + currentPercentile)
                .then(r => r.json())
                .then(data => {
                    if (!data || !data.bars || data.bars.length === 0) {
                        document.getElementById('chart-container').innerHTML = '<div style="color: #f44336; text-align: center; padding: 20px;">Errore caricamento</div>';
                        document.getElementById('loading').classList.remove('active');
                        return;
                    }
                    currentData = data;
                    renderStatsBar(data.stats);
                    return fetch('/api/orderbook');
                })
                .then(r => r.json())
                .then(obData => {
                    orderBookData = obData || {bids: [], asks: []};
                    updateObDisplay();
                    startObRefresh();
                    resetView();
                    document.getElementById('loading').classList.remove('active');
                })
                .catch(e => {
                    console.error(e);
                    document.getElementById('loading').classList.remove('active');
                });
        }
        
       function loadDataLastOnly() {
            const interval = document.getElementById('interval').value;
            const step = document.getElementById('step').value;

            Promise.all([
                fetch('/api/data?interval=' + interval + '&step=' + step + '&update_last=true' + '&filter_mode=' + (filterEnabled ? 'percentile' : 'none') + '&filter_percentile=' + currentPercentile).then(r => r.json()),
                fetch('/api/orderbook').then(r => r.json())
            ])
            .then(([data, obData]) => {
                if (data && data.bars && data.bars.length > 0) {
                    const newLastBar = data.bars[data.bars.length - 1];

                    if (currentData && currentData.bars && currentData.bars.length > 0) {
                        const currentLastBar = currentData.bars[currentData.bars.length - 1];

                        if (currentLastBar.time === newLastBar.time) {
                            // Stessa candela: aggiorna
                            currentData.bars[currentData.bars.length - 1] = newLastBar;
                        } else {
                            // Nuova candela: aggiungi
                            currentData.bars.push(newLastBar);
                        }
                        currentData.stats = data.stats;
                    } else {
                        currentData = { bars: data.bars, stats: data.stats };
                    }

                    orderBookData = obData || {bids: [], asks: []};
                    updateObDisplay();
                    renderStatsBar(data.stats);

                    viewStart = Math.max(0, currentData.bars.length - viewCount);
                    const slider = document.getElementById('rangeSlider');
                    if (slider) {
                        slider.max = Math.max(0, currentData.bars.length - viewCount);
                        slider.value = viewStart;
                    }

                    renderChart();
                }
            })
            .catch(e => {
                console.error("Errore in loadDataLastOnly:", e);
            });
         }


        
        function startObRefresh() {
            if (obRefreshInterval) clearInterval(obRefreshInterval);
            obRefreshInterval = setInterval(() => {
                fetch('/api/orderbook')
                    .then(r => r.json())
                    .then(obData => {
                        if (obData && obData.bids) {
                            orderBookData = obData;
                            updateObDisplay();
                            renderChart();
                            
                        }
                    })
                    .catch(e => console.error(e));
            }, 5000);
        }
        
        function updateObDisplay() {
            if (!orderBookData) return;
            const bidCount = (orderBookData.bids || []).length;
            const askCount = (orderBookData.asks || []).length;
            document.getElementById('obTotal').textContent = (bidCount + askCount);

            let bidQty = 0, askQty = 0;
            (orderBookData.bids || []).forEach(p => { bidQty += parseFloat(p[1] || 0); });
            (orderBookData.asks || []).forEach(p => { askQty += parseFloat(p[1] || 0); });
            const obDelta = bidQty - askQty;
            const obDeltaEl = document.getElementById('obDeltaHeader');
            if (obDeltaEl) {
                obDeltaEl.textContent = (obDelta >= 0 ? '+' : '') + obDelta.toFixed(2);
                obDeltaEl.style.color = obDelta >= 0 ? '#00ff00' : '#ff4444';
            }
        }

        function calculateTradingSignal() {
            if (!currentData || !currentData.stats || !orderBookData) {
                return { signal: 'neutral', strength: 0, obDelta: 0, footprintDelta: 0, volumeRatio: 0 };
            }

            const stats = currentData.stats;
            const currentPrice = stats.price || 0;

            // ============================================
            // CALCOLO OB DELTA PESATO (con distanza dal prezzo)
            // ============================================

            let weightedBidQty = 0, weightedAskQty = 0;
            let totalBidQty = 0, totalAskQty = 0;

            // Peso basato sulla distanza dal prezzo corrente
            const PRICE_WEIGHT_FACTOR = 0.02; // 2% di decadimento per ogni % di distanza

            orderBookData.bids.forEach(p => {
                const price = parseFloat(p[0] || 0);
                const qty = parseFloat(p[1] || 0);
                const distance = Math.abs(price - currentPrice) / currentPrice;

                // Peso esponenziale: più vicino = più importante
                const weight = Math.exp(-distance / PRICE_WEIGHT_FACTOR);

                weightedBidQty += qty * weight;
                totalBidQty += qty;
            });

            orderBookData.asks.forEach(p => {
                const price = parseFloat(p[0] || 0);
                const qty = parseFloat(p[1] || 0);
                const distance = Math.abs(price - currentPrice) / currentPrice;

                // Peso esponenziale: più vicino = più importante
                const weight = Math.exp(-distance / PRICE_WEIGHT_FACTOR);

                weightedAskQty += qty * weight;
                totalAskQty += qty;
            });

            // Delta OB pesato (più rilevanza agli ordini vicini)
            const weightedObDelta = weightedBidQty - weightedAskQty;

            // Delta OB totale (per riferimento)
            const totalObDelta = totalBidQty - totalAskQty;

            // ============================================
            // DELTA FOOTPRINT
            // ============================================
            const footprintDelta = stats.delta || 0;

            // ============================================
            // VOLUME ANALYSIS
            // ============================================
            const avgVolume = stats.volume / Math.max(1, currentData.bars.length);
            const currentBar = currentData.bars[currentData.bars.length - 1];
            const currentVolume = currentBar ? currentBar.volume : 0;
            const volumeRatio = avgVolume > 0 ? currentVolume / avgVolume : 1;

            // ============================================
            // STRATEGIA MIGLIORATA CON PESI
            // ============================================

            // PESO MAGGIORE AL DELTA OB (70%) vs DELTA FP (30%)
            const OB_WEIGHT = 0.70;
            const FP_WEIGHT = 0.30;

            // Normalizza i delta per poterli confrontare
            const totalQtyWeighted = Math.abs(weightedBidQty + weightedAskQty);
            const normalizedObDelta = totalQtyWeighted > 0 ? weightedObDelta / totalQtyWeighted : 0;

            // Normalizza footprint delta rispetto al volume medio
            const normalizedFpDelta = avgVolume > 0 ? footprintDelta / avgVolume : 0;

            // SCORE COMPOSITO
            const compositeScore = (normalizedObDelta * OB_WEIGHT) + (normalizedFpDelta * FP_WEIGHT);

            // Soglie dinamiche basate sulla volatilità OB
            const OB_THRESHOLD = 0.08; // 8% di sbilanciamento pesato
            const FP_THRESHOLD = 0.15; // 15% del volume medio
            const VOLUME_THRESHOLD = 1.2;

            let signal = 'neutral';
            let strength = 0;

            // ============================================
            // SEGNALI DI TRADING
            // ============================================

            // SEGNALE BUY FORTE
            if (compositeScore > OB_THRESHOLD && 
                footprintDelta > 0 && 
                volumeRatio > VOLUME_THRESHOLD) {
                signal = 'buy';
                // Strength basato principalmente su OB Delta pesato
                const obStrength = Math.min(60, (Math.abs(normalizedObDelta) / OB_THRESHOLD) * 60);
                const fpStrength = Math.min(25, (Math.abs(normalizedFpDelta) / FP_THRESHOLD) * 25);
                const volStrength = Math.min(15, (volumeRatio / VOLUME_THRESHOLD) * 15);
                strength = Math.round(obStrength + fpStrength + volStrength);
            }

            // SEGNALE SELL FORTE
            else if (compositeScore < -OB_THRESHOLD && 
                     footprintDelta < 0 && 
                     volumeRatio > VOLUME_THRESHOLD) {
                signal = 'sell';
                const obStrength = Math.min(60, (Math.abs(normalizedObDelta) / OB_THRESHOLD) * 60);
                const fpStrength = Math.min(25, (Math.abs(normalizedFpDelta) / FP_THRESHOLD) * 25);
                const volStrength = Math.min(15, (volumeRatio / VOLUME_THRESHOLD) * 15);
                strength = Math.round(obStrength + fpStrength + volStrength);
            }

            // SEGNALE BUY MODERATO (solo OB forte)
            else if (compositeScore > OB_THRESHOLD * 0.5) {
                signal = 'buy';
                strength = Math.min(100, Math.round((Math.abs(normalizedObDelta) / OB_THRESHOLD) * 80));
            }

            // SEGNALE SELL MODERATO (solo OB forte)
            else if (compositeScore < -OB_THRESHOLD * 0.5) {
                signal = 'sell';
                strength = Math.min(100, Math.round((Math.abs(normalizedObDelta) / OB_THRESHOLD) * 80));
            }


        // ============================================
        // TRACKING FASI STRATEGIA
        // ============================================

        if (!window.strategyPhaseTracker) {
            window.strategyPhaseTracker = {
                phases: [],
                currentPhase: null,
                totalBars: 0
            };
        }

        const tracker = window.strategyPhaseTracker;

        // Aggiorna tracking
        if (!tracker.currentPhase || tracker.currentPhase.signal !== signal) {
            if (tracker.currentPhase) {
                tracker.phases.push({...tracker.currentPhase});
                if (tracker.phases.length > 50) {
                    tracker.phases.shift();
                }
            }
            tracker.currentPhase = {
                signal: signal,
                duration: 1,
                totalStrength: strength,
                avgStrength: strength
            };
        } else {
            tracker.currentPhase.duration++;
            tracker.currentPhase.totalStrength += strength;
            tracker.currentPhase.avgStrength = tracker.currentPhase.totalStrength / tracker.currentPhase.duration;
        }

        tracker.totalBars++;

        // ============================================
        // TRACKING AVG INTENSITY STORICO
        // ============================================

        if (!window.avgIntensityHistory) {
            window.avgIntensityHistory = {
                timestamps: [],
                buyIntensity: [],
                sellIntensity: [],
                signals: [],
                maxHistory: 200
            };
        }

        const intensityHistory = window.avgIntensityHistory;
        const now = Date.now();

        intensityHistory.timestamps.push(now);
        intensityHistory.signals.push(signal);

        if (signal === 'buy') {
            intensityHistory.buyIntensity.push(strength);
            intensityHistory.sellIntensity.push(0);
        } else if (signal === 'sell') {
            intensityHistory.buyIntensity.push(0);
            intensityHistory.sellIntensity.push(strength);
        } else {
            intensityHistory.buyIntensity.push(0);
            intensityHistory.sellIntensity.push(0);
        }

        if (intensityHistory.timestamps.length > intensityHistory.maxHistory) {
            intensityHistory.timestamps.shift();
            intensityHistory.buyIntensity.shift();
            intensityHistory.sellIntensity.shift();
            intensityHistory.signals.shift();
        }


            return {
                signal: signal,
                strength: Math.min(100, strength),
                weightedObDelta: weightedObDelta,
                totalObDelta: totalObDelta,
                footprintDelta: footprintDelta,
                volumeRatio: volumeRatio.toFixed(2),
                compositeScore: compositeScore.toFixed(4),
                normalizedObDelta: normalizedObDelta.toFixed(4),
                normalizedFpDelta: normalizedFpDelta.toFixed(4)
            };
        }



        function renderTradingSignal() {
            const signalData = calculateTradingSignal();
            const signalDiv = document.getElementById('trading-signal');
            if (!signalDiv) return;

            let arrow = '', color = '', text = '';
            if (signalData.signal === 'buy') {
                arrow = '▲';
                color = '#26a69a';
                text = 'BUY';
            } else if (signalData.signal === 'sell') {
                arrow = '▼';
                color = '#ef5350';
                text = 'SELL';
            } else {
                arrow = '●';
                color = '#888';
                text = 'NEUTRAL';
            }

            const strengthBar = signalData.strength > 0 ? 
                `<div style="background: rgba(255,255,255,0.1); height: 4px; margin-top: 5px; border-radius: 2px">
                    <div style="background: ${color}; height: 100%; width: ${signalData.strength}%; border-radius: 2px"></div>
                </div>` : '';

            signalDiv.innerHTML = `
                <div style="text-align: center; padding: 10px; background: rgba(0,0,0,0.3); border-radius: 5px; border: 2px solid ${color}">
                    <div style="font-size: 32px; color: ${color}; font-weight: bold">${arrow}</div>
                    <div style="font-size: 14px; color: ${color}; font-weight: bold; margin-top: 5px">${text}</div>
                    <div style="font-size: 10px; color: #888; margin-top: 5px">Strength: ${signalData.strength}%</div>
                    ${strengthBar}
                    <div style="font-size: 9px; color: #666; margin-top: 8px; line-height: 1.6; text-align: left; padding: 0 5px">
                        <div style="color: #00d4ff; font-weight: 600; margin-bottom: 4px">📊 Metrics:</div>
                        <div style="display: flex; justify-content: space-between">
                            <span>OB W:</span>
                            <span style="color: ${signalData.weightedObDelta >= 0 ? '#26a69a' : '#ef5350'}">${signalData.weightedObDelta.toFixed(2)}</span>
                        </div>
                        <div style="display: flex; justify-content: space-between">
                            <span>OB Total:</span>
                            <span style="color: ${signalData.totalObDelta >= 0 ? '#26a69a' : '#ef5350'}">${signalData.totalObDelta.toFixed(2)}</span>
                        </div>
                        <div style="display: flex; justify-content: space-between">
                            <span>FP Δ:</span>
                            <span style="color: ${signalData.footprintDelta >= 0 ? '#26a69a' : '#ef5350'}">${signalData.footprintDelta.toFixed(2)}</span>
                        </div>
                        <div style="display: flex; justify-content: space-between">
                            <span>Vol:</span>
                            <span>${signalData.volumeRatio}x</span>
                        </div>
                        <div style="display: flex; justify-content: space-between; margin-top: 4px; padding-top: 4px; border-top: 1px solid #333">
                            <span>Score:</span>
                            <span style="color: #00d4ff; font-weight: 600">${signalData.compositeScore}</span>
                        </div>
                    </div>
                </div>
            `;
        }
        

        // ============================================
        // CALCOLO DISTRIBUZIONE PESATA
        // ============================================

        function calculateWeightedDistribution() {
            if (!window.strategyPhaseTracker) return null;

            const tracker = window.strategyPhaseTracker;
            const allPhases = [...tracker.phases];
            if (tracker.currentPhase) {
                allPhases.push({...tracker.currentPhase});
            }

            let buyWeight = 0, sellWeight = 0, neutralWeight = 0;
            let buyBars = 0, sellBars = 0, neutralBars = 0;

            allPhases.forEach(phase => {
                const weight = phase.duration * (phase.avgStrength / 100);

                if (phase.signal === 'buy') {
                    buyWeight += weight;
                    buyBars += phase.duration;
                } else if (phase.signal === 'sell') {
                    sellWeight += weight;
                    sellBars += phase.duration;
                } else {
                    neutralWeight += weight;
                    neutralBars += phase.duration;
                }
            });

            const totalWeight = buyWeight + sellWeight + neutralWeight;
            const totalBars = buyBars + sellBars + neutralBars;

            if (totalWeight === 0) {
                return {
                    weighted: { buy: 0, sell: 0, neutral: 0 },
                    time: { buy: 0, sell: 0, neutral: 0 },
                    bars: { buy: 0, sell: 0, neutral: 0, total: 0 },
                    avgStrength: { buy: 0, sell: 0, neutral: 0 }
                };
            }

            const buyPercent = (buyWeight / totalWeight) * 100;
            const sellPercent = (sellWeight / totalWeight) * 100;
            const neutralPercent = (neutralWeight / totalWeight) * 100;

            const buyTimePercent = totalBars > 0 ? (buyBars / totalBars) * 100 : 0;
            const sellTimePercent = totalBars > 0 ? (sellBars / totalBars) * 100 : 0;
            const neutralTimePercent = totalBars > 0 ? (neutralBars / totalBars) * 100 : 0;

            return {
                weighted: {
                    buy: buyPercent,
                    sell: sellPercent,
                    neutral: neutralPercent
                },
                time: {
                    buy: buyTimePercent,
                    sell: sellTimePercent,
                    neutral: neutralTimePercent
                },
                bars: {
                    buy: buyBars,
                    sell: sellBars,
                    neutral: neutralBars,
                    total: totalBars
                },
                avgStrength: {
                    buy: buyBars > 0 ? (buyWeight / buyBars) * 100 : 0,
                    sell: sellBars > 0 ? (sellWeight / sellBars) * 100 : 0,
                    neutral: neutralBars > 0 ? (neutralWeight / neutralBars) * 100 : 0
                }
            };
        }

        // ============================================
        // RENDERING BARRA DISTRIBUZIONE
        // ============================================

        function renderPhaseDistribution() {
            const dist = calculateWeightedDistribution();
            if (!dist) return;

            const phaseBar = document.getElementById('phaseBar');
            const phaseStats = document.getElementById('phaseStats');

            if (!phaseBar || !phaseStats) return;

            // Clear barra
            phaseBar.innerHTML = '';

            // Segmento BUY
            if (dist.weighted.buy > 0) {
                const buySegment = document.createElement('div');
                buySegment.style.width = dist.weighted.buy + '%';
                buySegment.style.background = 'linear-gradient(to right, #26a69a, #4caf50)';
                buySegment.style.display = 'flex';
                buySegment.style.alignItems = 'center';
                buySegment.style.justifyContent = 'center';
                buySegment.style.color = '#fff';
                buySegment.style.fontSize = '11px';
                buySegment.style.fontWeight = '600';
                buySegment.style.textShadow = '1px 1px 2px rgba(0,0,0,0.5)';
                buySegment.style.transition = 'all 0.3s ease';
                buySegment.title = `BUY: ${dist.weighted.buy.toFixed(1)}% (pesato)\n${dist.time.buy.toFixed(1)}% tempo\n${dist.bars.buy} barre\nIntensità media: ${dist.avgStrength.buy.toFixed(0)}%`;
                if (dist.weighted.buy > 8) {
                    buySegment.textContent = `▲ ${dist.weighted.buy.toFixed(0)}%`;
                }
                phaseBar.appendChild(buySegment);
            }

            // Segmento SELL
            if (dist.weighted.sell > 0) {
                const sellSegment = document.createElement('div');
                sellSegment.style.width = dist.weighted.sell + '%';
                sellSegment.style.background = 'linear-gradient(to right, #ef5350, #f44336)';
                sellSegment.style.display = 'flex';
                sellSegment.style.alignItems = 'center';
                sellSegment.style.justifyContent = 'center';
                sellSegment.style.color = '#fff';
                sellSegment.style.fontSize = '11px';
                sellSegment.style.fontWeight = '600';
                sellSegment.style.textShadow = '1px 1px 2px rgba(0,0,0,0.5)';
                sellSegment.style.transition = 'all 0.3s ease';
                sellSegment.title = `SELL: ${dist.weighted.sell.toFixed(1)}% (pesato)\n${dist.time.sell.toFixed(1)}% tempo\n${dist.bars.sell} barre\nIntensità media: ${dist.avgStrength.sell.toFixed(0)}%`;
                if (dist.weighted.sell > 8) {
                    sellSegment.textContent = `▼ ${dist.weighted.sell.toFixed(0)}%`;
                }
                phaseBar.appendChild(sellSegment);
            }

            // Segmento NEUTRAL
            if (dist.weighted.neutral > 0) {
                const neutralSegment = document.createElement('div');
                neutralSegment.style.width = dist.weighted.neutral + '%';
                neutralSegment.style.background = 'linear-gradient(to right, #78909c, #90a4ae)';
                neutralSegment.style.display = 'flex';
                neutralSegment.style.alignItems = 'center';
                neutralSegment.style.justifyContent = 'center';
                neutralSegment.style.color = '#fff';
                neutralSegment.style.fontSize = '11px';
                neutralSegment.style.fontWeight = '600';
                neutralSegment.style.textShadow = '1px 1px 2px rgba(0,0,0,0.5)';
                neutralSegment.style.transition = 'all 0.3s ease';
                neutralSegment.title = `NEUTRAL: ${dist.weighted.neutral.toFixed(1)}% (pesato)\n${dist.time.neutral.toFixed(1)}% tempo\n${dist.bars.neutral} barre`;
                if (dist.weighted.neutral > 8) {
                    neutralSegment.textContent = `⊡ ${dist.weighted.neutral.toFixed(0)}%`;
                }
                phaseBar.appendChild(neutralSegment);
            }

            // Statistiche dettagliate
            phaseStats.innerHTML = `
                <div style="flex: 1; min-width: 120px;">
                    <span style="color: #4caf50; font-weight: 600;">▲ BUY:</span> 
                    ${dist.bars.buy} barre (${dist.time.buy.toFixed(0)}%) • 
                    <span style="opacity: 0.8;">Avg ${dist.avgStrength.buy.toFixed(0)}%</span>
                </div>
                <div style="flex: 1; min-width: 120px;">
                    <span style="color: #f44336; font-weight: 600;">▼ SELL:</span> 
                    ${dist.bars.sell} barre (${dist.time.sell.toFixed(0)}%) • 
                    <span style="opacity: 0.8;">Avg ${dist.avgStrength.sell.toFixed(0)}%</span>
                </div>
                <div style="flex: 1; min-width: 120px;">
                    <span style="color: #90a4ae; font-weight: 600;">⊡ NEUTRAL:</span> 
                    ${dist.bars.neutral} barre (${dist.time.neutral.toFixed(0)}%)
                </div>
            `;
        }


        // ============================================
        // RENDERING GRAFICO AVG INTENSITY
        // ============================================

        function renderAvgIntensityChart() {
            const canvas = document.getElementById('avgIntensityCanvas');
            const timeWindowSelect = document.getElementById('intensityTimeWindow');

            if (!canvas || !timeWindowSelect || !window.avgIntensityHistory) return;

            const timeWindow = parseInt(timeWindowSelect.value);
            const ctx = canvas.getContext('2d');
            const history = window.avgIntensityHistory;

            if (history.timestamps.length === 0) return;

            now = Date.now();
            const cutoffTime = now - (timeWindow * 60 * 1000);

            const filtered = { timestamps: [], buyIntensity: [], sellIntensity: [], signals: [] };

            for (let i = 0; i < history.timestamps.length; i++) {
                if (history.timestamps[i] >= cutoffTime) {
                    filtered.timestamps.push(history.timestamps[i]);
                    filtered.buyIntensity.push(history.buyIntensity[i]);
                    filtered.sellIntensity.push(history.sellIntensity[i]);
                    filtered.signals.push(history.signals[i]);
                }
            }

            if (filtered.timestamps.length === 0) return;

            const width = canvas.width;
            const height = canvas.height;
            const pad = { top: 25, right: 30, bottom: 30, left: 45 };
            const chartWidth = width - pad.left - pad.right;
            const chartHeight = height - pad.top - pad.bottom;

            ctx.clearRect(0, 0, width, height);
            ctx.fillStyle = '#0a0a0a';
            ctx.fillRect(0, 0, width, height);

            ctx.strokeStyle = '#2a2a2a';
            ctx.lineWidth = 1;
            ctx.fillStyle = '#666';
            ctx.font = '9px Arial';
            ctx.textAlign = 'right';

            for (let i = 0; i <= 4; i++) {
                const y = pad.top + (chartHeight * i / 4);
                ctx.beginPath();
                ctx.moveTo(pad.left, y);
                ctx.lineTo(width - pad.right, y);
                ctx.stroke();
                ctx.fillText((100 - i * 25) + '%', pad.left - 5, y + 3);
            }

            const minTime = filtered.timestamps[0];
            const maxTime = filtered.timestamps[filtered.timestamps.length - 1];
            const timeRange = maxTime - minTime || 1;

            const getX = (timestamp) => pad.left + ((timestamp - minTime) / timeRange) * chartWidth;
            const getY = (intensity) => pad.top + chartHeight - (intensity / 100) * chartHeight;

            const drawIntensityLine = (intensities, signals, color, targetSignal) => {
                ctx.strokeStyle = color;
                ctx.lineWidth = 2.5;
                ctx.lineJoin = 'round';
                ctx.lineCap = 'round';

                let lastValue = 0, lastX = 0, lastY = 0, inSegment = false;
                ctx.beginPath();

                for (let i = 0; i < filtered.timestamps.length; i++) {
                    const x = getX(filtered.timestamps[i]);
                    const intensity = intensities[i];
                    const signal = signals[i];

                    if (signal === targetSignal && intensity > 0) {
                        const y = getY(intensity);
                        if (!inSegment) {
                            ctx.moveTo(x, y);
                            inSegment = true;
                        } else {
                            if (intensity === lastValue) {
                                ctx.lineTo(x, lastY);
                            } else {
                                ctx.lineTo(x, lastY);
                                ctx.lineTo(x, y);
                            }
                        }
                        lastValue = intensity;
                        lastX = x;
                        lastY = y;
                    } else {
                        if (inSegment) ctx.lineTo(x, lastY);
                        inSegment = false;
                    }
                }

                if (inSegment) {
                    const endX = pad.left + chartWidth;
                    ctx.lineTo(endX, lastY);
                }
                ctx.stroke();

                ctx.fillStyle = color;
                for (let i = 0; i < filtered.timestamps.length; i++) {
                    if (signals[i] === targetSignal && intensities[i] > 0) {
                        const x = getX(filtered.timestamps[i]);
                        const y = getY(intensities[i]);
                        ctx.beginPath();
                        ctx.arc(x, y, 3, 0, Math.PI * 2);
                        ctx.fill();
                    }
                }

                if (inSegment && lastValue > 0) {
                    ctx.fillStyle = '#fff';
                    ctx.font = 'bold 10px Arial';
                    ctx.textAlign = 'left';
                    ctx.fillText(Math.round(lastValue) + '%', lastX + 6, lastY + 3);
                }
            };

            drawIntensityLine(filtered.sellIntensity, filtered.signals, '#f44336', 'sell');
            drawIntensityLine(filtered.buyIntensity, filtered.signals, '#4caf50', 'buy');

            ctx.fillStyle = '#666';
            ctx.font = '9px Arial';
            ctx.textAlign = 'center';

            const formatTime = (date) => date.getHours().toString().padStart(2, '0') + ':' + date.getMinutes().toString().padStart(2, '0');

            if (filtered.timestamps.length > 0) {
                const startTime = new Date(filtered.timestamps[0]);
                const endTime = new Date(filtered.timestamps[filtered.timestamps.length - 1]);
                ctx.fillText(formatTime(startTime), pad.left, height - 10);
                ctx.fillText(formatTime(endTime), width - pad.right, height - 10);
                if (filtered.timestamps.length > 2) {
                    const midIdx = Math.floor(filtered.timestamps.length / 2);
                    const midTime = new Date(filtered.timestamps[midIdx]);
                    ctx.fillText(formatTime(midTime), pad.left + chartWidth / 2, height - 10);
                }
            }

            ctx.fillStyle = '#00d4ff';
            ctx.font = 'bold 11px Arial';
            ctx.textAlign = 'left';
            ctx.fillText('Signal Intensity Over Time', pad.left, 15);
        }

        function makeChartDraggable() {
            const chart = document.getElementById('avgIntensityChart');
            if (!chart) return;

            let isDragging = false, currentX, currentY, initialX, initialY;

            chart.addEventListener('mousedown', (e) => {
                if (e.target.tagName === 'SELECT' || e.target.tagName === 'BUTTON') return;
                isDragging = true;
                initialX = e.clientX - chart.offsetLeft;
                initialY = e.clientY - chart.offsetTop;
                chart.style.cursor = 'grabbing';
            });

            document.addEventListener('mousemove', (e) => {
                if (!isDragging) return;
                e.preventDefault();
                currentX = e.clientX - initialX;
                currentY = e.clientY - initialY;
                chart.style.left = currentX + 'px';
                chart.style.top = currentY + 'px';
                chart.style.right = 'auto';
            });

            document.addEventListener('mouseup', () => {
                if (isDragging) {
                    isDragging = false;
                    chart.style.cursor = 'move';
                }
            });
        }

        function toggleIntensityChart() {
            const chart = document.getElementById('avgIntensityChart');
            if (chart) chart.style.display = chart.style.display === 'none' ? 'block' : 'none';
        }

        setTimeout(() => {
            makeChartDraggable();
            renderAvgIntensityChart();
        }, 500);

        function renderStatsBar(stats) {
            if (!stats) return;
            const deltaClass = stats.delta >= 0 ? 'positive' : 'negative';
            const deltaSign = stats.delta >= 0 ? '+' : '';
            
            let html = '<div class="stat-item"><span class="stat-label">Prezzo:</span><span class="stat-value">$' + (stats.price || 0).toLocaleString() + '</span></div>';
            html += '<div class="stat-item"><span class="stat-label">Vol:</span><span class="stat-value">' + (stats.volume || 0).toFixed(2) + '</span></div>';
            html += '<div class="stat-item"><span class="stat-label">Delta:</span><span class="stat-value ' + deltaClass + '">' + deltaSign + (stats.delta || 0).toFixed(2) + '</span></div>';
            
            if (orderBookData && (orderBookData.bids || []).length > 0) {
                let bidQty = 0, askQty = 0;
                (orderBookData.bids || []).forEach(p => { bidQty += parseFloat(p[1] || 0); });
                (orderBookData.asks || []).forEach(p => { askQty += parseFloat(p[1] || 0); });
                const obDelta = bidQty - askQty;
                const obClass = obDelta >= 0 ? 'positive' : 'negative';
                html += '<div class="stat-item"><span class="stat-label">OB Delta:</span><span class="stat-value ' + obClass + '">' + (obDelta >= 0 ? '+' : '') + obDelta.toFixed(2) + '</span></div>';
            }
            
            document.getElementById('stats-bar').innerHTML = html;
        }
        
        function resetView() {
            if (!currentData) return;
            viewStart = Math.max(0, currentData.bars.length - viewCount);
            document.getElementById('rangeSlider').max = Math.max(0, currentData.bars.length - viewCount);
            document.getElementById('rangeSlider').value = viewStart;
            renderChart();
        }
        
        function scrollBars(delta) {
            if (!currentData) return;
            viewStart = Math.max(0, Math.min(currentData.bars.length - viewCount, viewStart + delta));
            document.getElementById('rangeSlider').value = viewStart;
            renderChart();
        }
        
        function updateRange(value) {
            viewStart = parseInt(value);
            renderChart();
        }
        
        function applyZoom(value) {
            const scale = value / 100;
            viewCount = Math.round(22 * (100 / value));
            viewCount = Math.max(10, Math.min(150, viewCount));
            const style = document.createElement('style');
            style.id = 'zoom-style';
            const old = document.getElementById('zoom-style');
            if (old) old.remove();
            // CRITICO: override width con !important per forzare il cambio
            const baseW = 70;
            const w = Math.round(baseW * scale);
            style.textContent = 
                '.bar-column { width: ' + w + 'px !important; max-width: ' + w + 'px !important; min-width: ' + w + 'px !important; flex: 0 0 ' + w + 'px !important; } ' +
                '.price-cell { width: ' + w + 'px !important; max-width: ' + w + 'px !important; flex: 0 0 ' + w + 'px !important; }';
            document.head.appendChild(style);
            if (currentData) {
                viewCount = Math.round(22 * (100 / value));
                viewStart = Math.max(0, currentData.bars.length - viewCount);
                const s = document.getElementById('rangeSlider');
                if (s) { s.max = Math.max(0, currentData.bars.length - viewCount); s.value = viewStart; }
                renderChart();
            }
        }
        
        function renderChart() {
            if (!currentData || !currentData.bars) return;
            
            const displayBars = currentData.bars.slice(viewStart, viewStart + viewCount);
            document.getElementById('rangeLabel').textContent = (viewStart + 1) + '-' + (viewStart + displayBars.length);
            
            let allPrices = new Set();
            displayBars.forEach(bar => {
                if (bar.levels) bar.levels.forEach(l => { allPrices.add(l.price); });
            });
            const sortedPrices = Array.from(allPrices).sort((a, b) => b - a);
            
            const obMap = {};
            if (orderBookData && orderBookData.bids) {
                const step = parseFloat(currentStep);
                (orderBookData.bids || []).forEach(pair => {
                    const p = Math.round(parseFloat(pair[0] || 0) / step) * step;
                    if (!obMap[p]) obMap[p] = {bid: 0, ask: 0};
                    obMap[p].bid += parseFloat(pair[1] || 0);
                });
                (orderBookData.asks || []).forEach(pair => {
                    const p = Math.round(parseFloat(pair[0] || 0) / step) * step;
                    if (!obMap[p]) obMap[p] = {bid: 0, ask: 0};
                    obMap[p].ask += parseFloat(pair[1] || 0);
                });
            }
            
            let maxObVol = 0;
            Object.keys(obMap).forEach(k => { maxObVol = Math.max(maxObVol, obMap[k].bid, obMap[k].ask); });
            
            let html = '<div style="display: flex;"><table class="footprint-table" style="width: 100%; border-collapse: collapse;"><tr>';
            displayBars.forEach(bar => {
                html += '<td class="bar-column time-header"><div class="time-text">' + bar.time + '</div><div class="ohlc-text">O:' + bar.open + ' H:' + bar.high + ' L:' + bar.low + ' C:' + bar.close + '</div></td>';
            });
            html += '</tr>';
            
            sortedPrices.forEach(price => {
                html += '<tr class="price-row">';
                displayBars.forEach((bar, idx) => {
                    const level = (bar.levels || []).find(l => l.price === price);
                    const isLast = (idx === displayBars.length - 1);
                    
                    let cellClass = 'bar-column price-cell';
                    if (level && level.in_body) cellClass += bar.bullish ? ' in-body bullish' : ' in-body bearish';
                    if (bar.open_rounded === price) cellClass += bar.bullish ? ' open-level' : ' open-level bearish';
                    if (bar.close_rounded === price) cellClass += bar.bullish ? ' close-level' : ' close-level bearish';
                    
                    let content = '';
                    if (level && (level.bid > 0 || level.ask > 0)) {
                        const bidSig = level.significant && level.bid > 0 ? ' significant' : '';
                        const askSig = level.significant && level.ask > 0 ? ' significant' : '';
                        content = '<div class="price-cell-content">' + 
                                  (level.bid > 0 ? '<div class="bid-value' + bidSig + '">' + level.bid.toFixed(1) + '</div>' : '') +
                                  (level.ask > 0 ? '<div class="ask-value' + askSig + '">' + level.ask.toFixed(1) + '</div>' : '') +
                                  '</div>' + content;
                    }
                    
                    let obOverlay = '';
                    if (isLast && obMap[price]) {
                        const bidW = maxObVol > 0 ? (obMap[price].bid / maxObVol) * 40 : 0;
                        const askW = maxObVol > 0 ? (obMap[price].ask / maxObVol) * 40 : 0;
                        if (bidW > 0) obOverlay += '<div class="ob-bid-bar" style="width: ' + bidW + '%"></div>';
                        if (askW > 0) obOverlay += '<div class="ob-ask-bar" style="width: ' + askW + '%; left: ' + bidW + '%;"></div>';
                    }
                    
                    html += '<td class="' + cellClass + '"><div class="orderbook-overlay">' + obOverlay + '</div>' + content + '</td>';
                });
                html += '</tr>';
            });
            
            html += '<tr>';
            displayBars.forEach(bar => {
                const deltaClass = bar.delta >= 0 ? 'positive' : 'negative';
                html += '<td class="bar-column delta-footer"><div class="delta-value ' + deltaClass + '">' + (bar.delta >= 0 ? '+' : '') + bar.delta.toFixed(1) + '</div></td>';
            });
            html += '</tr></table>';
            
            // Crea l'asse Y dei prezzi a DESTRA
            const priceAxisHtml = sortedPrices.map(price => {
                return '<div style="height: 13px; display: flex; align-items: center; justify-content: flex-start; font-size: 9px; color: #aaa; padding-left: 8px;">' + price + '</div>';
            }).join('');

            // Assembla tutto: tabella a sinistra + asse Y a destra
            const finalHtml = html + '</table><div style="display: flex; flex-direction: column; justify-content: space-between; padding-left: 8px; border-left: 1px solid #444; min-width: 70px;">' + priceAxisHtml + '</div></div>';
            document.getElementById('chart-container').innerHTML = finalHtml;
            renderTradingSignal();
            renderPhaseDistribution();
            renderAvgIntensityChart();
        }
        
        loadData();
    

    // ============================================
    // ORDER PANEL FUNCTIONS
    // ============================================

    let orderPanelCollapsed = false;

    function toggleOrderPanel() {
        const panel = document.getElementById('order-panel');
        orderPanelCollapsed = !orderPanelCollapsed;

        if (orderPanelCollapsed) {
            panel.classList.add('collapsed');
        } else {
            panel.classList.remove('collapsed');
        }
    }

    function loadRelevantOrders() {
        const interval = document.getElementById('interval').value;

        fetch(`/api/relevant_orders?interval=${interval}`)
            .then(r => r.json())
            .then(data => {
                if (data.error) {
                    console.error('Error loading orders:', data.error);
                    return;
                }

                renderOrderPanel(data);
            })
            .catch(e => {
                console.error('Error fetching relevant orders:', e);
            });
    }

    function renderOrderPanel(data) {
        // Render summary
        const summaryHtml = `
            <div class="order-summary-item">
                <span class="order-summary-label">Prezzo Corrente</span>
                <span class="order-summary-value">$${data.current_price.toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2})}</span>
            </div>
            <div class="order-summary-item">
                <span class="order-summary-label">Range (±1%)</span>
                <span class="order-summary-value">$${data.price_range.min.toFixed(2)} - $${data.price_range.max.toFixed(2)}</span>
            </div>
            <div class="order-summary-item">
                <span class="order-summary-label">Total Bids</span>
                <span class="order-summary-value positive">${data.summary.total_bid_qty.toFixed(4)} BTC</span>
            </div>
            <div class="order-summary-item">
                <span class="order-summary-label">Total Asks</span>
                <span class="order-summary-value negative">${data.summary.total_ask_qty.toFixed(4)} BTC</span>
            </div>
            <div class="order-summary-item">
                <span class="order-summary-label">OB Delta</span>
                <span class="order-summary-value ${data.summary.delta >= 0 ? 'positive' : 'negative'}">
                    ${data.summary.delta >= 0 ? '+' : ''}${data.summary.delta.toFixed(4)} BTC
                </span>
            </div>
            <div class="order-summary-item">
                <span class="order-summary-label">Bids Value</span>
                <span class="order-summary-value">$${(data.summary.total_bid_value).toLocaleString('en-US', {minimumFractionDigits: 0, maximumFractionDigits: 0})}</span>
            </div>
            <div class="order-summary-item">
                <span class="order-summary-label">Asks Value</span>
                <span class="order-summary-value">$${(data.summary.total_ask_value).toLocaleString('en-US', {minimumFractionDigits: 0, maximumFractionDigits: 0})}</span>
            </div>
        `;

        document.getElementById('order-summary').innerHTML = summaryHtml;

        // Render bids table
        const maxBidQty = Math.max(...data.bids.map(b => b.quantity), 0.0001);
        const bidsHtml = data.bids.map((bid, idx) => {
            const pct = ((bid.quantity / data.summary.total_bid_qty) * 100).toFixed(1);
            const barWidth = (bid.quantity / maxBidQty) * 100;
            return `
                <tr>
                    <td>
                        $${bid.price.toFixed(2)}
                        <div class="qty-bar">
                            <div class="qty-bar-fill bid-bar-fill" style="width: ${barWidth}%"></div>
                        </div>
                    </td>
                    <td>${bid.quantity.toFixed(4)}</td>
                    <td>$${bid.total.toLocaleString('en-US', {minimumFractionDigits: 0, maximumFractionDigits: 0})}</td>
                    <td>${pct}%</td>
                </tr>
            `;
        }).join('');

        document.getElementById('bids-tbody').innerHTML = bidsHtml;

        // Render asks table
        const maxAskQty = Math.max(...data.asks.map(a => a.quantity), 0.0001);
        const asksHtml = data.asks.map((ask, idx) => {
            const pct = ((ask.quantity / data.summary.total_ask_qty) * 100).toFixed(1);
            const barWidth = (ask.quantity / maxAskQty) * 100;
            return `
                <tr>
                    <td>
                        $${ask.price.toFixed(2)}
                        <div class="qty-bar">
                            <div class="qty-bar-fill ask-bar-fill" style="width: ${barWidth}%"></div>
                        </div>
                    </td>
                    <td>${ask.quantity.toFixed(4)}</td>
                    <td>$${ask.total.toLocaleString('en-US', {minimumFractionDigits: 0, maximumFractionDigits: 0})}</td>
                    <td>${pct}%</td>
                </tr>
            `;
        }).join('');

        document.getElementById('asks-tbody').innerHTML = asksHtml;
    }

    // Load relevant orders on page load and refresh
    function loadDataWithOrders() {
        loadData();
        setTimeout(() => {
            loadRelevantOrders();
        }, 500);
    }

    // Override loadData to also update orders
    const originalLoadData = loadData;
    loadData = function() {
        originalLoadData();
        setTimeout(() => {
            loadRelevantOrders();
        }, 500);
    };

    // Auto-refresh orders every 10 seconds
    setInterval(loadRelevantOrders, 10000);

    // Initial load
    window.addEventListener('load', () => {
        setTimeout(loadRelevantOrders, 1000);
    });

    // ==================== 20% FIXED RANGE CHART ====================

    

    // Toggle order panel display (collapsible)
    function toggleOrderPanelDisplay() {
        const modal = document.getElementById('chart-modal');
        const arrow = document.getElementById('toggle-arrow');

        if (modal) {
            modal.classList.toggle('active');
            if (arrow) {
                arrow.classList.toggle('rotated');
            }
            if (modal.classList.contains('active')) {
                loadRelevantOrders();
            }
        }
    }
    
    function toggleChartModal() {
        const modal = document.getElementById('chart-modal');
        if (modal) {
            modal.classList.toggle('active');
            if (modal.classList.contains('active')) {
                loadRelevantOrders();
            }
        }
    }

    function loadRelevantOrders() {
        const status = document.getElementById('chart-status');
        if (status) status.classList.add('loading');

        const interval = document.getElementById('interval') ? 
                        document.getElementById('interval').value : '1m';
        const chartTf = document.getElementById('chart-timeframe') ? 
                        document.getElementById('chart-timeframe').value : '15m';

        fetch(`/api/relevant_orders?interval=${interval}&chart_tf=${chartTf}`)
            .then(r => r.json())
            .then(data => {
                if (data.error) {
                    console.error('Error:', data.error);
                    return;
                }
                renderOrderPanel(data);
                if (status) status.classList.remove('loading');
            })
            .catch(e => {
                console.error('Error:', e);
                if (status) status.classList.remove('loading');
            });
    }

    function renderOrderPanel(data) {
        const rangeTotal = (data.price_range.total_range).toFixed(0);
        const summaryHtml = `
            <div class="stat-card primary">
                <span class="stat-label">💰 Price</span>
                <span class="stat-value primary">$${data.current_price.toLocaleString('en-US', {minimumFractionDigits: 2})}</span>
            </div>
            <div class="stat-card primary">
                <span class="stat-label">📊 TF</span>
                <span class="stat-value primary">${data.chart_timeframe.toUpperCase()}</span>
            </div>
            <div class="stat-card">
                <span class="stat-label">📏 Y-Range</span>
                <span class="stat-value">±${data.price_range.pct}% ($${rangeTotal})</span>
            </div>
            <div class="stat-card success">
                <span class="stat-label">🟢 BID</span>
                <span class="stat-value success">${data.bids.length} · ${data.summary.total_bid_qty.toFixed(2)} BTC</span>
            </div>
            <div class="stat-card danger">
                <span class="stat-label">🔴 ASK</span>
                <span class="stat-value danger">${data.asks.length} · ${data.summary.total_ask_qty.toFixed(2)} BTC</span>
            </div>
            <div class="stat-card ${data.summary.delta >= 0 ? 'success' : 'danger'}">
                <span class="stat-label">⚖️ Delta</span>
                <span class="stat-value ${data.summary.delta >= 0 ? 'success' : 'danger'}">
                    ${data.summary.delta >= 0 ? '+' : ''}${data.summary.delta.toFixed(3)} BTC
                </span>
            </div>
            <div class="stat-card">
                <span class="stat-label">💵 BID $</span>
                <span class="stat-value">$${(data.summary.total_bid_value / 1000).toFixed(1)}K</span>
            </div>
            <div class="stat-card">
                <span class="stat-label">💵 ASK $</span>
                <span class="stat-value">$${(data.summary.total_ask_value / 1000).toFixed(1)}K</span>
            </div>
        `;
        document.getElementById('order-summary').innerHTML = summaryHtml;
        draw20PercentChart(data);
    }

    function draw20PercentChart(data) {
        const canvas = document.getElementById('orderChart');
        if (!canvas) return;

        const ctx = canvas.getContext('2d');
        const container = canvas.parentElement;

        canvas.width = container.clientWidth - 50;
        canvas.height = container.clientHeight - 50;

        const width = canvas.width;
        const height = canvas.height;

        // PADDING AUMENTATO per 20% range
        const paddingTop = 420;
        const paddingBottom = 420;
        const paddingLeft = 160;
        const paddingRight = 80;

        ctx.clearRect(0, 0, width, height);

        const priceHistory = data.price_history || [];
        const allPrices = [
            ...priceHistory.map(p => p.price),
            ...data.bids.map(b => b.price),
            ...data.asks.map(a => a.price),
            data.current_price
        ];

        if (allPrices.length === 0) {
            ctx.fillStyle = '#888';
            ctx.font = '20px sans-serif';
            ctx.textAlign = 'center';
            ctx.fillText('No data', width/2, height/2);
            return;
        }

        // Use API provided min/max for exact 20% range
        const minPrice = data.price_range.min;
        const maxPrice = data.price_range.max;
        const priceRange = maxPrice - minPrice;

        function priceToY(price) {
         // ASSE Y CORRETTO: prezzi alti in ALTO, prezzi bassi in BASSO
         return paddingTop + ((maxPrice - price) / priceRange) * (height - paddingTop - paddingBottom);
        }


        function indexToX(index, total) {
            if (total <= 1) return paddingLeft;
            return paddingLeft + (index / (total - 1)) * (width - paddingLeft - paddingRight);
        }

        // Background
        const bgGradient = ctx.createLinearGradient(0, 0, 0, height);
        bgGradient.addColorStop(0, 'rgba(0, 10, 20, 0.3)');
        bgGradient.addColorStop(1, 'rgba(0, 0, 0, 0.5)');
        ctx.fillStyle = bgGradient;
        ctx.fillRect(0, 0, width, height);

        // === 20 GRID LEVELS ===
        const gridLevels = 20;

        for (let i = 0; i <= gridLevels; i++) {
            const y = paddingTop + (i / gridLevels) * (height - paddingTop - paddingBottom);
            const isMajor = i % 2 === 0;

            // Grid line
            ctx.strokeStyle = isMajor ? 'rgba(255, 255, 255, 0.12)' : 'rgba(255, 255, 255, 0.05)';
            ctx.lineWidth = isMajor ? 1.5 : 1;
            ctx.beginPath();
            ctx.moveTo(paddingLeft, y);
            ctx.lineTo(width - paddingRight, y);
            ctx.stroke();

            // Labels solo per major
            if (isMajor) {
                const price = maxPrice - (i / gridLevels) * priceRange;
                const labelText = '$' + price.toFixed(2);

                ctx.fillStyle = 'rgba(0, 0, 0, 0.8)';
                ctx.fillRect(paddingLeft - 145, y - 14, 135, 28);

                ctx.strokeStyle = i === 0 || i === gridLevels ? '#00d4ff' : 'rgba(0, 212, 255, 0.3)';
                ctx.lineWidth = i === 0 || i === gridLevels ? 2 : 1;
                ctx.strokeRect(paddingLeft - 145, y - 14, 135, 28);

                ctx.fillStyle = i === 0 || i === gridLevels ? '#00d4ff' : '#bbb';
                ctx.font = i === 0 || i === gridLevels ? 'bold 18px monospace' : 'bold 16px monospace';
                ctx.textAlign = 'right';
                ctx.fillText(labelText, paddingLeft - 18, y + 6);
            }
        }

        // Vertical grid
        ctx.strokeStyle = 'rgba(255, 255, 255, 0.04)';
        ctx.lineWidth = 1;
        for (let i = 0; i <= 10; i++) {
            const x = paddingLeft + (i / 10) * (width - paddingLeft - paddingRight);
            ctx.beginPath();
            ctx.moveTo(x, paddingTop);
            ctx.lineTo(x, height - paddingBottom);
            ctx.stroke();
        }

        // === ORDER LINES ===

        // BID lines
        data.bids.forEach((bid) => {
            const y = priceToY(bid.price);
            const thickness = Math.min(20, 5 + (bid.quantity / 3) * 15);
            const alpha = Math.min(0.9, 0.5 + (bid.quantity / 10) * 0.4);

            ctx.shadowColor = 'rgba(0, 255, 100, 0.7)';
            ctx.shadowBlur = 18;
            ctx.strokeStyle = `rgba(0, 255, 100, ${alpha})`;
            ctx.lineWidth = thickness;
            ctx.beginPath();
            ctx.moveTo(paddingLeft, y);
            ctx.lineTo(width - paddingRight, y);
            ctx.stroke();
            ctx.shadowBlur = 0;

            const labelText = `BID $${bid.price.toFixed(2)} • ${bid.quantity.toFixed(2)} BTC`;
            ctx.font = 'bold 14px system-ui';
            const textWidth = ctx.measureText(labelText).width;

            const pillX = paddingLeft + 30;
            const pillY = y - thickness/2 - 24;

            ctx.fillStyle = 'rgba(0, 120, 60, 0.85)';
            ctx.beginPath();
            ctx.roundRect(pillX - 10, pillY, textWidth + 20, 22, 11);
            ctx.fill();

            ctx.fillStyle = '#0f0';
            ctx.textAlign = 'left';
            ctx.fillText(labelText, pillX, pillY + 16);
        });

        // ASK lines
        data.asks.forEach((ask) => {
            const y = priceToY(ask.price);
            const thickness = Math.min(20, 5 + (ask.quantity / 3) * 15);
            const alpha = Math.min(0.9, 0.5 + (ask.quantity / 10) * 0.4);

            ctx.shadowColor = 'rgba(255, 50, 50, 0.7)';
            ctx.shadowBlur = 18;
            ctx.strokeStyle = `rgba(255, 50, 50, ${alpha})`;
            ctx.lineWidth = thickness;
            ctx.beginPath();
            ctx.moveTo(paddingLeft, y);
            ctx.lineTo(width - paddingRight, y);
            ctx.stroke();
            ctx.shadowBlur = 0;

            const labelText = `ASK $${ask.price.toFixed(2)} • ${ask.quantity.toFixed(2)} BTC`;
            ctx.font = 'bold 14px system-ui';
            const textWidth = ctx.measureText(labelText).width;

            const pillX = paddingLeft + 30;
            const pillY = y + thickness/2 + 6;

            ctx.fillStyle = 'rgba(120, 25, 25, 0.85)';
            ctx.beginPath();
            ctx.roundRect(pillX - 10, pillY, textWidth + 20, 22, 11);
            ctx.fill();

            ctx.fillStyle = '#f55';
            ctx.textAlign = 'left';
            ctx.fillText(labelText, pillX, pillY + 16);
        });

        // Current price
        const currentY = priceToY(data.current_price);
        ctx.shadowColor = 'rgba(255, 255, 0, 0.9)';
        ctx.shadowBlur = 25;
        ctx.strokeStyle = '#ffff00';
        ctx.lineWidth = 5;
        ctx.setLineDash([18, 12]);
        ctx.beginPath();
        ctx.moveTo(paddingLeft, currentY);
        ctx.lineTo(width - paddingRight, currentY);
        ctx.stroke();
        ctx.setLineDash([]);
        ctx.shadowBlur = 0;

        const priceText = 'CURRENT: $' + data.current_price.toFixed(2);
        ctx.font = 'bold 22px system-ui';
        const priceTextWidth = ctx.measureText(priceText).width;

        ctx.fillStyle = 'rgba(90, 90, 0, 0.9)';
        ctx.beginPath();
        ctx.roundRect(width - paddingRight - priceTextWidth - 50, currentY - 35, priceTextWidth + 35, 32, 16);
        ctx.fill();

        ctx.fillStyle = '#ffff00';
        ctx.textAlign = 'right';
        ctx.shadowColor = 'rgba(0, 0, 0, 1)';
        ctx.shadowBlur = 5;
        ctx.fillText(priceText, width - paddingRight - 22, currentY - 10);
        ctx.shadowBlur = 0;

        // === BLUE LINE ON TOP ===
        if (priceHistory.length > 1) {
            const lineGradient = ctx.createLinearGradient(paddingLeft, 0, width - paddingRight, 0);
            lineGradient.addColorStop(0, 'rgba(0, 180, 255, 0.6)');
            lineGradient.addColorStop(0.5, 'rgba(0, 212, 255, 1)');
            lineGradient.addColorStop(1, 'rgba(0, 180, 255, 0.6)');

            ctx.shadowColor = 'rgba(0, 212, 255, 1)';
            ctx.shadowBlur = 25;
            ctx.strokeStyle = lineGradient;
            ctx.lineWidth = 7;
            ctx.lineCap = 'round';
            ctx.lineJoin = 'round';

            ctx.beginPath();
            priceHistory.forEach((point, idx) => {
                const x = indexToX(idx, priceHistory.length);
                const y = priceToY(point.price);
                if (idx === 0) ctx.moveTo(x, y);
                else ctx.lineTo(x, y);
            });
            ctx.stroke();
            ctx.shadowBlur = 0;

            ctx.fillStyle = '#00d4ff';
            ctx.shadowColor = 'rgba(0, 212, 255, 0.8)';
            ctx.shadowBlur = 8;
            priceHistory.forEach((point, idx) => {
                if (idx % 4 === 0 || idx === priceHistory.length - 1) {
                    const x = indexToX(idx, priceHistory.length);
                    const y = priceToY(point.price);
                    ctx.beginPath();
                    ctx.arc(x, y, 5, 0, Math.PI * 2);
                    ctx.fill();
                }
            });
            ctx.shadowBlur = 0;
        }

        // Legend
        const legendY = 45;
        const legendX = 35;

        ctx.fillStyle = 'rgba(0, 0, 0, 0.85)';
        ctx.beginPath();
        ctx.roundRect(legendX - 12, legendY - 28, 750, 65, 14);
        ctx.fill();

        ctx.font = 'bold 17px system-ui';
        ctx.textAlign = 'left';

        ctx.fillStyle = 'rgba(0, 212, 255, 0.25)';
        ctx.beginPath();
        ctx.roundRect(legendX, legendY - 22, 130, 32, 10);
        ctx.fill();
        ctx.fillStyle = '#00d4ff';
        ctx.fillText('Range: ±20%', legendX + 12, legendY);

        let lx = legendX + 160;

        ctx.strokeStyle = '#00d4ff';
        ctx.lineWidth = 6;
        ctx.beginPath();
        ctx.moveTo(lx, legendY - 6);
        ctx.lineTo(lx + 65, legendY - 6);
        ctx.stroke();
        ctx.fillStyle = '#00d4ff';
        ctx.fillText('Price', lx + 75, legendY);

        lx += 170;

        ctx.strokeStyle = '#0f0';
        ctx.lineWidth = 7;
        ctx.beginPath();
        ctx.moveTo(lx, legendY - 6);
        ctx.lineTo(lx + 65, legendY - 6);
        ctx.stroke();
        ctx.fillStyle = '#0f0';
        ctx.fillText('BID', lx + 75, legendY);

        lx += 140;

        ctx.strokeStyle = '#f55';
        ctx.lineWidth = 7;
        ctx.beginPath();
        ctx.moveTo(lx, legendY - 6);
        ctx.lineTo(lx + 65, legendY - 6);
        ctx.stroke();
        ctx.fillStyle = '#f55';
        ctx.fillText('ASK', lx + 75, legendY);

        if (!CanvasRenderingContext2D.prototype.roundRect) {
            CanvasRenderingContext2D.prototype.roundRect = function(x, y, w, h, r) {
                if (w < 2 * r) r = w / 2;
                if (h < 2 * r) r = h / 2;
                this.beginPath();
                this.moveTo(x + r, y);
                this.arcTo(x + w, y, x + w, y + h, r);
                this.arcTo(x + w, y + h, x, y + h, r);
                this.arcTo(x, y + h, x, y, r);
                this.arcTo(x, y, x + w, y, r);
                this.closePath();
                return this;
            };
        }
    }

    function onChartTimeframeChange() {
        const status = document.getElementById('chart-status');
        if (status) status.classList.add('loading');
        loadRelevantOrders();
    }

    document.addEventListener('keydown', function(e) {
        if (e.key === 'Escape') {
            const modal = document.getElementById('chart-modal');
            if (modal && modal.classList.contains('active')) {
                toggleChartModal();
            }
        }
    });

    setInterval(function() {
        const modal = document.getElementById('chart-modal');
        if (modal && modal.classList.contains('active')) {
            loadRelevantOrders();
        }
    }, 15000);

    
    </script>

    <!-- GRAFICO AVG INTENSITY TRASCINABILE -->
    <div id="avgIntensityChart" class="draggable-chart" style="position: fixed; top: 400px; right: 20px; width: 500px; background: #1e1e1e; border-radius: 8px; border: 2px solid #00d4ff; box-shadow: 0 4px 12px rgba(0,212,255,0.3); z-index: 1000; cursor: move;">
        <div class="chart-header" style="padding: 10px 15px; background: #0f0f0f; border-bottom: 1px solid #333; cursor: move; display: flex; justify-content: space-between; align-items: center; border-radius: 6px 6px 0 0;">
            <div style="font-size: 13px; font-weight: 600; color: #00d4ff;">
                📊 Avg Intensity Trend
            </div>
            <div style="display: flex; gap: 8px; align-items: center;">
                <select id="intensityTimeWindow" onchange="renderAvgIntensityChart()" style="background: #2a2a2a; color: #fff; border: 1px solid #444; border-radius: 4px; padding: 3px 8px; font-size: 10px; cursor: pointer;">
                    <option value="10">10 min</option>
                    <option value="15">15 min</option>
                    <option value="30" selected>30 min</option>
                    <option value="60">60 min</option>
                    <option value="120">2 ore</option>
                </select>
                <button onclick="toggleIntensityChart()" style="background: #f44336; color: #fff; border: none; border-radius: 3px; padding: 3px 8px; font-size: 10px; cursor: pointer; font-weight: 600;">✕</button>
            </div>
        </div>
        <div style="padding: 15px;">
            <canvas id="avgIntensityCanvas" width="470" height="200"></canvas>
        </div>
        <div style="padding: 5px 15px 10px 15px; font-size: 10px; color: #888; display: flex; gap: 15px; justify-content: center;">
            <span><span style="color: #4caf50; font-size: 14px;">●</span> BUY</span>
            <span><span style="color: #f44336; font-size: 14px;">●</span> SELL</span>
            <span><span style="color: #888; font-size: 14px;">—</span> Intensity %</span>
        </div>
    </div>


    <!-- MODAL with FIXED 20% Y-Range -->
    <div class="chart-modal" id="chart-modal">
        <div class="chart-modal-backdrop" onclick="toggleChartModal()"></div>
        <div class="chart-modal-content">
            <div class="chart-modal-header">
                <div class="header-left">
                    <h2>📊 Order Block Analysis</h2>
                    <span class="header-subtitle">Ordini > 2 BTC • Range Fisso ±20%</span>
                </div>
                <div class="header-controls">
                    <div class="control-group">
                        <label>Timeframe</label>
                        <select id="chart-timeframe" onchange="onChartTimeframeChange()">
                            <option value="1m">1 Min</option>
                            <option value="5m">5 Min</option>
                            <option value="15m" selected>15 Min</option>
                            <option value="30m">30 Min</option>
                            <option value="1h">1 Hour</option>
                            <option value="4h">4 Hours</option>
                            <option value="1d">1 Day</option>
                        </select>
                    </div>
                    <span id="chart-status" class="status-indicator">●</span>
                    <button class="btn-close" onclick="toggleChartModal()">✕</button>
                </div>
            </div>

            <div class="chart-stats" id="order-summary">
                <div class="stats-loading">Loading...</div>
            </div>

            <div class="chart-canvas-wrapper">
                <canvas id="orderChart"></canvas>
            </div>
        </div>
    </div>

    
    <!-- COLLAPSIBLE ORDER PANEL (replaces FAB button) -->
    <div class="order-panel-toggle" id="order-panel-toggle" onclick="toggleOrderPanelDisplay()">
        <div class="toggle-bar">
            <div class="toggle-icon">📊</div>
            <span class="toggle-text">Order Block Analysis</span>
            <div class="toggle-arrow" id="toggle-arrow">▼</div>
        </div>
    </div>


</body>
</html>
   

    """
    return html

@app.route('/api/data')
def get_data():
    interval = request.args.get('interval', '1m')
    step = float(request.args.get('step', 10))
    update_last_only = request.args.get('update_last', 'false') == 'true'

    # Parametri filtro dalla richiesta
    filter_mode = request.args.get('filter_mode', TRADE_FILTER_MODE)
    filter_percentile = int(request.args.get('filter_percentile', TRADE_PERCENTILE))
    filter_min_qty = float(request.args.get('filter_min_qty', TRADE_MIN_QTY_PERCENT))
    filter_top_n = int(request.args.get('filter_top_n', TRADE_TOP_N))

    # Cache key include anche i parametri filtro
    cache_key = f"{interval}_{step}_{filter_mode}_{filter_percentile}"
    
    with CACHE['lock']:
        if cache_key in CACHE['data'] and not update_last_only:
            entry = CACHE['data'][cache_key]
            return jsonify(entry['data'])
        
        data = process_data(interval, step, update_last_only, filter_mode, filter_percentile, filter_min_qty, filter_top_n)
        
        if not update_last_only:
            CACHE['data'][cache_key] = {'data': data, 'timestamp': time.time()}
    
    return jsonify(data)

@app.route('/api/orderbook')
def get_orderbook():
    with CACHE['lock']:
        if 'orderbook' in CACHE and time.time() - CACHE['orderbook'].get('timestamp', 0) < 3:
            return jsonify(CACHE['orderbook']['data'])
        
        ob_data = fetch_orderbook()
        CACHE['orderbook'] = {'data': ob_data, 'timestamp': time.time()}
    
    return jsonify(ob_data)


@app.route('/api/relevant_orders')
def get_relevant_orders():
    """
    Ordini rilevanti orderbook - RANGE FISSO ±20%
    """
    try:
        interval = request.args.get('interval', '1m')
        chart_tf = request.args.get('chart_tf', '15m')

        klines = fetch_klines(chart_tf, limit=150)
        if not klines:
            return jsonify({'error': 'Cannot fetch price data'}), 500

        current_price = float(klines[-1][4])

        price_history = []
        for k in klines[-50:]:
            price_history.append({
                'time': int(k[0]),
                'price': float(k[4]),
                'high': float(k[2]),
                'low': float(k[3])
            })

        ob_data = fetch_orderbook()
        if not ob_data or 'bids' not in ob_data or 'asks' not in ob_data:
            return jsonify({'error': 'Cannot fetch orderbook'}), 500

        # RANGE FISSO: ±0.420%
        FIXED_RANGE_PCT = 0.420
        price_range = current_price * (FIXED_RANGE_PCT / 100.0)
        min_price = current_price - price_range
        max_price = current_price + price_range

        MIN_BTC_THRESHOLD = 3.0

        relevant_bids = []
        for bid in ob_data['bids']:
            price = float(bid[0])
            qty = float(bid[1])
            if min_price <= price <= max_price and qty > MIN_BTC_THRESHOLD:
                relevant_bids.append({
                    'price': price,
                    'quantity': qty,
                    'total': price * qty
                })

        relevant_asks = []
        for ask in ob_data['asks']:
            price = float(ask[0])
            qty = float(ask[1])
            if min_price <= price <= max_price and qty > MIN_BTC_THRESHOLD:
                relevant_asks.append({
                    'price': price,
                    'quantity': qty,
                    'total': price * qty
                })

        relevant_bids = sorted(relevant_bids, key=lambda x: x['quantity'], reverse=True)
        relevant_asks = sorted(relevant_asks, key=lambda x: x['quantity'], reverse=True)

        total_bid_qty = sum(b['quantity'] for b in relevant_bids)
        total_ask_qty = sum(a['quantity'] for a in relevant_asks)
        total_bid_value = sum(b['total'] for b in relevant_bids)
        total_ask_value = sum(a['total'] for a in relevant_asks)

        return jsonify({
            'current_price': current_price,
            'price_range': {
                'min': min_price, 
                'max': max_price, 
                'pct': FIXED_RANGE_PCT,
                'total_range': price_range * 2
            },
            'price_history': price_history,
            'chart_timeframe': chart_tf,
            'bids': relevant_asks,
            'asks': relevant_bids,
            'summary': {
                'total_bid_qty': total_bid_qty,
                'total_ask_qty': total_ask_qty,
                'total_bid_value': total_bid_value,
                'total_ask_value': total_ask_value,
                'delta': total_bid_qty - total_ask_qty,
                'min_btc_threshold': MIN_BTC_THRESHOLD
            }
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    print("=" * 70)
    print("BTC FOOTPRINT v9 - LAYOUT STESSO LATO")
    print("=" * 70)
    print("✅ Bid e Ask affiancati da sinistra")
    print("✅ Layout orizzontale comprensibile")
    print("✅ Cache infinita + Auto-refresh")
    print("=" * 70)
    print("http://localhost:5001")
    print("=" * 70)
    app.run(debug=True, host='0.0.0.0', port=5001, threaded=True)