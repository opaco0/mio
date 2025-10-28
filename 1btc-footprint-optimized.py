#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BTC FOOTPRINT ORDERFLOW - VERSIONE OTTIMIZZATA
Con zoom, performance migliorate e linee open/close
"""

from flask import Flask, jsonify, request
import time
import requests
from collections import defaultdict
from datetime import datetime
import threading

app = Flask(__name__)

SYMBOL_BINANCE = "BTCUSDT"
SYMBOL_BYBIT = "BTCUSDT"
CACHE_TTL = 30

# Cache globale con lock per thread-safety
CACHE = {
    'timestamp': 0,
    'data': {},
    'lock': threading.Lock()
}

def get_interval_ms(interval):
    """Converte intervallo in millisecondi"""
    intervals = {"1m": 60000, "5m": 300000, "15m": 900000, "30m": 1800000, "1h": 3600000}
    return intervals.get(interval, 60000)

def fetch_binance_klines(interval, limit=100):
    """Scarica candele da Binance (aumentato limite)"""
    try:
        url = "https://api.binance.com/api/v3/klines"
        params = {"symbol": SYMBOL_BINANCE, "interval": interval, "limit": limit}
        r = requests.get(url, params=params, timeout=8)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[ERROR] Binance Klines: {e}")
        return []

def fetch_binance_trades(start_ms, end_ms, max_trades=1000):
    """Scarica trade aggregati da Binance"""
    try:
        url = "https://api.binance.com/api/v3/aggTrades"
        params = {
            "symbol": SYMBOL_BINANCE,
            "startTime": start_ms,
            "endTime": end_ms,
            "limit": max_trades
        }
        r = requests.get(url, params=params, timeout=8)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[ERROR] Binance Trades: {e}")
        return []

def fetch_bybit_klines(interval, limit=100):
    """Scarica candele da Bybit"""
    try:
        interval_map = {"1m": "1", "5m": "5", "15m": "15", "30m": "30", "1h": "60"}
        bybit_interval = interval_map.get(interval, "1")
        
        url = "https://api.bybit.com/v5/market/kline"
        params = {
            "category": "spot",
            "symbol": SYMBOL_BYBIT,
            "interval": bybit_interval,
            "limit": limit
        }
        r = requests.get(url, params=params, timeout=8)
        r.raise_for_status()
        data = r.json()
        
        if data.get("retCode") == 0:
            result = []
            for k in data["result"]["list"]:
                result.append([
                    int(k[0]), k[1], k[2], k[3], k[4], k[5],
                    int(k[0]) + get_interval_ms(interval) - 1,
                    "0", "0", "0", "0", "0"
                ])
            return list(reversed(result))
        return []
    except Exception as e:
        print(f"[ERROR] Bybit Klines: {e}")
        return []

def fetch_bybit_trades(start_ms, end_ms):
    """Scarica trade da Bybit"""
    try:
        url = "https://api.bybit.com/v5/market/recent-trade"
        params = {"category": "spot", "symbol": SYMBOL_BYBIT, "limit": 1000}
        r = requests.get(url, params=params, timeout=8)
        r.raise_for_status()
        data = r.json()
        
        if data.get("retCode") == 0:
            trades = []
            for t in data["result"]["list"]:
                ts = int(t["time"])
                if start_ms <= ts <= end_ms:
                    trades.append({
                        'T': ts,
                        'p': t["price"],
                        'q': t["size"],
                        'm': t["side"] == "Sell"
                    })
            return trades
        return []
    except Exception as e:
        print(f"[ERROR] Bybit Trades: {e}")
        return []

def round_price(price, step):
    """Arrotonda prezzo al multiplo di step"""
    return round(price / step) * step

def process_data(interval, step):
    """Processa dati footprint - OTTIMIZZATO"""
    
    # Scarica candele (più dati per storia)
    binance_klines = fetch_binance_klines(interval, limit=100)
    
    if not binance_klines:
        return {"bars": [], "stats": {"error": "Nessun dato disponibile"}}

    bars = []
    total_volume = 0
    total_delta = 0
    interval_ms = get_interval_ms(interval)

    for i, k in enumerate(binance_klines):
        ts = int(k[0])
        o, h, l, c = float(k[1]), float(k[2]), float(k[3]), float(k[4])
        vol = float(k[5])

        min_price = round_price(l, step)
        max_price = round_price(h, step)

        # Scarica trade solo per ultime 20 candele (ottimizzato)
        trades_binance = []
        trades_bybit = []
        
        if i >= len(binance_klines) - 20:
            start_ms = ts
            end_ms = ts + interval_ms - 1
            trades_binance = fetch_binance_trades(start_ms, end_ms, max_trades=1000)
            trades_bybit = fetch_bybit_trades(start_ms, end_ms)

        bid_vol = defaultdict(float)
        ask_vol = defaultdict(float)
        
        # Processa trade Binance
        for t in trades_binance:
            price = round_price(float(t['p']), step)
            qty = float(t['q'])
            is_sell = t['m']

            if min_price <= price <= max_price:
                if is_sell:
                    bid_vol[price] += qty
                else:
                    ask_vol[price] += qty

        # Processa trade Bybit
        for t in trades_bybit:
            price = round_price(float(t['p']), step)
            qty = float(t['q'])
            is_sell = t['m']

            if min_price <= price <= max_price:
                if is_sell:
                    bid_vol[price] += qty
                else:
                    ask_vol[price] += qty

        # Crea livelli prezzo
        all_levels = sorted(set(list(bid_vol.keys()) + list(ask_vol.keys())), reverse=True)
        if not all_levels:
            all_levels = [round_price(p, step) for p in range(int(min_price), int(max_price) + int(step), int(step))]
            all_levels.sort(reverse=True)

        levels_data = []
        bar_total_bid = 0
        bar_total_ask = 0
        
        for price_level in all_levels:
            bid = bid_vol.get(price_level, 0)
            ask = ask_vol.get(price_level, 0)
            delta = ask - bid
            total_vol = bid + ask
            
            bar_total_bid += bid
            bar_total_ask += ask
            
            max_vol_in_bar = max(max(bid_vol.values(), default=0), max(ask_vol.values(), default=0))
            is_significant = total_vol > max_vol_in_bar * 0.15 if max_vol_in_bar > 0 else False
            
            levels_data.append({
                "price": price_level,
                "bid": round(bid, 3),
                "ask": round(ask, 3),
                "delta": round(delta, 3),
                "total": round(total_vol, 3),
                "significant": is_significant
            })

        bar_delta = bar_total_ask - bar_total_bid
        total_volume += vol
        total_delta += bar_delta

        bars.append({
            "timestamp": ts,
            "time": datetime.fromtimestamp(ts/1000).strftime("%H:%M"),
            "date": datetime.fromtimestamp(ts/1000).strftime("%Y-%m-%d"),
            "open": round(o, 2),
            "high": round(h, 2),
            "low": round(l, 2),
            "close": round(c, 2),
            "volume": round(vol, 2),
            "levels": levels_data,
            "bullish": c > o,
            "delta": round(bar_delta, 2),
            "total_bid": round(bar_total_bid, 2),
            "total_ask": round(bar_total_ask, 2)
        })

    current_price = bars[-1]["close"] if bars else 0
    stats = {
        "price": round(current_price, 2),
        "volume": round(total_volume, 2),
        "delta": round(total_delta, 2),
        "bars_count": len(bars)
    }

    return {"bars": bars, "stats": stats}

@app.route('/')
def index():
    """Serve pagina HTML ottimizzata con zoom"""
    
    html = """
<!DOCTYPE html>
<html>
<head>
    <title>BTC Footprint Orderflow - Binance + Bybit</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        
        body {
            font-family: 'Segoe UI', Arial, sans-serif;
            background: #0d0d0d;
            color: #e0e0e0;
            overflow: hidden;
        }
        
        .header {
            background: linear-gradient(180deg, #1a1a1a 0%, #0d0d0d 100%);
            padding: 10px 20px;
            border-bottom: 1px solid #333;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        
        .header h1 {
            font-size: 16px;
            font-weight: 600;
            color: #00d4ff;
        }
        
        .controls {
            display: flex;
            gap: 10px;
            align-items: center;
            flex-wrap: wrap;
        }
        
        .controls label {
            font-size: 11px;
            color: #999;
        }
        
        .controls select, .controls button, .controls input {
            background: #1a1a1a;
            color: #e0e0e0;
            border: 1px solid #333;
            padding: 5px 10px;
            border-radius: 3px;
            font-size: 11px;
            cursor: pointer;
        }
        
        .controls select:hover, .controls button:hover {
            border-color: #00d4ff;
        }
        
        .controls button {
            background: #00d4ff;
            color: #000;
            font-weight: 600;
            border: none;
        }
        
        .controls button:hover {
            background: #00b8e6;
        }
        
        .controls input[type="range"] {
            width: 120px;
        }
        
        .stats-bar {
            background: #1a1a1a;
            padding: 6px 20px;
            border-bottom: 1px solid #333;
            display: flex;
            gap: 20px;
            font-size: 11px;
            flex-wrap: wrap;
        }
        
        .stat-item {
            display: flex;
            align-items: center;
            gap: 6px;
        }
        
        .stat-label { color: #888; }
        .stat-value { color: #00d4ff; font-weight: 600; }
        .stat-value.positive { color: #26a69a; }
        .stat-value.negative { color: #ef5350; }
        
        .chart-container {
            height: calc(100vh - 100px);
            overflow: auto;
            padding: 15px;
            background: #0d0d0d;
            position: relative;
        }
        
        .footprint-wrapper {
            display: inline-block;
            min-width: 100%;
            transform-origin: left top;
            transition: transform 0.1s ease-out;
        }
        
        .footprint-table {
            border-collapse: collapse;
            background: #0d0d0d;
        }
        
        .bar-column {
            border-left: 1px solid #1a1a1a;
            border-right: 1px solid #1a1a1a;
            padding: 0;
            vertical-align: top;
            min-width: 70px;
            max-width: 70px;
            position: relative;
        }
        
        /* Linea verticale open/close */
        .bar-column::before {
            content: '';
            position: absolute;
            left: 2px;
            top: 0;
            bottom: 0;
            width: 2px;
            z-index: 5;
        }
        
        .bar-column.bullish::before {
            background: rgba(38, 166, 154, 0.6);
        }
        
        .bar-column.bearish::before {
            background: rgba(239, 83, 80, 0.6);
        }
        
        .bar-column::after {
            content: '';
            position: absolute;
            right: 2px;
            top: 0;
            bottom: 0;
            width: 2px;
            z-index: 5;
        }
        
        .bar-column.bullish::after {
            background: rgba(38, 166, 154, 0.6);
        }
        
        .bar-column.bearish::after {
            background: rgba(239, 83, 80, 0.6);
        }
        
        .time-header {
            background: #1a1a1a;
            padding: 6px;
            text-align: center;
            border-bottom: 2px solid #333;
            position: sticky;
            top: 0;
            z-index: 10;
        }
        
        .time-text {
            font-size: 10px;
            color: #00d4ff;
            font-weight: 600;
            margin-bottom: 3px;
        }
        
        .ohlc-text {
            font-size: 8px;
            color: #666;
            line-height: 1.2;
        }
        
        .price-row { min-height: 22px; }
        
        .price-cell {
            background: #0d0d0d;
            padding: 1px;
            min-height: 22px;
            position: relative;
        }
        
        .price-cell-content {
            display: flex;
            justify-content: space-between;
            align-items: center;
            gap: 1px;
            height: 100%;
        }
        
        .bid-value, .ask-value {
            flex: 1;
            text-align: center;
            font-size: 9px;
            padding: 2px 1px;
            border-radius: 2px;
            font-weight: 500;
        }
        
        .bid-value {
            background: rgba(239, 83, 80, 0.15);
            color: #ef5350;
        }
        
        .ask-value {
            background: rgba(38, 166, 154, 0.15);
            color: #26a69a;
        }
        
        .bid-value.significant {
            background: rgba(239, 83, 80, 0.45);
            font-weight: 700;
            box-shadow: 0 0 6px rgba(239, 83, 80, 0.6);
        }
        
        .ask-value.significant {
            background: rgba(38, 166, 154, 0.45);
            font-weight: 700;
            box-shadow: 0 0 6px rgba(38, 166, 154, 0.6);
        }
        
        .price-label {
            position: absolute;
            left: 50%;
            top: 50%;
            transform: translate(-50%, -50%);
            font-size: 7px;
            color: #444;
            pointer-events: none;
            z-index: 1;
        }
        
        .delta-footer {
            background: #1a1a1a;
            padding: 5px;
            text-align: center;
            border-top: 2px solid #333;
            position: sticky;
            bottom: 0;
        }
        
        .delta-value {
            font-size: 10px;
            font-weight: 700;
        }
        
        .delta-value.positive { color: #26a69a; }
        .delta-value.negative { color: #ef5350; }
        
        .empty-cell {
            background: #0d0d0d;
            min-height: 22px;
        }
        
        .loading {
            position: fixed;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%);
            background: rgba(0, 0, 0, 0.8);
            padding: 20px 40px;
            border-radius: 8px;
            z-index: 1000;
            display: none;
        }
        
        .loading.active { display: block; }
        
        ::-webkit-scrollbar { width: 6px; height: 6px; }
        ::-webkit-scrollbar-track { background: #0d0d0d; }
        ::-webkit-scrollbar-thumb { background: #333; border-radius: 3px; }
        ::-webkit-scrollbar-thumb:hover { background: #555; }
    </style>
</head>
<body>
    <div class="header">
        <h1>🔥 BTC Footprint - Binance + Bybit</h1>
        <div class="controls">
            <label>Intervallo:</label>
            <select id="interval" onchange="loadData()">
                <option value="1m" selected>1m</option>
                <option value="5m">5m</option>
                <option value="15m">15m</option>
                <option value="30m">30m</option>
                <option value="1h">1h</option>
            </select>
            
            <label>Step:</label>
            <select id="step" onchange="loadData()">
                <option value="1">1$</option>
                <option value="5">5$</option>
                <option value="10" selected>10$</option>
                <option value="25">25$</option>
                <option value="50">50$</option>
            </select>
            
            <label>Barre:</label>
            <input type="number" id="barCount" value="20" min="5" max="50" onchange="renderCurrentData()" style="width:50px;">
            
            <label>Zoom:</label>
            <input type="range" id="zoom" min="50" max="200" value="100" oninput="applyZoom(this.value)">
            <span id="zoomValue">100%</span>
            
            <button onclick="loadData()">↻</button>
            <button onclick="toggleAutoRefresh()">⏱</button>
        </div>
    </div>
    
    <div class="stats-bar" id="stats-bar"></div>
    
    <div class="loading" id="loading">Caricamento...</div>
    
    <div class="chart-container" id="chart-container">
        <div style="text-align: center; padding: 50px; color: #666;">
            Caricamento dati...
        </div>
    </div>

    <script>
        let autoRefreshInterval = null;
        let currentData = null;
        let currentZoom = 100;
        
        function showLoading() {
            document.getElementById('loading').classList.add('active');
        }
        
        function hideLoading() {
            document.getElementById('loading').classList.remove('active');
        }
        
        function loadData() {
            showLoading();
            const interval = document.getElementById('interval').value;
            const step = document.getElementById('step').value;
            
            fetch(`/api/data?interval=${interval}&step=${step}`)
                .then(response => response.json())
                .then(data => {
                    currentData = data;
                    renderStatsBar(data.stats);
                    renderCurrentData();
                    hideLoading();
                })
                .catch(error => {
                    console.error('Errore:', error);
                    document.getElementById('chart-container').innerHTML = 
                        '<div style="text-align:center;padding:50px;color:#ef5350;">Errore caricamento</div>';
                    hideLoading();
                });
        }
        
        function renderStatsBar(stats) {
            const deltaClass = stats.delta >= 0 ? 'positive' : 'negative';
            const deltaSign = stats.delta >= 0 ? '+' : '';
            
            document.getElementById('stats-bar').innerHTML = `
                <div class="stat-item">
                    <span class="stat-label">Prezzo:</span>
                    <span class="stat-value">$${stats.price.toLocaleString()}</span>
                </div>
                <div class="stat-item">
                    <span class="stat-label">Volume:</span>
                    <span class="stat-value">${stats.volume.toFixed(2)} BTC</span>
                </div>
                <div class="stat-item">
                    <span class="stat-label">Delta:</span>
                    <span class="stat-value ${deltaClass}">${deltaSign}${stats.delta.toFixed(2)}</span>
                </div>
                <div class="stat-item">
                    <span class="stat-label">Barre:</span>
                    <span class="stat-value">${stats.bars_count}</span>
                </div>
                <div class="stat-item">
                    <span class="stat-label">Fonte:</span>
                    <span class="stat-value">Binance+Bybit</span>
                </div>
            `;
        }
        
        function renderCurrentData() {
            if (!currentData) return;
            
            const barCount = parseInt(document.getElementById('barCount').value);
            const displayBars = currentData.bars.slice(-barCount);
            
            if (displayBars.length === 0) {
                document.getElementById('chart-container').innerHTML = 
                    '<div style="text-align:center;padding:50px;color:#666;">Nessun dato</div>';
                return;
            }
            
            // Raccogli prezzi
            let allPrices = new Set();
            displayBars.forEach(bar => {
                bar.levels.forEach(level => allPrices.add(level.price));
            });
            const sortedPrices = Array.from(allPrices).sort((a, b) => b - a);
            
            let html = '<div class="footprint-wrapper" id="footprint-wrapper"><table class="footprint-table">';
            
            // Header
            html += '<tr>';
            displayBars.forEach(bar => {
                const barClass = bar.bullish ? 'bullish' : 'bearish';
                html += `
                    <td class="bar-column time-header ${barClass}">
                        <div class="time-text">${bar.time}</div>
                        <div class="ohlc-text">
                            O:${bar.open}<br>
                            H:${bar.high}<br>
                            L:${bar.low}<br>
                            C:${bar.close}
                        </div>
                    </td>
                `;
            });
            html += '</tr>';
            
            // Righe prezzi
            sortedPrices.forEach(price => {
                html += '<tr class="price-row">';
                
                displayBars.forEach(bar => {
                    const level = bar.levels.find(l => l.price === price);
                    const barClass = bar.bullish ? 'bullish' : 'bearish';
                    
                    if (level && (level.bid > 0 || level.ask > 0)) {
                        const bidClass = level.significant && level.bid > 0 ? 'significant' : '';
                        const askClass = level.significant && level.ask > 0 ? 'significant' : '';
                        
                        html += `
                            <td class="bar-column price-cell ${barClass}">
                                <div class="price-cell-content">
                                    <div class="bid-value ${bidClass}">
                                        ${level.bid > 0 ? level.bid.toFixed(1) : ''}
                                    </div>
                                    <div class="ask-value ${askClass}">
                                        ${level.ask > 0 ? level.ask.toFixed(1) : ''}
                                    </div>
                                </div>
                                <div class="price-label">${price}</div>
                            </td>
                        `;
                    } else {
                        html += `
                            <td class="bar-column empty-cell ${barClass}">
                                <div class="price-label">${price}</div>
                            </td>
                        `;
                    }
                });
                
                html += '</tr>';
            });
            
            // Footer delta
            html += '<tr>';
            displayBars.forEach(bar => {
                const deltaClass = bar.delta >= 0 ? 'positive' : 'negative';
                const deltaSign = bar.delta >= 0 ? '+' : '';
                const barClass = bar.bullish ? 'bullish' : 'bearish';
                html += `
                    <td class="bar-column delta-footer ${barClass}">
                        <div class="delta-value ${deltaClass}">
                            ${deltaSign}${bar.delta.toFixed(1)}
                        </div>
                    </td>
                `;
            });
            html += '</tr>';
            
            html += '</table></div>';
            
            document.getElementById('chart-container').innerHTML = html;
            applyZoom(currentZoom);
        }
        
        function applyZoom(value) {
            currentZoom = value;
            document.getElementById('zoomValue').textContent = value + '%';
            const wrapper = document.getElementById('footprint-wrapper');
            if (wrapper) {
                wrapper.style.transform = `scale(${value / 100})`;
            }
        }
        
        function toggleAutoRefresh() {
            if (autoRefreshInterval) {
                clearInterval(autoRefreshInterval);
                autoRefreshInterval = null;
                alert('Auto-refresh OFF');
            } else {
                autoRefreshInterval = setInterval(loadData, 30000);
                alert('Auto-refresh ON (30s)');
            }
        }
        
        // Zoom con mouse wheel
        document.addEventListener('wheel', function(e) {
            if (e.ctrlKey) {
                e.preventDefault();
                const delta = e.deltaY > 0 ? -5 : 5;
                let newZoom = currentZoom + delta;
                newZoom = Math.max(50, Math.min(200, newZoom));
                document.getElementById('zoom').value = newZoom;
                applyZoom(newZoom);
            }
        }, { passive: false });
        
        // Carica dati iniziali
        loadData();
    </script>
</body>
</html>
    """
    return html

@app.route('/api/data')
def get_data():
    """API endpoint ottimizzato con cache"""
    interval = request.args.get('interval', '1m')
    step = float(request.args.get('step', 10))
    
    cache_key = f"{interval}_{step}"
    current_time = time.time()
    
    with CACHE['lock']:
        # Cache check
        if (cache_key in CACHE['data'] and 
            current_time - CACHE['timestamp'] < CACHE_TTL):
            return jsonify(CACHE['data'][cache_key])
        
        # Process new data
        data = process_data(interval, step)
        
        # Update cache
        CACHE['data'][cache_key] = data
        CACHE['timestamp'] = current_time
    
    return jsonify(data)

if __name__ == '__main__':
    print("=" * 60)
    print("BTC FOOTPRINT ORDERFLOW - OTTIMIZZATO")
    print("=" * 60)
    print("✨ Zoom: CTRL + Mouse Wheel o slider")
    print("✨ Linee verticali: Verde (bullish) / Rossa (bearish)")
    print("✨ Performance: Cache ottimizzata")
    print("=" * 60)
    print("Server: http://localhost:5001")
    print("CTRL+C per fermare")
    print("=" * 60)
    app.run(debug=True, host='0.0.0.0', port=5001, threaded=True)
