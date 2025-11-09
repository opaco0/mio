#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BTC FOOTPRINT ORDERFLOW - v5 (Gemini Mod)
✅ Modifica 1: LAB analizza 100% OB (Full Book)
✅ Modifica 2: Strategia usa Intensità + Conferma (non più 100% fisso)
✅ Modifica 3: Aggiunto pulsante Trend e fix timer
✅ Modifica 4: Aggiunte Entry/Target/Stop Zone alla strategia
✅ Modifica 5: Aggiunto selettore conteggio snapshot per LAB
✅ Modifica 6: Visualizzazione Entry/Target/Stop sul grafico
"""

from flask import Flask, jsonify, request
import time
import requests
import ccxt
import logging
from collections import defaultdict
from datetime import datetime
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# ORDERBOOK CACHE - Per velocizzare broadcast (10 FPS)
# ═══════════════════════════════════════════════════════════════════════════════

orderbook_cache = {'bids': [], 'asks': [], 'last_update': 0}
orderbook_cache_lock = threading.Lock()


# ═══════════════════════════════════════════════════════════════════════════════
# CACHE PER ULTIMA CANDELA - Evita refetch continuo dei trade
# ═══════════════════════════════════════════════════════════════════════════════
last_candle_cache = {
    'timestamp': 0,
    'trades': [],
    'last_fetch': 0,
    'lock': threading.Lock()
}

def get_current_candle_timestamp(interval_ms):
    """Ritorna il timestamp di inizio della candela corrente"""
    now_ms = int(time.time() * 1000)
    return (now_ms // interval_ms) * interval_ms

def fetch_last_candle_trades_cached(interval, interval_ms):
    """
    Fetch intelligente dei trade dell'ultima candela con cache.
    Aggiorna solo i trade nuovi invece di scaricare tutto ogni volta.
    """
    current_candle_ts = get_current_candle_timestamp(interval_ms)

    with last_candle_cache['lock']:
        now = time.time()
        if (last_candle_cache['timestamp'] == current_candle_ts and 
            now - last_candle_cache['last_fetch'] < 0.5):
            # logger.debug(f"📦 Cache HIT - usando {len(last_candle_cache['trades'])} trade in cache")
            return list(last_candle_cache['trades'])

        if last_candle_cache['timestamp'] != current_candle_ts:
            logger.info(f"🔄 Nuova candela {current_candle_ts} - reset cache")
            last_candle_cache['timestamp'] = current_candle_ts
            last_candle_cache['trades'] = []

        try:
            end_ts = current_candle_ts + interval_ms - 1
            # logger.debug(f"🔍 Fetching trades: {current_candle_ts} -> {end_ts}")
            new_trades = fetch_trades_multi(current_candle_ts, end_ts)

            if new_trades:
                existing_ts = {t.get('T', 0) for t in last_candle_cache['trades']}
                unique_new = [t for t in new_trades if t.get('T', 0) not in existing_ts]

                if unique_new:
                    last_candle_cache['trades'].extend(unique_new)
                    # logger.debug(f"✅ Aggiunti {len(unique_new)} nuovi trade (totale: {len(last_candle_cache['trades'])})")
                # else:
                    # logger.debug(f"📦 Nessun trade nuovo (già {len(last_candle_cache['trades'])} in cache)")

            last_candle_cache['last_fetch'] = now
            return list(last_candle_cache['trades'])

        except Exception as e:
            logger.error(f"❌ Error fetching cached trades: {e}")
            return list(last_candle_cache['trades'])


app = Flask(__name__)

SYMBOL_BINANCE = "BTCUSDT"

# ✅ Multi-Exchange Config
EXCHANGES_CONFIG = {
    'binance': {'ccxt_name': 'binance', 'symbol': 'BTC/USDT'},
    'okx': {'ccxt_name': 'okx', 'symbol': 'BTC/USDT'},
    'coinbase': {'ccxt_name': 'coinbase', 'symbol': 'BTC/USD'},
    'bybit': {'ccxt_name': 'bybit', 'symbol': 'BTC/USDT'},
    'kucoin': {'ccxt_name': 'kucoin', 'symbol': 'BTC/USDT'},
    'bitget': {'ccxt_name': 'bitget', 'symbol': 'BTC/USDT'},
}

EXCHANGES_MULTI = {}

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURAZIONE ORDERBOOK RANGE FILTER
# ═══════════════════════════════════════════════════════════════════════════════
ORDERBOOK_PRICE_RANGE_PERCENT = 0.1  # Default: 0.1% dal prezzo corrente
# Opzioni disponibili: 0.01, 0.042, 0.1, 0.42
# Questo parametro filtra gli ordini dell'orderbook per calcolare il delta pesato
# solo su ordini entro X% dal mid price


def init_multi_exchanges():
    global EXCHANGES_MULTI
    logger.info("\n" + "="*100)
    logger.info("🔧 INIZIALIZZAZIONE EXCHANGES MULTI")
    logger.info("="*100)

    for name, config in EXCHANGES_CONFIG.items():
        try:
            exchange_class = getattr(ccxt, config['ccxt_name'])
            EXCHANGES_MULTI[name] = exchange_class({'enableRateLimit': True, 'rateLimit': 500, 'timeout': 15000})
            logger.info(f"✅ {name.upper():12} → {config['symbol']}")
        except Exception as e:
            logger.error(f"❌ {name.upper():12} → {str(e)[:50]}")

    logger.info(f"📊 Total: {len(EXCHANGES_MULTI)}/6 exchange")
    logger.info("="*100 + "\n")

init_multi_exchanges()

CACHE = {'data': {}, 'orderbook': {}, 'delta_ob_snapshots': [], 'lock': threading.Lock()}
# Variabili filtro rimosse
# TRADE_FILTER_MODE = "percentile"
# TRADE_MIN_QTY_PERCENT = 0.5
# TRADE_PERCENTILE = 75
# TRADE_TOP_N = 300

def get_interval_ms(interval):
    intervals = {"1m": 60000, "5m": 300000, "15m": 900000, "30m": 1800000, "1h": 3600000, "1d": 8640000}
    return intervals.get(interval, 60000)

def fetch_with_retry(url, params, max_retries=3, timeout=15):
    for attempt in range(max_retries):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except:
            if attempt < max_retries - 1:
                time.sleep(1)
    return None

def fetch_orderbook_multi():
    """✅ Orderbook aggregato da 6 exchange - CON LOGGING DETTAGLIATO."""
    # logger.info("\n" + "="*100)
    # logger.info("📊 FETCH ORDERBOOK MULTI - AGGREGAZIONE ORDINI")
    # logger.info("="*100)

    def fetch_ex(name):
        try:
            ex = EXCHANGES_MULTI.get(name)
            if not ex:
                return name, None
            config = EXCHANGES_CONFIG[name]
            if name == 'kucoin':
                ob = ex.fetch_order_book(config['symbol'])
            else:
                # ✅ MODIFICA: Aumentato limite ordini (ATTENZIONE: Rischio API Ban)
                ob = ex.fetch_order_book(config['symbol'], limit=1000)
            bids_c = len(ob.get('bids', []))
            asks_c = len(ob.get('asks', []))
            return name, ob
        except:
            return name, None

    # logger.info("\n📥 FETCH DA OGNI EXCHANGE:")
    # logger.info("-"*100)

    ob_dict = {}
    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = [executor.submit(fetch_ex, name) for name in EXCHANGES_MULTI.keys()]
        for future in as_completed(futures):
            name, ob = future.result()
            if ob:
                bids_c = len(ob.get('bids', []))
                asks_c = len(ob.get('asks', []))
                # logger.info(f"  {name.upper():12} ✅ BIDs: {bids_c:5} | ASKs: {asks_c:5}")
                ob_dict[name] = ob
            else:
                logger.warning(f"  {name.upper():12} ❌ Failed")

    if not ob_dict:
        logger.warning("⚠️  FALLBACK: Usa Binance REST")
        return fetch_orderbook()

    # ✅ AGGREGAZIONE
    # logger.info("\n🔄 AGGREGAZIONE ORDINI PER PREZZO:")
    # logger.info("-"*100)

    agg_bids = {}
    agg_asks = {}
    bid_details = defaultdict(list)
    ask_details = defaultdict(list)

    for ex_name, ob in ob_dict.items():
        for bid in ob.get('bids', []):
            p, q = float(bid[0]), float(bid[1])
            agg_bids[p] = agg_bids.get(p, 0) + q
            bid_details[p].append((ex_name, q))

        for ask in ob.get('asks', []):
            p, q = float(ask[0]), float(ask[1])
            agg_asks[p] = agg_asks.get(p, 0) + q
            ask_details[p].append((ex_name, q))

    bids = sorted([(p, q) for p, q in agg_bids.items()], reverse=True)
    asks = sorted([(p, q) for p, q in agg_asks.items()])

    # # Top 3
    # logger.info("\n📍 TOP 3 BIDs:")
    # for i, (price, qty) in enumerate(bids[:3]):
    #     sources = bid_details[price]
    #     logger.info(f"  {i+1}. ${price:>10,.0f} → {qty:>12,.2f} BTC")

    # logger.info("\n📍 TOP 3 ASKs:")
    # for i, (price, qty) in enumerate(asks[:3]):
    #     logger.info(f"  {i+1}. ${price:>10,.0f} → {qty:>12,.2f} BTC")

    # logger.info("\n📊 TOTALI:")
    # total_bid_qty = sum(q for p, q in bids)
    # total_ask_qty = sum(q for p, q in asks)
    # logger.info(f"  BIDs: {total_bid_qty:>15,.2f} BTC | ASKs: {total_ask_qty:>15,.2f} BTC")
    # logger.info(f"  Livelli: {len(bids):>5} bids | {len(asks):>5} asks")
    # logger.info("="*100 + "\n")

    return {"bids": bids, "asks": asks}

def fetch_trades_multi(start_ms, end_ms):
    """
    Fetches and aggregates trades from multiple exchanges.
    Returns normalized format compatible with Binance aggTrades.
    Includes robust error handling and per-exchange fallback.
    """
    all_trades = []
    successful_exchanges = []
    failed_exchanges = []

    # ✅ FIX (Coinbase): Aggiunti start_ts, end_ts
    def fetch_single_exchange(ex_name, ex_config, start_ts, end_ts): 
        """Fetch from single exchange with error handling"""
        try:
            if ex_name not in EXCHANGES_MULTI or EXCHANGES_MULTI[ex_name] is None:
                return []

            exchange = EXCHANGES_MULTI[ex_name]
            symbol = ex_config['symbol']
            
            # ✅ FIX (Coinbase): Logica specifica per Coinbase
            params = {}
            if ex_name == 'coinbase':
                # 'until' in CCXT per Coinbase è l'end time (timestamp in ms)
                params = {'until': end_ts} 
                
            trades = exchange.fetch_trades(symbol, since=start_ts, limit=1000, params=params) # ✅ FIX: Usa start_ts e params

            if not trades:
                return []

            # Normalize to Binance format
            normalized = []
            for t in trades:
                try:
                    normalized.append({
                        'p': str(t['price']),
                        'q': str(t['amount']),
                        'T': int(t['timestamp']),
                        'm': not t.get('side') == 'buy',
                        'exchange': ex_name
                    })
                except:
                    continue

            # logger.info(f"[{ex_name}] ✓ {len(normalized)} trades")
            return normalized
        except Exception as e:
            logger.error(f"[{ex_name}] ✗ Error: {e}")
            return []

    # Parallel fetch
    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = {
             # ✅ FIX (Coinbase): Passa start_ms e end_ms
            executor.submit(fetch_single_exchange, name, config, start_ms, end_ms): name
            for name, config in EXCHANGES_CONFIG.items()
        }

        for future in as_completed(futures):
            ex_name = futures[future]
            try:
                trades = future.result(timeout=10)
                if trades:
                    all_trades.extend(trades)
                    successful_exchanges.append(ex_name)
                else:
                    failed_exchanges.append(ex_name)
            except Exception as e:
                logger.error(f"[{ex_name}] Timeout/Error: {e}")
                failed_exchanges.append(ex_name)

    # logger.info(f"Multi-exchange: {len(successful_exchanges)} OK, {len(failed_exchanges)} failed")
    # if successful_exchanges:
    #     logger.info(f"  Success: {successful_exchanges}")
    # if failed_exchanges:
    #     logger.warning(f"  Failed: {failed_exchanges}")
    # logger.info(f"  Total trades: {len(all_trades)}")

    if all_trades:
        all_trades.sort(key=lambda x: x['T'])

    return all_trades

def capture_delta_ob_snapshot():
    """Cattura snapshot - usa fetch_orderbook_multi() con filtro range"""
    global ORDERBOOK_PRICE_RANGE_PERCENT

    try:
        ob = fetch_orderbook_multi()
        if not ob or 'bids' not in ob or 'asks' not in ob:
            return None

        # ✅ FIX 3.3: Applica filtro DOPO aver preso i dati, non prima
        all_bids = ob['bids']
        all_asks = ob['asks']

        if not all_bids or not all_asks:
            return None

        mid_price = (float(all_bids[0][0]) + float(all_asks[0][0])) / 2

        # ═══ FILTRO RIMOSSO (RICHIESTA UTENTE) ═══
        # I calcoli ora usano 'all_bids' e 'all_asks' (l'order book completo)
        bids = all_bids
        asks = all_asks

        # Calcola volumi (su TUTTO l'order book)
        total_bid_vol = sum(float(b[1]) for b in bids)
        total_ask_vol = sum(float(a[1]) for a in asks)
        total_delta = total_bid_vol - total_ask_vol

        # Delta pesato (solo ordini filtrati)
        # ✅ FIX 3.1: Logica di calcolo unificata (esponenziale)
        weighted_bid = 0.0
        weighted_ask = 0.0
        PRICE_WEIGHT_FACTOR = 0.015 # Come nel frontend

        for bid_price, bid_qty in bids:
            bid_price = float(bid_price)
            bid_qty = float(bid_qty)
            distance = abs(bid_price - mid_price) / mid_price
            weight = (1.0 / (1.0 + abs(distance) * 10)) # Manteniamo logica originale del LAB
            weighted_bid += bid_qty * weight

        for ask_price, ask_qty in asks:
            ask_price = float(ask_price)
            ask_qty = float(ask_qty)
            distance = abs(ask_price - mid_price) / mid_price
            weight = (1.0 / (1.0 + abs(distance) * 10)) # Manteniamo logica originale del LAB
            weighted_ask += ask_qty * weight

        weighted_delta = weighted_bid - weighted_ask

        snapshot = {
            'timestamp': int(time.time() * 1000),
            'time': datetime.now().strftime("%H:%M:%S"),
            'price': round(mid_price, 2),
            'total_delta': round(total_delta, 4),
            'weighted_delta': round(weighted_delta, 4),
            'total_bid': round(total_bid_vol, 4),
            'total_ask': round(total_ask_vol, 4),
            'weighted_bid': round(weighted_bid, 4),
            'weighted_ask': round(weighted_ask, 4),
            'price_range_percent': "ALL", # <-- MODIFICATO: Indica che è il FULL BOOK
            'filtered_bids_count': len(bids), # Ora questo è il conteggio totale
            'filtered_asks_count': len(asks)  # Ora questo è il conteggio totale
        }

        with CACHE['lock']:
            snapshots = CACHE.get('delta_ob_snapshots', [])
            snapshots.append(snapshot)
            if len(snapshots) > 500:
                snapshots = snapshots[-500:]
            CACHE['delta_ob_snapshots'] = snapshots

        # logger.info(f"📸 SNAPSHOT: ${snapshot['price']:.0f} "
        #            f"delta={snapshot['total_delta']:.2f} "
        #            f"weighted={snapshot['weighted_delta']:.2f} "
        #            f"[range={ORDERBOOK_PRICE_RANGE_PERCENT}%, bids={len(bids)}, asks={len(asks)}]")

        return snapshot

    except Exception as e:
        logger.error(f"Error capture_delta_ob_snapshot: {e}")
        import traceback
        traceback.print_exc()
        return None

def add_delta_ob_snapshot():
    """✅ Aggiunge snapshot alla timeline."""
    snapshot = capture_delta_ob_snapshot()
    if snapshot:
        with CACHE['lock']:
            CACHE['delta_ob_snapshots'].append(snapshot)
            if len(CACHE['delta_ob_snapshots']) > 500:
                CACHE['delta_ob_snapshots'] = CACHE['delta_ob_snapshots'][-500:]

def start_delta_ob_capture():
    """✅ Avvia thread per catturare snapshot ogni minuto."""
    logger.info("\n" + "="*100)
    logger.info("🔬 LAB: Delta OB Timeline AVVIATO")
    logger.info("   Catturando snapshot ogni 60 secondi (max 500 snapshot)")
    logger.info("="*100 + "\n")

    def capture_loop():
        while True:
            add_delta_ob_snapshot()
            time.sleep(5)  # ✅ 5s invece di 60s

    thread = threading.Thread(target=capture_loop, daemon=True)
    thread.start()

# ✅ Avvia all'inizio
start_delta_ob_capture()

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
    # ✅ MODIFICA: Aumentato limite ordini (ATTENZIONE: Rischio API Ban)
    params = {"symbol": SYMBOL_BINANCE, "limit": 1000}
    result = fetch_with_retry(url, params, max_retries=2, timeout=10)
    return result if result else {"bids": [], "asks": []}

def round_price(price, step):
    # ✅ FIX 3.2: Usa math.floor per un arrotondamento deterministico
    # (Richiede `import math` all'inizio del file, ma per semplicità usiamo l'aritmetica)
    if step == 0: return price
    return (price // step) * step
    # return round(price / step) * step # Vecchia logica

# --- MODIFICA BACKEND ---
# Rimossi filter_mode, filter_percentile, filter_min_qty, filter_top_n dalla firma
def process_data(interval, step, update_last_only=False):
    klines = fetch_klines(interval, limit=150)
    if not klines:
        return {"bars": [], "stats": {"error": "Timeout API"}}

    bars = []
    interval_ms = get_interval_ms(interval)
    total_volume = 0
    total_delta = 0

    # ═══════════════════════════════════════════════════════════════════
    # ✅ 1. RACCOLTA DATI KLINES
    # ═══════════════════════════════════════════════════════════════════
    kline_data_map = {} # Usiamo un dizionario per mappare i TS
    
    for i, k in enumerate(klines):
        ts = int(k[0])
        o, h, l, c = float(k[1]), float(k[2]), float(k[3]), float(k[4])
        vol = float(k[5])

        open_rounded = round_price(o, step)
        close_rounded = round_price(c, step)
        high_rounded = round_price(h, step)
        low_rounded = round_price(l, step)

        is_last_candle = (i == len(klines) - 1)
        
        # Determina se dobbiamo scaricare i trade per questa candela
        should_calc_trades = False
        if update_last_only:
            should_calc_trades = is_last_candle
        else:
            should_calc_trades = True

        kline_data_map[ts] = {
            'k': k, 'ts': ts, 'o': o, 'h': h, 'l': l, 'c': c, 'vol': vol,
            'open_rounded': open_rounded, 'close_rounded': close_rounded,
            'high_rounded': high_rounded, 'low_rounded': low_rounded,
            'should_calc_trades': should_calc_trades,
            'is_last_candle': is_last_candle
        }

    # ═══════════════════════════════════════════════════════════════════
    # ✅ 2. FETCH PARALLELO DEI TRADES (LA MODIFICA CHIAVE)
    # ═══════════════════════════════════════════════════════════════════
    trades_map = {} # ts -> [lista di trades]

    # Usiamo un ThreadPoolExecutor per parallelizzare i download
    with ThreadPoolExecutor(max_workers=10) as executor: # 10 worker paralleli
        future_to_ts = {}
        
        for ts, data in kline_data_map.items():
            if not data['should_calc_trades']:
                continue # Non serve scaricare trade per questa candela

            # ═══ STRATEGIA IBRIDA: Multi-exchange SOLO per ultima candela ═══
            if data['is_last_candle']:
                # ULTIMA CANDELA: Usa cache intelligente ✅
                try:
                    trades = fetch_last_candle_trades_cached(interval, interval_ms)
                    if not trades:
                        logger.warning(f"[ULTIMA CANDELA {ts}] Empty cache, FALLBACK to direct fetch")
                        # Usa fetch_trades_multi come fallback se la cache è vuota
                        future = executor.submit(fetch_trades_multi, ts, ts + interval_ms - 1)
                        future_to_ts[future] = ts
                    else:
                        trades_map[ts] = trades # Mappa i trade dalla cache
                except Exception as e:
                    logger.error(f"[ULTIMA CANDELA {ts}] ✗ Cache error: {e}, FALLBACK to Binance REST")
                    # Usa fetch_trades (Binance REST) come fallback in caso di errore
                    future = executor.submit(fetch_trades, ts, ts + interval_ms - 1)
                    future_to_ts[future] = ts
            
            else:
                # CANDELE STORICHE: Solo Binance (veloce)
                future = executor.submit(fetch_trades, ts, ts + interval_ms - 1)
                future_to_ts[future] = ts
        
        # Raccogli i risultati
        for future in as_completed(future_to_ts):
            ts = future_to_ts[future]
            try:
                trades_map[ts] = future.result()
            except Exception as e:
                logger.error(f"Errore fetch trade per {ts}: {e}")
                trades_map[ts] = []
    
    # ═══════════════════════════════════════════════════════════════════
    # ✅ 3. PROCESSO DATI (ORA È VELOCE, I DATI SONO GIÀ SCARICATI)
    # ═══════════════════════════════════════════════════════════════════
    
    # Iteriamo sui klines nell'ordine originale
    for k in klines:
        ts = int(k[0])
        data = kline_data_map.get(ts) # Recupera i dati preparati
        if not data: continue # Sicurezza

        # Estrai le variabili per leggibilità
        o = data['o']; h = data['h']; l = data['l']; c = data['c']; vol = data['vol']
        open_rounded = data['open_rounded']; close_rounded = data['close_rounded']
        high_rounded = data['high_rounded']; low_rounded = data['low_rounded']

        bid_vol = defaultdict(float)
        ask_vol = defaultdict(float)
        
        # Prendi i trades dalla mappa (se esistono)
        trades = trades_map.get(ts) # Sarà None se should_calc_trades era False
                
        if trades:
            # Processa i trade
            for t in trades:
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
        
        # Assicura che il corpo sia sempre disegnato
        current_p_loop = min_body
        while current_p_loop <= max_body:
             active_prices.add(current_p_loop)
             if step == 0: break # Prevenzione loop infinito
             current_p_loop += step
        
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

        bar_delta =  bar_total_bid - bar_total_ask
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

# ========================================
# LAB: Delta OB Snapshot Functions
# ========================================

def capture_delta_ob_snapshot_REST():
    """⚠️ FALLBACK REST - Non usare."""
    try:
        ob = fetch_orderbook()
        if not ob or 'bids' not in ob or 'asks' not in ob:
            return None

        bids = ob['bids'][:100]
        asks = ob['asks'][:100]

        # Prezzo corrente (mid price)
        mid_price = (float(bids[0][0]) + float(asks[0][0])) / 2 if bids and asks else 0

        # Delta totale
        total_bid_vol = sum(float(b[1]) for b in bids)
        total_ask_vol = sum(float(a[1]) for a in asks)
        total_delta = total_bid_vol - total_ask_vol

        # Delta pesato (peso esponenziale per distanza)
        weighted_bid = 0.0
        weighted_ask = 0.0

        for bid_price, bid_qty in bids:
            bid_price = float(bid_price)
            bid_qty = float(bid_qty)
            distance = mid_price - bid_price
            weight = 1.0 / (1.0 + abs(distance) * 10)
            weighted_bid += bid_qty * weight

        for ask_price, ask_qty in asks:
            ask_price = float(ask_price)
            ask_qty = float(ask_qty)
            distance = ask_price - mid_price
            weight = 1.0 / (1.0 + abs(distance) * 10)
            weighted_ask += ask_qty * weight

        weighted_delta = weighted_bid - weighted_ask

        snapshot = {
            'timestamp': int(time.time() * 1000),
            'time': datetime.now().strftime("%H:%M:%S"),
            'price': round(mid_price, 2),
            'total_delta': round(total_delta, 4),
            'weighted_delta': round(weighted_delta, 4),
            'total_bid': round(total_bid_vol, 4),
            'total_ask': round(total_ask_vol, 4)
        }

        return snapshot
    except Exception as e:
        print(f"⚠️ Errore capture_delta_ob_snapshot: {e}")
        return None

# def add_delta_ob_snapshot(): # Funzione già definita sopra
#     """Aggiunge uno snapshot alla lista, mantenendo max 500 elementi."""
#     snapshot = capture_delta_ob_snapshot()
#     if snapshot:
#         with CACHE['lock']:
#             CACHE['delta_ob_snapshots'].append(snapshot)
#             # Mantieni solo ultimi 500 snapshot (circa 8 ore a 1 min)
#             if len(CACHE['delta_ob_snapshots']) > 500:
#                 CACHE['delta_ob_snapshots'] = CACHE['delta_ob_snapshots'][-500:]

def start_delta_ob_capture_OLD():
    """⚠️ OLD - Non usare."""
    pass

def start_delta_ob_capture_DISABLED():
    """⚠️ Disabilitato - Usa quello nel nuovo file."""
    pass

def start_delta_ob_capture_OLD_impl():
    """Avvia thread per catturare snapshot ogni minuto."""
    def capture_loop():
        while True:
            add_delta_ob_snapshot()
            time.sleep(5)  # ✅ 5s invece di 60s  # Ogni minuto

    thread = threading.Thread(target=capture_loop, daemon=True)
    thread.start()
    print("🔬 LAB: Delta OB capture avviato (1 snapshot/min)")


# ═════════════════════════════════════════════════════════════════════════════════
# WEBSOCKET HYBRID SYSTEM
# ═════════════════════════════════════════════════════════════════════════════════

try:
    from flask_socketio import SocketIO, emit
    WEBSOCKET_AVAILABLE = True
    logger.info("✅ WebSocket support ENABLED")
except ImportError:
    WEBSOCKET_AVAILABLE = False
    logger.info("⚠️  WebSocket support DISABLED")

# ✅ FIX 1.1: Variabili globali per stato WS e lock
current_ws_interval = '1m'
current_ws_step = 10.0
settings_lock = threading.Lock()


if WEBSOCKET_AVAILABLE:
    socketio = SocketIO(app, cors_allowed_origins="*", ping_timeout=60, ping_interval=15)  # ✅ 15s invece di 25s

    @socketio.on('connect')
    def handle_connect():
        logger.info(f"🔌 WebSocket connected")
        emit('connection_response', {'status': 'connected'})

    @socketio.on('disconnect')
    def handle_disconnect():
        logger.info(f"❌ WebSocket disconnected")

    @socketio.on('ping')
    def handle_ping():
        emit('pong', {'status': 'alive'})

    @socketio.on('subscribe')
    def handle_subscribe(data):
        # Questa non è usata, ma la lasciamo per compatibilità
        emit('subscribe_response', {'status': 'subscribed'})

    # ✅ FIX 1.1: Nuovo listener per aggiornamenti impostazioni
    @socketio.on('settings_update')
    def handle_settings_update(data):
        global current_ws_interval, current_ws_step
        try:
            new_interval = data.get('interval')
            new_step = data.get('step')
            
            with settings_lock:
                if new_interval:
                    current_ws_interval = new_interval
                # Usa check "is not None" per gestire "0" se mai fosse un'opzione
                if new_step is not None: 
                    current_ws_step = float(new_step)
            
            logger.info(f"🔧 [Socket.IO] Impostazioni WebSocket aggiornate: TF={current_ws_interval}, Step={current_ws_step}")
            # Invia una conferma (opzionale ma utile)
            emit('settings_confirmed', {'interval': current_ws_interval, 'step': current_ws_step})
        
        except Exception as e:
            logger.error(f"Errore handle_settings_update: {e}")


    # ═══════════════════════════════════════════════════════════════════════════
    # ✅ REAL-TIME WEBSOCKET STREAMS (ARCHITETTURA OTTIMIZZATA)
    # ═══════════════════════════════════════════════════════════════════════════
    
    def emit_orderbook_real_time():
        """
        ✅ Stream orderbook + footprint con ARCHITETTURA A 3 THREADS:
        - Thread 1: Fetch orderbook ogni 3s (lento, ma OK)
        - Thread 2: Emit orderbook da cache ogni 100ms (veloce, 10 Hz)
        - Thread 3: Emit footprint ogni 1s (1 Hz) ✅ NUOVO!
        """
        logger.info("🚀 AVVIO STREAM ORDERBOOK + FOOTPRINT (10 Hz + 1 Hz)")

        # ═══════════════════════════════════════════════════════════════════
        # THREAD 1: FETCH ORDERBOOK (ogni 3 secondi)
        # ═══════════════════════════════════════════════════════════════════
        def fetch_loop():
            fetch_counter = 0

            while True:
                try:
                    start_time = time.time()
                    ob = fetch_orderbook_multi()
                    fetch_duration = time.time() - start_time

                    if ob and ob.get("bids") and ob.get("asks"):
                        with orderbook_cache_lock:
                            orderbook_cache["bids"] = ob["bids"][:1000]
                            orderbook_cache["asks"] = ob["asks"][:1000]
                            orderbook_cache["last_update"] = int(time.time() * 1000)

                        if fetch_counter % 10 == 0:
                            logger.info(f"✅ Fetch #{fetch_counter}: {fetch_duration:.2f}s - Cache aggiornata")

                    fetch_counter += 1
                    time.sleep(3) # Fetch OB ogni 3 sec

                except Exception as e:
                    logger.error(f"❌ Error in fetch loop: {e}")
                    time.sleep(3)

        # ═══════════════════════════════════════════════════════════════════
        # THREAD 2: EMIT ORDERBOOK (10 Hz, ogni 100ms)
        # ═══════════════════════════════════════════════════════════════════
        def emit_orderbook_loop():
            update_counter = 0

            while True:
                try:
                    with orderbook_cache_lock:
                        if not orderbook_cache.get("bids") or not orderbook_cache.get("asks"):
                            time.sleep(0.1)
                            continue

                        bids = orderbook_cache["bids"][:1000]
                        asks = orderbook_cache["asks"][:1000]
                        last_update = orderbook_cache["last_update"]

                    if bids and asks:
                        mid_price = (float(bids[0][0]) + float(asks[0][0])) / 2
                    else:
                        mid_price = 0

                    # ✅ Emit con struttura corretta
                    socketio.emit("orderbook_update", {
                        "orderbook": {
                            "bids": bids,
                            "asks": asks,
                            "mid_price": round(mid_price, 2),
                        },
                        "timestamp": last_update,
                        "update_id": update_counter
                    })

                    update_counter += 1

                    # if update_counter % 100 == 0: # Troppo verboso
                    #     logger.info(f"📡 Orderbook emit: {update_counter} updates (10 Hz)")

                    time.sleep(0.1) # 10 Hz

                except Exception as e:
                    logger.error(f"❌ Error in orderbook emit loop: {e}")
                    time.sleep(1)

        # ═══════════════════════════════════════════════════════════════════
        # THREAD 3: EMIT FOOTPRINT (1 Hz, ogni 1 secondo) ✅ MODIFICATO!
        # ═══════════════════════════════════════════════════════════════════
        def emit_footprint_loop():
            update_counter = 0

            while True:
                try:
                    # ✅ FIX 1.1: Leggi le impostazioni globali dinamicamente
                    with settings_lock:
                        interval = current_ws_interval
                        step = current_ws_step
                    
                    # NOTA: La chiamata a process_data ora usa i valori dinamici
                    data = process_data(interval, step, update_last_only=True)

                    if data and data.get("bars") and len(data["bars"]) > 0:
                        # ✅ Emit footprint data separato
                        socketio.emit("footprint_update", {
                            "bars": data["bars"],
                            "stats": data["stats"],
                            "timestamp": int(time.time() * 1000),
                            "update_id": update_counter,
                            "interval": interval, # ✅ FIX 1.3: Invia l'intervallo al client
                            "step": step          # ✅ FIX 1.3: Invia lo step al client
                        })

                        update_counter += 1

                        if update_counter % 10 == 0:
                            logger.info(f"📊 Footprint emit: {update_counter} updates (1 Hz) - {len(data['bars'])} bars")
                    else:
                        logger.warning("⚠️ No footprint data available")

                    time.sleep(1) # 1 Hz

                except Exception as e:
                    logger.error(f"❌ Error in footprint emit loop: {e}")
                    import traceback
                    traceback.print_exc()
                    time.sleep(1)

        # ═══════════════════════════════════════════════════════════════════
        # AVVIO DEI 3 THREAD
        # ═══════════════════════════════════════════════════════════════════
        fetch_thread = threading.Thread(target=fetch_loop, daemon=True)
        emit_ob_thread = threading.Thread(target=emit_orderbook_loop, daemon=True)
        emit_fp_thread = threading.Thread(target=emit_footprint_loop, daemon=True)

        fetch_thread.start()
        emit_ob_thread.start()
        emit_fp_thread.start()

        logger.info("✅ Stream ATTIVO: Orderbook (10 Hz) + Footprint (1 Hz)")

    def emit_heartbeat():
        logger.info("💓 AVVIO HEARTBEAT (ogni 15s)")
        
        def heartbeat_loop():
            counter = 0
            while True:
                try:
                    socketio.emit("heartbeat", {
                        "status": "alive",
                        "timestamp": int(time.time() * 1000)
                    })
                    counter += 1
                    
                    if counter % 20 == 0:
                        logger.info(f"💓 Heartbeat: {counter} beats sent")
                    
                    time.sleep(15)
                except Exception as e:
                    logger.error(f"❌ Error heartbeat: {e}")
                    time.sleep(15)
        
        threading.Thread(target=heartbeat_loop, daemon=True).start()
        logger.info("✅ Heartbeat ATTIVO")
    
    # Avvia gli stream
    emit_orderbook_real_time()
    emit_heartbeat()
    
else:
    socketio = None


@app.route('/api/delta_ob_snapshots')
def get_delta_ob_snapshots():
    """✅ Delta OB Timeline - Real-time Snapshots aggregati."""
    
    # Leggi il parametro 'count' dalla richiesta, default 50
    try:
        count = int(request.args.get('count', 50))
    except ValueError:
        count = 50
    
    # Valida il conteggio (min 10, max 500, che è il massimo salvato)
    count = max(10, min(count, 500)) 

    with CACHE['lock']:
        # Usa il 'count' dinamico per lo slice
        snapshots = list(CACHE['delta_ob_snapshots'][-count:])  
    
    logger.info(f"📈 /api/delta_ob_snapshots: Ritornando {len(snapshots)} snapshot (richiesti: {count})")
    return jsonify({
        'snapshots': snapshots,
        'count': len(snapshots),
        'max_age_minutes': 500  # Max 500 snapshot = ~8 ore a 1 min
    })
@app.route('/')
def index():
    html = r"""
<!DOCTYPE html>
<html>
<head>
<script src="https://cdn.socket.io/4.5.4/socket.io.min.js"></script>
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
        
        .price-cell.in-body { border-right: 2px solid rgba(76, 175, 80, 0.7) !important; border-right: 2px solid rgba(76, 175, 80, 0.7) !important; }
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
          

        <div style="display: grid; grid-template-columns: repeat(4, 1fr); gap: 8px;">
                <button onclick="setOrderbookRange(0.01)" class="range-btn" data-range="0.01" 
                        style="padding: 8px 12px; background: rgba(60,70,90,0.8); border: 1px solid rgba(100,120,150,0.5); 
                               border-radius: 5px; color: #90caf9; font-size: 11px; font-weight: 500; cursor: pointer; 
                               transition: all 0.3s ease; text-align: center;">
                    0.01%
                    <div style="font-size: 9px; color: rgba(150,170,200,0.7); margin-top: 2px;">Ultra</div>
                </button>
                <button onclick="setOrderbookRange(0.042)" class="range-btn" data-range="0.042"
                        style="padding: 8px 12px; background: rgba(60,70,90,0.8); border: 1px solid rgba(100,120,150,0.5); 
                               border-radius: 5px; color: #90caf9; font-size: 11px; font-weight: 500; cursor: pointer; 
                               transition: all 0.3s ease; text-align: center;">
                    0.042%
                    <div style="font-size: 9px; color: rgba(150,170,200,0.7); margin-top: 2px;">Tight</div>
                </button>
                <button onclick="setOrderbookRange(0.1)" class="range-btn active" data-range="0.1"
                        style="padding: 8px 12px; background: linear-gradient(135deg, rgba(70,130,180,0.9), rgba(90,150,200,0.9)); 
                               border: 1px solid rgba(120,170,220,0.8); border-radius: 5px; color: white; font-size: 11px; 
                               font-weight: 600; cursor: pointer; transition: all 0.3s ease; text-align: center; 
                               box-shadow: 0 2px 6px rgba(70,130,180,0.4);">
                    0.1%
                    <div style="font-size: 9px; color: rgba(255,255,255,0.9); margin-top: 2px;">Default</div>
                </button>
                <button onclick="setOrderbookRange(0.42)" class="range-btn" data-range="0.42"
                        style="padding: 8px 12px; background: rgba(60,70,90,0.8); border: 1px solid rgba(100,120,150,0.5); 
                               border-radius: 5px; color: #90caf9; font-size: 11px; font-weight: 500; cursor: pointer; 
                               transition: all 0.3s ease; text-align: center;">
                    0.42%
                    <div style="font-size: 9px; color: rgba(150,170,200,0.7); margin-top: 2px;">Wide</div>
                </button>
            </div>
            <div style="margin-top: 8px; padding: 8px; background: rgba(50,60,80,0.5); border-radius: 4px; border-left: 3px solid #64b5f6;">
                <div style="font-size: 10px; color: rgba(180,200,220,0.9); line-height: 1.4;">
                    <strong>Info:</strong> Filtra ordini dell'orderbook entro X% dal prezzo corrente per il calcolo del delta pesato nella strategia.
                    Valori più bassi = più reattivo, valori più alti = più stabile.
                </div>
            </div>
        </div>

        <style>
        .range-btn:hover:not(.active) {
            background: rgba(70,80,100,0.9) !important;
            border-color: rgba(120,140,170,0.7) !important;
            transform: translateY(-1px);
            box-shadow: 0 2px 6px rgba(0,0,0,0.3);
        }
        .range-btn.active {
            background: linear-gradient(135deg, rgba(70,130,180,0.9), rgba(90,150,200,0.9)) !important;
            border-color: rgba(120,170,220,0.8) !important;
            color: white !important;
            box-shadow: 0 2px 6px rgba(70,130,180,0.4) !important;
        }
        .range-btn.active div {
            color: rgba(255,255,255,0.9) !important;
        }
        </style>
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
    
            <button onclick="toggleIntensityChart()" style="background: #4a148c; color: #e0e0e0; border-color: #7b1fa2; font-weight: 600;">📊 Trend</button>
            
            </div>
        </div>
    <div class="stats-bar" id="stats-bar">Caricamento...</div>
    <div class="trading-signal" id="trading-signal">Caricamento...</div>

    <div class="phase-distribution-container" style="margin: 4px 0; padding: 4px; background: #1e1e1e; border-radius: 4px;">
        <div style="margin-bottom: 2px; font-size: 9px; font-weight: 600; color: #fff;">
            Distribuzione Strategia (Pesata per Intensità)
        </div>
        <div id="phaseBar" class="phase-bar" style="display: flex; height: 30px; border-radius: 6px; overflow: hidden; box-shadow: 0 2px 4px rgba(0,0,0,0.3);">
            </div>
        <div id="phaseStats" style="margin-top: 3px; font-size: 8px; color: #aaa; display: flex; justify-content: space-between; flex-wrap: wrap; gap: 2px;">
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
        // --- MODIFICA JAVASCRIPT ---
        let currentData = null, orderBookData = null, viewStart = 0, viewCount = 22, isFirstLoad = true;
        // Rimosse variabili: autoRefreshInterval, obRefreshInterval
        let currentInterval = '1m', currentStep = '10';
        let currentObRangePercent = 0.1; // ✅ FIX: Aggiunta variabile per stato filtro
        let relevantOrderData = null; // ⭐️ AGGIUNTO: Salva i dati degli Order Block qui
        let currentSignalData = null; // ⭐️ AGGIUNTO: Salva i dati dell'ultimo segnale

        // Funzione di rounding JS che replica (price // step) * step del Python
        function roundPriceJS(price, step) {
            if (step === 0) return price;
            // Usiamo Math.floor per replicare l'integer division (//)
            return Math.floor(price / step) * step;
        }
        
        // Rimosse variabili filtro: filterEnabled, currentPercentile

        // Rimossa funzione: toggleFilter()
        // Rimossa funzione: updatePercentileLabel()

        function changeTimeframe() {
            currentInterval = document.getElementById('interval').value;
            currentStep = document.getElementById('step').value; // ✅ FIX: Leggi lo step attuale
            isFirstLoad = true;  // Reset per nuovo timeframe
            updateApiSettings(); // ✅ FIX 1.1: Notifica al backend
            loadData();
        }
        
        // Rimossa funzione: toggleAutoRefresh()
        
        function loadData() {
            document.getElementById('loading').classList.add('active');
            const interval = document.getElementById('interval').value;
            const step = document.getElementById('step').value;
            currentInterval = interval;
            currentStep = step; // ✅ FIX: Assicura che lo step sia sempre aggiornato
            
            updateApiSettings(); // ✅ FIX 1.1: Notifica al backend (anche su cambio step)
            
            // Fetch URL semplificato (senza filtri)
            fetch('/api/data?interval=' + interval + '&step=' + step)
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
                    // Chiamata a startObRefresh() rimossa
                    resetView();
                    document.getElementById('loading').classList.remove('active');
                })
                .catch(e => {
                    console.error(e);
                    document.getElementById('loading').classList.remove('active');
                });
        }
        
        // ✅ FIX 1.1: Nuova funzione per notificare il backend (MODIFICATA per Socket.IO)
        function updateApiSettings() {
            if (window.socket && window.socket.connected) {
                console.log(`🔧 [Socket.IO] Invio impostazioni al backend: TF=${currentInterval}, Step=${currentStep}`);
                window.socket.emit('settings_update', {
                    interval: currentInterval,
                    step: currentStep
                });
            } else {
                console.warn("Socket non connesso. Impossibile aggiornare impostazioni.");
            }
        }
        
       // Rimossa funzione: loadDataLastOnly()
       // Rimossa funzione: startObRefresh()
        
        // ✅ FIX: Funzione riportata all'originale (NON FILTRATA)
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

        // ✅ FIX: Funzione calculateTradingSignal() modificata per usare la variabile
        
function calculateTradingSignal() {
    try {
        if (!currentData || !currentData.stats || !orderBookData) {
            return { signal: 'neutral', strength: 0, obDelta: 0, footprintDelta: 0, volumeRatio: 0 };
        }

        const stats = currentData.stats;
        const currentPrice = stats.price || 0;
        
        if (currentPrice === 0) {
             return { signal: 'neutral', strength: 0, obDelta: 0, footprintDelta: 0, volumeRatio: 0 };
        }

        // --- existing computations (ATR/OB/FP composite) ---
        function calculateATR(bars, period = 14) {
            if (!bars || bars.length < period + 1) return 0;
            
            const trueRanges = [];
            for (let i = 1; i < bars.length; i++) {
                const high = bars[i].high;
                const low = bars[i].low;
                const prevClose = bars[i-1].close;
                
                const tr1 = high - low;
                const tr2 = Math.abs(high - prevClose);
                const tr3 = Math.abs(low - prevClose);
                
                trueRanges.push(Math.max(tr1, tr2, tr3));
            }
            
            if (trueRanges.length >= period) {
                const sum = trueRanges.slice(-period).reduce((a, b) => a + b, 0);
                return sum / period;
            }
            return 0;
        }
        
        const atr = calculateATR(currentData.bars, 12);
        const atrPercent = atr > 0 ? (atr / currentPrice) * 100 : 0;

        // OB weighted delta
        let weightedBidQty = 0, weightedAskQty = 0;
        let totalBidQty = 0, totalAskQty = 0;

        const PRICE_WEIGHT_FACTOR = 0.015;
        const price_range_percent = (currentObRangePercent || 0.1) / 100.0;
        const lower_bound = currentPrice * (1 - price_range_percent);
        const upper_bound = currentPrice * (1 + price_range_percent);

        orderBookData.bids.forEach(p => {
            const price = parseFloat(p[0] || 0);
            if (price < lower_bound) return;
            
            const qty = parseFloat(p[1] || 0);
            const distance = Math.abs(price - currentPrice) / currentPrice;
            const weight = Math.exp(-distance / PRICE_WEIGHT_FACTOR);

            weightedBidQty += qty * weight;
            totalBidQty += qty;
        });

        orderBookData.asks.forEach(p => {
            const price = parseFloat(p[0] || 0);
            if (price > upper_bound) return;

            const qty = parseFloat(p[1] || 0);
            const distance = Math.abs(price - currentPrice) / currentPrice;
            const weight = Math.exp(-distance / PRICE_WEIGHT_FACTOR);

            weightedAskQty += qty * weight;
            totalAskQty += qty;
        });

        const weightedObDelta = weightedBidQty - weightedAskQty;
        const totalObDelta = totalBidQty - totalAskQty;

        // Footprint metrics
        const footprintDelta = stats.delta || 0;
        const avgVolume = stats.volume / Math.max(1, currentData.bars.length);
        const currentBar = currentData.bars[currentData.bars.length - 1];
        const currentVolume = currentBar ? currentBar.volume : 0;
        const volumeRatio = avgVolume > 0 ? currentVolume / avgVolume : 1;

        const OB_WEIGHT = 0.70;
        const FP_WEIGHT = 0.30;

        const totalQtyWeighted = Math.abs(weightedBidQty + weightedAskQty);
        const normalizedObDelta = totalQtyWeighted > 0 ? weightedObDelta / totalQtyWeighted : 0;
        const normalizedFpDelta = avgVolume > 0 ? footprintDelta / avgVolume : 0;
        const compositeScore = (normalizedObDelta * OB_WEIGHT) + (normalizedFpDelta * FP_WEIGHT);

        const OB_THRESHOLD = 0.08;
        const FP_THRESHOLD = 0.15;
        const VOLUME_THRESHOLD = 0.8;

        let signal = 'neutral';
        let strength = 0;

        if (compositeScore > OB_THRESHOLD && footprintDelta > 42.0 && volumeRatio > VOLUME_THRESHOLD) {
            signal = 'buy';
        } else if (compositeScore < -OB_THRESHOLD && footprintDelta < -42.0 && volumeRatio > VOLUME_THRESHOLD) {
            signal = 'sell';
        } else if (compositeScore > OB_THRESHOLD * 0.5) {
            signal = 'buy';
        } else if (compositeScore < -OB_THRESHOLD * 0.5) {
            signal = 'sell';
        }

        // --- phase tracker (unchanged) ---
        if (!window.strategyPhaseTracker) {
            window.strategyPhaseTracker = {
                phases: [],
                currentPhase: null,
                totalBars: 0
            };
        }

        const tracker = window.strategyPhaseTracker;

        if (!tracker.currentPhase || tracker.currentPhase.signal !== signal) {
            if (tracker.currentPhase) {
                tracker.phases.push({...tracker.currentPhase});
                if (tracker.phases.length > 500) {
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

        // intensity history (unchanged)
        if (!window.avgIntensityHistory) {
            window.avgIntensityHistory = {
                timestamps: [],
                buyIntensity: [],
                sellIntensity: [],
                signals: [],
                maxHistory: 7200
            };
        }

        const intensityHistory = window.avgIntensityHistory;
        const now = Date.now();

        intensityHistory.timestamps.push(now);
        intensityHistory.signals.push(signal);

        if (!window.latestSignalData) window.latestSignalData = null;

        // --- Strength calculation (unchanged) ---
        const rawIntensity = Math.min(100, Math.abs(compositeScore) * 400);
        let currentDuration = 0;
        if (tracker.currentPhase && tracker.currentPhase.signal === signal) {
            currentDuration = tracker.currentPhase.duration;
        }

        const MAX_DURATION_TICKS = 20;
        const confirmationScore = Math.min(100, (currentDuration / MAX_DURATION_TICKS) * 100);

        if (signal !== 'neutral') {
            strength = Math.round((rawIntensity * confirmationScore) / 100);
        } else {
            strength = 0;
        }

        if (tracker.currentPhase) {
            tracker.currentPhase.totalStrength += strength;
            tracker.currentPhase.avgStrength = tracker.currentPhase.totalStrength / tracker.currentPhase.duration;
        }
        
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

        // === STRATEGY: calculate entry/stop/3TP using R-multiple ===
        let entryPrice = currentPrice;
        let stopLossPrice = 0;
        let target1 = 0, target2 = 0, target3 = 0;
        let riskRewardRatio = 0;
        let strategyType = 'immediate';

        // Decide relevantOrderData from orderBookData (best levels)
        let relevantOrderData = { bids: [], asks: [] };
        try {
            relevantOrderData.bids = (orderBookData.bids || []).slice(0,3).map(p => ({ price: parseFloat(p[0]), size: parseFloat(p[1]) }));
            relevantOrderData.asks = (orderBookData.asks || []).slice(0,3).map(p => ({ price: parseFloat(p[0]), size: parseFloat(p[1]) }));
        } catch(e){}

        if (signal !== 'neutral' && relevantOrderData && atr > 0) {
            // compute stop: use OB nearest level vs ATR (conservative)
            if (signal === 'buy') {
                const stopFromOB = relevantOrderData.bids[0] ? relevantOrderData.bids[0].price : (currentPrice - atr);
                const stopFromATR = currentPrice - (atr * 1.5);
                stopLossPrice = Math.min(stopFromOB, stopFromATR);
            } else if (signal === 'sell') {
                const stopFromOB = relevantOrderData.asks[0] ? relevantOrderData.asks[0].price : (currentPrice + atr);
                const stopFromATR = currentPrice + (atr * 1.5);
                stopLossPrice = Math.max(stopFromOB, stopFromATR);
            }

            // compute R
            const R = Math.abs(entryPrice - stopLossPrice);

            if (R > 0) {
                if (signal === 'buy') {
                    target1 = entryPrice + R * 1.0;
                    target2 = entryPrice + R * 2.0;
                    target3 = entryPrice + R * 3.0;
                } else {
                    target1 = entryPrice - R * 1.0;
                    target2 = entryPrice - R * 2.0;
                    target3 = entryPrice - R * 3.0;
                }
                // compute avg R:R
                const avgTargetDist = (Math.abs(target1-entryPrice)+Math.abs(target2-entryPrice)+Math.abs(target3-entryPrice))/3;
                riskRewardRatio = R > 0 ? (avgTargetDist / R) : 0;
            }
        }

        // build signal object to be used by UI and overlay
        const signalObj = {
            signal: signal,
            strength: Math.min(100, strength),
            entryPrice: entryPrice,
            target1: target1,
            target2: target2,
            target3: target3,
            stopLossPrice: stopLossPrice,
            riskRewardRatio: riskRewardRatio.toFixed ? (riskRewardRatio.toFixed(2)) : String(riskRewardRatio),
            strategyType: strategyType,
            atr: atr.toFixed ? atr.toFixed(2) : atr,
            atrPercent: atrPercent.toFixed ? atrPercent.toFixed(3) : atrPercent,
            weightedObDelta: weightedObDelta,
            totalObDelta: totalObDelta,
            footprintDelta: footprintDelta,
            volumeRatio: volumeRatio.toFixed ? volumeRatio.toFixed(2) : volumeRatio,
            compositeScore: compositeScore.toFixed ? compositeScore.toFixed(4) : compositeScore,
            normalizedObDelta: normalizedObDelta.toFixed ? normalizedObDelta.toFixed(4) : normalizedObDelta,
            normalizedFpDelta: normalizedFpDelta.toFixed ? normalizedFpDelta.toFixed(4) : normalizedFpDelta
        };

        // store global for other UI parts and overlay
        window.currentSignalData = signalObj;
        window.latestSignalData = signalObj;

        // call overlay update if available
        if (window._updateStrategyLines) {
            try { window._updateStrategyLines(signalObj, window.currentData || currentData); } catch(e){}
        }

        return signalObj;

    } catch (e) {
        console.warn("calculateTradingSignal error:", e);
        return { signal: 'neutral', strength: 0, obDelta: 0, footprintDelta: 0, volumeRatio: 0 };
    }
}




        
    // Aggiorna display della strategia quando cambia il range
    

    function renderTradingSignal() {
            // Legge i dati globali calcolati dal listener
            const signalData = currentSignalData; 
            const signalDiv = document.getElementById('trading-signal');
            if (!signalDiv) return;

            // ✅ VALIDAZIONE: Controlla se signalData è valido
            if (!signalData || !signalData.signal) { // Fallback se non ancora calcolato
                signalDiv.innerHTML = '<div style="text-align: center; padding: 10px; color: #888;">Calculating...</div>';
                return;
            }

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

            let zoneHtml = '';
            // ✅ FIX: Usa le variabili corrette (target1, target2, target3)
            if (signalData.entryPrice > 0 && signalData.stopLossPrice > 0) {
                const entryColor = '#FFFFFF';
                const stopColor = '#9B59B6';  // Viola
                const tp1Color = '#2ECC71'; // Verde
                const tp2Color = '#3498DB'; // Blu
                const tp3Color = '#E67E22'; // Arancione
                
                zoneHtml = `
                    <div style="font-size: 9px; text-align: left; margin-top: 8px; border-top: 1px solid ${color}; padding-top: 5px;">
                        <div style="display: flex; justify-content: space-between; color: ${entryColor};">
                            <span style="color: #888;">ENTRY:</span>
                            <span style="font-weight: 600;">$${signalData.entryPrice.toFixed(2)}</span>
                        </div>
                        
                        ${signalData.target1 > 0 ? `
                        <div style="display: flex; justify-content: space-between; color: ${tp1Color}; margin-top: 2px;">
                            <span style="color: #888;">TP1 (1R):</span>
                            <span style="font-weight: 600;">$${signalData.target1.toFixed(2)}</span>
                        </div>` : ''}
                        
                        ${signalData.target2 > 0 ? `
                        <div style="display: flex; justify-content: space-between; color: ${tp2Color}; margin-top: 2px;">
                            <span style="color: #888;">TP2 (2R):</span>
                            <span style="font-weight: 600;">$${signalData.target2.toFixed(2)}</span>
                        </div>` : ''}

                        ${signalData.target3 > 0 ? `
                        <div style="display: flex; justify-content: space-between; color: ${tp3Color}; margin-top: 2px;">
                            <span style="color: #888;">TP3 (3R):</span>
                            <span style="font-weight: 600;">$${signalData.target3.toFixed(2)}</span>
                        </div>` : ''}

                        <div style="display: flex; justify-content: space-between; color: ${stopColor}; margin-top: 2px;">
                            <span style="color: #888;">STOP:</span>
                            <span style="font-weight: 600;">$${signalData.stopLossPrice.toFixed(2)}</span>
                        </div>
                    </div>
                `;
            }

            // ✅ VALIDAZIONE: Usa .toFixed() solo se i valori sono numeri
            const weightedObDelta = signalData.weightedObDelta !== undefined ? signalData.weightedObDelta.toFixed(2) : '0.00';
            const totalObDelta = signalData.totalObDelta !== undefined ? signalData.totalObDelta.toFixed(2) : '0.00';
            const footprintDelta = signalData.footprintDelta !== undefined ? signalData.footprintDelta.toFixed(2) : '0.00';
            const volumeRatio = signalData.volumeRatio !== undefined ? signalData.volumeRatio : '1.00';

            signalDiv.innerHTML = `
                <div style="text-align: center; padding: 10px; background: rgba(0,0,0,0.3); border-radius: 5px; border: 2px solid ${color}">
                    <div style="font-size: 32px; color: ${color}; font-weight: bold">${arrow}</div>
                    <div style="font-size: 14px; color: ${color}; font-weight: bold; margin-top: 5px">${text}</div>
                    <div style="font-size: 10px; color: #888; margin-top: 5px">Strength: ${signalData.strength}</div>
                    ${strengthBar}
                    
                    ${zoneHtml} 

                    <div style="font-size: 9px; color: #666; margin-top: 8px; line-height: 1.6; text-align: left; padding: 0 5px;">
                        <div style="color: #00d4ff; font-weight: 600; margin-bottom: 4px">Metrics (Filtro: ${currentObRangePercent}%)</div>
                        <div style="display: flex; justify-content: space-between;">
                            <span>OB Weighted</span>
                            <span style="color: ${signalData.weightedObDelta >= 0 ? '#26a69a' : '#ef5350'}">${weightedObDelta}</span>
                        </div>
                        <div style="display: flex; justify-content: space-between;">
                            <span>OB Total</span>
                            <span style="color: ${signalData.totalObDelta >= 0 ? '#26a69a' : '#ef5350'}">${totalObDelta}</span>
                        </div>
                        <div style="display: flex; justify-content: space-between;">
                            <span>Footprint</span>
                            <span style="color: ${signalData.footprintDelta >= 0 ? '#26a69a' : '#ef5350'}">${footprintDelta}</span>
                        </div>
                        <div style="display: flex; justify-content: space-between;">
                            <span>Volume Ratio</span>
                            <span>${volumeRatio}x</span>
                        </div>
                        <div style="display: flex; justify-content: space-between; color: #ffa726; margin-top: 3px; border-top: 1px solid #444; padding-top: 3px;">
                            <span style="font-weight: 600;">Comp Score</span>
                            <span style="font-weight: 600;">${signalData.compositeScore}</span>
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

            const buyPercent = (totalWeight > 0 ? (buyWeight / totalWeight) : 0) * 100;
            const sellPercent = (totalWeight > 0 ? (sellWeight / totalWeight) : 0) * 100;
            const neutralPercent = (totalWeight > 0 ? (neutralWeight / totalWeight) : 0) * 100;

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
            if (!chart) return;

            const isHidden = chart.style.display === 'none';
            chart.style.display = isHidden ? 'block' : 'none';

            // Se il grafico viene mostrato (isHidden era true), forziamo un re-render
            // per assicurarci che sia disegnato con i dati più recenti.
            if (isHidden && typeof renderAvgIntensityChart === 'function') {
                // Chiamato con un leggero ritardo per dare al DOM il tempo di aggiornarsi
                setTimeout(renderAvgIntensityChart, 50); 
            }
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
        
        
        // COSTANTE STRATEGY: ±0.05%
        const STRATEGY_RANGE = 0.05;

function renderChart() {
            if (!currentData || !currentData.bars) return;
            
            const displayBars = currentData.bars.slice(viewStart, viewStart + viewCount);
            document.getElementById('rangeLabel').textContent = (viewStart + 1) + '-' + (viewStart + displayBars.length);

            // ⭐️ BLOCCO AGGIUNTO PER LE LINEE DI STRATEGIA (MODIFICATO) ⭐️
            const step = parseFloat(currentStep);
            
            // Definiamo i colori
            const entryColor = '#FFFFFF'; // Bianco
            const stopColor = '#9B59B6';  // Viola
            const tp1Color = '#2ECC71'; // Verde (TP1)
            const tp2Color = '#3498DB'; // Blu (TP2)
            const tp3Color = '#E67E22'; // Arancione (TP3)
            
            // Variabili per i prezzi arrotondati
            let roundedEntry = 0, roundedStop = 0, roundedTp1 = 0, roundedTp2 = 0, roundedTp3 = 0;

            // Leggiamo i dati globali del segnale (che sono prezzi non arrotondati)
            if (currentSignalData && currentSignalData.entryPrice > 0) {
                // Arrotondiamo i prezzi allo step attuale del grafico
                roundedEntry = roundPriceJS(currentSignalData.entryPrice, step);
                roundedStop = roundPriceJS(currentSignalData.stopLossPrice, step);
                roundedTp1 = roundPriceJS(currentSignalData.target1, step);
                roundedTp2 = roundPriceJS(currentSignalData.target2, step);
                roundedTp3 = roundPriceJS(currentSignalData.target3, step);
            }
            // ⭐️ FINE BLOCCO MODIFICATO ⭐️
            
            let allPrices = new Set();
            displayBars.forEach(bar => {
                if (bar.levels) bar.levels.forEach(l => { allPrices.add(l.price); });
            });
            
            // ✅ FIX: Aggiungi i prezzi di strategia al Set per forzare il rendering dell'asse Y
            if (roundedEntry > 0) allPrices.add(roundedEntry);
            if (roundedStop > 0) allPrices.add(roundedStop);
            if (roundedTp1 > 0) allPrices.add(roundedTp1);
            if (roundedTp2 > 0) allPrices.add(roundedTp2);
            if (roundedTp3 > 0) allPrices.add(roundedTp3);

            const sortedPrices = Array.from(allPrices).sort((a, b) => b - a);
            
            const obMap = {};
            if (orderBookData && orderBookData.bids) {
                // const step = parseFloat(currentStep); // ⭐️ SPOSTATO SU ⭐️
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
                    
                    // ⭐️ AGGIUNTO: Stile per le linee (MODIFICATO) ⭐️
                    let borderStyle = '';
                    // Disegna in ordine di priorità (TP3, TP2, TP1, Entry, Stop)
                    if (roundedTp3 > 0 && price === roundedTp3) {
                        borderStyle = 'border-bottom: 2px solid ' + tp3Color + ';'; // TP3 Solido
                    } else if (roundedTp2 > 0 && price === roundedTp2) {
                        borderStyle = 'border-bottom: 2px solid ' + tp2Color + ';'; // TP2 Solido
                    } else if (roundedTp1 > 0 && price === roundedTp1) {
                        borderStyle = 'border-bottom: 2px solid ' + tp1Color + ';'; // TP1 Solido
                    } else if (roundedEntry > 0 && price === roundedEntry) {
                        borderStyle = 'border-bottom: 2px dashed ' + entryColor + ';'; // Entry Tratteggiato
                    } else if (roundedStop > 0 && price === roundedStop) {
                        borderStyle = 'border-bottom: 2px dotted ' + stopColor + ';'; // Stop Punteggiato
                    }
                    // ⭐️ FINE AGGIUNTO ⭐️

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
                    
                    html += '<td class="' + cellClass + '" style="' + borderStyle + '"><div class="orderbook-overlay">' + obOverlay + '</div>' + content + '</td>';
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
                // ✅ FIX: Aggiunto toFixed(2) per consistenza
                return '<div style="height: 22px; display: flex; align-items: center; justify-content: flex-start; font-size: 9px; color: #aaa; padding-left: 8px;">' + price.toFixed(2) + '</div>';
            }).join('');

            // Assembla tutto: tabella a sinistra + asse Y a destra
            const finalHtml = html + '</table><div style="display: flex; flex-direction: column; justify-content: space-between; padding-left: 8px; border-left: 1px solid #444; min-width: 70px;">' + priceAxisHtml + '</div></div>';
            document.getElementById('chart-container').innerHTML = finalHtml;
            // renderTradingSignal(); // Spostato al listener 1Hz
            renderPhaseDistribution();
            renderAvgIntensityChart();
        }
        
        // Caricamento iniziale
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
                    relevantOrderData = null; // ⭐️ AGGIUNTO: Pulisci in caso di errore
                    return;
                }
                
                relevantOrderData = data; // ⭐️ AGGIUNTO: Salva i dati globalmente
                
                // Se il pannello modale è aperto, renderizza anche quello
                const modal = document.getElementById('chart-modal');
                if (modal && modal.classList.contains('active')) {
                    renderOrderPanel(data); 
                }
            })
            .catch(e => {
                console.error('Error fetching relevant orders:', e);
                relevantOrderData = null; // ⭐️ AGGIUNTO: Pulisci in caso di errore
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
    // setInterval(loadRelevantOrders, 10000); // <-- RIMOSSO per usare il tick counter

    // Initial load
    window.addEventListener('load', () => {
        setTimeout(loadRelevantOrders, 1000);
    });

    // ==================== 0.420% FIXED RANGE CHART ====================

    

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
                    relevantOrderData = null; // ⭐️ AGGIUNTO: Pulisci in caso di errore
                    return;
                }
                relevantOrderData = data; // ⭐️ AGGIUNTO: Salva i dati globalmente
                renderOrderPanel(data);
                if (status) status.classList.remove('loading');
            })
            .catch(e => {
                console.error('Error:', e);
                relevantOrderData = null; // ⭐️ AGGIUNTO: Pulisci in caso di errore
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
        const paddingTop = 520;
        const paddingBottom = 520;
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
         return paddingTop + ((price - minPrice) / priceRange) * (height - paddingTop - paddingBottom);
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
                const price = minPrice + (i / gridLevels) * priceRange;
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
            const thickness = Math.min(5, 1 + (bid.quantity / 5) * 4);
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
            const thickness = Math.min(5, 1 + (ask.quantity / 5) * 4);
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

    
    

    // ════════════════════════════════════════════════════════════════
    // ORDERBOOK RANGE CONTROLS
    // ════════════════════════════════════════════════════════════════

    function setOrderbookRange(rangePercent) {
        console.log('Setting orderbook range to:', rangePercent + '%');
        
        // ✅ FIX: Aggiorna la variabile globale
        currentObRangePercent = rangePercent;

        fetch('/api/orderbook_range', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                range_percent: rangePercent
            })
        })
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                console.log('✓ Orderbook range (server) updated:', rangePercent + '%');

                // Aggiorna stile bottoni
                document.querySelectorAll('.range-btn').forEach(btn => {
                    const btnRange = parseFloat(btn.getAttribute('data-range'));
                    if (btnRange === rangePercent) {
                        btn.classList.add('active');
                    } else {
                        btn.classList.remove('active');
                    }
                });

                // Notifica
                showNotification('✓ Orderbook range: ' + rangePercent + '%', 'success');
                
                // ✅ FIX 2.3: Aggiorna il segnale di trading (spostato da 10Hz)
                // ⭐️ NOTA: Questo ora non è più necessario qui,
                // perché renderTradingSignal() viene chiamato nel loop 1Hz (footprint_update).
                // Lo lasciamo per sicurezza in caso di cambi manuali.
                if (typeof calculateTradingSignal === 'function') {
                    currentSignalData = calculateTradingSignal(); // Ricalcola
                    if (typeof renderTradingSignal === 'function') {
                        renderTradingSignal(); // Ridisegna
                    }
                }
                
            } else {
                console.error('Error:', data.error);
                showNotification('✗ ' + (data.error || 'Errore sconosciuto'), 'error');
            }
        })
        .catch(error => {
            console.error('Fetch error:', error);
            showNotification('✗ Errore di connessione', 'error');
        });
    }

    function showNotification(message, type) {
        const notification = document.createElement('div');
        notification.textContent = message;
        notification.style.cssText = `
            position: fixed;
            top: 20px;
            right: 20px;
            padding: 12px 20px;
            border-radius: 6px;
            z-index: 10000;
            font-weight: 500;
            font-size: 13px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.4);
            transition: all 0.3s ease;
            animation: slideInRight 0.3s ease;
        `;

        if (type === 'success') {
            notification.style.background = 'linear-gradient(135deg, rgba(76, 175, 80, 0.95), rgba(56, 142, 60, 0.95))';
            notification.style.color = 'white';
            notification.style.border = '1px solid rgba(255,255,255,0.3)';
        } else {
            notification.style.background = 'linear-gradient(135deg, rgba(244, 67, 54, 0.95), rgba(211, 47, 47, 0.95))';
            notification.style.color = 'white';
            notification.style.border = '1px solid rgba(255,255,255,0.3)';
        }

        document.body.appendChild(notification);

        setTimeout(() => {
            notification.style.opacity = '0';
            notification.style.transform = 'translateX(100px)';
            setTimeout(() => notification.remove(), 300);
        }, 3000);
    }

    // Carica range corrente all'avvio
    function loadCurrentRange() {
        fetch('/api/orderbook_range')
            .then(response => response.json())
            .then(data => {
                if (data.range_percent) {
                    console.log('Current orderbook range:', data.range_percent + '%');
                    
                    // ✅ FIX: Inizializza la variabile globale
                    currentObRangePercent = data.range_percent;

                    const displayElement = document.getElementById('currentRangeDisplay');
                    if (displayElement) {
                        displayElement.textContent = data.range_percent + '%';
                    }

                    // Imposta bottone attivo
                    document.querySelectorAll('.range-btn').forEach(btn => {
                        const btnRange = parseFloat(btn.getAttribute('data-range'));
                        if (btnRange === data.range_percent) {
                            btn.classList.add('active');
                        } else {
                            btn.classList.remove('active');
                        }
                    });
                }
            })
            .catch(error => {
                console.error('Error loading orderbook range:', error);
            });
    }

    // Aggiungi animazione CSS
    const style = document.createElement('style');
    style.textContent = `
        @keyframes slideInRight {
            from {
                transform: translateX(100px);
                opacity: 0;
            }
            to {
                transform: translateX(0);
                opacity: 1;
            }
        }
    `;
    document.head.appendChild(style);

    // Carica range all'avvio della pagina
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', loadCurrentRange);
    } else {
        loadCurrentRange();
    }

    </script>

    <div id="avgIntensityChart" class="draggable-chart" style="position: fixed; top: 400px; right: 20px; width: 500px; background: #1e1e1e; border-radius: 8px; border: 2px solid #00d4ff; box-shadow: 0 4px 12px rgba(0,212,255,0.3); z-index: 1000; cursor: move; display: none;">
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


    <div class="chart-modal" id="chart-modal">
        <div class="chart-modal-backdrop" onclick="toggleChartModal()"></div>
        <div class="chart-modal-content">
            <div class="chart-modal-header">
                <div class="header-left">
                    <h2>📊 Order Block Analysis</h2>
                    <span class="header-subtitle">Ordini > 7.2 BTC • Range Fisso ±0.420%</span>
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

    
    <div class="order-panel-toggle" id="order-panel-toggle" onclick="toggleOrderPanelDisplay()">
        <div class="toggle-bar">
            <div class="toggle-icon">📊</div>
            <span class="toggle-text">Order Block Analysis</span>
            <div class="toggle-arrow" id="toggle-arrow">▼</div>
        </div>
    </div>



        <div id="labPanel" style="position: fixed; bottom: 0; right: 0; width: 700px; height: 400px; background: #1a1a1a; border: 2px solid #00ff00; border-radius: 8px; padding: 12px; font-family: 'Courier New', monospace; font-size: 12px; color: #00ff00; display: none; flex-direction: column; z-index: 9998; box-shadow: 0 0 20px rgba(0,255,0,0.3);">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px; border-bottom: 2px solid #00ff00; padding-bottom: 8px;">
                <strong style="font-size: 14px;">📊 LAB - Delta OB Timeline (FULL BOOK)</strong>
                <div style="display: flex; align-items: center; gap: 10px;">
                    <select id="labSnapshotCount" onchange="renderDeltaOBChart()" style="background: #1a1a1a; color: #00ff00; border: 1px solid #00ff00; padding: 3px; font-family: 'Courier New'; font-size: 11px; cursor: pointer;">
                        <option value="50" selected>Last 50</option>
                        <option value="100">Last 100</option>
                        <option value="250">Last 250</option>
                        <option value="500">Last 500 (Max)</option>
                    </select>
                    <button id="refreshLab" style="background: #1a1a1a; color: #00ff00; border: 1px solid #00ff00; padding: 4px 10px; cursor: pointer; font-family: 'Courier New'; font-size: 11px;">⟳ Refresh</button>
                    <button id="closeLab" style="background: #1a1a1a; color: #ff0000; border: 1px solid #ff0000; padding: 4px 10px; cursor: pointer; font-family: 'Courier New'; font-size: 11px; font-weight: bold;">[X]</button>
                </div>
            </div>
            <canvas id="canvasDeltaOB" style="flex: 1; border: 1px solid #00ff00; background: #0a0a0a;"></canvas>
            <div id="labStats" style="border-top: 1px solid #00ff00; padding-top: 8px; font-size: 10px; margin-top: 8px; line-height: 1.5;">
                <span id="labStatsText" style="color: #00ff00;">⏳ Caricamento...</span>
            </div>
        </div>

        <button id="toggleLab" style="position: fixed; bottom: 15px; right: 15px; background: linear-gradient(135deg, #00ff00, #00cc00); color: #000; border: none; padding: 10px 16px; border-radius: 6px; cursor: pointer; font-family: 'Courier New'; font-size: 13px; font-weight: bold; z-index: 9999; box-shadow: 0 4px 15px rgba(0,255,0,0.4); transition: all 0.3s;">
            🔬 LAB
        </button>

        <script>
        // ============================================
        // LAB: Delta OB History Functions
        // ============================================
        const labPanel = document.getElementById('labPanel');
        const toggleLabBtn = document.getElementById('toggleLab');
        const closeLabBtn = document.getElementById('closeLab');
        const refreshLabBtn = document.getElementById('refreshLab');
        const canvasDeltaOB = document.getElementById('canvasDeltaOB');
        const ctxDeltaOB = canvasDeltaOB.getContext('2d');

        // Toggle LAB Panel
        toggleLabBtn.addEventListener('click', () => {
            const wasHidden = labPanel.style.display === 'none';
            labPanel.style.display = wasHidden ? 'flex' : 'none';
            if (wasHidden) {
                setTimeout(() => {
                    canvasDeltaOB.width = canvasDeltaOB.offsetWidth;
                    canvasDeltaOB.height = canvasDeltaOB.offsetHeight;
                    renderDeltaOBChart();
                }, 150);
            }
        });

        // Close LAB
        closeLabBtn.addEventListener('click', () => {
            labPanel.style.display = 'none';
        });

        // Refresh
        refreshLabBtn.addEventListener('click', () => {
            renderDeltaOBChart();
        });

        // Hover effect
        toggleLabBtn.addEventListener('mouseenter', () => {
            toggleLabBtn.style.transform = 'scale(1.1)';
        });
        toggleLabBtn.addEventListener('mouseleave', () => {
            toggleLabBtn.style.transform = 'scale(1)';
        });

        // Render Delta OB Chart
        async function renderDeltaOBChart() {
            try {
                document.getElementById('labStatsText').innerHTML = '⏳ Caricamento dati...';

                // Leggi il conteggio selezionato dal nuovo dropdown
                const countSelect = document.getElementById('labSnapshotCount');
                const count = countSelect ? countSelect.value : 50; // Default a 50 se non trovato

                // Aggiungi il conteggio come query parameter
                const response = await fetch(`/api/delta_ob_snapshots?count=${count}`); 

                if (!response.ok) {
                    throw new Error(`HTTP ${response.status}: ${response.statusText}`);
                }

                const result = await response.json();

                // console.log('LAB: Dati ricevuti:', result);

                if (!result.snapshots || result.snapshots.length === 0) { // ✅ FIX: Chiave API corretta
                    ctxDeltaOB.fillStyle = '#ffaa00';
                    ctxDeltaOB.font = '14px Courier New';
                    ctxDeltaOB.fillText('⏳ Raccolta dati in corso...', 10, 30);
                    ctxDeltaOB.fillText('Gli snapshot verranno catturati ogni minuto.', 10, 50);
                    ctxDeltaOB.fillText(`Snapshot salvati: ${result.count}`, 10, 70);
                    document.getElementById('labStatsText').innerHTML = '⏳ Nessun dato ancora. Gli snapshot vengono salvati ogni minuto.';
                    return;
                }

                const data = result.snapshots; // ✅ FIX: Chiave API corretta
                const width = canvasDeltaOB.width;
                const height = canvasDeltaOB.height;
                const padding = 55;

                // Clear canvas
                ctxDeltaOB.fillStyle = '#0a0a0a';
                ctxDeltaOB.fillRect(0, 0, width, height);

                // Calculate min/max for scaling
                const weightedDeltas = data.map(d => d.weighted_delta);
                const totalDeltas = data.map(d => d.total_delta);
                const allDeltas = [...weightedDeltas, ...totalDeltas];
                const minDelta = Math.min(...allDeltas);
                const maxDelta = Math.max(...allDeltas);
                const deltaDiff = maxDelta - minDelta || 1;

                // Draw grid and axes
                ctxDeltaOB.strokeStyle = '#2a2a2a';
                ctxDeltaOB.lineWidth = 1;
                ctxDeltaOB.font = '10px Courier New';

                // Y-axis
                const ySteps = 6;
                for (let i = 0; i <= ySteps; i++) {
                    const y = padding + (height - 2*padding) * (i/ySteps);
                    const value = maxDelta - (deltaDiff * i/ySteps);

                    ctxDeltaOB.beginPath();
                    ctxDeltaOB.moveTo(padding, y);
                    ctxDeltaOB.lineTo(width - padding/2, y);
                    ctxDeltaOB.stroke();

                    ctxDeltaOB.fillStyle = '#666666';
                    ctxDeltaOB.fillText(value.toFixed(2), 5, y + 3);
                }

                // Zero line (se visibile)
                if (minDelta < 0 && maxDelta > 0) {
                    const zeroY = padding + (height - 2*padding) * (1 - (Math.abs(minDelta) / deltaDiff));
                    ctxDeltaOB.strokeStyle = '#ffffff';
                    ctxDeltaOB.lineWidth = 1.5;
                    ctxDeltaOB.setLineDash([5, 5]);
                    ctxDeltaOB.beginPath();
                    ctxDeltaOB.moveTo(padding, zeroY);
                    ctxDeltaOB.lineTo(width - padding/2, zeroY);
                    ctxDeltaOB.stroke();
                    ctxDeltaOB.setLineDash([]);
                }

                // X-axis labels
                const xSteps = Math.min(data.length, 10);
                const xStep = Math.max(1, Math.floor(data.length / xSteps));

                for (let i = 0; i < data.length; i += xStep) {
                    const x = padding + (width - padding*1.5) * (i / Math.max(1, data.length - 1));
                    ctxDeltaOB.fillStyle = '#666666';
                    ctxDeltaOB.fillText(data[i].time, x - 15, height - padding/2 + 5);
                }

                // Draw weighted delta line (GREEN)
                ctxDeltaOB.strokeStyle = '#00ff00';
                ctxDeltaOB.lineWidth = 2.5;
                ctxDeltaOB.beginPath();

                for (let i = 0; i < data.length; i++) {
                    const normalizedDelta = (data[i].weighted_delta - minDelta) / deltaDiff;
                    const x = padding + (width - padding*1.5) * (i / Math.max(1, data.length - 1));
                    const y = padding + (height - 2*padding) * (1 - normalizedDelta);

                    if (i === 0) {
                        ctxDeltaOB.moveTo(x, y);
                    } else {
                        ctxDeltaOB.lineTo(x, y);
                    }
                }
                ctxDeltaOB.stroke();

                // Draw dots on weighted line
                ctxDeltaOB.fillStyle = '#00ff00';
                for (let i = 0; i < data.length; i++) {
                    const normalizedDelta = (data[i].weighted_delta - minDelta) / deltaDiff;
                    const x = padding + (width - padding*1.5) * (i / Math.max(1, data.length - 1));
                    const y = padding + (height - 2*padding) * (1 - normalizedDelta);

                    ctxDeltaOB.beginPath();
                    ctxDeltaOB.arc(x, y, 2, 0, Math.PI * 2);
                    ctxDeltaOB.fill();
                }

                // Draw total delta as bars
                const barWidth = Math.max(2, (width - padding*1.5) / data.length * 0.7);
                const zeroY = padding + (height - 2*padding) * (1 - (Math.abs(minDelta) / deltaDiff));

                for (let i = 0; i < data.length; i++) {
                    const normalizedDelta = (data[i].total_delta - minDelta) / deltaDiff;
                    const x = padding + (width - padding*1.5) * (i / Math.max(1, data.length - 1));
                    const y = padding + (height - 2*padding) * (1 - normalizedDelta);

                    if (data[i].total_delta >= 0) {
                        ctxDeltaOB.fillStyle = 'rgba(0, 255, 0, 0.25)';
                    } else {
                        ctxDeltaOB.fillStyle = 'rgba(255, 0, 0, 0.25)';
                    }

                    const barHeight = Math.abs(y - zeroY);
                    ctxDeltaOB.fillRect(x - barWidth/2, Math.min(y, zeroY), barWidth, barHeight);
                }

                // Legend
                ctxDeltaOB.font = 'bold 11px Courier New';
                ctxDeltaOB.fillStyle = '#00ff00';
                ctxDeltaOB.fillText('━ Weighted Δ', width - 150, 20);
                ctxDeltaOB.fillStyle = 'rgba(0, 255, 0, 0.5)';
                ctxDeltaOB.fillText('▮ Total Δ', width - 150, 35);

                // Stats
                const lastData = data[data.length - 1];
                const firstData = data[0];
                const avgWeighted = (weightedDeltas.reduce((a, b) => a + b, 0) / weightedDeltas.length).toFixed(2);
                const avgTotal = (totalDeltas.reduce((a, b) => a + b, 0) / totalDeltas.length).toFixed(2);
                //const deltaChange = (lastData.weighted_delta - firstData.weighted_delta).toFixed(2);

                const currentRange = lastData.price_range_percent || 'N/A'; // Legge il valore 'ALL'

                document.getElementById('labStatsText').innerHTML = `
                    <strong>Range Filtro:</strong> <span style="color: #ffff00; font-weight: bold;">${currentRange}</span> | 
                    <strong>Last (W):</strong> <span style="color: ${lastData.weighted_delta >= 0 ? '#00ff00' : '#ff0000'}">${lastData.weighted_delta.toFixed(2)}</span> | 
                    <strong>Last (T):</strong> <span style="color: ${lastData.total_delta >= 0 ? '#00ff00' : '#ff0000'}">${lastData.total_delta.toFixed(2)}</span> | 
                    <strong>Avg (W):</strong> ${avgWeighted} | 
                    <strong>Snapshots:</strong> ${data.length}
                `;

            } catch (e) {
                console.error('❌ Error rendering delta OB:', e);
                ctxDeltaOB.fillStyle = '#ff0000';
                ctxDeltaOB.font = '12px Courier New';
                ctxDeltaOB.fillText('❌ Errore: ' + e.message, 10, 30);
                ctxDeltaOB.fillText('Controlla la console del browser (F12)', 10, 50);
                document.getElementById('labStatsText').innerHTML = '❌ Errore: ' + e.message;
            }
        }

        // Auto-update LAB every 15 seconds when visible
        setInterval(() => {
            if (labPanel.style.display === 'flex') {
                renderDeltaOBChart();
            }
        }, 15000);
        </script>
    
    <script>
        // Attendi che il DOM sia pronto
        document.addEventListener('DOMContentLoaded', function() {
            console.log('📡 Initializing WebSocket...');
            
            // ✅ FIX: Assicurati che 'io' sia definito
            if (typeof io === 'undefined') {
                console.error('❌ Socket.IO non è caricato. Gli aggiornamenti live non funzioneranno.');
                // Mostra un errore all'utente
                const indicator = createStatusIndicator();
                indicator.innerHTML = `<div style="font-weight:bold; color:#ff0000;">❌ ERRORE CRITICO</div><div style="color:#888;">Socket.IO non caricato.</div>`;
                indicator.style.borderColor = "#ff0000";
                return;
            }

            // ✅ CREA SOCKET GLOBALE (Unica istanza)
            window.socket = io({reconnection: true, reconnectionAttempts: Infinity});
            console.log('🔌 Socket creato:', typeof window.socket);
            
            // ✅ FIX: Crea l'indicatore di stato
            const wsStatusIndicator = createStatusIndicator();
            let wsUpdateCount = 0;
            updateStatusIndicator(wsStatusIndicator, false, 0); // Inizializza come disconnesso

            // ✅ LISTENER: DATI RICEVUTI
            // ═══════════════════════════════════════════════════════════════════

            // ═══════════════════════════════════════════════════════════════════
            // LISTENER 1: Orderbook updates (10 Hz)
            // ═══════════════════════════════════════════════════════════════════
            window.socket.on('orderbook_update', function(data) {
                // console.log('📊 Orderbook received: #' + (data.update_id || 0)); // Troppo verboso

                if (data.orderbook) {
                    // Aggiorna lo stato globale
                    orderBookData = data.orderbook;
                    
                    // Aggiorna solo l'header (veloce)
                    if (typeof updateObDisplay === 'function') updateObDisplay();
                    
                    // ✅ FIX 2.1: RIMOSSO renderChart() da 10Hz
                    // ✅ FIX 2.3: RIMOSSO renderTradingSignal() da 10Hz
                }
            });

            // ═══════════════════════════════════════════════════════════════════
            // LISTENER 2: Footprint updates (1 Hz) ✅ MODIFICATO!
            // ═══════════════════════════════════════════════════════════════════
            
            let orderDataTickCounter = 0; // ⭐️ AGGIUNTO
            const ORDER_DATA_REFRESH_TICKS = 5; // ⭐️ AGGIUNTO: Aggiorna prezzi ogni 5 sec

            window.socket.on('footprint_update', function(data) {
                // console.log('📈 Footprint received:', data.bars ? data.bars.length + ' bars' : 'no data', ' - Update #' + (data.update_id || 0));
                wsUpdateCount++;

                // ⭐️ BLOCCO AGGIUNTO ⭐️
                // Aggiorna i dati degli Order Block per i prezzi (ogni 5 secondi)
                orderDataTickCounter++;
                if (orderDataTickCounter >= ORDER_DATA_REFRESH_TICKS) {
                    if (typeof loadRelevantOrders === 'function') {
                        loadRelevantOrders();
                    }
                    orderDataTickCounter = 0;
                }
                // ⭐️ FINE BLOCCO ⭐️

                // ✅ FIX 1.3 (Race Condition): Verifica che i dati WS corrispondano
                // (Usiamo != per type coercion, es. 10.0 == "10")
                if (data.interval !== currentInterval || data.step != currentStep) {
                     console.warn(`⚠️ Footprint update ignorato: Dati WS (${data.interval}/${data.step}) non corrispondono allo stato UI (${currentInterval}/${currentStep}). Attendo...`);
                     return;
                }

                // ✅ GUARDIA (FIX BUG CARICAMENTO)
                // Se currentData non è ancora stato caricato (primo avvio), 
                // ignora questo aggiornamento. Sarà loadData() a popolarlo.
                if (!currentData || !currentData.bars || currentData.bars.length === 0) {
                    console.warn('⚠️ Footprint update ignorato: Dati iniziali (currentData) non ancora caricati.');
                    return; 
                }
                // ✅ FINE GUARDIA

                if (data.bars && data.bars.length > 0) {
                    // Visto che la guardia è attiva, currentData esiste.
                    const newLastBar = data.bars[data.bars.length - 1];

                    const normalizeTimestamp = (ts) => {
                        const intervalMS = get_interval_ms(currentInterval); // Usa l'intervallo corrente
                        return Math.floor(ts / intervalMS) * intervalMS;
                    };

                    const currentLastBar = currentData.bars[currentData.bars.length - 1];
                    const currentNormalized = normalizeTimestamp(currentLastBar.timestamp);
                    const newNormalized = normalizeTimestamp(newLastBar.timestamp);

                    if (currentNormalized === newNormalized) {
                        // console.log(`🔄 Update candela ${new Date(currentNormalized).toLocaleTimeString()}`);
                        // Aggiorna l'ultima candela (che è l'unica con dati aggiornati dal payload)
                        currentData.bars[currentData.bars.length - 1] = newLastBar;
                        currentData.stats = data.stats;
                    } else if (newNormalized > currentNormalized) { // Assicura che sia una nuova candela
                        console.log(`➕ Nuova candela: ${new Date(newNormalized).toLocaleTimeString()}`);
                        // Aggiungi la nuova candela (l'ultima del payload)
                        currentData.bars.push(newLastBar);
                        currentData.stats = data.stats;

                        const MAX_BARS = 200;
                        if (currentData.bars.length > MAX_BARS) {
                            currentData.bars = currentData.bars.slice(-MAX_BARS);
                            // console.log(`🗑️ Trimmed a ${MAX_BARS} candele`);
                        }
                    } else {
                         console.log(`❔ Candela ricevuta (${newNormalized}) è più vecchia dell'attuale (${currentNormalized}). Ignorata.`);
                    }

                    if (typeof updateObDisplay === 'function') updateObDisplay();
                    if (typeof renderStatsBar === 'function') renderStatsBar(data.stats);

                    viewStart = Math.max(0, currentData.bars.length - viewCount);
                    const slider = document.getElementById('rangeSlider');
                    if (slider) {
                        slider.max = Math.max(0, currentData.bars.length - viewCount);
                        slider.value = viewStart;
                    }

                    // ⭐️ NUOVA SEQUENZA DI RENDER ⭐️
                    // 1. Calcola i dati della strategia e salvali globalmente
                    if (typeof calculateTradingSignal === 'function') {
                        currentSignalData = calculateTradingSignal();
                    }
                    // 2. Renderizza il pannello laterale (che leggerà i dati globali)
                    if (typeof renderTradingSignal === 'function') {
                        renderTradingSignal(); 
                    }
                    // 3. Renderizza il grafico (che leggerà i dati globali)
                    if (typeof renderChart === 'function') {
                        renderChart();
                    }
                }

                // Aggiorna l'indicatore di stato
                updateStatusIndicator(wsStatusIndicator, true, wsUpdateCount);
            })

            // ✅ LISTENER: CONNESSIONE
            window.socket.on('connect', function() {
                console.log('✅ WebSocket CONNESSO! (Listener Principale)');
                updateStatusIndicator(wsStatusIndicator, true, wsUpdateCount);
                updateApiSettings(); // Sincronizza
            });
            
            // ✅ FIX 1.1: Listener per conferma impostazioni
            window.socket.on('settings_confirmed', function(data) {
                console.log(`✓ [Socket.IO] Impostazioni confermate dal backend: TF=${data.interval}, Step=${data.step}`);
            });

            // ✅ LISTENER: DISCONNESSIONE
            window.socket.on('disconnect', function() {
                console.log('❌ WebSocket disconnesso (Listener Principale)');
                updateStatusIndicator(wsStatusIndicator, false, wsUpdateCount);
            });
            
            window.socket.on('connect_error', (err) => {
                 console.error('❌ Errore connessione WebSocket:', err.message);
                 updateStatusIndicator(wsStatusIndicator, false, wsUpdateCount);
            });

            console.log('✅ WebSocket listeners registrati!');
            
            // ✅ FIX: Aggiungi helper get_interval_ms
            function get_interval_ms(interval) {
                const intervals = {"1m": 60000, "5m": 300000, "15m": 900000, "30m": 1800000, "1h": 3600000, "1d": 86400000};
                return intervals[interval] || 60000;
            }
            
            // ✅ FIX: Funzione helper per creare l'indicatore
            function createStatusIndicator() {
                const div = document.createElement('div');
                div.id = "ws-status-indicator";
                div.style.cssText = 'position:fixed;bottom:60px;right:15px;padding:12px;background:rgba(20,20,20,0.95);border:2px solid #00d4ff;border-radius:6px;font-size:10px;z-index:10000;font-family:monospace;color:#00d4ff';
                div.innerHTML = '<div style="font-weight:bold">🔧 INIT...</div>';
                document.body.appendChild(div);
                return div;
            }
            
            // ✅ FIX: Funzione helper per aggiornare l'indicatore
            function updateStatusIndicator(indicator, isConnected, count) {
                 if (indicator) {
                     if (isConnected) {
                        indicator.innerHTML = `<div style="font-weight:bold">✅ WS LIVE</div><div style="color:#888;">${currentInterval}/${currentStep}$ (${count} upd)</div>`;
                        indicator.style.borderColor = "#00ff00";
                     } else {
                        indicator.innerHTML = `<div style="font-weight:bold; color:#ff0000;">❌ WS DISCONNECTED</div>`;
                        indicator.style.borderColor = "#ff0000";
                     }
                }
            }
            
        });
    </script>


<!-- STRATEGY LINES OVERLAY (TP/SL with labels - TradingView style) -->
<script>
(function(){
    function ensureOverlay() {
        let container = document.getElementById('chart-container') || document.querySelector('.chart-container') || document.body;
        if (!container) return null;
        let overlay = document.getElementById('strategy-lines-overlay');
        if (!overlay) {
            overlay = document.createElementNS("http://www.w3.org/2000/svg", "svg");
            overlay.setAttribute('id', 'strategy-lines-overlay');
            overlay.style.position = 'absolute';
            overlay.style.top = '0';
            overlay.style.left = '0';
            overlay.style.width = '100%';
            overlay.style.height = '100%';
            overlay.style.pointerEvents = 'none';
            overlay.style.zIndex = '9999';
            const cstyle = window.getComputedStyle(container);
            if (cstyle.position === 'static') container.style.position = 'relative';
            container.appendChild(overlay);
        }
        return overlay;
    }

    function clearOverlay(overlay) {
        while (overlay.firstChild) overlay.removeChild(overlay.firstChild);
    }

    function priceToY(price, bars, containerHeight) {
        if (!bars || bars.length === 0) return null;
        let minP = Infinity, maxP = -Infinity;
        for (let b of bars) {
            if (b.low !== undefined) minP = Math.min(minP, b.low);
            if (b.high !== undefined) maxP = Math.max(maxP, b.high);
        }
        if (!isFinite(minP) || !isFinite(maxP) || maxP === minP) return null;
        const pct = (price - minP) / (maxP - minP);
        return Math.round((1 - pct) * containerHeight);
    }

    function drawLine(overlay, y, color, labelText) {
        if (y === null) return;
        const ns = "http://www.w3.org/2000/svg";
        const line = document.createElementNS(ns, 'line');
        line.setAttribute('x1', '0');
        line.setAttribute('x2', '100%');
        line.setAttribute('y1', y);
        line.setAttribute('y2', y);
        line.setAttribute('stroke', color);
        line.setAttribute('stroke-width', '2');
        line.setAttribute('stroke-linecap', 'round');
        line.style.opacity = '0.95';
        overlay.appendChild(line);

        const svgWidth = overlay.clientWidth || overlay.getBoundingClientRect().width || 300;
        const labelWidth = Math.min(140, Math.round(svgWidth * 0.18));
        const labelHeight = 18;
        const rect = document.createElementNS(ns, 'rect');
        rect.setAttribute('x', svgWidth - labelWidth - 8);
        rect.setAttribute('y', Math.max(2, y - Math.round(labelHeight/2)));
        rect.setAttribute('width', labelWidth);
        rect.setAttribute('height', labelHeight);
        rect.setAttribute('rx', 4);
        rect.setAttribute('ry', 4);
        rect.setAttribute('fill', 'rgba(0,0,0,0.6)');
        rect.setAttribute('stroke', color);
        rect.setAttribute('stroke-width', '1');
        overlay.appendChild(rect);

        const text = document.createElementNS(ns, 'text');
        text.setAttribute('x', svgWidth - labelWidth/2 - 8);
        text.setAttribute('y', Math.max(2 + Math.round(labelHeight/2), y + 5));
        text.setAttribute('fill', '#fff');
        text.setAttribute('font-size', '12');
        text.setAttribute('font-family', 'Arial, Helvetica, sans-serif');
        text.setAttribute('text-anchor', 'middle');
        text.textContent = labelText;
        overlay.appendChild(text);
    }

    function updateLines(signalData, currentData) {
        try {
            const overlay = ensureOverlay();
            if (!overlay) return;
            clearOverlay(overlay);

            if (!signalData) return;
            const bars = (currentData && currentData.bars) ? currentData.bars : [];
            const container = overlay.parentElement;
            const ch = container.clientHeight || overlay.getBoundingClientRect().height || 400;

            const entry = signalData.entryPrice || signalData.entry || 0;
            const t1 = signalData.target1 || signalData.t1 || 0;
            const t2 = signalData.target2 || signalData.t2 || 0;
            const t3 = signalData.target3 || signalData.t3 || 0;
            const stop = signalData.stopLossPrice || signalData.stop || 0;

            const y_t3 = t3 ? priceToY(t3, bars, ch) : null;
            const y_t2 = t2 ? priceToY(t2, bars, ch) : null;
            const y_t1 = t1 ? priceToY(t1, bars, ch) : null;
            const y_entry = entry ? priceToY(entry, bars, ch) : null;
            const y_stop = stop ? priceToY(stop, bars, ch) : null;

            if (y_t3 !== null) drawLine(overlay, y_t3, '#1b5e20', 'TP3 ' + formatPrice(t3));
            if (y_t2 !== null) drawLine(overlay, y_t2, '#2e7d32', 'TP2 ' + formatPrice(t2));
            if (y_t1 !== null) drawLine(overlay, y_t1, '#4caf50', 'TP1 ' + formatPrice(t1));
            if (y_entry !== null) drawLine(overlay, y_entry, '#ffffff', 'ENTRY ' + formatPrice(entry));
            if (y_stop !== null) drawLine(overlay, y_stop, '#f44336', 'STOP ' + formatPrice(stop));

        } catch (e) {
            console.warn("updateLines error", e);
        }
    }

    function formatPrice(p) {
        if (p === undefined || p === null) return '-';
        return (Math.round(p * 100) / 100).toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2});
    }

    function hookIntoSocket() {
        try {
            if (window.socket && window.socket.on) {
                window.socket.on('footprint_update', function(payload){
                    const signal = window.currentSignalData || window.currentSignal || (window.latestSignalData || null);
                    updateLines(signal, payload);
                });
                window.socket.on('orderbook_update', function(payload){ 
                    const signal = window.currentSignalData || window.currentSignal || (window.latestSignalData || null);
                    updateLines(signal, window.currentData || payload);
                });
            }
        } catch(e) {}
    }

    window._updateStrategyLines = updateLines;

    document.addEventListener('DOMContentLoaded', function(){
        setTimeout(function(){
            ensureOverlay();
            hookIntoSocket();
            const sig = window.currentSignalData || window.currentSignal || (window.latestSignalData || null);
            if (sig) {
                window._updateStrategyLines(sig, window.currentData || {});
            }
        }, 800);
    });

})();
</script>
<!-- END STRATEGY LINES OVERLAY -->

</body>
</html>
   

    """
    return html

# --- MODIFICA BACKEND ---

# ✅ FIX 1.1: Rimossa rotta HTTP
# @app.route('/api/settings', methods=['POST'])
# ... (tutta la vecchia funzione è stata rimossa) ...


@app.route('/api/data')
def get_data():
    interval = request.args.get('interval', '1m')
    step = float(request.args.get('step', 10))
    update_last_only = request.args.get('update_last', 'false') == 'true'

    # Parametri filtro rimossi

    # Cache key semplificata
    cache_key = f"{interval}_{step}"
    
    with CACHE['lock']:
        # if cache_key in CACHE['data'] and not update_last_only:
        #     entry = CACHE['data'][cache_key]
        #     return jsonify(entry['data'])
        # Disabilitiamo la cache HTTP per forzare il ricaricamento
        
        # Chiamata a process_data modificata (senza filtri)
        data = process_data(interval, step, update_last_only)
        
        if not update_last_only:
            CACHE['data'][cache_key] = {'data': data, 'timestamp': time.time()}
    
    return jsonify(data)

@app.route('/api/orderbook')
def get_orderbook():
    with CACHE['lock']:
        if 'orderbook' in CACHE and time.time() - CACHE['orderbook'].get('timestamp', 0) < 3:
            return jsonify(CACHE['orderbook']['data'])
        
        ob_data = fetch_orderbook_multi() 
        CACHE['orderbook'] = {'data': ob_data, 'timestamp': time.time()}
    
    return jsonify(ob_data)


@app.route('/api/relevant_orders')
def get_relevant_orders():
    """
    Ordini rilevanti orderbook - RANGE FISSO ±0.420%
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

        ob_data = fetch_orderbook_multi()
        if not ob_data or 'bids' not in ob_data or 'asks' not in ob_data:
            return jsonify({'error': 'Cannot fetch orderbook'}), 500

        # RANGE FISSO: ±0.420%
        FIXED_RANGE_PCT = 0.420
        price_range = current_price * (FIXED_RANGE_PCT / 100.0)
        min_price = current_price - price_range
        max_price = current_price + price_range

        MIN_BTC_THRESHOLD = 7.2 # <-- VALORE MODIFICABILE (Era 10.0)

        relevant_bids = []
        for price, quantity in ob_data['bids']:
            price = float(price)
            quantity = float(quantity)
            # Filtra per range E per soglia minima
            if min_price <= price <= max_price and quantity >= MIN_BTC_THRESHOLD:
                relevant_bids.append({
                    'price': price,
                    'quantity': quantity,
                    'total': price * quantity
                })

        relevant_asks = []
        for price, quantity in ob_data['asks']:
            price = float(price)
            quantity = float(quantity)
            # Filtra per range E per soglia minima
            if min_price <= price <= max_price and quantity >= MIN_BTC_THRESHOLD:
                relevant_asks.append({
                    'price': price,
                    'quantity': quantity,
                    'total': price * quantity
                })

        # Ordinamento (Cruciale per la strategia: [0] deve essere il più vicino)
        relevant_bids.sort(key=lambda x: x['price'], reverse=True) # Dal più alto (vicino) al più basso
        relevant_asks.sort(key=lambda x: x['price']) # Dal più basso (vicino) al più alto

        # Calcolo summary
        total_bid_qty = sum(b['quantity'] for b in relevant_bids)
        total_ask_qty = sum(a['quantity'] for a in relevant_asks)
        total_bid_value = sum(b['total'] for b in relevant_bids)
        total_ask_value = sum(a['total'] for a in relevant_asks)
        delta = total_bid_qty - total_ask_qty

        logger.info(f"API /relevant_orders: Bids={len(relevant_bids)}, Asks={len(relevant_asks)}, Delta={delta:.2f}")

        return jsonify({
            'current_price': current_price,
            'chart_timeframe': chart_tf,
            'price_range': {
                'min': min_price,
                'max': max_price,
                'pct': FIXED_RANGE_PCT,
                'total_range': max_price - min_price
            },
            'summary': {
                'total_bid_qty': total_bid_qty,
                'total_ask_qty': total_ask_qty,
                'total_bid_value': total_bid_value,
                'total_ask_value': total_ask_value,
                'delta': delta
            },
            'bids': relevant_bids[:25], # Limita a 25
            'asks': relevant_asks[:25], # Limita a 25
            'price_history': price_history
        })

    except Exception as e:
        logger.error(f"❌ Errore in /api/relevant_orders: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/api/delta-ob-history')
def get_delta_ob_history():
    """Endpoint per il grafico LAB (restituisce tutti i 500 snapshot)."""
    with CACHE['lock']:
        # Crea una copia della lista per evitare problemi di concorrenza
        snapshots = list(CACHE.get('delta_ob_snapshots', []))
    
    logger.info(f"🔬 LAB: /api/delta-ob-history: Ritornando {len(snapshots)} snapshot")
    return jsonify({
        'data': snapshots, # ✅ FIX: Corretto da 'snapshots' a 'data' per coerenza
        'count': len(snapshots)
    })

@app.route('/api/orderbook_range', methods=['GET', 'POST'])
def handle_orderbook_range():
    """Gestisce il range di prezzo per il calcolo del delta OB pesato."""
    global ORDERBOOK_PRICE_RANGE_PERCENT

    if request.method == 'POST':
        try:
            data = request.json
            new_range = float(data.get('range_percent'))
            
            # Validazione
            valid_ranges = [0.01, 0.042, 0.1, 0.42]
            if new_range not in valid_ranges:
                logger.warning(f"Range non valido ricevuto: {new_range}")
                return jsonify({'success': False, 'error': f'Range non valido. Usare uno di: {valid_ranges}'}), 400
            
            ORDERBOOK_PRICE_RANGE_PERCENT = new_range
            logger.info(f"🔔🔔🔔 Range OB Delta aggiornato a: {ORDERBOOK_PRICE_RANGE_PERCENT}% 🔔🔔🔔")
            return jsonify({'success': True, 'range_percent': ORDERBOOK_PRICE_RANGE_PERCENT})
        
        except Exception as e:
            logger.error(f"Errore /api/orderbook_range POST: {e}")
            return jsonify({'success': False, 'error': str(e)}), 500
    
    # Metodo GET (caricamento pagina)
    return jsonify({
        'range_percent': ORDERBOOK_PRICE_RANGE_PERCENT
    })

# ═══════════════════════════════════════════════════════════════════════════════
# AVVIO SERVER
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    logger.info("="*50)
    logger.info("🚀 Avvio server Flask...")
    
    if WEBSOCKET_AVAILABLE and socketio:
        logger.info("...in modalità WebSocket (SocketIO) su porta 5000.")
        logger.info("="*50)
        # allow_unsafe_werkzeug=True è necessario per le versioni recenti
        socketio.run(app, host='0.0.0.0', port=5000, debug=False,)



# --- Compatibility emitter: ensure frontend always receives unified strategy payload ---
def _safe_send_strategy_payload(sig):
    """Emit a unified strategy payload via available socket interfaces.
    This function attempts multiple emit methods to maximize compatibility with different setups."""
    try:
        # normalize numeric types
        def _norm(v):
            try:
                return float(v)
            except Exception:
                return v
        payload = {}
        if not sig:
            return
        # pick keys we expect; fall back to sig itself
        for k in ['signal','entry','stop','target1','target2','target3','timestamp','atr','composite','weighted_ob_delta']:
            if k in sig:
                payload[k] = _norm(sig[k]) if isinstance(sig[k], (int,float,str)) and k!='signal' else sig[k]
        # include everything else too
        for k,v in sig.items():
            if k not in payload:
                payload[k] = v
        # Try flask-socketio
        try:
            if 'socketio' in globals() and globals().get('socketio') is not None:
                try:
                    globals()['socketio'].emit('strategy_signal', payload)
                except Exception:
                    # sometimes need to use the app context
                    try:
                        globals()['socketio'].emit('strategy_signal', payload, namespace='/')
                    except Exception:
                        pass
        except Exception:
            pass
        # Try a global 'socket' if front-end uses plain socket.io client attached to window via a proxy endpoint
        try:
            # nothing to do in Python for window.socket; this is a placeholder for other transports
            pass
        except Exception:
            pass
    except Exception:
        try:
            import logging
            logging.getLogger(__name__).exception('_safe_send_strategy_payload error')
        except Exception:
            pass

# Background thread: watch CACHE['latest_signal'] and emit when it changes.
import threading as _threading
_last_emitted_ts = None
def _strategy_emitter_loop():
    global _last_emitted_ts
    while True:
        try:
            sig = None
            try:
                sig = globals().get('CACHE', {}).get('latest_signal', None)
            except Exception:
                sig = None
            if sig and isinstance(sig, dict):
                ts = sig.get('timestamp') or sig.get('time') or sig.get('ts') or None
                if ts is None:
                    # fabricate
                    try:
                        ts = int(time.time() * 1000)
                        sig['timestamp'] = ts
                    except Exception:
                        ts = None
                if ts and ts != _last_emitted_ts:
                    _last_emitted_ts = ts
                    _safe_send_strategy_payload(sig)
            time.sleep(0.8)
        except Exception:
            try:
                time.sleep(1)
            except Exception:
                pass

_emitter_thread = _threading.Thread(target=_strategy_emitter_loop, daemon=True)
_emitter_thread.start()

# --- End compatibility emitter ---
