import requests
import time

# Constants for the Binance API
BINANCE_API_URL = 'https://api.binance.com/api/v3'


def get_interval_ms(interval):
    """
    Converts the time interval from string format to milliseconds.
    Supported intervals: '1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w', '1M'
    """
    intervals = {
        '1m': 60000,
        '3m': 180000,
        '5m': 300000,
        '15m': 900000,
        '30m': 1800000,
        '1h': 3600000,
        '2h': 7200000,
        '4h': 14400000,
        '6h': 21600000,
        '8h': 28800000,
        '12h': 43200000,
        '1d': 86400000,
        '3d': 259200000,
        '1w': 604800000,
        '1M': 2629800000
    }
    return intervals.get(interval, None)


def fetch_with_retry(url, params=None, retries=3, delay=1):
    """
    Fetch a URL with retry logic.
    Args:
        url (str): The URL to fetch.
        params (dict, optional): URL parameters to send with the request.
        retries (int): Number of retry attempts.
        delay (int): Delay between retries in seconds.
    """
    for attempt in range(retries):
        response = requests.get(url, params=params)
        if response.status_code == 200:
            return response.json()
        time.sleep(delay)
    raise Exception('Failed to fetch data after {} attempts.'.format(retries))


def fetch_klines(symbol, interval, start_time=None, end_time=None):
    """
    Fetch candlestick data (klines) from Binance.
    Args:
        symbol (str): The symbol to fetch data for (e.g., 'BTCUSDT').
        interval (str): The interval for the kline data.
        start_time (int, optional): Start time in milliseconds.
        end_time (int, optional): End time in milliseconds.
    """
    url = f'{BINANCE_API_URL}/klines'
    params = {
        'symbol': symbol,
        'interval': interval,
        'startTime': start_time,
        'endTime': end_time
    }
    return fetch_with_retry(url, params)


def fetch_trades(symbol, limit=100):
    """
    Fetch recent trades from Binance.
    Args:
        symbol (str): The symbol to fetch trades for (e.g., 'BTCUSDT').
        limit (int): Number of trades to fetch.
    """
    url = f'{BINANCE_API_URL}/trades'
    params = {
        'symbol': symbol,
        'limit': limit
    }
    return fetch_with_retry(url, params)
