import concurrent.futures
import requests
import threading

# Thread-safe dictionary for cached orderbooks
cached_orderbooks = {}
cache_lock = threading.Lock()

def fetch_orderbook(exchange):
    """Fetch orderbook for a single exchange."""
    url = f"https://api.{exchange}.com/orderbook"
    response = requests.get(url)
    return exchange, response.json()

def fetch_orderbook_multi(exchanges):
    """Fetch orderbooks from multiple exchanges concurrently."""
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_exchange = {executor.submit(fetch_orderbook, exchange): exchange for exchange in exchanges}
        for future in concurrent.futures.as_completed(future_to_exchange):
            exchange, orderbook = future.result()
            update_orderbook_cache({exchange: orderbook})

def update_orderbook_cache(orderbooks):
    """Update cached orderbooks with the latest fetched data."""
    with cache_lock:
        cached_orderbooks.update(orderbooks)

def get_cached_orderbook(exchange):
    """Retrieve the cached orderbook for a specific exchange."""
    with cache_lock:
        return cached_orderbooks.get(exchange)