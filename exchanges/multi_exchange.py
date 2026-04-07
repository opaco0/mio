import ccxt
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)

# Global variable to hold multiple exchange instances
EXCHANGES_MULTI = {}

def init_multi_exchanges(exchange_ids):
    global EXCHANGES_MULTI
    
    for exchange_id in exchange_ids:
        try:
            if exchange_id in ccxt.exchanges:
                EXCHANGES_MULTI[exchange_id] = getattr(ccxt, exchange_id)()
                logging.info(f'{exchange_id} exchange initialized successfully.')
            else:
                logging.error(f'Exchange id {exchange_id} is not valid.')
        except Exception as e:
            logging.error(f'Failed to initialize {exchange_id}: {str(e)}')

# Example usage
# exchange_ids = ['binance', 'okx', 'coinbase', 'bybit', 'kucoin', 'bitget']
# init_multi_exchanges(exchange_ids)