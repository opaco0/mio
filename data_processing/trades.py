import requests
import pandas as pd

class BinanceTradeData:
    BASE_URL = 'https://api.binance.com/api/v3/'

    def __init__(self, symbol):
        self.symbol = symbol

    def fetch_trade_data(self):
        url = f'{self.BASE_URL}trades?symbol={self.symbol}'
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    def process_trade_data(self, trade_data):
        df = pd.DataFrame(trade_data)
        df['price'] = pd.to_numeric(df['price'])
        df['qty'] = pd.to_numeric(df['qty'])
        df['time'] = pd.to_datetime(df['time'], unit='ms')
        return df[['time', 'price', 'qty']]

if __name__ == '__main__':
    binance = BinanceTradeData('BTCUSDT')
    trades = binance.fetch_trade_data()
    df_trades = binance.process_trade_data(trades)
    print(df_trades)