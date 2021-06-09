
# API
import json
from pandas.core.frame import DataFrame
import requests

# Stocks
from stockstats import StockDataFrame
import pandas as pd

# Database
import pymongo

# Database
connection_url = 'mongodb://localhost:27017/'
client = pymongo.MongoClient(connection_url)
database = client.get_database('py-backtester')
stocks_table = database.stocks
stocks_history_table = database.stocks_history

# API
baseUrl = 'https://pselookup.vrymel.com/api/{endpoint}'
ep_stocks = "stocks"
ep_stock_info = "stocks/{symbol}"
ep_stocks_history = "stocks/{symbol}/history/{date}"
ep_stocks_history_range = "stocks/{symbol}/history/{start_date}/{end_date}"


def calculate_indicators(list):
    if list:
        df = pd.DataFrame(list)
        # df.columns = ['id', 'trading_date', 'low',
        #               'open', 'close', 'high', 'volume', 'symbol']
        df.columns = ['trading_date', 'low', 'open',
                    'close', 'high', 'volume', 'symbol']

        sdf = StockDataFrame.retype(df)
        sdf.get('macd')
        sdf.get('atr')
        sdf.get('rsi_14')
        sdf.get('close_5_sma')
        sdf.get('close_10_sma')
        sdf.get('close_20_sma')
        sdf.get('close_50_sma')
        sdf.get('close_100_sma')
        sdf.get('close_5_ema')
        sdf.get('close_10_ema')
        sdf.get('close_20_ema')
        sdf.get('close_50_ema')
        sdf.get('close_100_ema')

        pdframe = pd.DataFrame(sdf)
        result = pdframe.to_json(orient="records")
        parsed = json.loads(result)

        for stock in parsed:
            save_stock_history(stock)


def convert_to_json(data):
    return json.loads(data.content)


def delete_all_stock_history():
    stocks_history_table.delete_many({})
    print("All stocks history deleted")


def delete_all_stock_information():
    stocks_table.delete_many({})
    print("All stocks information deleted")


def delete_stock_history(symbol):
    stocks_history_table.delete({"symbol": symbol})
    print("{symbol} history deleted".format(symbol=symbol))


def delete_stock_information(symbol):
    stocks_table.delete({"symbol": symbol})
    print("{symbol} information deleted".format(symbol=symbol))


def get(endpoint):
    data = requests.get(baseUrl.format(endpoint=endpoint))
    return data


def get_stock_history(symbol, date):
    data = get(ep_stocks_history.format(symbol=symbol, date=date))

    stocks = convert_to_json(data)

    stocks['symbol'] = symbol
    stocks['trading_date'] = stocks['history']['trading_date']
    stocks['open'] = stocks['history']['open']
    stocks['high'] = stocks['history']['high']
    stocks['low'] = stocks['history']['low']
    stocks['close'] = stocks['history']['close']
    stocks['volume'] = stocks['history']['volume']
    del stocks['history']

    print("stock history", stocks)


def get_stock_history_range(symbol, start, end):
    data = get(ep_stocks_history_range.format(
        symbol=symbol, start_date=start, end_date=end))

    stocks = convert_to_json(data)

    for stock in stocks['history']:
        stock['symbol'] = symbol
        stock['trading_date'] = stock['trading_date']
        stock['open'] = stock['open']
        stock['high'] = stock['high']
        stock['low'] = stock['low']
        stock['close'] = stock['close']
        stock['volume'] = stock['volume']

        del stock['timestamp']

        # save_stock_history(stock)
    return stocks['history']


def get_stock_information(symbol):
    data = get(ep_stock_info.format(symbol=symbol))
    stocks = convert_to_json(data)

    print("stock information", stocks)


def get_stock_list():
    # Get /stocks
    data = get(ep_stocks)

    # Convert to Python object
    stocks = json.loads(data.content)

    # Delete stocks
    delete_all_stock_information()

    # Save to database
    save_stocks_information(stocks)


def main():
    # delete_all_stock_information()
    delete_all_stock_history()
    # get_stock_list()
    # get_stock_information("NOW")
    # get_stock_history("NOW", "2020-01-02")
    # get_stock_history_range("NOW", "2015-01-01", "2021-06-07")

    # For retrieving stocks on loop
    for stock in retrieve_all_stocks_information():
        if stock['status'] == 'OPEN':
            calculate_indicators(get_stock_history_range(stock['ticker_symbol'], "2014-01-01", '2021-06-07'))
    
    # For testing calculate indicators
    # calculate_indicators(get_stock_history_range("SRDC", "2015-01-01", "2021-06-07"))


def retrieve_all_stocks_information():
    return list(stocks_table.find())


def retrieve_all_stocks_history():
    return list(stocks_history_table.find())


def retrieve_stocks_information(symbol):
    return list(stocks_table.find({"symbol": symbol}))


def retrieve_stocks_history(symbol):
    return list(stocks_history_table.find({"symbol": symbol}))


def save_stock_history(stock):
    stocks_history_table.insert(stock)
    print("{symbol} {date}saved".format(symbol=stock['symbol'], date=stock['trading_date']))


def save_stocks_information(stocks):
    stocks_table.insert_many(stocks['stocks'])
    print("All stocks saved")


main()
