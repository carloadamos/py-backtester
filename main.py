import sys
import time

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


# Test
start_date = "2015-01-01"


def backtest():
    stock_choice = input(
        'Which stock? Type specific stock or type `ALL` to test all stocks: ')
    if (stock_choice == 'ALL' or stock_choice == 'all'):
        print("testing all")
    else:
        stocks = retrieve_stocks_history(stock_choice.upper())

        buy = True
        for stock in stocks:
            converted_start_date = time.strptime(start_date, "%Y-%m-%d")
            converted_trading_date = time.strptime(
                stock['trading_date'], "%Y-%m-%d")

            if (converted_start_date <= converted_trading_date):
                if buy == True:
                    if (stock['close'] >= stock['close_50_sma']) \
                        and (stock['close'] >= stock['close_100_sma']) \
                        and (stock['close_50_sma'] >= stock['close_100_sma']):
                        buy = False
                        print("buy here {date}".format(
                            date=stock['trading_date']))
                else:
                    if (stock['close'] <= stock['close_5_sma']):
                        buy = True
                        print("sold here {date}".format(
                            date=stock['trading_date']))


def calculate_indicators(list):
    if list:
        df = pd.DataFrame(list)
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
    sure = input('Are you sure you want to delete all stock history? [Y/N]: ')

    if (sure == 'y' or sure == 'Y'):
        stocks_history_table.delete_many({})
        print("All stocks history deleted")
    else:
        print("Cancelling delete")


def delete_all_stock_information():
    stocks_table.delete_many({})
    print("All stocks information deleted")


def delete_stock_history(symbol):
    stocks_history_table.delete({"symbol": symbol})
    print("{symbol} history deleted".format(symbol=symbol))


def delete_stock_information(symbol):
    stocks_table.delete({"symbol": symbol})
    print("{symbol} information deleted".format(symbol=symbol))


def fetch_all_open_stock_history():
    sure = input(
        'Are you sure you want to fetch all open stocks history? You may need to delete all history first. [Y/N]: ')

    if (sure == 'y' or sure == 'Y'):
        for stock in retrieve_all_stocks_information():
            if stock['status'] == 'OPEN':
                calculate_indicators(get_stock_history_range(
                    stock['ticker_symbol'], "2014-01-01", '2021-06-07'))
    else:
        print("Cancelling fetch")


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

    return stocks['history']


def get_stock_information(symbol):
    data = get(ep_stock_info.format(symbol=symbol))
    stocks = convert_to_json(data)

    print("stock information", stocks)


def get_stocks_information():
    # Get /stocks
    data = get(ep_stocks)

    # Convert to Python object
    stocks = json.loads(data.content)

    # Save to database
    save_stocks_information(stocks)


def main():
    action = input(
        '\n \
         1 - Backtest \
         2 - Fetch Data \
         3 - Delete All Data\n\n')

    if action == '1':
        backtest()
    elif action == '2':
        fetch_all_open_stock_history()
    elif action == '3':
        delete_all_stock_history()
    else:
        print("not recognized")

    sys.exit(0)


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
    print("{symbol} {date} SAVED!".format(
        symbol=stock['symbol'], date=stock['trading_date']))


def save_stocks_information(stocks):
    stocks_table.insert_many(stocks['stocks'])
    print("All stocks saved")


main()
