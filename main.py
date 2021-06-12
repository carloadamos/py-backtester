from re import S
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

    txns = []
    i = 0

    stock_choice = input(
        'Which stock? Type specific stock or type `ALL` to test all stocks: ')
    if (stock_choice == 'ALL' or stock_choice == 'all'):
        print("testing all")
    else:
        stocks = retrieve_stocks_history(stock_choice.upper())

        txn = {}
        buy = True
        for stock in stocks:
            i += 1
            converted_start_date = time.strptime(start_date, "%Y-%m-%d")
            converted_trading_date = time.strptime(
                stock['trading_date'], "%Y-%m-%d")

            # Start of trading
            if (converted_start_date <= converted_trading_date):

                if buy == True:
                    if (stock['close'] >= stock['close_50_sma']) \
                            and (stock['close'] >= stock['close_100_sma']) \
                            and (stock['close_50_sma'] >= stock['close_100_sma']):  # \
                        # and is_macd_crossver(stocks[i-2], stock):
                        txn = trade(stock, buy, 5000)

                        buy = False
                else:
                    if (stock['close'] <= stock['close_5_sma']):
                        txn = trade(stock, buy, 5000, txn)
                        pnl = compute_profit(
                            txn['buy_price'], txn['sell_price'])
                        txn['pnl'] = pnl
                        txns.append(txn)

                        buy = True

    for txn in txns:
        print(txn)


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


def compute_profit(buy_price, sell_price):
    return (((sell_price - buy_price) / buy_price) * 100)


def convert_date(strDate):
    return time.strptime(strDate, "%Y-%m-%d")


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


def fetch_open_stock_history_range(stock, from_date, to_date):
    sure = input(
        'Are you sure you want to fetch stock history for {stock} from {from_date} to {to_date}? : '.format(stock=stock, from_date=from_date, to_date=to_date))

    if (sure == 'y' or sure == 'Y'):
        last_save = convert_date(retrieve_last_saved_history(stock))
        from_date = convert_date(from_date)
        to_date = convert_date(to_date)

        if (last_save < from_date and last_save < to_date):
            for stock in retrieve_all_stocks_information():
                if stock['status'] == 'OPEN':
                    calculate_indicators(get_stock_history_range(
                        stock['ticker_symbol'], from_date, to_date))
        else:
            print("Cannot fetch. Issues with date.")
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


def is_macd_crossover(prev_stock, curr_stock):
    if (prev_stock['macd'] <= prev_stock['macds']) \
            and (curr_stock['macd'] > curr_stock['macds']):
        True

    return False


def is_macd_crossunder(prev_stock, curr_stock):
    if (prev_stock['macd'] >= prev_stock['macds']) \
            and (curr_stock['macd'] < curr_stock['macds']):
        True

    return False


def main():
    action = input(
        '\n \
         1 - Backtest \
         2 - Fetch Data \
         3 - Fetch Data (Range)\
         4 - Delete All Data\n\n')

    if action == '1':
        backtest()
    elif action == '2':
        fetch_all_open_stock_history()
    elif action == '3':
        symbol = input('Stock symbol: ("XXX"): ')
        print("Last saved stock history {last_save}".format(
            last_save=retrieve_last_saved_history(symbol)))
        from_date = input('From date: ("YYYY-MM-DD"): ')
        to_date = input('From date: ("YYYY-MM-DD"): ')

        fetch_open_stock_history_range(symbol, from_date, to_date)
    elif action == '4':
        delete_all_stock_history()
    else:
        print("not recognized")

    sys.exit(0)


def retrieve_all_stocks_information():
    return list(stocks_table.find())


def retrieve_all_stocks_history():
    return list(stocks_history_table.find())


def retrieve_last_saved_history(stock):
    return list(stocks_history_table.find({"symbol": stock.upper()}).sort("_id", pymongo.DESCENDING).limit(1))[0]['trading_date']


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


def trade(stock, buy, trade_capital=0, txn={}):
    if buy == True:
        txn = {"code": stock['symbol'], "buy_date": stock['trading_date'],
               "buy_price": stock['close'], "bought_shares": int(trade_capital/stock['close'])}
    else:
        txn["sell_date"] = stock['trading_date']
        txn["sell_price"] = stock['close']

    return txn


main()
