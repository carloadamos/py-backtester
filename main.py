import sys
from datetime import datetime, timedelta

# API
import json
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
start_date = "2012-01-01"
capital = 100000
COMM_RATE = 1.20


def backtest():
    all_win_rate = []
    txns = []

    stock_choice = input(
        'Which stock? Type specific stock or type `ALL` to test all stocks: ')

    print('Testing in progress ...')
    if (stock_choice == 'ALL' or stock_choice == 'all'):
        for stock in retrieve_all_stocks_information():
            symbol = stock['ticker_symbol']

            if (stock['status'] == 'OPEN'):
                stocks = retrieve_stocks_history(symbol)
                txns.extend(backtest_start(stocks))
                win_rate = calculate_win_rate(symbol, txns)
                all_win_rate.append(win_rate)

    else:
        stocks = retrieve_stocks_history(stock_choice.upper())
        txns.extend(backtest_start(stocks))
        win_rate = calculate_win_rate(stock_choice.upper(), txns)
        all_win_rate.append(win_rate)

    print('Test complete. See result below')
    print(pd.DataFrame(txns))
    print(pd.DataFrame(all_win_rate))


def backtest_start(stocks):
    txn = {}
    txns = []
    buy = True
    i = 0

    for stock in stocks:
        i += 1
        prev_stock = stocks[i-2]
        converted_start_date = datetime.strptime(start_date, "%Y-%m-%d")
        converted_trading_date = datetime.fromtimestamp(
            stock['trading_date'] / 1e3)

        # CURRENT
        close_price = stock['close']
        high_price = stock['high']
        
        # Start of trading
        if ((converted_start_date <= converted_trading_date)
                and (prev_stock['20dayhigh'] is not None and stock['20dayhigh'] is not None)):

            if buy == True:
                if (stock['20dayhigh'] > prev_stock['20dayhigh']
                        and prev_stock['20dayhigh'] != prev_stock['close']):
                    txn = trade(stock, buy, capital*.1) #10 percent of capital

                    buy = False
            else:
                curr_pnl = compute_profit(txn['buy_price'], stock['close'])
                top_wick = compute_profit(high_price, close_price)

                # Sell
                if (is_crossover(prev_stock, stock, 'close_50_sma', 'close_20_sma')):
                    txn = trade(stock, buy, 5000, txn)
                    pnl = compute_pnl(txn)
                    txn['pnl'] = pnl
                    txn['gl'] = pnl > 0 and 'GAIN' or '----'
                    txn['sold_by'] = 'Crossunder'
                    txns.append(txn)

                    buy = True

                # Stop loss
                elif (curr_pnl <= -5.0):
                    txn = trade(stock, buy, 5000, txn)
                    pnl = compute_pnl(txn)
                    txn['pnl'] = pnl
                    txn['gl'] = pnl > 0 and 'GAIN' or '----'
                    txn['sold_by'] = 'Stoploss'
                    txns.append(txn)

                    buy = True

                elif (stock['10daymin'] < prev_stock['10daymin']
                      and prev_stock['10daymin'] != prev_stock['close']):
                    txn = trade(stock, buy, 5000, txn)
                    pnl = compute_pnl(txn)
                    txn['pnl'] = pnl
                    txn['gl'] = pnl > 0 and 'GAIN' or '----'
                    txn['sold_by'] = '20daymin'
                    txns.append(txn)

                    buy = True

                # Wick stop
                # elif (top_wick <= -10):
                #     txn = trade(stock, buy, 5000, txn)
                #     pnl = compute_pnl(txn)
                #     txn['pnl'] = pnl
                #     txn['gl'] = pnl > 0 and 'GAIN' or '----'
                #     txn['sold_by'] = 'Wickstop'
                #     txns.append(txn)

                #     buy = True

                # Trail stop
                # elif (stock['close'] <= stock['close_10_ema']):
                #     txn = trade(stock, buy, 5000, txn)
                #     pnl = compute_pnl(txn)
                #     txn['pnl'] = pnl
                #     txn['sold_by'] = 'Trailstop'
                #     txns.append(txn)

                #     buy = True

    return txns


def calculate_indicators(list):
    if list:
        df = pd.DataFrame(list)
        df.columns = ['trading_date', 'low', 'open',
                      'close', 'high', 'volume', 'symbol']
        df["200dayhigh"] = df['close'].rolling(window=200).max()
        df["100dayhigh"] = df['close'].rolling(window=100).max()
        df["50dayhigh"] = df['close'].rolling(window=51).max()
        df["20dayhigh"] = df['close'].rolling(window=20).max()
        df["10dayhigh"] = df['close'].rolling(window=10).max()
        df["200daymin"] = df['close'].rolling(window=200).min()
        df["100daymin"] = df['close'].rolling(window=100).min()
        df["50daymin"] = df['close'].rolling(window=51).min()
        df["20daymin"] = df['close'].rolling(window=20).min()
        df["10daymin"] = df['close'].rolling(window=10).min()

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

        return parsed


def calculate_win_rate(symbol, txns):
    df = pd.DataFrame(txns)
    pd.set_option('display.max_rows', None)

    if len(txns) == 0:
        return {
            "symbol": symbol,
            "win_rate": 0,
            "wins": 0,
            "avg_win": 0,
            "max_win": 0,
            "loss": 0,
            "avg_loss": 0,
            "max_loss": 0,
            "total_trade": 0,
            "total": 0,
            "total_capital": 0
        }

    avg_win = 0
    avg_loss = 0
    has_open_position = False
    loss = 0
    total = 0
    total_amount = 0
    valid_txns = len(txns)
    win_rate = 0
    wins = 0
    winning_trade = 0

    # For scenario where:
    # there is only one transaction and
    # it is still open
    try:
        max_loss = df['pnl'].min() < 0 and df['pnl'].min() or 0
        max_win = df['pnl'].max() > 0 and df['pnl'].max() or 0
    except:
        max_loss = 0
        max_win = 0

    for txn in txns:
        try:
            pnl = txn['pnl']
            total += pnl
            total_amount += txn['pnl_amount']
            if pnl > 0:
                winning_trade += 1
                wins += pnl
            else:
                loss += pnl
        except:
            has_open_position = True

        valid_txns = has_open_position and (
            valid_txns - 1) or valid_txns

    if winning_trade != 0:
        win_rate = round(winning_trade/valid_txns * 100, 2)
        avg_win = wins != 0 and round(wins/winning_trade, 2) or 0
        avg_loss = loss != 0 and round(loss/(valid_txns-winning_trade), 2) or 0

    return {
        "symbol": symbol,
        "win_rate": win_rate,
        "wins": winning_trade,
        "avg_win": avg_win,
        "max_win": max_win,
        "loss": valid_txns - winning_trade,
        "avg_loss": avg_loss,
        "max_loss": max_loss,
        "total_trade": valid_txns,
        "total": round(total, 2),
        "total_capital": round(capital, 2),
        "total_pnl_amount": total_amount}


def candle_above(stock, value):
    return (stock['open'] > value and stock['close'] > value)


def compute_pnl(txn):
    return round(compute_profit(txn['buy_price'], txn['sell_price']) - COMM_RATE, 2)


def compute_profit(buy_price, sell_price):
    return (((sell_price - buy_price) / buy_price) * 100)


def convert_date(strDate):
    return datetime.strptime(strDate, "%Y-%m-%d")


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
                stocks = calculate_indicators(get_stock_history_range(
                    stock['ticker_symbol'], "2010-01-01", '2021-06-26'))

                save_stock_history_list(stocks)

    else:
        print("Cancelling fetch")


def fetch_all_stock_history_range(stock, last_save, to_date):
    last_save = convert_date(last_save)
    from_date = last_save + timedelta(days=1)
    to_date = convert_date(to_date)

    sure = input(
        'Are you sure you want to fetch stock history for {stock} from {from_date} to {to_date}? : '.format(stock="ALL", from_date=from_date, to_date=to_date))

    if (sure == 'y' or sure == 'Y'):
        if (last_save < from_date and last_save < to_date):
            from_date = from_date.strftime("%Y-%m-%d")
            to_date = to_date.strftime("%Y-%m-%d")
            for stock in retrieve_all_stocks_information():
                if stock['status'] == 'OPEN':
                    stocks = calculate_indicators(get_stock_history_range(
                        stock['ticker_symbol'], from_date, to_date))

                    save_stock_history_list(stocks)

        else:
            print("Cannot fetch. Issues with date.")
    else:
        print("Cancelling fetch")


def fetch_single_stock_history_range(stock, last_save, to_date):
    last_save = convert_date(last_save)
    from_date = last_save + timedelta(days=1)
    to_date = convert_date(to_date)
    symbol = stock.upper()

    sure = input(
        'Are you sure you want to fetch stock history for {stock} from {from_date} to {to_date}? : '.format(stock=stock.upper(), from_date=from_date, to_date=to_date))

    if (sure == 'y' or sure == 'Y'):
        if (last_save < from_date and last_save < to_date):
            from_date = from_date.strftime("%Y-%m-%d")
            to_date = to_date.strftime("%Y-%m-%d")
            for stock in retrieve_stocks_information(symbol):
                if stock['status'] == 'OPEN':
                    stocks = calculate_indicators(get_stock_history_range(
                        symbol, from_date, to_date))

                    save_stock_history_list(stocks)

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
    print(ep_stocks_history_range.format(
        symbol=symbol, start_date=start, end_date=end))
    data = get(ep_stocks_history_range.format(
        symbol=symbol, start_date=start, end_date=end))

    stocks = convert_to_json(data)

    for stock in stocks['history']:
        stock['symbol'] = symbol
        stock['trading_date'] = datetime.strptime(
            stock['trading_date'], "%Y-%m-%d")
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


def less_than(first_param, second_param):
    return first_param < second_param


def greater_than(first_param, second_param):
    return first_param > second_param


def is_crossover(prev, curr, prop1, prop2):
    return (prev[prop1] <= prev[prop2]) and (curr[prop1] > curr[prop2])


def is_decreasing(prev_stock, curr_stock, param):
    return prev_stock[param] > curr_stock[param]


def is_increasing(prev_stock, curr_stock, param):
    return prev_stock[param] < curr_stock[param]


def is_line_breakout(stock, prop):
    open_price = stock['open']
    close_price = stock['close']

    return open_price < stock[prop] and close_price > stock[prop]


def is_macd_above_macds(stock):
    return stock['macd'] > stock['macds']


def is_macd_crossover(prev_stock, curr_stock):
    if (prev_stock['macd'] <= prev_stock['macds']) \
            and (curr_stock['macd'] > curr_stock['macds']):
        return True

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
         3 - Fetch Data XXX (Range)\
         4 - Fetch Data ALL (Range)\
         5 - Delete All Data\n\n')

    if action == '1':
        backtest()
    elif action == '2':
        fetch_all_open_stock_history()
    elif action == '3':
        symbol = input('Stock symbol: ("XXX"): ')
        last_save = retrieve_last_saved_history(symbol)
        print("\nLast saved stock history {last_save}\n".format(
            last_save=last_save))
        to_date = input('To date: ("YYYY-MM-DD"): ')

        fetch_single_stock_history_range(symbol, last_save, to_date)
    elif action == '4':
        symbol = "NOW"
        last_save = retrieve_last_saved_history(symbol)
        print("\nLast saved stock history {last_save}\n".format(
            last_save=last_save))
        to_date = input('To date: ("YYYY-MM-DD"): ')

        fetch_all_stock_history_range(symbol, last_save, to_date)
    elif action == '5':
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
    return list(stocks_table.find({"ticker_symbol": symbol}))


def retrieve_stocks_history(symbol):
    return list(stocks_history_table.find({"symbol": symbol}).sort("trading_date", pymongo.ASCENDING))


def save_stock_history(stock):
    stocks_history_table.insert_one(stock)
    print("{symbol} {date} SAVED!".format(
        symbol=stock['symbol'], date=stock['trading_date']))


def save_stock_history_list(stocks):
    if (stocks):
        for stock in stocks:
            save_stock_history(stock)


def save_stocks_information(stocks):
    stocks_table.insert_many(stocks['stocks'])
    print("All stocks saved")


def trade(stock, buy, trade_capital=capital, txn={}):
    trade_date = datetime.fromtimestamp(
        stock['trading_date'] / 1e3).strftime("%Y-%m-%d")
    bought_shares = int(trade_capital/stock['close'])

    if buy == True:
        txn = {"code": stock['symbol'], "buy_date": trade_date,
               "buy_price": stock['close'], "bought_shares": bought_shares, "bought_amount": bought_shares * stock['close']}
    else:
        txn["sell_date"] = trade_date
        txn["sell_price"] = stock['close']
        txn["sold_amount"] = txn['bought_shares'] * stock['close']
        txn["pnl_amount"] = abs(txn['bought_amount'] - txn['sold_amount'])

    return txn


main()
