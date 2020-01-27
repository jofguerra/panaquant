# -*- coding: utf-8 -*-
"""
Created on Tue Nov 19 07:52:20 2019

@author: Nanda
"""
import re
import time
import requests
import numpy as np
import pandas as pd
import io
import os
import inspect

LOOKBACK_DAYS = 10


def get_data_alphavantage(symbol_list):
    # Update API key here
    api_key = 'VVFJ65QADWRFQDEQ'
    
    fun = 'TIME_SERIES_DAILY_ADJUSTED'
    nrow = len(symbol_list)
    
    print('Getting data for ' + str(nrow) + ' symbols')
    
    data_df = {}
    price_df = {}
    atr_df = {}
    error_tickers = []
    for i in range(nrow):
        symbol = symbol_list[i]
        print(str(i+1) + '. ' + symbol)
        
        try: 
            data_url = 'https://www.alphavantage.co/query?function=' + fun + '&symbol=' + symbol + '&outputsize=full' + '&apikey=' + api_key + '&datatype=csv'
            data = requests.get(data_url)
            data_df[symbol] = pd.read_csv(io.StringIO(data.content.decode('utf8')))
            price_df[symbol] = data_df[symbol]['adjusted_close'].iloc[:100]
    
            time.sleep(13)
            
            atr_period = 5
            atr_url = 'https://www.alphavantage.co/query?function=ATR&symbol=' + symbol + '&interval=daily&time_period=' + str(atr_period) + '&apikey=' + api_key + '&datatype=csv'
            atr_data = requests.get(atr_url)
            atr_df[symbol] = pd.read_csv(io.StringIO(atr_data.content.decode('utf8')))
            
            time.sleep(13)
            
        except:
            error_tickers.append(symbol)
            pass
    
    return price_df, data_df, atr_df


def green_red(data_df, lookback):
    data_df = data_df.iloc[:lookback,:]
    greens = data_df['close'] > data_df['open']
    reds = data_df['close'] < data_df['open']
    green_count = sum(greens)
    red_count = sum(reds)
    
    return green_count, red_count


def get_project_file_path(file_name):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(dir_path, 'data', file_name)


def update_excel_data():
    """
    New function to update the Excel File
    :return:
    """
    input_excel_path = get_project_file_path('Input.xlsx')
    output_excel_path = get_project_file_path('Output.xlsx')

    input_df = pd.read_excel(input_excel_path, sheet_name='Trade_Data')
    trade_config = pd.read_excel(input_excel_path, sheet_name='Trade_Config')

    symbols = input_df['symbol'].tolist()
    signals = input_df['signal'].tolist()

    price_dfs, data_dfs, atr_dfs = get_data_alphavantage(symbols)

    import pickle
    with open(get_project_file_path('prices.pkl'), 'wb') as f:
        pickle.dump(price_dfs, f)
    with open(get_project_file_path('data.pkl'), 'wb') as f:
        pickle.dump(data_dfs, f)
    with open(get_project_file_path('atr_dfs.pkl'), 'wb') as f:
        pickle.dump(atr_dfs, f)

    # with open(get_project_file_path('prices.pkl'), 'rb') as f:
    #     price_dfs = pickle.load(f)
    # with open(get_project_file_path('data.pkl'), 'rb') as f:
    #     data_dfs = pickle.load(f)
    # with open(get_project_file_path('atr_dfs.pkl'), 'rb') as f:
    #     atr_dfs = pickle.load(f)

    updated_data = []
    for tpl in zip(symbols, signals):
        symbol = tpl[0]
        signal = tpl[1]
        if symbol not in price_dfs:
            continue
        price_series = price_dfs[symbol]
        if symbol not in atr_dfs:
            continue
        atr_df = atr_dfs[symbol]
        price = price_series.iloc[0]
        atr = atr_df['ATR'].iloc[0]
        greens, reds = green_red(data_dfs[symbol], LOOKBACK_DAYS)
        updated_data.append({'signal': signal,
                             'ticker': symbol,
                             'price': price,
                             'atr': atr,
                             'lookback': LOOKBACK_DAYS,
                             'greens': greens,
                             'reds': reds})

    updated_df = pd.DataFrame(updated_data)
    with pd.ExcelWriter(output_excel_path) as writer:
        updated_df.to_excel(writer, sheet_name='New_Trade_Data')

    return symbols, trade_config


def read_finviz_data():
    buy_csv = get_project_file_path('BUY.csv')
    sell_csv = get_project_file_path('SELL.csv')
    buy_df = pd.read_csv(buy_csv)
    sell_df = pd.read_csv(sell_csv)
    columns = ['signal', 'symbol']
    buy_df['signal'] = 'BUY'
    sell_df['signal'] = 'SELL'
    buy_df['symbol'] = buy_df['Ticker']
    sell_df['symbol'] = sell_df['Ticker']
    return buy_df[columns], sell_df[columns]


def create_input_excel(buy_df, sell_df):
    input_df = pd.concat([buy_df, sell_df], ignore_index=True)
    input_excel_path = get_project_file_path('Input.xlsx')
    trade_config = pd.read_excel(input_excel_path, sheet_name='Trade_Config')
    with pd.ExcelWriter(input_excel_path) as writer:
        input_df.to_excel(writer, sheet_name='Trade_Data')
        trade_config.to_excel(writer, sheet_name='Trade_Config')


def get_order_pars():
    buy_df, sell_df = read_finviz_data()
    create_input_excel(buy_df, sell_df)
    _, _ = update_excel_data()

    input_excel_file = get_project_file_path('Input.xlsx')
    output_excel_file = get_project_file_path('Output.xlsx')
    orders_df = pd.read_excel(output_excel_file, sheet_name='New_Trade_Data')
    trade_config_df = pd.read_excel(input_excel_file, sheet_name='Trade_Config')
    trade_config = trade_config_df.to_dict('records')[0]

    orders_df['quantity'] = round(trade_config['order_size']/orders_df['price'])

    orders_df['stop_price'] = np.where(
        orders_df['signal'] == 'BUY',
        orders_df['price'] * 1.035,
        orders_df['price'] * 0.98
    )

    orders_df['trail_amount'] = orders_df['atr'] * 1.2 * trade_config['trail_atr']

    orders_df['stop_loss'] = np.where(
        orders_df['signal'] == 'BUY',
        orders_df['price'] * 0.98,
        orders_df['price'] * 1.035
    )

    orders_df['take_profit'] = np.where(
        orders_df['signal'] == 'BUY',
        orders_df['price'] * 1.02,
        orders_df['price'] * 0.965
    )

    orders_df['order_type'] = trade_config['order_type']

    orders_df = orders_df.round(2)
    orders_file = get_project_file_path('Orders.xlsx')
    with pd.ExcelWriter(orders_file) as writer:
        orders_df.to_excel(writer, sheet_name='Orders')

    return orders_df


if __name__ == '__main__':
    _ = get_order_pars()
