# -*- coding: utf-8 -*-
"""
Created on Sun Sep  8 17:23:48 2019

@author: Nanda
"""
import pandas as pd
import numpy as np
import os
import inspect
import ta
import requests
import io
import time
import schedule
import json
from pandas.io.json import json_normalize
from datetime import date
from datetime import datetime

# Set working directory
filename = inspect.getframeinfo(inspect.currentframe()).filename
os.chdir(os.path.dirname(os.path.abspath(filename)))

# Define AlphaVantage Function to download data
def get_data_alphavantage_daily(symbol_list):
    
    # Update API key here
    api_key = 'VVFJ65QADWRFQDEQ'
    
    fun = 'TIME_SERIES_DAILY_ADJUSTED'
    nrow = symbol_list.shape[0]
    
    print('Getting data for ' + str(nrow) + ' symbols')
    
    hist_data_df = {}
    error_tickers = []
    for i in (range(nrow)):
        symbol = symbol_list.loc[:,'ticker'].iloc[i]
        print(str(i+1) + '. ' + symbol)
        
        try:
            url = 'https://www.alphavantage.co/query?function=' + fun + '&symbol=' + symbol + '&outputsize=full' + '&apikey=' + api_key + '&datatype=csv'
            data = requests.get(url)
            hist_data_df[symbol] = pd.read_csv(io.StringIO(data.content.decode('utf8')))
            hist_data_df[symbol] = hist_data_df[symbol].iloc[:100,:].sort_values('timestamp', ascending = True).reset_index(drop=True)

            time.sleep(15)
        
        except:
            pass
            error_tickers = error_tickers.append(symbol)
        
    return hist_data_df, error_tickers

# Get Arizet data
#os.chdir('C:\\Users\\Nanda\\Documents\\Freelance\\Projects\\Stock Pattern Prediction - Phase 2\\Codes\\IBAPI')
def get_arizet(date = '2019-09-27'): # For manual input, enter date here
    symbols_signals = pd.DataFrame()
    try:
        url = 'https://arizet.com/api9/screens/signals_97_prcnt/daily/data?apiKey=dhLUa4Vc6Yr4QqpYhIR7&date=' + date
        data = requests.get(url)
        table = json.loads(data.content.decode('utf8'))
        df = json_normalize(table['Data']['Stocks'])
        df['price'] = df['price'].replace('[\$,]', '', regex=True).astype(float)
        
        # Filter symbols
        # Price > $10
        df_filtered = df.loc[df['price'] >= 10,:]
        # Volume >= 100k
        df_filtered = df_filtered.loc[df['volume'] >= 100000,:]
        # Trades >= 15
        df_filtered = df_filtered.loc[df['trades'] >= 15,:]
        # Days <= 10
        df_filtered = df_filtered.loc[df['days'] <= 10,:]
        
        # Extract signal tickers
        df_tickers = df_filtered.groupby(['ticker', 'signal']).size().unstack()
        df_tickers = df_tickers.loc[np.isnan(df_tickers['Buy']) | np.isnan(df_tickers['Sell']),:]
        df_tickers = df_tickers.sort_values(by=['Buy'], ascending=False)
        
        symbols_buy = df_tickers.loc[df_tickers['Buy'] > 0,:].index.tolist()
        symbols_sell = df_tickers.loc[df_tickers['Sell'] > 0,:].index.tolist()
        symbols_signals_buy = pd.DataFrame({'ticker': symbols_buy, 'signal': ['Buy']*len(symbols_buy)})
        symbols_signals_sell = pd.DataFrame({'ticker': symbols_sell, 'signal': ['Sell']*len(symbols_sell)})
        symbols_signals = symbols_signals_buy.append(symbols_signals_sell).reset_index(drop=True)
        symbols_signals = symbols_signals.merge(df_filtered.loc[:,['ticker', 'price']]).drop_duplicates().reset_index(drop=True)
    except:
        print('Arizet data not available')
        
    return symbols_signals

def filter_arizet():
    try:
        if datetime.now().hour >= 21:
            date_override = False
        else:
            date_override = True # This should be true if manually entering date, else sbould be false
        
        date_today = str(date.today())
        
        if date_override:
            symbols_signals = get_arizet()
        else:
            symbols_signals = get_arizet(date_today)
        
        # Get historical data
        hist_data_df, error_tickers = get_data_alphavantage_daily(symbols_signals)
        if len(error_tickers) > 0:
            print('Some tickers were skipped: ')
            print(', '.join(map(str, error_tickers)))
        
        # Get RSI
        rsi = {}
        nrow = symbols_signals.shape[0]
        for i in range(nrow):
            symbol = symbols_signals.loc[:,'ticker'].iloc[i]
            rsi[symbol] = ta.rsi(hist_data_df[symbol]['adjusted_close'], 14).iloc[-1]
            
        # Get ATR
        atr = {}
        nrow = symbols_signals.shape[0]
        for i in range(nrow):
            symbol = symbols_signals.loc[:,'ticker'].iloc[i]
            atr[symbol] = ta.average_true_range(hist_data_df[symbol]['high'], hist_data_df[symbol]['low'], hist_data_df[symbol]['adjusted_close'], 5).iloc[-1]
        
        # Filter symbols based on RSI
        rsi_df = pd.DataFrame(list(rsi.items()), columns=['ticker', 'RSI'])
        symbols_signals = symbols_signals.merge(rsi_df, on='ticker')
        
        atr_df = pd.DataFrame(list(atr.items()), columns=['ticker', 'ATR'])
        symbols_signals = symbols_signals.merge(atr_df, on='ticker')
        
        select_symbols = symbols_signals.loc[np.logical_or(np.logical_and(symbols_signals['signal'] == 'Sell', symbols_signals['RSI'] > 70),
                                                           np.logical_and(symbols_signals['signal'] == 'Buy', symbols_signals['RSI'] < 30)), :].reset_index(drop=True)
    
        select_symbols.to_csv('stocks_selected.csv', index=False)
        print('Selected stocks successfully saved!')
        
    except:
        print('Error in processing. Please reset kernel and try again')

schedule.every().monday.at("21:30").do(filter_arizet)
schedule.every().tuesday.at("21:30").do(filter_arizet)
schedule.every().wednesday.at("21:30").do(filter_arizet)
schedule.every().thursday.at("21:30").do(filter_arizet)
schedule.every().friday.at("21:30").do(filter_arizet)

while True:
    schedule.run_pending()
    time.sleep(1)