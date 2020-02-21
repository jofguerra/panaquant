import pandas as pd
import numpy as np
import requests
import json
import time
import os
import dropbox
import datetime as dt
from pandas.io.json import json_normalize


class TransferData:
    def __init__(self, access_token):
        self.access_token = access_token

    def upload_file(self, file_from, file_to):
        """upload a file to Dropbox using API v2
        """
        dbx = dropbox.Dropbox(self.access_token)

        with open(file_from, 'rb') as f:
            arizet_path = '/Apps/arizet_data/'
            # try:
            # #    dbx.files_delete(arizet_path + file_to)
            # except:
            #     print('file not found')
            dbx.files_upload(f.read(), arizet_path + file_to, mode=dropbox.dropbox.files.WriteMode.overwrite)


def save_arizet_file_to_dropbox(date, transfer_data):  # For manual input, enter date here
    out_df = pd.DataFrame()
    for confidence_percentage in ['75']:
        dir_path = os.path.dirname(os.path.realpath(__file__))
        save_dir = os.path.join(dir_path, 'new_csv_files', '{}_conf'.format(confidence_percentage))
        try:
            url = 'https://arizet.com/api9/screens/signals_{}_prcnt/' \
                  'daily/data?apiKey=dhLUa4Vc6Yr4QqpYhIR7&date={}'.format(confidence_percentage, date)
            data = requests.get(url)
            print(data)
            table = json.loads(data.content.decode('utf8'))
            df = json_normalize(table['Data']['Stocks'])
            print(df)
            df['price'] = df['price'].replace('[\$,]', '', regex=True).astype(float)
            df['name'] = df['name'].str.replace(',', '')
            file_name = 'Arizet_{}_{}.csv'.format(confidence_percentage, date)
            file_path = os.path.join(save_dir, file_name)
            df.to_csv(file_path)
            out_df = pd.concat([out_df, df])

        except:
            print('Arizet data not available')

    save_dir = os.path.join(dir_path, 'csv_file_history')
    full_file_path = os.path.join(save_dir, 'all_data.csv')
    out_df.to_csv(full_file_path)
    transfer_data.upload_file(full_file_path, 'all_data.csv')
    return out_df


def create_orders(df):
    df['frequency'] = df.groupby('ticker')['ticker'].transform('count')
    df['price'] = df['price'].replace(r'[\$,]', '', regex=True).astype(float)
    df_filtered = df.loc[df['price'] >= 4]
    df_filtered = df_filtered.loc[df_filtered['volume'] >= 1e5]
    df_filtered = df_filtered.loc[df_filtered['trades'] >= 3]
    df_filtered = df_filtered.loc[df_filtered['frequency'] > 5]
    df_filtered['amount'] = 3000 / df['price']

    stocks = set(zip(
        df_filtered['ticker'],
        df_filtered['signal'],
        df_filtered['amount'],
        df_filtered['price']
    ))

    stock_data_dict = [
        {
            'ticker': stock[0],
            'signal': stock[1],
            'quantity': stock[2],
            'price': stock[3]
        }
        for stock in stocks
    ]

    rows = []
    for stock in stock_data_dict:
        rows.append({
            'ticker': stock['ticker'],
            'signal': stock['signal'],
            'quantity': round(stock['quantity'], 2),
            'order_type': 'TRAIL',
            'stop_loss': stock['price'] - 750 if stock['signal'] == 'BUY' else stock['price'] + 750,
            'take_profit': stock['price'] + 320 if stock['signal'] == 'BUY' else stock['price'] - 320,
            'stop_price': stock['price'] + 1 if stock['signal'] == 'BUY' else stock['price'] - 1,
            'trail_amount': round(stock['price'] * 0.01, 2)
        })

    dir_path = os.path.dirname(os.path.realpath(__file__))
    full_file_path = os.path.join(dir_path, 'orders', 'orders.csv')
    date_df = pd.DataFrame(rows)
    date_df.to_csv(full_file_path)


if __name__ == '__main__':
    access_token = '9qwulo5PcvAAAAAAAAAAqQ1FOrcuIuphZxuIs4TQbJhTrWdMdbmFFtyFFVFh2bTR'
    trasfer_data = TransferData(access_token)
    data_date = pd.tseries.offsets.BusinessDay(0)
    ts = dt.datetime.today()
    file_date = data_date + ts
    last_file_date_str = file_date.strftime('%Y-%m-%d')
    arizet_df = save_arizet_file_to_dropbox(last_file_date_str, trasfer_data)
    create_orders(arizet_df)

    # Risk Management en QuantConnect
    # Reducir el DrawDown o Maximize Profit
    # Con stop loss o con RiskManagement Algos de ellos
    # Ordenes
    # 2 Productos en Paper Trading ()


