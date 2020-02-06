import pandas as pd
import numpy as np
import requests
import json
import time
import os
from pandas.io.json import json_normalize


def get_arizet_97(date):  # For manual input, enter date here
    dir_path = os.path.dirname(os.path.realpath(__file__))
    save_dir = os.path.join(dir_path, 'new_csv_files', '97_conf')
    try:
        url = 'https://arizet.com/api9/screens/signals_97_prcnt/daily/data?apiKey=dhLUa4Vc6Yr4QqpYhIR7&date=' + date
        data = requests.get(url)
        print(data)
        table = json.loads(data.content.decode('utf8'))
        df = json_normalize(table['Data']['Stocks'])
        print(df)
        df['price'] = df['price'].replace('[\$,]', '', regex=True).astype(float)
        df.to_csv(os.path.join(save_dir, 'Arizet_97_{}.csv'.format(date)))
        return df

    except:
        print('Arizet data not available')


def get_arizet_75(date='2019-09-27'):
    pass


def get_all_arizet_files():
    symbols_signals = pd.DataFrame()
    date_list = pd.bdate_range(start='9/6/2019', end='2/5/2020').tolist()
    parsed_date_list = [t.strftime('%Y-%m-%d') for t in date_list]
    for date in parsed_date_list:
        get_arizet_97(date)
        time.sleep(5)


if __name__ == "__main__":
    get_all_arizet_files()
