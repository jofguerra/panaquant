import pandas as pd
import numpy as np
import requests
import json
import time
import os
import datetime as dt
from pandas.io.json import json_normalize


def save_arizet_file_to_dropbox(date):  # For manual input, enter date here
    for confidence_percentage in ['97', '75']:
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
            df.to_csv(os.path.join(save_dir, 'Arizet_{}_{}.csv'.format(confidence_percentage, date)))
            return df

        except:
            print('Arizet data not available')


if __name__ == '__main__':
    data_date = pd.tseries.offsets.BusinessDay(-1)
    ts = dt.datetime.today()
    file_date = data_date + ts
    last_file_date_str = file_date.strftime('%Y-%m-%d')
    save_arizet_file_to_dropbox(last_file_date_str)